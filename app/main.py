"""FastAPI 应用：分批重启许可 API。"""
from __future__ import annotations

import os

from fastapi import FastAPI, HTTPException, Query, Response

from .logic import evaluate_finish, evaluate_permit_request
from .manifest import load_manifest
from .models import PermitRecord, ReplacementRecord, now_utc, to_iso
from .schemas import LivenessBatchIn, PermitRequestIn, ReplacementRequestIn
from .store import SourceIdMismatch, Store


def create_app(db_path: str, manifest_path: str) -> FastAPI:
    manifest = load_manifest(manifest_path)
    store = Store(db_path)

    app = FastAPI(title="restart-permit-api", version="1.1.0")
    app.state.store = store
    app.state.manifest = manifest

    def permit_view(p: PermitRecord) -> dict:
        return {
            "request_id": p.request_id,
            "nodes": list(p.nodes),
            "status": p.status,
            "approved_at": to_iso(p.approved_at),
            "finished_at": to_iso(p.finished_at) if p.finished_at else None,
            "boot_ids_at_approval": p.boot_ids_at_approval,
        }

    def replacement_view(r: ReplacementRecord) -> dict:
        return {
            "node_id": r.node_id,
            "source_id": r.source_id,
            "registered_at": to_iso(r.registered_at),
            "request_id": r.request_id,
        }

    @app.get("/healthz")
    def healthz():
        return {"status": "ok"}

    @app.get("/manifest")
    def get_manifest():
        return {
            "shards": {
                name: {
                    "voters": list(voters),
                    "majority_needed": manifest.majority_needed(name),
                }
                for name, voters in sorted(manifest.shards.items())
            }
        }

    # ---- 探活报告 ----

    @app.post("/liveness", status_code=201)
    def post_liveness(batch: LivenessBatchIn):
        unknown = sorted({r.node_id for r in batch.reports} - manifest.nodes)
        if unknown:
            raise HTTPException(
                status_code=422,
                detail={"error": "unknown_nodes", "unknown_nodes": unknown},
            )
        try:
            store.add_reports(batch.reports)
        except SourceIdMismatch as exc:
            # 换机节点漏填/填错来源编号：整批拒收，无一入库
            raise HTTPException(
                status_code=422,
                detail={"error": "source_id_mismatch", "problems": exc.problems},
            )
        return {"accepted": len(batch.reports)}

    @app.get("/liveness")
    def get_all_liveness():
        latest = store.latest_reports()
        current = store.current_sources()
        return {
            "nodes": {
                n: {
                    "sampled_at": to_iso(r.sampled_at),
                    "healthy": r.healthy,
                    "boot_id": r.boot_id,
                    "source_id": r.source_id,
                }
                for n, r in sorted(latest.items())
            },
            # 各节点当前生效的探活来源编号；未换机节点不出现
            "current_sources": {
                n: {
                    "source_id": rec.source_id,
                    "registered_at": to_iso(rec.registered_at),
                    "permit": rec.request_id,
                }
                for n, rec in sorted(current.items())
            },
            # 从未上报、或换机后首份带新编号的报告尚未到达的节点
            "nodes_without_reports": sorted(manifest.nodes - latest.keys()),
        }

    @app.get("/liveness/{node_id}")
    def get_node_liveness(node_id: str):
        rep = store.latest_reports().get(node_id)
        if rep is None:
            raise HTTPException(
                status_code=404,
                detail={"error": "no_liveness_report", "node": node_id},
            )
        return {
            "node_id": node_id,
            "sampled_at": to_iso(rep.sampled_at),
            "healthy": rep.healthy,
            "boot_id": rep.boot_id,
            "source_id": rep.source_id,
        }

    # ---- 许可 ----

    @app.post("/permits", status_code=201)
    def request_permit(req: PermitRequestIn, response: Response):
        nodes = sorted(set(req.nodes))  # 规范化：去重排序
        unknown = [n for n in nodes if n not in manifest.nodes]
        if unknown:
            raise HTTPException(
                status_code=422,
                detail={"error": "unknown_nodes", "unknown_nodes": unknown},
            )
        with store.transaction():  # 并发写路径在此串行，按实际生效顺序判定
            existing = store.get_permit(req.request_id)
            if existing is not None:
                if list(existing.nodes) == nodes:
                    # 同号同批重发 -> 返回原许可
                    response.status_code = 200
                    return permit_view(existing)
                raise HTTPException(
                    status_code=409,
                    detail={
                        "error": "request_id_conflict",
                        "message": "request_id 已存在且节点集合不同",
                        "existing_nodes": list(existing.nodes),
                    },
                )
            latest = store.latest_reports()
            ok, detail = evaluate_permit_request(
                manifest, nodes, latest, store.open_permits()
            )
            if not ok:
                raise HTTPException(status_code=409, detail=detail)
            record = PermitRecord(
                request_id=req.request_id,
                nodes=tuple(nodes),
                status="open",
                approved_at=now_utc(),
                finished_at=None,
                boot_ids_at_approval={n: latest[n].boot_id for n in nodes},
            )
            store.insert_permit(record)
            return permit_view(record)

    @app.get("/permits")
    def list_permits(status: str | None = Query(default=None, pattern="^(open|finished)$")):
        return {"permits": [permit_view(p) for p in store.list_permits(status)]}

    @app.get("/permits/{request_id}")
    def get_permit(request_id: str):
        p = store.get_permit(request_id)
        if p is None:
            raise HTTPException(
                status_code=404,
                detail={"error": "permit_not_found", "request_id": request_id},
            )
        return permit_view(p)

    @app.post("/permits/{request_id}/finish")
    def finish_permit(request_id: str):
        with store.transaction():
            p = store.get_permit(request_id)
            if p is None:
                raise HTTPException(
                    status_code=404,
                    detail={"error": "permit_not_found", "request_id": request_id},
                )
            if p.status == "finished":
                raise HTTPException(
                    status_code=409,
                    detail={"error": "permit_already_finished", "request_id": request_id},
                )
            problems = evaluate_finish(
                p,
                store.latest_reports(),
                store.current_sources(),
            )
            if problems:
                raise HTTPException(
                    status_code=409,
                    detail={"error": "finish_conditions_not_met", "nodes": problems},
                )
            store.mark_finished(request_id, now_utc())
            return permit_view(store.get_permit(request_id))

    # ---- 换机登记 ----

    def _replacements_payload(request_id: str) -> dict:
        return {
            "request_id": request_id,
            "replacements": [
                replacement_view(r) for r in store.list_replacements(request_id)
            ],
        }

    @app.post("/permits/{request_id}/replacements", status_code=201)
    def register_replacement(
        request_id: str, req: ReplacementRequestIn, response: Response
    ):
        """凭未结许可为批内节点登记换机，领取新的探活来源编号。

        - 同一许可下重复登记同一节点：返回原编号（200，幂等）；
        - 登记生效后，该节点只受理带当前编号的探活，首份到达前健康未知；
        - 全部写路径共用同一串行事务，并发时按实际生效顺序判定。
        """
        nodes = sorted(set(req.nodes))
        unknown = [n for n in nodes if n not in manifest.nodes]
        if unknown:
            raise HTTPException(
                status_code=422,
                detail={"error": "unknown_nodes", "unknown_nodes": unknown},
            )
        with store.transaction():
            permit = store.get_permit(request_id)
            if permit is None:
                raise HTTPException(
                    status_code=404,
                    detail={"error": "permit_not_found", "request_id": request_id},
                )
            if permit.status != "open":
                raise HTTPException(
                    status_code=409,
                    detail={
                        "error": "permit_not_open",
                        "request_id": request_id,
                        "status": permit.status,
                    },
                )
            outside = [n for n in nodes if n not in permit.nodes]
            if outside:
                raise HTTPException(
                    status_code=422,
                    detail={
                        "error": "nodes_not_in_permit",
                        "request_id": request_id,
                        "permit_nodes": list(permit.nodes),
                        "rejected_nodes": outside,
                    },
                )
            created_any = False
            for n in nodes:
                if store.get_replacement(request_id, n) is not None:
                    continue  # 同许可同节点重复登记：只返回原编号
                store.insert_replacement(request_id, n, now_utc())
                created_any = True
            if not created_any:
                response.status_code = 200
            return _replacements_payload(request_id)

    @app.get("/permits/{request_id}/replacements")
    def list_replacements(request_id: str):
        if store.get_permit(request_id) is None:
            raise HTTPException(
                status_code=404,
                detail={"error": "permit_not_found", "request_id": request_id},
            )
        return _replacements_payload(request_id)

    return app


def create_app_from_env() -> FastAPI:
    return create_app(
        db_path=os.environ.get("DB_PATH", "./data/permits.db"),
        manifest_path=os.environ.get("MANIFEST_PATH", "./manifest.json"),
    )


app = create_app_from_env()

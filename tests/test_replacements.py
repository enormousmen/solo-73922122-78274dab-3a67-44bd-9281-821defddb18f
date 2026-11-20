"""换机登记关键流程自测：

- 登记/幂等/校验
- 旧主机报告整批拒收、新编号报告受理
- 登记后首份报告到达前健康未知
- 结单须有登记后采集的新探活
- 重启后仍按当前编号受理、旧库平滑迁移
- 并发登记与上报按实际生效顺序收敛
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.main import create_app  # noqa: E402

MANIFEST = {
    "shards": {
        "shard-a": ["n1", "n2", "n3"],
        "shard-b": ["n2", "n3", "n4"],
        "shard-c": ["n3", "n4", "n5"],
    }
}
ALL_NODES = ["n1", "n2", "n3", "n4", "n5"]
T0 = datetime.now(timezone.utc) - timedelta(minutes=30)


@pytest.fixture()
def make_client(tmp_path):
    """工厂：同一 db 路径可反复建 app，模拟服务重启。"""
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(MANIFEST))
    db_path = tmp_path / "permits.db"
    clients = []

    def _make():
        app = create_app(db_path=str(db_path), manifest_path=str(manifest_path))
        c = TestClient(app)
        clients.append(c)
        return c

    yield _make
    for c in clients:
        c.close()


@pytest.fixture()
def client(make_client):
    return make_client()


def report(client, node, healthy=True, boot="boot-1", sampled_at=None, source_id=None):
    sampled_at = sampled_at or datetime.now(timezone.utc)
    payload = {
        "node_id": node,
        "sampled_at": sampled_at.isoformat(),
        "healthy": healthy,
        "boot_id": boot,
    }
    if source_id is not None:
        payload["source_id"] = source_id
    return client.post("/liveness", json={"reports": [payload]})


def batch_report(client, reports):
    return client.post("/liveness", json={"reports": reports})


def seed_all_healthy(client, boot="boot-1", sampled_at=T0):
    for n in ALL_NODES:
        r = report(client, n, healthy=True, boot=boot, sampled_at=sampled_at)
        assert r.status_code == 201, r.text


def request_permit(client, request_id, nodes):
    return client.post("/permits", json={"request_id": request_id, "nodes": nodes})


def register(client, request_id, nodes):
    return client.post(
        f"/permits/{request_id}/replacements", json={"nodes": nodes}
    )


@pytest.fixture()
def open_permit(client):
    """一个占用 n1 的未结许可，返回 (许可响应, 客户端)。"""
    seed_all_healthy(client)
    r = request_permit(client, "req-1", ["n1"])
    assert r.status_code == 201, r.text
    return r, client


# ---------- 登记与幂等 ----------


def test_register_replacement_issues_source_id(open_permit):
    _, client = open_permit
    r = register(client, "req-1", ["n1"])
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["request_id"] == "req-1"
    assert len(body["replacements"]) == 1
    rep = body["replacements"][0]
    assert rep["node_id"] == "n1" and rep["source_id"].startswith("src-")
    assert rep["registered_at"]

    # 同一许可下重复登记同一节点 -> 200，只返回原编号
    replay = register(client, "req-1", ["n1"])
    assert replay.status_code == 200
    assert replay.json()["replacements"][0]["source_id"] == rep["source_id"]

    # GET 可查
    got = client.get("/permits/req-1/replacements")
    assert got.status_code == 200
    assert got.json()["replacements"][0]["source_id"] == rep["source_id"]


def test_register_mixed_batch_idempotent_and_new(open_permit):
    _, client = open_permit
    # req-1 只含 n1：先登记 n1
    first = register(client, "req-1", ["n1"]).json()["replacements"][0]
    # 重复 n1 幂等，不产生新编号（即使和别的节点混发，本许可只有 n1 合法）
    again = register(client, "req-1", ["n1", "n1"])
    assert again.status_code == 200
    assert [x["source_id"] for x in again.json()["replacements"]] == [first["source_id"]]


def test_register_validation_errors(open_permit):
    _, client = open_permit

    # 许可不存在
    assert register(client, "nope", ["n1"]).status_code == 404
    assert client.get("/permits/nope/replacements").status_code == 404

    # 节点不在本许可批次内
    r = register(client, "req-1", ["n2"])
    assert r.status_code == 422
    assert r.json()["detail"]["error"] == "nodes_not_in_permit"
    assert r.json()["detail"]["rejected_nodes"] == ["n2"]

    # 清单外节点
    r = register(client, "req-1", ["ghost"])
    assert r.status_code == 422
    assert r.json()["detail"]["unknown_nodes"] == ["ghost"]

    # 许可结单后不可再登记
    report(client, "n1", boot="boot-2", sampled_at=datetime.now(timezone.utc))
    assert client.post("/permits/req-1/finish").status_code == 200
    r = register(client, "req-1", ["n1"])
    assert r.status_code == 409
    assert r.json()["detail"]["error"] == "permit_not_open"


def test_second_replacement_issues_new_source_and_retires_old_one(client):
    """结单后新许可再次换机：发放新编号，旧编号报告不再计入。"""
    seed_all_healthy(client)
    assert request_permit(client, "req-1", ["n1"]).status_code == 201
    src1 = register(client, "req-1", ["n1"]).json()["replacements"][0]["source_id"]

    # 新主机探活，结单
    later = datetime.now(timezone.utc) + timedelta(minutes=5)
    assert report(client, "n1", boot="boot-2", sampled_at=later, source_id=src1).status_code == 201
    assert client.post("/permits/req-1/finish").status_code == 200

    # 再次获批并第二次换机
    assert request_permit(client, "req-2", ["n1"]).status_code == 201
    r = register(client, "req-2", ["n1"])
    assert r.status_code == 201
    src2 = r.json()["replacements"][0]["source_id"]
    assert src2 != src1

    # 旧编号报告拒收
    r = report(client, "n1", boot="boot-3", source_id=src1)
    assert r.status_code == 422
    assert r.json()["detail"]["error"] == "source_id_mismatch"
    # 新编号受理，且查询只看新编号
    assert report(client, "n1", boot="boot-3", source_id=src2).status_code == 201
    got = client.get("/liveness/n1").json()
    assert got["source_id"] == src2 and got["boot_id"] == "boot-3"


# ---------- 报告拒收与健康未知 ----------


def test_old_host_reports_rejected_whole_batch(open_permit):
    _, client = open_permit
    src = register(client, "req-1", ["n1"]).json()["replacements"][0]["source_id"]

    # 漏填来源编号（旧主机口吻）-> 拒收
    r = report(client, "n1", boot="boot-old")
    assert r.status_code == 422
    detail = r.json()["detail"]
    assert detail["error"] == "source_id_mismatch"
    assert detail["problems"][0]["node_id"] == "n1"
    assert detail["problems"][0]["expected_source_id"] == src
    assert detail["problems"][0]["got_source_id"] is None

    # 填错编号 -> 拒收
    r = report(client, "n1", boot="boot-old", source_id="src-wrong")
    assert r.status_code == 422
    assert detail["problems"][0]["expected_source_id"] == src

    # 整批语义：同批里 n2（未换机、本身合法）的新报告也不入库
    n2_before = client.get("/liveness/n2").json()["sampled_at"]
    r = batch_report(client, [
        {"node_id": "n1", "sampled_at": datetime.now(timezone.utc).isoformat(),
         "healthy": True, "boot_id": "boot-old"},
        {"node_id": "n2", "sampled_at": datetime.now(timezone.utc).isoformat(),
         "healthy": True, "boot_id": "boot-new"},
    ])
    assert r.status_code == 422
    # n2 仍是整批拒收前的那份旧报告，未被覆盖
    n2_after = client.get("/liveness/n2").json()
    assert n2_after["sampled_at"] == n2_before and n2_after["boot_id"] == "boot-1"

    # 首份新报告到达前：n1 健康未知
    listing = client.get("/liveness").json()
    assert "n1" in listing["nodes_without_reports"]
    assert client.get("/liveness/n1").status_code == 404

    # 带当前编号的首份报告受理，健康未知解除
    assert report(client, "n1", boot="boot-2", source_id=src).status_code == 201
    got = client.get("/liveness/n1").json()
    assert got["source_id"] == src and got["boot_id"] == "boot-2"
    assert "n1" not in client.get("/liveness").json()["nodes_without_reports"]


def test_non_replaced_node_reports_as_before(open_permit):
    _, client = open_permit
    # n2 未换机：不带编号照常上报
    assert report(client, "n2", boot="boot-1").status_code == 201
    # 未换机节点擅自携带编号 -> 同样按来源不符拒收
    r = report(client, "n2", boot="boot-1", source_id="src-999")
    assert r.status_code == 422
    assert r.json()["detail"]["error"] == "source_id_mismatch"


def test_health_unknown_after_registration_blocks_quorum(client):
    """登记后、首份新探活前，换机节点按健康未知参与多数票扣除。"""
    seed_all_healthy(client)
    assert request_permit(client, "req-1", ["n1"]).status_code == 201
    register(client, "req-1", ["n1"])
    # n1 此刻健康未知；申请 n2 时 shard-a 三票全扣（n1 未知 + n2 本批），不过半
    r = request_permit(client, "req-2", ["n2"])
    assert r.status_code == 409
    shards = {s["shard"]: s for s in r.json()["detail"]["affected_shards"]}
    # shard-a：n1 健康未知且仍被未结许可占用、n2 本批被扣，仅剩 n3 = 1/3，不过半
    assert shards["shard-a"]["available"] == 1
    assert shards["shard-a"]["excluded"]["n1"] == "occupied_by:req-1"
    # 从查询视角，登记后的 n1 确实已无当前探活（健康未知）
    assert "n1" in client.get("/liveness").json()["nodes_without_reports"]


# ---------- 结单 ----------


def test_finish_requires_report_sampled_after_registration(open_permit):
    _, client = open_permit
    reg = register(client, "req-1", ["n1"]).json()["replacements"][0]
    src = reg["source_id"]
    registered_at = datetime.fromisoformat(reg["registered_at"])

    # 编号正确但采样于登记之前 -> 不能结单
    old = registered_at - timedelta(minutes=1)
    assert report(client, "n1", boot="boot-2", sampled_at=old, source_id=src).status_code == 201
    r = client.post("/permits/req-1/finish")
    assert r.status_code == 409
    assert r.json()["detail"]["nodes"] == {"n1": "not_sampled_after_replacement"}

    # 登记后采集、健康、新启动标识 -> 结单
    fresh = registered_at + timedelta(minutes=2)
    assert report(client, "n1", boot="boot-2", sampled_at=fresh, source_id=src).status_code == 201
    r = client.post("/permits/req-1/finish")
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "finished"


def test_finish_batch_with_replaced_and_plain_node(client):
    """同批中换机节点与未换机节点各自满足条件才能结单。"""
    seed_all_healthy(client)
    assert request_permit(client, "req-1", ["n1", "n4"]).status_code == 201
    src = register(client, "req-1", ["n1"]).json()["replacements"][0]["source_id"]

    after = datetime.now(timezone.utc) + timedelta(minutes=5)
    # n4 未换机，普通上报；n1 换新主机带编号上报
    assert report(client, "n4", boot="boot-9", sampled_at=after).status_code == 201
    assert report(client, "n1", boot="boot-2", sampled_at=after, source_id=src).status_code == 201
    assert client.post("/permits/req-1/finish").status_code == 200


# ---------- 重启持久化与旧库迁移 ----------


def test_current_source_enforced_after_restart(make_client):
    c1 = make_client()
    seed_all_healthy(c1)
    assert request_permit(c1, "req-1", ["n1"]).status_code == 201
    src = register(c1, "req-1", ["n1"]).json()["replacements"][0]["source_id"]
    c1.close()

    # 服务沿用原库重启：仍按当前编号受理
    c2 = make_client()
    assert c2.get("/permits/req-1/replacements").json()["replacements"][0]["source_id"] == src
    r = report(c2, "n1", boot="boot-old")  # 旧主机口吻仍拒收
    assert r.status_code == 422
    fresh = datetime.now(timezone.utc) + timedelta(minutes=10)
    assert report(c2, "n1", boot="boot-2", sampled_at=fresh, source_id=src).status_code == 201
    assert c2.post("/permits/req-1/finish").status_code == 200


def test_migrate_legacy_db_without_source_id_column(tmp_path):
    """旧版库（liveness_reports 无 source_id 列）平滑升级，旧报告可见。"""
    db_path = tmp_path / "legacy.db"
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(MANIFEST))
    con = sqlite3.connect(db_path)
    con.executescript(
        """
        CREATE TABLE liveness_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            node_id TEXT NOT NULL,
            sampled_at TEXT NOT NULL,
            healthy INTEGER NOT NULL,
            boot_id TEXT NOT NULL,
            received_at TEXT NOT NULL
        );
        CREATE TABLE permits (
            request_id TEXT PRIMARY KEY, nodes TEXT NOT NULL, status TEXT NOT NULL,
            approved_at TEXT NOT NULL, finished_at TEXT, boot_ids_at_approval TEXT NOT NULL
        );
        """
    )
    con.execute(
        "INSERT INTO liveness_reports VALUES (1,'n1','2026-11-19T10:00:00+00:00',1,'boot-1','2026-11-19T10:00:01+00:00')"
    )
    con.commit()
    con.close()

    app = create_app(db_path=str(db_path), manifest_path=str(manifest_path))
    with TestClient(app) as c:
        got = c.get("/liveness/n1")
        assert got.status_code == 200
        assert got.json()["boot_id"] == "boot-1" and got.json()["source_id"] is None
        # 未换机节点照常上报
        assert report(c, "n1", boot="boot-1").status_code == 201


# ---------- 并发 ----------


def test_concurrent_duplicate_registrations_return_single_source(open_permit):
    _, client = open_permit

    def do_register(_):
        return register(client, "req-1", ["n1"]).json()["replacements"][0]["source_id"]

    with ThreadPoolExecutor(max_workers=8) as pool:
        source_ids = list(pool.map(do_register, range(8)))

    assert set(source_ids) == {source_ids[0]}  # 全部拿到同一个编号
    rows = client.get("/permits/req-1/replacements").json()["replacements"]
    assert len(rows) == 1


def test_concurrent_registration_and_reports_converge(open_permit):
    """登记与无编号上报并发：无论实际生效顺序如何，最终只认当前编号。"""
    _, client = open_permit

    def register_call(_):
        return register(client, "req-1", ["n1"]).status_code

    def stale_report(_):
        return report(client, "n1", boot="boot-old").status_code

    with ThreadPoolExecutor(max_workers=8) as pool:
        reg_codes = list(pool.map(register_call, range(4)))
        report_codes = list(pool.map(stale_report, range(4)))

    assert all(code in (200, 201) for code in reg_codes)
    assert all(code in (201, 422) for code in report_codes)

    # 收敛后：n1 的当前编号已发放，任何无编号报告都不可能成为当前最新探活
    listing = client.get("/liveness").json()
    assert "n1" in listing["nodes_without_reports"]
    assert "n1" not in listing["nodes"]
    src = client.get("/permits/req-1/replacements").json()["replacements"][0]["source_id"]
    assert report(client, "n1", boot="boot-2", source_id=src).status_code == 201

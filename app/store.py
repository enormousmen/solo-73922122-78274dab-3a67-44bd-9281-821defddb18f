"""SQLite 持久化层。

单连接 + 可重入锁：所有读写串行；写事务使用 BEGIN IMMEDIATE，
即使未来跑多进程，也能在数据库层面串行化并发写路径（申请、换机登记、
探活上报、结单），按实际生效顺序判定。
许可、探活报告与换机登记均落盘，服务重启后保留，并继续按当前编号受理。
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
from contextlib import contextmanager
from typing import Iterable

from .models import (
    LatestReport,
    PermitRecord,
    ReplacementRecord,
    now_utc,
    parse_iso,
    to_iso,
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS liveness_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id TEXT NOT NULL,
    sampled_at TEXT NOT NULL,   -- ISO8601 UTC，采样时间
    healthy INTEGER NOT NULL,   -- 1/0
    boot_id TEXT NOT NULL,      -- 启动标识
    received_at TEXT NOT NULL,  -- 服务端接收时间（审计用）
    source_id TEXT              -- 探活来源编号；NULL 表示未换机节点的报告
);
CREATE INDEX IF NOT EXISTS idx_liveness_node_sampled
    ON liveness_reports(node_id, sampled_at);

CREATE TABLE IF NOT EXISTS permits (
    request_id TEXT PRIMARY KEY,          -- 申请号（幂等键）
    nodes TEXT NOT NULL,                  -- JSON 数组，有序去重
    status TEXT NOT NULL CHECK (status IN ('open', 'finished')),
    approved_at TEXT NOT NULL,
    finished_at TEXT,
    boot_ids_at_approval TEXT NOT NULL    -- JSON 对象 node -> boot_id
);

CREATE TABLE IF NOT EXISTS host_replacements (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,    -- 同时构成来源编号 src-{seq}
    request_id TEXT NOT NULL,                 -- 凭以登记的未结许可
    node_id TEXT NOT NULL,                    -- 分片仍沿用原节点名
    source_id TEXT NOT NULL UNIQUE,           -- 每次登记发放的新来源编号
    registered_at TEXT NOT NULL,
    UNIQUE(request_id, node_id)               -- 同许可同节点重复登记走幂等
);
CREATE INDEX IF NOT EXISTS idx_replacements_node
    ON host_replacements(node_id, seq);
"""

# 每个节点只取"当前生效来源"的报告：
# - 换过主机的节点（cur 命中）：只看带当前 source_id 的报告；
# - 未换机节点（cur 未命中）：只看 source_id IS NULL 的报告。
# 节点内按采样时间最新一份；并列时取后写入的一条。
_LATEST_SQL = """
WITH cur AS (
    SELECT h.node_id AS node_id, h.source_id AS source_id
    FROM host_replacements h
    JOIN (
        SELECT node_id, MAX(seq) AS max_seq
        FROM host_replacements GROUP BY node_id
    ) m ON h.node_id = m.node_id AND h.seq = m.max_seq
)
SELECT node_id, sampled_at, healthy, boot_id, source_id FROM (
    SELECT lr.node_id, lr.sampled_at, lr.healthy, lr.boot_id, lr.source_id,
           ROW_NUMBER() OVER (
               PARTITION BY lr.node_id ORDER BY lr.sampled_at DESC, lr.id DESC
           ) AS rn
    FROM liveness_reports lr
    LEFT JOIN cur ON cur.node_id = lr.node_id
    WHERE (cur.node_id IS NULL AND lr.source_id IS NULL)
       OR (cur.node_id IS NOT NULL AND lr.source_id = cur.source_id)
) t
WHERE rn = 1
"""


class SourceIdMismatch(Exception):
    """整批探活报告中存在来源编号缺失/错误，整批拒收（事务回滚，不入库）。"""

    def __init__(self, problems: list[dict]):
        super().__init__("source_id mismatch")
        self.problems = problems


class Store:
    def __init__(self, db_path: str):
        directory = os.path.dirname(os.path.abspath(db_path))
        os.makedirs(directory, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            db_path, check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA busy_timeout=5000")
            self._conn.executescript(SCHEMA)
            self._migrate()

    def _migrate(self) -> None:
        """旧库（无 source_id 列）平滑升级：旧报告来源编号视为 NULL。"""
        cols = {r["name"] for r in self._conn.execute("PRAGMA table_info(liveness_reports)")}
        if "source_id" not in cols:
            self._conn.execute(
                "ALTER TABLE liveness_reports ADD COLUMN source_id TEXT"
            )

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    @contextmanager
    def transaction(self):
        """写事务：BEGIN IMMEDIATE 立即拿写锁，并发写路径在此串行。"""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
            self._conn.execute("COMMIT")

    # ---- 探活报告 ----

    def add_reports(self, reports: Iterable) -> None:
        """整批写入；任一报告来源编号不符即抛 SourceIdMismatch，整批回滚。"""
        reports = list(reports)
        received = to_iso(now_utc())
        with self.transaction():
            current = self.current_sources()
            problems: list[dict] = []
            for r in reports:
                cur = current.get(r.node_id)
                expected = cur.source_id if cur is not None else None
                if r.source_id != expected:
                    problems.append(
                        {
                            "node_id": r.node_id,
                            "expected_source_id": expected,
                            "got_source_id": r.source_id,
                        }
                    )
            if problems:
                # 事务由 contextmanager 回滚：整批报告都不入库
                raise SourceIdMismatch(problems)
            rows = [
                (
                    r.node_id,
                    to_iso(r.sampled_at),
                    1 if r.healthy else 0,
                    r.boot_id,
                    received,
                    r.source_id,
                )
                for r in reports
            ]
            self._conn.executemany(
                "INSERT INTO liveness_reports "
                "(node_id, sampled_at, healthy, boot_id, received_at, source_id) "
                "VALUES (?,?,?,?,?,?)",
                rows,
            )

    def latest_reports(self) -> dict[str, LatestReport]:
        with self._lock:
            rows = self._conn.execute(_LATEST_SQL).fetchall()
        return {
            r["node_id"]: LatestReport(
                node_id=r["node_id"],
                sampled_at=parse_iso(r["sampled_at"]),
                healthy=bool(r["healthy"]),
                boot_id=r["boot_id"],
                source_id=r["source_id"],
            )
            for r in rows
        }

    # ---- 许可 ----

    @staticmethod
    def _row_to_permit(row: sqlite3.Row) -> PermitRecord:
        return PermitRecord(
            request_id=row["request_id"],
            nodes=tuple(json.loads(row["nodes"])),
            status=row["status"],
            approved_at=parse_iso(row["approved_at"]),
            finished_at=parse_iso(row["finished_at"]) if row["finished_at"] else None,
            boot_ids_at_approval=json.loads(row["boot_ids_at_approval"]),
        )

    def get_permit(self, request_id: str) -> PermitRecord | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM permits WHERE request_id = ?", (request_id,)
            ).fetchone()
        return self._row_to_permit(row) if row else None

    def list_permits(self, status: str | None = None) -> list[PermitRecord]:
        with self._lock:
            if status:
                rows = self._conn.execute(
                    "SELECT * FROM permits WHERE status = ? ORDER BY approved_at, request_id",
                    (status,),
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM permits ORDER BY approved_at, request_id"
                ).fetchall()
        return [self._row_to_permit(r) for r in rows]

    def open_permits(self) -> list[PermitRecord]:
        return self.list_permits(status="open")

    def insert_permit(self, p: PermitRecord) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO permits "
                "(request_id, nodes, status, approved_at, finished_at, boot_ids_at_approval) "
                "VALUES (?,?,?,?,?,?)",
                (
                    p.request_id,
                    json.dumps(list(p.nodes)),
                    p.status,
                    to_iso(p.approved_at),
                    to_iso(p.finished_at) if p.finished_at else None,
                    json.dumps(p.boot_ids_at_approval),
                ),
            )

    def mark_finished(self, request_id: str, finished_at) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE permits SET status = 'finished', finished_at = ? "
                "WHERE request_id = ?",
                (to_iso(finished_at), request_id),
            )

    # ---- 换机登记 ----

    @staticmethod
    def _row_to_replacement(row: sqlite3.Row) -> ReplacementRecord:
        return ReplacementRecord(
            seq=row["seq"],
            request_id=row["request_id"],
            node_id=row["node_id"],
            source_id=row["source_id"],
            registered_at=parse_iso(row["registered_at"]),
        )

    def current_sources(self) -> dict[str, ReplacementRecord]:
        """每个节点当前生效的换机登记（seq 最大的一条），未换机节点不在 dict 中。"""
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT h.* FROM host_replacements h
                JOIN (
                    SELECT node_id, MAX(seq) AS max_seq
                    FROM host_replacements GROUP BY node_id
                ) m ON h.node_id = m.node_id AND h.seq = m.max_seq
                """
            ).fetchall()
        return {r["node_id"]: self._row_to_replacement(r) for r in rows}

    def get_replacement(self, request_id: str, node_id: str) -> ReplacementRecord | None:
        """同一许可下该节点的既有登记（幂等重放时返回原编号）。"""
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM host_replacements WHERE request_id = ? AND node_id = ?",
                (request_id, node_id),
            ).fetchone()
        return self._row_to_replacement(row) if row else None

    def insert_replacement(
        self, request_id: str, node_id: str, registered_at
    ) -> ReplacementRecord:
        """发放新的探活来源编号。调用方须已持有写事务。"""
        row = self._conn.execute(
            "SELECT COALESCE(MAX(seq), 0) + 1 AS next_seq FROM host_replacements"
        ).fetchone()
        next_seq = row["next_seq"]
        source_id = f"src-{next_seq}"
        self._conn.execute(
            "INSERT INTO host_replacements "
            "(seq, request_id, node_id, source_id, registered_at) "
            "VALUES (?,?,?,?,?)",
            (
                next_seq,
                request_id,
                node_id,
                source_id,
                to_iso(registered_at),
            ),
        )
        return ReplacementRecord(
            seq=next_seq,
            request_id=request_id,
            node_id=node_id,
            source_id=source_id,
            registered_at=registered_at,
        )

    def list_replacements(self, request_id: str) -> list[ReplacementRecord]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM host_replacements WHERE request_id = ? ORDER BY seq",
                (request_id,),
            ).fetchall()
        return [self._row_to_replacement(r) for r in rows]

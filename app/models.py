"""领域模型与时间工具。所有时间统一为 UTC 的 aware datetime。"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def to_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def parse_iso(s: str) -> datetime:
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


@dataclass(frozen=True)
class LatestReport:
    """某节点当前生效的一份探活报告。

    换过主机的节点只统计带当前 source_id 的报告，因此这里也记录其来源编号；
    未换过主机的节点（报告来源列为空）source_id 为 None。
    """

    node_id: str
    sampled_at: datetime
    healthy: bool
    boot_id: str
    source_id: str | None = None


@dataclass(frozen=True)
class PermitRecord:
    """一条许可。status: open（未结束）| finished（已结束）。"""

    request_id: str
    nodes: tuple[str, ...]  # 有序、去重
    status: str
    approved_at: datetime
    finished_at: datetime | None
    boot_ids_at_approval: dict[str, str]  # 获批时各节点最新报告中的启动标识


@dataclass(frozen=True)
class ReplacementRecord:
    """一次节点换机登记。

    节点名不变（分片仍沿用原节点名），每次登记发放新的探活来源编号；
    同一许可下对同一节点重复登记只返回原记录（幂等）。
    """

    seq: int  # 表主键，同时构成来源编号 src-{seq}
    request_id: str  # 凭以登记的未结许可申请号
    node_id: str
    source_id: str
    registered_at: datetime

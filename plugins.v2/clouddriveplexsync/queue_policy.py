"""Hold old automatic jobs when switching to newly observed content only."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import asdict

from .event_worker import EventWorker
from .history_models import TTDHistoryMode
from .history_policy import HistoryPolicy
from .models import PendingScan

PENDING_QUEUE_KEY = "pending_scans"
HELD_HISTORY_KEY = "held_history_scans"
AUTOMATIC_SOURCES = frozenset({"ttd"})


async def read_queue(
    load: Callable[[str], object], key: str
) -> list[dict[str, object]]:
    raw = await asyncio.to_thread(load, key)
    if raw is None:
        return []
    if not isinstance(raw, list) or any(not isinstance(item, dict) for item in raw):
        raise TypeError("持久化扫描队列格式无效，已拒绝启动")
    return [asdict(PendingScan.from_dict(item)) for item in raw]


def is_automatic(item: dict[str, object]) -> bool:
    sources = item.get("sources")
    return (
        isinstance(sources, list)
        and bool(sources)
        and all(source in AUTOMATIC_SOURCES for source in sources)
    )


async def prepare_scan_queue(
    *,
    worker: EventWorker,
    policy: HistoryPolicy,
    load: Callable[[str], object],
    save: Callable[[str, object], None],
) -> int:
    pending = await read_queue(load, PENDING_QUEUE_KEY)
    held = await read_queue(load, HELD_HISTORY_KEY)
    if policy.needs_baseline:
        automatic = [item for item in pending if is_automatic(item)]
        pending = [item for item in pending if not is_automatic(item)]
        combined = {
            (item["section_id"], item["plex_path"]): item for item in held + automatic
        }
        held = list(combined.values())
        if automatic:
            await asyncio.to_thread(save, HELD_HISTORY_KEY, held)
        worker.load_queue(pending)
        await worker.persist_queue_state()
        worker.record_info(
            f"仅新增模式：已保留 {len(held)} 个历史自动扫描任务，不向 Plex 提交；手动任务继续处理"
        )
        return len(held)
    if policy.mode == TTDHistoryMode.INCLUDE_HISTORY and held:
        worker.load_queue(pending + held)
        await worker.persist_queue_state()
        await asyncio.to_thread(save, HELD_HISTORY_KEY, [])
        worker.record_info(f"历史处理模式：已恢复 {len(held)} 个保留的扫描任务")
        return 0
    worker.load_queue(pending)
    return len(held)

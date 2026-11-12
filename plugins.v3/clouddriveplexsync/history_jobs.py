"""Move historical jobs between active and held queues without dropping work."""

from __future__ import annotations

from dataclasses import asdict
from typing import TYPE_CHECKING

from .models import HISTORY_SOURCE, PendingScan
from .queue_storage import persist_values
from .scan_queue import ScanCoalescer

if TYPE_CHECKING:
    from .event_worker import EventWorker


def is_history_only(item: PendingScan) -> bool:
    return bool(item.sources) and set(item.sources) == {HISTORY_SOURCE}


async def hold_pending_history(worker: EventWorker) -> int:
    async with worker._persistence_lock:
        removed = {
            key: item
            for key, item in worker.coalescer.pending.items()
            if is_history_only(item)
        }
        removed_inflight = {
            key: item for key, item in worker.inflight.items() if is_history_only(item)
        }
        previous_held = dict(worker.held_history)
        held = ScanCoalescer(
            worker.coalescer.debounce_seconds,
            max(
                worker.coalescer.capacity,
                len(previous_held) + len(removed) + len(removed_inflight),
            ),
        )
        held.load(
            [asdict(item) for item in previous_held.values()]
            + [asdict(item) for item in removed_inflight.values()]
            + [asdict(item) for item in removed.values()]
        )
        worker.held_history = held.pending
        for key in removed:
            worker.coalescer.pending.pop(key)
        for key in removed_inflight:
            worker.inflight.pop(key)
        for item in worker.coalescer.pending.values():
            item.sources = [
                source for source in item.sources if source != HISTORY_SOURCE
            ]
        try:
            await persist_values(
                worker.persist_held,
                [asdict(item) for item in worker.held_history.values()],
            )
        except BaseException:
            worker.held_history = previous_held
            for item in list(removed_inflight.values()) + list(removed.values()):
                if not worker.coalescer.add(item):
                    worker.coalescer.pending[worker.coalescer.key_for(item)] = item
            raise
        await persist_values(worker.persist_queue, worker.queue_snapshot())
        return len(worker.held_history)


async def restore_held_history(worker: EventWorker) -> int:
    async with worker._persistence_lock:
        candidate = ScanCoalescer(
            worker.coalescer.debounce_seconds, worker.coalescer.capacity
        )
        candidate.load(
            worker.coalescer.snapshot()
            + [asdict(item) for item in worker.held_history.values()]
        )
        worker.coalescer.pending = candidate.pending
        await persist_values(worker.persist_queue, worker.queue_snapshot())
        restored = len(worker.held_history)
        worker.held_history.clear()
        await persist_values(worker.persist_held, [])
        return restored

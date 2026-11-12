"""Persist and log newly admitted Plex directory scan jobs."""

from __future__ import annotations

from typing import TYPE_CHECKING

from .models import MediaHint, PendingScan, PlexTarget, ScanPathKind
from .scan_logging import ScanStage, format_scan_log

if TYPE_CHECKING:
    from .event_worker import EventWorker


async def enqueue_target(
    worker: EventWorker,
    target: PlexTarget,
    *,
    cloud_path: str,
    source: str,
    path_kind: ScanPathKind,
    media: list[MediaHint] | None = None,
) -> bool:
    now = worker.clock()
    accepted = worker.coalescer.add(
        PendingScan(
            cloud_path=cloud_path,
            plex_path=target.path,
            section_id=target.section_id,
            section_title=target.section_title,
            first_seen=now,
            last_seen=now,
            sources=[source],
            path_kind=path_kind,
            ready_at=now if path_kind == "plex" else None,
            media=media or [],
        )
    )
    if not accepted:
        worker._record_error("扫描队列已满，记录将保留并重试")
        return False
    queued_path = worker.coalescer.covering_path(target, source, path_kind=path_kind)
    await worker._save_queue()
    worker.record_info(
        format_scan_log(
            stage=ScanStage.QUEUED,
            cloud_path=cloud_path,
            target=target,
            sources=[source],
            queued_path=queued_path,
        )
    )
    worker.flush_signal.set()
    return True

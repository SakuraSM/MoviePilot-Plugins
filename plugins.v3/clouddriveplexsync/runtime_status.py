"""Build a status snapshot even when Plex or the activity store is unavailable."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from .history_models import TTDHistoryMode

if TYPE_CHECKING:
    from .runtime import SyncRuntime

PLEX_STATUS_TIMEOUT_SECONDS = 5.0


async def _activity_status(runtime: SyncRuntime) -> dict[str, object]:
    result: dict[str, object] = {
        "activities": [],
        "indexed_media": [],
        "pending_media": [],
        "pending_verifications": 0,
        "alternative_media": [],
        "alternative_verifications": 0,
    }
    if runtime.activity:
        try:
            result["activities"] = [
                entry.to_dict() for entry in await runtime.activity.recent()
            ]
            result["indexed_media"] = [
                entry.to_dict() for entry in await runtime.activity.indexed()
            ]
            result["pending_media"] = [
                entry.to_dict() for entry in await runtime.activity.pending()
            ]
            result["pending_verifications"] = await runtime.activity.pending_count()
            result["alternative_media"] = [
                entry.to_dict() for entry in await runtime.activity.alternatives()
            ]
            result[
                "alternative_verifications"
            ] = await runtime.activity.alternative_count()
        except Exception as error:  # noqa: BLE001 - fall back to recent in-memory activity.
            message = f"读取日志与入库记录失败：{type(error).__name__}"
            if message != runtime.activity_error:
                runtime._report_activity_error(message)
    result["activity_error"] = runtime.activity_error
    return result


async def build_runtime_status(runtime: SyncRuntime) -> dict[str, object]:
    try:
        plex = await asyncio.wait_for(
            asyncio.to_thread(runtime.plex.describe),
            timeout=PLEX_STATUS_TIMEOUT_SECONDS,
        )
    except Exception as error:  # noqa: BLE001 - logs remain visible when Plex is offline.
        plex = {"error": type(error).__name__}
    result: dict[str, object] = {
        "ready": runtime.ready.is_set(),
        "pending_scans": len(runtime.worker.coalescer.pending),
        "inflight_scans": len(runtime.worker.inflight),
        "history_mode": (
            TTDHistoryMode.INCLUDE_HISTORY.value
            if runtime.history and runtime.history.enabled
            else TTDHistoryMode.NEW_ONLY.value
        ),
        "history_processing": runtime.history.status() if runtime.history else {},
        "held_history_scans": len(runtime.worker.held_history),
        "last_scan_at": runtime.worker.last_scan_at,
        "last_error": runtime.worker.last_error
        or (runtime.poller.status.last_error if runtime.poller else "")
        or (runtime.history.poller.status.last_error if runtime.history else ""),
        "recent": list(runtime.worker.recent),
        "stats": runtime.stats.to_dict(),
        "ttd": runtime.poller.status.to_dict() if runtime.poller else {},
        "plex": plex,
    }
    result.update(await _activity_status(runtime))
    return result

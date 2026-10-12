"""Run the history consumer and durable Plex scan queue."""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Callable
from pathlib import Path

from .configuration import PluginSettings
from .event_worker import EventWorker
from .history_models import TTDCursor, TTDSubmitResult
from .history_poller import TTDHistoryPoller
from .history_store import HistoryStore
from .models import PluginStats
from .path_mapper import is_under
from .plex_bridge import PlexBridge, PlexLibraryNotFoundError
from .ttd_client import TTDClient


class SyncRuntime:
    def __init__(
        self,
        *,
        settings: PluginSettings,
        service: object,
        data_path: Path,
        load: Callable[[str], object],
        save: Callable[[str, object], None],
        on_error: Callable[[str], None],
        on_info: Callable[[str], None] | None = None,
    ) -> None:
        self.settings = settings
        self.stats = PluginStats()
        self.plex = PlexBridge(service, settings.plex_sections, self.stats)
        self.worker = EventWorker(
            settings.mapper,
            self.plex,
            debounce_seconds=settings.debounce_seconds,
            capacity=settings.queue_capacity,
            scans_per_second=settings.scans_per_second,
            stats=self.stats,
            persist_queue=lambda values: save("pending_scans", values),
            on_error=on_error,
            on_info=on_info,
        )
        self.data_path, self.load, self.save = data_path, load, save
        self.on_error = on_error
        self.stop = asyncio.Event()
        self.ready = asyncio.Event()
        self.poller: TTDHistoryPoller | None = None

    async def submit_directory(self, path: str, source: str) -> TTDSubmitResult:
        if any(is_under(path, prefix) for prefix in self.settings.skip_paths):
            return TTDSubmitResult.skipped("命中配置的跳过路径")
        try:
            accepted = await self.worker.submit_scan_directory(path, source)
            return (
                TTDSubmitResult.queued()
                if accepted
                else TTDSubmitResult.retry("扫描队列已满")
            )
        except PlexLibraryNotFoundError as error:
            if self.settings.unmatched_policy == "skip":
                return TTDSubmitResult.skipped(str(error))
            return TTDSubmitResult.retry(str(error))
        except Exception as error:  # noqa: BLE001 - external Plex failures must leave history retryable.
            return TTDSubmitResult.retry(str(error))

    async def run(self) -> None:
        await asyncio.to_thread(self.plex.assert_supported)
        pending = self.load("pending_scans") or []
        if not isinstance(pending, list) or any(
            not isinstance(item, dict) for item in pending
        ):
            raise ValueError("持久化扫描队列格式无效，已拒绝启动")
        self.worker.load_queue(pending)
        client = TTDClient(
            self.settings.url,
            source=self.settings.source,
            api_token=self.settings.api_token,
            page_size=self.settings.page_size,
            verify=self.settings.verify_tls,
            stats=self.stats,
        )
        scope = hashlib.sha256(
            f"{client.base_url}|{client.source}|{self.settings.mapper.target_root}".encode()
        ).hexdigest()
        store: HistoryStore | None = None
        tasks: list[asyncio.Task[None]] = []
        try:
            store = await asyncio.to_thread(
                HistoryStore, self.data_path / "history.sqlite3", scope
            )
            cursor = TTDCursor.from_dict(self.load("ttd_cursor"))
            self.poller = TTDHistoryPoller(
                client,
                self.submit_directory,
                self.settings.mapper.target_root,
                poll_seconds=self.settings.poll_seconds,
                max_pages=self.settings.max_pages,
                overlap_pages=self.settings.overlap_pages,
                reconcile_hours=self.settings.reconcile_hours,
                cursor=cursor,
                store=store,
                persist_cursor=lambda value: self.save("ttd_cursor", value),
                on_error=self.on_error,
                on_skip=self.worker.record_info,
                stats=self.stats,
            )
            tasks = [
                asyncio.create_task(self.worker.run(self.stop)),
                asyncio.create_task(self.poller.run(self.stop)),
            ]
            stop_task = asyncio.create_task(self._wait_stop())
            tasks.append(stop_task)
            self.ready.set()
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                if task is not stop_task:
                    task.result()
                    raise RuntimeError("同步后台任务意外退出")
        finally:
            self.ready.clear()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await client.close()
            if store:
                await store.close()

    async def _wait_stop(self) -> None:
        await self.stop.wait()

    async def status(self) -> dict[str, object]:
        return {
            "ready": self.ready.is_set(),
            "pending_scans": len(self.worker.coalescer.pending),
            "inflight_scans": len(self.worker.inflight),
            "last_scan_at": self.worker.last_scan_at,
            "last_error": self.worker.last_error
            or (self.poller.status.last_error if self.poller else ""),
            "recent": list(self.worker.recent),
            "stats": self.stats.to_dict(),
            "ttd": self.poller.status.to_dict() if self.poller else {},
            "plex": await asyncio.to_thread(self.plex.describe),
        }

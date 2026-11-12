"""Durable directory queue and path-scoped Plex scan worker."""

from __future__ import annotations

import asyncio
import time
from collections import deque
from collections.abc import Awaitable, Callable, Iterable, Sequence
from dataclasses import asdict

from .activity_log import ActivityEntry
from .history_jobs import hold_pending_history, is_history_only, restore_held_history
from .history_models import TTDHistoryRecord
from .media_hints import build_media_hint, current_media_hints
from .models import (
    HISTORY_SOURCE,
    MediaHint,
    PendingScan,
    PlexTarget,
    PluginStats,
)
from .path_mapper import PathMapper, PathMappingError
from .plex_bridge import PlexBridge, PlexLibraryNotFoundError
from .queue_storage import persist_values
from .scan_logging import ScanStage, format_scan_log
from .scan_queue import ScanCoalescer
from .scan_submission import enqueue_target


class EventWorker:
    def __init__(
        self,
        mapper: PathMapper,
        plex: PlexBridge,
        *,
        debounce_seconds: int = 30,
        capacity: int = 10_000,
        scans_per_second: float = 1.0,
        stats: PluginStats | None = None,
        persist_queue: Callable[[list[dict[str, object]]], None] | None = None,
        on_error: Callable[[str], None] | None = None,
        on_info: Callable[[str], None] | None = None,
        persist_held: Callable[[list[dict[str, object]]], None] | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.mapper = mapper
        self.plex = plex
        self.coalescer = ScanCoalescer(debounce_seconds, capacity)
        self.scan_interval = 1.0 / max(0.1, scans_per_second)
        self.stats = stats
        self.persist_queue = persist_queue
        self.on_error = on_error
        self.on_info = on_info
        self.persist_held = persist_held
        self.history_enabled = False
        self.held_history: dict[str, PendingScan] = {}
        self._flush_task: asyncio.Task[int] | None = None
        self._active_item: PendingScan | None = None
        self._history_pausing = False
        self.clock = clock
        self.flush_signal = asyncio.Event()
        self.recent: deque[str] = deque(maxlen=100)
        self.inflight: dict[str, PendingScan] = {}
        self._flush_lock = asyncio.Lock()
        self._persistence_lock = asyncio.Lock()
        self.on_activity: Callable[[ActivityEntry], None] | None = None
        self.on_submitted: (
            Callable[[PlexTarget, list[MediaHint]], Awaitable[None]] | None
        ) = None
        self.last_scan_at = 0.0
        self.last_error = ""

    async def _save_queue(self) -> None:
        async with self._persistence_lock:
            await persist_values(self.persist_queue, self.queue_snapshot())

    def queue_snapshot(self) -> list[dict[str, object]]:
        return [
            asdict(item) for item in self.inflight.values()
        ] + self.coalescer.snapshot()

    def load_held_history(self, values: Iterable[dict[str, object]]) -> None:
        for value in values:
            item = PendingScan.from_dict(value)
            item.sources = [HISTORY_SOURCE]
            self.held_history[self.coalescer.key_for(item)] = item

    async def pause_history(self) -> int:
        self.history_enabled = False
        self._history_pausing = True
        try:
            if (
                self._flush_task
                and self._active_item
                and is_history_only(self._active_item)
            ):
                self._flush_task.cancel()
                await asyncio.gather(self._flush_task, return_exceptions=True)
            return await hold_pending_history(self)
        finally:
            self._history_pausing = False

    async def restore_history(self) -> int:
        return await restore_held_history(self)

    def load_queue(self, values: Iterable[dict[str, object]]) -> None:
        self.coalescer.load(values)

    async def persist_queue_state(self) -> None:
        await self._save_queue()

    def _record_error(self, message: str) -> None:
        should_report = message != self.last_error
        self.last_error = message
        self.recent.append(f"ERROR {message}")
        if self.on_activity and should_report:
            self.on_activity(ActivityEntry(self.clock(), "error", message))
        if should_report and self.on_error:
            self.on_error(message)

    def record_info(self, message: str) -> None:
        self.recent.append(message)
        if self.on_activity:
            self.on_activity(ActivityEntry(self.clock(), "info", message))
        if self.on_info:
            self.on_info(message)

    def record_external_error(self, message: str) -> None:
        self.recent.append(f"ERROR {message}")
        if self.on_activity:
            self.on_activity(ActivityEntry(self.clock(), "error", message))

    async def submit_scan_directory(
        self,
        cloud_directory: str,
        source: str = "manual",
        *,
        records: Sequence[TTDHistoryRecord] = (),
    ) -> bool:
        if source == HISTORY_SOURCE and not self.history_enabled:
            return False
        path = self.mapper.validate_cloud_path(cloud_directory)
        target = await asyncio.to_thread(
            self.plex.find_target, self.mapper.cloud_to_plex(path)
        )
        if source == HISTORY_SOURCE and not self.history_enabled:
            return False
        media = list(
            dict.fromkeys(
                hint
                for record in records
                if (hint := build_media_hint(self, path, target.path, record))
            )
        )
        return await enqueue_target(
            self, target, cloud_path=path, source=source, path_kind="cloud", media=media
        )

    async def submit_plex_scan_directory(self, plex_directory: str) -> bool:
        path = plex_directory.strip().replace("\\", "/")
        if not path.startswith("/"):
            raise PathMappingError("请输入 Plex 容器中的绝对目录路径")
        try:
            target = await asyncio.to_thread(self.plex.find_target, path)
        except PlexLibraryNotFoundError as error:
            raise PlexLibraryNotFoundError(
                f"目录不属于所选 Plex 媒体库：{path}"
            ) from error
        return await enqueue_target(
            self, target, cloud_path="", source="manual", path_kind="plex"
        )

    async def flush(self, force: bool = False) -> int:
        async with self._flush_lock:
            self._flush_task = asyncio.create_task(self._flush(force))
            try:
                return await self._flush_task
            except asyncio.CancelledError:
                current = asyncio.current_task()
                if self._history_pausing and current and not current.cancelling():
                    return 0
                raise
            finally:
                self._flush_task = None

    async def _flush(self, force: bool) -> int:
        items = self.coalescer.due(self.clock(), force=force)
        paused = [
            item for item in items if is_history_only(item) and not self.history_enabled
        ]
        items = [
            item
            for item in items
            if not (is_history_only(item) and not self.history_enabled)
        ]
        for item in paused:
            self.coalescer.add(item)
        if not items:
            return 0
        for item in items:
            item.state = "inflight"
            self.inflight[self.coalescer.key_for(item)] = item
        scanned = 0
        try:
            await self._save_queue()
            for item in items:
                if self.inflight.get(self.coalescer.key_for(item)) is not item:
                    continue
                self._active_item = item
                try:
                    if is_history_only(item) and not self.history_enabled:
                        continue
                    plex_path = (
                        item.plex_path
                        if item.path_kind == "plex"
                        else self.mapper.cloud_to_plex(item.cloud_path)
                    )
                    target = await asyncio.to_thread(self.plex.find_target, plex_path)
                    if is_history_only(item) and not self.history_enabled:
                        continue
                    if self.inflight.get(self.coalescer.key_for(item)) is not item:
                        continue
                    if target.section_id != item.section_id:
                        raise PathMappingError(
                            f"Plex 媒体库发生变化，保留任务：{item.plex_path}"
                        )
                    self.record_info(
                        format_scan_log(
                            stage=ScanStage.STARTED,
                            cloud_path=item.cloud_path,
                            target=target,
                            sources=item.sources,
                        )
                    )
                    await asyncio.to_thread(self.plex.scan, target)
                    self.record_info(
                        format_scan_log(
                            stage=ScanStage.SUBMITTED,
                            cloud_path=item.cloud_path,
                            target=target,
                            sources=item.sources,
                        )
                    )
                    if self.on_submitted and item.media:
                        try:
                            await self.on_submitted(
                                target, current_media_hints(self, item, target)
                            )
                        except Exception as error:  # noqa: BLE001 - a logging failure must not repeat a Plex scan.
                            self._record_error(
                                f"扫描已提交，但入库核验任务保存失败：{error}"
                            )
                except Exception as error:  # noqa: BLE001 - retain failed external Plex requests.
                    self._record_error(f"扫描失败，保留任务：{item.plex_path}：{error}")
                    item.state = "queued"
                    item.last_seen = self.clock()
                    item.ready_at = None
                    if not self.coalescer.add(item):
                        self.coalescer.pending[self.coalescer.key_for(item)] = item
                    self.inflight.pop(self.coalescer.key_for(item))
                    await self._save_queue()
                    await asyncio.sleep(self.scan_interval)
                    continue
                finally:
                    self._active_item = None
                self.inflight.pop(self.coalescer.key_for(item))
                await self._save_queue()
                scanned += 1
                self.last_scan_at = self.clock()
                await asyncio.sleep(self.scan_interval)
            if not self.coalescer.pending:
                self.last_error = ""
            return scanned
        finally:
            self._active_item = None
            for item in self.inflight.values():
                item.state = "queued"
                item.last_seen = self.clock()
                item.ready_at = None
                if not self.coalescer.add(item):
                    self.coalescer.pending[self.coalescer.key_for(item)] = item
            self.inflight.clear()
            await self._save_queue()

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(self.flush_signal.wait(), timeout=1.0)
            except TimeoutError:
                pass
            self.flush_signal.clear()
            try:
                await self.flush()
            except Exception as error:  # noqa: BLE001 - persistence errors must keep the worker alive.
                self._record_error(f"Plex 扫描失败，保留任务重试：{error}")

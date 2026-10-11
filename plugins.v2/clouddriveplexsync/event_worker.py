"""Durable directory queue and path-scoped Plex scan worker."""

from __future__ import annotations

import asyncio
import time
from collections import deque
from collections.abc import Callable, Iterable
from dataclasses import asdict

from .models import PendingScan, PluginStats
from .path_mapper import PathMapper, PathMappingError, is_under
from .plex_bridge import PlexBridge


class ScanCoalescer:
    """Bounded directory queue without common-ancestor promotion."""

    def __init__(self, debounce_seconds: int = 30, capacity: int = 10_000) -> None:
        self.debounce_seconds = max(0, int(debounce_seconds))
        self.capacity = max(1, int(capacity))
        self.pending: dict[str, PendingScan] = {}

    @staticmethod
    def _key(section_id: str, plex_path: str) -> str:
        return f"{section_id}\0{plex_path}"

    def add(self, item: PendingScan) -> bool:
        exact_key = self._key(item.section_id, item.plex_path)
        existing = self.pending.get(exact_key)
        if existing:
            existing.last_seen = max(existing.last_seen, item.last_seen)
            existing.sources = sorted(set(existing.sources).union(item.sources))
            return True

        # Suppress a new child when an explicit ancestor scan is already queued.
        for queued in self.pending.values():
            if queued.section_id == item.section_id and is_under(
                item.plex_path, queued.plex_path
            ):
                queued.last_seen = max(queued.last_seen, item.last_seen)
                queued.sources = sorted(set(queued.sources).union(item.sources))
                return True

        # An explicitly queued ancestor supersedes its existing descendants.
        descendant_keys = [
            key
            for key, queued in self.pending.items()
            if queued.section_id == item.section_id
            and queued.plex_path != item.plex_path
            and is_under(queued.plex_path, item.plex_path)
        ]
        for key in descendant_keys:
            self.pending.pop(key, None)

        if len(self.pending) >= self.capacity:
            return False
        self.pending[exact_key] = item
        return True

    def due(self, now: float | None = None, force: bool = False) -> list[PendingScan]:
        now = time.time() if now is None else now
        keys = [
            key
            for key, item in self.pending.items()
            if force or now - item.last_seen >= self.debounce_seconds
        ]
        items = [self.pending.pop(key) for key in keys]
        return sorted(items, key=lambda item: (item.first_seen, item.plex_path))

    def snapshot(self) -> list[dict[str, object]]:
        return [asdict(item) for item in self.pending.values()]

    def load(self, values: Iterable[dict[str, object]]) -> None:
        for value in values or []:
            if not self.add(PendingScan.from_dict(value)):
                raise ValueError("持久化扫描队列超过配置容量，请增大容量后重试")


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
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.mapper = mapper
        self.plex = plex
        self.coalescer = ScanCoalescer(debounce_seconds, capacity)
        self.scan_interval = 1.0 / max(0.1, scans_per_second)
        self.stats = stats
        self.persist_queue = persist_queue
        self.on_error = on_error
        self.clock = clock
        self.flush_signal = asyncio.Event()
        self.recent: deque[str] = deque(maxlen=100)
        self.inflight: dict[str, PendingScan] = {}
        self._flush_lock = asyncio.Lock()
        self._persistence_lock = asyncio.Lock()
        self.last_scan_at = 0.0
        self.last_error = ""

    async def _save_queue(self) -> None:
        async with self._persistence_lock:
            if self.persist_queue:
                snapshot = [
                    asdict(item) for item in self.inflight.values()
                ] + self.coalescer.snapshot()
                saving = asyncio.create_task(
                    asyncio.to_thread(self.persist_queue, snapshot)
                )
                try:
                    await asyncio.shield(saving)
                except asyncio.CancelledError:
                    await saving
                    raise

    def load_queue(self, values: Iterable[dict[str, object]]) -> None:
        self.coalescer.load(values)

    def _record_error(self, message: str) -> None:
        should_report = message != self.last_error
        self.last_error = message
        self.recent.append(f"ERROR {message}")
        if should_report and self.on_error:
            self.on_error(message)

    async def submit_scan_directory(
        self, cloud_directory: str, source: str = "manual"
    ) -> bool:
        path = self.mapper.validate_cloud_path(cloud_directory)
        target = await asyncio.to_thread(
            self.plex.find_target, self.mapper.cloud_to_plex(path)
        )
        now = self.clock()
        accepted = self.coalescer.add(
            PendingScan(
                cloud_path=path,
                plex_path=target.path,
                section_id=target.section_id,
                section_title=target.section_title,
                first_seen=now,
                last_seen=now,
                sources=[source],
            )
        )
        if not accepted:
            self._record_error("扫描队列已满，记录将保留并重试")
            return False
        await self._save_queue()
        self.recent.append(f"QUEUE {source} {target.path}")
        self.flush_signal.set()
        return True

    async def flush(self, force: bool = False) -> int:
        async with self._flush_lock:
            items = self.coalescer.due(self.clock(), force=force)
            if not items:
                return 0
            for item in items:
                item.state = "inflight"
                self.inflight[self.coalescer._key(item.section_id, item.plex_path)] = (
                    item
                )
            scanned = 0
            try:
                await self._save_queue()
                for item in items:
                    try:
                        target = await asyncio.to_thread(
                            self.plex.find_target,
                            self.mapper.cloud_to_plex(item.cloud_path),
                        )
                        if target.section_id != item.section_id:
                            raise PathMappingError(
                                f"Plex 媒体库发生变化，保留任务：{item.plex_path}"
                            )
                        await asyncio.to_thread(self.plex.scan, target)
                    except Exception as error:  # noqa: BLE001 - retain failed external Plex requests.
                        self._record_error(
                            f"扫描失败，保留任务：{item.plex_path}：{error}"
                        )
                        item.state = "queued"
                        item.last_seen = self.clock()
                        if not self.coalescer.add(item):
                            self.coalescer.pending[
                                self.coalescer._key(item.section_id, item.plex_path)
                            ] = item
                        self.inflight.pop(
                            self.coalescer._key(item.section_id, item.plex_path)
                        )
                        await self._save_queue()
                        await asyncio.sleep(self.scan_interval)
                        continue
                    self.inflight.pop(
                        self.coalescer._key(item.section_id, item.plex_path)
                    )
                    await self._save_queue()
                    scanned += 1
                    self.last_scan_at = self.clock()
                    self.recent.append(f"SCAN {target.path}")
                    await asyncio.sleep(self.scan_interval)
                if not self.coalescer.pending:
                    self.last_error = ""
                return scanned
            finally:
                for item in self.inflight.values():
                    item.state = "queued"
                    item.last_seen = self.clock()
                    if not self.coalescer.add(item):
                        self.coalescer.pending[
                            self.coalescer._key(item.section_id, item.plex_path)
                        ] = item
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

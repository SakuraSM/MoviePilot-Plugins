"""Event coalescing and path-scoped Plex scan worker."""

from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import asdict
from typing import Callable, Deque, Dict, Iterable, List, Optional, Sequence

from .buffer_manager import BufferManager
from .models import CloudApi, FileSystemChange, PendingScan, PluginStats
from .path_mapper import PathMapper, PathMappingError, cloud_for_path, is_under
from .plex_bridge import PlexBridge


class ScanCoalescer:
    """Bounded directory queue without common-ancestor promotion."""

    def __init__(self, debounce_seconds: int = 30, capacity: int = 10_000) -> None:
        self.debounce_seconds = max(0, int(debounce_seconds))
        self.capacity = max(1, int(capacity))
        self.pending: Dict[str, PendingScan] = {}

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
            if queued.section_id == item.section_id and is_under(item.plex_path, queued.plex_path):
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

    def due(self, now: Optional[float] = None, force: bool = False) -> List[PendingScan]:
        now = time.time() if now is None else now
        keys = [
            key
            for key, item in self.pending.items()
            if force or now - item.last_seen >= self.debounce_seconds
        ]
        items = [self.pending.pop(key) for key in keys]
        return sorted(items, key=lambda item: (item.first_seen, item.plex_path))

    def snapshot(self) -> List[dict]:
        return [asdict(item) for item in self.pending.values()]

    def load(self, values: Iterable[dict]) -> None:
        for value in values or []:
            try:
                self.add(PendingScan(**value))
            except (TypeError, ValueError):
                continue


class EventWorker:
    """Translate changes, debounce them, and submit exact Plex paths."""

    def __init__(
        self,
        mapper: PathMapper,
        plex: PlexBridge,
        buffer_manager: BufferManager,
        cloud_apis: Sequence[CloudApi],
        *,
        debounce_seconds: int = 30,
        capacity: int = 10_000,
        scans_per_second: float = 1.0,
        stats: Optional[PluginStats] = None,
        persist_queue: Optional[Callable[[List[dict]], None]] = None,
        on_error: Optional[Callable[[str], None]] = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.mapper = mapper
        self.plex = plex
        self.buffer_manager = buffer_manager
        self.cloud_apis = list(cloud_apis)
        self.coalescer = ScanCoalescer(debounce_seconds, capacity)
        self.scan_interval = 1.0 / max(0.1, float(scans_per_second))
        self.stats = stats
        self.persist_queue = persist_queue
        self.on_error = on_error
        self.clock = clock
        self.flush_signal = asyncio.Event()
        self.recent: Deque[str] = deque(maxlen=100)
        self.last_scan_at: float = 0
        self.last_error: str = ""

    def update_topology(
        self, mapper: PathMapper, cloud_apis: Sequence[CloudApi]
    ) -> None:
        self.mapper = mapper
        self.cloud_apis = list(cloud_apis)

    def _save_queue(self) -> None:
        if self.persist_queue:
            self.persist_queue(self.coalescer.snapshot())

    def load_queue(self, values: Iterable[dict]) -> None:
        self.coalescer.load(values)

    def _record_error(self, message: str) -> None:
        self.last_error = message
        self.recent.append(f"ERROR {message}")
        if self.on_error:
            self.on_error(message)

    def submit_cloud_path(self, cloud_path: str, source: str = "cd2") -> bool:
        try:
            scan_cloud_path = self.mapper.cloud_scan_directory(cloud_path)
            return self.submit_scan_directory(scan_cloud_path, source)
        except (PathMappingError, RuntimeError, ValueError) as exc:
            if self.stats:
                self.stats.unmapped_events += 1
            self._record_error(str(exc))
            return False

    def submit_scan_directory(self, cloud_directory: str, source: str = "manual") -> bool:
        """Queue one exact cloud directory without promoting it to its parent."""

        try:
            scan_cloud_path = self.mapper.validate_cloud_path(cloud_directory)
            plex_path = self.mapper.cloud_to_plex(scan_cloud_path)
            target = self.plex.find_target(plex_path)
            cloud = cloud_for_path(scan_cloud_path, self.cloud_apis)
            now = self.clock()
            queued = self.coalescer.add(
                PendingScan(
                    cloud_path=scan_cloud_path,
                    plex_path=target.path,
                    section_id=target.section_id,
                    section_title=target.section_title,
                    cloud_identity=cloud.identity if cloud else None,
                    first_seen=now,
                    last_seen=now,
                    sources=[source],
                )
            )
            if not queued:
                self._record_error("scan queue reached its configured capacity")
                return False
            self.recent.append(f"QUEUE {source} {target.section_title} {target.path}")
            self._save_queue()
            self.flush_signal.set()
            return True
        except (PathMappingError, RuntimeError, ValueError) as exc:
            if self.stats:
                self.stats.unmapped_events += 1
            self._record_error(str(exc))
            return False

    def submit_change(self, change: FileSystemChange, source: str = "cd2") -> int:
        paths = [change.path]
        if change.new_path:
            paths.append(change.new_path)
        return sum(1 for path in paths if self.submit_cloud_path(path, source))

    async def flush(self, force: bool = False) -> int:
        items = self.coalescer.due(self.clock(), force=force)
        if not items:
            return 0
        self._save_queue()

        grouped: Dict[Optional[str], List[PendingScan]] = {}
        for item in items:
            grouped.setdefault(item.cloud_identity, []).append(item)

        scanned = 0
        for identity, group in grouped.items():
            cloud = next((item for item in self.cloud_apis if item.identity == identity), None)
            prefixes = [item.plex_path for item in group]
            sections = {item.section_id for item in group}
            may_scan = await self.buffer_manager.prepare(cloud, prefixes, sections)
            if not may_scan:
                for item in group:
                    self.coalescer.add(item)
                self._save_queue()
                self._record_error("buffer update failed and fail-open is disabled")
                continue

            for item in group:
                try:
                    target = self.plex.find_target(item.plex_path)
                    if target.section_id != item.section_id:
                        raise PathMappingError(
                            f"Plex section changed for {item.plex_path}; refusing to scan"
                        )
                    self.plex.scan(target)
                    scanned += 1
                    self.last_scan_at = self.clock()
                    self.recent.append(f"SCAN {target.section_title} {target.path}")
                except Exception as exc:
                    item.last_seen = self.clock()
                    self.coalescer.add(item)
                    self._record_error(str(exc))
                await asyncio.sleep(self.scan_interval)
        self._save_queue()
        return scanned

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(self.flush_signal.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                pass
            self.flush_signal.clear()
            await self.flush(force=False)

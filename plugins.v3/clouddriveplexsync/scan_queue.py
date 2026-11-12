"""Coalesce directory tasks without promoting siblings to a common root."""

from __future__ import annotations

import time
from collections.abc import Iterable
from dataclasses import asdict

from .models import HISTORY_SOURCE, PendingScan, PlexTarget, ScanPathKind
from .path_mapper import is_under


class ScanCoalescer:
    """Bounded directory queue without common-ancestor promotion."""

    def __init__(self, debounce_seconds: int = 30, capacity: int = 10_000) -> None:
        self.debounce_seconds = max(0, int(debounce_seconds))
        self.capacity = max(1, int(capacity))
        self.pending: dict[str, PendingScan] = {}

    @staticmethod
    def _key(section_id: str, plex_path: str) -> str:
        return f"{section_id}\0{plex_path}"

    @staticmethod
    def historical(item: PendingScan) -> bool:
        return bool(item.sources) and set(item.sources) == {HISTORY_SOURCE}

    def key_for(self, item: PendingScan) -> str:
        return (
            self._key(item.section_id, item.plex_path)
            + ("\0history" if self.historical(item) else "")
            + ("\0plex" if item.path_kind == "plex" else "")
        )

    @staticmethod
    def _merge(queued: PendingScan, item: PendingScan) -> None:
        queued.last_seen = max(queued.last_seen, item.last_seen)
        queued.sources = sorted(set(queued.sources).union(item.sources))
        known = {
            (hint.record_key, hint.revision, hint.expected_path)
            for hint in queued.media
        }
        for hint in item.media:
            identity = (hint.record_key, hint.revision, hint.expected_path)
            if identity not in known:
                queued.media.append(hint)
                known.add(identity)
        if item.ready_at is not None:
            queued.ready_at = (
                min(queued.ready_at, item.ready_at)
                if queued.ready_at is not None
                else item.ready_at
            )

    def add(self, item: PendingScan) -> bool:
        exact_key = self.key_for(item)
        existing = self.pending.get(exact_key)
        if existing:
            self._merge(existing, item)
            return True

        # Suppress a new child when an explicit ancestor scan is already queued.
        for queued in self.pending.values():
            if (
                self.historical(queued) == self.historical(item)
                and queued.path_kind == item.path_kind
                and queued.section_id == item.section_id
                and is_under(item.plex_path, queued.plex_path)
            ):
                self._merge(queued, item)
                return True

        # An explicitly queued ancestor supersedes its existing descendants.
        descendant_keys = [
            key
            for key, queued in self.pending.items()
            if queued.section_id == item.section_id
            and queued.path_kind == item.path_kind
            and self.historical(queued) == self.historical(item)
            and queued.plex_path != item.plex_path
            and is_under(queued.plex_path, item.plex_path)
        ]
        for key in descendant_keys:
            descendant = self.pending.pop(key)
            self._merge(item, descendant)

        if len(self.pending) >= self.capacity:
            return False
        self.pending[exact_key] = item
        return True

    def due(self, now: float | None = None, force: bool = False) -> list[PendingScan]:
        now = time.time() if now is None else now
        keys = [
            key
            for key, item in self.pending.items()
            if force
            or (
                now >= item.ready_at
                if item.ready_at is not None
                else now - item.last_seen >= self.debounce_seconds
            )
        ]
        items = [self.pending.pop(key) for key in keys]
        return sorted(items, key=lambda item: (item.first_seen, item.plex_path))

    def snapshot(self) -> list[dict[str, object]]:
        return [asdict(item) for item in self.pending.values()]

    def covering_path(
        self,
        target: PlexTarget,
        source: str = "manual",
        *,
        path_kind: ScanPathKind = "cloud",
    ) -> str:
        for item in self.pending.values():
            if (
                self.historical(item) == (source == HISTORY_SOURCE)
                and item.path_kind == path_kind
                and item.section_id == target.section_id
                and is_under(target.path, item.plex_path)
            ):
                return item.plex_path
        raise RuntimeError("已接受的扫描任务未找到对应排队目录")

    def load(self, values: Iterable[dict[str, object]]) -> None:
        for value in values or []:
            if not self.add(PendingScan.from_dict(value)):
                raise ValueError("持久化扫描队列超过配置容量，请增大容量后重试")

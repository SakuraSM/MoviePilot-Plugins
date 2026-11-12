"""Choose between newly observed successes and historical backfill."""

from __future__ import annotations

import posixpath
from dataclasses import dataclass
from uuid import uuid4

from .history_models import TTDCursor, TTDHistoryMode, TTDHistoryRecord
from .history_store import HistoryStore
from .path_mapper import PathMappingError, is_under, normalize_path


@dataclass
class HistoryPolicy:
    mode: TTDHistoryMode
    cursor: TTDCursor

    @classmethod
    def prepare(
        cls, *, mode: TTDHistoryMode, cursor: TTDCursor, scope: str
    ) -> HistoryPolicy:
        if cursor.scope and cursor.scope != scope:
            cursor = TTDCursor()
        cursor.scope = scope
        if mode == TTDHistoryMode.NEW_ONLY:
            if cursor.history_mode != mode or not cursor.mode_epoch:
                cursor = TTDCursor(
                    scope=scope,
                    history_mode=mode,
                    mode_epoch=uuid4().hex,
                    audit_next_page=0,
                )
            cursor.audit_next_page = 0
        else:
            if cursor.history_mode == TTDHistoryMode.NEW_ONLY:
                cursor.audit_next_page = 1
                cursor.history_gap = False
            cursor.history_mode = mode
            cursor.mode_epoch = ""
            cursor.baseline_at = 0
        return cls(mode, cursor)

    @property
    def needs_baseline(self) -> bool:
        return self.mode == TTDHistoryMode.NEW_ONLY and not self.cursor.initialized

    def establish_baseline(
        self, records: list[TTDHistoryRecord], *, now: float
    ) -> None:
        self.cursor.seen_keys = [record.key for record in records]
        self.cursor.initialized = True
        self.cursor.baseline_at = now
        self.cursor.history_gap = False

    async def select_records(
        self, records: list[TTDHistoryRecord], *, store: HistoryStore | None
    ) -> list[TTDHistoryRecord]:
        if self.mode == TTDHistoryMode.INCLUDE_HISTORY:
            return records
        if store is None:
            raise ValueError("仅新增模式需要持久化去重账本")
        admitted = await store.known_keys(records, epoch=self.cursor.mode_epoch)
        anchors = set(self.cursor.seen_keys)
        selected = []
        newly_observed = []
        found_anchor = False
        for record in records:
            if record.key in anchors:
                found_anchor = True
            is_new = not found_anchor and record.key not in anchors
            if is_new:
                newly_observed.append(record)
            if record.key in admitted or is_new:
                selected.append(record)
        await store.admit(newly_observed, epoch=self.cursor.mode_epoch)
        return selected


def resolve_directory(root: str, record: TTDHistoryRecord) -> str:
    target = record.target_path.strip()
    path = normalize_path(
        target if target.startswith("/") else posixpath.join(root, target)
    )
    if not is_under(path, root):
        raise PathMappingError(f"TTD 目标路径越出配置根目录：{path}")
    return path

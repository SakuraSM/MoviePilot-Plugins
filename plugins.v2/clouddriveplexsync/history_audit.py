"""Resumable, bounded full-history reconciliation alongside fast polling."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from .history_models import TTDCursor, TTDHistoryRecord
from .history_store import HistoryStore

if TYPE_CHECKING:
    from .ttd_client import TTDClient

AUDIT_PAGES_PER_POLL = 2
SECONDS_PER_HOUR = 3600


class HistoryAudit:
    def __init__(
        self,
        *,
        client: TTDClient,
        cursor: TTDCursor,
        store: HistoryStore,
        process: Callable[[list[TTDHistoryRecord]], Awaitable[bool]],
        save_cursor: Callable[[], None],
        interval_hours: int = 6,
    ) -> None:
        self.client = client
        self.cursor = cursor
        self.store = store
        self.process = process
        self.save_cursor = save_cursor
        self.interval_seconds = max(1, interval_hours) * SECONDS_PER_HOUR

    def request(self, *, restart: bool = False) -> None:
        if restart or not self.cursor.audit_next_page:
            self.cursor.audit_next_page = 1
        self.save_cursor()

    async def step(self, now: float) -> None:
        if not self.cursor.audit_next_page:
            if now < self.cursor.audit_completed_at + self.interval_seconds:
                return
            self.cursor.audit_next_page = 1
        for _ in range(AUDIT_PAGES_PER_POLL):
            page = await self.client.fetch_page(self.cursor.audit_next_page)
            if not await self.process(await self.store.unseen(page.records)):
                return
            if page.has_next is False or page.item_count < self.client.page_size:
                self.cursor.audit_next_page = 0
                self.cursor.audit_completed_at = now
                self.cursor.history_gap = False
                self.save_cursor()
                return
            self.cursor.audit_next_page += 1
            self.save_cursor()

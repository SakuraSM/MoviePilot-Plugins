"""Incremental polling with overlapping history reads."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import posixpath
import time
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, Any

from .history_audit import HistoryAudit
from .history_models import (
    TTDCursor,
    TTDHistoryGapError,
    TTDHistoryRecord,
    TTDPollStatus,
    TTDSubmitResult,
    TTDSubmitState,
)
from .history_store import HistoryStore
from .models import PluginStats
from .path_mapper import PathMappingError, is_under, normalize_path
from .polling_loop import run_polling, wait_polling

if TYPE_CHECKING:
    from .ttd_client import TTDClient


class TTDHistoryPoller:
    """Incrementally consume newest-first history with a bounded page budget."""

    def __init__(
        self,
        client: TTDClient,
        submit_directory: Callable[[str, str], Any],
        target_root: str,
        *,
        poll_seconds: int = 30,
        max_pages: int = 5,
        cursor: TTDCursor | None = None,
        persist_cursor: Callable[[dict[str, Any]], None] | None = None,
        on_error: Callable[[str], None] | None = None,
        on_skip: Callable[[str], None] | None = None,
        stats: PluginStats | None = None,
        clock: Callable[[], float] = time.time,
        seen_limit: int = 1000,
        overlap_pages: int = 2,
        store: HistoryStore | None = None,
        reconcile_hours: int = 6,
    ) -> None:
        self.client = client
        self.submit_directory = submit_directory
        self.target_root = normalize_path(target_root)
        self.poll_seconds = max(10, int(poll_seconds))
        self.max_pages = max(1, min(20, int(max_pages)))
        scope_value = f"{client.base_url}|{client.source}|{self.target_root}"
        self.cursor_scope = hashlib.sha256(scope_value.encode("utf-8")).hexdigest()
        self.cursor = cursor or TTDCursor()
        if self.cursor.scope and self.cursor.scope != self.cursor_scope:
            self.cursor = TTDCursor(scope=self.cursor_scope)
        else:
            self.cursor.scope = self.cursor_scope
        self.persist_cursor = persist_cursor
        self.on_error = on_error
        self.on_skip = on_skip
        self.stats = stats
        self.clock = clock
        self.seen_limit = max(100, int(seen_limit))
        self.overlap_pages = max(1, int(overlap_pages))
        self.status = TTDPollStatus(
            cursor_initialized=self.cursor.initialized,
            seen_records=len(self.cursor.seen_keys),
        )
        self._wake_event = asyncio.Event()
        self._last_processing_error = ""

        self.store = store
        self.audit = (
            HistoryAudit(
                client=client,
                cursor=self.cursor,
                store=store,
                process=self._process_audit_records,
                save_cursor=self._save_cursor,
                interval_hours=reconcile_hours,
            )
            if store
            else None
        )

    async def _process_audit_records(self, records: list[TTDHistoryRecord]) -> bool:
        await self._process_records(records, update_cursor=False)
        return not bool(self.status.last_error)

    def request_reconcile(self) -> None:
        if self.audit:
            self.audit.request(restart=True)
        self.wake()

    @staticmethod
    def _submission_result(value: Any) -> TTDSubmitResult:
        if isinstance(value, TTDSubmitResult):
            return value
        # Preserve compatibility with callbacks written for the original bool contract.
        return (
            TTDSubmitResult.queued()
            if bool(value)
            else TTDSubmitResult.retry("目标目录暂未被扫描队列接受")
        )

    def wake(self) -> None:
        self._wake_event.set()

    def _save_cursor(self) -> None:
        self.cursor.seen_keys = list(dict.fromkeys(self.cursor.seen_keys))[
            : self.seen_limit
        ]
        self.status.audit_next_page = self.cursor.audit_next_page
        self.status.audit_completed_at = self.cursor.audit_completed_at
        self.status.history_gap = self.cursor.history_gap
        self.status.cursor_initialized = self.cursor.initialized
        self.status.seen_records = len(self.cursor.seen_keys)
        if self.persist_cursor:
            self.persist_cursor(self.cursor.to_dict())

    def _remember(self, keys: Iterable[str]) -> None:
        incoming = [str(item) for item in keys if str(item)]
        self.cursor.seen_keys = list(dict.fromkeys(incoming + self.cursor.seen_keys))[
            : self.seen_limit
        ]

    def _cloud_directory(self, record: TTDHistoryRecord) -> str:
        target = record.target_path.strip()
        if target.startswith("/"):
            cloud_path = normalize_path(target)
        else:
            cloud_path = normalize_path(posixpath.join(self.target_root, target))
        if not is_under(cloud_path, self.target_root):
            raise PathMappingError(f"TTD 目标路径越出配置根目录：{cloud_path}")
        return cloud_path

    async def _fetch_new_records(self) -> list[TTDHistoryRecord]:
        if not self.cursor.initialized:
            first_page = await self.client.fetch_page(1)
            return list(first_page.records)
        known = set(self.cursor.seen_keys)
        unseen: list[TTDHistoryRecord] = []
        unseen_keys = set()
        found_cursor = False
        cursor_page = 0
        exhausted = False
        for page in range(1, self.max_pages + 1):
            page_result = await self.client.fetch_page(page)
            records = page_result.records
            if not records:
                exhausted = True
                break
            for record in records:
                if record.key in known:
                    found_cursor = True
                    cursor_page = cursor_page or page
                    if not self.store:
                        continue
                if record.key not in unseen_keys:
                    unseen.append(record)
                    unseen_keys.add(record.key)
            if found_cursor and page >= cursor_page + self.overlap_pages:
                break
            if (
                page_result.has_next is False
                or page_result.item_count < self.client.page_size
            ):
                exhausted = True
                break
        if self.cursor.initialized and known and not found_cursor and not exhausted:
            if not self.audit:
                raise TTDHistoryGapError(
                    f"连续 {self.max_pages} 页未找到已保存游标；已停止推进，"
                    "请增大 TTD 最大补页数或手动确认历史缺口"
                )
            if not self.cursor.history_gap and self.on_error:
                self.on_error("TTD 未找到历史游标，已启动分批历史对账")
            self.audit.request(restart=not self.cursor.history_gap)
            self.cursor.history_gap = True
        return unseen

    async def poll_once(self) -> int:
        self.status.last_poll_at = self.clock()
        records = await self._fetch_new_records()
        if self.store:
            fetched = records
            records = await self.store.unseen(fetched)
            unseen_keys = {record.key for record in records}
            self._remember(
                record.key for record in fetched if record.key not in unseen_keys
            )
        self.status.connected = True
        self.status.authenticated = True
        self.status.last_success_at = self.clock()
        self.status.last_error = ""

        queued = await self._process_records(records)
        if self.audit:
            await self.audit.step(self.clock())
        return queued

    async def _process_records(
        self, records: list[TTDHistoryRecord], *, update_cursor: bool = True
    ) -> int:
        processed: list[str] = []
        submitted_directories: dict[str, TTDSubmitResult] = {}
        queued = 0
        retrying = False
        # API is newest-first; enqueue oldest-first so Plex sees moves in order.
        for record in reversed(records):
            try:
                cloud_directory = self._cloud_directory(record)
                outcome = submitted_directories.get(cloud_directory)
                is_new_directory = outcome is None
                if outcome is None:
                    result = self.submit_directory(cloud_directory, "ttd")
                    if inspect.isawaitable(result):
                        result = await result
                    outcome = self._submission_result(result)
                    submitted_directories[cloud_directory] = outcome
                if outcome.state == TTDSubmitState.RETRY:
                    message = f"TTD 整理记录暂未处理，将重试：{cloud_directory}" + (
                        f"：{outcome.reason}" if outcome.reason else ""
                    )
                    self.status.last_error = message
                    if message != self._last_processing_error and self.on_error:
                        self.on_error(message)
                    self._last_processing_error = message
                    retrying = True
                    break
                if outcome.state == TTDSubmitState.SKIPPED:
                    self.status.skipped_records += 1
                    if self.stats:
                        self.stats.ttd_skipped_records += 1
                    if is_new_directory:
                        message = f"TTD 记录已跳过：{cloud_directory}" + (
                            f"：{outcome.reason}" if outcome.reason else ""
                        )
                        self.status.last_skip = message
                        if self.on_skip:
                            self.on_skip(message)
                if outcome.state == TTDSubmitState.QUEUED and is_new_directory:
                    queued += 1
                processed.append(record.key)
                self.status.last_record_at = self.clock()
            except PathMappingError as exc:
                message = f"TTD 记录已跳过：{record.target_path}：{exc}"
                self.status.skipped_records += 1
                self.status.last_skip = message
                if self.stats:
                    self.stats.ttd_skipped_records += 1
                if self.on_skip:
                    self.on_skip(message)
                processed.append(record.key)
            except Exception as exc:  # noqa: BLE001 - callback failures must leave history retryable.
                message = f"TTD 整理记录处理失败，将重试：{exc}"
                self.status.last_error = message
                if message != self._last_processing_error and self.on_error:
                    self.on_error(message)
                self._last_processing_error = message
                retrying = True
                break
        if not retrying:
            self._last_processing_error = ""
            self.status.last_error = ""
        if self.store and processed:
            accepted = set(processed)
            await self.store.remember(
                [record for record in records if record.key in accepted]
            )
        if update_cursor and (processed or not self.cursor.initialized):
            self.cursor.initialized = True
            self._remember(reversed(processed))
            self._save_cursor()
        if self.stats:
            self.stats.ttd_records += len(processed)
        return queued

    async def _wait(self, stop_event: asyncio.Event, delay: float) -> None:
        await wait_polling(self, stop_event, delay)

    async def run(self, stop_event: asyncio.Event) -> None:
        await run_polling(self, stop_event)

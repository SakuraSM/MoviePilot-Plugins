"""Independently start and cancel resumable historical queries and scan jobs."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable

from .event_worker import EventWorker
from .history_models import TTDAuthenticationError
from .history_poller import TTDHistoryPoller
from .queue_storage import persist_values

TRANSIENT_RETRY_SECONDS = (5, 15, 30, 60)
AUTH_RETRY_SECONDS = (300, 900, 1800, 3600)


class HistoryControl:
    def __init__(
        self,
        poller: TTDHistoryPoller,
        worker: EventWorker,
        persist_enabled: Callable[[bool], None] | None = None,
    ) -> None:
        self.poller, self.worker = poller, worker
        self.persist_enabled = persist_enabled
        self.enabled = False
        self.query_in_progress = False
        self.task: asyncio.Task[None] | None = None
        self._lock = asyncio.Lock()
        self._generation = 0

    async def _cancel_query(self) -> None:
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
            self.task = None

    async def set_enabled(self, enabled: bool, *, persist: bool = True) -> None:
        self._generation += 1
        generation = self._generation
        self.enabled = enabled
        if not enabled:
            # Close admission before waiting for any storage or network cleanup.
            self.worker.history_enabled = False
            if self.task:
                self.task.cancel()
        async with self._lock:
            try:
                await self._apply_enabled(
                    enabled, generation=generation, persist=persist
                )
            except Exception as error:
                if generation == self._generation:
                    self.enabled = False
                    self.worker.history_enabled = False
                self.worker._record_error(
                    f"历史开关操作失败，历史处理保持停止：{error}"
                )
                raise

    async def _apply_enabled(
        self, enabled: bool, *, generation: int, persist: bool
    ) -> None:
        await self._cancel_query()
        if generation != self._generation:
            return
        self.worker.history_enabled = False
        if enabled:
            await self.worker.restore_history()
        else:
            await self.worker.pause_history()
        if generation != self._generation:
            return
        if persist and self.persist_enabled:
            await persist_values(self.persist_enabled, enabled)
        if generation != self._generation:
            return
        if enabled:
            self.worker.history_enabled = True
            self.task = asyncio.create_task(self._run())
            self.worker.flush_signal.set()
        self.worker.record_info(
            "HISTORY_STARTED 历史查询与扫描已开启，将继续断点"
            if enabled
            else f"HISTORY_STOPPED 历史查询与后续扫描提交已停止；保留 {len(self.worker.held_history)} 个任务；已发出的 Plex 请求无法撤回"
        )

    async def reconcile(self) -> None:
        async with self._lock:
            if not self.enabled or not self.worker.history_enabled:
                raise ValueError("历史处理已关闭，请先开启历史查询与扫描")
            await self._cancel_query()
            if self.enabled and self.worker.history_enabled:
                self.poller.request_reconcile()
                self.task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        retry_index = 0
        while self.enabled:
            delay = self.poller.poll_seconds
            try:
                self.query_in_progress = True
                self.poller.status.last_error = ""
                if self.poller.audit:
                    await self.poller.audit.step(time.time())
                retry_index = 0
            except Exception as error:  # noqa: BLE001 - retain the page for retry on external failures.
                self.poller.status.last_error = f"历史查询失败，将重试：{error}"
                self.worker._record_error(self.poller.status.last_error)
                delays = (
                    AUTH_RETRY_SECONDS
                    if isinstance(error, TTDAuthenticationError)
                    else TRANSIENT_RETRY_SECONDS
                )
                delay = delays[retry_index]
                retry_index = min(retry_index + 1, len(delays) - 1)
            finally:
                self.query_in_progress = False
            await asyncio.sleep(delay)

    async def close(self) -> None:
        self.worker.history_enabled = False
        await self._cancel_query()

    def status(self) -> dict[str, object]:
        return {
            "enabled": self.enabled,
            "running": bool(self.task and not self.task.done()),
            "query_in_progress": self.query_in_progress,
            "next_page": self.poller.cursor.audit_next_page,
            "completed_at": self.poller.cursor.audit_completed_at,
            "last_error": self.poller.status.last_error,
        }

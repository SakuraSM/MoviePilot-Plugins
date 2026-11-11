from __future__ import annotations

import asyncio
import unittest

import _bootstrap  # noqa: F401
from clouddriveplexsync.history_models import (
    TTDCursor,
    TTDHistoryGapError,
    TTDPollStatus,
)
from clouddriveplexsync.polling_loop import run_polling


class FailingPoller:
    poll_seconds = 30
    stats = None

    def __init__(self, errors: list[Exception]) -> None:
        self.errors = errors
        self.status = TTDPollStatus()
        self.cursor = TTDCursor()
        self.delays: list[float] = []
        self.messages: list[str] = []
        self.on_error = self.messages.append

    async def poll_once(self) -> int:
        raise self.errors[len(self.delays)]

    async def _wait(self, stop: asyncio.Event, delay: float) -> None:
        self.delays.append(delay)
        if len(self.delays) == len(self.errors):
            stop.set()


class PollingBackoffTests(unittest.IsolatedAsyncioTestCase):
    async def test_repeated_gap_retries_back_off_to_an_hour(self) -> None:
        poller = FailingPoller([TTDHistoryGapError("missing") for _ in range(6)])
        await run_polling(poller, asyncio.Event())
        self.assertEqual(poller.delays, [60, 300, 900, 1800, 3600, 3600])
        self.assertEqual(len(poller.messages), 1)

    async def test_empty_network_error_includes_its_type(self) -> None:
        poller = FailingPoller([TimeoutError()])
        await run_polling(poller, asyncio.Event())
        self.assertEqual(poller.messages, ["TTD 轮询失败：TimeoutError"])

    async def test_network_errors_during_gap_recovery_keep_gap_backoff(self) -> None:
        poller = FailingPoller(
            [TTDHistoryGapError("missing"), TimeoutError(), TimeoutError()]
        )
        poller.cursor.history_gap = True
        await run_polling(poller, asyncio.Event())
        self.assertEqual(poller.delays, [60, 300, 900])

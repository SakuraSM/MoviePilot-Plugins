from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import _bootstrap  # noqa: F401
from clouddriveplexsync.history_models import TTDCursor, TTDHistoryGapError
from clouddriveplexsync.history_poller import TTDHistoryPoller
from clouddriveplexsync.history_store import HistoryStore
from clouddriveplexsync.ttd_client import TTDClient
from test_ttd_client import FakeResponse


class PaginatedTransport:
    def __init__(self) -> None:
        self.records: list[dict[str, object]] = []
        self.requests: list[tuple[int, int]] = []

    async def get(self, _url: str, **kwargs: object) -> FakeResponse:
        params = kwargs["params"]
        assert isinstance(params, dict)
        page = int(params["page"])
        limit = int(params["limit"])
        self.requests.append((page, limit))
        start = (page - 1) * limit
        return FakeResponse(
            payload={
                "success": True,
                "has_next": start + limit < len(self.records),
                "records": self.records[start : start + limit],
            }
        )


def history_record(record_id: int, path: str) -> dict[str, object]:
    return {"id": record_id, "status": "success", "target_path": path}


class GapRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.store = HistoryStore(
            Path(self.directory.name) / "history.sqlite3", "scope"
        )
        self.transport = PaginatedTransport()
        self.transport.records = [history_record(1, "anchor"), history_record(0, "old")]
        self.client = TTDClient(
            "https://ttd.example",
            api_token="token",
            page_size=20,
            client=self.transport,
        )
        self.submitted: list[str] = []
        self.poller = TTDHistoryPoller(
            self.client,
            lambda path, _source: self.submitted.append(path) or True,
            "/cloud",
            max_pages=5,
            store=self.store,
        )
        await self.poller.poll_once()

    async def asyncTearDown(self) -> None:
        await self.store.close()
        self.directory.cleanup()

    async def test_gap_retry_searches_a_larger_window_before_processing(self) -> None:
        self.transport.records = [
            history_record(index, f"new/{index}") for index in range(121, 1, -1)
        ] + self.transport.records
        with self.assertRaises(TTDHistoryGapError):
            await self.poller.poll_once()
        self.assertEqual(self.submitted, [])

        self.transport.requests.clear()
        self.assertEqual(await self.poller.poll_once(), 120)
        self.assertEqual(len(self.submitted), 120)
        self.assertNotIn("/cloud/anchor", self.submitted)
        self.assertNotIn("/cloud/old", self.submitted)
        self.assertFalse(self.poller.cursor.history_gap)
        self.assertEqual(self.client.page_size, 20)
        self.assertEqual(self.transport.requests[:2], [(1, 100), (2, 100)])

        self.transport.requests.clear()
        self.assertEqual(await self.poller.poll_once(), 0)
        self.assertEqual(self.transport.requests[0], (1, 20))

    async def test_deleted_anchor_stays_blocked_during_recovery(self) -> None:
        self.transport.records = [history_record(-1, "older")]
        for _ in range(2):
            with self.assertRaises(TTDHistoryGapError):
                await self.poller.poll_once()
        self.assertTrue(self.poller.cursor.history_gap)
        self.assertEqual(self.submitted, [])

    async def test_recovery_resumes_after_a_poller_restart(self) -> None:
        self.transport.records = [
            history_record(index, f"new/{index}") for index in range(121, 1, -1)
        ] + self.transport.records
        with self.assertRaises(TTDHistoryGapError):
            await self.poller.poll_once()
        restored = TTDCursor.from_dict(self.poller.cursor.to_dict())
        restarted = TTDHistoryPoller(
            self.client,
            lambda path, _source: self.submitted.append(path) or True,
            "/cloud",
            max_pages=5,
            cursor=restored,
            store=self.store,
        )
        self.transport.requests.clear()
        self.assertEqual(await restarted.poll_once(), 120)
        self.assertEqual(self.transport.requests[0], (1, 100))
        self.assertFalse(restarted.cursor.history_gap)

    async def test_recovery_stays_bounded_if_anchor_is_too_far_away(self) -> None:
        self.transport.records = [
            history_record(index, f"new/{index}") for index in range(2001, 1, -1)
        ] + self.transport.records
        with self.assertRaises(TTDHistoryGapError):
            await self.poller.poll_once()
        self.transport.requests.clear()
        with self.assertRaises(TTDHistoryGapError):
            await self.poller.poll_once()
        self.assertEqual(
            self.transport.requests, [(page, 100) for page in range(1, 21)]
        )
        self.assertEqual(self.submitted, [])


if __name__ == "__main__":
    unittest.main()

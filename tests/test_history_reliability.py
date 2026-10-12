from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import _bootstrap  # noqa: F401
from clouddriveplexsync.history_models import TTDCursor, TTDHistoryMode, TTDSubmitResult
from clouddriveplexsync.history_poller import TTDHistoryPoller
from clouddriveplexsync.history_store import HistoryStore
from test_ttd_client import FakeHistoryClient, record


class HistoryReliabilityTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.database = Path(self.directory.name) / "history.sqlite3"
        self.store = HistoryStore(self.database, "scope")
        self.submitted = []

    async def asyncTearDown(self) -> None:
        await self.store.close()
        self.directory.cleanup()

    def make_poller(self, client, cursor=None, **kwargs):
        return TTDHistoryPoller(
            client,
            lambda path, _source: self.submitted.append(path) or True,
            "/cloud",
            cursor=cursor or TTDCursor(initialized=True),
            store=self.store,
            **kwargs,
            history_mode=TTDHistoryMode.INCLUDE_HISTORY,
        )

    async def test_late_success_behind_known_record_is_processed(self) -> None:
        client = FakeHistoryClient(
            {1: [record("3", "new"), record("2", "known"), record("1", "late")]}
        )
        await self.store.remember([record("2", "known")])
        poller = self.make_poller(
            client,
            TTDCursor(initialized=True, seen_keys=["id:2"]),
        )
        await poller.poll_once()
        self.assertEqual(set(self.submitted), {"/cloud/new", "/cloud/late"})

    async def test_overlap_reads_pages_after_first_known_id(self) -> None:
        client = FakeHistoryClient(
            {
                1: [record("3", "known")],
                2: [record("2", "late")],
                3: [record("1", "older")],
            },
            page_size=1,
        )
        poller = TTDHistoryPoller(
            client,
            lambda path, _source: self.submitted.append(path) or True,
            "/cloud",
            cursor=TTDCursor(initialized=True, seen_keys=["id:3"]),
            history_mode=TTDHistoryMode.INCLUDE_HISTORY,
        )
        await poller.poll_once()
        self.assertEqual(client.calls, [1, 2, 3])
        self.assertIn("/cloud/late", self.submitted)

    async def test_ledger_survives_restart_and_more_than_recent_id_limit(self) -> None:
        records = [record(str(index), f"Film/{index}") for index in range(1200)]
        await self.store.remember(records)
        await self.store.close()
        self.store = HistoryStore(self.database, "scope")
        self.assertEqual(await self.store.unseen(records), [])

    async def test_changed_target_for_same_id_creates_new_revision(self) -> None:
        previous, current = record("1", "old-target"), record("1", "new-target")
        await self.store.remember([previous])
        self.assertEqual(await self.store.unseen([current]), [current])
        await self.store.remember([current])
        self.assertEqual(await self.store.unseen([previous, current]), [])

    async def test_same_id_changed_target_is_found_in_fast_poll(self) -> None:
        await self.store.remember([record("1", "old-target")])
        client = FakeHistoryClient({1: [record("1", "new-target")]})
        poller = self.make_poller(
            client,
            TTDCursor(initialized=True, seen_keys=["id:1"]),
        )
        await poller.poll_once()
        self.assertEqual(self.submitted, ["/cloud/new-target"])

    async def test_ledger_isolated_by_source_scope(self) -> None:
        entry = record("1", "Film")
        await self.store.remember([entry])
        other = HistoryStore(self.database, "different-scope")
        try:
            self.assertEqual(await other.unseen([entry]), [entry])
        finally:
            await other.close()

    async def test_audit_budget_progress_and_restart_resume(self) -> None:
        client = FakeHistoryClient(
            {index: [record(str(index), f"Film/{index}")] for index in range(1, 6)},
            page_size=1,
        )
        await self.store.remember([record("1", "Film/1")])
        cursor = TTDCursor(initialized=True, seen_keys=["id:1"])
        saved = []
        poller = self.make_poller(
            client, cursor, max_pages=1, persist_cursor=saved.append
        )
        await poller.poll_once()
        self.assertEqual(cursor.audit_next_page, 3)
        restored = TTDCursor.from_dict(saved[-1])
        restarted = self.make_poller(client, restored, max_pages=1)
        await restarted.poll_once()
        self.assertEqual(restored.audit_next_page, 5)
        await restarted.poll_once()
        self.assertEqual(restored.audit_next_page, 0)
        self.assertEqual(
            set(self.submitted), {f"/cloud/Film/{index}" for index in range(2, 6)}
        )
        previous_count = len(self.submitted)
        restarted.request_reconcile()
        await restarted.poll_once()
        self.assertEqual(len(self.submitted), previous_count)

    async def test_old_late_success_outside_fast_window_is_reconciled(self) -> None:
        client = FakeHistoryClient(
            {1: [record("10", "known")], 2: [record("2", "late")]}, page_size=1
        )
        await self.store.remember([record("10", "known")])
        cursor = TTDCursor(
            initialized=True,
            seen_keys=["id:10"],
            audit_next_page=2,
        )
        poller = self.make_poller(client, cursor, max_pages=1)
        await poller.poll_once()
        self.assertEqual(self.submitted, ["/cloud/late"])
        self.assertEqual(cursor.audit_next_page, 0)

    async def test_gap_remains_visible_until_audit_finishes(self) -> None:
        client = FakeHistoryClient(
            {
                1: [record("5", "new")],
                2: [record("4", "middle")],
                3: [record("3", "old")],
            },
            page_size=1,
        )
        cursor = TTDCursor(initialized=True, seen_keys=["id:1"])
        errors = []
        poller = self.make_poller(client, cursor, max_pages=1, on_error=errors.append)
        await poller.poll_once()
        self.assertTrue(cursor.history_gap)
        self.assertEqual(len(errors), 1)
        await poller.poll_once()
        self.assertFalse(cursor.history_gap)
        self.assertEqual(
            set(self.submitted), {"/cloud/new", "/cloud/middle", "/cloud/old"}
        )

    async def test_failed_audit_submission_does_not_advance_page_or_ledger(
        self,
    ) -> None:
        client = FakeHistoryClient(
            {1: [record("3", "known")], 2: [record("2", "retry")]}, page_size=1
        )
        await self.store.remember([record("3", "known")])
        cursor = TTDCursor(
            initialized=True,
            seen_keys=["id:3"],
            audit_next_page=2,
        )
        poller = self.make_poller(client, cursor, max_pages=1)
        poller.submit_directory = lambda *_args: TTDSubmitResult.retry(
            "Plex unavailable"
        )
        await poller.poll_once()
        self.assertEqual(cursor.audit_next_page, 2)
        self.assertEqual(
            await self.store.unseen([record("2", "retry")]), [record("2", "retry")]
        )

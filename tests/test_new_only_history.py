from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import _bootstrap  # noqa: F401
from clouddriveplexsync.history_models import (
    TTDCursor,
    TTDHistoryGapError,
    TTDHistoryMode,
)
from clouddriveplexsync.history_poller import TTDHistoryPoller
from clouddriveplexsync.history_store import HistoryStore
from test_ttd_client import FakeHistoryClient, record


class NewOnlyHistoryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.store = HistoryStore(
            Path(self.directory.name) / "history.sqlite3", "new-only-test"
        )
        self.submitted: list[str] = []

    async def asyncTearDown(self) -> None:
        await self.store.close()
        self.directory.cleanup()

    def make_poller(
        self,
        client: FakeHistoryClient,
        cursor: TTDCursor | None = None,
        *,
        history_mode: TTDHistoryMode = TTDHistoryMode.NEW_ONLY,
        max_pages: int = 5,
    ) -> TTDHistoryPoller:
        return TTDHistoryPoller(
            client,
            lambda path, _source: self.submitted.append(path) or True,
            "/cloud",
            store=self.store,
            cursor=cursor,
            history_mode=history_mode,
            max_pages=max_pages,
        )

    async def test_first_query_only_establishes_baseline(self) -> None:
        old = record("10", "old")
        client = FakeHistoryClient({1: [old]})
        poller = self.make_poller(client)
        self.assertEqual(await poller.poll_once(), 0)
        self.assertEqual(self.submitted, [])
        self.assertEqual(client.calls, [1])
        self.assertTrue(poller.cursor.initialized)
        self.assertEqual(poller.cursor.audit_next_page, 0)
        self.assertIsNone(poller.audit)
        self.assertEqual(await self.store.unseen([old]), [old])

    async def test_only_new_records_before_anchor_are_scanned(self) -> None:
        client = FakeHistoryClient({1: [record("10", "old-anchor")]})
        poller = self.make_poller(client)
        await poller.poll_once()
        client.pages[1] = [
            record("11", "new"),
            record("10", "old-anchor"),
            record("9", "older-unseen"),
        ]
        self.assertEqual(await poller.poll_once(), 1)
        self.assertEqual(self.submitted, ["/cloud/new"])

    async def test_old_record_changes_remain_ignored_but_new_record_revisions_run(
        self,
    ) -> None:
        await self.store.remember([record("10", "old")])
        client = FakeHistoryClient({1: [record("10", "old")]})
        poller = self.make_poller(client)
        await poller.poll_once()
        client.pages[1] = [record("11", "temporary"), record("10", "old")]
        await poller.poll_once()
        client.pages[1] = [record("11", "final"), record("10", "old-renamed")]
        await poller.poll_once()
        self.assertEqual(self.submitted, ["/cloud/temporary", "/cloud/final"])

    async def test_new_record_admission_survives_database_reopen(self) -> None:
        client = FakeHistoryClient({1: [record("10", "old")]})
        poller = self.make_poller(client)
        await poller.poll_once()
        client.pages[1] = [record("11", "temporary"), record("10", "old")]
        await poller.poll_once()
        restored = TTDCursor.from_dict(poller.cursor.to_dict())
        await self.store.close()
        self.store = HistoryStore(
            Path(self.directory.name) / "history.sqlite3", "new-only-test"
        )
        client.pages[1] = [record("11", "final"), record("10", "old")]
        await self.make_poller(client, restored).poll_once()
        self.assertEqual(self.submitted, ["/cloud/temporary", "/cloud/final"])

    async def test_restart_keeps_baseline_and_processes_offline_new_records(
        self,
    ) -> None:
        client = FakeHistoryClient({1: [record("10", "old")]})
        poller = self.make_poller(client)
        await poller.poll_once()
        baseline = poller.cursor.baseline_at
        restored = TTDCursor.from_dict(poller.cursor.to_dict())
        client.pages[1] = [record("11", "during-downtime"), record("10", "old")]
        restarted = self.make_poller(client, restored)
        await restarted.poll_once()
        self.assertEqual(self.submitted, ["/cloud/during-downtime"])
        self.assertEqual(restarted.cursor.baseline_at, baseline)

    async def test_migrated_history_cursor_gets_fresh_new_only_baseline(self) -> None:
        cursor = TTDCursor(initialized=True, seen_keys=["id:1"], audit_next_page=108)
        poller = self.make_poller(
            FakeHistoryClient({1: [record("20", "existing")]}), cursor
        )
        await poller.poll_once()
        self.assertEqual(self.submitted, [])
        self.assertEqual(poller.cursor.seen_keys, ["id:20"])
        self.assertEqual(poller.cursor.history_mode, TTDHistoryMode.NEW_ONLY)
        self.assertEqual(poller.cursor.audit_next_page, 0)

    async def test_missing_anchor_fails_closed_without_starting_history_audit(
        self,
    ) -> None:
        client = FakeHistoryClient({1: [record("10", "old")]}, page_size=1)
        poller = self.make_poller(client, max_pages=1)
        await poller.poll_once()
        client.pages[1] = [record("20", "new")]
        with self.assertRaises(TTDHistoryGapError):
            await poller.poll_once()
        self.assertEqual(self.submitted, [])
        self.assertTrue(poller.cursor.history_gap)
        self.assertIsNone(poller.audit)

    async def test_empty_baseline_accepts_subsequent_first_record(self) -> None:
        client = FakeHistoryClient({1: []})
        poller = self.make_poller(client)
        await poller.poll_once()
        client.pages[1] = [record("1", "first-new-record")]
        await poller.poll_once()
        self.assertEqual(self.submitted, ["/cloud/first-new-record"])

    async def test_empty_baseline_does_not_silently_skip_a_large_new_burst(
        self,
    ) -> None:
        client = FakeHistoryClient({1: []}, page_size=1)
        poller = self.make_poller(client, max_pages=1)
        await poller.poll_once()
        client.pages[1] = [record("20", "new")]
        with self.assertRaises(TTDHistoryGapError):
            await poller.poll_once()
        self.assertEqual(self.submitted, [])

    async def test_removed_anchors_do_not_turn_older_history_into_new_content(
        self,
    ) -> None:
        client = FakeHistoryClient({1: [record("10", "old-anchor")]})
        poller = self.make_poller(client)
        await poller.poll_once()
        client.pages[1] = [record("9", "older-history")]
        with self.assertRaises(TTDHistoryGapError):
            await poller.poll_once()
        self.assertEqual(self.submitted, [])

    async def test_manual_history_reconcile_is_rejected_in_new_only_mode(self) -> None:
        poller = self.make_poller(FakeHistoryClient({1: []}))
        with self.assertRaisesRegex(ValueError, "仅新增"):
            poller.request_reconcile()

    async def test_explicit_history_mode_processes_existing_records(self) -> None:
        client = FakeHistoryClient({1: [record("10", "existing")]})
        poller = self.make_poller(client, history_mode=TTDHistoryMode.INCLUDE_HISTORY)
        await poller.poll_once()
        self.assertEqual(self.submitted, ["/cloud/existing"])

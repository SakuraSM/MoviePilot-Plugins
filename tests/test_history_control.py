from __future__ import annotations

import asyncio
import copy
import tempfile
import unittest
from pathlib import Path

import _bootstrap  # noqa: F401
from clouddriveplexsync.event_worker import EventWorker
from clouddriveplexsync.history_control import HistoryControl
from clouddriveplexsync.history_models import TTDCursor, TTDHistoryMode, TTDHistoryPage
from clouddriveplexsync.history_poller import TTDHistoryPoller
from clouddriveplexsync.history_store import HistoryStore
from clouddriveplexsync.models import HISTORY_SOURCE
from clouddriveplexsync.path_mapper import PathMapper
from clouddriveplexsync.plex_bridge import PlexBridge
from test_plex_bridge import FakePlex, FakeSection, FakeService
from test_ttd_client import record


class BlockingHistoryClient:
    base_url = "https://ttd.example"
    source = "cloud"
    page_size = 1

    def __init__(self):
        self.calls = []
        self.started = asyncio.Event()
        self.cancelled = asyncio.Event()
        self.release = asyncio.Event()

    async def fetch_page(self, page):
        self.calls.append(page)
        self.started.set()
        try:
            await self.release.wait()
        except asyncio.CancelledError:
            self.cancelled.set()
            raise
        return TTDHistoryPage([record("1", "old")], 1, has_next=False)


class HistoryControlTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.store = HistoryStore(
            Path(self.directory.name) / "history.sqlite3", "scope"
        )
        self.section = FakeSection("1", "Movies", ["/plex"])
        self.saved, self.config_flags, self.logs = {}, [], []
        self.worker = EventWorker(
            PathMapper("/cloud", [("/cloud", "/plex")]),
            PlexBridge(FakeService(FakePlex([self.section])), ["1"]),
            scans_per_second=10000,
            persist_queue=lambda value: self.save("pending", value),
            persist_held=lambda value: self.save("held", value),
            on_info=self.logs.append,
        )
        self.client = BlockingHistoryClient()
        self.cursors = []
        self.poller = TTDHistoryPoller(
            self.client,
            lambda path, _source: self.worker.submit_scan_directory(
                path, HISTORY_SOURCE
            ),
            "/cloud",
            cursor=TTDCursor(audit_next_page=7),
            store=self.store,
            history_mode=TTDHistoryMode.INCLUDE_HISTORY,
            persist_cursor=self.cursors.append,
        )
        self.control = HistoryControl(
            self.poller, self.worker, self.config_flags.append
        )

    def save(self, key, value):
        self.saved[key] = copy.deepcopy(value)

    async def asyncTearDown(self):
        await self.control.close()
        await self.store.close()
        self.directory.cleanup()

    async def test_stop_cancels_inflight_query_and_resume_keeps_page(self):
        await self.control.set_enabled(True)
        await self.client.started.wait()
        await asyncio.wait_for(self.control.set_enabled(False), timeout=1)
        self.assertTrue(self.client.cancelled.is_set())
        self.assertEqual(self.client.calls, [7])
        self.assertEqual(self.poller.cursor.audit_next_page, 7)
        self.assertFalse(self.control.status()["running"])
        self.assertFalse(self.control.status()["query_in_progress"])
        self.assertFalse(
            await self.worker.submit_scan_directory("/cloud/rejected", HISTORY_SOURCE)
        )
        self.client.release.set()
        await self.control.set_enabled(True)
        for _ in range(500):
            if self.poller.cursor.audit_next_page == 0:
                break
            await asyncio.sleep(0.001)
        self.assertEqual(self.client.calls, [7, 7])
        self.assertEqual(self.poller.cursor.audit_next_page, 0)
        self.assertEqual(await self.worker.flush(force=True), 1)
        self.assertEqual(self.section.updates, ["/plex/old"])
        self.assertEqual(self.config_flags, [True, False, True])

    async def test_stop_holds_history_but_new_and_manual_scans_continue(self):
        self.worker.history_enabled = True
        for path, source in [
            ("historical", HISTORY_SOURCE),
            ("fresh", "ttd"),
            ("manual", "manual"),
        ]:
            self.assertTrue(
                await self.worker.submit_scan_directory(f"/cloud/{path}", source)
            )
        await self.control.set_enabled(False)
        self.assertEqual(len(self.saved["held"]), 1)
        self.assertEqual(await self.worker.flush(force=True), 2)
        self.assertEqual(set(self.section.updates), {"/plex/fresh", "/plex/manual"})
        await self.control.set_enabled(True)
        self.assertEqual(await self.worker.flush(force=True), 1)
        self.assertIn("/plex/historical", self.section.updates)
        self.assertEqual(self.saved["held"], [])
        self.assertTrue(any(line.startswith("HISTORY_STOPPED") for line in self.logs))

    async def test_historical_parent_never_absorbs_new_child(self):
        self.worker.history_enabled = True
        await self.worker.submit_scan_directory("/cloud/Movies", HISTORY_SOURCE)
        await self.worker.submit_scan_directory("/cloud/Movies/New", "ttd")
        self.assertEqual(len(self.worker.coalescer.pending), 2)
        await self.control.set_enabled(False)
        await self.worker.flush(force=True)
        self.assertEqual(self.section.updates, ["/plex/Movies/New"])
        self.assertEqual(self.saved["held"][0]["plex_path"], "/plex/Movies")

    async def test_disabled_manual_reconcile_does_not_start_history(self):
        with self.assertRaisesRegex(ValueError, "关闭"):
            await self.control.reconcile()
        self.assertEqual(self.client.calls, [])

    async def test_failed_backup_retains_jobs_and_closes_scan_gate(self):
        self.worker.history_enabled = True
        await self.worker.submit_scan_directory("/cloud/old", HISTORY_SOURCE)
        original = copy.deepcopy(self.saved["pending"])

        def fail(_values):
            raise OSError("storage unavailable")

        self.worker.persist_held = fail
        with self.assertRaises(OSError):
            await self.control.set_enabled(False)
        self.assertFalse(self.worker.history_enabled)
        self.assertEqual(self.saved["pending"], original)
        self.assertEqual(len(self.worker.coalescer.pending), 1)
        self.assertEqual(await self.worker.flush(force=True), 0)
        self.assertEqual(self.section.updates, [])

    async def test_restart_recovers_held_and_keeps_disabled_until_explicit_start(self):
        self.worker.history_enabled = True
        await self.worker.submit_scan_directory("/cloud/old", HISTORY_SOURCE)
        await self.control.set_enabled(False)
        restarted = EventWorker(
            self.worker.mapper, self.worker.plex, scans_per_second=10000
        )
        restarted.load_held_history(self.saved["held"])
        restarted.load_queue(self.saved["pending"])
        self.assertEqual(await restarted.flush(force=True), 0)
        await restarted.restore_history()
        restarted.history_enabled = True
        self.assertEqual(await restarted.flush(force=True), 1)
        self.assertEqual(self.section.updates, ["/plex/old"])

    async def test_failed_start_persistence_keeps_historical_scan_gate_closed(self):
        self.worker.history_enabled = True
        await self.worker.submit_scan_directory("/cloud/old", HISTORY_SOURCE)
        await self.control.set_enabled(False)

        def fail(_enabled):
            raise OSError("config unavailable")

        self.control.persist_enabled = fail
        with self.assertRaises(OSError):
            await self.control.set_enabled(True)
        self.assertFalse(self.worker.history_enabled)
        self.assertFalse(self.control.status()["running"])
        self.assertEqual(await self.worker.flush(force=True), 0)
        self.assertEqual(self.section.updates, [])
        self.assertTrue(self.saved["pending"])

    async def test_reconcile_cancels_current_query_before_restarting_first_page(self):
        await self.control.set_enabled(True)
        await self.client.started.wait()
        await self.control.reconcile()
        for _ in range(500):
            if len(self.client.calls) == 2:
                break
            await asyncio.sleep(0.001)
        self.assertTrue(self.client.cancelled.is_set())
        self.assertEqual(self.client.calls, [7, 1])
        self.assertEqual(self.poller.cursor.audit_next_page, 1)

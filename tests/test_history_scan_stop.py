from __future__ import annotations

import asyncio
import copy
import threading
import unittest
from unittest.mock import patch

import _bootstrap  # noqa: F401
from clouddriveplexsync.event_worker import EventWorker
from clouddriveplexsync.models import HISTORY_SOURCE
from clouddriveplexsync.path_mapper import PathMapper
from clouddriveplexsync.plex_bridge import PlexBridge
from test_plex_bridge import FakePlex, FakeSection, FakeService


class HistoryScanStopTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.section = FakeSection("1", "Movies", ["/plex"])
        self.saved = {}
        self.worker = EventWorker(
            PathMapper("/cloud", [("/cloud", "/plex")]),
            PlexBridge(FakeService(FakePlex([self.section])), ["1"]),
            scans_per_second=10000,
            debounce_seconds=0,
            persist_queue=lambda values: self.save("active", values),
            persist_held=lambda values: self.save("held", values),
        )
        self.worker.history_enabled = True

    def save(self, key, values):
        self.saved[key] = copy.deepcopy(values)

    async def test_stop_during_scan_prevents_next_history_request_and_worker_survives(
        self,
    ):
        started, release, finished = (
            threading.Event(),
            threading.Event(),
            threading.Event(),
        )
        original = self.section.update

        def blocking_update(path=None):
            original(path)
            started.set()
            try:
                release.wait(2)
            finally:
                finished.set()

        self.section.update = blocking_update
        for path in ("old-a", "old-b"):
            await self.worker.submit_scan_directory(f"/cloud/{path}", HISTORY_SOURCE)
        stop = asyncio.Event()
        worker_task = asyncio.create_task(self.worker.run(stop))
        try:
            self.assertTrue(await asyncio.to_thread(started.wait, 1))
            await asyncio.wait_for(self.worker.pause_history(), timeout=1)
            self.assertEqual(self.section.updates, ["/plex/old-a"])
            self.assertFalse(worker_task.done())
            self.assertEqual(
                {item["plex_path"] for item in self.saved["held"]},
                {"/plex/old-a", "/plex/old-b"},
            )
            self.assertEqual(self.saved["active"], [])
            release.set()
            self.assertTrue(await asyncio.to_thread(finished.wait, 1))
            self.section.update = original
            await self.worker.submit_scan_directory("/cloud/new", "ttd")
            for _ in range(500):
                if "/plex/new" in self.section.updates:
                    break
                await asyncio.sleep(0.001)
            self.assertEqual(self.section.updates, ["/plex/old-a", "/plex/new"])
            self.assertNotIn("/plex/old-b", self.section.updates)
        finally:
            release.set()
            stop.set()
            self.worker.flush_signal.set()
            await worker_task

    async def test_stop_closes_admission_while_plex_target_lookup_is_pending(self):
        started, release = threading.Event(), threading.Event()
        original = self.worker.plex.find_target

        def lookup(path):
            started.set()
            release.wait(2)
            return original(path)

        with patch.object(self.worker.plex, "find_target", side_effect=lookup):
            submission = asyncio.create_task(
                self.worker.submit_scan_directory("/cloud/old", HISTORY_SOURCE)
            )
            try:
                self.assertTrue(await asyncio.to_thread(started.wait, 1))
                await asyncio.wait_for(self.worker.pause_history(), timeout=1)
            finally:
                release.set()
            self.assertFalse(await submission)
        self.assertEqual(self.worker.queue_snapshot(), [])
        self.assertEqual(self.section.updates, [])

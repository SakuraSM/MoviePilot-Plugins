from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import _bootstrap  # noqa: F401
from clouddriveplexsync.event_worker import EventWorker
from clouddriveplexsync.history_poller import TTDHistoryPoller
from clouddriveplexsync.history_store import HistoryStore
from clouddriveplexsync.path_mapper import PathMapper
from clouddriveplexsync.plex_bridge import PlexBridge
from test_plex_bridge import FakePlex, FakeSection, FakeService
from test_ttd_client import FakeHistoryClient, record


class QualityUpgradeTests(unittest.IsolatedAsyncioTestCase):
    async def test_new_success_record_rescans_an_existing_media_directory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = HistoryStore(Path(directory) / "history.sqlite3", "scope")
            try:
                section = FakeSection("1", "TV", ["/plex"])
                worker = EventWorker(
                    PathMapper("/cloud", [("/cloud", "/plex")]),
                    PlexBridge(FakeService(FakePlex([section]))),
                    debounce_seconds=0,
                    scans_per_second=10000,
                )
                path = "Show/Season 1"
                client = FakeHistoryClient({1: [record("1", path)]})
                poller = TTDHistoryPoller(
                    client, worker.submit_scan_directory, "/cloud", store=store
                )
                await poller.poll_once()
                client.pages[1] = [record("2", path), record("1", path)]

                self.assertEqual(await poller.poll_once(), 1)
                self.assertEqual(await worker.flush(force=True), 1)
                self.assertEqual(section.updates, ["/plex/Show/Season 1"])
            finally:
                await store.close()

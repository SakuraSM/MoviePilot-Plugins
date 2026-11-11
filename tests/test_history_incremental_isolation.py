from __future__ import annotations

import asyncio
import tempfile
import threading
import unittest
from pathlib import Path

import _bootstrap  # noqa: F401
from clouddriveplexsync.event_worker import EventWorker
from clouddriveplexsync.history_models import TTDHistoryMode
from clouddriveplexsync.history_poller import TTDHistoryPoller
from clouddriveplexsync.history_store import HistoryStore
from clouddriveplexsync.models import HISTORY_SOURCE
from clouddriveplexsync.path_mapper import PathMapper
from clouddriveplexsync.plex_bridge import PlexBridge
from test_plex_bridge import FakePlex, FakeSection, FakeService
from test_ttd_client import FakeHistoryClient, record


class HistoryIncrementalIsolationTests(unittest.IsolatedAsyncioTestCase):
    async def test_history_origin_survives_restart_until_new_listener_accepts(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "history.sqlite3"
            entry = record("2", "new")
            store = HistoryStore(database, "scope")
            await store.remember([entry], historical=True)
            await store.close()
            store = HistoryStore(database, "scope")
            try:
                self.assertEqual(await store.unseen([entry]), [])
                self.assertEqual(
                    await store.unseen([entry], include_historical_only=True), [entry]
                )
                await store.remember([entry])
                await store.remember([entry], historical=True)
                self.assertEqual(
                    await store.unseen([entry], include_historical_only=True), []
                )
            finally:
                await store.close()

    async def test_resuming_history_does_not_wait_for_an_inflight_new_scan(
        self,
    ) -> None:
        section = FakeSection("1", "Movies", ["/plex"])
        worker = EventWorker(
            PathMapper("/cloud", [("/cloud", "/plex")]),
            PlexBridge(FakeService(FakePlex([section]))),
            debounce_seconds=0,
            scans_per_second=10000,
        )
        worker.history_enabled = True
        started, release = threading.Event(), threading.Event()
        original_update = section.update

        def blocking_update(path: str | None = None) -> None:
            original_update(path)
            if path == "/plex/new":
                started.set()
                release.wait(3)

        section.update = blocking_update
        await worker.submit_scan_directory("/cloud/new", "ttd")
        await worker.submit_scan_directory("/cloud/old", HISTORY_SOURCE)
        flushing = asyncio.create_task(worker.flush(force=True))
        try:
            self.assertTrue(await asyncio.to_thread(started.wait, 1))
            await asyncio.wait_for(worker.pause_history(), timeout=1)
            await asyncio.wait_for(worker.restore_history(), timeout=1)
            worker.history_enabled = True
        finally:
            release.set()
        await flushing
        await worker.flush(force=True)
        self.assertEqual(section.updates, ["/plex/new", "/plex/old"])

    async def test_stopping_history_does_not_restart_an_inflight_new_scan(self) -> None:
        section = FakeSection("1", "Movies", ["/plex"])
        worker = EventWorker(
            PathMapper("/cloud", [("/cloud", "/plex")]),
            PlexBridge(FakeService(FakePlex([section]))),
            debounce_seconds=0,
            scans_per_second=10000,
        )
        worker.history_enabled = True
        started, release = threading.Event(), threading.Event()
        original_update = section.update

        def blocking_update(path: str | None = None) -> None:
            original_update(path)
            started.set()
            release.wait(3)

        section.update = blocking_update
        await worker.submit_scan_directory("/cloud/new", "ttd")
        await worker.submit_scan_directory("/cloud/old", HISTORY_SOURCE)
        flushing = asyncio.create_task(worker.flush(force=True))
        try:
            self.assertTrue(await asyncio.to_thread(started.wait, 1))
            await asyncio.wait_for(worker.pause_history(), timeout=1)
            self.assertEqual(len(worker.held_history), 1)
        finally:
            release.set()
        await flushing
        await worker.flush(force=True)
        self.assertEqual(section.updates, ["/plex/new"])

    async def test_stopping_history_cannot_strand_a_new_record(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = HistoryStore(Path(directory) / "history.sqlite3", "scope")
            try:
                section = FakeSection("1", "Movies", ["/plex"])
                worker = EventWorker(
                    PathMapper("/cloud", [("/cloud", "/plex")]),
                    PlexBridge(FakeService(FakePlex([section]))),
                    debounce_seconds=0,
                    scans_per_second=10000,
                )
                worker.history_enabled = True
                client = FakeHistoryClient({1: [record("1", "anchor")]})
                new = TTDHistoryPoller(
                    client,
                    worker.submit_scan_directory,
                    "/cloud",
                    store=store,
                )
                historical = TTDHistoryPoller(
                    client,
                    lambda path, _source: worker.submit_scan_directory(
                        path, HISTORY_SOURCE
                    ),
                    "/cloud",
                    store=store,
                    history_mode=TTDHistoryMode.INCLUDE_HISTORY,
                )
                await new.poll_once()
                latest = record("2", "new")
                client.pages[1] = [latest, record("1", "anchor")]

                await historical._process_audit_records([latest])
                await worker.pause_history()
                await new.poll_once()
                await worker.flush(force=True)

                self.assertEqual(section.updates, ["/plex/new"])
                self.assertEqual(len(worker.held_history), 1)
            finally:
                await store.close()


if __name__ == "__main__":
    unittest.main()

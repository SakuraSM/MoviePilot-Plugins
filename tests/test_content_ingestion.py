from __future__ import annotations

import asyncio
import threading
import unittest
from unittest.mock import AsyncMock

import _bootstrap  # noqa: F401
from clouddriveplexsync.event_worker import EventWorker
from clouddriveplexsync.history_models import (
    TTDCursor,
    TTDHistoryMode,
    TTDHistoryRecord,
    TTDSubmitResult,
)
from clouddriveplexsync.history_parser import parse_history_payload
from clouddriveplexsync.history_poller import TTDHistoryPoller
from clouddriveplexsync.models import HISTORY_SOURCE
from clouddriveplexsync.path_mapper import PathMapper
from clouddriveplexsync.plex_bridge import PlexBridge
from test_plex_bridge import FakePlex, FakeSection, FakeService
from test_ttd_client import FakeHistoryClient, record


def media_record(record_id: str, directory: str, filename: str) -> TTDHistoryRecord:
    return TTDHistoryRecord(
        key=f"id:{record_id}",
        record_id=record_id,
        target_path=directory,
        file_name=filename,
        title="节目",
        media_type="TV",
        season_num=1,
        episode_num=int(record_id),
    )


class ContentIngestionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.section = FakeSection("1", "动漫", ["/plex", "/relocated"])
        self.worker = EventWorker(
            PathMapper("/cloud", [("/cloud", "/plex")]),
            PlexBridge(FakeService(FakePlex([self.section]))),
            debounce_seconds=0,
            scans_per_second=10000,
        )
        self.submitted = AsyncMock()
        self.worker.on_submitted = self.submitted

    async def test_first_record_is_durable_before_a_concurrent_flush(self) -> None:
        started, release = threading.Event(), threading.Event()

        def persist(_snapshot):
            if not started.is_set():
                started.set()
                release.wait(2)

        self.worker.persist_queue = persist
        entry = media_record("2", "节目", "E02.mkv")
        submission = asyncio.create_task(
            self.worker.submit_scan_directory("/cloud/节目", "ttd", records=[entry])
        )
        self.assertTrue(await asyncio.to_thread(started.wait, 1))
        flushing = asyncio.create_task(self.worker.flush(force=True))
        await asyncio.sleep(0)
        release.set()
        self.assertTrue(await submission)
        await flushing
        self.assertEqual(self.section.updates, ["/plex/节目"])
        self.assertEqual(
            self.submitted.await_args.args[1][0].expected_path, "/plex/节目/E02.mkv"
        )

    async def test_each_record_is_attached_even_if_an_earlier_directory_was_flushed(
        self,
    ) -> None:
        records = [
            media_record("3", "A", "A3.mkv"),
            media_record("2", "B", "B2.mkv"),
            media_record("1", "A", "A1.mkv"),
            record("0", "anchor"),
        ]
        client = FakeHistoryClient({1: records})

        async def submit_records(
            path: str, grouped: list[TTDHistoryRecord]
        ) -> TTDSubmitResult:
            if path.endswith("/B"):
                await self.worker.flush(force=True)
            accepted = await self.worker.submit_scan_directory(
                path, "ttd", records=grouped
            )
            return (
                TTDSubmitResult.queued()
                if accepted
                else TTDSubmitResult.retry("queue full")
            )

        poller = TTDHistoryPoller(
            client,
            lambda _path, _source: True,
            "/cloud",
            cursor=TTDCursor(initialized=True, seen_keys=["id:0"]),
            history_mode=TTDHistoryMode.INCLUDE_HISTORY,
            submit_records=submit_records,
        )
        await poller.poll_once()
        await self.worker.flush(force=True)
        attached = [
            hint.expected_path
            for call in self.submitted.await_args_list
            for hint in call.args[1]
        ]
        self.assertEqual(
            set(attached), {"/plex/A/A1.mkv", "/plex/B/B2.mkv", "/plex/A/A3.mkv"}
        )

    async def test_mapping_change_recomputes_expected_file_before_confirmation(
        self,
    ) -> None:
        entry = media_record("2", "节目/Season 1", "E02.mkv")
        await self.worker.submit_scan_directory(
            "/cloud/节目/Season 1", "ttd", records=[entry]
        )
        snapshot = self.worker.queue_snapshot()
        restarted = EventWorker(
            self.worker.mapper, self.worker.plex, scans_per_second=10000
        )
        restarted.load_queue(snapshot)
        restarted.mapper.mappings = [("/cloud", "/relocated")]
        restarted.on_submitted = self.submitted
        await restarted.flush(force=True)
        self.assertEqual(
            self.submitted.await_args.args[1][0].expected_path,
            "/relocated/节目/Season 1/E02.mkv",
        )

    async def test_failed_plex_request_does_not_create_verification_work(self) -> None:
        await self.worker.submit_scan_directory(
            "/cloud/节目", "ttd", records=[media_record("2", "节目", "E02.mkv")]
        )

        def fail(_target):
            raise RuntimeError("Plex unavailable")

        self.worker.plex.scan = fail
        await self.worker.flush(force=True)
        self.submitted.assert_not_awaited()

    async def test_confirmation_storage_failure_does_not_repeat_a_successful_scan(
        self,
    ) -> None:
        await self.worker.submit_scan_directory(
            "/cloud/节目", "ttd", records=[media_record("2", "节目", "E02.mkv")]
        )
        self.submitted.side_effect = OSError("database unavailable")
        self.assertEqual(await self.worker.flush(force=True), 1)
        self.assertEqual(self.section.updates, ["/plex/节目"])
        self.assertEqual(len(self.worker.coalescer.pending), 0)
        self.assertTrue(
            any("核验任务保存失败" in message for message in self.worker.recent)
        )

    async def test_paused_history_preserves_media_for_later_confirmation(self) -> None:
        self.worker.history_enabled = True
        await self.worker.submit_scan_directory(
            "/cloud/节目",
            HISTORY_SOURCE,
            records=[media_record("2", "节目", "E02.mkv")],
        )
        await self.worker.pause_history()
        self.assertEqual(len(next(iter(self.worker.held_history.values())).media), 1)
        await self.worker.restore_history()
        self.worker.history_enabled = True
        await self.worker.flush(force=True)
        self.assertEqual(
            self.submitted.await_args.args[1][0].expected_path, "/plex/节目/E02.mkv"
        )

    def test_parser_keeps_revision_compatible_with_preexisting_ledger(self) -> None:
        payload = {
            "records": [
                {
                    "id": 1,
                    "target_path": "节目",
                    "status": "success",
                    "file_name": "new.mkv",
                    "title": "节目",
                    "media_type": "TV",
                    "season_num": 1,
                    "episode_num": 2,
                }
            ]
        }
        parsed = parse_history_payload(payload)[0]
        previous = TTDHistoryRecord(key="id:1", record_id="1", target_path="节目")
        self.assertEqual(parsed.revision, previous.revision)
        self.assertEqual(parsed.file_name, "new.mkv")
        self.assertEqual((parsed.season_num, parsed.episode_num), (1, 2))

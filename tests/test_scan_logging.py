from __future__ import annotations

import json
import unittest

import _bootstrap  # noqa: F401
from clouddriveplexsync.event_worker import EventWorker
from clouddriveplexsync.path_mapper import PathMapper
from clouddriveplexsync.plex_bridge import PlexBridge
from test_plex_bridge import FakePlex, FakeSection, FakeService


class ScanLoggingTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.messages: list[str] = []
        self.section = FakeSection("1", "电影", ["/plex", "/relocated"])
        self.worker = EventWorker(
            PathMapper("/cloud", [("/cloud", "/plex")]),
            PlexBridge(FakeService(FakePlex([self.section])), ["1"]),
            on_info=self.messages.append,
            scans_per_second=10000,
        )

    def log_fields(self, stage: str) -> list[dict[str, str]]:
        return [
            json.loads(message.partition(" ")[2])
            for message in self.messages
            if message.partition(" ")[0] == stage
        ]

    async def test_queue_and_scan_logs_match_actual_plex_request(self) -> None:
        await self.worker.submit_scan_directory("/cloud/Movies/Film", "ttd")
        self.assertEqual(await self.worker.flush(force=True), 1)
        for stage in ("QUEUE", "SCAN_START", "SCAN_SUBMITTED"):
            fields = self.log_fields(stage)[0]
            self.assertEqual(fields["source"], "ttd")
            self.assertEqual(fields["ttd_path"], "/cloud/Movies/Film")
            self.assertEqual(fields["plex_path"], self.section.updates[0])
            self.assertEqual(fields["section_id"], "1")
            self.assertEqual(fields["section_title"], "电影")
            self.assertEqual(fields["section_root"], "/plex")
        self.assertEqual(list(self.worker.recent), self.messages)

    async def test_execution_logs_use_recomputed_mapping_and_library_root(self) -> None:
        await self.worker.submit_scan_directory("/cloud/Movies/Film", "ttd")
        self.worker.mapper.mappings = [("/cloud", "/relocated")]
        await self.worker.flush(force=True)
        self.assertEqual(self.log_fields("QUEUE")[0]["plex_path"], "/plex/Movies/Film")
        for stage in ("SCAN_START", "SCAN_SUBMITTED"):
            fields = self.log_fields(stage)[0]
            self.assertEqual(fields["plex_path"], "/relocated/Movies/Film")
            self.assertEqual(fields["section_root"], "/relocated")
            self.assertEqual(fields["plex_path"], self.section.updates[0])

    async def test_merged_child_logs_effective_queued_directory(self) -> None:
        await self.worker.submit_scan_directory("/cloud/Movies", "manual")
        await self.worker.submit_scan_directory("/cloud/Movies/Film", "ttd")
        child = self.log_fields("QUEUE")[1]
        self.assertEqual(child["plex_path"], "/plex/Movies/Film")
        self.assertEqual(child["queued_plex_path"], "/plex/Movies")
        self.assertEqual(await self.worker.flush(force=True), 1)
        self.assertEqual(
            self.log_fields("SCAN_SUBMITTED")[0]["plex_path"], "/plex/Movies"
        )

    async def test_rejected_request_has_no_success_log(self) -> None:
        await self.worker.submit_scan_directory("/cloud/Movies/Film", "ttd")

        def reject(_target: object) -> None:
            raise RuntimeError("Plex unavailable")

        self.worker.plex.scan = reject
        self.assertEqual(await self.worker.flush(force=True), 0)
        self.assertEqual(len(self.log_fields("SCAN_START")), 1)
        self.assertEqual(self.log_fields("SCAN_SUBMITTED"), [])
        self.assertEqual(len(self.worker.coalescer.pending), 1)

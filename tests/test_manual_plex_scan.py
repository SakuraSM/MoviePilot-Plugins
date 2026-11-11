from __future__ import annotations

import copy
import json
import unittest

import _bootstrap  # noqa: F401
from clouddriveplexsync.event_worker import EventWorker
from clouddriveplexsync.path_mapper import PathMapper, PathMappingError
from clouddriveplexsync.plex_bridge import PlexBridge, PlexLibraryNotFoundError
from test_plex_bridge import FakePlex, FakeSection, FakeService


class ManualPlexScanTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.section = FakeSection("1", "Movies", ["/plex", "/media/movies"])
        other = FakeSection("2", "Other", ["/media/other"])
        self.bridge = PlexBridge(FakeService(FakePlex([self.section, other])), ["1"])
        self.saved: list[list[dict[str, object]]] = []
        self.worker = EventWorker(
            PathMapper("/cloud", [("/cloud", "/plex")]),
            self.bridge,
            debounce_seconds=30,
            scans_per_second=10000,
            persist_queue=lambda values: self.saved.append(copy.deepcopy(values)),
            clock=lambda: 100.0,
        )

    async def test_manual_plex_path_scans_without_cloud_mapping_or_debounce(
        self,
    ) -> None:
        self.assertTrue(
            await self.worker.submit_plex_scan_directory("/media/movies/Film")
        )
        self.assertEqual(await self.worker.flush(), 1)
        self.assertEqual(self.section.updates, ["/media/movies/Film"])

    async def test_manual_path_is_recovered_after_restart_without_remapping(
        self,
    ) -> None:
        await self.worker.submit_plex_scan_directory("/media/movies/Film")
        saved = self.saved[-1]
        restarted = EventWorker(
            PathMapper("/new-cloud", [("/new-cloud", "/another-plex")]),
            self.bridge,
            scans_per_second=10000,
            clock=lambda: 101.0,
        )
        restarted.load_queue(saved)
        self.assertEqual(await restarted.flush(), 1)
        self.assertEqual(self.section.updates, ["/media/movies/Film"])

    async def test_manual_path_must_be_absolute_and_in_a_selected_library(self) -> None:
        for path in ("relative/Film", "/media/movies/../other/Film"):
            with self.subTest(path=path), self.assertRaises(PathMappingError):
                await self.worker.submit_plex_scan_directory(path)
        with self.assertRaisesRegex(PlexLibraryNotFoundError, "目录不属于所选"):
            await self.worker.submit_plex_scan_directory("/media/other/Film")
        self.assertEqual(self.worker.queue_snapshot(), [])

    async def test_direct_manual_scan_does_not_force_cloud_queue(self) -> None:
        await self.worker.submit_scan_directory("/cloud/Film", "ttd")
        await self.worker.submit_plex_scan_directory("/plex/Film")
        self.assertEqual(await self.worker.flush(), 1)
        self.assertEqual(self.section.updates, ["/plex/Film"])
        self.assertEqual(len(self.worker.coalescer.pending), 1)

    async def test_failed_manual_scan_is_retained_and_uses_retry_debounce(self) -> None:
        await self.worker.submit_plex_scan_directory("/media/movies/Film")

        def fail(_target: object) -> None:
            raise RuntimeError("Plex unavailable")

        self.worker.plex.scan = fail
        self.assertEqual(await self.worker.flush(), 0)
        self.assertEqual(await self.worker.flush(), 0)
        self.assertEqual(len(self.worker.coalescer.pending), 1)
        saved = json.dumps(self.saved[-1], ensure_ascii=False)
        self.assertIn("/media/movies/Film", saved)

    async def test_new_child_request_retries_a_previously_failed_manual_parent(
        self,
    ) -> None:
        await self.worker.submit_plex_scan_directory("/media/movies/Film")
        original_scan = self.worker.plex.scan

        def fail(_target: object) -> None:
            raise RuntimeError("Plex unavailable")

        self.worker.plex.scan = fail
        self.assertEqual(await self.worker.flush(), 0)
        self.worker.plex.scan = original_scan
        await self.worker.submit_plex_scan_directory("/media/movies/Film/Season 1")
        self.assertEqual(await self.worker.flush(), 1)
        self.assertEqual(self.section.updates, ["/media/movies/Film"])

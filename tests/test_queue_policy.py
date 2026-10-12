from __future__ import annotations

import copy
import unittest
from dataclasses import asdict

import _bootstrap  # noqa: F401
from clouddriveplexsync.event_worker import EventWorker
from clouddriveplexsync.history_models import TTDCursor, TTDHistoryMode
from clouddriveplexsync.history_policy import HistoryPolicy
from clouddriveplexsync.models import PendingScan
from clouddriveplexsync.path_mapper import PathMapper
from clouddriveplexsync.plex_bridge import PlexBridge
from clouddriveplexsync.queue_policy import prepare_scan_queue
from test_plex_bridge import FakePlex, FakeSection, FakeService


def scan(path: str, source: str) -> dict[str, object]:
    return asdict(
        PendingScan(
            cloud_path=f"/cloud/{path}",
            plex_path=f"/plex/{path}",
            section_id="1",
            section_title="Movies",
            first_seen=0,
            last_seen=0,
            sources=[source],
        )
    )


class QueuePolicyTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.saved: dict[str, object] = {}
        self.section = FakeSection("1", "Movies", ["/plex"])
        self.worker = EventWorker(
            PathMapper("/cloud", [("/cloud", "/plex")]),
            PlexBridge(FakeService(FakePlex([self.section])), ["1"]),
            persist_queue=lambda values: self.save("pending_scans", values),
            scans_per_second=10000,
        )

    def save(self, key: str, value: object) -> None:
        self.saved[key] = copy.deepcopy(value)

    async def test_new_only_holds_automatic_jobs_and_preserves_manual_jobs(
        self,
    ) -> None:
        self.saved["pending_scans"] = [
            scan("historical", "ttd"),
            scan("manual", "manual"),
        ]
        policy = HistoryPolicy.prepare(
            mode=TTDHistoryMode.NEW_ONLY, cursor=TTDCursor(), scope="scope"
        )
        count = await prepare_scan_queue(
            worker=self.worker, policy=policy, load=self.saved.get, save=self.save
        )
        self.assertEqual(count, 1)
        self.assertEqual(self.saved["held_history_scans"], [scan("historical", "ttd")])
        self.assertEqual(await self.worker.flush(force=True), 1)
        self.assertEqual(self.section.updates, ["/plex/manual"])

    async def test_regular_restart_keeps_new_only_pending_jobs(self) -> None:
        policy = HistoryPolicy.prepare(
            mode=TTDHistoryMode.NEW_ONLY, cursor=TTDCursor(), scope="scope"
        )
        policy.establish_baseline([], now=10)
        self.saved["pending_scans"] = [scan("newly-observed", "ttd")]
        count = await prepare_scan_queue(
            worker=self.worker, policy=policy, load=self.saved.get, save=self.save
        )
        self.assertEqual(count, 0)
        self.assertEqual(await self.worker.flush(force=True), 1)
        self.assertEqual(self.section.updates, ["/plex/newly-observed"])

    async def test_history_mode_restores_held_jobs_after_persisting_queue(self) -> None:
        self.saved["held_history_scans"] = [scan("historical", "ttd")]
        policy = HistoryPolicy.prepare(
            mode=TTDHistoryMode.INCLUDE_HISTORY, cursor=TTDCursor(), scope="scope"
        )
        await prepare_scan_queue(
            worker=self.worker, policy=policy, load=self.saved.get, save=self.save
        )
        self.assertEqual(self.saved["held_history_scans"], [])
        self.assertEqual(self.saved["pending_scans"], [scan("historical", "ttd")])
        self.assertEqual(await self.worker.flush(force=True), 1)
        self.assertEqual(self.section.updates, ["/plex/historical"])

    async def test_failed_backup_does_not_remove_original_pending_jobs(self) -> None:
        original = [scan("historical", "ttd")]
        self.saved["pending_scans"] = copy.deepcopy(original)
        policy = HistoryPolicy.prepare(
            mode=TTDHistoryMode.NEW_ONLY, cursor=TTDCursor(), scope="scope"
        )

        def fail_backup(_key: str, _value: object) -> None:
            raise OSError("storage unavailable")

        with self.assertRaises(OSError):
            await prepare_scan_queue(
                worker=self.worker, policy=policy, load=self.saved.get, save=fail_backup
            )
        self.assertEqual(self.saved["pending_scans"], original)
        self.assertEqual(self.section.updates, [])

    async def test_failed_queue_restore_keeps_backup(self) -> None:
        held = [scan("historical", "ttd")]
        self.saved["held_history_scans"] = held
        policy = HistoryPolicy.prepare(
            mode=TTDHistoryMode.INCLUDE_HISTORY, cursor=TTDCursor(), scope="scope"
        )

        def fail_persist(_values: object) -> None:
            raise OSError("storage unavailable")

        self.worker.persist_queue = fail_persist
        with self.assertRaises(OSError):
            await prepare_scan_queue(
                worker=self.worker, policy=policy, load=self.saved.get, save=self.save
            )
        self.assertEqual(self.saved["held_history_scans"], held)

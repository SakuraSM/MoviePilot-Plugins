from __future__ import annotations

import asyncio
import copy
import threading
import unittest

import _bootstrap  # noqa: F401
from clouddriveplexsync.event_worker import EventWorker, ScanCoalescer
from clouddriveplexsync.models import PendingScan
from clouddriveplexsync.path_mapper import PathMapper
from clouddriveplexsync.plex_bridge import PlexBridge
from test_plex_bridge import FakePlex, FakeSection, FakeService


def pending(path: str, seen: float = 0) -> PendingScan:
    return PendingScan(
        cloud_path="/cloud" + path,
        plex_path=path,
        section_id="1",
        section_title="Movies",
        first_seen=seen,
        last_seen=seen,
        sources=["ttd"],
    )


class ScanCoalescerTests(unittest.TestCase):
    def test_same_directory_is_deduplicated(self) -> None:
        queue = ScanCoalescer(debounce_seconds=30)
        for index in range(100):
            queue.add(pending("/movies/Film", index))
        self.assertEqual(len(queue.pending), 1)
        self.assertEqual(queue.due(now=128), [])
        self.assertEqual(len(queue.due(now=129)), 1)

    def test_explicit_ancestor_replaces_descendants(self) -> None:
        queue = ScanCoalescer()
        queue.add(pending("/shows/A/Season 1"))
        queue.add(pending("/shows/A"))
        self.assertEqual(
            [item.plex_path for item in queue.pending.values()], ["/shows/A"]
        )

    def test_siblings_are_not_promoted_to_common_root(self) -> None:
        queue = ScanCoalescer()
        queue.add(pending("/movies/A"))
        queue.add(pending("/movies/B"))
        self.assertEqual(len(queue.pending), 2)


class ScanWorkerTests(unittest.IsolatedAsyncioTestCase):
    def make_worker(self, persist=None, capacity=10000):
        self.section = FakeSection("1", "Movies", ["/plex"])
        bridge = PlexBridge(FakeService(FakePlex([self.section])), ["1"])
        return EventWorker(
            PathMapper("/cloud", [("/cloud", "/plex")]),
            bridge,
            persist_queue=persist,
            scans_per_second=10000,
            capacity=capacity,
        )

    async def test_exact_directory_is_scanned_without_promoting_parent(self) -> None:
        snapshots = []
        worker = self.make_worker(snapshots.append)
        self.assertTrue(await worker.submit_scan_directory("/cloud/电影/片名"))
        self.assertEqual(await worker.flush(force=True), 1)
        self.assertEqual(self.section.updates, ["/plex/电影/片名"])
        self.assertEqual(snapshots[-1], [])

    async def test_crash_snapshot_retains_inflight_job_for_restart(self) -> None:
        snapshots = []
        worker = self.make_worker(snapshots.append)
        await worker.submit_scan_directory("/cloud/电影/片名")
        crash_snapshot = []

        def interrupted_scan(_target):
            crash_snapshot.extend(copy.deepcopy(snapshots[-1]))
            raise asyncio.CancelledError("interrupted before Plex accepted request")

        worker.plex.scan = interrupted_scan
        with self.assertRaises(asyncio.CancelledError):
            await worker.flush(force=True)
        self.assertEqual(crash_snapshot[0]["state"], "inflight")
        restarted = self.make_worker(snapshots.append)
        restarted.load_queue(crash_snapshot)
        self.assertEqual(await restarted.flush(force=True), 1)
        self.assertEqual(self.section.updates, ["/plex/电影/片名"])

    async def test_persistence_failure_prevents_scan_and_retains_job(self) -> None:
        worker = self.make_worker()
        await worker.submit_scan_directory("/cloud/电影/片名")

        def fail_save(_values):
            raise OSError("storage unavailable")

        worker.persist_queue = fail_save
        with self.assertRaises(OSError):
            await worker.flush(force=True)
        self.assertEqual(self.section.updates, [])
        self.assertEqual(len(worker.coalescer.pending), 1)

    async def test_new_change_during_scan_is_kept_for_another_scan(self) -> None:
        snapshots = []
        worker = self.make_worker(snapshots.append)
        await worker.submit_scan_directory("/cloud/电影/片名")
        started, release = threading.Event(), threading.Event()
        original_scan = worker.plex.scan

        def blocking_scan(target):
            started.set()
            release.wait(timeout=5)
            original_scan(target)

        worker.plex.scan = blocking_scan
        task = asyncio.create_task(worker.flush(force=True))
        try:
            self.assertTrue(await asyncio.to_thread(started.wait, 2))
            self.assertTrue(await worker.submit_scan_directory("/cloud/电影/片名"))
        finally:
            release.set()
        await task
        self.assertEqual(len(worker.coalescer.pending), 1)
        self.assertEqual(len(snapshots[-1]), 1)
        self.assertEqual(await worker.flush(force=True), 1)
        self.assertEqual(len(self.section.updates), 2)

    async def test_cancelled_flush_requeues_all_unfinished_jobs(self) -> None:
        snapshots = []
        worker = self.make_worker(snapshots.append)
        await worker.submit_scan_directory("/cloud/电影/A")
        await worker.submit_scan_directory("/cloud/电影/B")
        worker.scan_interval = 100
        task = asyncio.create_task(worker.flush(force=True))
        for _ in range(100):
            if self.section.updates:
                break
            await asyncio.sleep(0.001)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(len(snapshots[-1]), 1)
        self.assertEqual(snapshots[-1][0]["cloud_path"], "/cloud/电影/B")

    async def test_old_queue_fields_can_be_recovered(self) -> None:
        worker = self.make_worker()
        snapshot = pending("/Film").__dict__ | {"cloud_identity": "old-cloud"}
        snapshot["cloud_path"], snapshot["plex_path"] = "/cloud/Film", "/plex/Film"
        worker.load_queue([snapshot])
        self.assertEqual(await worker.flush(force=True), 1)

    async def test_full_queue_rejects_record_for_retry(self) -> None:
        worker = self.make_worker(capacity=1)
        self.assertTrue(await worker.submit_scan_directory("/cloud/A"))
        self.assertFalse(await worker.submit_scan_directory("/cloud/B"))

    async def test_failed_directory_does_not_block_other_scans(self) -> None:
        worker = self.make_worker()
        await worker.submit_scan_directory("/cloud/A")
        await worker.submit_scan_directory("/cloud/B")
        original_scan = worker.plex.scan

        def scan(target):
            if target.path.endswith("/A"):
                raise RuntimeError("temporarily unavailable")
            original_scan(target)

        worker.plex.scan = scan
        self.assertEqual(await worker.flush(force=True), 1)
        self.assertEqual(self.section.updates, ["/plex/B"])
        self.assertEqual(len(worker.coalescer.pending), 1)

    async def test_invalid_or_over_capacity_recovered_queue_is_rejected(self) -> None:
        worker = self.make_worker(capacity=1)
        with self.assertRaises(ValueError):
            worker.load_queue([pending("/A").__dict__, pending("/B").__dict__])
        with self.assertRaises(ValueError):
            worker.load_queue([{"cloud_path": "/cloud/A"}])

    async def test_cancelled_persistence_finishes_before_next_snapshot(self) -> None:
        worker = self.make_worker()
        await worker.submit_scan_directory("/cloud/A")
        started, release = threading.Event(), threading.Event()
        snapshots = []

        def save(values):
            if not snapshots:
                started.set()
                release.wait(timeout=5)
            snapshots.append(copy.deepcopy(values))

        worker.persist_queue = save
        flushing = asyncio.create_task(worker.flush(force=True))
        self.assertTrue(await asyncio.to_thread(started.wait, 2))
        flushing.cancel()
        submitting = asyncio.create_task(worker.submit_scan_directory("/cloud/B"))
        await asyncio.sleep(0)
        self.assertFalse(flushing.done())
        release.set()
        with self.assertRaises(asyncio.CancelledError):
            await flushing
        await submitting
        self.assertEqual(
            {item["cloud_path"] for item in snapshots[-1]}, {"/cloud/A", "/cloud/B"}
        )

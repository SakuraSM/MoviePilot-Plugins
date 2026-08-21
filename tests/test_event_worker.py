from __future__ import annotations

import asyncio
import unittest

import _bootstrap  # noqa: F401
from clouddriveplexsync.event_worker import EventWorker, ScanCoalescer
from clouddriveplexsync.models import ChangeType, CloudApi, FileSystemChange, MountPoint, PendingScan
from clouddriveplexsync.path_mapper import PathMapper, parse_override_lines
from clouddriveplexsync.plex_bridge import PlexBridge


def pending(path: str, seen: float = 0) -> PendingScan:
    return PendingScan(
        cloud_path="/cloud" + path,
        plex_path=path,
        section_id="1",
        section_title="Movies",
        cloud_identity="Cloud|user",
        first_seen=seen,
        last_seen=seen,
        sources=["cd2"],
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
        self.assertEqual([item.plex_path for item in queue.pending.values()], ["/shows/A"])

    def test_siblings_are_not_promoted_to_common_root(self) -> None:
        queue = ScanCoalescer()
        queue.add(pending("/movies/A"))
        queue.add(pending("/movies/B"))
        self.assertEqual(len(queue.pending), 2)


class FakeSection:
    key = "1"
    title = "Movies"
    locations = ["/data/CloudNas/Guangya/Media/Video/已整理"]
    refreshing = False

    def __init__(self) -> None:
        self.updates = []

    def update(self, path=None) -> None:
        self.updates.append(path)


class FakePlex:
    version = "1.40.0.0"
    activities = []

    def __init__(self, section) -> None:
        self.library = type("Library", (), {"sections": lambda _self: [section]})()

    def sessions(self):
        return []


class FakeService:
    def __init__(self, plex) -> None:
        self._plex = plex

    def get_plex(self):
        return self._plex


class FakeBufferManager:
    def __init__(self) -> None:
        self.prepared = []

    async def prepare(self, cloud, prefixes, sections):
        self.prepared.append((cloud.identity, list(prefixes), set(sections)))
        return True


class EventWorkerIntegrationTests(unittest.TestCase):
    def test_change_becomes_one_exact_plex_scan(self) -> None:
        mapper = PathMapper(
            watch_roots=["/光鸭云盘/Media/Video/已整理"],
            mounts=[MountPoint("/CloudNAS/Guangya", "/光鸭云盘", is_mounted=True)],
            plex_overrides=parse_override_lines(
                "/CloudNAS/Guangya => /data/CloudNas/Guangya"
            ),
        )
        section = FakeSection()
        bridge = PlexBridge(FakeService(FakePlex(section)), ["1"])
        buffer_manager = FakeBufferManager()
        worker = EventWorker(
            mapper,
            bridge,
            buffer_manager,
            [CloudApi("GuangYaPan", "u", path="/光鸭云盘")],
            debounce_seconds=30,
            scans_per_second=1000,
        )
        worker.submit_change(
            FileSystemChange(
                ChangeType.CREATE,
                False,
                "/光鸭云盘/Media/Video/已整理/电影/片名/movie.mkv",
            )
        )
        self.assertEqual(asyncio.run(worker.flush(force=True)), 1)
        self.assertEqual(
            section.updates,
            ["/data/CloudNas/Guangya/Media/Video/已整理/电影/片名"],
        )
        self.assertEqual(buffer_manager.prepared[0][0], "GuangYaPan|u")

    def test_manual_directory_is_not_promoted_to_parent(self) -> None:
        mapper = PathMapper(
            watch_roots=["/光鸭云盘/Media/Video/已整理"],
            mounts=[MountPoint("/CloudNAS/Guangya", "/光鸭云盘", is_mounted=True)],
            plex_overrides=parse_override_lines(
                "/CloudNAS/Guangya => /data/CloudNas/Guangya"
            ),
        )
        section = FakeSection()
        worker = EventWorker(
            mapper,
            PlexBridge(FakeService(FakePlex(section)), ["1"]),
            FakeBufferManager(),
            [CloudApi("GuangYaPan", "u", path="/光鸭云盘")],
            debounce_seconds=30,
            scans_per_second=1000,
        )
        directory = "/光鸭云盘/Media/Video/已整理/电影/片名"
        self.assertTrue(worker.submit_scan_directory(directory))
        self.assertEqual(asyncio.run(worker.flush(force=True)), 1)
        self.assertEqual(
            section.updates,
            ["/data/CloudNas/Guangya/Media/Video/已整理/电影/片名"],
        )

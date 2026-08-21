from __future__ import annotations

import unittest

import _bootstrap  # noqa: F401
from clouddriveplexsync.path_mapper import PathMappingError
from clouddriveplexsync.plex_bridge import PlexBridge


class FakeSection:
    def __init__(self, key: str, title: str, locations: list[str]) -> None:
        self.key = key
        self.title = title
        self.locations = locations
        self.refreshing = False
        self.updates = []

    def update(self, path=None) -> None:
        self.updates.append(path)


class FakeLibrary:
    def __init__(self, sections) -> None:
        self._sections = sections

    def sections(self):
        return self._sections


class FakePlex:
    version = "1.40.4.8679"
    activities = []

    def __init__(self, sections) -> None:
        self.library = FakeLibrary(sections)

    def sessions(self):
        return []


class FakeService:
    def __init__(self, plex) -> None:
        self._plex = plex

    def get_plex(self):
        return self._plex


class PlexBridgeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.movies = FakeSection("1", "Movies", ["/data/CloudNas/Movies"])
        self.tv = FakeSection("2", "TV", ["/data/CloudNas/TV"])
        self.bridge = PlexBridge(FakeService(FakePlex([self.movies, self.tv])), ["1", "2"])

    def test_exact_partial_scan(self) -> None:
        target = self.bridge.find_target("/data/CloudNas/Movies/Film")
        self.bridge.scan(target)
        self.assertEqual(self.movies.updates, ["/data/CloudNas/Movies/Film"])
        self.assertEqual(self.tv.updates, [])

    def test_unmapped_path_does_not_scan_anything(self) -> None:
        with self.assertRaises(PathMappingError):
            self.bridge.find_target("/data/Elsewhere/Film")
        self.assertEqual(self.movies.updates, [])
        self.assertEqual(self.tv.updates, [])

    def test_activities_method_is_supported(self) -> None:
        plex = FakePlex([self.movies])
        plex.activities = lambda: [type("Activity", (), {"type": "library.refresh"})()]
        bridge = PlexBridge(FakeService(plex), ["1"])
        self.assertTrue(bridge.is_scanning(["1"]))

from __future__ import annotations

import unittest

import _bootstrap  # noqa: F401
from clouddriveplexsync.plex_bridge import PlexBridge, PlexLibraryNotFoundError
from test_plex_bridge import FakePlex, FakeSection, FakeService


class PlexLinuxPathTests(unittest.TestCase):
    def test_linux_library_with_double_slash_matches_normal_mapped_path(self) -> None:
        section = FakeSection("4", "综艺", ["//data/CloudNas/Guangya/综艺/其他综艺"])
        plex = FakePlex([section])
        plex.platform = "Linux"
        bridge = PlexBridge(FakeService(plex))

        target = bridge.find_target("/data/CloudNas/Guangya/综艺/其他综艺/节目")
        bridge.scan(target)
        self.assertEqual(target.section_id, "4")
        self.assertEqual(section.updates, ["/data/CloudNas/Guangya/综艺/其他综艺/节目"])

    def test_windows_unc_path_is_not_collapsed_to_a_local_path(self) -> None:
        section = FakeSection("1", "Movies", ["//server/share/Movies"])
        plex = FakePlex([section])
        plex.platform = "Windows"
        bridge = PlexBridge(FakeService(plex))

        with self.assertRaises(PlexLibraryNotFoundError):
            bridge.find_target("/server/share/Movies/Film")
        self.assertEqual(
            bridge.find_target("//server/share/Movies/Film").location,
            "//server/share/Movies",
        )

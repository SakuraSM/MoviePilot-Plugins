from __future__ import annotations

import unittest

import _bootstrap  # noqa: F401
from clouddriveplexsync.path_mapper import (
    PathMapper,
    PathMappingError,
    is_under,
    parse_override_lines,
)


class PathMapperTests(unittest.TestCase):
    def setUp(self) -> None:
        self.mapper = PathMapper(
            "/cloud", parse_override_lines("/cloud => /plex\n/cloud/TV => /tv")
        )

    def test_longest_prefix_mapping_wins(self) -> None:
        self.assertEqual(
            self.mapper.cloud_to_plex("/cloud/TV/Show/Season 1"), "/tv/Show/Season 1"
        )
        self.assertEqual(
            self.mapper.cloud_to_plex("/cloud/Movies/Film"), "/plex/Movies/Film"
        )

    def test_path_boundaries_are_preserved(self) -> None:
        self.assertFalse(is_under("/cloud2/Film", "/cloud"))
        with self.assertRaises(PathMappingError):
            self.mapper.cloud_to_plex("/cloud2/Film")

    def test_parent_traversal_is_rejected(self) -> None:
        with self.assertRaises(PathMappingError):
            self.mapper.cloud_to_plex("/cloud/../private")

    def test_root_must_have_a_direct_mapping(self) -> None:
        with self.assertRaises(PathMappingError):
            PathMapper("/cloud", [("/other", "/plex")])

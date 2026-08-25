from __future__ import annotations

import unittest

import _bootstrap  # noqa: F401
from clouddriveplexsync.models import CloudApi, MountPoint
from clouddriveplexsync.path_mapper import (
    PathMapper,
    PathMappingError,
    cloud_for_path,
    parse_override_lines,
)


class PathMapperTests(unittest.TestCase):
    def setUp(self) -> None:
        self.mapper = PathMapper(
            watch_roots=["/光鸭云盘/Media/Video/已整理"],
            mounts=[
                MountPoint(
                    mount_point="/CloudNAS/Guangya",
                    source_dir="/光鸭云盘",
                    is_mounted=True,
                )
            ],
            plex_overrides=parse_override_lines(
                "/CloudNAS/Guangya => /data/CloudNas/Guangya"
            ),
            moviepilot_overrides=parse_override_lines(
                "/media/CloudNas/Guangya => /CloudNAS/Guangya"
            ),
        )

    def test_cloud_path_maps_without_existing_locally(self) -> None:
        cloud = "/光鸭云盘/Media/Video/已整理/电影/片名/movie.mkv"
        self.assertEqual(
            self.mapper.cloud_to_plex(cloud),
            "/data/CloudNas/Guangya/Media/Video/已整理/电影/片名/movie.mkv",
        )
        self.assertEqual(
            self.mapper.cloud_scan_directory(cloud),
            "/光鸭云盘/Media/Video/已整理/电影/片名",
        )

    def test_direct_cloud_to_plex_mapping_does_not_require_mounts(self) -> None:
        mapper = PathMapper(
            watch_roots=["/光鸭云盘/Media/Video/已整理"],
            mounts=[],
            plex_overrides=[],
            cloud_plex_overrides=parse_override_lines(
                "/光鸭云盘/Media/Video/已整理 => /data/CloudNas/Guangya"
            ),
        )

        self.assertEqual(
            mapper.cloud_to_plex(
                "/光鸭云盘/Media/Video/已整理/动漫/国产动漫/片名"
            ),
            "/data/CloudNas/Guangya/动漫/国产动漫/片名",
        )

    def test_direct_cloud_to_plex_mapping_takes_priority_over_mount_fallback(self) -> None:
        self.mapper.cloud_plex_overrides = parse_override_lines(
            "/光鸭云盘/Media/Video/已整理 => /data/Direct"
        )

        self.assertEqual(
            self.mapper.cloud_to_plex(
                "/光鸭云盘/Media/Video/已整理/电影/片名"
            ),
            "/data/Direct/电影/片名",
        )

    def test_moviepilot_path_reverses_to_cloud(self) -> None:
        self.assertEqual(
            self.mapper.moviepilot_to_cloud(
                "/media/CloudNas/Guangya/Media/Video/已整理/电影/片名/movie.mkv"
            ),
            "/光鸭云盘/Media/Video/已整理/电影/片名/movie.mkv",
        )

    def test_outside_root_is_rejected(self) -> None:
        with self.assertRaises(PathMappingError):
            self.mapper.cloud_to_plex("/光鸭云盘/Private/file.mkv")

    def test_parent_traversal_is_rejected(self) -> None:
        with self.assertRaises(PathMappingError):
            self.mapper.cloud_to_plex("/光鸭云盘/Media/Video/已整理/../Private/file")

    def test_cloud_api_longest_prefix(self) -> None:
        apis = [
            CloudApi(name="A", username="one", path="/光鸭云盘"),
            CloudApi(name="B", username="two", path="/光鸭云盘/Media"),
        ]
        self.assertEqual(
            cloud_for_path("/光鸭云盘/Media/a.mkv", apis).identity,
            "B|two",
        )

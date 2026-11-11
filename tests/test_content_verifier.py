from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import _bootstrap  # noqa: F401
from clouddriveplexsync.activity_log import ActivityJournal
from clouddriveplexsync.content_verifier import ContentVerifier
from clouddriveplexsync.models import MediaHint, PlexTarget
from clouddriveplexsync.plex_bridge import PlexBridge
from test_plex_bridge import FakePlex, FakeSection, FakeService


def media_hint(path: str = "/plex/节目/Season 1/节目.S01E02.2160p.mkv") -> MediaHint:
    return MediaHint(
        record_key="id:42",
        revision="revision-42",
        cloud_directory="/cloud/节目/Season 1",
        expected_path=path,
        title="节目",
        media_type="TV",
        season_num=1,
        episode_num=2,
    )


class FakeItem:
    def __init__(self, paths: list[str], *, exists: bool | None = True) -> None:
        self.parts = [
            SimpleNamespace(file=path, exists=exists, accessible=True) for path in paths
        ]

    def iterParts(self):
        return iter(self.parts)


class FakeShow:
    def __init__(self, item: FakeItem) -> None:
        self.item = item
        self.requested: list[tuple[int, int]] = []

    def episode(self, *, season: int, episode: int) -> FakeItem:
        self.requested.append((season, episode))
        return self.item


class SearchSection(FakeSection):
    TYPE = "show"

    def __init__(self) -> None:
        super().__init__("1", "动漫", ["/plex"])
        self.show = FakeShow(FakeItem([]))
        self.calls: list[tuple[str, object, int]] = []

    def search(self, *, libtype: str, maxresults: int, filters=None, title=None):
        self.calls.append(
            (libtype, filters if filters is not None else title, maxresults)
        )
        return [self.show.item]


class ContentVerifierTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.journal = await ActivityJournal.open(
            Path(self.directory.name) / "activity.sqlite3"
        )
        self.section = SearchSection()
        self.bridge = PlexBridge(FakeService(FakePlex([self.section])), ["1"])
        self.verifier = ContentVerifier(self.bridge, self.journal)
        self.target = PlexTarget("1", "动漫", "/plex", "/plex/节目/Season 1")

    async def asyncTearDown(self) -> None:
        await self.journal.close()
        self.directory.cleanup()

    async def test_only_exact_indexed_file_is_confirmed(self) -> None:
        hint = media_hint()
        await self.journal.add_submitted(self.target, [hint], 100.0)
        self.assertIsNone(await self.journal.next_due(114.0))
        record = await self.journal.next_due(115.0)
        self.section.show.item = FakeItem(["/plex/节目/Season 1/节目.S01E02.1080p.mkv"])
        await self.verifier.check_one(record, 115.0)
        self.assertEqual(await self.journal.indexed(), [])
        self.assertEqual(await self.journal.pending_count(), 1)
        self.assertIsNone(await self.journal.next_due(144.0))

        self.section.show.item = FakeItem([hint.expected_path])
        record = await self.journal.next_due(145.0)
        await self.verifier.check_one(record, 145.0)
        indexed = await self.journal.indexed()
        self.assertEqual(len(indexed), 1)
        self.assertEqual(indexed[0].expected_path, hint.expected_path)
        self.assertEqual(indexed[0].confirmed_at, 145.0)
        self.assertEqual(await self.journal.pending_count(), 0)
        self.assertEqual(
            self.section.calls,
            [
                (
                    "episode",
                    {"show.title": "节目", "season.index": 1, "episode.index": 2},
                    5,
                )
            ]
            * 2,
        )

    async def test_deleted_or_inaccessible_plex_part_is_not_confirmed(self) -> None:
        hint = media_hint()
        await self.journal.add_submitted(self.target, [hint], 100.0)
        self.section.show.item = FakeItem([hint.expected_path], exists=False)
        await self.verifier.check_one(await self.journal.next_due(115.0), 115.0)
        self.assertEqual(await self.journal.indexed(), [])
        self.assertEqual(await self.journal.pending_count(), 1)

    async def test_plex_error_stays_pending_and_uses_backoff(self) -> None:
        hint = media_hint()
        await self.journal.add_submitted(self.target, [hint], 100.0)

        def fail(**_kwargs):
            raise RuntimeError("Plex offline")

        self.section.search = fail
        await self.verifier.check_one(await self.journal.next_due(115.0), 115.0)
        self.assertEqual(await self.journal.indexed(), [])
        self.assertEqual(await self.journal.pending_count(), 1)
        self.assertIsNone(await self.journal.next_due(144.0))
        self.assertIn("RuntimeError", (await self.journal.next_due(145.0)).detail)

    async def test_movie_requires_an_exact_matching_file(self) -> None:
        movie = MediaHint(
            record_key="id:43",
            revision="revision-43",
            cloud_directory="/cloud/电影",
            expected_path="/plex/电影/影片.2026.2160p.mkv",
            title="影片",
            media_type="Movie",
        )
        self.section.TYPE = "movie"
        self.section.search = lambda **_kwargs: [FakeItem([movie.expected_path])]
        await self.journal.add_submitted(
            PlexTarget("1", "电影", "/plex", "/plex/电影"), [movie], 100.0
        )
        await self.verifier.check_one(await self.journal.next_due(115.0), 115.0)
        self.assertEqual((await self.journal.indexed())[0].title, "影片")

    async def test_missing_season_is_explained_without_claiming_success(self) -> None:
        incomplete = MediaHint(
            record_key="id:44",
            revision="revision-44",
            cloud_directory="/cloud/节目",
            expected_path="/plex/节目/episode.mkv",
            title="节目",
            media_type="TV",
        )
        await self.journal.add_submitted(
            PlexTarget("1", "动漫", "/plex", "/plex/节目"), [incomplete], 100.0
        )
        await self.verifier.check_one(await self.journal.next_due(115.0), 115.0)
        self.assertEqual(await self.journal.indexed(), [])
        self.assertIn("缺少季集编号", (await self.journal.pending())[0].detail)

    async def test_pending_verification_survives_journal_restart(self) -> None:
        hint = media_hint()
        await self.journal.add_submitted(self.target, [hint], 100.0)
        await self.verifier.check_one(await self.journal.next_due(115.0), 115.0)
        await self.journal.close()
        self.journal = await ActivityJournal.open(
            Path(self.directory.name) / "activity.sqlite3"
        )
        self.verifier = ContentVerifier(self.bridge, self.journal)
        self.section.show.item = FakeItem([hint.expected_path])
        self.assertIsNone(await self.journal.next_due(144.0))
        await self.verifier.check_one(await self.journal.next_due(145.0), 145.0)
        self.assertEqual(
            (await self.journal.indexed())[0].expected_path, hint.expected_path
        )

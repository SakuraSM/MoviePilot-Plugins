from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import _bootstrap  # noqa: F401
from clouddriveplexsync.activity_log import ActivityJournal
from clouddriveplexsync.content_verifier import ContentVerifier
from clouddriveplexsync.models import MediaHint, PlexTarget
from clouddriveplexsync.plex_bridge import PlexBridge
from test_content_verifier import FakeItem, SearchSection, media_hint
from test_plex_bridge import FakePlex, FakeSection, FakeService


class IdentityEpisode(FakeItem):
    def __init__(
        self,
        paths: list[str],
        *,
        season: int = 1,
        episode: int = 2,
        exists: bool | None = True,
    ) -> None:
        super().__init__(paths, exists=exists)
        self.parentIndex = season
        self.index = episode


class IdentityWork:
    TYPE = "show"

    def __init__(self, item: FakeItem, tmdb_id: str = "258989") -> None:
        self.guid = "plex://show/unique"
        self.guids = [SimpleNamespace(id=f"tmdb://{tmdb_id}")]
        self.item = item
        self.requested: list[tuple[int, int]] = []

    def episode(self, *, season: int, episode: int) -> FakeItem:
        self.requested.append((season, episode))
        return self.item


class IdentitySection(FakeSection):
    TYPE = "show"

    def __init__(self, works: list[IdentityWork]) -> None:
        super().__init__("1", "动漫", ["/plex"])
        self.works = works
        self.identities: list[str] = []
        self.searches: list[tuple[str, str, int]] = []

    def getGuid(self, guid: str) -> IdentityWork:
        self.identities.append(guid)
        return self.works[0]

    def search(self, *, guid: str, libtype: str, maxresults: int) -> list[IdentityWork]:
        self.searches.append((guid, libtype, maxresults))
        return self.works


class IdentityMovie(FakeItem):
    TYPE = "movie"

    def __init__(self, paths: list[str], tmdb_id: str) -> None:
        super().__init__(paths)
        self.guid = "plex://movie/unique"
        self.guids = [SimpleNamespace(id=f"tmdb://{tmdb_id}")]


class IdentityMovieSection(FakeSection):
    TYPE = "movie"

    def __init__(self, movie: IdentityMovie) -> None:
        super().__init__("1", "电影", ["/plex"])
        self.movie = movie

    def getGuid(self, _guid: str) -> IdentityMovie:
        return self.movie

    def search(
        self, *, guid: str, libtype: str, maxresults: int
    ) -> list[IdentityMovie]:
        return [self.movie]


class TmdbVerifierTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.journal = await ActivityJournal.open(
            Path(self.directory.name) / "activity.sqlite3"
        )
        self.section = SearchSection()
        self.verifier = ContentVerifier(
            PlexBridge(FakeService(FakePlex([self.section])), ["1"]), self.journal
        )
        self.target = PlexTarget("1", "动漫", "/plex", "/plex/节目/Season 1")

    async def asyncTearDown(self) -> None:
        await self.journal.close()
        self.directory.cleanup()

    async def test_tmdb_identity_confirms_original_despite_a_different_title(
        self,
    ) -> None:
        hint = replace(media_hint(), tmdb_id="258989", title="拳王阿里传奇")
        episode = IdentityEpisode([hint.expected_path])
        section = IdentitySection([IdentityWork(episode)])
        self.verifier = ContentVerifier(
            PlexBridge(FakeService(FakePlex([section]))), self.journal
        )
        await self.journal.add_submitted(self.target, [hint], 100.0)
        await self.verifier.check_one(await self.journal.next_due(115.0), 115.0)
        self.assertEqual(len(await self.journal.indexed()), 1)
        self.assertEqual(section.identities, ["tmdb://258989"])
        self.assertEqual(episode.parentIndex, 1)

    async def test_tmdb_identity_marks_another_version_without_confirming_original(
        self,
    ) -> None:
        hint = replace(media_hint(), tmdb_id="258989")
        other = "/plex/节目/Season 1/节目.S01E02.1080p.mkv"
        section = IdentitySection([IdentityWork(IdentityEpisode([other]))])
        self.verifier = ContentVerifier(
            PlexBridge(FakeService(FakePlex([section]))), self.journal
        )
        await self.journal.add_submitted(self.target, [hint], 100.0)
        await self.verifier.check_one(await self.journal.next_due(115.0), 115.0)
        self.assertEqual(await self.journal.pending_count(), 0)
        self.assertEqual(await self.journal.indexed(), [])
        self.assertEqual((await self.journal.alternatives())[0].matched_path, other)

    async def test_wrong_tmdb_identity_never_confirms_a_matching_path(self) -> None:
        hint = replace(media_hint(), tmdb_id="258989")
        section = IdentitySection(
            [IdentityWork(IdentityEpisode([hint.expected_path]), tmdb_id="123")]
        )
        self.verifier = ContentVerifier(
            PlexBridge(FakeService(FakePlex([section]))), self.journal
        )
        await self.journal.add_submitted(self.target, [hint], 100.0)
        await self.verifier.check_one(await self.journal.next_due(115.0), 115.0)
        self.assertEqual(await self.journal.indexed(), [])
        self.assertEqual(await self.journal.pending_count(), 1)
        self.assertIn("TMDB ID 不一致", (await self.journal.pending())[0].detail)

    async def test_wrong_season_or_episode_never_confirms_a_matching_path(self) -> None:
        hint = replace(media_hint(), tmdb_id="258989")
        section = IdentitySection(
            [IdentityWork(IdentityEpisode([hint.expected_path], episode=3))]
        )
        self.verifier = ContentVerifier(
            PlexBridge(FakeService(FakePlex([section]))), self.journal
        )
        await self.journal.add_submitted(self.target, [hint], 100.0)
        await self.verifier.check_one(await self.journal.next_due(115.0), 115.0)
        self.assertEqual(await self.journal.pending_count(), 1)
        self.assertIn("季集编号不一致", (await self.journal.pending())[0].detail)

    async def test_other_directory_does_not_resolve_original(self) -> None:
        hint = replace(media_hint(), tmdb_id="258989")
        section = IdentitySection(
            [IdentityWork(IdentityEpisode(["/plex/别处/E02.mkv"]))]
        )
        self.verifier = ContentVerifier(
            PlexBridge(FakeService(FakePlex([section]))), self.journal
        )
        await self.journal.add_submitted(self.target, [hint], 100.0)
        await self.verifier.check_one(await self.journal.next_due(115.0), 115.0)
        self.assertEqual(await self.journal.pending_count(), 1)
        self.assertEqual(await self.journal.alternatives(), [])

    async def test_exact_path_wins_over_alternative_across_duplicate_works(
        self,
    ) -> None:
        hint = replace(media_hint(), tmdb_id="258989")
        other = "/plex/节目/Season 1/节目.S01E02.1080p.mkv"
        section = IdentitySection(
            [
                IdentityWork(IdentityEpisode([other])),
                IdentityWork(IdentityEpisode([hint.expected_path])),
            ]
        )
        self.verifier = ContentVerifier(
            PlexBridge(FakeService(FakePlex([section]))), self.journal
        )
        await self.journal.add_submitted(self.target, [hint], 100.0)
        await self.verifier.check_one(await self.journal.next_due(115.0), 115.0)
        self.assertEqual(len(await self.journal.indexed()), 1)
        self.assertEqual(await self.journal.alternatives(), [])

    async def test_inaccessible_alternative_stays_pending(self) -> None:
        hint = replace(media_hint(), tmdb_id="258989")
        section = IdentitySection(
            [
                IdentityWork(
                    IdentityEpisode(["/plex/节目/Season 1/E02.mkv"], exists=False)
                )
            ]
        )
        self.verifier = ContentVerifier(
            PlexBridge(FakeService(FakePlex([section]))), self.journal
        )
        await self.journal.add_submitted(self.target, [hint], 100.0)
        await self.verifier.check_one(await self.journal.next_due(115.0), 115.0)
        self.assertEqual(await self.journal.pending_count(), 1)

    async def test_movie_other_version_is_resolved_by_tmdb(self) -> None:
        hint = MediaHint(
            record_key="id:43",
            revision="rev",
            cloud_directory="/cloud/电影",
            expected_path="/plex/电影/old.mkv",
            title="影片",
            media_type="Movie",
            tmdb_id="1234",
        )
        section = IdentityMovieSection(IdentityMovie(["/plex/电影/new.mkv"], "1234"))
        self.verifier = ContentVerifier(
            PlexBridge(FakeService(FakePlex([section]))), self.journal
        )
        await self.journal.add_submitted(self.target, [hint], 100.0)
        await self.verifier.check_one(await self.journal.next_due(115.0), 115.0)
        self.assertEqual(
            (await self.journal.alternatives())[0].matched_path, "/plex/电影/new.mkv"
        )
        self.assertEqual(await self.journal.indexed(), [])

    async def test_unsupported_tmdb_does_not_fall_back_to_title(self) -> None:
        hint = replace(media_hint(), tmdb_id="258989")
        await self.journal.add_submitted(self.target, [hint], 100.0)
        await self.verifier.check_one(await self.journal.next_due(115.0), 115.0)
        self.assertEqual(await self.journal.pending_count(), 1)
        self.assertIn("不支持按 TMDB ID 查询", (await self.journal.pending())[0].detail)
        self.assertEqual(self.section.calls, [])

    async def test_resubmission_enriches_the_same_pending_record_without_replacing_identity(
        self,
    ) -> None:
        hint = media_hint()
        await self.journal.add_submitted(self.target, [hint], 100.0)
        await self.journal.add_submitted(
            self.target, [replace(hint, tmdb_id="258989")], 101.0
        )
        await self.journal.add_submitted(
            self.target, [replace(hint, tmdb_id="123")], 102.0
        )
        self.assertEqual(await self.journal.pending_count(), 1)
        self.assertEqual((await self.journal.next_due(115.0)).tmdb_id, "258989")

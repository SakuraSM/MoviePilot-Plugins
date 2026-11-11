"""Confirm submitted TTD files against Plex's indexed media parts."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable

from .activity_log import ActivityJournal
from .plex_bridge import PlexBridge, normalize_plex_path
from .verification_store import MediaVerification

SEARCH_CANDIDATE_LIMIT = 5
VERIFICATION_INTERVAL_SECONDS = 1.0
IDLE_POLL_SECONDS = 5.0
STORE_RETRY_SECONDS = 60.0
TV_TYPES = frozenset({"tv", "show", "series", "tvshow"})
MOVIE_TYPES = frozenset({"movie", "film"})


class VerificationUnavailable(ValueError):
    """The TTD record or library configuration cannot identify a Plex item."""


class ContentVerifier:
    def __init__(
        self,
        plex: PlexBridge,
        journal: ActivityJournal,
        *,
        on_indexed: Callable[[MediaVerification], None] | None = None,
        on_error: Callable[[str], None] | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.plex = plex
        self.journal = journal
        self.on_indexed = on_indexed
        self.on_error = on_error
        self.clock = clock

    def _matches_path(self, item: object, expected_path: str, platform: str) -> bool:
        parts_method = getattr(item, "iterParts", None)
        if not callable(parts_method):
            return False
        expected = normalize_plex_path(expected_path, platform)
        for part in parts_method():
            path = getattr(part, "file", None)
            if not isinstance(path, str) or not path:
                continue
            if (
                getattr(part, "exists", None) is False
                or getattr(part, "accessible", None) is False
            ):
                continue
            actual = normalize_plex_path(path, platform)
            if platform.casefold() == "windows":
                actual, expected = actual.casefold(), expected.casefold()
            if actual == expected:
                return True
        return False

    def _is_indexed(self, entry: MediaVerification) -> bool:
        if not entry.title:
            raise VerificationUnavailable("整理记录缺少媒体名称")
        target = self.plex.find_target(entry.expected_path)
        if target.section_id != entry.section_id:
            raise VerificationUnavailable("媒体库映射已变化")
        section = next(
            (
                item
                for item in self.plex.sections()
                if str(getattr(item, "key", "")) == entry.section_id
            ),
            None,
        )
        if section is None:
            raise VerificationUnavailable("媒体库不存在")
        platform = str(getattr(self.plex.plex, "platform", "") or "")
        media_type = entry.media_type.casefold()
        section_type = str(getattr(section, "TYPE", "") or "").casefold()
        if media_type in TV_TYPES or (not media_type and section_type == "show"):
            if entry.season_num is None or entry.episode_num is None:
                raise VerificationUnavailable("整理记录缺少季集编号")
            episodes = section.search(
                libtype="episode",
                filters={
                    "show.title": entry.title,
                    "season.index": entry.season_num,
                    "episode.index": entry.episode_num,
                },
                maxresults=SEARCH_CANDIDATE_LIMIT,
            )
            return any(
                self._matches_path(episode, entry.expected_path, platform)
                for episode in episodes
            )
        if media_type in MOVIE_TYPES or (not media_type and section_type == "movie"):
            movies = section.search(
                title=entry.title, libtype="movie", maxresults=SEARCH_CANDIDATE_LIMIT
            )
            return any(
                self._matches_path(movie, entry.expected_path, platform)
                for movie in movies
            )
        raise VerificationUnavailable("媒体类型不支持自动核验")

    async def check_one(self, entry: MediaVerification, now: float) -> None:
        try:
            indexed = await asyncio.to_thread(self._is_indexed, entry)
        except VerificationUnavailable as error:
            await self.journal.defer(entry, now, str(error))
            return
        except Exception as error:  # noqa: BLE001 - Plex lookup failures remain retryable.
            await self.journal.defer(
                entry, now, f"Plex 查询失败（{type(error).__name__}）"
            )
            return
        if indexed:
            await self.journal.mark_indexed(entry.id, now)
            if self.on_indexed:
                self.on_indexed(entry)
        else:
            await self.journal.defer(entry, now, "尚未在 Plex 中找到对应文件")

    async def run(self, stop: asyncio.Event) -> None:
        last_error = ""
        while not stop.is_set():
            try:
                entry = await self.journal.next_due(self.clock())
                if entry:
                    await self.check_one(entry, self.clock())
                delay = VERIFICATION_INTERVAL_SECONDS if entry else IDLE_POLL_SECONDS
                last_error = ""
            except Exception as error:  # noqa: BLE001 - journal failures must not stop the scanner.
                message = f"Plex 入库核验失败：{type(error).__name__}"
                if self.on_error and message != last_error:
                    self.on_error(message)
                last_error = message
                delay = STORE_RETRY_SECONDS
            try:
                await asyncio.wait_for(stop.wait(), timeout=delay)
            except TimeoutError:
                pass

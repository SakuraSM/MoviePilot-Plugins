"""Confirm submitted TTD files against Plex's indexed media parts."""

from __future__ import annotations

import asyncio
import posixpath
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum

from .activity_log import ActivityJournal
from .plex_bridge import PlexBridge, normalize_plex_path
from .plex_identity import PlexIdentityError, PlexIdentityResolver
from .verification_store import MediaVerification

SEARCH_CANDIDATE_LIMIT = 5
VERIFICATION_INTERVAL_SECONDS = 1.0
IDLE_POLL_SECONDS = 5.0
STORE_RETRY_SECONDS = 60.0
TV_TYPES = frozenset({"tv", "show", "series", "tvshow"})
MOVIE_TYPES = frozenset({"movie", "film"})


class VerificationUnavailable(ValueError):
    """The TTD record or library configuration cannot identify a Plex item."""


class VerificationKind(str, Enum):
    EXACT = "exact"
    ALTERNATIVE = "alternative"
    PENDING = "pending"


@dataclass(frozen=True)
class VerificationResult:
    kind: VerificationKind
    matched_path: str = ""


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
        self.identities = PlexIdentityResolver()

    def _available_paths(self, item: object, platform: str) -> list[str]:
        parts_method = getattr(item, "iterParts", None)
        if not callable(parts_method):
            return []
        paths = []
        for part in parts_method():
            path = getattr(part, "file", None)
            if not isinstance(path, str) or not path:
                continue
            if (
                getattr(part, "exists", None) is False
                or getattr(part, "accessible", None) is False
            ):
                continue
            paths.append(normalize_plex_path(path, platform))
        return paths

    def _compare_paths(
        self,
        items: list[object],
        entry: MediaVerification,
        platform: str,
        *,
        allow_alternative: bool,
    ) -> VerificationResult:
        expected = normalize_plex_path(entry.expected_path, platform)
        key = str.casefold if platform.casefold() == "windows" else str
        alternative = ""
        for item in items:
            for path in self._available_paths(item, platform):
                if key(path) == key(expected):
                    return VerificationResult(VerificationKind.EXACT, path)
                if allow_alternative and key(posixpath.dirname(path)) == key(
                    posixpath.dirname(expected)
                ):
                    alternative = alternative or path
        if alternative:
            return VerificationResult(VerificationKind.ALTERNATIVE, alternative)
        return VerificationResult(VerificationKind.PENDING)

    def _legacy_items(
        self, section: object, entry: MediaVerification, is_tv: bool
    ) -> list[object]:
        if not entry.title:
            raise VerificationUnavailable("整理记录缺少媒体名称")
        search = getattr(section, "search", None)
        if not callable(search):
            raise VerificationUnavailable("Plex 媒体库不支持搜索")
        if is_tv:
            return list(
                search(
                    libtype="episode",
                    filters={
                        "show.title": entry.title,
                        "season.index": entry.season_num,
                        "episode.index": entry.episode_num,
                    },
                    maxresults=SEARCH_CANDIDATE_LIMIT,
                )
            )
        return list(
            search(
                title=entry.title, libtype="movie", maxresults=SEARCH_CANDIDATE_LIMIT
            )
        )

    def _identity_items(
        self, section: object, entry: MediaVerification, is_tv: bool
    ) -> list[object]:
        try:
            works = self.identities.resolve(
                section, entry.tmdb_id, "show" if is_tv else "movie"
            )
        except PlexIdentityError as error:
            raise VerificationUnavailable(str(error)) from error
        if not is_tv:
            return list(works)
        episodes = []
        for work in works:
            episode_method = getattr(work, "episode", None)
            if not callable(episode_method):
                raise VerificationUnavailable("Plex 剧集不支持季集查询")
            try:
                episode = episode_method(
                    season=entry.season_num, episode=entry.episode_num
                )
            except Exception as error:
                if type(error).__name__ == "NotFound":
                    continue
                raise
            if (
                getattr(episode, "parentIndex", None),
                getattr(episode, "index", None),
            ) != (entry.season_num, entry.episode_num):
                raise VerificationUnavailable("Plex 返回的季集编号不一致")
            episodes.append(episode)
        return episodes

    def _verify(self, entry: MediaVerification) -> VerificationResult:
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
        is_tv = media_type in TV_TYPES or (not media_type and section_type == "show")
        is_movie = media_type in MOVIE_TYPES or (
            not media_type and section_type == "movie"
        )
        if not is_tv and not is_movie:
            raise VerificationUnavailable("媒体类型不支持自动核验")
        if is_tv and (entry.season_num is None or entry.episode_num is None):
            raise VerificationUnavailable("整理记录缺少季集编号")
        items = (
            self._identity_items(section, entry, is_tv)
            if entry.tmdb_id
            else self._legacy_items(section, entry, is_tv)
        )
        return self._compare_paths(
            items, entry, platform, allow_alternative=bool(entry.tmdb_id)
        )

    def _is_indexed(self, entry: MediaVerification) -> bool:
        return self._verify(entry).kind == VerificationKind.EXACT

    async def check_one(self, entry: MediaVerification, now: float) -> None:
        try:
            result = await asyncio.to_thread(self._verify, entry)
        except VerificationUnavailable as error:
            await self.journal.defer(entry, now, str(error))
            return
        except Exception as error:  # noqa: BLE001 - Plex lookup failures remain retryable.
            await self.journal.defer(
                entry, now, f"Plex 查询失败（{type(error).__name__}）"
            )
            return
        if result.kind == VerificationKind.EXACT:
            await self.journal.mark_indexed(entry.id, now)
            if self.on_indexed:
                self.on_indexed(entry)
        elif result.kind == VerificationKind.ALTERNATIVE:
            await self.journal.mark_alternative(entry.id, now, result.matched_path)
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

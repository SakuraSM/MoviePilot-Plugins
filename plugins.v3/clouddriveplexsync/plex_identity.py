"""Resolve TMDB identities to entries in one Plex library section."""

from __future__ import annotations

import time
from collections.abc import Callable

IDENTITY_CACHE_SECONDS = 300.0
MAX_DUPLICATE_IDENTITIES = 50


class PlexIdentityError(ValueError):
    """Plex cannot establish the requested external identity safely."""


def _has_tmdb_identity(item: object, guid: str) -> bool:
    identities = getattr(item, "guids", None) or ()
    return any(str(getattr(identity, "id", "")) == guid for identity in identities)


class PlexIdentityResolver:
    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        self.clock = clock
        self._cache: dict[tuple[str, str, str], tuple[float, tuple[object, ...]]] = {}

    def resolve(
        self, section: object, tmdb_id: str, media_type: str
    ) -> tuple[object, ...]:
        if (
            not tmdb_id.isascii()
            or not tmdb_id.isdigit()
            or tmdb_id.startswith("0")
            or len(tmdb_id) > 12
        ):
            raise PlexIdentityError("TMDB ID 无效")
        cache_key = (str(getattr(section, "key", "")), tmdb_id, media_type)
        cached = self._cache.get(cache_key)
        if cached and self.clock() - cached[0] < IDENTITY_CACHE_SECONDS:
            return cached[1]
        get_guid = getattr(section, "getGuid", None)
        search = getattr(section, "search", None)
        if not callable(get_guid) or not callable(search):
            raise PlexIdentityError("Plex 媒体库不支持按 TMDB ID 查询")
        external_guid = f"tmdb://{tmdb_id}"
        try:
            first = get_guid(external_guid)
        except Exception as error:
            if type(error).__name__ == "NotFound":
                return ()
            raise
        internal_guid = str(getattr(first, "guid", "") or "")
        if not internal_guid.startswith("plex://") or not _has_tmdb_identity(
            first, external_guid
        ):
            raise PlexIdentityError("Plex 返回的作品与 TMDB ID 不一致")
        matches = search(
            guid=internal_guid,
            libtype=media_type,
            maxresults=MAX_DUPLICATE_IDENTITIES + 1,
        )
        if len(matches) > MAX_DUPLICATE_IDENTITIES:
            raise PlexIdentityError("Plex 同一作品的条目过多，无法安全确认")
        verified = tuple(
            item
            for item in matches
            if str(getattr(item, "TYPE", "")) == media_type
            and _has_tmdb_identity(item, external_guid)
        )
        if not verified:
            raise PlexIdentityError("Plex 未返回可验证的 TMDB 条目")
        self._cache[cache_key] = (self.clock(), verified)
        return verified

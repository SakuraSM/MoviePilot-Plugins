"""Plex scan requests and synchronization counters."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Literal

HISTORY_SOURCE = "ttd_history"
ScanPathKind = Literal["cloud", "plex"]


@dataclass(frozen=True)
class PlexTarget:
    """A validated Plex section and path to scan."""

    section_id: str
    section_title: str
    location: str
    path: str


@dataclass(frozen=True)
class MediaHint:
    """TTD media identity and the exact file path expected inside Plex."""

    record_key: str
    revision: str
    cloud_directory: str
    expected_path: str
    title: str
    media_type: str
    season_num: int | None = None
    episode_num: int | None = None
    tmdb_id: str = ""

    @classmethod
    def from_dict(cls, value: dict[str, object]) -> MediaHint:
        strings = {}
        for key in (
            "record_key",
            "revision",
            "cloud_directory",
            "expected_path",
            "title",
            "media_type",
        ):
            field_value = value.get(key)
            if not isinstance(field_value, str):
                raise TypeError(f"扫描队列媒体字段无效：{key}")
            strings[key] = field_value
        if not all(
            strings[key]
            for key in ("record_key", "revision", "cloud_directory", "expected_path")
        ):
            raise ValueError("扫描队列媒体标识或路径为空")
        season = value.get("season_num")
        episode = value.get("episode_num")
        if (season is not None and not isinstance(season, int)) or (
            episode is not None and not isinstance(episode, int)
        ):
            raise ValueError("扫描队列季集编号无效")
        tmdb_id = value.get("tmdb_id", "")
        if not isinstance(tmdb_id, str) or (
            tmdb_id
            and (
                not tmdb_id.isascii()
                or not tmdb_id.isdigit()
                or tmdb_id[0] == "0"
                or len(tmdb_id) > 12
            )
        ):
            raise ValueError("扫描队列 TMDB ID 无效")
        return cls(
            record_key=strings["record_key"],
            revision=strings["revision"],
            cloud_directory=strings["cloud_directory"],
            expected_path=strings["expected_path"],
            title=strings["title"],
            media_type=strings["media_type"],
            season_num=season,
            episode_num=episode,
            tmdb_id=tmdb_id,
        )


@dataclass
class PendingScan:
    """A coalesced directory scan request."""

    cloud_path: str
    plex_path: str
    section_id: str
    section_title: str
    first_seen: float
    last_seen: float
    sources: list[str] = field(default_factory=list)
    state: Literal["queued", "inflight"] = "queued"
    path_kind: ScanPathKind = "cloud"
    ready_at: float | None = None
    media: list[MediaHint] = field(default_factory=list)

    @classmethod
    def from_dict(cls, value: dict[str, object]) -> PendingScan:
        raw_path_kind = value.get("path_kind", "cloud")
        if raw_path_kind == "cloud":
            path_kind: ScanPathKind = "cloud"
        elif raw_path_kind == "plex":
            path_kind = "plex"
        else:
            raise ValueError("扫描队列路径类型无效")
        paths = {}
        for key in ("plex_path", "section_id", "section_title"):
            item = value.get(key)
            if not isinstance(item, str) or not item:
                raise ValueError(f"扫描队列字段无效：{key}")
            paths[key] = item
        cloud_path = value.get("cloud_path")
        if not isinstance(cloud_path, str) or (path_kind == "cloud" and not cloud_path):
            raise ValueError("扫描队列字段无效：cloud_path")
        first_seen, last_seen = value.get("first_seen"), value.get("last_seen")
        if not isinstance(first_seen, (int, float)) or not isinstance(
            last_seen, (int, float)
        ):
            raise TypeError("扫描队列时间字段无效")
        sources = value.get("sources") or []
        if not isinstance(sources, list) or not all(
            isinstance(item, str) for item in sources
        ):
            raise TypeError("扫描队列来源字段无效")
        ready_at = value.get("ready_at")
        if ready_at is not None and not isinstance(ready_at, (int, float)):
            raise TypeError("扫描队列就绪时间无效")
        media = value.get("media", [])
        if not isinstance(media, list) or not all(
            isinstance(item, dict) for item in media
        ):
            raise TypeError("扫描队列媒体记录无效")
        return cls(
            cloud_path=cloud_path,
            plex_path=paths["plex_path"],
            section_id=paths["section_id"],
            section_title=paths["section_title"],
            first_seen=first_seen,
            last_seen=last_seen,
            sources=[str(item) for item in sources],
            path_kind=path_kind,
            ready_at=ready_at,
            media=[MediaHint.from_dict(item) for item in media],
        )


@dataclass
class PluginStats:
    """Small runtime counter set exposed on the status page."""

    plex_scans: int = 0
    ttd_poll_requests: int = 0
    ttd_records: int = 0
    ttd_skipped_records: int = 0
    ttd_auth_failures: int = 0
    ttd_errors: int = 0

    def to_dict(self) -> dict[str, int]:
        return asdict(self)

"""Plex scan requests and synchronization counters."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Literal


@dataclass(frozen=True)
class PlexTarget:
    """A validated Plex section and path to scan."""

    section_id: str
    section_title: str
    location: str
    path: str


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

    @classmethod
    def from_dict(cls, value: dict[str, object]) -> PendingScan:
        paths = {}
        for key in ("cloud_path", "plex_path", "section_id", "section_title"):
            item = value.get(key)
            if not isinstance(item, str) or not item:
                raise ValueError(f"扫描队列字段无效：{key}")
            paths[key] = item
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
        return cls(
            cloud_path=paths["cloud_path"],
            plex_path=paths["plex_path"],
            section_id=paths["section_id"],
            section_title=paths["section_title"],
            first_seen=first_seen,
            last_seen=last_seen,
            sources=[str(item) for item in sources],
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

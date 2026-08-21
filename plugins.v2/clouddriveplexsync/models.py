"""Shared data models for the CloudDrive/Plex synchronization plugin."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import IntEnum
from typing import Any, Dict, List, Optional


class ChangeType(IntEnum):
    """CloudDrive file-system change types."""

    CREATE = 0
    DELETE = 1
    RENAME = 2


@dataclass(frozen=True)
class FileSystemChange:
    """One file-system change received from CloudDrive."""

    change_type: ChangeType
    is_directory: bool
    path: str
    new_path: Optional[str] = None


@dataclass(frozen=True)
class MountPoint:
    """CloudDrive cloud-path to mounted-path relationship."""

    mount_point: str
    source_dir: str
    local_mount: bool = False
    read_only: bool = False
    is_mounted: bool = False
    name: str = ""


@dataclass(frozen=True)
class CloudApi:
    """CloudDrive cloud provider/account descriptor."""

    name: str
    username: str
    nickname: str = ""
    path: str = ""
    event_listener_running: bool = False
    read_only: bool = False

    @property
    def identity(self) -> str:
        return f"{self.name}|{self.username}"


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
    cloud_identity: Optional[str]
    first_seen: float
    last_seen: float
    sources: List[str] = field(default_factory=list)


@dataclass
class BufferLease:
    """A recoverable lease for a temporary CloudDrive buffer value."""

    cloud_name: str
    username: str
    original_mb: int
    applied_mb: int
    started_at: float
    last_event_at: float
    lease_id: str
    section_ids: List[str] = field(default_factory=list)

    @property
    def cloud_identity(self) -> str:
        return f"{self.cloud_name}|{self.username}"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: Dict[str, Any]) -> "BufferLease":
        return cls(
            cloud_name=str(value["cloud_name"]),
            username=str(value.get("username") or ""),
            original_mb=int(value["original_mb"]),
            applied_mb=int(value["applied_mb"]),
            started_at=float(value["started_at"]),
            last_event_at=float(value["last_event_at"]),
            lease_id=str(value["lease_id"]),
            section_ids=[str(item) for item in value.get("section_ids") or []],
        )


@dataclass
class PluginStats:
    """Small runtime counter set exposed on the status page."""

    cd2_unary_requests: int = 0
    push_events: int = 0
    plex_scans: int = 0
    reconnects: int = 0
    unmapped_events: int = 0
    buffer_changes: int = 0

    def to_dict(self) -> Dict[str, int]:
        return asdict(self)

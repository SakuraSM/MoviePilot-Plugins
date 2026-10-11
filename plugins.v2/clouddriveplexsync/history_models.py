"""TgToDrive history records, cursor, and polling outcomes."""

from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any


class TTDClientError(RuntimeError):
    """Base class for safe-to-display TgToDrive client failures."""


class TTDAuthenticationError(TTDClientError):
    """The configured TgToDrive credential cannot read organize history."""


class TTDProtocolError(TTDClientError):
    """TgToDrive returned an unsupported response."""


class TTDHistoryGapError(TTDClientError):
    """The saved cursor was not found within the configured page budget."""


class TTDSubmitState(str, Enum):
    """Whether one TTD directory was accepted, permanently skipped, or should retry."""

    QUEUED = "queued"
    SKIPPED = "skipped"
    RETRY = "retry"


@dataclass(frozen=True)
class TTDSubmitResult:
    """Typed submission outcome used to keep permanent skips out of the retry loop."""

    state: TTDSubmitState
    reason: str = ""

    @classmethod
    def queued(cls, reason: str = "") -> TTDSubmitResult:
        return cls(TTDSubmitState.QUEUED, reason)

    @classmethod
    def skipped(cls, reason: str) -> TTDSubmitResult:
        return cls(TTDSubmitState.SKIPPED, reason)

    @classmethod
    def retry(cls, reason: str) -> TTDSubmitResult:
        return cls(TTDSubmitState.RETRY, reason)


@dataclass(frozen=True)
class TTDHistoryRecord:
    """Normalized subset of one organize-history entry."""

    key: str
    target_path: str
    status: str = "success"
    completed_at: str = ""
    source: str = ""
    record_id: str = ""

    @property
    def revision(self) -> str:
        value = f"{self.key}\0{self.target_path}\0{self.status}\0{self.completed_at}\0{self.source}"
        return hashlib.sha256(value.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class TTDHistoryPage:
    """Parsed records plus the server-side item count used for pagination."""

    records: list[TTDHistoryRecord]
    item_count: int
    has_next: bool | None = None


@dataclass
class TTDCursor:
    """Small persistent cursor resilient to non-monotonic record IDs."""

    initialized: bool = False
    scope: str = ""
    seen_keys: list[str] = field(default_factory=list)
    audit_next_page: int = 1
    audit_completed_at: float = 0
    history_gap: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: object) -> TTDCursor:
        if not isinstance(value, dict):
            return cls()
        keys = value.get("seen_keys") or []
        if not isinstance(keys, (list, tuple)):
            keys = []
        return cls(
            initialized=bool(value.get("initialized")),
            scope=str(value.get("scope") or ""),
            seen_keys=[str(item) for item in keys if str(item)],
            audit_next_page=max(0, int(value.get("audit_next_page", 1))),
            audit_completed_at=float(value.get("audit_completed_at") or 0),
            history_gap=bool(value.get("history_gap")),
        )


@dataclass
class TTDPollStatus:
    """Runtime state exposed without credentials."""

    enabled: bool = True
    connected: bool = False
    authenticated: bool = False
    cursor_initialized: bool = False
    seen_records: int = 0
    last_poll_at: float = 0
    last_success_at: float = 0
    last_record_at: float = 0
    skipped_records: int = 0
    last_skip: str = ""
    next_poll_at: float = 0
    last_error: str = ""
    audit_next_page: int = 1
    audit_completed_at: float = 0
    history_gap: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

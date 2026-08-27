"""Bounded, rate-limited logging for noisy background events."""

from __future__ import annotations

import threading
import time
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Callable, Dict, Optional


MAX_TRACKED_CATEGORIES = 200


class LogMode(str, Enum):
    """Amount of routine detail written to the MoviePilot plugin log."""

    QUIET = "quiet"
    NORMAL = "normal"
    VERBOSE = "verbose"


@dataclass(frozen=True)
class LogSettings:
    """User-configurable controls for repeated background messages."""

    mode: str = LogMode.NORMAL.value
    dedup_seconds: int = 300
    recent_limit: int = 50

    def normalized(self) -> "LogSettings":
        mode = str(self.mode or LogMode.NORMAL.value).strip().lower()
        if mode not in {item.value for item in LogMode}:
            raise ValueError("日志详细程度只能是 quiet、normal 或 verbose")
        return LogSettings(
            mode=mode,
            dedup_seconds=max(0, min(3600, int(self.dedup_seconds))),
            recent_limit=max(10, min(200, int(self.recent_limit))),
        )


@dataclass
class RepeatedLogState:
    """Mutable state for one logical log category."""

    last_emitted_at: float
    last_seen_at: float
    last_message: str
    suppressed: int = 0


@dataclass(frozen=True)
class LogDecision:
    """Whether a message should be emitted and its optional aggregate suffix."""

    should_emit: bool
    message: str
    suppressed_count: int = 0


class RepeatedLogLimiter:
    """Emit the first event, then aggregate repeats inside a fixed time window."""

    def __init__(
        self,
        settings: Optional[LogSettings] = None,
        *,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.settings = (settings or LogSettings()).normalized()
        self.clock = clock
        self._states: Dict[str, RepeatedLogState] = {}
        self._lock = threading.RLock()
        self.suppressed_total = 0
        self.emitted_total = 0

    def record(self, category: str, message: str) -> LogDecision:
        """Return an emission decision for one logical event category."""

        with self._lock:
            now = self.clock()
            key = str(category or message)
            text = str(message)
            if self.settings.mode == LogMode.VERBOSE.value or self.settings.dedup_seconds == 0:
                self.emitted_total += 1
                return LogDecision(True, text)

            state = self._states.get(key)
            if state is None:
                if len(self._states) >= MAX_TRACKED_CATEGORIES:
                    oldest_key = min(
                        self._states,
                        key=lambda item: self._states[item].last_seen_at,
                    )
                    self._states.pop(oldest_key, None)
                self._states[key] = RepeatedLogState(now, now, text)
                self.emitted_total += 1
                return LogDecision(True, text)

            state.last_seen_at = now
            state.last_message = text
            if now - state.last_emitted_at < self.settings.dedup_seconds:
                state.suppressed += 1
                self.suppressed_total += 1
                return LogDecision(False, text, state.suppressed)

            suppressed = state.suppressed
            state.last_emitted_at = now
            state.suppressed = 0
            self.emitted_total += 1
            if suppressed:
                text = f"{text}（期间已合并 {suppressed} 条同类日志）"
            return LogDecision(True, text, suppressed)

    def should_emit_routine(self) -> bool:
        """Quiet mode keeps routine skip details in status only."""

        return self.settings.mode != LogMode.QUIET.value

    def snapshot(self) -> Dict[str, object]:
        """Expose aggregate counters without retaining an unbounded event history."""

        with self._lock:
            active = {
                key: asdict(state)
                for key, state in self._states.items()
                if state.suppressed > 0
            }
            return {
                "mode": self.settings.mode,
                "dedup_seconds": self.settings.dedup_seconds,
                "recent_limit": self.settings.recent_limit,
                "emitted_total": self.emitted_total,
                "suppressed_total": self.suppressed_total,
                "active_categories": active,
            }

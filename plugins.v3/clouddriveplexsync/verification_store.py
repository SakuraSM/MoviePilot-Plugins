"""Persist and query per-file Plex confirmation work."""

from __future__ import annotations

import asyncio
import sqlite3
from contextlib import AbstractContextManager
from dataclasses import asdict, dataclass

from .models import MediaHint, PlexTarget

FIRST_VERIFICATION_DELAY = 15.0
VERIFICATION_RETRY_SECONDS = (30, 60, 180, 600, 1800, 3600)
MAX_INDEXED_RECORDS = 10000
DEFAULT_DISPLAY_LIMIT = 2000
ALTERNATIVE_RESOLUTION = "alternative"
VERIFICATION_COLUMNS = (
    "id, section_id, section_title, expected_path, title, media_type, "
    "season_num, episode_num, submitted_at, confirmed_at, attempts, detail"
)
MIGRATION_COLUMNS = {
    "tmdb_id": "TEXT NOT NULL DEFAULT ''",
    "resolution": "TEXT NOT NULL DEFAULT ''",
    "matched_path": "TEXT NOT NULL DEFAULT ''",
    "resolved_at": "REAL",
}


@dataclass(frozen=True)
class MediaVerification:
    id: int
    section_id: str
    section_title: str
    expected_path: str
    title: str
    media_type: str
    season_num: int | None
    episode_num: int | None
    submitted_at: float
    confirmed_at: float | None
    attempts: int
    detail: str
    tmdb_id: str = ""
    resolution: str = ""
    matched_path: str = ""
    resolved_at: float | None = None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def create_verification_schema(connection: sqlite3.Connection) -> None:
    connection.execute(
        "CREATE TABLE IF NOT EXISTS media_verification ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, record_key TEXT NOT NULL, "
        "revision TEXT NOT NULL, section_id TEXT NOT NULL, section_title TEXT NOT NULL, "
        "expected_path TEXT NOT NULL, title TEXT NOT NULL, media_type TEXT NOT NULL, "
        "season_num INTEGER, episode_num INTEGER, submitted_at REAL NOT NULL, "
        "confirmed_at REAL, next_check_at REAL NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, "
        "detail TEXT NOT NULL DEFAULT '', "
        "UNIQUE(record_key, revision, section_id, expected_path))"
    )
    existing = {
        row[1] for row in connection.execute("PRAGMA table_info(media_verification)")
    }
    for name, declaration in MIGRATION_COLUMNS.items():
        if name not in existing:
            connection.execute(
                f"ALTER TABLE media_verification ADD COLUMN {name} {declaration}"
            )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS media_verification_due "
        "ON media_verification(confirmed_at, next_check_at)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS media_verification_submitted "
        "ON media_verification(confirmed_at, submitted_at DESC)"
    )


class VerificationStoreMixin:
    _connection: sqlite3.Connection
    _lock: AbstractContextManager[object]

    async def add_submitted(
        self, target: PlexTarget, media: list[MediaHint], now: float
    ) -> None:
        await asyncio.to_thread(self._add_submitted, target, media, now)

    def _add_submitted(
        self, target: PlexTarget, media: list[MediaHint], now: float
    ) -> None:
        with self._lock, self._connection:
            self._connection.executemany(
                "INSERT INTO media_verification("
                "record_key, revision, section_id, section_title, expected_path, title, "
                "media_type, season_num, episode_num, submitted_at, next_check_at, tmdb_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(record_key, revision, section_id, expected_path) DO UPDATE SET tmdb_id=excluded.tmdb_id "
                "WHERE media_verification.tmdb_id='' AND excluded.tmdb_id<>'' "
                "AND media_verification.confirmed_at IS NULL AND media_verification.resolution=''",
                [
                    (
                        hint.record_key,
                        hint.revision,
                        target.section_id,
                        target.section_title,
                        hint.expected_path,
                        hint.title,
                        hint.media_type,
                        hint.season_num,
                        hint.episode_num,
                        now,
                        now + FIRST_VERIFICATION_DELAY,
                        hint.tmdb_id,
                    )
                    for hint in media
                ],
            )

    async def next_due(self, now: float) -> MediaVerification | None:
        return await asyncio.to_thread(self._next_due, now)

    def _next_due(self, now: float) -> MediaVerification | None:
        with self._lock:
            row = self._connection.execute(
                f"SELECT {VERIFICATION_COLUMNS}, tmdb_id, resolution, matched_path, resolved_at "
                "FROM media_verification WHERE confirmed_at IS NULL AND resolution='' AND next_check_at <= ? "
                "ORDER BY next_check_at, id LIMIT 1",
                (now,),
            ).fetchone()
        return MediaVerification(*row) if row else None

    async def mark_indexed(self, record_id: int, now: float) -> None:
        await asyncio.to_thread(self._mark_indexed, record_id, now)

    def _mark_indexed(self, record_id: int, now: float) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                "UPDATE media_verification SET confirmed_at=?, detail='' WHERE id=? AND resolution=''",
                (now, record_id),
            )
            self._connection.execute(
                "DELETE FROM media_verification WHERE confirmed_at IS NOT NULL AND id <= COALESCE("
                "(SELECT id FROM media_verification WHERE confirmed_at IS NOT NULL "
                "ORDER BY id DESC LIMIT 1 OFFSET ?), 0)",
                (MAX_INDEXED_RECORDS,),
            )

    async def mark_alternative(self, record_id: int, now: float, path: str) -> None:
        if not path:
            raise ValueError("替代版本路径不能为空")
        await asyncio.to_thread(self._mark_alternative, record_id, now, path)

    def _mark_alternative(self, record_id: int, now: float, path: str) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                "UPDATE media_verification SET resolution=?, resolved_at=?, matched_path=?, detail='' "
                "WHERE id=? AND confirmed_at IS NULL AND resolution=''",
                (ALTERNATIVE_RESOLUTION, now, path, record_id),
            )
            self._connection.execute(
                "DELETE FROM media_verification WHERE resolution=? AND id <= COALESCE("
                "(SELECT id FROM media_verification WHERE resolution=? "
                "ORDER BY id DESC LIMIT 1 OFFSET ?), 0)",
                (ALTERNATIVE_RESOLUTION, ALTERNATIVE_RESOLUTION, MAX_INDEXED_RECORDS),
            )

    async def defer(self, entry: MediaVerification, now: float, detail: str) -> None:
        delay = VERIFICATION_RETRY_SECONDS[
            min(entry.attempts, len(VERIFICATION_RETRY_SECONDS) - 1)
        ]
        await asyncio.to_thread(self._defer, entry.id, now + delay, detail)

    def _defer(self, record_id: int, next_check_at: float, detail: str) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                "UPDATE media_verification SET attempts=attempts+1, next_check_at=?, detail=? "
                "WHERE id=? AND confirmed_at IS NULL AND resolution=''",
                (next_check_at, detail, record_id),
            )

    async def indexed(
        self, limit: int = DEFAULT_DISPLAY_LIMIT
    ) -> list[MediaVerification]:
        return await asyncio.to_thread(self._indexed, min(limit, MAX_INDEXED_RECORDS))

    def _indexed(self, limit: int) -> list[MediaVerification]:
        with self._lock:
            rows = self._connection.execute(
                f"SELECT {self._projection()} "
                "FROM media_verification WHERE confirmed_at IS NOT NULL "
                "ORDER BY confirmed_at DESC, id DESC LIMIT ?",
                (max(0, limit),),
            ).fetchall()
        return [MediaVerification(*row) for row in rows]

    async def pending(
        self, limit: int = DEFAULT_DISPLAY_LIMIT
    ) -> list[MediaVerification]:
        return await asyncio.to_thread(self._pending, min(limit, DEFAULT_DISPLAY_LIMIT))

    def _pending(self, limit: int) -> list[MediaVerification]:
        with self._lock:
            rows = self._connection.execute(
                f"SELECT {self._projection()} "
                f"FROM media_verification WHERE {self._pending_predicate()} "
                "ORDER BY submitted_at DESC, id DESC LIMIT ?",
                (max(0, limit),),
            ).fetchall()
        return [MediaVerification(*row) for row in rows]

    async def pending_count(self) -> int:
        return await asyncio.to_thread(self._pending_count)

    def _pending_count(self) -> int:
        with self._lock:
            row = self._connection.execute(
                f"SELECT COUNT(*) FROM media_verification WHERE {self._pending_predicate()}"
            ).fetchone()
        return int(row[0])

    async def alternatives(
        self, limit: int = DEFAULT_DISPLAY_LIMIT
    ) -> list[MediaVerification]:
        return await asyncio.to_thread(
            self._alternatives, min(limit, MAX_INDEXED_RECORDS)
        )

    def _alternatives(self, limit: int) -> list[MediaVerification]:
        with self._lock:
            if "resolution" not in self._columns():
                return []
            rows = self._connection.execute(
                f"SELECT {self._projection()} FROM media_verification WHERE resolution=? "
                "ORDER BY resolved_at DESC, id DESC LIMIT ?",
                (ALTERNATIVE_RESOLUTION, max(0, limit)),
            ).fetchall()
        return [MediaVerification(*row) for row in rows]

    async def alternative_count(self) -> int:
        return await asyncio.to_thread(self._alternative_count)

    def _alternative_count(self) -> int:
        with self._lock:
            if "resolution" not in self._columns():
                return 0
            row = self._connection.execute(
                "SELECT COUNT(*) FROM media_verification WHERE resolution=?",
                (ALTERNATIVE_RESOLUTION,),
            ).fetchone()
        return int(row[0])

    def _columns(self) -> set[str]:
        return {
            row[1]
            for row in self._connection.execute("PRAGMA table_info(media_verification)")
        }

    def _projection(self) -> str:
        existing = self._columns()
        optional = [
            name if name in existing else ("NULL" if name == "resolved_at" else "''")
            for name in MIGRATION_COLUMNS
        ]
        return f"{VERIFICATION_COLUMNS}, " + ", ".join(optional)

    def _pending_predicate(self) -> str:
        return (
            "confirmed_at IS NULL AND resolution=''"
            if "resolution" in self._columns()
            else "confirmed_at IS NULL"
        )

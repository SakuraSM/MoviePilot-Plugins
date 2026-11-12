"""Bounded, asynchronous storage for plugin activity and Plex confirmations."""

from __future__ import annotations

import asyncio
import sqlite3
import threading
from collections.abc import Callable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal

from .verification_store import VerificationStoreMixin, create_verification_schema

ActivityLevel = Literal["info", "error"]
DEFAULT_ACTIVITY_CAPACITY = 2000
ACTIVITY_QUEUE_CAPACITY = 5000
ACTIVITY_BATCH_SIZE = 100


@dataclass(frozen=True)
class ActivityEntry:
    timestamp: float
    level: ActivityLevel
    message: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class ActivityJournal(VerificationStoreMixin):
    def __init__(
        self,
        connection: sqlite3.Connection,
        *,
        capacity: int,
        on_error: Callable[[str], None] | None,
    ) -> None:
        self._connection = connection
        self._lock = threading.RLock()
        self.capacity = max(1, capacity)
        self.on_error = on_error
        self._queue: asyncio.Queue[ActivityEntry | None] = asyncio.Queue(
            maxsize=ACTIVITY_QUEUE_CAPACITY
        )
        self._writer = asyncio.create_task(self._run())
        self._overflow_reported = False

    @classmethod
    async def open(
        cls,
        path: Path,
        *,
        capacity: int = DEFAULT_ACTIVITY_CAPACITY,
        on_error: Callable[[str], None] | None = None,
    ) -> ActivityJournal:
        connection = await asyncio.to_thread(cls._open_database, path)
        return cls(connection, capacity=capacity, on_error=on_error)

    @staticmethod
    def _open_database(path: Path) -> sqlite3.Connection:
        path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(path, check_same_thread=False)
        try:
            with connection:
                connection.execute(
                    "CREATE TABLE IF NOT EXISTS activity ("
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp REAL NOT NULL, "
                    "level TEXT NOT NULL, message TEXT NOT NULL)"
                )
                create_verification_schema(connection)
        except Exception:
            connection.close()
            raise
        return connection

    def append(self, entry: ActivityEntry) -> None:
        try:
            self._queue.put_nowait(entry)
            self._overflow_reported = False
        except asyncio.QueueFull:
            if not self._overflow_reported and self.on_error:
                self.on_error("插件日志写入队列已满，部分日志未保存")
            self._overflow_reported = True

    async def _run(self) -> None:
        while True:
            entry = await self._queue.get()
            if entry is None:
                self._queue.task_done()
                return
            batch = [entry]
            while len(batch) < ACTIVITY_BATCH_SIZE:
                try:
                    next_entry = self._queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                if next_entry is None:
                    self._queue.task_done()
                    await self._write(batch)
                    return
                batch.append(next_entry)
            await self._write(batch)

    async def _write(self, batch: list[ActivityEntry]) -> None:
        try:
            await asyncio.to_thread(self._write_batch, batch)
        except Exception as error:  # noqa: BLE001 - logging must not interrupt scans.
            if self.on_error:
                self.on_error(f"插件日志保存失败：{error}")
        finally:
            for _entry in batch:
                self._queue.task_done()

    def _write_batch(self, batch: list[ActivityEntry]) -> None:
        with self._lock, self._connection:
            self._connection.executemany(
                "INSERT INTO activity(timestamp, level, message) VALUES (?, ?, ?)",
                [(entry.timestamp, entry.level, entry.message) for entry in batch],
            )
            self._connection.execute(
                "DELETE FROM activity WHERE id <= COALESCE("
                "(SELECT id FROM activity ORDER BY id DESC LIMIT 1 OFFSET ?), 0)",
                (self.capacity,),
            )

    async def recent(
        self, limit: int = DEFAULT_ACTIVITY_CAPACITY
    ) -> list[ActivityEntry]:
        await self._queue.join()
        return await asyncio.to_thread(self._read_recent, min(self.capacity, limit))

    def _read_recent(self, limit: int) -> list[ActivityEntry]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT timestamp, level, message FROM activity ORDER BY id DESC LIMIT ?",
                (max(0, limit),),
            ).fetchall()
        return [
            ActivityEntry(timestamp, level, message)
            for timestamp, level, message in rows
        ]

    async def close(self) -> None:
        await self._queue.put(None)
        await self._writer
        await asyncio.to_thread(self._close_database)

    def _close_database(self) -> None:
        with self._lock:
            self._connection.close()


class ActivitySnapshotReader(VerificationStoreMixin):
    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection
        self._lock = threading.RLock()

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT timestamp, level, message FROM activity ORDER BY id DESC LIMIT ?",
                (DEFAULT_ACTIVITY_CAPACITY,),
            ).fetchall()
        return {
            "activities": [
                ActivityEntry(timestamp, level, message).to_dict()
                for timestamp, level, message in rows
            ],
            "indexed_media": [
                entry.to_dict() for entry in self._indexed(DEFAULT_ACTIVITY_CAPACITY)
            ],
            "pending_media": [
                entry.to_dict() for entry in self._pending(DEFAULT_ACTIVITY_CAPACITY)
            ],
            "pending_verifications": self._pending_count(),
            "alternative_media": [
                entry.to_dict()
                for entry in self._alternatives(DEFAULT_ACTIVITY_CAPACITY)
            ],
            "alternative_verifications": self._alternative_count(),
        }


def read_offline_activity(path: Path) -> dict[str, object]:
    if not path.is_file():
        return {}
    connection = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)
    try:
        return ActivitySnapshotReader(connection).snapshot()
    finally:
        connection.close()

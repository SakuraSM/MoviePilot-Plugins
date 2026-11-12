"""Durable history deduplication without a bounded in-memory ID window."""

from __future__ import annotations

import asyncio
import sqlite3
import threading
from pathlib import Path

from .history_models import TTDHistoryRecord


class HistoryStore:
    """Keep record revisions across restarts and full-history reconciliation."""

    def __init__(self, path: Path, scope: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.scope = scope
        self._lock = threading.RLock()
        self._connection = sqlite3.connect(path, check_same_thread=False)
        with self._connection:
            self._connection.execute("PRAGMA synchronous=FULL")
            self._connection.execute(
                "CREATE TABLE IF NOT EXISTS processed ("
                "scope TEXT NOT NULL, record_key TEXT NOT NULL, revision TEXT NOT NULL, "
                "PRIMARY KEY(scope, record_key, revision))"
            )
            self._connection.execute(
                "CREATE TABLE IF NOT EXISTS admitted ("
                "scope TEXT NOT NULL, epoch TEXT NOT NULL, record_key TEXT NOT NULL, "
                "PRIMARY KEY(scope, epoch, record_key))"
            )
            self._connection.execute(
                "CREATE TABLE IF NOT EXISTS historical_only ("
                "scope TEXT NOT NULL, record_key TEXT NOT NULL, revision TEXT NOT NULL, "
                "PRIMARY KEY(scope, record_key, revision))"
            )

    async def unseen(
        self, records: list[TTDHistoryRecord], *, include_historical_only: bool = False
    ) -> list[TTDHistoryRecord]:
        return await asyncio.to_thread(self._unseen, records, include_historical_only)

    def _unseen(
        self, records: list[TTDHistoryRecord], include_historical_only: bool
    ) -> list[TTDHistoryRecord]:
        unseen = []
        with self._lock, self._connection:
            for record in records:
                key = (self.scope, record.key, record.revision)
                processed = self._connection.execute(
                    "SELECT 1 FROM processed WHERE scope=? AND record_key=? "
                    "AND revision=?",
                    key,
                ).fetchone()
                historical_only = (
                    processed
                    and include_historical_only
                    and self._connection.execute(
                        "SELECT 1 FROM historical_only WHERE scope=? AND record_key=? "
                        "AND revision=?",
                        key,
                    ).fetchone()
                )
                if not processed or historical_only:
                    unseen.append(record)
        return unseen

    async def remember(
        self, records: list[TTDHistoryRecord], *, historical: bool = False
    ) -> None:
        await asyncio.to_thread(self._remember, records, historical)

    async def known_keys(
        self, records: list[TTDHistoryRecord], *, epoch: str
    ) -> set[str]:
        return await asyncio.to_thread(self._known_keys, records, epoch)

    def _known_keys(self, records: list[TTDHistoryRecord], epoch: str) -> set[str]:
        with self._lock:
            return {
                record.key
                for record in records
                if self._connection.execute(
                    "SELECT 1 FROM admitted WHERE scope=? AND epoch=? AND record_key=?",
                    (self.scope, epoch, record.key),
                ).fetchone()
            }

    async def admit(self, records: list[TTDHistoryRecord], *, epoch: str) -> None:
        await asyncio.to_thread(self._admit, records, epoch)

    def _admit(self, records: list[TTDHistoryRecord], epoch: str) -> None:
        with self._lock, self._connection:
            self._connection.executemany(
                "INSERT OR IGNORE INTO admitted VALUES (?, ?, ?)",
                [(self.scope, epoch, record.key) for record in records],
            )

    def _remember(self, records: list[TTDHistoryRecord], historical: bool) -> None:
        entries = [(self.scope, record.key, record.revision) for record in records]
        with self._lock, self._connection:
            if historical:
                self._connection.executemany(
                    "INSERT OR IGNORE INTO historical_only "
                    "SELECT ?, ?, ? WHERE NOT EXISTS ("
                    "SELECT 1 FROM processed WHERE scope=? AND record_key=? AND revision=?)",
                    [entry + entry for entry in entries],
                )
            self._connection.executemany(
                "INSERT OR IGNORE INTO processed VALUES (?, ?, ?)",
                entries,
            )
            if not historical:
                self._connection.executemany(
                    "DELETE FROM historical_only WHERE scope=? AND record_key=? AND revision=?",
                    entries,
                )

    async def close(self) -> None:
        await asyncio.to_thread(self._close)

    def _close(self) -> None:
        with self._lock:
            self._connection.close()

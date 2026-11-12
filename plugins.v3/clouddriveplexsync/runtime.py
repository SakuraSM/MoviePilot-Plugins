"""Run the history consumer and durable Plex scan queue."""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from collections.abc import Callable
from pathlib import Path

from .activity_log import ActivityJournal
from .configuration import PluginSettings
from .content_verifier import ContentVerifier
from .event_worker import EventWorker
from .history_control import HistoryControl
from .history_models import TTDCursor, TTDHistoryMode, TTDHistoryRecord, TTDSubmitResult
from .history_policy import HistoryPolicy
from .history_poller import TTDHistoryPoller
from .history_store import HistoryStore
from .models import HISTORY_SOURCE, MediaHint, PlexTarget, PluginStats
from .path_mapper import is_under
from .plex_bridge import PlexBridge, PlexLibraryNotFoundError
from .queue_policy import HELD_HISTORY_KEY, prepare_scan_queue, read_queue
from .runtime_status import build_runtime_status
from .ttd_client import TTDClient
from .verification_store import MediaVerification


class SyncRuntime:
    def __init__(
        self,
        *,
        settings: PluginSettings,
        service: object,
        data_path: Path,
        load: Callable[[str], object],
        save: Callable[[str, object], None],
        on_error: Callable[[str], None],
        on_info: Callable[[str], None] | None = None,
        persist_history_enabled: Callable[[bool], None] | None = None,
    ) -> None:
        self.settings = settings
        self.stats = PluginStats()
        self.plex = PlexBridge(service, settings.plex_sections, self.stats)
        self.worker = EventWorker(
            settings.mapper,
            self.plex,
            debounce_seconds=settings.debounce_seconds,
            capacity=settings.queue_capacity,
            scans_per_second=settings.scans_per_second,
            stats=self.stats,
            persist_queue=lambda values: save("pending_scans", values),
            persist_held=lambda values: save(HELD_HISTORY_KEY, values),
            on_error=on_error,
            on_info=on_info,
        )
        self.data_path, self.load, self.save = data_path, load, save
        self.on_error = on_error
        self.stop = asyncio.Event()
        self.ready = asyncio.Event()
        self.poller: TTDHistoryPoller | None = None
        self.history: HistoryControl | None = None
        self.persist_history_enabled = persist_history_enabled
        self.activity: ActivityJournal | None = None
        self.verifier: ContentVerifier | None = None
        self.activity_error = ""

    def _report_ttd_error(self, message: str) -> None:
        self.worker.record_external_error(message)
        self.on_error(message)

    def _report_activity_error(self, message: str) -> None:
        self.activity_error = message
        self.on_error(message)

    async def _track_submitted(
        self, target: PlexTarget, media: list[MediaHint]
    ) -> None:
        if self.activity:
            await self.activity.add_submitted(target, media, time.time())

    def _record_indexed(self, entry: MediaVerification) -> None:
        self.worker.record_info(
            "PLEX_INDEXED "
            + json.dumps(
                {
                    "title": entry.title,
                    "season": entry.season_num,
                    "episode": entry.episode_num,
                    "plex_path": entry.expected_path,
                    "section_title": entry.section_title,
                },
                ensure_ascii=False,
            )
        )

    async def set_history_enabled(self, enabled: bool) -> dict[str, object]:
        if not self.history:
            raise RuntimeError("历史处理后台尚未就绪")
        await self.history.set_enabled(enabled)
        return self.history.status()

    async def _submit_history(self, path: str, _source: str) -> TTDSubmitResult:
        return await self.submit_directory(path, HISTORY_SOURCE)

    def _build_history(
        self, client: TTDClient, store: HistoryStore, legacy: TTDCursor
    ) -> HistoryControl:
        saved = self.load("ttd_audit_cursor")
        cursor = TTDCursor.from_dict(saved)
        if saved is None and legacy.history_mode == TTDHistoryMode.INCLUDE_HISTORY:
            cursor = TTDCursor.from_dict(legacy.to_dict())
        poller = TTDHistoryPoller(
            client,
            self._submit_history,
            self.settings.mapper.target_root,
            poll_seconds=self.settings.poll_seconds,
            reconcile_hours=self.settings.reconcile_hours,
            cursor=cursor,
            history_mode=TTDHistoryMode.INCLUDE_HISTORY,
            store=store,
            persist_cursor=lambda value: self.save("ttd_audit_cursor", value),
            on_error=self._report_ttd_error,
            on_skip=self.worker.record_info,
            stats=self.stats,
            submit_records=lambda path, records: self.submit_directory(
                path, HISTORY_SOURCE, records=records
            ),
        )
        return HistoryControl(poller, self.worker, self.persist_history_enabled)

    async def submit_directory(
        self, path: str, source: str, *, records: list[TTDHistoryRecord] | None = None
    ) -> TTDSubmitResult:
        if any(is_under(path, prefix) for prefix in self.settings.skip_paths):
            return TTDSubmitResult.skipped("命中配置的跳过路径")
        try:
            accepted = await self.worker.submit_scan_directory(
                path, source, records=records or ()
            )
            return (
                TTDSubmitResult.queued()
                if accepted
                else TTDSubmitResult.retry("扫描队列已满")
            )
        except PlexLibraryNotFoundError as error:
            if self.settings.unmatched_policy == "skip":
                return TTDSubmitResult.skipped(str(error))
            return TTDSubmitResult.retry(str(error))
        except Exception as error:  # noqa: BLE001 - external Plex failures must leave history retryable.
            return TTDSubmitResult.retry(str(error))

    async def run(self) -> None:
        await asyncio.to_thread(self.plex.assert_supported)
        client = TTDClient(
            self.settings.url,
            source=self.settings.source,
            api_token=self.settings.api_token,
            page_size=self.settings.page_size,
            verify=self.settings.verify_tls,
            stats=self.stats,
        )
        scope = hashlib.sha256(
            f"{client.base_url}|{client.source}|{self.settings.mapper.target_root}".encode()
        ).hexdigest()
        store: HistoryStore | None = None
        tasks: list[asyncio.Task[None]] = []
        try:
            try:
                self.activity = await ActivityJournal.open(
                    self.data_path / "activity.sqlite3",
                    on_error=self._report_activity_error,
                )
                self.worker.on_activity = self.activity.append
                self.worker.on_submitted = self._track_submitted
                self.verifier = ContentVerifier(
                    self.plex,
                    self.activity,
                    on_indexed=self._record_indexed,
                    on_error=self._report_ttd_error,
                )
            except Exception as error:  # noqa: BLE001 - log storage must not prevent scans.
                self.activity_error = f"日志与入库核验不可用：{type(error).__name__}"
                self.on_error(self.activity_error)
            cursor = TTDCursor.from_dict(self.load("ttd_cursor"))
            legacy = TTDCursor.from_dict(cursor.to_dict())
            policy = HistoryPolicy.prepare(
                mode=TTDHistoryMode.NEW_ONLY, cursor=cursor, scope=scope
            )
            store = await asyncio.to_thread(
                HistoryStore, self.data_path / "history.sqlite3", policy.cursor.scope
            )
            await prepare_scan_queue(
                worker=self.worker,
                policy=policy,
                load=self.load,
                save=self.save,
            )
            self.poller = TTDHistoryPoller(
                client,
                self.submit_directory,
                self.settings.mapper.target_root,
                poll_seconds=self.settings.poll_seconds,
                max_pages=self.settings.max_pages,
                overlap_pages=self.settings.overlap_pages,
                reconcile_hours=self.settings.reconcile_hours,
                cursor=policy.cursor,
                history_mode=TTDHistoryMode.NEW_ONLY,
                store=store,
                persist_cursor=lambda value: self.save("ttd_cursor", value),
                on_error=self._report_ttd_error,
                on_skip=self.worker.record_info,
                stats=self.stats,
                submit_records=lambda path, records: self.submit_directory(
                    path, "ttd", records=records
                ),
            )
            self.worker.load_held_history(await read_queue(self.load, HELD_HISTORY_KEY))
            self.history = self._build_history(client, store, legacy)
            await self.history.set_enabled(self.settings.history_enabled, persist=False)
            tasks = [
                asyncio.create_task(self.worker.run(self.stop)),
                asyncio.create_task(self.poller.run(self.stop)),
            ]
            if self.verifier:
                tasks.append(asyncio.create_task(self.verifier.run(self.stop)))
            stop_task = asyncio.create_task(self._wait_stop())
            tasks.append(stop_task)
            self.ready.set()
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                if task is not stop_task:
                    task.result()
                    if not self.stop.is_set():
                        raise RuntimeError("同步后台任务意外退出")
        finally:
            self.ready.clear()
            if self.history:
                await self.history.close()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self.worker.on_activity = None
            self.worker.on_submitted = None
            if self.activity:
                await self.activity.close()
            await client.close()
            if store:
                await store.close()

    async def _wait_stop(self) -> None:
        await self.stop.wait()

    async def status(self) -> dict[str, object]:
        return await build_runtime_status(self)

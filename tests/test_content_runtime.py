from __future__ import annotations

import asyncio
import copy
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import _bootstrap  # noqa: F401
from clouddriveplexsync.activity_log import ActivityEntry, ActivityJournal
from clouddriveplexsync.configuration import PluginSettings
from clouddriveplexsync.runtime import SyncRuntime
from clouddriveplexsync.ttd_client import TTDClient
from test_content_verifier import FakeItem, SearchSection
from test_plex_bridge import FakePlex, FakeService
from test_ttd_client import FakeHTTPClient, FakeResponse


class ContentRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_logs_remain_readable_when_plex_is_unreachable(self) -> None:
        settings = PluginSettings.from_config(
            {"ttd_url": "https://ttd.example", "ttd_api_token": "test-token"}
        )
        with tempfile.TemporaryDirectory() as directory:
            runtime = SyncRuntime(
                settings=settings,
                service=FakeService(FakePlex([])),
                data_path=Path(directory),
                load=lambda _key: None,
                save=lambda _key, _value: None,
                on_error=lambda _message: None,
            )
            runtime.activity = await ActivityJournal.open(
                Path(directory) / "activity.sqlite3"
            )
            try:
                runtime.activity.append(ActivityEntry(1.0, "error", "测试日志"))
                with patch.object(
                    runtime.plex, "describe", side_effect=OSError("offline")
                ):
                    status = await runtime.status()
                self.assertEqual(status["activities"][0]["message"], "测试日志")
                self.assertEqual(status["plex"], {"error": "OSError"})
            finally:
                await runtime.activity.close()

    async def test_slow_plex_status_does_not_block_log_display_indefinitely(
        self,
    ) -> None:
        settings = PluginSettings.from_config(
            {"ttd_url": "https://ttd.example", "ttd_api_token": "test-token"}
        )
        with tempfile.TemporaryDirectory() as directory:
            runtime = SyncRuntime(
                settings=settings,
                service=FakeService(FakePlex([])),
                data_path=Path(directory),
                load=lambda _key: None,
                save=lambda _key, _value: None,
                on_error=lambda _message: None,
            )
            runtime.activity = await ActivityJournal.open(
                Path(directory) / "activity.sqlite3"
            )
            runtime.activity.append(ActivityEntry(1.0, "info", "可查看的日志"))
            release = threading.Event()

            def slow_describe():
                release.wait(2)
                return {}

            try:
                with (
                    patch.object(runtime.plex, "describe", side_effect=slow_describe),
                    patch(
                        "clouddriveplexsync.runtime_status.PLEX_STATUS_TIMEOUT_SECONDS",
                        0.05,
                    ),
                ):
                    status = await runtime.status()
                self.assertEqual(status["activities"][0]["message"], "可查看的日志")
                self.assertEqual(status["plex"], {"error": "TimeoutError"})
            finally:
                release.set()
                await runtime.activity.close()

    async def test_ttd_record_is_confirmed_only_after_plex_matches_the_file(
        self,
    ) -> None:
        settings = PluginSettings.from_config(
            {
                "ttd_url": "https://ttd.example",
                "ttd_api_token": "test-token",
                "ttd_target_root": "/cloud",
                "cloud_plex_path_overrides": "/cloud => /plex",
                "debounce_seconds": 0,
                "scans_per_second": 10000,
            }
        )
        section = SearchSection()
        expected = "/plex/节目/Season 1/节目.S01E02.2160p.mkv"
        section.show.item = FakeItem([expected])
        transport = FakeHTTPClient(
            FakeResponse(
                payload={
                    "success": True,
                    "has_next": False,
                    "records": [{"id": 1, "target_path": "old", "status": "success"}],
                }
            )
        )
        saved = {}

        def save(key, value):
            saved[key] = copy.deepcopy(value)

        def make_client(*args, **kwargs):
            return TTDClient(*args, **kwargs, client=transport)

        with tempfile.TemporaryDirectory() as directory:
            runtime = SyncRuntime(
                settings=settings,
                service=FakeService(FakePlex([section])),
                data_path=Path(directory),
                load=saved.get,
                save=save,
                on_error=lambda _message: None,
            )
            with patch("clouddriveplexsync.runtime.TTDClient", side_effect=make_client):
                task = asyncio.create_task(runtime.run())
                try:

                    async def wait_for_baseline():
                        while not (
                            runtime.ready.is_set() and runtime.poller.cursor.initialized
                        ):
                            await asyncio.sleep(0.01)

                    await asyncio.wait_for(wait_for_baseline(), timeout=5)
                    transport.response._payload["records"].insert(
                        0,
                        {
                            "id": 2,
                            "status": "success",
                            "target_path": "节目/Season 1",
                            "title": "节目",
                            "media_type": "TV",
                            "season_num": 1,
                            "episode_num": 2,
                            "file_name": "节目.S01E02.2160p.mkv",
                        },
                    )
                    await runtime.poller.poll_once()

                    async def wait_for_verification():
                        while not await runtime.activity.pending_count():
                            await asyncio.sleep(0.01)

                    await asyncio.wait_for(wait_for_verification(), timeout=5)
                    self.assertEqual(await runtime.activity.indexed(), [])
                    due = await runtime.activity.next_due(time.time() + 30)
                    await runtime.verifier.check_one(due, time.time() + 30)
                    status = await runtime.status()
                    self.assertEqual(
                        status["indexed_media"][0]["expected_path"], expected
                    )
                    self.assertTrue(
                        any(
                            "PLEX_INDEXED" in item["message"]
                            for item in status["activities"]
                        )
                    )
                finally:
                    runtime.stop.set()
                    await task
            reopened = await ActivityJournal.open(Path(directory) / "activity.sqlite3")
            try:
                self.assertEqual((await reopened.indexed())[0].expected_path, expected)
            finally:
                await reopened.close()

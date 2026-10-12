from __future__ import annotations

import asyncio
import copy
import tempfile
import unittest
from dataclasses import asdict
from pathlib import Path
from unittest.mock import patch

import _bootstrap  # noqa: F401
from clouddriveplexsync.configuration import PluginSettings
from clouddriveplexsync.models import PendingScan
from clouddriveplexsync.runtime import SyncRuntime
from clouddriveplexsync.ttd_client import TTDClient
from test_plex_bridge import FakePlex, FakeSection, FakeService
from test_ttd_client import FakeHTTPClient, FakeResponse


class RuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_stop_requested_by_worker_is_not_reported_as_unexpected_exit(
        self,
    ) -> None:
        settings = PluginSettings.from_config(
            {"ttd_url": "https://ttd.example", "ttd_api_token": "test-token"}
        )
        transport = FakeHTTPClient(
            FakeResponse(payload={"success": True, "records": []})
        )

        def make_client(*args, **kwargs):
            return TTDClient(*args, **kwargs, client=transport)

        async def stop_normally(stop: asyncio.Event) -> None:
            stop.set()

        with tempfile.TemporaryDirectory() as directory:
            runtime = SyncRuntime(
                settings=settings,
                service=FakeService(FakePlex([])),
                data_path=Path(directory),
                load=lambda _key: None,
                save=lambda _key, _value: None,
                on_error=lambda _message: None,
            )
            runtime.worker.run = stop_normally
            with patch("clouddriveplexsync.runtime.TTDClient", side_effect=make_client):
                await runtime.run()
            self.assertFalse(runtime.ready.is_set())

    async def test_default_mode_holds_old_tasks_and_only_scans_post_baseline_records(
        self,
    ) -> None:
        settings = PluginSettings.from_config(
            {
                "ttd_url": "https://ttd.example",
                "ttd_api_token": "test-token",
                "ttd_target_root": "/cloud",
                "cloud_plex_path_overrides": "/cloud => /plex",
                "scans_per_second": 10000,
            }
        )
        section = FakeSection("1", "Movies", ["/plex"])
        transport = FakeHTTPClient(
            FakeResponse(
                payload={
                    "success": True,
                    "has_next": False,
                    "records": [{"id": 10, "target_path": "old", "status": "success"}],
                }
            )
        )
        saved = {
            "pending_scans": [
                asdict(
                    PendingScan(
                        cloud_path="/cloud/historical",
                        plex_path="/plex/historical",
                        section_id="1",
                        section_title="Movies",
                        first_seen=0,
                        last_seen=0,
                        sources=["ttd"],
                    )
                )
            ]
        }

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
                    for _ in range(500):
                        if runtime.poller and runtime.poller.cursor.initialized:
                            break
                        await asyncio.sleep(0.001)
                    self.assertTrue(runtime.poller.cursor.initialized)
                    self.assertEqual(section.updates, [])
                    self.assertEqual(runtime.held_history_scans, 1)
                    self.assertIsNone(runtime.poller.audit)
                    self.assertEqual(len(saved["held_history_scans"]), 1)
                    transport.response._payload["records"].insert(
                        0,
                        {
                            "id": 11,
                            "target_path": "new",
                            "status": "success",
                        },
                    )
                    await runtime.poller.poll_once()
                    await runtime.worker.flush(force=True)
                    self.assertEqual(section.updates, ["/plex/new"])
                finally:
                    runtime.stop.set()
                    await task

    async def test_history_queue_plex_and_restart_are_connected(self) -> None:
        settings = PluginSettings.from_config(
            {
                "enabled": True,
                "ttd_history_mode": "include_history",
                "ttd_url": "https://ttd.example",
                "ttd_api_token": "test-token",
                "ttd_target_root": "/cloud",
                "cloud_plex_path_overrides": "/cloud => /plex",
                "plex_server": "Plex",
                "debounce_seconds": 0,
                "scans_per_second": 10000,
            }
        )
        section = FakeSection("1", "Movies", ["/plex"])
        service = FakeService(FakePlex([section]))
        transport = FakeHTTPClient(
            FakeResponse(
                payload={
                    "success": True,
                    "has_next": False,
                    "records": [
                        {"id": 1, "target_path": "Movies/Film", "status": "success"},
                        {"id": 2, "target_path": "Movies/Film", "status": "success"},
                    ],
                }
            )
        )
        saved, errors = {}, []
        activity_logs = []

        def save(key, value):
            saved[key] = copy.deepcopy(value)

        def make_client(*args, **kwargs):
            return TTDClient(*args, **kwargs, client=transport)

        with tempfile.TemporaryDirectory() as directory:
            for run_index in range(2):
                runtime = SyncRuntime(
                    settings=settings,
                    service=service,
                    data_path=Path(directory),
                    load=saved.get,
                    save=save,
                    on_error=errors.append,
                    on_info=activity_logs.append,
                )
                with patch(
                    "clouddriveplexsync.runtime.TTDClient", side_effect=make_client
                ):
                    task = asyncio.create_task(runtime.run())
                    try:
                        for _ in range(500):
                            if (
                                runtime.poller
                                and runtime.poller.cursor.audit_next_page == 0
                                and (run_index or section.updates)
                            ):
                                break
                            await asyncio.sleep(0.001)
                        self.assertTrue(runtime.ready.is_set())
                        self.assertEqual(runtime.poller.cursor.audit_next_page, 0)
                        self.assertEqual(section.updates, ["/plex/Movies/Film"])
                        self.assertNotIn("test-token", str(await runtime.status()))
                    finally:
                        runtime.stop.set()
                        await task
            self.assertTrue((Path(directory) / "history.sqlite3").exists())
        self.assertEqual(errors, [])
        self.assertEqual(saved["pending_scans"], [])
        self.assertTrue(saved["ttd_cursor"]["initialized"])
        self.assertEqual(
            transport.calls[0][1]["headers"]["authorization"], "Bearer test-token"
        )
        self.assertNotIn("cookie", transport.calls[0][1]["headers"])
        self.assertTrue(any(message.startswith("QUEUE ") for message in activity_logs))
        self.assertTrue(
            any(message.startswith("SCAN_START ") for message in activity_logs)
        )
        self.assertTrue(
            any(message.startswith("SCAN_SUBMITTED ") for message in activity_logs)
        )
        self.assertNotIn("test-token", "\n".join(activity_logs))

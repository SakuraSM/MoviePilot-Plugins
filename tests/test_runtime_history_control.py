from __future__ import annotations

import asyncio
import copy
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import _bootstrap  # noqa: F401
from clouddriveplexsync.configuration import PluginSettings
from clouddriveplexsync.runtime import SyncRuntime
from clouddriveplexsync.ttd_client import TTDClient
from test_plex_bridge import FakePlex, FakeSection, FakeService
from test_ttd_client import FakeHTTPClient, FakeResponse


class RuntimeHistoryControlTests(unittest.IsolatedAsyncioTestCase):
    async def test_live_toggle_keeps_new_baseline_and_disabled_state_survives_restart(
        self,
    ):
        saved = {}
        config = {
            "ttd_url": "https://ttd.example",
            "ttd_api_token": "test-token",
            "ttd_target_root": "/cloud",
            "cloud_plex_path_overrides": "/cloud => /plex",
            "scans_per_second": 10000,
        }
        section = FakeSection("1", "Movies", ["/plex"])
        transport = FakeHTTPClient(
            FakeResponse(
                payload={
                    "success": True,
                    "has_next": False,
                    "records": [{"id": 1, "target_path": "old", "status": "success"}],
                }
            )
        )

        def save(key, value):
            saved[key] = copy.deepcopy(value)

        def make_client(*args, **kwargs):
            return TTDClient(*args, **kwargs, client=transport)

        def persist_enabled(enabled):
            config["ttd_history_enabled"] = enabled

        previous_epoch = ""
        with (
            tempfile.TemporaryDirectory() as directory,
            patch("clouddriveplexsync.runtime.TTDClient", side_effect=make_client),
        ):
            for run_index in range(2):
                runtime = SyncRuntime(
                    settings=PluginSettings.from_config(config),
                    service=FakeService(FakePlex([section])),
                    data_path=Path(directory),
                    load=saved.get,
                    save=save,
                    on_error=lambda _message: None,
                    persist_history_enabled=persist_enabled,
                )
                task = asyncio.create_task(runtime.run())
                try:
                    for _ in range(500):
                        if runtime.ready.is_set() and runtime.poller.cursor.initialized:
                            break
                        await asyncio.sleep(0.001)
                    self.assertTrue(runtime.ready.is_set())
                    self.assertFalse(runtime.history.enabled)
                    baseline = runtime.poller.cursor.to_dict()
                    self.assertEqual(await runtime.worker.flush(force=True), 0)
                    if run_index:
                        self.assertEqual(baseline["mode_epoch"], previous_epoch)
                        self.assertFalse(config["ttd_history_enabled"])
                        self.assertFalse(
                            (await runtime.status())["history_processing"]["running"]
                        )
                        continue
                    await runtime.set_history_enabled(True)
                    for _ in range(500):
                        if runtime.history.poller.cursor.audit_next_page == 0:
                            break
                        await asyncio.sleep(0.001)
                    self.assertEqual(runtime.history.poller.cursor.audit_next_page, 0)
                    await runtime.set_history_enabled(False)
                    self.assertEqual(runtime.poller.cursor.to_dict(), baseline)
                    self.assertFalse(config["ttd_history_enabled"])
                    transport.response._payload["records"].insert(
                        0,
                        {
                            "id": 2,
                            "target_path": "new",
                            "status": "success",
                        },
                    )
                    await runtime.poller.poll_once()
                    await runtime.worker.flush(force=True)
                    self.assertEqual(section.updates, ["/plex/new"])
                    self.assertEqual(len(runtime.worker.held_history), 1)
                    previous_epoch = runtime.poller.cursor.mode_epoch
                finally:
                    runtime.stop.set()
                    await task
        self.assertEqual(section.updates, ["/plex/new"])

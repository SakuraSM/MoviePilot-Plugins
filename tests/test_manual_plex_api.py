from __future__ import annotations

import types
import unittest
from unittest.mock import AsyncMock, patch

import _bootstrap  # noqa: F401
from clouddriveplexsync.configuration import build_form
from test_plugin_contract import load_plugin_module


class ManualPlexAPITests(unittest.IsolatedAsyncioTestCase):
    async def test_direct_manual_request_uses_worker_and_reports_queue_result(
        self,
    ) -> None:
        plugin = load_plugin_module().CloudDrivePlexSync()
        submit = AsyncMock(return_value=True)
        plugin._runtime = types.SimpleNamespace(
            ready=types.SimpleNamespace(is_set=lambda: True),
            worker=types.SimpleNamespace(submit_plex_scan_directory=submit),
        )
        result = await plugin._api("resync_plex", "/plex/Film")
        submit.assert_awaited_once_with("/plex/Film")
        self.assertTrue(result["success"])
        self.assertIn("扫描队列", result["message"])

        submit.return_value = False
        result = await plugin._api("resync_plex", "/plex/Film")
        self.assertFalse(result["success"])
        self.assertIn("队列已满", result["message"])

    def test_endpoint_is_authenticated_and_receives_plex_path(self) -> None:
        plugin = load_plugin_module().CloudDrivePlexSync()
        endpoint = next(
            item for item in plugin.get_api() if item["path"] == "/resync/plex"
        )
        self.assertEqual(endpoint["auth"], "bear")
        self.assertEqual(endpoint["methods"], ["POST"])
        with patch.object(plugin, "_call", return_value={"success": True}) as call:
            plugin.api_resync_plex(types.SimpleNamespace(plex_path="/plex/Film"))
        call.assert_called_once_with("resync_plex", "/plex/Film")

    def test_form_exposes_direct_action_without_requiring_save(self) -> None:
        form, defaults = build_form([])
        card = next(
            node
            for node in form[0]["content"]
            if node.get("props", {}).get("title") == "手动刷新媒体库目录"
        )
        field, button, _feedback = card["content"]
        self.assertEqual(field["props"]["model"], "manual_plex_path")
        self.assertEqual(defaults["manual_plex_path"], "")
        self.assertEqual(button["props"]["type"], "button")
        self.assertIn(
            "plugin/CloudDrivePlexSync/resync/plex", button["props"]["onClick"]
        )

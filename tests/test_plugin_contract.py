from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from unittest.mock import patch

import _bootstrap
from clouddriveplexsync.configuration import PluginSettings


def install_moviepilot_stubs() -> None:
    class BaseModel:
        def __init__(self, **values):
            for key in self.__class__.__annotations__:
                setattr(self, key, values.get(key, getattr(self.__class__, key, None)))

        def model_dump(self):
            return {key: getattr(self, key) for key in self.__class__.__annotations__}

    class PluginBase:
        def __init__(self):
            self._data = {}

        def save_data(self, key, value):
            self._data[key] = value

        def get_data(self, key):
            return self._data.get(key)

        def get_config(self):
            return getattr(self, "_config", {})

        def update_config(self, config):
            self._config = config
            return True

        def post_message(self, **_kwargs):
            return None

    class MediaServerHelper:
        @staticmethod
        def get_configs():
            return {}

    logger = types.SimpleNamespace(
        error=lambda *_a, **_k: None,
        warning=lambda *_a, **_k: None,
        info=lambda *_a, **_k: None,
    )
    logger.exception = lambda *_a, **_k: None

    modules = {
        "pydantic": types.SimpleNamespace(BaseModel=BaseModel),
        "app": types.ModuleType("app"),
        "app.helper": types.ModuleType("app.helper"),
        "app.helper.mediaserver": types.SimpleNamespace(
            MediaServerHelper=MediaServerHelper
        ),
        "app.log": types.SimpleNamespace(logger=logger),
        "app.plugins": types.SimpleNamespace(_PluginBase=PluginBase),
        "app.sdk": types.ModuleType("app.sdk"),
        "app.sdk.plugin": types.SimpleNamespace(_PluginBase=PluginBase),
        "app.sdk.services": types.SimpleNamespace(MediaServerHelper=MediaServerHelper),
        "app.sdk.logging": types.SimpleNamespace(logger=logger),
    }
    for name, module in modules.items():
        sys.modules.setdefault(name, module)


def load_plugin_module():
    install_moviepilot_stubs()
    path = _bootstrap.PLUGIN_DIR / "__init__.py"
    spec = importlib.util.spec_from_file_location("clouddriveplexsync.entry", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class PluginContractTests(unittest.TestCase):
    def test_form_only_contains_ttd_plex_and_reliability_configuration(self) -> None:
        module = load_plugin_module()
        plugin = module.CloudDrivePlexSync()
        form, defaults = plugin.get_form()
        expected = "3.0.0" if _bootstrap.PLUGIN_GENERATION == "v3" else "2.2.1"
        self.assertEqual(plugin.plugin_version, expected)
        for key in defaults:
            self.assertFalse(
                any(
                    word in key
                    for word in (
                        "cd2",
                        "buffer",
                        "cookie",
                        "push",
                        "transfer",
                        "watch_roots",
                    )
                )
            )
        models = {}

        def collect(nodes):
            for node in nodes:
                props = node.get("props", {})
                if "model" in props:
                    models[props["model"]] = props
                collect(node.get("content", []))

        collect(form)
        self.assertEqual(set(models), set(defaults))
        self.assertEqual(models["ttd_api_token"]["type"], "password")
        self.assertEqual(defaults["ttd_unmatched_policy"], "retry")
        self.assertEqual(defaults["ttd_history_enabled"], False)
        switch = next(
            node
            for group in form[0]["content"]
            for node in group.get("content", [])
            if node.get("props", {}).get("model") == "ttd_history_enabled"
        )
        self.assertEqual(switch["component"], "VSwitch")
        self.assertEqual(
            models["ttd_reconcile_hours"]["show"],
            "{{ ttd_history_enabled }}",
        )
        self.assertEqual(defaults["ttd_page_size"], 100)
        self.assertEqual(
            {item["path"] for item in plugin.get_api()},
            {
                "/status",
                "/test",
                "/flush",
                "/resync",
                "/resync/plex",
                "/reconcile",
                "/history/start",
                "/history/stop",
            },
        )
        self.assertTrue(all(item["auth"] == "bear" for item in plugin.get_api()))
        for item in plugin.get_api():
            if item["path"].startswith("/history/"):
                self.assertEqual(item["methods"], ["POST"])

    def test_explicit_false_overrides_legacy_history_mode(self) -> None:
        self.assertTrue(
            PluginSettings.from_config(
                {"ttd_history_mode": "include_history"}
            ).history_enabled
        )
        for enabled in (True, False):
            settings = PluginSettings.from_config(
                {
                    "ttd_history_mode": "include_history",
                    "ttd_history_enabled": enabled,
                }
            )
            self.assertIs(settings.history_enabled, enabled)

    def test_live_history_control_persists_boolean_and_removes_legacy_key(self) -> None:
        plugin = load_plugin_module().CloudDrivePlexSync()
        plugin._config = {
            "ttd_history_mode": "include_history",
            "ttd_url": "https://ttd.example",
        }
        plugin._settings = PluginSettings.from_config(plugin._config)
        plugin._persist_history_enabled(False)
        self.assertIs(plugin._config["ttd_history_enabled"], False)
        self.assertNotIn("ttd_history_mode", plugin._config)
        self.assertEqual(plugin._config["ttd_url"], "https://ttd.example")
        self.assertFalse(plugin._settings.history_enabled)
        with (
            patch.object(plugin, "update_config", return_value=False),
            self.assertRaises(OSError),
        ):
            plugin._persist_history_enabled(True)
        self.assertFalse(plugin._settings.history_enabled)

    def test_detail_page_has_native_immediate_start_stop_actions(self) -> None:
        plugin = load_plugin_module().CloudDrivePlexSync()
        status = {
            "success": True,
            "data": {
                "ready": True,
                "history_processing": {"enabled": True, "running": True},
            },
        }
        with patch.object(plugin, "api_status", return_value=status):
            buttons = [
                node for node in plugin.get_page() if node["component"] == "VBtn"
            ]
        self.assertEqual(len(buttons), 2)
        self.assertTrue(buttons[0]["props"]["disabled"])
        self.assertFalse(buttons[1]["props"]["disabled"])
        self.assertEqual(
            buttons[1]["events"]["click"],
            {
                "api": "/plugin/CloudDrivePlexSync/history/stop",
                "method": "POST",
            },
        )

    def test_cookie_only_configuration_requires_api_token(self) -> None:
        plugin = load_plugin_module().CloudDrivePlexSync()
        plugin.init_plugin({"enabled": True, "ttd_cookie": "session=old"})
        self.assertFalse(plugin.get_state())
        self.assertIn("TTD API Token", plugin._last_error)
        self.assertNotIn("session=old", plugin._last_error)

    def test_api_token_configuration_starts_without_cd2(self) -> None:
        module = load_plugin_module()
        plugin = module.CloudDrivePlexSync()
        with patch.object(module.threading, "Thread") as thread:
            thread.return_value.is_alive.return_value = False
            plugin.init_plugin(
                {
                    "enabled": True,
                    "ttd_url": "https://ttd.example",
                    "ttd_api_token": "Bearer token",
                    "plex_server": "Plex",
                }
            )
            self.assertTrue(plugin.get_state())
            self.assertEqual(plugin._settings.api_token, "token")
            thread.return_value.start.assert_called_once()

    def test_malformed_token_stops_startup_without_exposing_it(self) -> None:
        plugin = load_plugin_module().CloudDrivePlexSync()
        plugin.init_plugin(
            {"enabled": True, "ttd_api_token": "secret-token\nInjected: value"}
        )
        self.assertFalse(plugin.get_state())
        self.assertNotIn("secret-token", plugin._last_error)

    def test_disabled_status_is_available_without_background(self) -> None:
        plugin = load_plugin_module().CloudDrivePlexSync()
        response = plugin.api_status()
        data = (
            response.data if _bootstrap.PLUGIN_GENERATION == "v3" else response["data"]
        )
        self.assertFalse(data["ready"])
        reconcile = plugin.api_reconcile()
        success = (
            reconcile.success
            if _bootstrap.PLUGIN_GENERATION == "v3"
            else reconcile["success"]
        )
        self.assertFalse(success)


if __name__ == "__main__":
    unittest.main()

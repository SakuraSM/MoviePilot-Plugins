from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from unittest.mock import patch

import _bootstrap


def install_moviepilot_stubs() -> None:
    class BaseModel:
        pass

    class PluginBase:
        def __init__(self):
            self._data = {}

        def save_data(self, key, value):
            self._data[key] = value

        def get_data(self, key):
            return self._data.get(key)

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
        self.assertEqual(plugin.plugin_version, "2.0.0")
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
        self.assertEqual(defaults["ttd_page_size"], 100)
        self.assertEqual(
            {item["path"] for item in plugin.get_api()},
            {"/status", "/test", "/flush", "/resync", "/reconcile"},
        )
        self.assertTrue(all(item["auth"] == "bear" for item in plugin.get_api()))

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
        self.assertFalse(plugin.api_status()["data"]["ready"])
        self.assertFalse(plugin.api_reconcile()["success"])


if __name__ == "__main__":
    unittest.main()

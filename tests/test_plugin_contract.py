from __future__ import annotations

import importlib.util
import sys
import types
import unittest

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

    class Event:
        event_data = {}

    class EventManager:
        @staticmethod
        def register(_event_type):
            return lambda function: function

    class MediaServerHelper:
        @staticmethod
        def get_configs():
            return {}

    logger = types.SimpleNamespace(error=lambda *_a, **_k: None, warning=lambda *_a, **_k: None)
    logger.exception = lambda *_a, **_k: None
    event_type = types.SimpleNamespace(TransferComplete="TransferComplete")

    modules = {
        "pydantic": types.SimpleNamespace(BaseModel=BaseModel),
        "app": types.ModuleType("app"),
        "app.core": types.ModuleType("app.core"),
        "app.core.event": types.SimpleNamespace(Event=Event, eventmanager=EventManager()),
        "app.helper": types.ModuleType("app.helper"),
        "app.helper.mediaserver": types.SimpleNamespace(MediaServerHelper=MediaServerHelper),
        "app.log": types.SimpleNamespace(logger=logger),
        "app.plugins": types.SimpleNamespace(_PluginBase=PluginBase),
        "app.schemas": types.ModuleType("app.schemas"),
        "app.schemas.types": types.SimpleNamespace(EventType=event_type),
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
    def test_moviepilot_v2_contract(self) -> None:
        module = load_plugin_module()
        plugin = module.CloudDrivePlexSync()
        form, defaults = plugin.get_form()
        apis = plugin.get_api()

        self.assertEqual(plugin.plugin_version, "1.1.0")
        self.assertEqual(len(apis), 7)
        self.assertTrue(all(item["auth"] == "bear" for item in apis))
        self.assertEqual(defaults["buffer_mode"], "adaptive")
        self.assertEqual(defaults["buffer_min_mb"], 1)
        self.assertFalse(defaults["enable_ttd"])
        self.assertEqual(defaults["ttd_initial_mode"], "baseline")
        self.assertEqual(form[0]["component"], "VForm")

    def test_failed_cd2_queue_is_not_used_to_suppress_ttd(self) -> None:
        module = load_plugin_module()

        class Worker:
            @staticmethod
            def submit_scan_directory(_path, _source):
                return False

            @staticmethod
            def has_pending_cloud_directory(_path):
                return False

        plugin = module.CloudDrivePlexSync()
        plugin._worker = Worker()
        cloud_path = "/光鸭云盘/Media/Video/已整理/电影/片名"

        self.assertFalse(plugin._submit_cd2_scan_directory(cloud_path))
        self.assertNotIn(cloud_path, plugin._recent_push_dirs)


if __name__ == "__main__":
    unittest.main()

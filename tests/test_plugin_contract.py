from __future__ import annotations

import asyncio
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

    logger = types.SimpleNamespace(
        error=lambda *_a, **_k: None,
        warning=lambda *_a, **_k: None,
        info=lambda *_a, **_k: None,
    )
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

        self.assertEqual(plugin.plugin_version, "1.4.0")
        self.assertEqual(len(apis), 7)
        self.assertTrue(all(item["auth"] == "bear" for item in apis))
        self.assertEqual(defaults["buffer_mode"], "adaptive")
        self.assertEqual(defaults["buffer_min_mb"], 1)
        self.assertFalse(defaults["enable_ttd"])
        self.assertEqual(defaults["ttd_initial_mode"], "baseline")
        self.assertEqual(defaults["ttd_unmatched_policy"], "skip")
        self.assertEqual(defaults["ttd_poll_seconds"], 30)
        self.assertEqual(defaults["log_mode"], "normal")
        self.assertEqual(defaults["log_dedup_seconds"], 300)
        self.assertEqual(defaults["recent_log_limit"], 50)
        self.assertEqual(
            defaults["cloud_plex_path_overrides"],
            "/光鸭云盘/Media/Video/已整理 => /data/CloudNas/Guangya",
        )
        self.assertEqual(form[0]["component"], "VForm")
        components = []
        models = []
        nodes = []

        def collect(items):
            for item in items:
                nodes.append(item)
                components.append(item.get("component"))
                model = (item.get("props") or {}).get("model")
                if model and model != "_tabs":
                    models.append(model)
                collect(item.get("content") or [])

        collect(form)
        self.assertIn("VTabs", components)
        self.assertEqual(components.count("VTab"), 5)
        self.assertEqual(components.count("VWindowItem"), 5)
        self.assertIn("VContainer", components)
        self.assertGreaterEqual(components.count("VCard"), 10)
        self.assertEqual(len(models), len(set(models)))
        self.assertEqual(set(models), set(defaults))

        tabs = next(item for item in nodes if item.get("component") == "VTabs")
        self.assertTrue(tabs["props"]["show-arrows"])
        self.assertNotIn("fixed-tabs", tabs["props"])
        self.assertNotIn("stacked", tabs["props"])

        window = next(item for item in nodes if item.get("component") == "VWindow")
        self.assertIn("overflow-visible", window["props"]["class"])

        text_fields = [
            item for item in nodes if item.get("component") == "VTextField"
        ]
        self.assertTrue(
            all(item["props"].get("variant") == "outlined" for item in text_fields)
        )
        fields_by_model = {
            item["props"].get("model"): item["props"]
            for item in text_fields
            if item["props"].get("model")
        }
        self.assertEqual(fields_by_model["ttd_poll_seconds"]["min"], 10)
        self.assertEqual(fields_by_model["ttd_poll_seconds"]["max"], 3600)
        self.assertEqual(fields_by_model["ttd_poll_seconds"]["suffix"], "秒")
        self.assertEqual(fields_by_model["debounce_seconds"]["min"], 0)
        self.assertEqual(fields_by_model["recent_log_limit"]["max"], 200)

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

    def test_repeated_refresh_warnings_are_aggregated_by_category(self) -> None:
        module = load_plugin_module()
        warnings = []
        notifications = []
        module.logger = types.SimpleNamespace(
            warning=warnings.append,
            info=lambda *_args, **_kwargs: None,
            error=lambda *_args, **_kwargs: None,
            exception=lambda *_args, **_kwargs: None,
        )
        plugin = module.CloudDrivePlexSync()
        plugin.post_message = lambda **kwargs: notifications.append(kwargs)

        plugin._set_error("TTD 目标目录刷新失败，仍提交局部扫描：/电影/A")
        plugin._set_error("TTD 目标目录刷新失败，仍提交局部扫描：/电影/B")

        self.assertEqual(len(warnings), 1)
        self.assertEqual(len(notifications), 1)
        self.assertEqual(plugin._log_limiter.snapshot()["suppressed_total"], 1)

    def test_ttd_poll_interval_rejects_values_below_ten_seconds(self) -> None:
        module = load_plugin_module()
        plugin = module.CloudDrivePlexSync()

        plugin.init_plugin({"enabled": False, "ttd_poll_seconds": 5})

        self.assertFalse(plugin.get_state())
        self.assertIn("TTD 轮询间隔必须在 10 到 3600 之间", plugin._last_error)

    def test_ttd_unmatched_plex_path_is_skipped_without_refreshing_cd2(self) -> None:
        module = load_plugin_module()

        class Mapper:
            @staticmethod
            def validate_cloud_path(path):
                return path

            @staticmethod
            def cloud_to_plex(_path):
                return "/data/CloudNas/Guangya/待整理-通用/片名"

        class Worker:
            mapper = Mapper()

        class Plex:
            @staticmethod
            def find_target(path):
                raise module.PlexLibraryNotFoundError(
                    f"no selected Plex library contains {path}"
                )

        class Client:
            def __init__(self):
                self.calls = []

            async def force_list(self, path):
                self.calls.append(path)

        plugin = module.CloudDrivePlexSync()
        plugin._worker = Worker()
        plugin._plex = Plex()
        plugin._client = Client()
        plugin._ttd_unmatched_policy = "skip"

        result = asyncio.run(
            plugin._submit_ttd_directory(
                "/光鸭云盘/Media/Video/已整理/待整理-通用/片名", "ttd"
            )
        )

        self.assertEqual(result.state.value, "skipped")
        self.assertIn("no selected Plex library", result.reason)
        self.assertEqual(plugin._client.calls, [])

    def test_ttd_mapping_failure_is_retried_even_when_unmatched_paths_are_skipped(self) -> None:
        module = load_plugin_module()

        class Mapper:
            @staticmethod
            def validate_cloud_path(path):
                return path

            @staticmethod
            def cloud_to_plex(path):
                raise module.PathMappingError(f"no mounted CloudDrive path matches {path}")

        class Worker:
            mapper = Mapper()

        plugin = module.CloudDrivePlexSync()
        plugin._worker = Worker()
        plugin._plex = object()
        plugin._client = object()
        plugin._ttd_unmatched_policy = "skip"

        result = asyncio.run(
            plugin._submit_ttd_directory(
                "/光鸭云盘/Media/Video/已整理/电影/片名", "ttd"
            )
        )

        self.assertEqual(result.state.value, "retry")
        self.assertIn("no mounted CloudDrive path", result.reason)

    def test_ttd_refreshes_only_the_exact_target_directory(self) -> None:
        module = load_plugin_module()
        root = "/光鸭云盘/Media/Video/已整理"
        target = f"{root}/电影/片名"

        class Mapper:
            @staticmethod
            def validate_cloud_path(path):
                return path

            @staticmethod
            def cloud_to_plex(_path):
                return "/data/CloudNas/Guangya/电影/片名"

        class Worker:
            mapper = Mapper()

            @staticmethod
            def has_pending_cloud_directory(_path):
                return False

            @staticmethod
            def submit_scan_directory(_path, _source):
                return True

        class Plex:
            @staticmethod
            def find_target(_path):
                return object()

        class Client:
            def __init__(self):
                self.calls = []

            async def force_list(self, path):
                self.calls.append(path)

        plugin = module.CloudDrivePlexSync()
        plugin._worker = Worker()
        plugin._plex = Plex()
        plugin._client = Client()
        plugin._watch_roots = [root]
        plugin._ttd_force_refresh = True

        result = asyncio.run(plugin._submit_ttd_directory(target, "ttd"))

        self.assertEqual(result.state.value, "queued")
        self.assertEqual(plugin._client.calls, [target])


if __name__ == "__main__":
    unittest.main()

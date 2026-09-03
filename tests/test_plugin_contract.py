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

        self.assertEqual(plugin.plugin_version, "1.3.1")
        self.assertEqual(len(apis), 7)
        self.assertTrue(all(item["auth"] == "bear" for item in apis))
        self.assertEqual(defaults["buffer_mode"], "adaptive")
        self.assertEqual(defaults["buffer_min_mb"], 1)
        self.assertTrue(defaults["enable_cd2"])
        self.assertFalse(defaults["enable_ttd"])
        self.assertEqual(defaults["ttd_initial_mode"], "baseline")
        self.assertEqual(defaults["ttd_unmatched_policy"], "skip")
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
            item["props"]["model"]: item["props"]
            for item in nodes
            if (item.get("props") or {}).get("model")
        }
        self.assertEqual(fields_by_model["cd2_url"]["show"], "{{ enable_cd2 }}")
        self.assertEqual(fields_by_model["buffer_mode"]["show"], "{{ enable_cd2 }}")
        self.assertEqual(
            fields_by_model["ttd_force_refresh"]["show"],
            "{{ enable_ttd && enable_cd2 }}",
        )

    def test_ttd_only_configuration_does_not_require_cd2_credentials(self) -> None:
        module = load_plugin_module()

        class FakeThread:
            def __init__(self, **_kwargs):
                self.started = False

            def start(self):
                self.started = True

            @staticmethod
            def is_alive():
                return False

        module.threading.Thread = FakeThread
        root = "/光鸭云盘/Media/Video/已整理"
        plugin = module.CloudDrivePlexSync()
        plugin.init_plugin(
            {
                "enabled": True,
                "enable_cd2": False,
                "plex_server": "Plex",
                "watch_roots": root,
                "cloud_plex_path_overrides": f"{root} => /data/CloudNas/Guangya",
                "enable_ttd": True,
                "ttd_url": "https://ttd.example",
                "ttd_cookie": "session=secret",
                "ttd_source": "光鸭云盘",
                "ttd_target_root": root,
            }
        )

        self.assertTrue(plugin.get_state())
        self.assertTrue(plugin._thread.started)
        self.assertFalse(plugin._enable_push)
        self.assertFalse(plugin._enable_transfer_event)
        self.assertFalse(plugin._ttd_force_refresh)
        self.assertEqual(plugin._buffer_settings.mode, "disabled")

    def test_ttd_only_configuration_requires_direct_mapping_for_target_root(self) -> None:
        module = load_plugin_module()
        root = "/光鸭云盘/Media/Video/已整理"
        plugin = module.CloudDrivePlexSync()
        plugin.init_plugin(
            {
                "enabled": True,
                "enable_cd2": False,
                "plex_server": "Plex",
                "watch_roots": root,
                "cloud_plex_path_overrides": "",
                "enable_ttd": True,
                "ttd_url": "https://ttd.example",
                "ttd_cookie": "session=secret",
                "ttd_source": "光鸭云盘",
                "ttd_target_root": root,
            }
        )

        self.assertFalse(plugin.get_state())
        self.assertIn("云端路径 → Plex 路径直接映射", plugin._last_error)

    def test_ttd_only_supervisor_skips_cd2_client_and_topology(self) -> None:
        module = load_plugin_module()

        def unexpected_cd2_client(*_args, **_kwargs):
            raise AssertionError("TTD-only mode must not construct a CD2 client")

        async def run_supervisor():
            root = "/光鸭云盘/Media/Video/已整理"
            plugin = module.CloudDrivePlexSync()
            plugin._enable_cd2 = False
            plugin._enable_push = False
            plugin._enable_ttd = False
            plugin._watch_roots = [root]
            plugin._plex_overrides = []
            plugin._cloud_plex_overrides = module.parse_override_lines(
                f"{root} => /data/CloudNas/Guangya"
            )
            plugin._moviepilot_overrides = []
            plugin._resolve_plex = lambda: object()
            module.CloudDriveClient = unexpected_cd2_client

            task = asyncio.create_task(plugin._supervisor())
            for _ in range(10):
                if plugin._ready.is_set():
                    break
                await asyncio.sleep(0)
            self.assertTrue(plugin._ready.is_set())
            self.assertIsNone(plugin._client)
            self.assertIsNone(plugin._buffer)
            self.assertEqual(plugin._mounts, [])
            self.assertEqual(plugin._cloud_apis, [])
            plugin._stop_event.set()
            await task

        asyncio.run(run_supervisor())

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

    def test_ttd_only_submission_queues_without_cd2_client(self) -> None:
        module = load_plugin_module()
        target = "/光鸭云盘/Media/Video/已整理/电影/片名"

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
            def submit_scan_directory(_path, _source):
                return True

        class Plex:
            @staticmethod
            def find_target(_path):
                return object()

        plugin = module.CloudDrivePlexSync()
        plugin._worker = Worker()
        plugin._plex = Plex()
        plugin._client = None
        plugin._ttd_force_refresh = False

        result = asyncio.run(plugin._submit_ttd_directory(target, "ttd"))

        self.assertEqual(result.state.value, "queued")


if __name__ == "__main__":
    unittest.main()

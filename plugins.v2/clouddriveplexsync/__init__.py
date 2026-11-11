"""MoviePilot V2 plugin: TgToDrive OpenAPI history to Plex directory scans."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Coroutine
from dataclasses import replace

from app.helper.mediaserver import MediaServerHelper
from app.log import logger
from app.plugins import _PluginBase
from pydantic import BaseModel

from .activity_log import read_offline_activity
from .configuration import PluginSettings, build_form
from .control_ui import build_page
from .runtime import SyncRuntime


class ResyncRequest(BaseModel):
    cloud_path: str


class PlexResyncRequest(BaseModel):
    plex_path: str


class CloudDrivePlexSync(_PluginBase):
    """Preserve the installed plugin identity while focusing on TgToDrive and Plex."""

    plugin_name = "TgToDrive Plex 增量同步"
    plugin_desc = "通过 TgToDrive 开放 API、历史对账和持久化队列触发 Plex 目录扫描。"
    plugin_icon = "https://raw.githubusercontent.com/jxxghp/MoviePilot-Plugins/main/icons/refresh2.png"
    plugin_version = "2.2.0"
    plugin_author = "community"
    author_url = "https://github.com"
    plugin_config_prefix = "clouddriveplexsync_"
    plugin_order = 15
    auth_level = 1

    def __init__(self) -> None:
        super().__init__()
        self._enabled = False
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._runtime: SyncRuntime | None = None
        self._settings: PluginSettings | None = None
        self._last_error = ""
        self._stop_requested = threading.Event()

    def init_plugin(self, config: dict[str, object] | None = None) -> None:
        self.stop_service()
        if self._thread and self._thread.is_alive():
            self._set_error("旧后台线程尚未停止，已拒绝重复启动")
            return
        try:
            self._settings = PluginSettings.from_config(config or {})
        except (ValueError, TypeError, OverflowError) as error:
            self._set_error(f"配置无效：{error}")
            return
        self._enabled = self._settings.enabled
        if self._enabled:
            self._last_error = ""
            self._stop_requested.clear()
            self._thread = threading.Thread(
                target=self._thread_main, name="ttd-plex-sync", daemon=True
            )
            self._thread.start()

    def get_state(self) -> bool:
        return self._enabled

    def _resolve_plex(self) -> object:
        if not self._settings:
            raise RuntimeError("插件尚未配置")
        name = self._settings.plex_server
        services = MediaServerHelper().get_services(name_filters=[name])
        if not services or name not in services:
            raise RuntimeError(f"无法获取 MoviePilot Plex 服务：{name}")
        service = services[name]
        if getattr(service, "type", "") != "plex" or service.instance.is_inactive():
            raise RuntimeError(f"所选 Plex 服务尚未连接：{name}")
        return service.instance

    def _thread_main(self) -> None:
        try:
            asyncio.run(self._run())
        except Exception as error:  # noqa: BLE001 - MoviePilot integration failures are reported to the user.
            self._set_error(f"后台服务退出：{error}")
            logger.exception("[CloudDrivePlexSync] 同步后台异常退出")
        finally:
            self._loop = None

    async def _run(self) -> None:
        if not self._settings:
            return
        self._loop = asyncio.get_running_loop()
        service = await asyncio.to_thread(self._resolve_plex)
        if self._stop_requested.is_set():
            return
        self._runtime = SyncRuntime(
            settings=self._settings,
            service=service,
            data_path=self.get_data_path(),
            load=self.get_data,
            save=self.save_data,
            on_error=self._set_error,
            on_info=self._record_info,
            persist_history_enabled=self._persist_history_enabled,
        )
        await self._runtime.run()

    def _persist_history_enabled(self, enabled: bool) -> None:
        config = dict(self.get_config() or {})
        config["ttd_history_enabled"] = enabled
        config.pop("ttd_history_mode", None)
        if self.update_config(config) is False:
            raise OSError("历史开关保存失败，历史处理保持停止，请重试")
        if self._settings:
            self._settings = replace(self._settings, history_enabled=enabled)

    @staticmethod
    def _record_info(message: str) -> None:
        logger.info(f"[CloudDrivePlexSync] {message}")

    def _set_error(self, message: str) -> None:
        self._last_error = message
        logger.warning(f"[CloudDrivePlexSync] {message}")
        if self._settings and self._settings.notify_errors:
            try:
                self.post_message(title=self.plugin_name, text=message)
            except Exception as error:  # noqa: BLE001 - MoviePilot integration failures are reported to the user.
                logger.warning(
                    f"[CloudDrivePlexSync] 通知发送失败：{type(error).__name__}"
                )

    def _submit(self, operation: Coroutine[object, object, object]) -> object:
        if not self._loop or not self._loop.is_running():
            operation.close()
            raise RuntimeError("插件后台尚未就绪")
        future = asyncio.run_coroutine_threadsafe(operation, self._loop)
        try:
            return future.result(timeout=60)
        except TimeoutError:
            future.cancel()
            raise RuntimeError("操作超时，未完成任务仍保留在队列中") from None

    async def _api(self, action: str, path: str = "") -> dict[str, object]:
        runtime = self._runtime
        if not runtime or not runtime.ready.is_set():
            raise RuntimeError(self._last_error or "插件后台尚未就绪")
        if action == "flush":
            return {"success": True, "scans": await runtime.worker.flush(force=True)}
        if action == "resync":
            return {"success": await runtime.worker.submit_scan_directory(path)}
        if action == "resync_plex":
            accepted = await runtime.worker.submit_plex_scan_directory(path)
            return {
                "success": accepted,
                "message": "已加入扫描队列，Plex 索引可能需要一些时间"
                if accepted
                else "扫描队列已满，请稍后重试",
            }
        if action in {"history_start", "history_stop"}:
            state = await runtime.set_history_enabled(action == "history_start")
            return {"success": True, "data": state}
        if action == "reconcile" and runtime.history:
            await runtime.history.reconcile()
            return {"success": True, "message": "已请求分批历史对账"}
        if action == "test" and runtime.poller:
            await runtime.poller.client.fetch_page(1)
            await asyncio.to_thread(runtime.plex.assert_supported)
        return {"success": True, "data": await runtime.status()}

    def _call(self, action: str, path: str = "") -> object:
        try:
            return self._submit(self._api(action, path))
        except (RuntimeError, ValueError, OSError) as error:
            return {"success": False, "message": str(error)}

    def api_status(self) -> object:
        if not self._runtime or not self._runtime.ready.is_set():
            return {
                "success": True,
                "data": {
                    "enabled": self._enabled,
                    "ready": False,
                    "last_error": self._last_error,
                    "history_mode": self._settings.history_mode.value
                    if self._settings
                    else "new_only",
                },
            }
        return self._call("status")

    def api_test(self) -> object:
        return self._call("test")

    def api_flush(self) -> object:
        return self._call("flush")

    def api_resync(self, request: ResyncRequest) -> object:
        return self._call("resync", request.cloud_path)

    def api_resync_plex(self, request: PlexResyncRequest) -> object:
        return self._call("resync_plex", request.plex_path)

    def api_reconcile(self) -> object:
        return self._call("reconcile")

    def api_history_start(self) -> object:
        return self._call("history_start")

    def api_history_stop(self) -> object:
        return self._call("history_stop")

    def get_api(self) -> list[dict[str, object]]:
        endpoints = [
            ("/history/start", self.api_history_start, "POST"),
            ("/history/stop", self.api_history_stop, "POST"),
            ("/status", self.api_status, "GET"),
            ("/test", self.api_test, "POST"),
            ("/flush", self.api_flush, "POST"),
            ("/resync", self.api_resync, "POST"),
            ("/resync/plex", self.api_resync_plex, "POST"),
            ("/reconcile", self.api_reconcile, "POST"),
        ]
        return [
            {"path": path, "endpoint": endpoint, "methods": [method], "auth": "bear"}
            for path, endpoint, method in endpoints
        ]

    @staticmethod
    def get_command() -> list[dict[str, object]]:
        return []

    def get_form(self) -> tuple[list[dict[str, object]], dict[str, object]]:
        servers = [
            {"title": value.name, "value": value.name}
            for value in MediaServerHelper().get_configs().values()
        ]
        form, defaults = build_form(servers)
        if self._settings:
            defaults["ttd_history_enabled"] = self._settings.history_enabled
        return form, defaults

    def get_page(self) -> list[dict[str, object]]:
        response = self.api_status()
        status = response.get("data", {}) if isinstance(response, dict) else {}
        if not isinstance(status, dict):
            status = {}
        if not status.get("ready"):
            try:
                status.update(
                    read_offline_activity(self.get_data_path() / "activity.sqlite3")
                )
            except Exception as error:  # noqa: BLE001 - preserve the status page if storage is unavailable.
                status["activity_error"] = f"读取日志失败：{type(error).__name__}"
        return build_page(
            status, bool(self._settings and self._settings.history_enabled)
        )

    def stop_service(self) -> None:
        self._enabled = False
        self._stop_requested.set()
        if self._loop and self._loop.is_running() and self._runtime:
            self._loop.call_soon_threadsafe(self._runtime.stop.set)
        thread = self._thread
        if thread and thread.is_alive() and thread is not threading.current_thread():
            thread.join(timeout=15)
        if thread and thread.is_alive():
            logger.warning("[CloudDrivePlexSync] 后台线程尚未停止")
            return
        self._thread = None
        self._runtime = None
        self._loop = None

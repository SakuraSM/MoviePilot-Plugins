"""MoviePilot V2 plugin: TgToDrive OpenAPI history to Plex directory scans."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Coroutine

from app.helper.mediaserver import MediaServerHelper
from app.log import logger
from app.plugins import _PluginBase
from pydantic import BaseModel

from .configuration import PluginSettings, build_form
from .runtime import SyncRuntime


class ResyncRequest(BaseModel):
    cloud_path: str


class CloudDrivePlexSync(_PluginBase):
    """Preserve the installed plugin identity while focusing on TgToDrive and Plex."""

    plugin_name = "TgToDrive Plex 增量同步"
    plugin_desc = "通过 TgToDrive 开放 API、历史对账和持久化队列触发 Plex 目录扫描。"
    plugin_icon = "https://raw.githubusercontent.com/jxxghp/MoviePilot-Plugins/main/icons/refresh2.png"
    plugin_version = "2.0.0"
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
        )
        await self._runtime.run()

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
        if action == "reconcile" and runtime.poller:
            runtime.poller.request_reconcile()
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
                },
            }
        return self._call("status")

    def api_test(self) -> object:
        return self._call("test")

    def api_flush(self) -> object:
        return self._call("flush")

    def api_resync(self, request: ResyncRequest) -> object:
        return self._call("resync", request.cloud_path)

    def api_reconcile(self) -> object:
        return self._call("reconcile")

    def get_api(self) -> list[dict[str, object]]:
        return [
            {
                "path": "/status",
                "endpoint": self.api_status,
                "methods": ["GET"],
                "auth": "bear",
            },
            {
                "path": "/test",
                "endpoint": self.api_test,
                "methods": ["POST"],
                "auth": "bear",
            },
            {
                "path": "/flush",
                "endpoint": self.api_flush,
                "methods": ["POST"],
                "auth": "bear",
            },
            {
                "path": "/resync",
                "endpoint": self.api_resync,
                "methods": ["POST"],
                "auth": "bear",
            },
            {
                "path": "/reconcile",
                "endpoint": self.api_reconcile,
                "methods": ["POST"],
                "auth": "bear",
            },
        ]

    @staticmethod
    def get_command() -> list[dict[str, object]]:
        return []

    def get_form(self) -> tuple[list[dict[str, object]], dict[str, object]]:
        servers = [
            {"title": value.name, "value": value.name}
            for value in MediaServerHelper().get_configs().values()
        ]
        return build_form(servers)

    def get_page(self) -> list[dict[str, object]]:
        response = self.api_status()
        status = response.get("data", {}) if isinstance(response, dict) else {}
        if not isinstance(status, dict):
            status = {}
        history = status.get("ttd") or {}
        if not isinstance(history, dict):
            history = {}
        text = (
            f"就绪：{status.get('ready', False)}；待扫描：{status.get('pending_scans', 0)}；"
            f"执行中：{status.get('inflight_scans', 0)}；对账下一页：{history.get('audit_next_page', 0)}；"
            f"历史缺口：{history.get('history_gap', False)}；错误：{status.get('last_error') or '无'}"
        )
        return [
            {"component": "VAlert", "props": {"type": "info", "text": text}},
            {
                "component": "VTextarea",
                "props": {
                    "label": "最近处理记录",
                    "readonly": True,
                    "modelValue": "\n".join(status.get("recent") or []),
                },
            },
        ]

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

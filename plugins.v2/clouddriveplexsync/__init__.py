"""MoviePilot V2 plugin: event-driven CloudDrive2 to Plex partial scans."""

from __future__ import annotations

import asyncio
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel

from app.core.event import Event, eventmanager
from app.helper.mediaserver import MediaServerHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import EventType

from .buffer_manager import BufferManager, BufferSettings, parse_buffer_overrides
from .cd2_client import CloudDriveClient
from .event_worker import EventWorker
from .models import FileSystemChange, MountPoint, PluginStats
from .path_mapper import PathMapper, PathMappingError, parse_override_lines, parse_roots
from .plex_bridge import PlexBridge


class ResyncRequest(BaseModel):
    cloud_path: str


class PreviewBufferRequest(BaseModel):
    cloud_path: str


class CloudDrivePlexSync(_PluginBase):
    """Bridge CloudDrive change pushes into exact Plex directory scans."""

    plugin_name = "CloudDrive Plex 增量同步"
    plugin_desc = "通过 CloudDrive2 变化推送低请求量地触发 Plex 局部扫描。"
    plugin_icon = "https://raw.githubusercontent.com/jxxghp/MoviePilot-Plugins/main/icons/refresh2.png"
    plugin_version = "1.0.0"
    plugin_author = "community"
    author_url = "https://github.com"
    plugin_config_prefix = "clouddriveplexsync_"
    plugin_order = 15
    auth_level = 1

    _enabled = False
    _cd2_url = ""
    _cd2_token = ""
    _verify_tls = True
    _plex_server = ""
    _plex_sections: List[str] = []
    _watch_roots: List[str] = []
    _plex_overrides: List[Tuple[str, str]] = []
    _moviepilot_overrides: List[Tuple[str, str]] = []
    _enable_push = True
    _enable_transfer_event = True
    _notify_errors = True
    _debounce_seconds = 30
    _queue_capacity = 10_000
    _scans_per_second = 1.0

    def __init__(self) -> None:
        super().__init__()
        self._thread: Optional[threading.Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._stop_event: Optional[asyncio.Event] = None
        self._reconnect_event: Optional[asyncio.Event] = None
        self._ready = threading.Event()
        self._client: Optional[CloudDriveClient] = None
        self._plex: Optional[PlexBridge] = None
        self._buffer: Optional[BufferManager] = None
        self._worker: Optional[EventWorker] = None
        self._mounts: List[MountPoint] = []
        self._cloud_apis = []
        self._stats = PluginStats()
        self._status_lock = threading.RLock()
        self._push_connected = False
        self._started_at = 0.0
        self._last_push_at = 0.0
        self._last_disconnect_at = 0.0
        self._last_error = ""
        self._cd2_authenticated = False
        self._cd2_version = ""
        self._recent_push_dirs: Dict[str, float] = {}
        self._buffer_settings = BufferSettings()

    def init_plugin(self, config: dict = None) -> None:
        self.stop_service()
        if self._thread and self._thread.is_alive():
            self._enabled = False
            self._last_error = "旧后台线程未能停止，已拒绝重复启动"
            logger.error(f"[CloudDrivePlexSync] {self._last_error}")
            return
        config = config or {}
        self._enabled = bool(config.get("enabled"))
        self._cd2_url = str(config.get("cd2_url") or "").strip().rstrip("/")
        self._cd2_token = str(config.get("cd2_token") or "").strip()
        self._verify_tls = bool(config.get("verify_tls", True))
        self._plex_server = str(config.get("plex_server") or "").strip()
        sections = config.get("plex_sections") or []
        if isinstance(sections, str):
            sections = [item.strip() for item in sections.replace("\n", ",").split(",")]
        self._plex_sections = [str(item) for item in sections if str(item).strip()]
        self._enable_push = bool(config.get("enable_push", True))
        self._enable_transfer_event = bool(config.get("enable_transfer_event", True))
        self._notify_errors = bool(config.get("notify_errors", True))
        self._debounce_seconds = max(0, int(config.get("debounce_seconds") or 30))
        self._queue_capacity = max(1, int(config.get("queue_capacity") or 10_000))
        self._scans_per_second = max(0.1, float(config.get("scans_per_second") or 1))

        try:
            buffer_min_mb = max(1, int(config.get("buffer_min_mb") or 1))
            self._watch_roots = parse_roots(config.get("watch_roots"))
            self._plex_overrides = parse_override_lines(config.get("plex_path_overrides"))
            self._moviepilot_overrides = parse_override_lines(
                config.get("moviepilot_path_overrides")
            )
            self._buffer_settings = BufferSettings(
                mode=str(config.get("buffer_mode") or "adaptive"),
                min_mb=buffer_min_mb,
                scan_mb=int(config.get("scan_buffer_mb") or 2),
                playback_mb=int(config.get("playback_scan_buffer_mb") or 8),
                apply_before_scan=bool(config.get("buffer_apply_before_scan", True)),
                restore_enabled=bool(config.get("buffer_restore_enabled", True)),
                restore_grace_seconds=int(config.get("buffer_restore_grace_seconds") or 60),
                max_lease_minutes=int(config.get("buffer_max_lease_minutes") or 15),
                fail_open=bool(config.get("buffer_fail_open", True)),
                overrides=parse_buffer_overrides(
                    config.get("buffer_overrides"), min_mb=buffer_min_mb
                ),
            )
        except (TypeError, ValueError, PathMappingError) as exc:
            self._enabled = False
            self._last_error = f"配置无效：{exc}"
            logger.error(f"[CloudDrivePlexSync] {self._last_error}")
            return

        if not self._enabled:
            return
        missing = []
        if not self._cd2_url:
            missing.append("CD2 地址")
        if not self._cd2_token:
            missing.append("CD2 API Token")
        if not self._plex_server:
            missing.append("Plex 服务")
        if not self._watch_roots:
            missing.append("监听根目录")
        if missing:
            self._enabled = False
            self._last_error = "缺少配置：" + "、".join(missing)
            logger.error(f"[CloudDrivePlexSync] {self._last_error}")
            return

        self._ready.clear()
        self._thread = threading.Thread(
            target=self._thread_main,
            name=f"{self.__class__.__name__}-worker",
            daemon=True,
        )
        self._thread.start()

    def get_state(self) -> bool:
        return self._enabled

    def _resolve_plex(self) -> PlexBridge:
        services = MediaServerHelper().get_services(name_filters=[self._plex_server])
        if not services or self._plex_server not in services:
            raise RuntimeError(f"无法获取 MoviePilot Plex 服务：{self._plex_server}")
        service = services[self._plex_server]
        if getattr(service, "type", "") != "plex":
            raise RuntimeError(f"所选媒体服务器不是 Plex：{self._plex_server}")
        if service.instance.is_inactive():
            raise RuntimeError(f"Plex 服务未连接：{self._plex_server}")
        bridge = PlexBridge(service.instance, self._plex_sections, self._stats)
        bridge.assert_supported()
        return bridge

    def _thread_main(self) -> None:
        try:
            asyncio.run(self._supervisor())
        except Exception as exc:
            self._set_error(f"后台服务退出：{exc}")
            logger.exception("[CloudDrivePlexSync] 后台服务异常退出")
        finally:
            self._loop = None
            self._ready.set()

    async def _supervisor(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._stop_event = asyncio.Event()
        self._reconnect_event = asyncio.Event()
        self._started_at = time.time()
        self._stats = PluginStats()
        self._plex = self._resolve_plex()
        self._client = CloudDriveClient(
            self._cd2_url,
            self._cd2_token,
            verify=self._verify_tls,
            stats=self._stats,
        )
        try:
            await self._refresh_topology()
            mapper = self._build_mapper()
            self._buffer = BufferManager(
                self._client,
                self._buffer_settings,
                playback_probe=self._plex.is_playing_under,
                scan_probe=self._plex.is_scanning,
                persist=self._persist_leases,
                stats=self._stats,
            )
            self._buffer.load_leases(self.get_data("buffer_leases") or [])
            await self._buffer.recover_stale()
            self._worker = EventWorker(
                mapper,
                self._plex,
                self._buffer,
                self._cloud_apis,
                debounce_seconds=self._debounce_seconds,
                capacity=self._queue_capacity,
                scans_per_second=self._scans_per_second,
                stats=self._stats,
                persist_queue=self._persist_queue,
                on_error=self._set_error,
            )
            self._worker.load_queue(self.get_data("pending_scans") or [])
            self._ready.set()

            tasks = [
                asyncio.create_task(self._worker.run(self._stop_event), name="cd2plex-events"),
                asyncio.create_task(self._lease_loop(), name="cd2plex-leases"),
            ]
            if self._enable_push:
                tasks.append(asyncio.create_task(self._push_supervisor(), name="cd2plex-push"))
            await self._stop_event.wait()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            self._push_connected = False
            if self._buffer:
                await self._buffer.restore_all()
            if self._client:
                await self._client.close()

    def _build_mapper(self) -> PathMapper:
        return PathMapper(
            watch_roots=self._watch_roots,
            mounts=self._mounts,
            plex_overrides=self._plex_overrides,
            moviepilot_overrides=self._moviepilot_overrides,
        )

    async def _refresh_topology(self) -> None:
        if not self._client:
            return
        info, mounts, apis = await asyncio.gather(
            self._client.get_system_info(),
            self._client.get_mount_points(),
            self._client.get_cloud_apis(),
        )
        if not info.get("ready"):
            raise RuntimeError(info.get("message") or "CloudDrive2 尚未就绪")
        self._cd2_authenticated = bool(info.get("logged_in"))
        self._mounts = mounts
        self._cloud_apis = apis
        if self._worker:
            self._worker.update_topology(self._build_mapper(), apis)

    async def _consume_push_once(self) -> None:
        if not self._client:
            return
        self._push_connected = False
        async for event in self._client.push_events():
            if self._stop_event and self._stop_event.is_set():
                return
            self._push_connected = True
            self._last_push_at = time.time()
            if event.kind == "filesystem" and self._worker:
                change: FileSystemChange = event.value
                self._worker.submit_change(change, "cd2")
                for path in (change.path, change.new_path):
                    if not path:
                        continue
                    try:
                        scan_dir = self._worker.mapper.cloud_scan_directory(path)
                        self._recent_push_dirs[scan_dir] = time.time()
                    except PathMappingError:
                        pass
                cutoff = time.time() - 120
                self._recent_push_dirs = {
                    path: seen for path, seen in self._recent_push_dirs.items() if seen >= cutoff
                }
            elif event.kind in {"mount_changed", "cloud_api_changed"}:
                await self._refresh_topology()
            elif event.kind == "status":
                self._cd2_version = str((event.value or {}).get("version") or self._cd2_version)

    async def _push_supervisor(self) -> None:
        backoff_steps = (1.0, 2.0, 5.0, 10.0, 30.0, 60.0)
        backoff_index = 0
        while self._stop_event and not self._stop_event.is_set():
            consume = asyncio.create_task(self._consume_push_once())
            reconnect = asyncio.create_task(self._reconnect_event.wait())
            try:
                done, pending = await asyncio.wait(
                    {consume, reconnect}, return_when=asyncio.FIRST_COMPLETED
                )
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
                if reconnect in done:
                    self._reconnect_event.clear()
                    consume.cancel()
                    await asyncio.gather(consume, return_exceptions=True)
                    backoff_index = 0
                    continue
                await consume
                raise RuntimeError("CloudDrive2 推送流已关闭")
            except asyncio.CancelledError:
                consume.cancel()
                reconnect.cancel()
                await asyncio.gather(consume, reconnect, return_exceptions=True)
                raise
            except Exception as exc:
                self._push_connected = False
                self._last_disconnect_at = time.time()
                self._stats.reconnects += 1
                self._set_error(f"CD2 推送断开：{exc}")
                try:
                    await asyncio.wait_for(
                        self._reconnect_event.wait(), timeout=backoff_steps[backoff_index]
                    )
                    self._reconnect_event.clear()
                    backoff_index = 0
                except asyncio.TimeoutError:
                    backoff_index = min(backoff_index + 1, len(backoff_steps) - 1)

    async def _lease_loop(self) -> None:
        while self._stop_event and not self._stop_event.is_set():
            if self._buffer and self._buffer.leases:
                await self._buffer.restore_due()
            await asyncio.sleep(2)

    async def _handle_transfer_path(self, moviepilot_path: str) -> None:
        if not self._worker or not self._client:
            return
        try:
            cloud_path = self._worker.mapper.moviepilot_to_cloud(moviepilot_path)
            scan_dir = self._worker.mapper.cloud_scan_directory(cloud_path)
            await asyncio.sleep(10)
            pushed_at = self._recent_push_dirs.get(scan_dir, 0)
            if time.time() - pushed_at > 20:
                await self._client.force_list(scan_dir)
            self._worker.submit_cloud_path(cloud_path, "moviepilot")
        except Exception as exc:
            self._set_error(f"MoviePilot 入库事件处理失败：{exc}")

    @eventmanager.register(EventType.TransferComplete)
    def on_transfer_complete(self, event: Event) -> None:
        if not self._enabled or not self._enable_transfer_event or not self._loop:
            return
        data = event.event_data or {}
        transfer = data.get("transferinfo")
        item = getattr(transfer, "target_diritem", None)
        path = getattr(item, "path", None)
        if not path:
            return
        asyncio.run_coroutine_threadsafe(self._handle_transfer_path(str(path)), self._loop)

    def _persist_leases(self, values: List[dict]) -> None:
        self.save_data("buffer_leases", values)

    def _persist_queue(self, values: List[dict]) -> None:
        self.save_data("pending_scans", values)

    def _set_error(self, message: str) -> None:
        with self._status_lock:
            self._last_error = str(message)
        logger.warning(f"[CloudDrivePlexSync] {message}")
        if self._notify_errors:
            try:
                self.post_message(title="CloudDrive Plex 增量同步", text=str(message))
            except Exception:
                pass

    def _submit(self, coroutine: Any, timeout: float = 30.0) -> Any:
        if not self._loop or not self._loop.is_running():
            raise RuntimeError("插件后台服务尚未运行")
        if not asyncio.iscoroutine(coroutine):
            raise RuntimeError("插件后台组件尚未就绪")
        return asyncio.run_coroutine_threadsafe(coroutine, self._loop).result(timeout=timeout)

    def _status_data(self) -> Dict[str, Any]:
        worker = self._worker
        buffer_manager = self._buffer
        cloud_status = [
            {
                "name": item.name,
                "username": item.username,
                "nickname": item.nickname,
                "path": item.path,
                "event_listener_running": item.event_listener_running,
                "read_only": item.read_only,
            }
            for item in self._cloud_apis
        ]
        return {
            "enabled": self._enabled,
            "ready": self._ready.is_set() and bool(worker),
            "push_connected": self._push_connected,
            "cd2_authenticated": self._cd2_authenticated,
            "cd2_version": self._cd2_version,
            "started_at": self._started_at,
            "last_push_at": self._last_push_at,
            "last_disconnect_at": self._last_disconnect_at,
            "last_scan_at": worker.last_scan_at if worker else 0,
            "last_error": self._last_error or (worker.last_error if worker else ""),
            "pending_scans": len(worker.coalescer.pending) if worker else 0,
            "buffer_mode": self._buffer_settings.mode,
            "buffer_leases": [item.to_dict() for item in buffer_manager.leases.values()]
            if buffer_manager
            else [],
            "cloud_apis": cloud_status,
            "mounts": [item.__dict__ for item in self._mounts],
            "recent": list(worker.recent) if worker else [],
            "stats": self._stats.to_dict(),
            "plex": self._plex.describe() if self._plex else {},
        }

    def api_status(self) -> Dict[str, Any]:
        return {"success": True, "data": self._status_data()}

    def api_test(self) -> Dict[str, Any]:
        if not self._ready.wait(timeout=10):
            return {"success": False, "message": self._last_error or "插件尚未就绪"}
        if not self._worker or not self._plex:
            return {"success": False, "message": self._last_error or "插件启动失败"}
        try:
            self._plex.assert_supported()
            return {"success": True, "data": self._status_data()}
        except Exception as exc:
            return {"success": False, "message": str(exc)}

    async def _api_resync_async(self, cloud_path: str) -> Dict[str, Any]:
        if not self._worker or not self._client:
            raise RuntimeError("插件尚未就绪")
        path = self._worker.mapper.validate_cloud_path(cloud_path)
        await self._client.force_list(path)
        queued = self._worker.submit_scan_directory(path, "manual")
        return {"success": queued, "data": {"cloud_path": path}}

    def api_resync(self, request: ResyncRequest) -> Dict[str, Any]:
        try:
            return self._submit(self._api_resync_async(request.cloud_path), timeout=60)
        except Exception as exc:
            return {"success": False, "message": str(exc)}

    def api_flush(self) -> Dict[str, Any]:
        try:
            count = self._submit(self._worker.flush(force=True) if self._worker else None)
            return {"success": True, "data": {"scans": count}}
        except Exception as exc:
            return {"success": False, "message": str(exc)}

    def api_reconnect(self) -> Dict[str, Any]:
        if not self._loop or not self._reconnect_event:
            return {"success": False, "message": "插件尚未就绪"}
        self._loop.call_soon_threadsafe(self._reconnect_event.set)
        return {"success": True, "message": "已请求重连"}

    def api_restore_buffer(self) -> Dict[str, Any]:
        try:
            restored = self._submit(self._buffer.restore_all() if self._buffer else None)
            return {"success": True, "data": {"restored": restored}}
        except Exception as exc:
            return {"success": False, "message": str(exc)}

    async def _preview_buffer_async(self, cloud_path: str) -> Dict[str, Any]:
        if not self._worker or not self._buffer:
            raise RuntimeError("插件尚未就绪")
        path = self._worker.mapper.validate_cloud_path(cloud_path)
        cloud = next(
            (
                item
                for item in sorted(self._cloud_apis, key=lambda value: len(value.path), reverse=True)
                if item.path and (path == item.path or path.startswith(item.path.rstrip("/") + "/"))
            ),
            None,
        )
        if not cloud:
            raise RuntimeError(f"找不到路径所属网盘：{path}")
        plex_path = self._worker.mapper.cloud_to_plex(path)
        return await self._buffer.preview(cloud, [plex_path])

    def api_preview_buffer(self, request: PreviewBufferRequest) -> Dict[str, Any]:
        try:
            data = self._submit(self._preview_buffer_async(request.cloud_path))
            return {"success": True, "data": data}
        except Exception as exc:
            return {"success": False, "message": str(exc)}

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {"path": "/status", "endpoint": self.api_status, "methods": ["GET"], "auth": "bear"},
            {"path": "/test", "endpoint": self.api_test, "methods": ["POST"], "auth": "bear"},
            {"path": "/flush", "endpoint": self.api_flush, "methods": ["POST"], "auth": "bear"},
            {"path": "/resync", "endpoint": self.api_resync, "methods": ["POST"], "auth": "bear"},
            {"path": "/reconnect", "endpoint": self.api_reconnect, "methods": ["POST"], "auth": "bear"},
            {
                "path": "/restore-buffer",
                "endpoint": self.api_restore_buffer,
                "methods": ["POST"],
                "auth": "bear",
            },
            {
                "path": "/preview-buffer",
                "endpoint": self.api_preview_buffer,
                "methods": ["POST"],
                "auth": "bear",
            },
        ]

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def _media_server_names(self) -> List[Dict[str, str]]:
        try:
            return [
                {"title": config.name, "value": config.name}
                for config in MediaServerHelper().get_configs().values()
            ]
        except Exception:
            return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        fields = [
            ("VSwitch", "enabled", "启用插件", None),
            ("VTextField", "cd2_url", "CloudDrive2 地址", "http://NAS_IP:19798"),
            ("VTextField", "cd2_token", "CloudDrive2 API Token", None),
            ("VSelect", "plex_server", "Plex 服务", None),
            ("VTextField", "plex_sections", "允许的 Plex 媒体库 ID（逗号分隔）", None),
            ("VTextarea", "watch_roots", "CD2 监听根目录（每行一个）", "/光鸭云盘/Media/Video/已整理"),
            ("VTextarea", "plex_path_overrides", "Plex 路径映射", "/CloudNAS/Guangya => /data/CloudNas/Guangya"),
            ("VTextarea", "moviepilot_path_overrides", "MoviePilot 路径映射（可选）", None),
            ("VTextField", "debounce_seconds", "防抖秒数", "30"),
            ("VSelect", "buffer_mode", "Buffer 联动模式", None),
            ("VTextField", "buffer_min_mb", "Buffer 最小值（MB）", "1"),
            ("VTextField", "scan_buffer_mb", "空闲扫描 Buffer（MB）", "2"),
            ("VTextField", "playback_scan_buffer_mb", "播放期间扫描 Buffer（MB）", "8"),
            ("VTextarea", "buffer_overrides", "网盘级 Buffer 覆盖", "光鸭云盘|2|8"),
            ("VTextField", "buffer_restore_grace_seconds", "扫描结束恢复延迟（秒）", "60"),
            ("VTextField", "buffer_max_lease_minutes", "最大 Buffer 租约（分钟）", "15"),
            ("VSwitch", "buffer_apply_before_scan", "扫描前应用 Buffer", None),
            ("VSwitch", "buffer_restore_enabled", "扫描后恢复 Buffer", None),
            ("VSwitch", "buffer_fail_open", "Buffer 修改失败时仍执行扫描", None),
            ("VSwitch", "enable_push", "启用 CD2 推送", None),
            ("VSwitch", "enable_transfer_event", "启用 MoviePilot 入库事件", None),
            ("VSwitch", "verify_tls", "验证 HTTPS 证书", None),
            ("VSwitch", "notify_errors", "错误通知", None),
        ]
        content = []
        for component, model, label, placeholder in fields:
            props: Dict[str, Any] = {"model": model, "label": label}
            if placeholder:
                props["placeholder"] = placeholder
            if model == "cd2_token":
                props["type"] = "password"
            if model == "plex_server":
                props["items"] = self._media_server_names()
            if model == "buffer_mode":
                props["items"] = [
                    {"title": "禁用", "value": "disabled"},
                    {"title": "固定值", "value": "fixed"},
                    {"title": "按播放状态自适应", "value": "adaptive"},
                ]
            if model in {
                "scan_buffer_mb",
                "buffer_min_mb",
                "buffer_overrides",
                "buffer_restore_grace_seconds",
                "buffer_max_lease_minutes",
                "buffer_apply_before_scan",
                "buffer_restore_enabled",
                "buffer_fail_open",
            }:
                props["show"] = "{{ buffer_mode !== 'disabled' }}"
            if model == "playback_scan_buffer_mb":
                props["show"] = "{{ buffer_mode === 'adaptive' }}"
            if model in {
                "debounce_seconds",
                "buffer_min_mb",
                "scan_buffer_mb",
                "playback_scan_buffer_mb",
                "buffer_restore_grace_seconds",
                "buffer_max_lease_minutes",
            }:
                props["type"] = "number"
                props["min"] = 1
            content.append(
                {"component": "VRow", "content": [{"component": "VCol", "props": {"cols": 12}, "content": [{"component": component, "props": props}]}]}
            )
        defaults = {
            "enabled": False,
            "cd2_url": "http://127.0.0.1:19798",
            "cd2_token": "",
            "verify_tls": True,
            "plex_server": "",
            "plex_sections": "",
            "watch_roots": "/光鸭云盘/Media/Video/已整理",
            "plex_path_overrides": "/CloudNAS/Guangya => /data/CloudNas/Guangya",
            "moviepilot_path_overrides": "",
            "debounce_seconds": 30,
            "buffer_mode": "adaptive",
            "buffer_min_mb": 1,
            "scan_buffer_mb": 2,
            "playback_scan_buffer_mb": 8,
            "buffer_overrides": "",
            "buffer_apply_before_scan": True,
            "buffer_restore_enabled": True,
            "buffer_restore_grace_seconds": 60,
            "buffer_max_lease_minutes": 15,
            "buffer_fail_open": True,
            "enable_push": True,
            "enable_transfer_event": True,
            "notify_errors": True,
        }
        return [{"component": "VForm", "content": content}], defaults

    def get_page(self) -> List[dict]:
        status = self._status_data()
        return [
            {
                "component": "VAlert",
                "props": {
                    "type": "success" if status["push_connected"] else "warning",
                    "variant": "tonal",
                    "text": (
                        f"CD2推送：{'已连接' if status['push_connected'] else '未连接'}；"
                        f"待扫描：{status['pending_scans']}；Buffer模式：{status['buffer_mode']}"
                    ),
                },
            },
            {
                "component": "VTextarea",
                "props": {
                    "modelValue": "\n".join(status.get("recent") or ["暂无处理记录"]),
                    "label": "最近处理记录",
                    "readonly": True,
                    "rows": 12,
                },
            },
        ]

    def stop_service(self) -> None:
        loop = self._loop
        stop_event = self._stop_event
        reconnect_event = self._reconnect_event
        if loop and loop.is_running():
            if stop_event:
                loop.call_soon_threadsafe(stop_event.set)
            if reconnect_event:
                loop.call_soon_threadsafe(reconnect_event.set)
        thread = self._thread
        if thread and thread.is_alive() and thread is not threading.current_thread():
            thread.join(timeout=15)
        if thread and thread.is_alive():
            logger.error("[CloudDrivePlexSync] 后台线程在 15 秒内未停止")
            self._thread = thread
            return
        self._thread = None
        self._loop = None
        self._stop_event = None
        self._reconnect_event = None
        self._push_connected = False
        self._cd2_authenticated = False
        self._ready.clear()
        self._enabled = False
        self._worker = None
        self._buffer = None
        self._client = None
        self._plex = None

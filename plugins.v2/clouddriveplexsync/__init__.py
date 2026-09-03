"""MoviePilot V2 plugin: event-driven CloudDrive2 to Plex partial scans."""

from __future__ import annotations

import asyncio
import threading
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlsplit

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
from .path_mapper import (
    PathMapper,
    PathMappingError,
    is_under,
    normalize_path,
    parse_override_lines,
    parse_roots,
)
from .plex_bridge import PlexBridge, PlexLibraryNotFoundError
from .ttd_client import (
    TTDClient,
    TTDCursor,
    TTDHistoryPoller,
    TTDSubmitResult,
    parse_ttd_skip_paths,
)


class ResyncRequest(BaseModel):
    cloud_path: str


class PreviewBufferRequest(BaseModel):
    cloud_path: str


class CloudDrivePlexSync(_PluginBase):
    """Bridge CloudDrive change pushes into exact Plex directory scans."""

    plugin_name = "CloudDrive Plex 增量同步"
    plugin_desc = "通过 CloudDrive2 推送或 TgToDrive 整理历史触发 Plex 局部扫描。"
    plugin_icon = "https://raw.githubusercontent.com/jxxghp/MoviePilot-Plugins/main/icons/refresh2.png"
    plugin_version = "1.3.1"
    plugin_author = "community"
    author_url = "https://github.com"
    plugin_config_prefix = "clouddriveplexsync_"
    plugin_order = 15
    auth_level = 1

    _enabled = False
    _enable_cd2 = True
    _cd2_url = ""
    _cd2_token = ""
    _verify_tls = True
    _plex_server = ""
    _plex_sections: List[str] = []
    _watch_roots: List[str] = []
    _plex_overrides: List[Tuple[str, str]] = []
    _cloud_plex_overrides: List[Tuple[str, str]] = []
    _moviepilot_overrides: List[Tuple[str, str]] = []
    _enable_push = True
    _enable_transfer_event = True
    _enable_ttd = False
    _ttd_url = ""
    _ttd_cookie = ""
    _ttd_source = "光鸭云盘"
    _ttd_target_root = ""
    _ttd_poll_seconds = 30
    _ttd_page_size = 20
    _ttd_max_pages = 5
    _ttd_initial_mode = "baseline"
    _ttd_force_refresh = True
    _ttd_skip_paths: List[str] = []
    _ttd_unmatched_policy = "skip"
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
        self._ttd_client: Optional[TTDClient] = None
        self._ttd_poller: Optional[TTDHistoryPoller] = None
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
        self._recent_ttd_dirs: Dict[str, float] = {}
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
        self._enable_cd2 = bool(config.get("enable_cd2", True))
        self._cd2_url = str(config.get("cd2_url") or "").strip().rstrip("/")
        self._cd2_token = str(config.get("cd2_token") or "").strip()
        self._verify_tls = bool(config.get("verify_tls", True))
        self._plex_server = str(config.get("plex_server") or "").strip()
        sections = config.get("plex_sections") or []
        if isinstance(sections, str):
            sections = [item.strip() for item in sections.replace("\n", ",").split(",")]
        self._plex_sections = [str(item) for item in sections if str(item).strip()]
        self._enable_push = self._enable_cd2 and bool(config.get("enable_push", True))
        self._enable_transfer_event = self._enable_cd2 and bool(
            config.get("enable_transfer_event", True)
        )
        self._enable_ttd = bool(config.get("enable_ttd", False))
        self._ttd_url = str(config.get("ttd_url") or "").strip().rstrip("/")
        self._ttd_cookie = str(config.get("ttd_cookie") or "").strip()
        self._ttd_source = str(config.get("ttd_source") or "光鸭云盘").strip()
        self._ttd_poll_seconds = max(10, int(config.get("ttd_poll_seconds") or 30))
        self._ttd_page_size = max(1, min(100, int(config.get("ttd_page_size") or 20)))
        self._ttd_max_pages = max(1, min(20, int(config.get("ttd_max_pages") or 5)))
        self._ttd_initial_mode = str(config.get("ttd_initial_mode") or "baseline").strip()
        self._ttd_force_refresh = self._enable_cd2 and bool(
            config.get("ttd_force_refresh", True)
        )
        self._ttd_unmatched_policy = str(
            config.get("ttd_unmatched_policy") or "skip"
        ).strip().lower()
        self._notify_errors = bool(config.get("notify_errors", True))
        self._debounce_seconds = max(0, int(config.get("debounce_seconds") or 30))
        self._queue_capacity = max(1, int(config.get("queue_capacity") or 10_000))
        self._scans_per_second = max(0.1, float(config.get("scans_per_second") or 1))

        try:
            self._watch_roots = parse_roots(config.get("watch_roots"))
            self._plex_overrides = (
                parse_override_lines(config.get("plex_path_overrides"))
                if self._enable_cd2
                else []
            )
            self._cloud_plex_overrides = parse_override_lines(
                config.get("cloud_plex_path_overrides")
            )
            self._moviepilot_overrides = (
                parse_override_lines(config.get("moviepilot_path_overrides"))
                if self._enable_cd2
                else []
            )
            configured_ttd_root = str(config.get("ttd_target_root") or "").strip()
            self._ttd_target_root = normalize_path(
                configured_ttd_root or (self._watch_roots[0] if self._watch_roots else "/")
            )
            self._ttd_skip_paths = parse_ttd_skip_paths(
                config.get("ttd_skip_paths"), self._ttd_target_root
            )
            if self._watch_roots and not any(
                is_under(self._ttd_target_root, root) for root in self._watch_roots
            ):
                raise PathMappingError("TTD 目标根目录必须位于某个监听根目录内")
            if self._ttd_initial_mode not in {"baseline", "replay_latest"}:
                raise ValueError("TTD 首次运行模式只能是 baseline 或 replay_latest")
            if self._ttd_unmatched_policy not in {"skip", "retry"}:
                raise ValueError("TTD 未匹配 Plex 路径策略只能是 skip 或 retry")
            if self._enable_ttd and self._ttd_url:
                parsed_ttd_url = urlsplit(self._ttd_url)
                if parsed_ttd_url.scheme not in {"http", "https"} or not parsed_ttd_url.netloc:
                    raise ValueError("TTD 地址必须是完整的 http:// 或 https:// URL")
            if "\r" in self._ttd_cookie or "\n" in self._ttd_cookie:
                raise ValueError("TTD Cookie 不能包含换行")
            if self._enable_cd2:
                buffer_min_mb = max(1, int(config.get("buffer_min_mb") or 1))
                self._buffer_settings = BufferSettings(
                    mode=str(config.get("buffer_mode") or "adaptive"),
                    min_mb=buffer_min_mb,
                    scan_mb=int(config.get("scan_buffer_mb") or 2),
                    playback_mb=int(config.get("playback_scan_buffer_mb") or 8),
                    apply_before_scan=bool(config.get("buffer_apply_before_scan", True)),
                    restore_enabled=bool(config.get("buffer_restore_enabled", True)),
                    restore_grace_seconds=int(
                        config.get("buffer_restore_grace_seconds") or 60
                    ),
                    max_lease_minutes=int(config.get("buffer_max_lease_minutes") or 15),
                    fail_open=bool(config.get("buffer_fail_open", True)),
                    overrides=parse_buffer_overrides(
                        config.get("buffer_overrides"), min_mb=buffer_min_mb
                    ),
                )
            else:
                self._buffer_settings = BufferSettings(mode="disabled")
        except (TypeError, ValueError, PathMappingError) as exc:
            self._enabled = False
            self._last_error = f"配置无效：{exc}"
            logger.error(f"[CloudDrivePlexSync] {self._last_error}")
            return

        if not self._enabled:
            return
        missing = []
        if self._enable_cd2 and not self._cd2_url:
            missing.append("CD2 地址")
        if self._enable_cd2 and not self._cd2_token:
            missing.append("CD2 API Token")
        if not self._plex_server:
            missing.append("Plex 服务")
        if not self._watch_roots:
            missing.append("监听根目录")
        if self._enable_ttd and not self._ttd_url:
            missing.append("TTD 地址")
        if self._enable_ttd and not self._ttd_cookie:
            missing.append("TTD Cookie")
        if self._enable_ttd and not self._ttd_source:
            missing.append("TTD 来源筛选")
        if not self._enable_cd2 and not self._enable_ttd:
            missing.append("TTD 整理历史轮询")
        if not self._enable_cd2 and not any(
            is_under(self._ttd_target_root, source)
            for source, _ in self._cloud_plex_overrides
        ):
            missing.append("覆盖 TTD 目标根目录的云端路径 → Plex 路径直接映射")
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
        try:
            if self._enable_cd2:
                self._client = CloudDriveClient(
                    self._cd2_url,
                    self._cd2_token,
                    verify=self._verify_tls,
                    stats=self._stats,
                )
                await self._refresh_topology()
            else:
                self._client = None
                self._mounts = []
                self._cloud_apis = []
                self._cd2_authenticated = False
                self._cd2_version = ""
            mapper = self._build_mapper()
            if self._client:
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
            else:
                self._buffer = None
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
            if self._enable_ttd:
                self._ttd_client = TTDClient(
                    self._ttd_url,
                    self._ttd_cookie,
                    self._ttd_source,
                    page_size=self._ttd_page_size,
                    verify=self._verify_tls,
                    stats=self._stats,
                )
                self._ttd_poller = TTDHistoryPoller(
                    self._ttd_client,
                    self._submit_ttd_directory,
                    self._ttd_target_root,
                    poll_seconds=self._ttd_poll_seconds,
                    max_pages=self._ttd_max_pages,
                    initial_mode=self._ttd_initial_mode,
                    cursor=TTDCursor.from_dict(self.get_data("ttd_cursor")),
                    persist_cursor=self._persist_ttd_cursor,
                    on_error=self._set_error,
                    on_skip=self._record_info,
                    stats=self._stats,
                )
            self._ready.set()

            tasks = [
                asyncio.create_task(self._worker.run(self._stop_event), name="cd2plex-events")
            ]
            if self._buffer:
                tasks.append(asyncio.create_task(self._lease_loop(), name="cd2plex-leases"))
            if self._enable_push:
                tasks.append(asyncio.create_task(self._push_supervisor(), name="cd2plex-push"))
            if self._ttd_poller:
                tasks.append(
                    asyncio.create_task(self._ttd_poller.run(self._stop_event), name="cd2plex-ttd")
                )
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
            if self._ttd_client:
                await self._ttd_client.close()

    def _build_mapper(self) -> PathMapper:
        return PathMapper(
            watch_roots=self._watch_roots,
            mounts=self._mounts,
            plex_overrides=self._plex_overrides,
            cloud_plex_overrides=self._cloud_plex_overrides,
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
                for path in (change.path, change.new_path):
                    if not path:
                        continue
                    try:
                        scan_dir = self._worker.mapper.cloud_scan_directory(path)
                        self._submit_cd2_scan_directory(scan_dir)
                    except PathMappingError:
                        pass
                cutoff = time.time() - 120
                self._recent_push_dirs = {
                    path: seen for path, seen in self._recent_push_dirs.items() if seen >= cutoff
                }
                self._recent_ttd_dirs = {
                    path: seen for path, seen in self._recent_ttd_dirs.items() if seen >= cutoff
                }
            elif event.kind in {"mount_changed", "cloud_api_changed"}:
                await self._refresh_topology()
            elif event.kind == "status":
                self._cd2_version = str((event.value or {}).get("version") or self._cd2_version)

    def _submit_cd2_scan_directory(self, scan_dir: str) -> bool:
        """Queue a CD2 event and remember it only after successful acceptance."""

        if not self._worker:
            return False
        ttd_at = self._recent_ttd_dirs.get(scan_dir, 0)
        if time.time() - ttd_at <= 120:
            if self._worker.has_pending_cloud_directory(scan_dir):
                return self._worker.submit_scan_directory(scan_dir, "cd2")
            return True
        queued = self._worker.submit_scan_directory(scan_dir, "cd2")
        if queued:
            self._recent_push_dirs[scan_dir] = time.time()
        return queued

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

    async def _submit_ttd_directory(
        self, cloud_directory: str, source: str
    ) -> TTDSubmitResult:
        """Validate one TTD directory, optionally refresh it, then enqueue its Plex path."""

        if not self._worker or not self._plex:
            return TTDSubmitResult.retry("插件后台组件尚未就绪")
        try:
            path = self._worker.mapper.validate_cloud_path(cloud_directory)
            skipped_prefix = next(
                (item for item in self._ttd_skip_paths if is_under(path, item)), None
            )
            if skipped_prefix:
                return TTDSubmitResult.skipped(f"命中配置的跳过路径 {skipped_prefix}")
            plex_path = self._worker.mapper.cloud_to_plex(path)
            self._plex.find_target(plex_path)
        except PlexLibraryNotFoundError as exc:
            if self._ttd_unmatched_policy == "skip":
                return TTDSubmitResult.skipped(str(exc))
            return TTDSubmitResult.retry(str(exc))
        except PathMappingError as exc:
            return TTDSubmitResult.retry(str(exc))
        except Exception as exc:
            return TTDSubmitResult.retry(f"Plex 路径检查失败：{exc}")
        pushed_at = self._recent_push_dirs.get(path, 0)
        if time.time() - pushed_at <= 120:
            if self._worker.has_pending_cloud_directory(path):
                if self._worker.submit_scan_directory(path, source):
                    return TTDSubmitResult.queued("已与 CD2 推送任务合并")
                return TTDSubmitResult.retry("扫描队列暂未接受目标目录")
            return TTDSubmitResult.queued("近期 CD2 推送已覆盖该目录")
        if self._ttd_force_refresh:
            if not self._client:
                return TTDSubmitResult.retry("CD2 未启用，无法刷新目标目录")
            try:
                await self._client.force_list(path)
            except Exception as exc:
                # Plex may still observe the path through FUSE. Keep the trigger fail-open.
                self._set_error(f"TTD 目标目录刷新失败，仍提交局部扫描：{path}：{exc}")
        queued = self._worker.submit_scan_directory(path, source)
        if queued:
            self._recent_ttd_dirs[path] = time.time()
            return TTDSubmitResult.queued()
        return TTDSubmitResult.retry("扫描队列暂未接受目标目录")

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

    def _persist_ttd_cursor(self, value: Dict[str, Any]) -> None:
        self.save_data("ttd_cursor", value)

    def _set_error(self, message: str) -> None:
        with self._status_lock:
            self._last_error = str(message)
        logger.warning(f"[CloudDrivePlexSync] {message}")
        if self._notify_errors:
            try:
                self.post_message(title="CloudDrive Plex 增量同步", text=str(message))
            except Exception:
                pass

    def _record_info(self, message: str) -> None:
        logger.info(f"[CloudDrivePlexSync] {message}")
        worker = self._worker
        if worker:
            worker.recent.append(f"SKIP {message}")

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
            "cd2_enabled": self._enable_cd2,
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
            "ttd": self._ttd_poller.status.to_dict()
            if self._ttd_poller
            else {"enabled": self._enable_ttd, "connected": False, "authenticated": False},
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
        if not self._worker:
            raise RuntimeError("插件尚未就绪")
        path = self._worker.mapper.validate_cloud_path(cloud_path)
        refreshed = False
        if self._client:
            await self._client.force_list(path)
            refreshed = True
        queued = self._worker.submit_scan_directory(path, "manual")
        return {
            "success": queued,
            "data": {"cloud_path": path, "cd2_refreshed": refreshed},
        }

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
        if self._ttd_poller:
            self._loop.call_soon_threadsafe(self._ttd_poller.wake)
        return {"success": True, "message": "已请求重连"}

    def api_restore_buffer(self) -> Dict[str, Any]:
        if not self._enable_cd2:
            return {"success": False, "message": "CD2 联动未启用"}
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
        if not self._enable_cd2:
            return {"success": False, "message": "CD2 联动未启用"}
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
        def field(
            component: str,
            model: str,
            label: str,
            placeholder: Optional[str] = None,
            *,
            hint: Optional[str] = None,
            cols: int = 12,
            md: int = 12,
            lg: int = 6,
        ) -> Dict[str, Any]:
            props: Dict[str, Any] = {
                "model": model,
                "label": label,
                "density": "comfortable",
                "hide-details": "auto",
            }
            if component in {"VTextField", "VTextarea", "VSelect"}:
                props["variant"] = "outlined"
            if component == "VTextarea":
                props["auto-grow"] = True
                props["rows"] = 2
            if component == "VSwitch":
                props["color"] = "primary"
                props["inset"] = True
                props["hide-details"] = True
            if placeholder:
                props["placeholder"] = placeholder
            if hint:
                props["hint"] = hint
                props["persistent-hint"] = True
            if model in {"cd2_token", "ttd_cookie"}:
                props["type"] = "password"
            if model in {"cd2_url", "cd2_token"}:
                props["show"] = "{{ enable_cd2 }}"
            if model == "plex_server":
                props["items"] = self._media_server_names()
            if model == "buffer_mode":
                props["items"] = [
                    {"title": "禁用", "value": "disabled"},
                    {"title": "固定值", "value": "fixed"},
                    {"title": "按播放状态自适应", "value": "adaptive"},
                ]
            if model == "ttd_initial_mode":
                props["items"] = [
                    {"title": "仅建立基线，不处理历史", "value": "baseline"},
                    {"title": "处理当前最新记录", "value": "replay_latest"},
                ]
            if model == "ttd_unmatched_policy":
                props["items"] = [
                    {"title": "跳过并推进游标（推荐）", "value": "skip"},
                    {"title": "保留记录并持续重试", "value": "retry"},
                ]
            if model.startswith("ttd_") and model != "enable_ttd":
                props["show"] = "{{ enable_ttd }}"
            if model == "ttd_force_refresh":
                props["show"] = "{{ enable_ttd && enable_cd2 }}"
            if model in {
                "enable_push",
                "enable_transfer_event",
                "plex_path_overrides",
                "moviepilot_path_overrides",
                "buffer_mode",
            }:
                props["show"] = "{{ enable_cd2 }}"
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
                props["show"] = "{{ enable_cd2 && buffer_mode !== 'disabled' }}"
            if model == "playback_scan_buffer_mb":
                props["show"] = "{{ enable_cd2 && buffer_mode === 'adaptive' }}"
            if model in {
                "debounce_seconds",
                "buffer_min_mb",
                "scan_buffer_mb",
                "playback_scan_buffer_mb",
                "buffer_restore_grace_seconds",
                "buffer_max_lease_minutes",
                "ttd_poll_seconds",
                "ttd_page_size",
                "ttd_max_pages",
                "queue_capacity",
                "scans_per_second",
            }:
                props["type"] = "number"
                props["min"] = 1
            if model == "ttd_poll_seconds":
                props["min"] = 10
            if model == "scans_per_second":
                props["min"] = 0.1
                props["step"] = 0.1
            return {
                "component": "VCol",
                "props": {
                    "cols": cols,
                    "md": md,
                    "lg": lg,
                    "class": "px-2 py-1",
                },
                "content": [{"component": component, "props": props}],
            }

        def grid(*items: Dict[str, Any]) -> List[Dict[str, Any]]:
            return [
                {
                    "component": "VRow",
                    "props": {"dense": True, "class": "ma-n1"},
                    "content": list(items),
                }
            ]

        def section(
            title: str,
            description: str,
            *items: Dict[str, Any],
        ) -> Dict[str, Any]:
            return {
                "component": "VCard",
                "props": {"variant": "outlined", "class": "mb-4 rounded-lg"},
                "content": [
                    {
                        "component": "VCardTitle",
                        "props": {"class": "text-subtitle-1 font-weight-medium pb-1"},
                        "text": title,
                    },
                    {
                        "component": "VCardSubtitle",
                        "props": {"class": "text-wrap text-medium-emphasis pb-2"},
                        "text": description,
                    },
                    {
                        "component": "VCardText",
                        "props": {"class": "pt-2 px-3 pb-3"},
                        "content": grid(*items),
                    },
                ],
            }

        basic = [
            section(
                "CloudDrive2 连接",
                "仅在需要 CD2 推送、目录刷新、入库事件或 Buffer 联动时启用。",
                field("VSwitch", "enable_cd2", "启用 CloudDrive2 联动", lg=12),
                field("VTextField", "cd2_url", "CloudDrive2 地址", "http://NAS_IP:19798"),
                field(
                    "VTextField",
                    "cd2_token",
                    "CloudDrive2 API Token",
                    hint="需要 Push Messages、Get Mounts、List Files、Get/Modify Cloud APIs 权限。",
                ),
            ),
            section(
                "Plex 媒体库",
                "插件只对选定媒体库提交精确目录扫描。",
                field("VSelect", "plex_server", "Plex 服务"),
                field(
                    "VTextField",
                    "plex_sections",
                    "允许的 Plex 媒体库 ID（逗号分隔）",
                    hint="路径不匹配时绝不退化为整库扫描。",
                ),
            ),
        ]
        paths = [
            {
                "component": "VAlert",
                "props": {
                    "type": "info",
                    "variant": "tonal",
                    "class": "mb-4 rounded-lg",
                    "text": (
                        "监听根目录填写云端逻辑路径；Plex 路径只填写在路径映射中。"
                        "关闭 CD2 联动时，必须配置覆盖 TTD 目标根目录的直接映射。"
                    ),
                },
            },
            section(
                "监听范围与直接映射",
                "直接把云端逻辑路径映射到 Plex 容器内路径；TTD-only 模式必须配置。",
                field(
                    "VTextarea",
                    "watch_roots",
                    "云端逻辑路径监听根目录（每行一个）",
                    "/光鸭云盘/Media/Video/已整理",
                    lg=12,
                ),
                field(
                    "VTextarea",
                    "cloud_plex_path_overrides",
                    "云端逻辑路径 → Plex 路径（推荐）",
                    "/光鸭云盘/Media/Video/已整理 => /data/CloudNas/Guangya",
                    hint="优先使用，不依赖 CD2 挂载目录结构；按最长路径前缀匹配。",
                    lg=12,
                ),
            ),
            section(
                "兼容路径映射",
                "仅在直接映射未命中或需要处理 MoviePilot 本地路径时使用。",
                field(
                    "VTextarea",
                    "plex_path_overrides",
                    "CD2 挂载路径 → Plex 路径（兼容）",
                    "/CloudNAS/Guangya => /data/CloudNas/Guangya",
                    hint="仅在云端直连映射未命中时，通过 CD2 MountPoint 推导后使用。",
                    lg=12,
                ),
                field(
                    "VTextarea",
                    "moviepilot_path_overrides",
                    "MoviePilot 路径映射（可选）",
                    lg=12,
                ),
            ),
            section(
                "事件触发",
                "控制事件来源和同目录变更的合并等待时间。",
                field("VTextField", "debounce_seconds", "防抖秒数", "30", lg=4),
                field("VSwitch", "enable_push", "启用 CD2 推送", lg=4),
                field(
                    "VSwitch", "enable_transfer_event", "启用 MoviePilot 入库事件", lg=4
                ),
            ),
        ]
        ttd = [
            {
                "component": "VAlert",
                "props": {
                    "type": "info",
                    "variant": "tonal",
                    "class": "mb-4 rounded-lg",
                    "text": "目标根目录填写 TTD 返回路径所属的云端逻辑根，例如 /光鸭云盘/Media/Video/已整理。",
                },
            },
            section(
                "连接与范围",
                "通过整理历史增量获取完成记录，不递归轮询网盘目录。",
                field("VSwitch", "enable_ttd", "启用 TgToDrive 整理历史轮询", lg=12),
                field("VTextField", "ttd_url", "TgToDrive 地址", "https://ttd.example.com"),
                field("VTextField", "ttd_cookie", "TgToDrive 登录 Cookie", "session=..."),
                field("VTextField", "ttd_source", "TgToDrive 来源筛选", "光鸭云盘"),
                field(
                    "VTextField",
                    "ttd_target_root",
                    "TgToDrive 目标根目录（云端逻辑路径）",
                    "/光鸭云盘/Media/Video/已整理",
                ),
            ),
            section(
                "过滤与异常策略",
                "跳过不应进入 Plex 的目录，并决定路径不匹配时是否阻塞游标。",
                field(
                    "VTextarea",
                    "ttd_skip_paths",
                    "TgToDrive 跳过路径（每行一个）",
                    "待整理-通用\n_整理中",
                    hint="支持相对目标根目录或绝对 CD2 路径；命中后只记录一次并推进游标。",
                    lg=12,
                ),
                field(
                    "VSelect",
                    "ttd_unmatched_policy",
                    "不属于所选 Plex 媒体库时",
                    hint="推荐跳过，避免永久不匹配的记录阻塞后续入库。",
                ),
                field("VSelect", "ttd_initial_mode", "TgToDrive 首次运行"),
            ),
            section(
                "轮询与刷新",
                "设置请求频率、单次补页上限和扫描前的精确目录刷新。",
                field("VTextField", "ttd_poll_seconds", "轮询间隔（秒）", "30", lg=4),
                field("VTextField", "ttd_page_size", "每页记录数", "20", lg=4),
                field("VTextField", "ttd_max_pages", "最大补页数", "5", lg=4),
                field(
                    "VSwitch", "ttd_force_refresh", "扫描前刷新准确的 CD2 目标目录", lg=12
                ),
            ),
        ]
        buffer = [
            section(
                "联动模式",
                "可完全禁用，或在扫描期间临时调整后安全恢复。",
                field("VSelect", "buffer_mode", "Buffer 联动模式", lg=12),
            ),
            section(
                "扫描 Buffer",
                "自适应模式会根据 Plex 播放状态选择空闲值或播放值。",
                field("VTextField", "buffer_min_mb", "Buffer 最小值（MB）", "1", lg=4),
                field("VTextField", "scan_buffer_mb", "空闲扫描 Buffer（MB）", "2", lg=4),
                field(
                    "VTextField",
                    "playback_scan_buffer_mb",
                    "播放期间扫描 Buffer（MB）",
                    "8",
                    lg=4,
                ),
                field(
                    "VTextarea",
                    "buffer_overrides",
                    "网盘级 Buffer 覆盖",
                    "光鸭云盘|2|8",
                    lg=12,
                ),
            ),
            section(
                "租约与恢复",
                "限制临时配置的持有时间，并控制修改失败时是否继续扫描。",
                field(
                    "VTextField",
                    "buffer_restore_grace_seconds",
                    "扫描结束恢复延迟（秒）",
                    "60",
                ),
                field(
                    "VTextField",
                    "buffer_max_lease_minutes",
                    "最大 Buffer 租约（分钟）",
                    "15",
                ),
                field("VSwitch", "buffer_apply_before_scan", "扫描前应用 Buffer", lg=4),
                field("VSwitch", "buffer_restore_enabled", "扫描后恢复 Buffer", lg=4),
                field("VSwitch", "buffer_fail_open", "修改失败仍执行扫描", lg=4),
            ),
        ]
        advanced = [
            section(
                "安全与通知",
                "生产环境建议保持证书验证，并开启错误通知。",
                field("VSwitch", "verify_tls", "验证 HTTPS 证书"),
                field("VSwitch", "notify_errors", "发送错误通知"),
            ),
            section(
                "扫描调度",
                "限制积压目录数量和 Plex 局部扫描的提交速率。",
                field("VTextField", "queue_capacity", "扫描队列上限", "10000"),
                field("VTextField", "scans_per_second", "每秒最多提交目录数", "1"),
            ),
        ]

        tabs = [
            ("basic_tab", "基础", basic),
            ("paths_tab", "路径与触发", paths),
            ("ttd_tab", "TgToDrive", ttd),
            ("buffer_tab", "Buffer", buffer),
            ("advanced_tab", "高级", advanced),
        ]
        content: List[Dict[str, Any]] = [
            {
                "component": "VCard",
                "props": {"variant": "tonal", "class": "mb-4 rounded-lg px-2"},
                "content": [
                    {
                        "component": "VRow",
                        "props": {"align": "center", "class": "ma-0"},
                        "content": [field("VSwitch", "enabled", "启用插件", lg=12)],
                    }
                ],
            },
            {
                "component": "VTabs",
                "props": {
                    "model": "_tabs",
                    "align-tabs": "start",
                    "color": "primary",
                    "density": "comfortable",
                    "show-arrows": True,
                    "class": "mb-4 rounded-lg border",
                },
                "content": [
                    {"component": "VTab", "props": {"value": value}, "text": title}
                    for value, title, _ in tabs
                ],
            },
            {
                "component": "VWindow",
                "props": {"model": "_tabs", "class": "overflow-visible"},
                "content": [
                    {
                        "component": "VWindowItem",
                        "props": {"value": value, "class": "px-1"},
                        "content": items,
                    }
                    for value, _, items in tabs
                ],
            },
        ]
        defaults = {
            "enabled": False,
            "enable_cd2": True,
            "cd2_url": "http://127.0.0.1:19798",
            "cd2_token": "",
            "verify_tls": True,
            "plex_server": "",
            "plex_sections": "",
            "watch_roots": "/光鸭云盘/Media/Video/已整理",
            "cloud_plex_path_overrides": (
                "/光鸭云盘/Media/Video/已整理 => /data/CloudNas/Guangya"
            ),
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
            "enable_ttd": False,
            "ttd_url": "",
            "ttd_cookie": "",
            "ttd_source": "光鸭云盘",
            "ttd_target_root": "",
            "ttd_poll_seconds": 30,
            "ttd_page_size": 20,
            "ttd_max_pages": 5,
            "ttd_initial_mode": "baseline",
            "ttd_force_refresh": True,
            "ttd_skip_paths": "",
            "ttd_unmatched_policy": "skip",
            "notify_errors": True,
            "queue_capacity": 10_000,
            "scans_per_second": 1,
        }
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VContainer",
                        "props": {"fluid": True, "class": "pa-0"},
                        "content": content,
                    }
                ],
            }
        ], defaults

    def get_page(self) -> List[dict]:
        status = self._status_data()
        ttd = status.get("ttd") or {}
        healthy = bool(status["push_connected"] or ttd.get("authenticated"))
        if not status["cd2_enabled"]:
            cd2_text = "未启用"
        else:
            cd2_text = "已连接" if status["push_connected"] else "未连接"
        return [
            {
                "component": "VAlert",
                "props": {
                    "type": "success" if healthy else "warning",
                    "variant": "tonal",
                    "text": (
                        f"CD2推送：{cd2_text}；"
                        f"TTD：{'已认证' if ttd.get('authenticated') else ('未启用' if not ttd.get('enabled') else '未连接')}；"
                        f"TTD跳过：{int(ttd.get('skipped_records') or 0)}；"
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
        self._ttd_client = None
        self._ttd_poller = None
        self._recent_push_dirs = {}
        self._recent_ttd_dirs = {}
        self._plex = None

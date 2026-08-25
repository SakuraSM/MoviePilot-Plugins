"""Cookie-authenticated TgToDrive organize-history polling."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import posixpath
import re
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence
from urllib.parse import urlsplit, urlunsplit

try:
    import httpx
except ImportError:  # Allows parser-only unit tests outside MoviePilot.
    httpx = None  # type: ignore[assignment]

from .models import PluginStats
from .path_mapper import PathMappingError, is_under, normalize_path


class TTDClientError(RuntimeError):
    """Base class for safe-to-display TgToDrive client failures."""


class TTDAuthenticationError(TTDClientError):
    """The configured TgToDrive web session is no longer authenticated."""


class TTDProtocolError(TTDClientError):
    """TgToDrive returned an unsupported response."""


class TTDHistoryGapError(TTDClientError):
    """The saved cursor was not found within the configured page budget."""


@dataclass(frozen=True)
class TTDHistoryRecord:
    """Normalized subset of one organize-history entry."""

    key: str
    target_path: str
    status: str = "success"
    completed_at: str = ""
    source: str = ""
    record_id: str = ""


@dataclass(frozen=True)
class TTDHistoryPage:
    """Parsed records plus the server-side item count used for pagination."""

    records: List[TTDHistoryRecord]
    item_count: int


@dataclass
class TTDCursor:
    """Small persistent cursor resilient to non-monotonic record IDs."""

    initialized: bool = False
    scope: str = ""
    seen_keys: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: object) -> "TTDCursor":
        if not isinstance(value, dict):
            return cls()
        keys = value.get("seen_keys") or []
        if not isinstance(keys, (list, tuple)):
            keys = []
        return cls(
            initialized=bool(value.get("initialized")),
            scope=str(value.get("scope") or ""),
            seen_keys=[str(item) for item in keys if str(item)],
        )


@dataclass
class TTDPollStatus:
    """Runtime state exposed without credentials."""

    enabled: bool = True
    connected: bool = False
    authenticated: bool = False
    cursor_initialized: bool = False
    seen_records: int = 0
    last_poll_at: float = 0
    last_success_at: float = 0
    last_record_at: float = 0
    next_poll_at: float = 0
    last_error: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


_ID_FIELDS = (
    "id",
    "history_id",
    "historyId",
    "record_id",
    "recordId",
    "organize_id",
    "organizeId",
    "task_id",
    "taskId",
)
_TARGET_FIELDS = (
    "target_path",
    "targetPath",
    "target",
    "destination_path",
    "destinationPath",
    "dest_path",
    "destPath",
    "organized_path",
    "organizedPath",
    "output_path",
    "outputPath",
    "new_path",
    "newPath",
    "target_dir",
    "targetDir",
    "target_folder",
    "targetFolder",
    "target_directory",
    "destination",
)
_TIME_FIELDS = (
    "completed_at",
    "completedAt",
    "finished_at",
    "finishedAt",
    "updated_at",
    "updatedAt",
    "created_at",
    "createdAt",
    "time",
    "timestamp",
)
_SOURCE_FIELDS = (
    "source",
    "cloud",
    "cloud_name",
    "cloudName",
    "cloud_type",
    "cloudType",
    "provider",
)
_STATUS_FIELDS = ("status", "state")
_NESTED_FIELDS = ("data", "detail", "record", "history", "result")


def _first_value(value: Dict[str, Any], names: Sequence[str]) -> Any:
    """Return the first populated direct or one-level nested field."""

    for name in names:
        candidate = value.get(name)
        if candidate not in (None, "", "-"):
            return candidate
    for nested_name in _NESTED_FIELDS:
        nested = value.get(nested_name)
        if not isinstance(nested, dict):
            continue
        for name in names:
            candidate = nested.get(name)
            if candidate not in (None, "", "-"):
                return candidate
    return None


def _path_value(value: Any) -> str:
    if isinstance(value, dict):
        value = _first_value(value, ("path", "full_path", "name", "value"))
    if isinstance(value, (list, tuple)):
        value = "/".join(str(item).strip(" /") for item in value if str(item).strip(" /"))
    text = str(value or "").strip()
    if not text or text == "-":
        return ""
    # TgToDrive's UI renders relative target components with spaced slashes.
    return re.sub(r"\s*/\s*", "/", text)


def _record_key(value: Dict[str, Any], target: str, completed_at: str) -> tuple[str, str]:
    record_id = str(_first_value(value, _ID_FIELDS) or "").strip()
    if record_id:
        return f"id:{record_id}", record_id
    stable = {
        "target": target,
        "completed_at": completed_at,
        "source": str(_first_value(value, _SOURCE_FIELDS) or ""),
        "status": str(_first_value(value, _STATUS_FIELDS) or ""),
        "name": str(_first_value(value, ("name", "file_name", "filename", "title")) or ""),
    }
    digest = hashlib.sha256(
        json.dumps(stable, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()
    return f"sha256:{digest}", ""


def _find_items(value: Any) -> Optional[List[Dict[str, Any]]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    if not isinstance(value, dict):
        return None
    for name in ("items", "records", "list", "history", "histories", "results"):
        candidate = value.get(name)
        if isinstance(candidate, list):
            return [item for item in candidate if isinstance(item, dict)]
    for name in ("data", "result", "history"):
        candidate = _find_items(value.get(name))
        if candidate is not None:
            return candidate
    return None


def parse_history_payload(payload: Any) -> List[TTDHistoryRecord]:
    """Parse known TgToDrive response shapes without depending on one release."""

    if isinstance(payload, dict) and payload.get("success") is False:
        raise TTDProtocolError(str(payload.get("error") or payload.get("message") or "请求失败"))
    items = _find_items(payload)
    if items is None:
        raise TTDProtocolError("整理历史响应中没有记录列表")

    records: List[TTDHistoryRecord] = []
    for item in items:
        target = _path_value(_first_value(item, _TARGET_FIELDS))
        if not target:
            continue
        status = str(_first_value(item, _STATUS_FIELDS) or "success").strip().lower()
        if status not in {"success", "succeeded", "ok", "completed", "complete"}:
            continue
        completed_at = str(_first_value(item, _TIME_FIELDS) or "").strip()
        key, record_id = _record_key(item, target, completed_at)
        records.append(
            TTDHistoryRecord(
                key=key,
                record_id=record_id,
                target_path=target,
                status=status,
                completed_at=completed_at,
                source=str(_first_value(item, _SOURCE_FIELDS) or "").strip(),
            )
        )
    return records


class TTDClient:
    """Minimal async client for the Cookie-protected organize-history API."""

    def __init__(
        self,
        base_url: str,
        cookie: str,
        source: str,
        *,
        page_size: int = 20,
        verify: bool = True,
        timeout: float = 10.0,
        stats: Optional[PluginStats] = None,
        client: Optional[Any] = None,
    ) -> None:
        if httpx is None and client is None:
            raise RuntimeError("httpx is required at runtime")
        clean_cookie = str(cookie or "").strip()
        if clean_cookie.lower().startswith("cookie:"):
            clean_cookie = clean_cookie.split(":", 1)[1].strip()
        if not clean_cookie or "\r" in clean_cookie or "\n" in clean_cookie:
            raise ValueError("TTD Cookie 为空或包含非法换行")
        raw_url = str(base_url or "").strip()
        parsed_url = urlsplit(raw_url)
        if parsed_url.scheme not in {"http", "https"} or not parsed_url.netloc:
            raise ValueError("TTD 地址必须是完整的 http:// 或 https:// URL")
        clean_path = parsed_url.path.rstrip("/")
        if clean_path.endswith("/api/organize-history"):
            self.endpoint_url = urlunsplit(
                (parsed_url.scheme, parsed_url.netloc, clean_path, "", "")
            )
            self.base_url = urlunsplit(
                (
                    parsed_url.scheme,
                    parsed_url.netloc,
                    clean_path[: -len("/api/organize-history")],
                    "",
                    "",
                )
            ).rstrip("/")
        else:
            self.base_url = urlunsplit(
                (parsed_url.scheme, parsed_url.netloc, clean_path, "", "")
            ).rstrip("/")
            self.endpoint_url = f"{self.base_url}/api/organize-history"
        self.cookie = clean_cookie
        self.source = str(source or "").strip()
        self.page_size = max(1, min(100, int(page_size)))
        self.timeout = max(1.0, float(timeout))
        self.stats = stats
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(verify=verify, follow_redirects=False)

    async def fetch_page(self, page: int) -> TTDHistoryPage:
        if self.stats:
            self.stats.ttd_poll_requests += 1
        response = await self._client.get(
            self.endpoint_url,
            params={
                "source": self.source,
                "page": max(1, int(page)),
                "limit": self.page_size,
                "status": "success",
            },
            headers={"accept": "application/json", "cookie": self.cookie},
            timeout=self.timeout,
        )
        if response.status_code in {301, 302, 303, 307, 308, 401, 403}:
            if self.stats:
                self.stats.ttd_auth_failures += 1
            raise TTDAuthenticationError("TTD Cookie 已失效或无权读取整理历史")
        try:
            response.raise_for_status()
        except Exception as exc:
            raise TTDClientError(f"TTD 整理历史请求失败：HTTP {response.status_code}") from exc
        content_type = str(response.headers.get("content-type") or "").lower()
        if "json" not in content_type:
            raise TTDProtocolError("TTD 返回了非 JSON 响应，可能已跳转到登录页")
        try:
            payload = response.json()
            if isinstance(payload, dict) and payload.get("success") is False:
                message = str(payload.get("error") or payload.get("message") or "")
                if "未登录" in message or "unauth" in message.lower():
                    if self.stats:
                        self.stats.ttd_auth_failures += 1
                    raise TTDAuthenticationError("TTD Cookie 已失效或无权读取整理历史")
            raw_items = _find_items(payload)
            records = parse_history_payload(payload)
            item_count = len(raw_items or [])
            if item_count != len(records):
                raise TTDProtocolError(
                    f"TTD 返回 {item_count} 条成功记录，但只有 {len(records)} 条含可识别目标路径"
                )
            return TTDHistoryPage(records=records, item_count=item_count)
        except TTDClientError:
            raise
        except Exception as exc:
            raise TTDProtocolError("无法解析 TTD 整理历史响应") from exc

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()


class TTDHistoryPoller:
    """Incrementally consume newest-first history with a bounded page budget."""

    def __init__(
        self,
        client: TTDClient,
        submit_directory: Callable[[str, str], Any],
        target_root: str,
        *,
        poll_seconds: int = 30,
        max_pages: int = 5,
        initial_mode: str = "baseline",
        cursor: Optional[TTDCursor] = None,
        persist_cursor: Optional[Callable[[Dict[str, Any]], None]] = None,
        on_error: Optional[Callable[[str], None]] = None,
        stats: Optional[PluginStats] = None,
        clock: Callable[[], float] = time.time,
        seen_limit: int = 1000,
    ) -> None:
        mode = str(initial_mode or "baseline").strip().lower()
        if mode not in {"baseline", "replay_latest"}:
            raise ValueError("TTD 首次运行模式只能是 baseline 或 replay_latest")
        self.client = client
        self.submit_directory = submit_directory
        self.target_root = normalize_path(target_root)
        self.poll_seconds = max(10, int(poll_seconds))
        self.max_pages = max(1, min(20, int(max_pages)))
        self.initial_mode = mode
        scope_value = f"{client.base_url}|{client.source}|{self.target_root}"
        self.cursor_scope = hashlib.sha256(scope_value.encode("utf-8")).hexdigest()
        self.cursor = cursor or TTDCursor()
        if self.cursor.scope and self.cursor.scope != self.cursor_scope:
            self.cursor = TTDCursor(scope=self.cursor_scope)
        else:
            self.cursor.scope = self.cursor_scope
        self.persist_cursor = persist_cursor
        self.on_error = on_error
        self.stats = stats
        self.clock = clock
        self.seen_limit = max(100, int(seen_limit))
        self.status = TTDPollStatus(
            cursor_initialized=self.cursor.initialized,
            seen_records=len(self.cursor.seen_keys),
        )
        self._wake_event = asyncio.Event()

    def wake(self) -> None:
        self._wake_event.set()

    def _save_cursor(self) -> None:
        self.cursor.seen_keys = list(dict.fromkeys(self.cursor.seen_keys))[: self.seen_limit]
        self.status.cursor_initialized = self.cursor.initialized
        self.status.seen_records = len(self.cursor.seen_keys)
        if self.persist_cursor:
            self.persist_cursor(self.cursor.to_dict())

    def _remember(self, keys: Iterable[str]) -> None:
        incoming = [str(item) for item in keys if str(item)]
        self.cursor.seen_keys = list(dict.fromkeys(incoming + self.cursor.seen_keys))[
            : self.seen_limit
        ]

    def _cloud_directory(self, record: TTDHistoryRecord) -> str:
        target = record.target_path.strip()
        if target.startswith("/"):
            cloud_path = normalize_path(target)
        else:
            cloud_path = normalize_path(posixpath.join(self.target_root, target))
        if not is_under(cloud_path, self.target_root):
            raise PathMappingError(f"TTD 目标路径越出配置根目录：{cloud_path}")
        return cloud_path

    async def _fetch_new_records(self) -> List[TTDHistoryRecord]:
        if not self.cursor.initialized:
            first_page = await self.client.fetch_page(1)
            return list(first_page.records)
        known = set(self.cursor.seen_keys)
        unseen: List[TTDHistoryRecord] = []
        unseen_keys = set()
        found_cursor = False
        exhausted = False
        for page in range(1, self.max_pages + 1):
            page_result = await self.client.fetch_page(page)
            records = page_result.records
            if not records:
                exhausted = True
                break
            for record in records:
                if record.key in known:
                    found_cursor = True
                    break
                if record.key not in unseen_keys:
                    unseen.append(record)
                    unseen_keys.add(record.key)
            if found_cursor:
                break
            if page_result.item_count < self.client.page_size:
                exhausted = True
                break
        if self.cursor.initialized and known and not found_cursor and not exhausted:
            raise TTDHistoryGapError(
                f"连续 {self.max_pages} 页未找到已保存游标；已停止推进，"
                "请增大 TTD 最大补页数或手动确认历史缺口"
            )
        return unseen

    async def poll_once(self) -> int:
        self.status.last_poll_at = self.clock()
        records = await self._fetch_new_records()
        self.status.connected = True
        self.status.authenticated = True
        self.status.last_success_at = self.clock()
        self.status.last_error = ""

        if not self.cursor.initialized and self.initial_mode == "baseline":
            self.cursor.initialized = True
            self._remember(record.key for record in records)
            self._save_cursor()
            return 0

        processed: List[str] = []
        submitted_directories: Dict[str, bool] = {}
        queued = 0
        # API is newest-first; enqueue oldest-first so Plex sees moves in order.
        for record in reversed(records):
            try:
                cloud_directory = self._cloud_directory(record)
                accepted = submitted_directories.get(cloud_directory)
                is_new_directory = accepted is None
                if accepted is None:
                    result = self.submit_directory(cloud_directory, "ttd")
                    if inspect.isawaitable(result):
                        result = await result
                    accepted = bool(result)
                    submitted_directories[cloud_directory] = accepted
                if not accepted:
                    break
                if is_new_directory:
                    queued += 1
                processed.append(record.key)
                self.status.last_record_at = self.clock()
            except Exception as exc:
                if self.on_error:
                    self.on_error(f"TTD 整理记录处理失败：{exc}")
                break
        if processed or not self.cursor.initialized:
            self.cursor.initialized = True
            self._remember(reversed(processed))
            self._save_cursor()
        if self.stats:
            self.stats.ttd_records += len(processed)
        return queued

    async def _wait(self, stop_event: asyncio.Event, delay: float) -> None:
        self.status.next_poll_at = self.clock() + delay
        stop_task = asyncio.create_task(stop_event.wait())
        wake_task = asyncio.create_task(self._wake_event.wait())
        try:
            await asyncio.wait_for(
                asyncio.wait(
                    {stop_task, wake_task},
                    return_when=asyncio.FIRST_COMPLETED,
                ),
                timeout=delay,
            )
        except asyncio.TimeoutError:
            pass
        finally:
            stop_task.cancel()
            wake_task.cancel()
            await asyncio.gather(stop_task, wake_task, return_exceptions=True)
            self._wake_event.clear()

    async def run(self, stop_event: asyncio.Event) -> None:
        transient_steps = (5.0, 15.0, 30.0, 60.0)
        auth_steps = (300.0, 900.0, 1800.0, 3600.0)
        transient_index = 0
        auth_index = 0
        while not stop_event.is_set():
            delay = float(self.poll_seconds)
            try:
                await self.poll_once()
                transient_index = 0
                auth_index = 0
            except asyncio.CancelledError:
                raise
            except TTDAuthenticationError as exc:
                should_report = self.status.last_error != str(exc)
                self.status.connected = True
                self.status.authenticated = False
                self.status.last_error = str(exc)
                delay = auth_steps[auth_index]
                auth_index = min(auth_index + 1, len(auth_steps) - 1)
                if should_report and self.on_error:
                    self.on_error(str(exc))
            except (TTDHistoryGapError, TTDProtocolError) as exc:
                should_report = self.status.last_error != str(exc)
                self.status.connected = True
                self.status.authenticated = True
                self.status.last_error = str(exc)
                if self.stats:
                    self.stats.ttd_errors += 1
                delay = max(float(self.poll_seconds), 60.0)
                if should_report and self.on_error:
                    self.on_error(f"TTD 轮询暂停推进：{exc}")
            except Exception as exc:
                should_report = self.status.last_error != str(exc)
                self.status.connected = False
                self.status.authenticated = False
                self.status.last_error = str(exc)
                if self.stats:
                    self.stats.ttd_errors += 1
                delay = transient_steps[transient_index]
                transient_index = min(transient_index + 1, len(transient_steps) - 1)
                if should_report and self.on_error:
                    self.on_error(f"TTD 轮询失败：{exc}")
            await self._wait(stop_event, delay)
        self.status.next_poll_at = 0

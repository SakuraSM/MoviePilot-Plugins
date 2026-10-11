"""TgToDrive OpenAPI client."""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlsplit, urlunsplit

try:
    import httpx
except ImportError:
    httpx = None  # type: ignore[assignment]

from .history_models import (
    TTDAuthenticationError,
    TTDClientError,
    TTDCursor,
    TTDHistoryGapError,
    TTDHistoryPage,
    TTDHistoryRecord,
    TTDProtocolError,
    TTDSubmitResult,
)
from .history_parser import _find_items, parse_history_payload, parse_ttd_skip_paths
from .history_poller import TTDHistoryPoller
from .models import PluginStats


def normalize_ttd_api_token(value: str) -> str:
    """Accept a raw token or a pasted Bearer header without exposing its value."""

    if "\r" in value or "\n" in value:
        raise ValueError("TTD API Token 不能包含换行")
    token = value.strip()
    if not token:
        return ""
    if token.lower().startswith("authorization:"):
        token = token.split(":", 1)[1].strip()
    scheme, _, credential = token.partition(" ")
    if scheme.lower() == "bearer":
        token = credential.strip()
    if not re.fullmatch(r"[A-Za-z0-9._~+/\-]+=*", token):
        raise ValueError("TTD API Token 格式无效，请填写 Token 或 Bearer 请求头值")
    return token


class TTDClient:
    """Read successful organize history using an OpenAPI Bearer token."""

    def __init__(
        self,
        base_url: str,
        source: str = "",
        *,
        api_token: str = "",
        page_size: int = 20,
        verify: bool = True,
        timeout: float = 10.0,
        stats: PluginStats | None = None,
        client: Any | None = None,
    ) -> None:
        if httpx is None and client is None:
            raise RuntimeError("httpx is required at runtime")
        self.api_token = normalize_ttd_api_token(api_token)
        if not self.api_token:
            raise ValueError("请配置 TTD OpenAPI Token；旧 Cookie 已不再支持")
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
        self.source = str(source or "").strip()
        self.page_size = max(1, min(100, int(page_size)))
        self.timeout = max(1.0, float(timeout))
        self.stats = stats
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            verify=verify, follow_redirects=False
        )

    async def fetch_page(self, page: int) -> TTDHistoryPage:
        if self.stats:
            self.stats.ttd_poll_requests += 1
        headers = {
            "accept": "application/json",
            "authorization": f"Bearer {self.api_token}",
        }
        response = await self._client.get(
            self.endpoint_url,
            params={
                "source": self.source,
                "page": max(1, int(page)),
                "limit": self.page_size,
                "status": "success",
            },
            headers=headers,
            timeout=self.timeout,
        )
        if response.status_code in {301, 302, 303, 307, 308, 401, 403}:
            raise self._authentication_error()
        try:
            response.raise_for_status()
        except Exception as exc:
            raise TTDClientError(
                f"TTD 整理历史请求失败：HTTP {response.status_code}"
            ) from exc
        content_type = str(response.headers.get("content-type") or "").lower()
        if "json" not in content_type:
            raise TTDProtocolError("TTD 返回了非 JSON 响应，可能已跳转到登录页")
        try:
            payload = response.json()
            if isinstance(payload, dict) and payload.get("success") is False:
                message = str(payload.get("error") or payload.get("message") or "")
                if "未登录" in message or "unauth" in message.lower():
                    raise self._authentication_error()
            source_data = (
                payload.get("source_data") if isinstance(payload, dict) else None
            )
            if isinstance(source_data, dict) and source_data.get("error"):
                raise TTDProtocolError(
                    f"TTD 整理历史数据源错误：{source_data['error']}"
                )
            raw_items = _find_items(payload)
            records = parse_history_payload(payload)
            item_count = len(raw_items or [])
            if item_count != len(records):
                raise TTDProtocolError(
                    f"TTD 返回 {item_count} 条成功记录，但只有 {len(records)} 条含可识别目标路径"
                )
            raw_has_next = (
                payload.get("has_next") if isinstance(payload, dict) else None
            )
            return TTDHistoryPage(
                records=records,
                item_count=item_count,
                has_next=raw_has_next if isinstance(raw_has_next, bool) else None,
            )
        except TTDClientError:
            raise
        except Exception as exc:
            raise TTDProtocolError("无法解析 TTD 整理历史响应") from exc

    def _authentication_error(self) -> TTDAuthenticationError:
        if self.stats:
            self.stats.ttd_auth_failures += 1
        return TTDAuthenticationError("TTD API Token 已失效或无权读取整理历史")

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()


__all__ = [
    "TTDAuthenticationError",
    "TTDClient",
    "TTDClientError",
    "TTDCursor",
    "TTDHistoryGapError",
    "TTDHistoryPage",
    "TTDHistoryPoller",
    "TTDHistoryRecord",
    "TTDProtocolError",
    "TTDSubmitResult",
    "normalize_ttd_api_token",
    "parse_history_payload",
    "parse_ttd_skip_paths",
]

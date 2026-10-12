"""Configuration for the TgToDrive OpenAPI to Plex pipeline."""

from __future__ import annotations

import math
from dataclasses import dataclass
from urllib.parse import urlsplit

from .history_models import TTDHistoryMode
from .history_parser import parse_ttd_skip_paths
from .path_mapper import PathMapper, normalize_path, parse_override_lines
from .ttd_client import normalize_ttd_api_token

DEFAULT_ROOT = "/光鸭云盘/Media/Video/已整理"
DEFAULTS: dict[str, object] = {
    "enabled": False,
    "ttd_url": "",
    "ttd_api_token": "",
    "ttd_source": "光鸭云盘",
    "ttd_target_root": DEFAULT_ROOT,
    "cloud_plex_path_overrides": f"{DEFAULT_ROOT} => /data/CloudNas/Guangya",
    "plex_server": "",
    "plex_sections": "",
    "ttd_skip_paths": "",
    "ttd_unmatched_policy": "retry",
    "ttd_history_mode": "new_only",
    "ttd_poll_seconds": 30,
    "ttd_page_size": 100,
    "ttd_max_pages": 5,
    "ttd_overlap_pages": 2,
    "ttd_reconcile_hours": 6,
    "debounce_seconds": 30,
    "queue_capacity": 10000,
    "scans_per_second": 1,
    "verify_tls": True,
    "notify_errors": True,
}


def number(config: dict[str, object], key: str) -> float:
    value = config.get(key, DEFAULTS[key])
    if value is None or value == "":
        value = DEFAULTS[key]
    if not isinstance(value, (str, int, float)):
        raise TypeError(f"{key} 必须是数字")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{key} 必须是有限数字")
    return result


@dataclass(frozen=True)
class PluginSettings:
    enabled: bool
    url: str
    api_token: str
    source: str
    mapper: PathMapper
    plex_server: str
    plex_sections: list[str]
    skip_paths: list[str]
    unmatched_policy: str
    history_mode: TTDHistoryMode
    poll_seconds: int
    page_size: int
    max_pages: int
    overlap_pages: int
    reconcile_hours: int
    debounce_seconds: int
    queue_capacity: int
    scans_per_second: float
    verify_tls: bool
    notify_errors: bool

    @classmethod
    def from_config(cls, config: dict[str, object]) -> PluginSettings:
        configured_roots = config.get("watch_roots") or ""
        legacy_roots = (
            configured_roots
            if isinstance(configured_roots, list)
            else str(configured_roots).splitlines()
        )
        root = normalize_path(
            str(config.get("ttd_target_root") or next(iter(legacy_roots), DEFAULT_ROOT))
        )
        mappings = parse_override_lines(
            config.get(
                "cloud_plex_path_overrides", DEFAULTS["cloud_plex_path_overrides"]
            )
        )
        sections = config.get("plex_sections") or []
        if isinstance(sections, str):
            sections = sections.replace("\n", ",").split(",")
        if not isinstance(sections, list):
            raise TypeError("Plex 媒体库必须是列表或逗号分隔的 ID")
        settings = cls(
            enabled=bool(config.get("enabled")),
            url=str(config.get("ttd_url") or "").strip(),
            api_token=normalize_ttd_api_token(str(config.get("ttd_api_token") or "")),
            source=str(config.get("ttd_source") or DEFAULTS["ttd_source"]).strip(),
            mapper=PathMapper(root, mappings),
            plex_server=str(config.get("plex_server") or "").strip(),
            plex_sections=[str(item).strip() for item in sections if str(item).strip()],
            skip_paths=parse_ttd_skip_paths(config.get("ttd_skip_paths"), root),
            unmatched_policy=str(config.get("ttd_unmatched_policy") or "retry"),
            history_mode=TTDHistoryMode(
                str(config.get("ttd_history_mode") or TTDHistoryMode.NEW_ONLY.value)
            ),
            poll_seconds=max(10, int(number(config, "ttd_poll_seconds"))),
            page_size=max(1, min(100, int(number(config, "ttd_page_size")))),
            max_pages=max(1, min(20, int(number(config, "ttd_max_pages")))),
            overlap_pages=max(1, min(20, int(number(config, "ttd_overlap_pages")))),
            reconcile_hours=max(1, int(number(config, "ttd_reconcile_hours"))),
            debounce_seconds=max(0, int(number(config, "debounce_seconds"))),
            queue_capacity=max(1, int(number(config, "queue_capacity"))),
            scans_per_second=max(0.1, number(config, "scans_per_second")),
            verify_tls=bool(config.get("verify_tls", True)),
            notify_errors=bool(config.get("notify_errors", True)),
        )
        if settings.unmatched_policy not in {"retry", "skip"}:
            raise ValueError("路径不匹配策略只能是 retry 或 skip")
        if settings.enabled:
            missing = [
                label
                for value, label in (
                    (settings.url, "TTD 地址"),
                    (settings.api_token, "TTD API Token"),
                    (settings.plex_server, "Plex 服务"),
                    (settings.source, "TTD 来源"),
                )
                if not value
            ]
            if missing:
                raise ValueError("缺少配置：" + "、".join(missing))
            parsed = urlsplit(settings.url)
            if (
                parsed.scheme not in {"http", "https"}
                or not parsed.netloc
                or parsed.username
            ):
                raise ValueError("TTD 地址必须是完整 HTTP URL，凭据请填写 API Token")
        return settings


def build_form(
    servers: list[dict[str, str]],
) -> tuple[list[dict[str, object]], dict[str, object]]:
    groups = (
        (
            "连接",
            (
                ("enabled", "启用插件"),
                ("ttd_url", "TgToDrive 地址"),
                ("ttd_api_token", "TgToDrive API Token"),
                ("ttd_source", "TgToDrive 来源"),
                ("plex_server", "Plex 服务"),
                ("plex_sections", "Plex 媒体库 ID，逗号分隔"),
            ),
        ),
        (
            "目录范围",
            (
                ("ttd_target_root", "TTD 目标根目录，云端逻辑路径"),
                (
                    "cloud_plex_path_overrides",
                    "TTD 逻辑路径 → Plex 容器路径，每行 source => target",
                ),
                ("ttd_skip_paths", "跳过目录，每行一个，可选"),
                ("ttd_unmatched_policy", "目标不属于所选 Plex 媒体库时"),
            ),
        ),
        (
            "轮询与可靠性",
            (
                ("ttd_history_mode", "整理记录处理范围"),
                ("ttd_poll_seconds", "轮询间隔，秒"),
                ("ttd_page_size", "每页记录数"),
                ("ttd_max_pages", "快轮询最大页数"),
                ("ttd_overlap_pages", "遇到旧记录后继续回查页数"),
                ("ttd_reconcile_hours", "完整历史对账间隔，小时"),
                ("debounce_seconds", "同目录扫描合并等待，秒"),
                ("queue_capacity", "扫描队列容量"),
                ("scans_per_second", "每秒最多提交扫描数"),
                ("verify_tls", "验证 HTTPS 证书"),
                ("notify_errors", "错误通知"),
            ),
        ),
    )
    content: list[dict[str, object]] = [
        {
            "component": "VAlert",
            "props": {
                "type": "info",
                "variant": "tonal",
                "text": "默认仅处理首次查询建立基线后新出现的成功记录，启用前历史不处理。需要补历史时切换处理范围。Token 在 TTD 实用工具 → OpenAPI 中创建。",
            },
        }
    ]
    for title, fields in groups:
        children: list[dict[str, object]] = []
        for key, label in fields:
            value = DEFAULTS[key]
            component = "VSwitch" if isinstance(value, bool) else "VTextField"
            if key in {"cloud_plex_path_overrides", "ttd_skip_paths"}:
                component = "VTextarea"
            props: dict[str, object] = {
                "model": key,
                "label": label,
                "variant": "outlined",
            }
            if key == "ttd_api_token":
                props["type"] = "password"
            if key == "plex_server":
                component = "VSelect"
                props["items"] = servers
            if key == "ttd_unmatched_policy":
                component = "VSelect"
                props["items"] = [
                    {"title": "保留并重试", "value": "retry"},
                    {"title": "跳过并记账", "value": "skip"},
                ]
            if key == "ttd_history_mode":
                component = "VSelect"
                props["items"] = [
                    {"title": "仅新增，历史记录不处理", "value": "new_only"},
                    {"title": "新增及未处理历史", "value": "include_history"},
                ]
                props["hint"] = (
                    "仅新增首次建立基线，普通重启沿用游标；历史模式会恢复保留的自动任务并分批补扫。"
                )
                props["persistent-hint"] = True
            if key == "ttd_reconcile_hours":
                props["show"] = "{{ ttd_history_mode === 'include_history' }}"
            children.append({"component": component, "props": props})
        content.append(
            {
                "component": "VCard",
                "props": {"title": title, "class": "my-3 pa-3"},
                "content": children,
            }
        )
    return [{"component": "VForm", "content": content}], dict(DEFAULTS)

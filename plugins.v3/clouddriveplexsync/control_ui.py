"""Native MoviePilot plugin detail page with immediate historical controls."""

from __future__ import annotations

import json
import posixpath
from datetime import datetime
from urllib.parse import quote

PAGE_SIZE = 5


def _timestamp(value: object) -> str:
    if not isinstance(value, (float, int)) or not value:
        return "时间未知"
    try:
        return datetime.fromtimestamp(value).astimezone().strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, OverflowError, OSError):
        return "时间未知"


def _media_label(
    title: object, season: object, episode: object, path: str, *, is_file: bool = False
) -> str:
    name = str(title or "").strip()
    if not name:
        directory = posixpath.dirname(path) if is_file else path
        basename = posixpath.basename(directory)
        name = (
            f"{posixpath.basename(posixpath.dirname(directory))} / {basename}"
            if basename.lower().startswith("season ")
            else basename or posixpath.basename(path) or path
        )
    if isinstance(season, int) and isinstance(episode, int):
        name += f" S{season:02d}E{episode:02d}"
    return name


def _activity_view(entry: dict[str, object]) -> dict[str, str]:
    message = str(entry.get("message") or "")
    stage, _, payload = message.partition(" ")
    detail: dict[str, object] = {}
    if stage in {"QUEUE", "SCAN_START", "SCAN_SUBMITTED", "PLEX_INDEXED"}:
        try:
            parsed = json.loads(payload)
            if isinstance(parsed, dict):
                detail = parsed
        except (TypeError, ValueError):
            pass
    if detail:
        path = str(detail.get("plex_path") or "")
        labels = {
            "QUEUE": "已入队",
            "SCAN_START": "开始提交扫描",
            "SCAN_SUBMITTED": "Plex 已接受扫描",
            "PLEX_INDEXED": "Plex 已确认收录",
        }
        return {
            "title": f"{labels[stage]} · {_media_label(detail.get('title'), detail.get('season'), detail.get('episode'), path, is_file=stage == 'PLEX_INDEXED')}",
            "subtitle": f"{_timestamp(entry.get('timestamp'))} · {detail.get('section_title') or '媒体库未知'}",
            "detail": path,
        }
    return {
        "title": "错误" if entry.get("level") == "error" else "运行信息",
        "subtitle": _timestamp(entry.get("timestamp")),
        "detail": message,
    }


def _indexed_view(entry: dict[str, object]) -> dict[str, str]:
    path = str(entry.get("expected_path") or "")
    return {
        "title": _media_label(
            entry.get("title"),
            entry.get("season_num"),
            entry.get("episode_num"),
            path,
            is_file=True,
        ),
        "subtitle": f"确认于 {_timestamp(entry.get('confirmed_at'))} · {entry.get('section_title') or '媒体库未知'}",
        "detail": path,
    }


def _pending_view(entry: dict[str, object]) -> dict[str, str]:
    row = _indexed_view(entry)
    row["subtitle"] = (
        f"提交于 {_timestamp(entry.get('submitted_at'))} · "
        f"{entry.get('detail') or '等待首次核验'}"
    )
    return row


def _alternative_view(entry: dict[str, object]) -> dict[str, str]:
    row = _indexed_view(entry)
    row["subtitle"] = (
        f"发现其他版本于 {_timestamp(entry.get('resolved_at'))} · 原文件路径未确认"
    )
    row["detail"] = (
        f"原路径：{entry.get('expected_path') or ''}\n其他版本：{entry.get('matched_path') or ''}"
    )
    return row


def _page_window(rows: list[dict[str, str]]) -> dict[str, object]:
    page_count = (len(rows) + PAGE_SIZE - 1) // PAGE_SIZE
    pages: list[dict[str, object]] = []
    for page_index in range(page_count):
        cards: list[dict[str, object]] = [
            {
                "component": "VCardSubtitle",
                "text": f"第 {page_index + 1} / {page_count} 页 · 共 {len(rows)} 条",
                "props": {"class": "py-2"},
            }
        ]
        for row in rows[page_index * PAGE_SIZE : (page_index + 1) * PAGE_SIZE]:
            cards.append(
                {
                    "component": "VCard",
                    "props": {"variant": "tonal", "class": "mb-2"},
                    "content": [
                        {
                            "component": "VListItem",
                            "props": {
                                "title": row["title"],
                                "subtitle": row["subtitle"],
                            },
                        },
                        {
                            "component": "VCardText",
                            "text": row["detail"],
                            "props": {
                                "class": "pt-0",
                                "style": "overflow-wrap: anywhere; white-space: pre-wrap;",
                            },
                        },
                    ],
                }
            )
        pages.append(
            {
                "component": "VWindowItem",
                "props": {
                    "value": page_index,
                    "class": "px-16" if page_count > 1 else "px-2",
                },
                "content": cards,
            }
        )
    return {"component": "VWindow", "props": {"show-arrows": True}, "content": pages}


def _entries(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list):
        return []
    return [
        {str(key): entry_value for key, entry_value in item.items()}
        for item in value
        if isinstance(item, dict)
    ]


def history_button(
    enabled: bool, ready: bool, active: bool, *, plugin_id: str = "CloudDrivePlexSync"
) -> dict[str, object]:
    action = "start" if enabled else "stop"
    return {
        "component": "VBtn",
        "text": "开启 / 继续历史处理" if enabled else "停止历史查询与扫描",
        "props": {
            "color": "primary" if enabled else "warning",
            "disabled": not ready or (active if enabled else not active),
            "class": "ma-2",
        },
        "events": {
            "click": {
                "api": f"/plugin/{quote(plugin_id, safe='')}/history/{action}",
                "method": "POST",
            }
        },
    }


def build_page(
    status: dict[str, object],
    history_enabled: bool,
    *,
    plugin_id: str = "CloudDrivePlexSync",
) -> list[dict[str, object]]:
    history = status.get("history_processing") or {}
    if not isinstance(history, dict):
        history = {}
    enabled = bool(history.get("enabled", history_enabled))
    ready = bool(status.get("ready"))
    activities = _entries(status.get("activities"))
    indexed = _entries(status.get("indexed_media"))
    pending = _entries(status.get("pending_media"))
    alternatives = _entries(status.get("alternative_media"))
    recent = status.get("recent")
    if not activities and isinstance(recent, list):
        for item in reversed(recent):
            message = str(item)
            is_error = message.startswith("ERROR ")
            activities.append(
                {
                    "message": message[6:] if is_error else message,
                    "level": "error" if is_error else "info",
                }
            )
    text = (
        f"后台：{'就绪' if ready else '未就绪'}；"
        f"待扫描：{status.get('pending_scans', 0)}；执行中：{status.get('inflight_scans', 0)}；"
        f"历史：{'开启' if enabled else '关闭'}；"
        f"运行：{'是' if history.get('running') else '否'}；"
        f"查询：{'是' if history.get('query_in_progress') else '否'}；"
        f"下一页：{history.get('next_page', 0)}；保留：{status.get('held_history_scans', 0)}；"
        f"错误：{status.get('last_error') or '无'}"
    )
    content: list[dict[str, object]] = [
        {"component": "VAlert", "props": {"type": "info", "text": text}},
        history_button(
            True, ready, enabled and bool(history.get("running")), plugin_id=plugin_id
        ),
        history_button(False, ready, enabled, plugin_id=plugin_id),
        {
            "component": "VAlert",
            "props": {
                "type": "warning",
                "text": (
                    "关闭后停止历史查询和后续扫描，保留断点与任务；新增监听和手动扫描继续。"
                    "已发给 Plex 的请求无法撤回。"
                ),
            },
        },
    ]
    content.extend(
        [
            {
                "component": "VCard",
                "props": {"variant": "text", "class": "mt-3"},
                "content": [
                    {
                        "component": "VBtn",
                        "text": "刷新记录",
                        "props": {
                            "color": "primary",
                            "variant": "tonal",
                            "disabled": not ready,
                        },
                        "events": {
                            "click": {
                                "api": f"/plugin/{quote(plugin_id, safe='')}/status",
                                "method": "GET",
                            }
                        },
                    }
                ],
            },
            {"component": "VCardTitle", "text": "Plex 已确认收录"},
            {
                "component": "VAlert",
                "props": {
                    "type": "info",
                    "variant": "tonal",
                    "text": (
                        f"待确认：{status.get('pending_verifications', 0)} 条。"
                        f"其他版本：{status.get('alternative_verifications', 0)} 条。"
                        "已确认表示 Plex 中找到对应文件路径；同名覆盖时不能仅凭路径确认文件版本。"
                        "这里只显示启用新版本后产生并完成核验的记录。"
                        "各列表最多显示最近 2000 条。"
                    ),
                },
            },
        ]
    )
    if indexed:
        content.append(_page_window([_indexed_view(entry) for entry in indexed]))
    else:
        content.append({"component": "VCardText", "text": "暂无已确认收录的内容"})
    if alternatives:
        content.append({"component": "VCardTitle", "text": "Plex 已收录其他版本"})
        content.append(
            _page_window([_alternative_view(entry) for entry in alternatives])
        )
    if pending:
        content.append({"component": "VCardTitle", "text": "等待 Plex 入库确认"})
        content.append(_page_window([_pending_view(entry) for entry in pending]))
    content.append({"component": "VCardTitle", "text": "扫描与运行日志"})
    if status.get("activity_error"):
        content.append(
            {
                "component": "VAlert",
                "props": {"type": "error", "text": str(status["activity_error"])},
            }
        )
    if activities:
        content.append(_page_window([_activity_view(entry) for entry in activities]))
    else:
        content.append({"component": "VCardText", "text": "暂无日志"})
    return content

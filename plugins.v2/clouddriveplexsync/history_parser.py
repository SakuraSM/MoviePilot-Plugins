"""Normalize TgToDrive history responses and configured skip paths."""

from __future__ import annotations

import hashlib
import json
import posixpath
import re
from collections.abc import Sequence
from typing import Any

from .history_models import TTDHistoryRecord, TTDProtocolError
from .path_mapper import PathMappingError, is_under, normalize_path


def parse_ttd_skip_paths(value: object, target_root: str) -> list[str]:
    """Parse absolute or target-root-relative directory prefixes to skip."""

    root = normalize_path(target_root)
    if not value:
        return []
    entries = value if isinstance(value, (list, tuple)) else str(value).splitlines()
    paths: list[str] = []
    for raw in entries:
        item = str(raw).strip()
        if not item or item.startswith("#"):
            continue
        path = normalize_path(
            item if item.startswith("/") else posixpath.join(root, item)
        )
        if not is_under(path, root):
            raise PathMappingError(f"TTD 跳过路径越出目标根目录：{path}")
        paths.append(path)
    return sorted(set(paths), key=lambda item: (-len(item), item))


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
    "event_time",
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


def _first_value(value: dict[str, Any], names: Sequence[str]) -> Any:
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
        value = "/".join(
            str(item).strip(" /") for item in value if str(item).strip(" /")
        )
    text = str(value or "").strip()
    if not text or text == "-":
        return ""
    # TgToDrive's UI renders relative target components with spaced slashes.
    return re.sub(r"\s*/\s*", "/", text)


def _record_key(
    value: dict[str, Any], target: str, completed_at: str
) -> tuple[str, str]:
    record_id = str(_first_value(value, _ID_FIELDS) or "").strip()
    if record_id:
        return f"id:{record_id}", record_id
    stable = {
        "target": target,
        "completed_at": completed_at,
        "source": str(_first_value(value, _SOURCE_FIELDS) or ""),
        "status": str(_first_value(value, _STATUS_FIELDS) or ""),
        "name": str(
            _first_value(value, ("name", "file_name", "filename", "title")) or ""
        ),
    }
    digest = hashlib.sha256(
        json.dumps(stable, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()
    return f"sha256:{digest}", ""


def _find_items(value: Any) -> list[dict[str, Any]] | None:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    if not isinstance(value, dict):
        return None
    for name in ("items", "records", "list", "history", "histories", "results"):
        candidate = value.get(name)
        if isinstance(candidate, list):
            return [item for item in candidate if isinstance(item, dict)]
    for name in ("data", "result", "history", "source_data"):
        candidate = _find_items(value.get(name))
        if candidate is not None:
            return candidate
    return None


def parse_history_payload(payload: Any) -> list[TTDHistoryRecord]:
    """Parse known TgToDrive response shapes without depending on one release."""

    if isinstance(payload, dict) and payload.get("success") is False:
        raise TTDProtocolError(
            str(payload.get("error") or payload.get("message") or "请求失败")
        )
    items = _find_items(payload)
    if items is None:
        raise TTDProtocolError("整理历史响应中没有记录列表")

    records: list[TTDHistoryRecord] = []
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

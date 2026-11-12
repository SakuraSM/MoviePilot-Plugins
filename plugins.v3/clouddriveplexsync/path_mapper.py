"""Map TgToDrive target directories into Plex-visible paths."""

from __future__ import annotations

import posixpath
from dataclasses import dataclass


class PathMappingError(ValueError):
    """Raised when a path is unsafe, outside scope, or cannot be mapped."""


def normalize_path(value: str) -> str:
    value = (value or "").strip().replace("\\", "/")
    if not value:
        raise PathMappingError("empty path")
    if not value.startswith("/"):
        value = "/" + value
    parts = value.split("/")
    if ".." in parts:
        raise PathMappingError("parent path segments are not allowed")
    normalized = posixpath.normpath(value)
    if not normalized.startswith("/"):
        normalized = "/" + normalized
    return normalized


def is_under(path: str, prefix: str, *, allow_equal: bool = True) -> bool:
    path = normalize_path(path)
    prefix = normalize_path(prefix)
    if allow_equal and path == prefix:
        return True
    if prefix == "/":
        return path.startswith("/")
    return path.startswith(prefix.rstrip("/") + "/")


def replace_prefix(path: str, source: str, target: str) -> str:
    path = normalize_path(path)
    source = normalize_path(source)
    target = normalize_path(target)
    if not is_under(path, source):
        raise PathMappingError(f"{path} is not under {source}")
    relative = path[len(source) :] if source != "/" else path
    return normalize_path(target.rstrip("/") + relative)


def parse_override_lines(value: object) -> list[tuple[str, str]]:
    """Parse newline/list mappings in ``source => target`` form."""

    if not value:
        return []
    entries = value if isinstance(value, (list, tuple)) else str(value).splitlines()
    result: list[tuple[str, str]] = []
    for raw in entries:
        line = str(raw).strip()
        if not line or line.startswith("#"):
            continue
        if "=>" not in line:
            raise PathMappingError(f"invalid mapping: {line}")
        source, target = (item.strip() for item in line.split("=>", 1))
        result.append((normalize_path(source), normalize_path(target)))
    result.sort(key=lambda item: len(item[0]), reverse=True)
    return result


@dataclass
class PathMapper:
    target_root: str
    mappings: list[tuple[str, str]]

    def __post_init__(self) -> None:
        self.target_root = normalize_path(self.target_root)
        self.mappings = sorted(
            self.mappings, key=lambda item: len(item[0]), reverse=True
        )
        if not any(is_under(self.target_root, source) for source, _ in self.mappings):
            raise PathMappingError("请配置覆盖 TTD 目标根目录的 Plex 路径映射")

    def validate_cloud_path(self, cloud_path: str) -> str:
        path = normalize_path(cloud_path)
        if not is_under(path, self.target_root):
            raise PathMappingError(f"路径越出 TTD 目标根目录：{path}")
        return path

    def cloud_to_plex(self, cloud_path: str) -> str:
        path = self.validate_cloud_path(cloud_path)
        for source, target in self.mappings:
            if is_under(path, source):
                return replace_prefix(path, source, target)
        raise PathMappingError(f"目标路径没有 Plex 映射：{path}")

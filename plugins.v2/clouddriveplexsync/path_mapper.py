"""Lexical, existence-independent path mapping for CloudDrive and Plex."""

from __future__ import annotations

import posixpath
from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence, Tuple

from .models import CloudApi, MountPoint


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
    relative = path[len(source):] if source != "/" else path
    return normalize_path(target.rstrip("/") + relative)


def parse_override_lines(value: object) -> List[Tuple[str, str]]:
    """Parse newline/list mappings in ``source => target`` form."""

    if not value:
        return []
    entries = value if isinstance(value, (list, tuple)) else str(value).splitlines()
    result: List[Tuple[str, str]] = []
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


def parse_roots(value: object) -> List[str]:
    if not value:
        return []
    entries = value if isinstance(value, (list, tuple)) else str(value).splitlines()
    roots = [normalize_path(str(item)) for item in entries if str(item).strip()]
    return sorted(set(roots), key=len, reverse=True)


@dataclass
class PathMapper:
    """Maps CloudDrive virtual paths into the paths Plex can see."""

    watch_roots: Sequence[str]
    mounts: Sequence[MountPoint]
    plex_overrides: Sequence[Tuple[str, str]]
    moviepilot_overrides: Sequence[Tuple[str, str]] = ()

    def __post_init__(self) -> None:
        self.watch_roots = [normalize_path(item) for item in self.watch_roots]
        self.plex_overrides = sorted(self.plex_overrides, key=lambda item: len(item[0]), reverse=True)
        self.moviepilot_overrides = sorted(
            self.moviepilot_overrides, key=lambda item: len(item[0]), reverse=True
        )

    def validate_cloud_path(self, cloud_path: str) -> str:
        normalized = normalize_path(cloud_path)
        if not self.watch_roots:
            raise PathMappingError("no CloudDrive watch roots are configured")
        if not any(is_under(normalized, root) for root in self.watch_roots):
            raise PathMappingError(f"path is outside configured watch roots: {normalized}")
        return normalized

    def cloud_to_mount(self, cloud_path: str) -> str:
        cloud_path = self.validate_cloud_path(cloud_path)
        candidates = [
            mount
            for mount in self.mounts
            if mount.is_mounted and mount.source_dir and mount.mount_point
            and is_under(cloud_path, mount.source_dir)
        ]
        if not candidates:
            raise PathMappingError(f"no mounted CloudDrive path matches {cloud_path}")
        candidates.sort(key=lambda item: len(normalize_path(item.source_dir)), reverse=True)
        best_length = len(normalize_path(candidates[0].source_dir))
        best = [item for item in candidates if len(normalize_path(item.source_dir)) == best_length]
        mapped = {replace_prefix(cloud_path, item.source_dir, item.mount_point) for item in best}
        if len(mapped) != 1:
            raise PathMappingError(f"ambiguous CloudDrive mounts for {cloud_path}")
        return mapped.pop()

    @staticmethod
    def _apply_override(path: str, mappings: Sequence[Tuple[str, str]]) -> str:
        candidates = [(source, target) for source, target in mappings if is_under(path, source)]
        if not candidates:
            return normalize_path(path)
        source, target = max(candidates, key=lambda item: len(item[0]))
        return replace_prefix(path, source, target)

    def cloud_to_plex(self, cloud_path: str) -> str:
        return self._apply_override(self.cloud_to_mount(cloud_path), self.plex_overrides)

    def cloud_scan_directory(self, cloud_path: str) -> str:
        cloud_path = self.validate_cloud_path(cloud_path)
        parent = normalize_path(posixpath.dirname(cloud_path))
        if not any(is_under(parent, root) for root in self.watch_roots):
            # A watch-root directory event scans the watch root itself, never its parent.
            return max((root for root in self.watch_roots if is_under(cloud_path, root)), key=len)
        return parent

    def moviepilot_to_cloud(self, moviepilot_path: str) -> str:
        """Reverse an optional MoviePilot prefix and the CD2 mount table."""

        path = normalize_path(moviepilot_path)
        mounted_path = self._apply_override(path, self.moviepilot_overrides)
        candidates = [
            mount
            for mount in self.mounts
            if mount.is_mounted and is_under(mounted_path, mount.mount_point)
        ]
        if not candidates:
            raise PathMappingError(f"MoviePilot path does not map to CloudDrive: {path}")
        mount = max(candidates, key=lambda item: len(normalize_path(item.mount_point)))
        cloud_path = replace_prefix(mounted_path, mount.mount_point, mount.source_dir)
        return self.validate_cloud_path(cloud_path)


def cloud_for_path(cloud_path: str, apis: Iterable[CloudApi]) -> Optional[CloudApi]:
    """Return the most specific CloudAPI owning a virtual cloud path."""

    path = normalize_path(cloud_path)
    candidates = [api for api in apis if api.path and is_under(path, api.path)]
    if not candidates:
        return None
    return max(candidates, key=lambda item: len(normalize_path(item.path)))

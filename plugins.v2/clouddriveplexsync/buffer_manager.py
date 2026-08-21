"""Temporary, crash-recoverable CloudDrive buffer configuration manager."""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, List, Optional, Tuple

from .cd2_client import CloudDriveClient
from .models import BufferLease, CloudApi, PluginStats


@dataclass(frozen=True)
class BufferSettings:
    mode: str = "adaptive"
    min_mb: int = 1
    scan_mb: int = 2
    playback_mb: int = 8
    apply_before_scan: bool = True
    restore_enabled: bool = True
    restore_grace_seconds: int = 60
    max_lease_minutes: int = 15
    fail_open: bool = True
    overrides: Dict[str, Tuple[int, int]] = None

    def __post_init__(self) -> None:
        if self.mode not in {"disabled", "fixed", "adaptive"}:
            object.__setattr__(self, "mode", "adaptive")
        object.__setattr__(self, "min_mb", max(1, int(self.min_mb)))
        object.__setattr__(self, "scan_mb", max(self.min_mb, int(self.scan_mb)))
        object.__setattr__(self, "playback_mb", max(self.min_mb, int(self.playback_mb)))
        object.__setattr__(self, "restore_grace_seconds", max(0, int(self.restore_grace_seconds)))
        object.__setattr__(self, "max_lease_minutes", max(1, int(self.max_lease_minutes)))
        if self.overrides is None:
            object.__setattr__(self, "overrides", {})


def parse_buffer_overrides(value: object, min_mb: int = 1) -> Dict[str, Tuple[int, int]]:
    if not value:
        return {}
    entries = value if isinstance(value, (list, tuple)) else str(value).splitlines()
    result: Dict[str, Tuple[int, int]] = {}
    for raw in entries:
        line = str(raw).strip()
        if not line or line.startswith("#"):
            continue
        parts = [item.strip() for item in line.split("|")]
        if len(parts) != 3 or not parts[0]:
            raise ValueError(f"invalid buffer override: {line}")
        result[parts[0]] = (
            max(min_mb, int(parts[1])),
            max(min_mb, int(parts[2])),
        )
    return result


class BufferManager:
    """Applies and restores per-cloud temporary buffer values."""

    def __init__(
        self,
        client: CloudDriveClient,
        settings: BufferSettings,
        *,
        playback_probe: Callable[[Iterable[str]], bool],
        scan_probe: Callable[[Iterable[str]], bool],
        persist: Optional[Callable[[List[dict]], None]] = None,
        stats: Optional[PluginStats] = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.client = client
        self.settings = settings
        self.playback_probe = playback_probe
        self.scan_probe = scan_probe
        self.persist = persist
        self.stats = stats
        self.clock = clock
        self.leases: Dict[str, BufferLease] = {}
        self.last_error: str = ""
        self._lock = asyncio.Lock()

    def load_leases(self, values: Iterable[dict]) -> None:
        for value in values or []:
            try:
                lease = BufferLease.from_dict(value)
                self.leases[lease.cloud_identity] = lease
            except (KeyError, TypeError, ValueError):
                continue

    def _save(self) -> None:
        if self.persist:
            self.persist([lease.to_dict() for lease in self.leases.values()])

    def _values_for(self, cloud: CloudApi) -> Tuple[int, int]:
        for key in (cloud.identity, cloud.path, cloud.nickname, cloud.name):
            if key and key in self.settings.overrides:
                return self.settings.overrides[key]
        return self.settings.scan_mb, self.settings.playback_mb

    def target_for(self, cloud: CloudApi, plex_prefixes: Iterable[str]) -> Tuple[int, bool]:
        scan_mb, playback_mb = self._values_for(cloud)
        if self.settings.mode == "fixed":
            return scan_mb, False
        try:
            playing = bool(self.playback_probe(plex_prefixes))
        except Exception:
            playing = True
        return (playback_mb if playing else scan_mb), playing

    async def prepare(
        self,
        cloud: Optional[CloudApi],
        plex_prefixes: Iterable[str],
        section_ids: Iterable[str],
    ) -> bool:
        """Prepare one cloud for scanning; return whether scanning may proceed."""

        if (
            cloud is None
            or self.settings.mode == "disabled"
            or not self.settings.apply_before_scan
        ):
            return True
        async with self._lock:
            now = self.clock()
            target_mb, _ = self.target_for(cloud, plex_prefixes)
            identity = cloud.identity
            existing = self.leases.get(identity)
            try:
                config = await self.client.get_cloud_config(cloud.name, cloud.username)
                if config.buffer_limit_mb:
                    target_mb = min(target_mb, config.buffer_limit_mb)

                if existing and config.buffer_mb != existing.applied_mb:
                    # Someone changed the value while the lease was active. Respect it
                    # and start a new lease from the new value if another change is needed.
                    self.leases.pop(identity, None)
                    existing = None

                if existing:
                    existing.last_event_at = now
                    existing.section_ids = sorted(
                        set(existing.section_ids).union(str(item) for item in section_ids)
                    )
                    if config.buffer_mb != target_mb:
                        await self.client.set_cloud_buffer(
                            cloud.name, cloud.username, config.raw, target_mb
                        )
                        existing.applied_mb = target_mb
                        if self.stats:
                            self.stats.buffer_changes += 1
                    self._save()
                    return True

                if config.buffer_mb == target_mb:
                    return True

                await self.client.set_cloud_buffer(cloud.name, cloud.username, config.raw, target_mb)
                self.leases[identity] = BufferLease(
                    cloud_name=cloud.name,
                    username=cloud.username,
                    original_mb=config.buffer_mb,
                    applied_mb=target_mb,
                    started_at=now,
                    last_event_at=now,
                    lease_id=uuid.uuid4().hex,
                    section_ids=sorted({str(item) for item in section_ids}),
                )
                if self.stats:
                    self.stats.buffer_changes += 1
                self.last_error = ""
                self._save()
                return True
            except Exception as exc:
                self.last_error = str(exc)
                return self.settings.fail_open

    async def _restore_identity(self, identity: str) -> bool:
        lease = self.leases.get(identity)
        if not lease:
            return True
        try:
            config = await self.client.get_cloud_config(lease.cloud_name, lease.username)
            if config.buffer_mb == lease.applied_mb and lease.original_mb != lease.applied_mb:
                await self.client.set_cloud_buffer(
                    lease.cloud_name, lease.username, config.raw, lease.original_mb
                )
                if self.stats:
                    self.stats.buffer_changes += 1
            # If the current value differs, the user has taken control; do not overwrite it.
            self.leases.pop(identity, None)
            self.last_error = ""
            self._save()
            return True
        except Exception as exc:
            self.last_error = str(exc)
            return False

    async def restore_due(self) -> List[str]:
        if not self.settings.restore_enabled:
            return []
        restored: List[str] = []
        async with self._lock:
            now = self.clock()
            for identity, lease in list(self.leases.items()):
                maximum_reached = now - lease.started_at >= self.settings.max_lease_minutes * 60
                quiet = now - lease.last_event_at >= max(30, self.settings.restore_grace_seconds)
                scanning = False
                if not maximum_reached and quiet:
                    try:
                        scanning = bool(self.scan_probe(lease.section_ids))
                    except Exception:
                        scanning = True
                if maximum_reached or (quiet and not scanning):
                    if await self._restore_identity(identity):
                        restored.append(identity)
        return restored

    async def restore_all(self) -> List[str]:
        restored: List[str] = []
        async with self._lock:
            for identity in list(self.leases):
                if await self._restore_identity(identity):
                    restored.append(identity)
        return restored

    async def recover_stale(self) -> List[str]:
        """Restore persisted leases on startup before accepting new work."""

        return await self.restore_all()

    async def preview(
        self, cloud: CloudApi, plex_prefixes: Iterable[str]
    ) -> Dict[str, object]:
        if self.settings.mode == "disabled":
            return {
                "cloud": cloud.nickname or cloud.name,
                "identity": cloud.identity,
                "mode": "disabled",
                "playing": False,
                "current_mb": None,
                "target_mb": None,
                "restore_mb": None,
            }
        target, playing = self.target_for(cloud, plex_prefixes)
        config = await self.client.get_cloud_config(cloud.name, cloud.username)
        if config.buffer_limit_mb:
            target = min(target, config.buffer_limit_mb)
        return {
            "cloud": cloud.nickname or cloud.name,
            "identity": cloud.identity,
            "mode": self.settings.mode,
            "playing": playing,
            "current_mb": config.buffer_mb,
            "target_mb": target,
            "restore_mb": config.buffer_mb,
        }

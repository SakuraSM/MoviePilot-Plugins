"""Typed CloudDrive2 API wrapper used by the plugin."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, AsyncIterator, Dict, List, Optional

from .grpc_web import GrpcWebClient
from .models import ChangeType, CloudApi, FileSystemChange, MountPoint, PluginStats
from .protobuf_codec import (
    bool_field,
    bytes_field,
    bytes_fields,
    encode_get_cloud_config_request,
    encode_list_sub_file_request,
    encode_set_cloud_config_request,
    patch_varint_field,
    string_field,
    varint_field,
)


SERVICE = "clouddrive.CloudDriveFileSrv"


@dataclass(frozen=True)
class CloudConfig:
    """Raw CloudAPIConfig plus the fields needed by the buffer manager."""

    raw: bytes
    buffer_mb: int
    buffer_limit_mb: Optional[int]


@dataclass(frozen=True)
class PushEvent:
    """Decoded subset of CloudDrivePushMessage."""

    kind: str
    value: Any = None


class CloudDriveClient:
    """Dependency-free CloudDrive2 gRPC-Web client."""

    def __init__(
        self,
        base_url: str,
        token: str,
        *,
        verify: bool = True,
        stats: Optional[PluginStats] = None,
        transport: Optional[GrpcWebClient] = None,
    ) -> None:
        self.transport = transport or GrpcWebClient(base_url, token, verify=verify)
        self.stats = stats

    async def _unary(self, method: str, payload: bytes = b"") -> bytes:
        if self.stats:
            self.stats.cd2_unary_requests += 1
        return await self.transport.unary(SERVICE, method, payload)

    async def get_system_info(self) -> Dict[str, Any]:
        raw = await self._unary("GetSystemInfo")
        return {
            "logged_in": bool_field(raw, 1),
            "username": string_field(raw, 2),
            "ready": bool_field(raw, 3),
            "message": string_field(raw, 4),
            "has_error": bool_field(raw, 5),
        }

    async def get_mount_points(self) -> List[MountPoint]:
        raw = await self._unary("GetMountPoints")
        mounts: List[MountPoint] = []
        for item in bytes_fields(raw, 1):
            mounts.append(
                MountPoint(
                    mount_point=string_field(item, 1),
                    source_dir=string_field(item, 2),
                    local_mount=bool_field(item, 3),
                    read_only=bool_field(item, 4),
                    is_mounted=bool_field(item, 9),
                    name=string_field(item, 11),
                )
            )
        return mounts

    async def get_cloud_apis(self) -> List[CloudApi]:
        raw = await self._unary("GetAllCloudApis")
        apis: List[CloudApi] = []
        for item in bytes_fields(raw, 1):
            apis.append(
                CloudApi(
                    name=string_field(item, 1),
                    username=string_field(item, 2),
                    nickname=string_field(item, 3),
                    event_listener_running=bool_field(item, 7),
                    path=string_field(item, 10),
                    read_only=bool_field(item, 12),
                )
            )
        return apis

    async def get_cloud_config(self, cloud_name: str, username: str) -> CloudConfig:
        request = encode_get_cloud_config_request(cloud_name, username)
        raw = await self._unary("GetCloudAPIConfig", request)
        limit = varint_field(raw, 19)
        return CloudConfig(
            raw=raw,
            buffer_mb=int(varint_field(raw, 5, 0) or 0),
            buffer_limit_mb=int(limit) if limit else None,
        )

    async def set_cloud_buffer(
        self, cloud_name: str, username: str, current_config: bytes, buffer_mb: int
    ) -> None:
        patched = patch_varint_field(current_config, 5, buffer_mb)
        request = encode_set_cloud_config_request(cloud_name, username, patched)
        await self._unary("SetCloudAPIConfig", request)

    async def force_list(self, cloud_path: str) -> int:
        """Force a single directory re-list and return the reply count."""

        if self.stats:
            self.stats.cd2_unary_requests += 1
        request = encode_list_sub_file_request(cloud_path, True)
        count = 0
        async for _ in self.transport.stream(SERVICE, "GetSubFiles", request):
            count += 1
        return count

    async def push_events(self) -> AsyncIterator[PushEvent]:
        async for raw in self.transport.stream(SERVICE, "PushMessage", b""):
            event = self._parse_push(raw)
            if event:
                if self.stats:
                    self.stats.push_events += 1
                yield event

    @staticmethod
    def _parse_push(raw: bytes) -> Optional[PushEvent]:
        message_type = int(varint_field(raw, 1, -1) or 0)
        if message_type == 4:
            payload = bytes_field(raw, 5)
            if not payload:
                return None
            raw_change_type = int(varint_field(payload, 1, 0) or 0)
            try:
                change_type = ChangeType(raw_change_type)
            except ValueError:
                return None
            path = string_field(payload, 3)
            if not path:
                return None
            return PushEvent(
                "filesystem",
                FileSystemChange(
                    change_type=change_type,
                    is_directory=bool_field(payload, 2),
                    path=path,
                    new_path=string_field(payload, 4) or None,
                ),
            )
        if message_type == 5:
            return PushEvent("mount_changed")
        if message_type == 9:
            return PushEvent("cloud_api_changed")
        if message_type in {0, 1, 6}:
            status = bytes_field(raw, 2)
            return PushEvent("status", {"version": string_field(status, 3)})
        return None

    async def close(self) -> None:
        await self.transport.close()

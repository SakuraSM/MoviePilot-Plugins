"""Minimal asynchronous gRPC-Web transport built on httpx."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, AsyncIterator, Dict, List, Optional
from urllib.parse import unquote

try:
    import httpx
except ImportError:  # Allows protocol-only unit tests outside MoviePilot.
    httpx = None  # type: ignore[assignment]


DATA_FRAME = 0x00
TRAILER_FRAME = 0x80


class GrpcWebError(RuntimeError):
    """A non-successful gRPC-Web call."""

    def __init__(self, status: int, message: str):
        self.status = status
        self.message = message
        super().__init__(f"gRPC status {status}: {message}")


@dataclass(frozen=True)
class GrpcFrame:
    flags: int
    payload: bytes

    @property
    def is_trailer(self) -> bool:
        return bool(self.flags & TRAILER_FRAME)


def frame_message(payload: bytes) -> bytes:
    return bytes([DATA_FRAME]) + len(payload).to_bytes(4, "big") + payload


class FrameDecoder:
    """Incremental gRPC-Web frame decoder."""

    def __init__(self) -> None:
        self._buffer = bytearray()

    def feed(self, chunk: bytes) -> List[GrpcFrame]:
        self._buffer.extend(chunk)
        frames: List[GrpcFrame] = []
        while len(self._buffer) >= 5:
            length = int.from_bytes(self._buffer[1:5], "big")
            frame_length = 5 + length
            if len(self._buffer) < frame_length:
                break
            flags = self._buffer[0]
            payload = bytes(self._buffer[5:frame_length])
            del self._buffer[:frame_length]
            frames.append(GrpcFrame(flags=flags, payload=payload))
        return frames

    def finish(self) -> None:
        if self._buffer:
            raise GrpcWebError(13, "stream ended in the middle of a frame")


def parse_trailers(payload: bytes) -> Dict[str, str]:
    headers: Dict[str, str] = {}
    for line in payload.decode("utf-8", errors="replace").splitlines():
        if ":" not in line:
            continue
        name, value = line.split(":", 1)
        headers[name.strip().lower()] = value.strip()
    return headers


def raise_for_grpc_status(headers: Dict[str, str]) -> None:
    raw_status = headers.get("grpc-status")
    if raw_status is None or raw_status == "0":
        return
    try:
        status = int(raw_status)
    except ValueError:
        status = 13
    raise GrpcWebError(status, unquote(headers.get("grpc-message") or "unknown gRPC error"))


class GrpcWebClient:
    """Unary and server-streaming client for CloudDrive's gRPC-Web endpoint."""

    def __init__(
        self,
        base_url: str,
        token: str = "",
        unary_timeout: float = 10.0,
        verify: bool = True,
        client: Optional[Any] = None,
    ) -> None:
        if httpx is None and client is None:
            raise RuntimeError("httpx is required at runtime")
        self.base_url = base_url.rstrip("/")
        self.token = token.strip()
        self.unary_timeout = unary_timeout
        self._owns_client = client is None
        # gRPC-Web works over HTTP/1.1, avoiding httpx's optional ``h2``
        # dependency in the MoviePilot runtime.
        self._client = client or httpx.AsyncClient(verify=verify)

    def _headers(self) -> Dict[str, str]:
        headers = {
            "content-type": "application/grpc-web+proto",
            "accept": "application/grpc-web+proto",
            "x-grpc-web": "1",
            "te": "trailers",
            "grpc-encoding": "identity",
            "grpc-accept-encoding": "identity",
        }
        if self.token:
            headers["authorization"] = f"Bearer {self.token}"
        return headers

    def _url(self, service: str, method: str) -> str:
        return f"{self.base_url}/{service}/{method}"

    async def unary(self, service: str, method: str, payload: bytes = b"") -> bytes:
        response = await self._client.post(
            self._url(service, method),
            content=frame_message(payload),
            headers=self._headers(),
            timeout=self.unary_timeout,
        )
        response.raise_for_status()
        header_status = {key.lower(): value for key, value in response.headers.items()}
        raise_for_grpc_status(header_status)

        messages: List[bytes] = []
        decoder = FrameDecoder()
        for frame in decoder.feed(response.content):
            if frame.is_trailer:
                raise_for_grpc_status(parse_trailers(frame.payload))
            else:
                messages.append(frame.payload)
        decoder.finish()
        if not messages:
            raise GrpcWebError(13, f"{method} returned no protobuf message")
        return messages[0]

    async def stream(
        self, service: str, method: str, payload: bytes = b""
    ) -> AsyncIterator[bytes]:
        async with self._client.stream(
            "POST",
            self._url(service, method),
            content=frame_message(payload),
            headers=self._headers(),
            timeout=None,
        ) as response:
            response.raise_for_status()
            header_status = {key.lower(): value for key, value in response.headers.items()}
            raise_for_grpc_status(header_status)
            decoder = FrameDecoder()
            async for chunk in response.aiter_bytes():
                for frame in decoder.feed(chunk):
                    if frame.is_trailer:
                        raise_for_grpc_status(parse_trailers(frame.payload))
                        return
                    yield frame.payload
            decoder.finish()

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

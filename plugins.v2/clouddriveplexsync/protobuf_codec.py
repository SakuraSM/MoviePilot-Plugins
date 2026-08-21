"""Small protobuf wire codec for the CloudDrive RPCs used by this plugin.

The plugin deliberately avoids grpcio/protobuf runtime dependencies.  It only
needs a handful of scalar/nested fields and preserves the raw CloudAPIConfig
message while replacing field 5 (maxBufferPoolSizeMB).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator, List, Optional, Tuple


class ProtobufError(ValueError):
    """Raised for malformed protobuf input."""


def encode_varint(value: int) -> bytes:
    if value < 0:
        raise ValueError("negative varints are not supported")
    output = bytearray()
    while value > 0x7F:
        output.append((value & 0x7F) | 0x80)
        value >>= 7
    output.append(value)
    return bytes(output)


def decode_varint(data: bytes, offset: int = 0) -> Tuple[int, int]:
    value = 0
    shift = 0
    for index in range(offset, len(data)):
        byte = data[index]
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, index + 1
        shift += 7
        if shift >= 70:
            raise ProtobufError("varint is too long")
    raise ProtobufError("truncated varint")


def encode_key(field_number: int, wire_type: int) -> bytes:
    if field_number <= 0:
        raise ValueError("field number must be positive")
    return encode_varint((field_number << 3) | wire_type)


def encode_uint(field_number: int, value: int) -> bytes:
    return encode_key(field_number, 0) + encode_varint(value)


def encode_bool(field_number: int, value: bool) -> bytes:
    return encode_uint(field_number, 1 if value else 0)


def encode_bytes(field_number: int, value: bytes) -> bytes:
    return encode_key(field_number, 2) + encode_varint(len(value)) + value


def encode_string(field_number: int, value: str) -> bytes:
    return encode_bytes(field_number, value.encode("utf-8"))


def encode_message(field_number: int, value: bytes) -> bytes:
    return encode_bytes(field_number, value)


@dataclass(frozen=True)
class Field:
    number: int
    wire_type: int
    value: object
    start: int
    end: int


def iter_fields(data: bytes) -> Iterator[Field]:
    offset = 0
    while offset < len(data):
        start = offset
        key, offset = decode_varint(data, offset)
        number = key >> 3
        wire_type = key & 0x07
        if number <= 0:
            raise ProtobufError("invalid field number")

        if wire_type == 0:
            value, offset = decode_varint(data, offset)
        elif wire_type == 1:
            end = offset + 8
            if end > len(data):
                raise ProtobufError("truncated fixed64")
            value = data[offset:end]
            offset = end
        elif wire_type == 2:
            length, offset = decode_varint(data, offset)
            end = offset + length
            if end > len(data):
                raise ProtobufError("truncated length-delimited field")
            value = data[offset:end]
            offset = end
        elif wire_type == 5:
            end = offset + 4
            if end > len(data):
                raise ProtobufError("truncated fixed32")
            value = data[offset:end]
            offset = end
        else:
            raise ProtobufError(f"unsupported wire type {wire_type}")
        yield Field(number=number, wire_type=wire_type, value=value, start=start, end=offset)


def varint_field(data: bytes, field_number: int, default: Optional[int] = None) -> Optional[int]:
    result = default
    for field in iter_fields(data):
        if field.number == field_number and field.wire_type == 0:
            result = int(field.value)
    return result


def bool_field(data: bytes, field_number: int, default: bool = False) -> bool:
    return bool(varint_field(data, field_number, 1 if default else 0))


def bytes_fields(data: bytes, field_number: int) -> List[bytes]:
    return [
        bytes(field.value)
        for field in iter_fields(data)
        if field.number == field_number and field.wire_type == 2
    ]


def bytes_field(data: bytes, field_number: int, default: bytes = b"") -> bytes:
    values = bytes_fields(data, field_number)
    return values[-1] if values else default


def string_field(data: bytes, field_number: int, default: str = "") -> str:
    raw = bytes_field(data, field_number)
    return raw.decode("utf-8") if raw else default


def patch_varint_field(data: bytes, field_number: int, value: int) -> bytes:
    """Replace a scalar varint while preserving every unrelated raw field."""

    output = bytearray()
    for field in iter_fields(data):
        if field.number != field_number:
            output.extend(data[field.start:field.end])
    output.extend(encode_uint(field_number, value))
    return bytes(output)


def encode_list_sub_file_request(path: str, force_refresh: bool = True) -> bytes:
    return encode_string(1, path) + encode_bool(2, force_refresh)


def encode_get_cloud_config_request(cloud_name: str, username: str) -> bytes:
    return encode_string(1, cloud_name) + encode_string(2, username)


def encode_set_cloud_config_request(cloud_name: str, username: str, config: bytes) -> bytes:
    return (
        encode_string(1, cloud_name)
        + encode_string(2, username)
        + encode_message(3, config)
    )

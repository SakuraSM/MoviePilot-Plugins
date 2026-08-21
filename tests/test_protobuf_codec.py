from __future__ import annotations

import unittest

import _bootstrap  # noqa: F401
from clouddriveplexsync.cd2_client import CloudDriveClient
from clouddriveplexsync.models import ChangeType
from clouddriveplexsync.protobuf_codec import (
    encode_bool,
    encode_message,
    encode_string,
    encode_uint,
    patch_varint_field,
    string_field,
    varint_field,
)


class ProtobufCodecTests(unittest.TestCase):
    def test_patch_preserves_unknown_fields(self) -> None:
        raw = encode_uint(1, 4) + encode_uint(5, 128) + encode_string(99, "future")
        patched = patch_varint_field(raw, 5, 2)
        self.assertEqual(varint_field(patched, 1), 4)
        self.assertEqual(varint_field(patched, 5), 2)
        self.assertEqual(string_field(patched, 99), "future")

    def test_parse_filesystem_rename(self) -> None:
        change = (
            encode_uint(1, int(ChangeType.RENAME))
            + encode_bool(2, False)
            + encode_string(3, "/光鸭云盘/old.mkv")
            + encode_string(4, "/光鸭云盘/new.mkv")
        )
        push = encode_uint(1, 4) + encode_message(5, change)
        parsed = CloudDriveClient._parse_push(push)
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.kind, "filesystem")
        self.assertEqual(parsed.value.change_type, ChangeType.RENAME)
        self.assertEqual(parsed.value.new_path, "/光鸭云盘/new.mkv")

    def test_parse_transfer_status_version(self) -> None:
        status = encode_string(3, "1.0.14")
        push = encode_uint(1, 0) + encode_message(2, status)
        parsed = CloudDriveClient._parse_push(push)
        self.assertEqual(parsed.kind, "status")
        self.assertEqual(parsed.value["version"], "1.0.14")

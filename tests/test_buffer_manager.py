from __future__ import annotations

import asyncio
import unittest

import _bootstrap  # noqa: F401
from clouddriveplexsync.buffer_manager import BufferManager, BufferSettings
from clouddriveplexsync.cd2_client import CloudConfig
from clouddriveplexsync.models import CloudApi
from clouddriveplexsync.protobuf_codec import encode_string, encode_uint, varint_field


class FakeClient:
    def __init__(self, buffer_mb: int = 128) -> None:
        self.buffer_mb = buffer_mb
        self.set_values = []
        self.get_count = 0

    async def get_cloud_config(self, cloud_name, username):
        self.get_count += 1
        raw = encode_uint(5, self.buffer_mb) + encode_string(99, "preserved")
        return CloudConfig(raw=raw, buffer_mb=self.buffer_mb, buffer_limit_mb=256)

    async def set_cloud_buffer(self, cloud_name, username, raw, buffer_mb):
        self.assert_preserved = raw
        self.buffer_mb = buffer_mb
        self.set_values.append(buffer_mb)


class BufferManagerTests(unittest.TestCase):
    cloud = CloudApi(name="GuangYaPan", username="u", nickname="光鸭云盘", path="/光鸭云盘")

    def test_disabled_mode_never_calls_config_api(self) -> None:
        client = FakeClient()
        manager = BufferManager(
            client,
            BufferSettings(mode="disabled"),
            playback_probe=lambda _: False,
            scan_probe=lambda _: False,
        )
        self.assertTrue(asyncio.run(manager.prepare(self.cloud, ["/plex"], ["1"])))
        self.assertEqual(client.set_values, [])
        preview = asyncio.run(manager.preview(self.cloud, ["/plex"]))
        self.assertIsNone(preview["current_mb"])
        self.assertEqual(client.get_count, 0)

    def test_minimum_applies_to_global_and_override_values(self) -> None:
        settings = BufferSettings(mode="adaptive", min_mb=4, scan_mb=1, playback_mb=2)
        self.assertEqual(settings.scan_mb, 4)
        self.assertEqual(settings.playback_mb, 4)

    def test_adaptive_idle_applies_and_restores(self) -> None:
        now = [100.0]
        client = FakeClient()
        manager = BufferManager(
            client,
            BufferSettings(mode="adaptive", scan_mb=2, playback_mb=8, restore_grace_seconds=60),
            playback_probe=lambda _: False,
            scan_probe=lambda _: False,
            clock=lambda: now[0],
        )
        asyncio.run(manager.prepare(self.cloud, ["/plex"], ["1"]))
        self.assertEqual(client.set_values, [2])
        self.assertEqual(varint_field(client.assert_preserved, 5), 128)
        now[0] = 161
        asyncio.run(manager.restore_due())
        self.assertEqual(client.set_values, [2, 128])

    def test_active_playback_uses_playback_value(self) -> None:
        client = FakeClient()
        manager = BufferManager(
            client,
            BufferSettings(mode="adaptive", scan_mb=2, playback_mb=8),
            playback_probe=lambda _: True,
            scan_probe=lambda _: False,
        )
        asyncio.run(manager.prepare(self.cloud, ["/plex"], ["1"]))
        self.assertEqual(client.set_values, [8])

    def test_manual_change_is_not_overwritten_on_restore(self) -> None:
        client = FakeClient()
        manager = BufferManager(
            client,
            BufferSettings(mode="fixed", scan_mb=2),
            playback_probe=lambda _: False,
            scan_probe=lambda _: False,
        )
        asyncio.run(manager.prepare(self.cloud, ["/plex"], ["1"]))
        client.buffer_mb = 16
        asyncio.run(manager.restore_all())
        self.assertEqual(client.set_values, [2])
        self.assertEqual(client.buffer_mb, 16)

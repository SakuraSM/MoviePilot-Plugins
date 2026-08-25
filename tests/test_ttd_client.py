from __future__ import annotations

import asyncio
import unittest

import _bootstrap  # noqa: F401
from clouddriveplexsync.models import PluginStats
from clouddriveplexsync.ttd_client import (
    TTDAuthenticationError,
    TTDClient,
    TTDCursor,
    TTDHistoryGapError,
    TTDHistoryPoller,
    TTDHistoryRecord,
    TTDHistoryPage,
    TTDProtocolError,
    parse_history_payload,
)


class FakeResponse:
    def __init__(self, status_code=200, payload=None, content_type="application/json"):
        self.status_code = status_code
        self._payload = payload
        self.headers = {"content-type": content_type}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


class FakeHTTPClient:
    def __init__(self, response):
        self.response = response
        self.calls = []

    async def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.response


class FakeHistoryClient:
    def __init__(self, pages, page_size=20):
        self.pages = pages
        self.page_size = page_size
        self.calls = []
        self.base_url = "https://ttd.example"
        self.source = "光鸭云盘"

    async def fetch_page(self, page):
        self.calls.append(page)
        records = list(self.pages.get(page, []))
        return TTDHistoryPage(records=records, item_count=len(records))


def record(record_id, target):
    return TTDHistoryRecord(
        key=f"id:{record_id}",
        record_id=str(record_id),
        target_path=target,
        completed_at=f"2026-08-25 12:00:{record_id}",
    )


class TTDParserTests(unittest.TestCase):
    def test_real_ttd_source_data_shape_is_parsed(self):
        records = parse_history_payload(
            {
                "available_sources": ["123云盘", "光鸭云盘"],
                "generated_at": 1787634209,
                "has_next": True,
                "limit": 20,
                "page": 1,
                "source": "光鸭云盘",
                "source_data": {
                    "all_total": 269825,
                    "error": None,
                    "records": [
                        {
                            "created_at": "2026-08-25 04:04:44",
                            "event_time": "2026-08-25 04:04:44",
                            "file_id": "1939073661111193702",
                            "file_name": "穹庐下的魔女.2026.S01E09.mkv",
                            "id": 269364,
                            "source": "光鸭云盘",
                            "status": "success",
                            "target_path": "动漫 / 日韩动漫 / 穹庐下的魔女 (2026) {tmdb-288971} / Season 1",
                        }
                    ],
                },
            }
        )
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].key, "id:269364")
        self.assertEqual(records[0].completed_at, "2026-08-25 04:04:44")
        self.assertEqual(
            records[0].target_path,
            "动漫/日韩动漫/穹庐下的魔女 (2026) {tmdb-288971}/Season 1",
        )

    def test_nested_payload_and_relative_target_are_parsed(self):
        records = parse_history_payload(
            {
                "success": True,
                "data": {
                    "items": [
                        {
                            "id": 42,
                            "status": "success",
                            "target_path": "动漫 / 变形金刚 / Season 1",
                            "completed_at": "2026-08-25 12:00:00",
                            "source": "光鸭云盘",
                        },
                        {"id": 43, "status": "failed", "target_path": "电影/失败"},
                    ]
                },
            }
        )
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].key, "id:42")
        self.assertEqual(records[0].target_path, "动漫/变形金刚/Season 1")

    def test_missing_id_uses_stable_fingerprint(self):
        payload = {
            "records": [
                {
                    "status": "success",
                    "destination_path": "电影/片名",
                    "finished_at": "2026-08-25 12:00:00",
                }
            ]
        }
        first = parse_history_payload(payload)[0]
        second = parse_history_payload(payload)[0]
        self.assertTrue(first.key.startswith("sha256:"))
        self.assertEqual(first.key, second.key)

    def test_nonempty_unknown_record_shape_fails_closed_in_client_parser(self):
        # The low-level parser skips unsupported rows; TTDClient checks the raw count.
        self.assertEqual(parse_history_payload({"items": [{"id": 1, "status": "success"}]}), [])


class TTDClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_cookie_is_sent_without_being_added_to_url(self):
        transport = FakeHTTPClient(
            FakeResponse(payload={"success": True, "items": []})
        )
        stats = PluginStats()
        client = TTDClient(
            "https://ttd.example",
            "Cookie: session=secret",
            "光鸭云盘",
            stats=stats,
            client=transport,
        )
        self.assertEqual((await client.fetch_page(1)).records, [])
        url, kwargs = transport.calls[0]
        self.assertNotIn("secret", url)
        self.assertEqual(kwargs["headers"]["cookie"], "session=secret")
        self.assertEqual(kwargs["params"]["status"], "success")
        self.assertEqual(stats.ttd_poll_requests, 1)

    async def test_unauthorized_response_is_classified_as_cookie_failure(self):
        transport = FakeHTTPClient(FakeResponse(status_code=401, payload={"success": False}))
        client = TTDClient("https://ttd.example", "session=expired", "光鸭云盘", client=transport)
        with self.assertRaises(TTDAuthenticationError):
            await client.fetch_page(1)

    async def test_full_history_endpoint_can_be_pasted_as_address(self):
        transport = FakeHTTPClient(FakeResponse(payload={"success": True, "items": []}))
        client = TTDClient(
            "https://ttd.example/api/organize-history?source=old&page=9",
            "session=secret",
            "光鸭云盘",
            client=transport,
        )
        await client.fetch_page(1)
        self.assertEqual(transport.calls[0][0], "https://ttd.example/api/organize-history")

    async def test_unknown_nonempty_record_shape_is_rejected(self):
        transport = FakeHTTPClient(
            FakeResponse(payload={"success": True, "items": [{"id": 1, "status": "success"}]})
        )
        client = TTDClient("https://ttd.example", "session=secret", "光鸭云盘", client=transport)
        with self.assertRaises(TTDProtocolError):
            await client.fetch_page(1)

    async def test_real_shape_exposes_server_pagination_flag(self):
        transport = FakeHTTPClient(
            FakeResponse(
                payload={
                    "has_next": False,
                    "source_data": {
                        "error": None,
                        "records": [
                            {
                                "id": 269364,
                                "status": "success",
                                "source": "光鸭云盘",
                                "created_at": "2026-08-25 04:04:44",
                                "target_path": "动漫 / 日韩动漫 / 片名 / Season 1",
                            }
                        ],
                    },
                }
            )
        )
        client = TTDClient("https://ttd.example", "session=secret", "光鸭云盘", client=transport)
        page = await client.fetch_page(1)
        self.assertFalse(page.has_next)
        self.assertEqual(page.records[0].record_id, "269364")

    async def test_source_data_error_is_rejected(self):
        transport = FakeHTTPClient(
            FakeResponse(payload={"source_data": {"error": "database busy", "records": []}})
        )
        client = TTDClient("https://ttd.example", "session=secret", "光鸭云盘", client=transport)
        with self.assertRaises(TTDProtocolError):
            await client.fetch_page(1)


class TTDPollerTests(unittest.IsolatedAsyncioTestCase):
    async def test_first_poll_establishes_baseline_without_scanning(self):
        client = FakeHistoryClient({1: [record("2", "电影/B"), record("1", "电影/A")]})
        submitted = []
        persisted = []
        poller = TTDHistoryPoller(
            client,
            lambda path, source: submitted.append((path, source)) or True,
            "/光鸭云盘/Media/Video/已整理",
            persist_cursor=persisted.append,
        )
        self.assertEqual(await poller.poll_once(), 0)
        self.assertEqual(submitted, [])
        self.assertTrue(persisted[-1]["initialized"])
        self.assertEqual(set(persisted[-1]["seen_keys"]), {"id:1", "id:2"})

    async def test_new_records_are_oldest_first_and_same_directory_is_deduplicated(self):
        client = FakeHistoryClient(
            {
                1: [
                    record("3", "剧集/节目/Season 1"),
                    record("2", "剧集/节目/Season 1"),
                    record("1", "剧集/旧节目/Season 1"),
                ]
            }
        )
        submitted = []

        async def submit(path, source):
            submitted.append((path, source))
            return True

        poller = TTDHistoryPoller(
            client,
            submit,
            "/光鸭云盘/Media/Video/已整理",
            cursor=TTDCursor(initialized=True, seen_keys=["id:1"]),
        )
        self.assertEqual(await poller.poll_once(), 1)
        self.assertEqual(
            submitted,
            [("/光鸭云盘/Media/Video/已整理/剧集/节目/Season 1", "ttd")],
        )
        self.assertEqual(poller.cursor.seen_keys[:2], ["id:3", "id:2"])

    async def test_cursor_scope_change_establishes_a_new_baseline(self):
        client = FakeHistoryClient({1: [record("2", "电影/B")]})
        client.base_url = "https://ttd.example"
        client.source = "光鸭云盘"
        submitted = []
        poller = TTDHistoryPoller(
            client,
            lambda path, source: submitted.append((path, source)) or True,
            "/光鸭云盘/Media/Video/已整理",
            cursor=TTDCursor(initialized=True, scope="old-scope", seen_keys=["id:1"]),
        )
        self.assertEqual(await poller.poll_once(), 0)
        self.assertEqual(submitted, [])
        self.assertNotEqual(poller.cursor.scope, "old-scope")

    async def test_missing_cursor_at_page_limit_fails_closed(self):
        client = FakeHistoryClient(
            {1: [record("3", "电影/C")], 2: [record("2", "电影/B")]},
            page_size=1,
        )
        submitted = []
        poller = TTDHistoryPoller(
            client,
            lambda path, source: submitted.append((path, source)) or True,
            "/光鸭云盘/Media/Video/已整理",
            max_pages=2,
            cursor=TTDCursor(initialized=True, seen_keys=["id:1"]),
        )
        with self.assertRaises(TTDHistoryGapError):
            await poller.poll_once()
        self.assertEqual(submitted, [])
        self.assertEqual(poller.cursor.seen_keys, ["id:1"])

    async def test_target_traversal_is_rejected(self):
        errors = []
        poller = TTDHistoryPoller(
            FakeHistoryClient({1: [record("2", "../Private")]}, page_size=20),
            lambda _path, _source: True,
            "/光鸭云盘/Media/Video/已整理",
            cursor=TTDCursor(initialized=True, seen_keys=[]),
            initial_mode="replay_latest",
            on_error=errors.append,
        )
        self.assertEqual(await poller.poll_once(), 0)
        self.assertIn("parent path segments", errors[0])


if __name__ == "__main__":
    unittest.main()

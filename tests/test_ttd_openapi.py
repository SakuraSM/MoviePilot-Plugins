from __future__ import annotations

import unittest

import _bootstrap  # noqa: F401
from clouddriveplexsync.models import PluginStats
from clouddriveplexsync.ttd_client import (
    TTDAuthenticationError,
    TTDClient,
    TTDCursor,
    TTDHistoryPoller,
    normalize_ttd_api_token,
)
from test_ttd_client import FakeHTTPClient, FakeResponse


class TTDOpenAPITests(unittest.IsolatedAsyncioTestCase):
    async def test_bearer_auth_queries_successful_history_without_cookie(self) -> None:
        transport = FakeHTTPClient(
            FakeResponse(
                payload={
                    "success": True,
                    "has_next": True,
                    "source_data": {
                        "error": None,
                        "records": [
                            {
                                "id": 42,
                                "status": "success",
                                "target_path": "电影 / 片名",
                                "created_at": "2026-10-12 00:00:00",
                                "source": "光鸭云盘",
                            }
                        ],
                    },
                }
            )
        )
        client = TTDClient(
            "https://ttd.example/prefix/api/organize-history?source=old&page=9",
            source="光鸭云盘",
            api_token="secret-token",
            page_size=2,
            client=transport,
        )

        page = await client.fetch_page(2)

        self.assertEqual(page.records[0].key, "id:42")
        self.assertEqual(page.records[0].target_path, "电影/片名")
        self.assertTrue(page.has_next)
        url, request = transport.calls[0]
        self.assertEqual(url, "https://ttd.example/prefix/api/organize-history")
        self.assertEqual(
            request["params"],
            {"source": "光鸭云盘", "status": "success", "page": 2, "limit": 2},
        )
        self.assertEqual(request["headers"]["authorization"], "Bearer secret-token")
        self.assertNotIn("cookie", request["headers"])
        self.assertNotIn("secret-token", url)
        self.assertNotIn("secret-token", str(request["params"]))

    async def test_rejected_token_does_not_retry_with_cookie(self) -> None:
        for status_code in (301, 302, 303, 307, 308, 401, 403):
            with self.subTest(status_code=status_code):
                transport = FakeHTTPClient(FakeResponse(status_code=status_code))
                stats = PluginStats()
                client = TTDClient(
                    "https://ttd.example",
                    source="光鸭云盘",
                    api_token="secret-token",
                    stats=stats,
                    client=transport,
                )

                with self.assertRaises(TTDAuthenticationError) as raised:
                    await client.fetch_page(1)

                self.assertIn("API Token", str(raised.exception))
                self.assertNotIn("secret-token", str(raised.exception))
                self.assertEqual(stats.ttd_auth_failures, 1)
                self.assertEqual(len(transport.calls), 1)
                self.assertNotIn("cookie", transport.calls[0][1]["headers"])

    async def test_unauthorized_json_is_classified_without_exposing_token(self) -> None:
        transport = FakeHTTPClient(
            FakeResponse(
                payload={"success": False, "error": "unauthorized secret-token"}
            )
        )
        client = TTDClient(
            "https://ttd.example", api_token="secret-token", client=transport
        )

        with self.assertRaises(TTDAuthenticationError) as raised:
            await client.fetch_page(1)

        self.assertNotIn("secret-token", str(raised.exception))

    def test_token_normalization_accepts_raw_tokens_and_bearer_headers(self) -> None:
        for value in (
            "secret-token",
            "  secret-token  ",
            "Bearer secret-token",
            "bearer  secret-token",
            "Authorization: Bearer secret-token",
        ):
            with self.subTest(value=value):
                self.assertEqual(normalize_ttd_api_token(value), "secret-token")

    def test_invalid_token_is_rejected(self) -> None:
        for value in (
            "secret-token\n",
            "secret-token\r\nCookie: injected",
            "Bearer ",
            "secret token",
            "不是Token",
        ):
            with self.subTest(value=value):
                with self.assertRaises(ValueError) as raised:
                    TTDClient(
                        "https://ttd.example",
                        api_token=value,
                        client=FakeHTTPClient(FakeResponse()),
                    )
                self.assertNotIn("secret-token", str(raised.exception))

    def test_missing_both_credentials_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "API Token"):
            TTDClient("https://ttd.example", client=FakeHTTPClient(FakeResponse()))

    async def test_token_rotation_preserves_cursor(self) -> None:
        transport = FakeHTTPClient(
            FakeResponse(
                payload={
                    "has_next": False,
                    "records": [{"id": 42, "target_path": "电影/片名"}],
                }
            )
        )
        legacy = TTDClient(
            "https://ttd.example",
            source="光鸭云盘",
            api_token="old-token",
            client=transport,
        )
        root = "/光鸭云盘/Media/Video/已整理"
        legacy_poller = TTDHistoryPoller(legacy, lambda *_args: True, root)
        cursor = TTDCursor(
            initialized=True, scope=legacy_poller.cursor_scope, seen_keys=["id:42"]
        )
        submitted = []
        for token in ("first-token", "rotated-token"):
            client = TTDClient(
                "https://ttd.example",
                source="光鸭云盘",
                api_token=token,
                client=transport,
            )
            poller = TTDHistoryPoller(
                client,
                lambda path, source: submitted.append((path, source)) or True,
                root,
                cursor=cursor,
            )

            self.assertEqual(await poller.poll_once(), 0)
            self.assertEqual(poller.cursor.scope, legacy_poller.cursor_scope)
            self.assertEqual(poller.cursor.seen_keys, ["id:42"])

        self.assertEqual(submitted, [])


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import _bootstrap  # noqa: F401
from clouddriveplexsync.activity_log import (
    ActivityEntry,
    ActivityJournal,
    read_offline_activity,
)
from clouddriveplexsync.control_ui import build_page
from test_plugin_contract import load_plugin_module


def submitted_log(path: str) -> str:
    return "SCAN_SUBMITTED " + json.dumps(
        {
            "source": "ttd",
            "ttd_path": "/cloud/show/Season 1",
            "plex_path": path,
            "section_id": "1",
            "section_title": "动漫",
            "section_root": "/plex",
        },
        ensure_ascii=False,
    )


class ActivityJournalTests(unittest.IsolatedAsyncioTestCase):
    async def test_retains_recent_entries_after_restart_and_limits_growth(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "activity.sqlite3"
            journal = await ActivityJournal.open(path, capacity=12)
            for index in range(15):
                journal.append(ActivityEntry(float(index), "info", f"entry-{index}"))
            await journal.close()
            reopened = await ActivityJournal.open(path, capacity=12)
            try:
                records = await reopened.recent()
                self.assertEqual(len(records), 12)
                self.assertEqual(records[0].message, "entry-14")
                self.assertEqual(records[-1].message, "entry-3")
            finally:
                await reopened.close()
            offline = read_offline_activity(path)
            self.assertEqual(offline["activities"][0]["message"], "entry-14")
            self.assertEqual(offline["pending_verifications"], 0)

    async def test_offline_reader_does_not_create_a_database(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "missing.sqlite3"
            self.assertEqual(read_offline_activity(path), {})
            self.assertFalse(path.exists())

    async def test_disabled_plugin_page_reads_saved_logs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "activity.sqlite3"
            journal = await ActivityJournal.open(path)
            journal.append(ActivityEntry(1000.0, "error", "之前的错误"))
            await journal.close()
            plugin = load_plugin_module().CloudDrivePlexSync()
            with patch.object(
                plugin, "get_data_path", return_value=Path(directory), create=True
            ):
                rendered = json.dumps(plugin.get_page(), ensure_ascii=False)
            self.assertIn("之前的错误", rendered)

    async def test_journal_failure_does_not_raise_into_scanning(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            errors: list[str] = []
            journal = await ActivityJournal.open(
                Path(directory) / "activity.sqlite3", on_error=errors.append
            )

            def fail(_batch: list[ActivityEntry]) -> None:
                raise OSError("storage unavailable")

            journal._write_batch = fail
            journal.append(ActivityEntry(1.0, "info", "scan event"))
            await journal.close()
            self.assertTrue(any("storage unavailable" in error for error in errors))


class ActivityPageTests(unittest.TestCase):
    def test_detail_page_paginates_and_labels_submitted_directories(self) -> None:
        activities = [
            {
                "timestamp": 1000.0 + index,
                "level": "info",
                "message": submitted_log(f"/plex/剧集 {index}/Season 1"),
            }
            for index in range(23)
        ]
        page = build_page({"ready": True, "activities": activities}, False)
        windows = [node for node in page if node.get("component") == "VWindow"]
        self.assertTrue(windows)
        self.assertEqual(len(windows[0]["content"]), 5)
        rendered = json.dumps(page, ensure_ascii=False)
        self.assertIn("剧集 22", rendered)
        self.assertIn("Plex 已接受扫描", rendered)
        self.assertNotIn("刮削成功", rendered)

    def test_indexed_and_unconfirmed_media_are_shown_separately(self) -> None:
        media = {
            "title": "节目",
            "season_num": 1,
            "episode_num": 2,
            "expected_path": "/plex/节目/Season 1/E02.mkv",
            "section_title": "动漫",
        }
        page = build_page(
            {
                "ready": True,
                "indexed_media": [media | {"confirmed_at": 1000.0}],
                "pending_media": [
                    media
                    | {"submitted_at": 900.0, "detail": "尚未在 Plex 中找到对应文件"}
                ],
                "pending_verifications": 1,
            },
            False,
        )
        rendered = json.dumps(page, ensure_ascii=False)
        self.assertIn("Plex 已确认收录", rendered)
        self.assertIn("等待 Plex 入库确认", rendered)
        self.assertIn("节目 S01E02", rendered)
        self.assertIn("尚未在 Plex 中找到对应文件", rendered)


if __name__ == "__main__":
    unittest.main()

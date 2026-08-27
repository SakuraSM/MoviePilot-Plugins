from __future__ import annotations

import unittest

import _bootstrap

from clouddriveplexsync.log_manager import LogSettings, RepeatedLogLimiter


class MutableClock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


class RepeatedLogLimiterTests(unittest.TestCase):
    def test_repeats_are_suppressed_then_reported_as_one_summary(self) -> None:
        clock = MutableClock()
        limiter = RepeatedLogLimiter(
            LogSettings(mode="normal", dedup_seconds=60, recent_limit=50),
            clock=clock,
        )

        first = limiter.record("ttd_refresh_failed", "目录 A 刷新失败")
        second = limiter.record("ttd_refresh_failed", "目录 B 刷新失败")
        clock.now += 61
        summary = limiter.record("ttd_refresh_failed", "目录 C 刷新失败")

        self.assertTrue(first.should_emit)
        self.assertFalse(second.should_emit)
        self.assertTrue(summary.should_emit)
        self.assertIn("已合并 1 条同类日志", summary.message)
        self.assertEqual(limiter.snapshot()["suppressed_total"], 1)

    def test_verbose_mode_emits_every_event(self) -> None:
        limiter = RepeatedLogLimiter(
            LogSettings(mode="verbose", dedup_seconds=300, recent_limit=50)
        )

        self.assertTrue(limiter.record("same", "first").should_emit)
        self.assertTrue(limiter.record("same", "second").should_emit)
        self.assertEqual(limiter.snapshot()["suppressed_total"], 0)

    def test_settings_are_bounded(self) -> None:
        settings = LogSettings(
            mode="quiet", dedup_seconds=9999, recent_limit=999
        ).normalized()

        self.assertEqual(settings.dedup_seconds, 3600)
        self.assertEqual(settings.recent_limit, 200)
        self.assertFalse(RepeatedLogLimiter(settings).should_emit_routine())


if __name__ == "__main__":
    unittest.main()

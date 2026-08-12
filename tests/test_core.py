import unittest
from datetime import datetime, timedelta, timezone

from app.collector import build_update
from app.core import (
    BEIJING,
    active_launch_events,
    incremental_window,
    query_groups,
    schedule_decision,
)


def event(day, *, source="官方确认", confirmed="已确认", brand="NYXI"):
    return {
        "事件名称": "Launch",
        "竞品品牌": brand,
        "正式开售日期": day,
        "来源类型": source,
        "人工确认状态": confirmed,
    }


class ScheduleTests(unittest.TestCase):
    def test_monday_runs_without_launch_event(self):
        now = datetime(2026, 9, 7, 9, 30, tzinfo=BEIJING)
        decision = schedule_decision(now, [], brand="NYXI")
        self.assertTrue(decision.should_run)
        self.assertEqual(decision.reason, "weekly_monday")

    def test_wednesday_and_friday_run_only_inside_confirmed_d0_plus_minus_30(self):
        wednesday = datetime(2026, 8, 12, 9, 30, tzinfo=BEIJING)
        friday = datetime(2026, 8, 14, 9, 30, tzinfo=BEIJING)
        rows = [event("2026-08-05 00:00:00")]
        self.assertTrue(schedule_decision(wednesday, rows, brand="NYXI").should_run)
        self.assertTrue(schedule_decision(friday, rows, brand="NYXI").should_run)
        self.assertFalse(
            schedule_decision(
                datetime(2026, 9, 9, 9, 30, tzinfo=BEIJING), rows, brand="NYXI"
            ).should_run
        )

    def test_launch_window_includes_both_30_day_boundaries(self):
        center = datetime(2026, 8, 5, 9, 30, tzinfo=BEIJING)
        for offset in (-30, 30):
            now = center + timedelta(days=offset)
            self.assertEqual(active_launch_events([event("2026-08-05 00:00:00")], now, brand="NYXI"), ("Launch",))

    def test_unconfirmed_or_nonofficial_event_never_enables_boost(self):
        now = datetime(2026, 8, 12, 9, 30, tzinfo=BEIJING)
        rows = [
            event("2026-08-05 00:00:00", source="AI推测待确认"),
            event("2026-08-05 00:00:00", confirmed="待确认"),
            event("2026-08-05 00:00:00", brand="Other"),
        ]
        self.assertFalse(schedule_decision(now, rows, brand="NYXI").should_run)


class IncrementalTests(unittest.TestCase):
    def test_window_overlaps_last_success_by_48_hours(self):
        now = datetime(2026, 8, 12, 1, 30, tzinfo=timezone.utc)
        start, end = incremental_window(
            {"最近成功采集时间": "2026-08-11T21:45:39+08:00"}, now
        )
        self.assertEqual(start, datetime(2026, 8, 9, 13, 45, 39, tzinfo=timezone.utc))
        self.assertEqual(end, now)

    def test_query_matrix_is_grouped_and_deduplicated(self):
        config = {
            "竞品品牌": "NYXI",
            "关键词别名": "NYXI, NYXI Gaming, nyxi hyperion",
            "排除词": "nyxi leon, lady nyxi",
            "产品系列词": "Hyperion\nWizard",
            "产品型号词": "Hyperion 3\nNJ12",
        }
        groups = query_groups(config, "nyxi")
        self.assertEqual(set(groups), {"brand", "series", "model"})
        self.assertIn("NYXI Hyperion 3", groups["model"])
        self.assertEqual(len(groups["brand"]), len(set(term.casefold() for term in groups["brand"])))

    def test_unchanged_row_does_not_refresh_capture_metadata(self):
        old = {
            "帖子标题": "Same",
            "曝光量": 100,
            "抓取时间": "old",
            "采集批次ID": "old-batch",
            "原始数据哈希": "old-hash",
        }
        incoming = dict(old, 抓取时间="new", 采集批次ID="new-batch", 原始数据哈希="new-hash")
        self.assertEqual(build_update(old, incoming), {})

    def test_changed_metric_adds_capture_metadata_but_preserves_review_fields(self):
        old = {"曝光量": 100, "合作信号": "A", "人工复核状态": "已确认"}
        incoming = {
            "曝光量": 110,
            "抓取时间": "2026-08-12 09:30:00",
            "采集批次ID": "batch",
            "原始数据哈希": "hash",
            "合作信号": "待分析",
            "人工复核状态": "待复核",
        }
        update = build_update(old, incoming)
        self.assertEqual(update["曝光量"], 110)
        self.assertNotIn("合作信号", update)
        self.assertNotIn("人工复核状态", update)


if __name__ == "__main__":
    unittest.main()


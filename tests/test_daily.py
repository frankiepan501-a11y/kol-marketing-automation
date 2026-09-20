import unittest
from datetime import datetime, timedelta, timezone

from app.clients import ApiError
from app.collector import is_recent_post, older_refresh_batch
from app.daily import DailyReporter, format_report


NOW = datetime(2026, 9, 21, 8, 30, tzinfo=timezone.utc)


class FakeFeishu:
    app_id = "cli_aa143b0a11b89be4"

    def __init__(self):
        self.config = {"最近采集水位": "2026-09-20 16:30:00"}
        self.sent = []
        self.rows = []
        self.fail_after_send = False

    def get_record(self, *_):
        return dict(self.config)

    def batch_update(self, _base, _table, updates):
        if self.fail_after_send and self.sent:
            raise ApiError("feishu", "write_failed", "simulated receipt failure")
        self.config.update(updates[0][1])

    def send_text_to_chat(self, chat_id, content):
        self.sent.append((chat_id, content))
        return "om_test_1"

    def list_records(self, *_):
        return self.rows


class DailyTests(unittest.TestCase):
    def test_recent_only_and_weekly_rotation(self):
        recent = {"发布时间": "2026-09-15 10:00:00", "帖子ID": "aaaaaaaaaaa"}
        old = [
            {"发布时间": "2026-01-01 10:00:00", "帖子ID": f"{i:011d}"}
            for i in range(205)
        ]
        self.assertTrue(is_recent_post(recent, NOW))
        self.assertFalse(is_recent_post(old[0], NOW))
        index, count, batch = older_refresh_batch([recent, *old], NOW)
        self.assertEqual((index, count, len(batch)), (0, 3, 100))
        index, count, batch = older_refresh_batch([recent, *old], NOW + timedelta(days=7))
        self.assertEqual((index, count, len(batch)), (1, 3, 100))

    def test_one_report_per_day(self):
        fake = FakeFeishu()
        reporter = DailyReporter(fake)
        reporter.begin(NOW)
        first = reporter.send_once(NOW, "测试", job_id="one")
        second = reporter.send_once(NOW, "测试", job_id="two")
        self.assertEqual(first["status"], "sent")
        self.assertEqual(second["status"], "already_sent")
        self.assertEqual(len(fake.sent), 1)

    def test_same_day_retry_uses_system_creation_field(self):
        fake = FakeFeishu()
        reporter = DailyReporter(fake)
        entry = reporter.begin(NOW)
        fake.rows = [
            {"竞品品牌": "NYXI", "平台": "YouTube", "帖子ID": "aaaaaaaaaaa",
             "唯一键": "5:aaaaaaaaaaa", "KOL平台ID": "channel-old",
             "创建时间": "2026-09-20T10:00:00Z"},
            {"竞品品牌": "NYXI", "平台": "YouTube", "帖子ID": "bbbbbbbbbbb",
             "唯一键": "5:bbbbbbbbbbb", "KOL平台ID": "channel-new",
             "创建时间": "2026-09-21T08:31:00Z"},
        ]
        self.assertEqual([row["帖子ID"] for row in reporter.today_posts(NOW, entry)], ["bbbbbbbbbbb"])
        self.assertEqual(reporter.known_channels_before(entry), {"channel-old"})

    def test_write_failure_after_send_keeps_unknown_and_never_resends(self):
        fake = FakeFeishu()
        reporter = DailyReporter(fake)
        reporter.begin(NOW)
        fake.fail_after_send = True
        with self.assertRaises(ApiError):
            reporter.send_once(NOW, "测试", job_id="one")
        with self.assertRaises(ApiError) as raised:
            reporter.send_once(NOW, "测试", job_id="two")
        self.assertEqual(raised.exception.code, "send_outcome_unknown")
        self.assertEqual(len(fake.sent), 1)

    def test_report_labels_failure_and_no_backfill(self):
        text = format_report(
            NOW, nyxi={"status": "failed", "error_type": "quotaExceeded", "job_id": "x"},
            backfill={"message": "NYXI 失败，未启动历史补采"}, posts=[],
        )
        self.assertIn("NYXI：采集失败", text)
        self.assertIn("未启动历史补采", text)

    def test_report_labels_internal_budget_as_non_project_wide(self):
        text = format_report(
            NOW,
            nyxi={"status": "completed", "search_calls": 7, "job_id": "x"},
            backfill={
                "message": "完成 1 个小段，新增 2 条帖子",
                "budget_limit": 100,
                "budget_reserve": 20,
                "budget_used": 17,
                "budget_remaining": 83,
            },
            posts=[],
        )
        self.assertIn("本次日任务已用 17/100 次", text)
        self.assertIn("不代表 Google 项目全局余量", text)


if __name__ == "__main__":
    unittest.main()

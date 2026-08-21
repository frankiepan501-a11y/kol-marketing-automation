import asyncio
import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

from app import launch_outcomes


class LaunchOutcomeParserTests(unittest.TestCase):
    def test_explicit_publish_commitment_requires_action_and_date(self):
        value = launch_outcomes.extract_explicit_commitment(
            "Yes, I can publish the review on September 15.",
            default_year=2026,
        )

        expected = int(datetime(2026, 9, 15, tzinfo=timezone.utc).timestamp() * 1000)
        self.assertEqual(expected, value)

    def test_quoted_campaign_date_is_not_treated_as_kol_commitment(self):
        value = launch_outcomes.extract_explicit_commitment(
            "Sounds good.\nOn Aug 21, Lisa wrote:\n"
            "We are coordinating coverage around September 15.",
            default_year=2026,
        )

        self.assertEqual(0, value)

    def test_campaign_date_in_a_different_clause_is_not_a_commitment_date(self):
        value = launch_outcomes.extract_explicit_commitment(
            "I can publish a review. Your campaign starts September 15, but I have not chosen my date.",
            default_year=2026,
        )

        self.assertEqual(0, value)

    def test_spanish_future_commitment_is_recognized(self):
        value = launch_outcomes.extract_explicit_commitment(
            "Publicaré el video el 15 de septiembre.", default_year=2026,
        )

        expected = int(datetime(2026, 9, 15, tzinfo=timezone.utc).timestamp() * 1000)
        self.assertEqual(expected, value)

    def test_channel_homepage_is_not_a_publication_link(self):
        value = launch_outcomes.extract_publication_url(
            "My channel is https://www.youtube.com/@IndieAlpaca", object_type="KOL",
        )

        self.assertEqual("", value)

    def test_live_link_extraction_ignores_quoted_product_link(self):
        value = launch_outcomes.extract_publication_url(
            "Here is the video: https://youtu.be/abc123\n"
            "On Aug 21, Lisa wrote:\nhttps://powkong.com/products/piranha",
            object_type="KOL",
        )

        self.assertEqual("https://youtu.be/abc123", value)


class LaunchOutcomeReconcileTests(unittest.TestCase):
    def setUp(self):
        launch_outcomes._DRAFT_CACHE_AT = 0.0
        launch_outcomes._DRAFT_CACHE_ROWS = []

    def test_draft_snapshot_reuses_one_full_scan_within_cache_window(self):
        rows = [{"record_id": "d1", "fields": {}}]
        fetch = AsyncMock(return_value=rows)
        async def exercise():
            first = await launch_outcomes.draft_snapshot()
            second = await launch_outcomes.draft_snapshot()
            return first, second

        with patch.object(launch_outcomes.config, "T_DRAFT", "drafts"), \
             patch.object(launch_outcomes.feishu, "fetch_all_records", new=fetch):
            first, second = asyncio.run(exercise())

        self.assertIs(first, second)
        self.assertEqual(1, fetch.await_count)

    def test_live_link_without_explicit_publish_date_writes_link_but_not_actual_time(self):
        participant = {"record_id": "p1", "fields": {
            "活动ID": "campaign1", "对象类型": "KOL", "参与状态": "已入围",
            "审核结论": "通过", "关联邮件草稿": ["d1"],
        }}
        draft = {"record_id": "d1", "fields": {
            "发送状态": "已发送", "发送时间": 1770000000000,
            "是否回复": True, "回复日期": 1788000000000,
            "场景标签": "live_link_received",
            "回复原文": "Here is the video: https://youtu.be/abc123",
        }}
        readback = {"record_id": "p1", "fields": {
            "上稿链接": {"link": "https://youtu.be/abc123", "text": "打开上稿内容"},
        }}
        with patch.object(launch_outcomes.config, "T_LAUNCH_PARTICIPANT", "participants"), \
             patch.object(launch_outcomes.feishu, "update_record", new=AsyncMock()) as update, \
             patch.object(launch_outcomes.feishu, "get_record", new=AsyncMock(return_value=readback)):
            result = asyncio.run(launch_outcomes.reconcile_campaign(
                "campaign1", dry_run=False,
                activity={"record_id": "a1", "fields": {}},
                participants=[participant], drafts=[draft],
            ))

        self.assertEqual(1, result["links_written"])
        self.assertEqual(0, result["actuals_written"])
        self.assertNotIn("实际上稿时间", update.await_args.args[2])

    def test_dry_run_never_reports_updates_as_written(self):
        participant = {"record_id": "p1", "fields": {
            "活动ID": "campaign1", "对象类型": "KOL", "参与状态": "已入围",
            "审核结论": "通过", "关联邮件草稿": ["d1"],
        }}
        draft = {"record_id": "d1", "fields": {
            "发送状态": "已发送", "发送时间": 1770000000000,
            "是否回复": True, "回复日期": 1788000000000,
            "场景标签": "live_link_received",
            "回复原文": "Here is the video: https://youtu.be/abc123",
        }}
        with patch.object(launch_outcomes.config, "T_LAUNCH_PARTICIPANT", "participants"):
            result = asyncio.run(launch_outcomes.reconcile_campaign(
                "campaign1", dry_run=True,
                activity={"record_id": "a1", "fields": {}},
                participants=[participant], drafts=[draft],
            ))

        self.assertEqual(1, result["updates_planned"])
        self.assertEqual(0, result["updates_written"])

    def test_write_is_not_counted_when_readback_does_not_match(self):
        participant = {"record_id": "p1", "fields": {
            "活动ID": "campaign1", "对象类型": "KOL", "参与状态": "已入围",
            "审核结论": "通过", "关联邮件草稿": ["d1"],
        }}
        draft = {"record_id": "d1", "fields": {
            "发送状态": "已发送", "发送时间": 1770000000000,
            "是否回复": True, "回复日期": 1788000000000,
            "场景标签": "live_link_received",
            "回复原文": "Here is the video: https://youtu.be/abc123",
        }}
        with patch.object(launch_outcomes.config, "T_LAUNCH_PARTICIPANT", "participants"), \
             patch.object(launch_outcomes.feishu, "update_record", new=AsyncMock()), \
             patch.object(
                 launch_outcomes.feishu, "get_record",
                 new=AsyncMock(return_value={"record_id": "p1", "fields": {}}),
             ):
            result = asyncio.run(launch_outcomes.reconcile_campaign(
                "campaign1", dry_run=False,
                activity={"record_id": "a1", "fields": {}},
                participants=[participant], drafts=[draft],
            ))

        self.assertEqual(0, result["updates_written"])
        self.assertEqual(1, len(result["errors"]))

    def test_reply_evidence_updates_commitment_and_actual_upload(self):
        expected = int(datetime(2026, 9, 15, tzinfo=timezone.utc).timestamp() * 1000)
        participant = {"record_id": "p1", "fields": {
            "活动ID": "campaign1", "对象类型": "KOL", "参与状态": "已入围",
            "审核结论": "通过",
            "关联KOL": {"link_record_ids": ["k1"]},
            "关联邮件草稿": {"link_record_ids": ["d1"]},
        }}
        draft = {"record_id": "d1", "fields": {
            "发送状态": "已发送", "发送时间": 1770000000000,
            "是否回复": True, "回复日期": expected + 86400000,
            "场景标签": "live_link_received",
            "回复原文": (
                "I can publish on September 15. I published it on September 15. "
                "The video is live here: https://youtube.com/watch?v=abc123"
            ),
        }}
        activity = {"record_id": "a1", "fields": {
            "活动ID": "campaign1", "窗口开始": 1789000000000,
            "窗口结束": 1791000000000,
        }}

        with patch.object(launch_outcomes.config, "T_LAUNCH_PARTICIPANT", "participants"), \
             patch.object(
                 launch_outcomes.feishu, "update_record",
                 new=AsyncMock(return_value={"record_id": "p1"}),
             ) as update, patch.object(
                 launch_outcomes.feishu, "get_record",
                 new=AsyncMock(return_value={"record_id": "p1", "fields": {
                     "承诺上稿时间": expected,
                     "实际上稿时间": expected,
                     "上稿链接": {"link": "https://youtube.com/watch?v=abc123", "text": "打开上稿内容"},
                 }}),
             ):
            result = asyncio.run(launch_outcomes.reconcile_campaign(
                "campaign1", dry_run=False, activity=activity,
                participants=[participant], drafts=[draft],
            ))

        self.assertEqual(1, result["commitments_written"])
        self.assertEqual(1, result["actuals_written"])
        fields = update.await_args.args[2]
        self.assertTrue(fields["承诺上稿时间"])
        self.assertEqual(expected, fields["实际上稿时间"])
        self.assertEqual(
            "https://youtube.com/watch?v=abc123", fields["上稿链接"]["link"],
        )

    def test_generic_interest_does_not_create_a_commitment(self):
        participant = {"record_id": "p1", "fields": {
            "活动ID": "campaign1", "对象类型": "KOL", "参与状态": "已入围",
            "审核结论": "通过",
            "关联KOL": {"link_record_ids": ["k1"]},
            "关联邮件草稿": {"link_record_ids": ["d1"]},
        }}
        draft = {"record_id": "d1", "fields": {
            "发送状态": "已发送", "发送时间": 1770000000000,
            "是否回复": True, "回复日期": 1788000000000,
            "场景标签": "interested_general",
            "回复原文": "Thanks, this looks interesting. Please send more details.",
        }}

        with patch.object(launch_outcomes.config, "T_LAUNCH_PARTICIPANT", "participants"), \
             patch.object(
                 launch_outcomes.feishu, "update_record", new=AsyncMock(),
             ) as update:
            result = asyncio.run(launch_outcomes.reconcile_campaign(
                "campaign1", dry_run=False,
                activity={"record_id": "a1", "fields": {}},
                participants=[participant], drafts=[draft],
            ))

        self.assertEqual(0, result["commitments_written"])
        self.assertEqual(0, result["actuals_written"])
        update.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()

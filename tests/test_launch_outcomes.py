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

    def test_german_and_spanish_quoted_threads_are_not_treated_as_new_facts(self):
        german = launch_outcomes.extract_explicit_commitment(
            "Danke.\nAm 20. August schrieb Lisa:\n"
            "Ich werde am 15. September veröffentlichen.",
            default_year=2026,
        )
        spanish = launch_outcomes.extract_publication_url(
            "Gracias.\nEl 20 de agosto Lisa escribió:\n"
            "Publiqué el 15 de septiembre: https://youtu.be/quoted123",
            object_type="KOL",
        )

        self.assertEqual(0, german)
        self.assertEqual("", spanish)

    def test_editor_homepage_or_section_is_not_a_publication_link(self):
        self.assertEqual("", launch_outcomes.extract_publication_url(
            "Published August 21. See https://example.com/", object_type="媒体人",
        ))
        self.assertEqual("", launch_outcomes.extract_publication_url(
            "Published August 21. See https://example.com/reviews", object_type="媒体人",
        ))
        for url in (
            "https://example.com/en/news",
            "https://example.com/category/reviews",
            "https://example.com/about/team",
            "https://example.com/authors/jane-doe",
            "https://example.com/2026/08/",
            "https://example.com/news/2026/08/",
            "https://example.com/2026/08/21/",
            "https://example.com/privacy-policy-updated",
            "https://example.com/terms-and-conditions",
            "https://example.com/privacy.html",
            "https://example.com/about.html",
            "https://example.com/contact.php",
            "https://example.com/terms.aspx",
            "https://example.com/legal/copyright.html",
            "https://example.com/support/troubleshooting.html",
            "https://example.com/careers/openings.html",
            "https://example.com/news/archive",
            "https://example.com/news/latest",
            "https://example.com/news/popular",
            "https://example.com/blog/page-2",
            "https://example.com/?p=12345",
            "https://example.com/index.php?article_id=12345",
            "https://example.com/story?id=privacy",
        ):
            self.assertEqual("", launch_outcomes.extract_publication_url(
                f"Published August 21. See {url}", object_type="媒体人",
            ))
        self.assertEqual(
            "https://example.com/reviews/piranha-plant-dock-review",
            launch_outcomes.extract_publication_url(
                "Published August 21. See "
                "https://example.com/reviews/piranha-plant-dock-review",
                object_type="媒体人",
            ),
        )
        self.assertEqual(
            "https://example.com/2026/08/21/piranha",
            launch_outcomes.extract_publication_url(
                "Published August 21. See https://example.com/2026/08/21/piranha",
                object_type="媒体人",
            ),
        )


class LaunchOutcomeReconcileTests(unittest.TestCase):
    def setUp(self):
        launch_outcomes._DRAFT_CACHE_AT = 0.0
        launch_outcomes._DRAFT_CACHE_ROWS = []
        launch_outcomes._DRAFT_CACHE_LOCK = None
        launch_outcomes._DRAFT_CACHE_LOCK_LOOP = None

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

    def test_concurrent_draft_snapshot_uses_single_flight(self):
        rows = [{"record_id": "d1", "fields": {}}]

        async def slow_fetch(*args, **kwargs):
            await asyncio.sleep(0.01)
            return rows

        async def exercise():
            return await asyncio.gather(
                launch_outcomes.draft_snapshot(), launch_outcomes.draft_snapshot(),
            )

        fetch = AsyncMock(side_effect=slow_fetch)
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
             patch.object(launch_outcomes, "_source_drafts_belong_only_to_participant", new=AsyncMock(return_value=(True, ""))), \
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
        with patch.object(launch_outcomes.config, "T_LAUNCH_PARTICIPANT", "participants"), \
             patch.object(launch_outcomes, "_source_drafts_belong_only_to_participant", new=AsyncMock(return_value=(True, ""))):
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
             patch.object(launch_outcomes, "_source_drafts_belong_only_to_participant", new=AsyncMock(return_value=(True, ""))), \
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
        self.assertFalse(result["ok"])

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
             patch.object(launch_outcomes, "_source_drafts_belong_only_to_participant", new=AsyncMock(return_value=(True, ""))), \
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
             patch.object(launch_outcomes, "_source_drafts_belong_only_to_participant", new=AsyncMock(return_value=(True, ""))), \
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

    def test_wrong_campaign_participant_is_rejected_before_fact_write(self):
        participant = {"record_id": "p1", "fields": {
            "活动ID": "other", "对象类型": "KOL", "参与状态": "已入围",
            "审核结论": "通过", "关联邮件草稿": ["d1"],
        }}
        draft = {"record_id": "d1", "fields": {
            "发送状态": "已发送", "是否回复": True,
            "场景标签": "live_link_received",
            "回复原文": "Here is the video: https://youtu.be/abc123",
        }}
        with patch.object(launch_outcomes.config, "T_LAUNCH_PARTICIPANT", "participants"), \
             patch.object(launch_outcomes.feishu, "update_record", new=AsyncMock()) as update:
            result = asyncio.run(launch_outcomes.reconcile_campaign(
                "campaign1", dry_run=False,
                activity={"record_id": "a1", "fields": {}},
                participants=[participant], drafts=[draft],
            ))

        self.assertEqual(1, result["participants_rejected_wrong_campaign"])
        self.assertEqual(0, result["updates_planned"])
        update.assert_not_awaited()

    def test_shared_draft_ownership_blocks_fact_write(self):
        participant = {"record_id": "p1", "fields": {
            "活动ID": "campaign1", "对象类型": "KOL", "参与状态": "已入围",
            "审核结论": "通过", "关联邮件草稿": ["d1"],
        }}
        draft = {"record_id": "d1", "fields": {
            "发送状态": "已发送", "是否回复": True,
            "场景标签": "live_link_received",
            "回复原文": "Here is the video: https://youtu.be/abc123",
        }}
        with patch.object(launch_outcomes.config, "T_LAUNCH_PARTICIPANT", "participants"), \
             patch.object(launch_outcomes, "_source_drafts_belong_only_to_participant", new=AsyncMock(return_value=(False, "草稿关联2条活动参与记录"))), \
             patch.object(launch_outcomes.feishu, "update_record", new=AsyncMock()) as update:
            result = asyncio.run(launch_outcomes.reconcile_campaign(
                "campaign1", dry_run=False,
                activity={"record_id": "a1", "fields": {}},
                participants=[participant], drafts=[draft],
            ))

        self.assertFalse(result["ok"])
        self.assertEqual(1, len(result["errors"]))
        self.assertEqual(0, result["updates_planned"])
        update.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()

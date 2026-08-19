import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from app import launch_candidate_preview as preview


class LaunchCandidatePreviewTests(unittest.TestCase):
    def test_locked_replay_uses_snapshot_from_current_participant_version(self):
        rows = [{"record_id": "part1", "fields": {
            "活动ID": "c1", "产品家族ID": "p1", "对象类型": "KOL",
            "关联KOL": ["kol1"], "名单版本": "rank-v2", "参与状态": "已入围",
            "排序快照历史": '[{"ranking_version":"rank-v1","final_priority":80},'
                           '{"ranking_version":"rank-v2","final_priority":3100}]',
        }}]
        with patch.object(preview.config, "T_LAUNCH_PARTICIPANT", "participants"), \
             patch.object(preview.feishu, "search_records", new=AsyncMock(return_value=rows)):
            snapshot = asyncio.run(preview._load_locked_snapshot(
                campaign_id="c1", product_family_id="p1", object_type="KOL",
                contact_id="kol1", ranking_version="rank-v2",
            ))
        self.assertEqual(3100, snapshot["final_priority"])

    def test_ready_evidence_drift_fails_closed(self):
        activity = {
            "record_id": "recvsFoRmeGj4Y",
            "fields": {
                "活动ID": "launch-20260915-funlab-dave-ys11-5",
                "产品主记录ID": "recvkJOoCsNb1s",
                "竞品证据模式": "引用历史证据",
                "竞品分析状态": "已就绪",
                "竞品品牌": "NYXI",
                "关联竞品帖子": ["post1"],
                "证据排序版本": "evidence-v4",
            },
        }
        invalid_post = {
            "record_id": "post1",
            "fields": {
                "竞品品牌": "NYXI", "人工复核状态": "待确认",
                "相关性": "相关", "合作信号": "明确合作",
                "KOL账号Handle": "@NyxiGaming",
            },
        }
        with patch.object(preview.launch_evidence, "get_activity", new=AsyncMock(return_value=activity)), \
             patch.object(preview.feishu, "get_record", new=AsyncMock(return_value=invalid_post)):
            context = asyncio.run(preview._load_activity_context(
                "launch-20260915-funlab-dave-ys11-5", "KOL"
            ))
        self.assertTrue(context["evidence_pending"])
        self.assertFalse(context["competitor_evidence_applied"])
        self.assertEqual(context["evidence_status"], "配置无效")

    def test_canonical_product_family_keeps_aliases_together(self):
        products = [
            {"record_id": "main", "fields": {"活动主记录ID": "main", "活动归并键": "piranha"}},
            {"record_id": "alias", "fields": {"活动主记录ID": "main", "活动归并键": "piranha"}},
            {"record_id": "other", "fields": {"活动主记录ID": "other"}},
        ]
        family = preview.canonical_product_family("alias", products)
        self.assertEqual("main", family["canonical_product_id"])
        self.assertEqual({"main", "alias"}, set(family["product_ids"]))

    def test_prior_same_product_routes_positive_contact_to_reactivation(self):
        contact = {
            "record_id": "kol1",
            "fields": {"邮箱": "Creator@Example.com", "合作状态": "已合作-免费"},
        }
        drafts = [{
            "record_id": "draft1",
            "fields": {
                "关联KOL": {"link_record_ids": ["kol1"]},
                "关联产品": {"link_record_ids": ["main"]},
                "邮件草稿来源": "cold",
                "邮件草稿状态": "已发送",
                "发送状态": "已发",
                "发送邮箱": "partner@fireflyfunlab.com",
                "是否回复": True,
            },
        }]
        result = preview.precheck_contact(
            contact,
            object_type="KOL",
            brand="FUNLAB",
            product_ids={"main", "alias"},
            drafts=drafts,
            email_owners={"creator@example.com": {("KOL", "kol1")}},
            now_ms=1_800_000_000_000,
        )
        self.assertEqual("reactivation_same_thread", result["decision"])
        self.assertFalse(result["allowed_as_new_cold"])

    def test_non_cold_history_also_blocks_new_cold(self):
        contact = {
            "record_id": "kol1",
            "fields": {"邮箱": "creator@example.com", "合作状态": "样品评估"},
        }
        drafts = [{
            "record_id": "ship1",
            "fields": {
                "关联KOL": {"link_record_ids": ["kol1"]},
                "关联产品": {"link_record_ids": ["main"]},
                "邮件草稿来源": "ship_confirm",
                "邮件草稿状态": "已发送",
                "发送状态": "已发",
                "发送邮箱": "partner@fireflyfunlab.com",
            },
        }]
        result = preview.precheck_contact(
            contact,
            object_type="KOL",
            brand="FUNLAB",
            product_ids={"main"},
            drafts=drafts,
            email_owners={"creator@example.com": {("KOL", "kol1")}},
            now_ms=1_800_000_000_000,
        )
        self.assertEqual("reactivation_same_thread", result["decision"])
        self.assertFalse(result["allowed_as_new_cold"])

    def test_cross_table_same_email_is_manual_hold(self):
        contact = {
            "record_id": "kol1",
            "fields": {"邮箱": "same@example.com", "合作状态": "未建联"},
        }
        result = preview.precheck_contact(
            contact,
            object_type="KOL",
            brand="FUNLAB",
            product_ids={"main"},
            drafts=[],
            email_owners={"same@example.com": {("KOL", "kol1"), ("媒体人", "ed1")}},
            now_ms=1_800_000_000_000,
        )
        self.assertEqual("hold_duplicate_identity", result["decision"])
        self.assertFalse(result["allowed_as_new_cold"])

    def test_invalid_email_and_blacklist_are_blocked(self):
        result = preview.precheck_contact(
            {"record_id": "kol1", "fields": {"邮箱": "dm only", "合作状态": "黑名单"}},
            object_type="KOL",
            brand="POWKONG",
            product_ids={"main"},
            drafts=[],
            email_owners={},
            now_ms=1_800_000_000_000,
        )
        self.assertEqual("blocked", result["decision"])
        self.assertIn("邮箱无效", " ".join(result["reasons"]))

    def test_preview_is_read_only_and_reports_global_counts(self):
        product = {
            "record_id": "main",
            "fields": {
                "产品名": "YS11 Dave",
                "产品英文名": "FUNLAB Dave the Diver Controller",
                "品牌": "FUNLAB",
                "品类": "手柄",
                "报价(USD)": 49.99,
                "销售国家": ["US"],
                "活动主记录ID": "main",
            },
        }
        kols = [{
            "record_id": "kol1",
            "fields": {
                "账号名": "Test Creator", "邮箱": "test@example.com", "合作状态": "未建联",
                "主平台": "YouTube", "国家": "US", "语言": "en", "粉丝数": 100000,
                "内容风格": ["手柄评测"], "IP喜好": "Dave the Diver",
            },
        }]

        async def fake_fetch(table_id, field_names=None, page_size=100):
            return {
                "products": [product], "kols": kols, "editors": [], "drafts": []
            }[table_id]

        with patch.object(preview.config, "T_PRODUCT", "products"), \
             patch.object(preview.config, "T_KOL", "kols"), \
             patch.object(preview.config, "T_EDITOR", "editors"), \
             patch.object(preview.config, "T_DRAFT", "drafts"), \
             patch.object(preview.feishu, "fetch_all_records", side_effect=fake_fetch), \
             patch.object(preview.dispatch, "fetch_mapping_for_product", new=AsyncMock(return_value={
                 "expected_styles": ["手柄评测"], "expected_report_cats": [],
                 "expected_media_types": [], "matched_rules": 1,
             })), \
             patch.object(preview.feishu, "create_record", new=AsyncMock()) as create_mock, \
             patch.object(preview.feishu, "update_record", new=AsyncMock()) as update_mock:
            result = asyncio.run(preview.preview_candidates("main", object_type="KOL", limit=10))

        self.assertTrue(result["read_only"])
        self.assertEqual(1, result["summary"]["eligible_new_cold"])
        self.assertEqual("eligible_new_cold", result["candidates"][0]["decision"])
        create_mock.assert_not_awaited()
        update_mock.assert_not_awaited()

    def test_dave_activity_applies_direct_nyxi_evidence_without_writes(self):
        product = {
            "record_id": "recvkJOoCsNb1s",
            "fields": {
                "产品名": "YS11 Dave", "产品英文名": "FUNLAB Dave the Diver Controller",
                "品牌": "FUNLAB", "品类": "手柄", "报价(USD)": 49.99,
                "销售国家": ["US"], "活动主记录ID": "recvkJOoCsNb1s",
            },
        }
        activity = {
            "record_id": "recvsFoRmeGj4Y",
            "fields": {
                "活动ID": "launch-20260915-funlab-dave-ys11-5",
                "产品主记录ID": "recvkJOoCsNb1s",
                "竞品证据模式": "引用历史证据", "竞品分析状态": "已就绪",
                "竞品品牌": "NYXI", "关联竞品帖子": [f"post{i}" for i in range(8)],
                "证据排序版本": "evidence-v3",
            },
        }
        kol = {
            "record_id": "kol1",
            "fields": {
                "账号名": "Test Creator", "邮箱": "test@example.com", "合作状态": "未建联",
                "主平台": "YouTube", "国家": "US", "语言": "en", "粉丝数": 100000,
                "内容风格": ["手柄评测"],
            },
        }
        posts = {}
        for i, views in enumerate(range(100, 900, 100)):
            posts[f"post{i}"] = {"record_id": f"post{i}", "fields": {
                "竞品品牌": "NYXI", "平台": "YouTube", "内容类型": "长视频",
                "曝光量": views, "发布时间": 1_690_000_000_000 + i * 1_200_000_000,
                "人工复核状态": "已确认", "相关性": "相关", "合作信号": "明确合作",
                "关联KOL": ["kol1"] if i in {0, 7} else ["other"],
            }}

        async def fake_fetch(table_id, field_names=None, page_size=100):
            return {"products": [product], "kols": [kol], "editors": [], "drafts": []}[table_id]

        with patch.object(preview.config, "T_PRODUCT", "products"), \
             patch.object(preview.config, "T_KOL", "kols"), \
             patch.object(preview.config, "T_EDITOR", "editors"), \
             patch.object(preview.config, "T_DRAFT", "drafts"), \
             patch.object(preview.launch_evidence, "get_activity", new=AsyncMock(return_value=activity)), \
             patch.object(preview.feishu, "fetch_all_records", side_effect=fake_fetch), \
             patch.object(preview.feishu, "get_record", new=AsyncMock(side_effect=lambda table, rid: posts[rid])), \
             patch.object(preview.dispatch, "fetch_mapping_for_product", new=AsyncMock(return_value={
                 "expected_styles": ["手柄评测"], "expected_report_cats": [],
                 "expected_media_types": [], "matched_rules": 1,
             })), \
             patch.object(preview.feishu, "create_record", new=AsyncMock()) as create_mock, \
             patch.object(preview.feishu, "update_record", new=AsyncMock()) as update_mock:
            result = asyncio.run(preview.preview_candidates(
                "", object_type="KOL", limit=10,
                campaign_id="launch-20260915-funlab-dave-ys11-5",
            ))

        self.assertEqual("A", result["candidates"][0]["evidence_level"])
        self.assertEqual("evidence-v3", result["ranking_version"])
        self.assertFalse(result["evidence_pending"])
        create_mock.assert_not_awaited()
        update_mock.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()

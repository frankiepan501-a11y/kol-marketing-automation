import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from app import launch_candidate_preview as preview


class LaunchCandidatePreviewTests(unittest.TestCase):
    def test_activity_market_and_language_override_broader_product_scope(self):
        product_fields = {
            "品类": "手柄", "报价(USD)": 49.99,
            "销售国家": ["US", "UK", "DE", "ES", "BR"],
        }
        mapping = {"expected_styles": ["手柄评测"]}
        brazil = {
            "国家": "BR", "语言": "pt", "主平台": "YouTube",
            "粉丝数": 100000, "内容风格": ["手柄评测"],
        }
        usa = dict(brazil, **{"国家": "US", "语言": "en"})

        matched_br, reasons_br = preview._base_filter_kol(
            brazil, product_fields, mapping,
            target_countries={"US", "UK", "DE", "ES"},
            target_languages={"en", "de", "es"},
        )
        matched_us, reasons_us = preview._base_filter_kol(
            usa, product_fields, mapping,
            target_countries={"US", "UK", "DE", "ES"},
            target_languages={"en", "de", "es"},
        )

        self.assertFalse(matched_br)
        self.assertIn("国家不在活动目标市场", reasons_br)
        self.assertIn("语言不在活动目标范围", reasons_br)
        self.assertTrue(matched_us)
        self.assertEqual([], reasons_us)

    def test_empty_campaign_market_scope_fails_closed_instead_of_using_product_scope(self):
        matched, reasons = preview._base_filter_kol(
            {
                "国家": "BR", "语言": "pt", "主平台": "YouTube",
                "粉丝数": 100000, "内容风格": ["手柄评测"],
            },
            {
                "品类": "手柄", "报价(USD)": 49.99,
                "销售国家": ["US", "BR"],
            },
            {"expected_styles": ["手柄评测"]},
            target_countries=set(), target_languages=set(),
        )

        self.assertFalse(matched)
        self.assertIn("活动目标国家未配置", reasons)
        self.assertIn("活动目标语言未配置", reasons)

    def test_piranha_rejects_generic_game_profile_even_when_style_overlaps(self):
        matched, reasons = preview._base_filter_kol(
            {
                "国家": "US", "语言": "en", "主平台": "YouTube",
                "粉丝数": 789000, "内容风格": ["游戏", "测评", "综合"],
                "内容垂类": "泛游戏娱乐",
                "主机生态": ["PC-Steam", "Mobile", "跨平台"],
                "资料可用状态": "人工核实有效",
            },
            {
                "品类": "Switch底座", "报价(USD)": 109.99,
                "销售国家": ["US"], "适配主机": ["Switch", "Switch 2"],
                "适配IP": ["马里奥系列", "Nintendo系列"],
            },
            {"expected_styles": ["游戏", "SETUP", "UNBOX", "测评"]},
            target_countries={"US"}, target_languages={"en"},
        )

        self.assertFalse(matched)
        self.assertIn("目标主机不匹配", reasons)

    def test_piranha_accepts_manually_verified_switch_profile(self):
        matched, reasons = preview._base_filter_kol(
            {
                "国家": "ES", "语言": "es", "主平台": "YouTube",
                "粉丝数": 1980000, "内容风格": ["游戏"],
                "内容垂类": "主机游戏", "主机生态": ["Switch", "Switch 2"],
                "资料可用状态": "人工核实有效", "IP喜好": "Yoshi, Tomodachi Life",
                "资料核实时间": 1_799_000_000_000,
            },
            {
                "品类": "Switch底座", "报价(USD)": 109.99,
                "销售国家": ["ES"], "适配主机": ["Switch", "Switch 2"],
                "适配IP": ["马里奥系列", "Nintendo系列"],
            },
            {"expected_styles": ["游戏", "SETUP", "UNBOX", "测评"]},
            target_countries={"ES"}, target_languages={"es"},
            now_ms=1_800_000_000_000,
        )

        self.assertTrue(matched)
        self.assertEqual([], reasons)

    def test_piranha_holds_strong_switch_profile_when_activity_is_too_low(self):
        matched, reasons = preview._base_filter_kol(
            {
                "国家": "US", "语言": "en", "主平台": "YouTube",
                "粉丝数": 2130000, "内容风格": ["游戏"],
                "内容垂类": "主机游戏", "主机生态": ["Switch", "Switch 2"],
                "资料可用状态": "活跃度不足", "IP喜好": "Mario, Zelda",
            },
            {
                "品类": "Switch底座", "报价(USD)": 109.99,
                "销售国家": ["US"], "适配主机": ["Switch", "Switch 2"],
                "适配IP": ["马里奥系列", "Nintendo系列"],
            },
            {"expected_styles": ["游戏", "SETUP", "UNBOX", "测评"]},
            target_countries={"US"}, target_languages={"en"},
        )

        self.assertFalse(matched)
        self.assertIn("活跃度不足", reasons)

    def test_manual_profile_expires_without_recent_verification_time(self):
        reasons = preview._nintendo_switch_profile_reasons({
            "内容垂类": "主机游戏", "主机生态": ["Switch"],
            "资料可用状态": "人工核实有效", "IP喜好": "Mario",
        }, now_ms=1_800_000_000_000)

        self.assertIn("人工核实已过期", reasons)

    def test_timestamp_parser_accepts_iso_seconds_and_milliseconds(self):
        expected = 1_800_000_000_000
        self.assertEqual(expected, preview._timestamp_ms(expected))
        self.assertEqual(expected, preview._timestamp_ms(1_800_000_000))
        self.assertEqual(
            1_787_219_100_000,
            preview._timestamp_ms("2026-08-20T17:45:00.000+08:00"),
        )

    def test_piranha_rejects_minecraft_even_when_switch_and_vertical_match(self):
        matched, reasons = preview._base_filter_kol(
            {
                "国家": "US", "语言": "en", "主平台": "YouTube",
                "粉丝数": 600000, "内容风格": ["游戏"],
                "内容垂类": "主机游戏", "主机生态": ["Switch"],
                "资料可用状态": "人工核实有效", "IP喜好": "Minecraft, Roblox",
            },
            {
                "品类": "Switch底座", "报价(USD)": 109.99,
                "销售国家": ["US"], "适配主机": ["Switch", "Switch 2"],
                "适配IP": ["马里奥系列", "Nintendo系列"],
            },
            {"expected_styles": ["游戏"]},
            target_countries={"US"}, target_languages={"en"},
        )

        self.assertFalse(matched)
        self.assertIn("Nintendo/Mario受众或近期硬件内容不匹配", reasons)

    def test_piranha_rejects_mixed_ip_when_recent_content_is_non_target(self):
        reasons = preview._nintendo_switch_profile_reasons({
            "内容垂类": "主机游戏", "主机生态": ["Switch"],
            "资料可用状态": "人工核实有效",
            "资料核实时间": 1_799_000_000_000,
            "IP喜好": "Mario, Minecraft, Roblox",
            "近期视频标题": "Minecraft challenge\nNew Roblox roleplay",
        }, now_ms=1_800_000_000_000)

        self.assertIn("近期或主要内容存在明显非目标游戏/IP信号", reasons)

    def test_piranha_effective_status_still_requires_v2_fresh_data_and_activity(self):
        matched, reasons = preview._base_filter_kol(
            {
                "国家": "US", "语言": "en", "主平台": "YouTube",
                "粉丝数": 100000, "内容风格": ["游戏"],
                "内容垂类": "主机游戏", "主机生态": ["Switch"],
                "资料可用状态": "有效", "IP喜好": "Mario",
                "标签版本": "", "近期视频抓取时间": 1_700_000_000_000,
                "最近发布日": 1_700_000_000_000, "近90天发布数": 0,
            },
            {
                "品类": "Switch底座", "报价(USD)": 109.99,
                "销售国家": ["US"], "适配主机": ["Switch", "Switch 2"],
                "适配IP": ["马里奥系列", "Nintendo系列"],
            },
            {"expected_styles": ["游戏"]},
            target_countries={"US"}, target_languages={"en"},
            now_ms=1_800_000_000_000,
        )

        self.assertFalse(matched)
        self.assertIn("标签版本不是v2", reasons)
        self.assertIn("资料缺失或过期", reasons)
        self.assertIn("活跃度不足", reasons)

    def test_piranha_machine_profile_accepts_v2_fresh_active_nintendo_creator(self):
        matched, reasons = preview._base_filter_kol(
            {
                "国家": "US", "语言": "en", "主平台": "YouTube",
                "粉丝数": 600000, "内容风格": ["游戏"],
                "内容垂类": "主机游戏", "主机生态": ["Switch 2"],
                "资料可用状态": "有效", "IP喜好": "Nintendo, Mario",
                "标签版本": "v2", "近期视频抓取时间": 1_799_000_000_000,
                "最近发布日": 1_799_000_000_000, "近90天发布数": 4,
            },
            {
                "品类": "Switch底座", "报价(USD)": 109.99,
                "销售国家": ["US"], "适配主机": ["Switch", "Switch 2"],
                "适配IP": ["马里奥系列", "Nintendo系列"],
            },
            {"expected_styles": ["游戏"]},
            target_countries={"US"}, target_languages={"en"},
            now_ms=1_800_000_000_000,
        )

        self.assertTrue(matched)
        self.assertEqual([], reasons)

    def test_review_snapshot_routes_head_kol_to_frankie_with_actionable_evidence(self):
        fields = {
            "主链接": {"link": "https://youtube.com/@creator"},
            "主平台": "YouTube", "国家": "US", "语言": "en",
            "粉丝数": 900000, "内容风格": ["手柄评测"],
            "近期视频标题": "Dave the Diver controller review\nBest Switch controllers",
            "近期视频抓取时间": 1_800_000_000_000,
            "合作状态": "未建联", "邮箱验真状态": "有效",
            "KOL级别": "头部KOL", "合作报价": 1200,
        }
        evidence = {
            "evidence_level": "A",
            "evidence_posts": [{
                "post_id": "post1", "post_url": "https://youtube.com/watch?v=nyxi",
                "post_title": "NYXI controller review", "platform": "YouTube",
                "metric_name": "曝光量", "metric_value": 200000,
                "is_high_performance": True, "evidence_basis": "rule_inferred_non_official",
            }],
        }

        snapshot = preview.build_review_snapshot(fields, evidence, now_ms=1_800_000_100_000)

        self.assertEqual("Frankie例外审核", snapshot["review_route"])
        self.assertEqual("待审核", snapshot["review_decision"])
        self.assertEqual("https://youtube.com/@creator", snapshot["profile_url"])
        self.assertEqual("https://youtube.com/watch?v=nyxi", snapshot["primary_evidence_url"])
        self.assertIn("头部KOL", snapshot["review_instruction"])
        self.assertIn("合作报价", snapshot["relationship_summary"])

    def test_review_snapshot_system_passes_complete_ordinary_candidate(self):
        fields = {
            "主链接": "https://youtube.com/@ordinary", "主平台": "YouTube",
            "国家": "US", "语言": "en", "粉丝数": 120000,
            "内容风格": ["手柄评测"], "近期视频标题": "Controller review",
            "近期视频抓取时间": 1_800_000_000_000,
            "合作状态": "未建联", "邮箱验真状态": "有效",
        }
        snapshot = preview.build_review_snapshot(
            fields, {"evidence_level": "无加分", "evidence_posts": []},
            now_ms=1_800_000_100_000,
        )
        self.assertEqual("系统建议通过", snapshot["review_route"])
        self.assertEqual("通过", snapshot["review_decision"])

    def test_reactivation_is_operator_boundary_even_when_main_status_is_not_positive(self):
        fields = {
            "主链接": "https://youtube.com/@reactivate", "主平台": "YouTube",
            "国家": "UK", "语言": "en", "粉丝数": 120000,
            "近期视频标题": "Controller review",
            "近期视频抓取时间": 1_800_000_000_000,
            "合作状态": "建联中", "邮箱验真状态": "有效",
        }
        snapshot = preview.build_review_snapshot(
            fields, {"evidence_level": "无加分", "evidence_posts": []},
            now_ms=1_800_000_100_000,
            precheck={"decision": "reactivation_same_thread"},
        )
        self.assertEqual("KOL运营审核", snapshot["review_route"])
        self.assertIn("原邮件线程", snapshot["review_instruction"])

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

    def test_participant_review_returns_reason_codes_for_single_replay(self):
        rows = [{"record_id": "part1", "fields": {
            "活动ID": "c1", "产品家族ID": "p1", "对象类型": "KOL",
            "名单版本": "rank-v2", "参与状态": "已入围",
            "关联KOL": ["kol1"], "审核结论": "排除",
            "审核原因": "受众不匹配",
            "审核原因代码": ["目标主机不匹配", "核心游戏IP不匹配"],
            "审核时间": 1_800_000_000_000,
        }}]
        with patch.object(preview.config, "T_LAUNCH_PARTICIPANT", "participants"), \
             patch.object(preview.feishu, "search_records", new=AsyncMock(return_value=rows)):
            result = asyncio.run(preview._load_participant_review(
                campaign_id="c1", product_family_id="p1", object_type="KOL",
                contact_id="kol1", ranking_version="rank-v2",
            ))

        self.assertTrue(result["is_current"])
        self.assertEqual("排除", result["review_decision"])
        self.assertEqual(
            ["核心游戏IP不匹配", "目标主机不匹配"],
            result["review_reason_codes"],
        )

    def test_participant_review_does_not_present_old_version_as_current(self):
        rows = [{"record_id": "part1", "fields": {
            "活动ID": "c1", "产品家族ID": "p1", "对象类型": "KOL",
            "名单版本": "rank-v1", "参与状态": "已取消",
            "关联KOL": ["kol1"], "审核结论": "通过",
        }}]
        with patch.object(preview.config, "T_LAUNCH_PARTICIPANT", "participants"), \
             patch.object(preview.feishu, "search_records", new=AsyncMock(return_value=rows)):
            result = asyncio.run(preview._load_participant_review(
                campaign_id="c1", product_family_id="p1", object_type="KOL",
                contact_id="kol1", ranking_version="rank-v2",
            ))

        self.assertFalse(result["is_current"])
        self.assertNotIn("review_decision", result)

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

    def test_prior_same_product_routes_positive_contact_to_existing_pipeline_pool(self):
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
        self.assertEqual("existing_pipeline_same_thread", result["decision"])
        self.assertEqual("existing_pipeline", result["campaign_pool"])
        self.assertFalse(result["allowed_as_new_cold"])

    def test_positive_relationship_without_product_draft_never_enters_new_cold(self):
        result = preview.precheck_contact(
            {
                "record_id": "kol1",
                "fields": {
                    "邮箱": "creator@example.com", "合作状态": "洽谈中",
                    "触达路由状态": "沿用原线程",
                },
            },
            object_type="KOL", brand="POWKONG", product_ids={"main"},
            drafts=[], email_owners={"creator@example.com": {("KOL", "kol1")}},
            now_ms=1_800_000_000_000,
        )

        self.assertEqual("existing_pipeline_same_thread", result["decision"])
        self.assertEqual("existing_pipeline", result["campaign_pool"])
        self.assertFalse(result["allowed_as_new_cold"])

    def test_waiting_reply_without_product_draft_never_enters_new_cold(self):
        result = preview.precheck_contact(
            {
                "record_id": "kol1",
                "fields": {"邮箱": "creator@example.com", "合作状态": "待回复"},
            },
            object_type="KOL", brand="POWKONG", product_ids={"main"},
            drafts=[], email_owners={"creator@example.com": {("KOL", "kol1")}},
            now_ms=1_800_000_000_000,
        )

        self.assertEqual("existing_pipeline_same_thread", result["decision"])
        self.assertFalse(result["allowed_as_new_cold"])

    def test_prior_same_product_with_upload_routes_to_republish_pool(self):
        contact = {
            "record_id": "kol1",
            "fields": {
                "邮箱": "creator@example.com", "合作状态": "已合作-免费",
                "上稿日期": 1_790_000_000_000,
            },
        }
        drafts = [{
            "record_id": "draft1",
            "fields": {
                "关联KOL": {"link_record_ids": ["kol1"]},
                "关联产品": {"link_record_ids": ["main"]},
                "邮件草稿来源": "cold", "邮件草稿状态": "已发送",
                "发送状态": "已发", "发送邮箱": "partner@fireflyfunlab.com",
                "是否回复": True,
            },
        }]
        result = preview.precheck_contact(
            contact, object_type="KOL", brand="FUNLAB", product_ids={"main"},
            drafts=drafts,
            email_owners={"creator@example.com": {("KOL", "kol1")}},
            now_ms=1_800_000_000_000,
        )
        self.assertEqual("republish_requires_commitment", result["decision"])
        self.assertEqual("republish", result["campaign_pool"])
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
        self.assertEqual("existing_pipeline_same_thread", result["decision"])
        self.assertEqual("existing_pipeline", result["campaign_pool"])
        self.assertFalse(result["allowed_as_new_cold"])

    def test_repeated_recent_malaysia_content_holds_structured_uk_candidate(self):
        result = preview.market_consistency_check(
            {
                "国家": "UK", "语言": "en",
                "近期视频标题": (
                    "Nintendo Switch Malaysia launch\n"
                    "Best controller deals in Malaysia\n"
                    "Malaysia gaming setup"
                ),
            },
            target_countries={"US", "UK", "DE", "FR", "ES"},
        )
        self.assertFalse(result["passed"])
        self.assertEqual("hold_market_conflict", result["decision"])
        self.assertIn("Malaysia", result["reasons"][0])

    def test_single_foreign_country_mention_does_not_rewrite_creator_market(self):
        result = preview.market_consistency_check(
            {"国家": "US", "近期视频标题": "Controller launch in Japan"},
            target_countries={"US", "UK", "DE", "FR", "ES"},
        )
        self.assertTrue(result["passed"])

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

    def test_preview_keeps_existing_relationship_route_even_when_profile_fails(self):
        product = {
            "record_id": "main",
            "fields": {
                "产品名": "食人花二代", "品牌": "POWKONG", "品类": "Switch底座",
                "报价(USD)": 89.99, "销售国家": ["US"],
                "适配主机": ["Switch", "Switch 2"],
                "适配IP": ["马里奥系列", "Nintendo系列"],
                "活动主记录ID": "main",
            },
        }
        kols = [{
            "record_id": "kol1",
            "fields": {
                "账号名": "Existing Creator", "邮箱": "existing@example.com",
                "合作状态": "洽谈中", "触达路由状态": "沿用原线程",
                "主平台": "YouTube", "国家": "US", "语言": "en", "粉丝数": 100000,
                "内容风格": ["游戏"], "内容垂类": "泛游戏娱乐",
                "主机生态": ["PC-Steam"], "IP喜好": "Minecraft",
                "资料可用状态": "人工核实有效",
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
                 "expected_styles": ["游戏"], "expected_report_cats": [],
                 "expected_media_types": [], "matched_rules": 1,
             })):
            result = asyncio.run(preview.preview_candidates("main", object_type="KOL", limit=10))

        candidate = result["candidates"][0]
        self.assertEqual("existing_pipeline_same_thread", candidate["decision"])
        self.assertFalse(candidate["base_filter_passed"])
        self.assertIn("目标主机不匹配", candidate["base_filter_reasons"])

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
        self.assertEqual(8, result["evidence_coverage"]["valid_partner_posts"])
        self.assertEqual(1, result["evidence_coverage"]["matched_contacts"])
        create_mock.assert_not_awaited()
        update_mock.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()

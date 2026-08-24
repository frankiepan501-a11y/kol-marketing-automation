import asyncio
import json
import unittest
from unittest.mock import AsyncMock, patch

from app import auto_send, enrich, launch_runtime


def _certificate():
    return json.dumps({
        "campaign_id": "campaign1", "product_id": "product1", "brand": "FUNLAB",
        "template_version": "launch-queue-v1", "passed": True,
    })


class LaunchRuntimeTests(unittest.TestCase):
    def test_zero_model_launch_date_uses_controlled_campaign_mapping_only(self):
        known = launch_runtime._launch_date_labels(
            "launch-20260915-powkong-piranha-v2",
        )
        unknown = launch_runtime._launch_date_labels("unmapped-campaign")

        self.assertEqual("September 15", known["en"])
        self.assertEqual("the launch window", unknown["en"])

    def test_pending_review_reconcile_auto_passes_only_deterministic_candidate(self):
        participant = {"record_id": "part1", "fields": {
            "参与状态": "已入围", "审核结论": "待补资料",
            "关联KOL": {"link_record_ids": ["kol1"]},
            "关联邮件草稿": [], "排序快照历史": "[]",
        }}
        candidate = {
            "contact_id": "kol1", "decision": "eligible_new_cold",
            "base_filter_passed": True, "review_decision": "通过",
            "review_route": "系统建议通过", "review_instruction": "系统检查通过",
            "score": 91, "final_priority": 91, "evidence_level": "无加分",
            "country": "US", "language": "en", "platform": "YouTube",
            "profile_url": "https://youtube.com/@one", "content_summary": "controller review",
        }

        with patch.object(
            launch_runtime, "_participants", new=AsyncMock(return_value=[participant]),
        ), patch.object(
            launch_runtime.launch_participation, "_update_and_confirm",
            new=AsyncMock(return_value={"record_id": "part1"}),
        ) as update:
            result = asyncio.run(launch_runtime.reconcile_pending_participant_reviews(
                campaign_id="campaign1", ranking_version="v1",
                preview={"candidates": [candidate], "profile_refresh_candidates": []},
            ))

        self.assertEqual(1, result["auto_passed"])
        self.assertEqual(0, result["actionable_pending"])
        fields = update.await_args.args[2]
        self.assertEqual("通过", fields["审核结论"])
        self.assertEqual("系统建议通过", fields["系统审核分流"])
        self.assertNotIn("关联邮件草稿", fields)

    def test_pending_review_reconcile_keeps_actionable_ambiguity_for_operator(self):
        participant = {"record_id": "part1", "fields": {
            "参与状态": "已入围", "审核结论": "待审核",
            "关联KOL": {"link_record_ids": ["kol1"]},
            "关联邮件草稿": [], "排序快照历史": "[]",
        }}
        candidate = {
            "contact_id": "kol1", "decision": "eligible_new_cold",
            "base_filter_passed": True, "review_decision": "待审核",
            "review_route": "KOL运营审核",
            "review_instruction": "只需确认辅助语言=de和近3个月内容",
            "base_filter_reason_codes": ["辅助语言待确认"],
            "score": 82, "final_priority": 82, "evidence_level": "无加分",
            "country": "DE", "language": "de", "platform": "YouTube",
            "profile_url": "https://youtube.com/@one", "content_summary": "game hardware",
        }

        with patch.object(
            launch_runtime, "_participants", new=AsyncMock(return_value=[participant]),
        ), patch.object(
            launch_runtime.launch_participation, "_update_and_confirm",
            new=AsyncMock(return_value={"record_id": "part1"}),
        ) as update:
            result = asyncio.run(launch_runtime.reconcile_pending_participant_reviews(
                campaign_id="campaign1", ranking_version="v1",
                preview={"candidates": [candidate], "profile_refresh_candidates": []},
            ))

        self.assertEqual(0, result["auto_passed"])
        self.assertEqual(1, result["actionable_pending"])
        fields = update.await_args.args[2]
        self.assertEqual("待审核", fields["审核结论"])
        self.assertEqual("KOL运营审核", fields["系统审核分流"])
        self.assertEqual(["辅助语言待确认"], fields["审核原因代码"])

    def test_pending_review_contacts_are_prioritized_for_profile_refresh(self):
        participants = [
            {"record_id": "p1", "fields": {
                "参与状态": "已入围", "审核结论": "待补资料",
                "关联KOL": {"link_record_ids": ["pending1"]},
            }},
            {"record_id": "p2", "fields": {
                "参与状态": "已入围", "审核结论": "通过",
                "关联KOL": {"link_record_ids": ["approved1"]},
            }},
        ]

        ids = launch_runtime._pending_review_contact_ids(
            participants, preview_refresh_ids=["other1", "pending1"], limit=10,
        )

        self.assertEqual(["pending1", "other1"], ids)

    def test_manual_approval_commits_only_controlled_import_route_only_hold(self):
        participant = {"record_id": "part1", "fields": {
            "活动ID": "campaign1", "参与状态": "已入围", "审核结论": "通过",
            "进入方式": "新开发", "活动分池": "新开发池",
            "关联KOL": {"link_record_ids": ["kol1"]}, "关联邮件草稿": [],
        }}
        kol = {"record_id": "kol1", "fields": {
            "合作状态": "未建联", "触达路由状态": "待核对",
            "资料可用状态": "有效",
            "迁移备注": "[CONTROLLED_IMPORT] campaign=campaign1; author=a1; no_auto_email=true",
        }}
        product = {"record_id": "product1", "fields": {"品牌": "FUNLAB"}}
        route_only_hold = {
            "decision": "hold_active_or_recent",
            "reasons": ["触达路由状态=待核对，禁止直接进入新开发池"],
            "evidence_draft_ids": [],
        }
        readback = {"record_id": "kol1", "fields": {
            **kol["fields"], "触达路由状态": "可新开发",
        }}

        with patch.object(
            launch_runtime.feishu, "get_record", new=AsyncMock(side_effect=[kol, readback]),
        ), patch.object(
            launch_runtime.launch_outreach, "_fast_precheck",
            new=AsyncMock(return_value=route_only_hold),
        ), patch.object(
            launch_runtime.feishu, "update_record",
            new=AsyncMock(return_value={"record_id": "kol1", "_accepted_without_record": True}),
        ) as update_record:
            result = asyncio.run(
                launch_runtime.reconcile_approved_controlled_import_routes(
                    campaign_id="campaign1", product=product,
                    product_id="product1", brand="FUNLAB",
                    participants=[participant],
                )
            )

        self.assertEqual(1, result["updated"])
        self.assertEqual("可新开发", update_record.await_args.args[2]["触达路由状态"])
        self.assertEqual("route_only_manual_hold_committed", result["details"][0]["result"])

    def test_manual_approval_keeps_existing_thread_and_recent_contact_blocks(self):
        participants = [
            {"record_id": "part-thread", "fields": {
                "活动ID": "campaign1", "参与状态": "已入围", "审核结论": "通过",
                "进入方式": "新开发", "活动分池": "新开发池",
                "关联KOL": {"link_record_ids": ["kol-thread"]},
            }},
            {"record_id": "part-recent", "fields": {
                "活动ID": "campaign1", "参与状态": "已入围", "审核结论": "通过",
                "进入方式": "新开发", "活动分池": "新开发池",
                "关联KOL": {"link_record_ids": ["kol-recent"]},
            }},
        ]
        controlled = {
            "合作状态": "未建联", "触达路由状态": "待核对",
            "资料可用状态": "有效",
            "迁移备注": "[CONTROLLED_IMPORT] campaign=campaign1; no_auto_email=true",
        }
        product = {"record_id": "product1", "fields": {"品牌": "FUNLAB"}}
        decisions = [
            {"decision": "existing_pipeline_same_thread", "reasons": ["已有线程"],
             "evidence_draft_ids": ["draft1"]},
            {"decision": "hold_active_or_recent", "reasons": ["同品牌 30 天内已有触达"],
             "evidence_draft_ids": ["draft2"]},
        ]

        with patch.object(
            launch_runtime.feishu, "get_record",
            new=AsyncMock(side_effect=[
                {"record_id": "kol-thread", "fields": controlled},
                {"record_id": "kol-recent", "fields": controlled},
            ]),
        ), patch.object(
            launch_runtime.launch_outreach, "_fast_precheck",
            new=AsyncMock(side_effect=decisions),
        ), patch.object(
            launch_runtime.feishu, "update_record", new=AsyncMock(),
        ) as update_record:
            result = asyncio.run(
                launch_runtime.reconcile_approved_controlled_import_routes(
                    campaign_id="campaign1", product=product,
                    product_id="product1", brand="FUNLAB",
                    participants=participants,
                )
            )

        self.assertEqual(0, result["updated"])
        self.assertEqual(2, result["kept_blocked"])
        update_record.assert_not_awaited()
    def test_zero_model_draft_validator_accepts_safe_template_and_rejects_placeholder(self):
        kol = {"fields": {
            "账号名": "Creator One", "国家": "US", "语言": "en",
        }}
        product = {"fields": {
            "产品英文名": "Piranha Plant 2 Dock",
            "官网链接": {"link": "https://powkong.com/products/piranha-dock"},
        }}
        draft = launch_runtime._deterministic_fallback_draft(kol, product, "POWKONG")

        valid = launch_runtime.validate_deterministic_launch_draft(draft)
        invalid = launch_runtime.validate_deterministic_launch_draft({
            **draft, "body": draft["body"] + " [TBD]",
        })

        self.assertTrue(valid["passed"])
        self.assertEqual([], valid["errors"])
        self.assertFalse(invalid["passed"])
        self.assertIn("unresolved_placeholder", invalid["errors"])

    def test_zero_model_refill_preview_has_no_model_calls_or_writes(self):
        activity = {"record_id": "a1", "fields": {
            "活动ID": "campaign1", "产品主记录ID": "product1",
            "活动目标语言": ["en"], "运行模式": "正式运行",
            "状态": "正式执行中",
        }}
        product = {"record_id": "product1", "fields": {
            "品牌": "POWKONG", "产品英文名": "Piranha Plant 2 Dock",
            "官网链接": {"link": "https://powkong.com/products/piranha-dock"},
        }}
        kol = {"record_id": "k1", "fields": {
            "账号名": "Creator One", "国家": "US", "语言": "en",
        }}
        preview = {
            "summary": {"eligible_new_cold": 1},
            "profile_refresh_candidate_ids": ["k2"],
            "profile_refresh_candidates": [{"contact_id": "k2"}],
            "candidates": [{
                "contact_id": "k1", "decision": "eligible_new_cold",
                "review_decision": "通过", "score": 91,
            }],
        }
        metrics = {"campaign_id": "campaign1", "participants": 10,
                   "action": "expand", "approved_new_development_24h": 2}

        async def get_record(table, record_id):
            return product if record_id == "product1" else kol

        with patch.object(
            launch_runtime.launch_evidence, "get_activity", new=AsyncMock(return_value=activity),
        ), patch.object(
            launch_runtime, "campaign_metrics", new=AsyncMock(return_value=metrics),
        ), patch.object(
            launch_runtime.feishu, "get_record", new=AsyncMock(side_effect=get_record),
        ), patch.object(
            launch_runtime, "_brand_quota_snapshot",
            new=AsyncMock(return_value={"brand": "POWKONG", "cap": 120,
                                        "sent_24h": 20, "remaining": 100}),
        ), patch.object(
            launch_runtime, "_campaign_ready_inventory",
            new=AsyncMock(return_value={"ready": 0, "pending_review": 0}),
        ), patch.object(
            launch_runtime.launch_candidate_preview, "preview_candidates",
            new=AsyncMock(return_value=preview),
        ), patch.object(
            launch_runtime.relabel, "run_profile_records",
            new=AsyncMock(return_value={"dry_run": True, "classification_mode": "deterministic",
                                        "model_calls": 0, "writes": 0, "processed": 1}),
        ) as refresh, patch.object(
            launch_runtime.keyword_supply, "ensure_campaign_supply",
            new=AsyncMock(return_value={"created": 0, "would_create": 3,
                                        "model_calls": 0, "shortfall_tasks": 0}),
        ) as keywords, patch.object(
            launch_runtime.enrich, "gen_draft", new=AsyncMock(),
        ) as generate, patch.object(
            launch_runtime.draft_router, "route_draft", new=AsyncMock(),
        ) as review, patch.object(
            launch_runtime.feishu, "create_record", new=AsyncMock(),
        ) as write:
            result = asyncio.run(launch_runtime.preview_zero_model_refill(
                campaign_id="campaign1", profile_refresh_limit=10, draft_preview_limit=5,
            ))

        self.assertTrue(result["read_only"])
        self.assertTrue(result["dry_run"])
        self.assertEqual("zero_model", result["ai_mode"])
        self.assertEqual(0, result["model_calls"])
        self.assertEqual(0, result["writes"])
        self.assertEqual(0, result["drafts_created"])
        self.assertEqual(0, result["emails_sent"])
        self.assertEqual(1, result["draft_preview_count"])
        self.assertTrue(result["draft_previews"][0]["validation"]["passed"])
        refresh.assert_awaited_once_with(
            ["k2"], dry_run=True, limit=1, classification_mode="deterministic",
        )
        self.assertFalse(keywords.await_args.kwargs["allow_ai"])
        generate.assert_not_awaited()
        review.assert_not_awaited()
        write.assert_not_awaited()

    def test_zero_model_preview_holds_inactive_campaign_without_downstream_reads(self):
        activity = {"record_id": "a1", "fields": {
            "活动ID": "campaign1", "运行模式": "预演", "状态": "已暂停",
        }}
        with patch.object(
            launch_runtime.launch_evidence, "get_activity", new=AsyncMock(return_value=activity),
        ), patch.object(
            launch_runtime, "campaign_metrics",
            new=AsyncMock(return_value={"campaign_id": "campaign1", "action": "expand"}),
        ), patch.object(
            launch_runtime.feishu, "get_record", new=AsyncMock(),
        ) as get_record, patch.object(
            launch_runtime.launch_candidate_preview, "preview_candidates", new=AsyncMock(),
        ) as preview:
            result = asyncio.run(launch_runtime.preview_zero_model_refill(
                campaign_id="campaign1",
            ))

        self.assertTrue(result["held"])
        self.assertEqual("held", result["business_outcome"])
        self.assertEqual(0, result["writes"])
        get_record.assert_not_awaited()
        preview.assert_not_awaited()

    def test_zero_model_preview_reports_degraded_when_fixed_keywords_have_shortfall(self):
        activity = {"record_id": "a1", "fields": {
            "活动ID": "campaign1", "产品主记录ID": "product1",
            "运行模式": "正式运行", "状态": "正式执行中",
        }}
        product = {"record_id": "product1", "fields": {
            "品牌": "POWKONG", "产品英文名": "Piranha Plant 2 Dock",
            "官网链接": {"link": "https://powkong.com/products/piranha-dock"},
        }}
        with patch.object(
            launch_runtime.launch_evidence, "get_activity", new=AsyncMock(return_value=activity),
        ), patch.object(
            launch_runtime, "campaign_metrics",
            new=AsyncMock(return_value={"action": "expand", "participants": 0}),
        ), patch.object(
            launch_runtime.feishu, "get_record", new=AsyncMock(return_value=product),
        ), patch.object(
            launch_runtime, "_brand_quota_snapshot",
            new=AsyncMock(return_value={"remaining": 100}),
        ), patch.object(
            launch_runtime, "_campaign_ready_inventory",
            new=AsyncMock(return_value={"ready": 0}),
        ), patch.object(
            launch_runtime.launch_candidate_preview, "preview_candidates",
            new=AsyncMock(return_value={"candidates": [], "summary": {}}),
        ), patch.object(
            launch_runtime.keyword_supply, "ensure_campaign_supply",
            new=AsyncMock(return_value={
                "ok": True, "model_calls": 0, "shortfall_tasks": 2,
            }),
        ):
            result = asyncio.run(launch_runtime.preview_zero_model_refill(
                campaign_id="campaign1",
            ))

        self.assertEqual("preview_degraded", result["business_outcome"])
        self.assertEqual("degraded", launch_runtime.runtime_job_status(result))
        self.assertIn("fixed_keywords_shortfall", result["readiness"]["reasons"])

    def test_zero_model_preview_reports_degraded_when_profile_scrape_fails(self):
        activity = {"record_id": "a1", "fields": {
            "活动ID": "campaign1", "产品主记录ID": "product1",
            "运行模式": "正式运行", "状态": "正式执行中",
        }}
        product = {"record_id": "product1", "fields": {
            "品牌": "POWKONG", "产品英文名": "Piranha Plant 2 Dock",
            "官网链接": {"link": "https://powkong.com/products/piranha-dock"},
        }}
        with patch.object(
            launch_runtime.launch_evidence, "get_activity", new=AsyncMock(return_value=activity),
        ), patch.object(
            launch_runtime, "campaign_metrics",
            new=AsyncMock(return_value={"action": "expand", "participants": 0}),
        ), patch.object(
            launch_runtime.feishu, "get_record", new=AsyncMock(return_value=product),
        ), patch.object(
            launch_runtime, "_brand_quota_snapshot",
            new=AsyncMock(return_value={"remaining": 100}),
        ), patch.object(
            launch_runtime, "_campaign_ready_inventory",
            new=AsyncMock(return_value={"ready": 0}),
        ), patch.object(
            launch_runtime.launch_candidate_preview, "preview_candidates",
            new=AsyncMock(return_value={
                "candidates": [], "summary": {},
                "profile_refresh_candidate_ids": ["k1"],
            }),
        ), patch.object(
            launch_runtime.relabel, "run_profile_records",
            new=AsyncMock(return_value={
                "dry_run": True, "model_calls": 0, "writes": 0,
                "processed": 1, "by_status": {"scrape_fail": 1},
            }),
        ), patch.object(
            launch_runtime.keyword_supply, "ensure_campaign_supply",
            new=AsyncMock(return_value={"ok": True, "shortfall_tasks": 0}),
        ):
            result = asyncio.run(launch_runtime.preview_zero_model_refill(
                campaign_id="campaign1",
            ))

        self.assertEqual("preview_degraded", result["business_outcome"])
        self.assertEqual(1, result["readiness"]["profile_error_count"])
        self.assertIn("profile_refresh_error", result["readiness"]["reasons"])

    def test_sync_outcomes_and_metrics_reuses_the_same_draft_snapshot(self):
        activity = {"record_id": "a1", "fields": {"活动ID": "campaign1"}}
        participants = [{"record_id": "p1", "fields": {}}]
        drafts = [{"record_id": "d1", "fields": {}}]
        with patch.object(
            launch_runtime.launch_evidence, "get_activity",
            new=AsyncMock(return_value=activity),
        ), patch.object(
            launch_runtime, "_participants", new=AsyncMock(return_value=participants),
        ), patch.object(
            launch_runtime.launch_outcomes, "draft_snapshot",
            new=AsyncMock(return_value=drafts),
        ) as snapshot, patch.object(
            launch_runtime.launch_outcomes, "reconcile_campaign",
            new=AsyncMock(return_value={"updates_written": 0}),
        ) as reconcile, patch.object(
            launch_runtime, "campaign_metrics",
            new=AsyncMock(return_value={"campaign_id": "campaign1"}),
        ) as metrics:
            result = asyncio.run(
                launch_runtime.sync_campaign_outcomes_and_metrics("campaign1")
            )

        self.assertEqual("campaign1", result["campaign_id"])
        snapshot.assert_awaited_once()
        self.assertIs(drafts, reconcile.await_args.kwargs["drafts"])
        self.assertIs(drafts, metrics.await_args.kwargs["drafts"])

    def test_outcome_reconcile_error_holds_control_instead_of_expanding(self):
        activity = {"record_id": "a1", "fields": {"活动ID": "campaign1"}}
        participants = [{"record_id": "p1", "fields": {}}]
        drafts = [{"record_id": "d1", "fields": {}}]
        with patch.object(
            launch_runtime.launch_evidence, "get_activity", new=AsyncMock(return_value=activity),
        ), patch.object(
            launch_runtime, "_participants", new=AsyncMock(return_value=participants),
        ), patch.object(
            launch_runtime.launch_outcomes, "draft_snapshot", new=AsyncMock(return_value=drafts),
        ), patch.object(
            launch_runtime.launch_outcomes, "reconcile_campaign",
            new=AsyncMock(return_value={
                "updates_written": 0,
                "errors": [{"participant_id": "p1", "error": "readback failed"}],
            }),
        ), patch.object(
            launch_runtime, "campaign_metrics",
            new=AsyncMock(return_value={"campaign_id": "campaign1", "action": "expand"}),
        ):
            result = asyncio.run(
                launch_runtime.sync_campaign_outcomes_and_metrics("campaign1")
            )

        self.assertEqual("hold", result["action"])
        self.assertIn("暂停扩池", result["reason"])
        self.assertEqual("degraded", launch_runtime.runtime_job_status(result))

    def test_campaign_metrics_counts_ontime_posts_from_actual_uploads_not_promises(self):
        activity = {"record_id": "a1", "fields": {
            "活动ID": "campaign1", "窗口结束": 1_800_000_000_000,
            "目标上稿数": 20, "目标承诺数": 29,
        }}
        now_ms = int(launch_runtime.time.time() * 1000)
        participants = [
            {"record_id": "p1", "created_time": now_ms, "fields": {
                "关联邮件草稿": ["d1"],
                "承诺上稿时间": 1_790_000_000_000,
                "实际上稿时间": 1_810_000_000_000,
                "参与状态": "已入围", "审核结论": "通过",
                "进入方式": "新开发", "活动分池": "新开发池",
            }},
            {"record_id": "p2", "fields": {
                "关联邮件草稿": ["d2"],
                "承诺上稿时间": 1_795_000_000_000,
                "参与状态": "已入围", "审核结论": "待审核",
                "进入方式": "新开发", "活动分池": "新开发池",
            }},
        ]
        drafts = [
            {"record_id": "d1", "fields": {"发送状态": "已发送", "是否回复": True}},
            {"record_id": "d2", "fields": {"发送状态": "已发送", "是否回复": True}},
        ]
        result = asyncio.run(launch_runtime.campaign_metrics(
            "campaign1", activity=activity, participants=participants, drafts=drafts,
        ))

        self.assertEqual(2, result["commitments"])
        self.assertEqual(1, result["actual_posts"])
        self.assertEqual(0, result["ontime_posts"])
        self.assertEqual(1, result["approved_new_development"])
        self.assertEqual(1, result["approved_new_development_24h"])

    def test_autonomous_append_runs_in_parallel_with_pending_operator_review(self):
        activity = {"record_id": "a1", "fields": {
            "活动ID": "campaign1", "产品主记录ID": "product1",
            "证据排序版本": "v1", "KOL名单阻塞代码": "",
        }}
        pending = {"record_id": "p0", "fields": {
            "关联KOL": {"link_record_ids": ["old"]},
            "参与状态": "已入围", "审核结论": "待审核",
        }}
        preview = {
            "ranking_version": "v1", "evidence_pending": False,
            "candidates": [{
                "contact_id": "new", "decision": "eligible_new_cold",
                "review_decision": "通过", "score": 90,
            }],
        }

        with patch.object(
            launch_runtime.config, "LAUNCH_ACTIVITY_QUEUE_ENABLED", True,
        ), patch.object(
            launch_runtime.launch_evidence, "get_activity", new=AsyncMock(return_value=activity),
        ), patch.object(
            launch_runtime, "_participants", new=AsyncMock(return_value=[pending]),
        ), patch.object(
            launch_runtime.launch_participation, "_participants_by_unique_key",
            new=AsyncMock(return_value=[]),
        ), patch.object(
            launch_runtime.feishu, "create_record", new=AsyncMock(return_value="part-new"),
        ) as create_record:
            result = asyncio.run(launch_runtime.append_auto_approved(
                campaign_id="campaign1", pool_target=20, preview=preview,
                allow_parallel_review=True,
            ))

        self.assertEqual(1, result["created"])
        self.assertEqual(1, result["pending_review_kept_parallel"])
        create_record.assert_awaited_once()

    def test_auto_append_stops_while_any_candidate_still_needs_review(self):
        activity = {"record_id": "a1", "fields": {
            "活动ID": "campaign1", "产品主记录ID": "product1",
            "证据排序版本": "v1", "KOL名单阻塞代码": "",
        }}
        pending = {"record_id": "p0", "fields": {
            "关联KOL": {"link_record_ids": ["old"]},
            "参与状态": "已入围", "审核结论": "待审核",
        }}
        preview = {
            "ranking_version": "v1", "evidence_pending": False,
            "candidates": [{
                "contact_id": "new", "decision": "eligible_new_cold",
                "review_decision": "通过",
            }],
        }

        with patch.object(
            launch_runtime.config, "LAUNCH_ACTIVITY_QUEUE_ENABLED", True,
        ), patch.object(
            launch_runtime.launch_evidence, "get_activity", new=AsyncMock(return_value=activity),
        ), patch.object(
            launch_runtime, "_participants", new=AsyncMock(return_value=[pending]),
        ), patch.object(
            launch_runtime.feishu, "create_record", new=AsyncMock(),
        ) as create_record:
            result = asyncio.run(launch_runtime.append_auto_approved(
                campaign_id="campaign1", pool_target=20, preview=preview,
            ))

        self.assertEqual(1, result["blocked_by_pending_review"])
        self.assertEqual(0, result["created"])
        create_record.assert_not_awaited()

    def test_review_pool_forces_new_candidates_to_pending_without_queueing(self):
        activity = {"record_id": "a1", "fields": {
            "活动ID": "campaign1", "产品主记录ID": "product1",
            "证据排序版本": "v1", "KOL名单阻塞代码": "",
        }}
        candidate = {
            "contact_id": "kol1", "decision": "eligible_new_cold",
            "base_filter_passed": True,
            "review_decision": "通过", "review_route": "系统建议通过",
            "score": 92, "final_priority": 92, "evidence_level": "无加分",
            "country": "US", "language": "en", "platform": "YouTube",
            "followers": 100000, "profile_url": "https://youtube.com/@one",
        }
        created = {"record_id": "part1", "fields": {
            "参与记录ID": "campaign1|product1|KOL|kol1",
            "活动ID": "campaign1", "审核结论": "待审核",
            "关联KOL": {"link_record_ids": ["kol1"]},
        }}

        with patch.object(
            launch_runtime.config, "LAUNCH_ACTIVITY_QUEUE_ENABLED", True,
        ), patch.object(
            launch_runtime.launch_evidence, "get_activity", new=AsyncMock(return_value=activity),
        ), patch.object(
            launch_runtime, "_participants", new=AsyncMock(return_value=[]),
        ), patch.object(
            launch_runtime.launch_participation, "_participants_by_unique_key",
            new=AsyncMock(side_effect=[[], [created]]),
        ), patch.object(
            launch_runtime.feishu, "create_record", new=AsyncMock(return_value="part1"),
        ) as create_record:
            result = asyncio.run(launch_runtime.append_review_candidates(
                campaign_id="campaign1", review_target=1,
                preview={"ranking_version": "v1", "evidence_pending": False,
                         "candidates": [candidate]},
            ))

        self.assertEqual(1, result["created"])
        fields = create_record.await_args.args[1]
        self.assertEqual("待审核", fields["审核结论"])
        self.assertEqual("系统建议通过", fields["系统审核分流"])
        self.assertNotIn("关联邮件草稿", fields)

    def test_autonomous_review_pool_only_adds_operator_boundary_items(self):
        activity = {"record_id": "a1", "fields": {
            "活动ID": "campaign1", "产品主记录ID": "product1",
            "证据排序版本": "v1", "KOL名单阻塞代码": "",
        }}
        common = {
            "decision": "eligible_new_cold", "base_filter_passed": True,
            "score": 80, "final_priority": 80, "evidence_level": "无加分",
            "country": "US", "language": "en", "platform": "YouTube",
            "followers": 10000, "profile_url": "https://youtube.com/@one",
        }
        system_candidate = {
            **common, "contact_id": "system1", "review_decision": "通过",
            "review_route": "系统建议通过",
        }
        operator_candidate = {
            **common, "contact_id": "operator1", "review_decision": "待审核",
            "review_route": "KOL运营审核",
        }
        created = {"record_id": "part1", "fields": {
            "参与记录ID": "campaign1|product1|KOL|operator1",
            "活动ID": "campaign1", "审核结论": "待审核",
            "关联KOL": {"link_record_ids": ["operator1"]},
        }}

        with patch.object(
            launch_runtime.config, "LAUNCH_ACTIVITY_QUEUE_ENABLED", True,
        ), patch.object(
            launch_runtime.launch_evidence, "get_activity", new=AsyncMock(return_value=activity),
        ), patch.object(
            launch_runtime, "_participants", new=AsyncMock(return_value=[]),
        ), patch.object(
            launch_runtime.launch_participation, "_participants_by_unique_key",
            new=AsyncMock(side_effect=[[], [created]]),
        ), patch.object(
            launch_runtime.feishu, "create_record", new=AsyncMock(return_value="part1"),
        ) as create_record:
            result = asyncio.run(launch_runtime.append_review_candidates(
                campaign_id="campaign1", review_target=5,
                preview={"ranking_version": "v1", "evidence_pending": False,
                         "candidates": [system_candidate, operator_candidate]},
                operator_only=True,
            ))

        self.assertEqual(1, result["created"])
        fields = create_record.await_args.args[1]
        self.assertEqual(["operator1"], fields["关联KOL"])

    def test_failed_profile_refresh_becomes_pending_information_not_sendable(self):
        activity = {"record_id": "a1", "fields": {
            "活动ID": "campaign1", "产品主记录ID": "product1",
            "证据排序版本": "v1", "KOL名单阻塞代码": "",
        }}
        candidate = {
            "contact_id": "missing1", "decision": "eligible_new_cold",
            "base_filter_passed": False, "base_filter_reasons": ["资料缺失或过期"],
            "base_filter_reason_codes": ["资料缺失或过期"],
            "profile_refresh_needed": True, "review_decision": "待补资料",
            "review_route": "KOL运营审核", "country": "US", "language": "en",
            "platform": "YouTube", "profile_url": "https://youtube.com/@missing",
        }
        created = {"record_id": "part1", "fields": {
            "审核结论": "待补资料", "关联KOL": {"link_record_ids": ["missing1"]},
        }}
        with patch.object(
            launch_runtime.config, "LAUNCH_ACTIVITY_QUEUE_ENABLED", True,
        ), patch.object(
            launch_runtime.launch_evidence, "get_activity", new=AsyncMock(return_value=activity),
        ), patch.object(
            launch_runtime, "_participants", new=AsyncMock(return_value=[]),
        ), patch.object(
            launch_runtime.launch_participation, "_participants_by_unique_key",
            new=AsyncMock(side_effect=[[], [created]]),
        ), patch.object(
            launch_runtime.feishu, "create_record", new=AsyncMock(return_value="part1"),
        ) as create_record:
            result = asyncio.run(launch_runtime.append_review_candidates(
                campaign_id="campaign1", review_target=5,
                preview={"ranking_version": "v1", "evidence_pending": False,
                         "candidates": [candidate]}, operator_only=True,
            ))

        self.assertEqual(1, result["created"])
        fields = create_record.await_args.args[1]
        self.assertEqual("待补资料", fields["审核结论"])
        self.assertEqual(["资料缺失或过期"], fields["审核原因代码"])
        self.assertNotIn("关联邮件草稿", fields)

    def test_autonomous_refill_reports_blocked_when_quota_remains_but_supply_makes_no_progress(self):
        metrics = {
            "campaign_id": "campaign1", "participants": 42, "sent": 4,
            "replies": 0, "commitments": 0, "ontime_posts": 0,
            "action": "expand", "reason": "commitment gap",
        }
        activity = {"record_id": "a1", "fields": {
            "活动ID": "campaign1", "产品主记录ID": "product1",
            "活动目标语言": ["en", "de", "es"],
            "运行模式": "正式运行", "状态": "正式执行中",
        }}
        product = {"record_id": "product1", "fields": {
            "品牌": "POWKONG", "产品英文名": "Piranha Plant 2 Dock",
        }}
        preview = {
            "summary": {"eligible_new_cold": 0},
            "profile_refresh_candidate_ids": [], "profile_refresh_candidates": [],
            "candidates": [],
        }
        inventories = [
            {"ready": 0, "pending_review": 20},
            {"ready": 0, "pending_review": 20},
            {"ready": 0, "pending_review": 20},
        ]

        with patch.object(
            launch_runtime, "sync_campaign_outcomes_and_metrics", new=AsyncMock(return_value=metrics),
        ), patch.object(
            launch_runtime.launch_evidence, "get_activity", new=AsyncMock(return_value=activity),
        ), patch.object(
            launch_runtime.feishu, "get_record", new=AsyncMock(return_value=product),
        ), patch.object(
            launch_runtime, "_brand_quota_snapshot",
            new=AsyncMock(return_value={"brand": "POWKONG", "cap": 120,
                                        "sent_24h": 13, "remaining": 107}),
        ), patch.object(
            launch_runtime, "_campaign_ready_inventory", new=AsyncMock(side_effect=inventories),
        ), patch.object(
            launch_runtime.launch_candidate_preview, "preview_candidates",
            new=AsyncMock(return_value=preview),
        ), patch.object(
            launch_runtime, "append_auto_approved", new=AsyncMock(return_value={"created": 0}),
        ), patch.object(
            launch_runtime, "queue_approved", new=AsyncMock(return_value={"queued": 0}),
        ), patch.object(
            launch_runtime.keyword_supply, "ensure_campaign_supply",
            new=AsyncMock(return_value={"ok": False, "created": 0,
                                        "error": "no unused discovery keywords"}),
        ), patch.object(
            launch_runtime, "append_review_candidates", new=AsyncMock(return_value={"created": 0}),
        ), patch.object(
            launch_runtime, "_notify_operator_review", new=AsyncMock(return_value={"sent": 0}),
        ):
            result = asyncio.run(launch_runtime.autonomous_refill(campaign_id="campaign1"))

        self.assertEqual("supply_blocked", result["business_outcome"])
        self.assertFalse(result["made_supply_progress"])
        self.assertEqual(0, sum(result["supply_progress_breakdown"].values()))
        self.assertEqual(107, result["quota"]["remaining"])
        self.assertEqual(0, result["inventory_after"])

    def test_autonomous_refill_refreshes_then_requests_discovery_without_lowering_filters(self):
        campaign_id = launch_runtime.launch_evidence_author_import.DAVE_CAMPAIGN_ID
        metrics = {
            "campaign_id": campaign_id, "participants": 20, "sent": 4,
            "approved_new_development": 9, "approved_new_development_24h": 3,
            "replies": 0, "commitments": 0, "ontime_posts": 0,
            "action": "expand", "reason": "commitment gap",
        }
        activity = {"record_id": "a1", "fields": {
            "活动ID": campaign_id, "产品主记录ID": "product1",
            "活动目标语言": ["en", "de", "es"],
            "运行模式": "正式运行", "状态": "正式执行中",
        }}
        product = {"record_id": "product1", "fields": {
            "品牌": "POWKONG", "产品英文名": "Piranha Plant 2 Dock",
        }}
        preview1 = {
            "summary": {"eligible_new_cold": 2}, "profile_refresh_candidate_ids": ["k1", "k2"],
        }
        preview2 = {
            "summary": {"eligible_new_cold": 3}, "profile_refresh_candidate_ids": [],
        }
        inventories = [
            {"ready": 0, "pending_review": 1,
             "pending_contact_ids": ["pending1"]},
            {"ready": 8, "pending_review": 0},
            {"ready": 12, "pending_review": 0},
        ]

        with patch.object(
            launch_runtime, "sync_campaign_outcomes_and_metrics", new=AsyncMock(return_value=metrics),
        ), patch.object(
            launch_runtime.launch_evidence, "get_activity", new=AsyncMock(return_value=activity),
        ), patch.object(
            launch_runtime.feishu, "get_record", new=AsyncMock(return_value=product),
        ), patch.object(
            launch_runtime, "_brand_quota_snapshot",
            new=AsyncMock(return_value={"brand": "POWKONG", "cap": 120,
                                        "sent_24h": 13, "remaining": 107}),
        ), patch.object(
            launch_runtime, "_campaign_ready_inventory",
            new=AsyncMock(side_effect=inventories),
        ), patch.object(
            launch_runtime.launch_candidate_preview, "preview_candidates",
            new=AsyncMock(side_effect=[preview1, preview2]),
        ), patch.object(
            launch_runtime, "append_auto_approved",
            new=AsyncMock(side_effect=[{"created": 2}, {"created": 3}]),
        ) as append_auto, patch.object(
            launch_runtime, "queue_approved",
            new=AsyncMock(side_effect=[{"queued": 8}, {"queued": 4}]),
        ), patch.object(
            launch_runtime.relabel, "run_profile_records",
            new=AsyncMock(return_value={"processed": 3, "writes": 3}),
        ) as refresh, patch.object(
            launch_runtime, "reconcile_pending_participant_reviews",
            new=AsyncMock(return_value={"checked": 1, "updated": 1,
                                        "auto_passed": 1, "actionable_pending": 0}),
        ) as reconcile, patch.object(
            launch_runtime.launch_evidence_author_import, "run_continuation_import",
            new=AsyncMock(return_value={
                "offset": 17, "next_offset": 34, "planned": 3,
                "participation_writes": 3, "drafts_created": 0, "emails_sent": 0,
            }),
        ) as continue_evidence, patch.object(
            launch_runtime.keyword_supply, "ensure_campaign_supply",
            new=AsyncMock(return_value={"ok": True, "created": 3}),
        ) as discover, patch.object(
            launch_runtime, "append_review_candidates",
            new=AsyncMock(return_value={"created": 1}),
        ) as review, patch.object(
            launch_runtime, "_notify_operator_review", new=AsyncMock(return_value={"sent": 1}),
        ) as notify_review:
            result = asyncio.run(launch_runtime.autonomous_refill(
                campaign_id=campaign_id, buffer_days=2,
                runtime_job_id="launchruntime-current-job",
            ))

        self.assertEqual("expand", result["action"])
        self.assertEqual(214, result["target_ready_inventory"])
        refresh.assert_awaited_once_with(
            ["pending1", "k1", "k2"], dry_run=False, limit=3,
        )
        reconcile.assert_awaited_once()
        continue_evidence.assert_awaited_once_with(
            campaign_id=campaign_id,
            source_job_id="launchruntime-current-job",
            offset=17, sample_limit=20, import_limit=3, commit=True,
        )
        self.assertEqual(2, append_auto.await_count)
        self.assertTrue(all(call.kwargs["allow_parallel_review"] for call in append_auto.await_args_list))
        discover.assert_awaited_once()
        self.assertEqual(3, discover.await_args.kwargs["approved_candidates"])
        review.assert_awaited_once()
        self.assertTrue(review.await_args.kwargs["operator_only"])
        notify_review.assert_awaited_once_with(
            campaign_id=campaign_id, activity=activity, created=4,
        )
        self.assertEqual(12, result["inventory_after"])
        self.assertEqual("ready_inventory_created", result["business_outcome"])
        self.assertTrue(result["made_supply_progress"])
        self.assertEqual(3, result["supply_progress_breakdown"]["evidence_candidates_imported"])
        self.assertGreater(sum(result["supply_progress_breakdown"].values()), 0)

    def test_dave_evidence_cursor_uses_latest_completed_runtime_result(self):
        old = launch_runtime.RUNTIME_JOB_PREFIX + json.dumps({
            "job_id": "launchruntime-old", "status": "success",
            "result": {"evidence_continuation": {"next_offset": 54}},
        })
        current = launch_runtime.RUNTIME_JOB_PREFIX + json.dumps({
            "job_id": "launchruntime-current", "status": "running",
        })

        offset = launch_runtime._dave_evidence_continuation_offset(
            {"数据口径备注": old + "\n" + current},
            current_job_id="launchruntime-current",
        )

        self.assertEqual(54, offset)

    def test_runtime_job_persistence_keeps_other_job_ids(self):
        state = {"note": "业务备注"}

        async def get_activity(_campaign_id):
            return {"record_id": "activity1", "fields": {
                "数据口径备注": state["note"],
            }}

        async def update_record(_table, _record_id, fields):
            state["note"] = fields["数据口径备注"]
            return {"record_id": "activity1", "fields": fields}

        async def scenario():
            await launch_runtime.persist_runtime_job(
                campaign_id="campaign1", job_id="launchruntime-a",
                mode="autonomous", status="running", started_ts=100,
            )
            await launch_runtime.persist_runtime_job(
                campaign_id="campaign1", job_id="launchruntime-b",
                mode="evidence_author_continuation", status="running", started_ts=101,
            )
            await launch_runtime.persist_runtime_job(
                campaign_id="campaign1", job_id="launchruntime-a",
                mode="autonomous", status="success",
                result=launch_runtime._with_business_outcome({
                    "action": "expand", "quota": {"remaining": 10},
                    "inventory_after": 1,
                }), started_ts=100,
            )
            return (
                await launch_runtime.load_runtime_job("campaign1", "launchruntime-a"),
                await launch_runtime.load_runtime_job("campaign1", "launchruntime-b"),
            )

        with patch.object(
            launch_runtime.launch_evidence, "get_activity", new=get_activity,
        ), patch.object(
            launch_runtime.feishu, "update_record", new=update_record,
        ):
            first, second = asyncio.run(scenario())

        self.assertEqual("success", first["status"])
        self.assertEqual("running", second["status"])
        self.assertIn("业务备注", state["note"])

    def test_existing_pending_discovery_tasks_keep_supply_in_progress(self):
        result = launch_runtime._with_business_outcome({
            "action": "expand",
            "quota": {"remaining": 107},
            "inventory_after": 0,
            "append": {"created": 0},
            "queue": {"queued": 0},
            "profile_refresh": {"writes": 0},
            "append_after_refresh": {"created": 0},
            "queue_after_refresh": {"queued": 0},
            "discovery": {"created": 0, "active_pending_before": 4,
                          "stale_pending_before": 0},
            "review_pool": {"created": 0},
        })

        self.assertEqual("supply_in_progress", result["business_outcome"])
        self.assertTrue(result["made_supply_progress"])
        self.assertEqual(4, result["supply_progress_breakdown"]["active_discovery_tasks"])

    def test_partial_evidence_continuation_is_degraded_even_with_review_progress(self):
        result = launch_runtime._with_business_outcome({
            "action": "expand", "quota": {"remaining": 100},
            "inventory_after": 0,
            "evidence_continuation": {
                "partial_failure": True, "participation_writes": 1,
                "incomplete_controlled_imports": 1,
            },
        })

        self.assertEqual("evidence_continuation_failed", result["business_outcome"])
        self.assertEqual("degraded", launch_runtime.runtime_job_status(result))
        summary = launch_runtime._runtime_result_summary(result)
        self.assertTrue(summary["evidence_continuation"]["partial_failure"])
        self.assertEqual(
            1, summary["evidence_continuation"]["incomplete_controlled_imports"],
        )

    def test_runtime_summary_keeps_discovery_quality_gate_for_audit(self):
        summary = launch_runtime._runtime_result_summary({
            "action": "expand", "quota": {"remaining": 107}, "inventory_after": 0,
            "business_outcome": "supply_blocked", "made_supply_progress": False,
            "supply_progress_breakdown": {},
            "discovery": {
                "created": 0, "skipped": "quality_cooldown",
                "quality_gate": {"mode": "cooldown", "recent_valid_emails": 0},
            },
        })

        self.assertEqual("quality_cooldown", summary["discovery"]["skipped"])
        self.assertEqual("cooldown", summary["discovery"]["quality_gate"]["mode"])

    def test_runtime_summary_keeps_pending_review_reconcile_for_restart_audit(self):
        summary = launch_runtime._runtime_result_summary({
            "action": "expand", "quota": {"remaining": 100}, "inventory_after": 0,
            "business_outcome": "supply_blocked", "made_supply_progress": False,
            "supply_progress_breakdown": {},
            "profile_refresh": {"processed": 20, "writes": 8},
            "pending_review_reconcile": {
                "updated": 8, "auto_passed": 3, "actionable_pending": 5,
                "missing_snapshot": 0,
            },
            "queue_after_refresh": {"queued": 3},
        })

        self.assertEqual(8, summary["profile_refresh"]["writes"])
        self.assertEqual(3, summary["pending_review_reconcile"]["auto_passed"])
        self.assertEqual(5, summary["pending_review_reconcile"]["actionable_pending"])
        self.assertEqual(3, summary["queue_after_refresh"]["queued"])

    def test_intentional_quality_cooldown_is_visible_without_false_failure(self):
        result = launch_runtime._with_business_outcome({
            "action": "expand", "quota": {"remaining": 107},
            "inventory_after": 0,
            "discovery": {"created": 0, "active_pending_before": 0,
                          "skipped": "quality_cooldown"},
        })

        self.assertEqual("supply_cooling_down", result["business_outcome"])
        self.assertEqual("success", launch_runtime.runtime_job_status(result))

    def test_stale_pending_discovery_tasks_do_not_mask_supply_block(self):
        result = launch_runtime._with_business_outcome({
            "action": "expand", "quota": {"remaining": 107}, "inventory_after": 0,
            "discovery": {"created": 0, "active_pending_before": 0,
                          "stale_pending_before": 4},
        })

        self.assertEqual("supply_blocked", result["business_outcome"])
        self.assertFalse(result["made_supply_progress"])

    def test_profile_refresh_writes_alone_do_not_mask_supply_block(self):
        result = launch_runtime._with_business_outcome({
            "action": "expand", "quota": {"remaining": 106}, "inventory_after": 0,
            "append": {"created": 0}, "queue": {"queued": 0},
            "profile_refresh": {"writes": 30},
            "append_after_refresh": {"created": 0},
            "queue_after_refresh": {"queued": 0},
            "discovery": {"created": 0, "active_pending_before": 0},
            "review_pool": {"created": 0},
        })

        self.assertEqual(30, result["supply_progress_breakdown"]["profile_refresh_writes"])
        self.assertEqual("supply_blocked", result["business_outcome"])
        self.assertFalse(result["made_supply_progress"])

    def test_early_hold_result_has_complete_business_contract(self):
        result = launch_runtime._with_business_outcome({
            "campaign_id": "campaign1", "action": "hold", "held": True,
            "runtime": "campaign_not_formally_active",
        })

        self.assertEqual("held", result["business_outcome"])
        self.assertEqual(0, result["quota"]["remaining"])
        self.assertEqual(0, result["inventory_after"])
        self.assertFalse(result["made_supply_progress"])

    def test_outcome_reconcile_failure_overrides_supply_success_and_is_persisted(self):
        result = launch_runtime._with_business_outcome({
            "action": "expand", "quota": {"remaining": 10}, "inventory_after": 5,
            "outcome_reconcile": {
                "ok": False, "updates_written": 0,
                "errors": [{"participant_id": "p1", "error": "readback failed"}],
            },
        })
        summary = launch_runtime._runtime_result_summary(result)

        self.assertEqual("outcome_reconcile_failed", result["business_outcome"])
        self.assertEqual("degraded", launch_runtime.runtime_job_status(result))
        self.assertEqual(1, result["outcome_reconcile_error_count"])
        self.assertEqual(["p1"], summary["outcome_reconcile"]["failed_participant_ids"])

    def test_autonomous_refill_holds_when_campaign_is_not_formally_active(self):
        metrics = {
            "campaign_id": "campaign1", "participants": 20, "sent": 4,
            "replies": 0, "commitments": 0, "ontime_posts": 0,
            "action": "expand", "reason": "commitment gap",
        }
        activity = {"record_id": "a1", "fields": {
            "活动ID": "campaign1", "产品主记录ID": "product1",
            "运行模式": "影子试跑", "状态": "待人工批准",
        }}
        with patch.object(
            launch_runtime, "sync_campaign_outcomes_and_metrics", new=AsyncMock(return_value=metrics),
        ), patch.object(
            launch_runtime.launch_evidence, "get_activity", new=AsyncMock(return_value=activity),
        ), patch.object(
            launch_runtime.launch_candidate_preview, "preview_candidates", new=AsyncMock(),
        ) as preview:
            result = asyncio.run(launch_runtime.autonomous_refill(campaign_id="campaign1"))

        self.assertTrue(result["held"])
        self.assertEqual("campaign_not_formally_active", result["runtime"])
        preview.assert_not_awaited()

    def test_autonomous_refill_stops_after_campaign_window_end(self):
        metrics = {
            "campaign_id": "campaign1", "participants": 20, "sent": 4,
            "replies": 0, "commitments": 0, "ontime_posts": 0,
            "action": "expand", "reason": "commitment gap",
        }
        activity = {"record_id": "a1", "fields": {
            "活动ID": "campaign1", "产品主记录ID": "product1",
            "运行模式": "正式运行", "状态": "正式执行中",
            "发送邮件授权": True, "窗口结束": 1_700_000_000_000,
        }}
        update = AsyncMock()
        with patch.object(
            launch_runtime, "sync_campaign_outcomes_and_metrics", new=AsyncMock(return_value=metrics),
        ), patch.object(
            launch_runtime.launch_evidence, "get_activity", new=AsyncMock(return_value=activity),
        ), patch.object(launch_runtime.time, "time", return_value=1_800_000_000), patch.object(
            launch_runtime.feishu, "update_record", new=update,
        ), patch.object(
            launch_runtime.launch_candidate_preview, "preview_candidates", new=AsyncMock(),
        ) as preview:
            result = asyncio.run(launch_runtime.autonomous_refill(campaign_id="campaign1"))

        self.assertTrue(result["stopped"])
        self.assertEqual("campaign_window_ended", result["runtime"])
        update.assert_awaited_once_with(
            launch_runtime.config.T_LAUNCH_CAMPAIGN, "a1", {"发送邮件授权": False},
        )
        preview.assert_not_awaited()

    def test_review_pool_requires_configured_competitor_evidence_to_be_applied(self):
        activity = {"record_id": "a1", "fields": {
            "活动ID": "campaign1", "产品主记录ID": "product1",
            "证据排序版本": "v1", "KOL名单阻塞代码": "",
            "竞品证据模式": launch_runtime.launch_evidence.MODE_REUSE,
        }}

        with patch.object(
            launch_runtime.config, "LAUNCH_ACTIVITY_QUEUE_ENABLED", True,
        ), patch.object(
            launch_runtime.launch_evidence, "get_activity", new=AsyncMock(return_value=activity),
        ):
            with self.assertRaises(launch_runtime.LaunchRuntimeError):
                asyncio.run(launch_runtime.append_review_candidates(
                    campaign_id="campaign1", review_target=1,
                    preview={
                        "ranking_version": "v1", "evidence_pending": False,
                        "evidence_status": "已就绪", "competitor_evidence_applied": False,
                        "candidates": [],
                    },
                ))

    def test_deterministic_fallback_is_product_specific_and_placeholder_free(self):
        kol = {"fields": {"账号名": "Indie Alpaca", "国家": "US"}}
        product = {"fields": {
            "产品英文名": "POWKONG Piranha Plant 2 Dock",
            "品牌": "POWKONG",
            "官网链接": {"link": "https://example.com/piranha"},
        }}
        draft = launch_runtime._deterministic_fallback_draft(kol, product, "POWKONG")
        self.assertIn("Piranha Plant 2 Dock", draft["body"])
        self.assertIn("https://example.com/piranha", draft["body"])
        self.assertNotRegex(draft["body"], r"\[TBD|待填|\$\d")
        self.assertTrue(draft["deterministic_fallback"])

    def test_activity_queue_gate_requires_formal_authorization_and_exact_links(self):
        draft = {
            "record_id": "draft1",
            "fields": {
                "邮件草稿ID": "launchq-abcd",
                "关联KOL": {"link_record_ids": ["kol1"]},
                "关联产品": {"link_record_ids": ["product1"]},
                "发送邮箱": "FUNLAB",
            },
        }
        participant = {
            "record_id": "part1",
            "fields": {
                "活动ID": "campaign1", "产品家族ID": "product1", "对象类型": "KOL",
                "关联KOL": {"link_record_ids": ["kol1"]},
                "关联邮件草稿": {"link_record_ids": ["draft1"]},
                "参与状态": "已入围", "审核结论": "通过",
                "进入方式": "新开发", "活动分池": "新开发池",
                "名单版本": "evidence-v1", "排序版本": "evidence-v1",
            },
        }
        activity = {
            "record_id": "activity1",
            "fields": {
                "活动ID": "campaign1", "产品主记录ID": "product1",
                "运行模式": "正式运行", "状态": "正式执行中",
                "发送邮件授权": True, "KOL已锁定名单版本": "evidence-v1",
                "证据排序版本": "evidence-v1", "名单锁定授权": True,
                "邮件Raw验证证书": _certificate(),
                "KOL名单阻塞代码": "",
            },
        }
        ok, reasons = auto_send.validate_activity_queue_gate(activity, participant, draft)
        self.assertTrue(ok, reasons)

        activity["fields"]["发送邮件授权"] = False
        ok, reasons = auto_send.validate_activity_queue_gate(activity, participant, draft)
        self.assertFalse(ok)
        self.assertIn("发送邮件授权", reasons)

    def test_regular_locked_draft_is_denied_but_valid_activity_draft_is_released(self):
        activity_draft = {
            "record_id": "draft1",
            "fields": {
                "邮件草稿ID": "launchq-abcd", "邮件草稿来源": "cold",
                "关联KOL": {"link_record_ids": ["kol1"]},
                "关联产品": {"link_record_ids": ["product1"]},
                "发送邮箱": "FUNLAB",
            },
        }
        regular_draft = {
            "record_id": "draft2",
            "fields": {
                "邮件草稿ID": "regular1", "邮件草稿来源": "cold",
                "关联KOL": {"link_record_ids": ["kol2"]},
                "关联产品": {"link_record_ids": ["product1"]},
            },
        }
        participant = {
            "record_id": "part1",
            "fields": {
                "活动ID": "campaign1", "产品家族ID": "product1", "对象类型": "KOL",
                "关联KOL": {"link_record_ids": ["kol1"]},
                "关联邮件草稿": {"link_record_ids": ["draft1"]},
                "参与状态": "已入围", "审核结论": "通过", "进入方式": "新开发",
                "活动分池": "新开发池", "名单版本": "evidence-v1", "排序版本": "evidence-v1",
            },
        }
        activity = {
            "record_id": "activity1",
            "fields": {
                "活动ID": "campaign1", "产品主记录ID": "product1",
                "运行模式": "正式运行", "状态": "正式执行中",
                "发送邮件授权": True, "KOL已锁定名单版本": "evidence-v1",
                "证据排序版本": "evidence-v1", "名单锁定授权": True,
                "邮件Raw验证证书": _certificate(),
                "KOL名单阻塞代码": "",
            },
        }

        with patch.object(
            auto_send.feishu, "fetch_all_records",
            new=AsyncMock(side_effect=[[participant], [activity]]),
        ):
            released, failures = asyncio.run(auto_send.resolve_activity_queue_releases(
                [activity_draft, regular_draft]
            ))
        self.assertEqual({"draft1"}, released)
        self.assertEqual(0, failures)

    def test_zoho_count_uses_sent_folder_timestamp(self):
        now_ms = 1_800_000_000_000
        with patch.object(auto_send.time, "time", return_value=now_ms / 1000), patch.object(
            auto_send.zoho, "list_sent_messages", new=AsyncMock(return_value={
                "messages": [
                    {"messageId": "new", "sentDateInGMT": now_ms - 1000},
                    {"messageId": "old", "sentDateInGMT": now_ms - 90_000_000},
                ]
            }),
        ):
            counts, errors = asyncio.run(auto_send.zoho_sent_counts_24h(["FUNLAB"]))
        self.assertEqual(1, counts["FUNLAB"])
        self.assertEqual({}, errors)

    def test_feedback_control_uses_configured_commitment_target(self):
        self.assertEqual(
            "expand",
            launch_runtime.recommend_feedback_action(
                target_posts=20, target_commitments=29, commitments=4, sent=30, replies=3,
            )["action"],
        )
        self.assertEqual(
            "hold",
            launch_runtime.recommend_feedback_action(
                target_posts=20, target_commitments=29, commitments=27, sent=90, replies=20,
            )["action"],
        )
        self.assertEqual(
            "stop",
            launch_runtime.recommend_feedback_action(
                target_posts=20, target_commitments=29, commitments=29, sent=100, replies=30,
            )["action"],
        )

    def test_gate_fails_without_exact_lock_version_or_raw_certificate(self):
        draft = {"record_id": "draft1", "fields": {"邮件草稿ID": "launchq-a", "关联KOL": {"link_record_ids": ["kol1"]}, "关联产品": {"link_record_ids": ["product1"]}, "发送邮箱": "FUNLAB"}}
        participant = {"record_id": "part1", "fields": {"活动ID": "campaign1", "产品家族ID": "product1", "关联KOL": {"link_record_ids": ["kol1"]}, "关联邮件草稿": {"link_record_ids": ["draft1"]}, "参与状态": "已入围", "审核结论": "通过", "进入方式": "新开发", "活动分池": "新开发池", "名单版本": "v1", "排序版本": "v1"}}
        activity = {"record_id": "a1", "fields": {"活动ID": "campaign1", "产品主记录ID": "product1", "运行模式": "正式运行", "状态": "正式执行中", "发送邮件授权": True, "名单锁定授权": True, "KOL已锁定名单版本": "v1", "证据排序版本": "v2", "KOL名单阻塞代码": ""}}
        ok, reasons = auto_send.validate_activity_queue_gate(activity, participant, draft)
        self.assertFalse(ok)
        self.assertIn("锁定版本一致", reasons)
        self.assertIn("Raw验证证书", reasons)

    def test_product_type_instruction_distinguishes_dock_from_controller(self):
        dock = enrich._product_type_instruction("POWKONG Piranha Plant 2 Dock", "充电底座")
        controller = enrich._product_type_instruction("FUNLAB Dave Controller", "游戏手柄")
        self.assertIn("dock", dock.lower())
        self.assertIn("严禁称为 controller", dock)
        self.assertIn("controller", controller.lower())
        self.assertIn("严禁把整个产品叫成 dock", controller)


if __name__ == "__main__":
    unittest.main()

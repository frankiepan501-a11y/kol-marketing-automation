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
            launch_runtime, "campaign_metrics", new=AsyncMock(return_value=metrics),
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
        metrics = {
            "campaign_id": "campaign1", "participants": 20, "sent": 4,
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
        preview1 = {
            "summary": {"eligible_new_cold": 2}, "profile_refresh_candidate_ids": ["k1", "k2"],
        }
        preview2 = {
            "summary": {"eligible_new_cold": 3}, "profile_refresh_candidate_ids": [],
        }
        inventories = [
            {"ready": 0, "pending_review": 0},
            {"ready": 8, "pending_review": 0},
            {"ready": 12, "pending_review": 0},
        ]

        with patch.object(
            launch_runtime, "campaign_metrics", new=AsyncMock(return_value=metrics),
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
            new=AsyncMock(return_value={"processed": 2, "writes": 2}),
        ) as refresh, patch.object(
            launch_runtime.keyword_supply, "ensure_campaign_supply",
            new=AsyncMock(return_value={"ok": True, "created": 3}),
        ) as discover, patch.object(
            launch_runtime, "append_review_candidates",
            new=AsyncMock(return_value={"created": 1}),
        ) as review, patch.object(
            launch_runtime, "_notify_operator_review", new=AsyncMock(return_value={"sent": 1}),
        ):
            result = asyncio.run(launch_runtime.autonomous_refill(
                campaign_id="campaign1", buffer_days=2,
            ))

        self.assertEqual("expand", result["action"])
        self.assertEqual(214, result["target_ready_inventory"])
        refresh.assert_awaited_once_with(["k1", "k2"], dry_run=False, limit=2)
        self.assertEqual(2, append_auto.await_count)
        self.assertTrue(all(call.kwargs["allow_parallel_review"] for call in append_auto.await_args_list))
        discover.assert_awaited_once()
        review.assert_awaited_once()
        self.assertTrue(review.await_args.kwargs["operator_only"])
        self.assertEqual(12, result["inventory_after"])
        self.assertEqual("ready_inventory_created", result["business_outcome"])
        self.assertTrue(result["made_supply_progress"])
        self.assertGreater(sum(result["supply_progress_breakdown"].values()), 0)

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
            launch_runtime, "campaign_metrics", new=AsyncMock(return_value=metrics),
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
            launch_runtime, "campaign_metrics", new=AsyncMock(return_value=metrics),
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

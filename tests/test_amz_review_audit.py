import asyncio
import json
import unittest

from app import amz_assistant
from app import amz_review_audit as audit


class AmzReviewAuditPureTests(unittest.TestCase):
    def test_normalize_issue_keeps_required_context_and_listing_link(self):
        issue = audit.normalize_issue({
            "source_type": "review",
            "review_id": "rv-1",
            "store_name": "Fanlepu-US",
            "site": "US",
            "erp_name": "FF05A Luminex Controller",
            "asin": "B0ABC12345",
            "principal_info": [{"principal_name": "黄奕纯"}],
            "rating": "2 stars",
            "title": "Bad connection",
            "review_text": "Disconnects every few minutes.",
        })

        self.assertEqual("review", issue["source_type"])
        self.assertEqual("黄奕纯", issue["owner"])
        self.assertEqual("P1", issue["severity"])
        self.assertEqual("https://www.amazon.com/dp/B0ABC12345", issue["listing_url"])
        self.assertIn("AMZ_REVIEW:US:B0ABC12345:rv-1", issue["issue_key"])

    def test_alert_thresholds_follow_plan(self):
        review_2 = audit.normalize_issue({"source_type": "review", "review_id": "r2", "site": "US", "asin": "A", "rating": 2})
        review_3_plain = audit.normalize_issue({"source_type": "review", "review_id": "r3", "site": "US", "asin": "B", "rating": 3})
        review_3_home = audit.normalize_issue({"source_type": "review", "review_id": "r3h", "site": "US", "asin": "C", "rating": 3, "homepage_visible": True})
        feedback_3 = audit.normalize_issue({"source_type": "feedback", "feedback_id": "f3", "site": "CA", "asin": "D", "rating": 3})
        feedback_4 = audit.normalize_issue({"source_type": "feedback", "feedback_id": "f4", "site": "CA", "asin": "E", "rating": 4})

        self.assertTrue(audit.should_alert_issue(review_2))
        self.assertFalse(audit.should_alert_issue(review_3_plain))
        self.assertTrue(audit.should_alert_issue(review_3_home))
        self.assertTrue(audit.should_alert_issue(feedback_3))
        self.assertFalse(audit.should_alert_issue(feedback_4))

    def test_issue_card_contains_multiselect_and_stable_actions(self):
        issue = audit.normalize_issue({
            "source_type": "feedback",
            "feedback_id": "fb-1",
            "site": "CA",
            "asin": "B0FB000001",
            "erp_name": "PK Dock",
            "rating": 1,
            "feedback_text": "Terrible seller feedback.",
        })
        issue["record_id"] = "rec_test"
        card = audit.build_issue_card(issue)
        rendered = json.dumps(card, ensure_ascii=False)

        self.assertIn("multi_select_static", rendered)
        self.assertIn("已投诉Amazon / 已开Case", rendered)
        self.assertIn("主动作：提交处理结果（可多选）", rendered)
        self.assertIn("不是排他选择", rendered)
        self.assertIn("辅助动作：不是处理方式，不是必点", rendered)
        self.assertIn("同步到客服库（可选）", rendered)
        self.assertIn("客观无法移除，申请观察", rendered)
        self.assertIn("amz_issue_submit_actions", rendered)
        self.assertIn("amz_issue_create_cs_ticket", rendered)
        self.assertNotIn("amz_issue_request_observation", rendered)
        self.assertIn("B0FB000001", rendered)
        self.assertIn("https://www.amazon.ca/dp/B0FB000001", rendered)
        self.assertIn("🚨 **处理要求**", rendered)
        self.assertIn("打开Listing前台", rendered)

    def test_homepage_group_uses_parent_issue_key_and_exports_position_fields(self):
        issue = audit.normalize_homepage_group_issue({
            "domain": "www.amazon.co.uk",
            "country": "英国",
            "store": "FunlabDirect-UK",
            "owner": "林明坚",
            "parent_asin": "B0PARENT01",
            "erp_name": "FF Controller",
            "tags": ["ERP", "战略"],
            "active_children": ["B0CHILD001(FD-001-UK)", "B0CHILD002(FD-002-UK)"],
            "representative_asin": "B0CHILD002",
            "parent_url": "https://www.amazon.co.uk/dp/B0CHILD002",
            "top8_negative_cards": 2,
            "min_negative_position": 2,
            "difficulty": "难：第1-2位，需要较多好评才能挤走",
            "positions": [
                {"asin": "B0CHILD002", "position": 2, "star": 1, "review_id": "rv-bad", "title": "Broken on arrival"},
                {"asin": "B0CHILD001", "position": 6, "star": 2, "review_id": "rv-low", "title": "Disconnects"},
            ],
            "cross_site_negative": ["德国 www.amazon.de FunlabDirect-DE B0CROSS001 pos5 1条"],
        })

        self.assertEqual("homepage", issue["source_type"])
        self.assertEqual("B0PARENT01", issue["asin"])
        self.assertEqual("B0PARENT01", issue["parent_asin"])
        self.assertEqual("B0CHILD002", issue["representative_asin"])
        self.assertEqual("AMZ_HOMEPAGE:英国:B0PARENT01", issue["issue_key"])
        self.assertEqual(2, issue["min_negative_position"])
        self.assertEqual(["ERP", "战略"], issue["listing_tags"])

        fields = audit.issue_to_fields(issue)
        self.assertEqual("Homepage", fields["来源类型"])
        self.assertEqual("B0PARENT01", fields["父体ASIN"])
        self.assertEqual("B0CHILD002", fields["代表子体ASIN"])
        self.assertIn("B0CHILD001", fields["在售子体ASIN"])
        self.assertEqual(["ERP", "战略"], fields["Listing标签"])
        self.assertIn("#2", fields["首页差评位置"])
        self.assertEqual(2, fields["最靠前差评位置"])
        self.assertIn("第1-2位", fields["挤走难度"])
        self.assertIn("www.amazon.de", fields["跨站点同ERP差评"])

    def test_homepage_parent_card_renders_children_positions_and_cross_site_context(self):
        issue = audit.normalize_homepage_group_issue({
            "domain": "www.amazon.ca",
            "country": "加拿大",
            "store": "BHANES-CA",
            "owner": "陈翔宇",
            "parent_asin": "B0PARENTCA",
            "erp_name": "Switch 2 Dock",
            "tags": ["ERP", "主力"],
            "active_children": ["B0CA000001(BS-001-CA)", "B0CA000002(BS-002-CA)"],
            "representative_asin": "B0CA000001",
            "parent_url": "https://www.amazon.ca/dp/B0CA000001",
            "min_negative_position": 1,
            "positions": [{"asin": "B0CA000001", "position": 1, "star": 1, "review_id": "rv-ca", "title": "Stopped charging"}],
            "cross_site_negative": ["美国 www.amazon.com BHANES-US B0US000001 pos4 1条"],
        })
        issue["record_id"] = "rec_homepage_parent"

        issue_card = json.dumps(audit.build_issue_card(issue), ensure_ascii=False)
        daily_card = json.dumps(audit.build_daily_digest_card("陈翔宇", [issue]), ensure_ascii=False)
        failed_card = json.dumps(audit.build_recheck_failed_card("陈翔宇", [issue]), ensure_ascii=False)

        self.assertIn("本卡按“站点父体”追踪", issue_card)
        self.assertIn("父体 / 子体范围", issue_card)
        self.assertIn("B0CA000002", issue_card)
        self.assertIn("首页位置与难度", issue_card)
        self.assertIn("#1", issue_card)
        self.assertIn("跨站点同ERP差评", issue_card)
        self.assertIn("www.amazon.com", issue_card)
        self.assertIn("父体:", daily_card)
        self.assertIn("位置:", daily_card)
        self.assertIn("首页差评位置", failed_card)

    def test_recheck_failed_and_success_cards_have_clear_visual_tone(self):
        issue = audit.normalize_issue({
            "source_type": "review",
            "review_id": "rv-tone",
            "site": "US",
            "asin": "B0TONE0001",
            "erp_name": "FF Controller",
            "rating": 1,
            "review_text": "Still bad after handling.",
        })
        issue["handled_at_ms"] = audit.now_ms() - 8 * 86_400_000
        issue["handled_actions"] = ["已投诉Amazon / 已开Case"]

        failed = json.dumps(audit.build_recheck_failed_card("黄奕纯", [issue]), ensure_ascii=False)
        success = json.dumps(audit.build_success_card(issue), ensure_ascii=False)

        self.assertIn("🚨 **公开升级原因**", failed)
        self.assertIn("点过“已处理”不会静默关闭", failed)
        self.assertIn("🎉 **恭喜恢复**", success)
        self.assertIn("本轮审计关闭", success)

    def test_audit_metrics_counts_failed_and_overdue(self):
        issues = [
            {"owner": "黄奕纯", "status": audit.STATE_RECHECK_FAIL, "first_seen_ms": audit.now_ms() - 10 * 86_400_000, "handled_at_ms": audit.now_ms() - 8 * 86_400_000},
            {"owner": "黄奕纯", "status": audit.STATE_SUBMITTED, "first_seen_ms": audit.now_ms() - 16 * 86_400_000, "handled_at_ms": audit.now_ms() - 15 * 86_400_000},
            {"owner": "陈翔宇", "status": audit.STATE_RECHECK_PASS, "first_seen_ms": audit.now_ms() - 3 * 86_400_000},
        ]
        metrics = audit.audit_metrics(issues)

        self.assertEqual(1, metrics["7天复检失败数"])
        self.assertEqual(1, metrics["14天以上未解决数"])
        self.assertEqual(1, metrics["首页无差评恢复数"])
        self.assertEqual(1, metrics["负责人待处理数/已处理未改善数"]["黄奕纯"]["已处理未改善"])


class AmzReviewAuditAsyncTests(unittest.TestCase):
    def test_amz_assistant_url_verification_returns_challenge(self):
        original_token = amz_assistant.VERIFICATION_TOKEN
        try:
            amz_assistant.VERIFICATION_TOKEN = "verify-token"
            result = asyncio.run(amz_assistant.handle_feishu_callback({
                "type": "url_verification",
                "token": "verify-token",
                "challenge": "challenge-ok",
            }))
        finally:
            amz_assistant.VERIFICATION_TOKEN = original_token

        self.assertEqual({"challenge": "challenge-ok"}, result)

    def test_amz_assistant_card_event_dispatches_to_amz_handler(self):
        original_token = amz_assistant.VERIFICATION_TOKEN
        original_handler = audit.handle_callback
        calls = []

        async def fake_handle(event):
            calls.append(event)
            return {"toast": {"type": "success", "content": "ok"}}

        try:
            amz_assistant.VERIFICATION_TOKEN = "verify-token"
            audit.handle_callback = fake_handle
            result = asyncio.run(amz_assistant.handle_feishu_callback({
                "schema": "2.0",
                "header": {"event_type": "card.action.trigger", "token": "verify-token"},
                "event": {
                    "operator": {"union_id": "on_operator"},
                    "context": {"open_message_id": "om_card"},
                    "action": {"value": {"action": "amz_issue_submit_actions", "issue_id": "rec_1"}},
                },
            }))
        finally:
            amz_assistant.VERIFICATION_TOKEN = original_token
            audit.handle_callback = original_handler

        self.assertEqual("success", result["toast"]["type"])
        self.assertEqual(1, len(calls))
        self.assertEqual("om_card", calls[0]["context"]["open_message_id"])
        self.assertEqual("card.action.trigger", calls[0]["_header"]["event_type"])

    def test_recheck_sample_splits_failed_and_passed(self):
        result = asyncio.run(audit.recheck_due(mode="dry_run", sample=True))
        self.assertEqual(2, result["due"])
        self.assertEqual(1, result["failed"])
        self.assertEqual(1, result["passed"])
        self.assertIn("7天复检失败数", result["metrics"])

    def test_recheck_observe_mode_suppresses_group_send(self):
        issue = audit.normalize_issue({
            "source_type": "review",
            "review_id": "rv-recheck-observe",
            "site": "US",
            "asin": "B0RECHECK01",
            "owner": "黄奕纯",
            "rating": 1,
        })
        fields = audit.issue_to_fields(issue, audit.STATE_SUBMITTED)
        fields.update({
            "处理时间": audit.now_ms() - 8 * 86_400_000,
            "处理方式": ["已投诉Amazon / 已开Case"],
            "T+7复检日期": audit.now_ms() - 86_400_000,
        })

        original_list_records = audit._list_audit_records
        original_homepage_check = audit._homepage_check
        original_send_group = audit._send_group
        original_send_union = audit._send_union
        original_group_id = audit.AMZ_OPS_GROUP_CHAT_ID
        original_observe = audit.OBSERVE
        group_calls = []
        owner_calls = []

        async def fake_list_records(statuses=None, limit=200):
            return [{"record_id": "rec_recheck_observe", "fields": fields}]

        async def fake_homepage_check(issue):
            return {"ok": True, "has_negative": True, "negative_count": 1, "status": "首页仍有1条差评"}

        async def fake_send_group(chat_id, card):
            group_calls.append((chat_id, card))
            return "om_group"

        async def fake_send_union(union_id, card):
            owner_calls.append((union_id, card))
            return "om_owner"

        try:
            audit._list_audit_records = fake_list_records
            audit._homepage_check = fake_homepage_check
            audit._send_group = fake_send_group
            audit._send_union = fake_send_union
            audit.AMZ_OPS_GROUP_CHAT_ID = "oc_test"
            audit.OBSERVE = True
            result = asyncio.run(audit.recheck_due(mode="commit", notify=True))
        finally:
            audit._list_audit_records = original_list_records
            audit._homepage_check = original_homepage_check
            audit._send_group = original_send_group
            audit._send_union = original_send_union
            audit.AMZ_OPS_GROUP_CHAT_ID = original_group_id
            audit.OBSERVE = original_observe

        self.assertEqual(0, result["sent_group"])
        self.assertEqual([], group_calls)
        self.assertGreaterEqual(len(owner_calls), 1)

    def test_handle_submit_requires_selected_action(self):
        event = {
            "action": {
                "value": {
                    "action": "amz_issue_submit_actions",
                    "issue_id": "rec_missing",
                    "source_type": "review",
                    "source_id": "rv-missing",
                    "site": "US",
                    "asin": "B0MISS",
                    "rating": 1,
                },
                "form_value": {},
            }
        }
        result = asyncio.run(audit.handle_callback(event))
        self.assertEqual("error", result["toast"]["type"])
        self.assertIn("至少选择", result["toast"]["content"])

    def test_handle_submit_patches_processed_card_without_live_table(self):
        original_update = audit.cs_dispatch._update_card
        calls = []

        async def fake_update(message_id, card):
            calls.append((message_id, card))
            return True

        try:
            audit.cs_dispatch._update_card = fake_update
            event = {
                "message_id": "om_test",
                "operator": {"union_id": "on_operator"},
                "action": {
                    "value": {
                        "action": "amz_issue_submit_actions",
                        "issue_id": "rec_submit",
                        "source_type": "review",
                        "source_id": "rv-submit",
                        "site": "US",
                        "asin": "B0SUBMIT",
                        "rating": 1,
                        "erp_name": "FF Controller",
                    },
                    "form_value": {
                        "amz_actions_rec_submit": ["已投诉Amazon / 已开Case", "已联系买家售后处理"],
                        "amz_note_rec_submit": "Case 123",
                    },
                },
            }
            result = asyncio.run(audit.handle_callback(event))
        finally:
            audit.cs_dispatch._update_card = original_update

        self.assertEqual("success", result["toast"]["type"])
        self.assertEqual(1, len(calls))
        rendered = json.dumps(calls[0][1], ensure_ascii=False)
        self.assertIn("T+7", rendered)
        self.assertIn("此卡片已处理", rendered)

    def test_handle_observation_option_patches_observe_card(self):
        original_update = audit.cs_dispatch._update_card
        calls = []

        async def fake_update(message_id, card):
            calls.append((message_id, card))
            return True

        try:
            audit.cs_dispatch._update_card = fake_update
            event = {
                "message_id": "om_observe",
                "operator": {"union_id": "on_operator"},
                "action": {
                    "value": {
                        "action": "amz_issue_submit_actions",
                        "issue_id": "rec_observe",
                        "source_type": "review",
                        "source_id": "rv-observe",
                        "site": "US",
                        "asin": "B0OBSERVE",
                        "rating": 1,
                        "erp_name": "FF Controller",
                    },
                    "form_value": {
                        "amz_actions_rec_observe": ["客观无法移除，申请观察"],
                        "amz_note_rec_observe": "Competitor complaint, awaiting manager approval.",
                    },
                },
            }
            result = asyncio.run(audit.handle_callback(event))
        finally:
            audit.cs_dispatch._update_card = original_update

        self.assertEqual("success", result["toast"]["type"])
        self.assertIn("观察", result["toast"]["content"])
        self.assertEqual(1, len(calls))
        rendered = json.dumps(calls[0][1], ensure_ascii=False)
        self.assertIn("观察申请已提交", rendered)
        self.assertIn("上级确认", rendered)


if __name__ == "__main__":
    unittest.main()

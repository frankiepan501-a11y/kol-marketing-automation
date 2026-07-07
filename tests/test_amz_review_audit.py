import asyncio
import json
import unittest

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
    def test_recheck_sample_splits_failed_and_passed(self):
        result = asyncio.run(audit.recheck_due(mode="dry_run", sample=True))
        self.assertEqual(2, result["due"])
        self.assertEqual(1, result["failed"])
        self.assertEqual(1, result["passed"])
        self.assertIn("7天复检失败数", result["metrics"])

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

import asyncio
import json
import os
import sys
import unittest
from unittest.mock import AsyncMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import cs_dispatch


class CsSocialReviewCardTests(unittest.TestCase):
    def test_card_is_frankie_only_manual_save_and_has_one_submit(self):
        fields = {
            "工单ID": "TEST-P0-5B-20260826-001",
            "客户标识": "P0-5B TEST · 非真实客户",
            "品牌": "POWKONG",
            "销售平台": "未知",
            "状态": "待回",
            "客诉摘要": "X @提及兼容性咨询测试，不对应真实客户。",
            "AI草稿": (
                "Thanks for reaching out. Could you share the exact product model "
                "so our team can verify the current compatibility details?"
            ),
        }

        card = cs_dispatch._build_social_review_card(
            "rec_test", fields, run_id="P0-5B-TEST-001"
        )
        rendered = json.dumps(card, ensure_ascii=False)

        self.assertIn("🟢 [CUS·P3] 客服审核测试 · X", rendered)
        self.assertIn("只保存审核", rendered)
        self.assertIn("不回复客户", rendered)
        self.assertIn("**风险:** R2", rendered)
        self.assertIn("**仍缺事实:**", rendered)
        self.assertIn("**禁止声明:**", rendered)
        self.assertIn("**中文审核说明:**", rendered)
        self.assertIn("social_cs_review_save", rendered)
        self.assertIn('"send_mode": "manual_only"', rendered)
        self.assertIn('"frankie_only": true', rendered)
        self.assertEqual(1, rendered.count('"action_type": "form_submit"'))
        self.assertNotIn("cs_send_reply", rendered)
        self.assertNotIn("发送回复给客户", rendered)
        self.assertEqual([], cs_dispatch._validate_social_review_card(card))

    def test_non_test_record_is_rejected(self):
        self.assertFalse(cs_dispatch._is_social_review_test_record({
            "工单ID": "CSP-real-ticket",
            "客户标识": "real@example.com",
        }))
        self.assertTrue(cs_dispatch._is_social_review_test_record({
            "工单ID": "TEST-P0-5B-20260826-001",
            "客户标识": "P0-5B TEST · 非真实客户",
        }))


class CsSocialReviewCallbackTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        cs_dispatch._recent.clear()

    async def test_callback_saves_review_without_marking_replied_or_sending(self):
        fields = {
            "工单ID": "TEST-P0-5B-20260826-001",
            "客户标识": "P0-5B TEST · 非真实客户",
            "品牌": "POWKONG",
            "销售平台": "未知",
            "状态": "待回",
            "AI草稿": (
                "Thanks for reaching out. Could you share the exact product model "
                "so our team can verify the current compatibility details?"
            ),
            "卡片消息ID": "om_p0_5b_test",
        }
        get_result = {"data": {"record": {"fields": fields}}}
        calls = []

        async def fake_api(method, path, body=None, which=""):
            calls.append((method, path, body, which))
            if method == "GET":
                return get_result
            return {"code": 0}

        event = {
            "open_message_id": "om_p0_5b_test",
            "operator": {"union_id": cs_dispatch.SOCIAL_REVIEW_FRANKIE_UNION},
            "action": {
                "value": {
                    "action": "social_cs_review_save",
                    "act": "social_cs_review_save",
                    "rid": "rec_test",
                    "run_id": "P0-5B-TEST-001",
                    "send_mode": "manual_only",
                    "frankie_only": True,
                    "test_mode": True,
                },
                "form_value": {"review_reply": ""},
            },
        }

        with patch.object(cs_dispatch.feishu, "api", side_effect=fake_api), \
             patch.object(cs_dispatch, "_update_card", new=AsyncMock(return_value=True)) as update, \
             patch.object(cs_dispatch, "_dispatch_reply", new=AsyncMock()) as dispatch:
            result = await cs_dispatch.handle_callback(event)
            await asyncio.sleep(0)

        self.assertIn("审核结果已保存", result["toast"]["content"])
        self.assertEqual(2, len(calls))
        self.assertEqual("GET", calls[0][0])
        self.assertEqual("PUT", calls[1][0])
        written = calls[1][2]["fields"]
        self.assertEqual(fields["AI草稿"], written["最终回复"])
        self.assertEqual("Frankie · P0-5B单卡测试", written["回复人"])
        audit = json.loads(written["资源命中JSON"])
        self.assertEqual(cs_dispatch.SOCIAL_REVIEW_ACTION, audit["event"])
        self.assertEqual("X", audit["source_platform"])
        self.assertEqual("manual_only", audit["send_mode"])
        self.assertEqual(0, audit["customer_send_attempts"])
        self.assertEqual(0, audit["social_platform_write_attempts"])
        self.assertFalse(audit["customer_send_function_called"])
        self.assertNotIn("状态", written)
        self.assertNotIn("已回复", json.dumps(written, ensure_ascii=False))
        dispatch.assert_not_awaited()
        update.assert_awaited_once()
        rendered = json.dumps(update.await_args.args[1], ensure_ascii=False)
        self.assertIn("待人工发送", rendered)
        self.assertIn("**审核人:** Frankie", rendered)
        self.assertIn("**review:**", rendered)
        self.assertIn("**run:** `P0-5B-TEST-001`", rendered)
        self.assertIn("**replay:**", rendered)
        self.assertNotIn('"tag": "button"', rendered)

    async def test_duplicate_and_non_frankie_callbacks_do_not_write(self):
        base_fields = {
            "工单ID": "TEST-P0-5B-20260826-001",
            "客户标识": "P0-5B TEST · 非真实客户",
            "品牌": "POWKONG",
            "销售平台": "未知",
            "状态": "待回",
            "AI草稿": "This is a safe draft with enough text for review.",
            "最终回复": "This review was already saved.",
            "卡片消息ID": "om_p0_5b_test",
        }

        async def fake_api(method, path, body=None, which=""):
            if method == "GET":
                return {"data": {"record": {"fields": dict(base_fields)}}}
            raise AssertionError("duplicate/unauthorized callback must not write")

        event = {
            "open_message_id": "om_p0_5b_test",
            "operator": {"union_id": cs_dispatch.SOCIAL_REVIEW_FRANKIE_UNION},
            "action": {
                "value": {
                    "action": "social_cs_review_save",
                    "rid": "rec_test",
                    "run_id": "P0-5B-TEST-001",
                    "send_mode": "manual_only",
                    "frankie_only": True,
                    "test_mode": True,
                },
                "form_value": {"review_reply": ""},
            },
        }

        with patch.object(cs_dispatch.feishu, "api", side_effect=fake_api), \
             patch.object(cs_dispatch, "_update_card", new=AsyncMock(return_value=True)):
            duplicate = await cs_dispatch.handle_callback(event)
            await asyncio.sleep(0)
        self.assertIn("已经保存", duplicate["toast"]["content"])

        event["operator"] = {"union_id": "on_not_frankie"}
        with patch.object(cs_dispatch.feishu, "api", side_effect=fake_api):
            unauthorized = await cs_dispatch.handle_callback(event)
        self.assertEqual("error", unauthorized["toast"]["type"])
        self.assertIn("仅限 Frankie", unauthorized["toast"]["content"])

    async def test_commit_sends_one_card_and_repeat_is_idempotent(self):
        fields = {
            "工单ID": "TEST-P0-5B-20260826-002",
            "客户标识": "P0-5B TEST · 非真实客户",
            "品牌": "POWKONG",
            "销售平台": "未知",
            "状态": "待回",
            "AI草稿": "This is a safe synthetic draft with enough text for review.",
            "卡片消息ID": "",
        }
        calls = []

        async def fake_api(method, path, body=None, which=""):
            calls.append((method, path, body, which))
            if method == "GET":
                return {"data": {"record": {"fields": dict(fields)}}}
            fields.update(body["fields"])
            return {"code": 0}

        send = AsyncMock(return_value="om_p0_5b_single")
        with patch.object(cs_dispatch.feishu, "api", side_effect=fake_api), \
             patch.object(cs_dispatch, "_send_card", new=send), \
             patch.object(cs_dispatch, "CS_ASSIST_SECRET", "configured-for-test"):
            first = await cs_dispatch.send_social_review_test_card(
                "rec_test", mode="commit", run_id="P0-5B-TEST-002"
            )
            repeated = await cs_dispatch.send_social_review_test_card(
                "rec_test", mode="commit", run_id="P0-5B-TEST-002"
            )

        self.assertTrue(first["sent"])
        self.assertEqual("om_p0_5b_single", first["message_id"])
        self.assertEqual(0, first["customer_writes"])
        self.assertEqual(0, first["social_platform_writes"])
        self.assertFalse(repeated["sent"])
        self.assertTrue(repeated["duplicate"])
        send.assert_awaited_once()
        self.assertEqual(cs_dispatch.SOCIAL_REVIEW_FRANKIE_UNION, send.await_args.args[0])
        self.assertEqual("social-review:rec_test:P0-5B-TEST-002",
                         send.await_args.kwargs["idempotency_key"])
        claim_put = next(call for call in calls
                         if call[0] == "PUT" and "线程ID" in call[2]["fields"])
        self.assertEqual("P0-5B-CARD-CLAIM:P0-5B-TEST-002",
                         claim_put[2]["fields"]["线程ID"])
        put = next(call for call in calls
                   if call[0] == "PUT" and "卡片消息ID" in call[2]["fields"])
        self.assertEqual({"卡片消息ID": "om_p0_5b_single", "状态": "待回"},
                         put[2]["fields"])

    async def test_readback_proves_processed_card_has_no_controls(self):
        outbound_audit = {
            "event": cs_dispatch.SOCIAL_REVIEW_ACTION,
            "run_id": "P0-5B-TEST-003",
            "source_platform": "X",
            "send_mode": cs_dispatch.SOCIAL_REVIEW_SEND_MODE,
            "customer_send_attempts": 0,
            "social_platform_write_attempts": 0,
            "customer_send_function_called": False,
            "reviewed_at": 1787702400000,
        }
        processed_card = cs_dispatch._build_social_review_result_card(
            "rec_test",
            {
                "工单ID": "TEST-P0-5B-20260826-003",
                "客户标识": "P0-5B TEST · 非真实客户",
                "品牌": "POWKONG",
                "最终回复": "Reviewed synthetic draft text.",
                "回复时间": 1787702400000,
            },
            run_id="P0-5B-TEST-003",
            reviewed_at=1787702400000,
        )
        fields = {
            "工单ID": "TEST-P0-5B-20260826-003",
            "客户标识": "P0-5B TEST · 非真实客户",
            "品牌": "POWKONG",
            "状态": "待回",
            "线程ID": "P0-5B-CARD-CLAIM:P0-5B-TEST-003",
            "卡片消息ID": "om_p0_5b_readback",
            "最终回复": "Reviewed synthetic draft text.",
            "回复人": "Frankie · P0-5B单卡测试",
            "回复时间": 1787702400000,
            "资源命中JSON": json.dumps(outbound_audit, ensure_ascii=False,
                                         separators=(",", ":")),
            "最近出站Message-ID": "",
        }

        class FakeResponse:
            def json(self):
                return {"code": 0, "data": {"items": [{"body": {
                    "content": json.dumps(processed_card, ensure_ascii=False)
                }}]}}

        class FakeClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return False

            async def get(self, *args, **kwargs):
                return FakeResponse()

        with patch.object(cs_dispatch.feishu, "api", new=AsyncMock(return_value={
                 "data": {"record": {"fields": fields}}
             })), \
             patch.object(cs_dispatch, "_token", new=AsyncMock(return_value="test-token")), \
             patch.object(cs_dispatch.httpx, "AsyncClient", return_value=FakeClient()):
            result = await cs_dispatch.read_social_review_test("rec_test")

        self.assertTrue(result["ok"])
        self.assertTrue(result["review_saved"])
        self.assertEqual("待回", result["ticket_status"])
        audit = result["outbound_audit"]
        self.assertTrue(audit["persisted"])
        self.assertEqual(0, audit["customer_send_attempts"])
        self.assertEqual(0, audit["social_platform_write_attempts"])
        self.assertTrue(audit["recent_outbound_message_id_empty"])
        self.assertTrue(audit["ticket_status_is_waiting_review"])
        self.assertTrue(audit["customer_zero_evidence_passed"])
        self.assertTrue(audit["social_platform_zero_evidence_passed"])
        self.assertTrue(result["card_readback"]["ok"])
        self.assertTrue(result["card_readback"]["processed"])
        self.assertTrue(result["card_readback"]["processed_static_result"])
        self.assertFalse(result["card_readback"]["api_normalized"])
        self.assertTrue(result["card_readback"]["controls_observable"])
        self.assertEqual(0, result["card_readback"]["form_count"])
        self.assertEqual(0, result["card_readback"]["input_count"])
        self.assertEqual(0, result["card_readback"]["button_count"])

    async def test_readback_labels_feishu_normalized_interactive_content(self):
        normalized_card = {
            "title": "🟢 [CUS·P3] 客服审核测试 · X · TEST-P0-5B-20260826-004",
            "elements": [[{"tag": "text", "text":
                            "请升级至最新版本客户端，以查看内容"}]],
        }
        fields = {
            "工单ID": "TEST-P0-5B-20260826-004",
            "客户标识": "P0-5B TEST · 非真实客户",
            "品牌": "POWKONG",
            "状态": "待回",
            "卡片消息ID": "om_p0_5b_normalized",
        }

        class FakeResponse:
            def json(self):
                return {"code": 0, "data": {"items": [{"body": {
                    "content": json.dumps(normalized_card, ensure_ascii=False)
                }}]}}

        class FakeClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return False

            async def get(self, *args, **kwargs):
                return FakeResponse()

        with patch.object(cs_dispatch.feishu, "api", new=AsyncMock(return_value={
                 "data": {"record": {"fields": fields}}
             })), \
             patch.object(cs_dispatch, "_token", new=AsyncMock(return_value="test-token")), \
             patch.object(cs_dispatch.httpx, "AsyncClient", return_value=FakeClient()):
            result = await cs_dispatch.read_social_review_test("rec_test")

        card = result["card_readback"]
        self.assertTrue(card["ok"])
        self.assertTrue(card["api_normalized"])
        self.assertFalse(card["controls_observable"])
        self.assertTrue(card["interactive_fallback_present"])
        self.assertIsNone(card["form_count"])
        self.assertIsNone(card["input_count"])
        self.assertIsNone(card["button_count"])
        self.assertFalse(card["processed"])


if __name__ == "__main__":
    unittest.main()

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
            "operator": {"union_id": cs_dispatch.OBSERVE_UNION},
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
        self.assertNotIn("状态", written)
        self.assertNotIn("已回复", json.dumps(written, ensure_ascii=False))
        dispatch.assert_not_awaited()
        update.assert_awaited_once()
        rendered = json.dumps(update.await_args.args[1], ensure_ascii=False)
        self.assertIn("待人工发送", rendered)
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
            "operator": {"union_id": cs_dispatch.OBSERVE_UNION},
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
        self.assertEqual(cs_dispatch.OBSERVE_UNION, send.await_args.args[0])
        put = next(call for call in calls if call[0] == "PUT")
        self.assertEqual({"卡片消息ID": "om_p0_5b_single", "状态": "待回"},
                         put[2]["fields"])


if __name__ == "__main__":
    unittest.main()

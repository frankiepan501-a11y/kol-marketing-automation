import unittest
import json
import asyncio
from unittest.mock import AsyncMock, patch

from app import cs_dispatch


class CustomerServiceDispatchRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_live_send_callback_acks_before_slow_network_work_and_dedupes_twin_delivery(self):
        gate = asyncio.Event()

        async def slow_handler(event):
            await gate.wait()
            return {"toast": {"type": "success", "content": "done"}}

        event = {
            "open_message_id": "om_test",
            "action": {
                "value": {"act": "send_reply", "action": "cs_send_reply", "rid": "rec_fast"},
                "form_value": {"custom_reply": "A complete reply", "custom_reply_extra": ""},
            },
        }
        cs_dispatch._callback_fast_inflight.clear()
        with patch.object(cs_dispatch, "CS_REPLY_LIVE", True), \
             patch.object(cs_dispatch, "CS_REPLY_DRY_RUN_TO", ""), \
             patch.object(cs_dispatch, "handle_callback", side_effect=slow_handler) as handler:
            first = await cs_dispatch.handle_callback_fast(event)
            second = await cs_dispatch.handle_callback_fast(event)
            await asyncio.sleep(0)
            self.assertEqual(1, handler.call_count)
            self.assertIn("正在发送", first["toast"]["content"])
            self.assertIn("请勿重复点击", second["toast"]["content"])
            gate.set()
            await asyncio.sleep(0)
            await asyncio.sleep(0)

        self.assertNotIn("rec_fast:send_reply", cs_dispatch._callback_fast_inflight)

    async def test_stale_send_button_in_manual_mode_never_closes_ticket(self):
        record = {"data": {"record": {"fields": {
            "状态": "待回",
            "渠道": "邮箱",
            "品牌": "FUNLAB",
            "销售平台": "独立站",
            "客户标识": "customer@example.com",
            "客诉摘要": "Do you ship worldwide?",
            "AI草稿": "Hello, yes, we ship worldwide.",
            "卡片消息ID": "om_test",
        }}}}
        event = {
            "open_message_id": "om_test",
            "action": {
                "value": {"act": "send_reply", "action": "cs_send_reply", "rid": "rec_manual"},
                "form_value": {"custom_reply": "", "custom_reply_extra": ""},
            },
        }
        with patch.object(cs_dispatch, "CS_REPLY_LIVE", False), \
             patch.object(cs_dispatch, "CS_REPLY_DRY_RUN_TO", ""), \
             patch.object(cs_dispatch.feishu, "api", new=AsyncMock(return_value=record)) as api, \
             patch.object(cs_dispatch.cs_resources, "active_resources", new=AsyncMock(return_value=[])):
            result = await cs_dispatch.handle_callback(event)

        writes = [call for call in api.await_args_list if call.args and call.args[0] == "PUT"]
        self.assertEqual([], writes)
        self.assertEqual("error", result["toast"]["type"])
        self.assertIn("未发送", result["toast"]["content"])

    async def test_dispatch_stops_when_observe_mode_is_not_explicitly_configured(self):
        with patch.object(cs_dispatch, "CS_ASSIST_SECRET", "configured"), \
             patch.object(cs_dispatch, "OBSERVE_CONFIGURED", False), \
             patch.object(cs_dispatch, "OBSERVE_UNION_CONFIGURED", True), \
             patch.object(cs_dispatch, "DISPATCH_NOT_BEFORE_RAW", "1700000000000"), \
             patch.object(cs_dispatch, "DISPATCH_NOT_BEFORE_MS", 1700000000000), \
             patch.object(cs_dispatch.feishu, "api", new=AsyncMock()) as api:
            result = await cs_dispatch.run(limit=5)

        api.assert_not_awaited()
        self.assertEqual(0, result["sent"])
        self.assertIn("CS_DISPATCH_OBSERVE", result["config_errors"])

    async def test_dispatch_stops_when_history_cutoff_is_not_configured(self):
        with patch.object(cs_dispatch, "CS_ASSIST_SECRET", "configured"), \
             patch.object(cs_dispatch, "OBSERVE_CONFIGURED", True), \
             patch.object(cs_dispatch, "OBSERVE_UNION_CONFIGURED", True), \
             patch.object(cs_dispatch, "DISPATCH_NOT_BEFORE_RAW", ""), \
             patch.object(cs_dispatch, "DISPATCH_NOT_BEFORE_MS", 0), \
             patch.object(cs_dispatch.feishu, "api", new=AsyncMock()) as api:
            result = await cs_dispatch.run(limit=5)

        api.assert_not_awaited()
        self.assertEqual(0, result["sent"])
        self.assertIn("CS_DISPATCH_NOT_BEFORE_MS", result["config_errors"])

    async def test_dispatch_rejects_seconds_based_history_cutoff(self):
        with patch.object(cs_dispatch, "CS_ASSIST_SECRET", "configured"), \
             patch.object(cs_dispatch, "OBSERVE_CONFIGURED", True), \
             patch.object(cs_dispatch, "OBSERVE_UNION_CONFIGURED", True), \
             patch.object(cs_dispatch, "DISPATCH_NOT_BEFORE_RAW", "1788867182"), \
             patch.object(cs_dispatch, "DISPATCH_NOT_BEFORE_MS", 1788867182), \
             patch.object(cs_dispatch.feishu, "api", new=AsyncMock()) as api:
            result = await cs_dispatch.run(limit=5)

        api.assert_not_awaited()
        self.assertIn("CS_DISPATCH_NOT_BEFORE_MS", result["config_errors"])

    async def test_dispatch_reports_feishu_send_failure_without_marking_ticket_sent(self):
        search_result = {"data": {"items": [{
            "record_id": "rec_failed",
            "fields": {
                "状态": "待派",
                "卡片消息ID": "",
                "入站时间": 1700000000001,
                "分配运营": "张佳烨",
                "产品": "Controller",
                "销售平台": "独立站",
                "客诉摘要": "Connection issue",
            },
        }]}}
        with patch.object(cs_dispatch, "CS_ASSIST_SECRET", "configured"), \
             patch.object(cs_dispatch, "OBSERVE_CONFIGURED", True), \
             patch.object(cs_dispatch, "OBSERVE_UNION_CONFIGURED", True), \
             patch.object(cs_dispatch, "DISPATCH_NOT_BEFORE_RAW", "1700000000000"), \
             patch.object(cs_dispatch, "DISPATCH_NOT_BEFORE_MS", 1700000000000), \
             patch.object(cs_dispatch, "OBSERVE", True), \
             patch.object(cs_dispatch.feishu, "api", new=AsyncMock(return_value=search_result)) as api, \
             patch.object(cs_dispatch.cs_resources, "active_resources", new=AsyncMock(return_value=[])), \
             patch.object(cs_dispatch, "_send_card_result", new=AsyncMock(return_value={
                 "ok": False,
                 "message_id": "",
                 "http_status": 200,
                 "feishu_code": 230013,
                 "error": "Bot has NO availability to this user",
             })) as send_card:
            result = await cs_dispatch.run(limit=5)

        self.assertEqual(0, result["sent"])
        self.assertEqual(1, len(result["send_errors"]))
        self.assertEqual(230013, result["send_errors"][0]["feishu_code"])
        self.assertEqual(1, api.await_count)
        self.assertEqual("cs_dispatch:rec_failed",
                         send_card.await_args.kwargs["idempotency_key"])

    async def test_historical_dispatch_rejects_multiple_record_ids(self):
        with patch.object(cs_dispatch, "CS_ASSIST_SECRET", "configured"), \
             patch.object(cs_dispatch, "OBSERVE_CONFIGURED", True), \
             patch.object(cs_dispatch, "OBSERVE_UNION_CONFIGURED", True), \
             patch.object(cs_dispatch, "DISPATCH_NOT_BEFORE_RAW", "1700000000000"), \
             patch.object(cs_dispatch, "DISPATCH_NOT_BEFORE_MS", 1700000000000), \
             patch.object(cs_dispatch.feishu, "api", new=AsyncMock()) as api:
            result = await cs_dispatch.run(limit=10, rids="rec_1,rec_2")

        api.assert_not_awaited()
        self.assertEqual(0, result["sent"])
        self.assertIn("exactly one", result["error"])

    async def test_historical_dispatch_reports_record_read_failure(self):
        with patch.object(cs_dispatch, "CS_ASSIST_SECRET", "configured"), \
             patch.object(cs_dispatch, "OBSERVE_CONFIGURED", True), \
             patch.object(cs_dispatch, "OBSERVE_UNION_CONFIGURED", True), \
             patch.object(cs_dispatch, "DISPATCH_NOT_BEFORE_RAW", "1700000000000"), \
             patch.object(cs_dispatch, "DISPATCH_NOT_BEFORE_MS", 1700000000000), \
             patch.object(cs_dispatch.feishu, "api", new=AsyncMock(side_effect=RuntimeError("read failed"))):
            result = await cs_dispatch.run(limit=1, rids="rec_missing")

        self.assertEqual(0, result["sent"])
        self.assertEqual("rec_missing", result["read_errors"][0]["record_id"])

    async def test_cron_dispatch_skips_historical_tickets_but_keeps_new_ones(self):
        search_result = {"data": {"items": [
            {"record_id": "rec_old", "fields": {"状态": "待派", "卡片消息ID": "",
             "入站时间": 1600000000000, "分配运营": "张佳烨"}},
            {"record_id": "rec_new", "fields": {"状态": "待派", "卡片消息ID": "",
             "入站时间": 1750000000000, "分配运营": "张佳烨"}},
        ]}}
        with patch.object(cs_dispatch, "CS_ASSIST_SECRET", "configured"), \
             patch.object(cs_dispatch, "OBSERVE_CONFIGURED", True), \
             patch.object(cs_dispatch, "OBSERVE_UNION_CONFIGURED", True), \
             patch.object(cs_dispatch, "DISPATCH_NOT_BEFORE_RAW", "1700000000000"), \
             patch.object(cs_dispatch, "DISPATCH_NOT_BEFORE_MS", 1700000000000), \
             patch.object(cs_dispatch, "OBSERVE", True), \
             patch.object(cs_dispatch.feishu, "api", new=AsyncMock(return_value=search_result)), \
             patch.object(cs_dispatch.cs_resources, "active_resources", new=AsyncMock(return_value=[])), \
             patch.object(cs_dispatch, "_send_card_result", new=AsyncMock(return_value={
                 "ok": True, "message_id": "om_new", "http_status": 200,
                 "feishu_code": 0, "error": "",
             })):
            result = await cs_dispatch.run(limit=5)

        self.assertEqual(1, result["historical_skipped"])
        self.assertEqual(1, result["eligible"])
        self.assertEqual(1, result["sent"])

    async def test_cron_dispatch_paginates_past_more_than_200_historical_tickets(self):
        old_items = [
            {"record_id": f"rec_old_{i}", "fields": {"状态": "待派", "卡片消息ID": "",
             "入站时间": 1600000000000, "分配运营": "张佳烨"}}
            for i in range(200)
        ]
        page_one = {"data": {"items": old_items, "has_more": True, "page_token": "next"}}
        page_two = {"data": {"items": [
            {"record_id": "rec_new", "fields": {"状态": "待派", "卡片消息ID": "",
             "入站时间": 1750000000000, "分配运营": "张佳烨"}},
        ], "has_more": False}}
        with patch.object(cs_dispatch, "CS_ASSIST_SECRET", "configured"), \
             patch.object(cs_dispatch, "OBSERVE_CONFIGURED", True), \
             patch.object(cs_dispatch, "OBSERVE_UNION_CONFIGURED", True), \
             patch.object(cs_dispatch, "DISPATCH_NOT_BEFORE_RAW", "1700000000000"), \
             patch.object(cs_dispatch, "DISPATCH_NOT_BEFORE_MS", 1700000000000), \
             patch.object(cs_dispatch, "OBSERVE", True), \
             patch.object(cs_dispatch.feishu, "api", new=AsyncMock(side_effect=[page_one, page_two, {}])) as api, \
             patch.object(cs_dispatch.cs_resources, "active_resources", new=AsyncMock(return_value=[])), \
             patch.object(cs_dispatch, "_send_card_result", new=AsyncMock(return_value={
                 "ok": True, "message_id": "om_new", "http_status": 200,
                 "feishu_code": 0, "error": "",
             })):
            result = await cs_dispatch.run(limit=1)

        self.assertEqual(200, result["historical_skipped"])
        self.assertEqual(1, result["sent"])
        self.assertEqual(3, api.await_count)

    async def test_dispatch_caps_one_run_at_ten_cards(self):
        new_items = [
            {"record_id": f"rec_new_{i}", "fields": {"状态": "待派", "卡片消息ID": "",
             "入站时间": 1750000000000, "分配运营": "张佳烨"}}
            for i in range(12)
        ]
        with patch.object(cs_dispatch, "CS_ASSIST_SECRET", "configured"), \
             patch.object(cs_dispatch, "OBSERVE_CONFIGURED", True), \
             patch.object(cs_dispatch, "OBSERVE_UNION_CONFIGURED", True), \
             patch.object(cs_dispatch, "DISPATCH_NOT_BEFORE_RAW", "1700000000000"), \
             patch.object(cs_dispatch, "DISPATCH_NOT_BEFORE_MS", 1700000000000), \
             patch.object(cs_dispatch, "OBSERVE", True), \
             patch.object(cs_dispatch.feishu, "api", new=AsyncMock(return_value={
                 "data": {"items": new_items, "has_more": False},
             })), \
             patch.object(cs_dispatch.cs_resources, "active_resources", new=AsyncMock(return_value=[])), \
             patch.object(cs_dispatch, "_send_card_result", new=AsyncMock(return_value={
                 "ok": True, "message_id": "om_new", "http_status": 200,
                 "feishu_code": 0, "error": "",
             })) as send_card:
            result = await cs_dispatch.run(limit=1000)

        self.assertEqual(10, result["sent"])
        self.assertEqual(10, send_card.await_count)

    async def test_dispatch_endpoint_returns_424_for_business_failure(self):
        from app import main as app_main

        business_result = {
            "observe": True,
            "observe_configured": False,
            "candidates": 0,
            "sent": 0,
            "config_errors": ["CS_DISPATCH_OBSERVE"],
            "fallbacks": [],
            "read_errors": [],
            "send_errors": [],
            "samples": [],
        }
        with patch.object(app_main, "_check_auth"), \
             patch.object(app_main.cs_dispatch, "run", new=AsyncMock(return_value=business_result)), \
             patch.object(app_main, "_alert_endpoint_failure", new=AsyncMock()) as alert:
            response = await app_main.run_cs_dispatch(authorization="Bearer test")

        self.assertEqual(424, response.status_code)
        payload = json.loads(response.body)
        self.assertFalse(payload["ok"])
        self.assertEqual(["CS_DISPATCH_OBSERVE"], payload["config_errors"])
        alert.assert_awaited_once()

    async def test_dispatch_endpoint_returns_500_for_unexpected_exception(self):
        from app import main as app_main

        with patch.object(app_main, "_check_auth"), \
             patch.object(app_main.cs_dispatch, "run", new=AsyncMock(side_effect=RuntimeError("boom"))), \
             patch.object(app_main, "_alert_endpoint_failure", new=AsyncMock()):
            response = await app_main.run_cs_dispatch(authorization="Bearer test")

        self.assertEqual(500, response.status_code)


if __name__ == "__main__":
    unittest.main()

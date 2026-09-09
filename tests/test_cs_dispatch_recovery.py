import unittest
import json
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from app import cs_dispatch


class CustomerServiceDispatchRecoveryTests(unittest.IsolatedAsyncioTestCase):
    def test_netease_smtp_rejected_recipient_is_not_treated_as_accepted(self):
        smtp = MagicMock()
        smtp.__enter__.return_value = smtp
        smtp.sendmail.return_value = {
            "owner@example.com": (550, b"mailbox unavailable")
        }

        with patch.object(cs_dispatch.smtplib, "SMTP_SSL", return_value=smtp):
            with self.assertRaisesRegex(RuntimeError, "拒收"):
                cs_dispatch._netease_send_sync(
                    "owner@example.com", "Test subject", "<p>Complete body</p>"
                )

    def test_netease_smtp_accepted_message_is_saved_to_sent_for_readback(self):
        smtp = MagicMock()
        smtp.__enter__.return_value = smtp
        smtp.sendmail.return_value = {}

        with patch.object(cs_dispatch.smtplib, "SMTP_SSL", return_value=smtp), \
             patch.object(cs_dispatch, "_save_netease_sent_copy_sync") as save_copy:
            provider_id = cs_dispatch._netease_send_sync(
                "owner@example.com", "Test subject", "<p>Complete body</p>"
            )

        self.assertTrue(provider_id.startswith("<"))
        raw_message, saved_id = save_copy.call_args.args
        self.assertIsInstance(raw_message, bytes)
        self.assertEqual(provider_id, saved_id)
        self.assertIn(b"Message-ID:", raw_message)

    def test_save_netease_sent_copy_appends_only_when_message_is_absent(self):
        conn = MagicMock()
        conn.list.return_value = ("OK", [b'(\\HasNoChildren \\Sent) "/" "Sent"'])
        conn.select.return_value = ("OK", [b"1"])
        conn.search.return_value = ("OK", [b""])
        conn.append.return_value = ("OK", [b"1"])

        with patch.object(cs_dispatch.imaplib, "IMAP4_SSL", return_value=conn):
            cs_dispatch._save_netease_sent_copy_sync(
                b"Message-ID: <proof@funlabswitch.com>\r\n\r\nbody",
                "<proof@funlabswitch.com>",
            )

        conn.append.assert_called_once()
        self.assertEqual("Sent", conn.append.call_args.args[0])

    async def test_funlab_dry_run_uses_netease_and_verifies_sent_copy(self):
        fields = {
            "工单ID": "CSF-inbound", "品牌": "FUNLAB", "渠道": "邮箱",
            "销售平台": "独立站", "客户标识": "customer@example.com",
            "邮件主题": "Order update",
        }
        with patch.object(cs_dispatch, "CS_REPLY_DRY_RUN_TO", "frankiepan501@gmail.com"), \
             patch.object(cs_dispatch, "_netease_send", new=AsyncMock(
                 return_value="<dry-run@funlabswitch.com>")) as netease_send, \
             patch.object(cs_dispatch, "_verify_netease_outbound", new=AsyncMock(
                 return_value="<dry-run@funlabswitch.com>")) as verify_netease, \
             patch.object(cs_dispatch, "_zoho_send", new=AsyncMock()) as zoho_send, \
             patch.object(cs_dispatch, "_verify_zoho_outbound", new=AsyncMock()) as verify_zoho:
            ok, detail, evidence = await cs_dispatch._dispatch_reply(
                fields, "A complete customer reply."
            )

        self.assertTrue(ok)
        self.assertIn("DRY-RUN", detail)
        self.assertEqual("<dry-run@funlabswitch.com>", evidence)
        netease_send.assert_awaited_once()
        self.assertEqual("frankiepan501@gmail.com", netease_send.await_args.args[0])
        self.assertIn("CS-DRY-RUN", netease_send.await_args.args[1])
        self.assertIn("CS DRY-RUN", netease_send.await_args.args[2])
        verify_netease.assert_awaited_once()
        zoho_send.assert_not_awaited()
        verify_zoho.assert_not_awaited()

    async def test_dry_run_rejects_non_whitelisted_recipient_before_provider_call(self):
        fields = {
            "工单ID": "CSF-inbound", "品牌": "FUNLAB", "渠道": "邮箱",
            "销售平台": "独立站", "客户标识": "customer@example.com",
            "邮件主题": "Order update",
        }
        with patch.object(cs_dispatch, "CS_REPLY_DRY_RUN_TO", "customer@example.com"), \
             patch.object(cs_dispatch, "_netease_send", new=AsyncMock()) as netease_send:
            with self.assertRaisesRegex(RuntimeError, "白名单"):
                await cs_dispatch._dispatch_reply(fields, "A complete customer reply.")

        netease_send.assert_not_awaited()

    async def test_live_send_callback_acks_before_slow_network_work_and_dedupes_twin_delivery(self):
        gate = asyncio.Event()

        async def slow_handler(event):
            await gate.wait()
            return {"toast": {"type": "success", "content": "done"}}

        legacy_event = {
            "open_message_id": "om_test",
            "action": {
                "value": {"act": "send_reply", "action": "cs_send_reply", "rid": "rec_fast"},
                "form_value": {"custom_reply": "A complete reply", "custom_reply_extra": ""},
            },
        }
        schema2_event = {
            "schema": "2.0",
            "header": {"event_type": "card.action.trigger"},
            "event": {
                "operator": {"union_id": "on_operator"},
                "context": {"open_message_id": "om_test", "open_chat_id": "oc_test"},
                "action": {
                    "value": {"act": "send_reply", "action": "cs_send_reply", "rid": "rec_fast"},
                    "form_value": {"custom_reply": "A complete reply", "custom_reply_extra": ""},
                },
            },
        }
        cs_dispatch._callback_fast_inflight.clear()
        with patch.object(cs_dispatch, "CS_REPLY_LIVE", True), \
             patch.object(cs_dispatch, "CS_REPLY_DRY_RUN_TO", ""), \
             patch.object(cs_dispatch, "handle_callback", side_effect=slow_handler) as handler:
            first = await cs_dispatch.handle_callback_fast(legacy_event)
            second = await cs_dispatch.handle_callback_fast(schema2_event)
            await asyncio.sleep(0)
            self.assertEqual(1, handler.call_count)
            self.assertIn("正在发送", first["toast"]["content"])
            self.assertIn("请勿重复点击", second["toast"]["content"])
            gate.set()
            await asyncio.sleep(0)
            await asyncio.sleep(0)

        self.assertNotIn("rec_fast:send_reply", cs_dispatch._callback_fast_inflight)

    async def test_verified_outbound_id_is_required_before_ticket_closes(self):
        rid = "rec_verified"
        fields = {
            "工单ID": "CSF-inbound", "品牌": "FUNLAB", "销售平台": "独立站",
            "客户标识": "customer@example.com", "状态": "待回",
        }
        event = {"operator": {"union_id": "on_operator"}}
        cs_dispatch._recent.pop(rid, None)
        with patch.object(cs_dispatch, "_dispatch_reply", new=AsyncMock(
                 return_value=(True, "网易→customer@example.com", "<proof@funlabswitch.com>"))), \
             patch.object(cs_dispatch.feishu, "api", new=AsyncMock()) as api, \
             patch.object(cs_dispatch, "_update_card", new=AsyncMock()), \
             patch.object(cs_dispatch, "_notify_union", new=AsyncMock()):
            await cs_dispatch._send_async(rid, fields, "A complete customer reply.", event, "om_test")

        writes = [call for call in api.await_args_list if call.args and call.args[0] == "PUT"]
        self.assertEqual(1, len(writes))
        update = writes[0].args[2]["fields"]
        self.assertEqual("已回复", update["状态"])
        self.assertEqual("<proof@funlabswitch.com>", update["最近出站Message-ID"])
        cs_dispatch._recent.pop(rid, None)

    async def test_provider_accept_without_sent_proof_keeps_ticket_open_and_blocks_retry(self):
        rid = "rec_unproven"
        fields = {
            "工单ID": "CSF-inbound", "品牌": "FUNLAB", "销售平台": "独立站",
            "客户标识": "customer@example.com", "状态": "待回",
        }
        event = {"operator": {"union_id": "on_operator"}}
        cs_dispatch._recent.pop(rid, None)
        failure = cs_dispatch.OutboundEvidenceError(
            "<accepted@funlabswitch.com>", "网易→customer@example.com", "sent readback missing"
        )
        with patch.object(cs_dispatch, "_dispatch_reply", new=AsyncMock(side_effect=failure)), \
             patch.object(cs_dispatch.feishu, "api", new=AsyncMock()) as api, \
             patch.object(cs_dispatch, "_update_card", new=AsyncMock()) as update_card, \
             patch.object(cs_dispatch, "_notify_union", new=AsyncMock()):
            await cs_dispatch._send_async(rid, fields, "A complete customer reply.", event, "om_test")

        writes = [call for call in api.await_args_list if call.args and call.args[0] == "PUT"]
        self.assertEqual(1, len(writes))
        pending = writes[0].args[2]["fields"]
        self.assertEqual("待回", pending["状态"])
        self.assertIn("CS_OUTBOUND_PENDING:<accepted@funlabswitch.com>", pending["沟通历史摘要"])
        self.assertEqual("A complete customer reply.", pending["AI草稿"])
        self.assertIn(rid, cs_dispatch._recent)
        self.assertIn("待核实", update_card.await_args_list[0].args[1]["header"]["title"]["content"])
        cs_dispatch._recent.pop(rid, None)

    async def test_verified_send_with_ticket_write_failure_says_do_not_resend_and_persists_lock(self):
        rid = "rec_write_failed"
        fields = {
            "工单ID": "CSF-inbound", "品牌": "FUNLAB", "销售平台": "独立站",
            "客户标识": "customer@example.com", "状态": "待回",
        }
        event = {"operator": {"union_id": "on_operator"}}
        cs_dispatch._recent.pop(rid, None)
        api = AsyncMock(side_effect=[RuntimeError("first write failed"), {}])
        with patch.object(cs_dispatch, "_dispatch_reply", new=AsyncMock(
                 return_value=(True, "网易→customer@example.com", "<verified@funlabswitch.com>"))), \
             patch.object(cs_dispatch.feishu, "api", new=api), \
             patch.object(cs_dispatch, "_update_card", new=AsyncMock()) as update_card, \
             patch.object(cs_dispatch, "_notify_union", new=AsyncMock()):
            await cs_dispatch._send_async(rid, fields, "A complete customer reply.", event, "om_test")

        self.assertEqual(2, api.await_count)
        recovery = api.await_args_list[1].args[2]["fields"]
        self.assertEqual("待回", recovery["状态"])
        self.assertIn("CS_OUTBOUND_PENDING:<verified@funlabswitch.com>", recovery["沟通历史摘要"])
        self.assertIn("勿重复发送", update_card.await_args_list[0].args[1]["header"]["title"]["content"])
        self.assertIn(rid, cs_dispatch._recent)
        cs_dispatch._recent.pop(rid, None)

    def test_outbound_body_proof_rejects_truncated_tail_or_missing_link(self):
        expected = "<p>Hello customer, here is the tracking update.</p><p>https://example.com/track/123</p>"
        self.assertTrue(cs_dispatch._outbound_body_matches(expected, expected, expected))
        self.assertFalse(cs_dispatch._outbound_body_matches(
            expected, "Hello customer, here is the tracking update.", ""
        ))
        linked = '<p>Hello customer.</p><a href="https://example.com/track/123">Track here</a>'
        self.assertFalse(cs_dispatch._outbound_body_matches(
            linked, "Hello customer. Track here", ""
        ))

    def test_pending_outbound_lock_survives_full_history_truncation(self):
        error = cs_dispatch.OutboundEvidenceError(
            "<proof@funlabswitch.com>", "网易", "readback pending"
        )
        update = cs_dispatch._pending_outbound_update(
            {"沟通历史摘要": "x" * 5000}, error, "A complete customer reply."
        )

        self.assertEqual(5000, len(update["沟通历史摘要"]))
        self.assertEqual("<proof@funlabswitch.com>", cs_dispatch._pending_outbound_id(update))

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

    async def test_persisted_pending_outbound_marker_blocks_resend_after_restart(self):
        record = {"data": {"record": {"fields": {
            "状态": "待回", "渠道": "邮箱", "品牌": "FUNLAB", "销售平台": "独立站",
            "客户标识": "customer@example.com", "客诉摘要": "Question",
            "AI草稿": "A complete customer reply.", "卡片消息ID": "om_test",
            "沟通历史摘要": "CS_OUTBOUND_PENDING:<accepted@funlabswitch.com> · readback pending",
        }}}}
        event = {
            "open_message_id": "om_test",
            "action": {"value": {"act": "send_reply", "rid": "rec_pending"},
                       "form_value": {"custom_reply": "", "custom_reply_extra": ""}},
        }
        cs_dispatch._recent.pop("rec_pending", None)
        with patch.object(cs_dispatch, "CS_REPLY_LIVE", True), \
             patch.object(cs_dispatch, "CS_REPLY_DRY_RUN_TO", ""), \
             patch.object(cs_dispatch.feishu, "api", new=AsyncMock(return_value=record)), \
             patch.object(cs_dispatch.cs_resources, "active_resources", new=AsyncMock(return_value=[])), \
             patch.object(cs_dispatch, "_dispatch_reply", new=AsyncMock()) as dispatch:
            result = await cs_dispatch.handle_callback(event)

        dispatch.assert_not_awaited()
        self.assertEqual("error", result["toast"]["type"])
        self.assertIn("请勿重复发送", result["toast"]["content"])

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

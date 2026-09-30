import unittest
from unittest.mock import AsyncMock, patch
from app import auto_send, draft_router, followup, nyxi_trial, reply_monitor, sla_check


def draft(key=nyxi_trial.DRAFT_PREFIX + "unique"):
    return {"record_id": "rec-test", "fields": {"邮件草稿ID": key,
            "邮件草稿来源": "reply", "邮件草稿状态": "待审", "生成时间": 1}}


class IsolationTests(unittest.IsolatedAsyncioTestCase):
    def test_marker_does_not_capture_old_business(self):
        for key in ("", "cold-nyxi", "NYXI", nyxi_trial.DRAFT_PREFIX, "reply-1"):
            self.assertFalse(nyxi_trial.owns_draft(draft(key)))
        self.assertTrue(nyxi_trial.owns_draft(draft()))
        self.assertFalse(nyxi_trial.owns_draft({"fields": {"邮件主题": nyxi_trial.DRAFT_PREFIX + "x"}}))

    async def test_router_holds_without_review_or_write(self):
        with patch.object(draft_router.feishu, "get_record", AsyncMock(return_value=draft())), \
             patch.object(draft_router.reviewer, "review_draft", AsyncMock()) as review, \
             patch.object(draft_router.feishu, "update_record", AsyncMock()) as update:
            r = await draft_router.route_draft("rec-test")
        self.assertEqual(r["reason"], "nyxi_session_owned")
        review.assert_not_awaited()
        update.assert_not_awaited()

    async def test_old_router_still_reviews(self):
        with patch.object(draft_router.feishu, "get_record", AsyncMock(return_value=draft("reply-old"))), \
             patch.object(draft_router.reviewer, "review_draft", AsyncMock(side_effect=RuntimeError("review reached"))) as review:
            with self.assertRaisesRegex(RuntimeError, "review reached"):
                await draft_router.route_draft("rec-test")
        review.assert_awaited_once()

    async def test_direct_send_cannot_bypass_with_activity_release(self):
        with patch.object(auto_send.zoho, "send_email", AsyncMock()) as send, \
             patch.object(auto_send.feishu, "update_record", AsyncMock()) as update:
            r = await auto_send.send_one(draft(), activity_release=auto_send._LAUNCH_ACTIVITY_RELEASE)
        self.assertTrue(r["skipped"])
        send.assert_not_awaited()
        update.assert_not_awaited()

    async def test_ready_scan_excludes_owned_keeps_old(self):
        owned = draft()
        old = draft("reply-old")
        old["record_id"] = "rec-old"
        with patch.object(auto_send.feishu, "search_records", AsyncMock(side_effect=[[owned, old], []])), \
             patch.object(auto_send.feishu, "fetch_all_records", AsyncMock(return_value=[])):
            result = await auto_send.scan_ready()
        self.assertEqual([r["record_id"] for r in result[0]], ["rec-old"])

    async def test_direct_followup_blocks_before_generation(self):
        with patch.object(followup.deepseek, "chat_json", AsyncMock()) as generate:
            with self.assertRaisesRegex(ValueError, "session-owned"):
                await followup.generate_followup(2, draft(), {}, {}, "FUNLAB", "Sig", "en")
        generate.assert_not_awaited()

    async def test_followup_run_holds_owned_first(self):
        owned = draft()
        owned["fields"].update({"关联KOL": {"record_ids": ["kol1"]}, "Follow-up轮次": "第1封", "发送状态": "已发"})
        with patch.object(followup.feishu, "fetch_all_records", AsyncMock(side_effect=[[owned], [], []])), \
             patch.object(followup, "generate_followup", AsyncMock()) as generate:
            result = await followup.run()
        self.assertEqual(result["skipped"], 1)
        generate.assert_not_awaited()

    async def test_sla_summary_preserves_old_draft(self):
        owned, old = draft(), draft("cold-old")
        old["record_id"] = "rec-old"
        with patch.object(sla_check.feishu, "search_records", AsyncMock(side_effect=[[owned, old], []])):
            result = await sla_check.collect_sla_overdue_drafts(2000000000000)
        self.assertEqual([r["record_id"] for r in result["overdue"]], ["rec-old"])

    async def test_inbound_stays_pending_without_classification_or_writes(self):
        owned = draft()
        msg = {"fromAddress": "kol@example.invalid", "messageId": "inbound1", "receivedTime": 1}
        with patch.dict(reply_monitor.config.BRAND_CONFIG, {"FUNLAB": {"alias_from": "partner@brand.invalid", "domain": "brand.invalid"}}, clear=True), \
             patch.object(reply_monitor.zoho, "list_inbox", AsyncMock(return_value=[msg])), \
             patch.object(reply_monitor, "find_contact", AsyncMock(return_value=({"record_id": "kol1", "fields": {}}, "KOL"))), \
             patch.object(reply_monitor, "find_draft", AsyncMock(return_value=(owned, [owned]))), \
             patch.object(reply_monitor, "classify_intent", AsyncMock()) as classify, \
             patch.object(reply_monitor.feishu, "update_record", AsyncMock()) as update, \
             patch.object(reply_monitor.feishu, "create_record", AsyncMock()) as create, \
             patch.object(reply_monitor.reply_drafter, "draft_reply", AsyncMock()) as reply:
            result = await reply_monitor.run()
        self.assertEqual(result["processed"], 0)
        self.assertEqual(result["results"][0]["skipped"], "nyxi_session_owned")
        for mock in (classify, update, create, reply):
            mock.assert_not_awaited()


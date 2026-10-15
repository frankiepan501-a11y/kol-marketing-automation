import unittest
from unittest.mock import AsyncMock, patch

from app import auto_send, reply_monitor


class SandboxBrandGuardTests(unittest.IsolatedAsyncioTestCase):
    async def test_wrong_mailbox_never_inherits_other_brand_draft(self):
        powkong = {"record_id": "pk-draft", "fields": {
            "关联KOL": {"record_ids": ["sandbox"]},
            "发送邮箱": "partner@powkong.com",
            "邮件主题": "POWKONG x SandboxxMode - Plantdock 2",
            "发送时间": 100,
            "是否回复": False,
        }}
        with patch.object(reply_monitor, "_get_sent_drafts", AsyncMock(return_value=[powkong])):
            chosen, matched = await reply_monitor.find_draft(
                "sandbox", "KOL", brand="FUNLAB",
                subject="Re: POWKONG x SandboxxMode - Plantdock 2",
            )
        self.assertIsNone(chosen)
        self.assertEqual(["pk-draft"], [r["record_id"] for r in matched])

    def test_multiple_brand_history_requires_manual_review(self):
        rows = [
            {"fields": {"发送邮箱": "partner@fireflyfunlab.com"}},
            {"fields": {"发送邮箱": "partner@powkong.com"}},
        ]
        self.assertTrue(reply_monitor._has_cross_brand_history(rows))
        self.assertFalse(reply_monitor._has_cross_brand_history(rows[:1]))

    async def test_sandbox_auto_approved_reply_requires_human_review(self):
        draft = {"record_id": "reply-sandbox", "fields": {
            "邮件草稿来源": "reply",
            "邮件草稿状态": "自动通过",
            "收件邮箱": "sandboxxmode.collab@gmail.com",
            "发送邮箱": "partner@fireflyfunlab.com",
            "邮件主题": "Re: POWKONG x SandboxxMode - Plantdock 2",
            "邮件正文": "Hi SandboxxMode,",
        }}
        with patch.object(auto_send.feishu, "update_record", AsyncMock()) as update, \
             patch.object(auto_send.zoho, "send_email", AsyncMock()) as send:
            result = await auto_send.send_one(draft)
        self.assertFalse(result["ok"])
        self.assertTrue(result["skipped"])
        send.assert_not_awaited()
        update.assert_awaited_once()
        self.assertEqual("待审", update.await_args.args[2]["邮件草稿状态"])


if __name__ == "__main__":
    unittest.main()

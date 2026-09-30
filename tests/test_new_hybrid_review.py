import copy
import json
import unittest
from unittest.mock import AsyncMock, patch

from app import draft_router


class NewHybridReviewTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.fields = {
            "邮件草稿状态": "待审", "审核路径": "待人审",
            "邮件草稿来源": "cold", "AI评分理由": "[hybrid-ai-exception] test",
        }
        async def read(*args):
            return {"fields": copy.deepcopy(self.fields)}
        async def update(table, rid, fields):
            self.fields.update(fields)
        async def api(method, path, data):
            self.fields.update(data["fields"])
        self.read = patch("app.draft_router.feishu.get_record", side_effect=read).start()
        self.update = patch("app.draft_router.feishu.update_record", side_effect=update).start()
        self.api = patch("app.draft_router.feishu.api", side_effect=api).start()
        self.targets = patch("app.draft_router.feishu.resolve_draft_notify_targets",
                             new=AsyncMock(return_value=[("BD", "union_bd")])).start()
        self.send = patch("app.draft_router.feishu.send_card_message",
                          new=AsyncMock(return_value="om_test")).start()
        patch("app.card_resend._build_resend_card", new=AsyncMock(return_value={"elements": []})).start()
        self.addCleanup(patch.stopall)

    async def test_success_and_second_call_does_not_duplicate(self):
        result = await draft_router.notify_new_hybrid_review("new")
        self.assertEqual(1, result["delivered"])
        self.assertEqual("待审", self.fields["邮件草稿状态"])
        self.assertEqual("kol_assistant", self.send.call_args.kwargs["which"])
        self.assertEqual(("union_id", "union_bd"), self.send.call_args.args[:2])
        self.assertEqual(50, len(self.send.call_args.kwargs["message_uuid"]))
        self.assertIn("union_bd", json.loads(self.fields["卡片个人消息IDs"]))
        self.assertEqual(0, (await draft_router.notify_new_hybrid_review("new"))["delivered"])
        self.send.assert_awaited_once()

    async def test_send_failure_blocks_retry(self):
        self.send.side_effect = TimeoutError()
        with self.assertRaises(TimeoutError):
            await draft_router.notify_new_hybrid_review("new")
        with self.assertRaisesRegex(RuntimeError, "reconciliation"):
            await draft_router.notify_new_hybrid_review("new")
        self.send.assert_awaited_once()

    async def test_second_recipient_failure_keeps_first_receipt(self):
        self.targets.return_value = [("BD", "union_bd"), ("Frankie", "union_f")]
        self.send.side_effect = ["om_first", TimeoutError()]
        with self.assertRaises(TimeoutError):
            await draft_router.notify_new_hybrid_review("new")
        self.assertIn("union_bd", json.loads(self.fields["卡片个人消息IDs"]))
        with self.assertRaisesRegex(RuntimeError, "reconciliation"):
            await draft_router.notify_new_hybrid_review("new")
        self.assertEqual(2, self.send.await_count)

    async def test_receipt_write_failure_blocks_retry(self):
        self.api.side_effect = RuntimeError("write failed")
        with self.assertRaises(RuntimeError):
            await draft_router.notify_new_hybrid_review("new")
        with self.assertRaisesRegex(RuntimeError, "reconciliation"):
            await draft_router.notify_new_hybrid_review("new")
        self.send.assert_awaited_once()

    async def test_marker_write_failure_sends_nothing(self):
        self.update.side_effect = RuntimeError("write failed")
        with self.assertRaises(RuntimeError):
            await draft_router.notify_new_hybrid_review("new")
        self.send.assert_not_awaited()

    async def test_empty_reviewer_sends_nothing(self):
        self.targets.return_value = []
        with self.assertRaises(RuntimeError):
            await draft_router.notify_new_hybrid_review("new")
        self.send.assert_not_awaited()

    async def test_terminal_and_nonhybrid_rejected(self):
        self.fields["邮件草稿状态"] = "已否决"
        with self.assertRaises(RuntimeError):
            await draft_router.notify_new_hybrid_review("new")
        self.fields["邮件草稿状态"] = "待审"
        self.fields["AI评分理由"] = "other"
        with self.assertRaises(RuntimeError):
            await draft_router.notify_new_hybrid_review("new")
        self.send.assert_not_awaited()

    async def test_malformed_receipt_fails_closed(self):
        self.fields["卡片个人消息IDs"] = "broken"
        with self.assertRaises(ValueError):
            await draft_router.notify_new_hybrid_review("new")
        self.send.assert_not_awaited()

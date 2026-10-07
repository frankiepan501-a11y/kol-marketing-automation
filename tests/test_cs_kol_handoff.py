import json
import unittest
from unittest.mock import AsyncMock, patch

from app import cs_kol_handoff


class CustomerServiceKolHandoffTests(unittest.IsolatedAsyncioTestCase):
    def test_review_card_is_read_only_and_contains_operational_context(self):
        card = cs_kol_handoff.build_review_card(
            "rec_ticket",
            {
                "工单ID": "CSZ-message-1",
                "品牌": "FUNLAB",
                "客户标识": "creator@example.com",
                "客诉摘要": "Creator asks about collaboration.",
            },
            contact={"record_id": "rec_kol", "fields": {"账号名": "Creator A", "合作状态": "洽谈中"}},
            contact_type="KOL",
        )

        rendered = str(card)
        self.assertIn("当前轮到谁", rendered)
        self.assertIn("我方需审核/操作", rendered)
        self.assertIn("允许结论", rendered)
        self.assertIn("回填位置", rendered)
        self.assertIn("截止时间", rendered)
        self.assertIn("rec_kol", rendered)
        self.assertNotIn("button", rendered)
        self.assertNotIn("form_submit", rendered)
        self.assertNotIn("发送邮件", rendered)

    async def test_send_review_card_uses_kol_assistant_and_target_specific_uuid(self):
        with patch.object(cs_kol_handoff.feishu, "resolve_partnership_targets",
                          new=AsyncMock(return_value=[("Frankie", "on_frankie")])), \
             patch.object(cs_kol_handoff.feishu, "send_card_message",
                          new=AsyncMock(return_value="om_card")) as send:
            result = await cs_kol_handoff.send_review_card(
                "rec_ticket",
                {"品牌": "FUNLAB", "客户标识": "creator@example.com",
                 "客诉摘要": "Creator asks about collaboration."},
                contact=None,
                contact_type="",
            )

        self.assertTrue(result["ok"])
        self.assertEqual(["om_card"], result["message_ids"])
        kwargs = send.await_args.kwargs
        self.assertEqual("KOL", kwargs["biz"])
        self.assertEqual("P1", kwargs["level"])
        self.assertEqual("kol_assistant", kwargs["which"])
        self.assertTrue(kwargs["message_uuid"].startswith("cs-kol-"))

    async def test_lookup_contact_checks_kol_then_editor(self):
        existing = {"record_id": "rec_kol", "fields": {"邮箱": "creator@example.com"}}
        with patch.object(cs_kol_handoff.feishu, "search_records",
                          new=AsyncMock(side_effect=[[existing], []])) as search:
            record, kind = await cs_kol_handoff.lookup_contact("creator@example.com")

        self.assertEqual("rec_kol", record["record_id"])
        self.assertEqual("KOL", kind)
        self.assertEqual(1, search.await_count)

    async def test_readback_requires_read_only_kol_card(self):
        card = cs_kol_handoff.build_review_card(
            "rec_ticket",
            {"品牌": "FUNLAB", "客户标识": "creator@example.com",
             "客诉摘要": "Creator collaboration."},
        )
        response = {"data": {"items": [{"body": {"content": json.dumps(card)}}]}}
        with patch.object(cs_kol_handoff.feishu, "api",
                          new=AsyncMock(return_value=response)):
            result = await cs_kol_handoff.read_review_cards(["om_card"])

        self.assertTrue(result["ok"])
        self.assertEqual(1, result["verified"])

    async def test_readback_permission_gap_uses_send_receipt_and_local_shape(self):
        card = cs_kol_handoff.build_review_card(
            "rec_ticket",
            {"品牌": "FUNLAB", "客户标识": "creator@example.com",
             "客诉摘要": "Creator collaboration."},
        )
        denied = cs_kol_handoff.feishu.FeishuAPIError(
            method="GET", path="/im/v1/messages/om_card", status_code=400,
            feishu_code=99991672, feishu_msg="missing message read scope",
        )
        with patch.object(cs_kol_handoff.feishu, "api",
                          new=AsyncMock(side_effect=denied)):
            result = await cs_kol_handoff.read_review_cards(
                ["om_card"], expected_card=card,
            )

        self.assertTrue(result["ok"])
        self.assertEqual(1, result["verified"])
        self.assertEqual("send_receipt_and_local_shape",
                         result["results"][0]["verification_mode"])

    async def test_readback_permission_gap_rejects_actionable_local_card(self):
        card = cs_kol_handoff.build_review_card(
            "rec_ticket",
            {"品牌": "FUNLAB", "客户标识": "creator@example.com",
             "客诉摘要": "Creator collaboration."},
        )
        card["elements"].append({"tag": "action", "actions": [{"tag": "button"}]})
        denied = cs_kol_handoff.feishu.FeishuAPIError(
            method="GET", path="/im/v1/messages/om_card", status_code=400,
            feishu_code=99991672, feishu_msg="missing message read scope",
        )
        with patch.object(cs_kol_handoff.feishu, "api",
                          new=AsyncMock(side_effect=denied)):
            with self.assertRaises(cs_kol_handoff.feishu.FeishuAPIError):
                await cs_kol_handoff.read_review_cards(
                    ["om_card"], expected_card=card,
                )

    async def test_commit_rejects_records_outside_confirmed_scope(self):
        with self.assertRaisesRegex(ValueError, "outside"):
            await cs_kol_handoff.correct_confirmed_ticket(
                "rec_not_authorized", dry_run=False, confirm=True, run_id="run-1",
            )

    def test_pending_marker_is_persisted_before_card_send(self):
        fields = cs_kol_handoff.mark_pending_fields({"沟通历史摘要": "original"})

        self.assertTrue(fields["沟通历史摘要"].startswith(cs_kol_handoff.PENDING_MARKER))
        self.assertIn("original", fields["沟通历史摘要"])

    async def test_pending_handoff_is_retried_and_marked(self):
        pending = {
            "record_id": "rec_pending",
            "fields": {
                "状态": "归档非客服",
                "品牌": "FUNLAB",
                "客户标识": "creator@example.com",
                "客诉摘要": "[→KOL红人] Creator collaboration",
                "沟通历史摘要": cs_kol_handoff.PENDING_MARKER,
            },
        }
        search_response = {"data": {"items": [pending]}}
        with patch.object(cs_kol_handoff.feishu, "api",
                          new=AsyncMock(return_value=search_response)), \
             patch.object(cs_kol_handoff, "lookup_contact",
                          new=AsyncMock(return_value=(None, None))), \
             patch.object(cs_kol_handoff, "send_review_card",
                          new=AsyncMock(return_value={"ok": True, "message_ids": ["om_1"]})), \
             patch.object(cs_kol_handoff, "record_handoff_marker",
                          new=AsyncMock(return_value={"marker": "done"})) as mark:
            result = await cs_kol_handoff.retry_pending_handoffs(limit=10)

        self.assertTrue(result["ok"])
        self.assertEqual(1, result["sent"])
        mark.assert_awaited_once()


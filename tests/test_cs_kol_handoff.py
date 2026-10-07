import json
import unittest
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException

from app import cs_kol_handoff


class CustomerServiceKolHandoffTests(unittest.IsolatedAsyncioTestCase):
    def test_new_creator_is_created_as_controlled_inbound_kol(self):
        fields = cs_kol_handoff.controlled_kol_fields(
            "rec_ticket",
            {
                "品牌": "FUNLAB",
                "客户标识": "creator@example.com",
                "客诉摘要": "Creator asks about collaboration.",
            },
        )

        self.assertEqual("creator@example.com", fields["邮箱"])
        self.assertEqual("未建联", fields["合作状态"])
        self.assertEqual("待核对", fields["触达路由状态"])
        self.assertEqual("缺资料", fields["资料可用状态"])
        self.assertIn("[CS_INBOUND_KOL] ticket=rec_ticket", fields["迁移备注"])
        self.assertIn("no_auto_email=true", fields["迁移备注"])

    async def test_existing_kol_reuses_master_and_creates_followup_without_draft(self):
        contact = {
            "record_id": "rec_kol",
            "fields": {"账号名": "Creator A", "邮箱": "creator@example.com"},
        }
        creates = []

        async def create_record(table_id, fields):
            creates.append((table_id, fields))
            return "rec_fu"

        with patch.multiple(cs_kol_handoff.config, T_KOL="tbl_kol",
                            T_KOL_FU="tbl_fu"), \
             patch.object(cs_kol_handoff, "lookup_contact",
                          new=AsyncMock(return_value=(contact, "KOL"))), \
             patch.object(cs_kol_handoff, "_find_existing_followup",
                          new=AsyncMock(return_value=None)), \
             patch.object(cs_kol_handoff.feishu, "create_record",
                          new=create_record), \
             patch.object(cs_kol_handoff, "send_intake_card_once",
                          new=AsyncMock(return_value={
                              "ok": True, "message_ids": ["om_intake"],
                              "sent_to": ["Frankie"],
                          })):
            result = await cs_kol_handoff.ensure_kol_workflow(
                "rec_ticket",
                {"品牌": "FUNLAB", "客户标识": "creator@example.com",
                 "原文": "Partnership request\n\nI would like to collaborate."},
            )

        self.assertEqual("rec_kol", result["contact_record_id"])
        self.assertFalse(result["kol_master_created"])
        self.assertEqual("", result["draft_record_id"])
        self.assertEqual(["tbl_fu"], [row[0] for row in creates])
        self.assertEqual(["om_intake"], result["message_ids"])
        self.assertEqual(0, result["emails_sent"])

    async def test_new_creator_master_is_controlled_then_followup_is_created(self):
        created = []

        async def create_record(table_id, fields):
            created.append((table_id, fields))
            return {
                cs_kol_handoff.config.T_KOL: "rec_new_kol",
                cs_kol_handoff.config.T_KOL_FU: "rec_fu",
            }[table_id]

        async def get_record(table_id, record_id):
            if table_id == cs_kol_handoff.config.T_KOL:
                return {"record_id": record_id, "fields": created[0][1]}
            raise AssertionError("unexpected table")

        with patch.multiple(cs_kol_handoff.config, T_KOL="tbl_kol",
                            T_KOL_FU="tbl_fu"), \
             patch.object(cs_kol_handoff, "lookup_contact",
                          new=AsyncMock(side_effect=[(None, None), (None, None)])), \
             patch.object(cs_kol_handoff, "_find_existing_followup",
                          new=AsyncMock(return_value=None)), \
             patch.object(cs_kol_handoff.feishu, "create_record", new=create_record), \
             patch.object(cs_kol_handoff.feishu, "get_record", new=get_record), \
             patch.object(cs_kol_handoff, "_exact_kol_email_matches",
                          new=AsyncMock(return_value=[{
                              "record_id": "rec_new_kol",
                              "fields": {"邮箱": "creator@example.com"},
                          }])), \
             patch.object(cs_kol_handoff, "send_intake_card_once",
                          new=AsyncMock(return_value={
                              "ok": True, "message_ids": ["om_intake"],
                              "sent_to": ["Frankie"],
                          })):
            result = await cs_kol_handoff.ensure_kol_workflow(
                "rec_ticket",
                {"品牌": "FUNLAB", "客户标识": "creator@example.com",
                 "原文": "Collaboration\n\nI make gaming videos."},
            )

        self.assertTrue(result["kol_master_created"])
        self.assertEqual("rec_new_kol", result["contact_record_id"])
        self.assertEqual(["tbl_kol", "tbl_fu"],
                         [row[0] for row in created])
        self.assertEqual("待核对", created[0][1]["触达路由状态"])
        self.assertEqual("", result["draft_record_id"])
        self.assertEqual(0, result["emails_sent"])

    async def test_legacy_quarantine_card_is_closed_with_kol_assistant(self):
        with patch.object(cs_kol_handoff.feishu, "update_card_message_with_app",
                          new=AsyncMock(return_value=True)) as update:
            result = await cs_kol_handoff.close_legacy_review_cards(
                ["om_legacy"], "rec_kol", "rec_fu",
            )

        self.assertTrue(result["ok"])
        card = update.await_args.args[1]
        self.assertEqual("kol_assistant", update.await_args.kwargs["which"])
        rendered = json.dumps(card, ensure_ascii=False)
        self.assertIn("已迁移至标准 KOL 工作流", rendered)
        self.assertNotIn("draft_approve", rendered)
        self.assertNotIn("draft_reject", rendered)

    async def test_duplicate_email_after_create_stops_before_followup_or_card(self):
        created_master = {"record_id": "rec_new", "fields": {
            "邮箱": "creator@example.com", "合作状态": "未建联",
            "触达路由状态": "待核对", "资料可用状态": "缺资料",
        }}
        with patch.object(cs_kol_handoff, "lookup_contact",
                          new=AsyncMock(side_effect=[(None, None), (None, None)])), \
             patch.object(cs_kol_handoff.feishu, "create_record",
                          new=AsyncMock(return_value="rec_new")), \
             patch.object(cs_kol_handoff.feishu, "get_record",
                          new=AsyncMock(return_value=created_master)), \
             patch.object(cs_kol_handoff, "_exact_kol_email_matches",
                          new=AsyncMock(return_value=[
                              created_master,
                              {"record_id": "rec_other", "fields": {"邮箱": "creator@example.com"}},
                          ])), \
             patch.object(cs_kol_handoff, "_find_existing_followup",
                          new=AsyncMock()) as find_followup, \
             patch.object(cs_kol_handoff, "send_intake_card_once",
                          new=AsyncMock()) as send:
            with self.assertRaisesRegex(RuntimeError, "duplicate KOL email"):
                await cs_kol_handoff.ensure_kol_workflow(
                    "rec_duplicate_guard",
                    {"客户标识": "creator@example.com", "品牌": "FUNLAB"},
                )

        find_followup.assert_not_awaited()
        send.assert_not_awaited()

    def test_intake_card_uses_kol_records_and_has_no_send_action(self):
        card = cs_kol_handoff.build_intake_card(
            "rec_ticket",
            {
                "工单ID": "CSZ-message-1",
                "品牌": "FUNLAB",
                "客户标识": "creator@example.com",
                "客诉摘要": "Creator asks about collaboration.",
            },
            contact={"record_id": "rec_kol", "fields": {
                "账号名": "Creator A", "合作状态": "未建联",
                "触达路由状态": "待核对", "资料可用状态": "缺资料",
            }},
            followup_id="rec_fu",
        )

        rendered = str(card)
        self.assertIn("当前轮到谁", rendered)
        self.assertIn("KOL 运营审核/补资料", rendered)
        self.assertIn("回填位置", rendered)
        self.assertIn("截止时间", rendered)
        self.assertIn("rec_kol", rendered)
        self.assertIn("rec_fu", rendered)
        self.assertIn("待核对", rendered)
        self.assertNotIn("draft_approve", rendered)
        self.assertNotIn("draft_reject", rendered)
        self.assertNotIn("value.action", rendered)
        self.assertNotIn("form_submit", rendered)

    async def test_send_intake_card_claims_before_send_and_uses_stable_uuids(self):
        source_updates = []
        async def write_history(_record_id, history):
            source_updates.append(history)

        with patch.object(cs_kol_handoff.feishu, "resolve_partnership_targets",
                          new=AsyncMock(return_value=[("Frankie", "on_frankie")])), \
             patch.object(cs_kol_handoff.feishu, "send_card_message",
                          new=AsyncMock(return_value="om_personal")) as send, \
             patch.object(cs_kol_handoff, "_write_source_history", new=write_history):
            result = await cs_kol_handoff.send_intake_card_once(
                "rec_ticket",
                {"品牌": "FUNLAB", "客户标识": "creator@example.com",
                 "客诉摘要": "Creator asks about collaboration."},
                contact={"record_id": "rec_kol", "fields": {"账号名": "Creator"}},
                contact_type="KOL", followup_id="rec_fu",
            )

        self.assertTrue(result["ok"])
        self.assertEqual(["om_personal"], result["message_ids"])
        self.assertIn(cs_kol_handoff.INTAKE_ATTEMPT_MARKER, source_updates[0])
        self.assertIn(cs_kol_handoff.INTAKE_RECEIPT_MARKER, source_updates[-1])
        self.assertEqual(1, send.await_count)
        for call in send.await_args_list:
            self.assertEqual("kol_assistant", call.kwargs["which"])
            self.assertTrue(call.kwargs["message_uuid"].startswith("cs-kol-intake-"))

    async def test_ambiguous_intake_attempt_blocks_automatic_resend(self):
        fields = {"沟通历史摘要": f"{cs_kol_handoff.INTAKE_ATTEMPT_MARKER}rec_ticket"}
        with patch.object(cs_kol_handoff.feishu, "send_card_message",
                          new=AsyncMock()) as send:
            with self.assertRaisesRegex(RuntimeError, "reconciliation"):
                await cs_kol_handoff.send_intake_card_once(
                    "rec_ticket", fields,
                    contact={"record_id": "rec_kol", "fields": {}},
                    contact_type="KOL", followup_id="rec_fu",
                )
        send.assert_not_awaited()

    async def test_lookup_contact_checks_kol_then_editor(self):
        existing = {"record_id": "rec_kol", "fields": {"邮箱": "creator@example.com"}}
        with patch.object(cs_kol_handoff.feishu, "search_records",
                          new=AsyncMock(side_effect=[[existing], []])) as search:
            record, kind = await cs_kol_handoff.lookup_contact("creator@example.com")

        self.assertEqual("rec_kol", record["record_id"])
        self.assertEqual("KOL", kind)
        self.assertEqual(1, search.await_count)

    async def test_commit_rejects_records_outside_confirmed_scope(self):
        with self.assertRaisesRegex(ValueError, "outside"):
            await cs_kol_handoff.correct_confirmed_ticket(
                "rec_not_authorized", dry_run=False, confirm=True, run_id="run-1",
            )

    async def test_admin_endpoint_requires_explicit_commit_confirmation(self):
        from app import main

        with patch.object(main, "_check_auth"):
            with self.assertRaises(HTTPException) as caught:
                await main.run_cs_kol_workflow_correct(
                    authorization="Bearer test", record_id="rec_ticket",
                    dry_run=False, confirm=False, run_id="run-1",
                )

        self.assertEqual(400, caught.exception.status_code)

    async def test_admin_endpoint_calls_scoped_correction(self):
        from app import main

        expected = {"ok": True, "workflow_followup_id": "rec_fu"}
        with patch.object(main, "_check_auth"), \
             patch.object(main.cs_kol_handoff, "correct_confirmed_ticket",
                          new=AsyncMock(return_value=expected)) as correct:
            result = await main.run_cs_kol_workflow_correct(
                authorization="Bearer test", record_id="rec_ticket",
                dry_run=False, confirm=True, run_id="run-1",
            )

        self.assertEqual(expected, result)
        correct.assert_awaited_once_with(
            "rec_ticket", dry_run=False, confirm=True, run_id="run-1",
        )

    def test_pending_marker_is_persisted_before_card_send(self):
        fields = cs_kol_handoff.mark_pending_fields({"沟通历史摘要": "original"})

        self.assertTrue(fields["沟通历史摘要"].startswith(cs_kol_handoff.PENDING_MARKER))
        self.assertIn("original", fields["沟通历史摘要"])

    def test_old_quarantine_marker_does_not_block_new_workflow_pending(self):
        fields = cs_kol_handoff.mark_pending_fields({
            "沟通历史摘要": f"{cs_kol_handoff.HANDOFF_MARKER}[\"om_old\"]",
        })
        self.assertTrue(fields["沟通历史摘要"].startswith(cs_kol_handoff.PENDING_MARKER))

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
             patch.object(cs_kol_handoff, "ensure_kol_workflow",
                          new=AsyncMock(return_value={
                              "ok": True, "message_ids": ["om_1"],
                              "followup_record_id": "rec_fu",
                              "draft_record_id": "",
                          })), \
             patch.object(cs_kol_handoff, "record_workflow_marker",
                          new=AsyncMock(return_value={"marker": "done"})) as mark:
            result = await cs_kol_handoff.retry_pending_handoffs(limit=10)

        self.assertTrue(result["ok"])
        self.assertEqual(1, result["sent"])
        mark.assert_awaited_once()


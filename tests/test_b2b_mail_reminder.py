import json
import os
import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

from app import b2b_mail_reminder as reminder


class B2BMailReminderDatetimeTests(unittest.TestCase):
    def test_all_bitable_datetime_fields_are_normalized_to_epoch_milliseconds(self):
        expected = int(datetime(2026, 9, 12, 10, 0, tzinfo=reminder.BJ).timestamp() * 1000)
        fields = {
            "最后来信时间": "2026-09-12 10:00:00",
            "最后发件时间": "2026-09-12T02:00:00Z",
            "最后草稿时间": expected // 1000,
            "回执时间": expected,
            "首次提醒时间": str(expected // 1000),
            "升级提醒时间": str(expected),
            "最后扫描时间": datetime(2026, 9, 12, 2, 0, tzinfo=timezone.utc),
            "线程Key": "mailbox@example.com|customer.example",
        }

        normalized = reminder._normalize_reminder_fields(fields)

        for name in reminder.REMINDER_DATETIME_FIELDS:
            self.assertEqual(expected, normalized[name], name)
            self.assertIsInstance(normalized[name], int, name)
        self.assertEqual(fields["线程Key"], normalized["线程Key"])

    def test_invalid_datetime_fails_before_it_can_be_sent(self):
        with self.assertRaises(reminder.ReminderWriteError):
            reminder._normalize_reminder_fields({"最后来信时间": "not-a-date"})


class B2BMailReminderWriteTests(unittest.IsolatedAsyncioTestCase):
    async def test_http_200_business_error_raises_without_relation_retry(self):
        api = AsyncMock(return_value={"code": 1254064, "msg": "DatetimeFieldConvFail"})
        fields = {
            "线程Key": "mailbox@example.com|rec_customer",
            "最后来信时间": "2026-09-12 10:00:00",
            "关联CRM客户": ["rec_customer"],
        }

        with patch.object(reminder.feishu, "api", api):
            with self.assertRaisesRegex(reminder.ReminderWriteError, r"code=1254064.*DatetimeFieldConvFail"):
                await reminder._upsert_reminder(fields)

        self.assertEqual(1, api.await_count)
        sent_fields = api.await_args.args[2]["fields"]
        self.assertIsInstance(sent_fields["最后来信时间"], int)

    async def test_create_without_record_id_fails_closed(self):
        api = AsyncMock(return_value={"code": 0, "data": {"record": {}}})

        with patch.object(reminder.feishu, "api", api):
            with self.assertRaisesRegex(reminder.ReminderWriteError, "missing record_id"):
                await reminder._upsert_reminder(
                    {"线程Key": "mailbox@example.com|customer.example", "最后扫描时间": "2026-09-12 10:00:00"}
                )

    async def test_reminder_write_failure_prevents_crm_sync(self):
        row = {
            "mailbox_account": "silvia.wu@powkong.com",
            "mailbox_owner": "吴晓丹",
            "identity": "customer.example",
            "company": "Customer",
            "last_in_at": "2026-09-12T02:00:00Z",
            "last_in_message_id": "<reply@example.com>",
            "last_in_from": "buyer@customer.example",
            "last_in_subject": "Re: catalogue",
            "last_in_folder": "INBOX",
            "risk": "P1",
            "status": "待回复",
            "hours_open": 2,
            "total_events": 1,
            "record_id": "rec_customer",
        }
        write = AsyncMock(side_effect=reminder.ReminderWriteError("code=1254064 msg=DatetimeFieldConvFail"))
        crm = AsyncMock()

        with patch.object(reminder, "_upsert_reminder", write), patch.object(
            reminder.b2b_crm_sync, "sync_inbound_reply", crm
        ):
            with self.assertRaises(reminder.ReminderWriteError):
                await reminder._sync_rows([row], {}, commit=True)

        crm.assert_not_awaited()

    async def test_public_run_stops_before_eligibility_and_notification_on_write_error(self):
        row = {
            "mailbox_account": "silvia.wu@powkong.com",
            "mailbox_owner": "吴晓丹",
            "identity": "customer.example",
            "company": "Customer",
            "last_in_at": "2026-09-12T02:00:00Z",
            "last_in_message_id": "<reply@example.com>",
            "last_in_from": "buyer@customer.example",
            "last_in_subject": "Re: catalogue",
            "last_in_folder": "INBOX",
            "risk": "P1",
            "status": "待回复",
            "hours_open": 2,
            "total_events": 1,
            "record_id": "rec_customer",
        }
        api = AsyncMock(return_value={"code": 1254064, "msg": "DatetimeFieldConvFail"})
        eligible = AsyncMock()
        send_card = AsyncMock()
        crm = AsyncMock()

        with patch.object(reminder, "_existing_reminders_by_key", AsyncMock(return_value={})), patch.object(
            reminder, "_load_accounts", AsyncMock(return_value=[])
        ), patch.object(reminder, "_load_customers", AsyncMock(return_value=[])), patch.object(
            reminder, "_collect_mail_events", AsyncMock(return_value=([], []))
        ), patch.object(reminder, "_audit_groups", return_value=([row], [row])), patch.object(
            reminder.feishu, "api", api
        ), patch.object(reminder, "_eligible_rows", eligible), patch.object(
            reminder.feishu, "send_card_via_b2b_assistant", send_card
        ), patch.object(reminder.b2b_crm_sync, "sync_inbound_reply", crm):
            with self.assertRaises(reminder.ReminderWriteError):
                await reminder.run(commit=True, notify=True, limit=10, days=1)

        self.assertEqual(1, api.await_count)
        crm.assert_not_awaited()
        eligible.assert_not_awaited()
        send_card.assert_not_awaited()

    async def test_mark_card_sent_writes_epoch_milliseconds(self):
        api = AsyncMock(return_value={"code": 0, "data": {"record": {"record_id": "rec_reminder"}}})
        rows = [{"record_id": "rec_reminder", "status": "待首次提醒"}]

        with patch.object(reminder.feishu, "api", api):
            updates = await reminder._mark_card_sent(rows)

        sent_value = api.await_args.args[2]["fields"]["首次提醒时间"]
        self.assertIsInstance(sent_value, int)
        self.assertEqual(13, len(str(sent_value)))
        self.assertIsInstance(updates[0]["首次提醒时间"], int)

    async def test_receipt_business_error_skips_crm_and_uses_epoch_milliseconds(self):
        api = AsyncMock(
            side_effect=[
                {"code": 0, "data": {"record": {"record_id": "rec_reminder", "fields": {}}}},
                {"code": 1254064, "msg": "DatetimeFieldConvFail"},
                {"code": 0, "data": {"message_id": "om_reply"}},
            ]
        )
        crm = AsyncMock()
        payload = {
            "sender_open_id": "ou_operator",
            "chat_id": "oc_chat",
            "card_action": {
                "action": "b2b_mail_replied",
                "record_id": "rec_reminder",
                "customer": "Customer",
            },
        }

        with patch.object(reminder.feishu, "api", api), patch.object(
            reminder, "_b2b_operator_name", AsyncMock(return_value="吴晓丹")
        ), patch.object(reminder.b2b_crm_sync, "sync_mail_receipt_to_customer", crm):
            result = await reminder.handle_receipt(payload)

        self.assertFalse(result["ok"])
        self.assertIn("code=1254064", result["write_error"])
        crm.assert_not_awaited()
        self.assertEqual(
            ["b2b_base", "b2b_base", "b2b_assistant"],
            [call.kwargs.get("which") for call in api.await_args_list],
        )
        receipt_fields = api.await_args_list[1].args[2]["fields"]
        self.assertIsInstance(receipt_fields["回执时间"], int)
        self.assertEqual(13, len(str(receipt_fields["回执时间"])))


class B2BMailReminderOwnerRoutingTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _eligible_row(record_id: str, owner: str, *, status: str = "待首次提醒") -> dict:
        return {
            "record_id": record_id,
            "thread_key": f"mailbox@example.com|{record_id}",
            "mailbox": "mailbox@example.com",
            "owner": owner,
            "external_email": f"buyer-{record_id}@customer.example",
            "customer": f"Customer {record_id}",
            "last_in_at": "2026-09-14T01:00:00+00:00",
            "subject": f"Subject {record_id}",
            "status": status,
            "risk": "P0" if status == "24h待升级" else "P1",
            "trigger_reason": "客户来信后未回复",
            "hours_open": "25" if status == "24h待升级" else "2",
            "first_reminded_at": "",
            "escalated_at": "",
        }

    async def _run(
        self,
        eligible: list[dict],
        send_card: AsyncMock,
        mark_card_sent: AsyncMock,
        *,
        notify: bool = True,
    ):
        with patch.object(reminder, "_existing_reminders_by_key", AsyncMock(return_value={})), patch.object(
            reminder, "_load_accounts", AsyncMock(return_value=[])
        ), patch.object(reminder, "_load_customers", AsyncMock(return_value=[])), patch.object(
            reminder, "_collect_mail_events", AsyncMock(return_value=([], []))
        ), patch.object(reminder, "_audit_groups", return_value=([], [])), patch.object(
            reminder, "_sync_rows", AsyncMock(return_value={"rows": 0})
        ), patch.object(reminder, "_eligible_rows", AsyncMock(return_value=eligible)), patch.object(
            reminder, "_mark_card_sent", mark_card_sent
        ), patch.object(reminder.feishu, "send_card_via_b2b_assistant", send_card):
            return await reminder.run(commit=True, notify=notify, limit=10, days=1)

    async def test_splits_main_cards_by_owner_and_uses_only_private_targets(self):
        wu = self._eligible_row("rec_wu", "吴晓丹")
        li = self._eligible_row("rec_li", "李桐欣")
        send_card = AsyncMock(side_effect=["om_wu", "om_li"])
        mark_card_sent = AsyncMock(
            side_effect=lambda rows: [{"record_id": row["record_id"]} for row in rows]
        )
        mapping = {
            "吴晓丹": "open_id:ou_wu",
            "李桐欣": {"receive_type": "union_id", "receive_id": "on_li"},
        }

        with patch.dict(
            os.environ,
            {"B2B_LINKEDIN_OWNER_NOTIFY_JSON": json.dumps(mapping, ensure_ascii=False)},
            clear=False,
        ):
            result = await self._run([wu, li], send_card, mark_card_sent)

        self.assertEqual(2, send_card.await_count)
        calls_by_target = {call.args[1]: call for call in send_card.await_args_list}
        self.assertEqual({"ou_wu", "on_li"}, set(calls_by_target))
        self.assertEqual("open_id", calls_by_target["ou_wu"].args[0])
        self.assertEqual("union_id", calls_by_target["on_li"].args[0])
        wu_card = json.dumps(calls_by_target["ou_wu"].args[2], ensure_ascii=False)
        li_card = json.dumps(calls_by_target["on_li"].args[2], ensure_ascii=False)
        self.assertIn("rec_wu", wu_card)
        self.assertNotIn("rec_li", wu_card)
        self.assertIn("rec_li", li_card)
        self.assertNotIn("rec_wu", li_card)
        mark_card_sent.assert_awaited_once_with([wu, li])
        self.assertEqual("om_wu", result["message_id"])
        self.assertEqual(
            [
                {"owner": "吴晓丹", "receive_type": "open_id", "message_id": "om_wu"},
                {"owner": "李桐欣", "receive_type": "union_id", "message_id": "om_li"},
            ],
            result["message_ids"],
        )

    async def test_missing_owner_mapping_stops_before_any_card_or_mark(self):
        li = self._eligible_row("rec_li", "李桐欣")
        send_card = AsyncMock()
        mark_card_sent = AsyncMock()

        with patch.dict(
            os.environ,
            {"B2B_LINKEDIN_OWNER_NOTIFY_JSON": json.dumps({"吴晓丹": "open_id:ou_wu"}, ensure_ascii=False)},
            clear=False,
        ):
            with self.assertRaisesRegex(ValueError, "李桐欣"):
                await self._run([li], send_card, mark_card_sent)

        send_card.assert_not_awaited()
        mark_card_sent.assert_not_awaited()

    async def test_missing_mapping_stops_before_any_card_or_mark(self):
        wu = self._eligible_row("rec_wu", "吴晓丹")
        send_card = AsyncMock()
        mark_card_sent = AsyncMock()

        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("B2B_LINKEDIN_OWNER_NOTIFY_JSON", None)
            with self.assertRaisesRegex(ValueError, "missing B2B owner private notify mapping"):
                await self._run([wu], send_card, mark_card_sent)

        send_card.assert_not_awaited()
        mark_card_sent.assert_not_awaited()

    async def test_group_target_stops_before_any_card_or_mark(self):
        wu = self._eligible_row("rec_wu", "吴晓丹")
        send_card = AsyncMock()
        mark_card_sent = AsyncMock()

        with patch.dict(
            os.environ,
            {"B2B_LINKEDIN_OWNER_NOTIFY_JSON": json.dumps({"吴晓丹": "chat_id:oc_group"}, ensure_ascii=False)},
            clear=False,
        ):
            with self.assertRaisesRegex(ValueError, "private target required"):
                await self._run([wu], send_card, mark_card_sent)

        send_card.assert_not_awaited()
        mark_card_sent.assert_not_awaited()

    async def test_malformed_email_wildcard_and_placeholder_routes_fail_closed(self):
        cases = [
            ("{", "bad JSON"),
            (json.dumps({"吴晓丹": "email:owner@example.com"}, ensure_ascii=False), "email target"),
            (json.dumps({"*": "open_id:ou_wildcard"}, ensure_ascii=False), "wildcard"),
            (json.dumps({"吴晓丹": "open_id:"}, ensure_ascii=False), "blank ID"),
        ]
        for raw_mapping, label in cases:
            with self.subTest(label=label):
                wu = self._eligible_row("rec_wu", "吴晓丹")
                send_card = AsyncMock()
                mark_card_sent = AsyncMock()
                with patch.dict(
                    os.environ,
                    {"B2B_LINKEDIN_OWNER_NOTIFY_JSON": raw_mapping},
                    clear=False,
                ):
                    with self.assertRaises(ValueError):
                        await self._run([wu], send_card, mark_card_sent)
                send_card.assert_not_awaited()
                mark_card_sent.assert_not_awaited()

        placeholder = self._eligible_row("rec_unknown", "待确认")
        send_card = AsyncMock()
        mark_card_sent = AsyncMock()
        with patch.dict(
            os.environ,
            {"B2B_LINKEDIN_OWNER_NOTIFY_JSON": json.dumps({"待确认": "open_id:ou_unknown"}, ensure_ascii=False)},
            clear=False,
        ):
            with self.assertRaises(ValueError):
                await self._run([placeholder], send_card, mark_card_sent)
        send_card.assert_not_awaited()
        mark_card_sent.assert_not_awaited()

    async def test_duplicate_private_target_stops_before_any_card_or_mark(self):
        wu = self._eligible_row("rec_wu", "吴晓丹")
        li = self._eligible_row("rec_li", "李桐欣")
        send_card = AsyncMock()
        mark_card_sent = AsyncMock()
        mapping = {"吴晓丹": "open_id:ou_same", "李桐欣": "open_id:ou_same"}

        with patch.dict(
            os.environ,
            {"B2B_LINKEDIN_OWNER_NOTIFY_JSON": json.dumps(mapping, ensure_ascii=False)},
            clear=False,
        ):
            with self.assertRaisesRegex(ValueError, "duplicate private notify target"):
                await self._run([wu, li], send_card, mark_card_sent)

        send_card.assert_not_awaited()
        mark_card_sent.assert_not_awaited()

    async def test_partial_send_marks_only_successful_owner_and_raises(self):
        wu = self._eligible_row("rec_wu", "吴晓丹")
        li = self._eligible_row("rec_li", "李桐欣")

        async def send(receive_type, receive_id, card):
            if receive_id == "ou_li":
                raise RuntimeError("temporary Feishu error")
            return "om_wu"

        send_card = AsyncMock(side_effect=send)
        mark_card_sent = AsyncMock(
            side_effect=lambda rows: [{"record_id": row["record_id"]} for row in rows]
        )
        mapping = {"吴晓丹": "open_id:ou_wu", "李桐欣": "open_id:ou_li"}

        with patch.dict(
            os.environ,
            {"B2B_LINKEDIN_OWNER_NOTIFY_JSON": json.dumps(mapping, ensure_ascii=False)},
            clear=False,
        ):
            with self.assertRaisesRegex(RuntimeError, "李桐欣"):
                await self._run([wu, li], send_card, mark_card_sent)

        mark_card_sent.assert_awaited_once_with([wu])

    async def test_partial_send_escalates_only_successful_24h_owner_and_keeps_failure_evidence(self):
        wu = self._eligible_row("rec_wu", "吴晓丹", status="24h待升级")
        li = self._eligible_row("rec_li", "李桐欣", status="24h待升级")

        async def send(receive_type, receive_id, card):
            if receive_id == "ou_li":
                raise reminder.feishu.FeishuAPIError(
                    method="POST",
                    path="/im/v1/messages?receive_id_type=open_id",
                    status_code=400,
                    feishu_code=230013,
                    feishu_msg="recipient ou_leaked on_leaked owner@example.com unavailable",
                )
            if receive_id == reminder.B2B_WU_NOTIFY_UNION_ID:
                raise reminder.feishu.FeishuAPIError(
                    method="POST",
                    path="/im/v1/messages?receive_id_type=union_id",
                    status_code=503,
                    feishu_code=2200,
                    feishu_msg="escalation on_escalation_leaked escalation@example.com unavailable",
                )
            return "om_wu"

        send_card = AsyncMock(side_effect=send)
        mark_card_sent = AsyncMock(
            side_effect=lambda rows: [{"record_id": row["record_id"]} for row in rows]
        )
        mapping = {"吴晓丹": "open_id:ou_wu", "李桐欣": "open_id:ou_li"}

        with patch.dict(
            os.environ,
            {"B2B_LINKEDIN_OWNER_NOTIFY_JSON": json.dumps(mapping, ensure_ascii=False)},
            clear=False,
        ):
            with self.assertRaises(reminder.ReminderNotificationError) as raised:
                await self._run([wu, li], send_card, mark_card_sent)

        self.assertEqual(3, send_card.await_count)
        escalation_card = json.dumps(send_card.await_args_list[2].args[2], ensure_ascii=False)
        self.assertIn("rec_wu", escalation_card)
        self.assertNotIn("rec_li", escalation_card)
        mark_card_sent.assert_awaited_once_with([wu])
        partial = raised.exception.partial_result
        self.assertEqual(["李桐欣"], partial["failed_owners"])
        self.assertEqual([{"record_id": "rec_wu"}], partial["marked_sent"])
        self.assertEqual(
            [{"owner": "吴晓丹", "receive_type": "open_id", "message_id": "om_wu"}],
            partial["message_ids"],
        )
        self.assertNotIn("ou_wu", json.dumps(partial, ensure_ascii=False))
        self.assertNotIn("ou_li", json.dumps(partial, ensure_ascii=False))
        self.assertNotIn("ou_leaked", json.dumps(partial, ensure_ascii=False))
        self.assertNotIn("on_leaked", json.dumps(partial, ensure_ascii=False))
        self.assertNotIn("owner@example.com", json.dumps(partial, ensure_ascii=False))
        self.assertNotIn("on_escalation_leaked", json.dumps(partial, ensure_ascii=False))
        self.assertNotIn("escalation@example.com", json.dumps(partial, ensure_ascii=False))
        safe_errors = " | ".join(partial["notify_errors"])
        self.assertIn("HTTP=400", safe_errors)
        self.assertIn("FeishuCode=230013", safe_errors)
        self.assertIn("HTTP=503", safe_errors)
        self.assertIn("FeishuCode=2200", safe_errors)

    async def test_empty_message_id_is_a_failed_owner_delivery(self):
        wu = self._eligible_row("rec_wu", "吴晓丹")
        send_card = AsyncMock(return_value="")
        mark_card_sent = AsyncMock()

        with patch.dict(
            os.environ,
            {"B2B_LINKEDIN_OWNER_NOTIFY_JSON": json.dumps({"吴晓丹": "open_id:ou_wu"}, ensure_ascii=False)},
            clear=False,
        ):
            with self.assertRaisesRegex(RuntimeError, "missing_message_id"):
                await self._run([wu], send_card, mark_card_sent)

        mark_card_sent.assert_not_awaited()

    async def test_existing_24h_escalation_private_copy_is_preserved(self):
        wu = self._eligible_row("rec_wu", "吴晓丹", status="24h待升级")
        send_card = AsyncMock(side_effect=["om_owner", "om_escalation"])
        mark_card_sent = AsyncMock(return_value=[{"record_id": "rec_wu"}])

        with patch.dict(
            os.environ,
            {"B2B_LINKEDIN_OWNER_NOTIFY_JSON": json.dumps({"吴晓丹": "open_id:ou_wu"}, ensure_ascii=False)},
            clear=False,
        ):
            result = await self._run([wu], send_card, mark_card_sent)

        self.assertEqual(2, send_card.await_count)
        self.assertEqual("open_id", send_card.await_args_list[0].args[0])
        self.assertEqual("union_id", send_card.await_args_list[1].args[0])
        self.assertEqual("om_escalation", result["wu_message_id"])
        escalation_card = json.dumps(send_card.await_args_list[1].args[2], ensure_ascii=False)
        self.assertIn("B2B邮件24h升级确认", escalation_card)
        mark_card_sent.assert_awaited_once_with([wu])

    async def test_notify_false_does_not_require_private_mapping(self):
        wu = self._eligible_row("rec_wu", "吴晓丹")
        send_card = AsyncMock()
        mark_card_sent = AsyncMock()

        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("B2B_LINKEDIN_OWNER_NOTIFY_JSON", None)
            result = await self._run([wu], send_card, mark_card_sent, notify=False)

        send_card.assert_not_awaited()
        mark_card_sent.assert_not_awaited()
        self.assertEqual(1, result["eligible_count"])


class B2BMailReminderJobResultTests(unittest.IsolatedAsyncioTestCase):
    def test_compact_async_job_result_keeps_per_owner_delivery_evidence(self):
        from app import main

        message_ids = [
            {"owner": "吴晓丹", "receive_type": "open_id", "message_id": "om_wu"}
        ]

        compact = main._compact_b2b_result({"ok": True, "message_ids": message_ids})

        self.assertEqual(message_ids, compact["message_ids"])

    async def test_async_job_keeps_sanitized_partial_delivery_evidence_on_error(self):
        from app import main

        job_id = "test-partial-owner-delivery"
        partial = {
            "ok": False,
            "commit": True,
            "notify": True,
            "eligible_count": 2,
            "message_ids": [
                {"owner": "吴晓丹", "receive_type": "open_id", "message_id": "om_wu"}
            ],
            "marked_sent": [{"record_id": "rec_wu"}],
            "failed_owners": ["李桐欣"],
            "notify_errors": ["李桐欣负责人私聊失败: RuntimeError: temporary Feishu error"],
        }
        error = reminder.ReminderNotificationError(
            "李桐欣负责人私聊失败",
            partial_result=partial,
        )
        main._b2b_mail_jobs[job_id] = {"status": "running", "started_ts": 0}
        try:
            with patch.object(
                main.b2b_mail_reminder,
                "run",
                AsyncMock(side_effect=error),
            ), patch.object(main, "_alert_endpoint_failure", AsyncMock()):
                await main._run_b2b_mail_job(job_id, True, True, 10, 30)

            job = main._b2b_mail_jobs[job_id]
            self.assertEqual("error", job["status"])
            self.assertEqual(partial["message_ids"], job["result"]["message_ids"])
            self.assertEqual(partial["marked_sent"], job["result"]["marked_sent"])
            self.assertEqual(partial["failed_owners"], job["result"]["failed_owners"])
            self.assertNotIn("receive_id", json.dumps(job["result"], ensure_ascii=False))
        finally:
            main._b2b_mail_jobs.pop(job_id, None)


if __name__ == "__main__":
    unittest.main()

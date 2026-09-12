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


if __name__ == "__main__":
    unittest.main()

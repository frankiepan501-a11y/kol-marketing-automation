import os
import sys
import unittest
from unittest.mock import AsyncMock, patch


os.environ.setdefault("FEISHU_APP3_ID", "test")
os.environ.setdefault("FEISHU_APP3_SECRET", "test")
os.environ.setdefault("INTERNAL_TOKEN", "test")
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app import kol_contact_acquisition as contact


class KolContactAcquisitionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        contact._TEST_RECEIPTS.clear()
        self.record = {
            "record_id": "rec_test",
            "fields": {
                "账号名": "Test Creator",
                "主平台": "YouTube",
                "主链接": "https://youtube.com/@test",
                "国家": "US",
                "语言": "en",
                "粉丝数": 12000,
                "内容风格": ["游戏"],
            },
        }

    def test_card_has_context_links_and_safe_form_layout(self):
        card = contact.build_card(self.record, test_mode=True)
        forms = [element for element in card["elements"] if element.get("tag") == "form"]
        self.assertEqual(1, len(forms))
        submit = [element for element in forms[0]["elements"] if element.get("action_type") == "form_submit"]
        self.assertEqual(1, len(submit))
        self.assertEqual("kol_contact_email", submit[0]["value"]["action"])
        self.assertTrue(submit[0]["value"]["test_mode"])
        actions = [element for element in card["elements"] if element.get("tag") == "action"]
        result_buttons = actions[-1]["actions"]
        self.assertEqual(3, len(result_buttons))
        self.assertIn("Test Creator", str(card))
        self.assertIn("测试边界", str(card))

    async def test_test_callback_patches_card_without_writing_record(self):
        event = {
            "open_message_id": "om_test",
            "action": {
                "value": contact._action_value("rec_test", "platform", test_mode=True),
                "form_value": {},
            },
        }
        with patch.object(contact.feishu, "update_record", new=AsyncMock()) as writer, patch.object(
            contact.feishu, "update_card_message_with_app", new=AsyncMock(return_value=True)
        ) as patcher:
            result = await contact.handle_callback(event)
        self.assertTrue(result["ok"])
        writer.assert_not_awaited()
        patcher.assert_awaited_once()

    async def test_test_callback_is_idempotent(self):
        event = {"action": {"value": contact._action_value("rec_test", "no_reply", test_mode=True)}}
        first = await contact.handle_callback(event)
        second = await contact.handle_callback(event)
        self.assertTrue(first["ok"])
        self.assertTrue(second["idempotent"])

    async def test_email_requires_valid_shape(self):
        event = {
            "action": {
                "value": contact._action_value("rec_test", "email", test_mode=True),
                "form_value": {"contact_email": "not-an-email"},
            }
        }
        result = await contact.handle_callback(event)
        self.assertEqual("warning", result["toast"]["type"])
        self.assertNotIn("p0-3a-frankie-only-20260901:rec_test", contact._TEST_RECEIPTS)

    async def test_live_email_writes_whitelist_and_reads_back(self):
        event = {
            "action": {
                "value": contact._action_value("rec_test", "email", test_mode=False),
                "form_value": {"contact_email": "Business@Test.Example"},
            }
        }
        readback = {"record_id": "rec_test", "fields": {"联系状态": "邮件可用"}}
        with patch.object(contact.feishu, "update_record", new=AsyncMock()) as writer, patch.object(
            contact.feishu, "get_record", new=AsyncMock(return_value=readback)
        ):
            result = await contact.handle_callback(event)
        self.assertTrue(result["ok"])
        fields = writer.await_args.args[2]
        self.assertEqual("business@test.example", fields["邮箱"])
        self.assertEqual("邮件可用", fields["联系状态"])
        self.assertEqual("未验", fields["邮箱验真状态"])
        self.assertEqual("可新开发", fields["触达路由状态"])


if __name__ == "__main__":
    unittest.main()

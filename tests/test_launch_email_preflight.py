import asyncio
import os
import unittest
from unittest.mock import AsyncMock, patch

from app import launch_email_preflight as preflight


class LaunchEmailPreflightTests(unittest.TestCase):
    def test_requires_global_dry_run_target(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(RuntimeError, "EMAIL_DRY_RUN_TO"):
                preflight.require_test_mode("TEST_ONLY")

    def test_requires_explicit_confirmation(self):
        with patch.dict(os.environ, {"EMAIL_DRY_RUN_TO": "test@example.com"}, clear=True):
            with self.assertRaisesRegex(RuntimeError, "TEST_ONLY"):
                preflight.require_test_mode("no")

    def test_rejects_non_allowlisted_or_multi_address_target(self):
        with patch.dict(os.environ, {"EMAIL_DRY_RUN_TO": "creator@example.com"}, clear=True):
            with self.assertRaisesRegex(RuntimeError, "白名单"):
                preflight.require_test_mode("TEST_ONLY")
        with patch.dict(os.environ, {"EMAIL_DRY_RUN_TO": "frankiepan501@gmail.com,creator@example.com"}, clear=True):
            with self.assertRaisesRegex(RuntimeError, "单个规范邮箱"):
                preflight.require_test_mode("TEST_ONLY")

    def test_raw_validator_checks_product_links_and_placeholders(self):
        expected = {
            "subject": "[Launch preflight:run-0001] FUNLAB Dave the Diver Controller",
            "body": '<p>FUNLAB Dave the Diver Controller launch preview.</p><p><a href="https://example.com/p">Product page</a></p>',
            "product_name": "FUNLAB Dave the Diver Controller",
            "links": ["https://example.com/p"],
            "from_address": "partner@fireflyfunlab.com",
        }
        result = preflight.validate_raw_content(
            raw_subject="[DRY-RUN→launch-preflight@example.invalid] " + expected["subject"],
            raw_body=expected["body"],
            actual_to="test@example.com",
            actual_from="partner@fireflyfunlab.com",
            expected_to="test@example.com",
            expected=expected,
        )
        self.assertTrue(result["passed"])

        bad = preflight.validate_raw_content(
            raw_subject="[DRY-RUN] test",
            raw_body="<p>[TBD]</p>",
            actual_to="test@example.com",
            actual_from="wrong@example.com",
            expected_to="test@example.com",
            expected=expected,
        )
        self.assertFalse(bad["passed"])
        self.assertIn("placeholder_free", bad["checks"])

    def test_send_and_validate_is_test_only_and_writes_no_bitable_rows(self):
        product = {
            "record_id": "product1",
            "fields": {
                "产品英文名": "FUNLAB Dave the Diver Controller",
                "品牌": "FUNLAB",
                "官网链接": {"link": "https://example.com/dave", "text": "Dave"},
            },
        }
        draft = {
            "record_id": "draft1",
            "fields": {
                "关联产品": {"link_record_ids": ["product1"]},
                "邮件草稿来源": "cold",
                "发送邮箱": "FUNLAB邮箱(@funlabswitch.com)",
                "邮件主题": "A controller built for Dave the Diver fans",
                "邮件正文": (
                    '<p>Hi Creator,</p><p>FUNLAB Dave the Diver Controller combines a precise '
                    'Hall-effect design with an officially licensed ocean-inspired look.</p>'
                    '<p><a href="https://example.com/dave?utm_source=kol">See the product</a></p>'
                    '<p>Would you like to review it?</p>'
                ),
            },
        }
        sent_message = {
            "messageId": "msg1",
            "subject": "[DRY-RUN→launch-preflight@example.invalid] "
                       "[Launch preflight:run-0001] A controller built for Dave the Diver fans",
            "toAddress": "frankiepan501@gmail.com",
            "fromAddress": "partner@fireflyfunlab.com",
        }
        raw_body = (
            '<div><strong>DRY-RUN MODE</strong></div>'
            '<p>Hi Creator,</p><p>FUNLAB Dave the Diver Controller combines a precise '
            'Hall-effect design with an officially licensed ocean-inspired look.</p>'
            '<p><a href="https://example.com/dave?utm_source=kol">See the product</a></p>'
            '<p>Would you like to review it?</p>'
        )

        with patch.dict(os.environ, {"EMAIL_DRY_RUN_TO": "frankiepan501@gmail.com"}, clear=False), \
             patch.object(preflight.feishu, "get_record", new=AsyncMock(side_effect=[product, draft])), \
             patch.object(preflight.zoho, "send_email", new=AsyncMock(return_value="msg1")) as send_mock, \
             patch.object(preflight.zoho, "_get_folder_ids", new=AsyncMock(return_value=("drafts", "sent"))), \
             patch.object(preflight.zoho, "get_message_content", new=AsyncMock(return_value=raw_body)), \
             patch.object(preflight.zoho, "list_sent_messages", new=AsyncMock(side_effect=[
                 {"messages": []}, {"messages": [sent_message]},
             ])), \
             patch.object(preflight.feishu, "create_record", new=AsyncMock()) as create_mock, \
             patch.object(preflight.feishu, "update_record", new=AsyncMock()) as update_mock:
            result = asyncio.run(preflight.send_and_validate(
                "product1", "draft1", "FUNLAB", confirm="TEST_ONLY", run_key="run-0001"
            ))

        self.assertTrue(result["ok"])
        self.assertTrue(result["validation"]["passed"])
        self.assertEqual(0, result["production_draft_rows_written"])
        send_mock.assert_awaited_once()
        self.assertEqual("launch-preflight@example.invalid", send_mock.await_args.args[1])
        create_mock.assert_not_awaited()
        update_mock.assert_not_awaited()

    def test_rejects_brand_mismatch_before_send(self):
        product = {
            "record_id": "product1",
            "fields": {
                "产品英文名": "FUNLAB Dave the Diver Controller",
                "品牌": "FUNLAB",
                "官网链接": {"link": "https://example.com/dave", "text": "Dave"},
            },
        }
        with patch.dict(os.environ, {"EMAIL_DRY_RUN_TO": "frankiepan501@gmail.com"}, clear=False), \
             patch.object(preflight.feishu, "get_record", new=AsyncMock(return_value=product)), \
            patch.object(preflight.zoho, "send_email", new=AsyncMock()) as send_mock:
            with self.assertRaisesRegex(RuntimeError, "不能使用 POWKONG"):
                asyncio.run(preflight.send_and_validate(
                    "product1", "draft1", "POWKONG", confirm="TEST_ONLY", run_key="run-0002"
                ))
        send_mock.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()

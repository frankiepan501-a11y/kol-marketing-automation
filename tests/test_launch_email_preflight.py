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

    def test_raw_validator_checks_product_links_and_placeholders(self):
        expected = {
            "subject": "[Launch preflight] FUNLAB Dave the Diver Controller",
            "body": '<p>FUNLAB Dave the Diver Controller launch preview.</p><p><a href="https://example.com/p">Product page</a></p>',
            "product_name": "FUNLAB Dave the Diver Controller",
            "links": ["https://example.com/p"],
        }
        result = preflight.validate_raw_content(
            raw_subject="[DRY-RUN→launch-preflight@example.invalid] " + expected["subject"],
            raw_body=expected["body"],
            actual_to="test@example.com",
            expected_to="test@example.com",
            expected=expected,
        )
        self.assertTrue(result["passed"])

        bad = preflight.validate_raw_content(
            raw_subject="[DRY-RUN] test",
            raw_body="<p>[TBD]</p>",
            actual_to="test@example.com",
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
                "官网链接": {"link": "https://example.com/dave", "text": "Dave"},
            },
        }
        sent_message = {
            "messageId": "msg1",
            "subject": "[DRY-RUN→launch-preflight@example.invalid] "
                       "[Launch preflight] FUNLAB Dave the Diver Controller",
            "toAddress": "frankiepan501@gmail.com",
        }
        raw_body = (
            '<div><strong>DRY-RUN MODE</strong></div>'
            '<p>Hey Frankie,</p><p>This is the controlled preflight for '
            '<strong>FUNLAB Dave the Diver Controller</strong> from FUNLAB. '
            'It verifies the same Zoho draft, HTML rendering, sending and sent-folder raw-content path '
            'that will protect the first real launch outreach.</p>'
            '<p><a href="https://example.com/dave">Product page 1</a></p>'
            '<p>No KOL or media contact receives this message. -- Launch preflight</p>'
        )

        with patch.dict(os.environ, {"EMAIL_DRY_RUN_TO": "frankiepan501@gmail.com"}, clear=False), \
             patch.object(preflight.feishu, "get_record", new=AsyncMock(return_value=product)), \
             patch.object(preflight.zoho, "send_email", new=AsyncMock(return_value="msg1")) as send_mock, \
             patch.object(preflight.zoho, "_get_folder_ids", new=AsyncMock(return_value=("drafts", "sent"))), \
             patch.object(preflight.zoho, "get_message_content", new=AsyncMock(return_value=raw_body)), \
             patch.object(preflight.zoho, "list_sent_messages", new=AsyncMock(return_value={"messages": [sent_message]})), \
             patch.object(preflight.feishu, "create_record", new=AsyncMock()) as create_mock, \
             patch.object(preflight.feishu, "update_record", new=AsyncMock()) as update_mock:
            result = asyncio.run(preflight.send_and_validate("product1", "FUNLAB", confirm="TEST_ONLY"))

        self.assertTrue(result["ok"])
        self.assertTrue(result["validation"]["passed"])
        self.assertEqual(0, result["production_draft_rows_written"])
        send_mock.assert_awaited_once()
        self.assertEqual("launch-preflight@example.invalid", send_mock.await_args.args[1])
        create_mock.assert_not_awaited()
        update_mock.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()

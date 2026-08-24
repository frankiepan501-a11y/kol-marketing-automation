import asyncio
import os
import unittest
from unittest.mock import AsyncMock, patch

from app import launch_email_preflight as preflight


class LaunchEmailPreflightTests(unittest.TestCase):
    def test_campaign_certificate_rejects_draft_without_matching_template_marker(self):
        product = {"record_id": "product1", "fields": {
            "产品英文名": "FUNLAB Dave the Diver Controller", "品牌": "FUNLAB",
            "官网链接": {"link": "https://example.com/dave", "text": "Dave"},
        }}
        draft = {"record_id": "draft1", "fields": {
            "关联产品": ["product1"], "邮件草稿来源": "cold",
            "发送邮箱": "FUNLAB邮箱(@funlabswitch.com)",
            "邮件主题": "Old draft", "邮件正文": "<p>Old unmarked cold draft body long enough.</p>",
        }}
        with patch.dict(os.environ, {"EMAIL_DRY_RUN_TO": "frankiepan501@gmail.com"}, clear=False), \
             patch.object(preflight.feishu, "get_record", new=AsyncMock(side_effect=[product, draft])), \
             patch.object(preflight.zoho, "send_email", new=AsyncMock()) as send_mock:
            with self.assertRaisesRegex(RuntimeError, "模板版本标记"):
                asyncio.run(preflight.send_and_validate(
                    "product1", "draft1", "FUNLAB", confirm="TEST_ONLY",
                    run_key="run-cert-1", campaign_id="campaign1",
                    template_version="launch-queue-v1",
                ))
        send_mock.assert_not_awaited()

    def test_template_only_preflight_can_validate_exact_current_cold_template(self):
        product = {
            "record_id": "product1",
            "fields": {
                "产品英文名": "FUNLAB Dave the Diver Controller",
                "品牌": "FUNLAB", "品类": "controller",
                "官网链接": {"link": "https://example.com/dave", "text": "Dave"},
            },
        }
        sent_message = {
            "messageId": "msg-template",
            "subject": "[DRY-RUN→launch-preflight@example.invalid] "
                       "[Launch preflight:run-template-1] Launch Test, a controller for your setup",
            "toAddress": "frankiepan501@gmail.com",
            "fromAddress": "partner@fireflyfunlab.com",
        }
        generated_body = preflight.enrich._build_template_draft(
            {"record_id": "template-test", "fields": {
                "账号名": "Launch Test", "邮箱": "launch-test@example.invalid",
                "国家": "US", "语言": "en",
            }}, product, "FUNLAB", "Tom from FUNLAB Team", {}, 0,
        )["body"]
        raw_body = '<div><strong>DRY-RUN MODE</strong></div>' + generated_body
        with patch.dict(os.environ, {"EMAIL_DRY_RUN_TO": "frankiepan501@gmail.com"}, clear=False), \
             patch.object(preflight.feishu, "get_record", new=AsyncMock(return_value=product)), \
             patch.object(preflight.zoho, "send_email", new=AsyncMock(return_value="msg-template")) as send_mock, \
             patch.object(preflight.zoho, "_get_folder_ids", new=AsyncMock(return_value=("drafts", "sent"))), \
             patch.object(preflight.zoho, "get_message_content", new=AsyncMock(return_value=raw_body)), \
             patch.object(preflight.zoho, "list_sent_messages", new=AsyncMock(side_effect=[
                 {"messages": []}, {"messages": [sent_message]},
             ])):
            result = asyncio.run(preflight.send_and_validate(
                "product1", "", "FUNLAB", confirm="TEST_ONLY", run_key="run-template-1",
                template_version="kol-cold-template-v1",
            ))

        self.assertTrue(result["ok"])
        self.assertEqual("template:kol-cold-template-v1", result["draft_id"])
        self.assertEqual("kol-cold-template-v1", result["template_version"])
        self.assertEqual(0, result["production_draft_rows_written"])
        send_mock.assert_awaited_once()

    def setUp(self):
        preflight._RUN_LOCKS.clear()
        preflight._RUN_STATES.clear()

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
            "product_identity_rules": {
                "exact_name": "FUNLAB Dave the Diver Controller",
                "ip_markers": ["Dave the Diver"],
                "keyword_tokens": ["pro", "hall"],
            },
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

        generic = preflight.validate_raw_content(
            raw_subject="[DRY-RUN] " + expected["subject"],
            raw_body='<p>A great controller with the correct link https://example.com/p</p>',
            actual_to="test@example.com",
            actual_from="partner@fireflyfunlab.com",
            expected_to="test@example.com",
            expected=expected,
        )
        self.assertFalse(generic["checks"]["product_identity_present"])

    def test_product_identity_allows_one_remaining_distinctive_keyword(self):
        rules = {
            "exact_name": "FUNLAB Luminex Dave THE DIVER Edition - Switch 2 Pro Controller",
            "ip_markers": ["Dave the Diver"],
            "keyword_tokens": ["pro"],
        }
        self.assertTrue(preflight._product_identity_present(
            "Official Dave the Diver Pro Controller with charging dock.", rules
        ))
        self.assertFalse(preflight._product_identity_present(
            "Official Dave the Diver collectible with charging dock.", rules
        ))

    def test_product_identity_accepts_ip_plus_product_type_without_model_modifier(self):
        product_name = "FUNLAB Luminex Dave THE DIVER Edition - Switch 2 Pro Controller"
        rules = preflight._identity_rules({
            "适配IP": ["Dave the Diver"],
            "主关键词(英文)": "Switch 2 Pro Controller",
        }, product_name)
        self.assertTrue(preflight._product_identity_present(
            "We've made an officially licensed Dave the Diver controller for Switch 2.",
            rules,
        ))
        self.assertFalse(preflight._product_identity_present(
            "A collector item for Dave the Diver fans.", rules,
        ))

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
             patch.object(preflight.feishu, "get_record", new=AsyncMock(side_effect=[product, draft, product, draft])), \
             patch.object(preflight.zoho, "send_email", new=AsyncMock(return_value="msg1")) as send_mock, \
             patch.object(preflight.zoho, "_get_folder_ids", new=AsyncMock(return_value=("drafts", "sent"))), \
             patch.object(preflight.zoho, "get_message_content", new=AsyncMock(return_value=raw_body)), \
             patch.object(preflight.zoho, "list_sent_messages", new=AsyncMock(side_effect=[
                 {"messages": []}, {"messages": [sent_message]},
                 {"messages": [sent_message]}, {"messages": [sent_message]},
             ])), \
             patch.object(preflight.feishu, "create_record", new=AsyncMock()) as create_mock, \
             patch.object(preflight.feishu, "update_record", new=AsyncMock()) as update_mock:
            result = asyncio.run(preflight.send_and_validate(
                "product1", "draft1", "FUNLAB", confirm="TEST_ONLY", run_key="run-0001"
            ))
            reused = asyncio.run(preflight.send_and_validate(
                "product1", "draft1", "FUNLAB", confirm="TEST_ONLY", run_key="run-0001"
            ))

        self.assertTrue(result["ok"])
        self.assertTrue(result["validation"]["passed"])
        self.assertEqual(0, result["production_draft_rows_written"])
        self.assertTrue(reused["reused"])
        send_mock.assert_awaited_once()
        self.assertEqual("launch-preflight@example.invalid", send_mock.await_args.args[1])
        create_mock.assert_not_awaited()
        update_mock.assert_not_awaited()

    def test_concurrent_same_run_key_sends_once(self):
        product = {
            "record_id": "product1",
            "fields": {
                "产品英文名": "Piranha Plant Switch 2 Dock", "品牌": "POWKONG",
                "官网链接": {"link": "https://example.com/plant", "text": "Plant"},
            },
        }
        draft = {
            "record_id": "draft1",
            "fields": {
                "关联产品": {"link_record_ids": ["product1"]}, "邮件草稿来源": "cold",
                "发送邮箱": "partner@powkong.com", "邮件主题": "A dock for your setup",
                "邮件正文": '<p>Piranha Plant Switch 2 Dock is ready for review.</p>'
                            '<p><a href="https://example.com/plant">See it</a></p>',
            },
        }
        sent_message = {
            "messageId": "msg-concurrent",
            "subject": "[DRY-RUN→launch-preflight@example.invalid] "
                       "[Launch preflight:run-concurrent] A dock for your setup",
            "toAddress": "frankiepan501@gmail.com", "fromAddress": "partner@powkong.com",
        }
        sent_messages = []
        send_count = 0

        async def fake_get_record(table_id, record_id):
            return product if record_id == "product1" else draft

        async def fake_send(*args, **kwargs):
            nonlocal send_count
            send_count += 1
            await asyncio.sleep(0.01)
            sent_messages.append(sent_message)
            return "msg-concurrent"

        async def fake_list(*args, **kwargs):
            return {"messages": list(sent_messages)}

        async def run_both():
            return await asyncio.gather(*[
                preflight.send_and_validate(
                    "product1", "draft1", "POWKONG", confirm="TEST_ONLY", run_key="run-concurrent"
                ) for _ in range(2)
            ])

        with patch.dict(os.environ, {"EMAIL_DRY_RUN_TO": "frankiepan501@gmail.com"}, clear=False), \
             patch.object(preflight.feishu, "get_record", new=fake_get_record), \
             patch.object(preflight.zoho, "send_email", new=fake_send), \
             patch.object(preflight.zoho, "_get_folder_ids", new=AsyncMock(return_value=("drafts", "sent"))), \
             patch.object(preflight.zoho, "get_message_content", new=AsyncMock(return_value=draft["fields"]["邮件正文"])), \
             patch.object(preflight.zoho, "list_sent_messages", new=fake_list):
            results = asyncio.run(run_both())

        self.assertEqual(1, send_count)
        self.assertEqual([False, True], [x["reused"] for x in results])

    def test_timeout_retry_same_run_key_does_not_resend(self):
        product = {
            "record_id": "product1",
            "fields": {
                "产品英文名": "Piranha Plant Switch 2 Dock", "品牌": "POWKONG",
                "官网链接": {"link": "https://example.com/plant", "text": "Plant"},
            },
        }
        draft = {
            "record_id": "draft1",
            "fields": {
                "关联产品": {"link_record_ids": ["product1"]}, "邮件草稿来源": "cold",
                "发送邮箱": "partner@powkong.com", "邮件主题": "A dock for your setup",
                "邮件正文": '<p>Piranha Plant Switch 2 Dock is ready for review.</p>'
                            '<p><a href="https://example.com/plant">See it</a></p>',
            },
        }
        with patch.dict(os.environ, {"EMAIL_DRY_RUN_TO": "frankiepan501@gmail.com"}, clear=False), \
             patch.object(preflight.feishu, "get_record", new=AsyncMock(side_effect=[product, draft, product, draft])), \
             patch.object(preflight.zoho, "send_email", new=AsyncMock(return_value="msg-timeout")) as send_mock, \
             patch.object(preflight.zoho, "_get_folder_ids", new=AsyncMock(return_value=("drafts", "sent"))), \
             patch.object(preflight.zoho, "list_sent_messages", new=AsyncMock(return_value={"messages": []})), \
             patch.object(preflight, "_monotonic", side_effect=[0, 60, 70, 130]):
            for _ in range(2):
                with self.assertRaisesRegex(preflight.RawValidationError, "禁止自动"):
                    asyncio.run(preflight.send_and_validate(
                        "product1", "draft1", "POWKONG", confirm="TEST_ONLY", run_key="run-timeout"
                    ))
        send_mock.assert_awaited_once()

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

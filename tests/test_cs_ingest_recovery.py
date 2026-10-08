import unittest
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from app import cs_ingest, cs_kol_handoff


class CustomerServiceIngestRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_codex_owned_old_ticket_keeps_customer_supplement_without_reassigning(self):
        row = {"record_id": "rec_old", "fields": {
            "分配运营": "Codex代处理", "状态": "待客户补充",
            "客户标识": "buyer@example.com", "沟通历史摘要": "MAIL_THREAD_ID:t1",
            "补充信息次数": 1,
        }}
        msg = {"id": "new-mail", "id_prefix": "CSP", "body": "Amazon order 111-1234567-1234567",
               "received_ms": 1}
        with patch.object(cs_ingest, "_reroute_from_supplement", new=AsyncMock(return_value=(
                 "111-1234567-1234567", "亚马逊-US", "陈翔宇", "order_lookup"))), \
             patch.object(cs_ingest, "_operator_draft_after_supplement", new=AsyncMock()) as draft, \
             patch.object(cs_ingest, "_send_info_request", new=AsyncMock()) as send, \
             patch.object(cs_ingest.feishu, "api", new=AsyncMock()) as update:
            result = await cs_ingest._handle_waiting_info_reply(row, msg, resources=[])

        self.assertEqual("wait_reply_agent_owned", result["action"])
        draft.assert_not_awaited()
        send.assert_not_awaited()
        fields = update.await_args.args[2]["fields"]
        self.assertEqual("Codex代处理", fields["分配运营"])
        self.assertEqual("待客户补充", fields["状态"])
        self.assertIn("new-mail", fields["沟通历史摘要"])
        self.assertIn("111-1234567-1234567", fields["最近客户补充"])

    async def test_waiting_info_customer_field_accepts_bitable_rich_text(self):
        customer = [{"text": "buyer@example.com"}]
        self.assertEqual("buyer@example.com", cs_ingest._valid_customer_email(customer))
        with patch.object(cs_ingest, "CS_INFO_REQUEST_DRY_RUN_TO", ""), \
             patch.object(cs_ingest, "CS_INFO_REQUEST_LIVE", False), \
             patch.object(cs_ingest, "_zoho_send_reply", new=AsyncMock()) as send:
            mode, message_id = await cs_ingest._send_info_request(
                {"id_prefix": "CSP", "subj": "Order question"},
                {"客户标识": customer}, "Please share the order number.",
            )
        self.assertEqual(("disabled", ""), (mode, message_id))
        send.assert_not_called()

    async def test_message_failure_reports_replay_id_without_mail_content(self):
        message = {
            "id": "failed-provider-id", "id_prefix": "CSP",
            "frm": "buyer@example.com", "body": "private customer message",
            "received_ms": 1,
        }
        with patch.object(cs_ingest, "_fetch_powkong", new=AsyncMock(return_value=[message])), \
             patch.object(cs_ingest, "_existing_thread_ids", new=AsyncMock(return_value=set())), \
             patch.object(cs_ingest, "_waiting_info_tickets", new=AsyncMock(return_value=[])), \
             patch.object(cs_ingest.cs_resources, "active_resources", new=AsyncMock(return_value=[])), \
             patch.object(cs_ingest, "_classify", new=AsyncMock(side_effect=ValueError("private customer message"))):
            result = await cs_ingest.run(source="powkong", limit=1, dry_run=True)

        self.assertEqual(1, result["errors"])
        self.assertEqual(1, len(result["message_failures"]))
        failure = result["message_failures"][0]
        self.assertEqual({
            "source": "CSP", "message_id": "failed-provider-id",
            "stage": "classify", "error_type": "ValueError",
        }, {key: failure[key] for key in ("source", "message_id", "stage", "error_type")})
        self.assertRegex(failure["error_location"], r"^[\w.]+:[\w]+:\d+$")
        self.assertNotIn("private customer message", str(result))

    async def test_zoho_message_retains_reply_to_address(self):
        meta = {
            "messageId": "m1",
            "fromAddress": "mailer@shopify.com",
            "replyToAddress": "buyer@example.com",
            "subject": "New customer message",
            "receivedTime": "1",
        }
        with patch.object(cs_ingest, "_zget", new=AsyncMock(return_value={
                "data": {"content": "Email: buyer@example.com"},
             })), \
             patch.object(cs_ingest, "_fetch_zoho_attachments", new=AsyncMock(return_value=[])):
            msg = await cs_ingest._zoho_message(
                "token", "account", "inbox", meta, "CSP", "POWKONG"
            )

        self.assertEqual("buyer@example.com", msg["reply_to"])

    async def test_shopify_email_backfill_updates_only_unique_customer_address(self):
        records = [{
            "record_id": "rec_unique",
            "fields": {
                "客户标识": "mailer@shopify.com",
                "原文": "New customer message\n\nE-Mail: buyer@example.com Comment: Help.",
                "沟通历史摘要": "首封已入库。",
            },
        }, {
            "record_id": "rec_ambiguous",
            "fields": {
                "客户标识": "mailer@shopify.com",
                "原文": "E-Mail: first@example.com Alternate Email: second@example.com",
            },
        }, {
            "record_id": "rec_normal",
            "fields": {
                "客户标识": "normal@example.com",
                "原文": "Email: normal@example.com",
            },
        }]
        writes = []

        async def api(method, path, body=None, which="notify"):
            if method == "GET":
                if path.endswith("/records/rec_unique"):
                    return {"data": {"record": {"fields": {
                        **records[0]["fields"],
                        "客户标识": "buyer@example.com",
                    }}}}
                return {"data": {"items": records, "has_more": False}}
            if method == "PUT":
                writes.append((path, body))
                return {"code": 0}
            raise AssertionError((method, path))

        with patch.object(cs_ingest.feishu, "api", new=api):
            result = await cs_ingest.backfill_shopify_customer_emails(dry_run=True)

        self.assertEqual(3, result["scanned"])
        self.assertEqual(1, result["updated"])
        self.assertEqual(0, result["verified"])
        self.assertEqual(1, result["ambiguous"])
        self.assertEqual(["rec_unique"], result["updated_record_ids"])
        self.assertEqual(0, len(writes))

        reads = 0

        async def single_api(method, path, body=None, which="notify"):
            nonlocal reads
            if method == "GET":
                reads += 1
                fields = dict(records[0]["fields"])
                fields["沟通历史摘要"] = "x" * 4999
                if reads > 1:
                    fields["客户标识"] = "buyer@example.com"
                    fields["沟通历史摘要"] = (
                        "系统纠偏: 客户邮箱已从 Shopify平台代发地址改为表单内唯一客户邮箱。\n"
                        + fields["沟通历史摘要"]
                    )[:5000]
                return {"data": {"record": {"fields": fields}}}
            if method == "PUT":
                writes.append((path, body))
                return {"code": 0}
            raise AssertionError((method, path))

        with patch.object(cs_ingest.feishu, "api", new=single_api):
            committed = await cs_ingest.backfill_shopify_customer_emails(
                record_id="rec_unique", dry_run=False,
            )

        self.assertEqual(1, committed["updated"])
        self.assertEqual(1, committed["verified"])
        self.assertEqual(1, len(writes))
        self.assertEqual("buyer@example.com", writes[0][1]["fields"]["客户标识"])
        self.assertIn("Shopify平台代发地址", writes[0][1]["fields"]["沟通历史摘要"])

        already_corrected = {
            "data": {"record": {"fields": {
                **records[0]["fields"],
                "客户标识": "buyer@example.com",
                "沟通历史摘要": "系统纠偏: 客户邮箱已从 Shopify平台代发地址改为表单内唯一客户邮箱。",
            }}}
        }
        with patch.object(cs_ingest.feishu, "api", new=AsyncMock(
                return_value=already_corrected)) as replay_api:
            replayed = await cs_ingest.backfill_shopify_customer_emails(
                record_id="rec_unique", dry_run=False,
            )
        self.assertEqual(1, replayed["verified"])
        self.assertEqual(["rec_unique"], replayed["verified_record_ids"])
        self.assertEqual(1, replay_api.await_count)

    async def test_shopify_backfill_encodes_page_token_and_commit_requires_one_record(self):
        paths = []

        async def api(method, path, body=None, which="notify"):
            paths.append(path)
            if len(paths) == 1:
                return {"data": {"items": [], "has_more": True,
                                  "page_token": "a+b/c="}}
            return {"data": {"items": [], "has_more": False}}

        with patch.object(cs_ingest.feishu, "api", new=api):
            await cs_ingest.backfill_shopify_customer_emails(dry_run=True)
        self.assertIn("page_token=a%2Bb%2Fc%3D", paths[1])
        with self.assertRaisesRegex(ValueError, "record_id"):
            await cs_ingest.backfill_shopify_customer_emails(dry_run=False)

    async def test_customer_email_backfill_endpoint_requires_confirm_and_refreshes_cards(self):
        from app import main as app_main

        result = {
            "ok": True, "dry_run": False, "updated": 1, "verified": 1,
            "updated_record_ids": ["rec_unique"],
            "verified_record_ids": ["rec_unique"], "ambiguous": 0,
        }
        with patch.object(app_main, "_check_auth"), \
             patch.object(app_main.cs_ingest, "backfill_shopify_customer_emails",
                          new=AsyncMock(return_value=result)) as backfill, \
             patch.object(app_main.cs_dispatch, "refresh_ticket_card",
                          new=AsyncMock(return_value={
                              "ok": True, "record_id": "rec_unique",
                              "cards_updated": 1, "cards_verified": 1,
                          })) as refresh:
            response = await app_main.run_cs_customer_email_backfill(
                authorization="Bearer test", record_id="rec_unique",
                dry_run=False, confirm=True,
                update_cards=True, run_id="fix-20261007",
            )

        self.assertTrue(response["ok"])
        self.assertEqual(1, response["cards_updated"])
        self.assertEqual(1, response["cards_verified"])
        backfill.assert_awaited_once()
        refresh.assert_awaited_once_with("rec_unique")

    async def test_firefly_paginates_past_kol_mail_before_applying_limit(self):
        kol_page = [{"messageId": f"k{i}", "toAddress": "partner@fireflyfunlab.com",
                     "fromAddress": "creator@example.com", "subject": "Collaboration"}
                    for i in range(200)]
        customer = {"messageId": "customer-1", "toAddress": "support@fireflyfunlab.com",
                    "fromAddress": "buyer@example.com", "subject": "Order help",
                    "receivedTime": "200"}
        with patch.object(cs_ingest, "_zget", new=AsyncMock(side_effect=[
                {"data": kol_page}, {"data": [customer]},
             ])) as zget, \
             patch.object(cs_ingest, "_zoho_message", new=AsyncMock(return_value={
                 "id": "customer-1", "id_prefix": "CSZ", "received_ms": 200,
             })):
            rows = await cs_ingest._fetch_zoho(
                1, tok="token", account_id="account", folder_id="inbox",
                id_prefix="CSZ", brand="FUNLAB",
                predicate=cs_ingest._firefly_customer_message, scan_limit=400,
            )

        self.assertEqual(["customer-1"], [x["id"] for x in rows])
        self.assertEqual(2, zget.await_count)

    async def test_known_thread_does_not_hide_new_message_in_same_thread(self):
        meta = {"messageId": "m2", "threadId": "t1",
                "toAddress": "support@powkong.com", "receivedTime": "200"}
        with patch.object(cs_ingest, "_zget", new=AsyncMock(return_value={"data": [meta]})), \
             patch.object(cs_ingest, "_zoho_message", new=AsyncMock(return_value={
                 "id": "m2", "id_prefix": "CSP", "received_ms": 200,
                 "mail_thread_id": "t1",
             })) as fetch_message:
            rows = await cs_ingest._fetch_zoho(
                1, tok="token", account_id="account", folder_id="inbox",
                id_prefix="CSP", brand="POWKONG", scan_limit=200,
                known_keys={"CSP:thread:t1"},
            )

        self.assertEqual(["m2"], [x["id"] for x in rows])
        fetch_message.assert_awaited_once()

    async def test_scan_limit_exhaustion_is_reported(self):
        rows = [{"messageId": f"known-{i}", "receivedTime": str(1000 - i)}
                for i in range(200)]
        known = {f"CSP:msg:known-{i}" for i in range(200)}
        with patch.object(cs_ingest, "_zget", new=AsyncMock(return_value={"data": rows})):
            result = await cs_ingest._fetch_zoho(
                1, tok="token", account_id="account", folder_id="inbox",
                id_prefix="CSP", brand="POWKONG", scan_limit=200,
                known_keys=known,
            )

        self.assertEqual([], result)
        self.assertTrue(cs_ingest._SOURCE_SCAN_EXHAUSTED["CSP"])
        cs_ingest._SOURCE_SCAN_EXHAUSTED.pop("CSP", None)

    def test_unknown_zoho_profile_fails_closed(self):
        with self.assertRaises(ValueError):
            cs_ingest._zoho_profile("firelfy")

    def test_waiting_info_sender_fallback_does_not_cross_mailbox_brand(self):
        msg = {"id": "firefly-message", "id_prefix": "CSZ", "brand_default": "FUNLAB",
               "frm": "same-customer@example.com", "in_reply_to": "", "references": ""}
        waiting = [{"record_id": "rec_powkong", "fields": {
            "工单ID": "CSP-powkong-message", "品牌": "POWKONG",
            "客户标识": "same-customer@example.com", "线程ID": "powkong-message",
        }}]

        self.assertIsNone(cs_ingest._match_waiting_info_ticket(msg, waiting))

    def test_waiting_info_matches_provider_thread_even_if_sender_changes(self):
        msg = {"id": "m2", "id_prefix": "CSZ", "brand_default": "FUNLAB",
               "mail_thread_id": "thread-1077", "frm": "mailer@shopify.com",
               "in_reply_to": "", "references": ""}
        waiting = [{"record_id": "rec_firefly", "fields": {
            "工单ID": "CSZ-m1", "品牌": "FUNLAB",
            "客户标识": "buyer@example.com", "线程ID": "m1",
            "沟通历史摘要": "首封问题\nMAIL_THREAD_ID:thread-1077",
        }}]

        self.assertEqual("rec_firefly",
                         cs_ingest._match_waiting_info_ticket(msg, waiting)["record_id"])

    def test_firefly_filter_accepts_support_and_shopify_contact_but_rejects_partner_mail(self):
        self.assertTrue(cs_ingest._firefly_customer_message({
            "toAddress": "support@fireflyfunlab.com", "fromAddress": "buyer@example.com",
            "subject": "Order FL1077",
        }))
        self.assertTrue(cs_ingest._firefly_customer_message({
            "toAddress": "partner@fireflyfunlab.com", "fromAddress": "mailer@shopify.com",
            "subject": "New customer message on October 7",
        }))
        self.assertFalse(cs_ingest._firefly_customer_message({
            "toAddress": "partner@fireflyfunlab.com", "fromAddress": "creator@example.com",
            "subject": "Collaboration proposal",
        }))

    def test_independent_site_ticket_routes_to_job_title_not_person(self):
        message = {
            "id": "<role-route@example.com>",
            "id_prefix": "CSF",
            "frm": "customer@example.com",
            "subj": "Product question",
            "body": "Can you help me?",
            "channel": "邮箱",
            "brand_default": "FUNLAB",
            "received_ms": 1700000000000,
            "attachments": [],
        }
        classification = {
            "is_cs": True,
            "platform": "独立站",
            "summary": "Product question",
        }

        fields = cs_ingest._to_fields(message, classification, resources=[])

        self.assertEqual("独立站", fields["销售平台"])
        self.assertEqual("独立站运营专员", fields["分配运营"])
        self.assertNotEqual("张佳烨", fields["分配运营"])

    def test_kol_collaboration_gate_overrides_customer_classification(self):
        message = {
            "frm": "creator@example.com",
            "subj": "Content creator collaboration",
            "body": "I am a content creator and would like to collaborate to promote your products.",
        }
        mistaken = {
            "is_cs": True,
            "platform": "独立站",
            "summary": "Creator asks about cooperation",
            "draft_reply": "A customer-service reply that must not be used.",
        }

        corrected = cs_ingest._apply_kol_collaboration_gate(message, mistaken)

        self.assertFalse(corrected["is_cs"])
        self.assertEqual("KOL红人", corrected["route"])
        self.assertEqual("", corrected["draft_reply"])

    def test_kol_collaboration_gate_catches_affiliate_link_relationship(self):
        message = {
            "frm": "partner@example.com",
            "subj": "Affiliate link stopped working",
            "body": "My affiliate link no longer works. Are we still working together?",
        }

        corrected = cs_ingest._apply_kol_collaboration_gate(
            message, {"is_cs": True, "summary": "Affiliate link issue"},
        )

        self.assertFalse(corrected["is_cs"])
        self.assertEqual("KOL红人", corrected["route"])

    def test_kol_collaboration_gate_catches_still_affiliated_relationship(self):
        message = {
            "frm": "partner@example.com",
            "subj": "Affiliate relationship",
            "body": "I was wondering if I am still affiliated with you. My link does not work.",
        }

        corrected = cs_ingest._apply_kol_collaboration_gate(
            message, {"is_cs": True, "summary": "Affiliate link issue"},
        )

        self.assertFalse(corrected["is_cs"])
        self.assertEqual("KOL红人", corrected["route"])

    def test_kol_gate_does_not_capture_real_customer_mentioning_influencer(self):
        message = {
            "frm": "buyer@example.com",
            "subj": "Three month delivery delay",
            "body": "I am not an influencer. I paid for my order and need delivery status now.",
        }

        corrected = cs_ingest._apply_kol_collaboration_gate(
            message, {"is_cs": True, "summary": "Order not delivered"},
        )

        self.assertTrue(corrected["is_cs"])
        self.assertNotEqual("KOL红人", corrected.get("route"))

    def test_kol_gate_does_not_capture_customer_influenced_by_reviewer(self):
        message = {
            "frm": "buyer@example.com",
            "subj": "Defective controller",
            "body": ("I bought this controller after a reviewer promoted it, but it is defective "
                     "and I need a replacement."),
        }

        corrected = cs_ingest._apply_kol_collaboration_gate(
            message, {"is_cs": True, "summary": "Defective product"},
        )

        self.assertTrue(corrected["is_cs"])

    def test_kol_gate_does_not_capture_customer_buying_through_affiliate_link(self):
        message = {
            "frm": "buyer@example.com",
            "subj": "Order not delivered",
            "body": ("I purchased through an affiliate link. Order PK123 has not arrived "
                     "and I need a refund."),
        }

        corrected = cs_ingest._apply_kol_collaboration_gate(
            message, {"is_cs": True, "summary": "Order not delivered"},
        )

        self.assertTrue(corrected["is_cs"])

    def test_kol_gate_does_not_capture_chinese_customer_using_creator_link(self):
        message = {
            "frm": "buyer@example.com",
            "subj": "订单未收到",
            "body": "我是通过达人合作推广链接下单的消费者，订单一直没收到，请退款。",
        }

        corrected = cs_ingest._apply_kol_collaboration_gate(
            message, {"is_cs": True, "summary": "订单未收到"},
        )

        self.assertTrue(corrected["is_cs"])

    def test_kol_gate_accepts_first_person_brand_ambassador_relationship(self):
        message = {
            "frm": "partner@example.com",
            "subj": "Brand ambassador partnership",
            "body": "I am a brand ambassador and would like to continue our partnership.",
        }

        corrected = cs_ingest._apply_kol_collaboration_gate(
            message, {"is_cs": True, "summary": "Partnership"},
        )

        self.assertFalse(corrected["is_cs"])
        self.assertEqual("KOL红人", corrected["route"])

    def test_platform_relay_does_not_use_unlabeled_third_party_email(self):
        message = {
            "frm": "no-reply@mail.myshopline.com",
            "subj": "Message from client",
            "body": "Please contact my installer at installer@example.com. My own email is missing.",
        }

        self.assertEqual("", cs_ingest._message_customer_email(message))

    def test_shopline_relay_uses_labeled_contact_email(self):
        message = {
            "frm": "no-reply@mail.myshopline.com",
            "subj": "New message from your online store",
            "body": "E-mail: lary01tech@gmail.com\nComment: I am a creator seeking collaboration.",
        }

        self.assertTrue(cs_ingest._is_platform_or_system_email(message["frm"]))
        self.assertEqual("lary01tech@gmail.com", cs_ingest._message_customer_email(message))

    def test_non_customer_kol_ticket_never_gets_resource_reply(self):
        message = {
            "id": "m-kol", "id_prefix": "CSZ", "frm": "creator@example.com",
            "subj": "Creator collaboration", "body": "I am a creator seeking collaboration.",
            "channel": "邮箱", "brand_default": "FUNLAB", "received_ms": 1,
            "attachments": [],
        }
        classification = {
            "is_cs": False, "route": "KOL红人", "summary": "Creator collaboration",
            "draft_reply": "", "confidence": "必须人工",
        }

        with patch.object(cs_ingest.cs_resources, "build_resource_reply",
                          return_value="A support reply that must not be generated") as build:
            fields = cs_ingest._to_fields(message, classification, resources=[])

        self.assertEqual("", fields["AI草稿"])
        build.assert_not_called()

    async def test_kol_handoff_persists_pending_state_before_sending_card(self):
        message = {
            "id": "m-kol", "id_prefix": "CSP", "frm": "creator@example.com",
            "subj": "Creator collaboration",
            "body": "I am a content creator and would like to collaborate to promote products.",
            "channel": "邮箱", "brand_default": "FUNLAB", "received_ms": 1,
            "attachments": [],
        }
        api = AsyncMock(return_value={"data": {"record": {"record_id": "rec_kol_ticket"}}})
        with patch.object(cs_ingest, "_fetch_powkong", new=AsyncMock(return_value=[message])), \
             patch.object(cs_ingest, "_existing_thread_ids", new=AsyncMock(return_value=set())), \
             patch.object(cs_ingest, "_waiting_info_tickets", new=AsyncMock(return_value=[])), \
             patch.object(cs_ingest.cs_resources, "active_resources", new=AsyncMock(return_value=[])), \
             patch.object(cs_ingest, "_classify", new=AsyncMock(return_value={
                 "is_cs": True, "platform": "独立站", "summary": "Creator collaboration",
             })), \
             patch.object(cs_kol_handoff, "retry_pending_handoffs",
                          new=AsyncMock(return_value={"ok": True, "sent": 0})), \
             patch.object(cs_kol_handoff, "lookup_contact",
                          new=AsyncMock(return_value=(None, None))), \
             patch.object(cs_kol_handoff, "ensure_kol_workflow",
                          new=AsyncMock(return_value={
                              "ok": True, "message_ids": ["om_kol"],
                              "followup_record_id": "rec_fu",
                              "draft_record_id": "",
                          })), \
             patch.object(cs_kol_handoff, "record_workflow_marker",
                          new=AsyncMock(return_value={"marker": "done"})), \
             patch.object(cs_ingest.feishu, "api", new=api):
            result = await cs_ingest.run(source="powkong", limit=1, dry_run=False)

        self.assertEqual(1, result["new"])
        posted = api.await_args.args[2]["fields"]
        self.assertTrue(posted["沟通历史摘要"].startswith(
            cs_kol_handoff.PENDING_MARKER
        ))

    async def test_funlab_missing_credentials_is_reported_as_source_error(self):
        with patch.object(cs_ingest, "NE_USER", ""), \
             patch.object(cs_ingest, "NE_CODE", ""), \
             patch.object(cs_ingest, "_existing_thread_ids", new=AsyncMock(return_value=set())), \
             patch.object(cs_ingest, "_waiting_info_tickets", new=AsyncMock(return_value=[])), \
             patch.object(cs_ingest.cs_resources, "active_resources", new=AsyncMock(return_value=[])):
            result = await cs_ingest.run(source="funlab", limit=1, dry_run=True)

        self.assertEqual(0, result["fetched"])
        self.assertIn("funlab", result["source_errors"])
        self.assertIn("NETEASE_FUNLAB_CS_USER", result["source_errors"]["funlab"])

    async def test_powkong_missing_credentials_is_reported_as_source_error(self):
        with patch.object(cs_ingest, "ZCID", ""), \
             patch.object(cs_ingest, "ZSEC", ""), \
             patch.object(cs_ingest, "ZRT", ""), \
             patch.object(cs_ingest, "ZACC", ""), \
             patch.object(cs_ingest, "_existing_thread_ids", new=AsyncMock(return_value=set())), \
             patch.object(cs_ingest, "_waiting_info_tickets", new=AsyncMock(return_value=[])), \
             patch.object(cs_ingest.cs_resources, "active_resources", new=AsyncMock(return_value=[])):
            result = await cs_ingest.run(source="powkong", limit=1, dry_run=True)

        self.assertEqual(0, result["fetched"])
        self.assertIn("powkong", result["source_errors"])
        self.assertIn("ZOHO_POWKONG_CS_CLIENT_ID", result["source_errors"]["powkong"])

    async def test_firefly_zoho_missing_credentials_is_reported_as_source_error(self):
        with patch.object(cs_ingest, "ZFCID", ""), \
             patch.object(cs_ingest, "ZFSEC", ""), \
             patch.object(cs_ingest, "ZFRT", ""), \
             patch.object(cs_ingest, "ZFACC", ""), \
             patch.object(cs_ingest, "_existing_thread_ids", new=AsyncMock(return_value=set())), \
             patch.object(cs_ingest, "_waiting_info_tickets", new=AsyncMock(return_value=[])), \
             patch.object(cs_ingest.cs_resources, "active_resources", new=AsyncMock(return_value=[])):
            result = await cs_ingest.run(source="firefly", limit=1, dry_run=True)

        self.assertEqual(0, result["fetched"])
        self.assertIn("firefly", result["source_errors"])
        self.assertIn("ZOHO_FUNLAB_CLIENT_ID", result["source_errors"]["firefly"])

    async def test_mailbox_watermark_regression_is_reported_as_source_error(self):
        first = {"id": "m1", "id_prefix": "CSP", "received_ms": 200,
                 "mail_thread_id": "t1"}
        second = {"id": "m2", "id_prefix": "CSP", "received_ms": 100,
                  "mail_thread_id": "t2"}
        cs_ingest._SOURCE_WATERMARKS.pop("powkong", None)
        cs_ingest._SOURCE_HEADS.pop("CSP", None)
        cs_ingest._SOURCE_FILTERED_HEADS.pop("CSP", None)
        cs_ingest._SOURCE_WATERMARK_REGRESSIONS.pop("powkong", None)
        common = [
            patch.object(cs_ingest, "_existing_thread_ids", new=AsyncMock(
                side_effect=[{"m1"}, {"m2"}, {"m2"}])),
            patch.object(cs_ingest, "_waiting_info_tickets", new=AsyncMock(return_value=[])),
            patch.object(cs_ingest.cs_resources, "active_resources", new=AsyncMock(return_value=[])),
            patch.object(cs_ingest, "_fetch_powkong", new=AsyncMock(
                side_effect=[[first], [second], [second]])),
        ]
        with common[0], common[1], common[2], common[3]:
            ok = await cs_ingest.run(source="powkong", limit=1, dry_run=True)
            diagnostic = await cs_ingest.run(source="powkong", limit=1, dry_run=True)
            regressed = await cs_ingest.run(source="powkong", limit=1, dry_run=True)

        self.assertEqual("ok", ok["source_health"]["powkong"]["status"])
        self.assertEqual("diagnostic", diagnostic["source_health"]["powkong"]["status"])
        self.assertNotIn("powkong", diagnostic["source_errors"])
        self.assertEqual("anomaly", regressed["source_health"]["powkong"]["status"])
        self.assertIn("watermark regressed twice", regressed["source_errors"]["powkong"])
        cs_ingest._SOURCE_WATERMARKS.pop("powkong", None)
        cs_ingest._SOURCE_WATERMARK_REGRESSIONS.pop("powkong", None)

    async def test_zero_filtered_watermark_is_confirmed_as_regression(self):
        cs_ingest._SOURCE_WATERMARKS["firefly"] = 500
        cs_ingest._SOURCE_FILTERED_HEADS["CSZ"] = 0
        cs_ingest._SOURCE_HEADS["CSZ"] = 800
        cs_ingest._SOURCE_SCAN_EXHAUSTED["CSZ"] = False
        cs_ingest._SOURCE_WATERMARK_REGRESSIONS.pop("firefly", None)
        with patch.object(cs_ingest, "_existing_thread_ids", new=AsyncMock(return_value=set())), \
             patch.object(cs_ingest, "_waiting_info_tickets", new=AsyncMock(return_value=[])), \
             patch.object(cs_ingest.cs_resources, "active_resources", new=AsyncMock(return_value=[])), \
             patch.object(cs_ingest, "_fetch_firefly", new=AsyncMock(return_value=[])):
            diagnostic = await cs_ingest.run(source="firefly", limit=1, dry_run=True)
            anomaly = await cs_ingest.run(source="firefly", limit=1, dry_run=True)

        self.assertEqual("diagnostic", diagnostic["source_health"]["firefly"]["status"])
        self.assertEqual("anomaly", anomaly["source_health"]["firefly"]["status"])
        self.assertIn("watermark regressed twice", anomaly["source_errors"]["firefly"])
        for bucket, key in ((cs_ingest._SOURCE_WATERMARKS, "firefly"),
                            (cs_ingest._SOURCE_WATERMARK_REGRESSIONS, "firefly"),
                            (cs_ingest._SOURCE_FILTERED_HEADS, "CSZ"),
                            (cs_ingest._SOURCE_HEADS, "CSZ"),
                            (cs_ingest._SOURCE_SCAN_EXHAUSTED, "CSZ")):
            bucket.pop(key, None)

    async def test_scan_exhaustion_below_known_watermark_stays_quiet(self):
        cs_ingest._SOURCE_WATERMARKS["powkong"] = 500
        cs_ingest._SOURCE_FILTERED_HEADS["CSP"] = 500
        cs_ingest._SOURCE_HEADS["CSP"] = 800
        cs_ingest._SOURCE_SCAN_EXHAUSTED["CSP"] = True
        cs_ingest._SOURCE_SCAN_OLDEST["CSP"] = 400
        with patch.object(cs_ingest, "_existing_thread_ids", new=AsyncMock(return_value=set())), \
             patch.object(cs_ingest, "_waiting_info_tickets", new=AsyncMock(return_value=[])), \
             patch.object(cs_ingest.cs_resources, "active_resources", new=AsyncMock(return_value=[])), \
             patch.object(cs_ingest, "_fetch_powkong", new=AsyncMock(return_value=[])):
            result = await cs_ingest.run(source="powkong", limit=20, dry_run=True)

        self.assertEqual("ok", result["source_health"]["powkong"]["status"])
        self.assertNotIn("powkong", result["source_errors"])
        for bucket, key in ((cs_ingest._SOURCE_WATERMARKS, "powkong"),
                            (cs_ingest._SOURCE_FILTERED_HEADS, "CSP"),
                            (cs_ingest._SOURCE_HEADS, "CSP"),
                            (cs_ingest._SOURCE_SCAN_EXHAUSTED, "CSP"),
                            (cs_ingest._SOURCE_SCAN_OLDEST, "CSP")):
            bucket.pop(key, None)

    async def test_new_reply_in_known_thread_reaches_waiting_info_handler(self):
        message = {
            "id": "m2", "id_prefix": "CSP", "mail_thread_id": "t1",
            "frm": "buyer@example.com", "subj": "Re: order details",
            "body": "My order is 123-1234567-1234567 on Amazon US.",
            "channel": "邮箱", "brand_default": "POWKONG", "received_ms": 200,
            "attachments": [],
        }
        waiting = [{"record_id": "rec_wait", "fields": {
            "工单ID": "CSP-m1", "品牌": "POWKONG",
            "客户标识": "buyer@example.com", "线程ID": "m1",
            "沟通历史摘要": "MAIL_THREAD_ID:t1",
        }}]
        with patch.object(cs_ingest, "_fetch_powkong", new=AsyncMock(return_value=[message])), \
             patch.object(cs_ingest, "_existing_thread_ids", new=AsyncMock(return_value={
                 "CSP:msg:m1", "CSP:thread:t1",
             })), \
             patch.object(cs_ingest, "_waiting_info_tickets", new=AsyncMock(return_value=waiting)), \
             patch.object(cs_ingest.cs_resources, "active_resources", new=AsyncMock(return_value=[])), \
             patch.object(cs_ingest, "_handle_waiting_info_reply", new=AsyncMock(return_value={
                 "action": "wait_reply_rerouted", "record_id": "rec_wait",
             })) as handle, \
             patch.object(cs_ingest, "_classify", new=AsyncMock()) as classify:
            result = await cs_ingest.run(source="powkong", limit=1, dry_run=True)

        handle.assert_awaited_once()
        classify.assert_not_awaited()
        self.assertEqual(1, result["new"])
        self.assertEqual(0, result["skipped"])

    async def test_new_reply_in_known_thread_creates_new_action_when_not_waiting(self):
        message = {
            "id": "m2", "id_prefix": "CSP", "mail_thread_id": "t1",
            "frm": "buyer@example.com", "subj": "Still not solved",
            "body": "The problem is still not solved.", "channel": "邮箱",
            "brand_default": "POWKONG", "received_ms": 200, "attachments": [],
        }
        fields = {"工单ID": "CSP-m2", "品牌": "POWKONG", "销售平台": "独立站",
                  "分配运营": "独立站运营专员", "状态": "待派",
                  "客诉摘要": "Still not solved"}
        with patch.object(cs_ingest, "_fetch_powkong", new=AsyncMock(return_value=[message])), \
             patch.object(cs_ingest, "_existing_thread_ids", new=AsyncMock(return_value={
                 "CSP:msg:m1", "CSP:thread:t1",
             })), \
             patch.object(cs_ingest, "_waiting_info_tickets", new=AsyncMock(return_value=[])), \
             patch.object(cs_ingest.cs_resources, "active_resources", new=AsyncMock(return_value=[])), \
             patch.object(cs_ingest, "_classify", new=AsyncMock(return_value={"is_cs": True})) as classify, \
             patch.object(cs_ingest, "_to_fields", return_value=fields):
            result = await cs_ingest.run(source="powkong", limit=1, dry_run=True)

        classify.assert_awaited_once()
        self.assertEqual(1, result["new"])
        self.assertEqual(0, result["skipped"])

    async def test_same_thread_batch_records_all_message_ids_and_does_not_repeat(self):
        old = {
            "id": "old", "id_prefix": "CSP", "mail_thread_id": "t1",
            "frm": "buyer@example.com", "subj": "Initial problem", "body": "first body",
            "channel": "邮箱", "brand_default": "POWKONG", "received_ms": 100,
            "attachments": [],
        }
        new = {
            "id": "new", "id_prefix": "CSP", "mail_thread_id": "t1",
            "frm": "buyer@example.com", "subj": "Still not solved", "body": "second body",
            "channel": "邮箱", "brand_default": "POWKONG", "received_ms": 200,
            "attachments": [],
        }
        api = AsyncMock(return_value={"data": {"record": {"record_id": "rec_merged"}}})
        with patch.object(cs_ingest, "_fetch_powkong", new=AsyncMock(
                 side_effect=[[new, old], [new, old]])), \
             patch.object(cs_ingest, "_existing_thread_ids", new=AsyncMock(side_effect=[
                 set(), {"CSP:msg:new", "CSP:msg:old"},
             ])), \
             patch.object(cs_ingest, "_waiting_info_tickets", new=AsyncMock(return_value=[])), \
             patch.object(cs_ingest.cs_resources, "active_resources", new=AsyncMock(return_value=[])), \
             patch.object(cs_ingest, "_classify", new=AsyncMock(return_value={
                 "is_cs": True, "platform": "独立站", "summary": "Still not solved",
             })), \
             patch.object(cs_kol_handoff, "retry_pending_handoffs",
                          new=AsyncMock(return_value={"ok": True, "sent": 0})), \
             patch.object(cs_ingest.feishu, "api", new=api):
            first = await cs_ingest.run(source="powkong", limit=2, dry_run=False)
            second = await cs_ingest.run(source="powkong", limit=2, dry_run=False)

        self.assertEqual(1, first["new"])
        self.assertEqual(0, second["new"])
        self.assertEqual(1, second["skipped"])
        self.assertEqual(1, api.await_count)
        posted_fields = api.await_args.args[2]["fields"]
        self.assertIn("first body", posted_fields["原文"])
        self.assertIn("second body", posted_fields["原文"])
        self.assertIn("SOURCE_MESSAGE_ID:old", posted_fields["沟通历史摘要"])

    async def test_single_funlab_message_can_be_replayed_in_dry_run(self):
        message = {
            "id": "<missing-message@example.com>",
            "id_prefix": "CSF",
            "frm": "customer@example.com",
            "subj": "Controller issue",
            "body": "The controller does not connect.",
            "channel": "邮箱",
            "brand_default": "FUNLAB",
            "attachments": [],
        }
        fields = {
            "品牌": "FUNLAB",
            "销售平台": "独立站",
            "分配运营": "张佳烨",
            "状态": "待派",
            "客诉摘要": "Controller does not connect",
        }
        with patch.object(cs_ingest, "_fetch_funlab_one", new=AsyncMock(return_value=message)) as fetch_one, \
             patch.object(cs_ingest, "_existing_thread_ids", new=AsyncMock(return_value=set())), \
             patch.object(cs_ingest, "_waiting_info_tickets", new=AsyncMock(return_value=[])), \
             patch.object(cs_ingest.cs_resources, "active_resources", new=AsyncMock(return_value=[])), \
             patch.object(cs_ingest, "_classify", new=AsyncMock(return_value={"is_cs": True})), \
             patch.object(cs_ingest, "_to_fields", return_value=fields), \
             patch.object(cs_ingest.feishu, "api", new=AsyncMock()) as api:
            result = await cs_ingest.run(
                source="funlab",
                limit=1,
                dry_run=True,
                message_id="<missing-message@example.com>",
                scan_limit=750,
            )

        fetch_one.assert_awaited_once_with("<missing-message@example.com>", scan_limit=750)
        api.assert_not_awaited()
        self.assertTrue(result["replay_mode"])
        self.assertEqual(1, result["fetched"])
        self.assertEqual(1, result["new"])

    async def test_single_firefly_message_can_be_replayed_in_dry_run(self):
        message = {
            "id": "1791344517645162600",
            "id_prefix": "CSZ",
            "frm": "customer@example.com",
            "subj": "Re: Order FL1077",
            "body": "I agree to the refund.",
            "channel": "邮箱",
            "brand_default": "FUNLAB",
            "attachments": [],
        }
        fields = {
            "品牌": "FUNLAB",
            "销售平台": "独立站",
            "分配运营": "独立站运营专员",
            "状态": "待派",
            "客诉摘要": "Customer approved refund",
        }
        with patch.object(cs_ingest, "_fetch_firefly_one", new=AsyncMock(return_value=message)) as fetch_one, \
             patch.object(cs_ingest, "_existing_thread_ids", new=AsyncMock(return_value=set())), \
             patch.object(cs_ingest, "_waiting_info_tickets", new=AsyncMock(return_value=[])), \
             patch.object(cs_ingest.cs_resources, "active_resources", new=AsyncMock(return_value=[])), \
             patch.object(cs_ingest, "_classify", new=AsyncMock(return_value={"is_cs": True})), \
             patch.object(cs_ingest, "_to_fields", return_value=fields), \
             patch.object(cs_ingest.feishu, "api", new=AsyncMock()) as api:
            result = await cs_ingest.run(
                source="firefly",
                limit=1,
                dry_run=True,
                message_id="1791344517645162600",
                scan_limit=750,
            )

        fetch_one.assert_awaited_once_with("1791344517645162600", scan_limit=750)
        api.assert_not_awaited()
        self.assertTrue(result["replay_mode"])
        self.assertEqual(1, result["fetched"])
        self.assertEqual(1, result["new"])
        self.assertEqual(1, result["source_health"]["firefly"]["fetched"])
        self.assertEqual("ok", result["source_health"]["firefly"]["status"])

    async def test_ingest_endpoint_returns_424_when_a_source_failed(self):
        from app import main as app_main

        business_result = {
            "sources": "all",
            "fetched": 25,
            "new": 0,
            "skipped": 25,
            "errors": 0,
            "source_errors": {"funlab": "funlab_source_not_configured"},
            "dry_run": False,
            "replay_mode": False,
            "samples": [],
        }
        with patch.object(app_main, "_check_auth"), \
             patch.object(app_main.cs_ingest, "run", new=AsyncMock(return_value=business_result)), \
             patch.object(app_main, "_alert_endpoint_failure", new=AsyncMock()) as alert:
            response = await app_main.run_cs_ingest(authorization="Bearer test")

        self.assertEqual(424, response.status_code)
        payload = json.loads(response.body)
        self.assertFalse(payload["ok"])
        self.assertEqual(business_result["source_errors"], payload["source_errors"])
        alert.assert_awaited_once()

    async def test_replay_endpoint_passes_message_id_in_request_body(self):
        from app import main as app_main

        request = SimpleNamespace(json=AsyncMock(return_value={
            "message_id": "<one@example.com>", "dry_run": True, "scan_limit": 750,
        }))
        result = {"sources": "funlab", "fetched": 1, "new": 1, "skipped": 0,
                  "errors": 0, "source_errors": {}, "dry_run": True,
                  "replay_mode": True, "samples": []}
        with patch.object(app_main, "_check_auth"), \
             patch.object(app_main.cs_ingest, "run", new=AsyncMock(return_value=result)) as run:
            response = await app_main.replay_cs_ingest(request, authorization="Bearer test")

        self.assertTrue(response["ok"])
        run.assert_awaited_once_with(source="funlab", limit=750, dry_run=True,
                                     message_id="<one@example.com>", scan_limit=750,
                                     allow_info_request=False)

    async def test_replay_endpoint_accepts_explicit_firefly_source(self):
        from app import main as app_main

        request = SimpleNamespace(json=AsyncMock(return_value={
            "source": "firefly", "message_id": "1791344517645162600",
            "dry_run": True, "scan_limit": 750,
        }))
        result = {"sources": "firefly", "fetched": 1, "new": 1, "skipped": 0,
                  "errors": 0, "source_errors": {}, "dry_run": True,
                  "replay_mode": True, "samples": []}
        with patch.object(app_main, "_check_auth"), \
             patch.object(app_main.cs_ingest, "run", new=AsyncMock(return_value=result)) as run:
            response = await app_main.replay_cs_ingest(request, authorization="Bearer test")

        self.assertTrue(response["ok"])
        run.assert_awaited_once_with(source="firefly", limit=750, dry_run=True,
                                     message_id="1791344517645162600", scan_limit=750,
                                     allow_info_request=False)

    async def test_commit_replay_never_sends_customer_info_request(self):
        message = {"id": "<history@example.com>", "id_prefix": "CSF",
                   "frm": "customer@example.com", "subj": "Need help", "body": "Issue",
                   "channel": "邮箱", "brand_default": "FUNLAB", "attachments": []}
        fields = {"品牌": "FUNLAB", "销售平台": "未知", "分配运营": "待定",
                  "状态": cs_ingest.STATUS_WAIT_INFO, "客诉摘要": "Need order"}
        with patch.object(cs_ingest, "_fetch_funlab_one", new=AsyncMock(return_value=message)), \
             patch.object(cs_ingest, "_existing_thread_ids", new=AsyncMock(return_value=set())), \
             patch.object(cs_ingest, "_waiting_info_tickets", new=AsyncMock(return_value=[])), \
             patch.object(cs_ingest.cs_resources, "active_resources", new=AsyncMock(return_value=[])), \
             patch.object(cs_ingest, "_classify", new=AsyncMock(return_value={"is_cs": True})), \
             patch.object(cs_ingest, "_to_fields", return_value=fields), \
             patch.object(cs_ingest, "_send_info_request", new=AsyncMock()) as send_info, \
             patch.object(cs_ingest.feishu, "api", new=AsyncMock(return_value={
                 "data": {"record": {"record_id": "rec_history"}},
             })):
            result = await cs_ingest.run(source="funlab", dry_run=False,
                                         message_id="<history@example.com>",
                                         allow_info_request=False)

        send_info.assert_not_awaited()
        self.assertEqual(1, result["new"])
        self.assertEqual(0, result["errors"])
        self.assertEqual([{"message_id": "<history@example.com>",
                           "ticket_id": "", "record_id": "rec_history"}],
                         result["created_records"])

    async def test_commit_replay_reports_missing_created_record_id(self):
        message = {"id": "<missing-write@example.com>", "id_prefix": "CSF",
                   "frm": "customer@example.com", "subj": "Need help", "body": "Issue",
                   "channel": "邮箱", "brand_default": "FUNLAB", "attachments": []}
        fields = {"品牌": "FUNLAB", "销售平台": "独立站", "分配运营": "张佳烨",
                  "状态": "待派", "客诉摘要": "Issue"}
        with patch.object(cs_ingest, "_fetch_funlab_one", new=AsyncMock(return_value=message)), \
             patch.object(cs_ingest, "_existing_thread_ids", new=AsyncMock(return_value=set())), \
             patch.object(cs_ingest, "_waiting_info_tickets", new=AsyncMock(return_value=[])), \
             patch.object(cs_ingest.cs_resources, "active_resources", new=AsyncMock(return_value=[])), \
             patch.object(cs_ingest, "_classify", new=AsyncMock(return_value={"is_cs": True})), \
             patch.object(cs_ingest, "_to_fields", return_value=fields), \
             patch.object(cs_ingest.feishu, "api", new=AsyncMock(return_value={"data": {}})):
            result = await cs_ingest.run(source="funlab", dry_run=False,
                                         message_id="<missing-write@example.com>",
                                         allow_info_request=False)

        self.assertEqual(0, result["new"])
        self.assertEqual(1, result["errors"])

    async def test_ingest_endpoint_returns_500_for_unexpected_exception(self):
        from app import main as app_main

        with patch.object(app_main, "_check_auth"), \
             patch.object(app_main.cs_ingest, "run", new=AsyncMock(side_effect=RuntimeError("boom"))), \
             patch.object(app_main, "_alert_endpoint_failure", new=AsyncMock()):
            response = await app_main.run_cs_ingest(authorization="Bearer test")

        self.assertEqual(500, response.status_code)


if __name__ == "__main__":
    unittest.main()

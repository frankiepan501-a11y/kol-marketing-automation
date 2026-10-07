import unittest
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from app import cs_ingest


class CustomerServiceIngestRecoveryTests(unittest.IsolatedAsyncioTestCase):
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
        common = [
            patch.object(cs_ingest, "_existing_thread_ids", new=AsyncMock(
                side_effect=[{"m1"}, {"m2"}])),
            patch.object(cs_ingest, "_waiting_info_tickets", new=AsyncMock(return_value=[])),
            patch.object(cs_ingest.cs_resources, "active_resources", new=AsyncMock(return_value=[])),
            patch.object(cs_ingest, "_fetch_powkong", new=AsyncMock(
                side_effect=[[first], [second]])),
        ]
        with common[0], common[1], common[2], common[3]:
            ok = await cs_ingest.run(source="powkong", limit=1, dry_run=True)
            regressed = await cs_ingest.run(source="powkong", limit=1, dry_run=True)

        self.assertEqual("ok", ok["source_health"]["powkong"]["status"])
        self.assertEqual("anomaly", regressed["source_health"]["powkong"]["status"])
        self.assertIn("watermark regressed", regressed["source_errors"]["powkong"])
        cs_ingest._SOURCE_WATERMARKS.pop("powkong", None)

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

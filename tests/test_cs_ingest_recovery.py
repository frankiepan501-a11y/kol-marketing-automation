import unittest
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from app import cs_ingest


class CustomerServiceIngestRecoveryTests(unittest.IsolatedAsyncioTestCase):
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

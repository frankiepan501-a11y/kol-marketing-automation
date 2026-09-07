import asyncio
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("FEISHU_KOL_ASSISTANT_APP_ID", "cli_test_kol")
os.environ.setdefault("FEISHU_KOL_ASSISTANT_APP_SECRET", "secret_test_kol")
os.environ.setdefault("KOL_ASSISTANT_FRANKIE_UNION_ID", "on_test_frankie")

from app import completion_report, kol_assistant, main


class CompletionReportKolAssistantTests(unittest.TestCase):
    def test_kol_assistant_route_sends_only_one_frankie_card(self):
        async def fetch_records(_table_id, **_kwargs):
            return []

        with patch.object(completion_report.feishu, "fetch_all_records", new=fetch_records), \
             patch.object(
                 completion_report.kol_assistant,
                 "send_card_to_frankie",
                 new=AsyncMock(return_value="om_r7_once"),
             ) as sender:
            result = asyncio.run(completion_report.run(
                dry_run=False,
                delivery_identity="kol_assistant",
                frankie_only=True,
            ))

        self.assertEqual(result["delivery_identity"], "kol_assistant")
        self.assertTrue(result["frankie_only"])
        self.assertEqual(result["notified"], 1)
        self.assertEqual(result["message_ids"], ["om_r7_once"])
        sender.assert_awaited_once()

    def test_kol_assistant_route_rejects_group_delivery(self):
        with self.assertRaisesRegex(ValueError, "Frankie-only"):
            asyncio.run(completion_report.run(
                dry_run=True,
                delivery_identity="kol_assistant",
                frankie_only=False,
            ))

    def test_legacy_route_rejects_false_frankie_only_label(self):
        with self.assertRaisesRegex(ValueError, "legacy"):
            asyncio.run(completion_report.run(
                dry_run=True,
                delivery_identity="legacy",
                frankie_only=True,
            ))

    def test_kol_assistant_dry_run_does_not_send(self):
        async def fetch_records(_table_id, **_kwargs):
            return []

        with patch.object(completion_report.feishu, "fetch_all_records", new=fetch_records), \
             patch.object(
                 completion_report.kol_assistant,
                 "send_card_to_frankie",
                 new=AsyncMock(return_value="om_should_not_send"),
             ) as sender:
            result = asyncio.run(completion_report.run(
                dry_run=True,
                delivery_identity="kol_assistant",
                frankie_only=True,
            ))

        self.assertEqual(result["notified"], 0)
        self.assertEqual(result["message_ids"], [])
        sender.assert_not_awaited()

    def test_report_reads_only_required_fields_with_large_pages(self):
        calls = []

        async def fetch_records(table_id, **kwargs):
            calls.append((table_id, kwargs))
            return []

        with patch.object(completion_report.feishu, "fetch_all_records", new=fetch_records):
            result = asyncio.run(completion_report.run(
                dry_run=True,
                delivery_identity="kol_assistant",
                frankie_only=True,
            ))

        self.assertEqual(result["notified"], 0)
        self.assertEqual(len(calls), 3)
        self.assertTrue(all(kwargs["page_size"] == 500 for _, kwargs in calls))
        draft_call = calls[0][1]
        self.assertEqual(draft_call["field_names"], completion_report.DRAFT_FIELDS)
        for spec, (_, contact_call) in zip(completion_report.SPECS, calls[1:]):
            self.assertEqual(contact_call["field_names"], completion_report._contact_fields(spec))

    def test_missing_kol_app_credentials_fail_without_legacy_fallback(self):
        with patch.object(kol_assistant, "APP_ID", ""), \
             patch.object(kol_assistant, "APP_SECRET", ""):
            with self.assertRaisesRegex(RuntimeError, "KOL媒体助手未配置"):
                asyncio.run(kol_assistant.send_card_to_frankie(
                    {"elements": []},
                    message_uuid="kol-r7-missing-config",
                ))

    def test_missing_dedicated_frankie_union_fails_without_cross_domain_fallback(self):
        with patch.object(kol_assistant, "FRANKIE_UNION_ID", ""):
            with self.assertRaisesRegex(RuntimeError, "Frankie union_id"):
                asyncio.run(kol_assistant.send_card_to_frankie(
                    {"elements": []},
                    message_uuid="kol-r7-missing-union",
                ))

    def test_http_endpoint_exposes_async_r7_result_with_message_id(self):
        async def exercise():
            main._completion_report_jobs.clear()
            final = {
                "dry_run": False,
                "report": {},
                "delivery_identity": "kol_assistant",
                "frankie_only": True,
                "notified": 1,
                "message_ids": ["om_async_r7"],
            }
            with patch.object(main, "_check_auth"), \
                 patch.object(
                     completion_report,
                     "run",
                     new=AsyncMock(return_value=final),
                 ):
                accepted = await main.run_completion_report(
                    authorization="Bearer test",
                    dry_run=False,
                    async_mode=True,
                    delivery_identity="kol_assistant",
                    frankie_only=True,
                )
                for _ in range(10):
                    await asyncio.sleep(0)
                    if main._completion_report_jobs[accepted["job_id"]]["status"] != "running":
                        break
                status = await main.get_completion_report_job(
                    accepted["job_id"], authorization="Bearer test"
                )
            return accepted, status

        accepted, status = asyncio.run(exercise())
        self.assertTrue(accepted["accepted"])
        self.assertEqual(status["status"], "success")
        self.assertEqual(status["result"]["message_ids"], ["om_async_r7"])

    def test_duplicate_async_r7_start_reuses_running_job(self):
        async def exercise():
            main._completion_report_jobs.clear()
            gate = asyncio.Event()

            async def blocked_run(**_kwargs):
                await gate.wait()
                return {
                    "delivery_identity": "kol_assistant",
                    "frankie_only": True,
                    "notified": 1,
                    "message_ids": ["om_once"],
                }

            with patch.object(main, "_check_auth"), \
                 patch.object(completion_report, "run", new=blocked_run):
                first = await main.run_completion_report(
                    authorization="Bearer test",
                    async_mode=True,
                    delivery_identity="kol_assistant",
                    frankie_only=True,
                )
                second = await main.run_completion_report(
                    authorization="Bearer test",
                    async_mode=True,
                    delivery_identity="kol_assistant",
                    frankie_only=True,
                )
                gate.set()
                for _ in range(10):
                    await asyncio.sleep(0)
                    if main._completion_report_jobs[first["job_id"]]["status"] != "running":
                        break
            return first, second

        first, second = asyncio.run(exercise())
        self.assertFalse(first["already_running"])
        self.assertTrue(second["already_running"])
        self.assertEqual(second["job_id"], first["job_id"])

    def test_http_endpoint_forwards_r7_delivery_gate(self):
        mocked_result = {
            "dry_run": True,
            "report": {},
            "delivery_identity": "kol_assistant",
            "frankie_only": True,
            "notified": 0,
            "message_ids": [],
        }
        with patch.object(main, "_check_auth"), \
             patch.object(
                 completion_report,
                 "run",
                 new=AsyncMock(return_value=mocked_result),
             ) as runner:
            result = asyncio.run(main.run_completion_report(
                authorization="Bearer test",
                dry_run=True,
                async_mode=False,
                delivery_identity="kol_assistant",
                frankie_only=True,
            ))

        self.assertTrue(result["ok"])
        self.assertEqual(result["delivery_identity"], "kol_assistant")
        runner.assert_awaited_once_with(
            dry_run=True,
            delivery_identity="kol_assistant",
            frankie_only=True,
        )


if __name__ == "__main__":
    unittest.main()

import json
import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from app import auto_send, launch_email_preflight, launch_raw_certificate


class LaunchRawCertificateTests(unittest.TestCase):
    def setUp(self):
        launch_email_preflight._CERTIFICATE_LOCKS.clear()

    def test_adding_cold_template_certificate_preserves_launch_queue_certificate(self):
        launch_cert = {
            "campaign_id": "campaign1",
            "product_id": "product1",
            "brand": "FUNLAB",
            "template_version": "launch-queue-v1",
            "passed": True,
            "run_key": "launch-run",
        }
        cold_cert = {
            "campaign_id": "campaign1",
            "product_id": "product1",
            "brand": "FUNLAB",
            "template_version": "kol-cold-template-v1",
            "passed": True,
            "run_key": "cold-run",
        }

        merged = launch_raw_certificate.merge(
            json.dumps(launch_cert, separators=(",", ":")),
            cold_cert,
        )

        self.assertEqual(
            launch_cert,
            launch_raw_certificate.select(merged, "launch-queue-v1"),
        )
        self.assertEqual(
            cold_cert,
            launch_raw_certificate.select(merged, "kol-cold-template-v1"),
        )

    def test_revalidating_one_template_replaces_only_that_template(self):
        old_launch = {
            "template_version": "launch-queue-v1", "passed": True,
            "run_key": "launch-old",
        }
        cold = {
            "template_version": "kol-cold-template-v1", "passed": True,
            "run_key": "cold-current",
        }
        raw = launch_raw_certificate.merge("", old_launch)
        raw = launch_raw_certificate.merge(raw, cold)
        new_launch = {
            "template_version": "launch-queue-v1", "passed": True,
            "run_key": "launch-new",
        }

        raw = launch_raw_certificate.merge(raw, new_launch)

        self.assertEqual(
            new_launch, launch_raw_certificate.select(raw, "launch-queue-v1"),
        )
        self.assertEqual(
            cold, launch_raw_certificate.select(raw, "kol-cold-template-v1"),
        )

    def test_malformed_or_mismatched_certificate_map_fails_closed(self):
        self.assertEqual({}, launch_raw_certificate.select("not-json", "launch-queue-v1"))
        mismatched = json.dumps({
            "schema_version": 2,
            "certificates": {
                "launch-queue-v1": {
                    "template_version": "kol-cold-template-v1", "passed": True,
                },
            },
        })
        self.assertEqual(
            {}, launch_raw_certificate.select(mismatched, "launch-queue-v1"),
        )

    def test_auto_send_reads_launch_certificate_when_cold_certificate_coexists(self):
        launch_cert = {
            "campaign_id": "campaign1", "product_id": "product1",
            "brand": "FUNLAB", "template_version": "launch-queue-v1",
            "passed": True,
        }
        cold_cert = {
            "campaign_id": "campaign1", "product_id": "product1",
            "brand": "FUNLAB", "template_version": "kol-cold-template-v1",
            "passed": True,
        }
        raw = launch_raw_certificate.merge("", launch_cert)
        raw = launch_raw_certificate.merge(raw, cold_cert)

        self.assertTrue(auto_send._valid_raw_certificate(
            {"邮件Raw验证证书": raw}, campaign_id="campaign1",
            product_id="product1", brand="FUNLAB",
        ))

    def test_persisting_cold_certificate_keeps_existing_launch_certificate(self):
        launch_cert = {
            "campaign_id": "campaign1", "product_id": "product1",
            "brand": "FUNLAB", "template_version": "launch-queue-v1",
            "passed": True, "run_key": "launch-run",
        }
        state = {
            "raw": json.dumps(launch_cert, separators=(",", ":")),
        }
        activity = {
            "record_id": "activity1",
            "fields": {"邮件Raw验证证书": state["raw"]},
        }

        async def fake_update(_table_id, _record_id, fields):
            state["raw"] = fields["邮件Raw验证证书"]

        async def fake_get(_table_id, _record_id):
            return {
                "record_id": "activity1",
                "fields": {"邮件Raw验证证书": state["raw"]},
            }

        with patch.object(
            launch_email_preflight, "_activity_for_certificate",
            new=AsyncMock(return_value=activity),
        ), patch.object(
            launch_email_preflight.feishu, "update_record", side_effect=fake_update,
        ), patch.object(
            launch_email_preflight.feishu, "get_record", side_effect=fake_get,
        ):
            cold_cert = asyncio.run(launch_email_preflight._persist_certificate(
                campaign_id="campaign1", product_id="product1", draft_id="template:cold",
                brand="FUNLAB", run_key="cold-run", validation={"checks": {"raw": True}},
                template_version="kol-cold-template-v1",
            ))

        self.assertEqual(
            launch_cert,
            launch_raw_certificate.select(state["raw"], "launch-queue-v1"),
        )
        self.assertEqual(
            cold_cert,
            launch_raw_certificate.select(state["raw"], "kol-cold-template-v1"),
        )

    def test_concurrent_template_validations_do_not_lose_either_certificate(self):
        state = {"raw": "", "reads": 0}
        both_read = asyncio.Event()

        async def fake_activity(_campaign_id, _product_id):
            snapshot = state["raw"]
            state["reads"] += 1
            if state["reads"] >= 2:
                both_read.set()
            try:
                await asyncio.wait_for(both_read.wait(), timeout=0.02)
            except asyncio.TimeoutError:
                pass
            return {
                "record_id": "activity1",
                "fields": {"邮件Raw验证证书": snapshot},
            }

        async def fake_update(_table_id, _record_id, fields):
            state["raw"] = fields["邮件Raw验证证书"]

        async def fake_get(_table_id, _record_id):
            return {
                "record_id": "activity1",
                "fields": {"邮件Raw验证证书": state["raw"]},
            }

        async def run_both():
            await asyncio.gather(
                launch_email_preflight._persist_certificate(
                    campaign_id="campaign1", product_id="product1",
                    draft_id="template:launch", brand="FUNLAB",
                    run_key="launch-run", validation={"checks": {}},
                    template_version="launch-queue-v1",
                ),
                launch_email_preflight._persist_certificate(
                    campaign_id="campaign1", product_id="product1",
                    draft_id="template:cold", brand="FUNLAB",
                    run_key="cold-run", validation={"checks": {}},
                    template_version="kol-cold-template-v1",
                ),
            )

        with patch.object(
            launch_email_preflight, "_activity_for_certificate", side_effect=fake_activity,
        ), patch.object(
            launch_email_preflight.feishu, "update_record", side_effect=fake_update,
        ), patch.object(
            launch_email_preflight.feishu, "get_record", side_effect=fake_get,
        ):
            asyncio.run(run_both())

        self.assertTrue(launch_raw_certificate.select(state["raw"], "launch-queue-v1"))
        self.assertTrue(launch_raw_certificate.select(state["raw"], "kol-cold-template-v1"))


if __name__ == "__main__":
    unittest.main()

import asyncio
import datetime as dt
import os
import unittest

from app import b2b_linkedin_async_audit as audit


class B2BLinkedInAsyncAuditTest(unittest.TestCase):
    def setUp(self):
        self.old_env = {
            key: os.environ.get(key)
            for key in [
                "GOOGLE_SEARCH_PROVIDER",
                "GOOGLE_SEARCH_API_KEY",
                "GOOGLE_SEARCH_ENGINE_ID",
                "SERPAPI_API_KEY",
                "B2B_DISCOVERY_MANUAL_RESULTS_JSON",
            ]
        }

    def tearDown(self):
        for key, value in self.old_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def test_extract_job_id_from_http_node_output(self):
        detail = {
            "data": {
                "resultData": {
                    "runData": {
                        "Call B2B External Discovery": [
                            {
                                "data": {
                                    "main": [[{"json": {"ok": True, "job_id": "b2bdisc-abc123"}}]]
                                }
                            }
                        ]
                    }
                }
            }
        }
        self.assertEqual(
            "b2bdisc-abc123",
            audit._extract_job_id(detail, ["Call B2B External Discovery"]),
        )

    def test_discovery_high_waterline_zero_is_ok(self):
        health, message = audit._status_from_created(
            audit.WORKFLOWS["discovery"],
            {"created_candidates": 0, "planned_candidates": 0, "waterline_status": "skip_target_met"},
        )
        self.assertEqual("ok", health)
        self.assertIn("高水位", message)

    def test_discovery_zero_with_provider_errors_is_error(self):
        health, message = audit._status_from_created(
            audit.WORKFLOWS["discovery"],
            {
                "created_candidates": 0,
                "planned_candidates": 0,
                "waterline_status": "refill_needed",
                "provider_errors": [{"provider": "custom_search", "error": "missing key"}],
            },
        )
        self.assertEqual("error", health)
        self.assertIn("provider_errors=1", message)

    def test_auto_pool_zero_created_is_error(self):
        health, message = audit._status_from_created(
            audit.WORKFLOWS["auto_pool"],
            {"created_records": 0, "planned_records": 12},
        )
        self.assertEqual("error", health)
        self.assertIn("created=0", message)

    def test_search_provider_readiness_reports_missing_custom_search_keys(self):
        os.environ["GOOGLE_SEARCH_PROVIDER"] = "custom_search"
        os.environ.pop("GOOGLE_SEARCH_API_KEY", None)
        os.environ.pop("GOOGLE_SEARCH_ENGINE_ID", None)
        readiness = audit._search_provider_readiness()
        self.assertFalse(readiness["ok"])
        self.assertIn("GOOGLE_SEARCH_API_KEY", readiness["missing"])
        self.assertIn("GOOGLE_SEARCH_ENGINE_ID", readiness["missing"])

    def test_run_flags_n8n_success_backend_zero_output(self):
        original_execs = audit._fetch_executions
        original_detail = audit._fetch_execution_detail
        original_job = audit._fetch_job
        original_ready = audit._search_provider_readiness
        try:
            async def fake_execs(workflow_id, limit=5):
                now = dt.datetime.now(dt.timezone.utc).isoformat()
                return [{"id": "ex1", "status": "success", "startedAt": now, "stoppedAt": now}]

            async def fake_detail(execution_id):
                return {
                    "data": {
                        "resultData": {
                            "runData": {
                                "Call B2B LinkedIn Auto Pool": [
                                    {"data": {"main": [[{"json": {"job_id": "b2bpool-zero"}}]]}}
                                ]
                            }
                        }
                    }
                }

            async def fake_job(job_path, job_id):
                return {"ok": True, "status": "success", "result": {"created_records": 0, "planned_records": 10}}

            audit._fetch_executions = fake_execs
            audit._fetch_execution_detail = fake_detail
            audit._fetch_job = fake_job
            audit._search_provider_readiness = lambda: {"provider": "serpapi", "ok": True, "missing": []}

            result = asyncio.run(audit.run(workflow="auto_pool", lookback_hours=2))
        finally:
            audit._fetch_executions = original_execs
            audit._fetch_execution_detail = original_detail
            audit._fetch_job = original_job
            audit._search_provider_readiness = original_ready

        self.assertFalse(result["ok"])
        self.assertEqual(1, result["issue_count"])
        self.assertEqual("error", result["items"][0]["health"])
        self.assertIn("created=0", result["items"][0]["issues"][0])


if __name__ == "__main__":
    unittest.main()

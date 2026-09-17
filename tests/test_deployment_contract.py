import json
import unittest
from pathlib import Path


ROOT = Path(__file__).parents[1]


class DeploymentContractTests(unittest.TestCase):
    def test_n8n_schedule_and_service_scope_are_locked(self):
        workflow = json.loads((ROOT / "n8n-workflow.json").read_text(encoding="utf-8"))
        self.assertEqual(workflow["settings"]["timezone"], "Asia/Shanghai")
        schedule = workflow["nodes"][0]["parameters"]["rule"]["interval"][0]
        self.assertEqual(schedule["expression"], "30 16 * * *")

        start = next(node for node in workflow["nodes"] if node["id"] == "start")
        self.assertTrue(start["parameters"]["url"].endswith("/daily"))
        self.assertEqual(start["parameters"]["sendBody"], False)
        self.assertFalse(start.get("retryOnFail", False))

        wait = next(node for node in workflow["nodes"] if node["id"] == "wait")
        self.assertLessEqual(wait["parameters"]["amount"], 2)
        poll = next(node for node in workflow["nodes"] if node["id"] == "poll")
        self.assertEqual(poll.get("retryOnFail"), True)
        self.assertGreaterEqual(poll.get("maxTries", 0), 15)

    def test_service_writes_no_kol_master_and_report_has_only_approved_destination(self):
        sources = "\n".join(
            path.read_text(encoding="utf-8")
            for path in (ROOT / "app").glob("*.py")
        )
        self.assertNotIn("tblMMhnj2hEbhF6y", sources)
        self.assertIn("oc_4ddd938ddb73201ed7354337eb2226ac", sources)
        self.assertIn("cli_aa143b0a11b89be4", sources)

    def test_single_video_replay_endpoint_is_available(self):
        main = (ROOT / "app" / "main.py").read_text(encoding="utf-8")
        self.assertIn('@app.post("/replay/{video_id}")', main)
        self.assertIn("replay_video", main)
        self.assertIn('@app.get("/status")', main)

    def test_reverse_history_backfill_endpoint_is_available(self):
        main = (ROOT / "app" / "main.py").read_text(encoding="utf-8")
        self.assertIn('@app.post("/backfill")', main)
        self.assertIn("window_days", main)
        collector = (ROOT / "app" / "collector.py").read_text(encoding="utf-8")
        self.assertIn("waterline_advanced", collector)
        self.assertIn("refresh_existing_ids=False", collector)

    def test_assert_endpoint_distinguishes_all_job_states(self):
        from app.job_status import finished_status

        self.assertEqual(finished_status(None)[0], 404)
        self.assertEqual(finished_status({"status": "running"})[0], 409)
        self.assertEqual(
            finished_status({"status": "failed", "error_type": "ApiError"})[0],
            500,
        )
        status, payload = finished_status(
            {"status": "completed", "new_posts": 3}
        )
        self.assertEqual(status, 200)
        self.assertEqual(payload["new_posts"], 3)


if __name__ == "__main__":
    unittest.main()

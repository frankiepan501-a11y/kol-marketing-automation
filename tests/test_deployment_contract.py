import json
import unittest
from pathlib import Path


ROOT = Path(__file__).parents[1]


class DeploymentContractTests(unittest.TestCase):
    def test_n8n_schedule_and_service_scope_are_locked(self):
        workflow = json.loads((ROOT / "n8n-workflow.json").read_text(encoding="utf-8"))
        self.assertEqual(workflow["settings"]["timezone"], "Asia/Shanghai")
        schedule = workflow["nodes"][0]["parameters"]["rule"]["interval"][0]
        self.assertEqual(schedule["expression"], "30 9 * * 1,3,5")

        start = next(node for node in workflow["nodes"] if node["id"] == "start")
        self.assertIn('"brand":"NYXI"', start["parameters"]["jsonBody"])
        self.assertIn('"platform":"YouTube"', start["parameters"]["jsonBody"])
        self.assertIn('"force":false', start["parameters"]["jsonBody"])
        self.assertFalse(start.get("retryOnFail", False))

        wait = next(node for node in workflow["nodes"] if node["id"] == "wait")
        self.assertLessEqual(wait["parameters"]["amount"], 2)
        poll = next(node for node in workflow["nodes"] if node["id"] == "poll")
        self.assertEqual(poll.get("retryOnFail"), True)
        self.assertGreaterEqual(poll.get("maxTries", 0), 15)

    def test_service_has_no_kol_master_or_outbound_destination(self):
        sources = "\n".join(
            path.read_text(encoding="utf-8")
            for path in (ROOT / "app").glob("*.py")
        )
        self.assertNotIn("tblMMhnj2hEbhF6y", sources)
        self.assertNotIn("oc_4ddd938ddb73201ed7354337eb2226ac", sources)

    def test_single_video_replay_endpoint_is_available(self):
        main = (ROOT / "app" / "main.py").read_text(encoding="utf-8")
        self.assertIn('@app.post("/replay/{video_id}")', main)
        self.assertIn("replay_video", main)
        self.assertIn('@app.get("/status")', main)

    def test_assert_endpoint_distinguishes_all_job_states(self):
        source = (ROOT / "app" / "main.py").read_text(encoding="utf-8")
        namespace = {}
        function_source = source[
            source.index("def finished_status(") : source.index("\n\ndef _authorized(")
        ]
        exec(function_source, {"Any": object}, namespace)
        finished_status = namespace["finished_status"]
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

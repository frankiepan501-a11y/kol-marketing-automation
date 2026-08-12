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

    def test_service_has_no_kol_master_or_outbound_destination(self):
        sources = "\n".join(
            path.read_text(encoding="utf-8")
            for path in (ROOT / "app").glob("*.py")
        )
        self.assertNotIn("tblMMhnj2hEbhF6y", sources)
        self.assertNotIn("oc_4ddd938ddb73201ed7354337eb2226ac", sources)


if __name__ == "__main__":
    unittest.main()

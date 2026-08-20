import pathlib
import unittest


class LaunchAutonomyWorkflowScriptTests(unittest.TestCase):
    def test_piranha_audit_requires_complete_business_result_contract(self):
        script = (
            pathlib.Path(__file__).parents[1]
            / "scripts"
            / "upsert_launch_autonomy_workflows.ps1"
        ).read_text(encoding="utf-8")

        self.assertIn("allowedOutcomes", script)
        self.assertIn("made_supply_progress", script)
        self.assertIn("supply_progress_breakdown", script)
        self.assertIn("missing or invalid business result fields", script)
        self.assertNotIn("Number(result.supply_progress || 0)", script)

    def test_audit_upsert_reuses_existing_workflow_and_deactivates_duplicate_legacy(self):
        script = (
            pathlib.Path(__file__).parents[1]
            / "scripts"
            / "upsert_launch_autonomy_workflows.ps1"
        ).read_text(encoding="utf-8")

        self.assertIn(
            "Upsert-Workflow $auditName $auditNodes $auditConnections $settings $auditExisting",
            script,
        )
        self.assertIn("$legacyAuditExisting", script)
        self.assertIn("/deactivate", script)

    def test_dave_running_job_does_not_block_piranha_audit_branch(self):
        script = (
            pathlib.Path(__file__).parents[1]
            / "scripts"
            / "upsert_launch_autonomy_workflows.ps1"
        ).read_text(encoding="utf-8")

        self.assertIn("dave_running_within_expected_window", script)


if __name__ == "__main__":
    unittest.main()

import pathlib
import unittest


class LaunchAutonomyWorkflowScriptTests(unittest.TestCase):
    def test_both_audits_require_complete_business_result_contract(self):
        script = (
            pathlib.Path(__file__).parents[1]
            / "scripts"
            / "upsert_launch_autonomy_workflows.ps1"
        ).read_text(encoding="utf-8")

        self.assertGreaterEqual(script.count("allowedOutcomes"), 2)
        self.assertGreaterEqual(script.count("hasQuota"), 2)
        self.assertGreaterEqual(script.count("hasInventory"), 2)
        self.assertGreaterEqual(script.count("hasProgress"), 2)
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

    def test_audit_collects_both_campaign_results_before_failing(self):
        script = (
            pathlib.Path(__file__).parents[1]
            / "scripts"
            / "upsert_launch_autonomy_workflows.ps1"
        ).read_text(encoding="utf-8")

        dave_validator = script.split("$validateDave = @'", 1)[1].split("'@", 1)[0]
        piranha_validator = script.split("$validatePiranha = @'", 1)[1].split("'@", 1)[0]
        self.assertNotIn("throw new Error", dave_validator)
        self.assertNotIn("throw new Error", piranha_validator)
        self.assertIn("$validateBoth", script)
        self.assertIn("Campaign Audit Merge", script)
        self.assertIn("Validate Both Campaigns", script)
        self.assertIn("both_campaigns_checked", script)
        self.assertGreaterEqual(script.count("onError = 'continueRegularOutput'"), 2)

    def test_audit_parses_service_timezone_and_throws_a_business_readable_summary(self):
        script = (
            pathlib.Path(__file__).parents[1]
            / "scripts"
            / "upsert_launch_autonomy_workflows.ps1"
        ).read_text(encoding="utf-8")

        self.assertIn("parseServiceTimestamp", script)
        self.assertIn("replace(/([+-]\\d{2})(\\d{2})$/", script)
        self.assertIn("activity: 'Dave'", script)
        self.assertIn("activity: 'Piranha'", script)
        self.assertIn("`activity=${report.activity}", script)
        self.assertIn("inventory=", script)
        self.assertIn("latest=", script)
        self.assertIn("quota=", script)
        self.assertIn("parts=", script)
        self.assertIn("supply=", script)
        self.assertIn("next=", script)
        self.assertNotIn("JSON.stringify(summary).slice(0, 5000)", script)

    def test_upsert_preserves_unmanaged_remote_nodes_and_restores_active_state(self):
        script = (
            pathlib.Path(__file__).parents[1]
            / "scripts"
            / "upsert_launch_autonomy_workflows.ps1"
        ).read_text(encoding="utf-8")

        self.assertIn("$current.nodes | Where-Object", script)
        self.assertIn("Merge-MissingProperties", script)
        self.assertIn("$target.node -notin $managedNames", script)
        self.assertIn("$current.connections.PSObject.Properties", script)
        self.assertIn("$current.settings.PSObject.Properties", script)
        self.assertIn("$wasActive = [bool]$current.active", script)
        self.assertIn("automatic reactivation also failed", script)


if __name__ == "__main__":
    unittest.main()

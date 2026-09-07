from pathlib import Path


ROOT = Path(__file__).parents[1]
PATCH_SCRIPT = ROOT / "scripts" / "patch_kol_p0_p1_workflows.ps1"


def test_patch_preserves_workflows_and_adds_reply_monitor_job_polling():
    script = PATCH_SCRIPT.read_text(encoding="utf-8")

    assert "Get-Workflow" in script
    assert "Update-WorkflowSafely" in script
    assert "/reply-monitor/run?async_mode=true" in script
    assert "/reply-monitor/jobs/" in script
    assert "Wait Reply Monitor" in script
    assert "Get Reply Monitor Status" in script
    assert "Check Reply Monitor Deadline" in script
    assert "Require Reply Monitor Success" in script
    assert "REPLY_MONITOR_TIMEOUT" in script


def test_patch_uses_shared_environment_auth_for_both_status_nodes():
    script = PATCH_SCRIPT.read_text(encoding="utf-8")

    assert "KOL_AUTOMATION_API_TOKEN" in script
    assert "Get Cleanup Status" in script
    assert "$status.parameters.headerParameters = New-KolAuthHeaders" in script
    assert "$replyStatusParams.headerParameters = New-KolAuthHeaders" in script

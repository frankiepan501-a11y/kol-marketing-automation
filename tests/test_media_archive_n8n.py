from tools.migrations import media_archive_n8n


def test_extract_authorization_reuses_existing_kol_internal_header():
    source = {
        "nodes": [{
            "type": "n8n-nodes-base.httpRequest",
            "parameters": {
                "headerParameters": {
                    "parameters": [{"name": "Authorization", "value": "Bearer existing-secret"}],
                },
            },
        }],
    }

    assert media_archive_n8n.extract_authorization(source) == "Bearer existing-secret"


def test_workflow_specs_cover_queue_metrics_and_external_worker_audit():
    workflows = media_archive_n8n.build_workflows("Bearer hidden")
    by_name = {workflow["name"]: workflow for workflow in workflows}

    assert set(by_name) == {
        "KOL - 素材归档队列扫描 (每10分钟)",
        "KOL - YouTube作品数据刷新 (每日09:15)",
        "KOL - 素材归档终端掉线检查 (每10分钟)",
    }
    urls = {
        node["parameters"].get("url")
        for workflow in workflows
        for node in workflow["nodes"]
        if node["type"] == "n8n-nodes-base.httpRequest"
    }
    assert "https://kol-auto.zeabur.app/media-archive/tick" in urls
    assert "https://kol-auto.zeabur.app/media-archive/youtube-metrics/tick" in urls
    assert (
        "https://kol-auto.zeabur.app/media-archive/worker/audit?stale_minutes=10&notify=true"
        in urls
    )


def test_public_plan_never_contains_the_reused_authorization_value():
    plan = media_archive_n8n.public_plan(
        media_archive_n8n.build_workflows("Bearer must-not-print"), activate=True,
    )

    assert "must-not-print" not in str(plan)
    assert all(item["activate"] is True for item in plan["workflows"])

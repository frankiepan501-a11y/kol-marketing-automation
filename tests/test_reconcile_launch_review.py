import argparse
import json

import pytest

from scripts import reconcile_launch_review as reconcile


def args():
    return argparse.Namespace(
        campaign_id="launch-20260915-funlab-dave-ys11-5",
        product_id="recvkJOoCsNb1s",
        ranking_version="evidence-v4",
        target_count=1,
    )


def valid_payload():
    return {
        "status": "success",
        "result": {
            "read_only": True,
            "writes": 0,
            "campaign_id": "launch-20260915-funlab-dave-ys11-5",
            "product": {"requested_product_id": "recvkJOoCsNb1s"},
            "ranking_version": "evidence-v4",
            "evidence_source": "activity_node_snapshot",
            "competitor_evidence_applied": True,
            "evidence_coverage": {"valid_partner_posts": 2988},
            "summary": {"evaluated": 1},
            "candidates": [{"contact_id": "kol1", "decision": "eligible_new_cold"}],
        },
    }


def participants():
    return [{"fields": {
        "联系人记录ID": "kol1", "参与状态": "已入围",
    }}]


def activity():
    return {"fields": {
        "证据排序版本": "evidence-v4",
        "产品主记录ID": "recvkJOoCsNb1s",
    }}


def test_load_preview_json_accepts_exact_read_only_background_result(tmp_path):
    path = tmp_path / "preview.json"
    path.write_text(json.dumps(valid_payload()), encoding="utf-8")

    preview = reconcile.load_preview_json(str(path), args(), participants(), activity())

    assert preview["candidates"][0]["contact_id"] == "kol1"


@pytest.mark.parametrize("field,value", [
    ("ranking_version", "evidence-v3"),
    ("evidence_source", "activity_relation"),
    ("competitor_evidence_applied", False),
    ("read_only", False),
])
def test_load_preview_json_rejects_stale_or_unsafe_result(tmp_path, field, value):
    payload = valid_payload()
    payload["result"][field] = value
    path = tmp_path / "preview.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(RuntimeError):
        reconcile.load_preview_json(str(path), args(), participants(), activity())


def test_load_preview_json_rejects_partial_result_missing_current_participant(tmp_path):
    path = tmp_path / "preview.json"
    path.write_text(json.dumps(valid_payload()), encoding="utf-8")
    missing = [{"fields": {"联系人记录ID": "kol-not-in-preview", "参与状态": "已入围"}}]

    with pytest.raises(RuntimeError, match="候选可安全补位"):
        reconcile.load_preview_json(str(path), args(), missing, activity())


def test_load_preview_json_rejects_when_production_activity_version_advanced(tmp_path):
    path = tmp_path / "preview.json"
    path.write_text(json.dumps(valid_payload()), encoding="utf-8")
    advanced = activity()
    advanced["fields"]["证据排序版本"] = "evidence-v5"

    with pytest.raises(RuntimeError, match="生产活动版本未变化"):
        reconcile.load_preview_json(str(path), args(), participants(), advanced)

"""R9: remove legacy identities from new KOL n8n traffic.

The shared Event Hub deliberately keeps its App 3 branch for unmarked historical
cards. New KOL callbacks carry ``_delivery_identity=kol_assistant`` and use the
dedicated App. Run without ``--commit`` for a read-only preview.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parent))
import migrate_kol_feishu_identity_r8 as r8


WORKFLOWS = {
    "KNxD9ES93iyEs9mB": "daily_discovery",
    "LwP6SYw2FSrkENV0": "influencer_sync",
    "hgM7unABBW7hr5dw": "phase2_publish_check",
    "kIQFipyMAWTPMLdS": "media_discovery",
    "YjTXaoWAcy89xZpT": "event_hub_history_compat",
}


def _target_token_body() -> str:
    return (
        '={{ { "app_id": $env.FEISHU_KOL_ASSISTANT_APP_ID, '
        '"app_secret": $env.FEISHU_KOL_ASSISTANT_APP_SECRET } }}'
    )


def _set_target_token(workflow: dict, name: str) -> list[str]:
    node = r8._node(workflow, name)
    before = str(node.get("parameters", {}).get("jsonBody") or "")
    after = _target_token_body()
    if before == after:
        return []
    node["parameters"]["jsonBody"] = after
    return [name]


def _set_all_kol_tokens(workflow: dict) -> list[str]:
    changes = []
    for name in ("HTTP — token KOL", "HTTP — token influencers"):
        changes.extend(_set_target_token(workflow, name))
    return changes


def patch_daily_discovery(workflow: dict) -> list[str]:
    changes = _set_all_kol_tokens(workflow)
    node = r8._node(workflow, "Code — 日报引擎")
    code = node["parameters"]["jsCode"]
    updated = code
    updated = updated.replace(
        "const KOL_CARDS_ENABLED = $env.KOL_FEISHU_CARDS_ENABLED === '1';\n", "", 1
    )
    updated = re.sub(
        r"^const FRANKIE_LEGACY_OPEN_ID = 'ou_[^']+';\n?", "", updated,
        count=1, flags=re.MULTILINE,
    )
    updated = updated.replace(
        "if (KOL_CARDS_ENABLED && !FRANKIE_UNION_ID) throw new Error('KOL assistant Frankie union_id missing');",
        "if (!FRANKIE_UNION_ID) throw new Error('KOL assistant Frankie union_id missing');",
        1,
    )
    updated = updated.replace(
        "if (SEND_FRANKIE) targets.push(KOL_CARDS_ENABLED ? ['union_id', FRANKIE_UNION_ID] : ['open_id', FRANKIE_LEGACY_OPEN_ID]);",
        "if (SEND_FRANKIE) targets.push(['union_id', FRANKIE_UNION_ID]);",
        1,
    )
    updated = updated.replace(
        "Authorization: 'Bearer ' + (KOL_CARDS_ENABLED ? t2 : t1)",
        "Authorization: 'Bearer ' + t2",
        1,
    )
    forbidden = ("KOL_FEISHU_CARDS_ENABLED", "FRANKIE_LEGACY_OPEN_ID")
    if any(value in updated for value in forbidden):
        raise RuntimeError("daily discovery still has a legacy KOL card route")
    if updated != code:
        node["parameters"]["jsCode"] = updated
        changes.append(node["name"])
    return changes


def patch_influencer_sync(workflow: dict) -> list[str]:
    return _set_all_kol_tokens(workflow)


def patch_phase2(workflow: dict) -> list[str]:
    node = r8._node(workflow, "Insert Crawl Task (Phase 2)")
    code = node["parameters"]["jsCode"]
    updated = code.replace(
        "const USE_KOL_ASSISTANT=$env.KOL_FEISHU_BASE_ENABLED === '1';\n", "", 1
    )
    conditional = (
        "const APP_ID=USE_KOL_ASSISTANT ? $env.FEISHU_KOL_ASSISTANT_APP_ID : "
        "$env.FEISHU_KOL_LEGACY_BITABLE_APP_ID, APP_SECRET=USE_KOL_ASSISTANT ? "
        "$env.FEISHU_KOL_ASSISTANT_APP_SECRET : $env.FEISHU_KOL_LEGACY_BITABLE_APP_SECRET;"
    )
    target = (
        "const APP_ID=$env.FEISHU_KOL_ASSISTANT_APP_ID, "
        "APP_SECRET=$env.FEISHU_KOL_ASSISTANT_APP_SECRET;"
    )
    updated = updated.replace(conditional, target, 1)
    if "KOL_FEISHU_BASE_ENABLED" in updated or "FEISHU_KOL_LEGACY_BITABLE" in updated:
        raise RuntimeError("phase2 still has a legacy KOL Base route")
    if updated == code:
        return []
    node["parameters"]["jsCode"] = updated
    return [node["name"]]


def patch_media_discovery(workflow: dict) -> list[str]:
    node = r8._node(workflow, "Discover Media")
    code = node["parameters"]["jsCode"]
    updated = code
    for line in (
        "const USE_KOL_ASSISTANT=$env.KOL_FEISHU_BASE_ENABLED === '1';\n",
        "const KOL_CARDS_ENABLED=$env.KOL_FEISHU_CARDS_ENABLED === '1';\n",
    ):
        updated = updated.replace(line, "", 1)
    updated = re.sub(
        r"^const FRANKIE_LEGACY_OPEN_ID='ou_[^']+';\n?", "", updated,
        count=1, flags=re.MULTILINE,
    )
    updated = updated.replace(
        "const FEISHU_APP_ID=USE_KOL_ASSISTANT ? $env.FEISHU_KOL_ASSISTANT_APP_ID : $env.FEISHU_KOL_LEGACY_BITABLE_APP_ID;",
        "const FEISHU_APP_ID=$env.FEISHU_KOL_ASSISTANT_APP_ID;",
        1,
    )
    updated = updated.replace(
        "const FEISHU_APP_SECRET=USE_KOL_ASSISTANT ? $env.FEISHU_KOL_ASSISTANT_APP_SECRET : $env.FEISHU_KOL_LEGACY_BITABLE_APP_SECRET;",
        "const FEISHU_APP_SECRET=$env.FEISHU_KOL_ASSISTANT_APP_SECRET;",
        1,
    )
    updated = updated.replace(
        "const FRANKIE=KOL_CARDS_ENABLED ? $env.KOL_ASSISTANT_FRANKIE_UNION_ID : FRANKIE_LEGACY_OPEN_ID;",
        "const FRANKIE=$env.KOL_ASSISTANT_FRANKIE_UNION_ID;",
        1,
    )
    updated = updated.replace(
        "receive_id_type='+(KOL_CARDS_ENABLED?'union_id':'open_id')+'",
        "receive_id_type=union_id",
        1,
    )
    forbidden = (
        "KOL_FEISHU_BASE_ENABLED",
        "KOL_FEISHU_CARDS_ENABLED",
        "FEISHU_KOL_LEGACY_BITABLE",
        "FRANKIE_LEGACY_OPEN_ID",
    )
    if any(value in updated for value in forbidden):
        raise RuntimeError("media discovery still has a legacy KOL route")
    if updated == code:
        return []
    node["parameters"]["jsCode"] = updated
    return [node["name"]]


def validate_event_hub(workflow: dict) -> list[str]:
    for name in ("Warm Recap Handler", "Draft Action Handler", "TP Action Handler"):
        code = r8._node(workflow, name)["parameters"]["jsCode"]
        if "_delivery_identity === 'kol_assistant'" not in code:
            raise RuntimeError(f"{name}: missing KOL delivery marker")
        if "FEISHU_KOL_ASSISTANT_APP_ID" not in code:
            raise RuntimeError(f"{name}: missing KOL target identity")
        if "cli_a9457898bd78dccc" not in code:
            raise RuntimeError(f"{name}: missing historical App 3 branch")
        if "useKolAssistant ? $env.FEISHU_KOL_ASSISTANT_APP_ID :" not in code:
            raise RuntimeError(f"{name}: App ownership is not selected by delivery marker")
        if "useKolAssistant ? $env.FEISHU_KOL_ASSISTANT_APP_SECRET :" not in code:
            raise RuntimeError(f"{name}: App secret is not selected by delivery marker")
        if name == "Draft Action Handler":
            delivery_marker = (
                "delivery_identity=' + encodeURIComponent("
                "useKolAssistant ? 'kol_assistant' : 'app3')"
            )
            if delivery_marker not in code:
                raise RuntimeError(
                    "Draft Action Handler: delivery identity is not forwarded to service"
                )
    return []


PATCHERS = {
    "daily_discovery": patch_daily_discovery,
    "influencer_sync": patch_influencer_sync,
    "phase2_publish_check": patch_phase2,
    "media_discovery": patch_media_discovery,
    "event_hub_history_compat": validate_event_hub,
}


def migrate(client: r8.N8N, commit: bool) -> list[dict]:
    results = []
    for workflow_id, kind in WORKFLOWS.items():
        workflow = client.request("GET", f"/workflows/{workflow_id}")
        was_active = bool(workflow.get("active"))
        changed_nodes = PATCHERS[kind](workflow)
        if commit and changed_nodes:
            client.request("PUT", f"/workflows/{workflow_id}", r8._workflow_update_body(workflow))
            current = client.request("GET", f"/workflows/{workflow_id}")
            if was_active and not current.get("active"):
                client.request("POST", f"/workflows/{workflow_id}/activate")
        results.append({
            "id": workflow_id,
            "name": workflow.get("name"),
            "active_before": was_active,
            "changed_nodes": changed_nodes,
            "history_compat": kind == "event_hub_history_compat",
            "mode": "commit" if commit else "dry-run",
        })
    return results


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--commit", action="store_true")
    args = parser.parse_args()
    base_url = os.environ.get("N8N_BASE_URL", "").rstrip("/")
    api_key = os.environ.get("N8N_API_KEY", "")
    if not base_url or not api_key:
        raise SystemExit("N8N_BASE_URL and N8N_API_KEY are required")
    print(json.dumps(migrate(r8.N8N(base_url, api_key), args.commit), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

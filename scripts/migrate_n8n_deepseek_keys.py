"""Migrate hard-coded DeepSeek keys in n8n workflows to channel env vars.

Dry-run is the default.  The script never prints secret values.
"""
from __future__ import annotations

import argparse
import copy
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request


WORKFLOW_CHANNELS = {
    "1NLuHkoupe0SukJO": "KOL_DEEPSEEK_API_KEY",
    "DikpD5OJlkfluFnU": "KOL_DEEPSEEK_API_KEY",
    "ablDLAks2q7r98Zs": "SEO_DEEPSEEK_API_KEY",
    "bxqthAOVFjGviUEr": "SEO_DEEPSEEK_API_KEY",
    "ee779GzBI8Bj4Bx3": "SEO_DEEPSEEK_API_KEY",
    "b3r7PdyvlLBGFui7": "FEISHU_EVENTHUB_DEEPSEEK_API_KEY",
    "LFzjIGTTrOiERGTK": "FEISHU_EVENTHUB_DEEPSEEK_API_KEY",
    "YjTXaoWAcy89xZpT": "FEISHU_EVENTHUB_DEEPSEEK_API_KEY",
    "LE9r3DhrTLJuPRoS": "MANUAL_TOOLS_DEEPSEEK_API_KEY",
}

ZEABUR_GRAPHQL_URL = "https://api.zeabur.com/graphql"
ZEABUR_PROJECT_ID = "69856f0c2e156a6efa59a9a9"
N8N_PRODUCTION_DOMAIN = "frankiepan501.zeabur.app"
N8N_PRODUCTION_SERVICE_ID = "69856f0d2e156a6efa59a9ce"

SECRET_RE = re.compile(r"sk-[A-Za-z0-9_-]{16,}")
DEEPSEEK_ENV_RE = re.compile(r"\$env\.([A-Z0-9_]*DEEPSEEK_API_KEY)")
QUOTED_BEARER_RE = re.compile(r"(['\"])Bearer\s+sk-[A-Za-z0-9_-]{16,}\1")
QUOTED_KEY_RE = re.compile(r"(['\"])sk-[A-Za-z0-9_-]{16,}\1")


def resolve_service_by_domain(services: list[dict], domain: str) -> dict:
    matches = [
        service
        for service in services
        if domain in {
            str(row.get("domain") or "").strip()
            for row in service.get("domains") or []
        }
    ]
    if len(matches) != 1:
        raise ValueError(
            f"expected exactly one Zeabur service for production domain {domain}; found {len(matches)}"
        )
    service = matches[0]
    if service.get("_id") != N8N_PRODUCTION_SERVICE_ID:
        raise ValueError(
            "production domain resolved to unexpected service ID: "
            f"{service.get('_id')}; expected {N8N_PRODUCTION_SERVICE_ID}"
        )
    if service.get("status") != "RUNNING":
        raise ValueError(
            f"production n8n service is not RUNNING: {service.get('status')}"
        )
    return service


def verify_required_variables(rows: list[dict], required: set[str]) -> None:
    by_key = {}
    for row in rows:
        by_key.setdefault(row.get("key"), []).append(row)
    invalid = []
    for key in sorted(required):
        matches = by_key.get(key) or []
        if len(matches) != 1 or not str(matches[0].get("value") or "").strip():
            invalid.append(key)
    if invalid:
        raise ValueError(
            "required DeepSeek variable missing or empty on production n8n service: "
            + ", ".join(invalid)
        )


def _zeabur_request(api_key: str, query: str, variables: dict) -> dict:
    payload = json.dumps(
        {"query": query, "variables": variables},
        separators=(",", ":"),
    ).encode("utf-8")
    request = urllib.request.Request(
        ZEABUR_GRAPHQL_URL,
        data=payload,
        method="POST",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "User-Agent": "n8n-deepseek-key-migration",
        },
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        result = json.loads(response.read().decode("utf-8"))
    if result.get("errors"):
        raise ValueError("Zeabur preflight query failed")
    return result["data"]


def verify_production_environment(api_key: str, required: set[str]) -> dict:
    project_query = """
    query($projectID:ObjectID!){
      project(_id:$projectID){services{_id name status domains{domain}}}
    }
    """
    project = _zeabur_request(
        api_key,
        project_query,
        {"projectID": ZEABUR_PROJECT_ID},
    )["project"]
    service = resolve_service_by_domain(
        project.get("services") or [],
        N8N_PRODUCTION_DOMAIN,
    )
    variables_query = """
    query($serviceID:ObjectID!,$environmentID:ObjectID!){
      service(_id:$serviceID){variables(environmentID:$environmentID){key value}}
    }
    """
    environment_id = os.environ.get("ZEABUR_ENVIRONMENT_ID", "69856f0c86311f632dc2c2c9")
    rows = _zeabur_request(
        api_key,
        variables_query,
        {"serviceID": service["_id"], "environmentID": environment_id},
    )["service"]["variables"]
    verify_required_variables(rows, required)
    return service


def _guarded_header_expression(variable: str) -> str:
    return (
        "={{ (() => { const k = $env."
        + variable
        + "; if (!k) throw new Error('[DEEPSEEK_KEY_MISSING] "
        + variable
        + " not configured'); return 'Bearer ' + k; })() }}"
    )


def _migrate_code(source: str, variable: str) -> str:
    if not SECRET_RE.search(source):
        return source
    symbol = "__DEEPSEEK_API_KEY__"
    migrated = QUOTED_BEARER_RE.sub(f"'Bearer ' + {symbol}", source)
    migrated = QUOTED_KEY_RE.sub(symbol, migrated)
    if SECRET_RE.search(migrated):
        raise ValueError("unsupported DeepSeek key literal context")
    guard = (
        f"const {symbol} = $env.{variable};\n"
        f"if (!{symbol}) throw new Error('[DEEPSEEK_KEY_MISSING] {variable} not configured');\n"
    )
    return guard + migrated


def _migrate_value(value, variable: str, key: str | None = None):
    changes = 0
    if isinstance(value, dict):
        out = {}
        for child_key, child_value in value.items():
            out[child_key], count = _migrate_value(child_value, variable, child_key)
            changes += count
        return out, changes
    if isinstance(value, list):
        out = []
        for child in value:
            migrated, count = _migrate_value(child, variable)
            out.append(migrated)
            changes += count
        return out, changes
    if isinstance(value, str) and value == f"={{{{ 'Bearer ' + $env.{variable} }}}}":
        return _guarded_header_expression(variable), 1
    if not isinstance(value, str) or not SECRET_RE.search(value):
        return value, 0
    if key == "jsCode":
        return _migrate_code(value, variable), 1
    if re.fullmatch(r"Bearer\s+sk-[A-Za-z0-9_-]{16,}", value):
        return _guarded_header_expression(variable), 1
    raise ValueError(f"unsupported DeepSeek key field: {key or '<list>'}")


def migrate_workflow(workflow: dict, variable: str) -> tuple[dict, list[str]]:
    migrated = copy.deepcopy(workflow)
    changed_nodes = []
    for node in migrated.get("nodes") or []:
        original_params = node.get("parameters") or {}
        marker_text = json.dumps(original_params, ensure_ascii=False).lower()
        if "deepseek" not in marker_text and "api.deepseek.com" not in marker_text:
            continue
        params, count = _migrate_value(original_params, variable)
        if count:
            node["parameters"] = params
            changed_nodes.append(node.get("name") or "<unnamed>")
            if SECRET_RE.search(json.dumps(params, ensure_ascii=False)):
                raise ValueError("hard-coded DeepSeek key remains after migration")
    return migrated, changed_nodes


def verify_workflow(workflow: dict, variable: str, expected_nodes: list[str] | None = None) -> int:
    nodes = workflow.get("nodes") or []
    by_name = {node.get("name") or "<unnamed>": node for node in nodes}
    for name, node in by_name.items():
        serialized = json.dumps(node, ensure_ascii=False)
        marker_text = serialized.lower()
        if "deepseek" in marker_text or "api.deepseek.com" in marker_text:
            if SECRET_RE.search(serialized):
                raise ValueError(f"hard-coded DeepSeek key remains anywhere in workflow: {name}")
            referenced_variables = set(DEEPSEEK_ENV_RE.findall(serialized))
            wrong_variables = referenced_variables.difference({variable})
            if wrong_variables:
                raise ValueError(
                    f"wrong DeepSeek channel variable in node {name}: "
                    + ", ".join(sorted(wrong_variables))
                )
    names = expected_nodes or [
        name for name, node in by_name.items()
        if f"$env.{variable}" in json.dumps(node.get("parameters") or {}, ensure_ascii=False)
    ]
    if not names:
        raise ValueError(f"no nodes reference $env.{variable}")
    for name in names:
        node = by_name.get(name)
        if node is None:
            raise ValueError(f"migrated node missing after readback: {name}")
        serialized = json.dumps(node.get("parameters") or {}, ensure_ascii=False)
        if SECRET_RE.search(serialized):
            raise ValueError(f"hard-coded DeepSeek key remains after readback: {name}")
        if f"$env.{variable}" not in serialized:
            raise ValueError(f"wrong or missing channel variable after readback: {name}")
        if node.get("type") == "n8n-nodes-base.httpRequest" and "[DEEPSEEK_KEY_MISSING]" not in serialized:
            raise ValueError(f"HTTP node missing pre-request key guard: {name}")
        if node.get("type") == "n8n-nodes-base.code" and "[DEEPSEEK_KEY_MISSING]" not in serialized:
            raise ValueError(f"Code node missing pre-request key guard: {name}")
    if workflow.get("active") is not True:
        raise ValueError("workflow was not active after migration")
    version_id = workflow.get("versionId")
    active_version_id = workflow.get("activeVersionId")
    if not isinstance(version_id, str) or not version_id:
        raise ValueError("workflow versionId is missing after migration")
    if not isinstance(active_version_id, str) or not active_version_id:
        raise ValueError("workflow activeVersionId is missing after migration")
    if version_id != active_version_id:
        raise ValueError("workflow activeVersionId does not match saved versionId")
    return len(names)


def _request(base_url: str, api_key: str, method: str, path: str, payload=None):
    api_root = base_url.rstrip("/")
    if not api_root.endswith("/api/v1"):
        api_root += "/api/v1"
    data = None if payload is None else json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        api_root + path,
        data=data,
        method=method,
        headers={"X-N8N-API-KEY": api_key, "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        raw = response.read().decode("utf-8")
        return json.loads(raw) if raw else {}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--commit", action="store_true")
    parser.add_argument("--workflow-id", action="append", default=[])
    args = parser.parse_args()
    base_url = os.environ.get("N8N_BASE_URL", "").strip()
    api_key = os.environ.get("N8N_API_KEY", "").strip()
    if not base_url or not api_key:
        raise SystemExit("N8N_BASE_URL and N8N_API_KEY are required")

    reports = []
    selected = set(args.workflow_id)
    unknown = selected.difference(WORKFLOW_CHANNELS)
    if unknown:
        raise SystemExit("unknown workflow id(s): " + ", ".join(sorted(unknown)))
    targets = {
        workflow_id: variable
        for workflow_id, variable in WORKFLOW_CHANNELS.items()
        if not selected or workflow_id in selected
    }

    production_service = None
    if args.commit:
        zeabur_api_key = os.environ.get("ZEABUR_API_KEY", "").strip()
        if not zeabur_api_key:
            raise SystemExit("ZEABUR_API_KEY is required for production commit preflight")
        production_service = verify_production_environment(
            zeabur_api_key,
            set(targets.values()),
        )

    for workflow_id, variable in targets.items():
        current = _request(base_url, api_key, "GET", f"/workflows/{workflow_id}")
        migrated, nodes = migrate_workflow(current, variable)
        if args.commit and nodes:
            payload = {
                "name": migrated["name"],
                "nodes": migrated["nodes"],
                "connections": migrated.get("connections") or {},
                "settings": migrated.get("settings") or {},
            }
            _request(base_url, api_key, "PUT", f"/workflows/{workflow_id}", payload)
            if current.get("active") is True:
                _request(base_url, api_key, "POST", f"/workflows/{workflow_id}/activate")
        readback = _request(base_url, api_key, "GET", f"/workflows/{workflow_id}") if args.commit else migrated
        verified_nodes = verify_workflow(readback, variable)
        reports.append({
            "workflow_id": workflow_id,
            "variable": variable,
            "changed_nodes": nodes,
            "mode": "commit" if args.commit else "dry-run",
            "verified_nodes": verified_nodes,
            "production_service_id": production_service.get("_id") if production_service else None,
            "production_domain": N8N_PRODUCTION_DOMAIN if production_service else None,
        })
    print(json.dumps(reports, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

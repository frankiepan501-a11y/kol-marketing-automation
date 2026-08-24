"""Create the three KOL media-archive cron workflows without copying secrets.

Dry-run is the default.  On commit, the script reads the existing KOL dispatch
workflow and reuses its in-memory Authorization header.  The value is never
written to source files or printed in output.
"""

from __future__ import annotations

import argparse
from copy import deepcopy
import json
import os
from typing import Any
from urllib import error, parse, request
import uuid


SOURCE_AUTH_WORKFLOW_ID = "DUDrULRSOgt5KVVf"
CONTROLLER = "https://kol-auto.zeabur.app"


def extract_authorization(workflow: dict) -> str:
    for node in workflow.get("nodes") or []:
        parameters = node.get("parameters") or {}
        headers = ((parameters.get("headerParameters") or {}).get("parameters") or [])
        for header in headers:
            if str(header.get("name") or "").lower() == "authorization" and header.get("value"):
                return str(header["value"])
    raise RuntimeError("source KOL workflow has no Authorization header to reuse")


def _schedule_http(name: str, cron: str, method: str, url: str,
                   authorization: str) -> dict:
    schedule_name = "Schedule"
    call_name = "Call Controller"
    return {
        "name": name,
        "nodes": [
            {
                "id": str(uuid.uuid4()),
                "name": schedule_name,
                "type": "n8n-nodes-base.scheduleTrigger",
                "typeVersion": 1.1,
                "position": [240, 300],
                "parameters": {
                    "rule": {"interval": [{"field": "cronExpression", "expression": cron}]},
                },
            },
            {
                "id": str(uuid.uuid4()),
                "name": call_name,
                "type": "n8n-nodes-base.httpRequest",
                "typeVersion": 4.2,
                "position": [500, 300],
                "parameters": {
                    "method": method,
                    "url": url,
                    "sendHeaders": True,
                    "headerParameters": {
                        "parameters": [{"name": "Authorization", "value": authorization}],
                    },
                    "options": {"timeout": 60000},
                },
            },
        ],
        "connections": {
            schedule_name: {"main": [[{"node": call_name, "type": "main", "index": 0}]]},
        },
        "settings": {
            "executionOrder": "v1",
            "timezone": "Asia/Shanghai",
        },
    }


def build_workflows(authorization: str) -> list[dict]:
    return [
        _schedule_http(
            "KOL - 素材归档队列扫描 (每10分钟)", "*/10 * * * *", "POST",
            f"{CONTROLLER}/media-archive/tick", authorization,
        ),
        _schedule_http(
            "KOL - YouTube作品数据刷新 (每日09:15)", "15 9 * * *", "POST",
            f"{CONTROLLER}/media-archive/youtube-metrics/tick", authorization,
        ),
        _schedule_http(
            "KOL - 素材归档终端掉线检查 (每10分钟)", "*/10 * * * *", "GET",
            f"{CONTROLLER}/media-archive/worker/audit?stale_minutes=10&notify=true",
            authorization,
        ),
    ]


def public_plan(workflows: list[dict], activate: bool) -> dict:
    return {
        "mode": "dry-run",
        "source_authorization_workflow": SOURCE_AUTH_WORKFLOW_ID,
        "stores_new_credentials": False,
        "workflows": [
            {
                "name": workflow["name"],
                "cron": workflow["nodes"][0]["parameters"]["rule"]["interval"][0]["expression"],
                "method": workflow["nodes"][1]["parameters"]["method"],
                "url": workflow["nodes"][1]["parameters"]["url"],
                "activate": activate,
            }
            for workflow in workflows
        ],
    }


class N8nApi:
    def __init__(self, base_url: str, api_key: str):
        if not base_url or not api_key:
            raise ValueError("N8N_BASE_URL and N8N_API_KEY are required")
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key

    def call(self, method: str, path: str, data: dict | None = None) -> dict:
        body = None if data is None else json.dumps(data, ensure_ascii=False).encode("utf-8")
        req = request.Request(
            f"{self.base_url}{path}", data=body, method=method,
            headers={"X-N8N-API-KEY": self.api_key, "Content-Type": "application/json"},
        )
        try:
            with request.urlopen(req, timeout=45) as response:
                return json.loads(response.read().decode("utf-8"))
        except error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")[-1000:]
            raise RuntimeError(f"n8n API {method} {path} failed: HTTP {exc.code} {detail}") from exc

    def workflow(self, workflow_id: str) -> dict:
        return self.call("GET", f"/workflows/{parse.quote(workflow_id)}")

    def workflows(self) -> list[dict]:
        return self.call("GET", "/workflows?limit=250").get("data") or []

    def create(self, workflow: dict) -> str:
        payload = {key: deepcopy(workflow[key]) for key in ("name", "nodes", "connections", "settings")}
        result = self.call("POST", "/workflows", payload)
        workflow_id = str(result.get("id") or "")
        if not workflow_id:
            raise RuntimeError(f"n8n did not return workflow id for {workflow['name']}")
        return workflow_id

    def activate(self, workflow_id: str) -> None:
        self.call("POST", f"/workflows/{parse.quote(workflow_id)}/activate", {})


def commit(api: N8nApi, activate: bool) -> dict:
    source = api.workflow(SOURCE_AUTH_WORKFLOW_ID)
    workflows = build_workflows(extract_authorization(source))
    existing = {str(item.get("name") or ""): item for item in api.workflows()}
    created = []
    activated = []
    for workflow in workflows:
        if workflow["name"] in existing:
            raise RuntimeError(
                f"workflow already exists; refusing full replacement without manual diff: {workflow['name']}"
            )
        workflow_id = api.create(workflow)
        created.append({"id": workflow_id, "name": workflow["name"]})
        if activate:
            api.activate(workflow_id)
            activated.append(workflow_id)
    return {
        "mode": "commit",
        "created": created,
        "activated_ids": activated,
        "credentials_reused_in_memory": True,
        "credential_value_printed": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--commit", action="store_true")
    parser.add_argument("--activate", action="store_true")
    args = parser.parse_args()
    if args.activate and not args.commit:
        raise SystemExit("--activate requires --commit")
    placeholder = build_workflows("<reused at commit; never printed>")
    result = (
        commit(N8nApi(os.environ.get("N8N_BASE_URL", ""), os.environ.get("N8N_API_KEY", "")), args.activate)
        if args.commit else public_plan(placeholder, activate=args.activate)
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

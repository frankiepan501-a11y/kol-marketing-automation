"""R8: move only KOL Feishu calls in n8n to KOL媒体助手.

The script performs full workflow GET -> surgical node mutation -> full workflow
PUT.  It never writes workflow payloads to disk and never prints credentials.
Run without ``--commit`` for a read-only preview.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import urllib.error
import urllib.request


KOL_APP_ID_ENV = "FEISHU_KOL_ASSISTANT_APP_ID"
KOL_APP_SECRET_ENV = "FEISHU_KOL_ASSISTANT_APP_SECRET"
FRANKIE_UNION_ENV = "KOL_ASSISTANT_FRANKIE_UNION_ID"

WORKFLOWS = {
    "KNxD9ES93iyEs9mB": "daily_discovery",
    "LwP6SYw2FSrkENV0": "influencer_sync",
    "hgM7unABBW7hr5dw": "phase2_publish_check",
    "kIQFipyMAWTPMLdS": "media_discovery",
    "YjTXaoWAcy89xZpT": "event_hub",
}


def _target_token_body() -> str:
    return (
        "={{ { \"app_id\": $env.KOL_FEISHU_BASE_ENABLED === \"1\" ? $env."
        + KOL_APP_ID_ENV
        + " : $env.FEISHU_KOL_LEGACY_BITABLE_APP_ID, \"app_secret\": $env.KOL_FEISHU_BASE_ENABLED === \"1\" ? $env."
        + KOL_APP_SECRET_ENV
        + " : $env.FEISHU_KOL_LEGACY_BITABLE_APP_SECRET } }}"
    )


def _replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count == 0 and new in text:
        return text
    if count != 1:
        raise RuntimeError(f"{label}: expected one legacy occurrence, found {count}")
    return text.replace(old, new, 1)


def _replace_line(text: str, pattern: str, replacement: str, label: str) -> str:
    updated, count = re.subn(pattern, replacement, text, count=1, flags=re.MULTILINE)
    if count == 0 and replacement in text:
        return text
    if count != 1:
        raise RuntimeError(f"{label}: expected one legacy line, found {count}")
    return updated


def _node(workflow: dict, name: str) -> dict:
    matches = [node for node in workflow.get("nodes", []) if node.get("name") == name]
    if len(matches) != 1:
        raise RuntimeError(f"{workflow.get('id')}: node {name!r} count={len(matches)}")
    return matches[0]


def _patch_token_node(workflow: dict, name: str = "HTTP — token KOL") -> list[str]:
    node = _node(workflow, name)
    before = str(node.get("parameters", {}).get("jsonBody") or "")
    after = _target_token_body()
    if before == after:
        return []
    node["parameters"]["jsonBody"] = after
    return [name]


def patch_daily_discovery(workflow: dict) -> list[str]:
    changes = _patch_token_node(workflow)
    node = _node(workflow, "Code — 日报引擎")
    code = node["parameters"]["jsCode"]
    if "const KOL_CARDS_ENABLED =" in code:
        return changes
    updated = _replace_line(
        code,
        r"^const FRANKIE_UNION_ID = \$env\.KOL_ASSISTANT_FRANKIE_UNION_ID;$",
        "const KOL_CARDS_ENABLED = $env.KOL_FEISHU_CARDS_ENABLED === '1';\n"
        "const FRANKIE_LEGACY_OPEN_ID = 'ou_629ce01f4bc31de078e10fcb038dbf78';\n"
        f"const FRANKIE_UNION_ID = $env.{FRANKIE_UNION_ENV};",
        "daily staged identities",
    )
    updated = _replace_line(
        updated,
        r"^if \(!FRANKIE_UNION_ID\) throw new Error\('KOL assistant Frankie union_id missing'\);$",
        "if (KOL_CARDS_ENABLED && !FRANKIE_UNION_ID) throw new Error('KOL assistant Frankie union_id missing');",
        "daily staged identity gate",
    )
    updated = _replace_once(
        updated,
        "if (SEND_FRANKIE) targets.push(['union_id', FRANKIE_UNION_ID]);",
        "if (SEND_FRANKIE) targets.push(KOL_CARDS_ENABLED ? ['union_id', FRANKIE_UNION_ID] : ['open_id', FRANKIE_LEGACY_OPEN_ID]);",
        "daily Frankie recipient",
    )
    updated = _replace_once(
        updated,
        "Authorization: 'Bearer ' + t2, 'Content-Type': 'application/json; charset=utf-8'",
        "Authorization: 'Bearer ' + (KOL_CARDS_ENABLED ? t2 : t1), 'Content-Type': 'application/json; charset=utf-8'",
        "daily card sender",
    )
    if updated != code:
        node["parameters"]["jsCode"] = updated
        changes.append(node["name"])
    return changes


def patch_influencer_sync(workflow: dict) -> list[str]:
    return _patch_token_node(workflow)


def patch_phase2(workflow: dict) -> list[str]:
    node = _node(workflow, "Insert Crawl Task (Phase 2)")
    code = node["parameters"]["jsCode"]
    app_line = (
        f"const APP_ID=USE_KOL_ASSISTANT ? $env.{KOL_APP_ID_ENV} : $env.FEISHU_KOL_LEGACY_BITABLE_APP_ID, "
        f"APP_SECRET=USE_KOL_ASSISTANT ? $env.{KOL_APP_SECRET_ENV} : $env.FEISHU_KOL_LEGACY_BITABLE_APP_SECRET;"
    )
    if "const USE_KOL_ASSISTANT=" in code:
        updated = _replace_line(
            code,
            r"^const APP_ID=USE_KOL_ASSISTANT \? \$env\.FEISHU_KOL_ASSISTANT_APP_ID : \$env\.FEISHU_APP_ID, APP_SECRET=USE_KOL_ASSISTANT \? \$env\.FEISHU_KOL_ASSISTANT_APP_SECRET : \$env\.FEISHU_APP_SECRET;$",
            app_line,
            "phase2 explicit legacy credentials",
        )
    else:
        updated = _replace_line(
            code,
            r"^const APP_ID=\$env\.FEISHU_KOL_ASSISTANT_APP_ID, APP_SECRET=\$env\.FEISHU_KOL_ASSISTANT_APP_SECRET;$",
            "const USE_KOL_ASSISTANT=$env.KOL_FEISHU_BASE_ENABLED === '1';\n" + app_line,
            "phase2 credentials",
        )
    if updated == code:
        return []
    node["parameters"]["jsCode"] = updated
    return [node["name"]]


def patch_media_discovery(workflow: dict) -> list[str]:
    node = _node(workflow, "Discover Media")
    code = node["parameters"]["jsCode"]
    use_line = "const USE_KOL_ASSISTANT=$env.KOL_FEISHU_BASE_ENABLED === '1';"
    updated = code.replace(use_line + "\n" + use_line, use_line, 1)
    updated = _replace_once(
        updated,
        "const FEISHU_APP_ID=USE_KOL_ASSISTANT ? $env.FEISHU_KOL_ASSISTANT_APP_ID : $env.FEISHU_APP_ID;",
        f"const FEISHU_APP_ID=USE_KOL_ASSISTANT ? $env.{KOL_APP_ID_ENV} : $env.FEISHU_KOL_LEGACY_BITABLE_APP_ID;",
        "media app id",
    )
    updated = _replace_once(
        updated,
        "const FEISHU_APP_SECRET=USE_KOL_ASSISTANT ? $env.FEISHU_KOL_ASSISTANT_APP_SECRET : $env.FEISHU_APP_SECRET;",
        f"const FEISHU_APP_SECRET=USE_KOL_ASSISTANT ? $env.{KOL_APP_SECRET_ENV} : $env.FEISHU_KOL_LEGACY_BITABLE_APP_SECRET;",
        "media app secret",
    )
    updated = _replace_line(
        updated,
        r"^const FRANKIE = \$env\.KOL_ASSISTANT_FRANKIE_UNION_ID;$",
        "const KOL_CARDS_ENABLED=$env.KOL_FEISHU_CARDS_ENABLED === '1';\n"
        "const FRANKIE_LEGACY_OPEN_ID='ou_629ce01f4bc31de078e10fcb038dbf78';\n"
        f"const FRANKIE=KOL_CARDS_ENABLED ? $env.{FRANKIE_UNION_ENV} : FRANKIE_LEGACY_OPEN_ID;",
        "media Frankie identity",
    )
    updated = _replace_once(
        updated,
        "receive_id_type=union_id",
        "receive_id_type='+(KOL_CARDS_ENABLED?'union_id':'open_id')+'",
        "media Frankie recipient type",
    )
    if updated == code:
        return []
    node["parameters"]["jsCode"] = updated
    return [node["name"]]


_CREDENTIALS = re.compile(
    r"body: \{ app_id: '([^']+)', app_secret: '([^']+)' \}, json: true \}"
)


def _conditional_token_line(line: str, var_name: str, helper: str) -> str:
    match = _CREDENTIALS.search(line)
    if not match:
        if f"$env.{KOL_APP_ID_ENV}" in line:
            return line
        raise RuntimeError(f"event hub {var_name}: legacy credentials not found")
    legacy_id, legacy_secret = match.groups()
    prefix = line[: match.start()]
    suffix = line[match.end() :]
    body = (
        "body: { app_id: useKolAssistant ? $env."
        + KOL_APP_ID_ENV
        + f" : '{legacy_id}', app_secret: useKolAssistant ? $env."
        + KOL_APP_SECRET_ENV
        + f" : '{legacy_secret}' }}, json: true }}"
    )
    return prefix + body + suffix


def _patch_handler(code: str, *, has_base_token: bool, helper: str) -> str:
    if "const useKolAssistant =" not in code:
        code = _replace_once(
            code,
            "const av = data.card_action || {};",
            "const av = data.card_action || {};\n"
            "const useKolAssistant = av._delivery_identity === 'kol_assistant';",
            "event hub delivery marker",
        )
    lines = code.splitlines()
    for index, line in enumerate(lines):
        if re.match(r"^const tr3 = ", line):
            lines[index] = _conditional_token_line(line, "tr3", helper)
        elif has_base_token and re.match(r"^const tr2 = ", line):
            lines[index] = _conditional_token_line(line, "tr2", helper)
    return "\n".join(lines)


def _patch_parse_message(code: str) -> str:
    marker = "const kolR8Key = cardAction._kol_idempotency_key || '';"
    if marker in code:
        return code
    guard = """const kolR8Key = cardAction._kol_idempotency_key || '';
  if (kolR8Key) {
    const state = $getWorkflowStaticData('global');
    const now = Date.now();
    state.kolR8Seen = state.kolR8Seen || {};
    for (const [key, seenAt] of Object.entries(state.kolR8Seen)) {
      if (now - Number(seenAt || 0) > 7 * 24 * 3600 * 1000) delete state.kolR8Seen[key];
    }
    if (state.kolR8Seen[kolR8Key]) {
      return [{ json: { command: 'kol_r8_duplicate', duplicate: true, idempotency_key: kolR8Key } }];
    }
    state.kolR8Seen[kolR8Key] = now;
  }

  """
    return _replace_once(
        code,
        "  const actionName = cardAction.action || '';",
        guard + "const actionName = cardAction.action || '';",
        "event hub KOL idempotency gate",
    )


def patch_event_hub(workflow: dict) -> list[str]:
    changes = []
    for name, has_base, helper in (
        ("Warm Recap Handler", True, "this.helpers.httpRequest"),
        ("Draft Action Handler", True, "HR"),
        ("TP Action Handler", False, "HR"),
    ):
        node = _node(workflow, name)
        code = node["parameters"]["jsCode"]
        updated = _patch_handler(code, has_base_token=has_base, helper=helper)
        if name == "Draft Action Handler" and "delivery_identity=" not in updated:
            updated = _replace_once(
                updated,
                "&operator_open_id=' + encodeURIComponent(opId)",
                "&operator_open_id=' + encodeURIComponent(opId) + "
                "'&delivery_identity=' + encodeURIComponent(useKolAssistant ? 'kol_assistant' : 'app3')",
                "draft regen delivery identity",
            )
        if updated != code:
            node["parameters"]["jsCode"] = updated
            changes.append(name)
    parse_node = _node(workflow, "Parse Message")
    parse_code = parse_node["parameters"]["jsCode"]
    parse_updated = _patch_parse_message(parse_code)
    if parse_updated != parse_code:
        parse_node["parameters"]["jsCode"] = parse_updated
        changes.append("Parse Message")
    return changes


PATCHERS = {
    "daily_discovery": patch_daily_discovery,
    "influencer_sync": patch_influencer_sync,
    "phase2_publish_check": patch_phase2,
    "media_discovery": patch_media_discovery,
    "event_hub": patch_event_hub,
}


class N8N:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key

    def request(self, method: str, path: str, body: dict | None = None) -> dict:
        payload = None if body is None else json.dumps(body).encode("utf-8")
        request = urllib.request.Request(
            self.base_url + path,
            data=payload,
            method=method,
            headers={
                "X-N8N-API-KEY": self.api_key,
                "Content-Type": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", "replace")[:500]
            raise RuntimeError(f"n8n {method} {path} -> HTTP {exc.code}: {detail}") from exc


def _workflow_update_body(workflow: dict) -> dict:
    body = {
        "name": workflow["name"],
        "nodes": workflow["nodes"],
        "connections": workflow["connections"],
        "settings": workflow.get("settings") or {},
    }
    if "staticData" in workflow:
        body["staticData"] = workflow["staticData"]
    return body


def migrate(client: N8N, commit: bool) -> list[dict]:
    results = []
    for workflow_id, kind in WORKFLOWS.items():
        workflow = client.request("GET", f"/workflows/{workflow_id}")
        was_active = bool(workflow.get("active"))
        changed_nodes = PATCHERS[kind](workflow)
        if commit and changed_nodes:
            body = _workflow_update_body(workflow)
            client.request("PUT", f"/workflows/{workflow_id}", body)
            current = client.request("GET", f"/workflows/{workflow_id}")
            if was_active and not current.get("active"):
                client.request("POST", f"/workflows/{workflow_id}/activate")
        results.append(
            {
                "id": workflow_id,
                "name": workflow["name"],
                "active_before": was_active,
                "changed_nodes": changed_nodes,
                "mode": "commit" if commit else "dry-run",
            }
        )
    return results


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--commit", action="store_true")
    args = parser.parse_args()
    base_url = os.environ.get("N8N_BASE_URL", "").rstrip("/")
    api_key = os.environ.get("N8N_API_KEY", "")
    if not base_url or not api_key:
        raise SystemExit("N8N_BASE_URL and N8N_API_KEY are required")
    results = migrate(N8N(base_url, api_key), args.commit)
    print(json.dumps(results, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

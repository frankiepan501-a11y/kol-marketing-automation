"""Patch the production Event Hub draft-regeneration callback safely.

Dry-run is the default. The workflow is fetched in full, only the target Code
node is changed, and no workflow parameters or credentials are printed.
"""
from __future__ import annotations

import argparse
import copy
import json
import os
import urllib.request


WORKFLOW_ID = "YjTXaoWAcy89xZpT"
NODE_NAME = "Draft Action Handler"

_HELPER = r"""
async function markRegenCard(template, title, msg) {
  const msgId = data.message_id || '';
  if (!msgId) return;
  try {
    const card = { config: { wide_screen_mode: true }, header: { template: template, title: { tag: 'plain_text', content: title } }, elements: [{ tag: 'div', text: { tag: 'lark_md', content: msg } }] };
    await HR({ method: 'PATCH', url: 'https://open.feishu.cn/open-apis/im/v1/messages/' + msgId, headers: { 'Authorization': 'Bearer ' + token3, 'Content-Type': 'application/json' }, body: { content: JSON.stringify(card) }, json: true });
  } catch (e) {}
}
""".strip()

_OLD_URL = (
    "url: 'https://kol-auto.zeabur.app/draft/regen?record_id=' + rid + "
    "'&feedback=' + fb + '&async_mode=true',"
)
_NEW_URL = (
    "url: 'https://kol-auto.zeabur.app/draft/regen?record_id=' + rid + "
    "'&feedback=' + fb + '&async_mode=true&message_id=' + "
    "encodeURIComponent(data.message_id || '') + '&approver=' + encodeURIComponent(approver) + "
    "'&operator_open_id=' + encodeURIComponent(opId),"
)
_OLD_ERROR = "if (err) { reply = '⚠️ 重生调用异常: ' + err + ' (by ' + approver + ')'; }"
_NEW_ERROR = (
    "if (err) { const why = /KOL AI is not configured|missing KOL_DEEPSEEK_API_KEY/.test(err) "
    "? 'KOL AI 配置缺失，未生成新草稿，请联系系统管理员。' : "
    "('重生调用异常: ' + err); reply = '⚠️ ' + why + ' (by ' + approver + ')'; "
    "await markRegenCard('red', '⚠️ [重生失败] 未生成新草稿', reply); }"
)
_OLD_ACCEPTED = (
    "else if (rr && rr.ok && (rr.accepted || rr.job_id)) { const note = rr.already_running ? "
    "'已有重生任务在处理中' : '已提交后台重生任务'; reply = '🔁 ' + note + "
    "(decodeURIComponent(fb) ? ' [已按你填的方向]' : '') + ', 稍后新审核卡会发来 (by ' + "
    "approver + ')'; await markCard('退回重生', reply); }"
)
_NEW_ACCEPTED = (
    "else if (rr && rr.ok && (rr.accepted || rr.job_id)) { reply = '🔄 后台正在生成新版草稿' + "
    "(decodeURIComponent(fb) ? ' [已按你填的方向]' : '') + '，完成或失败后会更新本卡 (by ' + "
    "approver + ')'; await markRegenCard('orange', '🔄 [重生处理中] 后台正在生成新版草稿', reply); }"
)
_OLD_SYNC_SUCCESS = (
    "else if (rr && rr.ok && rr.new_rid) { reply = '🔁 已真重新生成新版草稿(第' + "
    "(rr.retries || 1) + '次)' + (decodeURIComponent(fb) ? ' [已按你填的方向]' : '') + "
    "', 稍后新审核卡会发来 (by ' + approver + ')'; await markCard('退回重生', reply); }"
)
_NEW_SYNC_SUCCESS = (
    "else if (rr && rr.ok && rr.new_rid) { reply = '✅ 已生成新版草稿(第' + (rr.retries || 1) + "
    "'次)，新审核卡会按正常流程发出 (by ' + approver + ')'; await markRegenCard('green', "
    "'✅ [重生完成] 新草稿已生成', reply); }"
)
_OLD_SKIP = (
    "else if (rr && rr.skip) { reply = 'ℹ️ ' + rr.skip + ' (by ' + approver + ')'; "
    "await markCard('退回重生', reply); }"
)
_NEW_SKIP = (
    "else if (rr && rr.skip) { reply = 'ℹ️ ' + rr.skip + ' (by ' + approver + ')'; "
    "await markRegenCard('grey', 'ℹ️ [无需重生] 草稿状态已变化', reply); }"
)


def patch_js(source: str) -> tuple[str, bool]:
    markers = ("rr.suppress_reply", "[重生处理中]", "&message_id=", "async function markRegenCard")
    if all(marker in source for marker in markers):
        return source, False
    required = (_OLD_URL, _OLD_ERROR, _OLD_ACCEPTED, _OLD_SYNC_SUCCESS, _OLD_SKIP, "\n\nlet reply = '';" )
    missing = [part[:60] for part in required if part not in source]
    if missing:
        raise ValueError("Event Hub draft_regen source does not match the verified production version")

    patched = source.replace("\n\nlet reply = '';", "\n\n" + _HELPER + "\n\nlet reply = '';", 1)
    patched = patched.replace(_OLD_URL, _NEW_URL, 1)
    patched = patched.replace(
        _OLD_ERROR,
        "if (rr && rr.suppress_reply) return [];\n      " + _NEW_ERROR,
        1,
    )
    patched = patched.replace(_OLD_ACCEPTED, _NEW_ACCEPTED, 1)
    patched = patched.replace(_OLD_SYNC_SUCCESS, _NEW_SYNC_SUCCESS, 1)
    patched = patched.replace(_OLD_SKIP, _NEW_SKIP, 1)
    verify_js(patched)
    return patched, True


def verify_js(source: str) -> None:
    for marker in (
        "rr.suppress_reply",
        "return [];",
        "&message_id=",
        "&approver=",
        "&operator_open_id=",
        "[重生处理中]",
        "[重生失败]",
        "async function markRegenCard",
    ):
        if marker not in source:
            raise ValueError(f"patched Event Hub node missing marker: {marker}")
    if "await markCard('退回重生', reply)" in source:
        raise ValueError("accepted callback still marks the card as completed")


def patch_workflow(workflow: dict) -> tuple[dict, bool]:
    patched = copy.deepcopy(workflow)
    nodes = patched.get("nodes") or []
    target = next((node for node in nodes if node.get("name") == NODE_NAME), None)
    if target is None:
        raise ValueError(f"missing node: {NODE_NAME}")
    source = str((target.get("parameters") or {}).get("jsCode") or "")
    new_source, changed = patch_js(source)
    if changed:
        target.setdefault("parameters", {})["jsCode"] = new_source
    return patched, changed


def verify_workflow(before: dict, after: dict) -> None:
    if len(before.get("nodes") or []) != len(after.get("nodes") or []):
        raise ValueError("node count changed")
    if before.get("connections") != after.get("connections"):
        raise ValueError("workflow connections changed")
    if before.get("settings") != after.get("settings"):
        raise ValueError("workflow settings changed")
    before_nodes = {node.get("name"): node for node in before.get("nodes") or []}
    after_nodes = {node.get("name"): node for node in after.get("nodes") or []}
    if set(before_nodes) != set(after_nodes):
        raise ValueError("workflow node names changed")
    for name, before_node in before_nodes.items():
        if name == NODE_NAME:
            before_copy = copy.deepcopy(before_node)
            after_copy = copy.deepcopy(after_nodes[name])
            before_copy.setdefault("parameters", {})["jsCode"] = "<target-js>"
            after_copy.setdefault("parameters", {})["jsCode"] = "<target-js>"
            if before_copy != after_copy:
                raise ValueError("target node changed outside jsCode")
        elif before_node != after_nodes[name]:
            raise ValueError(f"non-target node changed: {name}")
    node = next(node for node in after.get("nodes") or [] if node.get("name") == NODE_NAME)
    verify_js(str((node.get("parameters") or {}).get("jsCode") or ""))


def verify_rollback(expected: dict, actual: dict) -> None:
    expected_nodes = {node.get("name"): node for node in expected.get("nodes") or []}
    actual_nodes = {node.get("name"): node for node in actual.get("nodes") or []}
    if expected_nodes != actual_nodes:
        raise ValueError("Event Hub rollback node readback mismatch")
    if expected.get("connections") != actual.get("connections"):
        raise ValueError("Event Hub rollback connections readback mismatch")
    if expected.get("settings") != actual.get("settings"):
        raise ValueError("Event Hub rollback settings readback mismatch")
    if bool(expected.get("active")) != bool(actual.get("active")):
        raise ValueError("Event Hub rollback active state mismatch")


def _request(base_url: str, api_key: str, method: str, path: str, payload=None):
    root = base_url.rstrip("/")
    if not root.endswith("/api/v1"):
        root += "/api/v1"
    data = None if payload is None else json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        root + path,
        data=data,
        method=method,
        headers={"X-N8N-API-KEY": api_key, "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        raw = response.read().decode("utf-8")
        return json.loads(raw) if raw else {}


def _payload(workflow: dict) -> dict:
    return {
        "name": workflow["name"],
        "nodes": workflow["nodes"],
        "connections": workflow.get("connections") or {},
        "settings": workflow.get("settings") or {},
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--commit", action="store_true")
    args = parser.parse_args()
    base_url = os.environ.get("N8N_BASE_URL", "").strip()
    api_key = os.environ.get("N8N_API_KEY", "").strip()
    if not base_url or not api_key:
        raise SystemExit("N8N_BASE_URL and N8N_API_KEY are required")

    current = _request(base_url, api_key, "GET", f"/workflows/{WORKFLOW_ID}")
    patched, changed = patch_workflow(current)
    verify_workflow(current, patched)
    if args.commit and changed:
        try:
            _request(base_url, api_key, "PUT", f"/workflows/{WORKFLOW_ID}", _payload(patched))
            if current.get("active") is True:
                _request(base_url, api_key, "POST", f"/workflows/{WORKFLOW_ID}/activate")
            readback = _request(base_url, api_key, "GET", f"/workflows/{WORKFLOW_ID}")
            verify_workflow(current, readback)
            if readback.get("active") is not True or readback.get("versionId") != readback.get("activeVersionId"):
                raise ValueError("Event Hub readback is not active/published")
        except Exception:
            _request(base_url, api_key, "PUT", f"/workflows/{WORKFLOW_ID}", _payload(current))
            if current.get("active") is True:
                _request(base_url, api_key, "POST", f"/workflows/{WORKFLOW_ID}/activate")
            restored = _request(base_url, api_key, "GET", f"/workflows/{WORKFLOW_ID}")
            verify_rollback(current, restored)
            raise
    print(json.dumps({
        "workflow_id": WORKFLOW_ID,
        "node": NODE_NAME,
        "mode": "commit" if args.commit else "dry-run",
        "changed": changed,
        "active_before": bool(current.get("active")),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

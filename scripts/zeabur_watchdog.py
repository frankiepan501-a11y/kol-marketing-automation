"""External Zeabur watchdog for the Tokyo server and core KOL services.

This script is intentionally stdlib-only so it can run from GitHub Actions
without installing the application dependencies.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


ZEABUR_GRAPHQL = "https://api.zeabur.com/graphql"
DEFAULT_PROJECT_ID = "69856f0c2e156a6efa59a9a9"
DEFAULT_ENVIRONMENT_ID = "69856f0c86311f632dc2c2c9"
DEFAULT_SERVER_ID = "69856dfd2a96ae7705ff2930"

DEFAULT_SERVICES = [
    {
        "name": "n8n-hual",
        "service_id": "69856f0d2e156a6efa59a9ce",
        "health_url": "https://frankiepan501.zeabur.app/healthz",
        "restart_on_fail": True,
    },
    {
        "name": "kol-automation",
        "service_id": "69eae010c5278d4159c1f664",
        "health_url": "https://kol-auto.zeabur.app/health",
        "restart_on_fail": True,
    },
]

SERVER_QUERY = """
query Watchdog($projectID: ObjectID!) {
  servers {
    _id
    name
    provider
    hasK3s
    provisioningStatus
    events { message time }
    status {
      isOnline
      vmStatus
      totalCPU
      usedCPU
      totalMemory
      usedMemory
      totalDisk
      usedDisk
    }
  }
  project(_id: $projectID) {
    services {
      _id
      name
      status
      suspendedAt
      domains { domain }
    }
  }
}
"""

RESTART_MUTATION = """
mutation RestartService($serviceID: ObjectID!, $environmentID: ObjectID!) {
  restartService(serviceID: $serviceID, environmentID: $environmentID)
}
"""

DEPLOYMENTS_QUERY = """
query WatchdogDeployments($serviceID: ObjectID!, $environmentID: ObjectID!, $perPage: Int!) {
  deployments(serviceID: $serviceID, environmentID: $environmentID, perPage: $perPage) {
    edges {
      node {
        _id
        serviceID
        status
        createdAt
        startedAt
        finishedAt
        commitSHA
        commitMessage
      }
    }
  }
}
"""

BUILD_LOGS_QUERY = """
query WatchdogBuildLogs($deploymentID: ObjectID!) {
  buildLogs(deploymentID: $deploymentID) {
    message
    timestamp
  }
}
"""

DEFAULT_DEPLOYMENT_FAILURE_STATUSES = {"FAILED"}
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


@dataclass
class Issue:
    key: str
    severity: str
    message: str
    target: str = ""


@dataclass
class ProbeResult:
    ok: bool
    status: int | None
    elapsed_ms: int
    error: str = ""


def utc_now_ts() -> int:
    return int(time.time())


def iso_utc(ts: int | None = None) -> str:
    if ts is None:
        ts = utc_now_ts()
    return datetime.fromtimestamp(ts, timezone.utc).isoformat()


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return int(value)


def env_set(name: str, default: set[str]) -> set[str]:
    value = os.getenv(name)
    if not value or not value.strip():
        return set(default)
    return {part.upper() for part in split_targets(value)}


def load_json_env(name: str, default: Any) -> Any:
    value = os.getenv(name)
    if not value:
        return default
    return json.loads(value)


def split_targets(value: str) -> list[str]:
    if not value:
        return []
    return [part for part in re.split(r"[\s,;]+", value.strip()) if part]


def parse_utc_ts(value: str | None) -> int | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return int(dt.timestamp())


def strip_ansi(value: str) -> str:
    return ANSI_RE.sub("", value or "").strip()


def safe_print(value: Any, file: Any = None) -> None:
    target = file or sys.stdout
    text = str(value)
    encoding = getattr(target, "encoding", None) or "utf-8"
    safe = text.encode(encoding, errors="replace").decode(encoding, errors="replace")
    print(safe, file=target)


def http_json(
    url: str,
    payload: dict[str, Any],
    headers: dict[str, str] | None = None,
    timeout: int = 20,
) -> dict[str, Any]:
    data = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 zeabur-watchdog/1.0",
            **(headers or {}),
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} {url}: {body}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"URL error {url}: {exc}") from exc


def should_retry_error(exc: Exception) -> bool:
    text = str(exc)
    return "URL error" in text or "HTTP 500" in text or "HTTP 502" in text or "HTTP 503" in text or "HTTP 504" in text


def zeabur_graphql(query: str, variables: dict[str, Any]) -> dict[str, Any]:
    key = os.getenv("ZEABUR_API_KEY", "").strip()
    if not key:
        raise RuntimeError("ZEABUR_API_KEY is required")
    attempts = max(1, env_int("WATCHDOG_ZEABUR_API_RETRIES", 3))
    last_exc: Exception | None = None
    for attempt in range(attempts):
        try:
            data = http_json(
                ZEABUR_GRAPHQL,
                {"query": query, "variables": variables},
                headers={"Authorization": f"Bearer {key}"},
                timeout=30,
            )
            break
        except Exception as exc:
            last_exc = exc
            if attempt >= attempts - 1 or not should_retry_error(exc):
                raise
            time.sleep(1.5 * (attempt + 1))
    else:
        raise last_exc or RuntimeError("Zeabur GraphQL failed")
    if data.get("errors"):
        raise RuntimeError(json.dumps(data["errors"], ensure_ascii=False))
    return data["data"]


def load_state(path: str) -> dict[str, Any]:
    if not path or not os.path.exists(path):
        return {"alerts": {}, "restarts": {}, "deployments": {}}
    with open(path, "r", encoding="utf-8") as fh:
        state = json.load(fh)
    state.setdefault("alerts", {})
    state.setdefault("restarts", {})
    state.setdefault("deployments", {})
    return state


def save_state(path: str, state: dict[str, Any]) -> None:
    if not path:
        return
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(state, fh, ensure_ascii=False, indent=2, sort_keys=True)
    os.replace(tmp, path)


def pct(used: Any, total: Any) -> float | None:
    if not used or not total:
        return None
    total_f = float(total)
    if total_f <= 0:
        return None
    return float(used) / total_f * 100.0


def severity_for(value: float, warning: int, critical: int) -> str | None:
    if value >= critical:
        return "critical"
    if value >= warning:
        return "warning"
    return None


def evaluate_server(server: dict[str, Any]) -> list[Issue]:
    status = server.get("status") or {}
    name = server.get("name") or server.get("_id", "server")
    issues: list[Issue] = []
    if not status.get("isOnline"):
        issues.append(Issue("server_offline", "critical", f"{name} is offline", name))
        return issues
    if status.get("vmStatus") and status.get("vmStatus") != "RUNNING":
        issues.append(
            Issue(
                "server_vm_not_running",
                "critical",
                f"{name} vmStatus={status.get('vmStatus')}",
                name,
            )
        )

    checks = [
        ("cpu_high", "CPU", pct(status.get("usedCPU"), status.get("totalCPU")), 90, 95),
        (
            "memory_high",
            "memory",
            pct(status.get("usedMemory"), status.get("totalMemory")),
            90,
            95,
        ),
        ("disk_high", "disk", pct(status.get("usedDisk"), status.get("totalDisk")), 85, 95),
    ]
    for key, label, value, warning, critical in checks:
        if value is None:
            continue
        sev = severity_for(value, warning, critical)
        if sev:
            issues.append(
                Issue(key, sev, f"{name} {label} usage {value:.1f}% exceeds {sev} threshold", name)
            )
    return issues


def probe_url(url: str, timeout: int = 15) -> ProbeResult:
    attempts = max(1, env_int("WATCHDOG_HEALTH_RETRIES", 2))
    last: ProbeResult | None = None
    for attempt in range(attempts):
        started = time.monotonic()
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 zeabur-watchdog/1.0"})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                elapsed = int((time.monotonic() - started) * 1000)
                status = getattr(resp, "status", None)
                return ProbeResult(
                    ok=bool(status and 200 <= status < 400),
                    status=status,
                    elapsed_ms=elapsed,
                )
        except urllib.error.HTTPError as exc:
            elapsed = int((time.monotonic() - started) * 1000)
            last = ProbeResult(
                ok=False,
                status=exc.code,
                elapsed_ms=elapsed,
                error=f"HTTP {exc.code}",
            )
            if exc.code < 500 or attempt >= attempts - 1:
                return last
        except Exception as exc:
            elapsed = int((time.monotonic() - started) * 1000)
            last = ProbeResult(ok=False, status=None, elapsed_ms=elapsed, error=str(exc))
            if attempt >= attempts - 1:
                return last
        time.sleep(1.0 * (attempt + 1))
    return last or ProbeResult(ok=False, status=None, elapsed_ms=0, error="probe failed")


def restart_service(service_id: str, environment_id: str, dry_run: bool) -> bool:
    if dry_run:
        return True
    data = zeabur_graphql(
        RESTART_MUTATION,
        {"serviceID": service_id, "environmentID": environment_id},
    )
    return bool(data.get("restartService"))


def fetch_service_deployments(service_id: str, environment_id: str, per_page: int) -> list[dict[str, Any]]:
    data = zeabur_graphql(
        DEPLOYMENTS_QUERY,
        {"serviceID": service_id, "environmentID": environment_id, "perPage": per_page},
    )
    edges = ((data.get("deployments") or {}).get("edges") or [])
    return [edge.get("node") or {} for edge in edges if edge.get("node")]


def fetch_build_log_summary(deployment_id: str, max_chars: int = 500) -> str:
    data = zeabur_graphql(BUILD_LOGS_QUERY, {"deploymentID": deployment_id})
    logs = data.get("buildLogs") or []
    messages = [strip_ansi(str(log.get("message") or "")) for log in logs]
    messages = [msg for msg in messages if msg]
    if not messages:
        return ""
    interesting = [
        msg for msg in messages if "error" in msg.lower() or "failed" in msg.lower()
    ]
    msg = (interesting or messages)[0]
    return msg[:max_chars]


def evaluate_deployments(
    service: dict[str, Any],
    deployments: list[dict[str, Any]],
    seen_deployments: dict[str, int],
    now: int,
    lookback_minutes: int,
    failure_statuses: set[str],
    include_build_logs: bool,
) -> list[Issue]:
    name = service.get("name") or service.get("_id", "service")
    service_id = service.get("_id") or ""
    issues: list[Issue] = []
    cutoff = now - lookback_minutes * 60 if lookback_minutes > 0 else 0
    for deployment in deployments:
        deployment_id = str(deployment.get("_id") or "")
        status = str(deployment.get("status") or "").upper()
        if not deployment_id or status not in failure_statuses:
            continue
        created_ts = parse_utc_ts(deployment.get("createdAt") or deployment.get("startedAt"))
        if created_ts is not None and created_ts < cutoff:
            continue
        if deployment_id in seen_deployments:
            continue
        commit = str(deployment.get("commitSHA") or "")
        commit_short = commit[:7] if commit else "unknown"
        commit_msg = str(deployment.get("commitMessage") or "").strip()
        created = deployment.get("createdAt") or "unknown time"
        log_summary = ""
        if include_build_logs:
            try:
                log_summary = fetch_build_log_summary(deployment_id)
            except Exception as exc:
                log_summary = f"build log unavailable: {exc}"
        message = (
            f"{name} deployment {deployment_id} status={status} at {created}; "
            f"commit={commit_short}"
        )
        if commit_msg:
            message += f" {commit_msg[:120]}"
        if log_summary:
            message += f"; log={log_summary}"
        issues.append(
            Issue(
                f"deployment_failed:{service_id or name}:{deployment_id}",
                "critical",
                message,
                name,
            )
        )
    return issues


def mark_seen_deployment_failures(state_bucket: dict[str, int], issues: list[Issue], now: int) -> None:
    prefix = "deployment_failed:"
    for issue in issues:
        if not issue.key.startswith(prefix):
            continue
        deployment_id = issue.key.rsplit(":", 1)[-1]
        state_bucket[deployment_id] = now


def prune_seen_deployments(state_bucket: dict[str, int], now: int, retention_minutes: int) -> None:
    if retention_minutes <= 0:
        return
    cutoff = now - retention_minutes * 60
    for deployment_id, seen_ts in list(state_bucket.items()):
        if int(seen_ts or 0) < cutoff:
            state_bucket.pop(deployment_id, None)


def should_fire(state_bucket: dict[str, int], key: str, cooldown_minutes: int, now: int) -> bool:
    last = int(state_bucket.get(key) or 0)
    return now - last >= cooldown_minutes * 60


def mark_fired(state_bucket: dict[str, int], key: str, now: int) -> None:
    state_bucket[key] = now


def feishu_token(app_id: str, app_secret: str) -> str:
    data = http_json(
        "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
        {"app_id": app_id, "app_secret": app_secret},
        timeout=20,
    )
    token = data.get("tenant_access_token")
    if not token:
        raise RuntimeError(f"Feishu tenant token missing: {data}")
    return token


def send_feishu(text: str, dry_run: bool) -> bool:
    app_id = os.getenv("FEISHU_NOTIFY_APP_ID", "").strip()
    app_secret = os.getenv("FEISHU_NOTIFY_APP_SECRET", "").strip()
    open_ids = split_targets(os.getenv("FEISHU_NOTIFY_OPEN_ID", ""))
    chat_ids = split_targets(os.getenv("FEISHU_NOTIFY_CHAT_ID", ""))
    if dry_run:
        safe_print("DRY_RUN_FEISHU_ALERT:")
        safe_print(text)
        return True
    if not app_id or not app_secret or not (open_ids or chat_ids):
        print("Feishu notify skipped: missing FEISHU_NOTIFY_APP_ID/SECRET and target")
        return False
    token = feishu_token(app_id, app_secret)
    sent = 0
    for receive_type, receive_ids in (("open_id", open_ids), ("chat_id", chat_ids)):
        for receive_id in receive_ids:
            http_json(
                f"https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type={receive_type}",
                {
                    "receive_id": receive_id,
                    "msg_type": "text",
                    "content": json.dumps({"text": text}, ensure_ascii=False),
                },
                headers={"Authorization": f"Bearer {token}"},
                timeout=20,
            )
            sent += 1
    return sent > 0


def format_alert(
    issues: list[Issue],
    restarts: list[str],
    server: dict[str, Any],
    probes: dict[str, ProbeResult],
) -> str:
    status = (server.get("status") or {}) if server else {}
    lines = [
        "[AUDIT P1] Zeabur Tokyo watchdog",
        f"Time UTC: {iso_utc()}",
        f"Server: {server.get('name', 'unknown') if server else 'unknown'}",
        f"Online: {status.get('isOnline')} / vmStatus: {status.get('vmStatus')}",
    ]
    cpu = pct(status.get("usedCPU"), status.get("totalCPU"))
    mem = pct(status.get("usedMemory"), status.get("totalMemory"))
    disk = pct(status.get("usedDisk"), status.get("totalDisk"))
    lines.append(
        "Usage: "
        + ", ".join(
            [
                f"CPU {cpu:.1f}%" if cpu is not None else "CPU n/a",
                f"MEM {mem:.1f}%" if mem is not None else "MEM n/a",
                f"DISK {disk:.1f}%" if disk is not None else "DISK n/a",
            ]
        )
    )
    lines.append("")
    lines.append("Issues:")
    for issue in issues:
        lines.append(f"- {issue.severity.upper()} {issue.key}: {issue.message}")
    if probes:
        lines.append("")
        lines.append("Health probes:")
        for name, probe in probes.items():
            if probe.ok:
                lines.append(f"- {name}: OK {probe.status} {probe.elapsed_ms}ms")
            else:
                lines.append(f"- {name}: FAIL {probe.error or probe.status} {probe.elapsed_ms}ms")
    if restarts:
        lines.append("")
        lines.append("Auto actions:")
        for item in restarts:
            lines.append(f"- {item}")
    return "\n".join(lines)


def service_status_map(services: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(s.get("_id")): s for s in services}


def run_once(args: argparse.Namespace) -> dict[str, Any]:
    now = utc_now_ts()
    state = load_state(args.state_file)
    services_cfg = load_json_env("WATCHDOG_SERVICES_JSON", DEFAULT_SERVICES)
    data = zeabur_graphql(SERVER_QUERY, {"projectID": args.project_id})
    servers = data.get("servers") or []
    server = next((s for s in servers if s.get("_id") == args.server_id), servers[0] if servers else {})
    project_services = ((data.get("project") or {}).get("services") or [])
    service_by_id = service_status_map(project_services)

    issues = evaluate_server(server)
    server_online = bool((server.get("status") or {}).get("isOnline")) and (
        (server.get("status") or {}).get("vmStatus") in (None, "RUNNING")
    )

    probes: dict[str, ProbeResult] = {}
    restart_actions: list[str] = []
    restart_candidates: list[dict[str, Any]] = []
    deployment_issues: list[Issue] = []
    for svc in services_cfg:
        name = svc["name"]
        service_id = svc["service_id"]
        service = service_by_id.get(service_id) or {}
        service_status = service.get("status")
        if service_status and service_status != "RUNNING":
            issues.append(
                Issue(
                    f"service_status:{name}",
                    "critical",
                    f"{name} service status={service_status}",
                    name,
                )
            )
            if svc.get("restart_on_fail"):
                restart_candidates.append(svc)
        health_url = svc.get("health_url")
        if health_url:
            probe = probe_url(health_url, timeout=args.health_timeout)
            probes[name] = probe
            if not probe.ok:
                issues.append(
                    Issue(
                        f"health_fail:{name}",
                        "critical",
                        f"{name} health probe failed: {probe.error or probe.status}",
                        name,
                    )
                )
                if svc.get("restart_on_fail"):
                    restart_candidates.append(svc)

    if getattr(args, "check_deployments", True):
        deployment_state = state.setdefault("deployments", {})
        prune_seen_deployments(
            deployment_state,
            now,
            getattr(args, "deployment_seen_retention", 7 * 24 * 60),
        )
        for service in project_services:
            service_id = str(service.get("_id") or "")
            if not service_id:
                continue
            try:
                deployments = fetch_service_deployments(
                    service_id,
                    args.environment_id,
                    getattr(args, "deployment_per_page", 10),
                )
                found = evaluate_deployments(
                    service,
                    deployments,
                    deployment_state,
                    now,
                    getattr(args, "deployment_lookback", 6 * 60),
                    getattr(args, "deployment_failure_statuses", DEFAULT_DEPLOYMENT_FAILURE_STATUSES),
                    getattr(args, "include_build_logs", True),
                )
                deployment_issues.extend(found)
                issues.extend(found)
            except Exception as exc:
                name = service.get("name") or service_id
                issues.append(
                    Issue(
                        f"deployment_check_error:{service_id}",
                        "warning",
                        f"{name} deployment check failed: {exc}",
                        name,
                    )
                )

    if args.auto_restart_services and server_online:
        seen: set[str] = set()
        for svc in restart_candidates:
            service_id = svc["service_id"]
            if service_id in seen:
                continue
            seen.add(service_id)
            cooldown_key = f"service:{service_id}"
            if not should_fire(state.setdefault("restarts", {}), cooldown_key, args.restart_cooldown, now):
                restart_actions.append(f"skip restart {svc['name']}: cooldown")
                continue
            ok = restart_service(service_id, args.environment_id, dry_run=args.dry_run)
            mark_fired(state["restarts"], cooldown_key, now)
            restart_actions.append(f"restart {svc['name']}: {'ok' if ok else 'failed'}")
    elif restart_candidates and not server_online:
        restart_actions.append("skip service restart: server is offline")
    elif restart_candidates and not args.auto_restart_services:
        restart_actions.append("skip service restart: disabled")

    fired_alert = False
    if issues:
        alert_key = "|".join(sorted(issue.key for issue in issues))
        if should_fire(state.setdefault("alerts", {}), alert_key, args.alert_cooldown, now):
            alert_text = format_alert(issues, restart_actions, server, probes)
            fired_alert = send_feishu(alert_text, dry_run=args.dry_run)
            mark_fired(state["alerts"], alert_key, now)
            if fired_alert:
                mark_seen_deployment_failures(state.setdefault("deployments", {}), deployment_issues, now)
        else:
            safe_print(f"Alert suppressed by cooldown: {alert_key}")
            mark_seen_deployment_failures(state.setdefault("deployments", {}), deployment_issues, now)

    save_state(args.state_file, state)
    summary = {
        "ok": not issues,
        "issue_count": len(issues),
        "issues": [issue.__dict__ for issue in issues],
        "alert_sent": fired_alert,
        "restart_actions": restart_actions,
        "server": {
            "id": server.get("_id"),
            "name": server.get("name"),
            "status": server.get("status"),
        },
        "deployment_issue_count": len(deployment_issues),
        "probes": {name: probe.__dict__ for name, probe in probes.items()},
    }
    if getattr(args, "summary_file", ""):
        os.makedirs(os.path.dirname(args.summary_file) or ".", exist_ok=True)
        with open(args.summary_file, "w", encoding="utf-8") as fh:
            json.dump(summary, fh, ensure_ascii=False, indent=2, sort_keys=True)
    safe_print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return summary


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Monitor Zeabur Tokyo server from outside it.")
    parser.add_argument("--project-id", default=os.getenv("ZEABUR_PROJECT_ID", DEFAULT_PROJECT_ID))
    parser.add_argument("--environment-id", default=os.getenv("ZEABUR_ENVIRONMENT_ID", DEFAULT_ENVIRONMENT_ID))
    parser.add_argument("--server-id", default=os.getenv("ZEABUR_SERVER_ID", DEFAULT_SERVER_ID))
    parser.add_argument(
        "--state-file",
        default=os.getenv("WATCHDOG_STATE_FILE", ".watchdog-state/zeabur_watchdog_state.json"),
    )
    parser.add_argument("--summary-file", default=os.getenv("WATCHDOG_SUMMARY_FILE", ""))
    parser.add_argument("--health-timeout", type=int, default=env_int("WATCHDOG_HEALTH_TIMEOUT", 15))
    parser.add_argument("--alert-cooldown", type=int, default=env_int("WATCHDOG_ALERT_COOLDOWN_MINUTES", 60))
    parser.add_argument(
        "--restart-cooldown",
        type=int,
        default=env_int("WATCHDOG_RESTART_COOLDOWN_MINUTES", 60),
    )
    parser.add_argument(
        "--auto-restart-services",
        action=argparse.BooleanOptionalAction,
        default=env_bool("WATCHDOG_AUTO_RESTART_SERVICES", True),
    )
    parser.add_argument(
        "--check-deployments",
        action=argparse.BooleanOptionalAction,
        default=env_bool("WATCHDOG_CHECK_DEPLOYMENTS", True),
    )
    parser.add_argument(
        "--deployment-per-page",
        type=int,
        default=env_int("WATCHDOG_DEPLOYMENTS_PER_SERVICE", 10),
    )
    parser.add_argument(
        "--deployment-lookback",
        type=int,
        default=env_int("WATCHDOG_DEPLOYMENT_LOOKBACK_MINUTES", 6 * 60),
    )
    parser.add_argument(
        "--deployment-seen-retention",
        type=int,
        default=env_int("WATCHDOG_DEPLOYMENT_SEEN_RETENTION_MINUTES", 7 * 24 * 60),
    )
    parser.add_argument(
        "--deployment-failure-statuses",
        default=env_set("WATCHDOG_DEPLOYMENT_FAILURE_STATUSES", DEFAULT_DEPLOYMENT_FAILURE_STATUSES),
    )
    parser.add_argument(
        "--include-build-logs",
        action=argparse.BooleanOptionalAction,
        default=env_bool("WATCHDOG_INCLUDE_BUILD_LOGS", True),
    )
    parser.add_argument("--dry-run", action="store_true", default=env_bool("WATCHDOG_DRY_RUN", False))
    parser.add_argument(
        "--fail-on-issue",
        action=argparse.BooleanOptionalAction,
        default=env_bool("WATCHDOG_FAIL_ON_ISSUE", True),
    )
    args = parser.parse_args(argv)
    if isinstance(args.deployment_failure_statuses, str):
        args.deployment_failure_statuses = {
            part.upper() for part in split_targets(args.deployment_failure_statuses)
        }
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    try:
        summary = run_once(args)
    except Exception as exc:
        safe_print(f"watchdog fatal: {exc}", file=sys.stderr)
        if args.fail_on_issue:
            return 2
        return 0
    if args.fail_on_issue and not summary["ok"]:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

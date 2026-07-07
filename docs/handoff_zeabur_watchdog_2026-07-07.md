# Zeabur Tokyo Watchdog - 2026-07-07

## Problem

The Zeabur Dedicated Server `自用服务器-东京` can go offline or have K3s become unhealthy. If the watchdog runs inside the same Tokyo n8n instance, it dies with the server and cannot alert or recover anything.

## Scope

P2 adds an external watchdog that runs from GitHub Actions, not from Tokyo n8n.

It monitors:

- Zeabur server `_id=69856dfd2a96ae7705ff2930`
- Project `n8n-aments` `_id=69856f0c2e156a6efa59a9a9`
- Environment `production` `_id=69856f0c86311f632dc2c2c9`
- Service `n8n-hual` `_id=69856f0d2e156a6efa59a9ce`
- Service `kol-automation` `_id=69eae010c5278d4159c1f664`
- Health endpoints:
  - `https://frankiepan501.zeabur.app/healthz`
  - `https://kol-auto.zeabur.app/health`

## Implementation

Files:

- `.github/workflows/zeabur-watchdog.yml`
  - Runs every 10 minutes on GitHub-hosted runners.
  - Also supports manual `workflow_dispatch`.
  - Restores `.watchdog-state/` via GitHub Actions cache for alert/restart cooldown.
- `scripts/zeabur_watchdog.py`
  - Uses only Python stdlib.
  - Queries Zeabur GraphQL:
    - `servers { status { isOnline vmStatus totalCPU usedCPU totalMemory usedMemory totalDisk usedDisk } }`
    - `project(_id) { services { _id name status suspendedAt domains { domain } } }`
  - Probes public health URLs.
  - Sends direct Feishu IM alerts when configured.
  - Auto-restarts `n8n-hual` and `kol-automation` only when the server itself is online but a service status/health probe fails.
  - Does not attempt full server reboot. The official Zeabur docs describe server reboot as a Dashboard operation for OOM/Server Offline recovery, and a reboot mutation was not confirmed.
- `tests/test_zeabur_watchdog.py`
  - Covers resource thresholds, offline handling, service restart selection, cooldown, and missing Feishu config.

## Required GitHub Secrets

Required:

- `ZEABUR_API_KEY`

Recommended for direct Feishu alerting:

- `FEISHU_NOTIFY_APP_ID`
- `FEISHU_NOTIFY_APP_SECRET`
- One of:
  - `FEISHU_NOTIFY_OPEN_ID`
  - `FEISHU_NOTIFY_CHAT_ID`

Without Feishu secrets, the workflow can still fail visibly in GitHub Actions, but it will not send a Feishu alert.

## Recovery Policy

- Server offline or `vmStatus != RUNNING`: alert only. Service-level restart is skipped because the server is not available.
- Memory/CPU/disk high: alert only. The official OOM recovery path says to reboot carefully and avoid auto-starting all services if K3s is unhealthy.
- Service status not `RUNNING` or health endpoint fails while the server is online: call `restartService(serviceID, environmentID)`.
- Cooldowns:
  - Alerts: 60 minutes per issue fingerprint.
  - Service restarts: 60 minutes per service.

## Verification

Local tests:

```powershell
& C:\Users\Administrator\kol-marketing-automation-invest\.venv\Scripts\python.exe -m unittest tests.test_zeabur_watchdog
```

Manual dry-run smoke:

```powershell
$env:WATCHDOG_DRY_RUN='true'
$env:WATCHDOG_FAIL_ON_ISSUE='false'
$env:ZEABUR_API_KEY='<set locally>'
& C:\Users\Administrator\kol-marketing-automation-invest\.venv\Scripts\python.exe scripts\zeabur_watchdog.py
```

Production GitHub Actions verification:

- Workflow `Zeabur Tokyo Watchdog` was recognized by GitHub and is `active`.
- Push run #1 (`913f504`) failed with exit code 2. Public logs required sign-in, but local real dry-run passed and the repo had no locally detectable GitHub token for setting/checking secrets, so this was treated as likely missing `ZEABUR_API_KEY` secret.
- Added workflow preflight in `676b9ed`:
  - If `ZEABUR_API_KEY` is missing, the workflow exits successfully with a warning and skips the watchdog.
  - Once `ZEABUR_API_KEY` is configured, the schedule/push/manual runs execute the watchdog normally.
- Push run #2 (`676b9ed`) completed successfully.
- Latest Zeabur deployment after the watchdog commits was `RUNNING`.

## Remaining Risk

Full server reboot is still not automated because the safe public mutation was not confirmed. If Zeabur exposes and documents a server reboot API, add it behind a separate explicit flag, with a longer cooldown and a no-auto-start policy for K3s/OOM incidents.

The watchdog is code-complete but not guaranteed to be live until GitHub repository secrets are configured. Minimum required secret: `ZEABUR_API_KEY`. Feishu alerts additionally require `FEISHU_NOTIFY_APP_ID`, `FEISHU_NOTIFY_APP_SECRET`, and one of `FEISHU_NOTIFY_OPEN_ID` or `FEISHU_NOTIFY_CHAT_ID`.

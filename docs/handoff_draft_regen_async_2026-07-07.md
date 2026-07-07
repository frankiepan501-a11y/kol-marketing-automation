# Draft Regen Card Callback Fix - 2026-07-07

## Problem

运营在 KOL/媒体人邮件审核卡里填写「重生方向」后点击「退回重生(真重新生成)」，飞书客户端弹出：

`出错了，请稍后重试 code: 200341`

## Root Cause

`draft_regen` is a long-running operation: it calls DeepSeek, writes Feishu Bitable records, denies the old draft, and routes the new draft through the human-review card flow. If n8n event-hub waits for `POST /draft/regen` to finish inside the `card.action.trigger` callback path, Feishu/Lark times out the card callback before the operation completes.

This matches the historical card-callback rule in memory: card actions must ACK quickly and should provide explicit operation feedback.

## Code Change

Files changed:

- `app/main.py`
  - `POST /draft/regen` now defaults to `async_mode=true`.
  - The endpoint returns quickly with `{accepted: true, job_id, record_id}`.
  - Actual `draft_regen.regen_draft()` runs in a background task.
  - Same `record_id` running job is deduped to prevent double-click duplicate regenerations.
  - Added `GET /draft/regen/jobs/{job_id}` for observability.
  - `async_mode=false` still gives the old synchronous behavior for manual debugging.
- `tests/test_draft_regen_async.py`
  - Covers default async behavior and duplicate-click dedupe.

## Verification

Command:

```powershell
& C:\Users\Administrator\kol-marketing-automation-invest\.venv\Scripts\python.exe -m unittest tests.test_draft_regen_async tests.test_draft_duplicate_audit tests.test_draft_status_audit
```

Result:

`Ran 12 tests ... OK`

Also ran:

```powershell
& C:\Users\Administrator\kol-marketing-automation-invest\.venv\Scripts\python.exe -m compileall app\main.py
```

## Deployment / n8n Follow-Up

Production follow-up completed after the Tokyo Zeabur server recovered:

- Zeabur service `kol-automation` redeployed successfully.
  - Deployment id: `6a4c72c8c3ed30bb38a68628`
  - Status: `RUNNING`
  - Commit: `chore: trigger kol zeabur deploy`
- `https://kol-auto.zeabur.app/health` returned `{"status":"ok"}`.
- `https://kol-auto.zeabur.app/openapi.json` exposes:
  - `POST /draft/regen` with `async_mode` defaulting to `true`.
  - `GET /draft/regen/jobs/{job_id}`.
- n8n event-hub `YjTXaoWAcy89xZpT` node `Draft Action Handler` was updated in production:
  - The `draft_regen` HTTP call now explicitly appends `async_mode=true`.
  - The request timeout was reduced from `90000` ms to `15000` ms.
  - `{ok:true, accepted:true, job_id}` is treated as success.
  - The reviewed card is patched with a "background regeneration accepted" message.
- n8n read-back verification:
  - Workflow is still `active=true`.
  - `Draft Action Handler` contains `&async_mode=true`.
  - It contains the `rr.accepted || rr.job_id` branch.
  - It no longer contains `timeout: 90000`.
- Low-risk smoke test with a fake record id:
  - `POST /draft/regen?...&async_mode=true` returned `ok=true`, `accepted=true`, and a `job_id` in about 1.6s.
  - The background job had no real Bitable record to mutate, so this only verified callback latency and API shape.

Remaining manual validation:

1. Do a real Feishu click smoke test on one test draft and confirm the client no longer shows `200341`.
2. Confirm the follow-up review card for the newly generated draft arrives normally.

## Zeabur Server Monitoring Notes

No existing production workflow was found that monitors Zeabur Dedicated Server health and automatically reboots the server.

Relevant current API findings:

- `servers { _id name provider status { isOnline vmStatus totalCPU usedCPU totalMemory usedMemory totalDisk usedDisk } events { message time } }`
- Current server:
  - `_id`: `69856dfd2a96ae7705ff2930`
  - `name`: `自用服务器-东京`
  - `provider`: `TENCENT`
  - `status.isOnline`: `true`
  - `status.vmStatus`: `RUNNING`
  - Last relevant event: `Server rebooted` at `2026-07-07T03:21:34.756Z`
- Dashboard frontend uses the same fields to flag:
  - server offline as critical.
  - CPU and memory warning at 90%, critical at 95%.
  - disk warning at 85%, critical at 95%.

Important design constraint:

Do not run a Tokyo-server health watchdog inside the same Tokyo n8n instance. If the server goes offline, the watchdog is offline too. Server-level monitoring should run outside this server, for example GitHub Actions, another cloud service, or a separate Zeabur/shared-cluster service.

## Residual Risk

The in-memory job ledger is process-local. It is enough for short callback observability and double-click dedupe, but jobs disappear on service restart. The source of truth remains Feishu Bitable: the old draft should become `已否决`, and the new draft should be created and routed to human review.

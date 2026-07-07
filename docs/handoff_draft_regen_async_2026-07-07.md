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

After deploy, verify the n8n event-hub `YjTXaoWAcy89xZpT` Draft Action Handler for `draft_regen`:

1. It should treat `POST /draft/regen` response `{accepted:true, job_id}` as success.
2. The operator-facing reply should say the regeneration has started, not that it has already completed.
3. If the handler currently parses `new_rid` synchronously, update it to use `job_id` or just ACK and let the newly routed review card appear after the background job finishes.
4. Do a real Feishu click smoke test on one test draft and confirm the client no longer shows `200341`.

## Residual Risk

The in-memory job ledger is process-local. It is enough for short callback observability and double-click dedupe, but jobs disappear on service restart. The source of truth remains Feishu Bitable: the old draft should become `已否决`, and the new draft should be created and routed to human review.

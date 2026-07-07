# Event Hub Connection Fix - 2026-07-07

## Problem

KOL/媒体人审核卡点击 `通过` 后，飞书客户端提示：

`出错了，请稍后重试 code: 200671`

The issue reproduced around 2026-07-07 14:25-14:34 Asia/Taipei. Recent executions of n8n workflow `YjTXaoWAcy89xZpT` were all failing before any node ran.

## Root Cause

n8n workflow `飞书事件中心 - Event Hub` had one broken connection:

- Source: `Is ML Profit Action`
- Branch: false / main index 1
- Bad target: `Is Customer Intake`
- Correct target: `Is Customer Intake?`

Because the target node name did not exist, n8n failed during workflow readiness checks with:

`Cannot read properties of undefined (reading 'disabled')`

This broke all Feishu webhook/card callbacks, including KOL `draft_approve`, before business logic started.

## Production Fix

Only one connection target was changed in production n8n:

```text
Is ML Profit Action false -> Is Customer Intake
```

to:

```text
Is ML Profit Action false -> Is Customer Intake?
```

Workflow was then reactivated.

Production workflow:

- n8n workflow id: `YjTXaoWAcy89xZpT`
- workflow name: `飞书事件中心 - Event Hub`
- activeVersionId after fix: `e2941a26-eaf0-48a6-ae56-b875b962a4d8`
- backup before fix: `%TEMP%\eventhub-backup-20260707-143533.json`

## Verification

Connection graph validation after fix:

- missing sources: `0`
- missing targets: `0`
- `Is ML Profit Action` false branch target: `Is Customer Intake?`

Webhook URL verification:

- POST `https://frankiepan501.zeabur.app/webhook/feishu-event-hub`
- response: `{"challenge":"codex_eventhub_fix_ok"}`
- execution `459790`: success

Safe simulated card action:

- action: `draft_approve`
- record_id: omitted intentionally, so no Bitable record was modified
- webhook response: `{"code":0}`
- execution `459801`: success
- run path included `Draft Action Handler` and `Draft Action Reply`

Recent executions after fix:

- `459801`: success, simulated draft action
- `459802`: success
- `459803`: success

Pre-fix failing executions included `459776`, `459775`, `459772`, all failing with the readiness-check error above and empty `runData`.

## Remaining Risk

The original operator card should now work if clicked again. If a stale card still fails, check whether the card was sent by the correct callback-owning app and whether `value.action`, `record_id`, and `open_message_id` are present.

The unrelated Zeabur build-failure email for service `ml-sync` is not covered by this fix. That belongs to the external Zeabur watchdog P2 build/deployment monitoring extension.

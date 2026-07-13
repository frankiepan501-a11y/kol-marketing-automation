# Weekly Report Delivery Fix - 2026-07-13

## Problem

Zhang Jiaye reported that she did not receive the KOL weekly report, and also did not receive the SEO/DTC HTML weekly report.

## Findings

- KOL n8n workflows `5oHkITIOaMZ4n60t` and `MH5vk9FZJD0yTHpS` both triggered successfully on 2026-07-13 at 11:00 and 11:30 BJ.
- Both KOL endpoints run in background mode, so n8n can report success even if the later background notification fails.
- The KOL report modules only sent cards to `NOTIFY_CHAT_ID` plus Frankie private chat. They intentionally skipped Zhang Jiaye private delivery.
- `NOTIFY_CHAT_ID` does include Zhang Jiaye as a group member, but group-only delivery is not a reliable owner handoff.
- DTC weekly report `ofL443rQbbcAdHMK` was already moved to Tuesday 00:00 BJ in the 2026-07-01 timezone fix, so a Monday afternoon absence is expected.
- DTC health monitor `pZbaThINDCiAe6Hx` was still on the old Monday check path and had a broken duplicate `DOW Filter (Mon)` self-connection.

## Changes

- `app/completion_report.py`: KOL completion weekly report now sends to the KOL notification group, active `reviewer` targets (independent-site operators), and Frankie.
- `app/upload_task_report.py`: KOL upload-task weekly report now uses the same private reviewer delivery by default, while keeping `frankie_only` for format review.
- `app/main.py`: background KOL report jobs now print completion summaries with notification counts, products, and written rows.
- n8n workflow `pZbaThINDCiAe6Hx`: rebuilt to 5 nodes, changed schedule to Tuesday 09:07 BJ, and fixed BJ-date execution checks. Backup saved locally at `C:/tmp/n8n_dtc_health_monitor_before_20260713_155119.json`.

## Verification

- Local syntax check passed:
  `python -m py_compile app/completion_report.py app/upload_task_report.py app/main.py`
- n8n health monitor is active after update.
- n8n health monitor node chain is:
  `周二 09:07 BJ -> 查 n8n executions -> 拿飞书 token -> 整合 + 拼消息 -> 发飞书私聊给 Frankie`

## Remaining Risk

- KOL endpoints still use async background execution because synchronous dry-run can approach Zeabur gateway timeout. n8n success should not be treated as final delivery proof.
- Next improvement should persist per-run delivery audit rows or emit structured logs with message ids for each recipient.

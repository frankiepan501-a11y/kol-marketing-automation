# KOL endpoint failure alert card update - 2026-07-23

## Problem

Frankie received an `[AUDIT-P2] KOL 发信链运行异常 · /auto-send/run` card for Feishu error `1254607 Data not ready`.

The real failure point was a Feishu Bitable read during `/auto-send/run`, not proof of a bad draft approval or email mis-send. The old card showed the raw Feishu URL, JSON, and trace too prominently, so Frankie and operators could not quickly tell:

- whether any email was sent,
- whether operations needed to handle drafts,
- whether the system would retry,
- what technical evidence mattered.

## Root Cause

`app/feishu.py` already treats Feishu `1254607` as a transient Bitable data/index readiness error and retries it with `5s + 10s + 20s`. If retries are exhausted, `app/main.py::_alert_endpoint_failure()` sends an endpoint alert.

The alert card was technically correct but not operationally clear.

## Change

- `app/main.py`
  - Added `_build_endpoint_failure_card()` and helpers.
  - Special-cased `/auto-send/run` + `1254607/Data not ready` as a P2 "read table temporarily failed" card.
  - Card now separates:
    - status,
    - affected step,
    - impact,
    - who should do what,
    - system retry behavior,
    - technical evidence.
  - Trace is kept in a small note section instead of dominating the card.

- `tests/test_endpoint_failure_card.py`
  - Added unit tests for transient Feishu read failure and non-transient auto-send failure.

- `app/feishu.py`
  - `fetch_all_records()` now supports `field_names` and configurable `page_size`.
  - Page size is clamped to Feishu's documented maximum of 500.

- `app/auto_send.py`
  - `/auto-send/run` still uses list API for the full-draft safety scan.
  - The scan now fetches only the fields needed for follow-up guards, 24h send caps, and duplicate-send prevention.
  - This reduces page count and payload size for the Bitable read that triggered the `1254607` alert.

- `tests/test_feishu_fetch_all_records.py`
  - Added a unit test for `field_names`, page-size clamping, and `page_token` propagation.

## Validation

Run from `D:\Documents\亚马逊\kol-marketing-automation`:

```powershell
& 'C:\Users\Administrator\kol-marketing-automation-invest\.venv\Scripts\python.exe' -m unittest tests.test_endpoint_failure_card tests.test_feishu_fetch_all_records
& 'C:\Users\Administrator\kol-marketing-automation-invest\.venv\Scripts\python.exe' -m unittest tests.test_draft_duplicate_audit tests.test_draft_status_audit
& 'C:\Users\Administrator\kol-marketing-automation-invest\.venv\Scripts\python.exe' -m py_compile app\main.py app\feishu.py app\auto_send.py tests\test_endpoint_failure_card.py tests\test_feishu_fetch_all_records.py
```

Result: all passed. The existing draft audit tests print missing-env warnings in this local shell, but do not call live services.

## Rollout

No Feishu message was sent, no Bitable data was written, and no Zeabur env/config was changed while making this code change.

Before production rollout:

1. Review the local diff.
2. Commit and push intentionally.
3. Deploy the updated `kol-automation` service.
4. Trigger a Frankie-only or synthetic alert sample if available, then inspect the rendered card.

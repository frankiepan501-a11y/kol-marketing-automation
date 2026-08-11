# Brazil KOL Portuguese keyword supply — 2026-08-11

## Problem

The continuous YouTube keyword supplier claimed to cover Brazil, but `MARKETS` did not contain Portuguese or Brazil. The only three Portuguese tasks in the Base had failed with `ProxyError`, leaving no pending Portuguese supply.

## Root cause

- `app/keyword_supply.py` only configured `en/de/fr/es`.
- The generic multilingual prompt was dominated by English examples, so the first real `pt` dry-run still returned English-only phrases.

## Change

- Added `pt` with country `BR` and queue target `6`.
- Added a Brazilian-Portuguese prompt guard, deterministic localization markers, and one strict retry when a generated batch is English-only.
- A region-only suffix such as `Brasil` is not sufficient; at least one actual Portuguese descriptor must be present.
- English-only output is rejected instead of being queued under a false Portuguese label.
- If both attempts contain no localized keyword, the market and top-level run return an explicit error instead of a false success.
- Any Feishu task-creation error also makes the top-level run fail; a zero-write result cannot be reported as healthy.

## Verification

- `tests/test_keyword_supply.py`: 5 tests pass.
- Real local dry-run against the current crawler task table reported `pending=0`, `need=6`, and six localized Portuguese phrases.
- `dry_run=True` performed no Base writes and created no crawler task.

## Remaining production step

Deploy the commit to the `kol-auto` Zeabur service and verify its running version before allowing the scheduled keyword-supply workflow to create real tasks.

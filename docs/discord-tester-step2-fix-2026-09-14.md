# Discord tester Step 2 repair — 2026-09-14

## Problem

Two interested members reached the second application step but could not submit valid answers. The modal required applicants to type internal codes such as `COUNT=...` and `SWITCH=...`; ordinary answers were rejected and an error forced the applicant to restart.

## Root cause

Internal storage syntax was exposed as the user input format. Validation also converted repeated keys into a dictionary before checking them, so it could not explain category omissions reliably.

## Change

- Replaced the two coded text fields with Discord modal multi-select controls using plain-English option labels.
- Kept the existing internal values and parser so modals opened before deployment remain compatible.
- Added server-side checks requiring exactly one answer from each business category.
- On a Step 2 error, preserves the selected options and three text answers and offers `Correct Step 2`; no application is written until every check passes.

## Verification

- `tests/test_discord_tester_program.py`: 45 tests pass, including select submission, duplicate-category rejection, safe correction defaults, value preservation, legacy free-text submission, and no ledger write on invalid input.
- `py_compile` and `git diff --check` pass.
- Production release must additionally confirm the Zeabur deployment commit, `/health`, the Discord interaction route, and a hidden-channel render without completing a real application.

## Remaining risk

Discord client rendering can only be proven after deployment in a real modal. The hidden rehearsal must not make a valid final submission because that would write a production Feishu application record.

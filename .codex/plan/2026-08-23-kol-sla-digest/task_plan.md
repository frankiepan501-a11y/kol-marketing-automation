# KOL SLA digest routing repair

## Goal

Replace per-draft KOL SLA card floods with role-based digest cards that reduce Frankie review load without hiding overdue customer replies.

## Confirmed scope

- Reviewer: one P1 digest per 6-hour SLA run; no business-group copy.
- Frankie: one exception digest only when P1 drafts remain overdue for more than 48 hours.
- P1 sources: `reply`, `affiliate_quote`, `ship_confirm`, `tracking_followup`.
- P2 sources: all remaining overdue draft sources; one daily digest.
- Release gate: local tests and self-test, then one Frankie-only real sample card; production deploy only after Frankie confirms the sample.

## Test seams

1. Public SLA endpoint result: counts, routing outcomes, and no group sends.
2. Card payload: title level, actionable context, direct record links, queue link, owner/deadline wording.
3. Time gate: P2 digest sends only in the configured daily hour.
4. Delivery routing: reviewer excludes Frankie; Frankie exception uses only the explicit Frankie target.

## Phases

| Phase | Status | Success check |
|---|---|---|
| Inspect current SLA and notification code | complete | Current production behavior and send seams identified |
| Add red tests for digest behavior | complete | Tests failed on existing per-record implementation |
| Implement P1/P2/48h digest routing | complete | Six targeted tests pass |
| Add card self-test and run regressions | complete | Fixture/live target checks pass; 24/25 test files pass and the only failure matches baseline |
| Send Frankie-only sample | complete | Message `om_x100b679b655d64a0c318f10608ed503` read back with expected interactive payload and URLs |
| Frankie approval and production deploy | blocked on visual sample approval | Deployment verified by version/execution output |
| Documentation and closeout | pending | Handoff, plan, lesson, and final priorities recorded |

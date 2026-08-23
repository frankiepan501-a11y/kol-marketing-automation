# Progress

## 2026-08-23

- Read Feishu App routing, notification rules, groups registry, and five card-design memories including the Frankie-only production gate.
- Verified production n8n workflow `j6RR1iVdM413KsW7`: 27 L1 escalations today, primarily KOL replies; deduplication worked but each record generated separate cards.
- User confirmed routing choices 1A/2A/3A.
- Protected unrelated dirty worktree changes; target files are currently clean.
- Test seams recorded before TDD.
- Added `tests/test_sla_digest.py` for endpoint routing, card actionability, 48-hour Frankie exception, and P2 daily time gate.
- Red run confirmed: all three tests fail because current `sla_check.run()` has no fixed-time seam or digest implementation.
- Standard unittest discovery is polluted by `C:\tmp\ml-data-sync\app`; use an importlib runner that prepends this repository and removes stale `app` modules.
- Implemented digest classification, reviewer-only P1 routing, Frankie-only 48h exception routing, and daily 12:00 P2 gate.
- Added exact-record links and the existing `在途草稿` view `vewMC2JoMf` to the card.
- Added fixture/live self-test. Live read-only result: P1=25, P2=2, P1 over 48h=0; no writes, email sends, or group sends.
- Added project handoff `docs/handoff_kol_sla_digest_2026-08-23.md`.

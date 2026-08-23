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
- Code review found and the implementation now fixes: a default-on Frankie-only production gate, explicit Frankie identity, persistent P2 daily replay protection, tracking-followup instructions, accurate metadata-write wording, live view/record target checks, and sent-message read-back checks.
- Target suite now has 6 passing tests, including an atomic date-level P2 claim independent of queue membership. All 25 test files were rerun individually: 24 passed; only the pre-existing `test_zeabur_watchdog.py` baseline failure remains.
- Both standards and spec re-reviews report no remaining findings after the card wording and P2 idempotency fixes.
- Re-ran the production read-only preflight after review fixes: P1=25, P2=2, P1 over 48h=0; exact view and top-five record targets are reachable; no writes, email sends, or group sends.
- Sent exactly one Frankie-only live sample card: `om_x100b679b655d64a0c318f10608ed503`. Message read-back confirms `interactive` type, expected title, content, and queue URL. No draft writes, email sends, or group sends occurred.
- Production push/deploy remains intentionally blocked until Frankie visually confirms the sample in the Feishu client.
- Frankie rejected the first sample wording as too technical. Reworked the card into plain operational Chinese: action-first title and steps, translated source labels, explicit approve/edit/reject/tracking instructions, and a concrete “去审核这 N 封邮件” button. Internal terms such as SLA/source codes/queue are no longer shown.
- Two-axis review found no hard standards violation. Fixed both actionable review points: shared the repeated review steps, changed “回复” to the broader “邮件草稿/发送”, and renamed the fixed-view button to “打开待审核邮件列表” so it does not promise an exact count the view cannot guarantee.

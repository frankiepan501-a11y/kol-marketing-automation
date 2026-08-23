# Decision log

## 2026-08-23 — SLA digest routing

- P1 digest goes only to the live reviewer role every six-hour run.
- The KOL business group receives no SLA digest.
- Frankie receives no routine P1 copy; only a separate digest for items still pending after 48 hours.
- `reply`, `affiliate_quote`, `ship_confirm`, and `tracking_followup` are P1; all other sources are P2 daily.
- The sample is sent Frankie-only before deployment. The sample must not change draft state or send email.
- Digest cards are queue-management cards, not approval cards. They must explain owner, required action, deadline, system checks, and provide direct evidence links.

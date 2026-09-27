# Temporary CS / existing-partnership handoff (2026-09-27)

## Scope and status

User authorizes independent-site customer service to 陈翔宇, and existing KOL/media fulfilment to 杨佳敬 (exact job title 商务BD专员). SocialEcho interaction replies are separately assigned to the foreign-trade team; Discord/tester incidents and synthetic social-review cards are NOT included.

Status: local implementation and tests only; new flags default inactive/staged. Not a completed production handoff. Existing dirty enrich/scoring/launch files must not be committed with this change.

## Configuration

- `CS_INDEPENDENT_SITE_TEMP_OPERATOR`: empty preserves normal role routing. Set to the verified existing operator 陈翔宇 for temporary coverage; contact name, union ID and active status are checked per lookup. Other platform routing is unchanged.
- `CS_INDEPENDENT_SITE_TEMP_FRANKIE_ONLY`: default 1, routes this override to Frankie until acceptance; 0 enables intended owner. It controls recipients only, NOT outbound email safety. Never use live customer rows as tests.
- `KOL_PARTNERSHIP_JOB_TITLE`: empty preserves old routes; target 商务BD专员. Do not change the global KOL_REVIEWER_JOB_TITLE (would also change new prospecting/report recipients).
- `KOL_PARTNERSHIP_FRANKIE_ONLY`: default 1; 0 enables live partnership recipients only after acceptance.

Explicit reply/affiliate_quote/ship_confirm/tracking_followup/warm_recap drafts use the partnership route. Cold/followup/unknown sources retain their existing route; this does not authorize new outreach. Ship confirmation, tracking-card notification, warm recap, upload registration and SLA source-separated digests use the new resolver. Mail body, mail sending and approval conditions are unchanged.

## Verification and remaining gates

- Live readonly contact lookup: 商务BD专员 matched 杨佳敬; 陈翔宇 active, title 亚马逊运营专员; old 独立站运营专员 still matched 叶星. These are current lookup observations, not completion of HR departure.
- Final 1112 local tests passed (0 failures/errors); existing user work was present during this full-suite run.
- Local commits: e0a1d43 and f9ddd12. Review fixes include explicit reminder-/nudge- fulfilment scope, independent existing/legacy P2 claims, success-group-only record marking, and categorized CS identity errors. Both review axes re-reviewed with no new blocking findings; a persistent-marker isolation test was then added and passed.
- Readonly backlog count (before any test records): 136 waiting CS tickets and 3 escalated tickets under 张佳烨; 4 pending KOL drafts (1 reply, 3 warm_recap, all 待修改). Closed and non-CS records are excluded from handoff.
- Production version, recipient availability in sending App, Base operation permissions, actual Frankie-only card/callback acceptance and unclosed-ticket migration still need validation before enabling.
- Prior cards and associated owners must be audited and handed over with bounded record IDs; do not reset completed statuses, clear all message IDs, or mass re-send. A changed future resolver alone does not migrate old cards.
- `auto_send.py` change is limited to the tracking notification resolver. Before deployment use the required email test guard and avoid running mail pipelines against real rows; never conflate recipient flags with EMAIL_DRY_RUN_TO.
- Rollback: unset the new keys and redeploy verified previous commit; this restores old role logic but does not solve the vacant role. Do not declare rollback operationally safe without temporary manual coverage.

## Business boundary

Social-review code currently includes a Frankie-only synthetic X review test: save an edited English draft/review result, zero customer/platform sends. Discord tester safety alerts concern product-test safety reports and go to the existing escalation recipients. Neither is equivalent to normal SocialEcho comments/DM replies. No SocialEcho or Discord owner was changed here.

# Temporary CS / existing-partnership handoff (2026-09-27)

## Scope and status

User authorizes independent-site customer service to 陈翔宇, and existing KOL/media fulfilment to 杨佳敬 (exact job title 商务BD专员). SocialEcho interaction replies are separately assigned to the foreign-trade team; Discord/tester incidents and synthetic social-review cards are NOT included.

Status (2026-09-27): scoped production release a46caf2 is RUNNING on kol-automation, deployment 6ab8edcea26d1d2fd28529c7, release branch handoff-p0-20260927. DTC remains on master and was not deployed. Historical notes below describe earlier gates, not the latest state. Old-ticket transfer is in progress; recipient client confirmation remains open. Existing dirty enrich/scoring/launch files are excluded.

## Production acceptance and permission decision

- 1118 tests passed on a clean release worktree; six n8n guard assertions passed. Both code-review axes completed.
- The Event Hub YjTXaoWAcy89xZpT now contains Temporary Handoff Authorization after Parse Message. Its original 80 nodes are unchanged. Guard failures stop processing before business handlers; execution 1406533 reached the handler and 1406534 was denied before it.
- CS callbacks independently enforce current independent-site owner or Frankie. Actual live synthetic-terminal replay allows Chen, rejects a former/fake identity, and makes no repeated state change. KOL readonly live authorization allows Yang, rejects an invalid identity.
- User confirmed screenshots of synthetic CS escalation and KOL rejection with original buttons removed. These are replay tests, not user approvals of real customer business.
- CS temporary operator=陈翔宇; partnership title=商务BD专员; both Frankie-only flags=0. New prospecting retains its original role; SocialEcho/Discord and DTC are outside this change.
- EMAIL_DRY_RUN_TO was enabled during deployment then removed and redeployed. Auto Send, Hourly Autonomous Refill, Daily Feedback Control were briefly paused and all three restored to their original active=true state. No real mail pipeline was invoked for testing.
- User chose card-only handling with existing Base permissions unchanged. Yang's direct KOL permission remains view; no advanced permissions enabled. If a workflow requires direct table editing, list the specific gap rather than granting whole-Base edit.
- Four existing-partnership pending drafts were resent to Yang with receipts and readback, original 待修改 state preserved. Three old cards could not be patched because Feishu returned 230013 (bot unavailable to former recipient); six other old cards were retired. Backend authorization still rejects non-current operators. Do not falsely mark all old card visuals removed.
- CS transfer completed for all 139 unclosed records (136 waiting, three escalated), excluding synthetic/closed records. All owner writes and unchanged statuses were read back individually and by final filtered scan. Ten waiting cards sent to Chen; 126 remain unsent (ten within 30 days, 116 older). Three escalated cases need priority review. All 136 old CS cards could not be visually retired; sampled same-App GET/PATCH returned 230013. Callback owner enforcement is live. Recipient client acknowledgement remains pending.
- Receipt journal (IDs/status only; no customer bodies): C:/tmp/handoff-transfer-20260927/receipts.jsonl. Do not rerun sends without checking this journal.

## Configuration

- `CS_INDEPENDENT_SITE_TEMP_OPERATOR`: empty preserves normal role routing. Set to the verified existing operator 陈翔宇 for temporary coverage; contact name, union ID and active status are checked per lookup. Other platform routing is unchanged.
- `CS_INDEPENDENT_SITE_TEMP_FRANKIE_ONLY`: default 1, routes this override to Frankie until acceptance; 0 enables intended owner. It controls recipients only, NOT outbound email safety. Never use live customer rows as tests.
- `KOL_PARTNERSHIP_JOB_TITLE`: empty preserves old routes; target 商务BD专员. Do not change the global KOL_REVIEWER_JOB_TITLE (would also change new prospecting/report recipients).
- `KOL_PARTNERSHIP_FRANKIE_ONLY`: default 1; 0 enables live partnership recipients only after acceptance.

Explicit reply/affiliate_quote/ship_confirm/tracking_followup/warm_recap drafts use the partnership route. Cold/followup/unknown sources retain their existing route; this does not authorize new outreach. Ship confirmation, tracking-card notification, warm recap, upload registration and SLA source-separated digests use the new resolver. Mail body, mail sending and approval conditions are unchanged.

## Historical implementation checkpoint (before production acceptance above)

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

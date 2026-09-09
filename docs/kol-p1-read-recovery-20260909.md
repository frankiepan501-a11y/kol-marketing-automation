# KOL P1 recovery scope and verification

## Approved scope

Frankie approved KOL-only production remediation based on current production
87367c1: bounded verification, no customer emails, no bulk business replay,
no CS/B2B changes. Test seams explicitly confirmed: complete KOL table reads
must not return partial data on failure; cancelled jobs must not remain running
or replay business effects.

## Changes

- `app/feishu.py`: only T_KOL/T_EDITOR/T_DRAFT complete reads get at most two
  additional attempts, after 30/60 seconds, for exhausted 1254607 errors.
  The same page and collected rows are retained; budget is per complete scan.
  Existing duplicate/missing-token guards and terminal failure remain intact.
  Other tables and writes do not receive this policy.
- `app/main.py`: launch job cancellation records error in memory, attempts
  durable error persistence with a five-second bound, and re-raises cancellation.
  It does not retry the business operation or send an interruption card.

## Evidence and limits

Both new regression scenarios failed before their fixes. Targeted suite:
63 passed and 3 subtests passed. Production 1254607 is intermittent; this is
bounded recovery, not proof that the external source's cause was fixed.
Extra waits total at most 90 seconds per complete read, in addition to existing
API retry time. These are background jobs, not synchronous HTTP batch requests.
Forced process termination can prevent durable persistence; the existing restart
status reconciliation remains necessary. No automatic replay is introduced.

Deployment must use a quiet window, recheck origin/master before publication,
and verify the deployed commit and read-only health afterward. Do not overwrite
concurrent commits. R10 stays unaccepted until natural business evidence passes.

## Rollback

Revert only this change on the latest branch, never reset the shared service to
an old commit containing outdated CS/B2B code. No env or schema changes required.

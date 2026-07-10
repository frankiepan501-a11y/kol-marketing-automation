# B2B LinkedIn Market Priority And Async Audit Handoff - 2026-07-10

## Problem

- 2026-07-10 online audit showed both 08:20 discovery and 09:00 auto-pool n8n executions were `success`, but backend `/jobs/{job_id}` returned `job not found`.
- The same day pool summary showed real production output: `new_records=50`, `new_companies=13`, `current_queue_total=211`.
- The false audit error was caused by async job results being stored only in service memory; a service restart or redeploy can erase that cache after the business write already happened.
- Frankie also asked recent B2B LinkedIn intake to prioritize Thailand, Japan, and Southeast Asia.

## Changes

- Added `B2B_LINKEDIN_PRIORITY_MARKETS`, defaulting to Thailand, Japan, Singapore, Malaysia, Vietnam, Indonesia, and Philippines.
- Candidate refill and 09:00 auto-pool candidate consumption now sort by market priority before score/company fallback.
- External discovery query packs now include Thailand, Malaysia, Philippines, Indonesia, and Vietnam, and query-pack execution order uses the same market priority.
- Added maintained seed companies for Japan, Thailand, Singapore, Malaysia, Vietnam, Indonesia, and Philippines so the candidate pool can be replenished near-term even while no search provider key is configured.
- Async audit now falls back to durable Bitable state when a backend job cache is missing:
  - `auto_pool`: checks same-day pool summary.
  - `discovery`: checks current discovery dry-run/waterline status.
- Missing search-provider credentials no longer trigger daily red alerts while discovery is skipped because the candidate pool is already above target.

## Verification

- Targeted B2B tests:
  - `.venv\Scripts\python.exe -m unittest tests.test_b2b_linkedin_auto_pool tests.test_b2b_linkedin_discovery tests.test_b2b_linkedin_async_audit`
  - Result: 23 tests passed.
- Syntax check:
  - `.venv\Scripts\python.exe -m py_compile app\b2b_linkedin_auto_pool.py app\b2b_linkedin_discovery.py app\b2b_linkedin_async_audit.py`
  - Result: passed.
- Full test note:
  - `.venv\Scripts\python.exe -m unittest discover -s tests` ran 105 tests with one unrelated existing failure in `tests/test_zeabur_watchdog.py::test_run_once_alerts_any_project_service_failed_deployment`.

## Operational Notes

- To adjust priority without code changes, update Zeabur env `B2B_LINKEDIN_PRIORITY_MARKETS`.
- Search provider credentials are still not configured. This is acceptable while candidate waterline is high, but if pending candidates fall below target, discovery will alert until a real provider key or manual result source is configured.
- This change does not alter salesperson card dispatch limits or LinkedIn execution behavior. It only changes which candidate companies are consumed first.
- Maintained seed expansion is a stopgap, not a replacement for external discovery. Once the new SEA/Japan seeds are consumed, fresh company discovery still needs Google/SerpAPI/manual input.

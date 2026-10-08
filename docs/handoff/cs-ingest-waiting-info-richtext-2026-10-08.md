# Customer service ingest: waiting-info reply recovery (2026-10-08)

Two Powkong messages repeatedly failed in `/cs/ingest` while the mailbox sources were healthy. A diagnostic-only change identified `waiting_info_reply` and then `_customer_email` as the failure point. The waiting ticket's `客户标识` came back from Bitable as a rich-text list; `_customer_email` passed that list directly to `parseaddr`, raising `AttributeError` before the reply could be merged into the ticket.

`app/cs_ingest.py` now normalizes the field through the existing `_field_text` helper before parsing the address. The change does not alter the outgoing template or enable customer email; production `CS_INFO_REQUEST_LIVE` was `0` when checked. The targeted test covers a Bitable rich-text address and confirms the disabled-send path makes no Zoho call. Run `tests/test_cs_ingest_recovery.py` for regression; 43 tests passed locally.

After deployment, check the next natural `/cs/ingest` result for `message_errors=0` and verify the two waiting tickets are processed once. Keep single-message replay as a fallback only after confirming no duplicate ticket or message; do not rerun the whole mailbox. If the new deployment changes unrelated behavior, restore the prior running deployment and keep the existing deduplication records intact.

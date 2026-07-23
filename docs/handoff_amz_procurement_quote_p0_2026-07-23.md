# Amazon Europe procurement quote card P0 handoff (2026-07-23)

## Status

P0 code is implemented and pushed to `master`.

Deploy checkpoint:
- Latest commit: `7a63d1b`
- Zeabur `kol-automation` production deployment `6a618ce99cfc4cd5e689680c` is `RUNNING` on commit `7a63d1b79d3e9ad02f7c37112f535572333ecb2c`.
- Online `/openapi.json` includes `POST /cs/amz-procurement-quote/send`.
- Unauthenticated call to the endpoint returns `401`, confirming the route is live and protected.

Scope:
- One category/batch sends one shared card.
- Each product row has its own procurement cost, 1688 supplier link, note input, and submit button.
- Submitting one product updates only that candidate record.
- The original card is patched after callback. Completed products render as read-only, pending products keep their own inputs.
- P0 sending is Frankie-only by default.

## Candidate Table

Base:
- app token: `UvNcbvWufaPMSvseOogcBhbFn1y`
- table id: `tblrIPsxm3E8ZCXn`
- batch id: `AMZ-DE-PROCQ-20260723-P0`

Fields added on 2026-07-23:
- `产品中文名`
- `样本ASIN主图URL`
- `套装内容`
- `1688供应商链接`
- `采购回填状态`
- `采购回填人`
- `采购回填时间`
- `采购备注`
- `采购卡片批次ID`
- `采购卡片消息ID`

P0 records prepared:
- `recvq1QtafnVjX` / `B0CH1817WW` / 2 pieces
- `recvq1QtFKPwoI` / `B0CSCXSHPQ` / 11 pieces
- `recvq1QtUEEcXv` / `B0D1CLBFD9` / 2 pieces
- `recvq1Quaar3h2` / `B0CNRH4GRJ` / 5 pieces

Current P0 state after Frankie-only click tests and manual reconciliation:
- `recvq1QtafnVjX` / `B0CH1817WW`: `采购回填状态=已回填`, `采购成本RMB=4`, `1688供应商链接` and legacy `采购链接` filled.
- `recvq1QtFKPwoI` / `B0CSCXSHPQ`: `采购回填状态=已回填`, `采购成本RMB=40.1`, `1688供应商链接` and legacy `采购链接` filled.
- `recvq1QtUEEcXv` / `B0D1CLBFD9`: `采购回填状态=已回填`, `采购成本RMB=12.5`, `1688供应商链接` and legacy `采购链接` filled.
- `recvq1Quaar3h2` / `B0CNRH4GRJ`: remains `采购回填状态=待回填`.
- All four have `采购卡片批次ID=AMZ-DE-PROCQ-20260723-P0`.
- Latest card message id is `om_x100b69249b8e70a0c00088987697b04`; the original card is patched to `待采购回填 1/4`.
- Amazon main-image URL is filled from the public Amazon page `og:image`.

## Code Changes

Files:
- `app/amz_procurement_quote.py`
- `app/amz_assistant.py`
- `app/main.py`
- `.env.example`
- `tests/test_amz_procurement_quote.py`

New endpoint:

```text
POST /cs/amz-procurement-quote/send
```

Parameters:
- `mode=dry_run|commit`
- `limit=4`
- `batch_id=AMZ-DE-PROCQ-20260723-P0`
- `record_ids=rec1,rec2` optional explicit record ids
- `frankie_only=true` default
- `gray_union_ids=on_x,on_y` optional, only used when `frankie_only=false` and env allows gray
- `gray_chat_ids=oc_x` optional, only used when `frankie_only=false` and env allows gray

Callback route:
- Feishu callback still enters `POST /amz/feishu/callback`.
- `value.action` starting with `amz_proc_quote_` routes to `app.amz_procurement_quote.handle_callback`.
- Existing `amz_issue_*` review-audit cards are unchanged.

## Environment

New optional env:

```text
AMZ_PROCUREMENT_CANDIDATE_APP_TOKEN=UvNcbvWufaPMSvseOogcBhbFn1y
AMZ_PROCUREMENT_CANDIDATE_TABLE_ID=tblrIPsxm3E8ZCXn
AMZ_PROCUREMENT_FEISHU_API_WHICH=notify
AMZ_PROCUREMENT_CARD_FRANKIE_ONLY=1
AMZ_PROCUREMENT_DEFAULT_BATCH_ID=AMZ-DE-PROCQ-20260723-P0
AMZ_PROCUREMENT_GRAY_UNION_IDS=
AMZ_PROCUREMENT_GRAY_CHAT_IDS=
```

The callback writes Bitable through `feishu.api(which=AMZ_PROCUREMENT_FEISHU_API_WHICH)`.
Before live card sending, confirm the chosen Feishu App has access to the candidate Base.
Callback writes the new field `1688供应商链接` and also mirrors the same URL into legacy field `采购链接`.

Important failure guard added on 2026-07-23:
- Feishu HTTP 200 is not enough. `app.amz_procurement_quote._feishu_api()` checks Feishu response body `code`; non-zero codes raise and prevent the card from being patched as a false success.
- This was added after the first click test patched the card to "采购已回填" while the candidate Base row remained `待回填`.
- The manually reconciled row is `recvq1QtafnVjX` / `B0CH1817WW`.

Second click guard added on 2026-07-23:
- The card can show a success toast from the fast callback path while the background write has not actually completed. A duplicate click inside the in-memory callback window must not be treated as final success unless the candidate Base row already has `采购回填状态=已回填` and `采购成本RMB`.
- `handle_callback()` now checks the current candidate row on duplicate callbacks. If the row is still pending, it retries the background write and original-card PATCH.
- Feishu millisecond datetime values are formatted before rendering the card, so `采购回填时间` displays as `YYYY-MM-DD HH:mm` instead of a raw timestamp.
- The manually reconciled second row is `recvq1QtFKPwoI` / `B0CSCXSHPQ`, cost `40.1`, supplier link from 1688 offer `934664075097`.

Third click/input fix added on 2026-07-23:
- The previous parser only covered flat `action.form_value`. It now flattens flat form values, nested form-name payloads such as `proc_quote_form_<record_id>`, `input_values` lists, and JSON-string `card_form_value`.
- `send_quote_card()` now runs `validate_quote_card()` before sending. If any product is missing the Amazon Listing button, image button, candidate record button, cost/link/note inputs, or `form_submit` payload, the card raises instead of being sent.
- New local pre-send self-test: `scripts/amz_procurement_card_selftest.py`. It validates button/input wiring and simulates record writeback + original-card PATCH for flat, nested, and list callback shapes.
- The manually reconciled third row is `recvq1QtUEEcXv` / `B0D1CLBFD9`, cost `12.5`, supplier link from 1688 offer `1049232514744`.

## Verification

Local:

```powershell
C:\tmp\py311-embed\python.exe -m py_compile app\amz_procurement_quote.py app\amz_assistant.py app\main.py
```

```powershell
@'
import os, sys, unittest
sys.path.insert(0, os.getcwd())
suite = unittest.defaultTestLoader.discover('tests', pattern='test_amz_procurement_quote.py')
result = unittest.TextTestRunner(verbosity=2).run(suite)
raise SystemExit(0 if result.wasSuccessful() else 1)
'@ | C:\tmp\py311-embed\python.exe -
```

```powershell
@'
import os, sys, unittest
sys.path.insert(0, os.getcwd())
suite = unittest.defaultTestLoader.discover('tests', pattern='test_amz_review_audit.py')
result = unittest.TextTestRunner(verbosity=1).run(suite)
raise SystemExit(0 if result.wasSuccessful() else 1)
'@ | C:\tmp\py311-embed\python.exe -
```

Result:
- `test_amz_procurement_quote.py`: 16 tests passed.
- `test_amz_review_audit.py`: 18 tests passed.

2026-07-23 card self-test command:

```powershell
C:\tmp\py311-embed\python.exe scripts\amz_procurement_card_selftest.py
```

Result:
- Card structure passed.
- Checked: Amazon Listing button, image button, candidate record button, cost/link/note inputs, form_submit payload, callback record update, original card patch.
- Callback shapes passed: `flat_form_value`, `nested_form_value`, `input_values_list`.

2026-07-23 post-click verification:
- Candidate Base single-record read confirms `B0CH1817WW` is now `已回填`, cost `4`, both supplier URL fields populated.
- IM message read confirms card `om_x100b69249b8e70a0c00088987697b04` is `msg_type=interactive`, `updated=true`, title `待采购回填 3/4`, and only `B0CH1817WW` is read-only.
- Online `/health` returned `{"status":"ok"}`.
- Zeabur latest deployment is `RUNNING` on commit `7a63d1b79d3e9ad02f7c37112f535572333ecb2c`.

2026-07-23 second-product verification:
- Candidate Base single-record read confirms `B0CSCXSHPQ` is now `已回填`, cost `40.1`, and both `1688供应商链接` and `采购链接` contain the provided 1688 offer URL.
- IM message read confirms card `om_x100b69249b8e70a0c00088987697b04` is still `msg_type=interactive`, title `待采购回填 2/4`, and the card text includes `B0CSCXSHPQ` with `采购已回填` and `采购成本: 40.1 RMB`.

2026-07-23 third-product verification:
- Candidate Base single-record read confirms `B0D1CLBFD9` is now `已回填`, cost `12.5`, and both `1688供应商链接` and `采购链接` contain the provided 1688 offer URL.
- IM message read confirms card `om_x100b69249b8e70a0c00088987697b04` is still `msg_type=interactive`, title `待采购回填 1/4`, and the card text includes `B0D1CLBFD9` with `采购已回填` and `采购成本: 12.5 RMB`.
- Real-record dry-run validation for all four P0 rows returned `errors=[]`; the only pending product is `B0CNRH4GRJ`.

Online health checked:

```text
GET https://kol-auto.zeabur.app/amz/feishu/callback
-> {"ok":true,"service":"amz-feishu-callback","configured":true}
```

## Remaining Before Real P0 Send

P0 live card is not sent from local because this machine does not expose:
- `INTERNAL_TOKEN`
- `FEISHU_NOTIFY_APP_ID`
- `FEISHU_AMZ_ASSISTANT_APP_ID`

On 2026-07-23, n8n MCP list-workflows returned `AUTHENTICATION_ERROR`, so it could not be used as a safe token-bearing trigger path.

Next smallest live step after deploy:

```text
POST https://kol-auto.zeabur.app/cs/amz-procurement-quote/send?mode=dry_run&batch_id=AMZ-DE-PROCQ-20260723-P0&limit=4
Authorization: Bearer <INTERNAL_TOKEN>
```

If dry-run succeeds, run:

```text
POST https://kol-auto.zeabur.app/cs/amz-procurement-quote/send?mode=commit&batch_id=AMZ-DE-PROCQ-20260723-P0&limit=4&frankie_only=true
Authorization: Bearer <INTERNAL_TOKEN>
```

Before any new procurement card is sent, run `scripts/amz_procurement_card_selftest.py` and a real-record dry-run validation. Do not ask Frankie or procurement to be the first tester for button/input wiring.

Then click one remaining product on the received card only after the above self-tests pass, and verify:
- that product row becomes `采购回填状态=已回填`;
- `采购成本RMB`, `1688供应商链接`, and legacy `采购链接` are populated;
- original card is patched, with completed products read-only and pending products still editable.

After Frankie-only passes, enable a procurement gray run:

```text
AMZ_PROCUREMENT_CARD_FRANKIE_ONLY=0
AMZ_PROCUREMENT_GRAY_UNION_IDS=<采购灰度人员union_id逗号分隔>
AMZ_PROCUREMENT_GRAY_CHAT_IDS=<采购灰度群chat_id，可选>
```

Then call:

```text
POST https://kol-auto.zeabur.app/cs/amz-procurement-quote/send?mode=commit&batch_id=AMZ-DE-PROCQ-20260723-P0&limit=4&frankie_only=false
Authorization: Bearer <INTERNAL_TOKEN>
```

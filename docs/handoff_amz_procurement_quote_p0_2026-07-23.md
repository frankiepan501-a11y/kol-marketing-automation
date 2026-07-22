# Amazon Europe procurement quote card P0 handoff (2026-07-23)

## Status

P0 code is implemented locally for the Germany candidate-table procurement-cost callback loop.

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

All four have:
- `采购回填状态=待回填`
- `采购卡片批次ID=AMZ-DE-PROCQ-20260723-P0`
- Amazon main-image URL filled from the public Amazon page `og:image`.

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
- `test_amz_procurement_quote.py`: 6 tests passed.
- `test_amz_review_audit.py`: 18 tests passed.

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

Then click one product on the received card and verify:
- that product row becomes `采购回填状态=已回填`;
- `采购成本RMB`, `1688供应商链接`, and legacy `采购链接` are populated;
- original card is patched, with only that product read-only and other products still editable.

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

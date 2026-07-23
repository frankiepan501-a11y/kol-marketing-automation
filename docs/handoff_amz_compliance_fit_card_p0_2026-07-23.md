# Amazon Europe compliance / fitment card P0 handoff (2026-07-23)

## Status

P0 code is implemented locally and verified with local tests.

Scope:
- One batch sends one shared compliance/fitment card.
- Each product row has its own compliance result, IP/appearance risk, note input, and submit button.
- Submitting one product updates only that candidate record.
- The original card is patched after callback. Completed products render as read-only, pending products keep their own controls.
- P0 sending is Frankie-only by default.

Default P0 rows:
- `recvq1QtafnVjX` / `B0CH1817WW`
- `recvq1QtUEEcXv` / `B0D1CLBFD9`

## Candidate Table

Base:
- app token: `UvNcbvWufaPMSvseOogcBhbFn1y`
- table id: `tblrIPsxm3E8ZCXn`
- batch id: `AMZ-DE-FITCHECK-20260723-P0`

This P0 does not add new fields. It writes existing candidate fields:
- `合规闸结论`
- `IP/外观风险`
- `侵权风险说明`
- `当前状态`
- `综合结论`
- `数据缺口`
- `下一步动作`
- `人审备注`

Writeback mapping:

| Card selection | Required note | Fields written |
|---|---:|---|
| `Go` + risk `低/中` | optional | `合规闸结论=Go`, `当前状态=待50件验证`, `综合结论=50件验证`, `下一步动作=发起50件验证`, `数据缺口=[]` |
| `需整改` or risk `高` | required | `合规闸结论=暂缓`, `当前状态=待合规核查`, `综合结论=暂缓`, `下一步动作=查合规/型号适配`, `数据缺口=["认证"]` |
| `No-Go` or risk `不可做` | required | `合规闸结论=No-Go`, `当前状态=淘汰`, `综合结论=淘汰`, `下一步动作=淘汰归档`, `数据缺口=["认证"]` |

## Code Changes

Files:
- `app/amz_compliance_fit_card.py`
- `app/amz_assistant.py`
- `app/main.py`
- `.env.example`
- `scripts/amz_compliance_fit_card_selftest.py`
- `tests/test_amz_compliance_fit_card.py`

New endpoint:

```text
POST /cs/amz-compliance-fit/send
```

Parameters:
- `mode=dry_run|commit`
- `limit=2`
- `batch_id=AMZ-DE-FITCHECK-20260723-P0`
- `record_ids=rec1,rec2` optional explicit record ids; when omitted, defaults to the two P0 rows above.
- `frankie_only=true` default
- `gray_union_ids=on_x,on_y` optional, only used when `frankie_only=false` and env allows gray
- `gray_chat_ids=oc_x` optional, only used when `frankie_only=false` and env allows gray

Callback route:
- Feishu callback still enters `POST /amz/feishu/callback`.
- `value.action=amz_fit_check_submit` routes to `app.amz_compliance_fit_card.handle_callback`.
- Existing `amz_proc_quote_*` procurement cards and `amz_issue_*` review-audit cards are unchanged.

## Environment

New optional env:

```text
AMZ_COMPLIANCE_DEFAULT_BATCH_ID=AMZ-DE-FITCHECK-20260723-P0
AMZ_COMPLIANCE_DEFAULT_RECORD_IDS=recvq1QtafnVjX,recvq1QtUEEcXv
AMZ_COMPLIANCE_CARD_FRANKIE_ONLY=1
AMZ_COMPLIANCE_GRAY_UNION_IDS=
AMZ_COMPLIANCE_GRAY_CHAT_IDS=
```

The callback reuses the procurement candidate table helpers:
- `AMZ_PROCUREMENT_CANDIDATE_APP_TOKEN`
- `AMZ_PROCUREMENT_CANDIDATE_TABLE_ID`
- `AMZ_PROCUREMENT_FEISHU_API_WHICH`

## Card Content

Each product row includes:
- product image embedded from Feishu `image_key` when commit mode can upload it;
- buttons: `打开 Listing`, `查看主图原图`, `打开候选表记录`, and `打开1688供应商` when supplier URL exists;
- ASIN, status, recommended fulfillment, procurement cost, package size, weight, set count, FBA fee, commission;
- A/B/C three-channel margin summary;
- compliance audit hints for fitment, compatible-brand wording, IP/appearance risk, GPSR, labels, packaging, and manuals.

## Verification

Local:

```powershell
C:\tmp\py311-embed\python.exe -m py_compile app\amz_compliance_fit_card.py app\amz_assistant.py app\main.py
```

```powershell
C:\tmp\py311-embed\python.exe scripts\amz_compliance_fit_card_selftest.py
```

Result:
- Card structure passed.
- Checked: Amazon Listing button, image button, candidate record button, 1688 supplier button, embedded image, three-channel margin section, result/risk/note controls, form_submit payload, callback record update, original card patch.
- Callback shapes passed: `flat_form_value`, `nested_form_value`, `input_values_list`.

Target tests:
- `test_amz_compliance_fit_card.py`: 9 tests passed.
- `test_amz_procurement_quote.py`: 17 tests passed.
- `test_amz_review_audit.py`: 18 tests passed.

Known local test runner pitfall:
- The local machine has a stale `C:\tmp\ml-data-sync\app` package on `sys.path`.
- Run tests with the repository root forced to the first `sys.path` entry and that stale path removed, or use the self-test script.

## Next Live Step

After commit/push and Zeabur deployment:

```text
POST https://kol-auto.zeabur.app/cs/amz-compliance-fit/send?mode=dry_run&batch_id=AMZ-DE-FITCHECK-20260723-P0&record_ids=recvq1QtafnVjX,recvq1QtUEEcXv
Authorization: Bearer <INTERNAL_TOKEN>
```

If protected dry-run succeeds, send Frankie-only:

```text
POST https://kol-auto.zeabur.app/cs/amz-compliance-fit/send?mode=commit&batch_id=AMZ-DE-FITCHECK-20260723-P0&record_ids=recvq1QtafnVjX,recvq1QtUEEcXv&frankie_only=true
Authorization: Bearer <INTERNAL_TOKEN>
```

Before any operations/采购/合规 group rollout, require:
- local `scripts/amz_compliance_fit_card_selftest.py` passed;
- protected online dry-run passed;
- Frankie-only real card render confirmed;
- at least one callback writeback verified by candidate table readback and original-card PATCH readback.

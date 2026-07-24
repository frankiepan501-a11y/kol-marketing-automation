# Amazon Europe automated compliance / fitment risk card P0 handoff (2026-07-23, corrected 2026-07-24)

## Status

2026-07-24 correction: the first P0 card was incorrectly designed as a manual `Go / No-Go` compliance review card. That responsibility boundary was wrong.

Current P0 direction:
- the system runs an automated risk scan first;
- risk score `<=60` is puhuo fast-pass: write `Go / 待50件验证` directly and keep issue notes in `侵权风险说明`;
- the card shows automatic findings, evidence, severity, and suggested action only for records above the fast-pass threshold or hard-stop records;
- humans only handle exceptions: confirm the system suggestion, mark a false positive, ask procurement to provide missing evidence, or escalate compliance review;
- procurement is not responsible for IP / appearance / patent risk analysis.

The old manual card action `amz_fit_check_submit` is now treated as a disabled legacy action.

Latest confirmed production state:
- Code commit: `5982587 fix: fast-pass AMZ puhuo compliance scan`
- Zeabur deployment: `6a62e5599cfc4cd5e689a0d2`, status `RUNNING`, commit `5982587`
- Current Frankie-only sample card sent: `om_x100b69190ff3a8b4c4cdbdacbd8da8c`
- First real callback writeback verified for `B0CH1817WW / recvq1QtafnVjX`.
- Fast-pass commit verified for `B0CH1817WW / recvq1QtafnVjX` and `B0D1CLBFD9 / recvq1QtUEEcXv`: both now `Go / 待50件验证`; no new card was sent.

Scope:
- One batch sends one shared automated risk-scan result card.
- Before sending, records with score `<=60` are auto-written to the candidate table and removed from the card queue.
- Each card product row shows automatic findings for fitment, compatible-brand wording, IP/appearance, patent clues, EU/GPSR, supplier evidence, and data gaps.
- Each product row has one exception-handling form with business-facing options: `采纳系统建议，自动进入下一步 / 系统判断有误，退回复核 / 资料不够，采购补资料 / 风险较高，升级合规复核`.
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

Automated scan output:

| Scan dimension | P0 check |
|---|---|
| 型号适配 | title / Chinese name / set content / size / weight / set count |
| 品牌词/IP | Dreame, Xiaomi, Roborock, Dyson, iRobot, etc.; original/genuine/OEM-style wording |
| 外观/专利线索 | known-brand consumable/accessory wording, generic structure risk, missing real-product evidence |
| EU/GPSR | GPSR responsible person, local-language label/warning/manual readiness |
| 采购资料 | 1688 supplier URL, package size, weight, set content |
| 限制类线索 | battery, charger, toy, child, food contact, cosmetic, medical, adult, etc. |

Writeback mapping:

| System condition | Card? | Fields written |
|---|---:|---|
| risk score `<=60` and no hard-stop decision | no | system writes `合规闸结论=Go`, `当前状态=待50件验证`, `综合结论=50件验证`, `下一步动作=发起50件验证`; issue list remains in `侵权风险说明` |
| risk score `>60`, reject score, or hard-stop | yes | send exception card for operator handling |

| Human action | Required note | Fields written |
|---|---:|---|
| `采纳系统建议，自动进入下一步` | optional | only applies to carded exception rows; agrees with the automated scan and writes the automatic decision: review needed -> `暂缓 / 待合规核查`; reject recommended -> `No-Go / 淘汰` |
| `系统判断有误，退回复核` | required | operator says the automated scan is a false positive; writes `合规闸结论=暂缓`, `下一步动作=复核系统误报后重跑扫描` |
| `资料不够，采购补资料` | optional | missing supplier/product proof; writes `合规闸结论=暂缓`, `数据缺口=["认证","供应商资料"]`, `下一步动作=采购补供应商/包装/实物资料后重跑扫描` |
| `风险较高，升级合规复核` | required | trademark/IP/appearance/patent/platform/EU compliance concern; writes `合规闸结论=暂缓`, `下一步动作=升级合规/IP复核` |

`侵权风险说明` is generated from the automated scan findings plus the human feedback note.

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
- `value.action=amz_fit_check_feedback_submit` routes to `app.amz_compliance_fit_card.handle_callback`.
- `value.action=amz_fit_check_submit` is the old manual review action and returns a disabled-card toast.
- Existing `amz_proc_quote_*` procurement cards and `amz_issue_*` review-audit cards are unchanged.

## Environment

New optional env:

```text
AMZ_COMPLIANCE_DEFAULT_BATCH_ID=AMZ-DE-FITCHECK-20260723-P0
AMZ_COMPLIANCE_DEFAULT_RECORD_IDS=recvq1QtafnVjX,recvq1QtUEEcXv
AMZ_COMPLIANCE_CARD_FRANKIE_ONLY=1
AMZ_COMPLIANCE_FAST_PASS_SCORE=60
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
- automated risk score, risk level, system suggestion, issue list, evidence, and suggested action;
- one exception-handling form, not a blank manual compliance review form.

Rows with risk score `<=60` are not supposed to reach this card in normal send flow. Their issue list is still written to the candidate table so procurement and operations can see attention points during 50-piece validation.

The card must not contain legacy controls:
- `fit_result_*`
- `fit_iprisk_*`
- `确认核查本产品`
- wording that asks procurement or operations to inspect compliance from scratch.

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
- Checked: Amazon Listing button, image button, candidate record button, 1688 supplier button, embedded image, three-channel margin section, automated risk finding section, risk action/note controls, legacy Go/No-Go controls absent, form_submit payload, callback record update, original card patch.
- Callback shapes passed: `flat_form_value`, `nested_form_value`, `input_values_list`.

Target tests:
- `test_amz_compliance_fit_card.py`: 12 tests passed.
- `test_amz_procurement_quote.py`: 17 tests passed.
- `test_amz_review_audit.py`: 18 tests passed.

Known local test runner pitfall:
- The local machine has a stale `C:\tmp\ml-data-sync\app` package on `sys.path`.
- Run tests with the repository root forced to the first `sys.path` entry and that stale path removed, or use the self-test script.

Historical online result before the 2026-07-24 correction:

```text
POST https://kol-auto.zeabur.app/cs/amz-compliance-fit/send?mode=dry_run&batch_id=AMZ-DE-FITCHECK-20260723-P0&record_ids=recvq1QtafnVjX,recvq1QtUEEcXv
```

Old result:
- `ok=true`
- `count=2`
- `card_selftest=passed`
- generated card structure contained the now-disabled manual review controls. Do not reuse that card for business testing.

Historical Frankie-only real send:

```text
POST https://kol-auto.zeabur.app/cs/amz-compliance-fit/send?mode=commit&batch_id=AMZ-DE-FITCHECK-20260723-P0&record_ids=recvq1QtafnVjX,recvq1QtUEEcXv&frankie_only=true
```

Result:
- sent to Frankie union id only;
- `message_id=om_x100b692b9e03c0a4df9d31f797d0b99`;
- commit mode uploaded and embedded 2 product images;
- Feishu message readback confirmed `msg_type=interactive`, product images, Listing buttons, image buttons, candidate-record buttons, supplier buttons, and three-channel margin text.
- This card is obsolete after the 2026-07-24 correction. Its legacy submit action is disabled in code.

Corrected online result after the 2026-07-24 correction:

```text
POST https://kol-auto.zeabur.app/cs/amz-compliance-fit/send?mode=dry_run&batch_id=AMZ-DE-FITCHECK-20260723-P0&record_ids=recvq1QtafnVjX,recvq1QtUEEcXv
```

Result:
- `ok=true`
- `count=2`
- `card_selftest=passed`
- generated card structure contained 2 exception-handling forms, 2 selects, 2 note inputs, `amz_fit_check_feedback_submit`, automated risk findings, Listing links, and the three-channel margin section.
- generated card did not contain legacy `fit_result_*`, `fit_iprisk_*`, or `确认核查本产品`.

Corrected Frankie-only real send:

```text
POST https://kol-auto.zeabur.app/cs/amz-compliance-fit/send?mode=commit&batch_id=AMZ-DE-FITCHECK-20260723-P0&record_ids=recvq1QtafnVjX,recvq1QtUEEcXv&frankie_only=true
```

Result:
- sent to Frankie union id only;
- `message_id=om_x100b6910aa1d9ca0ded8a5f95a39ce0`;
- commit mode uploaded and embedded 2 product images;
- Feishu message readback confirmed `msg_type=interactive`, product images, 8 buttons, automated risk result text, automated issue list text, and no legacy `fit_result_*` / `fit_iprisk_*`.

Readback caveat:
- Feishu message readback returns a simplified/collapsed card body for interactive cards, so it did not expose form controls as normal top-level `form` nodes.
- The online protected dry-run and local selftest both confirm the generated card contains active per-product forms.

## Next Live Step

After commit/push and Zeabur deployment, run the corrected online dry-run:

```text
POST https://kol-auto.zeabur.app/cs/amz-compliance-fit/send?mode=dry_run&batch_id=AMZ-DE-FITCHECK-20260723-P0&record_ids=recvq1QtafnVjX,recvq1QtUEEcXv
Authorization: Bearer <INTERNAL_TOKEN>
```

Expected corrected dry-run:
- `card_selftest=passed`
- card contains `自动风险扫描结果`, `自动发现的问题点`, `risk_action_*`, `risk_note_*`, and `amz_fit_check_feedback_submit`
- card contains `怎么选` and the four business-facing actions;
- card does not contain `fit_result_*`, `fit_iprisk_*`, `确认核查本产品`, `确认系统建议`, or `处理系统建议`.

Then send Frankie-only corrected sample:

```text
POST https://kol-auto.zeabur.app/cs/amz-compliance-fit/send?mode=commit&batch_id=AMZ-DE-FITCHECK-20260723-P0&record_ids=recvq1QtafnVjX,recvq1QtUEEcXv&frankie_only=true
Authorization: Bearer <INTERNAL_TOKEN>
```

Frankie should test one product with:
- action: `采纳系统建议，自动进入下一步`
- note: `P0 automated risk callback test`

Then verify:
- candidate table row is updated according to the automatic scan decision;
- `侵权风险说明` includes the automatic issue list;
- original card is patched in place, the clicked product becomes read-only, and the other product remains actionable.

Before any operations/采购/合规 group rollout, require:
- local `scripts/amz_compliance_fit_card_selftest.py` passed;
- protected online dry-run passed;
- Frankie-only real card render confirmed;
- at least one callback writeback verified by candidate table readback and original-card PATCH readback.

2026-07-24 clarity fix verification:
- Commit `2c5902a` deployed as Zeabur deployment `6a62d3389cfc4cd5e6899e36`, status `RUNNING`; `/health` returned `{"status":"ok"}`.
- Protected online dry-run returned `ok=true`, `count=2`, `card_selftest=passed`.
- Dry-run card JSON confirmed `怎么选`, `采纳系统建议，自动进入下一步`, `系统判断有误，退回复核`, `资料不够，采购补资料`, `风险较高，升级合规复核`.
- Dry-run card JSON confirmed old wording/control absence: no `确认系统建议`, no `处理系统建议`, no `fit_result_*`.
- Frankie-only sample card sent: `message_id=om_x100b69190ff3a8b4c4cdbdacbd8da8c`.
- Feishu IM readback confirmed `msg_type=interactive` and the same new action wording; old wording/control strings were absent.

2026-07-24 first real callback verification:
- Frankie clicked `采纳系统建议，自动进入下一步` on `B0CH1817WW / recvq1QtafnVjX`.
- Candidate table readback confirmed:
  - `合规闸结论=暂缓`
  - `IP/外观风险=中`
  - `当前状态=待合规核查`
  - `综合结论=暂缓`
  - `数据缺口=认证`
  - `下一步动作=按自动风险点补资料/改文案后重扫`
  - `人审备注` includes `处理动作=采纳系统建议，自动进入下一步`
  - `侵权风险说明` includes the automated issue list.
- Original Feishu card readback confirmed `msg_type=interactive`, `updated=true`, and `自动风险处理已完成`.
- Same-card second product `B0D1CLBFD9 / recvq1QtUEEcXv` remained pending (`合规闸结论=待核`), confirming row-level callback isolation.

2026-07-24 puhuo fast-pass correction:
- Commit `5982587` deployed as Zeabur deployment `6a62e5599cfc4cd5e689a0d2`, status `RUNNING`; `/health` returned `{"status":"ok"}`.
- Protected online dry-run for `recvq1QtafnVjX,recvq1QtUEEcXv` returned:
  - `auto_pass_threshold=60`
  - `auto_pass_count=2`
  - `auto_write_count=2`
  - `card_count=0`
  - `card_selftest=skipped_no_card_candidates`
  - both records scored `60 / 中 / auto_pass_with_notes`
- Protected online commit for the same two records returned:
  - `auto_written_record_ids=["recvq1QtafnVjX","recvq1QtUEEcXv"]`
  - `sent=false`
  - `message_ids=[]`
- Candidate table readback confirmed both rows now have:
  - `合规闸结论=Go`
  - `IP/外观风险=中`
  - `当前状态=待50件验证`
  - `综合结论=50件验证`
  - `数据缺口=null`
  - `下一步动作=发起50件验证`
  - `侵权风险说明` includes the automatic issue list: brand/IP wording, appearance/patent clue, EU/GPSR preparation.

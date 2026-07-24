# AMZ Europe Selection Confirmation P0 Handoff

## Purpose

This node closes the selection phase. It produces a Feishu interactive card for
operations / boss confirmation before procurement starts.

It does not start listing validation and does not use a fixed 50-unit quantity.
The old `/cs/amz-validation50/start` endpoint is now a later-stage reference,
not the current selection-phase next step.

## Implemented

- New module: `app/amz_selection_confirmation.py`
- New protected endpoint: `POST /cs/amz-selection-confirmation/send`
- New callback routing: `value.action` prefix `amz_selection_`
- New local self-test: `scripts/amz_selection_confirmation_selftest.py`
- New unit tests: `tests/test_amz_selection_confirmation.py`

## Card Contents

Each candidate product shows:

- Product image, ASIN, Amazon Listing button, main image button, candidate record button, and 1688 supplier button.
- Competitor price and suggested price for DE / UK / FR / IT / ES.
- Suggested purchase quantity per site and total suggested purchase quantity.
- Three-channel economics: A FBA economy, B FBA fast, C FBM-4PX.
- Payback / investment analysis based on procurement cost, selected logistics cost, available gross margin, and suggested quantity.
- Compliance / fitment notes and procurement package notes.

## Quantity Logic

```text
reference_monthly_sales = competitor_avg_monthly_sales * 60%
                        + category_new_product_avg_monthly_sales * 40%

estimated_new_product_monthly_sales = reference_monthly_sales * entry_factor

raw_purchase_qty = estimated_new_product_monthly_sales / 30 * coverage_days
```

Default entry factors:

| Decision | Entry Factor |
|---|---:|
| Go | 12% |
| 条件推进 | 8% |
| 暂缓 | 0% |
| 淘汰 | 0% |

Default coverage days:

| Channel | Coverage Days |
|---|---:|
| A FBA经济线 | 30 |
| B FBA快速线 | 21 |
| C FBM-4PX | 14 |

Final quantity is rounded up by pack multiple and capped by decision:

| Decision | Site Minimum | Site Maximum | Total Maximum |
|---|---:|---:|---:|
| Go | 10 | 80 | 150 |
| 条件推进 | 5 | 30 | 60 |
| 暂缓 | 0 | 0 | 0 |
| 淘汰 | 0 | 0 | 0 |

If monthly-sales fields are missing, the card displays `需补月销` and does not invent purchase quantities.

## Suggested Price Logic

```text
competitor_anchor_price = competitor_median_price * 70%
                        + sample_competitor_price * 30%

suggested_price = competitor_anchor_price * decision_price_coefficient
```

Default coefficients:

- Go: 95%
- 条件推进: 92%
- 暂缓 / 淘汰: no suggested price unless an existing field already supplies one.

## Button Semantics

| Button | Writeback |
|---|---|
| Go | `当前状态=待采购确认`, `综合结论=Go`, `下一步动作=进入采购阶段：采购复核MOQ/交期/同款后下单` |
| 条件推进 | `当前状态=待采购复核`, `综合结论=条件推进`, `下一步动作=条件进入采购阶段：限站点/压价/补月销/复核套装后再下单` |
| 暂缓 | `当前状态=暂缓`, `综合结论=暂缓`, `下一步动作=暂缓采购：补售价/月销/FBA费/合规或供应链资料后重算` |
| 淘汰 | `当前状态=淘汰`, `综合结论=淘汰`, `下一步动作=淘汰归档：不进入本批采购` |

All actions append a line to `人审备注` with actor, batch, system suggestion, and suggested total quantity.

## Current Data Boundary

The current Germany candidate table can already provide procurement cost,
supplier URL, image URL, FBA fee, commission, A/B/C logistics, margins, freight
ratio, finance gate, compliance gate, and risk notes.

The following fields are not yet guaranteed to exist in the candidate table:

- Site-level competitor median / average price for UK / FR / IT / ES.
- Site-level competitor average monthly sales.
- Site-level category-new-product average monthly sales.
- Structured local-account margin fields.
- Selection confirmation batch / product / site-detail tables.

The P0 code supports these fields when present. If they are missing, the card
labels the missing values instead of calculating fake purchase quantities.

## Validation

Passed locally:

```powershell
C:\tmp\py311-embed\python.exe -m py_compile app\amz_selection_confirmation.py app\amz_assistant.py app\main.py
C:\tmp\py311-embed\python.exe tests\test_amz_selection_confirmation.py
C:\tmp\py311-embed\python.exe scripts\amz_selection_confirmation_selftest.py
```

`tests\test_amz_selection_confirmation.py` covers:

- Monthly-sales quantity formula.
- Card content: image, Listing, candidate link, supplier link, site price lines, quantity lines, three-channel economics, payback / investment analysis.
- Four decision buttons.
- Callback writeback and original-card PATCH.
- `amz_assistant` routing for `amz_selection_` actions.

## Recommended Next Step

Run a protected online `dry_run` for the four P0 records, inspect the generated
card JSON, then send one Frankie-only sample card after the missing site-level
monthly-sales fields are either populated or explicitly accepted as missing.

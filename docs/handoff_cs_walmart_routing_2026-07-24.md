# CS Walmart Routing Fix - 2026-07-24

## Problem

Walmart relay support emails were being classified as independent-site tickets because the CS routing fallback sent all non-Amazon, non-Mercado Libre tickets to `独立站 / 张佳烨`.

Example ticket:

- Record: `recvq9Xs5mFsUJ`
- Customer: `pc-3C93FAEC67834D5C9CFC023D2F0004BD@relay.walmart.com`
- Previous route: `独立站 / 张佳烨`
- Correct route: `沃尔玛 / 林明坚`

## Fix

- `app/cs_ingest.py` now treats `walmart`, `relay.walmart.com`, `marketplace.walmart.com`, and `沃尔玛` as deterministic platform signals.
- Walmart tickets are written as `销售平台=沃尔玛` and `分配运营=林明坚`.
- The deterministic rule overrides a bad AI classification such as `platform=独立站`.
- The classifier prompt now lists Walmart as a first-class platform option.

## Live Repair

Ticket `recvq9Xs5mFsUJ` was manually corrected in the CS ticket table:

- `销售平台`: `沃尔玛`
- `分配运营`: `林明坚`
- `状态`: `待回`
- `信息缺口`: `缺订单号`
- New card message ID: `om_x100b69070c840ca8de71b3e8cdcc606`
- The old Zhang Jiaye card was patched to a processed state: `已改派给林明坚`.

No customer email was sent during this repair.

## Verification

- Unit tests: `python -m unittest tests.test_cs_info_request tests.test_cs_dispatch_card tests.test_cs_resources`
- Result: `Ran 24 tests ... OK`
- Code commit: `1f3359b Fix CS Walmart ticket routing`
- Zeabur deployment: `6a63345f4727f1da77de4d4c`
- Deployment status: `RUNNING`
- Health check: `GET https://kol-auto.zeabur.app/health -> 200 {"status":"ok"}`

## Follow-Up Rule

If a marketplace has a dedicated owner, do not let the generic independent-site fallback capture it. Add deterministic sender/domain/platform detection before the fallback route.

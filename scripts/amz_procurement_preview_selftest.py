"""Local self-test for AMZ procurement-stage cards.

This script does not write Feishu records and does not send messages. It checks
both:
- Frankie preview cards stay read-only.
- Procurement review cards contain per-product supplier-review fields and
  submit controls.
"""
from __future__ import annotations

import asyncio
import json
import os
import pathlib
import sys
from typing import Any


ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

for key in (
    "FEISHU_NOTIFY_APP_ID",
    "FEISHU_NOTIFY_APP_SECRET",
    "FEISHU_APP3_ID",
    "FEISHU_APP3_SECRET",
    "FEISHU_APP_TOKEN",
    "T_KOL",
    "T_EDITOR",
    "T_DRAFT",
    "T_KOL_FU",
    "T_EDITOR_FU",
    "T_DASH",
    "T_PRODUCT",
    "T_TASK_KOL",
    "T_TASK_EDITOR",
    "SNOV_CLIENT_ID",
    "SNOV_CLIENT_SECRET",
    "INTERNAL_TOKEN",
):
    os.environ.setdefault(key, "selftest")

from app import amz_procurement_preview as preview  # noqa: E402


def _record(rid: str, asin: str, decision: str, qty: int, cost: float, current_status: str) -> dict[str, Any]:
    return {
        "record_id": rid,
        "fields": {
            "ASIN": asin,
            "候选标题": f"{asin} replacement accessory kit",
            "产品中文名": f"{asin} 替换配件套装",
            "Amazon链接": {"link": f"https://www.amazon.de/dp/{asin}", "text": "Listing"},
            "样本ASIN主图URL": {"link": f"https://m.media-amazon.com/images/I/{asin}.jpg", "text": "Image"},
            "包装尺寸": "12.9,5.5,3.6",
            "商品重量g": "50",
            "套装件数": "2",
            "套装内容": "采购需按Amazon主图核对套装内容和适配型号",
            "采购成本RMB": cost,
            "1688供应商链接": {"link": "https://detail.1688.com/offer/selftest.html", "text": "1688"},
            "采购回填状态": "已回填",
            "三方案推荐履约": "FBA头程-经济线",
            "FBA€": "2.75",
            "佣金€": "3.9",
            "A-采购前可用毛利RMB": "128.38",
            "A-采购前毛利率%": "58.1",
            "A-物流成本RMB": "0.74",
            "A-货运比": "0",
            "A-毛利RMB": "124.38",
            "A-毛利率%": "56.3",
            "B-采购前可用毛利RMB": "127.14",
            "B-采购前毛利率%": "57.5",
            "B-物流成本RMB": "1.98",
            "B-货运比": "0.01",
            "B-毛利RMB": "123.14",
            "B-毛利率%": "55.7",
            "C-采购前可用毛利RMB": "121.44",
            "C-采购前毛利率%": "55.0",
            "C-物流成本RMB": "31.05",
            "C-货运比": "0.17",
            "C-毛利RMB": "117.44",
            "C-毛利率%": "53.2",
            "当前状态": current_status,
            "综合结论": decision,
            "下一步动作": "进入采购阶段：采购复核MOQ/交期/同款后下单",
            "侵权风险说明": "品牌词/IP：兼容配件；Listing 和包装只写 compatible/replacement。",
            "人审备注": f"2026-07-29 selftest: 选品结果确认={decision}; 系统建议={decision}; 建议采购总量={qty}件; 批次=selftest.",
            "DE样本竞品售价": 25.99,
            "DE竞品平均月销量": 100,
            "DE类目新品平均月销量": 50,
        },
    }


async def main() -> dict[str, Any]:
    candidates = [
        preview._candidate_from_record(_record("rec_go", "B0CH1817WW", "Go", 150, 4, "待采购确认")),
        preview._candidate_from_record(_record("rec_go2", "B0D1CLBFD9", "Go", 60, 12.5, "待采购确认")),
        preview._candidate_from_record(_record("rec_cond", "B0CSCXSHPQ", "条件推进", 15, 40.1, "待采购复核")),
        preview._candidate_from_record(_record("rec_hold", "B0CNRH4GRJ", "暂缓", 0, 20, "暂缓")),
    ]
    for candidate in candidates:
        candidate["image_key"] = "img_selftest_key"
    card = preview.build_procurement_preview_card(candidates, "selftest-batch")
    errors = preview.validate_procurement_preview_card(card, candidates)
    if errors:
        raise AssertionError("; ".join(errors))
    preview_rendered = json.dumps(card, ensure_ascii=False)
    if "B FBA快速线（推荐）" in preview_rendered:
        raise AssertionError("B FBA快速线 must not be marked recommended when fulfillment is FBA头程-经济线")
    review_card = preview.build_procurement_preview_card(candidates, "selftest-batch", audience="procurement")
    review_errors = preview.validate_procurement_preview_card(review_card, candidates, audience="procurement")
    if review_errors:
        raise AssertionError("; ".join(review_errors))
    review_rendered = json.dumps(review_card, ensure_ascii=False)
    if "form_submit" not in review_rendered or "提交本产品复核" not in review_rendered:
        raise AssertionError("procurement review card must contain per-product submit forms")
    return {
        "ok": True,
        "card_selftest": "passed",
        "checked": [
            "Frankie preview card without forms",
            "Frankie preview card without callback payload buttons",
            "procurement review card with forms",
            "procurement review card submit payloads",
            "same-product confirmation field",
            "required MOQ / lead time fields",
            "optional tier price field",
            "optional carton-size and weight field",
            "stock and stock quantity fields",
            "supplier conclusion and purchase suggestion fields",
            "ERP boundary wording",
            "route summary",
            "product images",
            "Amazon Listing buttons",
            "main image buttons",
            "candidate record buttons",
            "1688 supplier buttons",
            "three-channel margin comparison",
            "unit-cost wording",
        ],
        "routes": {label: preview_rendered.count(label) for label in preview.ROUTE_LABEL.values()},
    }


if __name__ == "__main__":
    print(json.dumps(asyncio.run(main()), ensure_ascii=False, indent=2))

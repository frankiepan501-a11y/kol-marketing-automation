"""Local self-test for the Amazon selection confirmation card.

This script does not call Feishu. It checks that the card contains the required
business context and that each decision button maps to the expected candidate
table writeback fields.
"""
from __future__ import annotations

import json
import os
import sys


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from app import amz_selection_confirmation as sel  # noqa: E402


def sample_candidate() -> dict:
    record = {
        "record_id": "rec_selftest",
        "fields": {
            "ASIN": "B0CH1817WW",
            "候选标题": "Dreame L20 Ultra replacement filter",
            "产品中文名": "Dreame L20 Ultra 扫地机替换滤网",
            "Amazon链接": {"link": "https://www.amazon.de/dp/B0CH1817WW", "text": "Listing"},
            "样本ASIN主图URL": {"link": "https://m.media-amazon.com/images/I/41Bum-N615L._AC_.jpg", "text": "Image"},
            "包装尺寸": "12.9,5.5,3.6",
            "商品重量g": "50",
            "套装件数": "2",
            "套装内容": "2个替换滤网；采购需按Amazon主图核对滤网尺寸和适配型号 Dreame L20 Ultra",
            "采购成本RMB": 4,
            "1688供应商链接": {"link": "https://detail.1688.com/offer/test.html", "text": "1688"},
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
            "B-物流成本RMB": "1.98",
            "B-毛利RMB": "123.14",
            "B-毛利率%": "55.7",
            "B-货运比": "0.01",
            "C-物流成本RMB": "31.05",
            "C-毛利RMB": "117.44",
            "C-毛利率%": "53.2",
            "C-货运比": "0.17",
            "财务闸结论": "通过",
            "合规闸结论": "Go",
            "当前状态": "待50件验证",
            "综合结论": "50件验证",
            "下一步动作": "发起50件验证",
            "DE样本竞品售价": 25.99,
            "DE竞品中位价": 24.99,
            "DE竞品均价": 26.5,
            "DE竞品平均月销量": 100,
            "DE类目新品平均月销量": 50,
            "UK样本竞品售价": 21.99,
            "UK竞品中位价": 20.99,
            "UK竞品平均月销量": 80,
            "UK类目新品平均月销量": 40,
        },
    }
    candidate = sel._candidate_from_record(record)
    candidate["image_key"] = "img_selftest"
    return candidate


def main() -> int:
    candidate = sample_candidate()
    card = sel.build_selection_confirmation_card([candidate], "AMZ-EU-SELCONF-SELFTEST")
    errors = sel.validate_selection_confirmation_card(card, [candidate])
    if errors:
        print(json.dumps({"ok": False, "errors": errors}, ensure_ascii=False, indent=2))
        return 1

    button_writebacks = {}
    record_ids = [candidate["record_id"]]
    for action in sel.DECISION_ACTIONS:
        payload = sel._payload(candidate, record_ids, action)
        fields = sel._build_update_fields(candidate, action, "selftest", payload)
        button_writebacks[sel.ACTION_TO_DECISION[action]] = fields

    rendered = json.dumps(card, ensure_ascii=False)
    summary = {
        "ok": True,
        "card_selftest": "passed",
        "button_count": rendered.count('"tag": "button"'),
        "contains_image": '"tag": "img"' in rendered,
        "contains_listing_link": "https://www.amazon.de/dp/B0CH1817WW" in rendered,
        "suggested_total_qty": sel._total_suggested_qty(candidate),
        "button_writebacks": button_writebacks,
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

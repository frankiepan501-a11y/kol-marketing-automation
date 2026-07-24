"""Local self-test for AMZ 50-unit validation start cards.

This does not write production Feishu records. It validates card structure and
simulates the commit path with monkeypatched Feishu helpers.
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

from app import amz_validation50 as val50  # noqa: E402


def _candidate(rid: str = "rec_selftest_1") -> dict[str, Any]:
    return {
        "record_id": rid,
        "asin": "B0SELFTEST",
        "site": "DE",
        "title": "Dreame L20 Ultra replacement filter self-test",
        "cn_name": "Dreame L20 Ultra 扫地机替换滤网 self-test",
        "amazon_url": "https://www.amazon.de/dp/B0SELFTEST",
        "image_url": "https://m.media-amazon.com/images/I/selftest.jpg",
        "image_key": "img_selftest_key",
        "package_size": "12,6,4",
        "weight_g": "80",
        "set_count": "2",
        "set_content": "2 filters; fitment must match Dreame L20 Ultra",
        "quote_status": "已回填",
        "quote_cost": 4,
        "supplier_link": "https://detail.1688.com/offer/selftest.html",
        "fulfillment": "FBA头程-经济线",
        "fba_fee_eur": "2.75",
        "commission_eur": "3.9",
        "channels": [
            {
                "code": "A",
                "label": "FBA经济线",
                "aliases": ["FBA头程-经济线", "经济线"],
                "logistics_rmb": "0.74",
                "freight_ratio": "0",
                "margin_rmb": "124.38",
                "margin_rate": "56.3",
            },
            {
                "code": "B",
                "label": "FBA快速线",
                "aliases": ["FBA头程-快速线", "快速线"],
                "logistics_rmb": "1.98",
                "freight_ratio": "0.01",
                "margin_rmb": "123.14",
                "margin_rate": "55.7",
            },
            {
                "code": "C",
                "label": "FBM-4PX",
                "aliases": ["FBM", "4PX", "自发货"],
                "logistics_rmb": "31.05",
                "freight_ratio": "0.14",
                "margin_rmb": "117.44",
                "margin_rate": "53.2",
            },
        ],
        "finance_gate": "通过",
        "compliance_gate": "Go",
        "ip_risk": "中",
        "risk_note": (
            "自动风险扫描：中风险 / 60分 / 60分内快速通过，问题点留档\n"
            "问题点：\n"
            "- [中] 品牌词/IP: 识别到兼容品牌词：Dreame 建议: 只能写兼容/适配关系。\n"
            "- [低] EU/GPSR: 欧洲站上架前需要准备 GPSR 责任人。"
        ),
        "current_status": "待50件验证",
        "overall_decision": "50件验证",
        "next_action": "发起50件验证",
        "validation_status": "未开始",
        "review_note": "selftest previous note",
    }


async def main() -> dict[str, Any]:
    original_get_many = val50._get_candidates_by_ids
    original_update = val50._update_candidate
    original_prepare = val50._prepare_card_images
    original_send = val50.amz_assistant.send_card_to_union
    updates: list[tuple[str, dict[str, Any]]] = []
    sends: list[tuple[str, dict[str, Any]]] = []

    async def fake_get_many(record_ids: list[str]) -> list[dict[str, Any]]:
        return [_candidate(rid) for rid in record_ids]

    async def fake_update(record_id: str, fields: dict[str, Any]) -> None:
        updates.append((record_id, fields))

    async def fake_prepare(candidates: list[dict[str, Any]]) -> None:
        return None

    async def fake_send(union_id: str, card: dict[str, Any]) -> str:
        sends.append((union_id, card))
        return "om_selftest_val50"

    try:
        candidates = [_candidate("rec_selftest_1"), _candidate("rec_selftest_2")]
        card = val50.build_validation50_card(candidates, "selftest-batch", qty=50)
        errors = val50.validate_validation50_card(card, candidates)
        if errors:
            raise AssertionError("; ".join(errors))

        val50._get_candidates_by_ids = fake_get_many
        val50._update_candidate = fake_update
        val50._prepare_card_images = fake_prepare
        val50.amz_assistant.send_card_to_union = fake_send
        result = await val50.start_validation50(
            mode="commit",
            record_ids=["rec_selftest_1", "rec_selftest_2"],
            batch_id="selftest-batch",
            frankie_only=True,
        )
    finally:
        val50._get_candidates_by_ids = original_get_many
        val50._update_candidate = original_update
        val50._prepare_card_images = original_prepare
        val50.amz_assistant.send_card_to_union = original_send

    if result.get("message_ids") != ["om_selftest_val50"]:
        raise AssertionError(f"send path failed: {result}")
    if len(updates) != 2:
        raise AssertionError(f"expected 2 record updates, got {len(updates)}")
    for _, fields in updates:
        if fields.get("50件验证状态") != "进行中":
            raise AssertionError("50件验证状态 not set to 进行中")
        if "进入50件验证" not in (fields.get("人审备注") or ""):
            raise AssertionError("start note missing")

    return {
        "ok": True,
        "card_structure": "passed",
        "commit_path": "passed",
        "checked": [
            "embedded product image",
            "Amazon Listing button",
            "image button",
            "candidate record button",
            "1688 supplier button",
            "three-channel economics",
            "50-unit budget line",
            "validation checklist",
            "no approval form or form_submit",
            "record update fields",
            "Frankie-only send path",
        ],
        "updated": [rid for rid, _ in updates],
        "message_ids": result.get("message_ids"),
    }


if __name__ == "__main__":
    print(json.dumps(asyncio.run(main()), ensure_ascii=False, indent=2))


"""Local self-test for AMZ compliance/fitment cards.

This does not write production Feishu records. It validates card wiring and
simulates callback writeback with monkeypatched Feishu helpers.
"""
from __future__ import annotations

import asyncio
import json
import os
import pathlib
import sys
from typing import Any, Callable


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

from app import amz_compliance_fit_card as fit  # noqa: E402


def _candidate(rid: str = "rec_selftest_1", status: str = "pending") -> dict[str, Any]:
    done = status == "done"
    return {
        "record_id": rid,
        "asin": "B0SELFTEST",
        "title": "Dreame L20 Ultra replacement filter self-test",
        "cn_name": "Dreame L20 Ultra 扫地机替换滤网 self-test",
        "amazon_url": "https://www.amazon.de/dp/B0SELFTEST",
        "image_url": "https://m.media-amazon.com/images/I/selftest.jpg",
        "image_key": "img_selftest_key",
        "package_size": "12,6,4",
        "weight_g": "80",
        "set_count": "2",
        "set_content": "2 filters; fitment must match Dreame L20 Ultra",
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
                "margin_rmb": "99.16",
                "margin_rate": "53.3",
            },
            {
                "code": "B",
                "label": "FBA快速线",
                "aliases": ["FBA头程-快速线", "快速线"],
                "logistics_rmb": "1.98",
                "freight_ratio": "0.01",
                "margin_rmb": "97.93",
                "margin_rate": "52.6",
            },
            {
                "code": "C",
                "label": "FBM-4PX",
                "aliases": ["FBM", "4PX", "自发货"],
                "logistics_rmb": "31.05",
                "freight_ratio": "0.17",
                "margin_rmb": "92.23",
                "margin_rate": "49.5",
            },
        ],
        "current_status": "待合规核查",
        "overall_decision": "50件验证",
        "finance_gate": "财务通过",
        "compliance_gate": "Go" if done else "待核",
        "ip_risk": "低" if done else "待核",
        "risk_note": "selftest done" if done else "",
        "data_gaps": ["认证"],
        "next_action": "发起50件验证" if done else "查合规/型号适配",
        "batch_id": "selftest-batch",
    }


def _form_flat(record_id: str) -> dict[str, Any]:
    sid = fit._safe_id(record_id)
    return {
        f"risk_action_{sid}": "确认系统建议",
        f"risk_note_{sid}": "selftest confirm automated findings",
    }


def _form_nested(record_id: str) -> dict[str, Any]:
    sid = fit._safe_id(record_id)
    return {
        f"risk_feedback_form_{sid}": {
            f"risk_action_{sid}": {"value": "确认系统建议"},
            f"risk_note_{sid}": {"input_value": "selftest nested"},
        }
    }


def _form_list(record_id: str) -> list[dict[str, Any]]:
    sid = fit._safe_id(record_id)
    return [
        {"name": f"risk_action_{sid}", "value": "确认系统建议"},
        {"name": f"risk_note_{sid}", "input_value": "selftest list"},
    ]


async def _callback_smoke(name: str, form_builder: Callable[[str], Any]) -> dict[str, Any]:
    original_get = fit._get_candidate
    original_update = fit._update_candidate
    original_get_many = fit._get_candidates_by_ids
    original_patch = fit.amz_assistant.update_card
    original_prepare = fit._prepare_card_images
    updates: list[tuple[str, dict[str, Any]]] = []
    patches: list[tuple[str, dict[str, Any]]] = []

    async def fake_get(record_id: str) -> dict[str, Any]:
        return _candidate(record_id)

    async def fake_update(record_id: str, fields: dict[str, Any]) -> None:
        updates.append((record_id, fields))

    async def fake_get_many(record_ids: list[str]) -> list[dict[str, Any]]:
        return [_candidate(rid) for rid in record_ids]

    async def fake_patch(message_id: str, card: dict[str, Any]) -> bool:
        patches.append((message_id, card))
        return True

    async def fake_prepare(candidates: list[dict[str, Any]]) -> None:
        return None

    try:
        fit._get_candidate = fake_get
        fit._update_candidate = fake_update
        fit._get_candidates_by_ids = fake_get_many
        fit.amz_assistant.update_card = fake_patch
        fit._prepare_card_images = fake_prepare
        record_id = "rec_selftest_1"
        event = {
            "context": {"open_message_id": "om_selftest"},
            "operator": {"union_id": "on_selftest"},
            "action": {
                "value": {
                    "action": fit.ACTION_SUBMIT,
                    "record_id": record_id,
                    "batch_id": "selftest-batch",
                    "card_record_ids": [record_id, "rec_selftest_2"],
                },
                "form_value": form_builder(record_id),
            },
        }
        result = await fit._process_callback(event)
    finally:
        fit._get_candidate = original_get
        fit._update_candidate = original_update
        fit._get_candidates_by_ids = original_get_many
        fit.amz_assistant.update_card = original_patch
        fit._prepare_card_images = original_prepare

    if (result.get("toast") or {}).get("type") != "success":
        raise AssertionError(f"{name}: callback returned {result}")
    if len(updates) != 1:
        raise AssertionError(f"{name}: expected 1 record update, got {len(updates)}")
    if len(patches) != 1:
        raise AssertionError(f"{name}: expected 1 card patch, got {len(patches)}")
    fields = updates[0][1]
    if fields.get("合规闸结论") != "暂缓":
        raise AssertionError(f"{name}: compliance gate not written from automated scan correctly")
    if fields.get("当前状态") != "待合规核查":
        raise AssertionError(f"{name}: next status not written from automated scan correctly")
    if "自动风险扫描" not in (fields.get("侵权风险说明") or ""):
        raise AssertionError(f"{name}: risk note does not contain automated scan findings")
    rendered = json.dumps(patches[0][1], ensure_ascii=False)
    if "自动风险处理已完成" not in rendered:
        raise AssertionError(f"{name}: patched card does not show completed state")
    return {"shape": name, "updated": updates[0][0], "patched": patches[0][0]}


async def main() -> dict[str, Any]:
    candidates = [_candidate("rec_selftest_1"), _candidate("rec_selftest_2")]
    fit._attach_risk_scans(candidates)
    card = fit.build_fit_card(candidates, "selftest-batch")
    errors = fit.validate_fit_card(card, candidates)
    if errors:
        raise AssertionError("; ".join(errors))
    callback_results = [
        await _callback_smoke("flat_form_value", _form_flat),
        await _callback_smoke("nested_form_value", _form_nested),
        await _callback_smoke("input_values_list", _form_list),
    ]
    return {
        "ok": True,
        "card_structure": "passed",
        "checked": [
            "Amazon Listing button",
            "image button",
            "candidate record button",
            "1688 supplier button",
            "embedded product image",
            "three-channel margin section",
            "automated risk finding section",
            "risk action/note controls",
            "legacy manual Go/No-Go controls absent",
            "form_submit payload",
            "callback record update",
            "original card patch",
        ],
        "callback_results": callback_results,
    }


if __name__ == "__main__":
    print(json.dumps(asyncio.run(main()), ensure_ascii=False, indent=2))

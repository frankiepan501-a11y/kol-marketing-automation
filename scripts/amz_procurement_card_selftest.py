"""Local self-test for AMZ procurement quote cards.

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

from app import amz_procurement_quote as quote  # noqa: E402


def _candidate(rid: str = "rec_selftest_1", status: str = "pending") -> dict[str, Any]:
    completed = status == "done"
    return {
        "record_id": rid,
        "asin": "B0SELFTEST",
        "title": "Self-test replacement filter kit",
        "cn_name": "Self-test filter kit",
        "amazon_url": "https://www.amazon.de/dp/B0SELFTEST",
        "image_url": "https://m.media-amazon.com/images/I/selftest.jpg",
        "image_key": "img_selftest_key",
        "package_size": "12,6,4",
        "weight_g": "80",
        "set_count": "2",
        "set_content": "2 filters",
        "quote_status": "已回填" if completed else "待回填",
        "quote_cost": 18.5 if completed else None,
        "supplier_link": "https://detail.1688.com/offer/done.html" if completed else "",
        "fulfillment": "FBA",
        "fba_fee_eur": "3.04",
        "commission_eur": "2.39",
        "channels": [
            {
                "code": "A",
                "label": "FBA经济线",
                "aliases": ["FBA头程-经济线", "经济线"],
                "pre_margin_rmb": "50",
                "pre_margin_rate": "30",
                "logistics_rmb": "10",
                "freight_ratio": "0.2",
                "margin_rmb": "31.5",
                "margin_rate": "18.9",
            },
            {
                "code": "B",
                "label": "FBA快速线",
                "aliases": ["FBA头程-快速线", "快速线"],
                "pre_margin_rmb": "46",
                "pre_margin_rate": "27.6",
                "logistics_rmb": "14",
                "freight_ratio": "0.28",
                "margin_rmb": "27.5",
                "margin_rate": "16.5",
            },
            {
                "code": "C",
                "label": "FBM-4PX",
                "aliases": ["FBM", "4PX", "自发货"],
                "pre_margin_rmb": "40",
                "pre_margin_rate": "24",
                "logistics_rmb": "20",
                "freight_ratio": "0.4",
                "margin_rmb": "21.5",
                "margin_rate": "12.9",
            },
        ],
        "pre_margin_rmb": "50",
        "pre_margin_rate": "30",
        "logistics_rmb": "10",
        "freight_ratio": "0.2",
        "batch_id": "selftest-batch",
    }


def _form_flat(record_id: str) -> dict[str, Any]:
    sid = quote._safe_id(record_id)
    return {
        f"proc_cost_{sid}": "18.5",
        f"proc_link_{sid}": "https://detail.1688.com/offer/selftest.html",
        f"proc_note_{sid}": "selftest",
    }


def _form_nested(record_id: str) -> dict[str, Any]:
    sid = quote._safe_id(record_id)
    return {
        f"proc_quote_form_{sid}": {
            f"proc_cost_{sid}": {"value": "18.5"},
            f"proc_link_{sid}": {"value": "https://detail.1688.com/offer/selftest.html"},
            f"proc_note_{sid}": {"value": "selftest"},
        }
    }


def _form_list(record_id: str) -> list[dict[str, Any]]:
    sid = quote._safe_id(record_id)
    return [
        {"name": f"proc_cost_{sid}", "value": "18.5"},
        {"name": f"proc_link_{sid}", "value": "https://detail.1688.com/offer/selftest.html"},
        {"name": f"proc_note_{sid}", "input_value": "selftest"},
    ]


async def _callback_smoke(name: str, form_builder: Callable[[str], Any]) -> dict[str, Any]:
    original_get = quote._get_candidate
    original_update = quote._update_candidate
    original_get_many = quote._get_candidates_by_ids
    original_patch = quote.amz_assistant.update_card
    original_prepare = quote._prepare_card_images
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
        quote._get_candidate = fake_get
        quote._update_candidate = fake_update
        quote._get_candidates_by_ids = fake_get_many
        quote.amz_assistant.update_card = fake_patch
        quote._prepare_card_images = fake_prepare
        record_id = "rec_selftest_1"
        event = {
            "context": {"open_message_id": "om_selftest"},
            "operator": {"union_id": "on_selftest"},
            "action": {
                "value": {
                    "action": quote.ACTION_SUBMIT,
                    "record_id": record_id,
                    "batch_id": "selftest-batch",
                    "card_record_ids": [record_id, "rec_selftest_2"],
                },
                "form_value": form_builder(record_id),
            },
        }
        result = await quote._process_callback(event)
    finally:
        quote._get_candidate = original_get
        quote._update_candidate = original_update
        quote._get_candidates_by_ids = original_get_many
        quote.amz_assistant.update_card = original_patch
        quote._prepare_card_images = original_prepare

    if (result.get("toast") or {}).get("type") != "success":
        raise AssertionError(f"{name}: callback returned {result}")
    if len(updates) != 1:
        raise AssertionError(f"{name}: expected 1 record update, got {len(updates)}")
    if len(patches) != 1:
        raise AssertionError(f"{name}: expected 1 card patch, got {len(patches)}")
    fields = updates[0][1]
    if fields.get("采购成本RMB") != 18.5:
        raise AssertionError(f"{name}: cost not written correctly")
    if quote._url(fields.get("1688供应商链接")) != "https://detail.1688.com/offer/selftest.html":
        raise AssertionError(f"{name}: supplier link not written correctly")
    if not isinstance(fields.get("1688供应商链接"), dict):
        raise AssertionError(f"{name}: supplier link must use Feishu URL cell format")
    rendered = json.dumps(patches[0][1], ensure_ascii=False)
    if "采购已回填" not in rendered:
        raise AssertionError(f"{name}: patched card does not show completed state")
    return {"shape": name, "updated": updates[0][0], "patched": patches[0][0]}


async def main() -> dict[str, Any]:
    candidates = [_candidate("rec_selftest_1"), _candidate("rec_selftest_2")]
    card = quote.build_quote_card(candidates, "selftest-batch")
    errors = quote.validate_quote_card(card, candidates)
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
            "three-channel comparison section",
            "cost/link/note inputs",
            "form_submit payload",
            "callback record update",
            "original card patch",
        ],
        "callback_results": callback_results,
    }


if __name__ == "__main__":
    print(json.dumps(asyncio.run(main()), ensure_ascii=False, indent=2))

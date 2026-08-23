"""Preflight KOL SLA digest cards; default is fixture-only and never sends."""
import argparse
import asyncio
import copy
import datetime as dt
import json
import os
import sys
from zoneinfo import ZoneInfo


REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

# Non-secret defaults from this repository's production registry.
os.environ.setdefault("FEISHU_APP_TOKEN", "KINabIENjak8fRsB6AHcIDALntc")
os.environ.setdefault("T_DRAFT", "tblpWteXNX34vds4")

from app import feishu, sla_check  # noqa: E402


def fixture(now_ms):
    def row(rid, source, age):
        return {
            "record_id": rid,
            "fields": {
                "邮件草稿状态": "待审",
                "邮件草稿来源": source,
                "对象类型": "KOL",
                "邮件主题": f"Fixture subject {rid}",
                "生成时间": now_ms - age * 3600 * 1000,
            },
        }

    return [
        row("rec_fixture_reply", "reply", 29),
        row("rec_fixture_quote", "affiliate_quote", 52),
        row("rec_fixture_ship", "ship_confirm", 31),
    ]


def validate_card(card, records):
    payload = json.dumps(card, ensure_ascii=False)
    required = [
        "负责人", "截止", "系统已检查", "不会发送邮件", "真实邮件发送队列",
        "打开在途草稿队列",
    ]
    missing = [text for text in required if text not in payload]
    urls = []
    for element in card.get("elements") or []:
        if element.get("tag") == "action":
            for action in element.get("actions") or []:
                if action.get("url"):
                    urls.append(action["url"])
    direct_links = [
        sla_check._record_url(rec.get("record_id") or "")
        for rec in records[:5]
    ]
    errors = []
    if missing:
        errors.append(f"missing_text={missing}")
    if not urls or any(not url.startswith("https://") for url in urls):
        errors.append("invalid_queue_button")
    if any(link not in payload for link in direct_links):
        errors.append("missing_direct_record_link")
    if '"tag": "form"' in payload or "form_submit" in payload:
        errors.append("digest_must_not_have_form_or_callback")
    return errors


async def read_live(now_ms):
    collected = await sla_check.collect_sla_overdue_drafts(now_ms)
    return collected["p1"], collected


async def send_frankie_sample(card):
    sample = copy.deepcopy(card)
    sample["header"]["title"]["content"] = (
        "Frankie-only 样卡｜" + sample["header"]["title"]["content"]
    )
    sample["elements"].insert(0, {
        "tag": "div",
        "text": {"tag": "lark_md", "content":
            "⚠️ **这是上线前样卡**：只验证排版、数量和链接；不会修改草稿，也不会发送邮件。"},
    })
    targets = await feishu.resolve_notify_targets("frankie")
    if len(targets) != 1:
        raise RuntimeError(f"expected exactly one Frankie target, got {len(targets)}")
    _, open_id = targets[0]
    message_id = await feishu.send_card_message("open_id", open_id, sample, biz="KOL", level="P1")
    readback = await feishu.api("GET", f"/im/v1/messages/{message_id}", which="notify")
    message = (readback.get("data") or {}).get("items") or []
    first = message[0] if message else {}
    return {
        "message_id": message_id,
        "readback_msg_type": first.get("msg_type"),
        "readback_has_content": bool(first.get("body", {}).get("content") or first.get("content")),
    }


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--live", action="store_true", help="Read current Feishu drafts; no writes.")
    parser.add_argument("--send-frankie", action="store_true", help="Send one live sample to Frankie only.")
    args = parser.parse_args()
    if args.send_frankie and not args.live:
        parser.error("--send-frankie requires --live")

    now_ms = int(dt.datetime.now(tz=ZoneInfo("Asia/Shanghai")).timestamp() * 1000)
    if args.live:
        records, collected = await read_live(now_ms)
    else:
        records = fixture(now_ms)
        collected = {"p1": records, "p2": [], "p1_over_48h": [records[1]]}
    if not records:
        raise RuntimeError("no P1 overdue drafts available for the sample")

    card = sla_check.build_sla_digest_card(records, now_ms, audience="reviewer", level="P1")
    errors = validate_card(card, records)
    result = {
        "ok": not errors,
        "mode": "live" if args.live else "fixture",
        "p1_count": len(records),
        "p2_count": len(collected.get("p2") or []),
        "p1_over_48h_count": len(collected.get("p1_over_48h") or []),
        "errors": errors,
        "would_write_drafts": False,
        "would_send_email": False,
        "would_send_group": False,
    }
    if errors:
        print(json.dumps(result, ensure_ascii=False, indent=2))
        raise SystemExit(1)
    if args.send_frankie:
        result["sample"] = await send_frankie_sample(card)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())

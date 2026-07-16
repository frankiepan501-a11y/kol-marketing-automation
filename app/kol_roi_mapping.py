# -*- coding: utf-8 -*-
"""KOL ROI attribution mapping cards.

P1 scope:
- scan outputs create actionable records in `T_ROI_ATTR_GAP`
- this module sends a small operator card for each gap
- callback writes/updates `T_ROI_ATTR_MAP`, closes the gap row, and patches the
  original card.
"""
from __future__ import annotations

import json
import time
from . import config, feishu
from .feishu import ext


ACTION_CONFIRM = "kol_roi_map_confirm"
ACTION_IGNORE = "kol_roi_map_ignore"


def _text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def _now_ms() -> int:
    return int(time.time() * 1000)


def _message_id(event: dict) -> str:
    for value in (
        event.get("message_id"),
        event.get("open_message_id"),
        event.get("card_open_message_id"),
        (event.get("message") or {}).get("message_id"),
        (event.get("context") or {}).get("open_message_id"),
        (event.get("context") or {}).get("message_id"),
    ):
        if value:
            return _text(value)
    return ""


def _operator_label(event: dict) -> str:
    op = event.get("operator") or event.get("operator_user") or event.get("user") or {}
    return (
        _text(op.get("name"))
        or _text(op.get("open_id"))
        or _text(event.get("operator_open_id"))
        or _text(event.get("open_id"))
        or "unknown"
    )


def _extract_action(event: dict) -> tuple[str, dict, dict]:
    action = event.get("action") or {}
    value = action.get("value") or event.get("value") or event.get("card_action") or {}
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            value = {"action": value}
    form = action.get("form_value") or event.get("card_form_value") or {}
    if isinstance(form, str):
        try:
            form = json.loads(form)
        except Exception:
            form = {}
    return _text(value.get("action") or value.get("act")), value, form


async def _find_mapping_record(mapping_key: str) -> str:
    if not mapping_key:
        return ""
    rows = await feishu.search_records(config.T_ROI_ATTR_MAP, [
        {"field_name": "映射键", "operator": "is", "value": [mapping_key]},
    ], field_names=["映射键"])
    return (rows[0] or {}).get("record_id", "") if rows else ""


def build_gap_card(gap_record: dict) -> dict:
    rid = gap_record.get("record_id", "")
    f = gap_record.get("fields", {})
    gap_id = ext(f.get("缺口ID"))
    source = ext(f.get("来源系统"))
    gap_type = ext(f.get("缺口类型"))
    raw_name = ext(f.get("原始名称"))
    raw_email = ext(f.get("原始邮箱"))
    raw_key = ext(f.get("原始链接或活动")) or ext(f.get("来源记录ID"))
    hint = ext(f.get("推荐动作"))
    default_key = raw_email or raw_key or raw_name or gap_id
    value = {
        "action": ACTION_CONFIRM,
        "gap_id": gap_id,
        "gap_record_id": rid,
        "source": source,
    }
    ignore_value = dict(value)
    ignore_value["action"] = ACTION_IGNORE
    return {
        "config": {"wide_screen_mode": True, "update_multi": True},
        "header": {
            "template": "yellow",
            "title": {"tag": "plain_text", "content": f"KOL ROI 归因缺口 · {gap_type}"},
        },
        "elements": [
            {"tag": "div", "fields": [
                {"is_short": True, "text": {"tag": "lark_md", "content": f"**来源**: {source or '-'}"}},
                {"is_short": True, "text": {"tag": "lark_md", "content": f"**缺口ID**: {gap_id or '-'}"}},
                {"is_short": True, "text": {"tag": "lark_md", "content": f"**原始名称**: {raw_name or '-'}"}},
                {"is_short": True, "text": {"tag": "lark_md", "content": f"**原始邮箱**: {raw_email or '-'}"}},
            ]},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"**原始活动/链接**\n{raw_key or '-'}"}},
            {"tag": "note", "elements": [{"tag": "plain_text", "content": hint or "请选择对应 KOL/媒体人，系统写映射表。"}]},
            {"tag": "form", "name": f"roi_map_{gap_id}", "elements": [
                {"tag": "input", "name": "mapping_key", "label_position": "left",
                 "label": {"tag": "plain_text", "content": "映射键"},
                 "default_value": default_key[:200],
                 "placeholder": {"tag": "plain_text", "content": "邮箱、campaignId 或 KOL handle"}},
                {"tag": "input", "name": "kol_record_id", "label_position": "left",
                 "label": {"tag": "plain_text", "content": "KOL记录ID"},
                 "placeholder": {"tag": "plain_text", "content": "rec...，不知道可留空"}},
                {"tag": "input", "name": "kol_name", "label_position": "left",
                 "label": {"tag": "plain_text", "content": "KOL名称"},
                 "placeholder": {"tag": "plain_text", "content": "填写正确账号名/媒体人名"}},
                {"tag": "input", "name": "object_type", "label_position": "left",
                 "label": {"tag": "plain_text", "content": "对象类型"},
                 "default_value": "KOL",
                 "placeholder": {"tag": "plain_text", "content": "KOL / 媒体人 / 未知"}},
                {"tag": "input", "name": "note", "label_position": "left",
                 "label": {"tag": "plain_text", "content": "备注"},
                 "placeholder": {"tag": "plain_text", "content": "可写：新KOL、名称错拼、需要入库"}},
                {"tag": "button", "action_type": "form_submit", "name": "submit",
                 "text": {"tag": "plain_text", "content": "确认写入映射"}, "type": "primary",
                 "value": value},
            ]},
            {"tag": "action", "actions": [
                {"tag": "button", "text": {"tag": "plain_text", "content": "忽略该缺口"},
                 "type": "default", "value": ignore_value},
            ]},
        ],
    }


def build_processed_card(title: str, body: str, template: str = "green") -> dict:
    return {
        "config": {"wide_screen_mode": True, "update_multi": True},
        "header": {
            "template": template,
            "title": {"tag": "plain_text", "content": title},
        },
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md", "content": body}},
        ],
    }


async def _send_to_targets(card: dict, frankie_only: bool) -> list[dict]:
    if frankie_only:
        targets = [u for u in config.NOTIFY_USERS if u[0].startswith("潘")]
    else:
        targets = await feishu.resolve_notify_targets("reviewer")
    sent = []
    for name, open_id in targets:
        union_id = await feishu.open_id_to_union_id(open_id)
        if not union_id:
            sent.append({"name": name, "ok": False, "error": "open_id_to_union_id failed"})
            continue
        try:
            msg_id = await feishu.send_card_via_app3("union_id", union_id, card)
            sent.append({"name": name, "ok": bool(msg_id), "message_id": msg_id})
        except Exception as exc:
            sent.append({"name": name, "ok": False, "error": str(exc)[:200]})
    return sent


async def send_pending_gap_cards(limit: int = 2, dry_run: bool = True,
                                 frankie_only: bool = True, source: str = "") -> dict:
    filters = [
        {"field_name": "处理状态", "operator": "is", "value": ["待发卡"]},
    ]
    if source:
        filters.append({"field_name": "来源系统", "operator": "is", "value": [source]})
    rows = await feishu.search_records(config.T_ROI_ATTR_GAP, filters, field_names=[
        "缺口ID", "缺口类型", "处理状态", "来源系统", "来源记录ID",
        "原始名称", "原始邮箱", "原始链接或活动", "推荐动作", "备注",
    ])
    out = []
    for row in rows[:max(0, int(limit or 0))]:
        card = build_gap_card(row)
        item = {
            "gap_record_id": row.get("record_id"),
            "gap_id": ext(row.get("fields", {}).get("缺口ID")),
            "card_title": card["header"]["title"]["content"],
        }
        if dry_run:
            item["dry_run_card"] = card
        else:
            sent = await _send_to_targets(card, frankie_only=frankie_only)
            ok_msg = next((s.get("message_id") for s in sent if s.get("message_id")), "")
            fields = {"处理状态": "待运营确认"}
            if ok_msg:
                fields["卡片message_id"] = ok_msg
            await feishu.update_record(config.T_ROI_ATTR_GAP, row["record_id"], fields)
            item["sent"] = sent
        out.append(item)
    return {
        "ok": True,
        "dry_run": dry_run,
        "frankie_only": frankie_only,
        "source": source,
        "count": len(out),
        "items": out,
    }


async def handle_callback(event: dict) -> dict:
    action, value, form = _extract_action(event)
    if action not in (ACTION_CONFIRM, ACTION_IGNORE):
        return {"ok": False, "ignored": True, "action": action}
    gap_rid = _text(value.get("gap_record_id"))
    if not gap_rid:
        return {"ok": False, "error": "missing gap_record_id"}

    gap = await feishu.get_record(config.T_ROI_ATTR_GAP, gap_rid)
    gf = gap.get("fields", {})
    gap_id = ext(gf.get("缺口ID"))
    operator = _operator_label(event)
    msg_id = _message_id(event) or ext(gf.get("卡片message_id"))

    if action == ACTION_IGNORE:
        await feishu.update_record(config.T_ROI_ATTR_GAP, gap_rid, {
            "处理状态": "已忽略",
            "操作人": operator,
            "处理时间": _now_ms(),
            "确认结果": "运营卡片选择忽略",
            "回调payload摘要": json.dumps({"action": action, "gap_id": gap_id}, ensure_ascii=False)[:1000],
        })
        if msg_id:
            await feishu.update_card_message_with_app(
                msg_id,
                build_processed_card("KOL ROI 缺口已忽略", f"**缺口ID**: {gap_id}\n**操作人**: {operator}", "grey"),
                which="app3",
            )
        return {"ok": True, "action": action, "gap_id": gap_id, "patched": bool(msg_id)}

    mapping_key = _text(form.get("mapping_key")) or ext(gf.get("原始邮箱")) or ext(gf.get("原始链接或活动")) or gap_id
    kol_record_id = _text(form.get("kol_record_id"))
    kol_name = _text(form.get("kol_name")) or ext(gf.get("原始名称"))
    object_type = _text(form.get("object_type")) or "未知"
    if object_type not in ("KOL", "媒体人", "官方账号", "未知"):
        object_type = "未知"
    note = _text(form.get("note"))
    source = ext(gf.get("来源系统"))
    mapping_fields = {
        "映射键": mapping_key,
        "映射状态": "已确认",
        "对象类型": object_type,
        "KOL记录ID": kol_record_id,
        "KOL名称": kol_name,
        "KOL邮箱": ext(gf.get("原始邮箱")),
        "匹配置信度": "强",
        "来源系统": [s for s in (source, "运营卡片") if s],
        "确认人": operator,
        "确认时间": _now_ms(),
        "最近命中订单": ext(gf.get("来源记录ID")),
        "备注": note,
    }
    if source == "UpPromote":
        mapping_fields["UpPromote affiliate_name"] = ext(gf.get("原始名称"))
        mapping_fields["UpPromote email"] = ext(gf.get("原始邮箱"))
    if source == "Amazon Attribution":
        mapping_fields["Amazon Attribution campaign"] = ext(gf.get("原始链接或活动")) or ext(gf.get("来源记录ID"))

    existing = await _find_mapping_record(mapping_key)
    if existing:
        await feishu.update_record(config.T_ROI_ATTR_MAP, existing, mapping_fields)
        mapping_record_id = existing
        mode = "updated"
    else:
        mapping_record_id = await feishu.create_record(config.T_ROI_ATTR_MAP, mapping_fields)
        mode = "created"

    await feishu.update_record(config.T_ROI_ATTR_GAP, gap_rid, {
        "处理状态": "已确认",
        "操作人": operator,
        "处理时间": _now_ms(),
        "确认结果": f"{mode}: {mapping_key} -> {kol_name or kol_record_id}",
        "回调payload摘要": json.dumps({"action": action, "gap_id": gap_id, "mapping_key": mapping_key}, ensure_ascii=False)[:1000],
    })

    if msg_id:
        await feishu.update_card_message_with_app(
            msg_id,
            build_processed_card(
                "KOL ROI 映射已写入",
                f"**缺口ID**: {gap_id}\n**映射键**: {mapping_key}\n**KOL**: {kol_name or kol_record_id}\n**操作人**: {operator}",
                "green",
            ),
            which="app3",
        )
    return {
        "ok": True,
        "action": action,
        "gap_id": gap_id,
        "mapping_record_id": mapping_record_id,
        "mode": mode,
        "patched": bool(msg_id),
    }

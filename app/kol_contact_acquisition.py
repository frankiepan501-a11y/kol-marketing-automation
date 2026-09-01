"""P0-3A: no-email KOL contact-acquisition card and callback.

The live rollout is deliberately Frankie-only. A test card exercises rendering,
routing, idempotency and original-card PATCH without changing a KOL record.
Operational cards can reuse the same callback after a separate rollout approval.
"""
from __future__ import annotations

import re
import time
from typing import Any

from . import config, feishu


ACTION_PREFIX = "kol_contact_"
TEST_BATCH = "p0-3a-frankie-only-20260901"
_TEST_RECEIPTS: dict[str, dict[str, Any]] = {}
_EMAIL_RE = re.compile(r"^[A-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Z0-9.-]+\.[A-Z]{2,}$", re.I)


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        return " / ".join(filter(None, (_text(item) for item in value)))
    if isinstance(value, dict):
        return _text(value.get("text") or value.get("name") or value.get("link"))
    return str(value).strip()


def _action_value(record_id: str, action: str, *, test_mode: bool) -> dict:
    return {
        "action": f"{ACTION_PREFIX}{action}",
        "record_id": record_id,
        "app_token": config.FEISHU_APP_TOKEN,
        "table_id": config.T_KOL,
        "test_mode": bool(test_mode),
        "test_batch": TEST_BATCH if test_mode else "",
    }


def build_card(record: dict, *, test_mode: bool = True) -> dict:
    fields = record.get("fields") or {}
    record_id = record.get("record_id") or ""
    name = _text(fields.get("账号名")) or "未命名 KOL"
    platform = _text(fields.get("主平台")) or "待确认"
    profile = _text(fields.get("主链接"))
    country = _text(fields.get("国家")) or "待补"
    language = _text(fields.get("语言")) or "待补"
    followers = _text(fields.get("粉丝数")) or "待补"
    content = _text(fields.get("内容垂类") or fields.get("内容风格")) or "待补"
    base_url = (
        "https://u1wpma3xuhr.feishu.cn/base/"
        f"{config.FEISHU_APP_TOKEN}?table={config.T_KOL}&record={record_id}"
    )
    title = "无邮箱首触达卡测试" if test_mode else "请联系 KOL 取得商务邮箱"
    note = (
        "**测试边界**：只验证卡片、回调、重复点击和原卡结果态；"
        "点击不会写 KOL 主表，也不会发邮件/平台私信。"
        if test_mode else
        "系统已完成质量筛选；运营只需在平台原生界面联系一次，取得商务邮箱。"
    )
    profile_button = []
    if profile.startswith(("http://", "https://")):
        profile_button.append({"tag": "button", "text": {"tag": "plain_text", "content": "打开 KOL 主页"}, "url": profile, "type": "primary"})
    profile_button.append({"tag": "button", "text": {"tag": "plain_text", "content": "打开主表记录"}, "url": base_url, "type": "default"})
    return {
        "config": {"wide_screen_mode": True},
        "header": {"template": "blue", "title": {"tag": "plain_text", "content": f"🟢 [KOL·P3] {title}"}},
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md", "content": note}},
            {"tag": "div", "fields": [
                {"is_short": True, "text": {"tag": "lark_md", "content": f"**对象**\n{name}"}},
                {"is_short": True, "text": {"tag": "lark_md", "content": f"**平台**\n{platform}"}},
                {"is_short": True, "text": {"tag": "lark_md", "content": f"**国家 / 语言**\n{country} / {language}"}},
                {"is_short": True, "text": {"tag": "lark_md", "content": f"**粉丝 / 内容**\n{followers} / {content}"}},
                {"is_short": False, "text": {"tag": "lark_md", "content": "**任务**\nP0-3A 功能测试；正式卡只要求取得商务邮箱，不展开商务谈判。"}},
            ]},
            {"tag": "action", "actions": profile_button},
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md", "content": "**英文首触达建议**\nHi [Name], we’re reaching out from [Brand] about a possible collaboration for [Product]. What’s the best business email for us to send the full brief and timeline?\n\n**中文意思**\n你好，我们想就 [产品] 的合作与你联系。请问哪个商务邮箱方便接收完整方案和时间安排？"}},
            {"tag": "form", "name": "email_form", "elements": [
                {"tag": "input", "name": "contact_email", "placeholder": {"tag": "plain_text", "content": "取得邮箱后填写；测试卡可留空验证拦截"}},
                {"tag": "button", "text": {"tag": "plain_text", "content": "已取得邮箱"}, "type": "primary", "action_type": "form_submit", "value": _action_value(record_id, "email", test_mode=test_mode)},
            ]},
            {"tag": "action", "actions": [
                {"tag": "button", "text": {"tag": "plain_text", "content": "对方要求平台继续沟通"}, "type": "default", "value": _action_value(record_id, "platform", test_mode=test_mode)},
                {"tag": "button", "text": {"tag": "plain_text", "content": "拒绝或不相关"}, "type": "danger", "value": _action_value(record_id, "reject", test_mode=test_mode)},
                {"tag": "button", "text": {"tag": "plain_text", "content": "暂无回复"}, "type": "default", "value": _action_value(record_id, "no_reply", test_mode=test_mode)},
            ]},
        ],
    }


def _form_value(event: dict, name: str) -> str:
    action = event.get("action") or {}
    candidates = [action.get("form_value"), event.get("card_form_value"), action.get("input_values")]
    for candidate in candidates:
        if isinstance(candidate, dict):
            value = candidate.get(name)
            if isinstance(value, dict):
                value = value.get("value") or value.get("text")
            if value is not None:
                return _text(value)
    return ""


def _resolved_card(label: str, detail: str, *, test_mode: bool) -> dict:
    boundary = "测试卡未写主表、未发邮件或私信。" if test_mode else "结果已回填原 KOL 记录。"
    return {
        "config": {"wide_screen_mode": True},
        "header": {"template": "grey", "title": {"tag": "plain_text", "content": f"✅ [KOL·P3] 已处理 · {label}"}},
        "elements": [{"tag": "div", "text": {"tag": "lark_md", "content": f"**结果**：{detail}\n\n{boundary}\n\n此卡已结束，无需重复点击。"}}],
    }


async def send_frankie_test_card(record_id: str) -> dict:
    record = await feishu.get_record(config.T_KOL, record_id)
    union_id = await feishu.open_id_to_union_id(config.KOL_FRANKIE_OPEN_ID)
    if not union_id:
        raise RuntimeError("无法在聪哥3号命名空间解析 Frankie union_id")
    card = build_card(record, test_mode=True)
    message_id = await feishu.send_card_via_app3("union_id", union_id, card)
    if not message_id:
        raise RuntimeError("飞书未返回测试卡 message_id")
    return {"sent": True, "record_id": record_id, "message_id": message_id, "test_mode": True}


async def handle_callback(event: dict) -> dict:
    action = event.get("action") or {}
    value = action.get("value") or {}
    action_name = _text(value.get("action"))
    if not action_name.startswith(ACTION_PREFIX):
        return {"toast": {"type": "error", "content": "未知的 KOL 联系卡动作"}}
    record_id = _text(value.get("record_id"))
    message_id = _text(event.get("open_message_id") or event.get("message_id") or (event.get("context") or {}).get("open_message_id"))
    test_mode = bool(value.get("test_mode"))
    receipt_key = f"{value.get('test_batch') or 'live'}:{record_id}"
    prior = _TEST_RECEIPTS.get(receipt_key) if test_mode else None
    if prior:
        return {"toast": {"type": "info", "content": f"该测试卡已处理：{prior['label']}，无需重复点击"}, "idempotent": True}

    suffix = action_name[len(ACTION_PREFIX):]
    labels = {"email": "已取得邮箱", "platform": "平台继续沟通", "reject": "拒绝或不相关", "no_reply": "暂无回复"}
    if suffix not in labels or not record_id:
        return {"toast": {"type": "error", "content": "卡片缺少记录或动作信息"}}
    email = _form_value(event, "contact_email")
    if suffix == "email" and not _EMAIL_RE.fullmatch(email):
        return {"toast": {"type": "warning", "content": "请填写有效商务邮箱后再提交"}}

    label = labels[suffix]
    if test_mode:
        _TEST_RECEIPTS[receipt_key] = {"label": label, "at": int(time.time()), "email_shape_ok": bool(email)}
    else:
        fields = {"联系状态": {"email": "邮件可用", "platform": "平台持续沟通", "reject": "明确拒绝", "no_reply": "待取得邮箱"}[suffix]}
        if suffix == "email":
            fields.update({"邮箱": email.lower(), "邮箱验真状态": "未验", "触达路由状态": "可新开发"})
        await feishu.update_record(config.T_KOL, record_id, fields)
        readback = await feishu.get_record(config.T_KOL, record_id)
        if _text((readback.get("fields") or {}).get("联系状态")) != fields["联系状态"]:
            raise RuntimeError("KOL 联系状态回读不一致")

    if message_id:
        await feishu.update_card_message_with_app(message_id, _resolved_card(label, label, test_mode=test_mode), which="app3")
    return {"toast": {"type": "success", "content": f"已记录：{label}"}, "ok": True, "test_mode": test_mode}

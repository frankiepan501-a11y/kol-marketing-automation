"""客服助手 v0 — 派单卡片(观察期)

扫工单台「状态=待派 且 卡片消息ID 空」→ 用客服助手 App 发卡(渠道/品牌/产品/平台/建议运营/
客诉摘要 + AI草稿全文 + 输入框 + 按钮) → 回标 卡片消息ID + 状态=待回。

观察期 CS_DISPATCH_OBSERVE=1(默认): 全部卡片发 Frankie 一人(校准路由/草稿质量), 卡片按钮回调
当前只 ack 不真回客户(安全)。观察稳定后 =0 → 按「分配运营」路由到对应运营(需 open_id→union)。
凭据走 env(public 仓铁律)。
"""
import json
import os
import time

import httpx

from . import feishu

CS_APP = os.environ.get("CS_TICKET_APP_TOKEN", "J2fibLgBZaLGTNsQOPHcQXLonZe")
T_TICKET = os.environ.get("CS_TICKET_TABLE_ID", "tblAhXMA9uDbGEMS")
CS_ASSIST_ID = os.environ.get("FEISHU_CS_ASSISTANT_APP_ID", "cli_aab6bdb724e1dcdb")
CS_ASSIST_SECRET = os.environ.get("FEISHU_CS_ASSISTANT_APP_SECRET", "")
OBSERVE = (os.environ.get("CS_DISPATCH_OBSERVE", "1") or "1") != "0"
OBSERVE_UNION = os.environ.get("CS_DISPATCH_OBSERVE_UNION",
                               "on_6e85dd60606f76f2d5af892785ac1dfe")  # Frankie union_id
# 一键回客户闭环是否已上线(默认否 → 卡片提示同事先在原渠道回; 闭环建好+DRY-RUN验证后置 1)
CS_REPLY_LIVE = (os.environ.get("CS_REPLY_LIVE", "0") or "0") != "0"
# 销售平台→运营 的 open_id(聪哥1号 namespace); 兜底/待定 → 降级 Frankie
OP_OPENID = {
    "黄奕纯": "ou_1b981067ce8edfd82af7c70c109310e4",
    "陈翔宇": "ou_9c322382284a7a6672a091b9f4c0a551",
    "林明坚": "ou_35aa6883c0598bac5c7e06fcb06f7c4d",
    "张佳烨": "ou_d850dab47bdbaea6736709d354de4b0f",
    "梁俊辉": "ou_b9dd2272e72908fe68964d7bba53109f",
}
_union_cache = {}

_tok = {"v": "", "exp": 0.0}


async def _resolve_union(operator: str) -> str:
    """运营姓名 → union_id(经聪哥1号 open_id→union, 跨app通用)。兜底/待定/查不到 → 返回''(调用方降级 Frankie)。"""
    oid = OP_OPENID.get((operator or "").strip())
    if not oid:
        return ""
    if oid in _union_cache:
        return _union_cache[oid]
    try:
        d = await feishu.api("GET", f"/contact/v3/users/{oid}?user_id_type=open_id", which="notify")
        u = ((d.get("data", {}) or {}).get("user", {}) or {}).get("union_id", "")
        if u:
            _union_cache[oid] = u
        return u
    except Exception:
        return ""


async def _token() -> str:
    if _tok["v"] and _tok["exp"] > time.time():
        return _tok["v"]
    async with httpx.AsyncClient(timeout=30.0) as c:
        r = await c.post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
                         json={"app_id": CS_ASSIST_ID, "app_secret": CS_ASSIST_SECRET})
        d = r.json()
    _tok["v"] = d.get("tenant_access_token", "")
    _tok["exp"] = time.time() + (int(d.get("expire", 3600)) - 300)
    return _tok["v"]


async def _send_card(union_id: str, card: dict) -> str:
    tok = await _token()
    async with httpx.AsyncClient(timeout=30.0) as c:
        r = await c.post("https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=union_id",
                         headers={"Authorization": f"Bearer {tok}"},
                         json={"receive_id": union_id, "msg_type": "interactive",
                               "content": json.dumps(card, ensure_ascii=False)})
        d = r.json()
    return d.get("data", {}).get("message_id", "") if d.get("code") == 0 else ""


def _x(f: dict, key: str) -> str:
    v = f.get(key)
    if isinstance(v, list) and v:
        return v[0].get("text", "") if isinstance(v[0], dict) else str(v[0])
    return v if isinstance(v, str) else ("" if v is None else str(v))


def _build_card(rid: str, f: dict) -> dict:
    brand = _x(f, "品牌"); product = _x(f, "产品") or "未识别"; platform = _x(f, "销售平台")
    channel = _x(f, "渠道"); customer = _x(f, "客户标识"); order = _x(f, "订单号")
    summary = _x(f, "客诉摘要"); operator = _x(f, "分配运营") or "未定"
    conf = _x(f, "AI置信度"); ctype = _x(f, "客诉类型")
    draft = (_x(f, "AI草稿") or "(无 AI 草稿)")[:2000]
    info = (f"**渠道:** {channel}  ·  **品牌:** {brand}  ·  **平台:** {platform}\n"
            f"**客户:** {customer}" + (f"  ·  **订单:** {order}" if order else "") + "\n"
            f"**类型:** {ctype or '-'}  ·  **置信度:** {conf}  ·  **建议派给:** {operator}\n"
            f"**客诉:** {summary}")
    elements = [
        {"tag": "div", "text": {"tag": "lark_md", "content": info}},
        {"tag": "hr"},
        {"tag": "div", "text": {"tag": "lark_md",
                                "content": "**🤖 AI 建议回复全文**（可复制；满意直接发）：\n" + draft}},
        {"tag": "hr"},
        {"tag": "form", "name": f"r_{rid}", "elements": [
            {"tag": "input", "name": "custom_reply", "width": "fill", "label_position": "top",
             "label": {"tag": "plain_text", "content": "✍️ 如需修改：输入最终回复（留空=用上方草稿）"},
             "placeholder": {"tag": "plain_text", "content": "留空则用上方 AI 草稿；要改就在此输入"}},
            {"tag": "button", "action_type": "form_submit", "name": "send", "type": "primary",
             "text": {"tag": "plain_text", "content": "✅ 发送回复"},
             "value": {"act": "send_reply", "rid": rid}},
        ]},
        {"tag": "action", "actions": [
            {"tag": "button", "text": {"tag": "plain_text", "content": "🔁 改派"},
             "value": {"act": "reassign", "rid": rid}},
            {"tag": "button", "text": {"tag": "plain_text", "content": "⬆️ 升级"},
             "value": {"act": "escalate", "rid": rid}},
        ]},
    ]
    if OBSERVE:
        elements.append({"tag": "note", "elements": [{"tag": "plain_text",
                        "content": "🔎 观察期：全部卡片暂发你一人；点按钮暂不真回客户，仅供你校准路由/草稿质量"}]})
    elif not CS_REPLY_LIVE:
        elements.append({"tag": "note", "elements": [{"tag": "plain_text",
                        "content": "💬 一键回客户闭环灰度中：请先复制上方 AI 草稿，在原渠道(邮箱/Discord)回复客户；闭环验证完即开"}]})
    return {"config": {"wide_screen_mode": True},
            "header": {"template": "orange",
                       "title": {"tag": "plain_text", "content": f"🟠 [客服·待回] {brand} · {product} · {platform}"}},
            "elements": elements}


async def run(limit: int = 10) -> dict:
    if not CS_ASSIST_SECRET:
        return {"error": "FEISHU_CS_ASSISTANT_APP_SECRET 未配"}
    body = {"filter": {"conjunction": "and", "conditions": [
        {"field_name": "状态", "operator": "is", "value": ["待派"]}]},
        "page_size": min(int(limit) * 3, 200)}
    d = await feishu.api("POST", f"/bitable/v1/apps/{CS_APP}/tables/{T_TICKET}/records/search",
                         body, which="notify")
    items = d.get("data", {}).get("items", [])
    sent, samples = 0, []
    for it in items:
        if sent >= limit:
            break
        f = it.get("fields", {})
        rid = it.get("record_id")
        if _x(f, "卡片消息ID") or _x(f, "状态") != "待派":
            continue
        # 观察期统一发 Frankie; 生产期按「分配运营」路由(兜底/待定/查不到 → 降级 Frankie)
        union = OBSERVE_UNION if OBSERVE else (await _resolve_union(_x(f, "分配运营")) or OBSERVE_UNION)
        mid = await _send_card(union, _build_card(rid, f))
        if mid:
            await feishu.api("PUT", f"/bitable/v1/apps/{CS_APP}/tables/{T_TICKET}/records/{rid}",
                             {"fields": {"卡片消息ID": mid, "状态": "待回"}}, which="notify")
            sent += 1
            if len(samples) < 12:
                samples.append({"产品": _x(f, "产品"), "平台": _x(f, "销售平台"),
                                "建议运营": _x(f, "分配运营"), "摘要": _x(f, "客诉摘要")[:40]})
    return {"observe": OBSERVE, "candidates": len(items), "sent": sent, "samples": samples}


# ===== 卡片按钮回调处理 (card.action.trigger, 经 n8n 转发到 /cs/callback) =====
def _toast(content: str, typ: str = "success") -> dict:
    return {"toast": {"type": typ, "content": content}}


async def _notify_frankie(text: str):
    try:
        tok = await _token()
        async with httpx.AsyncClient(timeout=30.0) as c:
            await c.post("https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=union_id",
                         headers={"Authorization": f"Bearer {tok}"},
                         json={"receive_id": OBSERVE_UNION, "msg_type": "text",
                               "content": json.dumps({"text": text}, ensure_ascii=False)})
    except Exception:
        pass


async def handle_callback(event: dict) -> dict:
    """飞书卡片按钮回调: 发送回复 / 改派 / 升级。返回 toast 给操作人即时反馈。"""
    action = event.get("action", {}) or {}
    val = action.get("value", {}) or {}
    act = val.get("act")
    rid = val.get("rid")
    if not rid:
        return _toast("缺少工单ID", "error")
    try:
        rec = await feishu.api("GET", f"/bitable/v1/apps/{CS_APP}/tables/{T_TICKET}/records/{rid}",
                               which="notify")
        f = ((rec.get("data", {}) or {}).get("record", {}) or {}).get("fields", {}) or {}
    except Exception:
        f = {}
    tag = f"{_x(f, '品牌')}·{_x(f, '销售平台')}·{_x(f, '客户标识')}"

    if act == "escalate":
        await feishu.api("PUT", f"/bitable/v1/apps/{CS_APP}/tables/{T_TICKET}/records/{rid}",
                         {"fields": {"状态": "已升级"}}, which="notify")
        await _notify_frankie(f"⬆️ 工单升级\n{tag}\n{_x(f, '客诉摘要')}")
        return _toast("已升级给负责人 ✓")

    if act == "reassign":
        await _notify_frankie(f"🔁 改派请求（原派 {_x(f, '分配运营')}）\n{tag}\n{_x(f, '客诉摘要')}")
        return _toast("已通知负责人改派 ✓")

    if act == "send_reply":
        form = action.get("form_value", {}) or {}
        reply = (form.get("custom_reply") or "").strip() or _x(f, "AI草稿")
        if len((reply or "").strip()) < 10:
            return _toast("回复内容过短，请填写后再发", "error")
        # CS_REPLY_LIVE=1 时才真发客户(待建 Zoho/网易/Discord 发送 + DRY-RUN 验证)
        fields = {"最终回复": reply[:5000], "状态": "已回复"}
        await feishu.api("PUT", f"/bitable/v1/apps/{CS_APP}/tables/{T_TICKET}/records/{rid}",
                         fields, which="notify")
        if CS_REPLY_LIVE:
            return _toast("已发送给客户 ✓")
        return _toast("已记录回复 ✓ 发送闭环灰度中，请暂在原渠道发给客户")

    return _toast("未知操作", "error")

"""客服助手 v0 — 派单卡片(观察期)

扫工单台「状态=待派 且 卡片消息ID 空」→ 用客服助手 App 发卡(渠道/品牌/产品/平台/建议运营/
客诉摘要 + AI草稿全文 + 输入框 + 按钮) → 回标 卡片消息ID + 状态=待回。

观察期 CS_DISPATCH_OBSERVE=1(默认): 全部卡片发 Frankie 一人(校准路由/草稿质量), 卡片按钮回调
当前只 ack 不真回客户(安全)。观察稳定后 =0 → 按「分配运营」路由到对应运营(需 open_id→union)。
凭据走 env(public 仓铁律)。
"""
import asyncio
import json
import os
import re
import ssl
import smtplib
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr, formatdate, make_msgid, parseaddr

import httpx

from . import feishu
from . import cs_ingest as _csi  # 复用采集侧 Zoho/网易/Discord 凭据与 token

CS_APP = os.environ.get("CS_TICKET_APP_TOKEN", "J2fibLgBZaLGTNsQOPHcQXLonZe")
T_TICKET = os.environ.get("CS_TICKET_TABLE_ID", "tblAhXMA9uDbGEMS")
CS_ASSIST_ID = os.environ.get("FEISHU_CS_ASSISTANT_APP_ID", "cli_aab6bdb724e1dcdb")
CS_ASSIST_SECRET = os.environ.get("FEISHU_CS_ASSISTANT_APP_SECRET", "")
OBSERVE = (os.environ.get("CS_DISPATCH_OBSERVE", "1") or "1") != "0"
OBSERVE_UNION = os.environ.get("CS_DISPATCH_OBSERVE_UNION",
                               "on_6e85dd60606f76f2d5af892785ac1dfe")  # Frankie union_id
# 一键回客户闭环是否已上线(默认否 → 卡片提示同事先在原渠道回; 闭环建好+DRY-RUN验证后置 1)
CS_REPLY_LIVE = (os.environ.get("CS_REPLY_LIVE", "0") or "0") != "0"
# Scott Stein 铁律: 改/上线回客户代码先开此 env → 全部回复改发测试邮箱(真客户/频道不收), 验证 raw 完整再删
CS_REPLY_DRY_RUN_TO = (os.environ.get("CS_REPLY_DRY_RUN_TO", "") or "").strip()
# 发件身份 (Zoho send-as / 网易登录账号)
ZOHO_CS_FROM = os.environ.get("ZOHO_POWKONG_CS_FROM", "support@powkong.com")
NE_SMTP = os.environ.get("NETEASE_SMTP_HOST", "smtp.qiye.163.com")

# 占位符黑名单 — 发送前扫描, 命中即拦截(防把"待确认/[TBD]"发给客户)
_PLACEHOLDER_BLACKLIST = [
    "待确认", "待填", "占位", "tbd", "[carrier", "[tracking", "[address",
    "[price", "[eta", "[quantity", "[xxx", "<placeholder",
]
_PLACEHOLDER_RE = re.compile(r"[\[【][^\]】]{0,30}(待确认|待填|占位|tbd|placeholder)[^\]】]{0,30}[\]】]", re.I)
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

# 防重复发送(飞书卡片回调 >3s 会 timeout+自动重试 → 同一回复重发给客户):
# _inflight = 正在异步发送中的 rid(并发去重); _recent = 刚发完的 rid→ts(防飞书读后写延迟
# 导致状态读到旧"待派"再发一次, 单进程内存即时可见, 不受 bitable 同步延迟影响);
# 配合"工单状态已是终态"持久去重(跨进程/重启/超 _RECENT_TTL)。三层互补。
_inflight = set()
_recent = {}
_bg_tasks = set()
_RECENT_TTL = 300  # 秒: 刚发完 5 分钟内同 rid 再点 → 拦下


def _recent_seen(rid: str) -> bool:
    now = time.time()
    if len(_recent) > 500:  # 轻量清理
        for k in [k for k, ts in _recent.items() if now - ts > _RECENT_TTL]:
            _recent.pop(k, None)
    return _recent.get(rid, 0) > now - _RECENT_TTL


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
    if CS_REPLY_DRY_RUN_TO:
        note = "🧪 DRY-RUN 验证中：点「发送回复」会改发测试邮箱（真客户/频道不会收到），用于核对内容完整"
    elif CS_REPLY_LIVE:
        note = "✅ 点「发送回复」将直接回复到客户原渠道（邮箱串原 thread / Discord 回原频道）"
    elif OBSERVE:
        note = "🔎 观察期：全部卡片暂发你一人；点按钮暂不真回客户，仅供你校准路由/草稿质量"
    else:
        note = "💬 一键回客户闭环灰度中：请先复制上方 AI 草稿，在原渠道(邮箱/Discord)回复客户；闭环验证完即开"
    elements.append({"tag": "note", "elements": [{"tag": "plain_text", "content": note}]})
    return {"config": {"wide_screen_mode": True},
            "header": {"template": "orange",
                       "title": {"tag": "plain_text", "content": f"🟠 [客服·待回] {brand} · {product} · {platform}"}},
            "elements": elements}


async def run(limit: int = 10, rids: str = "") -> dict:
    if not CS_ASSIST_SECRET:
        return {"error": "FEISHU_CS_ASSISTANT_APP_SECRET 未配"}
    if rids:
        # 定向派单: 只派指定 rid(逐个 GET), 用于审计后精确放行(避开未审计渠道如 Discord 待派)
        items = []
        for rid in [x.strip() for x in rids.split(",") if x.strip()]:
            try:
                rec = await feishu.api("GET", f"/bitable/v1/apps/{CS_APP}/tables/{T_TICKET}/records/{rid}",
                                       which="notify")
                rf = ((rec.get("data", {}) or {}).get("record", {}) or {})
                if rf:
                    items.append({"record_id": rid, "fields": rf.get("fields", {})})
            except Exception:
                continue
    else:
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


# ===== 回客户真实渠道发送 (CSP=Powkong Zoho / CSF=Funlab 网易 SMTP / CSD·CSDT=Discord) =====
def _placeholder_hit(text: str) -> str:
    """回复正文含未替换占位符 → 返回命中片段(供拦截), 否则 ''。"""
    low = (text or "").lower()
    for kw in _PLACEHOLDER_BLACKLIST:
        if kw in low:
            return kw
    m = _PLACEHOLDER_RE.search(text or "")
    return m.group(0)[:30] if m else ""


def _to_html(body: str) -> str:
    """纯文本回复 → 简单 HTML (邮件用); 已含标签则原样返回。"""
    if not body:
        return ""
    if re.search(r"<(p|div|br|table|strong|a)\b", body, re.I):
        return body
    s = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", body)
    paras = [p.strip() for p in s.split("\n\n") if p.strip()]
    return "".join(f"<p>{p.replace(chr(10), '<br>')}</p>" for p in paras)


def _orig_subject(f: dict) -> str:
    """从「原文」(subj\\n\\nbody) 还原原邮件主题, 去掉已有 Re:/Fwd: 前缀。"""
    raw = _x(f, "原文")
    subj = raw.split("\n\n", 1)[0].strip() if raw else ""
    subj = re.sub(r"^\s*(re|fwd|fw)\s*:\s*", "", subj, flags=re.I).strip()
    return subj or "your message"


async def _zoho_send(to_addr: str, subject: str, html: str, reply_to_msgid: str = "") -> str:
    """Powkong 客服 Zoho 发信; 带 reply_to_msgid 走 action:reply 串原 thread, 失败降级新邮件。"""
    tok = await _csi._ztoken()
    base = f"https://mail.zoho.com/api/accounts/{_csi.ZACC}/messages"
    url, payload = base, {"fromAddress": ZOHO_CS_FROM, "toAddress": to_addr,
                          "subject": subject, "content": html, "mailFormat": "html"}
    if reply_to_msgid:
        url = f"{base}/{reply_to_msgid}"
        payload["action"] = "reply"
    async with httpx.AsyncClient(timeout=45.0) as c:
        r = await c.post(url, json=payload, headers={"Authorization": f"Zoho-oauthtoken {tok}"})
        d = r.json()
    if (d.get("status", {}) or {}).get("code") != 200:
        if reply_to_msgid:  # reply 端点失败(原 msgId 失效) → 降级普通新邮件
            return await _zoho_send(to_addr, subject, html, "")
        raise Exception(f"Zoho 发送失败: {str(d)[:300]}")
    return (d.get("data", {}) or {}).get("messageId", "ok")


def _netease_send_sync(to_addr: str, subject: str, html: str, in_reply_to: str = "") -> str:
    msg = MIMEMultipart("alternative")
    msg["From"] = formataddr(("FUNLAB Support", _csi.NE_USER))
    msg["To"] = to_addr
    msg["Subject"] = subject
    msg["Date"] = formatdate(localtime=True)
    msg["Message-ID"] = make_msgid(domain="funlabswitch.com")
    if in_reply_to:
        msg["In-Reply-To"] = in_reply_to
        msg["References"] = in_reply_to
    plain = re.sub(r"<[^>]+>", "", html).replace("&nbsp;", " ").strip()
    msg.attach(MIMEText(plain or " ", "plain", "utf-8"))
    msg.attach(MIMEText(html, "html", "utf-8"))
    with smtplib.SMTP_SSL(NE_SMTP, 465, context=ssl.create_default_context(), timeout=30) as s:
        s.login(_csi.NE_USER, _csi.NE_CODE)
        s.sendmail(_csi.NE_USER, [to_addr], msg.as_string())
    return "ok"


async def _netease_send(to_addr: str, subject: str, html: str, in_reply_to: str = "") -> str:
    return await asyncio.to_thread(_netease_send_sync, to_addr, subject, html, in_reply_to)


async def _discord_send(channel_id: str, content: str, reply_to_msgid: str = "") -> str:
    payload = {"content": (content or "")[:1900]}
    if reply_to_msgid and reply_to_msgid.isdigit():
        payload["message_reference"] = {"message_id": reply_to_msgid, "fail_if_not_exists": False}
    async with httpx.AsyncClient(timeout=30.0) as c:
        r = await c.post(f"https://discord.com/api/v10/channels/{channel_id}/messages",
                         headers={"Authorization": f"Bot {_csi.DISCORD_BOT_TOKEN}",
                                  "User-Agent": "DiscordBot (cs,1.0)"}, json=payload)
    if r.status_code not in (200, 201):
        raise Exception(f"Discord 发送失败 {r.status_code}: {r.text[:200]}")
    return "ok"


def _route_label(prefix: str, channel: str, cust_email: str, thread: str) -> str:
    if prefix == "CSP":
        return f"Powkong邮箱:{cust_email}"
    if prefix == "CSF":
        return f"Funlab邮箱:{cust_email}"
    if prefix in ("CSDT", "CSD"):
        return f"Discord:{thread}"
    return f"{channel}:{cust_email or thread}"


async def _dispatch_reply(f: dict, reply: str) -> tuple:
    """把运营确认的回复真发到客户原渠道。返回 (ok, detail)。
    DRY-RUN(CS_REPLY_DRY_RUN_TO 有值): 所有渠道一律改发测试邮箱 + banner 标真实去向, 真客户/频道不收。"""
    ticket_id = _x(f, "工单ID")
    prefix = ticket_id.split("-", 1)[0] if ticket_id else ""
    channel = _x(f, "渠道")
    brand = _x(f, "品牌")
    thread = _x(f, "线程ID")
    customer = _x(f, "客户标识")
    cust_email = parseaddr(customer)[1] or customer
    subj = "Re: " + _orig_subject(f)
    html = _to_html(reply)

    if CS_REPLY_DRY_RUN_TO:
        target = _route_label(prefix, channel, cust_email, thread)
        banner = (f'<div style="background:#fff3cd;padding:8px;border:1px solid #ffc107;margin-bottom:12px">'
                  f'<strong>⚠️ CS DRY-RUN</strong> — 本应发往 <code>{target}</code>，真客户/频道不会收到。'
                  f'渠道={channel} / 品牌={brand} / 工单={ticket_id}</div>')
        await _zoho_send(CS_REPLY_DRY_RUN_TO, f"[CS-DRY-RUN→{target}] {subj}", banner + html, "")
        return True, f"DRY-RUN→{CS_REPLY_DRY_RUN_TO}（本应 {target}）"

    if prefix == "CSP":  # Powkong → Zoho reply(串原 thread)
        if "@" not in cust_email:
            return False, "无有效客户邮箱"
        await _zoho_send(cust_email, subj, html, thread)
        return True, f"Zoho→{cust_email}"
    if prefix == "CSF":  # Funlab → 网易 SMTP
        if "@" not in cust_email:
            return False, "无有效客户邮箱"
        await _netease_send(cust_email, subj, html, thread)
        return True, f"网易→{cust_email}"
    if prefix in ("CSDT", "CSD"):  # Discord
        if prefix == "CSDT" and thread.startswith("ticket-"):
            await _discord_send(thread[len("ticket-"):], reply, "")  # 工单频道: thread=ticket-{cid}
        else:
            await _discord_send(_csi.DC_SUPPORT_CHAN, reply, thread)  # 公开频道: 回 #support-center
        return True, f"Discord→{prefix}"
    # 兜底: 工单ID 前缀不明但渠道=邮箱 → 按品牌选发信通道
    if channel == "邮箱" and "@" in cust_email:
        if brand == "FUNLAB":
            await _netease_send(cust_email, subj, html, thread)
        else:
            await _zoho_send(cust_email, subj, html, thread)
        return True, f"邮箱(兜底brand={brand})→{cust_email}"
    return False, f"无法路由(prefix={prefix} 渠道={channel})"


# ===== 卡片按钮回调处理 (card.action.trigger, 经 n8n 转发到 /cs/callback) =====
def _toast(content: str, typ: str = "success") -> dict:
    return {"toast": {"type": typ, "content": content}}


def _operator_label(event: dict) -> str:
    op = event.get("operator", {}) or {}
    return (op.get("union_id") or op.get("open_id") or "运营自助")[:60]


async def _notify_union(union: str, text: str):
    try:
        tok = await _token()
        async with httpx.AsyncClient(timeout=30.0) as c:
            await c.post("https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=union_id",
                         headers={"Authorization": f"Bearer {tok}"},
                         json={"receive_id": union, "msg_type": "text",
                               "content": json.dumps({"text": text}, ensure_ascii=False)})
    except Exception:
        pass


async def _notify_frankie(text: str):
    await _notify_union(OBSERVE_UNION, text)


async def _send_async(rid: str, f: dict, reply: str, event: dict):
    """后台真发(不阻塞卡片回调, 防飞书 3s timeout)。成功回写工单台 + 保留 _recent 防重发;
    失败 → 清 _recent(放行重试) + IM 通知操作人+Frankie。"""
    op_union = ((event.get("operator", {}) or {}).get("union_id") or "")
    tag = f"{_x(f, '品牌')}·{_x(f, '销售平台')}·{_x(f, '客户标识')}"
    success = False
    try:
        ok, detail = await _dispatch_reply(f, reply)
        if not ok:
            for u in {op_union, OBSERVE_UNION} - {""}:
                await _notify_union(u, f"❌ 客服回复发送失败\n{tag}\n原因: {detail}\n请重试或在原渠道手动回复。")
            return
        success = True
        if CS_REPLY_DRY_RUN_TO:
            return  # dry-run: 不改真工单状态(但保留 _recent 防 5min 内重复测试)
        await feishu.api("PUT", f"/bitable/v1/apps/{CS_APP}/tables/{T_TICKET}/records/{rid}",
                         {"fields": {"最终回复": reply[:5000], "状态": "已回复",
                                     "回复时间": int(time.time() * 1000),
                                     "回复人": _operator_label(event)}}, which="notify")
    except Exception as e:
        for u in {op_union, OBSERVE_UNION} - {""}:
            await _notify_union(u, f"❌ 客服回复发送异常\n{tag}\n{str(e)[:160]}\n请重试或在原渠道手动回复。")
    finally:
        _inflight.discard(rid)
        if success:
            _recent[rid] = time.time()   # 成功 → 保持去重窗口
        else:
            _recent.pop(rid, None)       # 失败 → 清掉 claim 时的标记, 放行操作人重试


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
        reply = ((form.get("custom_reply") or "").strip() or _x(f, "AI草稿") or "").strip()
        if len(reply) < 10:
            return _toast("回复内容过短，请填写后再发", "error")
        # 发送前占位符校验拦截 (Scott Stein 铁律)
        ph = _placeholder_hit(reply)
        if ph:
            return _toast(f"回复含未替换占位符「{ph}」，请改完再发", "error")
        # 🚨 去重①(内存即时): 正在发送中 / 刚发完 5min 内(防 bitable 读后写延迟漏判) → 拦下
        if rid in _inflight or _recent_seen(rid):
            return _toast("该回复正在发送或刚已发送，请勿重复点击")
        # 🚨 去重②(持久): 工单已终态(已回复/已解决/已升级) → 跨进程/重启/超 5min 兜底
        if _x(f, "状态") in ("已回复", "已解决", "已升级"):
            return _toast("该工单已处理 ✓ 无需重复发送")

        # 闭环未开 且 未开 DRY-RUN → 旧行为: 只记录(快, 同步)
        if not CS_REPLY_LIVE and not CS_REPLY_DRY_RUN_TO:
            _recent[rid] = time.time()
            await feishu.api("PUT", f"/bitable/v1/apps/{CS_APP}/tables/{T_TICKET}/records/{rid}",
                             {"fields": {"最终回复": reply[:5000], "状态": "已回复"}}, which="notify")
            return _toast("已记录回复 ✓ 发送闭环灰度中，请暂在原渠道发给客户")

        # DRY-RUN 或 LIVE → 异步真发(立即返回 toast, 防飞书卡片回调 >3s timeout+重试导致重复发送)
        _inflight.add(rid)
        _recent[rid] = time.time()  # 立即标记, 防 bitable 读后写延迟下的重复
        t = asyncio.create_task(_send_async(rid, f, reply, event))
        _bg_tasks.add(t)
        t.add_done_callback(_bg_tasks.discard)
        if CS_REPLY_DRY_RUN_TO:
            return _toast("🧪 DRY-RUN 已提交（发测试邮箱，工单状态不变）")
        return _toast("✅ 已提交，正在发送给客户…稍候在工单台看「已回复」")

    return _toast("未知操作", "error")

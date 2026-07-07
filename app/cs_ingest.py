"""客服助手 v0 — 邮箱采集 → AI 分类/路由 → 写客服工单台

源: ① Powkong support@powkong.com (Zoho API)  ② Funlab support@funlabswitch.com (网易企业邮箱 IMAP)
只采集+分类+写工单台(只读观察), 不发卡、不回客户。所有凭据走 env(public 仓铁律)。
设计稿: memory `cs-channel-apiization-2026-06-24`。

分类/路由规则 v1 (Frankie 2026-06-25 封板):
- 真实客户: 亚马逊单(订单号 3-7-7) → 站点待领星反查(不自动派站点, 防误派); 美客多单 → 梁俊辉;
  其余非亚马逊客诉 → 独立站(张佳烨); 无订单号+身份不明(客户vs分销商) → 默认当客户走独立站.
- 非客户: 供应商/B2B/合作 → 标记推 B2B 群; 营销/SEO/平台通知/垃圾 → 忽略归档.
- 置信度: 操作咨询=AI直答 / 质量补发=AI起草人工审 / 投诉升级·退款=必须人工.
"""
import asyncio
import os
import re
import time
import httpx
from . import deepseek, feishu, cs_resources

# ---- 资源 (非 secret, 可 env 覆盖) ----
CS_APP_TOKEN = os.environ.get("CS_TICKET_APP_TOKEN", "J2fibLgBZaLGTNsQOPHcQXLonZe")
T_TICKET = os.environ.get("CS_TICKET_TABLE_ID", "tblAhXMA9uDbGEMS")
POWKONG_INBOX_FID = os.environ.get("ZOHO_POWKONG_CS_INBOX_FID", "7855434000000008014")
B2B_GROUP = os.environ.get("CS_B2B_GROUP_CHAT_ID", "oc_2e878553984592d7396401fdd6a37d61")

# ---- Zoho POWKONG_CS (env, secret) ----
ZCID = os.environ.get("ZOHO_POWKONG_CS_CLIENT_ID", "")
ZSEC = os.environ.get("ZOHO_POWKONG_CS_CLIENT_SECRET", "")
ZRT = os.environ.get("ZOHO_POWKONG_CS_REFRESH_TOKEN", "")
ZACC = os.environ.get("ZOHO_POWKONG_CS_ACCOUNT_ID", "")
ZREGION = os.environ.get("ZOHO_REGION", ".com")

# ---- 网易 FUNLAB_CS (env, secret) ----
NE_USER = os.environ.get("NETEASE_FUNLAB_CS_USER", "")
NE_CODE = os.environ.get("NETEASE_FUNLAB_CS_AUTHCODE", "")
NE_IMAP = os.environ.get("NETEASE_IMAP_HOST", "imap.qiye.163.com")

# ---- Discord FUN Bot (token=env secret; 频道 id 非 secret 给默认值) ----
# v0 只接 FUNLAB 公开 #support-center (FUN Bot 可读)。私有工单(MEE6)待官号 2FA 授权后补。
# Zeabur 东京可直连 Discord API, 无需代理。
DISCORD_BOT_TOKEN = os.environ.get("DISCORD_BOT_TOKEN", "")
DC_SUPPORT_CHAN = os.environ.get("DISCORD_FUNLAB_SUPPORT_CHANNEL_ID", "1012184626640470089")
DC_GUILD = os.environ.get("DISCORD_FUNLAB_GUILD_ID", "1009762946437619742")

PLATFORM_OPTS = ["亚马逊-美国", "亚马逊-墨西哥", "亚马逊-加拿大", "亚马逊-日本",
                 "亚马逊-英国", "亚马逊-欧洲", "独立站", "美客多", "沃尔玛", "TikTok", "未知"]
TYPE_OPTS = ["物流", "产品", "退换货", "售后", "投诉升级"]
LANG_OPTS = ["EN", "中文", "德", "法", "西", "葡", "日", "其他"]
CONF_OPTS = ["AI直答", "AI起草人工审", "必须人工"]
AMZ_ORDER_RE = re.compile(r"\d{3}-\d{7}-\d{7}")

# ---- 领星反查 (亚马逊订单号 → sid → 店铺 country → 运营) ----
LX_PROXY_URL = os.environ.get("LINGXING_PROXY_URL", "")
LX_PROXY_TOKEN = os.environ.get("LINGXING_PROXY_TOKEN", "")
# country → (销售平台选项, 运营). 巴西/澳洲等未映射 → 兜底待人工。
COUNTRY_MAP = {
    "美国": ("亚马逊-美国", "黄奕纯"),
    "加拿大": ("亚马逊-加拿大", "陈翔宇"),
    "墨西哥": ("亚马逊-墨西哥", "陈翔宇"),
    "日本": ("亚马逊-日本", "陈翔宇"),
    "英国": ("亚马逊-英国", "林明坚"),
    "德国": ("亚马逊-欧洲", "林明坚"), "法国": ("亚马逊-欧洲", "林明坚"),
    "西班牙": ("亚马逊-欧洲", "林明坚"), "意大利": ("亚马逊-欧洲", "林明坚"),
    "荷兰": ("亚马逊-欧洲", "林明坚"), "比利时": ("亚马逊-欧洲", "林明坚"),
    "波兰": ("亚马逊-欧洲", "林明坚"), "瑞典": ("亚马逊-欧洲", "林明坚"),
    "爱尔兰": ("亚马逊-欧洲", "林明坚"), "土耳其": ("亚马逊-欧洲", "林明坚"),
}
_seller_cache = {"map": {}, "ts": 0.0}
_SELLER_TTL = 3600


def _strip_html(s: str) -> str:
    s = re.sub(r"<[^>]+>", " ", s or "")
    return re.sub(r"\s+", " ", s.replace("&nbsp;", " ")).strip()


# ===== 领星反查 (亚马逊订单号 → 站点/运营) =====
async def _lx_proxy(method: str, path: str, params: dict) -> dict:
    async with httpx.AsyncClient(timeout=40.0) as c:
        r = await c.post(LX_PROXY_URL,
                         headers={"Authorization": f"Bearer {LX_PROXY_TOKEN}",
                                  "Content-Type": "application/json"},
                         json={"method": method, "path": path, "params": params})
        r.raise_for_status()
        return r.json()


async def _get_sid_country() -> dict:
    if _seller_cache["map"] and (time.time() - _seller_cache["ts"] < _SELLER_TTL):
        return _seller_cache["map"]
    rows = (await _lx_proxy("GET", "/erp/sc/data/seller/lists", {})).get("data") or []
    m = {str(x.get("sid")): x.get("country") for x in rows if x.get("sid")}
    if m:
        _seller_cache["map"], _seller_cache["ts"] = m, time.time()
    return m


async def _lookup_amazon_route(order_id: str):
    """亚马逊订单号 → (销售平台, 运营)。查不到/未映射(巴西/澳洲等) → (None, None)。"""
    if not (order_id and LX_PROXY_URL and LX_PROXY_TOKEN):
        return None, None
    try:
        data = (await _lx_proxy("POST", "/erp/sc/data/mws/orderDetail",
                                {"order_id": order_id})).get("data") or []
        row = data[0] if isinstance(data, list) and data else (data if isinstance(data, dict) else None)
        if not row:
            return None, None
        country = (await _get_sid_country()).get(str(row.get("sid") or ""))
        return COUNTRY_MAP.get(country, (None, None))
    except Exception:
        return None, None


# ===== 源 ① Powkong (Zoho) =====
async def _ztoken() -> str:
    async with httpx.AsyncClient(timeout=30.0) as c:
        r = await c.post(f"https://accounts.zoho{ZREGION}/oauth/v2/token",
                         data={"refresh_token": ZRT, "client_id": ZCID,
                               "client_secret": ZSEC, "grant_type": "refresh_token"})
        r.raise_for_status()
        return r.json()["access_token"]


async def _zget(url: str, tok: str) -> dict:
    async with httpx.AsyncClient(timeout=40.0) as c:
        r = await c.get(url, headers={"Authorization": f"Zoho-oauthtoken {tok}"})
        r.raise_for_status()
        return r.json()


async def _fetch_powkong(limit: int) -> list:
    if not (ZCID and ZRT and ZACC):
        return []
    tok = await _ztoken()
    listing = await _zget(
        f"https://mail.zoho.com/api/accounts/{ZACC}/messages/view"
        f"?folderId={POWKONG_INBOX_FID}&limit={limit}&start=0", tok)
    out = []
    for m in (listing.get("data") or []):
        mid = m.get("messageId")
        if not mid:
            continue
        try:
            content = await _zget(
                f"https://mail.zoho.com/api/accounts/{ZACC}/folders/{POWKONG_INBOX_FID}"
                f"/messages/{mid}/content", tok)
            d = content.get("data")
            body = _strip_html(d.get("content", "") if isinstance(d, dict) else "")
        except Exception:
            body = ""
        out.append({"id": mid, "id_prefix": "CSP", "frm": m.get("fromAddress", ""),
                    "subj": m.get("subject", ""), "received_ms": int(m.get("receivedTime") or 0),
                    "body": body, "channel": "邮箱", "brand_default": "POWKONG"})
    return out


# ===== 源 ② Funlab (网易 IMAP, 同步, 跑在线程里) =====
def _extract_body(msg) -> str:
    try:
        if msg.is_multipart():
            plain, html = "", ""
            for part in msg.walk():
                ct = part.get_content_type()
                if part.get("Content-Disposition", "").startswith("attachment"):
                    continue
                try:
                    payload = part.get_payload(decode=True)
                    if not payload:
                        continue
                    txt = payload.decode(part.get_content_charset() or "utf-8", "replace")
                except Exception:
                    continue
                if ct == "text/plain" and not plain:
                    plain = txt
                elif ct == "text/html" and not html:
                    html = txt
            return (plain or _strip_html(html))
        payload = msg.get_payload(decode=True)
        txt = payload.decode(msg.get_content_charset() or "utf-8", "replace") if payload else ""
        return txt if msg.get_content_type() == "text/plain" else _strip_html(txt)
    except Exception:
        return ""


def _fetch_funlab_sync(limit: int) -> list:
    import imaplib, ssl, email
    from email.header import decode_header, make_header
    from email.utils import parsedate_to_datetime, parseaddr
    if not (NE_USER and NE_CODE):
        return []
    out = []
    conn = imaplib.IMAP4_SSL(NE_IMAP, 993, ssl_context=ssl.create_default_context(), timeout=30)
    try:
        conn.login(NE_USER, NE_CODE)
        # 网易必须发 ID 命令, 否则 SELECT 报 Unsafe Login
        imaplib.Commands["ID"] = ("AUTH", "SELECTED")
        conn._simple_command(
            "ID", '("name" "funlab-cs" "version" "1.0" "vendor" "python" "contact" "%s")' % NE_USER)
        conn.select("INBOX", readonly=True)
        typ, data = conn.search(None, "ALL")
        ids = data[0].split()
        for mid in ids[-limit:][::-1]:
            typ, d = conn.fetch(mid, "(BODY.PEEK[])")
            if not d or not d[0]:
                continue
            msg = email.message_from_bytes(d[0][1])
            try:
                subj = str(make_header(decode_header(msg.get("Subject", ""))))
            except Exception:
                subj = msg.get("Subject", "")
            frm = parseaddr(msg.get("From", ""))[1] or msg.get("From", "")
            msgid = (msg.get("Message-ID", "") or "").strip() or f"netease-{mid.decode()}"
            try:
                received_ms = int(parsedate_to_datetime(msg.get("Date")).timestamp() * 1000)
            except Exception:
                received_ms = 0
            out.append({"id": msgid, "id_prefix": "CSF", "frm": frm, "subj": subj,
                        "received_ms": received_ms, "body": _extract_body(msg)[:8000],
                        "channel": "邮箱", "brand_default": "FUNLAB"})
    finally:
        try:
            conn.logout()
        except Exception:
            pass
    return out


async def _fetch_funlab(limit: int) -> list:
    return await asyncio.to_thread(_fetch_funlab_sync, limit)


# ===== 源 ③ Discord (FUN Bot REST: 公开 #support-center + 私有工单 MEE6) =====
def _dc_hdr():
    return {"Authorization": f"Bot {DISCORD_BOT_TOKEN}", "User-Agent": "DiscordBot (cs,1.0)"}


def _dc_ts(m: dict) -> int:
    from datetime import datetime
    try:
        return int(datetime.fromisoformat(
            (m.get("timestamp", "") or "").replace("Z", "+00:00")).timestamp() * 1000)
    except Exception:
        return 0


def _dc_name(au: dict) -> str:
    return au.get("global_name") or au.get("username") or str(au.get("id", ""))


async def _fetch_discord(limit: int) -> list:
    """降噪聚合: 工单频道→1工单1条(合并客户全部消息); 公开频道→合并连续同人消息+跳碎片。"""
    if not DISCORD_BOT_TOKEN:
        return []
    out, per = [], min(int(limit), 50)
    async with httpx.AsyncClient(timeout=30.0) as c:
        targets = []  # (channel_id, ticket_name)
        if DC_SUPPORT_CHAN:
            targets.append((DC_SUPPORT_CHAN, ""))
        if DC_GUILD:  # 枚举 🔧SUPPORT 分类下的 MEE6 工单频道(#N-name)
            try:
                gr = await c.get(f"https://discord.com/api/v10/guilds/{DC_GUILD}/channels", headers=_dc_hdr())
                if gr.status_code == 200:
                    chans = gr.json()
                    cats = {x["id"] for x in chans if x.get("type") == 4
                            and ("SUPPORT" in (x.get("name", "").upper()) or "🔧" in x.get("name", ""))}
                    for x in chans:
                        if (x.get("type") == 0 and x.get("parent_id") in cats
                                and re.match(r"^\d+-", x.get("name", ""))):
                            targets.append((x["id"], x.get("name", "")))
            except Exception:
                pass
        for cid, tname in targets:
            try:
                r = await c.get(f"https://discord.com/api/v10/channels/{cid}/messages?limit={per}", headers=_dc_hdr())
                if r.status_code != 200:
                    continue
                msgs = r.json()
            except Exception:
                continue
            hm = [m for m in (msgs if isinstance(msgs, list) else [])
                  if not (m.get("author") or {}).get("bot") and (m.get("content") or "").strip()]
            hm.reverse()  # API 返回最新在前 → 转成时间正序
            if not hm:
                continue
            if tname:
                # 工单频道: 整票客户消息聚合成 1 条工单
                body = "\n".join((m.get("content") or "").strip() for m in hm)[:6000]
                au = hm[0].get("author") or {}
                out.append({"id": f"ticket-{cid}", "id_prefix": "CSDT",
                            "frm": f"{_dc_name(au)} (Discord·工单{tname})", "subj": "",
                            "received_ms": _dc_ts(hm[-1]), "body": body,
                            "channel": "Discord", "brand_default": "FUNLAB"})
            else:
                # 公开频道: 合并连续同一作者的消息为 1 条, 跳过纯碎片
                groups = []
                for m in hm:
                    aid = (m.get("author") or {}).get("id")
                    if groups and groups[-1][0] == aid:
                        groups[-1][1].append(m)
                    else:
                        groups.append([aid, [m]])
                for _aid, grp in groups:
                    body = "\n".join((g.get("content") or "").strip() for g in grp)[:6000]
                    if len(body) < 12 and "?" not in body:   # 纯寒暄/碎片("ok"/"thanks")跳过
                        continue
                    au = grp[0].get("author") or {}
                    out.append({"id": grp[0].get("id"), "id_prefix": "CSD",
                                "frm": f"{_dc_name(au)} (Discord)", "subj": "",
                                "received_ms": _dc_ts(grp[-1]), "body": body,
                                "channel": "Discord", "brand_default": "FUNLAB"})
    return out


# ===== 去重 =====
async def _existing_thread_ids() -> set:
    ids, page = set(), ""
    while True:
        path = (f"/bitable/v1/apps/{CS_APP_TOKEN}/tables/{T_TICKET}"
                f"/records?page_size=200" + (f"&page_token={page}" if page else ""))
        d = await feishu.api("GET", path, which="notify")
        data = d.get("data", {})
        for it in data.get("items", []):
            v = it.get("fields", {}).get("线程ID")
            if isinstance(v, list) and v:
                v = v[0].get("text") if isinstance(v[0], dict) else v[0]
            if v:
                ids.add(str(v).strip())
        if data.get("has_more"):
            page = data.get("page_token", "")
        else:
            break
    return ids


# ===== 分类 =====
CLASSIFY_PROMPT = """你是跨境电商(游戏配件 POWKONG/FUNLAB)客服分诊AI。判断这封邮件输出JSON。
规则:
1. 真实客户(客诉/咨询/售后)→is_cs=true:
   - 订单号是亚马逊格式(3位-7位-7位数字) → is_amazon=true(站点稍后由领星定);
   - 美客多订单 → platform=美客多;
   - 其余一切非亚马逊客诉(独立站如PK+数字, 或任何国家不在亚马逊运营范围) → platform=独立站(兜底);
   - 无订单号且分不清是客户还是分销商 → 默认当客户, platform=独立站.
2. 供应商/B2B/合作/分销 询盘 → is_cs=false, route=B2B群.
3. 营销推广/SEO外链/平台系统通知/纯垃圾 → is_cs=false, route=忽略.
4. 纯寒暄/致谢/确认收到/无实际问题或诉求的对话碎片(尤其 Discord 闲聊) → is_cs=false, route=忽略.
   Discord 降噪: 只有 "same issue" / "did anyone solve this" / "any update" / "thanks" 等跟帖碎片,
   且没有订单号、产品型号、个人故障细节、明确售后诉求时, 不要单独建客服工单; 只有能看出独立客户诉求时才 is_cs=true.

【公司售后政策 v1 (2026-06-26 Frankie 拍板) — draft_reply 必须严格遵守, 禁止"待确认/TBD/占位"】
- 质保期: 统一 12 个月(霍尔摇杆款可称 18 个月)。
- 缺陷处理: 默认免费补发/换新(不默认退款); 要客户提供故障视频/照片确认; 不要求寄回坏件;
  $50+ 手柄或可疑重复索赔 → 要求销毁视频(剪线/壳体写字); <$20 配件凭照片即补。
- 运费: 质保内缺陷 → 公司全担; 非缺陷(客户原因/不喜欢) → 客户担; Amazon 30 天内按平台规则。
- 退款 vs 补发: 默认补发; 退款只在 客户坚持退/补发后仍故障/缺货/Amazon窗口内要退/物流确认丢件不愿等。
【已发货+客户称未收到 或 要求退款 — 先查物流定性, draft 不直接承诺退款, 要引导查物流/分情况】
  ① 在途未超正常时效 → 不退, 告知最新物流+预计送达, 请客户再等;
  ② 物流停滞/明显超时/查无更新 → 判丢件 → 补发或退款(公司担);
  ③ 显示已投递/已签收但客户称没收到 → 不直接退, 引导查门口/邻居/快递柜+联系承运商查投递点; $50+或可疑重复→需核实防欺诈;
  ④ 地址错误/被退回 → 联系客户核对地址后重发。
draft_reply 用英文自然体现以上, 给客户清晰下一步(如缺陷:"within the 12-month warranty we'll ship a free replacement, no need to return the faulty unit, we cover shipping — please share a short video/photo of the issue"; 未收到:先说会核实物流/给下一步, 不承诺退款)。
【置信度 confidence — 收紧: 只有红线才"必须人工", 不要因"涉及退款/换货"就标必须人工】
- 必须人工 = 仅红线: 单笔退款>$150 / 法律威胁 / Amazon A-to-z 或差评要挟 / 疑似欺诈(重复索赔) / 政策完全未覆盖的全新情况;
- AI直答 = 操作类咨询(物流查询/确认地址/固件使用指导);
- AI起草人工审 = 质量补发/换货/未发货按树处理/质保内 且 金额≤$150 的常规客诉(运营审核草稿后自己发, 不升级)。
字段:
is_cs(bool), is_amazon(bool), route(B2B群/忽略/空),
brand(FUNLAB或POWKONG, 据产品判断, 不确定用给定的默认品牌),
platform(美客多/独立站/未知 三选一; 亚马逊单填未知),
complaint_type(物流/产品/退换货/售后/投诉升级, 非客服留空),
product, order_no, language(EN/中文/德/法/西/葡/日/其他),
summary(一句中文摘要), confidence(AI直答/AI起草人工审/必须人工),
draft_reply(给客户的英文回复草稿, 非客服留空), reason(一句中文).
只输出JSON。"""


async def _classify(msg: dict) -> dict:
    prompt = (CLASSIFY_PROMPT
              + f"\n\n[此邮箱默认品牌:{msg['brand_default']}]\n发件人:{msg['frm']}\n"
              + f"主题:{msg['subj']}\n正文:{msg['body'][:1800]}")
    return await deepseek.chat_json(prompt, max_tokens=900, temperature=0.2)


def _pick(v, opts, default=None):
    return v if v in opts else default


def _to_fields(msg: dict, c: dict, amz_override=None, resources: list | None = None) -> dict:
    order_no = (c.get("order_no") or "").strip()
    is_amazon = bool(c.get("is_amazon")) or bool(AMZ_ORDER_RE.search(order_no))
    is_cs = bool(c.get("is_cs"))
    summary = (c.get("summary") or "").strip()

    if not is_cs:
        route = c.get("route") or "忽略"
        platform, operator, status = "未知", "", "归档非客服"
        summary = f"[→{route}] {summary}" if route else summary
    else:
        status = "待派"
        if is_amazon:
            if amz_override and amz_override[0]:
                platform, operator = amz_override  # 领星反查命中真实站点
            else:
                platform, operator = "未知", "待定·领星反查站点"
                summary = f"[亚马逊单·待领星定站点] {summary}"
        elif c.get("platform") == "美客多":
            platform, operator = "美客多", "梁俊辉"
        else:
            platform, operator = "独立站", "张佳烨"

    brand = _pick(c.get("brand"), ["FUNLAB", "POWKONG"], msg["brand_default"])
    fields = {
        "工单ID": f"{msg['id_prefix']}-{msg['id']}"[:200],
        "入站时间": int(msg.get("received_ms") or 0),
        "渠道": msg["channel"],
        "品牌": brand,
        "销售平台": _pick(platform, PLATFORM_OPTS, "未知"),
        "产品": (c.get("product") or "")[:200],
        "客户标识": msg["frm"],
        "订单号": order_no,
        "客诉摘要": summary[:500],
        "原文": (msg["subj"] + "\n\n" + msg["body"])[:8000],
        "语种": _pick(c.get("language"), LANG_OPTS, "其他"),
        "AI置信度": _pick(c.get("confidence"), CONF_OPTS, "必须人工"),
        "AI草稿": (c.get("draft_reply") or "")[:5000],
        "分配运营": operator,
        "状态": status,
        "线程ID": msg["id"],
    }
    ct = _pick(c.get("complaint_type"), TYPE_OPTS, None)
    if ct:
        fields["客诉类型"] = ct
    ctx = cs_resources.resolve_for_ticket(fields, resources=resources)
    resource_reply = cs_resources.build_resource_reply(fields, ctx)
    if resource_reply:
        fields["AI草稿"] = resource_reply[:5000]
    if cs_resources.WRITEBACK_TICKET_FIELDS:
        fields.update(cs_resources.ticket_resource_fields(ctx))
    return fields


# ===== 主入口 =====
async def run(source: str = "all", limit: int = 20, dry_run: bool = False) -> dict:
    src_err = {}
    msgs = []
    if source in ("all", "powkong"):
        try:
            msgs += await _fetch_powkong(limit)
        except Exception as e:
            src_err["powkong"] = str(e)[:200]
    if source in ("all", "funlab"):
        try:
            msgs += await _fetch_funlab(limit)
        except Exception as e:
            src_err["funlab"] = str(e)[:200]
    if source in ("all", "discord"):
        try:
            msgs += await _fetch_discord(limit)
        except Exception as e:
            src_err["discord"] = str(e)[:200]

    try:
        resources = await cs_resources.active_resources()
    except Exception:
        resources = cs_resources.builtin_resources()
    existing = await _existing_thread_ids()
    new_cnt, skip_cnt, err_cnt = 0, 0, 0
    samples = []
    for m in msgs:
        if not m.get("id") or m["id"] in existing:
            skip_cnt += 1
            continue
        try:
            c = await _classify(m)
        except Exception:
            err_cnt += 1
            continue
        # 亚马逊客诉 → 领星反查真实站点 → 对应运营(订单号格式判不出站点)
        amz_override = None
        if c.get("is_cs"):
            mo = AMZ_ORDER_RE.search(c.get("order_no") or "")
            if mo:
                p, op = await _lookup_amazon_route(mo.group(0))
                if p:
                    amz_override = (p, op)
        fields = _to_fields(m, c, amz_override, resources=resources)
        if len(samples) < 14:
            samples.append({"渠道品牌": f"{fields['品牌']}", "from": m["frm"][:26],
                            "is_cs": c.get("is_cs"), "平台": fields["销售平台"],
                            "运营": fields["分配运营"], "状态": fields["状态"],
                            "摘要": fields["客诉摘要"][:48]})
        if not dry_run:
            await feishu.api(
                "POST", f"/bitable/v1/apps/{CS_APP_TOKEN}/tables/{T_TICKET}/records",
                {"fields": fields}, which="notify")
        new_cnt += 1

    return {"sources": source, "fetched": len(msgs), "new": new_cnt, "skipped": skip_cnt,
            "errors": err_cnt, "source_errors": src_err, "dry_run": dry_run, "samples": samples}

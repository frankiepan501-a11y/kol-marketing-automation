"""客服助手 v0 — Powkong 邮箱采集 → AI 分类/路由 → 写客服工单台

设计稿: memory `cs-channel-apiization-2026-06-24`。本模块只负责 **采集+分类+写工单台**(只读观察),
不发卡、不回客户(卡片派单是下一阶段)。所有凭据走 env(public 仓铁律)。

分类/路由规则 v1 (Frankie 2026-06-25 封板):
- 真实客户: 亚马逊单(订单号 3-7-7) → 站点待领星反查(暂不自动派站点, 防 paulcruz 误派);
  美客多单 → 梁俊辉; 其余非亚马逊客诉 → 独立站(张佳烨);
  无订单号+身份不明(客户 vs 分销商) → 默认当客户走独立站.
- 非客户: 供应商/B2B/合作 → 标记推 B2B 群; 营销/SEO/平台通知/垃圾 → 忽略归档.
- 置信度: 操作咨询=AI直答 / 质量补发=AI起草人工审 / 投诉升级·退款=必须人工.
"""
import os
import re
import httpx
from . import deepseek, feishu

# ---- 资源 (非 secret, 可 env 覆盖) ----
CS_APP_TOKEN = os.environ.get("CS_TICKET_APP_TOKEN", "J2fibLgBZaLGTNsQOPHcQXLonZe")
T_TICKET = os.environ.get("CS_TICKET_TABLE_ID", "tblAhXMA9uDbGEMS")
POWKONG_INBOX_FID = os.environ.get("ZOHO_POWKONG_CS_INBOX_FID", "7855434000000008014")
B2B_GROUP = os.environ.get("CS_B2B_GROUP_CHAT_ID", "oc_2e878553984592d7396401fdd6a37d61")

# ---- Zoho POWKONG_CS 凭据 (env, secret) ----
ZCID = os.environ.get("ZOHO_POWKONG_CS_CLIENT_ID", "")
ZSEC = os.environ.get("ZOHO_POWKONG_CS_CLIENT_SECRET", "")
ZRT = os.environ.get("ZOHO_POWKONG_CS_REFRESH_TOKEN", "")
ZACC = os.environ.get("ZOHO_POWKONG_CS_ACCOUNT_ID", "")
ZREGION = os.environ.get("ZOHO_REGION", ".com")

NON_AMZ_OPERATOR = {"独立站": "张佳烨", "美客多": "梁俊辉"}
PLATFORM_OPTS = ["亚马逊-美国", "亚马逊-墨西哥", "亚马逊-加拿大", "亚马逊-日本",
                 "亚马逊-英国", "亚马逊-欧洲", "独立站", "美客多", "沃尔玛", "TikTok", "未知"]
TYPE_OPTS = ["物流", "产品", "退换货", "售后", "投诉升级"]
LANG_OPTS = ["EN", "中文", "德", "法", "西", "葡", "日", "其他"]
CONF_OPTS = ["AI直答", "AI起草人工审", "必须人工"]

AMZ_ORDER_RE = re.compile(r"\b\d{3}-\d{7}-\d{7}\b")


# ===== Zoho (自包含, 不污染 KOL BRAND_CONFIG) =====
async def _ztoken() -> str:
    async with httpx.AsyncClient(timeout=30.0) as c:
        r = await c.post(
            f"https://accounts.zoho{ZREGION}/oauth/v2/token",
            data={"refresh_token": ZRT, "client_id": ZCID,
                  "client_secret": ZSEC, "grant_type": "refresh_token"},
        )
        r.raise_for_status()
        return r.json()["access_token"]


async def _zget(url: str, tok: str) -> dict:
    async with httpx.AsyncClient(timeout=40.0) as c:
        r = await c.get(url, headers={"Authorization": f"Zoho-oauthtoken {tok}"})
        r.raise_for_status()
        return r.json()


def _strip_html(s: str) -> str:
    s = re.sub(r"<[^>]+>", " ", s or "")
    return re.sub(r"\s+", " ", s.replace("&nbsp;", " ")).strip()


# ===== 去重: 已在工单台的 线程ID =====
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


# ===== 分类 (DeepSeek, 封板规则) =====
CLASSIFY_PROMPT = """你是跨境电商(游戏配件 POWKONG/FUNLAB)客服分诊AI, 处理 support@powkong.com 收件箱。判断这封邮件输出JSON。
规则:
1. 真实客户(客诉/咨询/售后)→is_cs=true:
   - 订单号是亚马逊格式(3位-7位-7位数字) → is_amazon=true(站点稍后由领星定, 你只标 is_amazon);
   - 美客多订单 → platform=美客多;
   - 其余一切非亚马逊客诉(独立站如PK+数字, 或任何国家不在亚马逊运营范围) → platform=独立站(兜底);
   - 无订单号且分不清是客户还是分销商 → 默认当客户, platform=独立站.
2. 供应商/B2B/合作/分销 询盘 → is_cs=false, route=B2B群.
3. 营销推广/SEO外链/平台系统通知/纯垃圾 → is_cs=false, route=忽略.
字段:
is_cs(bool), is_amazon(bool, 亚马逊订单格式才true), route(B2B群/忽略/空),
brand(FUNLAB或POWKONG,据产品判断,默认POWKONG), platform(美客多/独立站/未知 三选一; 亚马逊单填未知),
complaint_type(物流/产品/退换货/售后/投诉升级, 非客服留空),
product(产品名/型号,无则空), order_no(订单号,无则空), language(EN/中文/德/法/西/葡/日/其他),
summary(一句中文摘要), confidence(AI直答/AI起草人工审/必须人工: 操作咨询=AI直答,质量补发=AI起草人工审,投诉升级/退款=必须人工),
draft_reply(给客户的英文回复草稿,非客服留空), reason(为何is_cs真/假,一句中文).
只输出JSON。"""


async def _classify(frm: str, subj: str, body: str) -> dict:
    prompt = CLASSIFY_PROMPT + f"\n\n发件人:{frm}\n主题:{subj}\n正文:{body[:1800]}"
    return await deepseek.chat_json(prompt, max_tokens=900, temperature=0.2)


def _pick(v, opts, default=None):
    return v if v in opts else default


def _to_fields(m: dict, frm: str, subj: str, body: str, c: dict) -> dict:
    order_no = (c.get("order_no") or "").strip()
    is_amazon = bool(c.get("is_amazon")) or bool(AMZ_ORDER_RE.search(order_no))
    is_cs = bool(c.get("is_cs"))
    summary = (c.get("summary") or "").strip()

    if not is_cs:
        route = c.get("route") or "忽略"
        platform = "未知"
        operator = ""
        status = "归档非客服"
        summary = f"[→{route}] {summary}" if route else summary
    else:
        status = "待派"
        if is_amazon:
            # 亚马逊站点不能靠订单号格式判 → 待领星反查, 不自动派站点运营(防 paulcruz 误派)
            platform = "未知"
            operator = "待定·领星反查站点"
            summary = f"[亚马逊单·待领星定站点] {summary}"
        elif c.get("platform") == "美客多":
            platform, operator = "美客多", "梁俊辉"
        else:
            platform, operator = "独立站", "张佳烨"

    fields = {
        "工单ID": f"CSP-{m.get('messageId')}",
        "入站时间": int(m.get("receivedTime") or 0),
        "渠道": "邮箱",
        "品牌": _pick(c.get("brand"), ["FUNLAB", "POWKONG"], "POWKONG"),
        "销售平台": _pick(platform, PLATFORM_OPTS, "未知"),
        "产品": (c.get("product") or "")[:200],
        "客户标识": frm,
        "订单号": order_no,
        "客诉摘要": summary[:500],
        "原文": (subj + "\n\n" + body)[:8000],
        "语种": _pick(c.get("language"), LANG_OPTS, "其他"),
        "AI置信度": _pick(c.get("confidence"), CONF_OPTS, "必须人工"),
        "AI草稿": (c.get("draft_reply") or "")[:5000],
        "分配运营": operator,
        "状态": status,
        "线程ID": m.get("messageId") or "",
    }
    ct = _pick(c.get("complaint_type"), TYPE_OPTS, None)
    if ct:
        fields["客诉类型"] = ct
    return fields


# ===== 主入口 =====
async def run(limit: int = 20, dry_run: bool = False) -> dict:
    if not (ZCID and ZRT and ZACC):
        return {"error": "ZOHO_POWKONG_CS_* env 未配齐"}
    tok = await _ztoken()
    listing = await _zget(
        f"https://mail.zoho.com/api/accounts/{ZACC}/messages/view"
        f"?folderId={POWKONG_INBOX_FID}&limit={limit}&start=0", tok)
    msgs = listing.get("data", []) or []
    existing = await _existing_thread_ids()

    new_cnt, skip_cnt, err_cnt = 0, 0, 0
    samples = []
    for m in msgs:
        mid = m.get("messageId")
        if not mid or mid in existing:
            skip_cnt += 1
            continue
        frm = m.get("fromAddress", "")
        subj = m.get("subject", "")
        try:
            content = await _zget(
                f"https://mail.zoho.com/api/accounts/{ZACC}/folders/{POWKONG_INBOX_FID}"
                f"/messages/{mid}/content", tok)
            d = content.get("data")
            body = _strip_html(d.get("content", "") if isinstance(d, dict) else "")
        except Exception:
            body = ""
        try:
            c = await _classify(frm, subj, body[:1800])
        except Exception:
            err_cnt += 1
            continue
        fields = _to_fields(m, frm, subj, body, c)
        if len(samples) < 12:
            samples.append({"from": frm, "is_cs": c.get("is_cs"),
                            "平台": fields["销售平台"], "运营": fields["分配运营"],
                            "状态": fields["状态"], "摘要": fields["客诉摘要"][:60]})
        if not dry_run:
            await feishu.api(
                "POST", f"/bitable/v1/apps/{CS_APP_TOKEN}/tables/{T_TICKET}/records",
                {"fields": fields}, which="notify")
        new_cnt += 1

    return {"fetched": len(msgs), "new": new_cnt, "skipped": skip_cnt,
            "errors": err_cnt, "dry_run": dry_run, "samples": samples}

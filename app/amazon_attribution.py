# -*- coding: utf-8 -*-
"""Phase B — Amazon Attribution per-KOL ROI (POWKONG US).

闭环: 运营在 Amazon Attribution **控制台**为每个亚马逊 KOL 建一个独立 campaign
(命名可识别如 KOL_<handle>) + 生成 measurement URL 发给 KOL → 把该 campaign 的
**campaignId(数字)** 填进 KOL 主表「亚马逊CampaignID」→ 本模块每日拉
POST /attribution/report (PERFORMANCE, groupBy=CAMPAIGN, 14 天归因窗) → 按 campaignId
聚合 clicks/detail-page-views/purchases/sales → sales_attribution 按 campaignId 映射回 KOL,
与独立站折扣码归因**并列合并**成双源 ROI (累计GMV = 独立站 + 亚马逊)。

⚠️ Amazon Attribution **没有创建 tag/campaign 的写 API** (OpenAPI spec 确认: tag 端点全是 GET).
campaign 必须控制台手建; API 只负责取 tag 串 + 拉报告。映射 key 是报告行的 **campaignId**
(报告响应里**没有 campaignName**), 所以飞书表存的必须是 campaignId。

⚠️ env-gated + fail-safe: AMZ_ADS_* 任一未配 → is_enabled()=False → sales_attribution
跳过亚马逊源, 现网行为完全不变。

凭据 (全进 Zeabur env, 不入仓):
  AMZ_ADS_CLIENT_ID / AMZ_ADS_CLIENT_SECRET   ← Login with Amazon (LWA) 应用
  AMZ_ADS_REFRESH_TOKEN                         ← OAuth 授权码流程换得 (/amazon/oauth/callback, 一次性)
  AMZ_ADS_PROFILE_ID                            ← GET /v2/profiles 里 US/POWKONG 的 profileId

权威来源: Amazon Attribution OpenAPI spec
  https://dtrnk0o2zy01c.cloudfront.net/openapi/en-us/dest/AmazonAttribution_prod_3p.json
LWA scope = advertising::campaign_management (无专用 attribution scope)。access_token 1h 过期。
"""
import os
import time
import asyncio
import httpx

LWA_TOKEN_URL = "https://api.amazon.com/auth/o2/token"
ADS_HOST = os.environ.get("AMZ_ADS_HOST", "https://advertising-api.amazon.com")  # NA
ATTR_LOOKBACK_DAYS = int(os.environ.get("AMZ_ATTR_LOOKBACK_DAYS", "14"))  # 14 天归因窗

# PERFORMANCE 报告四个核心指标的**确切列名** (OpenAPI spec verbatim):
M_CLICKS = "Click-throughs"
M_DPV = "attributedDetailPageViewsClicks14d"
M_PURCHASES = "attributedPurchases14d"
M_SALES = "attributedSales14d"
M_BRB = "brb_bonus_amount"  # BRB 返点金额 (顺带可拿, Phase C 毛利口径)
_REPORT_METRICS = ",".join([M_CLICKS, M_DPV, M_PURCHASES, M_SALES, M_BRB])


def is_enabled() -> bool:
    """凭据配齐才启用; 否则 sales_attribution 跳过亚马逊源 (现网不变)。"""
    return all(os.environ.get(k) for k in
               ("AMZ_ADS_CLIENT_ID", "AMZ_ADS_CLIENT_SECRET",
                "AMZ_ADS_REFRESH_TOKEN", "AMZ_ADS_PROFILE_ID"))


# === access_token 刷新 (1h 缓存) ===
_token_cache = {"tok": None, "exp": 0}


async def get_access_token() -> str:
    if _token_cache["tok"] and _token_cache["exp"] > time.time():
        return _token_cache["tok"]
    cid = os.environ.get("AMZ_ADS_CLIENT_ID", "")
    secret = os.environ.get("AMZ_ADS_CLIENT_SECRET", "")
    refresh = os.environ.get("AMZ_ADS_REFRESH_TOKEN", "")
    if not (cid and secret and refresh):
        raise RuntimeError("AMZ_ADS_CLIENT_ID/SECRET/REFRESH_TOKEN env 未配齐")
    async with httpx.AsyncClient(timeout=30.0) as c:
        r = await c.post(LWA_TOKEN_URL, data={
            "grant_type": "refresh_token", "refresh_token": refresh,
            "client_id": cid, "client_secret": secret,
        }, headers={"Content-Type": "application/x-www-form-urlencoded"})
        r.raise_for_status()
        d = r.json()
    _token_cache["tok"] = d["access_token"]
    _token_cache["exp"] = time.time() + int(d.get("expires_in", 3600)) - 120
    return _token_cache["tok"]


async def exchange_code_for_refresh_token(code: str, redirect_uri: str) -> dict:
    """OAuth 授权码 → refresh_token (一次性, /amazon/oauth/callback 调). 不缓存。
    返回 {access_token, refresh_token, expires_in, ...} 或 {error, error_description}。"""
    cid = os.environ.get("AMZ_ADS_CLIENT_ID", "")
    secret = os.environ.get("AMZ_ADS_CLIENT_SECRET", "")
    if not (cid and secret):
        raise RuntimeError("AMZ_ADS_CLIENT_ID/SECRET env 未配 (建 LWA 应用后先配这两个)")
    async with httpx.AsyncClient(timeout=30.0) as c:
        r = await c.post(LWA_TOKEN_URL, data={
            "grant_type": "authorization_code", "code": code,
            "client_id": cid, "client_secret": secret, "redirect_uri": redirect_uri,
        }, headers={"Content-Type": "application/x-www-form-urlencoded"})
        return r.json()


async def _headers(scope_profile: bool = True) -> dict:
    tok = await get_access_token()
    h = {
        "Authorization": f"Bearer {tok}",
        "Amazon-Advertising-API-ClientId": os.environ.get("AMZ_ADS_CLIENT_ID", ""),
        "Content-Type": "application/json",
    }
    if scope_profile:
        h["Amazon-Advertising-API-Scope"] = os.environ.get("AMZ_ADS_PROFILE_ID", "")
    return h


# === profiles / advertisers ===
async def list_profiles() -> list:
    """GET /v2/profiles — 找 US/POWKONG 的 profileId 用 (不需要 Scope header)。
    每个 profile: {profileId, countryCode, currencyCode, accountInfo:{id,type,name,...}}"""
    async with httpx.AsyncClient(timeout=30.0) as c:
        r = await c.get(f"{ADS_HOST}/v2/profiles", headers=await _headers(scope_profile=False))
        r.raise_for_status()
        return r.json()


async def list_advertisers() -> list:
    """GET /attribution/advertisers → [{advertiserId, advertiserName}]。报告 body 要 advertiserIds。"""
    async with httpx.AsyncClient(timeout=30.0) as c:
        r = await c.get(f"{ADS_HOST}/attribution/advertisers", headers=await _headers())
        r.raise_for_status()
        d = r.json()
    return d.get("advertisers", []) if isinstance(d, dict) else (d or [])


async def list_publishers() -> list:
    """GET /attribution/publishers → [{id, name, macroEnabled}]。"""
    async with httpx.AsyncClient(timeout=30.0) as c:
        r = await c.get(f"{ADS_HOST}/attribution/publishers", headers=await _headers())
        r.raise_for_status()
        d = r.json()
    return d.get("publisher", []) if isinstance(d, dict) else (d or [])


# === 报告拉取 (B3) ===
def _num(v) -> float:
    try:
        return float(v or 0)
    except (ValueError, TypeError):
        return 0.0


async def pull_report(days: int = None, advertiser_ids: str = None) -> dict:
    """POST /attribution/report (PERFORMANCE, groupBy=CAMPAIGN) → 按 campaignId 聚合:
      {campaignId(str): {clicks, dpv, purchases, sales, brb}}

    advertiser_ids 不传则先 GET /attribution/advertisers 全取。报告行按 date×campaign 返回,
    这里跨 date 按 campaignId 累加 (得 N 天窗口合计)。失败抛异常 (上层捕获, 不影响独立站源)。
    """
    days = days or ATTR_LOOKBACK_DAYS
    if not advertiser_ids:
        advs = await list_advertisers()
        advertiser_ids = ",".join(str(a.get("advertiserId")) for a in advs if a.get("advertiserId"))
    if not advertiser_ids:
        return {}  # 无广告主 = 无可拉数据

    end = time.strftime("%Y%m%d", time.gmtime())
    start = time.strftime("%Y%m%d", time.gmtime(time.time() - days * 86400))
    body = {
        "reportType": "PERFORMANCE",
        "advertiserIds": advertiser_ids,
        "startDate": start,
        "endDate": end,
        "metrics": _REPORT_METRICS,
        "groupBy": "CAMPAIGN",
        "cursorId": "",
        "count": 5000,
    }

    rows = []
    async with httpx.AsyncClient(timeout=60.0) as c:
        for _ in range(100):  # 分页保护上限
            r = await c.post(f"{ADS_HOST}/attribution/report",
                             headers=await _headers(), json=body)
            if r.status_code == 429:
                await asyncio.sleep(2)
                continue
            r.raise_for_status()
            d = r.json()
            rows.extend(d.get("reports") or d.get("rows") or [])
            cursor = d.get("cursorId") or d.get("nextCursor") or ""
            if not cursor:
                break
            body["cursorId"] = cursor

    out = {}
    for row in rows:
        cid = row.get("campaignId")
        if cid in (None, ""):
            continue
        key = str(cid).strip()
        a = out.setdefault(key, {"clicks": 0.0, "dpv": 0.0, "purchases": 0.0,
                                 "sales": 0.0, "brb": 0.0})
        a["clicks"] += _num(row.get(M_CLICKS))
        a["dpv"] += _num(row.get(M_DPV))
        a["purchases"] += _num(row.get(M_PURCHASES))
        a["sales"] += _num(row.get(M_SALES))
        a["brb"] += _num(row.get(M_BRB))
    return out


async def selftest() -> dict:
    """云端 smoke (凭据到位后): 刷 token + 列 profiles + 列 advertisers, 不写任何数据。"""
    if not is_enabled():
        return {"ok": False, "enabled": False,
                "msg": "AMZ_ADS_* env 未配齐, Amazon Attribution 未启用"}
    out = {"ok": True, "enabled": True}
    try:
        profs = await list_profiles()
        out["profiles"] = [
            {"profileId": p.get("profileId"), "country": p.get("countryCode"),
             "name": (p.get("accountInfo") or {}).get("name"),
             "type": (p.get("accountInfo") or {}).get("type")}
            for p in (profs if isinstance(profs, list) else [])
        ]
    except Exception as e:
        out["profiles_err"] = str(e)[:300]
    try:
        out["advertisers"] = await list_advertisers()
    except Exception as e:
        out["advertisers_err"] = str(e)[:300]
    return out

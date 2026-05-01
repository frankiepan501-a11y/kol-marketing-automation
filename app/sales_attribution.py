# -*- coding: utf-8 -*-
"""Phase 3 ROI 闭环: Shopify 订单销售归因

链路:
1. 双店并行拉最近 N 天的 paid/fulfilled 订单 (Powkong + Funlab)
2. 解析订单 landing_site_ref / referring_site / note_attributes 提取 utm_content
3. utm_content = kol_{handle_slug}, 与 KOL 主表「UTM ID」字段一致
4. 按 utm_content group by → 累计订单数 + GMV + 上次订单日期
5. 写回飞书 KOL 主表 (idempotent — 重跑不会重复加)

Shopify API 凭证从 env 读:
- SHOPIFY_FUNLAB_TOKEN (shpat_, offline access token, 直接 X-Shopify-Access-Token header)
- SHOPIFY_FUNLAB_SHOP (funlabstore.myshopify.com)
- SHOPIFY_POWKONG_CLIENT_ID + SHOPIFY_POWKONG_CLIENT_SECRET (client_credentials grant 流换 token)
- SHOPIFY_POWKONG_SHOP (powkong.myshopify.com)
"""
import os
import re
import time
import asyncio
import httpx
from urllib.parse import urlparse, parse_qs
from . import config, feishu
from .feishu import ext


SHOPIFY_API_VERSION = "2025-01"
ATTRIBUTION_LOOKBACK_DAYS = int(os.environ.get("SHOPIFY_LOOKBACK_DAYS", "90"))


# === 1. 双店 token 管理 ===
_token_cache = {}  # {brand: (token, expiry_ts)}


async def get_token(brand: str) -> str:
    """获取 Shopify Admin API access token.
    Funlab: 直接用 env 里的 shpat_; Powkong: client_credentials grant"""
    cached = _token_cache.get(brand)
    if cached and cached[1] > time.time():
        return cached[0]

    if brand == "FUNLAB":
        tok = os.environ.get("SHOPIFY_FUNLAB_TOKEN", "").strip()
        if not tok:
            raise RuntimeError("SHOPIFY_FUNLAB_TOKEN env 未设")
        _token_cache[brand] = (tok, time.time() + 365 * 86400)  # offline token, 视为长期
        return tok

    if brand == "POWKONG":
        cid = os.environ.get("SHOPIFY_POWKONG_CLIENT_ID", "")
        secret = os.environ.get("SHOPIFY_POWKONG_CLIENT_SECRET", "")
        shop = os.environ.get("SHOPIFY_POWKONG_SHOP", "powkong.myshopify.com")
        if not cid or not secret:
            raise RuntimeError("SHOPIFY_POWKONG_CLIENT_ID/SECRET env 未设")
        async with httpx.AsyncClient(timeout=30.0) as cli:
            r = await cli.post(
                f"https://{shop}/admin/oauth/access_token",
                json={"client_id": cid, "client_secret": secret,
                      "grant_type": "client_credentials"},
            )
            r.raise_for_status()
            tok = r.json()["access_token"]
        _token_cache[brand] = (tok, time.time() + 23 * 3600)  # 24h-ish
        return tok

    raise RuntimeError(f"unknown brand {brand}")


def get_shop(brand: str) -> str:
    if brand == "FUNLAB":
        return os.environ.get("SHOPIFY_FUNLAB_SHOP", "funlabstore.myshopify.com")
    if brand == "POWKONG":
        return os.environ.get("SHOPIFY_POWKONG_SHOP", "powkong.myshopify.com")
    raise RuntimeError(f"unknown brand {brand}")


# === 2. 拉订单 (REST API + 分页) ===
async def fetch_orders(brand: str, days: int = 90) -> list:
    """拉最近 N 天的所有 paid 订单 (含 fulfilled/partial/unfulfilled). 返回 [order_dict, ...]"""
    tok = await get_token(brand)
    shop = get_shop(brand)
    since_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ",
                               time.gmtime(time.time() - days * 86400))

    orders = []
    url = (f"https://{shop}/admin/api/{SHOPIFY_API_VERSION}/orders.json"
           f"?status=any&financial_status=paid&created_at_min={since_iso}&limit=250"
           f"&fields=id,name,order_number,created_at,total_price,currency,"
           f"landing_site,landing_site_ref,referring_site,note_attributes,"
           f"customer_journey_summary,source_name")
    async with httpx.AsyncClient(timeout=60.0) as cli:
        while url:
            r = await cli.get(url, headers={"X-Shopify-Access-Token": tok})
            if r.status_code == 429:
                await asyncio.sleep(2)
                continue
            r.raise_for_status()
            data = r.json()
            orders.extend(data.get("orders", []))
            # Shopify cursor pagination via Link header
            link = r.headers.get("Link", "")
            m = re.search(r'<([^>]+)>;\s*rel="next"', link)
            url = m.group(1) if m else None
    return orders


# === 3. 从订单字段抽 utm_content ===
UTM_CONTENT_RE = re.compile(r"utm_content=([^&\s]+)", re.I)
UTM_SOURCE_RE = re.compile(r"utm_source=([^&\s]+)", re.I)


def extract_utm_content(order: dict) -> tuple:
    """返回 (utm_content, utm_source) 或 (None, None).

    依次查: landing_site -> landing_site_ref -> referring_site -> note_attributes
    """
    for field in ("landing_site", "landing_site_ref", "referring_site"):
        s = order.get(field) or ""
        m_c = UTM_CONTENT_RE.search(s)
        m_s = UTM_SOURCE_RE.search(s)
        if m_c:
            return (m_c.group(1).lower(), m_s.group(1).lower() if m_s else "")

    # note_attributes 也存可能含 utm
    for attr in (order.get("note_attributes") or []):
        nv = (attr.get("name", "") + "=" + str(attr.get("value", "")))
        m_c = UTM_CONTENT_RE.search(nv)
        m_s = UTM_SOURCE_RE.search(nv)
        if m_c:
            return (m_c.group(1).lower(), m_s.group(1).lower() if m_s else "")

    return (None, None)


# === 4. 按 utm_content 聚合 ===
def aggregate_orders(orders: list, brand: str) -> dict:
    """{utm_content: {orders, gmv_usd, last_order_at_ms, last_order_id}}"""
    agg = {}
    for o in orders:
        utm_content, utm_source = extract_utm_content(o)
        if not utm_content or utm_source != "kol":
            continue  # 只统计 utm_source=kol 来源
        if not utm_content.startswith("kol_"):
            continue

        try:
            price = float(o.get("total_price") or 0)
        except (ValueError, TypeError):
            price = 0
        # Funlab USD 直接, Powkong 货币 currency 字段也是 USD
        gmv = price

        created = o.get("created_at", "")
        try:
            ts_ms = int(time.mktime(time.strptime(created[:19], "%Y-%m-%dT%H:%M:%S")) * 1000)
        except Exception:
            ts_ms = 0

        a = agg.setdefault(utm_content, {
            "orders": 0, "gmv_usd": 0.0, "last_order_at_ms": 0,
            "last_order_id": "", "brands": set(),
        })
        a["orders"] += 1
        a["gmv_usd"] += gmv
        a["brands"].add(brand)
        if ts_ms > a["last_order_at_ms"]:
            a["last_order_at_ms"] = ts_ms
            a["last_order_id"] = str(o.get("name") or o.get("id") or "")
    return agg


# === 5. 写回飞书 KOL 主表 ===
async def write_attribution_to_kol(agg: dict) -> dict:
    """根据 utm_content 找到 KOL 主表对应记录, 写累计订单/GMV.
    idempotent: 直接 PUT 当前累计值 (不是 +=)"""
    stats = {"matched": 0, "no_kol_found": 0, "write_err": 0, "details": []}
    for utm_content, a in agg.items():
        # 在 KOL 主表搜 UTM ID = utm_content
        try:
            items = await feishu.search_records(config.T_KOL, [
                {"field_name": "UTM ID", "operator": "is", "value": [utm_content]}
            ])
        except Exception as e:
            stats["write_err"] += 1
            stats["details"].append({"utm": utm_content, "err": f"search: {str(e)[:80]}"})
            continue

        if not items:
            # 也试 编辑表 (媒体人也可能有 utm)
            try:
                items = await feishu.search_records(config.T_EDITOR, [
                    {"field_name": "UTM ID", "operator": "is", "value": [utm_content]}
                ])
                target_table = config.T_EDITOR
            except Exception:
                items = []
                target_table = config.T_KOL
            if not items:
                stats["no_kol_found"] += 1
                stats["details"].append({"utm": utm_content, "status": "no_kol_found",
                                          "orders": a["orders"], "gmv": round(a["gmv_usd"], 2)})
                continue
        else:
            target_table = config.T_KOL

        rid = items[0]["record_id"]
        try:
            await feishu.update_record(target_table, rid, {
                "累计订单数": a["orders"],
                "累计GMV": round(a["gmv_usd"], 2),
                "上次订单日期": a["last_order_at_ms"],
                "上次订单ID": a["last_order_id"],
            })
            stats["matched"] += 1
            stats["details"].append({
                "utm": utm_content, "rid": rid, "orders": a["orders"],
                "gmv": round(a["gmv_usd"], 2),
            })
        except Exception as e:
            stats["write_err"] += 1
            stats["details"].append({"utm": utm_content, "err": f"update: {str(e)[:80]}"})

    return stats


# === 6. 主流程 ===
async def run():
    """每日 cron 调: 双店拉单 + 聚合 + 写飞书"""
    started_at = time.time()
    summary = {"started_at": int(started_at), "brands": {}}

    # 双店并行拉单
    try:
        funlab_orders, powkong_orders = await asyncio.gather(
            fetch_orders("FUNLAB", ATTRIBUTION_LOOKBACK_DAYS),
            fetch_orders("POWKONG", ATTRIBUTION_LOOKBACK_DAYS),
            return_exceptions=True,
        )
    except Exception as e:
        return {"ok": False, "error": f"gather: {str(e)[:200]}"}

    if isinstance(funlab_orders, Exception):
        summary["brands"]["FUNLAB"] = {"error": str(funlab_orders)[:200]}
        funlab_orders = []
    if isinstance(powkong_orders, Exception):
        summary["brands"]["POWKONG"] = {"error": str(powkong_orders)[:200]}
        powkong_orders = []

    summary["brands"]["FUNLAB"] = summary["brands"].get("FUNLAB", {})
    summary["brands"]["FUNLAB"]["orders_total"] = len(funlab_orders)
    summary["brands"]["POWKONG"] = summary["brands"].get("POWKONG", {})
    summary["brands"]["POWKONG"]["orders_total"] = len(powkong_orders)

    # 聚合 (双店合并到一个 utm_content map, 因为同一 KOL 的 UTM ID 跨品牌共用)
    agg_funlab = aggregate_orders(funlab_orders, "FUNLAB")
    agg_powkong = aggregate_orders(powkong_orders, "POWKONG")
    agg = {}
    for src in (agg_funlab, agg_powkong):
        for utm, a in src.items():
            if utm not in agg:
                agg[utm] = a
            else:
                # 同 KOL 双品牌都有订单 → 合并
                agg[utm]["orders"] += a["orders"]
                agg[utm]["gmv_usd"] += a["gmv_usd"]
                agg[utm]["brands"] |= a["brands"]
                if a["last_order_at_ms"] > agg[utm]["last_order_at_ms"]:
                    agg[utm]["last_order_at_ms"] = a["last_order_at_ms"]
                    agg[utm]["last_order_id"] = a["last_order_id"]

    summary["utm_kols_found"] = len(agg)
    summary["brands"]["FUNLAB"]["utm_orders"] = sum(a["orders"] for a in agg_funlab.values())
    summary["brands"]["POWKONG"]["utm_orders"] = sum(a["orders"] for a in agg_powkong.values())

    # 写回飞书
    write_stats = await write_attribution_to_kol(agg)
    summary["write"] = write_stats
    summary["elapsed_s"] = round(time.time() - started_at, 1)
    summary["lookback_days"] = ATTRIBUTION_LOOKBACK_DAYS
    return {"ok": True, **summary}

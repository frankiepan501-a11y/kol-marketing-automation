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
           f"discount_codes,customer_journey_summary,source_name")
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


# === 4. 建 KOL 归因映射 (折扣码 + UTM ID → KOL/编辑记录) ===
def _norm_code(s: str) -> str:
    return (s or "").strip().upper()


async def build_kol_maps() -> dict:
    """一次性读 KOL + 编辑主表, 建两张内存映射:
      code: {折扣码大写: (table, rid, name)}   ← 折扣码归因 (顾客主动用码=强信号)
      utm:  {utm_id小写: (table, rid, name)}    ← UTM 归因 (cold 信链接)
    撞码/撞 utm 先到先得 (setdefault), dup_codes 记冲突数供观测.
    """
    maps = {"code": {}, "utm": {}, "dup_codes": 0}
    for table, name_field in [(config.T_KOL, "账号名"), (config.T_EDITOR, "媒体人姓名")]:
        try:
            items = await feishu.search_records(
                table, [], field_names=["折扣码", "UTM ID", name_field])
        except Exception:
            items = await feishu.fetch_all_records(table)
        for it in items:
            f = it.get("fields", {})
            rid = it.get("record_id")
            nm = ext(f.get(name_field))
            code = _norm_code(ext(f.get("折扣码")))
            if code:
                if code in maps["code"]:
                    maps["dup_codes"] += 1
                else:
                    maps["code"][code] = (table, rid, nm)
            utmid = ext(f.get("UTM ID")).strip().lower()
            if utmid:
                maps["utm"].setdefault(utmid, (table, rid, nm))
    return maps


def _order_ts_ms(order: dict) -> int:
    created = order.get("created_at", "")
    try:
        return int(time.mktime(time.strptime(created[:19], "%Y-%m-%dT%H:%M:%S")) * 1000)
    except Exception:
        return 0


def _order_price(order: dict) -> float:
    try:
        return float(order.get("total_price") or 0)  # 折后实付 = 真实营收
    except (ValueError, TypeError):
        return 0.0


def match_order(order: dict, maps: dict) -> tuple:
    """把一单归因到唯一 KOL. 折扣码优先 (顾客主动用码), utm 回退.
    返回 (kol_tuple, via) 或 (None, None). kol_tuple = (table, rid, name)."""
    # 折扣码归因
    for c in (order.get("discount_codes") or []):
        code = _norm_code(c.get("code"))
        if code and code in maps["code"]:
            return maps["code"][code], "code"
    # UTM 归因 (回退)
    utm_content, utm_source = extract_utm_content(order)
    if (utm_content and utm_source == "kol" and utm_content.startswith("kol_")
            and utm_content in maps["utm"]):
        return maps["utm"][utm_content], "utm"
    return None, None


# === 5. 按 KOL 记录聚合 (折扣码 + utm 并集, 每单只算一次) ===
def attribute_into(agg: dict, orders: list, brand: str, maps: dict, perbrand: dict):
    """归因 orders 并累加进 agg (key = 'table::rid'). perbrand: {code,utm} via 计数."""
    for o in orders:
        kol, via = match_order(o, maps)
        if not kol:
            continue
        table, rid, name = kol
        key = f"{table}::{rid}"
        a = agg.setdefault(key, {
            "table": table, "rid": rid, "name": name,
            "orders": 0, "gmv_usd": 0.0, "last_order_at_ms": 0,
            "last_order_id": "", "via": set(), "brands": set(),
        })
        a["orders"] += 1
        a["gmv_usd"] += _order_price(o)
        a["via"].add(via)
        a["brands"].add(brand)
        ts_ms = _order_ts_ms(o)
        if ts_ms > a["last_order_at_ms"]:
            a["last_order_at_ms"] = ts_ms
            a["last_order_id"] = str(o.get("name") or o.get("id") or "")
        perbrand[via] = perbrand.get(via, 0) + 1


# === 6. 写回飞书 (按 record_id 直写, 无需 search) ===
async def write_attribution(agg: dict) -> dict:
    """idempotent: 直接 PUT 当前累计值 (不是 +=)."""
    stats = {"matched": 0, "write_err": 0, "details": []}
    for a in agg.values():
        try:
            await feishu.update_record(a["table"], a["rid"], {
                "累计订单数": a["orders"],
                "累计GMV": round(a["gmv_usd"], 2),
                "上次订单日期": a["last_order_at_ms"],
                "上次订单ID": a["last_order_id"],
            })
            stats["matched"] += 1
            stats["details"].append({
                "name": a["name"], "rid": a["rid"], "orders": a["orders"],
                "gmv": round(a["gmv_usd"], 2), "via": sorted(a["via"]),
            })
        except Exception as e:
            stats["write_err"] += 1
            stats["details"].append({"rid": a["rid"], "err": f"update: {str(e)[:80]}"})
    return stats


# === 7. 主流程 ===
async def run():
    """每日 cron 调: 建映射 + 双店拉单 + 折扣码/utm 并集归因 + 写飞书"""
    started_at = time.time()
    summary = {"started_at": int(started_at), "brands": {}}

    # 先建归因映射 (折扣码 + utm)
    try:
        maps = await build_kol_maps()
    except Exception as e:
        return {"ok": False, "error": f"build_kol_maps: {str(e)[:200]}"}
    summary["codes_indexed"] = len(maps["code"])
    summary["utm_ids_indexed"] = len(maps["utm"])
    summary["dup_codes_ignored"] = maps["dup_codes"]

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

    summary["brands"].setdefault("FUNLAB", {})["orders_total"] = len(funlab_orders)
    summary["brands"].setdefault("POWKONG", {})["orders_total"] = len(powkong_orders)

    # 归因聚合 (key=table::rid, 跨店合并, 每单只算一次)
    agg = {}
    for orders, brand in [(funlab_orders, "FUNLAB"), (powkong_orders, "POWKONG")]:
        perbrand = {}
        attribute_into(agg, orders, brand, maps, perbrand)
        summary["brands"][brand]["attributed_by_code"] = perbrand.get("code", 0)
        summary["brands"][brand]["attributed_by_utm"] = perbrand.get("utm", 0)

    summary["kols_matched"] = len(agg)
    summary["attributed_total"] = sum(a["orders"] for a in agg.values())

    # 写回飞书
    write_stats = await write_attribution(agg)
    summary["write"] = write_stats
    summary["elapsed_s"] = round(time.time() - started_at, 1)
    summary["lookback_days"] = ATTRIBUTION_LOOKBACK_DAYS
    return {"ok": True, **summary}

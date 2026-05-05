"""Funlab Shopline collector.

数据源: Shopline OpenAPI v2 (env SHOPLINE_FL_TOKEN, JWT 长期至 2029)
店铺: funlab.myshopline.com (后台域名), 前端是 funlabswitch.com.

⚠️ funlab.net Shopify 迁移完成 + DNS cutover 后, 此 collector 归档.

Shopline OpenAPI 文档: https://developer.shopline.com/docs/admin-rest-api
- Base URL: https://{shop}.myshopline.com/admin/openapi/v20251201
- Auth: Authorization: Bearer {jwt}
- 字段名 / 状态枚举与 Shopify 不同, 见下注释

Shopline 订单状态字段:
- financial_status:  paid / partially_paid / pending / refunded / partially_refunded / voided
- fulfillment_status: fulfilled / partial / unfulfilled
- order_status:       open / archived / cancelled
- 取消订单: order_status == "cancelled" 或 cancelled_at 非空
"""
import asyncio
import logging
import os
import datetime
from collections import defaultdict

import httpx

log = logging.getLogger("weekly_report.shopline")

API_VERSION = "v20251201"
TIMEOUT = 60.0


def _get_creds():
    tok = (os.environ.get("SHOPLINE_FL_TOKEN")
           or os.environ.get("SHOPLINE_FUNLAB_TOKEN")
           or os.environ.get("SHOPLINE_FL_JWT", "")).strip()
    shop = os.environ.get("SHOPLINE_FL_SHOP", "funlab.myshopline.com")
    if not tok:
        raise RuntimeError("SHOPLINE_FL_TOKEN env 未设")
    return tok, shop


async def _fetch_all_orders(start_date, end_date) -> list:
    """拉指定周所有订单 (含 cancelled).

    Shopline 分页用 page + per_page (REST 风格), 不像 Shopify cursor + Link header.
    """
    tok, shop = _get_creds()
    since = start_date.strftime("%Y-%m-%dT00:00:00Z")
    until = (end_date + datetime.timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z")

    orders = []
    page = 1
    per_page = 100

    async with httpx.AsyncClient(timeout=TIMEOUT) as cli:
        while True:
            url = (f"https://{shop}/admin/openapi/{API_VERSION}/orders.json"
                   f"?created_at_min={since}&created_at_max={until}"
                   f"&page={page}&per_page={per_page}&status=any")
            r = await cli.get(url, headers={
                "Authorization": f"Bearer {tok}",
                "Accept": "application/json",
            })
            if r.status_code == 429:
                await asyncio.sleep(2)
                continue
            r.raise_for_status()
            data = r.json()
            batch = data.get("orders") or data.get("data", {}).get("orders") or []
            if not batch:
                break
            orders.extend(batch)
            if len(batch) < per_page:
                break
            page += 1
            if page > 50:
                log.warning("shopline pagination cap 50 hit")
                break
    return orders


def _calc_metrics(orders: list) -> dict:
    """从 Shopline 订单列表计算周报指标.

    字段名 fallback (Shopline 不同版本/区域可能略有差异):
      total_amount / total_price
      subtotal_amount / subtotal_price
      total_discount / discount_amount
    """
    def f(o, *keys, default=0):
        for k in keys:
            v = o.get(k)
            if v is not None:
                return v
        return default

    paid = [o for o in orders if (o.get("financial_status") or "").lower() == "paid"]
    cancelled = [o for o in orders if o.get("cancelled_at") or
                 (o.get("order_status") or "").lower() == "cancelled"]
    fulfilled = [o for o in orders if (o.get("fulfillment_status") or "").lower() == "fulfilled"]

    gross = sum(float(f(o, "subtotal_amount", "subtotal_price") or 0) for o in paid)
    net = sum(float(f(o, "total_amount", "total_price") or 0) for o in paid)
    discounts = sum(float(f(o, "total_discount", "discount_amount", "total_discounts") or 0) for o in paid)

    refund_count = sum(1 for o in orders if (o.get("financial_status") or "").lower()
                       in ("refunded", "partially_refunded"))
    refund_amount = sum(float(f(o, "total_refunded", "refunded_amount") or 0) for o in orders)

    # 异常 - 同 email 24h
    by_email_day = defaultdict(list)
    for o in paid:
        cust = o.get("customer") or {}
        email = (cust.get("email") or o.get("email") or "").lower().strip()
        if not email:
            continue
        day = (o.get("created_at") or "")[:10]
        by_email_day[(email, day)].append(o.get("name") or o.get("order_number"))
    duplicate_24h = sum(1 for v in by_email_day.values() if len(v) >= 2)

    currency = (orders[0].get("currency") if orders else None) or "USD"

    return {
        "total_orders": len(orders),
        "paid_orders": len(paid),
        "gross_sales": round(gross, 2),
        "net_sales": round(net, 2),
        "total_discounts": round(discounts, 2),
        "fulfilled_orders": len(fulfilled),
        "cancelled_orders": len(cancelled),
        "refund_count": refund_count,
        "refund_amount": round(refund_amount, 2),
        "refund_rate": round(refund_count / max(len(paid), 1), 4),
        "abnormal": {
            "cancelled": len(cancelled),
            "duplicate_email_24h": duplicate_24h,
        },
        "currency": currency,
    }


async def collect(start_date, end_date) -> dict:
    """Funlab Shopline 周报数据."""
    log.info("shopline.collect Funlab %s ~ %s", start_date, end_date)
    try:
        orders = await _fetch_all_orders(start_date, end_date)
    except Exception as e:
        log.exception("shopline fetch_all_orders failed")
        return {"status": "error", "error": f"{type(e).__name__}: {e}", "data": {}}

    metrics = _calc_metrics(orders)
    metrics["brand"] = "Funlab"
    metrics["shop"] = os.environ.get("SHOPLINE_FL_SHOP", "funlab.myshopline.com")
    metrics["window"] = f"{start_date}~{end_date}"

    return {"status": "ok", "data": metrics}


if __name__ == "__main__":
    import datetime as dt
    today = dt.date.today()
    last_sun = today - dt.timedelta(days=today.weekday() + 1)
    last_mon = last_sun - dt.timedelta(days=6)
    print(asyncio.run(collect(last_mon, last_sun)))

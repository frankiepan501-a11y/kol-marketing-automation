"""Powkong Shopify collector.

数据源: Shopify Admin REST API (复用 sales_attribution.get_token / get_shop)
当前只拉 Powkong (powkong.myshopify.com); funlab.net 迁完后再扩 FUNLAB.

输出 collected.shopify.data:
{
  "brand": "Powkong",
  "shop": "powkong.myshopify.com",
  "total_orders": int,         # 含 cancelled
  "paid_orders": int,
  "gross_sales": float,        # 商品小计 (subtotal)
  "net_sales": float,          # total_price (含税运)
  "total_discounts": float,
  "fulfilled_orders": int,
  "cancelled_orders": int,
  "refund_count": int,
  "refund_amount": float,
  "refund_rate": float,        # refund_count / paid_orders
  "refund_reasons_top5": [(note, count), ...],
  "discount_orders": int,      # 有用折扣码的订单
  "discount_total": float,
  "abnormal": {
    "cancelled": int,
    "duplicate_email_24h": int,   # 同 email 24h 内 >= 2 单
  },
}
"""
import asyncio
import logging
import re
import datetime
from collections import defaultdict

import httpx

log = logging.getLogger("weekly_report.shopify")


def _get_sa():
    """Lazy import sales_attribution to avoid config env check at module load."""
    from app import sales_attribution
    return sales_attribution

API_VERSION = "2025-01"
TIMEOUT = 60.0


async def _fetch_all_orders(brand: str, start_date, end_date) -> list:
    """拉指定周所有 status=any 订单 (含 cancelled/refunded)."""
    tok = await _get_sa().get_token(brand)
    shop = _get_sa().get_shop(brand)
    since = start_date.strftime("%Y-%m-%dT00:00:00Z")
    until = (end_date + datetime.timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z")

    orders = []
    fields = (
        "id,name,order_number,created_at,total_price,subtotal_price,"
        "total_tax,total_discounts,financial_status,fulfillment_status,"
        "cancel_reason,cancelled_at,refunds,customer,note,"
        "landing_site,referring_site,note_attributes,line_items"
    )
    url = (f"https://{shop}/admin/api/{API_VERSION}/orders.json"
           f"?status=any&created_at_min={since}&created_at_max={until}"
           f"&limit=250&fields={fields}")

    async with httpx.AsyncClient(timeout=TIMEOUT) as cli:
        while url:
            r = await cli.get(url, headers={"X-Shopify-Access-Token": tok})
            if r.status_code == 429:
                await asyncio.sleep(2)
                continue
            r.raise_for_status()
            data = r.json()
            orders.extend(data.get("orders", []))
            link = r.headers.get("Link", "")
            m = re.search(r'<([^>]+)>;\s*rel="next"', link)
            url = m.group(1) if m else None
    return orders


def _calc_top_products(orders: list, top_n: int = 10) -> list:
    """聚合 line_items → 按销量排序的 top N 产品.

    返回 [{product_id, title, sku, qty, revenue, qty_pct, revenue_pct, orders}, ...]
    """
    by_product = defaultdict(lambda: {"title": "", "sku": "", "qty": 0,
                                       "revenue": 0.0, "orders": set()})
    total_qty = 0
    total_revenue = 0.0
    paid_only = [o for o in orders if o.get("financial_status") == "paid"]
    for o in paid_only:
        oid = o.get("id") or o.get("name")
        for li in (o.get("line_items") or []):
            pid = li.get("product_id") or li.get("variant_id") or li.get("title")
            qty = int(li.get("quantity") or 0)
            price = float(li.get("price") or 0)
            rev = price * qty
            entry = by_product[pid]
            entry["title"] = li.get("title") or entry["title"]
            entry["sku"] = li.get("sku") or entry["sku"]
            entry["qty"] += qty
            entry["revenue"] += rev
            entry["orders"].add(oid)
            total_qty += qty
            total_revenue += rev

    products = []
    for pid, e in by_product.items():
        products.append({
            "product_id": pid,
            "title": e["title"],
            "sku": e["sku"],
            "qty": e["qty"],
            "revenue": round(e["revenue"], 2),
            "qty_pct": round(e["qty"] / max(total_qty, 1), 4),
            "revenue_pct": round(e["revenue"] / max(total_revenue, 1), 4),
            "orders": len(e["orders"]),
        })
    products.sort(key=lambda p: (-p["qty"], -p["revenue"]))
    return products[:top_n]


def _calc_metrics(orders: list) -> dict:
    """从订单列表计算周报指标."""
    paid = [o for o in orders if o.get("financial_status") == "paid"]
    cancelled = [o for o in orders if o.get("cancelled_at")]
    fulfilled = [o for o in orders if o.get("fulfillment_status") == "fulfilled"]

    gross = sum(float(o.get("subtotal_price") or 0) for o in paid)
    net = sum(float(o.get("total_price") or 0) for o in paid)
    discounts = sum(float(o.get("total_discounts") or 0) for o in paid)

    # 退款
    refund_count = 0
    refund_amount = 0.0
    refund_reasons = defaultdict(int)
    for o in orders:
        for refund in (o.get("refunds") or []):
            for trans in (refund.get("transactions") or []):
                if trans.get("kind") == "refund" and trans.get("status") == "success":
                    refund_amount += float(trans.get("amount") or 0)
                    refund_count += 1
            note = (refund.get("note") or "(未填)")[:80]
            refund_reasons[note] += 1

    # 折扣使用
    discount_orders_list = [o for o in paid if float(o.get("total_discounts") or 0) > 0]
    discount_total = sum(float(o.get("total_discounts") or 0) for o in discount_orders_list)

    # 异常订单 -- 同 email 24h 内 >= 2 单
    by_email_day = defaultdict(list)
    for o in paid:
        cust = o.get("customer") or {}
        email = (cust.get("email") or "").lower().strip()
        if not email:
            continue
        day = (o.get("created_at") or "")[:10]
        by_email_day[(email, day)].append(o.get("name"))
    duplicate_24h = sum(1 for v in by_email_day.values() if len(v) >= 2)

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
        "refund_reasons_top5": sorted(refund_reasons.items(), key=lambda x: -x[1])[:5],
        "discount_orders": len(discount_orders_list),
        "discount_total": round(discount_total, 2),
        "abnormal": {
            "cancelled": len(cancelled),
            "duplicate_email_24h": duplicate_24h,
        },
        "top_products": _calc_top_products(orders, top_n=10),
    }


async def collect(start_date, end_date) -> dict:
    """Powkong Shopify 周报数据."""
    log.info("shopify.collect Powkong %s ~ %s", start_date, end_date)
    try:
        orders = await _fetch_all_orders("POWKONG", start_date, end_date)
    except Exception as e:
        log.exception("shopify fetch_all_orders failed")
        return {"status": "error", "error": f"{type(e).__name__}: {e}", "data": {}}

    metrics = _calc_metrics(orders)
    metrics["brand"] = "Powkong"
    metrics["shop"] = _get_sa().get_shop("POWKONG")
    metrics["window"] = f"{start_date}~{end_date}"

    return {"status": "ok", "data": metrics}


if __name__ == "__main__":
    import datetime as dt
    today = dt.date.today()
    last_sun = today - dt.timedelta(days=today.weekday() + 1)
    last_mon = last_sun - dt.timedelta(days=6)
    print(asyncio.run(collect(last_mon, last_sun)))

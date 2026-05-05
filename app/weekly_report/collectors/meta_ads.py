"""Meta Ads collector.

数据源: Meta Marketing API v22.0
Accounts:
  - PK act_1498442934673297 (env META_PK_ACCESS_TOKEN)
  - FL act_1705425610151698 (env META_FL_ACCESS_TOKEN, 可与 PK 共用 META_ACCESS_TOKEN)

输出 collected.meta_ads.data:
{
  "powkong": {
    "summary": {
      "spend": float, "impressions": int, "clicks": int,
      "ctr": float, "cpc": float, "frequency": float,
      "purchases": int, "purchase_value": float,  # ROAS = value/spend
      "roas": float, "cpa": float,
      "add_to_cart": int, "add_to_cart_cost": float,
      "landing_page_view": int, "view_content": int,
      "initiate_checkout": int,
    },
    "daily": [{date, spend, impressions, clicks, ctr, cpc, purchases, roas, add_to_cart}, ...] x 7,
  },
  "funlab": {同上结构, FL 可能本周 0 投放}
}
"""
import asyncio
import logging
import os

import httpx

log = logging.getLogger("weekly_report.meta_ads")

API_VERSION = "v22.0"
PK_ACCOUNT = "act_1498442934673297"
FL_ACCOUNT = "act_1705425610151698"
TIMEOUT = 60.0

INSIGHTS_FIELDS = (
    "spend,impressions,clicks,ctr,cpc,frequency,reach,"
    "actions,action_values,cost_per_action_type,"
    "purchase_roas,date_start,date_stop"
)


def _get_token(brand: str) -> str:
    if brand == "POWKONG":
        tok = (os.environ.get("META_PK_ACCESS_TOKEN")
               or os.environ.get("META_ACCESS_TOKEN", "")).strip()
    else:
        tok = (os.environ.get("META_FL_ACCESS_TOKEN")
               or os.environ.get("META_ACCESS_TOKEN", "")).strip()
    if not tok:
        raise RuntimeError(f"Meta access token env 未设 (brand={brand})")
    return tok


def _action_value(actions: list, key: str) -> float:
    """从 actions 数组找指定 action_type 的 value."""
    if not actions:
        return 0.0
    for a in actions:
        if a.get("action_type") == key:
            try:
                return float(a.get("value") or 0)
            except (TypeError, ValueError):
                return 0.0
    return 0.0


def _action_cost(costs: list, key: str) -> float:
    """从 cost_per_action_type 数组找."""
    if not costs:
        return 0.0
    for c in costs:
        if c.get("action_type") == key:
            try:
                return float(c.get("value") or 0)
            except (TypeError, ValueError):
                return 0.0
    return 0.0


def _parse_insight(row: dict) -> dict:
    actions = row.get("actions") or []
    action_vals = row.get("action_values") or []
    costs = row.get("cost_per_action_type") or []

    spend = float(row.get("spend") or 0)
    impressions = int(float(row.get("impressions") or 0))
    clicks = int(float(row.get("clicks") or 0))

    purchases = int(_action_value(actions, "purchase"))
    purchase_value = _action_value(action_vals, "purchase")
    roas_list = row.get("purchase_roas") or []
    roas = float(roas_list[0].get("value")) if roas_list else (purchase_value / spend if spend > 0 else 0)

    add_to_cart = int(_action_value(actions, "add_to_cart"))
    landing_page_view = int(_action_value(actions, "landing_page_view"))
    view_content = int(_action_value(actions, "view_content"))
    initiate_checkout = int(_action_value(actions, "initiate_checkout"))

    return {
        "spend": round(spend, 2),
        "impressions": impressions,
        "clicks": clicks,
        "ctr": round(float(row.get("ctr") or 0), 4),
        "cpc": round(float(row.get("cpc") or 0), 4),
        "frequency": round(float(row.get("frequency") or 0), 2),
        "reach": int(float(row.get("reach") or 0)),
        "purchases": purchases,
        "purchase_value": round(purchase_value, 2),
        "roas": round(roas, 2),
        "cpa": round(_action_cost(costs, "purchase"), 2),
        "add_to_cart": add_to_cart,
        "add_to_cart_cost": round(_action_cost(costs, "add_to_cart"), 2),
        "landing_page_view": landing_page_view,
        "view_content": view_content,
        "initiate_checkout": initiate_checkout,
    }


async def _fetch_brand(brand: str, account: str, start_date, end_date) -> dict:
    tok = _get_token(brand)
    sd = start_date.isoformat()
    ed = end_date.isoformat()

    url = f"https://graph.facebook.com/{API_VERSION}/{account}/insights"
    params_summary = {
        "fields": INSIGHTS_FIELDS,
        "time_range": f'{{"since":"{sd}","until":"{ed}"}}',
        "level": "account",
        "access_token": tok,
    }
    params_daily = {
        **params_summary,
        "time_increment": 1,  # 按天 breakdown
    }

    async with httpx.AsyncClient(timeout=TIMEOUT) as cli:
        s_resp = await cli.get(url, params=params_summary)
        s_resp.raise_for_status()
        s_data = s_resp.json()
        d_resp = await cli.get(url, params=params_daily)
        d_resp.raise_for_status()
        d_data = d_resp.json()

    summary_rows = s_data.get("data") or []
    summary = _parse_insight(summary_rows[0]) if summary_rows else {"empty": True}

    daily = []
    for row in (d_data.get("data") or []):
        item = _parse_insight(row)
        item["date"] = row.get("date_start")
        daily.append(item)
    daily.sort(key=lambda x: x.get("date") or "")

    return {"summary": summary, "daily": daily}


async def collect(start_date, end_date) -> dict:
    """Meta Ads 双品牌数据收集."""
    log.info("meta_ads.collect %s ~ %s", start_date, end_date)
    try:
        pk, fl = await asyncio.gather(
            _fetch_brand("POWKONG", PK_ACCOUNT, start_date, end_date),
            _fetch_brand("FUNLAB", FL_ACCOUNT, start_date, end_date),
            return_exceptions=True,
        )
        if isinstance(pk, Exception):
            log.exception("meta PK fetch failed")
            pk = {"error": f"{type(pk).__name__}: {pk}"}
        if isinstance(fl, Exception):
            log.exception("meta FL fetch failed")
            fl = {"error": f"{type(fl).__name__}: {fl}"}
        return {"status": "ok", "data": {"powkong": pk, "funlab": fl,
                                          "window": f"{start_date}~{end_date}"}}
    except Exception as e:
        log.exception("meta_ads collect outer fail")
        return {"status": "error", "error": f"{type(e).__name__}: {e}", "data": {}}


if __name__ == "__main__":
    import datetime as dt
    today = dt.date.today()
    last_sun = today - dt.timedelta(days=today.weekday() + 1)
    last_mon = last_sun - dt.timedelta(days=6)
    print(asyncio.run(collect(last_mon, last_sun)))

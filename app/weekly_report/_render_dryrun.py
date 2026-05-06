"""Dry-run test: 用 mock collected_data 渲染 W18 周报, 输出到 D:/Desktop/.

跑法:
  cd C:/Users/Administrator/tmp/kol-marketing-automation
  python -m app.weekly_report._render_dryrun
"""
import asyncio
import datetime
from pathlib import Path

from app.weekly_report import renderer


def _mock_collected_w18() -> dict:
    return {
        "shopify": {
            "status": "ok",
            "data": {
                "brand": "Powkong",
                "total_orders": 19, "paid_orders": 19,
                "gross_sales": 2266.0, "net_sales": 2266.0,
                "total_discounts": 0.0,
                "fulfilled_orders": 5, "cancelled_orders": 0,
                "refund_count": 0, "refund_amount": 0.0, "refund_rate": 0.0,
                "discount_orders": 0, "discount_total": 0.0,
                "abnormal": {"cancelled": 0, "duplicate_email_24h": 0},
                "top_products": [],
            },
        },
        "shopline": {
            "status": "ok",
            "data": {
                "brand": "Funlab",
                "total_orders": 16, "paid_orders": 16,
                "gross_sales": 797.0, "net_sales": 797.0,
                "total_discounts": 0.0,
                "fulfilled_orders": 16, "cancelled_orders": 0,
                "refund_count": 0, "refund_amount": 0.0, "refund_rate": 0.0,
                "abnormal": {"cancelled": 0, "duplicate_email_24h": 2},
                "top_products": [],
            },
        },
        "ga4": {
            "status": "ok",
            "data": {
                "powkong": {
                    "core": {
                        "active_users": 2104, "sessions": 2501,
                        "total_revenue": 2110.0, "ecommerce_purchases": 17,
                        "session_conversion_rate": 0.0068, "bounce_rate": 0.694,
                        "avg_engagement_time": 46.0,
                    },
                    "channels": [
                        {"channel": "Paid Social", "sessions": 840, "revenue": 1050},
                        {"channel": "Direct", "sessions": 785, "revenue": 600},
                        {"channel": "Organic Search", "sessions": 268, "revenue": 200},
                        {"channel": "Organic Social", "sessions": 265, "revenue": 100},
                        {"channel": "Organic Shopping", "sessions": 153, "revenue": 80},
                        {"channel": "Paid Search", "sessions": 101, "revenue": 80},
                    ],
                    "social_breakdown": [], "countries": [],
                    "utm_kol": {"sessions": 0, "revenue": 0, "purchases": 0, "top5_campaigns": []},
                    "funnel": {"sessions": 2501, "add_to_cart": 83,
                               "begin_checkout": 96, "purchase": 17},
                },
                "funlab": {
                    "core": {
                        "active_users": 3310, "sessions": 3985,
                        "total_revenue": 215.0, "ecommerce_purchases": 5,
                        "session_conversion_rate": 0.0013, "bounce_rate": 0.521,
                        "avg_engagement_time": 128.0,
                    },
                    "channels": [
                        {"channel": "Direct", "sessions": 1594, "revenue": 90},
                        {"channel": "Organic Social", "sessions": 996, "revenue": 50},
                        {"channel": "Organic Search", "sessions": 717, "revenue": 30},
                        {"channel": "Referral", "sessions": 399, "revenue": 25},
                        {"channel": "Paid Social", "sessions": 199, "revenue": 15},
                        {"channel": "Email", "sessions": 80, "revenue": 5},
                    ],
                    "social_breakdown": [], "countries": [],
                    "utm_kol": {"sessions": 0, "revenue": 0, "purchases": 0, "top5_campaigns": []},
                    "funnel": {"sessions": 3985, "add_to_cart": 50,
                               "begin_checkout": 15, "purchase": 5},
                },
            },
        },
        "gsc": {
            "status": "ok",
            "data": {
                "powkong": {
                    "summary": {"clicks": 226, "impressions": 14273,
                                 "ctr": 0.0158, "position": 8.1},
                    "top_queries": [
                        {"key": "how do i enable streaming on discord twitch",
                         "clicks": 12, "impressions": 8422, "ctr": 0.0014, "position": 7.3},
                        {"key": "how long do joy-cons take to charge",
                         "clicks": 9, "impressions": 4474, "ctr": 0.0020, "position": 7.7},
                    ],
                    "top_pages": [],
                    "blogs": {"clicks": 200, "impressions": 12000, "ctr": 0.017,
                              "position": 8.0, "top_articles": []},
                },
                "funlab": {
                    "summary": {"clicks": 377, "impressions": 35355,
                                 "ctr": 0.0107, "position": 8.0},
                    "top_queries": [],
                    "top_pages": [],
                    "blogs": {"clicks": 0, "impressions": 0, "ctr": 0,
                              "position": 0, "top_articles": []},
                },
            },
        },
        "meta_ads": {
            "status": "ok",
            "data": {
                "powkong": {
                    "summary": {
                        "spend": 1011.0, "impressions": 47758, "clicks": 1483,
                        "ctr": 3.11, "cpc": 0.68, "frequency": 2.17, "reach": 22000,
                        "purchases": 12, "purchase_value": 1224.0,
                        "roas": 1.21, "cpa": 84.27,
                        "add_to_cart": 76, "add_to_cart_cost": 13.30,
                        "landing_page_view": 770, "view_content": 600,
                        "initiate_checkout": 66,
                    },
                    "daily": [
                        {"date": "2026-04-27", "spend": 144, "roas": 2.71, "purchases": 2,
                         "impressions": 6800, "clicks": 210, "ctr": 3.0, "cpc": 0.69, "add_to_cart": 11},
                        {"date": "2026-04-28", "spend": 145, "roas": 0.00, "purchases": 0,
                         "impressions": 6900, "clicks": 215, "ctr": 3.1, "cpc": 0.67, "add_to_cart": 10},
                        {"date": "2026-04-29", "spend": 144, "roas": 0.00, "purchases": 0,
                         "impressions": 6700, "clicks": 200, "ctr": 3.0, "cpc": 0.72, "add_to_cart": 9},
                        {"date": "2026-04-30", "spend": 145, "roas": 0.71, "purchases": 1,
                         "impressions": 6800, "clicks": 210, "ctr": 3.1, "cpc": 0.69, "add_to_cart": 12},
                        {"date": "2026-05-01", "spend": 145, "roas": 3.31, "purchases": 4,
                         "impressions": 6900, "clicks": 220, "ctr": 3.2, "cpc": 0.66, "add_to_cart": 14},
                        {"date": "2026-05-02", "spend": 144, "roas": 1.76, "purchases": 2,
                         "impressions": 6700, "clicks": 215, "ctr": 3.2, "cpc": 0.67, "add_to_cart": 13},
                        {"date": "2026-05-03", "spend": 144, "roas": 0.00, "purchases": 0,
                         "impressions": 6958, "clicks": 213, "ctr": 3.1, "cpc": 0.68, "add_to_cart": 7},
                    ],
                },
                "funlab": {"summary": {"empty": True}, "daily": []},
            },
        },
        "kol": {"status": "stub", "data": {}},
        "complaints": {"status": "stub", "data": {}},
        "n8n": {"status": "stub", "data": {}},
        "psi": {"status": "stub", "data": {}},
        "history": {"status": "stub", "data": {}},
    }


SAMPLE_MARKDOWN = """# 双品牌运营周报 W18

## 01. 双品牌核心指标总览

PK Shopify 销售额 $2,266 / 19 单, FL Shopline $797 / 16 单. 双框架视角: PK ROAS 1.21
低于盈亏, 需重审 Meta 受众; FL 流量大但转化弱, 是归因模型差异.

## 02. Powkong GA4 详情

转化率 0.68% 优秀, 但跳出率 69.4% 偏高 (移动端).

## 03. Powkong Meta 广告

ROAS 1.21 综合不达, CPA $84.27 偏高. 5/1 单日 ROAS 3.31 是亮点.
"""


async def main():
    start = datetime.date(2026, 4, 27)
    end = datetime.date(2026, 5, 3)
    collected = _mock_collected_w18()
    html = await renderer.render(SAMPLE_MARKDOWN, collected, start, end)

    out = Path("D:/Desktop/weekly_report_W18_renderer_dryrun.html")
    out.write_text(html, encoding="utf-8")
    print(f"OK. html_size={len(html)} written to {out}")
    print(f"开头 200 字: {html[:200]}")


if __name__ == "__main__":
    asyncio.run(main())

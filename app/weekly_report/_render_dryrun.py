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
                "top_products": [
                    {"product_id": "p1", "title": "Cubedock Pro", "sku": "CDP-01",
                     "qty": 6, "revenue": 1194.0, "qty_pct": 0.316, "revenue_pct": 0.527, "orders": 6},
                    {"product_id": "p2", "title": "Switch 2 Travel Dock", "sku": "TVD-02",
                     "qty": 4, "revenue": 552.0, "qty_pct": 0.211, "revenue_pct": 0.244, "orders": 4},
                    {"product_id": "p3", "title": "Mario Cable Pack", "sku": "MCP-03",
                     "qty": 3, "revenue": 234.0, "qty_pct": 0.158, "revenue_pct": 0.103, "orders": 3},
                    {"product_id": "p4", "title": "Toad Storage Bag", "sku": "TSB-04",
                     "qty": 2, "revenue": 130.0, "qty_pct": 0.105, "revenue_pct": 0.057, "orders": 2},
                    {"product_id": "p5", "title": "Luigi Charging Cable", "sku": "LCC-05",
                     "qty": 2, "revenue": 80.0, "qty_pct": 0.105, "revenue_pct": 0.035, "orders": 2},
                    {"product_id": "p6", "title": "Bowser Dock Skin", "sku": "BDS-06",
                     "qty": 1, "revenue": 36.0, "qty_pct": 0.053, "revenue_pct": 0.016, "orders": 1},
                    {"product_id": "p7", "title": "Yoshi Joycon Strap", "sku": "YJS-07",
                     "qty": 1, "revenue": 40.0, "qty_pct": 0.053, "revenue_pct": 0.018, "orders": 1},
                ],
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
                "top_products": [
                    {"product_id": "f1", "title": "Funlite", "sku": "FF11",
                     "qty": 5, "revenue": 200.0, "qty_pct": 0.313, "revenue_pct": 0.251, "orders": 5},
                    {"product_id": "f2", "title": "Firefly", "sku": "FF01",
                     "qty": 4, "revenue": 239.0, "qty_pct": 0.250, "revenue_pct": 0.300, "orders": 4},
                    {"product_id": "f3", "title": "Lumindock", "sku": "FF06",
                     "qty": 3, "revenue": 179.0, "qty_pct": 0.188, "revenue_pct": 0.225, "orders": 3},
                    {"product_id": "f4", "title": "Luminous", "sku": "FF02",
                     "qty": 2, "revenue": 99.0, "qty_pct": 0.125, "revenue_pct": 0.124, "orders": 2},
                    {"product_id": "f5", "title": "Luminex", "sku": "FF05",
                     "qty": 1, "revenue": 40.0, "qty_pct": 0.063, "revenue_pct": 0.050, "orders": 1},
                    {"product_id": "f6", "title": "Lumingrip", "sku": "FF03",
                     "qty": 1, "revenue": 40.0, "qty_pct": 0.063, "revenue_pct": 0.050, "orders": 1},
                ],
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
                    "social_breakdown": [],
                    "countries": [
                        {"country": "United States", "sessions": 1500, "revenue": 1490.0, "purchases": 12, "pct": 0.600, "cvr": 0.0080},
                        {"country": "United Kingdom", "sessions": 225, "revenue": 248.0, "purchases": 2, "pct": 0.090, "cvr": 0.0089},
                        {"country": "Canada", "sessions": 175, "revenue": 124.0, "purchases": 1, "pct": 0.070, "cvr": 0.0057},
                        {"country": "Germany", "sessions": 125, "revenue": 124.0, "purchases": 1, "pct": 0.050, "cvr": 0.0080},
                        {"country": "Australia", "sessions": 100, "revenue": 124.0, "purchases": 1, "pct": 0.040, "cvr": 0.0100},
                        {"country": "France", "sessions": 75, "revenue": 0, "purchases": 0, "pct": 0.030, "cvr": 0},
                        {"country": "Japan", "sessions": 60, "revenue": 0, "purchases": 0, "pct": 0.024, "cvr": 0},
                        {"country": "Netherlands", "sessions": 50, "revenue": 0, "purchases": 0, "pct": 0.020, "cvr": 0},
                        {"country": "Italy", "sessions": 40, "revenue": 0, "purchases": 0, "pct": 0.016, "cvr": 0},
                        {"country": "Spain", "sessions": 35, "revenue": 0, "purchases": 0, "pct": 0.014, "cvr": 0},
                        {"country": "Other", "sessions": 116, "revenue": 0, "purchases": 0, "pct": 0.046, "cvr": 0},
                    ],
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
                    "social_breakdown": [],
                    "countries": [
                        {"country": "United States", "sessions": 1000, "revenue": 86.0, "purchases": 2, "pct": 0.250, "cvr": 0.0020},
                        {"country": "Taiwan", "sessions": 600, "revenue": 43.0, "purchases": 1, "pct": 0.150, "cvr": 0.0017},
                        {"country": "Hong Kong", "sessions": 480, "revenue": 43.0, "purchases": 1, "pct": 0.120, "cvr": 0.0021},
                        {"country": "Japan", "sessions": 400, "revenue": 43.0, "purchases": 1, "pct": 0.100, "cvr": 0.0025},
                        {"country": "Germany", "sessions": 320, "revenue": 0, "purchases": 0, "pct": 0.080, "cvr": 0},
                        {"country": "United Kingdom", "sessions": 240, "revenue": 0, "purchases": 0, "pct": 0.060, "cvr": 0},
                        {"country": "Canada", "sessions": 200, "revenue": 0, "purchases": 0, "pct": 0.050, "cvr": 0},
                        {"country": "Singapore", "sessions": 160, "revenue": 0, "purchases": 0, "pct": 0.040, "cvr": 0},
                        {"country": "Malaysia", "sessions": 120, "revenue": 0, "purchases": 0, "pct": 0.030, "cvr": 0},
                        {"country": "Australia", "sessions": 100, "revenue": 0, "purchases": 0, "pct": 0.025, "cvr": 0},
                        {"country": "Other", "sessions": 365, "revenue": 0, "purchases": 0, "pct": 0.092, "cvr": 0},
                    ],
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
        "kol": {
            "status": "ok",
            "data": {
                "kol": {
                    "tasks_created": 0, "qualified": 0, "sent": 0,
                    "replied": 0, "intent_replies": 0, "decline": 0,
                    "interest_rate": 0, "reply_rate": 0, "decline_rate": 0,
                },
                "editor": {
                    "tasks_created": 0, "qualified": 0, "sent": 0,
                    "replied": 0, "intent_replies": 0, "decline": 0,
                    "interest_rate": 0, "reply_rate": 0, "decline_rate": 0,
                },
                "topic_pool": {
                    "candidates_total": 25, "waiting": 20,
                    "by_status": {"待选题": 20, "已写双站": 4, "已写 PK": 1, "已写 FL": 0},
                    "new_this_week": 0, "consumed_this_week": 0,
                    "low_stock_alert": False,
                },
            },
        },
        "complaints": {"status": "stub", "data": {}},
        "n8n": {
            "status": "ok",
            "data": {
                "workflows": {
                    "ee779GzBI8Bj4Bx3": {
                        "name": "SEO 新闻稿 (双站)", "expected_per_week": 12,
                        "in_window": {"success": 1, "error": 0, "crashed": 0, "total": 1},
                        "last_run": "2026-04-29T01:00:00Z", "health": "degraded",
                    },
                    "bxqthAOVFjGviUEr": {
                        "name": "SEO 商业意图", "expected_per_week": 2,
                        "in_window": {"success": 0, "error": 0, "crashed": 0, "total": 0},
                        "last_run": None, "health": "stale",
                    },
                    "PEzTmqGwOqcHOPfc": {
                        "name": "SEO 周报", "expected_per_week": 1,
                        "in_window": {"success": 0, "error": 0, "crashed": 0, "total": 0},
                        "last_run": None, "health": "stale",
                    },
                    "xLEIAVos3YmynRsq": {
                        "name": "竞品 Gap 扫描", "expected_per_week": 1,
                        "in_window": {"success": 0, "error": 0, "crashed": 0, "total": 0},
                        "last_run": None, "health": "stale",
                    },
                    "9gMvXqs3mjS1zBZJ": {
                        "name": "PSI 月度审计", "expected_per_week": 0,
                        "in_window": {"success": 0, "error": 0, "crashed": 0, "total": 0},
                        "last_run": None, "health": "healthy",
                    },
                    "z8OmSc1gWqc9cnsH": {
                        "name": "GSC 排名追踪", "expected_per_week": 0,
                        "in_window": {"success": 0, "error": 0, "crashed": 0, "total": 0},
                        "last_run": None, "health": "healthy",
                    },
                },
                "summary": {
                    "total_runs": 1, "total_errors": 0, "error_rate": 0,
                    "stale_workflows": [
                        {"id": "bxqthAOVFjGviUEr", "name": "SEO 商业意图"},
                        {"id": "PEzTmqGwOqcHOPfc", "name": "SEO 周报"},
                        {"id": "xLEIAVos3YmynRsq", "name": "竞品 Gap 扫描"},
                    ],
                },
            },
        },
        "psi": {
            "status": "ok",
            "data": {
                "pages": [
                    {"name": "Powkong Home", "url": "https://powkong.com/",
                     "scores": {"performance": 46, "accessibility": 92, "best_practices": 73, "seo": 92},
                     "metrics": {"lcp_ms": 17548, "tbt_ms": 583, "cls": 0.0, "fcp_ms": 3729, "si_ms": 7337, "inp_ms": 0},
                     "opportunities_top3": [
                         {"id": "unused-js", "title": "减少未使用的 JavaScript", "savings_ms": 4000, "description": ""},
                         {"id": "minify-js", "title": "压缩 JavaScript", "savings_ms": 300, "description": ""},
                     ]},
                    {"name": "Powkong Blog", "url": "https://powkong.com/blogs/news",
                     "scores": {"performance": 47, "accessibility": 95, "best_practices": 73, "seo": 92},
                     "metrics": {"lcp_ms": 14629, "tbt_ms": 583, "cls": 0.0, "fcp_ms": 3702, "si_ms": 6873, "inp_ms": 0},
                     "opportunities_top3": [
                         {"id": "unused-js", "title": "减少未使用的 JavaScript", "savings_ms": 3260, "description": ""},
                     ]},
                    {"name": "Funlab Home", "url": "https://funlabswitch.com/",
                     "scores": {"performance": 52, "accessibility": 79, "best_practices": 92, "seo": 85},
                     "metrics": {"lcp_ms": 4502, "tbt_ms": 1021, "cls": 0.0, "fcp_ms": 2300, "si_ms": 7464, "inp_ms": 0},
                     "opportunities_top3": []},
                    {"name": "Funlab Blog", "url": "https://funlabswitch.com/blogs/news",
                     "scores": {"performance": 62, "accessibility": 81, "best_practices": 96, "seo": 100},
                     "metrics": {"lcp_ms": 4052, "tbt_ms": 875, "cls": 0.02, "fcp_ms": 2299, "si_ms": 4156, "inp_ms": 0},
                     "opportunities_top3": []},
                ],
                "summary": {
                    "performance_avg": 51.8,
                    "red_flags": [{"page": "Powkong Home", "category": "performance", "score": 46},
                                  {"page": "Powkong Blog", "category": "performance", "score": 47}],
                },
            },
        },
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

## 08. 下周任务清单

**Powkong · Meta 广告**
- ▲ 4/28、4/29、5/3 三天零转化 — 分析受众/素材，A/B 测试
- ◈ 综合 ROAS 1.21 低于盈亏，CPA $84.27 偏高，需重审定向
- ✦ 5/1 ROAS 3.31 是亮点，复盘成功因素，预算向高 ROAS 日倾斜

**Powkong · SEO & 性能**
- ▲ LCP 17 秒严重 — 紧急减少未使用的 JavaScript
- ◈ 49 个未收录页面：按 5 类原因逐一排查
- ◉ 2 个高展现低 CTR 关键词：优化 Title 和 Meta Description

**FUNLAB · 数据 + 收录**
- ▲ 817 页未收录 (收录率仅 26.3%) — 远低于 PK，逐页 URL Inspect
- ◈ GA4 vs Shopline 收入差 73% — 检查像素 + 归因配置

**SEO 产能 + 改造闭环**
- ▲ 本周 SEO 仅 1 篇 vs 目标 14 篇 (-93%) — 检查 n8n cron 触发
- ◈ 选题池 20 待选未消费 — 工作流恢复后立即降库存
- ◉ 客诉 Zoho service@ OAuth 待配 — 双框架 KPI 待激活
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

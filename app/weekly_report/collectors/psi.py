"""Lighthouse PageSpeed collector.

模块 L · 周报新增模块（Lighthouse 性能监控轻量版）.

数据源: Google PSI API (Google Cloud project claude-analytics-489703)
- env PSI_API_KEY (限制为只能调 PSI, 免费 25000 次/天)

监控 4 个核心页 × 4 维度 (Performance / Accessibility / Best Practices / SEO).
Performance 子项权重 (2024+): LCP 25 / TBT 25 / CLS 25 / FCP 10 / SI 10 / INP.

性能阈值:
- LCP: < 2.5s 优 / 2.5-4s 良 / > 4s 差
- TBT: < 200ms 优 / 200-600ms 良 / > 600ms 差
- CLS: < 0.1 优 / 0.1-0.25 良 / > 0.25 差
- FCP: < 1.8s 优 / 1.8-3s 良 / > 3s 差
- SI:  < 3.4s 优 / 3.4-5.8s 良 / > 5.8s 差
- INP: < 200ms 优 / 200-500ms 良 / > 500ms 差

输出 collected.psi.data:
{
  "pages": [
    {
      "url": "https://powkong.com/",
      "name": "Powkong Home",
      "scores": {  # 0-100
        "performance": int, "accessibility": int,
        "best_practices": int, "seo": int,
      },
      "metrics": {  # numeric raw values
        "lcp_ms": float, "tbt_ms": float, "cls": float,
        "fcp_ms": float, "si_ms": float, "inp_ms": float,
      },
      "opportunities_top3": [
        {"id": ..., "title": ..., "savings_ms": ..., "description": ...},
      ],
    },
    ... 4 页
  ],
  "summary": {
    "performance_avg": float,
    "red_flags": [...]   # 任一维度 < 50 的页面
  },
}
"""
import asyncio
import logging
import os

import httpx

log = logging.getLogger("weekly_report.psi")

PSI_ENDPOINT = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"
TIMEOUT = 90.0  # PSI 真跑 ~30s/页, 留余地

PAGES = [
    ("Powkong Home", "https://powkong.com/"),
    ("Powkong Blog", "https://powkong.com/blogs/news"),
    ("Funlab Home", "https://funlabswitch.com/"),
    ("Funlab Blog", "https://funlabswitch.com/blogs/news"),
]

CATEGORIES = ["performance", "accessibility", "best-practices", "seo"]

# Performance 6 子项 audit IDs
PERF_AUDITS = {
    "lcp_ms": "largest-contentful-paint",
    "tbt_ms": "total-blocking-time",
    "cls": "cumulative-layout-shift",
    "fcp_ms": "first-contentful-paint",
    "si_ms": "speed-index",
    "inp_ms": "interaction-to-next-paint",
}


def _get_key():
    k = os.environ.get("PSI_API_KEY", "").strip()
    if not k:
        raise RuntimeError("PSI_API_KEY env 未设")
    return k


async def _fetch_page(name: str, url: str, key: str) -> dict:
    params = [("url", url), ("strategy", "mobile"), ("key", key)]
    for cat in CATEGORIES:
        params.append(("category", cat.upper()))

    async with httpx.AsyncClient(timeout=TIMEOUT) as cli:
        for attempt in range(3):
            r = await cli.get(PSI_ENDPOINT, params=params)
            if r.status_code == 429:
                await asyncio.sleep(30)
                continue
            r.raise_for_status()
            return _parse_psi_response(name, url, r.json())
    return {"name": name, "url": url, "error": "PSI 429 retry exhausted"}


def _parse_psi_response(name: str, url: str, resp: dict) -> dict:
    lhr = resp.get("lighthouseResult") or {}
    cats = lhr.get("categories") or {}
    audits = lhr.get("audits") or {}

    scores = {
        "performance": int(round((cats.get("performance", {}).get("score") or 0) * 100)),
        "accessibility": int(round((cats.get("accessibility", {}).get("score") or 0) * 100)),
        "best_practices": int(round((cats.get("best-practices", {}).get("score") or 0) * 100)),
        "seo": int(round((cats.get("seo", {}).get("score") or 0) * 100)),
    }

    # Performance 子项 numeric values
    metrics = {}
    for k, audit_id in PERF_AUDITS.items():
        a = audits.get(audit_id) or {}
        metrics[k] = round(float(a.get("numericValue") or 0), 2)

    # Top 3 opportunities (按 overallSavingsMs 降序)
    opps = []
    for aid, audit in audits.items():
        details = audit.get("details") or {}
        if details.get("type") == "opportunity":
            savings = details.get("overallSavingsMs") or 0
            if savings > 0:
                opps.append({
                    "id": aid,
                    "title": audit.get("title", ""),
                    "savings_ms": int(savings),
                    "description": (audit.get("description") or "")[:200],
                })
    opps.sort(key=lambda x: -x["savings_ms"])

    return {
        "name": name,
        "url": url,
        "scores": scores,
        "metrics": metrics,
        "opportunities_top3": opps[:3],
    }


def _summarize(pages: list) -> dict:
    valid = [p for p in pages if "scores" in p]
    if not valid:
        return {"performance_avg": 0, "red_flags": [], "note": "all pages errored"}

    perf_avg = round(sum(p["scores"]["performance"] for p in valid) / len(valid), 1)
    red = []
    for p in valid:
        for cat, score in p["scores"].items():
            if score < 50:
                red.append({"page": p["name"], "category": cat, "score": score})
    return {"performance_avg": perf_avg, "red_flags": red}


async def collect(start_date, end_date) -> dict:
    """Lighthouse 4 核心页 × 4 维度. start/end_date 仅用于 metadata, PSI 始终是即时跑."""
    log.info("psi.collect %s ~ %s", start_date, end_date)
    try:
        key = _get_key()
        # 4 页并发 (注意: PSI ~30s/页, 4 并发实际墙钟 ~30-40s)
        pages = await asyncio.gather(
            *[_fetch_page(name, url, key) for name, url in PAGES],
            return_exceptions=True,
        )
        normalized = []
        for (name, url), p in zip(PAGES, pages):
            if isinstance(p, Exception):
                normalized.append({"name": name, "url": url,
                                    "error": f"{type(p).__name__}: {p}"})
            else:
                normalized.append(p)

        return {
            "status": "ok",
            "data": {
                "pages": normalized,
                "summary": _summarize(normalized),
                "window": f"{start_date}~{end_date}",
                "note": "PSI 是即时跑, 不按时间窗口区分",
            },
        }
    except Exception as e:
        log.exception("psi collect outer fail")
        return {"status": "error", "error": f"{type(e).__name__}: {e}", "data": {}}


if __name__ == "__main__":
    import datetime as dt
    today = dt.date.today()
    last_sun = today - dt.timedelta(days=today.weekday() + 1)
    last_mon = last_sun - dt.timedelta(days=6)
    print(asyncio.run(collect(last_mon, last_sun)))

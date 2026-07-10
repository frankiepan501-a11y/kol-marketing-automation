"""External company discovery for the B2B LinkedIn candidate pool.

This module feeds company-level candidates only. It does not write LinkedIn
lead-pool rows, send cards, touch LinkedIn, or write CRM customer records.
"""
import json
import os
import re
from collections import Counter
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

import httpx

from . import b2b_linkedin_auto_pool as pool

BJ = timezone(timedelta(hours=8))

DEFAULT_QUERY_PACKS = [
    {
        "market": "United States",
        "query": '"Nintendo Switch accessories" distributor wholesale',
        "country": "United States",
        "company_type": "分销商",
        "channels": ["分销", "本地电商"],
        "category": "Nintendo Switch gaming accessories distributor",
    },
    {
        "market": "United States",
        "query": '"video game distributor" "gaming accessories" USA',
        "country": "United States",
        "company_type": "分销商",
        "channels": ["分销"],
        "category": "video game and gaming accessories distribution",
    },
    {
        "market": "United Kingdom",
        "query": '"Nintendo Switch accessories" "UK" distributor retailer',
        "country": "United Kingdom",
        "company_type": "零售商",
        "channels": ["本地电商", "线下连锁"],
        "category": "Nintendo Switch and gaming accessories retail",
    },
    {
        "market": "United Kingdom",
        "query": '"gaming accessories" wholesale "United Kingdom"',
        "country": "United Kingdom",
        "company_type": "批发商",
        "channels": ["批发", "分销"],
        "category": "gaming accessories wholesale",
    },
    {
        "market": "Germany",
        "query": '"gaming accessories" distributor Germany Nintendo',
        "country": "Germany",
        "company_type": "分销商",
        "channels": ["分销", "本地电商"],
        "category": "gaming and console accessories distribution",
    },
    {
        "market": "France",
        "query": '"accessoires Nintendo Switch" distributeur grossiste France',
        "country": "France",
        "company_type": "分销商",
        "channels": ["分销", "本地电商"],
        "category": "Nintendo Switch and video game accessories distribution",
    },
    {
        "market": "Spain",
        "query": '"accesorios Nintendo Switch" distribuidor mayorista España',
        "country": "Spain",
        "company_type": "分销商",
        "channels": ["分销", "本地电商"],
        "category": "Nintendo Switch and gaming accessories distribution",
    },
    {
        "market": "United Arab Emirates",
        "query": '"gaming accessories" distributor UAE Nintendo',
        "country": "United Arab Emirates",
        "company_type": "分销商",
        "channels": ["分销", "线下连锁"],
        "category": "gaming accessories distributor in Middle East",
    },
    {
        "market": "Saudi Arabia",
        "query": '"gaming accessories" distributor Saudi Arabia Nintendo',
        "country": "Saudi Arabia",
        "company_type": "分销商",
        "channels": ["分销", "线下连锁"],
        "category": "gaming accessories distributor in Saudi Arabia",
    },
    {
        "market": "Australia",
        "query": '"Nintendo Switch accessories" distributor Australia',
        "country": "Australia",
        "company_type": "分销商",
        "channels": ["分销", "本地电商"],
        "category": "Nintendo Switch gaming accessories distributor",
    },
    {
        "market": "New Zealand",
        "query": '"gaming accessories" distributor New Zealand Nintendo',
        "country": "New Zealand",
        "company_type": "分销商",
        "channels": ["分销", "本地电商"],
        "category": "gaming and console accessories distribution",
    },
    {
        "market": "Japan",
        "query": '"Nintendo Switch accessories" distributor Japan retail',
        "country": "Japan",
        "company_type": "分销商",
        "channels": ["分销", "本地电商"],
        "category": "Nintendo Switch and console accessories distribution",
    },
    {
        "market": "Thailand",
        "query": '"Nintendo Switch accessories" distributor Thailand gaming',
        "country": "Thailand",
        "company_type": "分销商",
        "channels": ["分销", "本地电商", "线下连锁"],
        "category": "Nintendo Switch and gaming accessories distribution",
    },
    {
        "market": "Singapore",
        "query": '"gaming accessories" distributor Singapore Nintendo',
        "country": "Singapore",
        "company_type": "分销商",
        "channels": ["分销", "本地电商"],
        "category": "gaming accessories distributor",
    },
    {
        "market": "Malaysia",
        "query": '"gaming accessories" distributor Malaysia Nintendo',
        "country": "Malaysia",
        "company_type": "分销商",
        "channels": ["分销", "本地电商", "线下连锁"],
        "category": "gaming accessories distributor",
    },
    {
        "market": "Philippines",
        "query": '"Nintendo Switch accessories" distributor Philippines gaming',
        "country": "Philippines",
        "company_type": "分销商",
        "channels": ["分销", "本地电商", "线下连锁"],
        "category": "Nintendo Switch and gaming accessories distribution",
    },
    {
        "market": "Indonesia",
        "query": '"gaming accessories" distributor Indonesia Nintendo',
        "country": "Indonesia",
        "company_type": "分销商",
        "channels": ["分销", "本地电商"],
        "category": "gaming accessories distributor",
    },
    {
        "market": "Vietnam",
        "query": '"gaming accessories" distributor Vietnam Nintendo',
        "country": "Vietnam",
        "company_type": "分销商",
        "channels": ["分销", "本地电商", "线下连锁"],
        "category": "gaming accessories distributor",
    },
    {
        "market": "Canada",
        "query": '"Nintendo Switch accessories" distributor Canada',
        "country": "Canada",
        "company_type": "分销商",
        "channels": ["分销", "本地电商"],
        "category": "Nintendo Switch gaming accessories distributor",
    },
    {
        "market": "Mexico",
        "query": '"accesorios Nintendo Switch" distribuidor Mexico',
        "country": "Mexico",
        "company_type": "分销商",
        "channels": ["分销", "本地电商"],
        "category": "Nintendo Switch gaming accessories distributor",
    },
    {
        "market": "Brazil",
        "query": '"acessórios Nintendo Switch" distribuidor Brasil',
        "country": "Brazil",
        "company_type": "分销商",
        "channels": ["分销", "本地电商"],
        "category": "Nintendo Switch gaming accessories distributor",
    },
]

BLOCKED_DOMAIN_SUFFIXES = (
    "linkedin.com", "facebook.com", "instagram.com", "twitter.com", "x.com",
    "youtube.com", "youtu.be", "tiktok.com", "pinterest.com", "reddit.com",
    "wikipedia.org", "wikidata.org", "medium.com", "blogspot.com",
    "wordpress.com", "amazon.com", "amazon.co.uk", "amazon.de", "amazon.fr",
    "amazon.es", "amazon.it", "amazon.ae", "amazon.sa", "ebay.com",
    "ebay.co.uk", "ebay.com.au", "shopee.com", "shopee.sg", "tokopedia.com",
    "lazada.com", "aliexpress.com", "alibaba.com", "1688.com", "etsy.com",
    "glassdoor.com", "indeed.com", "crunchbase.com", "zoominfo.com",
    "rocketreach.co", "apollo.io", "snov.io", "hunter.io", "myshopify.com",
)

BLOCKED_TITLE_TERMS = (
    "job", "jobs", "career", "careers", "hiring", "coupon", "promo code",
    "review", "news", "press release", "reddit", "wikipedia", "amazon.com",
)

DISCOVERY_LAST_RUN: dict = {}


def get_last_run() -> dict:
    return dict(DISCOVERY_LAST_RUN)


def _now_bj() -> datetime:
    return datetime.now(BJ)


def _int_env(name: str, default: int, min_value: int, max_value: int) -> int:
    try:
        value = int(os.environ.get(name, str(default)) or default)
    except Exception:
        value = default
    return max(min_value, min(value, max_value))


def _load_query_packs() -> list[dict]:
    raw = os.environ.get("B2B_DISCOVERY_QUERY_PACK_JSON", "").strip()
    if raw:
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                return [x for x in parsed if isinstance(x, dict) and x.get("query")]
        except Exception as exc:
            print(f"[b2b_linkedin_discovery] bad B2B_DISCOVERY_QUERY_PACK_JSON: {exc}")
    return list(DEFAULT_QUERY_PACKS)


def _filter_query_packs(market: str) -> list[dict]:
    packs = _load_query_packs()
    packs = pool._sort_seeds_by_market_priority(packs)
    market_key = (market or "").strip().lower()
    if not market_key:
        return packs
    filtered = []
    for pack in packs:
        haystack = " ".join([
            str(pack.get("market") or ""),
            str(pack.get("country") or ""),
            str(pack.get("query") or ""),
        ]).lower()
        if market_key in haystack:
            filtered.append(pack)
    return filtered


def _google_provider() -> str:
    return (os.environ.get("GOOGLE_SEARCH_PROVIDER") or "custom_search").strip().lower()


def _manual_results() -> list[dict]:
    raw = os.environ.get("B2B_DISCOVERY_MANUAL_RESULTS_JSON", "").strip()
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except Exception as exc:
        raise RuntimeError(f"B2B_DISCOVERY_MANUAL_RESULTS_JSON invalid: {exc}")
    if not isinstance(parsed, list):
        raise RuntimeError("B2B_DISCOVERY_MANUAL_RESULTS_JSON must be a list")
    return [x for x in parsed if isinstance(x, dict)]


async def _search_custom_search(query: str, *, num: int) -> list[dict]:
    key = os.environ.get("GOOGLE_SEARCH_API_KEY", "").strip()
    cx = os.environ.get("GOOGLE_SEARCH_ENGINE_ID", "").strip() or os.environ.get("GOOGLE_SEARCH_CX", "").strip()
    if not key or not cx:
        raise RuntimeError("GOOGLE_SEARCH_API_KEY/GOOGLE_SEARCH_ENGINE_ID 未配置")
    params = {"key": key, "cx": cx, "q": query, "num": max(1, min(num, 10))}
    async with httpx.AsyncClient(timeout=30.0) as cli:
        resp = await cli.get("https://www.googleapis.com/customsearch/v1", params=params)
        resp.raise_for_status()
        data = resp.json()
    return [
        {
            "title": item.get("title") or "",
            "link": item.get("link") or "",
            "snippet": item.get("snippet") or "",
            "provider": "custom_search",
            "raw": item,
        }
        for item in data.get("items") or []
        if isinstance(item, dict)
    ]


async def _search_serpapi(query: str, *, num: int) -> list[dict]:
    key = os.environ.get("SERPAPI_API_KEY", "").strip() or os.environ.get("GOOGLE_SEARCH_API_KEY", "").strip()
    if not key:
        raise RuntimeError("SERPAPI_API_KEY 未配置")
    params = {"engine": "google", "q": query, "api_key": key, "num": max(1, min(num, 20))}
    async with httpx.AsyncClient(timeout=30.0) as cli:
        resp = await cli.get("https://serpapi.com/search.json", params=params)
        resp.raise_for_status()
        data = resp.json()
    return [
        {
            "title": item.get("title") or "",
            "link": item.get("link") or "",
            "snippet": item.get("snippet") or "",
            "provider": "serpapi",
            "raw": item,
        }
        for item in data.get("organic_results") or []
        if isinstance(item, dict)
    ]


async def _search_items(query: str, *, num: int, provider: str) -> list[dict]:
    provider = (provider or _google_provider()).strip().lower()
    if provider == "manual":
        return _manual_results()
    if provider == "serpapi":
        return await _search_serpapi(query, num=num)
    if provider in {"custom_search", "cse", "google"}:
        return await _search_custom_search(query, num=num)
    raise RuntimeError(f"unsupported GOOGLE_SEARCH_PROVIDER={provider}")


def _domain(value: str) -> str:
    return pool._domain_of(value)


def _domain_blocked(domain: str, *, title: str = "", link: str = "", snippet: str = "") -> str:
    domain = (domain or "").lower().strip()
    if not domain:
        return "missing_domain"
    for suffix in BLOCKED_DOMAIN_SUFFIXES:
        if domain == suffix or domain.endswith("." + suffix):
            return "blocked_platform_domain"
    text = " ".join([title or "", link or "", snippet or ""]).lower()
    if any(term in text for term in BLOCKED_TITLE_TERMS):
        return "blocked_low_intent_page"
    return ""


def _company_from_title(title: str, domain: str) -> str:
    title = re.sub(r"\s+", " ", (title or "")).strip()
    if title:
        first = re.split(r"\s+[|:–-]\s+", title, maxsplit=1)[0].strip()
        first = re.sub(r"\b(official site|homepage|home|nintendo switch accessories|gaming accessories|wholesale|distributor)\b", "", first, flags=re.I)
        first = re.sub(r"\s+", " ", first).strip(" -|:")
        if 2 <= len(first) <= 80 and not re.search(r"\b(best|top|near me|review|coupon|amazon)\b", first, flags=re.I):
            return first
    base = re.sub(r"^www\.", "", domain or "")
    base = re.sub(r"\.(com|net|org|io|co|co\.uk|com\.au|co\.nz|de|fr|es|it|nl|dk|se|no|fi|jp|sg|ca|mx|br|ae|sa)$", "", base, flags=re.I)
    return re.sub(r"[-_]+", " ", base).title().strip()


def _result_to_seed(item: dict, pack: dict) -> tuple[dict | None, str]:
    link = str(item.get("link") or item.get("url") or item.get("website") or "").strip()
    title = str(item.get("title") or item.get("company") or "").strip()
    snippet = str(item.get("snippet") or item.get("description") or "").strip()
    domain = _domain(link or str(item.get("domain") or ""))
    blocked = _domain_blocked(domain, title=title, link=link, snippet=snippet)
    if blocked:
        return None, blocked
    company = str(item.get("company") or "").strip() or _company_from_title(title, domain)
    if not company:
        return None, "missing_company"
    source_query = str(pack.get("query") or item.get("query") or "").strip()
    provider = str(item.get("provider") or "search").strip()
    note_parts = [
        f"外部搜索补给 provider={provider}",
        f"query={source_query}" if source_query else "",
        f"title={title}" if title else "",
        f"snippet={snippet[:220]}" if snippet else "",
    ]
    return {
        "company": company,
        "domain": domain,
        "website": link or domain,
        "country": str(item.get("country") or pack.get("country") or pack.get("market") or "").strip(),
        "company_type": str(item.get("company_type") or pack.get("company_type") or "待判断").strip(),
        "channels": item.get("channels") or pack.get("channels") or [],
        "category": str(item.get("category") or pack.get("category") or "gaming accessories").strip(),
        "source": "Google搜索补给",
        "candidate_source": "搜索补给",
        "notes": "；".join([x for x in note_parts if x]),
        "_search_title": title,
        "_search_snippet": snippet,
        "_search_query": source_query,
        "_search_provider": provider,
    }, ""


def _score_discovered_lead(lead: dict, seed: dict, *, snov_count: int, snov_status: str) -> dict:
    base = pool._score_lead(lead)
    score = int(base.get("score") or 0)
    reasons = list(base.get("reasons") or [])
    haystack = " ".join([
        seed.get("_search_title") or "",
        seed.get("_search_snippet") or "",
        seed.get("_search_query") or "",
        lead.get("category") or "",
        lead.get("company_type") or "",
        " ".join(lead.get("channels") or []),
    ]).lower()
    if re.search(r"\b(distributor|wholesale|importer|reseller|supplier|grossiste|mayorista)\b", haystack):
        score += 8
        reasons.append("搜索结果含分销、批发、进口或供货信号")
    if re.search(r"\b(nintendo|switch|gaming|video game|console|controller|accessor)", haystack):
        score += 5
        reasons.append("搜索结果含游戏、主机或配件信号")
    if snov_count > 0:
        score += min(10, 5 + snov_count)
        reasons.append(f"Snov 返回 {snov_count} 个公司联系人候选")
    elif snov_status == "无结果":
        reasons.append("Snov 暂无联系人，保留公司级候选")
    score = min(score, 100)
    if score >= 75:
        return {"score": score, "icp": "是", "grade": "A-优先开发", "reasons": reasons}
    if score >= 55:
        return {"score": score, "icp": "待判断", "grade": "B-可开发", "reasons": reasons}
    return {"score": score, "icp": "否", "grade": "C-低优先", "reasons": reasons}


async def _validate_snov(domain: str, *, enabled: bool) -> tuple[str, str, int]:
    if not enabled or not domain:
        return "未查询", "", 0
    try:
        prospects, summary = await pool._snov_prospects(domain, max_prospects=5)
        count = len(prospects)
        top_roles = []
        for p in prospects[:5]:
            name = " ".join([str(p.get("first_name") or p.get("firstName") or "").strip(), str(p.get("last_name") or p.get("lastName") or "").strip()]).strip()
            title = str(p.get("position") or p.get("job_title") or p.get("title") or "").strip()
            if name or title:
                top_roles.append(f"{name} / {title}".strip(" /"))
        compact = json.dumps({
            "domain": domain,
            "prospects": count,
            "top_roles": top_roles,
            "source_summary": summary,
        }, ensure_ascii=False)
        return ("查询成功" if count else "无结果"), compact[:1200], count
    except Exception as exc:
        return "查询失败", f"{type(exc).__name__}: {str(exc)[:300]}", 0


async def _candidate_indexes() -> tuple[Counter, set[str], set[str]]:
    status_counts = Counter()
    domains = set()
    company_keys = set()
    for rec in await pool._list_candidate_records():
        fields = rec.get("fields") or {}
        status = pool._text(fields.get("候选状态")) or "空"
        status_counts[status] += 1
        key = pool._candidate_key_from_fields(fields)
        domain = _domain(key)
        if domain:
            domains.add(domain)
        company_key = pool._text_key(pool._text(fields.get("公司名称")))
        if company_key:
            company_keys.add(company_key)
        if key and not domain:
            company_keys.add(key)
    return status_counts, domains, company_keys


def _pending_total(status_counts: Counter) -> int:
    return sum(count for status, count in status_counts.items() if status in {"空", "待入池", ""})


async def run(
    *,
    provider: str = "all",
    commit: bool = False,
    limit: int = 0,
    pending_target: int = 0,
    market: str = "",
    min_score: int = 0,
) -> dict:
    provider = (provider or "all").strip().lower()
    if provider not in {"all", "google", "snov"}:
        raise ValueError("provider must be all, google, or snov")
    pending_target = max(1, min(int(pending_target or _int_env("B2B_DISCOVERY_PENDING_TARGET", 200, 1, 5000)), 5000))
    daily_limit = _int_env("B2B_DISCOVERY_DAILY_CREATE_LIMIT", 100, 1, 1000)
    min_score = max(0, min(int(min_score or _int_env("B2B_DISCOVERY_MIN_SCORE", 55, 0, 100)), 100))
    requested_limit = int(limit or daily_limit)
    started_at = _now_bj().strftime("%Y-%m-%d %H:%M:%S")
    batch = "external-discovery-" + _now_bj().strftime("%Y%m%d-%H%M")

    candidate_status_counts, candidate_domains, candidate_company_keys = await _candidate_indexes()
    pending = _pending_total(candidate_status_counts)
    gap = max(0, pending_target - pending)
    effective_limit = max(0, min(requested_limit, daily_limit, gap))
    low_water_alert = pending < _int_env("B2B_DISCOVERY_LOW_WATER_ALERT", 80, 1, 5000)

    base_result = {
        "commit": commit,
        "provider": provider,
        "google_search_provider": _google_provider(),
        "started_at_bj": started_at,
        "batch": batch,
        "candidate_table": pool.B2B_LINKEDIN_CANDIDATE_TABLE,
        "candidate_pending_total": pending,
        "candidate_status_counts": dict(candidate_status_counts),
        "pending_target": pending_target,
        "daily_create_limit": daily_limit,
        "requested_limit": requested_limit,
        "effective_limit": effective_limit,
        "low_water_alert": low_water_alert,
    }
    if effective_limit <= 0:
        result = {
            **base_result,
            "waterline_status": "skip_target_met",
            "raw_results": 0,
            "normalized_domains": 0,
            "planned_candidates": 0,
            "created_candidates": 0,
            "snov_available_candidates": 0,
            "created": [],
            "planned_preview": [],
            "skip_reasons": {"candidate_pool_target_met": 1},
            "provider_errors": [],
        }
        DISCOVERY_LAST_RUN.clear()
        DISCOVERY_LAST_RUN.update(result)
        return result

    query_packs = _filter_query_packs(market)
    max_results_per_query = _int_env("B2B_DISCOVERY_MAX_RESULTS_PER_QUERY", 10, 1, 20)
    search_provider = _google_provider()
    raw_items: list[dict] = []
    provider_errors = []

    if provider in {"all", "google"}:
        if search_provider == "manual":
            try:
                items = await _search_items("", num=max_results_per_query, provider=search_provider)
                for item in items:
                    item["_query_pack"] = {}
                    item["query"] = str(item.get("query") or "")
                raw_items.extend(items)
            except Exception as exc:
                provider_errors.append({"provider": search_provider, "query": "", "error": f"{type(exc).__name__}: {str(exc)[:240]}"})
        else:
            for pack in query_packs:
                if len(raw_items) >= effective_limit * 5:
                    break
                query = str(pack.get("query") or "").strip()
                if not query:
                    continue
                try:
                    items = await _search_items(query, num=max_results_per_query, provider=search_provider)
                    for item in items:
                        item["_query_pack"] = pack
                        item["query"] = query
                    raw_items.extend(items)
                except Exception as exc:
                    provider_errors.append({"provider": search_provider, "query": query, "error": f"{type(exc).__name__}: {str(exc)[:240]}"})
    elif provider == "snov":
        try:
            for item in _manual_results():
                item["_query_pack"] = {}
                raw_items.append(item)
        except Exception as exc:
            provider_errors.append({"provider": "manual", "query": "", "error": f"{type(exc).__name__}: {str(exc)[:240]}"})

    lead_domains, lead_company_keys, all_domains, all_company_keys = await pool._load_existing_keys()
    skip_reasons = Counter()
    planned = []
    created = []
    seen_domains = set()
    seen_company_keys = set()
    snov_available = 0
    normalized_domains = 0

    for item in raw_items:
        if len(planned) >= effective_limit:
            break
        pack = item.get("_query_pack") if isinstance(item.get("_query_pack"), dict) else {}
        seed, skip = _result_to_seed(item, pack)
        if skip:
            skip_reasons[skip] += 1
            continue
        assert seed is not None
        lead = pool._seed_to_lead(seed)
        domain = lead.get("domain") or _domain(lead.get("website") or "")
        company_key = pool._text_key(lead.get("company") or "")
        normalized_domains += 1
        if domain and domain in seen_domains:
            skip_reasons["duplicate_domain_this_run"] += 1
            continue
        if company_key and company_key in seen_company_keys:
            skip_reasons["duplicate_company_this_run"] += 1
            continue
        if domain:
            seen_domains.add(domain)
        if company_key:
            seen_company_keys.add(company_key)
        if domain and domain in candidate_domains:
            skip_reasons["duplicate_candidate_domain"] += 1
            continue
        if company_key and company_key in candidate_company_keys:
            skip_reasons["duplicate_candidate_company"] += 1
            continue
        if domain and domain in lead_domains:
            skip_reasons["duplicate_lead_pool_domain"] += 1
            continue
        if company_key and company_key in lead_company_keys:
            skip_reasons["duplicate_lead_pool_company"] += 1
            continue
        if domain and domain in all_domains:
            skip_reasons["duplicate_crm_domain"] += 1
            continue
        if company_key and company_key in all_company_keys:
            skip_reasons["duplicate_crm_company"] += 1
            continue

        snov_status, snov_summary, snov_count = await _validate_snov(domain, enabled=provider in {"all", "snov"})
        if snov_count > 0:
            snov_available += 1
        score = _score_discovered_lead(lead, seed, snov_count=snov_count, snov_status=snov_status)
        if int(score.get("score") or 0) < min_score or score.get("icp") == "否":
            skip_reasons["low_score"] += 1
            continue
        fields = pool._candidate_fields_for_seed(seed, lead, score, batch=batch)
        fields["Snov查询状态"] = snov_status
        if snov_summary:
            fields["Snov原始摘要"] = snov_summary
        planned.append({
            "company": lead.get("company"),
            "domain": domain,
            "score": score["score"],
            "grade": score["grade"],
            "snov_status": snov_status,
            "snov_prospects": snov_count,
            "source": fields.get("来源"),
            "fields": fields,
        })

    if commit and planned:
        record_ids = await pool._create_table_records(pool.B2B_LINKEDIN_CANDIDATE_TABLE, [row["fields"] for row in planned])
        for row, record_id in zip(planned, record_ids):
            created.append({
                "record_id": record_id,
                "company": row["company"],
                "domain": row["domain"],
                "score": row["score"],
                "snov_status": row["snov_status"],
            })

    result = {
        **base_result,
        "waterline_status": "refill_needed",
        "query_pack_count": len(query_packs),
        "raw_results": len(raw_items),
        "normalized_domains": normalized_domains,
        "planned_candidates": len(planned),
        "created_candidates": len(created),
        "snov_available_candidates": snov_available,
        "created": created,
        "planned_preview": [
            {k: row[k] for k in ["company", "domain", "score", "grade", "snov_status", "snov_prospects", "source"]}
            for row in planned[:30]
        ],
        "skip_reasons": dict(skip_reasons),
        "provider_errors": provider_errors[:20],
    }
    DISCOVERY_LAST_RUN.clear()
    DISCOVERY_LAST_RUN.update(result)
    return result

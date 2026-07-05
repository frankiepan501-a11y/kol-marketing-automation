"""B2B LinkedIn/Snov automatic lead-pool intake.

The job is intentionally deterministic: take a maintained seed list, enrich
contacts with Snov when possible, dedupe against CRM and the LinkedIn pool,
score with the same local rules as the original PowerShell scripts, then write
only qualified new leads into the LinkedIn lead pool.
"""
import json
import os
import re
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

import httpx

from . import config, feishu, snov

BJ = timezone(timedelta(hours=8))

B2B_APP_TOKEN = os.environ.get("B2B_CUSTOMER_APP_TOKEN", "E1kkbx1tVaJvQGsKf94cJG88nzb")
B2B_CRM_TABLE = os.environ.get("B2B_CUSTOMER_TABLE", "tbl2OoqVb7Uf1pWd")
B2B_LINKEDIN_TABLE = os.environ.get("B2B_LINKEDIN_TABLE", "tblN8XszEatuTJgP")

COMPANY_TYPE_OPTIONS = {"贸易商", "分销商", "品牌商", "批发商", "混合型", "游戏IP", "电商卖家", "电商平台", "行业协会", "零售商", "待判断"}
CHANNEL_OPTIONS = {"线下连锁", "独立店", "本地电商", "海外众筹", "商超", "EBAY", "虾皮", "Amazon", "分销"}

DEFAULT_SEEDS = [
    {"company": "Game Retail Limited", "domain": "game.co.uk", "country": "United Kingdom", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "video games and gaming accessories retail", "notes": "UK game retailer with console and accessory category"},
    {"company": "Smyths Toys", "domain": "smythstoys.com", "country": "Ireland", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "toys, video games, Nintendo Switch and console accessories", "notes": "EU/UK retail chain carrying Nintendo and gaming products"},
    {"company": "MediaMarkt", "domain": "mediamarkt.com", "country": "Germany", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "consumer electronics, gaming, console accessories", "notes": "European electronics retailer with gaming category"},
    {"company": "Saturn", "domain": "saturn.de", "country": "Germany", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "consumer electronics, gaming and console accessories", "notes": "German electronics retailer with console category"},
    {"company": "Coolblue", "domain": "coolblue.nl", "country": "Netherlands", "company_type": "电商平台", "channels": ["本地电商"], "category": "consumer electronics, gaming accessories and controllers", "notes": "Benelux ecommerce retailer"},
    {"company": "Bol.com", "domain": "bol.com", "country": "Netherlands", "company_type": "电商平台", "channels": ["本地电商"], "category": "marketplace, Nintendo Switch, gaming accessories", "notes": "Benelux marketplace with Switch accessories category"},
    {"company": "Fnac Darty", "domain": "fnac.com", "country": "France", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "video games, Nintendo Switch, console accessories", "notes": "France retail group with gaming category"},
    {"company": "LDLC", "domain": "ldlc.com", "country": "France", "company_type": "电商卖家", "channels": ["本地电商"], "category": "computer, gaming, console and PC accessories", "notes": "French ecommerce retailer with gaming hardware"},
    {"company": "Cdiscount", "domain": "cdiscount.com", "country": "France", "company_type": "电商平台", "channels": ["本地电商"], "category": "marketplace, video games, console accessories", "notes": "French marketplace with gaming products"},
    {"company": "PCComponentes", "domain": "pccomponentes.com", "country": "Spain", "company_type": "电商卖家", "channels": ["本地电商"], "category": "gaming, Nintendo Switch, controllers, accessories", "notes": "Spanish ecommerce retailer with gaming category"},
    {"company": "Worten", "domain": "worten.pt", "country": "Portugal", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "consumer electronics, gaming and console accessories", "notes": "Portugal electronics retailer"},
    {"company": "Caseking", "domain": "caseking.de", "country": "Germany", "company_type": "电商卖家", "channels": ["本地电商"], "category": "gaming hardware, controllers and accessories", "notes": "German gaming hardware ecommerce specialist"},
    {"company": "Alternate", "domain": "alternate.de", "country": "Germany", "company_type": "电商卖家", "channels": ["本地电商"], "category": "consumer electronics, gaming hardware and accessories", "notes": "German ecommerce retailer with gaming category"},
    {"company": "Proshop", "domain": "proshop.dk", "country": "Denmark", "company_type": "电商卖家", "channels": ["本地电商"], "category": "consumer electronics, gaming and Nintendo accessories", "notes": "Nordic ecommerce retailer"},
    {"company": "Elgiganten", "domain": "elgiganten.dk", "country": "Denmark", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "electronics, gaming, consoles and accessories", "notes": "Nordic electronics retailer"},
    {"company": "Webhallen", "domain": "webhallen.com", "country": "Sweden", "company_type": "电商卖家", "channels": ["本地电商"], "category": "gaming, consoles and accessories", "notes": "Swedish game and electronics ecommerce"},
    {"company": "NetOnNet", "domain": "netonnet.se", "country": "Sweden", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "consumer electronics, gaming products and accessories", "notes": "Nordic electronics retailer"},
    {"company": "Alza", "domain": "alza.cz", "country": "Czech Republic", "company_type": "电商平台", "channels": ["本地电商"], "category": "marketplace, gaming, Nintendo Switch accessories", "notes": "CEE ecommerce retailer with gaming category"},
    {"company": "Digitec Galaxus", "domain": "digitec.ch", "country": "Switzerland", "company_type": "电商平台", "channels": ["本地电商"], "category": "consumer electronics, gaming and console accessories", "notes": "Swiss ecommerce retailer"},
    {"company": "Brack", "domain": "brack.ch", "country": "Switzerland", "company_type": "电商卖家", "channels": ["本地电商"], "category": "consumer electronics, gaming, accessories", "notes": "Swiss ecommerce retailer"},
    {"company": "JB Hi-Fi", "domain": "jbhifi.com.au", "country": "Australia", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "consumer electronics, Nintendo Switch, gaming accessories", "notes": "Australia electronics retailer with gaming category"},
    {"company": "Harvey Norman", "domain": "harveynorman.com.au", "country": "Australia", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "consumer electronics and gaming accessories", "notes": "Australia retail chain"},
    {"company": "Takealot", "domain": "takealot.com", "country": "South Africa", "company_type": "电商平台", "channels": ["本地电商"], "category": "marketplace, gaming accessories, Nintendo Switch", "notes": "South Africa ecommerce marketplace"},
    {"company": "Virgin Megastore Middle East", "domain": "virginmegastore.me", "country": "United Arab Emirates", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "gaming, consoles, Nintendo Switch accessories", "notes": "Middle East retailer with gaming category"},
    {"company": "Sharaf DG", "domain": "sharafdg.com", "country": "United Arab Emirates", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "consumer electronics, gaming accessories", "notes": "UAE electronics retailer"},
    {"company": "Jarir Bookstore", "domain": "jarir.com", "country": "Saudi Arabia", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "electronics, gaming and console accessories", "notes": "Saudi retail chain with gaming products"},
    {"company": "Yodobashi Camera", "domain": "yodobashi.com", "country": "Japan", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "electronics, video games, Nintendo Switch accessories", "notes": "Japan electronics retailer"},
    {"company": "Bic Camera", "domain": "biccamera.com", "country": "Japan", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "electronics, gaming and Nintendo accessories", "notes": "Japan electronics retailer"},
    {"company": "Joshin", "domain": "joshinweb.jp", "country": "Japan", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "electronics, games, console accessories", "notes": "Japan retailer with video game category"},
    {"company": "Maxsoft", "domain": "maxsoftonline.com", "country": "Singapore", "company_type": "分销商", "channels": ["分销", "本地电商"], "category": "Nintendo and video game distribution", "notes": "Singapore game distributor adjacency"},
]

ROLE_TERMS = [
    "purchase", "purchasing", "buyer", "procurement", "sourcing", "achats", "acheteur",
    "compras", "zakup", "category", "product manager", "chef de produit", "product owner",
    "merchand", "business development", "partnership", "publishing", "export",
    "international sales", "bd", "ceo", "founder", "owner", "managing director",
    "general manager", "president", "director",
]

_SNOV_TOKEN = {"value": "", "ts": 0.0}
_LAST_RUN: dict = {}


def get_last_run() -> dict:
    return dict(_LAST_RUN)


def _now_bj() -> datetime:
    return datetime.now(BJ)


def _text(value) -> str:
    return str(feishu.ext(value) or "").strip()


def _url(value) -> str:
    return str(feishu.ext_url(value) or "").strip()


def _normalize_url(value: str) -> str:
    value = (value or "").strip()
    if value and not re.match(r"^https?://", value, flags=re.I):
        value = "https://" + value
    return value


def _domain_of(value: str) -> str:
    raw = (value or "").strip().lower()
    if not raw:
        return ""
    m = re.search(r"@([a-z0-9.-]+\.[a-z]{2,})", raw)
    if m:
        return re.sub(r"^www\.", "", m.group(1))
    raw = _normalize_url(raw)
    m = re.match(r"^https?://([^/]+)", raw)
    if not m:
        return ""
    return re.sub(r"^www\.", "", m.group(1).split(":")[0])


def _text_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").lower())


def _first_name(name: str) -> str:
    parts = [p for p in (name or "").strip().split() if p]
    return parts[0] if parts else "there"


def _has_any(text: str, terms: list[str]) -> bool:
    text = (text or "").lower()
    return any(term.lower() in text for term in terms)


def _score_lead(lead: dict) -> dict:
    score = 0
    reasons = []
    role = f"{lead.get('title', '')} {lead.get('contact', '')}".lower()
    context = " ".join([
        str(lead.get("company_type") or ""),
        " ".join(lead.get("channels") or []),
        str(lead.get("category") or ""),
        str(lead.get("competitors") or ""),
        str(lead.get("notes") or ""),
        str(lead.get("company") or ""),
    ]).lower()
    country = (lead.get("country") or "").upper()

    if _has_any(role, ["owner", "founder", "ceo", "president", "director", "general manager"]):
        score += 20
        reasons.append("联系人是老板或高层")
    elif _has_any(role, ["purchasing", "procurement", "buyer", "category", "sourcing", "business development", "product manager", "sales manager"]):
        score += 18
        reasons.append("联系人接近采购、品类或BD角色")
    elif lead.get("title"):
        score += 8
        reasons.append("联系人职位已知")

    if _has_any(context, ["distributor", "wholesale", "retail", "reseller", "importer", "trading", "分销", "批发", "零售", "贸易"]):
        score += 22
        reasons.append("公司类型接近分销、批发或零售")
    elif _has_any(context, ["ecommerce", "marketplace", "amazon", "shopify", "电商"]):
        score += 14
        reasons.append("公司有电商渠道信号")

    if _has_any(context, ["gaming", "game", "console", "nintendo", "switch", "playstation", "xbox", "controller", "accessories", "游戏", "手柄", "配件"]):
        score += 22
        reasons.append("主营或描述含游戏、主机或配件信号")

    if _has_any(context, ["8bitdo", "gamesir", "hori", "powera", "nacon", "turtle beach", "skull", "nyxi", "dobe", "iine", "gulikit"]):
        score += 12
        reasons.append("出现游戏配件竞品或相邻品牌")

    priority = {"US", "USA", "UNITED STATES", "UK", "UNITED KINGDOM", "DE", "GERMANY", "FR", "FRANCE", "ES", "SPAIN", "IT", "ITALY", "NL", "NETHERLANDS", "PL", "POLAND", "AE", "UAE", "SA", "SAUDI", "KW", "KUWAIT", "AU", "AUSTRALIA", "NZ", "JAPAN", "JP", "KOREA", "KR", "SG", "SINGAPORE"}
    if country in priority or any(x in country for x in priority):
        score += 10
        reasons.append("国家属于优先开发市场")
    elif lead.get("country"):
        score += 5
        reasons.append("国家信息完整")

    complete = 0
    if lead.get("website"):
        complete += 4
    if lead.get("linkedin_company") or lead.get("linkedin_profile"):
        complete += 4
    if lead.get("contact"):
        complete += 3
    if lead.get("title"):
        complete += 3
    if lead.get("country"):
        complete += 2
    score += min(complete, 10)
    if complete >= 8:
        reasons.append("线索关键字段较完整")

    score = min(score, 100)
    if score >= 75:
        return {"score": score, "icp": "是", "grade": "A-优先开发", "reasons": reasons}
    if score >= 55:
        return {"score": score, "icp": "待判断", "grade": "B-可开发", "reasons": reasons}
    return {"score": score, "icp": "否", "grade": "C-低优先", "reasons": reasons}


def _copy_for_lead(lead: dict, score: dict) -> dict:
    first = _first_name(lead.get("contact") or "")
    company = lead.get("company") or "your company"
    market = lead.get("country") or "your market"
    category = lead.get("category") or "gaming accessories"
    reason = "；".join(score.get("reasons") or []) or "字段不足，需要人工复核"

    connect = f"Hi {first}, I noticed {company} works around {category}. We make Nintendo Switch gaming accessories for distributors and retailers. Open to connect?"
    if len(connect) > 280:
        connect = f"Hi {first}, I saw {company} in gaming accessories/distribution. We make Switch accessories for retailers and distributors. Open to connect?"
    message = f"Thanks for connecting, {first}. Quick question: are you currently sourcing Switch or Switch 2 accessories for {market}? If yes, I can send a short line sheet and distributor pricing."
    email = (
        f"Subject: Switch accessories for {company}\r\n\r\n"
        f"Hi {first},\r\n\r\n"
        f"I found {company} while researching gaming accessories distributors and retailers in {market}.\r\n\r\n"
        "We make Nintendo Switch accessories such as controllers, docks, carrying cases, and related add-ons under FUNLAB and POWKONG. "
        "If this category fits your current sourcing plan, I can send a short line sheet with MOQ, pricing logic, and available samples.\r\n\r\n"
        "Would it be useful to compare a few SKUs for your channel?\r\n\r\n"
        "Best regards,\r\nFrankie"
    )
    cn_reason = f"{company} 符合现有 B2B 相似客户开发逻辑：{reason}。建议先 LinkedIn 连接，接受后用低压问题确认是否采购 Switch/游戏配件；未接受则转 email 或官网表单。"
    return {"connect": connect, "message": message, "email": email, "reason": cn_reason}


def _url_cell(url: str) -> dict | None:
    url = _normalize_url(url)
    return {"link": url, "text": url} if url else None


def _clean_channels(values) -> list[str]:
    if isinstance(values, str):
        values = re.split(r"[,;；、/]", values)
    out = []
    for value in values or []:
        value = str(value).strip()
        if value in CHANNEL_OPTIONS and value not in out:
            out.append(value)
    return out


def _load_seeds() -> list[dict]:
    raw = os.environ.get("B2B_LINKEDIN_AUTO_SEEDS_JSON", "").strip()
    if raw:
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                return [x for x in parsed if isinstance(x, dict)]
        except Exception as exc:
            print(f"[b2b_linkedin_auto_pool] bad B2B_LINKEDIN_AUTO_SEEDS_JSON: {exc}")
    return list(DEFAULT_SEEDS)


async def _list_records(table_id: str, *, field_names: list[str]) -> list[dict]:
    items = []
    page_token = ""
    encoded_fields = "&field_names=" + quote(json.dumps(field_names, ensure_ascii=False), safe="")
    while True:
        path = f"/bitable/v1/apps/{B2B_APP_TOKEN}/tables/{table_id}/records?page_size=500{encoded_fields}"
        if page_token:
            path += "&page_token=" + quote(page_token, safe="")
        resp = await feishu.api("GET", path, which="bitable")
        data = resp.get("data") or {}
        items.extend(data.get("items") or [])
        if not data.get("has_more"):
            break
        page_token = data.get("page_token") or ""
        if not page_token:
            break
    return items


async def _load_existing_keys() -> tuple[set[str], set[str], set[str], set[str]]:
    lead_domains = set()
    lead_company_keys = set()
    crm_domains = set()
    crm_company_keys = set()

    lead_fields = ["公司名称", "公司官网", "去重Key"]
    for rec in await _list_records(B2B_LINKEDIN_TABLE, field_names=lead_fields):
        fields = rec.get("fields") or {}
        domain = _domain_of(_text(fields.get("去重Key"))) or _domain_of(_url(fields.get("公司官网")))
        company_key = _text_key(_text(fields.get("公司名称")))
        if domain:
            lead_domains.add(domain)
        if company_key:
            lead_company_keys.add(company_key)

    crm_fields = ["公司名称", "公司官网", "邮箱", "LinkedIn"]
    for rec in await _list_records(B2B_CRM_TABLE, field_names=crm_fields):
        fields = rec.get("fields") or {}
        for value in [_url(fields.get("公司官网")), _text(fields.get("邮箱"))]:
            domain = _domain_of(value)
            if domain:
                crm_domains.add(domain)
        company_key = _text_key(_text(fields.get("公司名称")))
        if company_key:
            crm_company_keys.add(company_key)

    return lead_domains, lead_company_keys, crm_domains | lead_domains, crm_company_keys | lead_company_keys


async def _snov_token() -> str:
    if _SNOV_TOKEN["value"] and time.time() - _SNOV_TOKEN["ts"] < 3000:
        return _SNOV_TOKEN["value"]
    if not config.SNOV_CLIENT_ID or not config.SNOV_CLIENT_SECRET:
        raise RuntimeError("SNOV_CLIENT_ID/SECRET 未配置")
    async with httpx.AsyncClient(timeout=30.0) as cli:
        r = await cli.post("https://api.snov.io/v1/oauth/access_token", data={
            "grant_type": "client_credentials",
            "client_id": config.SNOV_CLIENT_ID,
            "client_secret": config.SNOV_CLIENT_SECRET,
        })
        r.raise_for_status()
        data = r.json()
    token = data.get("access_token") or ""
    if not token:
        raise RuntimeError("Snov OAuth returned no access_token")
    _SNOV_TOKEN["value"] = token
    _SNOV_TOKEN["ts"] = time.time()
    return token


async def _snov_json(method: str, url: str, token: str, body: dict | None = None) -> dict:
    headers = {"Authorization": f"Bearer {token}"}
    async with httpx.AsyncClient(timeout=40.0) as cli:
        if body is None:
            r = await cli.request(method, url, headers=headers)
        else:
            r = await cli.request(method, url, headers=headers, json=body)
        r.raise_for_status()
        return r.json()


async def _poll_result(start_resp: dict, token: str, *, poll_seconds: int = 3, max_polls: int = 8) -> dict:
    result_url = (
        start_resp.get("result_url")
        or ((start_resp.get("links") or {}).get("result"))
        or ((start_resp.get("data") or {}).get("result_url") if isinstance(start_resp.get("data"), dict) else "")
    )
    if not result_url:
        return start_resp
    import asyncio
    result = start_resp
    for _ in range(max_polls):
        await asyncio.sleep(poll_seconds)
        result = await _snov_json("GET", result_url, token)
        status = str(result.get("status") or ((result.get("data") or {}).get("status") if isinstance(result.get("data"), dict) else "")).lower()
        if status and not re.search(r"progress|pending|processing|queued", status):
            return result
        if not status:
            return result
    return {"status": "timeout", "result_url": result_url}


def _collect_prospects(obj) -> list[dict]:
    if not isinstance(obj, dict):
        return []
    data = obj.get("data")
    if isinstance(data, dict) and isinstance(data.get("prospects"), list):
        return [x for x in data["prospects"] if isinstance(x, dict)]
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(obj.get("prospects"), list):
        return [x for x in obj["prospects"] if isinstance(x, dict)]
    return []


def _role_score(prospect: dict) -> int:
    position = str(prospect.get("position") or prospect.get("job_title") or prospect.get("title") or "").lower()
    if _has_any(position, ["purchase", "purchasing", "buyer", "procurement", "sourcing", "achats", "acheteur", "compras", "zakup"]):
        return 100
    if _has_any(position, ["category", "product manager", "chef de produit", "product owner", "merchand"]):
        return 90
    if _has_any(position, ["business development", "partnership", "publishing", "export", "international sales", "bd"]):
        return 80
    if _has_any(position, ["ceo", "founder", "owner", "managing director", "general manager", "president", "director"]):
        return 70
    if _has_any(position, ["sales", "key account", "account manager", "retail"]):
        return 55
    return 0


async def _snov_prospects(domain: str, *, max_prospects: int) -> tuple[list[dict], str]:
    token = await _snov_token()
    query = "domain=" + quote(domain, safe="") + "&page=1"
    url = "https://api.snov.io/v2/domain-search/prospects/start?" + query
    start = await _snov_json("POST", url, token)
    result = await _poll_result(start, token)
    prospects = _collect_prospects(result)
    prospects = sorted(prospects, key=lambda p: -_role_score(p))
    summary = json.dumps({
        "domain": domain,
        "prospects": len(prospects),
        "status": result.get("status") or ((result.get("data") or {}).get("status") if isinstance(result.get("data"), dict) else ""),
    }, ensure_ascii=False)
    return prospects[:max_prospects], summary[:1200]


def _prospect_to_lead(seed: dict, prospect: dict, *, email: str = "", email_status: str = "") -> dict:
    first = str(prospect.get("first_name") or prospect.get("firstName") or "").strip()
    last = str(prospect.get("last_name") or prospect.get("lastName") or "").strip()
    contact = " ".join(x for x in [first, last] if x).strip() or str(seed.get("contact") or "").strip()
    title = str(prospect.get("position") or prospect.get("job_title") or prospect.get("title") or seed.get("title") or "").strip()
    linkedin = ""
    for key in ["linkedin_url", "linkedin", "source_page", "url"]:
        value = str(prospect.get(key) or "")
        if "linkedin.com" in value:
            linkedin = _normalize_url(value)
            break
    return _seed_to_lead(seed) | {
        "contact": contact,
        "title": title,
        "linkedin_profile": linkedin or str(seed.get("linkedin_profile") or ""),
        "email": email,
        "email_status": email_status,
    }


def _seed_to_lead(seed: dict) -> dict:
    domain = str(seed.get("domain") or _domain_of(seed.get("website") or "")).strip().lower()
    website = _normalize_url(str(seed.get("website") or domain or ""))
    company_type = str(seed.get("company_type") or "待判断").strip()
    if company_type not in COMPANY_TYPE_OPTIONS:
        company_type = "待判断"
    return {
        "company": str(seed.get("company") or "").strip(),
        "contact": str(seed.get("contact") or "").strip(),
        "title": str(seed.get("title") or "").strip(),
        "website": website,
        "domain": domain,
        "linkedin_company": _normalize_url(str(seed.get("linkedin_company") or "")),
        "linkedin_profile": _normalize_url(str(seed.get("linkedin_profile") or "")),
        "country": str(seed.get("country") or "").strip(),
        "company_type": company_type,
        "channels": _clean_channels(seed.get("channels") or []),
        "competitors": str(seed.get("competitors") or "").strip(),
        "category": str(seed.get("category") or "").strip(),
        "owner": str(seed.get("owner") or "").strip(),
        "source": str(seed.get("source") or "LinkedIn-现有客户相似").strip(),
        "notes": str(seed.get("notes") or "").strip(),
        "email": str(seed.get("email") or "").strip(),
        "email_status": str(seed.get("email_status") or "").strip(),
    }


async def _create_record(fields: dict) -> str:
    resp = await feishu.api("POST", f"/bitable/v1/apps/{B2B_APP_TOKEN}/tables/{B2B_LINKEDIN_TABLE}/records", {"fields": fields}, which="bitable")
    return (((resp.get("data") or {}).get("record") or {}).get("record_id")) or ""


def _lead_fields(lead: dict, score: dict, copy: dict, *, batch: str, snov_status: str, snov_source: str, snov_summary: str) -> dict:
    name = lead.get("company") or lead.get("linkedin_profile") or lead.get("domain")
    if lead.get("contact"):
        name = f"{lead.get('company')} - {lead.get('contact')}"
    fields = {
        "线索名称": name,
        "公司名称": lead.get("company"),
        "线索来源": lead.get("source") or "LinkedIn-现有客户相似",
        "开发状态": "待开发",
        "触达状态": "待触达",
        "联系人姓名": lead.get("contact"),
        "职位": lead.get("title"),
        "国家/地区": lead.get("country"),
        "公司类型": lead.get("company_type") or "待判断",
        "主力渠道": lead.get("channels") or [],
        "代理竞品": lead.get("competitors"),
        "主营类目": lead.get("category"),
        "AI开发评分": int(score["score"]),
        "ICP匹配": score["icp"],
        "AI建议等级": score["grade"],
        "AI开发理由": copy["reason"],
        "推荐连接语": copy["connect"],
        "推荐私信": copy["message"],
        "推荐开发信": copy["email"],
        "跟进人": lead.get("owner"),
        "邮箱": lead.get("email"),
        "邮箱验真状态": lead.get("email_status"),
        "Snov查询状态": snov_status,
        "Snov来源": snov_source,
        "Snov原始摘要": snov_summary,
        "Snov最后查询时间": int(_now_bj().timestamp() * 1000),
        "下一步行动": "业务员手动核对 LinkedIn profile；合格则手动加人并发送推荐连接语",
        "CRM匹配状态": "新线索",
        "去重Key": lead.get("domain") or _text_key(lead.get("company") or ""),
        "创建批次": batch,
        "备注": lead.get("notes"),
    }
    for key, value in [("公司官网", _url_cell(lead.get("website") or "")), ("LinkedIn公司页", _url_cell(lead.get("linkedin_company") or "")), ("LinkedIn联系人页", _url_cell(lead.get("linkedin_profile") or ""))]:
        if value:
            fields[key] = value
    return {k: v for k, v in fields.items() if v not in (None, "", [])}


async def run(
    *,
    commit: bool = False,
    domain_limit: int = 10,
    record_limit: int = 10,
    max_prospects_per_domain: int = 3,
    min_score: int = 55,
    allow_company_fallback: bool = True,
) -> dict:
    domain_limit = max(1, min(int(domain_limit or 10), 50))
    record_limit = max(1, min(int(record_limit or 10), 50))
    max_prospects_per_domain = max(1, min(int(max_prospects_per_domain or 3), 5))
    batch = "auto-linkedin-" + _now_bj().strftime("%Y%m%d-%H%M")
    started_at = _now_bj().strftime("%Y-%m-%d %H:%M:%S")

    seeds = _load_seeds()
    lead_domains, lead_company_keys, all_domains, all_company_keys = await _load_existing_keys()
    skip_reasons = Counter()
    created = []
    planned = []
    selected_domains = 0
    snov_errors = []

    for seed in seeds:
        if selected_domains >= domain_limit or len(planned) >= record_limit:
            break
        base_lead = _seed_to_lead(seed)
        domain = base_lead.get("domain")
        company_key = _text_key(base_lead.get("company") or "")
        if not domain and not company_key:
            skip_reasons["missing_domain_company"] += 1
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

        selected_domains += 1
        prospects = []
        snov_summary = ""
        snov_status = "未查询"
        if domain:
            try:
                prospects, snov_summary = await _snov_prospects(domain, max_prospects=max_prospects_per_domain)
                snov_status = "查询成功" if prospects else "无结果"
            except Exception as exc:
                snov_status = "查询失败"
                snov_summary = f"{type(exc).__name__}: {str(exc)[:300]}"
                snov_errors.append({"domain": domain, "error": snov_summary})

        candidate_leads = []
        for prospect in prospects:
            lead = _prospect_to_lead(seed, prospect)
            if lead.get("contact") and domain:
                try:
                    found = await snov.find_email(lead["contact"], domain)
                    lead["email"] = found.get("email") or ""
                    lead["email_status"] = found.get("status") or ""
                except Exception:
                    lead["email_status"] = "unavailable"
            candidate_leads.append((lead, snov_status, "Domain Search", snov_summary))

        if not candidate_leads and allow_company_fallback:
            candidate_leads.append((base_lead, "无结果", "Company seed fallback", snov_summary or f"{domain or base_lead.get('company')} 无联系人，保留公司级线索待人工核对"))

        for lead, status, source, summary in candidate_leads:
            if len(planned) >= record_limit:
                break
            score = _score_lead(lead)
            if score["score"] < min_score or score["icp"] == "否":
                skip_reasons["low_icp"] += 1
                continue
            copy = _copy_for_lead(lead, score)
            fields = _lead_fields(lead, score, copy, batch=batch, snov_status=status, snov_source=source, snov_summary=summary)
            planned.append({"company": lead.get("company"), "domain": lead.get("domain"), "contact": lead.get("contact"), "score": score["score"], "grade": score["grade"], "fields": fields})

    if commit:
        for row in planned:
            record_id = await _create_record(row["fields"])
            created.append({
                "record_id": record_id,
                "company": row["company"],
                "domain": row["domain"],
                "contact": row["contact"],
                "score": row["score"],
                "grade": row["grade"],
            })

    result = {
        "commit": commit,
        "started_at_bj": started_at,
        "batch": batch,
        "seed_total": len(seeds),
        "domain_limit": domain_limit,
        "record_limit": record_limit,
        "selected_domains": selected_domains,
        "planned_records": len(planned),
        "created_records": len(created),
        "created": created,
        "planned_preview": [
            {k: row[k] for k in ["company", "domain", "contact", "score", "grade"]}
            for row in planned[:20]
        ],
        "skip_reasons": dict(skip_reasons),
        "snov_errors": snov_errors[:10],
    }
    _LAST_RUN.clear()
    _LAST_RUN.update(result)
    return result

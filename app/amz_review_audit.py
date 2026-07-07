"""Amazon review / seller-feedback audit cards.

This module implements the audit loop for negative Listing reviews:

1. New negative review / seller feedback enters an audit state table.
2. Operators submit handled actions from a Feishu interactive card.
3. T+7 recheck decides whether the Listing homepage improved.
4. Failed rechecks are grouped by owner and escalated visibly.

The module is deliberately conservative: live Lingxing ingestion, Feishu writes,
and public group notification are all gated by env/config and endpoint flags.
"""
from __future__ import annotations

import json
import os
import re
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from . import cs_dispatch, feishu


BJ = timezone(timedelta(hours=8))

AUDIT_APP_TOKEN = os.environ.get("AMZ_REVIEW_AUDIT_APP_TOKEN", "")
AUDIT_TABLE_ID = os.environ.get("AMZ_REVIEW_AUDIT_TABLE_ID", "")
AMZ_OPS_GROUP_CHAT_ID = os.environ.get("AMZ_OPS_GROUP_CHAT_ID", "")
FRANKIE_UNION_ID = os.environ.get("AMZ_REVIEW_OBSERVE_UNION", cs_dispatch.OBSERVE_UNION)
OBSERVE = (os.environ.get("AMZ_REVIEW_AUDIT_OBSERVE", "1") or "1") != "0"
LX_PROXY_URL = os.environ.get("LINGXING_PROXY_URL", "")
LX_PROXY_TOKEN = os.environ.get("LINGXING_PROXY_TOKEN", "")
FRONTEND_CHECK_URL = os.environ.get("AMZ_REVIEW_FRONTEND_CHECK_URL", "")

STATE_NEW = "待处理"
STATE_SUBMITTED = "T+7待复检"
STATE_RECHECK_PASS = "复检通过：首页无差评"
STATE_RECHECK_FAIL = "复检失败：首页仍有差评"
STATE_OBSERVE = "客观无法移除，观察中"
STATE_ESCALATED = "已升级"

ACTION_OPTIONS = [
    "已发起合规留评拉升动作",
    "已提交违规评价举报 / 删除申请",
    "已投诉Amazon / 已开Case",
    "已联系买家售后处理",
    "已完成Listing / 产品整改",
    "客观无法移除，申请观察",
]

_SITE_DOMAINS = {
    "US": "amazon.com",
    "美国": "amazon.com",
    "CA": "amazon.ca",
    "加拿大": "amazon.ca",
    "MX": "amazon.com.mx",
    "墨西哥": "amazon.com.mx",
    "UK": "amazon.co.uk",
    "英国": "amazon.co.uk",
    "DE": "amazon.de",
    "德国": "amazon.de",
    "FR": "amazon.fr",
    "法国": "amazon.fr",
    "IT": "amazon.it",
    "意大利": "amazon.it",
    "ES": "amazon.es",
    "西班牙": "amazon.es",
    "JP": "amazon.co.jp",
    "日本": "amazon.co.jp",
}

_PLATFORM_FALLBACK_OWNER = {
    "亚马逊-美国": "黄奕纯",
    "亚马逊-加拿大": "陈翔宇",
    "亚马逊-墨西哥": "陈翔宇",
    "亚马逊-日本": "陈翔宇",
    "亚马逊-英国": "林明坚",
    "亚马逊-欧洲": "林明坚",
}

_HIGH_RISK_RE = re.compile(
    r"(fire|burn|smoke|injur|danger|unsafe|lawsuit|legal|a-to-z|A-to-z|A2Z|explode|battery|触电|起火|受伤|法律|索赔)",
    re.I,
)


def now_ms() -> int:
    return int(time.time() * 1000)


def _fmt_ms(ts_ms: int | None) -> str:
    if not ts_ms:
        return "-"
    return datetime.fromtimestamp(int(ts_ms) / 1000, BJ).strftime("%Y-%m-%d")


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float, bool)):
        return str(value)
    if isinstance(value, list):
        parts = []
        for item in value:
            if isinstance(item, dict):
                parts.append(str(item.get("text") or item.get("name") or item.get("link") or item.get("url") or ""))
            else:
                parts.append(str(item))
        return "".join(parts).strip()
    if isinstance(value, dict):
        return str(value.get("text") or value.get("name") or value.get("link") or value.get("url") or "").strip()
    return str(value).strip()


def _url(value: Any) -> str:
    if isinstance(value, dict):
        return _text(value.get("link") or value.get("url") or value.get("text"))
    if isinstance(value, list) and value and isinstance(value[0], dict):
        return _text(value[0].get("link") or value[0].get("url") or value[0].get("text"))
    return _text(value)


def _short(value: Any, limit: int = 120) -> str:
    text = re.sub(r"\s+", " ", _text(value))
    return text if len(text) <= limit else text[: limit - 1] + "…"


def parse_ms(value: Any) -> int | None:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        n = int(value)
        return n * 1000 if n < 10_000_000_000 else n
    text = _text(value)
    if not text:
        return None
    if text.isdigit():
        n = int(text)
        return n * 1000 if n < 10_000_000_000 else n
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d", "%Y/%m/%d %H:%M", "%Y/%m/%d"):
        try:
            return int(datetime.strptime(text, fmt).replace(tzinfo=BJ).timestamp() * 1000)
        except ValueError:
            pass
    return None


def _field(fields: dict, *names: str) -> Any:
    for name in names:
        if name in fields and fields.get(name) not in (None, ""):
            return fields.get(name)
    return ""


def rating_int(value: Any) -> int:
    text = _text(value)
    match = re.search(r"\d+", text)
    return int(match.group(0)) if match else 0


def amazon_listing_url(site: str, asin: str) -> str:
    if not asin:
        return ""
    site_text = _text(site).replace("亚马逊-", "")
    domain = _SITE_DOMAINS.get(site_text.upper()) or _SITE_DOMAINS.get(site_text) or "amazon.com"
    return f"https://www.{domain}/dp/{asin}"


def site_platform(site: str) -> str:
    text = _text(site)
    if not text:
        return "亚马逊-未知"
    if text.startswith("亚马逊-"):
        return text
    if text in ("US", "美国"):
        return "亚马逊-美国"
    if text in ("CA", "加拿大"):
        return "亚马逊-加拿大"
    if text in ("MX", "墨西哥"):
        return "亚马逊-墨西哥"
    if text in ("JP", "日本"):
        return "亚马逊-日本"
    if text in ("UK", "英国"):
        return "亚马逊-英国"
    if text in ("DE", "FR", "IT", "ES", "德国", "法国", "意大利", "西班牙"):
        return "亚马逊-欧洲"
    return "亚马逊-" + text


def owner_for_issue(issue: dict) -> str:
    owner = _text(issue.get("owner"))
    if owner and owner not in ("待定", "未分配", "None"):
        return owner
    return _PLATFORM_FALLBACK_OWNER.get(site_platform(issue.get("site", "")), "未分配")


def _high_risk(issue: dict) -> bool:
    blob = " ".join(
        _text(issue.get(k))
        for k in ("title", "summary", "review_text", "feedback_text", "current_homepage_status")
    )
    return bool(_HIGH_RISK_RE.search(blob))


def severity_for_issue(issue: dict) -> str:
    if _high_risk(issue):
        return "P0"
    source_type = _text(issue.get("source_type")).lower()
    rating = rating_int(issue.get("rating"))
    if source_type == "feedback":
        return "P1" if rating <= 3 else "P2"
    if rating <= 2:
        return "P1"
    if rating == 3 and (issue.get("homepage_visible") or _high_risk(issue)):
        return "P1"
    return "P2"


def should_alert_issue(issue: dict) -> bool:
    source_type = _text(issue.get("source_type")).lower()
    rating = rating_int(issue.get("rating"))
    if source_type == "feedback":
        return 1 <= rating <= 3
    if rating in (1, 2):
        return True
    return rating == 3 and (bool(issue.get("homepage_visible")) or _high_risk(issue))


def issue_key(issue: dict) -> str:
    source_type = _text(issue.get("source_type")).upper() or "REVIEW"
    source_id = _text(issue.get("source_id")) or _text(issue.get("review_id")) or _text(issue.get("feedback_id"))
    site = _text(issue.get("site")) or "UNKNOWN"
    asin = _text(issue.get("asin")) or "NOASIN"
    if not source_id:
        seed = "|".join([site, asin, _text(issue.get("rating")), _text(issue.get("title")), _text(issue.get("first_seen_ms"))])
        source_id = re.sub(r"[^A-Za-z0-9_-]+", "-", seed)[:80] or "nosource"
    return f"AMZ_{source_type}:{site}:{asin}:{source_id}"


def normalize_issue(raw: dict) -> dict:
    source_type = _text(_field(raw, "source_type", "来源类型", "type"))
    if not source_type:
        source_type = "feedback" if _field(raw, "feedback_id", "seller_feedback_id") else "review"
    source_type = "feedback" if source_type.lower() in ("feedback", "seller_feedback", "seller feedback") else "review"
    asin = _text(_field(raw, "asin", "ASIN"))
    site = _text(_field(raw, "site", "站点", "country", "marketplace", "marketplace_name"))
    owner = _text(_field(raw, "owner", "负责人", "principal_name"))
    if not owner:
        principal = raw.get("principal_info")
        if isinstance(principal, list) and principal:
            owner = _text(principal[0].get("principal_name"))
    issue = {
        "source_type": source_type,
        "source_id": _text(_field(raw, "source_id", "review_id", "feedback_id", "id", "reviewId")),
        "store_name": _text(_field(raw, "store_name", "店铺名", "seller_name", "shop_name")),
        "site": site,
        "erp_name": _text(_field(raw, "erp_name", "ERP品名", "product_name", "item_name", "title_name")),
        "asin": asin,
        "listing_url": _text(_field(raw, "listing_url", "ASIN链接")) or amazon_listing_url(site, asin),
        "owner": owner,
        "rating": rating_int(_field(raw, "rating", "star", "stars", "星级", "review_rating")),
        "title": _short(_field(raw, "title", "标题"), 120),
        "summary": _short(_field(raw, "summary", "摘要", "review_text", "feedback_text", "content", "body"), 500),
        "first_seen_ms": parse_ms(_field(raw, "first_seen_ms", "首次发现时间", "date", "created_at", "review_date")) or now_ms(),
        "homepage_visible": bool(raw.get("homepage_visible") or raw.get("首页可见")),
        "homepage_negative_count": raw.get("homepage_negative_count", raw.get("首页差评数")),
    }
    issue["owner"] = owner_for_issue(issue)
    issue["severity"] = severity_for_issue(issue)
    issue["issue_key"] = issue_key(issue)
    return issue


def issue_to_fields(issue: dict, status: str = STATE_NEW) -> dict:
    url = issue.get("listing_url") or amazon_listing_url(issue.get("site", ""), issue.get("asin", ""))
    return {
        "问题键": issue["issue_key"],
        "来源类型": "Feedback" if issue.get("source_type") == "feedback" else "Review",
        "来源ID": issue.get("source_id", ""),
        "状态": status,
        "店铺名": issue.get("store_name", ""),
        "站点": issue.get("site", ""),
        "ERP品名": issue.get("erp_name", ""),
        "ASIN": issue.get("asin", ""),
        "ASIN链接": {"link": url, "text": issue.get("asin", "") or "打开Listing"} if url else "",
        "负责人": issue.get("owner", "未分配"),
        "严重级别": issue.get("severity", "P2"),
        "星级": issue.get("rating", 0),
        "标题": issue.get("title", ""),
        "摘要": issue.get("summary", ""),
        "首次发现时间": issue.get("first_seen_ms") or now_ms(),
        "当前首页状态": "首页可见差评" if issue.get("homepage_visible") else "",
        "首页差评数": int(issue.get("homepage_negative_count") or 0),
        "最近提醒时间": now_ms(),
    }


def fields_to_issue(record_id: str, fields: dict) -> dict:
    asin = _text(fields.get("ASIN"))
    site = _text(fields.get("站点"))
    return {
        "record_id": record_id,
        "issue_key": _text(fields.get("问题键")) or record_id,
        "source_type": "feedback" if "Feedback" in _text(fields.get("来源类型")) else "review",
        "source_id": _text(fields.get("来源ID")),
        "store_name": _text(fields.get("店铺名")),
        "site": site,
        "erp_name": _text(fields.get("ERP品名")),
        "asin": asin,
        "listing_url": _url(fields.get("ASIN链接")) or amazon_listing_url(site, asin),
        "owner": _text(fields.get("负责人")) or "未分配",
        "severity": _text(fields.get("严重级别")) or "P2",
        "rating": rating_int(fields.get("星级")),
        "title": _text(fields.get("标题")),
        "summary": _text(fields.get("摘要")),
        "status": _text(fields.get("状态")),
        "first_seen_ms": parse_ms(fields.get("首次发现时间")) or 0,
        "handled_at_ms": parse_ms(fields.get("处理时间")),
        "handled_actions": _list_values(fields.get("处理方式")),
        "handled_note": _text(fields.get("处理备注")),
        "recheck_due_ms": parse_ms(fields.get("T+7复检日期")),
        "homepage_negative_count": rating_int(fields.get("首页差评数")),
        "current_homepage_status": _text(fields.get("当前首页状态")),
        "card_message_id": _text(fields.get("卡片消息ID")),
        "cs_ticket_id": _text(fields.get("客服工单ID")),
        "success_sent": _text(fields.get("恭喜已发送")) in ("true", "True", "1", "是", "已发送"),
    }


def _list_values(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        out = []
        for item in value:
            if isinstance(item, dict):
                text = _text(item.get("text") or item.get("name") or item.get("value"))
            else:
                text = _text(item)
            if text:
                out.append(text)
        return out
    text = _text(value)
    if not text:
        return []
    if text.startswith("["):
        try:
            arr = json.loads(text)
            return [_text(x) for x in arr if _text(x)]
        except Exception:
            pass
    return [x.strip() for x in re.split(r"[,，/、]", text) if x.strip()]


def _issue_md(issue: dict, include_status: bool = True) -> str:
    asin = issue.get("asin") or "-"
    url = issue.get("listing_url") or amazon_listing_url(issue.get("site", ""), asin)
    asin_md = f"[{asin}]({url})" if url and asin != "-" else asin
    lines = [
        f"**店铺名:** {issue.get('store_name') or '-'}  ·  **站点:** {issue.get('site') or '-'}",
        f"**ERP品名:** {issue.get('erp_name') or '-'}",
        f"**ASIN:** {asin_md}  ·  **负责人:** {issue.get('owner') or '-'}",
        f"**来源:** {issue.get('source_type') or '-'}  ·  **星级:** {issue.get('rating') or '-'}  ·  **级别:** {issue.get('severity') or '-'}",
    ]
    if include_status:
        lines.append(f"**当前状态:** {issue.get('status') or STATE_NEW}")
    if issue.get("title"):
        lines.append(f"**标题:** {issue['title']}")
    if issue.get("summary"):
        lines.append(f"**摘要:** {_short(issue['summary'], 220)}")
    return "\n".join(lines)


def _button(text: str, value: dict, typ: str = "default") -> dict:
    return {"tag": "button", "text": {"tag": "plain_text", "content": text}, "type": typ, "value": value}


def _url_button(text: str, url: str, typ: str = "default") -> dict:
    return {"tag": "button", "text": {"tag": "plain_text", "content": text}, "type": typ, "url": url}


def _field_md(label: str, value: str) -> dict:
    return {"is_short": True, "text": {"tag": "lark_md", "content": f"**{label}**\n{value or '-'}"}}


def _issue_fact_fields(issue: dict, include_status: bool = False) -> list[dict]:
    url = issue.get("listing_url") or amazon_listing_url(issue.get("site", ""), issue.get("asin", ""))
    asin = f"[{issue.get('asin')}]({url})" if url else issue.get("asin", "-")
    fields = [
        _field_md("店铺 / 站点", f"{issue.get('store_name') or '-'} / {issue.get('site') or '-'}"),
        _field_md("负责人", issue.get("owner") or "未分配"),
        _field_md("ERP品名", issue.get("erp_name") or "-"),
        _field_md("ASIN", asin),
        _field_md("来源 / 星级", f"{issue.get('source_type') or '-'} / {issue.get('rating') or '-'}星"),
        _field_md("级别 / 首次发现", f"{issue.get('severity') or 'P2'} / {_fmt_ms(issue.get('first_seen_ms'))}"),
    ]
    if include_status:
        fields.append(_field_md("当前状态", issue.get("status") or STATE_NEW))
        fields.append(_field_md("首页差评数", str(issue.get("homepage_negative_count") or 0)))
    return fields


def _payload(action: str, issue: dict) -> dict:
    return {
        "source": "amz_review_audit",
        "action": action,
        "issue_id": issue.get("record_id") or issue.get("issue_key"),
        "issue_key": issue.get("issue_key"),
        "source_type": issue.get("source_type"),
        "source_id": issue.get("source_id"),
        "asin": issue.get("asin"),
        "site": issue.get("site"),
        "store_name": issue.get("store_name"),
        "owner": issue.get("owner"),
        "batch_id": issue.get("batch_id") or f"amzrev-{int(time.time())}",
    }


def build_issue_card(issue: dict) -> dict:
    level = issue.get("severity", "P1")
    priority_emoji = "🔴" if level == "P0" else "🟠"
    template = "red" if level == "P0" else "orange"
    source_label = "Feedback" if issue.get("source_type") == "feedback" else "差评"
    title = f"{priority_emoji} [AMZ·{level}] 新增{source_label}待处理 · {issue.get('erp_name') or issue.get('asin')}"
    rid = issue.get("record_id") or re.sub(r"[^A-Za-z0-9]", "_", issue.get("issue_key", "issue"))[:40]
    url = issue.get("listing_url") or amazon_listing_url(issue.get("site", ""), issue.get("asin", ""))
    summary_lines = []
    if issue.get("title"):
        summary_lines.append(f"**标题:** {issue['title']}")
    if issue.get("summary"):
        summary_lines.append(f"**摘要:** {_short(issue['summary'], 260)}")
    return {
        "config": {"wide_screen_mode": True},
        "header": {"template": template, "title": {"tag": "plain_text", "content": title}},
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md", "content": f"🚨 **处理要求**\n该{source_label}已进入审计闭环：提交处理不等于关闭，系统会在 T+7 自动复检 Listing 首页。"}},
            {"tag": "div", "fields": _issue_fact_fields(issue)},
            *([{"tag": "action", "actions": [_url_button("打开Listing前台", url, "primary")]}] if url else []),
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md", "content": "📝 **差评 / Feedback 摘要**\n" + ("\n".join(summary_lines) if summary_lines else "-")}},
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md", "content": "✅ **主动作：提交处理结果（可多选）**\n下拉框不是四选一。运营可以同时选择多个已执行动作，例如“已开Case + 已完成Listing整改”。点确认后原卡会变灰，并进入复检或观察流程。"}},
            {"tag": "form", "name": f"amz_actions_f_{rid}", "elements": [
                {
                    "tag": "multi_select_static",
                    "name": f"amz_actions_{rid}",
                    "placeholder": {"tag": "plain_text", "content": "选择已执行动作（可多选，不是排他选择）"},
                    "options": [{"text": {"tag": "plain_text", "content": x}, "value": x} for x in ACTION_OPTIONS],
                },
                {
                    "tag": "input",
                    "name": f"amz_note_{rid}",
                    "label_position": "left",
                    "label": {"tag": "plain_text", "content": "备注:"},
                    "placeholder": {"tag": "plain_text", "content": "Case ID、处理证据、观察原因等，简短填写"},
                },
                {
                    "tag": "button",
                    "action_type": "form_submit",
                    "name": f"amz_submit_{rid}",
                    "type": "primary",
                    "text": {"tag": "plain_text", "content": "确认提交处理结果"},
                    "value": _payload("amz_issue_submit_actions", issue),
                },
            ]},
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md", "content": "🧩 **辅助动作：不是处理方式，不是必点**\n- **同步到客服库（可选）**：需要客服售后跟进时才点，和上方处理提交不互斥。\n- **异常升级主管**：账号风险、差评爆发、需要主管/Frankie 介入时才点。"}},
            {"tag": "action", "actions": [
                _button("同步到客服库（可选）", _payload("amz_issue_create_cs_ticket", issue)),
                _button("异常升级主管", _payload("amz_issue_escalate", issue), "danger"),
            ]},
            {"tag": "note", "elements": [{"tag": "plain_text", "content": "合规提醒：只记录合规处理动作。提交后进入 T+7 复检；关闭条件是首页无差评或上级确认观察。"}]},
        ],
    }


def build_daily_digest_card(owner: str, issues: list[dict], today_label: str = "") -> dict:
    today_label = today_label or datetime.now(BJ).strftime("%Y-%m-%d")
    lines = [
        f"📌 **今日需要处理:** {len(issues)} 个 Listing 首页仍有差评",
        f"**负责人:** {owner}  ·  **日期:** {today_label}",
    ]
    for idx, issue in enumerate(issues[:8], 1):
        url = issue.get("listing_url") or amazon_listing_url(issue.get("site", ""), issue.get("asin", ""))
        asin = f"[{issue.get('asin')}]({url})" if url else issue.get("asin", "-")
        lines.append(
            f"**{idx}. {issue.get('erp_name') or '-'}**\n"
            f"店铺/站点: {issue.get('store_name') or '-'} / {issue.get('site') or '-'}  ·  ASIN: {asin}  ·  星级: {issue.get('rating') or '-'}星"
        )
    if len(issues) > 8:
        lines.append(f"... 另有 {len(issues) - 8} 条，请打开日看板处理。")
    return {
        "config": {"wide_screen_mode": True},
        "header": {"template": "orange", "title": {"tag": "plain_text", "content": f"🟠 [AMZ·P1] Listing首页差评巡检 · {owner} · {today_label}"}},
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md", "content": "\n".join(lines)}},
            {"tag": "note", "elements": [{"tag": "plain_text", "content": "这是每日巡检卡：卡片只展示核心项；逐条处理请以新增提醒卡或日看板为准。"}]},
        ],
    }


def build_recheck_failed_card(owner: str, issues: list[dict], day14: bool = False) -> dict:
    level = "P0" if day14 else "P1"
    priority_emoji = "🔴" if day14 else "🟠"
    template = "red" if day14 else "orange"
    lines = [
        "🚨 **公开升级原因**",
        f"负责人 **{owner}** 已提交处理，但复检发现 Listing 首页差评仍未改善。",
        f"**复检失败:** {len(issues)} 条  ·  **升级级别:** {level}",
        "**审计口径:** 点过“已处理”不会静默关闭；只有首页无差评或上级确认观察才关闭。",
    ]
    for idx, issue in enumerate(issues[:10], 1):
        first_days = _days_since(issue.get("first_seen_ms"))
        handled_days = _days_since(issue.get("handled_at_ms"))
        url = issue.get("listing_url") or amazon_listing_url(issue.get("site", ""), issue.get("asin", ""))
        asin = f"[{issue.get('asin')}]({url})" if url else issue.get("asin", "-")
        actions = "、".join(issue.get("handled_actions") or []) or "-"
        lines.append(
            f"⚠️ **{idx}. {issue.get('erp_name') or '-'}**\n"
            f"店铺/站点: {issue.get('store_name') or '-'} / {issue.get('site') or '-'}  ·  ASIN: {asin}\n"
            f"星级: {issue.get('rating') or '-'}星  ·  首次发现后 {first_days} 天  ·  标记处理后 {handled_days} 天\n"
            f"当时处理方式: {actions}"
        )
    return {
        "config": {"wide_screen_mode": True},
        "header": {"template": template, "title": {"tag": "plain_text", "content": f"{priority_emoji} [AMZ·{level}] 差评处理复检失败 · {owner} · {len(issues)}条"}},
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md", "content": "\n".join(lines)}},
            {"tag": "note", "elements": [{"tag": "plain_text", "content": "这是公开审计卡：负责人私聊 + 亚马逊群可见，用于防止“标记已处理但结果未改善”。"}]},
        ],
    }


def build_success_card(issue: dict) -> dict:
    url = issue.get("listing_url") or amazon_listing_url(issue.get("site", ""), issue.get("asin", ""))
    return {
        "config": {"wide_screen_mode": True},
        "header": {"template": "green", "title": {"tag": "plain_text", "content": f"🟢 [AMZ·P3] Listing首页已无差评 · {issue.get('erp_name') or issue.get('asin')}"}},
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md", "content": "🎉 **恭喜恢复**\n系统复检确认：该 Listing 首页当前已无差评，本轮审计关闭。"}},
            {"tag": "div", "fields": _issue_fact_fields(issue, include_status=False)},
            *([{"tag": "action", "actions": [_url_button("查看Listing前台", url, "primary")]}] if url else []),
            {"tag": "note", "elements": [{"tag": "plain_text", "content": "该恭喜卡每轮只发一次；未来出现新差评会重新进入审计。"}]},
        ],
    }


def build_processed_card(issue: dict, title: str, result: str, template: str = "green") -> dict:
    content = _issue_md(issue)
    content += f"\n\n**处理结果:** {result}\n\n_此卡片已处理，无需重复点击。_"
    return {
        "config": {"wide_screen_mode": True},
        "header": {"template": template, "title": {"tag": "plain_text", "content": title}},
        "elements": [{"tag": "div", "text": {"tag": "lark_md", "content": content}}],
    }


def _days_since(ts_ms: int | None) -> int:
    if not ts_ms:
        return 0
    return max(0, int((now_ms() - int(ts_ms)) / 86_400_000))


def _audit_configured() -> bool:
    return bool(AUDIT_APP_TOKEN and AUDIT_TABLE_ID)


def _audit_path(record_id: str = "") -> str:
    base = f"/bitable/v1/apps/{AUDIT_APP_TOKEN}/tables/{AUDIT_TABLE_ID}/records"
    return f"{base}/{record_id}" if record_id else base


async def _search_audit_by_key(key: str) -> dict | None:
    if not _audit_configured():
        return None
    body = {"filter": {"conjunction": "and", "conditions": [{"field_name": "问题键", "operator": "is", "value": [key]}]}, "page_size": 1}
    data = await feishu.api("POST", _audit_path() + "/search", body, which="notify")
    items = ((data.get("data") or {}).get("items") or data.get("items") or [])
    return items[0] if items else None


async def _create_audit(fields: dict) -> str:
    data = await feishu.api("POST", _audit_path(), {"fields": fields}, which="notify")
    return (((data.get("data") or {}).get("record") or {}).get("record_id") or "")


async def _update_audit(record_id: str, fields: dict) -> None:
    await feishu.api("PUT", _audit_path(record_id), {"fields": fields}, which="notify")


async def _list_audit_records(statuses: list[str] | None = None, limit: int = 200) -> list[dict]:
    if not _audit_configured():
        return []
    body: dict[str, Any] = {"page_size": min(limit, 500)}
    if statuses:
        body["filter"] = {"conjunction": "or", "conditions": [{"field_name": "状态", "operator": "is", "value": [s]} for s in statuses]}
    data = await feishu.api("POST", _audit_path() + "/search", body, which="notify")
    return ((data.get("data") or {}).get("items") or data.get("items") or [])[:limit]


async def _send_union(union_id: str, card: dict) -> str:
    return await cs_dispatch._send_card(union_id, card)


async def _send_group(chat_id: str, card: dict) -> str:
    tok = await cs_dispatch._token()
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id",
            headers={"Authorization": f"Bearer {tok}"},
            json={"receive_id": chat_id, "msg_type": "interactive", "content": json.dumps(card, ensure_ascii=False)},
        )
        data = resp.json()
    return data.get("data", {}).get("message_id", "") if data.get("code") == 0 else ""


async def _owner_union(owner: str) -> str:
    if OBSERVE:
        return FRANKIE_UNION_ID
    return await cs_dispatch._resolve_union(owner) or FRANKIE_UNION_ID


async def _lx_proxy(method: str, path: str, params: dict) -> dict:
    if not (LX_PROXY_URL and LX_PROXY_TOKEN):
        raise RuntimeError("LINGXING_PROXY_URL / LINGXING_PROXY_TOKEN 未配置")
    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.post(
            LX_PROXY_URL,
            headers={"Authorization": f"Bearer {LX_PROXY_TOKEN}", "Content-Type": "application/json"},
            json={"method": method, "path": path, "params": params},
        )
        resp.raise_for_status()
        return resp.json()


async def _fetch_lingxing_issues(limit: int = 50) -> list[dict]:
    """Fetch raw review / feedback rows through the existing Lingxing proxy.

    Lingxing review endpoints differ by account/API version. Keep this as a
    best-effort adapter and normalize only rows that the proxy returns. Missing
    proxy config is a setup error for commit mode, not a silent success.
    """
    rows: list[dict] = []
    try:
        review_data = await _lx_proxy("POST", "/erp/sc/v2/data/mws/reviews", {"offset": 0, "length": min(limit, 200)})
        raw_reviews = review_data.get("data") or review_data.get("rows") or []
        if isinstance(raw_reviews, dict):
            raw_reviews = raw_reviews.get("list") or raw_reviews.get("items") or []
        rows.extend(raw_reviews if isinstance(raw_reviews, list) else [])
    except Exception as exc:
        print(f"[amz_review_audit] review fetch skipped: {exc}")
    try:
        fb_data = await _lx_proxy("POST", "/erp/sc/routing/service/FeedbackListMws", {"offset": 0, "length": min(limit, 200)})
        raw_fb = fb_data.get("data") or fb_data.get("rows") or []
        if isinstance(raw_fb, dict):
            raw_fb = raw_fb.get("list") or raw_fb.get("items") or []
        for row in raw_fb if isinstance(raw_fb, list) else []:
            row = dict(row)
            row["source_type"] = "feedback"
            rows.append(row)
    except Exception as exc:
        print(f"[amz_review_audit] feedback fetch skipped: {exc}")
    return rows[:limit]


def _sample_raw_issues() -> list[dict]:
    base = now_ms() - 2 * 86_400_000
    return [
        {
            "source_type": "review",
            "review_id": "sample-review-001",
            "store_name": "Fanlepu-US",
            "site": "US",
            "erp_name": "FF05A Luminex Controller",
            "asin": "B0TEST001",
            "principal_name": "黄奕纯",
            "rating": 1,
            "title": "Stopped working after two days",
            "review_text": "The controller disconnects and does not charge.",
            "first_seen_ms": base,
            "homepage_visible": True,
            "homepage_negative_count": 1,
        },
        {
            "source_type": "feedback",
            "feedback_id": "sample-feedback-001",
            "store_name": "Fanlepu-CA",
            "site": "CA",
            "erp_name": "PK Docking Station",
            "asin": "B0TEST002",
            "principal_name": "陈翔宇",
            "rating": 2,
            "feedback_text": "Late delivery and no response from seller.",
            "first_seen_ms": base,
            "homepage_visible": False,
        },
    ]


async def ingest_delta(mode: str = "dry_run", notify: bool = False, limit: int = 50, sample: bool = False) -> dict:
    raw_rows = _sample_raw_issues() if sample or mode == "dry_run" and not LX_PROXY_URL else await _fetch_lingxing_issues(limit)
    issues = [normalize_issue(row) for row in raw_rows]
    issues = [issue for issue in issues if should_alert_issue(issue)]
    created, updated, sent = 0, 0, 0
    previews = []
    for issue in issues[:limit]:
        card = build_issue_card(issue)
        previews.append({"issue": issue, "card_title": card["header"]["title"]["content"]})
        if mode != "commit":
            continue
        if not _audit_configured():
            raise RuntimeError("AMZ_REVIEW_AUDIT_APP_TOKEN / AMZ_REVIEW_AUDIT_TABLE_ID 未配置")
        existing = await _search_audit_by_key(issue["issue_key"])
        if existing:
            record_id = existing.get("record_id", "")
            existing_status = _text((existing.get("fields") or {}).get("状态"))
            if existing_status in (STATE_SUBMITTED, STATE_RECHECK_PASS, STATE_OBSERVE, STATE_ESCALATED):
                continue
            await _update_audit(record_id, {"最近提醒时间": now_ms()})
            issue["record_id"] = record_id
            updated += 1
        else:
            record_id = await _create_audit(issue_to_fields(issue))
            issue["record_id"] = record_id
            created += 1
        if notify:
            union = await _owner_union(issue["owner"])
            msg_id = await _send_union(union, build_issue_card(issue))
            if msg_id:
                await _update_audit(issue["record_id"], {"卡片消息ID": msg_id})
                sent += 1
    return {"mode": mode, "source_rows": len(raw_rows), "eligible": len(issues), "created": created, "updated": updated, "sent": sent, "preview": previews[:10]}


async def daily_digest(mode: str = "dry_run", notify: bool = False, limit: int = 200, sample: bool = False) -> dict:
    if mode == "dry_run" and sample:
        issues = [normalize_issue(row) for row in _sample_raw_issues()]
    else:
        records = await _list_audit_records([STATE_NEW, STATE_SUBMITTED, STATE_RECHECK_FAIL], limit=limit)
        issues = [fields_to_issue(rec.get("record_id", ""), rec.get("fields") or {}) for rec in records]
    grouped: dict[str, list[dict]] = defaultdict(list)
    for issue in issues:
        grouped[issue.get("owner") or "未分配"].append(issue)
    sent = 0
    cards = []
    for owner, rows in grouped.items():
        card = build_daily_digest_card(owner, rows)
        cards.append({"owner": owner, "count": len(rows), "card_title": card["header"]["title"]["content"]})
        if mode == "commit" and notify:
            msg_id = await _send_union(await _owner_union(owner), card)
            sent += 1 if msg_id else 0
    return {"mode": mode, "owners": len(grouped), "issues": len(issues), "sent": sent, "cards": cards}


async def _homepage_check(issue: dict) -> dict:
    if issue.get("homepage_negative_count") not in (None, ""):
        count = int(issue.get("homepage_negative_count") or 0)
        return {"ok": True, "has_negative": count > 0, "negative_count": count, "status": f"首页差评数={count}"}
    if not FRONTEND_CHECK_URL:
        return {"ok": False, "has_negative": None, "negative_count": None, "status": "未配置 AMZ_REVIEW_FRONTEND_CHECK_URL，无法自动确认首页状态"}
    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.post(FRONTEND_CHECK_URL, json={"asin": issue.get("asin"), "site": issue.get("site"), "url": issue.get("listing_url")})
        resp.raise_for_status()
        data = resp.json()
    count = int(data.get("negative_count") or data.get("homepage_negative_count") or 0)
    return {"ok": True, "has_negative": bool(data.get("has_negative", count > 0)), "negative_count": count, "status": _text(data.get("status")) or f"首页差评数={count}"}


async def recheck_due(mode: str = "dry_run", notify: bool = False, limit: int = 200, sample: bool = False) -> dict:
    if mode == "dry_run" and sample:
        base = normalize_issue(_sample_raw_issues()[0])
        base.update({"record_id": "rec_sample_fail", "status": STATE_SUBMITTED, "handled_at_ms": now_ms() - 8 * 86_400_000, "recheck_due_ms": now_ms() - 86_400_000, "handled_actions": ["已投诉Amazon / 已开Case"], "homepage_negative_count": 1})
        ok = normalize_issue(_sample_raw_issues()[1])
        ok.update({"record_id": "rec_sample_ok", "status": STATE_SUBMITTED, "handled_at_ms": now_ms() - 8 * 86_400_000, "recheck_due_ms": now_ms() - 86_400_000, "handled_actions": ["已联系买家售后处理"], "homepage_negative_count": 0})
        issues = [base, ok]
    else:
        records = await _list_audit_records([STATE_SUBMITTED], limit=limit)
        issues = [fields_to_issue(rec.get("record_id", ""), rec.get("fields") or {}) for rec in records]
    due = [issue for issue in issues if not issue.get("recheck_due_ms") or int(issue.get("recheck_due_ms") or 0) <= now_ms()]
    failed: list[dict] = []
    passed: list[dict] = []
    unknown: list[dict] = []
    for issue in due:
        check = await _homepage_check(issue)
        issue["current_homepage_status"] = check["status"]
        issue["homepage_negative_count"] = check.get("negative_count")
        if not check["ok"]:
            unknown.append(issue)
            continue
        if check["has_negative"]:
            failed.append(issue)
            if mode == "commit" and _audit_configured():
                await _update_audit(issue["record_id"], {"状态": STATE_RECHECK_FAIL, "当前首页状态": check["status"], "首页差评数": check.get("negative_count") or 0, "最近提醒时间": now_ms()})
        else:
            passed.append(issue)
            if mode == "commit" and _audit_configured():
                await _update_audit(issue["record_id"], {"状态": STATE_RECHECK_PASS, "当前首页状态": check["status"], "首页差评数": 0, "恭喜已发送": True})
    sent_owner = 0
    sent_group = 0
    if mode == "commit" and notify:
        grouped: dict[str, list[dict]] = defaultdict(list)
        for issue in failed:
            grouped[issue.get("owner") or "未分配"].append(issue)
        for owner, rows in grouped.items():
            day14 = any(_days_since(x.get("handled_at_ms")) >= 14 for x in rows)
            card = build_recheck_failed_card(owner, rows, day14=day14)
            if await _send_union(await _owner_union(owner), card):
                sent_owner += 1
            if AMZ_OPS_GROUP_CHAT_ID and not OBSERVE:
                if await _send_group(AMZ_OPS_GROUP_CHAT_ID, card):
                    sent_group += 1
        for issue in passed:
            if not issue.get("success_sent"):
                if await _send_union(await _owner_union(issue.get("owner", "")), build_success_card(issue)):
                    sent_owner += 1
    metrics = audit_metrics(issues)
    return {"mode": mode, "due": len(due), "failed": len(failed), "passed": len(passed), "unknown": len(unknown), "sent_owner": sent_owner, "sent_group": sent_group, "metrics": metrics}


def audit_metrics(issues: list[dict]) -> dict:
    unresolved = [x for x in issues if x.get("status") not in (STATE_RECHECK_PASS, STATE_OBSERVE)]
    fail7 = [x for x in issues if x.get("status") == STATE_RECHECK_FAIL]
    over14 = [x for x in unresolved if _days_since(x.get("handled_at_ms") or x.get("first_seen_ms")) >= 14]
    avg_days = int(sum(_days_since(x.get("first_seen_ms")) for x in unresolved) / len(unresolved)) if unresolved else 0
    by_owner: dict[str, dict[str, int]] = defaultdict(lambda: {"待处理": 0, "已处理未改善": 0})
    for issue in issues:
        owner = issue.get("owner") or "未分配"
        if issue.get("status") == STATE_RECHECK_FAIL:
            by_owner[owner]["已处理未改善"] += 1
        elif issue.get("status") in (STATE_NEW, STATE_SUBMITTED):
            by_owner[owner]["待处理"] += 1
    return {
        "7天复检失败数": len(fail7),
        "平均未解决天数": avg_days,
        "14天以上未解决数": len(over14),
        "首页无差评恢复数": sum(1 for x in issues if x.get("status") == STATE_RECHECK_PASS),
        "负责人待处理数/已处理未改善数": dict(by_owner),
    }


async def create_cs_ticket(issue: dict, actor: str = "") -> str:
    ticket_id = ("AMZF" if issue.get("source_type") == "feedback" else "AMZR") + "-" + (_text(issue.get("source_id")) or _text(issue.get("issue_key"))[-40:])
    existing = await _search_cs_ticket(ticket_id)
    if existing:
        return existing.get("record_id", "")
    summary = (
        f"{issue.get('source_type')} {issue.get('rating')}星；"
        f"{issue.get('store_name')} / {issue.get('site')} / {issue.get('erp_name')} / {issue.get('asin')}；"
        f"{_short(issue.get('summary'), 500)}"
    )
    fields = {
        "工单ID": ticket_id,
        "原文": f"{issue.get('title') or ''}\n\n{issue.get('summary') or ''}\n\nListing: {issue.get('listing_url') or amazon_listing_url(issue.get('site',''), issue.get('asin',''))}",
        "品牌": _brand_for_issue(issue),
        "销售平台": site_platform(issue.get("site", "")),
        "客户标识": issue.get("source_id") or issue.get("asin"),
        "客诉摘要": summary[:1000],
        "AI置信度": "AI起草人工审",
        "AI草稿": "该工单来自亚马逊差评/Feedback 审计。请先按平台政策处理售后或举报违规评价；不要承诺返现换改评/删评。",
        "分配运营": issue.get("owner") or "未分配",
        "状态": "待派",
        "线程ID": ticket_id,
    }
    data = await feishu.api("POST", f"/bitable/v1/apps/{cs_dispatch.CS_APP}/tables/{cs_dispatch.T_TICKET}/records", {"fields": fields}, which="notify")
    return (((data.get("data") or {}).get("record") or {}).get("record_id") or "")


def _brand_for_issue(issue: dict) -> str:
    blob = f"{issue.get('store_name') or ''} {issue.get('erp_name') or ''}".upper()
    if "FUNLAB" in blob or re.search(r"\bFF\d", blob):
        return "FUNLAB"
    if "POWKONG" in blob or re.search(r"\bPK\d", blob):
        return "POWKONG"
    return ""


async def _search_cs_ticket(ticket_id: str) -> dict | None:
    body = {"filter": {"conjunction": "and", "conditions": [{"field_name": "工单ID", "operator": "is", "value": [ticket_id]}]}, "page_size": 1}
    data = await feishu.api("POST", f"/bitable/v1/apps/{cs_dispatch.CS_APP}/tables/{cs_dispatch.T_TICKET}/records/search", body, which="notify")
    items = ((data.get("data") or {}).get("items") or data.get("items") or [])
    return items[0] if items else None


def _card_message_id(event: dict) -> str:
    candidates = [
        event.get("message_id"),
        event.get("open_message_id"),
        event.get("card_open_message_id"),
        (event.get("message") or {}).get("message_id"),
        (event.get("context") or {}).get("open_message_id"),
        (event.get("context") or {}).get("message_id"),
    ]
    for value in candidates:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _operator_label(event: dict) -> str:
    op = event.get("operator", {}) or {}
    return (op.get("union_id") or op.get("open_id") or "运营自助")[:80]


def _extract_action(event: dict) -> tuple[str, dict, dict]:
    action = event.get("action", {}) or {}
    value = action.get("value", {}) or {}
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            value = {"action": value}
    form = action.get("form_value", {}) or event.get("card_form_value", {}) or {}
    return _text(value.get("action") or value.get("act")), value, form


def _form_selection(form: dict, issue_id: str, suffix: str) -> Any:
    keys = [f"amz_{suffix}_{issue_id}", f"amz_{suffix}_{re.sub(r'[^A-Za-z0-9]', '_', issue_id)[:40]}"]
    for key in keys:
        if key in form:
            return form.get(key)
    for key, value in form.items():
        if key.startswith(f"amz_{suffix}_"):
            return value
    return ""


async def handle_callback(event: dict) -> dict:
    action, value, form = _extract_action(event)
    if not action.startswith("amz_issue_"):
        return {"toast": {"type": "error", "content": "未知 Amazon 差评动作"}}
    issue_id = _text(value.get("issue_id") or value.get("issue_key"))
    if not issue_id:
        return {"toast": {"type": "error", "content": "缺少 issue_id"}}
    record = None
    if _audit_configured() and issue_id.startswith("rec"):
        try:
            data = await feishu.api("GET", _audit_path(issue_id), which="notify")
            record = ((data.get("data") or {}).get("record") or {})
        except Exception:
            record = None
    if not record and _audit_configured():
        record = await _search_audit_by_key(_text(value.get("issue_key")) or issue_id)
    fields = (record or {}).get("fields") or {}
    issue = fields_to_issue((record or {}).get("record_id") or issue_id, fields) if fields else normalize_issue(value)
    issue["record_id"] = (record or {}).get("record_id") or issue.get("record_id") or issue_id
    msg_id = _card_message_id(event) or issue.get("card_message_id", "")
    actor = _operator_label(event)
    current_status = issue.get("status")

    if action == "amz_issue_submit_actions":
        if current_status in (STATE_SUBMITTED, STATE_RECHECK_PASS, STATE_OBSERVE, STATE_ESCALATED):
            await cs_dispatch._update_card(msg_id, build_processed_card(issue, "✅ [AMZ·已处理]", "该差评已提交过处理结果，重复点击已拦截。"))
            return {"toast": {"type": "success", "content": "该差评已处理过，无需重复提交"}}
        actions = _list_values(_form_selection(form, issue["record_id"], "actions"))
        note = _text(_form_selection(form, issue["record_id"], "note"))
        if not actions:
            return {"toast": {"type": "error", "content": "请至少选择一种处理方式"}}
        if any("客观无法移除" in x or "申请观察" in x for x in actions):
            update = {
                "状态": STATE_OBSERVE,
                "处理方式": actions,
                "处理备注": note,
                "处理时间": now_ms(),
                "处理人": actor,
                "最近提醒时间": now_ms(),
            }
            if _audit_configured() and issue.get("record_id", "").startswith("rec"):
                await _update_audit(issue["record_id"], update)
            issue.update({
                "status": STATE_OBSERVE,
                "handled_actions": actions,
                "handled_note": note,
                "handled_at_ms": now_ms(),
            })
            await cs_dispatch._update_card(
                msg_id,
                build_processed_card(
                    issue,
                    "🟡 [AMZ·观察申请已提交]",
                    f"已进入观察申请：{'、'.join(actions)}。关闭仍需上级确认，或后续复检首页无差评。",
                    "yellow",
                ),
            )
            return {"toast": {"type": "success", "content": "已提交观察申请"}}
        due = now_ms() + 7 * 86_400_000
        update = {"状态": STATE_SUBMITTED, "处理方式": actions, "处理备注": note, "处理时间": now_ms(), "处理人": actor, "T+7复检日期": due}
        if _audit_configured() and issue.get("record_id", "").startswith("rec"):
            await _update_audit(issue["record_id"], update)
        issue.update({"status": STATE_SUBMITTED, "handled_actions": actions, "handled_note": note, "handled_at_ms": now_ms(), "recheck_due_ms": due})
        await cs_dispatch._update_card(msg_id, build_processed_card(issue, "✅ [AMZ·处理已提交]", f"已进入 T+7 复检：{'、'.join(actions)}"))
        return {"toast": {"type": "success", "content": "已提交处理结果，7天后系统复检首页"}}

    if action == "amz_issue_create_cs_ticket":
        if issue.get("cs_ticket_id"):
            await cs_dispatch._update_card(msg_id, build_processed_card(issue, "✅ [AMZ·客服工单已存在]", f"客服工单已存在：{issue['cs_ticket_id']}"))
            return {"toast": {"type": "success", "content": "客服工单已存在"}}
        rid = await create_cs_ticket(issue, actor=actor)
        if _audit_configured() and issue.get("record_id", "").startswith("rec"):
            await _update_audit(issue["record_id"], {"客服工单ID": rid, "最近提醒时间": now_ms()})
        issue["cs_ticket_id"] = rid
        await cs_dispatch._update_card(msg_id, build_processed_card(issue, "✅ [AMZ·已录入客服库]", f"已创建客服工单：{rid}"))
        return {"toast": {"type": "success", "content": "已录入客服库"}}

    if action == "amz_issue_request_observation":
        if _audit_configured() and issue.get("record_id", "").startswith("rec"):
            await _update_audit(issue["record_id"], {"状态": STATE_OBSERVE, "处理备注": "卡片申请观察", "处理时间": now_ms(), "处理人": actor})
        issue["status"] = STATE_OBSERVE
        await cs_dispatch._update_card(msg_id, build_processed_card(issue, "🟡 [AMZ·已申请观察]", "已标记为客观无法移除/观察中；后续进入审计指标而非重复私聊。", "yellow"))
        return {"toast": {"type": "success", "content": "已申请观察"}}

    if action == "amz_issue_escalate":
        if _audit_configured() and issue.get("record_id", "").startswith("rec"):
            await _update_audit(issue["record_id"], {"状态": STATE_ESCALATED, "处理时间": now_ms(), "处理人": actor})
        issue["status"] = STATE_ESCALATED
        await cs_dispatch._notify_frankie(f"🔴 Amazon差评红线升级\n{issue.get('owner')} / {issue.get('site')} / {issue.get('asin')}\n{issue.get('summary')}")
        await cs_dispatch._update_card(msg_id, build_processed_card(issue, "🔴 [AMZ·已升级红线]", "已通知 Frankie/上级介入；请不要在这张卡重复点击。", "red"))
        return {"toast": {"type": "success", "content": "已升级红线"}}

    return {"toast": {"type": "error", "content": "未知 Amazon 差评动作"}}


async def run(kind: str = "all", mode: str = "dry_run", notify: bool = False, limit: int = 50, sample: bool = False) -> dict:
    if mode not in ("dry_run", "commit"):
        raise ValueError("mode must be dry_run or commit")
    if mode == "commit" and not _audit_configured():
        raise RuntimeError("commit 模式必须配置 AMZ_REVIEW_AUDIT_APP_TOKEN / AMZ_REVIEW_AUDIT_TABLE_ID")
    result: dict[str, Any] = {"mode": mode, "kind": kind, "observe": OBSERVE}
    if kind in ("delta", "all"):
        result["delta"] = await ingest_delta(mode=mode, notify=notify, limit=limit, sample=sample)
    if kind in ("daily", "all"):
        result["daily"] = await daily_digest(mode=mode, notify=notify, limit=limit, sample=sample)
    if kind in ("recheck", "all"):
        result["recheck"] = await recheck_due(mode=mode, notify=notify, limit=limit, sample=sample)
    if kind not in ("delta", "daily", "recheck", "all"):
        raise ValueError("kind must be delta, daily, recheck, or all")
    return result

# -*- coding: utf-8 -*-
"""Amazon Europe automated compliance / fitment risk feedback cards.

P0 scope:
- run deterministic risk scanning from candidate fields;
- send a Frankie-only card with automatic findings, evidence, and suggested actions;
- humans handle exceptions only: confirm system suggestion, mark false positive,
  request procurement evidence, or escalate for manual compliance review.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from . import amz_assistant, amz_procurement_quote as proc


BJ = timezone(timedelta(hours=8))

ACTION_SUBMIT = "amz_fit_check_feedback_submit"
ACTION_LEGACY_SUBMIT = "amz_fit_check_submit"

DEFAULT_BATCH_ID = os.environ.get("AMZ_COMPLIANCE_DEFAULT_BATCH_ID", "AMZ-DE-FITCHECK-20260723-P0")
DEFAULT_RECORD_IDS = [
    x.strip()
    for x in os.environ.get(
        "AMZ_COMPLIANCE_DEFAULT_RECORD_IDS",
        "recvq1QtafnVjX,recvq1QtUEEcXv",
    ).split(",")
    if x.strip()
]
FRANKIE_ONLY = (os.environ.get("AMZ_COMPLIANCE_CARD_FRANKIE_ONLY", "1") or "1") != "0"
FRANKIE_UNION_ID = os.environ.get("AMZ_REVIEW_OBSERVE_UNION", amz_assistant.FRANKIE_UNION_ID)
GRAY_UNION_IDS = [x.strip() for x in os.environ.get("AMZ_COMPLIANCE_GRAY_UNION_IDS", "").split(",") if x.strip()]
GRAY_CHAT_IDS = [x.strip() for x in os.environ.get("AMZ_COMPLIANCE_GRAY_CHAT_IDS", "").split(",") if x.strip()]

FIELD_NAMES = [
    "ASIN",
    "候选标题",
    "产品中文名",
    "Amazon链接",
    "样本ASIN主图URL",
    "包装尺寸",
    "商品重量g",
    "套装件数",
    "套装内容",
    "采购成本RMB",
    "1688供应商链接",
    "采购链接",
    "三方案推荐履约",
    "FBA€",
    "佣金€",
    "A-物流成本RMB",
    "A-货运比",
    "A-毛利RMB",
    "A-毛利率%",
    "B-物流成本RMB",
    "B-货运比",
    "B-毛利RMB",
    "B-毛利率%",
    "C-物流成本RMB",
    "C-货运比",
    "C-毛利RMB",
    "C-毛利率%",
    "财务闸结论",
    "合规闸结论",
    "IP/外观风险",
    "侵权风险说明",
    "当前状态",
    "综合结论",
    "数据缺口",
    "下一步动作",
    "人审备注",
]

IP_RISKS = ("低", "中", "高", "不可做")
ACTION_ACCEPT_AUTO = "采纳系统建议，自动进入下一步"
ACTION_FALSE_POSITIVE = "系统判断有误，退回复核"
ACTION_NEED_PROCUREMENT = "资料不够，采购补资料"
ACTION_ESCALATE_REVIEW = "风险较高，升级合规复核"
HUMAN_ACTIONS = (
    ACTION_ACCEPT_AUTO,
    ACTION_FALSE_POSITIVE,
    ACTION_NEED_PROCUREMENT,
    ACTION_ESCALATE_REVIEW,
)
DONE_GATES = ("Go", "暂缓", "No-Go")

_bg_tasks: set[asyncio.Task] = set()
_recent_callbacks: dict[str, float] = {}


def _now_ms() -> int:
    return int(time.time() * 1000)


def _now_label() -> str:
    return datetime.now(BJ).strftime("%Y-%m-%d %H:%M")


def _text(value: Any) -> str:
    return proc._text(value)


def _safe_id(value: str) -> str:
    return proc._safe_id(value)


def _field(label: str, value: Any) -> dict:
    return proc._field(label, value)


def _url_button(text: str, url: str, typ: str = "default") -> dict:
    return {"tag": "button", "text": {"tag": "plain_text", "content": text}, "type": typ, "url": url}


def _button_option(value: str) -> dict:
    return {"text": {"tag": "plain_text", "content": value}, "value": value}


def _action_help_text() -> str:
    return (
        "**怎么选**\n"
        f"- **{ACTION_ACCEPT_AUTO}**：同意系统判断；系统会按自动扫描结果写入“推进50件验证 / 暂缓补资料 / 淘汰”。\n"
        f"- **{ACTION_FALSE_POSITIVE}**：系统把正常点误判成风险；备注写明为什么是误报，系统先退回复核，不直接推进。\n"
        f"- **{ACTION_NEED_PROCUREMENT}**：缺供应商链接、包装图、实物图、标签/说明书、套装件数或适配型号；采购补齐后再重扫。\n"
        f"- **{ACTION_ESCALATE_REVIEW}**：涉及商标/IP、外观、专利、平台政策或欧盟合规高风险；先不要推进，升级复核。"
    )


def _candidate_from_record(record: dict) -> dict:
    base = proc._candidate_from_record(record)
    fields = record.get("fields") or {}
    base.update(
        {
            "current_status": " / ".join(proc._list_values(fields.get("当前状态"))) or "-",
            "overall_decision": " / ".join(proc._list_values(fields.get("综合结论"))) or "-",
            "finance_gate": " / ".join(proc._list_values(fields.get("财务闸结论"))) or "-",
            "compliance_gate": " / ".join(proc._list_values(fields.get("合规闸结论"))) or "待核",
            "ip_risk": " / ".join(proc._list_values(fields.get("IP/外观风险"))) or "待核",
            "risk_note": _text(fields.get("侵权风险说明")),
            "data_gaps": proc._list_values(fields.get("数据缺口")),
            "next_action": " / ".join(proc._list_values(fields.get("下一步动作"))) or "-",
            "review_note": _text(fields.get("人审备注")),
        }
    )
    return base


def _path(record_id: str = "") -> str:
    return proc._path(record_id)


async def _feishu_api(method: str, path: str, body: dict | None = None) -> dict:
    return await proc._feishu_api(method, path, body)


async def _get_candidate(record_id: str) -> dict:
    data = await _feishu_api("GET", _path(record_id))
    record = ((data.get("data") or {}).get("record") or {})
    return _candidate_from_record(record)


async def _update_candidate(record_id: str, fields: dict) -> None:
    await proc._update_candidate(record_id, fields)


async def _search_candidates(batch_id: str = "", limit: int = 2) -> list[dict]:
    conditions = [
        {"field_name": "当前状态", "operator": "contains", "value": ["待合规核查"]},
    ]
    body = {
        "page_size": min(max(int(limit or 2), 1), 20),
        "field_names": FIELD_NAMES,
        "filter": {"conjunction": "and", "conditions": conditions},
    }
    data = await _feishu_api("POST", _path() + "/search", body)
    rows = ((data.get("data") or {}).get("items") or [])
    return [_candidate_from_record(row) for row in rows]


async def _get_candidates_by_ids(record_ids: list[str]) -> list[dict]:
    out = []
    for rid in record_ids:
        if rid:
            out.append(await _get_candidate(rid))
    return out


async def _prepare_card_images(candidates: list[dict]) -> None:
    await proc._prepare_card_images(candidates)


def _record_url(record_id: str) -> str:
    return proc._record_url(record_id)


def _completed(candidate: dict) -> bool:
    gate = _text(candidate.get("compliance_gate"))
    return gate in DONE_GATES


def _payload(candidate: dict, card_record_ids: list[str]) -> dict:
    scan = candidate.get("risk_scan") or scan_candidate(candidate)
    return {
        "source": "amz_compliance_fit",
        "action": ACTION_SUBMIT,
        "record_id": candidate.get("record_id"),
        "asin": candidate.get("asin"),
        "batch_id": candidate.get("fit_batch_id") or DEFAULT_BATCH_ID,
        "card_record_ids": card_record_ids,
        "auto_risk_level": scan.get("level"),
        "auto_decision": scan.get("decision"),
        "auto_score": scan.get("score"),
    }


BRAND_TERMS = (
    "Dreame",
    "Xiaomi",
    "Roborock",
    "Dyson",
    "iRobot",
    "Roomba",
    "Shark",
    "Miele",
    "Bosch",
    "Philips",
    "Braun",
    "Oral-B",
    "Karcher",
)
COMPATIBLE_TERMS = ("replacement", "compatible", "fit", "fits", "for ", "ersatz", "zubehor", "zubehör", "适配", "替换", "兼容")
ORIGINAL_CLAIM_TERMS = ("original", "official", "genuine", "authentic", "oem", "正版", "原装", "原厂", "官方")
EU_RESTRICTED_TERMS = (
    "battery",
    "charger",
    "adapter",
    "power supply",
    "toy",
    "child",
    "kids",
    "food contact",
    "cosmetic",
    "medical",
    "adult",
    "sex",
    "电池",
    "充电",
    "儿童",
    "食品接触",
    "化妆",
    "医疗",
    "成人",
)
CONSUMABLE_TERMS = ("filter", "brush", "roller", "mop", "dust bag", "滤网", "刷", "滚刷", "拖布", "尘袋")


def _combined_text(candidate: dict) -> str:
    parts = [
        candidate.get("title"),
        candidate.get("cn_name"),
        candidate.get("set_content"),
        candidate.get("package_size"),
        candidate.get("fulfillment"),
    ]
    return " ".join(_text(x) for x in parts if _text(x))


def _term_hits(text: str, terms: tuple[str, ...]) -> list[str]:
    hits = []
    lower = text.lower()
    for term in terms:
        if term.lower() in lower:
            hits.append(term)
    return hits


def _issue(severity: str, dimension: str, finding: str, evidence: str, action: str) -> dict:
    return {
        "severity": severity,
        "dimension": dimension,
        "finding": finding,
        "evidence": evidence,
        "action": action,
    }


def scan_candidate(candidate: dict) -> dict:
    """Run P0 automated risk scan.

    This is not legal advice. It produces business risk signals and evidence so
    operators do not need to inspect the product from scratch.
    """
    text = _combined_text(candidate)
    title = _text(candidate.get("title")) or _text(candidate.get("cn_name")) or "-"
    issues: list[dict] = []

    brand_hits = _term_hits(text, BRAND_TERMS)
    compatible_hits = _term_hits(text, COMPATIBLE_TERMS)
    original_hits = _term_hits(text, ORIGINAL_CLAIM_TERMS)
    restricted_hits = _term_hits(text, EU_RESTRICTED_TERMS)
    consumable_hits = _term_hits(text, CONSUMABLE_TERMS)

    if brand_hits:
        sev = "高" if original_hits else "中"
        issues.append(
            _issue(
                sev,
                "品牌词/IP",
                f"识别到兼容品牌词：{', '.join(sorted(set(brand_hits)))}",
                f"候选标题/套装内容：{proc._short(title, 140)}",
                "Listing、包装和说明书只能写兼容/适配关系，不能出现原厂、官方、正版或品牌Logo暗示。",
            )
        )
    if brand_hits and not compatible_hits:
        issues.append(
            _issue(
                "高",
                "型号适配",
                "出现品牌/型号词，但未识别到 replacement/compatible/for/适配 等兼容关系表达。",
                f"文本：{proc._short(text, 180)}",
                "改标题和包装文案，明确为第三方兼容配件；进入 50 件验证前必须复核。",
            )
        )
    if original_hits:
        issues.append(
            _issue(
                "高",
                "商标/误导",
                f"识别到可能暗示原厂/正版的词：{', '.join(sorted(set(original_hits)))}",
                f"文本：{proc._short(text, 180)}",
                "删除原厂/官方/正版/OEM等表述；确认供应商图片和包装无品牌Logo。",
            )
        )
    if not candidate.get("supplier_link"):
        issues.append(
            _issue(
                "中",
                "采购资料",
                "缺少1688供应商链接，无法自动核对供应商图、包装和套装。",
                "候选表未提供供应商链接。",
                "要求采购补供应商链接、实物图、包装图后重跑自动扫描。",
            )
        )
    if not candidate.get("package_size") or not candidate.get("weight_g"):
        issues.append(
            _issue(
                "中",
                "型号/物流资料",
                "包装尺寸或重量缺失，型号适配和履约成本复核不完整。",
                f"尺寸={candidate.get('package_size') or '-'}，重量={candidate.get('weight_g') or '-'}",
                "补齐尺寸重量后再进入 50 件验证。",
            )
        )
    if not candidate.get("set_count") or not candidate.get("set_content"):
        issues.append(
            _issue(
                "中",
                "套装适配",
                "套装件数或套装内容缺失，采购无法稳定对齐 Amazon 主图和供应商报价。",
                f"件数={candidate.get('set_count') or '-'}，套装内容={proc._short(candidate.get('set_content'), 120) or '-'}",
                "补套装件数、套装内容、适配型号后再重算风险。",
            )
        )
    if consumable_hits and brand_hits:
        issues.append(
            _issue(
                "中",
                "外观/专利线索",
                "第三方耗材适配知名品牌设备，外观/卡扣/滤网结构可能存在设计或专利线索。",
                f"耗材词：{', '.join(sorted(set(consumable_hits)))}；品牌词：{', '.join(sorted(set(brand_hits)))}",
                "P0 可继续做商业筛选，但进入样品前需核主图、包装、实物是否带Logo或过度复刻原厂外观。",
            )
        )
    if restricted_hits:
        issues.append(
            _issue(
                "高",
                "EU合规/限制类",
                f"识别到可能触发欧盟特殊合规的词：{', '.join(sorted(set(restricted_hits)))}",
                f"文本：{proc._short(text, 180)}",
                "需补对应认证/警示/说明书；高风险品不得自动进入验证。",
            )
        )
    issues.append(
        _issue(
            "低",
            "EU/GPSR",
            "欧洲站上架前需要准备 GPSR 责任人、德语/当地语言标签、警示语、包装和说明书信息。",
            "P0 候选表尚未记录这些资料。",
            "进入 50 件验证前建立包装/标签资料清单；低客单小配件也不能跳过。",
        )
    )

    score_map = {"低": 10, "中": 25, "高": 45, "不可做": 100}
    score = min(100, sum(score_map.get(x.get("severity"), 0) for x in issues))
    max_sev = "低"
    for sev in ("不可做", "高", "中"):
        if any(x.get("severity") == sev for x in issues):
            max_sev = sev
            break
    if any(x.get("severity") == "不可做" for x in issues) or score >= 90:
        decision = "reject_recommended"
        decision_label = "建议淘汰/不推进"
        next_action = "淘汰归档或升级合规复核"
    elif max_sev in ("高", "中") or score >= 25:
        decision = "review_required"
        decision_label = "暂缓，先处理风险点"
        next_action = "按自动风险点补资料/改文案后重扫"
    else:
        decision = "auto_pass"
        decision_label = "自动低风险，可进入50件验证"
        next_action = "发起50件验证"
    return {
        "score": score,
        "level": max_sev,
        "decision": decision,
        "decision_label": decision_label,
        "next_action": next_action,
        "brand_hits": sorted(set(brand_hits)),
        "issues": issues,
    }


def _attach_risk_scans(candidates: list[dict]) -> None:
    for candidate in candidates:
        candidate["risk_scan"] = scan_candidate(candidate)


def _severity_icon(severity: str) -> str:
    return {"低": "🟢", "中": "🟡", "高": "🟠", "不可做": "🔴"}.get(_text(severity), "🟡")


def _scan_issue_lines(scan: dict, limit: int = 6) -> list[str]:
    lines = []
    for issue in (scan.get("issues") or [])[:limit]:
        lines.append(
            f"{_severity_icon(issue.get('severity'))} **{issue.get('dimension')}**："
            f"{issue.get('finding')}｜证据：{issue.get('evidence')}｜建议：{issue.get('action')}"
        )
    return lines or ["🟢 未发现需要运营处理的明显风险点。"]


def _scan_summary(scan: dict) -> str:
    return f"{scan.get('level')}风险 / {scan.get('score')}分 / {scan.get('decision_label')}"


def _scan_note(scan: dict, note: str = "") -> str:
    lines = [
        f"自动风险扫描：{_scan_summary(scan)}",
        "问题点：",
    ]
    for issue in (scan.get("issues") or [])[:8]:
        lines.append(f"- [{issue.get('severity')}] {issue.get('dimension')}: {issue.get('finding')} 建议: {issue.get('action')}")
    if note:
        lines.append(f"人工反馈：{note}")
    return "\n".join(lines)


def _line_item(label: str, value: Any) -> str:
    return f"**{label}**: {_text(value) or '-'}"


def _margin_line(candidate: dict) -> str:
    channels = [c for c in (candidate.get("channels") or []) if proc._channel_has_data(c)]
    if not channels:
        return "A/B/C 毛利暂缺"
    parts = []
    for channel in channels:
        suffix = proc._recommended_suffix(channel, candidate.get("fulfillment"))
        parts.append(
            f"{channel.get('code')}{suffix}: "
            f"{proc._format_rmb(channel.get('margin_rmb'))}/{proc._format_rate(channel.get('margin_rate'))}"
        )
    return " ｜ ".join(parts)


def _risk_hint(candidate: dict) -> str:
    title = " ".join(
        x
        for x in [
            _text(candidate.get("title")),
            _text(candidate.get("cn_name")),
            _text(candidate.get("set_content")),
        ]
        if x
    )
    brand_hits = []
    for name in ("Dreame", "Xiaomi", "Roborock", "Dyson"):
        if re.search(name, title, re.I):
            brand_hits.append(name)
    brand_text = "、".join(brand_hits) if brand_hits else "兼容品牌词待核"
    return (
        f"型号适配：按 Listing、主图、1688实物和套装件数核对；品牌/型号词：{brand_text}，只能写兼容，不能暗示原厂；"
        "欧洲上架还需核 GPSR 负责人、警示/标签、包装和说明书语言。"
    )


def _product_elements(candidate: dict, card_record_ids: list[str]) -> list[dict]:
    rid = candidate.get("record_id", "")
    sid = _safe_id(rid)
    completed = _completed(candidate)
    scan = candidate.get("risk_scan") or scan_candidate(candidate)
    title = candidate.get("cn_name") or candidate.get("title") or candidate.get("asin") or rid
    amazon = candidate.get("amazon_url")
    image = candidate.get("image_url")
    supplier = candidate.get("supplier_link")
    elements: list[dict] = [
        {"tag": "hr"},
        {"tag": "div", "text": {"tag": "lark_md", "content": f"**{title}**\n{proc._short(candidate.get('title'), 180)}"}},
    ]
    if candidate.get("image_key"):
        elements.append(
            {
                "tag": "img",
                "img_key": candidate["image_key"],
                "alt": {"tag": "plain_text", "content": f"{title} 主图"},
                "mode": "fit_horizontal",
                "preview": True,
            }
        )
    elements.append(
        {
            "tag": "div",
            "fields": [
                _field("ASIN", candidate.get("asin")),
                _field("当前状态", candidate.get("current_status")),
                _field("建议履约", candidate.get("fulfillment")),
                _field("采购成本", proc._format_rmb(candidate.get("quote_cost"))),
                _field("包装尺寸", candidate.get("package_size") or "待核"),
                _field("重量", f"{candidate.get('weight_g')}g" if candidate.get("weight_g") else "待核"),
                _field("件数", candidate.get("set_count") or "待核"),
                _field("FBA配送费 / 佣金", f"{proc._format_eur(candidate.get('fba_fee_eur'))} / {proc._format_eur(candidate.get('commission_eur'))}"),
            ],
        }
    )
    elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "**三渠道毛利**\n" + _margin_line(candidate)}})
    elements.append(
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": (
                    "**自动风险扫描结果**\n"
                    f"- 系统判断：{_scan_summary(scan)}\n"
                    f"- 系统建议：{scan.get('next_action')}\n"
                    "- 说明：这是自动风险线索，不是法律结论；人只处理系统发现的例外。"
                ),
            },
        }
    )
    elements.append(
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": "**自动发现的问题点**\n" + "\n".join(f"- {line}" for line in _scan_issue_lines(scan)),
            },
        }
    )
    elements.append(
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": f"**套装内容/采购注意**\n{candidate.get('set_content') or '待按主图和供应商页核对'}",
            },
        }
    )
    actions = []
    if amazon:
        actions.append(_url_button("打开 Listing", amazon, "primary"))
    if image:
        actions.append(_url_button("查看主图原图", image))
    actions.append(_url_button("打开候选表记录", _record_url(rid)))
    if supplier:
        actions.append(_url_button("打开1688供应商", supplier))
    elements.append({"tag": "action", "actions": actions})
    if completed:
        elements.append(
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": (
                        "**自动风险处理已完成**\n"
                        f"{_line_item('合规结论', candidate.get('compliance_gate'))}\n"
                        f"{_line_item('IP/外观风险', candidate.get('ip_risk'))}\n"
                        f"{_line_item('说明', candidate.get('risk_note'))}\n"
                        f"{_line_item('下一步', candidate.get('next_action'))}"
                    ),
                },
            }
        )
        return elements
    elements.append(
        {
            "tag": "form",
            "name": f"risk_feedback_form_{sid}",
            "elements": [
                {
                    "tag": "select_static",
                    "name": f"risk_action_{sid}",
                    "placeholder": {"tag": "plain_text", "content": "选择处理动作（先看系统建议和问题点）"},
                    "options": [_button_option(x) for x in HUMAN_ACTIONS],
                },
                {
                    "tag": "input",
                    "name": f"risk_note_{sid}",
                    "label_position": "left",
                    "label": {"tag": "plain_text", "content": "处理备注"},
                    "placeholder": {"tag": "plain_text", "content": "按所选动作填写：误报原因 / 采购需补资料 / 升级复核原因"},
                },
                {
                    "tag": "button",
                    "action_type": "form_submit",
                    "name": f"risk_submit_{sid}",
                    "type": "primary",
                    "text": {"tag": "plain_text", "content": "提交处理动作"},
                    "value": _payload(candidate, card_record_ids),
                },
            ],
        }
    )
    return elements


def build_fit_card(candidates: list[dict], batch_id: str = "") -> dict:
    batch = batch_id or DEFAULT_BATCH_ID
    total = len(candidates)
    done = sum(1 for item in candidates if _completed(item))
    pending = total - done
    template = "green" if total and pending == 0 else "yellow"
    title_status = "已全部核查" if total and pending == 0 else f"待核查 {pending}/{total}"
    record_ids = [c.get("record_id", "") for c in candidates if c.get("record_id")]
    elements: list[dict] = [
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": (
                    f"**批次**: {batch}\n"
                    f"**状态**: {title_status}\n"
                    "**定位**: 系统先自动扫描型号适配、兼容品牌词、IP/外观、专利线索和EU/GPSR资料缺口；卡片只反馈系统发现的问题点，不要求采购或运营从零核查。\n\n"
                    + _action_help_text()
                ),
            },
        },
        {"tag": "note", "elements": [{"tag": "plain_text", "content": "P0 默认只发 Frankie 样卡确认。低风险或处理完风险点后才进入 50 件验证，不代表已正式上架或法律无风险。"}]},
    ]
    for candidate in candidates:
        elements.extend(_product_elements(candidate, record_ids))
    return {
        "config": {"wide_screen_mode": True, "update_multi": True},
        "header": {
            "template": template,
            "title": {"tag": "plain_text", "content": f"🟡 [AMZ·P0] 德国站合规/适配核查 · {title_status}"},
        },
        "elements": elements,
    }


def _card_text(value: Any) -> str:
    return proc._card_text(value)


def _card_nodes(value: Any):
    yield from proc._card_nodes(value)


def validate_fit_card(card: dict, candidates: list[dict]) -> list[str]:
    errors: list[str] = []
    nodes = list(_card_nodes(card))
    rendered = json.dumps(card, ensure_ascii=False)
    buttons = [n for n in nodes if n.get("tag") == "button"]
    forms = {n.get("name"): n for n in nodes if n.get("tag") == "form" and n.get("name")}

    def url_button_exists(label: str, expected_url: str) -> bool:
        for button in buttons:
            if _card_text(button.get("text")) != label:
                continue
            url = _text(button.get("url"))
            if url == expected_url and url.startswith(("http://", "https://")):
                return True
        return False

    for candidate in candidates:
        rid = candidate.get("record_id") or ""
        sid = _safe_id(rid)
        label = candidate.get("asin") or rid or "unknown"
        if candidate.get("amazon_url") and not url_button_exists("打开 Listing", candidate["amazon_url"]):
            errors.append(f"{label}: missing or invalid Amazon Listing button")
        if candidate.get("image_url") and not url_button_exists("查看主图原图", candidate["image_url"]):
            errors.append(f"{label}: missing or invalid image button")
        if not url_button_exists("打开候选表记录", _record_url(rid)):
            errors.append(f"{label}: missing or invalid candidate-record button")
        if candidate.get("supplier_link") and not url_button_exists("打开1688供应商", candidate["supplier_link"]):
            errors.append(f"{label}: missing or invalid supplier button")
        if _completed(candidate):
            continue
        form_name = f"risk_feedback_form_{sid}"
        form = forms.get(form_name)
        if not form:
            errors.append(f"{label}: missing form {form_name}")
            continue
        form_elements = form.get("elements") or []
        names = {x.get("name"): x.get("tag") for x in form_elements if isinstance(x, dict) and x.get("name")}
        expected = {
            f"risk_action_{sid}": "select_static",
            f"risk_note_{sid}": "input",
        }
        for name, tag in expected.items():
            if names.get(name) != tag:
                errors.append(f"{label}: missing {tag} {name}")
        submit = None
        for item in form_elements:
            if isinstance(item, dict) and item.get("tag") == "button" and item.get("action_type") == "form_submit":
                submit = item
                break
        if not submit:
            errors.append(f"{label}: missing form_submit button")
            continue
        value = submit.get("value") or {}
        if _text(value.get("action")) != ACTION_SUBMIT:
            errors.append(f"{label}: submit payload action is invalid")
        if _text(value.get("record_id")) != rid:
            errors.append(f"{label}: submit payload record_id is invalid")
        record_ids = [_text(x) for x in (value.get("card_record_ids") or []) if _text(x)]
        expected_ids = [c.get("record_id") for c in candidates if c.get("record_id")]
        if record_ids != expected_ids:
            errors.append(f"{label}: submit payload card_record_ids is invalid")
    if "fit_result_" in rendered or "选择IP/外观风险" in rendered or "确认核查本产品" in rendered:
        errors.append("card still contains legacy manual compliance controls")
    for required in (
        "自动风险扫描结果",
        "自动发现的问题点",
        "系统建议",
        "三渠道毛利",
        "GPSR",
        "人只处理系统发现的例外",
        "怎么选",
        ACTION_ACCEPT_AUTO,
        ACTION_FALSE_POSITIVE,
        ACTION_NEED_PROCUREMENT,
        ACTION_ESCALATE_REVIEW,
    ):
        if required not in rendered:
            errors.append(f"card missing {required}")
    return errors


def _extract_action(event: dict) -> tuple[str, dict, dict]:
    return proc._extract_action(event)


def _merge_form_values(out: dict[str, str], raw: Any) -> None:
    raw = proc._jsonish(raw)
    if not raw:
        return
    if isinstance(raw, list):
        for item in raw:
            item = proc._jsonish(item)
            if isinstance(item, dict):
                name = _text(item.get("name") or item.get("key") or item.get("id") or item.get("field"))
                has_value = any(k in item for k in ("value", "input_value", "selected_value", "text", "content", "link", "url"))
                if name and has_value:
                    out[name] = proc._form_scalar(item)
                _merge_form_values(out, item)
        return
    if not isinstance(raw, dict):
        return
    wrapper_keys = {"form_value", "form_values", "card_form_value", "input_values", "inputs", "form", "fields", "elements"}
    field_prefixes = ("fit_", "amz_fit_", "risk_")
    for key, value in raw.items():
        key_text = _text(key)
        value = proc._jsonish(value)
        if key_text in wrapper_keys:
            _merge_form_values(out, value)
            continue
        if key_text.startswith(field_prefixes) or key_text in ("result", "iprisk", "note"):
            out[key_text] = proc._form_scalar(value)
        if isinstance(value, (dict, list, str)):
            _merge_form_values(out, value)


def _extract_form_values(event: dict, action: dict | None = None) -> dict[str, str]:
    action = action or event.get("action") or {}
    out: dict[str, str] = {}
    for raw in (
        action.get("form_value"),
        action.get("form_values"),
        action.get("input_values"),
        action.get("inputs"),
        event.get("card_form_value"),
        event.get("form_value"),
        event.get("form_values"),
        event.get("input_values"),
        event.get("inputs"),
    ):
        _merge_form_values(out, raw)
    return out


def _form_value(form: dict, record_id: str, suffix: str) -> str:
    sid = _safe_id(record_id)
    keys = [f"risk_{suffix}_{sid}", f"fit_{suffix}_{sid}", f"amz_fit_{suffix}_{sid}", suffix]
    for key in keys:
        if key in form:
            return _text(form.get(key))
    for key, value in form.items():
        if key.startswith(f"risk_{suffix}_") or key.startswith(f"fit_{suffix}_") or key.startswith(f"amz_fit_{suffix}_"):
            return _text(value)
    return ""


def _normalize_human_action(raw: str) -> str:
    text = _text(raw)
    aliases = {
        "确认": ACTION_ACCEPT_AUTO,
        "确认建议": ACTION_ACCEPT_AUTO,
        "确认系统判断": ACTION_ACCEPT_AUTO,
        "确认系统建议": ACTION_ACCEPT_AUTO,
        "误报": ACTION_FALSE_POSITIVE,
        "标记误报": ACTION_FALSE_POSITIVE,
        "标记系统误报": ACTION_FALSE_POSITIVE,
        "采购补资料": ACTION_NEED_PROCUREMENT,
        "补资料": ACTION_NEED_PROCUREMENT,
        "要求采购补资料": ACTION_NEED_PROCUREMENT,
        "升级": ACTION_ESCALATE_REVIEW,
        "人工复核": ACTION_ESCALATE_REVIEW,
        "升级合规复核": ACTION_ESCALATE_REVIEW,
    }
    return aliases.get(text, text)


def _risk_for_write(level: str) -> str:
    level = _text(level)
    return level if level in IP_RISKS else "中"


def _message_id(event: dict) -> str:
    return proc._message_id(event)


def _operator_label(event: dict) -> str:
    return proc._operator_label(event)


def _toast(content: str, typ: str = "success") -> dict:
    return proc._toast(content, typ)


def _spawn(coro) -> None:
    task = asyncio.create_task(coro)
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


def _callback_key(record_id: str, form: dict) -> str:
    text = json.dumps(form or {}, ensure_ascii=False, sort_keys=True)
    return f"{record_id}:{hash(text)}"


def _recent_seen(key: str, ttl_sec: int = 300) -> bool:
    now = time.time()
    for old, ts in list(_recent_callbacks.items()):
        if now - ts > ttl_sec:
            _recent_callbacks.pop(old, None)
    return key in _recent_callbacks and now - _recent_callbacks[key] <= ttl_sec


def _auto_decision_fields(scan: dict) -> dict:
    risk = _risk_for_write(scan.get("level"))
    if scan.get("decision") == "auto_pass":
        return {
            "合规闸结论": "Go",
            "IP/外观风险": risk,
            "当前状态": "待50件验证",
            "综合结论": "50件验证",
            "下一步动作": "发起50件验证",
            "数据缺口": [],
        }
    if scan.get("decision") == "reject_recommended":
        return {
            "合规闸结论": "No-Go",
            "IP/外观风险": risk,
            "当前状态": "淘汰",
            "综合结论": "淘汰",
            "下一步动作": "淘汰归档",
            "数据缺口": ["认证"],
        }
    return {
        "合规闸结论": "暂缓",
        "IP/外观风险": risk,
        "当前状态": "待合规核查",
        "综合结论": "暂缓",
        "下一步动作": scan.get("next_action") or "按自动风险点补资料/改文案后重扫",
        "数据缺口": ["认证"],
    }


def _build_update_fields(human_action: str, note: str, actor: str, scan: dict) -> dict:
    reviewed = (
        f"{_now_label()} {actor}: 自动扫描={_scan_summary(scan)}; "
        f"处理动作={human_action}; 备注={note or '-'}"
    )
    fields = _auto_decision_fields(scan) if human_action == ACTION_ACCEPT_AUTO else {
        "合规闸结论": "暂缓",
        "IP/外观风险": _risk_for_write(scan.get("level")),
        "当前状态": "待合规核查",
        "综合结论": "暂缓",
        "数据缺口": ["认证"],
    }
    if human_action == ACTION_FALSE_POSITIVE:
        fields["下一步动作"] = "复核系统误报后重跑扫描"
    elif human_action == ACTION_NEED_PROCUREMENT:
        fields["下一步动作"] = "采购补供应商/包装/实物资料后重跑扫描"
        fields["数据缺口"] = ["认证", "供应商资料"]
    elif human_action == ACTION_ESCALATE_REVIEW:
        fields["下一步动作"] = "升级合规/IP复核"
    fields["侵权风险说明"] = _scan_note(scan, note)
    fields["人审备注"] = reviewed
    return fields


async def _process_callback_background(event: dict, callback_key: str) -> None:
    try:
        result = await _process_callback(event)
        if ((result.get("toast") or {}).get("type") or "") == "error":
            _recent_callbacks.pop(callback_key, None)
    except Exception as exc:
        _recent_callbacks.pop(callback_key, None)
        print(f"[amz_compliance_fit.callback_bg] {callback_key} fail: {exc}")


async def _process_callback(event: dict) -> dict:
    action, value, _ = _extract_action(event)
    form = _extract_form_values(event, event.get("action") or {})
    if action == ACTION_LEGACY_SUBMIT:
        return _toast("旧人工核查卡已停用，请使用新的自动风险扫描结果卡", "error")
    if action != ACTION_SUBMIT:
        return _toast("未知自动风险处理动作", "error")
    record_id = _text(value.get("record_id"))
    if not record_id:
        return _toast("缺少候选记录ID", "error")
    human_action = _normalize_human_action(_form_value(form, record_id, "action"))
    note = _form_value(form, record_id, "note")
    if human_action not in HUMAN_ACTIONS:
        return _toast("请选择一个处理动作", "error")
    if human_action in (ACTION_FALSE_POSITIVE, ACTION_ESCALATE_REVIEW) and not note:
        return _toast("退回复核或升级复核时必须填写处理备注", "error")

    candidate = await _get_candidate(record_id)
    scan = scan_candidate(candidate)
    msg_id = _message_id(event) or candidate.get("fit_message_id")
    actor = _operator_label(event)
    fields = _build_update_fields(human_action, note, actor, scan)
    await _update_candidate(record_id, fields)
    candidate.update(
        {
            "compliance_gate": fields.get("合规闸结论"),
            "ip_risk": fields.get("IP/外观风险"),
            "risk_note": fields.get("侵权风险说明"),
            "current_status": fields.get("当前状态"),
            "overall_decision": fields.get("综合结论"),
            "next_action": fields.get("下一步动作"),
            "review_note": fields.get("人审备注"),
            "data_gaps": fields.get("数据缺口") or [],
            "risk_scan": scan,
        }
    )
    record_ids = [x for x in (value.get("card_record_ids") or []) if _text(x)]
    if msg_id:
        if record_ids:
            candidates = await _get_candidates_by_ids(record_ids)
            for idx, item in enumerate(candidates):
                if item.get("record_id") == record_id:
                    candidates[idx] = candidate
                    break
            await _prepare_card_images(candidates)
            await amz_assistant.update_card(msg_id, build_fit_card(candidates, _text(value.get("batch_id"))))
        else:
            await amz_assistant.update_card(msg_id, build_fit_card([candidate], _text(value.get("batch_id"))))
    return _toast("本产品自动风险处理结果已写回")


async def handle_callback(event: dict) -> dict:
    action, value, _ = _extract_action(event)
    form = _extract_form_values(event, event.get("action") or {})
    if action == ACTION_LEGACY_SUBMIT:
        return _toast("旧人工核查卡已停用，请使用新的自动风险扫描结果卡", "error")
    if action != ACTION_SUBMIT:
        return {"ok": False, "ignored": True, "action": action}
    record_id = _text(value.get("record_id"))
    if not record_id:
        return _toast("缺少候选记录ID", "error")
    human_action = _normalize_human_action(_form_value(form, record_id, "action"))
    note = _form_value(form, record_id, "note")
    if human_action not in HUMAN_ACTIONS:
        return _toast("请选择一个处理动作", "error")
    if human_action in (ACTION_FALSE_POSITIVE, ACTION_ESCALATE_REVIEW) and not note:
        return _toast("退回复核或升级复核时必须填写处理备注", "error")
    callback_key = _callback_key(record_id, form)
    if _recent_seen(callback_key):
        try:
            current = await _get_candidate(record_id)
            if _completed(current):
                return _toast("该产品已核查，无需重复点击")
        except Exception as exc:
            print(f"[amz_compliance_fit.callback_duplicate_check] {record_id} fail: {exc}")
        _recent_callbacks.pop(callback_key, None)
        _recent_callbacks[callback_key] = time.time()
        _spawn(_process_callback_background(event, callback_key))
        return _toast("已重新收到本产品自动风险处理结果，正在补写候选表并更新原卡")
    _recent_callbacks[callback_key] = time.time()
    _spawn(_process_callback_background(event, callback_key))
    return _toast("已收到本产品自动风险处理结果，正在写回候选表并更新原卡")


async def send_fit_card(
    *,
    mode: str = "dry_run",
    limit: int = 2,
    batch_id: str = "",
    record_ids: list[str] | None = None,
    frankie_only: bool = True,
    gray_union_ids: list[str] | None = None,
    gray_chat_ids: list[str] | None = None,
) -> dict:
    if mode not in ("dry_run", "commit"):
        raise ValueError("mode must be dry_run or commit")
    batch = batch_id or DEFAULT_BATCH_ID
    ids = record_ids if record_ids is not None else DEFAULT_RECORD_IDS
    candidates = await _get_candidates_by_ids(ids) if ids else await _search_candidates(batch, limit=limit)
    _attach_risk_scans(candidates)
    if mode == "commit":
        await _prepare_card_images(candidates)
    card = build_fit_card(candidates, batch)
    validation_errors = validate_fit_card(card, candidates)
    if validation_errors:
        raise RuntimeError("Compliance fit card self-test failed: " + "; ".join(validation_errors))
    effective_frankie_only = bool(frankie_only or FRANKIE_ONLY)
    result: dict[str, Any] = {
        "ok": True,
        "mode": mode,
        "frankie_only": effective_frankie_only,
        "batch_id": batch,
        "count": len(candidates),
        "record_ids": [c.get("record_id") for c in candidates],
        "card_selftest": "passed",
        **proc._card_media_stats(candidates),
    }
    if mode == "dry_run":
        result["card"] = card
        return result
    if not candidates:
        result["sent"] = False
        result["message_id"] = ""
        return result
    message_ids: list[str] = []
    recipients: list[dict[str, str]] = []
    if effective_frankie_only:
        recipients.append({"type": "union_id", "id": FRANKIE_UNION_ID})
        msg_id = await amz_assistant.send_card_to_union(FRANKIE_UNION_ID, card)
        if msg_id:
            message_ids.append(msg_id)
    else:
        unions = [x for x in (gray_union_ids if gray_union_ids is not None else GRAY_UNION_IDS) if x]
        chats = [x for x in (gray_chat_ids if gray_chat_ids is not None else GRAY_CHAT_IDS) if x]
        if not unions and not chats:
            raise RuntimeError("Compliance gray recipients are not configured. Set AMZ_COMPLIANCE_GRAY_UNION_IDS or AMZ_COMPLIANCE_GRAY_CHAT_IDS.")
        for chat_id in chats:
            recipients.append({"type": "chat_id", "id": chat_id})
            msg_id = await amz_assistant.send_card_to_chat(chat_id, card)
            if msg_id:
                message_ids.append(msg_id)
        for union_id in unions:
            recipients.append({"type": "union_id", "id": union_id})
            msg_id = await amz_assistant.send_card_to_union(union_id, card)
            if msg_id:
                message_ids.append(msg_id)
    result["sent"] = bool(message_ids)
    result["message_id"] = message_ids[0] if message_ids else ""
    result["message_ids"] = message_ids
    result["recipients"] = recipients
    return result

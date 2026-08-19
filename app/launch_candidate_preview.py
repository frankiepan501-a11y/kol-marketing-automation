"""新品集中上稿活动候选预览与全局重复触达预检。

只读模块：只调用 Feishu 读取接口，不创建任务、草稿、卡片或邮件。
活动候选和日常派单共享联系人、评分与历史草稿，但活动决策单独输出，
避免把活动状态写回联系人主状态。
"""

from __future__ import annotations

import time
from collections import Counter, defaultdict

from . import config, dispatch, feishu
from .feishu import ext
from .scoring import _parse_multiselect, score_editor, score_kol


HARD_BLOCK_STATES = {"不合适", "黑名单"}
POSITIVE_RELATION_STATES = {
    "已合作", "已合作-免费", "已合作-免费(多次)", "已合作-付费",
    "洽谈中", "样品评估", "未产出",
}
REUSABLE_DRAFT_STATES = {"已否决", "发送失败"}
PROACTIVE_SOURCES = {"cold", "followup", "secondary_outreach"}
RECENT_SAME_BRAND_DAYS = 7

PRODUCT_FIELDS = [
    "产品名", "产品英文名", "品牌", "品类", "报价(USD)", "销售国家",
    "适配主机", "适配IP", "活动归并键", "活动主记录ID", "活动主记录",
]
KOL_FIELDS = [
    "账号名", "邮箱", "合作状态", "主平台", "国家", "语言", "粉丝数",
    "内容风格", "IP喜好", "合作竞品", "竞品帖子证据", "邮箱验真状态",
]
EDITOR_FIELDS = [
    "媒体人姓名", "所属媒体", "主要媒体", "邮箱", "合作状态", "国家", "语言",
    "媒体类型", "媒体集团", "报道品类", "邮箱验真状态",
]
DRAFT_FIELDS = [
    "关联KOL", "关联媒体人", "关联产品", "收件邮箱", "发送邮箱",
    "邮件草稿来源", "邮件草稿状态", "发送状态", "发送时间", "生成时间",
    "是否回复", "回复意图", "邮件草稿ID",
]


def _link_ids(value) -> set[str]:
    if isinstance(value, dict):
        return set(value.get("link_record_ids") or value.get("record_ids") or [])
    if isinstance(value, list):
        out = set()
        for item in value:
            if isinstance(item, str):
                out.add(item)
            elif isinstance(item, dict):
                out.update(item.get("link_record_ids") or item.get("record_ids") or [])
        return out
    return set()


def _canonical_id(product: dict) -> str:
    fields = product.get("fields") or {}
    return ext(fields.get("活动主记录ID")).strip() or product.get("record_id", "")


def canonical_product_family(product_id: str, products: list[dict]) -> dict:
    """把活动别名产品折叠到同一个主记录，防止换产品行绕过去重。"""
    target = next((p for p in products if p.get("record_id") == product_id), None)
    if not target:
        raise ValueError(f"product not found: {product_id}")
    canonical_id = _canonical_id(target)
    merge_key = ext((target.get("fields") or {}).get("活动归并键")).strip()
    members = []
    for product in products:
        fields = product.get("fields") or {}
        same_canonical = _canonical_id(product) == canonical_id
        same_merge_key = bool(merge_key and ext(fields.get("活动归并键")).strip() == merge_key)
        if same_canonical or same_merge_key:
            members.append(product.get("record_id", ""))
    members = sorted({x for x in members if x} | {canonical_id})
    return {
        "canonical_product_id": canonical_id,
        "merge_key": merge_key,
        "product_ids": members,
        "target": target,
    }


def _contact_id(draft_fields: dict, object_type: str) -> str:
    field = "关联媒体人" if object_type == "媒体人" else "关联KOL"
    ids = _link_ids(draft_fields.get(field))
    return sorted(ids)[0] if ids else ""


def _brand_of_draft(fields: dict) -> str:
    return config.brand_from_text(ext(fields.get("发送邮箱"))) or ""


def _is_nonterminal_or_sent(fields: dict) -> bool:
    status = ext(fields.get("邮件草稿状态"))
    send_status = ext(fields.get("发送状态"))
    return status not in REUSABLE_DRAFT_STATES or send_status in {"已发", "已发送"}


def _masked_email(email: str) -> str:
    if "@" not in email:
        return ""
    local, domain = email.split("@", 1)
    return (local[:2] + "***@" + domain) if local else ("***@" + domain)


def _email_owners(kols: list[dict], editors: list[dict]) -> dict[str, set[tuple[str, str]]]:
    owners: dict[str, set[tuple[str, str]]] = defaultdict(set)
    for object_type, records in (("KOL", kols), ("媒体人", editors)):
        for record in records:
            email, _ = feishu.clean_email(ext((record.get("fields") or {}).get("邮箱")))
            if email:
                owners[email].add((object_type, record.get("record_id", "")))
    return owners


def precheck_contact(
    contact: dict,
    *,
    object_type: str,
    brand: str,
    product_ids: set[str],
    drafts: list[dict],
    email_owners: dict[str, set[tuple[str, str]]],
    now_ms: int,
) -> dict:
    """对一位联系人做全局预检；返回可解释决策，不写任何表。"""
    fields = contact.get("fields") or {}
    rid = contact.get("record_id", "")
    email, email_note = feishu.clean_email(ext(fields.get("邮箱")))
    coop = ext(fields.get("合作状态"))
    email_state = ext(fields.get("邮箱验真状态"))
    reasons: list[str] = []
    evidence_drafts: list[str] = []

    if not email:
        reasons.append(f"邮箱无效: {email_note}")
    if email_state == "无效":
        reasons.append("邮箱验真状态=无效")
    if coop in HARD_BLOCK_STATES:
        reasons.append(f"合作状态={coop}")
    if reasons:
        return {
            "decision": "blocked", "allowed_as_new_cold": False,
            "recommended_route": "exclude", "reasons": reasons,
            "evidence_draft_ids": [], "email": _masked_email(email),
        }

    owners = email_owners.get(email, set())
    if len(owners) > 1:
        return {
            "decision": "hold_duplicate_identity", "allowed_as_new_cold": False,
            "recommended_route": "merge_identity_before_contact",
            "reasons": [f"同一邮箱对应 {len(owners)} 个 KOL/媒体人身份"],
            "evidence_draft_ids": [], "email": _masked_email(email),
        }

    same_product = []
    same_brand_active = []
    same_email_other_record = []
    cutoff = now_ms - RECENT_SAME_BRAND_DAYS * 86400 * 1000
    for draft in drafts:
        df = draft.get("fields") or {}
        source = ext(df.get("邮件草稿来源")) or "cold"
        if source not in PROACTIVE_SOURCES or not _is_nonterminal_or_sent(df):
            continue
        draft_object_type = "媒体人" if _link_ids(df.get("关联媒体人")) else "KOL"
        draft_contact_id = _contact_id(df, draft_object_type)
        draft_email, _ = feishu.clean_email(ext(df.get("收件邮箱")))
        same_identity = (
            (draft_object_type == object_type and draft_contact_id == rid)
            or bool(email and draft_email == email)
        )
        if not same_identity:
            continue
        if draft_contact_id and (draft_object_type != object_type or draft_contact_id != rid):
            same_email_other_record.append(draft)
        if _link_ids(df.get("关联产品")) & product_ids:
            same_product.append(draft)
        try:
            event_ms = int(df.get("发送时间") or df.get("生成时间") or 0)
        except (TypeError, ValueError):
            event_ms = 0
        if _brand_of_draft(df) == brand and (event_ms >= cutoff or ext(df.get("发送状态")) not in {"已发", "已发送"}):
            same_brand_active.append(draft)

    for draft in same_product + same_brand_active + same_email_other_record:
        did = draft.get("record_id", "")
        if did and did not in evidence_drafts:
            evidence_drafts.append(did)

    any_reply = any(bool((d.get("fields") or {}).get("是否回复")) for d in same_product)
    if same_product:
        if coop in POSITIVE_RELATION_STATES or any_reply:
            return {
                "decision": "reactivation_same_thread", "allowed_as_new_cold": False,
                "recommended_route": "continue_existing_thread",
                "reasons": ["同一活动产品家族已有触达记录，且存在正向关系/回复"],
                "evidence_draft_ids": evidence_drafts[:10], "email": _masked_email(email),
            }
        return {
            "decision": "blocked_prior_same_product", "allowed_as_new_cold": False,
            "recommended_route": "do_not_resend_cold",
            "reasons": ["同一活动产品家族已有有效 cold/follow-up 记录"],
            "evidence_draft_ids": evidence_drafts[:10], "email": _masked_email(email),
        }

    if same_email_other_record:
        return {
            "decision": "hold_duplicate_identity", "allowed_as_new_cold": False,
            "recommended_route": "merge_identity_before_contact",
            "reasons": ["历史草稿命中同邮箱的另一条 KOL/媒体人记录"],
            "evidence_draft_ids": evidence_drafts[:10], "email": _masked_email(email),
        }

    if same_brand_active:
        return {
            "decision": "hold_active_or_recent", "allowed_as_new_cold": False,
            "recommended_route": "wait_or_continue_existing_thread",
            "reasons": [f"同品牌 {RECENT_SAME_BRAND_DAYS} 天内已有触达或仍在流程中"],
            "evidence_draft_ids": evidence_drafts[:10], "email": _masked_email(email),
        }

    return {
        "decision": "eligible_new_cold", "allowed_as_new_cold": True,
        "recommended_route": "activity_cold_pool", "reasons": ["全局重复触达预检通过"],
        "evidence_draft_ids": [], "email": _masked_email(email),
    }


def _base_filter_kol(fields: dict, product_fields: dict, mapping: dict) -> tuple[bool, list[str]]:
    reasons = []
    countries = _parse_multiselect(product_fields.get("销售国家"))
    languages = {lang for country in countries for lang in dispatch.COUNTRY_TO_LANGS.get(country, [])}
    language_iso = {"英语": "en", "德语": "de", "西班牙语": "es", "法语": "fr", "葡萄牙语": "pt"}
    languages = {language_iso.get(x, x) for x in languages}
    platforms = set(dispatch.CATEGORY_PLATFORMS.get(ext(product_fields.get("品类")), []))
    expected_styles = set(mapping.get("expected_styles") or [])
    fans_min, fans_max = dispatch._fans_range_for_price(float(product_fields.get("报价(USD)") or 0))
    try:
        fans = int(fields.get("粉丝数") or 0)
    except (TypeError, ValueError):
        fans = 0
    if countries and ext(fields.get("国家")) not in countries:
        reasons.append("国家不在销售市场")
    if languages and ext(fields.get("语言")) not in languages:
        reasons.append("语言不匹配")
    if platforms and ext(fields.get("主平台")) not in platforms:
        reasons.append("主平台不匹配")
    if fans < fans_min or (fans_max and fans > fans_max):
        reasons.append("粉丝量级不匹配")
    if expected_styles and not (_parse_multiselect(fields.get("内容风格")) & expected_styles):
        reasons.append("内容风格不匹配")
    return not reasons, reasons


def _candidate_name(fields: dict, object_type: str) -> str:
    if object_type == "媒体人":
        return ext(fields.get("媒体人姓名")) or ext(fields.get("所属媒体")) or ext(fields.get("主要媒体"))
    return ext(fields.get("账号名"))


async def _load_context(product_id: str) -> dict:
    products = await feishu.fetch_all_records(config.T_PRODUCT, field_names=PRODUCT_FIELDS, page_size=100)
    family = canonical_product_family(product_id, products)
    kols = await feishu.fetch_all_records(config.T_KOL, field_names=KOL_FIELDS, page_size=500)
    editors = await feishu.fetch_all_records(config.T_EDITOR, field_names=EDITOR_FIELDS, page_size=500)
    drafts = await feishu.fetch_all_records(config.T_DRAFT, field_names=DRAFT_FIELDS, page_size=500)
    product_fields = family["target"].get("fields") or {}
    mapping = await dispatch.fetch_mapping_for_product(
        ext(product_fields.get("品类")), list(_parse_multiselect(product_fields.get("适配主机")))
    )
    return {
        "family": family, "kols": kols, "editors": editors, "drafts": drafts,
        "mapping": mapping, "owners": _email_owners(kols, editors),
    }


async def preview_candidates(product_id: str, *, object_type: str = "KOL", limit: int = 100) -> dict:
    if object_type not in {"KOL", "媒体人"}:
        raise ValueError("object_type must be KOL or 媒体人")
    limit = max(1, min(int(limit), 500))
    ctx = await _load_context(product_id)
    family = ctx["family"]
    product_fields = family["target"].get("fields") or {}
    brand = config.brand_from_text(ext(product_fields.get("品牌"))) or ext(product_fields.get("品牌")).upper()
    records = ctx["editors"] if object_type == "媒体人" else ctx["kols"]
    now_ms = int(time.time() * 1000)
    candidates = []
    filtered_out = 0

    for record in records:
        fields = record.get("fields") or {}
        if object_type == "KOL":
            matched, filter_reasons = _base_filter_kol(fields, product_fields, ctx["mapping"])
            if not matched:
                filtered_out += 1
                continue
            score, breakdown = score_kol(
                fields, product_fields, set(ctx["mapping"].get("expected_styles") or []),
                set(dispatch.CATEGORY_PLATFORMS.get(ext(product_fields.get("品类")), [])),
            )
        else:
            score, breakdown = score_editor(
                fields, product_fields,
                set(ctx["mapping"].get("expected_report_cats") or []),
                set(ctx["mapping"].get("expected_media_types") or []),
            )
            filter_reasons = []

        check = precheck_contact(
            record, object_type=object_type, brand=brand,
            product_ids=set(family["product_ids"]), drafts=ctx["drafts"],
            email_owners=ctx["owners"], now_ms=now_ms,
        )
        candidates.append({
            "contact_id": record.get("record_id", ""),
            "name": _candidate_name(fields, object_type),
            "platform": ext(fields.get("主平台")) if object_type == "KOL" else ext(fields.get("主要媒体")),
            "country": ext(fields.get("国家")), "language": ext(fields.get("语言")),
            "score": score, "breakdown": breakdown,
            "competitor_signal": (ext(fields.get("合作竞品"))[:300] if object_type == "KOL" else ""),
            "competitor_evidence": (ext(fields.get("竞品帖子证据"))[:500] if object_type == "KOL" else ""),
            **check,
        })

    decision_order = {
        "reactivation_same_thread": 0, "eligible_new_cold": 1,
        "hold_active_or_recent": 2, "hold_duplicate_identity": 3,
        "blocked_prior_same_product": 4, "blocked": 5,
    }
    candidates.sort(key=lambda x: (decision_order.get(x["decision"], 9), -float(x["score"] or 0), x["contact_id"]))
    counts = Counter(x["decision"] for x in candidates)
    return {
        "read_only": True, "writes": 0,
        "product": {
            "requested_product_id": product_id,
            "canonical_product_id": family["canonical_product_id"],
            "product_ids": family["product_ids"], "merge_key": family["merge_key"],
            "name": ext(product_fields.get("产品名")), "brand": brand,
        },
        "object_type": object_type,
        "summary": {
            "pool_records": len(records), "base_filter_excluded": filtered_out,
            "evaluated": len(candidates), "eligible_new_cold": counts["eligible_new_cold"],
            "reactivation_same_thread": counts["reactivation_same_thread"],
            "held_or_blocked": len(candidates) - counts["eligible_new_cold"] - counts["reactivation_same_thread"],
            "by_decision": dict(counts),
        },
        "candidates": candidates[:limit],
    }


async def replay_candidate(product_id: str, contact_id: str, *, object_type: str = "KOL") -> dict:
    result = await preview_candidates(product_id, object_type=object_type, limit=500)
    candidate = next((x for x in result["candidates"] if x["contact_id"] == contact_id), None)
    if candidate:
        return {"read_only": True, "writes": 0, "product": result["product"], "candidate": candidate}

    # 候选可能在基础筛选阶段被排除；直接回读一次并解释，仍不写入。
    ctx = await _load_context(product_id)
    records = ctx["editors"] if object_type == "媒体人" else ctx["kols"]
    record = next((x for x in records if x.get("record_id") == contact_id), None)
    if not record:
        raise ValueError(f"contact not found: {contact_id}")
    family = ctx["family"]
    product_fields = family["target"].get("fields") or {}
    fields = record.get("fields") or {}
    if object_type == "KOL":
        matched, filter_reasons = _base_filter_kol(fields, product_fields, ctx["mapping"])
    else:
        matched, filter_reasons = True, []
    check = precheck_contact(
        record, object_type=object_type, brand=ext(product_fields.get("品牌")),
        product_ids=set(family["product_ids"]), drafts=ctx["drafts"],
        email_owners=ctx["owners"], now_ms=int(time.time() * 1000),
    )
    return {
        "read_only": True, "writes": 0,
        "product": {"requested_product_id": product_id, "canonical_product_id": family["canonical_product_id"]},
        "candidate": {
            "contact_id": contact_id, "name": _candidate_name(fields, object_type),
            "base_filter_passed": matched, "base_filter_reasons": filter_reasons, **check,
        },
    }

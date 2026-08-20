"""新品集中上稿活动候选预览与全局重复触达预检。

只读模块：只调用 Feishu 读取接口，不创建任务、草稿、卡片或邮件。
活动候选和日常派单共享联系人、评分与历史草稿，但活动决策单独输出，
避免把活动状态写回联系人主状态。
"""

from __future__ import annotations

import json
import re
import time
from collections import Counter, defaultdict
from datetime import datetime

from . import config, dispatch, feishu, launch_competitor_evidence, launch_evidence
from .feishu import ext
from .scoring import _parse_multiselect, score_editor, score_kol


HARD_BLOCK_STATES = {"不合适", "黑名单"}
POSITIVE_RELATION_STATES = {
    "已合作", "已合作-免费", "已合作-免费(多次)", "已合作-付费",
    "洽谈中", "样品评估", "未产出",
}
THREAD_ONLY_STATES = POSITIVE_RELATION_STATES | {"待回复", "建联中"}
REUSABLE_DRAFT_STATES = {"已否决", "发送失败"}
PROACTIVE_SOURCES = {"cold", "followup", "secondary_outreach"}
RECENT_SAME_BRAND_DAYS = 7

PRODUCT_FIELDS = [
    "产品名", "产品英文名", "品牌", "品类", "报价(USD)", "销售国家",
    "适配主机", "适配IP", "活动归并键", "活动主记录ID", "活动主记录",
]
KOL_FIELDS = [
    "账号名", "邮箱", "合作状态", "主平台", "国家", "国家原文", "语言", "粉丝数",
    "内容风格", "IP喜好", "合作竞品", "竞品帖子证据", "邮箱验真状态",
    "YouTube频道ID", "主链接", "近期视频标题", "近期视频抓取时间",
    "上次二次接触时间", "上稿日期", "上稿标题", "寄样次数", "KOL级别", "合作报价",
    "标签版本", "内容垂类", "主机生态", "最近发布日", "近90天发布数",
    "资料可用状态", "资料核实时间", "触达路由状态",
]

SWITCH_ECOSYSTEMS = {"Switch", "Switch 2"}
SWITCH_PROFILE_VERTICALS = {"游戏硬件评测", "主机游戏"}
PROFILE_READY_STATES = {"有效", "人工核实有效"}
PROFILE_FRESH_DAYS = 60
PROFILE_ACTIVE_DAYS = 90
PROFILE_MIN_POSTS_90D = 1
NINTENDO_AUDIENCE_CUES = {
    "nintendo", "任天堂", "mario", "马里奥", "yoshi", "耀西",
    "zelda", "塞尔达", "tomodachi", "动物森友会", "animal crossing",
}
HARDWARE_CONTENT_CUES = {
    "unbox", "unboxing", "setup", "dock", "controller", "gamepad",
    "accessory", "accessories", "hardware", "开箱", "桌搭", "底座", "手柄", "配件", "硬件",
}
NINTENDO_TITLE_CUES = NINTENDO_AUDIENCE_CUES | {
    "switch", "joy-con", "joycon", "gameboy", "game boy", "snes", "3ds",
}
GAME_OR_CONSOLE_CONTENT_CUES = NINTENDO_TITLE_CUES | HARDWARE_CONTENT_CUES | {
    "video game", "videogame", "videojuego", "videojuegos", "gaming", "gamer",
    "jrpg", "rpg", "console", "consola", "consolas", "retro", "playstation",
    "xbox", "pokemon", "pokémon", "gameroom", "game room",
}
PROFILE_MIN_TARGET_TITLES = 3
NON_TARGET_AUDIENCE_CUES = {
    "roblox", "minecraft", "fortnite", "fall guys", "gta", "valorant",
    "league of legends", "英雄联盟", "call of duty", "warcraft", "魔兽世界",
    "movie", "movies", "cinema", "电影", "影视",
}

# 这里只拦“近期内容反复指向活动范围外地区”的强信号。单条标题可能只是
# 评测某个海外版本，不能据此改写达人国家；重复出现才进入人工冻结。
RECENT_CONTENT_MARKET_CUES = {
    "MY": ("malaysia", "malaysian"),
    "SG": ("singapore", "singaporean"),
    "PH": ("philippines", "filipino"),
    "ID": ("indonesia", "indonesian"),
    "IN": ("india", "indian"),
    "JP": ("japan", "japanese"),
    "KR": ("south korea", "korean"),
    "BR": ("brazil", "brazilian"),
    "MX": ("mexico", "mexican"),
    "CA": ("canada", "canadian"),
    "AU": ("australia", "australian"),
    "NZ": ("new zealand",),
}


def market_consistency_check(fields: dict, *, target_countries: set[str] | None) -> dict:
    """核对结构化国家与近期内容；冲突时冻结，不自动改主表国家。"""
    if target_countries is None:
        return {"passed": True, "decision": "market_consistent", "reasons": []}
    recent = ext(fields.get("近期视频标题")).lower()
    if not recent:
        return {"passed": True, "decision": "market_consistent", "reasons": []}
    structured_country = ext(fields.get("国家")).strip()
    for country, cues in RECENT_CONTENT_MARKET_CUES.items():
        hits = sum(len(re.findall(rf"\b{re.escape(cue)}\b", recent)) for cue in cues)
        if hits >= 2 and country not in target_countries and structured_country != country:
            label = cues[0].title()
            return {
                "passed": False,
                "decision": "hold_market_conflict",
                "campaign_pool": "held",
                "allowed_as_new_cold": False,
                "recommended_route": "verify_market_before_contact",
                "reasons": [
                    f"近期内容反复出现 {label}（{hits}次），与结构化国家 "
                    f"{structured_country or '空'} 及本次目标市场不一致"
                ],
                "evidence_draft_ids": [],
            }
    return {"passed": True, "decision": "market_consistent", "reasons": []}
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


def _draft_identity_index(drafts: list[dict]) -> dict[tuple[str, str], list[dict]]:
    """一次建立历史触达索引，避免每位候选都全表扫描草稿。"""
    index: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for draft in drafts:
        fields = draft.get("fields") or {}
        object_type = "媒体人" if _link_ids(fields.get("关联媒体人")) else "KOL"
        contact_id = _contact_id(fields, object_type)
        if contact_id:
            index[(object_type, contact_id)].append(draft)
        email, _ = feishu.clean_email(ext(fields.get("收件邮箱")))
        if email:
            index[("email", email)].append(draft)
    return index


def _drafts_for_contact(contact: dict, object_type: str,
                        index: dict[tuple[str, str], list[dict]]) -> list[dict]:
    rid = contact.get("record_id", "")
    email, _ = feishu.clean_email(ext((contact.get("fields") or {}).get("邮箱")))
    seen = set()
    out = []
    for key in ((object_type, rid), ("email", email)):
        if not key[1]:
            continue
        for draft in index.get(key, []):
            did = draft.get("record_id", "")
            if did not in seen:
                seen.add(did)
                out.append(draft)
    return out


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
    route_state = ext(fields.get("触达路由状态"))
    email_state = ext(fields.get("邮箱验真状态"))
    reasons: list[str] = []
    evidence_drafts: list[str] = []

    if not email:
        reasons.append(f"邮箱无效: {email_note}")
    if email_state == "无效":
        reasons.append("邮箱验真状态=无效")
    if coop in HARD_BLOCK_STATES:
        reasons.append(f"合作状态={coop}")
    if route_state == "禁止新开发":
        reasons.append("触达路由状态=禁止新开发")
    if reasons:
        return {
            "decision": "blocked", "allowed_as_new_cold": False,
            "campaign_pool": "excluded",
            "recommended_route": "exclude", "reasons": reasons,
            "evidence_draft_ids": [], "email": _masked_email(email),
        }

    owners = email_owners.get(email, set())
    if len(owners) > 1:
        return {
            "decision": "hold_duplicate_identity", "allowed_as_new_cold": False,
            "campaign_pool": "held",
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
        if not _is_nonterminal_or_sent(df):
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
        if (source in PROACTIVE_SOURCES and _brand_of_draft(df) == brand
                and (event_ms >= cutoff or ext(df.get("发送状态")) not in {"已发", "已发送"})):
            same_brand_active.append(draft)

    for draft in same_product + same_brand_active + same_email_other_record:
        did = draft.get("record_id", "")
        if did and did not in evidence_drafts:
            evidence_drafts.append(did)

    any_reply = any(bool((d.get("fields") or {}).get("是否回复")) for d in same_product)
    if same_product:
        if coop in POSITIVE_RELATION_STATES or any_reply:
            if object_type == "KOL" and fields.get("上稿日期") not in (None, "", 0):
                return {
                    "decision": "republish_requires_commitment",
                    "campaign_pool": "republish",
                    "allowed_as_new_cold": False,
                    "recommended_route": "request_republish_commitment_in_existing_thread",
                    "reasons": ["同一活动产品家族已有触达且联系人已有上稿记录；仅在明确承诺本次窗口二次发布后计入"],
                    "evidence_draft_ids": evidence_drafts[:10], "email": _masked_email(email),
                }
            return {
                "decision": "existing_pipeline_same_thread",
                "campaign_pool": "existing_pipeline", "allowed_as_new_cold": False,
                "recommended_route": "continue_existing_thread",
                "reasons": ["同一活动产品家族已有触达记录，且存在正向关系/回复"],
                "evidence_draft_ids": evidence_drafts[:10], "email": _masked_email(email),
            }
        return {
            "decision": "blocked_prior_same_product", "allowed_as_new_cold": False,
            "campaign_pool": "excluded",
            "recommended_route": "do_not_resend_cold",
            "reasons": ["同一活动产品家族已有有效 cold/follow-up 记录"],
            "evidence_draft_ids": evidence_drafts[:10], "email": _masked_email(email),
        }

    if same_email_other_record:
        return {
            "decision": "hold_duplicate_identity", "allowed_as_new_cold": False,
            "campaign_pool": "held",
            "recommended_route": "merge_identity_before_contact",
            "reasons": ["历史草稿命中同邮箱的另一条 KOL/媒体人记录"],
            "evidence_draft_ids": evidence_drafts[:10], "email": _masked_email(email),
        }

    if same_brand_active:
        return {
            "decision": "hold_active_or_recent", "allowed_as_new_cold": False,
            "campaign_pool": "existing_pipeline",
            "recommended_route": "wait_or_continue_existing_thread",
            "reasons": [f"同品牌 {RECENT_SAME_BRAND_DAYS} 天内已有触达或仍在流程中"],
            "evidence_draft_ids": evidence_drafts[:10], "email": _masked_email(email),
        }

    if route_state == "待核对":
        return {
            "decision": "hold_active_or_recent", "allowed_as_new_cold": False,
            "campaign_pool": "existing_pipeline",
            "recommended_route": "verify_relationship_before_contact",
            "reasons": ["触达路由状态=待核对，禁止直接进入新开发池"],
            "evidence_draft_ids": evidence_drafts[:10], "email": _masked_email(email),
        }

    if route_state == "沿用原线程" or coop in THREAD_ONLY_STATES:
        return {
            "decision": "existing_pipeline_same_thread", "allowed_as_new_cold": False,
            "campaign_pool": "existing_pipeline",
            "recommended_route": "continue_existing_thread",
            "reasons": [
                f"历史关系={coop or '未标记'}；只能沿用原邮件线程，禁止新 cold"
            ],
            "evidence_draft_ids": evidence_drafts[:10], "email": _masked_email(email),
        }

    return {
        "decision": "eligible_new_cold", "allowed_as_new_cold": True,
        "campaign_pool": "new_development",
        "recommended_route": "activity_cold_pool", "reasons": ["全局重复触达预检通过"],
        "evidence_draft_ids": [], "email": _masked_email(email),
    }


def _requires_nintendo_switch_profile(product_fields: dict) -> bool:
    """仅对明确的 Nintendo/Mario Switch 产品启用严格受众闸。"""
    hosts = _parse_multiselect(product_fields.get("适配主机"))
    ips = " ".join(_parse_multiselect(product_fields.get("适配IP"))).lower()
    return bool(hosts & SWITCH_ECOSYSTEMS) and any(
        cue in ips for cue in ("nintendo", "任天堂", "mario", "马里奥")
    )


def _timestamp_ms(value) -> int:
    if value in (None, ""):
        return 0
    if isinstance(value, (int, float)):
        raw = int(value)
        return raw * 1000 if 0 < raw < 10_000_000_000 else raw
    text = ext(value).strip()
    if not text:
        return 0
    if text.isdigit():
        raw = int(text)
        return raw * 1000 if raw < 10_000_000_000 else raw
    try:
        return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp() * 1000)
    except ValueError:
        return 0


def _matched_cues(text: str, cues: set[str]) -> list[str]:
    lowered = (text or "").lower()
    return sorted(cue for cue in cues if cue in lowered)


def _recent_title_lines(value) -> list[str]:
    return [line.strip() for line in ext(value).splitlines() if line.strip()]


def _matching_title_count(titles: list[str], cues: set[str]) -> int:
    return sum(bool(_matched_cues(title, cues)) for title in titles)


def _nintendo_switch_profile_evidence(fields: dict, *, now_ms: int) -> dict:
    """返回硬闸使用的原始字段和命中信号，供单条回放解释。"""
    readiness = ext(fields.get("资料可用状态"))
    version = ext(fields.get("标签版本"))
    captured_ms = _timestamp_ms(fields.get("近期视频抓取时间"))
    last_post_ms = _timestamp_ms(fields.get("最近发布日"))
    verified_ms = _timestamp_ms(fields.get("资料核实时间"))
    try:
        posts_90d = int(float(fields.get("近90天发布数") or 0))
    except (TypeError, ValueError):
        posts_90d = 0
    ip_text = ext(fields.get("IP喜好"))
    recent_titles = ext(fields.get("近期视频标题"))
    title_lines = _recent_title_lines(fields.get("近期视频标题"))
    ip_matches = _matched_cues(ip_text, NINTENDO_AUDIENCE_CUES)
    recent_nintendo_matches = _matched_cues(recent_titles, NINTENDO_AUDIENCE_CUES)
    recent_hardware_matches = _matched_cues(recent_titles, HARDWARE_CONTENT_CUES)
    negative_ip_matches = _matched_cues(ip_text, NON_TARGET_AUDIENCE_CUES)
    negative_recent_matches = _matched_cues(recent_titles, NON_TARGET_AUDIENCE_CUES)
    return {
        "manual_verified": readiness == "人工核实有效",
        "readiness": readiness,
        "tag_version": version,
        "verified_at_ms": verified_ms,
        "verified_age_days": (
            round(max(0, now_ms - verified_ms) / 86_400_000, 1) if verified_ms else None
        ),
        "captured_at_ms": captured_ms,
        "captured_age_days": (
            round(max(0, now_ms - captured_ms) / 86_400_000, 1) if captured_ms else None
        ),
        "last_post_at_ms": last_post_ms,
        "last_post_age_days": (
            round(max(0, now_ms - last_post_ms) / 86_400_000, 1) if last_post_ms else None
        ),
        "posts_90d": posts_90d,
        "content_vertical": ext(fields.get("内容垂类")),
        "ecosystems": sorted(_parse_multiselect(fields.get("主机生态"))),
        "ip_matches": ip_matches,
        "recent_nintendo_matches": recent_nintendo_matches,
        "recent_hardware_matches": recent_hardware_matches,
        "recent_title_count": len(title_lines),
        "recent_target_title_count": _matching_title_count(
            title_lines, GAME_OR_CONSOLE_CONTENT_CUES,
        ),
        "recent_nintendo_title_count": _matching_title_count(
            title_lines, NINTENDO_TITLE_CUES,
        ),
        "negative_ip_matches": negative_ip_matches,
        "negative_recent_matches": negative_recent_matches,
        "recent_titles_excerpt": recent_titles[:600],
    }


def _nintendo_switch_profile_reasons(fields: dict, *, now_ms: int | None = None) -> list[str]:
    """把本次人工审核结论固化为可回放的结构化硬规则。"""
    now_ms = now_ms or int(time.time() * 1000)
    evidence = _nintendo_switch_profile_evidence(fields, now_ms=now_ms)
    reasons: list[str] = []
    readiness = evidence["readiness"]
    if readiness == "活跃度不足":
        reasons.append("活跃度不足")
    elif readiness not in PROFILE_READY_STATES:
        reasons.append("资料缺失或过期")
    elif evidence["manual_verified"]:
        verified_age = evidence["verified_age_days"]
        if verified_age is None or verified_age > PROFILE_FRESH_DAYS:
            reasons.append("人工核实已过期")
    else:
        if evidence["tag_version"] != "v2":
            reasons.append("标签版本不是v2")
        age = evidence["captured_age_days"]
        if age is None or age > PROFILE_FRESH_DAYS:
            reasons.append("资料缺失或过期")
        if evidence["posts_90d"] < PROFILE_MIN_POSTS_90D:
            reasons.append("活跃度不足")
        last_post_age = evidence["last_post_age_days"]
        if last_post_age is None or last_post_age > PROFILE_ACTIVE_DAYS:
            reasons.append("最近发布记录缺失或过期")

    ecosystems = set(evidence["ecosystems"])
    if not (ecosystems & SWITCH_ECOSYSTEMS):
        reasons.append("目标主机不匹配")

    vertical = evidence["content_vertical"]
    if vertical not in SWITCH_PROFILE_VERTICALS:
        reasons.append("内容垂类不是主机游戏或游戏硬件评测")
    has_nintendo_audience = bool(evidence["ip_matches"] or evidence["recent_nintendo_matches"])
    has_recent_hardware = bool(
        vertical == "游戏硬件评测" and evidence["recent_hardware_matches"]
    )
    if not (has_nintendo_audience or has_recent_hardware):
        reasons.append("Nintendo/Mario受众或近期硬件内容不匹配")
    if not evidence["manual_verified"]:
        if evidence["recent_target_title_count"] < PROFILE_MIN_TARGET_TITLES:
            reasons.append("近期目标游戏/主机内容占比不足")
        if not (evidence["recent_nintendo_title_count"] or has_recent_hardware):
            reasons.append("近期内容缺少Nintendo/Switch或硬件评测证据")
    if ((evidence["negative_ip_matches"] or evidence["negative_recent_matches"])
            and not (evidence["recent_nintendo_matches"] or has_recent_hardware)):
        reasons.append("近期或主要内容存在明显非目标游戏/IP信号")
    return reasons


def _base_filter_kol(
    fields: dict, product_fields: dict, mapping: dict, *,
    target_countries: set[str] | None = None,
    target_languages: set[str] | None = None,
    target_fans_min: int | None = None,
    target_fans_max: int | None = None,
    now_ms: int | None = None,
) -> tuple[bool, list[str]]:
    reasons = []
    product_countries = _parse_multiselect(product_fields.get("销售国家"))
    countries = (
        set(target_countries) if target_countries is not None else product_countries
    )
    languages = (
        set(target_languages) if target_languages is not None else
        {lang for country in countries for lang in dispatch.COUNTRY_TO_LANGS.get(country, [])}
    )
    language_iso = {"英语": "en", "德语": "de", "西班牙语": "es", "法语": "fr", "葡萄牙语": "pt"}
    languages = {language_iso.get(x, x) for x in languages}
    platforms = set(dispatch.CATEGORY_PLATFORMS.get(ext(product_fields.get("品类")), []))
    expected_styles = set(mapping.get("expected_styles") or [])
    default_fans_min, default_fans_max = dispatch._fans_range_for_price(
        float(product_fields.get("报价(USD)") or 0)
    )
    fans_min = int(target_fans_min) if target_fans_min is not None else default_fans_min
    fans_max = int(target_fans_max) if target_fans_max is not None else default_fans_max
    try:
        fans = int(fields.get("粉丝数") or 0)
    except (TypeError, ValueError):
        fans = 0
    if target_countries is not None and not countries:
        reasons.append("活动目标国家未配置")
    elif countries and ext(fields.get("国家")) not in countries:
        reasons.append("国家不在活动目标市场" if target_countries else "国家不在销售市场")
    if target_languages is not None and not languages:
        reasons.append("活动目标语言未配置")
    elif languages and ext(fields.get("语言")) not in languages:
        reasons.append("语言不在活动目标范围" if target_languages else "语言不匹配")
    if platforms and ext(fields.get("主平台")) not in platforms:
        reasons.append("主平台不匹配")
    if fans_min < 0 or fans_max < 0 or (fans_max and fans_min > fans_max):
        reasons.append("活动粉丝范围配置无效")
    elif fans < fans_min or (fans_max and fans > fans_max):
        reasons.append("粉丝量级不匹配")
    if expected_styles and not (_parse_multiselect(fields.get("内容风格")) & expected_styles):
        reasons.append("内容风格不匹配")
    if _requires_nintendo_switch_profile(product_fields):
        reasons.extend(_nintendo_switch_profile_reasons(fields, now_ms=now_ms))
    reasons = list(dict.fromkeys(reasons))
    return not reasons, reasons


def _numeric(value) -> float:
    try:
        return float(str(value or 0).replace(",", "").replace("¥", "").replace("$", "").strip())
    except (TypeError, ValueError):
        return 0.0


def build_review_snapshot(fields: dict, evidence_rank: dict, *, now_ms: int,
                          precheck: dict | None = None) -> dict:
    """生成运营可直接审核的快照和分流说明。"""
    profile_url = feishu.ext_url(fields.get("主链接")).strip()
    recent_titles = ext(fields.get("近期视频标题")).strip()
    content_updated_at = _timestamp_ms(fields.get("近期视频抓取时间"))
    language = ext(fields.get("语言")).strip()
    coop = ext(fields.get("合作状态")).strip() or "未标记"
    kol_level = ext(fields.get("KOL级别")).strip()
    quote = _numeric(fields.get("合作报价"))
    stale_cutoff = now_ms - 180 * 86_400_000

    posts = sorted(
        evidence_rank.get("evidence_posts") or [],
        key=lambda post: (
            not bool(post.get("is_high_performance")),
            -_numeric(post.get("metric_value")),
            -_timestamp_ms(post.get("published_at")),
        ),
    )
    primary = posts[0] if posts else {}
    primary_url = str(primary.get("post_url") or "").strip()
    evidence_level = evidence_rank.get("evidence_level") or "无加分"
    evidence_summary = f"等级={evidence_level}；命中帖子={len(posts)}条"
    if posts:
        evidence_summary += (
            f"；主证据={primary.get('platform') or '未知平台'}"
            f"/{primary.get('post_title') or primary.get('post_id') or '未命名'}"
        )
        if primary.get("metric_value") is not None:
            evidence_summary += f"；{primary.get('metric_name') or '效果值'}={primary.get('metric_value')}"
        evidence_summary += f"；归因依据={primary.get('evidence_basis') or '未标记'}"

    relationship_parts = [
        f"合作状态={coop}",
        f"邮箱验真={ext(fields.get('邮箱验真状态')).strip() or '未验'}",
    ]
    if precheck:
        relationship_parts.append(f"重复触达预检={precheck.get('decision') or '未知'}")
    if kol_level:
        relationship_parts.append(f"KOL级别={kol_level}")
    if quote > 0:
        relationship_parts.append(f"合作报价={quote:g}")
    for label, field_name in (
        ("寄样次数", "寄样次数"), ("历史上稿日期", "上稿日期"),
        ("上次二次接触", "上次二次接触时间"),
    ):
        value = fields.get(field_name)
        if value not in (None, "", 0):
            relationship_parts.append(f"{label}={ext(value)}")

    frankie_reasons = []
    if "头部" in kol_level:
        frankie_reasons.append("头部KOL")
    if quote > 0:
        frankie_reasons.append("已有合作报价，涉及预算取舍")
    operator_reasons = []
    if language in {"de", "es"}:
        operator_reasons.append(f"辅助语言={language}，需确认实际内容语言")
    if not profile_url:
        operator_reasons.append("缺少可打开的达人主页")
    if not recent_titles:
        operator_reasons.append("缺少近期内容标题")
    if not content_updated_at or content_updated_at < stale_cutoff:
        operator_reasons.append("近期内容数据缺失或超过180天")
    if coop in POSITIVE_RELATION_STATES:
        operator_reasons.append(f"历史关系={coop}，需确认本次重新合作语境")
    if precheck and precheck.get("decision") in {
        "reactivation_same_thread", "existing_pipeline_same_thread",
    }:
        operator_reasons.append("已命中历史触达/回复，需确认沿用原邮件线程的复联语境")
    if precheck and precheck.get("decision") == "republish_requires_commitment":
        operator_reasons.append("已有上稿记录；必须先确认愿意在本次窗口二次发布，未承诺不计入目标")
    if precheck and precheck.get("decision") == "hold_market_conflict":
        operator_reasons.extend(precheck.get("reasons") or ["近期内容地区与主表国家冲突"])

    if frankie_reasons:
        route = "Frankie例外审核"
        decision = "待审核"
        instruction = (
            "请打开达人主页和主证据帖子，只判断是否值得使用例外预算/"
            "重点关系：" + "；".join(frankie_reasons)
        )
    elif operator_reasons:
        route = "KOL运营审核"
        decision = "待审核"
        instruction = (
            "请打开达人主页，确认近3个月仍以游戏硬件/手柄内容为主，"
            "内容语言符合本次活动，且没有明显品牌冲突。待确认点：" + "；".join(operator_reasons)
        )
    else:
        route = "系统建议通过"
        decision = "通过"
        instruction = (
            "系统已检查活动国家/语言、平台、粉丝量级、内容风格、邮箱和全局重复触达；"
            "无需逐条人工审核，可抽检。"
        )

    return {
        "profile_url": profile_url,
        "followers": int(_numeric(fields.get("粉丝数"))),
        "content_summary": recent_titles[:1000],
        "content_updated_at": content_updated_at,
        "relationship_summary": "；".join(relationship_parts)[:1000],
        "evidence_summary": evidence_summary[:1000],
        "primary_evidence_url": primary_url,
        "review_route": route,
        "review_instruction": instruction[:1000],
        "review_decision": decision,
    }


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
        "draft_index": _draft_identity_index(drafts),
    }


def _activity_product_id(fields: dict) -> str:
    return ext(fields.get("产品主记录ID")).strip() or (
        sorted(_link_ids(fields.get("关联产品主记录")))[0]
        if _link_ids(fields.get("关联产品主记录")) else ""
    )


async def _load_activity_context(campaign_id: str, object_type: str) -> dict:
    activity = await launch_evidence.get_activity(campaign_id)
    fields = activity.get("fields") or {}
    mode = ext(fields.get("竞品证据模式"))
    status = ext(fields.get("竞品分析状态"))
    brand = ext(fields.get("竞品品牌"))
    post_ids = sorted(_link_ids(fields.get("关联竞品帖子")))
    event_ids = sorted(_link_ids(fields.get("关联竞品营销事件")))
    ranking_version = ext(fields.get("证据排序版本"))
    evidence_source = "activity_relation"
    try:
        snapshot_ids = await launch_evidence.load_full_snapshot_post_ids(
            campaign_id=campaign_id, activity_fields=fields,
        )
        if snapshot_ids:
            post_ids = snapshot_ids
            evidence_source = "activity_node_snapshot"
    except Exception as exc:
        snapshot_error = str(exc)
    else:
        snapshot_error = ""
    # A legacy activity may predate these fields entirely.  Preserve the old
    # product-level scope in that case, but fail closed when a current activity
    # explicitly contains an empty target field.
    target_countries = (
        _parse_multiselect(fields.get("活动目标国家"))
        if "活动目标国家" in fields
        else None
    )
    target_languages = (
        _parse_multiselect(fields.get("活动目标语言"))
        if "活动目标语言" in fields
        else None
    )
    target_fans_min = (
        int(_numeric(fields.get("KOL粉丝下限")))
        if fields.get("KOL粉丝下限") not in (None, "") else None
    )
    target_fans_max = (
        int(_numeric(fields.get("KOL粉丝上限")))
        if fields.get("KOL粉丝上限") not in (None, "") else None
    )
    evidence_pending = bool(
        not mode or status == "配置无效"
        or (mode == launch_evidence.MODE_NEW and status != "已就绪")
        or (mode == launch_evidence.MODE_REUSE and status != "已就绪")
    )
    exact_scope = bool(
        object_type == "KOL"
        and campaign_id == "launch-20260915-funlab-dave-ys11-5"
        and activity.get("record_id") == "recvsFoRmeGj4Y"
        and _activity_product_id(fields) == "recvkJOoCsNb1s"
        and mode == launch_evidence.MODE_REUSE
        and status == "已就绪"
        and brand.upper() == "NYXI"
    )
    posts = []
    evidence_error = snapshot_error
    if mode == launch_evidence.MODE_NONE:
        if status != "不适用" or brand or post_ids or event_ids:
            evidence_error = "不使用竞品证据的字段组合无效"
    elif mode in {launch_evidence.MODE_NEW, launch_evidence.MODE_REUSE} and status == "已就绪":
        if not brand or not post_ids or not ranking_version:
            evidence_error = "已就绪证据缺少品牌、帖子或排序版本"
    elif mode not in launch_evidence.VALID_MODES:
        evidence_error = "竞品证据模式为空或未知"

    if not evidence_error and mode in {launch_evidence.MODE_NEW, launch_evidence.MODE_REUSE} and status == "已就绪":
        try:
            posts, _ = await launch_evidence._validate_linked_records(
                competitor_brand=brand,
                post_record_ids=post_ids,
                event_record_ids=event_ids,
            )
        except Exception as exc:
            evidence_error = str(exc)
            posts = []
    if evidence_error:
        evidence_pending = True
        status = "配置无效"
    applicable = bool(exact_scope and posts and not evidence_error)
    return {
        "activity": activity,
        "product_id": _activity_product_id(fields),
        "evidence_mode": mode,
        "evidence_status": status or "配置无效",
        "evidence_pending": evidence_pending,
        "ranking_version": ranking_version,
        "competitor_posts": posts if applicable else [],
        "competitor_evidence_applied": applicable,
        "evidence_source": evidence_source,
        "evidence_error": evidence_error,
        "target_countries": target_countries,
        "target_languages": target_languages,
        "target_fans_min": target_fans_min,
        "target_fans_max": target_fans_max,
    }


async def _load_locked_snapshot(
    *, campaign_id: str, product_family_id: str, object_type: str,
    contact_id: str, ranking_version: str,
) -> dict | None:
    if not config.T_LAUNCH_PARTICIPANT or not ranking_version:
        return None
    rows = await feishu.search_records(config.T_LAUNCH_PARTICIPANT, [
        {"field_name": "活动ID", "operator": "is", "value": [campaign_id]},
        {"field_name": "产品家族ID", "operator": "is", "value": [product_family_id]},
        {"field_name": "对象类型", "operator": "is", "value": [object_type]},
    ])
    link_field = "关联媒体人" if object_type == "媒体人" else "关联KOL"
    matched = [row for row in rows if (
        contact_id in _link_ids((row.get("fields") or {}).get(link_field))
        and ext((row.get("fields") or {}).get("名单版本")) == ranking_version
        and ext((row.get("fields") or {}).get("参与状态")) in {"已入围", "锁定准备中"}
    )]
    if len(matched) > 1:
        raise ValueError("当前名单存在重复参与记录，无法可靠回放")
    if not matched:
        return None
    raw = (matched[0].get("fields") or {}).get("排序快照历史")
    try:
        history = raw if isinstance(raw, list) else json.loads(ext(raw) or "[]")
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("参与记录排序快照历史损坏") from exc
    snapshots = [x for x in history if (
        isinstance(x, dict) and x.get("ranking_version") == ranking_version
    )]
    return snapshots[-1] if snapshots else None


async def _load_participant_review(
    *, campaign_id: str, product_family_id: str, object_type: str, contact_id: str,
    ranking_version: str,
) -> dict | None:
    """读取活动级人工结论；只用于解释回放，不把产品结论写回 KOL 主表。"""
    if not config.T_LAUNCH_PARTICIPANT:
        return None
    rows = await feishu.search_records(config.T_LAUNCH_PARTICIPANT, [
        {"field_name": "活动ID", "operator": "is", "value": [campaign_id]},
        {"field_name": "产品家族ID", "operator": "is", "value": [product_family_id]},
        {"field_name": "对象类型", "operator": "is", "value": [object_type]},
    ])
    link_field = "关联媒体人" if object_type == "媒体人" else "关联KOL"
    contact_rows = [row for row in rows if contact_id in _link_ids(
        (row.get("fields") or {}).get(link_field)
    )]
    matched = [row for row in contact_rows if (
        ext((row.get("fields") or {}).get("名单版本")) == ranking_version
        and ext((row.get("fields") or {}).get("参与状态")) in {"已入围", "锁定准备中"}
    )]
    if len(matched) > 1:
        raise ValueError("当前活动存在重复参与记录，无法可靠读取人工审核结论")
    if not matched:
        if contact_rows:
            return {
                "is_current": False,
                "historical_review_count": len(contact_rows),
                "note": "当前名单版本没有有效参与记录；历史审核结论未作为当前结论展示",
            }
        return None
    row = matched[0]
    fields = row.get("fields") or {}
    return {
        "is_current": True,
        "participant_record_id": row.get("record_id", ""),
        "list_version": ext(fields.get("名单版本")),
        "participant_status": ext(fields.get("参与状态")),
        "review_decision": ext(fields.get("审核结论")),
        "review_reason": ext(fields.get("审核原因")),
        "review_reason_codes": sorted(_parse_multiselect(fields.get("审核原因代码"))),
        "reviewed_at_ms": _timestamp_ms(fields.get("审核时间")),
    }


async def preview_candidates(
    product_id: str = "", *, object_type: str = "KOL", limit: int = 100,
    campaign_id: str = "", internal_full: bool = False,
) -> dict:
    if object_type not in {"KOL", "媒体人"}:
        raise ValueError("object_type must be KOL or 媒体人")
    limit = max(1, min(int(limit), 500))
    activity_ctx = None
    if campaign_id:
        activity_ctx = await _load_activity_context(campaign_id, object_type)
        activity_product_id = activity_ctx["product_id"]
        if not activity_product_id:
            raise ValueError(f"活动缺少产品主记录ID: {campaign_id}")
        if product_id and product_id != activity_product_id:
            raise ValueError("product_id 与活动产品主记录不一致")
        product_id = activity_product_id
    if not product_id:
        raise ValueError("product_id or campaign_id required")
    ctx = await _load_context(product_id)
    family = ctx["family"]
    product_fields = family["target"].get("fields") or {}
    brand = config.brand_from_text(ext(product_fields.get("品牌"))) or ext(product_fields.get("品牌")).upper()
    records = ctx["editors"] if object_type == "媒体人" else ctx["kols"]
    now_ms = int(time.time() * 1000)
    candidates = []
    filtered_out = 0
    evidence_index = (
        launch_competitor_evidence.build_evidence_index(activity_ctx["competitor_posts"])
        if activity_ctx and activity_ctx["competitor_evidence_applied"] and object_type == "KOL"
        else None
    )
    evidence_coverage = (
        launch_competitor_evidence.summarize_evidence_coverage(evidence_index, records)
        if evidence_index else {
            "linked_posts_total": 0, "valid_partner_posts": 0,
            "official_excluded": 0, "invalid_excluded": 0,
            "distinct_authors": 0, "matched_contacts": 0,
            "matched_authors": 0, "unmatched_authors": 0,
        }
    )

    for record in records:
        fields = record.get("fields") or {}
        check = precheck_contact(
            record, object_type=object_type, brand=brand,
            product_ids=set(family["product_ids"]),
            drafts=_drafts_for_contact(record, object_type, ctx["draft_index"]),
            email_owners=ctx["owners"], now_ms=now_ms,
        )
        if object_type == "KOL":
            matched, filter_reasons = _base_filter_kol(
                fields, product_fields, ctx["mapping"],
                target_countries=(activity_ctx or {}).get("target_countries"),
                target_languages=(activity_ctx or {}).get("target_languages"),
                target_fans_min=(activity_ctx or {}).get("target_fans_min"),
                target_fans_max=(activity_ctx or {}).get("target_fans_max"),
                now_ms=now_ms,
            )
            if not matched:
                filtered_out += 1
                # 全局关系路由优先。已有关系对象即使产品画像不适配，也要保留在
                # 回放结果中说明“沿用原线程/禁止新 cold”，不能被基础筛选静默吞掉。
                if check["decision"] == "eligible_new_cold":
                    continue
            score, breakdown = score_kol(
                fields, product_fields, set(ctx["mapping"].get("expected_styles") or []),
                set(dispatch.CATEGORY_PLATFORMS.get(ext(product_fields.get("品类")), [])),
            )
        else:
            matched = True
            score, breakdown = score_editor(
                fields, product_fields,
                set(ctx["mapping"].get("expected_report_cats") or []),
                set(ctx["mapping"].get("expected_media_types") or []),
            )
            filter_reasons = []

        if object_type == "KOL" and check["decision"] == "eligible_new_cold":
            market_check = market_consistency_check(
                fields,
                target_countries=(activity_ctx or {}).get("target_countries"),
            )
            if not market_check["passed"]:
                market_check["email"] = check.get("email", "")
                check = market_check
        evidence_rank = (
            launch_competitor_evidence.rank_contact_evidence_from_index(
                record, evidence_index, base_score=score,
            )
            if evidence_index
            else {
                "evidence_level": "无加分", "final_priority": score,
                "long_term": False, "long_term_span_days": 0,
                "high_performance": False, "identity_paths": [],
                "stable_identity_keys": [],
                "matched_post_ids": [], "evidence_posts": [],
                "p75_thresholds": {}, "p75_samples": {},
            }
        )
        review_snapshot = (
            build_review_snapshot(fields, evidence_rank, now_ms=now_ms, precheck=check)
            if object_type == "KOL" else {}
        )
        candidates.append({
            "contact_id": record.get("record_id", ""),
            "name": _candidate_name(fields, object_type),
            "platform": ext(fields.get("主平台")) if object_type == "KOL" else ext(fields.get("主要媒体")),
            "country": ext(fields.get("国家")), "language": ext(fields.get("语言")),
            "base_filter_passed": matched, "base_filter_reasons": filter_reasons,
            "score": score, "breakdown": breakdown,
            "competitor_signal": (ext(fields.get("合作竞品"))[:300] if object_type == "KOL" else ""),
            "competitor_evidence": (ext(fields.get("竞品帖子证据"))[:500] if object_type == "KOL" else ""),
            **check, **evidence_rank, **review_snapshot,
        })

    decision_order = {
        "eligible_new_cold": 0, "existing_pipeline_same_thread": 1,
        "republish_requires_commitment": 2, "hold_active_or_recent": 3,
        "hold_market_conflict": 4, "hold_duplicate_identity": 5,
        "blocked_prior_same_product": 6, "blocked": 7,
    }
    candidates.sort(key=lambda x: (
        decision_order.get(x["decision"], 9),
        -float(x.get("final_priority") or x["score"] or 0), x["contact_id"],
    ))
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
        "campaign_id": campaign_id,
        "evidence_mode": activity_ctx["evidence_mode"] if activity_ctx else "",
        "evidence_status": activity_ctx["evidence_status"] if activity_ctx else "",
        "evidence_pending": activity_ctx["evidence_pending"] if activity_ctx else False,
        "ranking_version": activity_ctx["ranking_version"] if activity_ctx else "",
        "evidence_source": activity_ctx["evidence_source"] if activity_ctx else "",
        "target_countries": sorted(activity_ctx["target_countries"] or []) if activity_ctx else [],
        "target_languages": sorted(activity_ctx["target_languages"] or []) if activity_ctx else [],
        "target_fans_min": activity_ctx["target_fans_min"] if activity_ctx else None,
        "target_fans_max": activity_ctx["target_fans_max"] if activity_ctx else None,
        "competitor_evidence_applied": (
            activity_ctx["competitor_evidence_applied"] if activity_ctx else False
        ),
        "evidence_coverage": evidence_coverage,
        "summary": {
            "pool_records": len(records), "base_filter_excluded": filtered_out,
            "evaluated": len(candidates), "eligible_new_cold": counts["eligible_new_cold"],
            "existing_pipeline": counts["existing_pipeline_same_thread"],
            "republish": counts["republish_requires_commitment"],
            "held_or_blocked": len(candidates) - counts["eligible_new_cold"]
            - counts["existing_pipeline_same_thread"] - counts["republish_requires_commitment"],
            "by_decision": dict(counts),
        },
        "candidates": candidates if internal_full else candidates[:limit],
    }


async def replay_candidate(
    product_id: str, contact_id: str, *, object_type: str = "KOL", campaign_id: str = "",
) -> dict:
    if object_type not in {"KOL", "媒体人"}:
        raise ValueError("object_type must be KOL or 媒体人")
    activity_ctx = None
    if campaign_id:
        activity_ctx = await _load_activity_context(campaign_id, object_type)
        activity_product_id = activity_ctx["product_id"]
        if product_id and product_id != activity_product_id:
            raise ValueError("product_id 与活动产品主记录不一致")
        product_id = activity_product_id
    if not product_id:
        raise ValueError("product_id or campaign_id required")
    # 单条只加载一次上下文；不先跑 500 条预览再重复全表读取。
    ctx = await _load_context(product_id)
    records = ctx["editors"] if object_type == "媒体人" else ctx["kols"]
    record = next((x for x in records if x.get("record_id") == contact_id), None)
    if not record:
        raise ValueError(f"contact not found: {contact_id}")
    family = ctx["family"]
    product_fields = family["target"].get("fields") or {}
    fields = record.get("fields") or {}
    now_ms = int(time.time() * 1000)
    if object_type == "KOL":
        matched, filter_reasons = _base_filter_kol(
            fields, product_fields, ctx["mapping"],
            target_countries=(activity_ctx or {}).get("target_countries"),
            target_languages=(activity_ctx or {}).get("target_languages"),
            target_fans_min=(activity_ctx or {}).get("target_fans_min"),
            target_fans_max=(activity_ctx or {}).get("target_fans_max"),
            now_ms=now_ms,
        )
    else:
        matched, filter_reasons = True, []
    check = precheck_contact(
        record, object_type=object_type,
        brand=config.brand_from_text(ext(product_fields.get("品牌"))) or ext(product_fields.get("品牌")).upper(),
        product_ids=set(family["product_ids"]),
        drafts=_drafts_for_contact(record, object_type, ctx["draft_index"]),
        email_owners=ctx["owners"], now_ms=now_ms,
    )
    if object_type == "KOL":
        score, _ = score_kol(
            fields, product_fields, set(ctx["mapping"].get("expected_styles") or []),
            set(dispatch.CATEGORY_PLATFORMS.get(ext(product_fields.get("品类")), [])),
        )
    else:
        score, _ = score_editor(
            fields, product_fields,
            set(ctx["mapping"].get("expected_report_cats") or []),
            set(ctx["mapping"].get("expected_media_types") or []),
        )
    evidence_rank = (
        launch_competitor_evidence.rank_contact_evidence(
            record, activity_ctx["competitor_posts"], base_score=score,
        )
        if activity_ctx and activity_ctx["competitor_evidence_applied"] and object_type == "KOL"
        else {"evidence_level": "无加分", "final_priority": score,
              "identity_paths": [], "stable_identity_keys": [],
              "matched_post_ids": [], "evidence_posts": []}
    )
    review_snapshot = (
        build_review_snapshot(fields, evidence_rank, now_ms=now_ms, precheck=check)
        if object_type == "KOL" else {}
    )
    locked_snapshot = None
    participant_review = None
    if activity_ctx:
        locked_version_field = (
            "媒体人已锁定名单版本" if object_type == "媒体人" else "KOL已锁定名单版本"
        )
        locked_version = ext(
            (activity_ctx["activity"].get("fields") or {}).get(locked_version_field)
        )
        locked_snapshot = await _load_locked_snapshot(
            campaign_id=campaign_id,
            product_family_id=family["canonical_product_id"],
            object_type=object_type,
            contact_id=contact_id,
            ranking_version=locked_version,
        )
        participant_review = await _load_participant_review(
            campaign_id=campaign_id,
            product_family_id=family["canonical_product_id"],
            object_type=object_type,
            contact_id=contact_id,
            ranking_version=activity_ctx["ranking_version"],
        )
    profile_evidence = (
        _nintendo_switch_profile_evidence(fields, now_ms=now_ms)
        if object_type == "KOL" and _requires_nintendo_switch_profile(product_fields)
        else {}
    )
    return {
        "read_only": True, "writes": 0,
        "product": {"requested_product_id": product_id, "canonical_product_id": family["canonical_product_id"]},
        "campaign_id": campaign_id,
        "ranking_version": activity_ctx["ranking_version"] if activity_ctx else "",
        "ranking_source": "locked_snapshot" if locked_snapshot else "current_preview",
        "locked_ranking_snapshot": locked_snapshot,
        "participant_review": participant_review,
        "candidate": {
            "contact_id": contact_id, "name": _candidate_name(fields, object_type),
            "base_filter_passed": matched, "base_filter_reasons": filter_reasons,
            "profile_evidence": profile_evidence,
            **check, **evidence_rank, **review_snapshot,
        },
    }

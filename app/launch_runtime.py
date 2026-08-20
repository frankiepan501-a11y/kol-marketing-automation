"""集中上稿活动运行器。

只负责三件事：追加系统可自动通过的活动参与人、生成活动草稿、
根据每日反馈决定继续扩池/保持/停止。真实发送始终由 auto_send 完成。
"""

from __future__ import annotations

import asyncio
import hashlib
import html
import json
import re
import time

from . import (
    config,
    auto_send,
    draft_router,
    enrich,
    feishu,
    launch_candidate_preview,
    launch_evidence,
    launch_outreach,
    launch_participation,
    keyword_supply,
    relabel,
    utm,
)
from .feishu import ext, xrid


class LaunchRuntimeError(RuntimeError):
    pass


_LOCKS: dict[str, asyncio.Lock] = {}
LAUNCH_QUEUE_TEMPLATE_VERSION = "launch-queue-v1"
RUNTIME_JOB_PREFIX = "[AUTONOMY_JOB]"
CAMPAIGN_REVIEW_VIEWS = {
    "launch-20260915-funlab-dave-ys11-5": "vewH5ud840",
    "launch-20260915-powkong-piranha-v2": "vewTsJRx9G",
}


def _ids(value) -> list[str]:
    return sorted(launch_evidence._ids(value))


def _queue_key(campaign_id: str, participant_id: str) -> str:
    digest = hashlib.sha1(f"{campaign_id}|{participant_id}".encode("utf-8")).hexdigest()[:20]
    return f"launchq-{digest}"


def _deterministic_fallback_draft(kol: dict, product: dict, brand: str) -> dict:
    """DeepSeek 余额/计费故障时的保守模板；不编造内容、不写价格或佣金。"""
    kf = kol.get("fields") or {}
    pf = product.get("fields") or {}
    kol_name = ext(kf.get("账号名")).strip() or "there"
    product_name = ext(pf.get("产品英文名")).strip()
    if not product_name:
        raise LaunchRuntimeError("备用模板要求产品英文名")
    country = ext(kf.get("国家")).strip()
    language = ext(kf.get("语言")).strip().lower() or enrich.COUNTRY_TO_LANG.get(country, "en")
    if language not in {"en", "de", "es"}:
        language = "en"
    links = []
    for label, url in feishu.product_links(pf):
        tracked = utm.make_utm_link(url, brand, product_name, kol_name)
        links.append((label, tracked))
    if not links:
        raise LaunchRuntimeError("备用模板要求至少一个产品链接")
    link_html = "".join(
        f'<p><a href="{html.escape(url, quote=True)}">{html.escape(label)}</a></p>'
        for label, url in links
    )
    safe_name = html.escape(kol_name)
    safe_product = html.escape(product_name)
    signature = "Tom from FUNLAB Team" if brand == "FUNLAB" else "Lisa @ POWKONG Team"
    if language == "de":
        subject = f"{kol_name}, ein Launch-Sample für dich"[:80]
        body = (
            f"<p>Hey {safe_name},</p><p>dein Gaming-Content passt gut zu unserem kommenden "
            f"Launch. Wir möchten dir gern den {safe_product} zum Testen anbieten.</p>{link_html}"
            "<p>Wir koordinieren die Berichterstattung rund um den 15. September. "
            "Hättest du Interesse, ein Sample unverbindlich auszuprobieren? Wir senden es dir gern zu.</p>"
            f"<p>-- {html.escape(signature)}</p>"
        )
    elif language == "es":
        subject = f"{kol_name}, una muestra para el lanzamiento"[:80]
        body = (
            f"<p>Hey {safe_name},</p><p>tu contenido de gaming encaja bien con nuestro próximo "
            f"lanzamiento. Nos gustaría ofrecerte el {safe_product} para que lo pruebes.</p>{link_html}"
            "<p>Estamos coordinando publicaciones alrededor del 15 de septiembre. "
            "¿Te interesaría probar una muestra sin compromiso? Estaremos encantados de enviártela.</p>"
            f"<p>-- {html.escape(signature)}</p>"
        )
    else:
        subject = f"{kol_name}, a launch sample for you"[:80]
        body = (
            f"<p>Hey {safe_name},</p><p>Your gaming content looks like a good fit for our upcoming "
            f"launch. We would love to offer you the {safe_product} to try.</p>{link_html}"
            "<p>We are coordinating coverage around September 15. Would you be curious to try a sample, "
            "with no strings attached? We would be happy to send one over.</p>"
            f"<p>-- {html.escape(signature)}</p>"
        )
    return {
        "subject": subject, "body": body,
        "highlights": "Activity filters and global duplicate-contact precheck passed.",
        "angle": "Upcoming September 15 launch sample.",
        "ban_phrase_failed": False, "utm_url": links[0][1],
        "utm_id": utm.kol_utm_id(kol_name), "language": language,
        "deterministic_fallback": True,
    }


def recommend_feedback_action(*, target_posts: int, target_commitments: int,
                              commitments: int, sent: int, replies: int,
                              ontime_posts: int = 0) -> dict:
    """只按本活动回填数据控制；达到活动承诺目标即停，接近目标先保持。"""
    target = max(1, int(target_posts or 0))
    stop_at = max(target, int(target_commitments or target))
    hold_at = max(target, stop_at - 2)
    if commitments >= stop_at or ontime_posts >= target:
        action = "stop"
        reason = f"本活动明确承诺 {commitments}/{stop_at}，预计按时上稿 {ontime_posts}/{target}"
    elif commitments >= hold_at:
        action = "hold"
        reason = f"承诺/深度洽谈 {commitments} 已达到备份线 {hold_at}"
    else:
        action = "expand"
        reply_rate = (replies / sent) if sent else 0
        reason = (
            f"承诺/深度洽谈 {commitments} 未达到备份线 {hold_at}；"
            f"已发 {sent}，回复 {replies}（{reply_rate:.1%}）"
        )
    return {
        "action": action, "reason": reason,
        "target_posts": target, "target_commitments": stop_at,
        "hold_at": hold_at, "stop_at": stop_at,
    }


async def _participants(campaign_id: str) -> list[dict]:
    return await feishu.search_records(config.T_LAUNCH_PARTICIPANT, [{
        "field_name": "活动ID", "operator": "is", "value": [campaign_id],
    }])


async def append_auto_approved(*, campaign_id: str, pool_target: int,
                               preview: dict | None = None,
                               allow_parallel_review: bool = False) -> dict:
    """只追加系统已有充分资料且 review_decision=通过 的新开发对象；不取消旧记录。"""
    if not config.LAUNCH_ACTIVITY_QUEUE_ENABLED:
        raise LaunchRuntimeError("LAUNCH_ACTIVITY_QUEUE_ENABLED 未开启")
    activity = await launch_evidence.get_activity(campaign_id)
    af = activity.get("fields") or {}
    product_id = ext(af.get("产品主记录ID")).strip()
    ranking_version = ext(af.get("证据排序版本")).strip()
    if not product_id or not ranking_version:
        raise LaunchRuntimeError("活动缺少产品主记录ID或证据排序版本")
    if ext(af.get("KOL名单阻塞代码")):
        raise LaunchRuntimeError("KOL 名单仍处于阻塞状态")

    preview = preview or await launch_candidate_preview.preview_candidates(
        product_id, campaign_id=campaign_id, object_type="KOL", internal_full=True,
    )
    if preview.get("ranking_version") != ranking_version:
        raise LaunchRuntimeError("预览排序版本与活动证据版本不一致")
    if preview.get("evidence_pending"):
        raise LaunchRuntimeError("竞品证据仍待处理，禁止自动补池")
    evidence_mode = ext(af.get("竞品证据模式"))
    if evidence_mode in {launch_evidence.MODE_NEW, launch_evidence.MODE_REUSE}:
        if (preview.get("evidence_status") != "已就绪"
                or not preview.get("competitor_evidence_applied")):
            raise LaunchRuntimeError("活动要求竞品证据，但证据未实际应用到排序")
    existing = await _participants(campaign_id)
    existing_contacts = {
        cid for row in existing for cid in _ids((row.get("fields") or {}).get("关联KOL"))
    }
    active_count = sum(
        ext((row.get("fields") or {}).get("参与状态")) in {"锁定准备中", "已入围"}
        for row in existing
    )
    outstanding_review = sum(
        ext((row.get("fields") or {}).get("参与状态")) in {"锁定准备中", "已入围"}
        and ext((row.get("fields") or {}).get("审核结论")) in {"待审核", "待补资料"}
        for row in existing
    )
    if outstanding_review and not allow_parallel_review:
        return {
            "campaign_id": campaign_id, "pool_before": active_count,
            "eligible_auto_approved": 0, "created": 0,
            "pool_after": active_count, "participant_ids": [],
            "blocked_by_pending_review": outstanding_review,
        }
    room = max(0, int(pool_target) - active_count)
    batch_room = min(room, 120)
    candidates = [
        c for c in (preview.get("candidates") or [])
        if c.get("contact_id") not in existing_contacts
        and c.get("decision") == "eligible_new_cold"
        and c.get("review_decision") == "通过"
    ][:batch_room]

    created = []
    for candidate in candidates:
        contact_id = candidate["contact_id"]
        unique_key = launch_participation.participant_key(
            campaign_id, product_id, "KOL", contact_id,
        )
        # 幂等回查；任何旧历史行都不自动复活。
        if await launch_participation._participants_by_unique_key(unique_key):
            continue
        fields = {
            "参与记录ID": unique_key, "活动ID": campaign_id,
            "关联活动": [activity["record_id"]], "产品家族ID": product_id,
            "对象类型": "KOL", "关联KOL": [contact_id],
            **launch_participation._ranking_fields(candidate, ranking_version),
            "参与状态": "已入围", "名单版本": ranking_version,
            "锁定批次ID": f"auto-{int(time.time())}", "取消原因代码": "",
            "排序快照历史": launch_participation._with_snapshot(
                {}, candidate, ranking_version,
            ),
        }
        record_id = await feishu.create_record(config.T_LAUNCH_PARTICIPANT, fields)
        created.append(record_id)
    return {
        "campaign_id": campaign_id, "pool_before": active_count,
        "eligible_auto_approved": len(candidates), "created": len(created),
        "pool_after": active_count + len(created), "participant_ids": created,
        "pending_review_kept_parallel": outstanding_review if allow_parallel_review else 0,
    }


async def append_review_candidates(*, campaign_id: str, review_target: int = 20,
                                   preview: dict | None = None,
                                   operator_only: bool = False) -> dict:
    """补充一个待审核批次；不生成草稿，也不沿用系统自动通过结论。"""
    if not config.LAUNCH_ACTIVITY_QUEUE_ENABLED:
        raise LaunchRuntimeError("LAUNCH_ACTIVITY_QUEUE_ENABLED 未开启")
    activity = await launch_evidence.get_activity(campaign_id)
    af = activity.get("fields") or {}
    product_id = ext(af.get("产品主记录ID")).strip()
    ranking_version = ext(af.get("证据排序版本")).strip()
    if not product_id or not ranking_version:
        raise LaunchRuntimeError("活动缺少产品主记录ID或证据排序版本")
    if ext(af.get("KOL名单阻塞代码")):
        raise LaunchRuntimeError("KOL 名单仍处于阻塞状态")

    preview = preview or await launch_candidate_preview.preview_candidates(
        product_id, campaign_id=campaign_id, object_type="KOL", internal_full=True,
    )
    if preview.get("ranking_version") != ranking_version:
        raise LaunchRuntimeError("预览排序版本与活动证据版本不一致")
    if preview.get("evidence_pending"):
        raise LaunchRuntimeError("竞品证据仍待处理，禁止补充审核池")
    evidence_mode = ext(af.get("竞品证据模式"))
    if evidence_mode in {launch_evidence.MODE_NEW, launch_evidence.MODE_REUSE}:
        if (preview.get("evidence_status") != "已就绪"
                or not preview.get("competitor_evidence_applied")):
            raise LaunchRuntimeError("活动要求竞品证据，但证据未实际应用到排序")

    existing = await _participants(campaign_id)
    existing_contacts = {
        cid for row in existing for cid in _ids((row.get("fields") or {}).get("关联KOL"))
    }
    pending_count = sum(
        ext((row.get("fields") or {}).get("参与状态")) in {"锁定准备中", "已入围"}
        and ext((row.get("fields") or {}).get("审核结论")) in {"待审核", "待补资料"}
        for row in existing
    )
    target = max(1, min(int(review_target), 50))
    room = min(max(0, target - pending_count), 20)
    candidates = [
        candidate for candidate in (preview.get("candidates") or [])
        if candidate.get("contact_id") not in existing_contacts
        and candidate.get("decision") == "eligible_new_cold"
        and (
            candidate.get("base_filter_passed") is True
            or (
                operator_only
                and candidate.get("profile_refresh_needed") is True
                and candidate.get("review_decision") == "待补资料"
            )
        )
        and (
            not operator_only
            or (
                candidate.get("review_route") == "KOL运营审核"
                and candidate.get("review_decision") != "通过"
            )
        )
    ][:room]

    batch_id = f"review-{time.strftime('%Y%m%d')}-{int(time.time())}"
    created: list[str] = []
    try:
        for candidate in candidates:
            contact_id = candidate["contact_id"]
            unique_key = launch_participation.participant_key(
                campaign_id, product_id, "KOL", contact_id,
            )
            if await launch_participation._participants_by_unique_key(unique_key):
                continue
            ranking_fields = launch_participation._ranking_fields(candidate, ranking_version)
            pending_decision = (
                "待补资料" if candidate.get("review_decision") == "待补资料" else "待审核"
            )
            ranking_fields["审核结论"] = pending_decision
            ranking_fields["审核原因"] = ""
            ranking_fields["审核人"] = None
            ranking_fields["审核时间"] = None
            reason_codes = _pending_review_reason_codes(candidate)
            if reason_codes:
                ranking_fields["审核原因代码"] = reason_codes
            if ranking_fields.get("系统审核分流") == "系统建议通过":
                ranking_fields["系统审核说明"] = (
                    "系统规则已建议通过；本批为新品活动候选质量灰度，请运营抽检主页、"
                    "近90天内容和实际语言后选择通过/待补资料/排除。"
                )
            fields = {
                "参与记录ID": unique_key, "活动ID": campaign_id,
                "关联活动": [activity["record_id"]], "产品家族ID": product_id,
                "对象类型": "KOL", "关联KOL": [contact_id],
                **ranking_fields,
                "参与状态": "已入围", "名单版本": ranking_version,
                "锁定批次ID": batch_id, "取消原因代码": "",
                "排序快照历史": launch_participation._with_snapshot(
                    {}, candidate, ranking_version,
                ),
            }
            record_id = await feishu.create_record(config.T_LAUNCH_PARTICIPANT, fields)
            created.append(record_id)
            matches = await launch_participation._participants_by_unique_key(unique_key)
            if len(matches) != 1:
                raise LaunchRuntimeError("待审核候选创建后唯一键回读不一致")
            readback = matches[0].get("fields") or {}
            if ext(readback.get("审核结论")) != pending_decision:
                raise LaunchRuntimeError("待审核候选被意外放行为通过")
            if _ids(readback.get("关联邮件草稿")):
                raise LaunchRuntimeError("待审核候选意外关联邮件草稿")
    except Exception:
        for record_id in created:
            try:
                await feishu.update_record(config.T_LAUNCH_PARTICIPANT, record_id, {
                    "参与状态": "已取消", "取消原因代码": "P1待审核补池失败回滚",
                })
            except Exception:
                pass
        raise

    return {
        "campaign_id": campaign_id, "review_target": target,
        "pending_before": pending_count, "eligible_candidates": len(candidates),
        "created": len(created), "pending_after": pending_count + len(created),
        "batch_id": batch_id, "participant_ids": created,
        "drafts_created": 0, "emails_sent": 0,
    }


def _pending_review_reason_codes(candidate: dict) -> list[str]:
    """把机器筛选原因收敛为运营可复用的活动原因码。"""
    mapping = {
        "目标主机不匹配": "目标主机不匹配",
        "Nintendo/Mario受众或近期硬件内容不匹配": "核心游戏/IP不匹配",
        "近期目标游戏/主机内容占比不足": "核心游戏/IP不匹配",
        "近期内容缺少Nintendo/Switch或硬件评测证据": "核心游戏/IP不匹配",
        "内容风格不匹配": "硬件/配件内容不足",
        "内容垂类不是主机游戏或游戏硬件评测": "非游戏或泛娱乐",
        "最近发布记录缺失或过期": "活跃度不足",
        "国家不在活动目标市场": "地区/语言不匹配",
        "国家不在销售市场": "地区/语言不匹配",
        "语言不在活动目标范围": "地区/语言不匹配",
        "语言不匹配": "地区/语言不匹配",
        "资料缺失或过期": "资料缺失或过期",
        "人工核实已过期": "资料缺失或过期",
        "标签版本不是v2": "资料缺失或过期",
    }
    values = []
    for reason in candidate.get("base_filter_reasons") or []:
        code = mapping.get(str(reason).strip())
        if code and code not in values:
            values.append(code)
    return values


async def _find_queue_draft(queue_key: str) -> dict | None:
    rows = await feishu.search_records(config.T_DRAFT, [{
        "field_name": "邮件草稿ID", "operator": "is", "value": [queue_key],
    }])
    exact = [r for r in rows if ext((r.get("fields") or {}).get("邮件草稿ID")) == queue_key]
    if len(exact) > 1:
        raise LaunchRuntimeError(f"活动队列草稿ID重复: {queue_key}")
    return exact[0] if exact else None


async def _queue_one(*, activity: dict, participant: dict, product: dict,
                     brand: str) -> dict:
    af = activity.get("fields") or {}
    pf = participant.get("fields") or {}
    campaign_id = ext(af.get("活动ID"))
    product_id = ext(af.get("产品主记录ID"))
    participant_id = participant["record_id"]
    contact_ids = _ids(pf.get("关联KOL"))
    if len(contact_ids) != 1:
        raise LaunchRuntimeError("活动参与记录必须且只能关联一个 KOL")
    contact_id = contact_ids[0]
    queue_key = _queue_key(campaign_id, participant_id)
    existing = await _find_queue_draft(queue_key)
    if existing:
        if existing["record_id"] not in _ids(pf.get("关联邮件草稿")):
            await feishu.update_record(
                config.T_LAUNCH_PARTICIPANT, participant_id,
                {"关联邮件草稿": [existing["record_id"]]},
            )
        return {"participant_id": participant_id, "draft_id": existing["record_id"], "reused": True}
    if _ids(pf.get("关联邮件草稿")):
        return {"participant_id": participant_id, "skipped": "participant_already_has_draft"}

    kol = await feishu.get_record(config.T_KOL, contact_id)
    precheck = await launch_outreach._fast_precheck(
        kol=kol, product=product, product_id=product_id,
        contact_id=contact_id, brand=brand,
    )
    if precheck.get("decision") != "eligible_new_cold":
        return {
            "participant_id": participant_id, "skipped": precheck.get("decision") or "precheck_failed",
        }
    email, reason = feishu.clean_email(ext((kol.get("fields") or {}).get("邮箱")))
    if not email:
        return {"participant_id": participant_id, "skipped": f"bad_email:{reason}"}

    score = float(pf.get("基础评分快照") or 0)
    signature = "Tom from FUNLAB Team" if brand == "FUNLAB" else "Lisa @ POWKONG Team"
    generated = await enrich.gen_draft(
        kol, product, brand, signature,
        {"活动名单": {"score": score, "reason": "活动名单和全局重复触达预检已通过"}},
        score,
    )
    if generated.get("error") and "402 Payment Required" in str(generated.get("error")):
        generated = _deterministic_fallback_draft(kol, product, brand)
    if generated.get("error") or generated.get("skip"):
        return {
            "participant_id": participant_id,
            "skipped": str(generated.get("error") or generated.get("skip")),
        }
    subject = str(generated.get("subject") or "").strip()
    body = str(generated.get("body") or "").strip()
    if not subject or len(re.sub(r"<[^>]+>", "", body)) < 50:
        return {"participant_id": participant_id, "skipped": "generated_body_invalid"}
    body += (
        '<span style="display:none;font-size:0;color:transparent">'
        f"launch-queue:{queue_key};template:{LAUNCH_QUEUE_TEMPLATE_VERSION}</span>"
    )
    now_ms = int(time.time() * 1000)
    fields = {
        "邮件草稿ID": queue_key, "关联KOL": [contact_id], "关联产品": [product_id],
        "匹配度总分": score, "匹配亮点": str(generated.get("highlights") or "")[:500],
        "建议切入点": str(generated.get("angle") or "")[:200], "收件邮箱": email,
        "邮件主题": subject[:200], "邮件正文": body,
        "邮件语言": generated.get("language") or "en",
        "邮件草稿状态": "待审", "邮件草稿来源": "cold", "对象类型": "KOL",
        "发送邮箱": config.BRAND_CONFIG[brand]["sender_label"], "发送人署名": signature,
        "生成时间": now_ms, "建议发送时间": now_ms,
        "发送时区说明": "集中上稿活动；由统一发送中心按品牌额度发送",
        "重生次数": 0, "UTM 链接": generated.get("utm_url") or "",
        "审批意见": (
            f"[活动队列] campaign={campaign_id}; participant={participant_id}; "
            f"ranking={ext(pf.get('排序版本'))}; 真实发送仍需活动正式授权"
        )[:500],
    }
    draft_id = await feishu.create_record(config.T_DRAFT, fields)
    await feishu.update_record(
        config.T_LAUNCH_PARTICIPANT, participant_id, {"关联邮件草稿": [draft_id]},
    )
    readback = await feishu.get_record(config.T_LAUNCH_PARTICIPANT, participant_id)
    if draft_id not in _ids((readback.get("fields") or {}).get("关联邮件草稿")):
        raise LaunchRuntimeError("草稿已创建但参与记录关联回读失败，已阻止放行")
    utm_id = str(generated.get("utm_id") or "").strip()
    if utm_id:
        current_utm = ext((kol.get("fields") or {}).get("UTM ID"))
        if not current_utm:
            await feishu.update_record(config.T_KOL, contact_id, {"UTM ID": utm_id})
    if generated.get("ban_phrase_failed"):
        await feishu.update_record(config.T_DRAFT, draft_id, {
            "审核路径": "需人改",
            "AI评分理由": "[ban-phrase] 重生后仍命中禁用句式，禁止自动发送",
        })
        return {"participant_id": participant_id, "draft_id": draft_id,
                "reused": False, "path": "需人改"}
    if generated.get("deterministic_fallback"):
        await feishu.update_record(config.T_DRAFT, draft_id, {
            "邮件草稿状态": "自动通过", "审核路径": "自动通过",
            "AI评分理由": "[fallback-template] DeepSeek 402；使用无价格、无虚构内容的受控活动模板",
        })
        return {"participant_id": participant_id, "draft_id": draft_id,
                "reused": False, "path": "自动通过-受控备用模板"}
    route = await draft_router.route_draft(draft_id)
    return {"participant_id": participant_id, "draft_id": draft_id,
            "reused": False, "path": route.get("path"), "score": route.get("score")}


async def queue_approved(*, campaign_id: str, limit: int = 120) -> dict:
    """为已审通过的新开发参与人生成草稿；不发送。"""
    if not config.LAUNCH_ACTIVITY_QUEUE_ENABLED:
        raise LaunchRuntimeError("LAUNCH_ACTIVITY_QUEUE_ENABLED 未开启")
    activity = await launch_evidence.get_activity(campaign_id)
    af = activity.get("fields") or {}
    product_id = ext(af.get("产品主记录ID")).strip()
    product = await feishu.get_record(config.T_PRODUCT, product_id)
    brand = config.brand_from_text(ext((product.get("fields") or {}).get("品牌")))
    if brand not in config.BRAND_CONFIG:
        raise LaunchRuntimeError("活动产品品牌无法匹配 Zoho 发件账号")
    if ext((product.get("fields") or {}).get("派单模式")) != "活动专用":
        raise LaunchRuntimeError("活动产品未处于活动专用锁")
    rows = await _participants(campaign_id)
    eligible = [
        row for row in rows
        if ext((row.get("fields") or {}).get("参与状态")) == "已入围"
        and ext((row.get("fields") or {}).get("审核结论")) == "通过"
        and ext((row.get("fields") or {}).get("进入方式")) == "新开发"
        and ext((row.get("fields") or {}).get("活动分池")) == "新开发池"
        and not _ids((row.get("fields") or {}).get("关联邮件草稿"))
    ][:max(1, min(int(limit), 500))]
    semaphore = asyncio.Semaphore(4)

    async def one(row):
        async with semaphore:
            try:
                return await _queue_one(
                    activity=activity, participant=row, product=product, brand=brand,
                )
            except Exception as exc:
                return {"participant_id": row.get("record_id"), "error": str(exc)[:300]}

    details = await asyncio.gather(*(one(row) for row in eligible))
    return {
        "campaign_id": campaign_id, "brand": brand, "eligible": len(eligible),
        "queued": sum(bool(x.get("draft_id")) and not x.get("reused") for x in details),
        "reused": sum(bool(x.get("reused")) for x in details),
        "skipped_or_failed": sum(not x.get("draft_id") for x in details),
        "details": details,
    }


async def run_campaign(*, campaign_id: str, pool_target: int = 100,
                       queue_limit: int = 120) -> dict:
    lock = _LOCKS.setdefault(campaign_id, asyncio.Lock())
    if lock.locked():
        return {"campaign_id": campaign_id, "already_running": True}
    async with lock:
        preview = await launch_candidate_preview.preview_candidates(
            "", campaign_id=campaign_id, object_type="KOL", internal_full=True,
        )
        appended = await append_auto_approved(
            campaign_id=campaign_id, pool_target=pool_target, preview=preview,
        )
        queued = await queue_approved(campaign_id=campaign_id, limit=queue_limit)
        return {
            "campaign_id": campaign_id, "already_running": False,
            "preview_summary": preview.get("summary") or {},
            "append": appended, "queue": queued,
        }


async def campaign_metrics(campaign_id: str) -> dict:
    activity = await launch_evidence.get_activity(campaign_id)
    af = activity.get("fields") or {}
    participants = await _participants(campaign_id)
    draft_ids = {
        did for row in participants for did in _ids((row.get("fields") or {}).get("关联邮件草稿"))
    }
    drafts = await feishu.fetch_all_records(
        config.T_DRAFT, field_names=["发送状态", "是否回复"], page_size=500,
    )
    sent = sum(
        row.get("record_id") in draft_ids
        and ext((row.get("fields") or {}).get("发送状态")) in {"已发", "已发送"}
        for row in drafts
    )
    replies = sum(
        row.get("record_id") in draft_ids
        and bool((row.get("fields") or {}).get("是否回复"))
        for row in drafts
    )
    try:
        window_end = int(af.get("窗口结束") or 0)
    except (TypeError, ValueError):
        window_end = 0
    promised_times = []
    for row in participants:
        try:
            value = int((row.get("fields") or {}).get("承诺上稿时间") or 0)
        except (TypeError, ValueError):
            value = 0
        if value:
            promised_times.append(value)
    commitments = len(promised_times)
    ontime_posts = sum(not window_end or value <= window_end for value in promised_times)
    try:
        target_posts = int(af.get("目标上稿数") or 20)
    except (TypeError, ValueError):
        target_posts = 20
    try:
        target_commitments = int(af.get("目标承诺数") or target_posts)
    except (TypeError, ValueError):
        target_commitments = target_posts
    control = recommend_feedback_action(
        target_posts=target_posts, target_commitments=target_commitments,
        commitments=commitments, sent=sent, replies=replies, ontime_posts=ontime_posts,
    )
    return {
        "campaign_id": campaign_id, "participants": len(participants),
        "sent": sent, "replies": replies, "commitments": commitments,
        "ontime_posts": ontime_posts,
        **control,
    }


async def _brand_quota_snapshot(brand: str) -> dict:
    counts, errors = await auto_send.zoho_sent_counts_24h([brand])
    if errors.get(brand):
        raise LaunchRuntimeError(f"{brand} Zoho滚动24小时计数失败，自治补池已停止")
    sent = max(0, int(counts.get(brand, 0) or 0))
    cap = max(1, int(auto_send.SEND_DAILY_CAP))
    return {
        "brand": brand, "cap": cap, "sent_24h": sent,
        "remaining": max(0, cap - sent),
    }


async def _campaign_ready_inventory(campaign_id: str) -> dict:
    participants = await _participants(campaign_id)
    active = [
        row for row in participants
        if ext((row.get("fields") or {}).get("参与状态")) in {"锁定准备中", "已入围"}
    ]
    draft_ids = {
        draft_id for row in active
        for draft_id in _ids((row.get("fields") or {}).get("关联邮件草稿"))
    }
    drafts = await feishu.fetch_all_records(
        config.T_DRAFT,
        field_names=["邮件草稿状态", "发送状态", "建议发送时间"],
        page_size=500,
    )
    approved_unsent = 0
    due_now = 0
    now_ms = int(time.time() * 1000)
    for row in drafts:
        if row.get("record_id") not in draft_ids:
            continue
        fields = row.get("fields") or {}
        if ext(fields.get("邮件草稿状态")) not in {"自动通过", "通过"}:
            continue
        if ext(fields.get("发送状态")) not in {"", "未发"}:
            continue
        approved_unsent += 1
        try:
            scheduled = int(fields.get("建议发送时间") or 0)
        except (TypeError, ValueError):
            scheduled = 0
        if not scheduled or scheduled <= now_ms:
            due_now += 1
    return {
        "participants": len(participants), "active_participants": len(active),
        "ready": approved_unsent, "due_now": due_now,
        "pending_review": sum(
            ext((row.get("fields") or {}).get("审核结论")) in {"待审核", "待补资料"}
            for row in active
        ),
    }


async def _notify_operator_review(*, campaign_id: str, activity: dict,
                                  created: int) -> dict:
    if created <= 0:
        return {"sent": 0, "targets": 0}
    targets = await feishu.fetch_users_by_job_title(config.KOL_REVIEWER_JOB_TITLE)
    if not targets:
        return {"sent": 0, "targets": 0, "error": "未找到在职KOL运营审核人"}
    activity_name = (
        ext((activity.get("fields") or {}).get("活动名称")).strip() or campaign_id
    )
    table_url = (
        f"https://u1wpma3xuhr.feishu.cn/base/{config.FEISHU_APP_TOKEN}"
        f"?table={config.T_LAUNCH_PARTICIPANT}"
    )
    review_view = CAMPAIGN_REVIEW_VIEWS.get(campaign_id)
    if review_view:
        table_url += f"&view={review_view}"
    card = {
        "config": {"wide_screen_mode": True},
        "header": {
            "template": "blue",
            "title": {"tag": "plain_text", "content": "活动候选待运营审核"},
        },
        "elements": [
            {"tag": "div", "fields": [
                {"is_short": True, "text": {"tag": "lark_md", "content": f"**活动**\n{activity_name}"}},
                {"is_short": True, "text": {"tag": "lark_md", "content": f"**新增边界项**\n{created} 名"}},
            ]},
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md", "content": (
                "系统已先检查国家、语言、平台、粉丝范围、邮箱和全局重复触达。"
                "请只打开达人主页，核对近3个月内容品类、实际语言及缺失资料，"
                "在活动专属审核视图回填通过 / 待补资料 / 排除和原因。"
                "本批无需 Frankie 逐条审核。"
            )}},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"[打开活动参与记录]({table_url})"}},
        ],
    }
    sent = 0
    errors = []
    for name, open_id in targets:
        try:
            await feishu.send_card_message(
                "open_id", open_id, card, biz="KOL", level="P2",
            )
            sent += 1
        except Exception as exc:
            errors.append(f"{name}: {str(exc)[:120]}")
    return {"sent": sent, "targets": len(targets), "errors": errors}


async def autonomous_refill(*, campaign_id: str, buffer_days: int = 2,
                            queue_limit: int = 120, review_target: int = 20,
                            profile_refresh_limit: int = 30) -> dict:
    """按活动进度与邮箱余量自治补池；不直接发送邮件，也不降低筛选标准。"""
    lock = _LOCKS.setdefault(campaign_id, asyncio.Lock())
    if lock.locked():
        return {"campaign_id": campaign_id, "already_running": True}
    async with lock:
        activity = await launch_evidence.get_activity(campaign_id)
        activity_fields = activity.get("fields") or {}
        if (
            ext(activity_fields.get("运行模式")) != "正式运行"
            or ext(activity_fields.get("状态")) != "正式执行中"
        ):
            return {
                "campaign_id": campaign_id, "already_running": False,
                "action": "hold", "held": True,
                "runtime": "campaign_not_formally_active",
                "reason": "活动不是正式运行/正式执行中，自治补池保持暂停",
                "quality_filters_lowered": False,
            }
        try:
            window_end = int(activity_fields.get("窗口结束") or 0)
        except (TypeError, ValueError):
            window_end = 0
        if window_end and int(time.time() * 1000) > window_end:
            await feishu.update_record(
                config.T_LAUNCH_CAMPAIGN, activity["record_id"],
                {"发送邮件授权": False},
            )
            return {
                "campaign_id": campaign_id, "already_running": False,
                "action": "stop", "stopped": True,
                "runtime": "campaign_window_ended",
                "reason": "活动窗口已结束，已关闭邮件授权并停止补池",
                "quality_filters_lowered": False,
            }
        metrics = await campaign_metrics(campaign_id)
        if metrics["action"] == "stop":
            await feishu.update_record(
                config.T_LAUNCH_CAMPAIGN, activity["record_id"],
                {"发送邮件授权": False},
            )
            return {**metrics, "already_running": False, "stopped": True}
        if metrics["action"] == "hold":
            return {**metrics, "already_running": False, "held": True}

        product_id = ext(activity_fields.get("产品主记录ID")).strip()
        if not product_id:
            raise LaunchRuntimeError("活动缺少产品主记录ID")
        product = await feishu.get_record(config.T_PRODUCT, product_id)
        brand = config.brand_from_text(ext((product.get("fields") or {}).get("品牌")))
        if brand not in config.BRAND_CONFIG:
            raise LaunchRuntimeError("活动产品品牌无法匹配Zoho邮箱")
        quota = await _brand_quota_snapshot(brand)
        days = max(1, min(int(buffer_days), 3))
        target_ready = min(
            auto_send.SEND_DAILY_CAP * 2,
            max(auto_send.SEND_DAILY_CAP, quota["remaining"] * days),
        )
        inventory_before = await _campaign_ready_inventory(campaign_id)
        if inventory_before["ready"] >= target_ready:
            return {
                **metrics, "already_running": False, "brand": brand,
                "quota": quota, "target_ready_inventory": target_ready,
                "inventory_before": inventory_before["ready"],
                "inventory_after": inventory_before["ready"],
                "runtime": "inventory_sufficient",
            }

        preview = await launch_candidate_preview.preview_candidates(
            "", campaign_id=campaign_id, object_type="KOL", internal_full=True,
        )
        deficit = max(0, target_ready - inventory_before["ready"])
        first_append = await append_auto_approved(
            campaign_id=campaign_id,
            pool_target=metrics["participants"] + min(deficit, 120),
            preview=preview, allow_parallel_review=True,
        )
        first_queue = await queue_approved(campaign_id=campaign_id, limit=queue_limit)
        inventory_after_master = await _campaign_ready_inventory(campaign_id)

        refresh_result = {"processed": 0, "writes": 0}
        second_append = {"created": 0}
        second_queue = {"queued": 0}
        latest_preview = preview
        if inventory_after_master["ready"] < target_ready:
            refresh_ids = list(dict.fromkeys(
                preview.get("profile_refresh_candidate_ids") or []
            ))[:max(0, min(int(profile_refresh_limit), 100))]
            if refresh_ids:
                refresh_result = await relabel.run_profile_records(
                    refresh_ids, dry_run=False, limit=len(refresh_ids),
                )
                latest_preview = await launch_candidate_preview.preview_candidates(
                    "", campaign_id=campaign_id, object_type="KOL", internal_full=True,
                )
                remaining = max(0, target_ready - inventory_after_master["ready"])
                second_append = await append_auto_approved(
                    campaign_id=campaign_id,
                    pool_target=(
                        int(inventory_after_master.get("active_participants")
                            or metrics["participants"]) + min(remaining, 120)
                    ),
                    preview=latest_preview, allow_parallel_review=True,
                )
                second_queue = await queue_approved(
                    campaign_id=campaign_id, limit=queue_limit,
                )
        inventory_after = await _campaign_ready_inventory(campaign_id)
        remaining = max(0, target_ready - inventory_after["ready"])

        discovery = {"ok": True, "created": 0, "skipped": "inventory_sufficient"}
        review_pool = {"created": 0}
        review_notification = {"sent": 0}
        if remaining:
            discovery = await keyword_supply.ensure_campaign_supply(
                campaign_id=campaign_id, activity=activity, product=product,
                required_candidates=remaining, dry_run=False,
            )
            review_preview = dict(latest_preview)
            review_preview["candidates"] = list(latest_preview.get("candidates") or []) + list(
                latest_preview.get("profile_refresh_candidates") or []
            )
            review_pool = await append_review_candidates(
                campaign_id=campaign_id, review_target=review_target,
                preview=review_preview, operator_only=True,
            )
            review_notification = await _notify_operator_review(
                campaign_id=campaign_id, activity=activity,
                created=int(review_pool.get("created") or 0),
            )

        result = {
            **metrics, "already_running": False, "brand": brand,
            "quota": quota, "target_ready_inventory": target_ready,
            "inventory_before": inventory_before["ready"],
            "inventory_after_master": inventory_after_master["ready"],
            "inventory_after": inventory_after["ready"],
            "append": first_append, "queue": first_queue,
            "profile_refresh": refresh_result,
            "append_after_refresh": second_append,
            "queue_after_refresh": second_queue,
            "discovery": discovery, "review_pool": review_pool,
            "review_notification": review_notification,
            "quality_filters_lowered": False,
        }
        return _with_business_outcome(result)


def _section_count(result: dict, section: str, key: str) -> int:
    try:
        return max(0, int((result.get(section) or {}).get(key) or 0))
    except (TypeError, ValueError):
        return 0


def _with_business_outcome(result: dict) -> dict:
    """给后台任务补业务结果；HTTP没报错不再等同于补池成功。"""
    progress = sum((
        _section_count(result, "append", "created"),
        _section_count(result, "queue", "queued"),
        _section_count(result, "profile_refresh", "writes"),
        _section_count(result, "append_after_refresh", "created"),
        _section_count(result, "queue_after_refresh", "queued"),
        _section_count(result, "discovery", "created"),
        _section_count(result, "review_pool", "created"),
    ))
    try:
        remaining = max(0, int((result.get("quota") or {}).get("remaining") or 0))
    except (TypeError, ValueError):
        remaining = 0
    try:
        inventory_after = max(0, int(result.get("inventory_after") or 0))
    except (TypeError, ValueError):
        inventory_after = 0

    if result.get("stopped") or result.get("action") == "stop":
        outcome = "stopped"
    elif result.get("held") or result.get("action") == "hold":
        outcome = "held"
    elif result.get("runtime") == "inventory_sufficient":
        outcome = "inventory_sufficient"
    elif remaining <= 0:
        outcome = "quota_exhausted"
    elif inventory_after > 0:
        outcome = "ready_inventory_created"
    elif progress > 0:
        outcome = "supply_in_progress"
    elif result.get("action") == "expand":
        outcome = "supply_blocked"
    else:
        outcome = "no_action_needed"
    return {**result, "business_outcome": outcome, "supply_progress": progress}


def runtime_job_status(result: dict | None) -> str:
    return "degraded" if (result or {}).get("business_outcome") == "supply_blocked" else "success"


def _runtime_result_summary(result: dict | None) -> dict:
    result = result or {}
    quota = result.get("quota") or {}
    return {
        key: result.get(key) for key in (
            "action", "brand", "participants", "sent", "replies", "commitments",
            "ontime_posts", "target_ready_inventory", "inventory_before", "inventory_after",
            "inventory_after_master", "stopped", "held", "quality_filters_lowered",
            "runtime", "business_outcome", "supply_progress",
        ) if key in result
    } | ({"quota": quota} if quota else {})


async def load_runtime_job(campaign_id: str, job_id: str = "") -> dict | None:
    activity = await launch_evidence.get_activity(campaign_id)
    note = ext((activity.get("fields") or {}).get("数据口径备注"))
    for line in reversed(note.splitlines()):
        if not line.startswith(RUNTIME_JOB_PREFIX):
            continue
        try:
            payload = json.loads(line[len(RUNTIME_JOB_PREFIX):])
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if not job_id or payload.get("job_id") == job_id:
            return payload
    return None


async def persist_runtime_job(*, campaign_id: str, job_id: str, mode: str,
                              status: str, result: dict | None = None,
                              error: str = "", started_ts: float | None = None) -> dict:
    activity = await launch_evidence.get_activity(campaign_id)
    fields = activity.get("fields") or {}
    now_ts = int(time.time())
    previous = None
    clean_lines = []
    for line in ext(fields.get("数据口径备注")).splitlines():
        if line.startswith(RUNTIME_JOB_PREFIX):
            try:
                value = json.loads(line[len(RUNTIME_JOB_PREFIX):])
                if value.get("job_id") == job_id:
                    previous = value
            except (TypeError, ValueError, json.JSONDecodeError):
                pass
            continue
        clean_lines.append(line)
    payload = {
        "job_id": job_id, "campaign_id": campaign_id, "mode": mode,
        "status": status,
        "started_ts": int(started_ts or (previous or {}).get("started_ts") or now_ts),
        "updated_ts": now_ts,
    }
    if status in {"success", "degraded"}:
        payload["result"] = _runtime_result_summary(result)
    if error:
        payload["error"] = str(error)[:300]
    line = RUNTIME_JOB_PREFIX + json.dumps(
        payload, ensure_ascii=False, separators=(",", ":"),
    )
    base = "\n".join(clean_lines).strip()
    note = ((base[-1800:] + "\n") if base else "") + line
    await feishu.update_record(
        config.T_LAUNCH_CAMPAIGN, activity["record_id"],
        {"数据口径备注": note[-3000:]},
    )
    return payload


async def daily_feedback(campaign_id: str) -> dict:
    metrics = await campaign_metrics(campaign_id)
    activity = await launch_evidence.get_activity(campaign_id)
    af = activity.get("fields") or {}
    note = (
        f"每日反馈 {time.strftime('%Y-%m-%d %H:%M')}：参与{metrics['participants']}，"
        f"已发{metrics['sent']}，回复{metrics['replies']}，明确承诺{metrics['commitments']}，"
        f"预计按时上稿{metrics['ontime_posts']}；"
        f"动作={metrics['action']}；{metrics['reason']}"
    )
    history = ext(af.get("数据口径备注"))
    await feishu.update_record(config.T_LAUNCH_CAMPAIGN, activity["record_id"], {
        "数据口径备注": (history + "\n" + note).strip()[-3000:],
    })
    if metrics["action"] == "stop":
        await feishu.update_record(
            config.T_LAUNCH_CAMPAIGN, activity["record_id"], {"发送邮件授权": False},
        )
        return {**metrics, "runtime": {"stopped": True}}
    if metrics["action"] == "expand":
        target = max(100, metrics["participants"] + 100)
        runtime = await run_campaign(
            campaign_id=campaign_id, pool_target=target, queue_limit=120,
        )
        return {**metrics, "runtime": runtime}
    return {**metrics, "runtime": {"held": True}}

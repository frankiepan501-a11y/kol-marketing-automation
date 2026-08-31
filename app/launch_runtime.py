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
    launch_evidence_author_import,
    launch_outcomes,
    launch_outreach,
    launch_participation,
    keyword_supply,
    relabel,
    utm,
)
from .feishu import ext, xrid
from .enrich_model_guard import EnrichModelBudget


class LaunchRuntimeError(RuntimeError):
    pass


_LOCKS: dict[str, asyncio.Lock] = {}
_JOB_NOTE_LOCKS: dict[str, asyncio.Lock] = {}
_ZERO_MODEL_CANDIDATE_PREVIEW_TIMEOUT_SECONDS = 120
LAUNCH_QUEUE_TEMPLATE_VERSION = "launch-queue-v1"
RUNTIME_JOB_PREFIX = "[AUTONOMY_JOB]"
CAMPAIGN_REVIEW_VIEWS = {
    "launch-20260915-funlab-dave-ys11-5": "vewH5ud840",
    "launch-20260915-powkong-piranha-v2": "vewTsJRx9G",
}
CAMPAIGN_LAUNCH_DATES = {
    "launch-20260915-funlab-dave-ys11-5": {
        "en": "September 15", "de": "15. September", "es": "15 de septiembre",
    },
    "launch-20260915-powkong-piranha-v2": {
        "en": "September 15", "de": "15. September", "es": "15 de septiembre",
    },
}
ACTIVE_PARTICIPANT_STATES = frozenset({"锁定准备中", "已入围"})
PENDING_REVIEW_DECISIONS = frozenset({"待审核", "待补资料"})
QUEUEABLE_INFLIGHT_TTL_SECONDS = 30 * 60


def _non_review_active_count(active_count: int, pending_review: int) -> int:
    """待审是运营旁路，不占用明确可开发池名额。"""
    return max(0, int(active_count or 0) - int(pending_review or 0))


def _effective_ready_inventory(inventory: dict | None) -> int:
    """可发草稿与已通过待生成草稿共同构成在途供给。"""
    inventory = inventory or {}
    return max(0, int(inventory.get("ready") or 0)) + max(
        0, int(inventory.get("queueable_approved") or 0),
    )


def _participant_activity_ts(row: dict) -> int:
    """取最近锁定/审核/创建时间，统一为秒。"""
    fields = row.get("fields") or {}
    timestamps: list[int] = []
    batch_match = re.search(r"-(\d{10})$", ext(fields.get("锁定批次ID")))
    if batch_match:
        timestamps.append(int(batch_match.group(1)))
    for value in (fields.get("审核时间"), row.get("created_time")):
        try:
            timestamp = int(value or 0)
        except (TypeError, ValueError):
            continue
        if timestamp > 10_000_000_000:
            timestamp //= 1000
        if timestamp > 0:
            timestamps.append(timestamp)
    return max(timestamps, default=0)


def _is_recent_queueable(row: dict, *, now_ts: int | None = None) -> bool:
    activity_ts = _participant_activity_ts(row)
    if not activity_ts:
        return False
    age = int(now_ts or time.time()) - activity_ts
    return -300 <= age <= QUEUEABLE_INFLIGHT_TTL_SECONDS


def new_launch_model_budget() -> EnrichModelBudget:
    return EnrichModelBudget(
        per_task=config.KOL_LAUNCH_MODEL_PER_TASK,
        per_run=config.KOL_LAUNCH_MODEL_PER_RUN,
        daily=config.KOL_LAUNCH_MODEL_DAILY,
        failure_threshold=config.KOL_LAUNCH_MODEL_FAILURE_THRESHOLD,
        state_path=config.KOL_LAUNCH_MODEL_STATE_PATH,
    )


def _ids(value) -> list[str]:
    return sorted(launch_evidence._ids(value))


def _queue_key(campaign_id: str, participant_id: str) -> str:
    digest = hashlib.sha1(f"{campaign_id}|{participant_id}".encode("utf-8")).hexdigest()[:20]
    return f"launchq-{digest}"


def _launch_date_labels(campaign_id: str) -> dict[str, str]:
    """只使用已确认的正式发布日期；绝不拿活动准备窗口猜发布日期。"""
    return CAMPAIGN_LAUNCH_DATES.get(campaign_id) or {
        "en": "the launch window", "de": "unseren Launch-Zeitraum",
        "es": "la ventana de lanzamiento",
    }


def _deterministic_fallback_draft(kol: dict, product: dict, brand: str,
                                  *, launch_dates: dict[str, str] | None = None) -> dict:
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
    launch_dates = launch_dates or {
        "en": "September 15", "de": "15. September", "es": "15 de septiembre",
    }
    if language == "de":
        subject = f"{kol_name}, ein Launch-Sample für dich"[:80]
        body = (
            f"<p>Hey {safe_name},</p><p>dein Gaming-Content passt gut zu unserem kommenden "
            f"Launch. Wir möchten dir gern den {safe_product} zum Testen anbieten.</p>{link_html}"
            f"<p>Wir koordinieren die Berichterstattung rund um {html.escape(launch_dates['de'])}. "
            "Hättest du Interesse, ein Sample unverbindlich auszuprobieren? Wir senden es dir gern zu.</p>"
            f"<p>-- {html.escape(signature)}</p>"
        )
    elif language == "es":
        subject = f"{kol_name}, una muestra para el lanzamiento"[:80]
        body = (
            f"<p>Hey {safe_name},</p><p>tu contenido de gaming encaja bien con nuestro próximo "
            f"lanzamiento. Nos gustaría ofrecerte el {safe_product} para que lo pruebes.</p>{link_html}"
            f"<p>Estamos coordinando publicaciones alrededor de {html.escape(launch_dates['es'])}. "
            "¿Te interesaría probar una muestra sin compromiso? Estaremos encantados de enviártela.</p>"
            f"<p>-- {html.escape(signature)}</p>"
        )
    else:
        subject = f"{kol_name}, a launch sample for you"[:80]
        body = (
            f"<p>Hey {safe_name},</p><p>Your gaming content looks like a good fit for our upcoming "
            f"launch. We would love to offer you the {safe_product} to try.</p>{link_html}"
            f"<p>We are coordinating coverage around {html.escape(launch_dates['en'])}. "
            "Would you be curious to try a sample, "
            "with no strings attached? We would be happy to send one over.</p>"
            f"<p>-- {html.escape(signature)}</p>"
        )
    return {
        "subject": subject, "body": body,
        "highlights": "Activity filters and global duplicate-contact precheck passed.",
        "angle": f"Upcoming {launch_dates['en']} launch sample.",
        "ban_phrase_failed": False, "utm_url": links[0][1],
        "utm_id": utm.kol_utm_id(kol_name), "language": language,
        "deterministic_fallback": True,
    }


def validate_deterministic_launch_draft(draft: dict) -> dict:
    """对固定活动模板做可回放检查；不调用模型。"""
    subject = str(draft.get("subject") or "").strip()
    body = str(draft.get("body") or "").strip()
    plain_body = re.sub(r"<[^>]+>", " ", body)
    errors: list[str] = []
    if not subject or len(subject) > 80:
        errors.append("invalid_subject")
    if len(re.sub(r"\s+", " ", plain_body).strip()) < 80:
        errors.append("body_too_short")
    if re.search(
        r"\[(?:TBD|待填|CARRIER|TRACKING|ETA|ADDRESS|PRICE|QUANTITY|"
        r"CREATOR|KOL|NAME|PRODUCT|LINK|URL|XXX)[^\]]*\]|<%=|\{\{",
        body, re.I,
    ):
        errors.append("unresolved_placeholder")
    if re.search(
        r"\b(?:YM\d{2,4}[A-Z0-9-]*|PK\d{2}[A-Z]?(?:-\d+)?|"
        r"FF\d{2}[A-Z]?(?:-\d+)?|FL-[A-Z0-9-]+|SKU[-_]?[A-Z0-9-]+)\b|"
        r"内部(?:代号|\s*SKU)",
                 subject + "\n" + body, re.I):
        errors.append("internal_sku_leak")
    if re.search(
        r"(?:\$\s*\d|USD\s*\d|\b\d+(?:\.\d+)?\s*%\s*"
        r"(?:discount|off|commission|royalty|promo|coupon))",
        subject + "\n" + body, re.I,
    ):
        errors.append("price_or_commission")
    links = re.findall(r'href="([^"]+)"', body, re.I)
    if not links or any(not link.startswith("https://") for link in links):
        errors.append("invalid_or_missing_link")
    if enrich._check_ban_phrases(body):
        errors.append("fabricated_content_claim")
    return {"passed": not errors, "errors": errors, "link_count": len(links)}


async def preview_zero_model_refill(*, campaign_id: str, buffer_days: int = 2,
                                    profile_refresh_limit: int = 30,
                                    draft_preview_limit: int = 20,
                                    progress_callback=None) -> dict:
    """只读演练活动补池；固定规则、固定模板、零模型、零生产写入。"""
    def report(stage: str, detail: dict | None = None) -> None:
        if progress_callback:
            progress_callback(stage, detail or {})

    report("activity_metrics")
    activity = await launch_evidence.get_activity(campaign_id)
    activity_fields = activity.get("fields") or {}
    metrics = await campaign_metrics(campaign_id, activity=activity)
    base_result = {
        "campaign_id": campaign_id, "read_only": True, "dry_run": True,
        "ai_mode": "zero_model", "model_calls": 0, "writes": 0,
        "drafts_created": 0, "emails_sent": 0, "safe_to_commit": False,
    }
    if (
        ext(activity_fields.get("运行模式")) != "正式运行"
        or ext(activity_fields.get("状态")) != "正式执行中"
    ):
        return {
            **base_result, **metrics, "action": "hold", "held": True,
            "runtime": "campaign_not_formally_active", "business_outcome": "held",
            "reason": "活动不是正式运行/正式执行中，零模型演练保持暂停",
        }
    try:
        window_end = int(activity_fields.get("窗口结束") or 0)
    except (TypeError, ValueError):
        window_end = 0
    if window_end and int(time.time() * 1000) > window_end:
        return {
            **base_result, **metrics, "action": "stop", "stopped": True,
            "runtime": "campaign_window_ended", "business_outcome": "stopped",
            "reason": "活动窗口已结束；dry-run 未修改邮件授权",
        }
    if metrics.get("action") in {"stop", "hold"}:
        action = metrics["action"]
        return {
            **base_result, **metrics,
            ("stopped" if action == "stop" else "held"): True,
            "runtime": f"campaign_metrics_{action}",
            "business_outcome": "stopped" if action == "stop" else "held",
        }
    report("product_quota")
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
    inventory = await _campaign_ready_inventory(campaign_id)
    report("candidate_preview")
    candidate_preview_timed_out = False
    try:
        preview = await asyncio.wait_for(
            launch_candidate_preview.preview_candidates(
                "", campaign_id=campaign_id, object_type="KOL", internal_full=True,
            ),
            timeout=_ZERO_MODEL_CANDIDATE_PREVIEW_TIMEOUT_SECONDS,
        )
    except asyncio.TimeoutError:
        candidate_preview_timed_out = True
        preview = {
            "candidates": [], "profile_refresh_candidate_ids": [],
            "summary": {
                "status": "timeout",
                "timeout_seconds": _ZERO_MODEL_CANDIDATE_PREVIEW_TIMEOUT_SECONDS,
            },
        }
        report("candidate_preview_timeout", {
            "timeout_seconds": _ZERO_MODEL_CANDIDATE_PREVIEW_TIMEOUT_SECONDS,
        })

    refresh_ids = list(dict.fromkeys(
        preview.get("profile_refresh_candidate_ids") or []
    ))[:max(0, min(int(profile_refresh_limit), 100))]
    profile_refresh = {
        "dry_run": True, "classification_mode": "deterministic",
        "model_calls": 0, "writes": 0, "processed": 0,
    }
    report("profile_refresh", {"planned": len(refresh_ids), "processed": 0})
    if refresh_ids:
        profile_refresh = await relabel.run_profile_records(
            refresh_ids, dry_run=True, limit=len(refresh_ids),
            classification_mode="deterministic",
        )

    eligible = [
        candidate for candidate in (preview.get("candidates") or [])
        if candidate.get("decision") == "eligible_new_cold"
        and candidate.get("review_decision") == "通过"
    ]
    report("draft_preview", {"planned": min(len(eligible), draft_preview_limit)})
    draft_previews = []
    for candidate in eligible[:max(0, min(int(draft_preview_limit), 50))]:
        contact_id = str(candidate.get("contact_id") or "").strip()
        if not contact_id:
            continue
        try:
            kol = await feishu.get_record(config.T_KOL, contact_id)
            draft = _deterministic_fallback_draft(
                kol, product, brand,
                launch_dates=_launch_date_labels(campaign_id),
            )
            draft_previews.append({
                "contact_id": contact_id,
                "score": candidate.get("score"),
                "subject": draft["subject"], "body": draft["body"],
                "language": draft.get("language"),
                "validation": validate_deterministic_launch_draft(draft),
            })
        except Exception as exc:
            draft_previews.append({
                "contact_id": contact_id,
                "validation": {"passed": False, "errors": [str(exc)[:160]]},
            })

    remaining = max(0, target_ready - _effective_ready_inventory(inventory))
    discovery = {
        "created": 0, "would_create": 0, "model_calls": 0,
        "shortfall_tasks": 0, "skipped": "inventory_sufficient",
    }
    if remaining:
        report("keyword_preview", {"required_candidates": remaining})
        discovery = await keyword_supply.ensure_campaign_supply(
            campaign_id=campaign_id, activity=activity, product=product,
            required_candidates=remaining,
            approved_candidates=int(metrics.get("approved_new_development_24h") or 0),
            dry_run=True, allow_ai=False, volume_priority=True,
        )

    profile_statuses = profile_refresh.get("by_status") or {}
    profile_errors = sum(
        int(count or 0) for status, count in profile_statuses.items() if status != "ok"
    )
    draft_errors = sum(
        not bool((item.get("validation") or {}).get("passed"))
        for item in draft_previews
    )
    discovery_shortfall = int(discovery.get("shortfall_tasks") or 0)
    degraded_reasons = []
    if candidate_preview_timed_out:
        degraded_reasons.append("candidate_preview_timeout")
    if profile_errors:
        degraded_reasons.append("profile_refresh_error")
    if draft_errors:
        degraded_reasons.append("draft_validation_failed")
    if not discovery.get("ok", True):
        degraded_reasons.append("discovery_preview_failed")
    if discovery_shortfall:
        degraded_reasons.append("fixed_keywords_shortfall")

    report("finalize")
    return {
        **base_result,
        "brand": brand, "metrics": metrics, "quota": quota,
        "target_ready_inventory": target_ready,
        "inventory_before": int(inventory.get("ready") or 0),
        "candidate_preview": {
            "timed_out": candidate_preview_timed_out,
            "timeout_seconds": _ZERO_MODEL_CANDIDATE_PREVIEW_TIMEOUT_SECONDS,
        },
        "preview_summary": preview.get("summary") or {},
        "eligible_template_candidates": len(eligible),
        "profile_refresh": profile_refresh,
        "discovery": discovery,
        "draft_preview_count": len(draft_previews),
        "draft_previews": draft_previews,
        "readiness": {
            "status": "degraded" if degraded_reasons else "ready",
            "reasons": degraded_reasons,
            "profile_error_count": profile_errors,
            "draft_validation_error_count": draft_errors,
            "discovery_shortfall_tasks": discovery_shortfall,
        },
        "business_outcome": (
            "preview_degraded" if degraded_reasons else "preview_ready"
        ),
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
        reason = f"明确承诺 {commitments} 已达到备份线 {hold_at}"
    else:
        action = "expand"
        reply_rate = (replies / sent) if sent else 0
        reason = (
            f"明确承诺 {commitments} 未达到备份线 {hold_at}；"
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


def _pending_review_contact_ids(participants: list[dict], *,
                                preview_refresh_ids: list[str] | None = None,
                                limit: int = 100) -> list[str]:
    """待补资料参与人优先刷新；不把已审通过/排除对象重复送去抓取。"""
    pending = []
    for row in participants:
        fields = row.get("fields") or {}
        if (
            ext(fields.get("参与状态")) not in {"锁定准备中", "已入围"}
            or ext(fields.get("审核结论")) not in {"待审核", "待补资料"}
        ):
            continue
        linked = _ids(fields.get("关联KOL"))
        if len(linked) == 1:
            pending.append(linked[0])
    merged = list(dict.fromkeys(pending + list(preview_refresh_ids or [])))
    return merged[:max(0, min(int(limit), 100))]


def _review_reason_codes(candidate: dict) -> list[str]:
    return list(dict.fromkeys(
        str(code).strip()
        for code in (candidate.get("base_filter_reason_codes") or [])
        if str(code).strip()
    ))


def _is_deterministic_system_pass(candidate: dict) -> bool:
    """只有内部口径一致的严格筛选结果，才允许跳过人工审核。"""
    return bool(
        candidate.get("decision") == "eligible_new_cold"
        and candidate.get("base_filter_passed") is True
        and not _review_reason_codes(candidate)
        and candidate.get("review_route") == "系统建议通过"
        and candidate.get("review_decision") == "通过"
    )


def _pending_review_terminal_route(candidate: dict) -> str:
    """Only eligible new-cold candidates may remain in the operator queue."""
    decision = str(candidate.get("decision") or "").strip()
    if decision in {
        "existing_pipeline_same_thread",
        "hold_active_or_recent",
        "republish_requires_commitment",
    }:
        return "existing_thread"
    if (
        decision and decision != "eligible_new_cold"
    ) or candidate.get("review_route") == "系统排除" or (
        candidate.get("review_decision") == "排除"
    ):
        return "system_excluded"
    return ""


async def reconcile_pending_participant_reviews(*, campaign_id: str,
                                                ranking_version: str,
                                                preview: dict) -> dict:
    """把主表最新画像写回活动待审行；确定项自动通过，边界项保留给运营。"""
    participants = await _participants(campaign_id)
    candidate_by_id = {
        candidate.get("contact_id"): candidate
        for candidate in (
            list(preview.get("profile_refresh_candidates") or [])
            + list(preview.get("candidates") or [])
        )
        if candidate.get("contact_id")
    }
    result = {
        "campaign_id": campaign_id, "checked": 0, "updated": 0,
        "auto_passed": 0, "actionable_pending": 0, "missing_snapshot": 0,
        "existing_thread_routed": 0, "system_excluded": 0,
        "details": [],
    }
    for row in participants:
        fields = row.get("fields") or {}
        if (
            ext(fields.get("参与状态")) not in {"锁定准备中", "已入围"}
            or ext(fields.get("审核结论")) not in {"待审核", "待补资料"}
        ):
            continue
        result["checked"] += 1
        participant_id = row.get("record_id", "")
        linked = _ids(fields.get("关联KOL"))
        if len(linked) != 1 or _ids(fields.get("关联邮件草稿")):
            result["details"].append({
                "participant_id": participant_id,
                "result": "unsafe_participant_shape_skipped",
            })
            continue
        contact_id = linked[0]
        candidate = candidate_by_id.get(contact_id)
        if not candidate:
            result["missing_snapshot"] += 1
            result["details"].append({
                "participant_id": participant_id, "contact_id": contact_id,
                "result": "latest_snapshot_missing",
            })
            continue

        deterministic_pass = _is_deterministic_system_pass(candidate)
        reason_codes = _review_reason_codes(candidate)
        ranking_fields = launch_participation._ranking_fields(
            candidate, ranking_version,
        )
        terminal_route = _pending_review_terminal_route(candidate)
        if terminal_route:
            existing_thread = terminal_route == "existing_thread"
            ranking_fields.update({
                "参与状态": "已取消",
                "审核结论": "排除",
                "审核时间": int(time.time() * 1000),
                "系统审核分流": "自动排除",
                "系统审核说明": (
                    (
                        "该对象已有活动中的原邮件线程；已退出新开发待审，继续走原线程。"
                        if existing_thread else
                        "该对象命中确定性排除规则；已退出运营待审，无需人工复核。"
                    )
                    + (
                        f" 依据：{candidate.get('review_instruction')}"
                        if candidate.get("review_instruction") else ""
                    )
                ),
                "取消原因代码": "不再符合",
                "审核原因代码": reason_codes,
                "排序快照历史": launch_participation._with_snapshot(
                    fields, candidate, ranking_version,
                ),
            })
            await launch_participation._update_and_confirm(
                config.T_LAUNCH_PARTICIPANT, participant_id, ranking_fields,
            )
            result["updated"] += 1
            result["existing_thread_routed" if existing_thread else "system_excluded"] += 1
            result["details"].append({
                "participant_id": participant_id, "contact_id": contact_id,
                "result": (
                    "existing_thread_routed" if existing_thread else "system_excluded"
                ),
                "review_instruction": ext(ranking_fields.get("系统审核说明"))[:240],
            })
            continue
        ranking_fields["审核结论"] = "通过" if deterministic_pass else (
            "待补资料"
            if candidate.get("review_decision") == "待补资料"
            else "待审核"
        )
        if deterministic_pass:
            # 自动转通过也要记录本次决定时间，供草稿在途TTL从此刻开始计算。
            ranking_fields["审核时间"] = int(time.time() * 1000)
        elif ranking_fields.get("系统审核分流") == "系统建议通过":
            # 旧数据可能同时写着“建议通过”和基础筛选失败原因。此类矛盾项必须
            # 回到运营边界，不能自动发信，也不能继续伪装成系统已判定通过。
            issue = "、".join(reason_codes) or "基础筛选未明确通过"
            ranking_fields["系统审核分流"] = "KOL运营审核"
            ranking_fields["系统审核说明"] = (
                f"系统发现基础筛选结果与建议通过结论不一致；请只核实：{issue}。"
                "未确认前不进入发送。"
            )
        ranking_fields["审核原因代码"] = reason_codes
        ranking_fields["排序快照历史"] = launch_participation._with_snapshot(
            fields, candidate, ranking_version,
        )
        await launch_participation._update_and_confirm(
            config.T_LAUNCH_PARTICIPANT, participant_id, ranking_fields,
        )
        result["updated"] += 1
        key = "auto_passed" if deterministic_pass else "actionable_pending"
        result[key] += 1
        result["details"].append({
            "participant_id": participant_id, "contact_id": contact_id,
            "result": "auto_passed" if deterministic_pass else "operator_actionable",
            "review_instruction": ext(ranking_fields.get("系统审核说明"))[:240],
        })
    return result


async def append_auto_approved(*, campaign_id: str, pool_target: int,
                               preview: dict | None = None) -> dict:
    """只追加系统明确通过的新开发对象；待审对象始终保留在旁路。"""
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
        ext((row.get("fields") or {}).get("参与状态")) in ACTIVE_PARTICIPANT_STATES
        for row in existing
    )
    outstanding_review = sum(
        ext((row.get("fields") or {}).get("参与状态")) in ACTIVE_PARTICIPANT_STATES
        and ext((row.get("fields") or {}).get("审核结论")) in PENDING_REVIEW_DECISIONS
        for row in existing
    )
    # 待审记录只进入运营旁路，不占用“明确可开发对象”的目标名额。
    # 历史记录可能没有审核结论，因此只剔除明确待审/待补资料的记录。
    non_review_active_count = _non_review_active_count(
        active_count, outstanding_review,
    )
    room = max(0, int(pool_target) - non_review_active_count)
    batch_room = min(room, 120)
    candidates = [
        c for c in (preview.get("candidates") or [])
        if c.get("contact_id") not in existing_contacts
        and _is_deterministic_system_pass(c)
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
        "non_review_pool_before": non_review_active_count,
        "eligible_auto_approved": len(candidates), "created": len(created),
        "pool_after": active_count + len(created), "participant_ids": created,
        "pending_review_kept_parallel": outstanding_review,
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
        and not _is_deterministic_system_pass(candidate)
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
            reason_codes = _review_reason_codes(candidate)
            if reason_codes:
                ranking_fields["审核原因代码"] = reason_codes
            if ranking_fields.get("系统审核分流") == "系统建议通过":
                issue = "、".join(reason_codes) or "系统通过结论仍有待确认项"
                ranking_fields["系统审核分流"] = "KOL运营审核"
                ranking_fields["系统审核说明"] = (
                    f"系统结论尚不足以自动通过；请只核实：{issue}。"
                    "未确认前不进入发送。"
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


async def _find_queue_draft(queue_key: str) -> dict | None:
    rows = await feishu.search_records(config.T_DRAFT, [{
        "field_name": "邮件草稿ID", "operator": "is", "value": [queue_key],
    }])
    exact = [r for r in rows if ext((r.get("fields") or {}).get("邮件草稿ID")) == queue_key]
    if len(exact) > 1:
        raise LaunchRuntimeError(f"活动队列草稿ID重复: {queue_key}")
    return exact[0] if exact else None


async def reconcile_approved_controlled_import_routes(
    *, campaign_id: str, product: dict, product_id: str, brand: str,
    participants: list[dict],
) -> dict:
    """提交活动人工审核结论，但只解除受控导入记录的单一“待核对”闸。

    同产品历史、已有线程、近期同品牌触达、重复身份和无效邮箱仍由
    ``_fast_precheck`` 优先裁决；任何这些证据存在时都不改主表路由。
    """
    details: list[dict] = []
    updated = 0
    kept_blocked = 0
    route_only_reason = "触达路由状态=待核对，禁止直接进入新开发池"

    for participant in participants:
        pf = participant.get("fields") or {}
        participant_id = participant.get("record_id", "")
        base = {"participant_id": participant_id}
        if (
            ext(pf.get("活动ID")) != campaign_id
            or ext(pf.get("参与状态")) != "已入围"
            or ext(pf.get("审核结论")) != "通过"
            or ext(pf.get("进入方式")) != "新开发"
            or ext(pf.get("活动分池")) != "新开发池"
            or _ids(pf.get("关联邮件草稿"))
        ):
            details.append({**base, "result": "participant_gate_not_eligible"})
            continue

        contact_ids = _ids(pf.get("关联KOL"))
        if len(contact_ids) != 1:
            details.append({**base, "result": "participant_contact_not_unique"})
            continue
        contact_id = contact_ids[0]
        kol = await feishu.get_record(config.T_KOL, contact_id)
        kf = kol.get("fields") or {}
        marker = ext(kf.get("迁移备注"))
        if (
            ext(kf.get("合作状态")) != "未建联"
            or ext(kf.get("触达路由状态")) != "待核对"
            or ext(kf.get("资料可用状态")) not in {"有效", "人工核实有效"}
            or "[CONTROLLED_IMPORT]" not in marker
            or f"campaign={campaign_id}" not in marker
            or "no_auto_email=true" not in marker
        ):
            details.append({**base, "contact_id": contact_id,
                            "result": "controlled_import_gate_not_eligible"})
            continue

        precheck = await launch_outreach._fast_precheck(
            kol=kol, product=product, product_id=product_id,
            contact_id=contact_id, brand=brand,
        )
        if not (
            precheck.get("decision") == "hold_active_or_recent"
            and list(precheck.get("reasons") or []) == [route_only_reason]
            and not list(precheck.get("evidence_draft_ids") or [])
        ):
            kept_blocked += 1
            details.append({
                **base, "contact_id": contact_id,
                "result": "kept_global_precheck_block",
                "decision": precheck.get("decision") or "unknown",
            })
            continue

        await feishu.update_record(
            config.T_KOL, contact_id, {"触达路由状态": "可新开发"},
        )
        readback = await feishu.get_record(config.T_KOL, contact_id)
        if ext((readback.get("fields") or {}).get("触达路由状态")) != "可新开发":
            raise LaunchRuntimeError(
                f"人工审核路由提交后回读不一致: participant={participant_id}"
            )
        updated += 1
        details.append({
            **base, "contact_id": contact_id,
            "result": "route_only_manual_hold_committed",
        })

    return {
        "campaign_id": campaign_id, "checked": len(participants),
        "updated": updated, "kept_blocked": kept_blocked, "details": details,
    }


async def _queue_one(*, activity: dict, participant: dict, product: dict,
                     brand: str, model_budget, generation_lock: asyncio.Lock) -> dict:
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
    email, reason = feishu.clean_email(ext((kol.get("fields") or {}).get("邮箱")))
    if not email:
        return {
            "participant_id": participant_id,
            "skipped": f"bad_email:{reason}",
            "terminal_failure": True,
        }
    precheck = await launch_outreach._fast_precheck(
        kol=kol, product=product, product_id=product_id,
        contact_id=contact_id, brand=brand,
    )
    if precheck.get("decision") != "eligible_new_cold":
        return {
            "participant_id": participant_id,
            "skipped": precheck.get("decision") or "precheck_failed",
            "terminal_failure": True,
        }

    score = float(pf.get("基础评分快照") or 0)
    signature = "Tom from FUNLAB Team" if brand == "FUNLAB" else "Lisa @ POWKONG Team"
    breakdown = {
        "活动名单": {"score": score, "reason": "活动名单和全局重复触达预检已通过"},
    }
    async with generation_lock:
        generated = await enrich.generate_controlled_draft(
            kol, product, brand, signature, breakdown, score,
            model_budget=model_budget, task_id=campaign_id,
            template_factory=lambda: _deterministic_fallback_draft(
                kol, product, brand, launch_dates=_launch_date_labels(campaign_id),
            ),
        )
    if generated.get("error") or generated.get("skip") or generated.get("model_skip_reason"):
        return {
            "participant_id": participant_id,
            "skipped": str(
                generated.get("error") or generated.get("skip")
                or generated.get("model_skip_reason")
            ),
        }
    if generated.get("generation_mode") == "ai" and not (
        generated.get("output_validation") or {}
    ).get("passed", False):
        return {"participant_id": participant_id, "skipped": "model_output_validation_failed"}
    if generated.get("deterministic_fallback"):
        validation = validate_deterministic_launch_draft(generated)
        if not validation["passed"]:
            return {
                "participant_id": participant_id,
                "skipped": "launch_template_validation_failed:" + ",".join(validation["errors"]),
            }
    subject = str(generated.get("subject") or "").strip()
    body = str(generated.get("body") or "").strip()
    if not subject or len(re.sub(r"<[^>]+>", "", body)) < 50:
        return {"participant_id": participant_id, "skipped": "generated_body_invalid"}
    generation_marker = (
        f"template:{LAUNCH_QUEUE_TEMPLATE_VERSION}"
        if generated.get("deterministic_fallback")
        else "generation:ai-exception"
    )
    body += (
        '<span style="display:none;font-size:0;color:transparent">'
        f"launch-queue:{queue_key};{generation_marker}</span>"
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
            "AI评分理由": (
                f"[{LAUNCH_QUEUE_TEMPLATE_VERSION}] 受控活动模板；"
                f"model_calls={generated.get('model_calls', 0)}；"
                f"fallback={generated.get('model_fallback_reason') or 'routine'}"
            )[:500],
        })
        return {"participant_id": participant_id, "draft_id": draft_id,
                "reused": False, "path": "自动通过-受控备用模板"}
    await feishu.update_record(config.T_DRAFT, draft_id, {
        "邮件草稿状态": "待审", "审核路径": "待人审",
        "AI评分理由": (
            "[hybrid-ai-exception] 模型例外草稿已通过确定性安全检查；"
            "禁止二次 AI reviewer，固定进入人工审核"
        )[:500],
    })
    return {"participant_id": participant_id, "draft_id": draft_id,
            "reused": False, "path": "待人审", "generation_mode": "ai"}


async def queue_approved(*, campaign_id: str, limit: int = 120,
                         model_budget: EnrichModelBudget | None = None) -> dict:
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
    route_reconcile = await reconcile_approved_controlled_import_routes(
        campaign_id=campaign_id, product=product, product_id=product_id,
        brand=brand, participants=rows,
    )
    eligible = sorted([
        row for row in rows
        if ext((row.get("fields") or {}).get("参与状态")) == "已入围"
        and ext((row.get("fields") or {}).get("审核结论")) == "通过"
        and ext((row.get("fields") or {}).get("进入方式")) == "新开发"
        and ext((row.get("fields") or {}).get("活动分池")) == "新开发池"
        and not _ids((row.get("fields") or {}).get("关联邮件草稿"))
    ], key=_participant_activity_ts, reverse=True)[:max(1, min(int(limit), 500))]
    semaphore = asyncio.Semaphore(4)
    generation_lock = asyncio.Lock()
    model_budget = model_budget or new_launch_model_budget()

    async def one(row):
        async with semaphore:
            try:
                return await _queue_one(
                    activity=activity, participant=row, product=product, brand=brand,
                    model_budget=model_budget, generation_lock=generation_lock,
                )
            except Exception as exc:
                return {"participant_id": row.get("record_id"), "error": str(exc)[:300]}

    details = await asyncio.gather(*(one(row) for row in eligible))
    terminal_cancelled = 0
    terminal_cancel_errors = []
    for item in details:
        if not item.get("terminal_failure"):
            continue
        participant_id = str(item.get("participant_id") or "").strip()
        if not participant_id:
            continue
        try:
            await launch_participation._update_and_confirm(
                config.T_LAUNCH_PARTICIPANT, participant_id,
                {"参与状态": "已取消", "取消原因代码": "不再符合"},
            )
            item["terminal_cancelled"] = True
            terminal_cancelled += 1
        except Exception as exc:
            item["terminal_cancel_error"] = str(exc)[:200]
            terminal_cancel_errors.append(participant_id)
    return {
        "campaign_id": campaign_id, "brand": brand, "eligible": len(eligible),
        "route_reconcile": route_reconcile,
        "queued": sum(bool(x.get("draft_id")) and not x.get("reused") for x in details),
        "reused": sum(bool(x.get("reused")) for x in details),
        "skipped_or_failed": sum(not x.get("draft_id") for x in details),
        "terminal_cancelled": terminal_cancelled,
        "terminal_cancel_errors": terminal_cancel_errors,
        "model_budget": model_budget.snapshot(),
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


async def campaign_metrics(
    campaign_id: str, *, activity: dict | None = None,
    participants: list[dict] | None = None, drafts: list[dict] | None = None,
) -> dict:
    """只读计算活动事实指标；本函数不写生产表。"""
    activity = activity or await launch_evidence.get_activity(campaign_id)
    af = activity.get("fields") or {}
    participants = participants if participants is not None else await _participants(campaign_id)
    draft_ids = {
        did for row in participants for did in _ids((row.get("fields") or {}).get("关联邮件草稿"))
    }
    drafts = drafts if drafts is not None else await launch_outcomes.draft_snapshot()
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
    actual_times = []
    for row in participants:
        try:
            value = int((row.get("fields") or {}).get("实际上稿时间") or 0)
        except (TypeError, ValueError):
            value = 0
        if value:
            actual_times.append(value)
    actual_posts = len(actual_times)
    ontime_posts = sum(not window_end or value <= window_end for value in actual_times)
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
    def is_approved_new_development(row: dict) -> bool:
        fields = row.get("fields") or {}
        return bool(
            ext(fields.get("参与状态")) in {"锁定准备中", "已入围"}
            and ext(fields.get("审核结论")) == "通过"
            and ext(fields.get("进入方式")) == "新开发"
            and ext(fields.get("活动分池")) == "新开发池"
        )

    approved_new_development = sum(map(is_approved_new_development, participants))
    approved_cutoff_ms = int(time.time() * 1000) - 24 * 60 * 60 * 1000

    def record_created_ms(row: dict) -> int:
        try:
            value = int(float(row.get("created_time") or row.get("createdTime") or 0))
        except (TypeError, ValueError):
            return 0
        return value * 1000 if 0 < value < 100_000_000_000 else value

    approved_new_development_24h = sum(
        is_approved_new_development(row)
        and record_created_ms(row) >= approved_cutoff_ms
        for row in participants
    )
    return {
        "campaign_id": campaign_id, "participants": len(participants),
        "approved_new_development": approved_new_development,
        "approved_new_development_24h": approved_new_development_24h,
        "sent": sent, "replies": replies, "commitments": commitments,
        "actual_posts": actual_posts, "ontime_posts": ontime_posts,
        **control,
    }


async def sync_campaign_outcomes_and_metrics(campaign_id: str) -> dict:
    """复用同一批草稿快照完成事实回填和指标计算，并保留写入回读结果。"""
    activity = await launch_evidence.get_activity(campaign_id)
    participants = await _participants(campaign_id)
    drafts = await launch_outcomes.draft_snapshot()
    outcome_reconcile = await launch_outcomes.reconcile_campaign(
        campaign_id, dry_run=False, activity=activity,
        participants=participants, drafts=drafts,
    )
    if outcome_reconcile.get("updates_written"):
        participants = await _participants(campaign_id)
    metrics = await campaign_metrics(
        campaign_id, activity=activity, participants=participants, drafts=drafts,
    )
    if outcome_reconcile.get("errors"):
        metrics.update({
            "action": "hold",
            "reason": (
                f"活动事实回填失败{len(outcome_reconcile['errors'])}条；"
                "已暂停扩池，等待回填恢复后再按最新数据控制"
            ),
        })
    return {**metrics, "outcome_reconcile": outcome_reconcile}


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
        if ext((row.get("fields") or {}).get("参与状态")) in ACTIVE_PARTICIPANT_STATES
    ]
    inventory_now_ts = int(time.time())
    queueable_approved = sum(
        ext((row.get("fields") or {}).get("参与状态")) == "已入围"
        and ext((row.get("fields") or {}).get("审核结论")) == "通过"
        and ext((row.get("fields") or {}).get("进入方式")) == "新开发"
        and ext((row.get("fields") or {}).get("活动分池")) == "新开发池"
        and not _ids((row.get("fields") or {}).get("关联邮件草稿"))
        and _is_recent_queueable(row, now_ts=inventory_now_ts)
        for row in active
    )
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
        "queueable_approved": queueable_approved,
        "pending_review": sum(
            ext((row.get("fields") or {}).get("审核结论")) in PENDING_REVIEW_DECISIONS
            for row in active
        ),
        "pending_contact_ids": _pending_review_contact_ids(
            active, preview_refresh_ids=[], limit=100,
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
                            profile_refresh_limit: int = 30,
                            runtime_job_id: str = "") -> dict:
    """按活动进度与邮箱余量自治补池；不直接发送邮件，也不降低筛选标准。"""
    lock = _LOCKS.setdefault(campaign_id, asyncio.Lock())
    model_budget = new_launch_model_budget()
    if lock.locked():
        return _with_business_outcome({
            "campaign_id": campaign_id, "already_running": True,
            "action": "hold", "held": True, "runtime": "already_running",
            "model_budget": model_budget.snapshot(),
        })
    async with lock:
        activity = await launch_evidence.get_activity(campaign_id)
        activity_fields = activity.get("fields") or {}
        if (
            ext(activity_fields.get("运行模式")) != "正式运行"
            or ext(activity_fields.get("状态")) != "正式执行中"
        ):
            return _with_business_outcome({
                "campaign_id": campaign_id, "already_running": False,
                "action": "hold", "held": True,
                "runtime": "campaign_not_formally_active",
                "reason": "活动不是正式运行/正式执行中，自治补池保持暂停",
                "model_budget": model_budget.snapshot(),
                "quality_filters_lowered": False,
            })
        try:
            window_end = int(activity_fields.get("窗口结束") or 0)
        except (TypeError, ValueError):
            window_end = 0
        if window_end and int(time.time() * 1000) > window_end:
            await feishu.update_record(
                config.T_LAUNCH_CAMPAIGN, activity["record_id"],
                {"发送邮件授权": False},
            )
            return _with_business_outcome({
                "campaign_id": campaign_id, "already_running": False,
                "action": "stop", "stopped": True,
                "runtime": "campaign_window_ended",
                "reason": "活动窗口已结束，已关闭邮件授权并停止补池",
                "model_budget": model_budget.snapshot(),
                "quality_filters_lowered": False,
            })
        metrics = await sync_campaign_outcomes_and_metrics(campaign_id)
        if metrics["action"] == "stop":
            await feishu.update_record(
                config.T_LAUNCH_CAMPAIGN, activity["record_id"],
                {"发送邮件授权": False},
            )
            return _with_business_outcome({
                **metrics, "already_running": False, "stopped": True,
                "model_budget": model_budget.snapshot(),
            })
        if metrics["action"] == "hold":
            return _with_business_outcome({
                **metrics, "already_running": False, "held": True,
                "model_budget": model_budget.snapshot(),
            })

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
            return _with_business_outcome({
                **metrics, "already_running": False, "brand": brand,
                "quota": quota, "target_ready_inventory": target_ready,
                "inventory_before": inventory_before["ready"],
                "inventory_after": inventory_before["ready"],
                "effective_inventory_before": _effective_ready_inventory(
                    inventory_before,
                ),
                "effective_inventory_after": _effective_ready_inventory(
                    inventory_before,
                ),
                "runtime": "inventory_sufficient",
                "model_budget": model_budget.snapshot(),
            })

        preview = await launch_candidate_preview.preview_candidates(
            "", campaign_id=campaign_id, object_type="KOL", internal_full=True,
        )
        deficit = max(
            0, target_ready - _effective_ready_inventory(inventory_before),
        )
        active_before = int(
            inventory_before.get("active_participants", metrics["participants"])
        )
        non_review_before = _non_review_active_count(
            active_before, int(inventory_before.get("pending_review") or 0),
        )
        first_append = await append_auto_approved(
            campaign_id=campaign_id,
            pool_target=non_review_before + min(deficit, 120),
            preview=preview,
        )
        first_queue = await queue_approved(
            campaign_id=campaign_id, limit=queue_limit, model_budget=model_budget,
        )
        inventory_after_master = await _campaign_ready_inventory(campaign_id)

        refresh_result = {"processed": 0, "writes": 0}
        pending_review_reconcile = {
            "checked": 0, "updated": 0, "auto_passed": 0,
            "actionable_pending": 0, "missing_snapshot": 0,
        }
        second_append = {"created": 0}
        second_queue = {"queued": 0}
        latest_preview = preview
        if _effective_ready_inventory(inventory_after_master) < target_ready:
            refresh_ids = _pending_review_contact_ids(
                [], preview_refresh_ids=(
                    list(inventory_before.get("pending_contact_ids") or [])
                    + list(preview.get("profile_refresh_candidate_ids") or [])
                ), limit=profile_refresh_limit,
            )
            if refresh_ids:
                refresh_result = await relabel.run_profile_records(
                    refresh_ids, dry_run=False, limit=len(refresh_ids),
                    model_budget=model_budget,
                )
                latest_preview = await launch_candidate_preview.preview_candidates(
                    "", campaign_id=campaign_id, object_type="KOL", internal_full=True,
                )
            if inventory_before.get("pending_contact_ids"):
                pending_review_reconcile = await reconcile_pending_participant_reviews(
                    campaign_id=campaign_id,
                    ranking_version=ext(activity_fields.get("证据排序版本")),
                    preview=latest_preview,
                )
            auto_passed = int(pending_review_reconcile.get("auto_passed") or 0)
            effective_after_reconcile = (
                _effective_ready_inventory(inventory_after_master) + auto_passed
            )
            remaining_after_reconcile = max(
                0, target_ready - effective_after_reconcile,
            )
            if refresh_ids and remaining_after_reconcile:
                active_after_master = int(
                    inventory_after_master.get(
                        "active_participants", metrics["participants"],
                    )
                )
                non_review_after_reconcile = _non_review_active_count(
                    active_after_master,
                    int(inventory_after_master.get("pending_review") or 0),
                ) + auto_passed
                second_append = await append_auto_approved(
                    campaign_id=campaign_id,
                    pool_target=(
                        non_review_after_reconcile
                        + min(remaining_after_reconcile, 120)
                    ),
                    preview=latest_preview,
                )
            if refresh_ids or pending_review_reconcile.get("auto_passed"):
                second_queue = await queue_approved(
                    campaign_id=campaign_id, limit=queue_limit,
                    model_budget=model_budget,
                )
        inventory_after = await _campaign_ready_inventory(campaign_id)
        effective_inventory_after = _effective_ready_inventory(inventory_after)
        remaining = max(0, target_ready - effective_inventory_after)

        evidence_continuation = {"planned": 0, "participation_writes": 0}
        if (
            remaining
            and campaign_id == launch_evidence_author_import.DAVE_CAMPAIGN_ID
        ):
            continuation_job_id = runtime_job_id or (
                "launchruntime-autonomous-" + hashlib.sha1(
                    f"{campaign_id}|{int(time.time())}".encode("utf-8")
                ).hexdigest()[:12]
            )
            continuation_offset = _dave_evidence_continuation_offset(
                activity_fields, current_job_id=continuation_job_id,
            )
            try:
                evidence_continuation = await (
                    launch_evidence_author_import.run_continuation_import(
                        campaign_id=campaign_id,
                        source_job_id=continuation_job_id,
                        offset=continuation_offset,
                        sample_limit=20,
                        import_limit=3,
                        commit=True,
                    )
                )
            except Exception as exc:
                evidence_continuation = {
                    "offset": continuation_offset,
                    "planned": 0,
                    "participation_writes": 0,
                    "error": str(exc)[:240],
                }

        discovery = {"ok": True, "created": 0, "skipped": "inventory_sufficient"}
        review_pool = {"created": 0}
        review_notification = {"sent": 0}
        if remaining:
            discovery = await keyword_supply.ensure_campaign_supply(
                campaign_id=campaign_id, activity=activity, product=product,
                required_candidates=remaining,
                approved_candidates=int(
                    metrics.get("approved_new_development_24h") or 0
                ),
                dry_run=False, volume_priority=True,
                model_budget=model_budget,
                source_outcomes=latest_preview.get("summary") or {},
            )
            review_preview = dict(latest_preview)
            review_preview["candidates"] = list(latest_preview.get("candidates") or []) + list(
                latest_preview.get("profile_refresh_candidates") or []
            )
            review_pool = await append_review_candidates(
                campaign_id=campaign_id, review_target=review_target,
                preview=review_preview, operator_only=True,
            )
            new_review_count = (
                int(review_pool.get("created") or 0)
                + int(evidence_continuation.get("participation_writes") or 0)
            )
            review_notification = await _notify_operator_review(
                campaign_id=campaign_id, activity=activity,
                created=new_review_count,
            )

        result = {
            **metrics, "already_running": False, "brand": brand,
            "quota": quota, "target_ready_inventory": target_ready,
            "inventory_before": inventory_before["ready"],
            "inventory_after_master": inventory_after_master["ready"],
            "inventory_after": inventory_after["ready"],
            "effective_inventory_before": _effective_ready_inventory(inventory_before),
            "effective_inventory_after_master": _effective_ready_inventory(
                inventory_after_master,
            ),
            "effective_inventory_after": effective_inventory_after,
            "append": first_append, "queue": first_queue,
            "profile_refresh": refresh_result,
            "pending_review_reconcile": pending_review_reconcile,
            "append_after_refresh": second_append,
            "queue_after_refresh": second_queue,
            "evidence_continuation": evidence_continuation,
            "discovery": discovery, "review_pool": review_pool,
            "review_notification": review_notification,
            "model_budget": model_budget.snapshot(),
            "quality_filters_lowered": False,
        }
        preview_candidates = list(latest_preview.get("candidates") or [])
        preview_summary = latest_preview.get("summary") or {}
        by_source = preview_summary.get("by_source") or {}
        known_external_sources = {
            source: values for source, values in by_source.items()
            if source and source != "unknown"
        }
        result["internal_pool"] = {
            "scanned": int(preview_summary.get("pool_records") or 0),
            "evaluated": int(preview_summary.get("evaluated") or len(preview_candidates)),
            "auto_approved": int(preview_summary.get("system_approved") or 0),
            "participants_created": (
                int(first_append.get("created") or 0)
                + int(second_append.get("created") or 0)
            ),
            "operator_review": sum(
                candidate.get("review_route") == "KOL运营审核"
                for candidate in preview_candidates
            ) if "operator_review" not in preview_summary else int(
                preview_summary.get("operator_review") or 0
            ),
            "hard_filtered": int(preview_summary.get("base_filter_excluded") or 0),
            "base_filter_failed": int(preview_summary.get("base_filter_excluded") or 0),
            "drafts_queued": (
                int(first_queue.get("queued") or 0)
                + int(second_queue.get("queued") or 0)
            ),
            "pending_review": int(inventory_after.get("pending_review") or 0),
        }
        result["external_youtube_discovery"] = dict(
            discovery.get("external_youtube_discovery") or {
                "created": int(discovery.get("created") or 0),
                "active_pending": int(discovery.get("active_pending_before") or 0),
                "shortfall_tasks": int(discovery.get("shortfall_tasks") or 0),
            }
        )
        external_outcome = dict(discovery.get("external_youtube_outcome") or {})
        external_outcome.update({
            "source_breakdown": known_external_sources,
            "by_task": preview_summary.get("by_task") or {},
            "auto_approved": sum(
                int(values.get("auto_approved") or 0)
                for values in known_external_sources.values()
            ),
            "operator_review": sum(
                int(values.get("operator_review") or 0)
                for values in known_external_sources.values()
            ),
        })
        result["external_youtube_outcome"] = external_outcome
        return _with_business_outcome(result)


def _section_count(result: dict, section: str, key: str) -> int:
    try:
        return max(0, int((result.get(section) or {}).get(key) or 0))
    except (TypeError, ValueError):
        return 0


def _with_business_outcome(result: dict) -> dict:
    """给后台任务补业务结果；HTTP没报错不再等同于补池成功。"""
    progress_breakdown = {
        "auto_approved_created": _section_count(result, "append", "created"),
        "drafts_queued": _section_count(result, "queue", "queued"),
        "profile_refresh_writes": _section_count(result, "profile_refresh", "writes"),
        "auto_approved_after_refresh": _section_count(
            result, "append_after_refresh", "created",
        ),
        "drafts_queued_after_refresh": _section_count(
            result, "queue_after_refresh", "queued",
        ),
        "discovery_tasks_created": _section_count(result, "discovery", "created"),
        "active_discovery_tasks": _section_count(
            result, "discovery", "active_pending_before",
        ),
        "review_candidates_created": _section_count(result, "review_pool", "created"),
        "evidence_candidates_imported": _section_count(
            result, "evidence_continuation", "participation_writes",
        ),
    }
    # 刷新旧资料只是补全信息，不会增加可发送名单、候选任务或待审对象。
    # 单独出现刷新写入时，仍应暴露为 supply_blocked，避免“池仍为 0”却报成功。
    supply_progress_keys = (
        "auto_approved_created",
        "drafts_queued",
        "auto_approved_after_refresh",
        "drafts_queued_after_refresh",
        "discovery_tasks_created",
        "active_discovery_tasks",
        "review_candidates_created",
        "evidence_candidates_imported",
    )
    made_supply_progress = any(
        progress_breakdown[key] > 0 for key in supply_progress_keys
    )
    raw_quota = result.get("quota") if isinstance(result.get("quota"), dict) else {}
    try:
        remaining = max(0, int(raw_quota.get("remaining") or 0))
    except (TypeError, ValueError):
        remaining = 0
    try:
        inventory_after = max(0, int(result.get("inventory_after") or 0))
    except (TypeError, ValueError):
        inventory_after = 0

    outcome_errors = len((result.get("outcome_reconcile") or {}).get("errors") or [])
    evidence_continuation_failed = bool(
        (result.get("evidence_continuation") or {}).get("partial_failure")
        or (result.get("evidence_continuation") or {}).get("error")
    )
    if outcome_errors:
        outcome = "outcome_reconcile_failed"
    elif evidence_continuation_failed:
        outcome = "evidence_continuation_failed"
    elif result.get("stopped") or result.get("action") == "stop":
        outcome = "stopped"
    elif result.get("held") or result.get("action") == "hold":
        outcome = "held"
    elif result.get("runtime") == "inventory_sufficient":
        outcome = "inventory_sufficient"
    elif remaining <= 0:
        outcome = "quota_exhausted"
    elif inventory_after > 0:
        outcome = "ready_inventory_created"
    elif made_supply_progress:
        outcome = "supply_in_progress"
    elif (result.get("discovery") or {}).get("skipped") == "quality_cooldown":
        outcome = "supply_cooling_down"
    elif result.get("action") == "expand":
        outcome = "supply_blocked"
    else:
        outcome = "no_action_needed"
    return {
        **result,
        "quota": {**raw_quota, "remaining": remaining},
        "inventory_after": inventory_after,
        "business_outcome": outcome,
        "outcome_reconcile_error_count": outcome_errors,
        "made_supply_progress": made_supply_progress,
        "supply_progress_breakdown": progress_breakdown,
    }


def runtime_job_status(result: dict | None) -> str:
    result = result or {}
    if result.get("business_outcome") in {
        "supply_blocked", "outcome_reconcile_failed", "evidence_continuation_failed",
        "preview_degraded",
    }:
        return "degraded"
    if (result.get("outcome_reconcile") or {}).get("errors"):
        return "degraded"
    return "success"


def _runtime_result_summary(result: dict | None) -> dict:
    result = result or {}
    quota = result.get("quota") or {}
    summary = {
        key: result.get(key) for key in (
            "action", "brand", "participants", "sent", "replies", "commitments",
            "ontime_posts", "target_ready_inventory", "inventory_before", "inventory_after",
            "inventory_after_master", "effective_inventory_before",
            "effective_inventory_after_master", "effective_inventory_after",
            "stopped", "held", "quality_filters_lowered",
            "runtime", "business_outcome", "made_supply_progress",
            "supply_progress_breakdown", "outcome_reconcile_error_count",
        ) if key in result
    } | ({"quota": quota} if quota else {})
    if result.get("model_budget"):
        summary["model_budget"] = result["model_budget"]
    for key in (
        "internal_pool", "external_youtube_discovery", "external_youtube_outcome",
    ):
        if isinstance(result.get(key), dict):
            summary[key] = result[key]
    reconcile = result.get("outcome_reconcile") or {}
    discovery = result.get("discovery") or {}
    if discovery:
        summary["discovery"] = {
            key: discovery.get(key) for key in (
                "created", "skipped", "keyword_source", "active_pending_before",
                "stale_pending_before", "target_tasks", "quality_gate",
                "quality_filters_lowered", "volume_priority",
                "quality_cooldown_overridden",
            ) if key in discovery
        }
    if reconcile:
        errors = reconcile.get("errors") or []
        summary["outcome_reconcile"] = {
            key: reconcile.get(key) for key in (
                "ok", "participants_scanned", "updates_planned", "updates_written",
                "commitments_written", "actuals_written", "links_written",
            ) if key in reconcile
        } | {
            "error_count": len(errors),
            "failed_participant_ids": [
                item.get("participant_id") for item in errors if item.get("participant_id")
            ],
        }
    for key in (
        "append", "queue", "profile_refresh", "pending_review_reconcile",
        "append_after_refresh", "queue_after_refresh", "review_pool",
        "evidence_continuation",
    ):
        section = result.get(key)
        if isinstance(section, dict):
            summary[key] = {
                name: section.get(name) for name in (
                    "processed", "writes", "updated", "auto_passed",
                    "actionable_pending", "missing_snapshot", "created", "queued",
                    "skipped", "errors", "offset", "next_offset", "sample_size",
                    "eligible", "planned", "participation_writes",
                    "partial_failure", "incomplete_controlled_imports",
                    "skipped_or_failed", "terminal_cancelled",
                    "terminal_cancel_errors",
                ) if name in section
            }
    for key in (
        "read_only", "planned", "imported", "writes", "master_writes",
        "participation_writes", "participation_records", "drafts_created",
        "draft_count", "emails_sent", "guard", "durable_audit",
    ):
        if key in result:
            summary[key] = result.get(key)
    if result.get("blocked"):
        summary["blocked"] = [
            {"handle": row.get("handle"), "reasons": row.get("reasons") or []}
            for row in result.get("blocked") or []
        ]
    if result.get("results"):
        summary["results"] = [
            {key: row.get(key) for key in (
                "handle", "kol_id", "participant_id", "participant_ids",
                "participant_count", "review_status", "review_statuses",
                "master_action", "participant_action", "draft_count",
            ) if key in row}
            for row in result.get("results") or []
        ]
    return summary


def _runtime_persisted_result_summary(result: dict | None) -> dict:
    """生成可持久化的小摘要，保住审计字段，不把大诊断数组写进活动备注。"""
    summary = _runtime_result_summary(result)

    def scalars(section):
        if not isinstance(section, dict):
            return {}
        return {
            key: value for key, value in section.items()
            if value is None or isinstance(value, (bool, int, float, str))
        }

    for key in (
        "quota", "model_budget", "supply_progress_breakdown",
        "internal_pool", "external_youtube_discovery", "external_youtube_outcome",
    ):
        if key in summary:
            summary[key] = scalars(summary[key])
    discovery = summary.get("discovery")
    if isinstance(discovery, dict) and isinstance(discovery.get("quality_gate"), dict):
        summary["discovery"] = {
            **discovery, "quality_gate": scalars(discovery["quality_gate"]),
        }
    # 单条对象明细留在后台 job 内存/日志里；活动备注只承担任务恢复和业务审计。
    summary.pop("blocked", None)
    summary.pop("results", None)
    return summary


def _runtime_minimum_result_summary(result: dict | None) -> dict:
    """极端情况下仍保留审计判定所需的最小业务字段。"""
    result = result or {}
    keys = (
        "action", "brand", "business_outcome", "made_supply_progress",
        "quality_filters_lowered", "inventory_before", "inventory_after",
        "effective_inventory_before", "effective_inventory_after",
    )
    compact = {key: result.get(key) for key in keys if key in result}
    for key in (
        "quota", "model_budget", "internal_pool",
        "external_youtube_discovery", "external_youtube_outcome",
    ):
        section = result.get(key)
        if isinstance(section, dict):
            compact[key] = {
                name: value for name, value in section.items()
                if value is None or isinstance(value, (bool, int, float, str))
            }
    return compact


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


def _dave_evidence_continuation_offset(
    activity_fields: dict, *, current_job_id: str = "",
) -> int:
    """从最近一次已完成自治任务继续证据窗口；NYXI只作用于Dave当前活动。"""
    note = ext((activity_fields or {}).get("数据口径备注"))
    for line in reversed(note.splitlines()):
        if not line.startswith(RUNTIME_JOB_PREFIX):
            continue
        try:
            payload = json.loads(line[len(RUNTIME_JOB_PREFIX):])
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if current_job_id and payload.get("job_id") == current_job_id:
            continue
        continuation = ((payload.get("result") or {}).get("evidence_continuation") or {})
        try:
            offset = int(continuation.get("next_offset"))
        except (TypeError, ValueError):
            continue
        return max(17, offset)
    return 17


async def persist_runtime_job(*, campaign_id: str, job_id: str, mode: str,
                              status: str, result: dict | None = None,
                              error: str = "", started_ts: float | None = None) -> dict:
    lock = _JOB_NOTE_LOCKS.setdefault(campaign_id, asyncio.Lock())
    async with lock:
        activity = await launch_evidence.get_activity(campaign_id)
        fields = activity.get("fields") or {}
        now_ts = int(time.time())
        previous = None
        ordinary_lines = []
        other_job_lines = []
        for existing_line in ext(fields.get("数据口径备注")).splitlines():
            if existing_line.startswith(RUNTIME_JOB_PREFIX):
                try:
                    value = json.loads(existing_line[len(RUNTIME_JOB_PREFIX):])
                    if value.get("job_id") == job_id:
                        previous = value
                        continue
                except (TypeError, ValueError, json.JSONDecodeError):
                    pass
                other_job_lines.append(existing_line)
            else:
                ordinary_lines.append(existing_line)
        payload = {
            "job_id": job_id, "campaign_id": campaign_id, "mode": mode,
            "status": status,
            "started_ts": int(started_ts or (previous or {}).get("started_ts") or now_ts),
            "updated_ts": now_ts,
        }
        if result is not None and status in {"success", "degraded", "error"}:
            payload["result"] = _runtime_persisted_result_summary(result)
        if error:
            payload["error"] = str(error)[:300]
        current_line = RUNTIME_JOB_PREFIX + json.dumps(
            payload, ensure_ascii=False, separators=(",", ":"),
        )
        if len(current_line) > 3000 and result is not None:
            payload["result"] = _runtime_minimum_result_summary(result)
            current_line = RUNTIME_JOB_PREFIX + json.dumps(
                payload, ensure_ascii=False, separators=(",", ":"),
            )
        if len(current_line) > 3000:
            payload.pop("result", None)
            payload["error"] = "runtime_result_summary_exceeded_note_limit"
            current_line = RUNTIME_JOB_PREFIX + json.dumps(
                payload, ensure_ascii=False, separators=(",", ":"),
            )
        # 保留同活动其他后台任务，确保自治补池与证据续供可分别按job_id回查。
        retained = ordinary_lines + other_job_lines[-5:] + [current_line]
        while len("\n".join(retained)) > 3000 and len(retained) > 1:
            retained.pop(0)
        note = "\n".join(retained)
        await feishu.update_record(
            config.T_LAUNCH_CAMPAIGN, activity["record_id"],
            {"数据口径备注": note},
        )
        return payload


def _append_activity_note_preserving_runtime_jobs(
    history: str, note: str, *, limit: int = 3000,
) -> str:
    """追加业务备注时优先保留完整后台任务标记，避免从行中间截断。"""
    ordinary_lines = []
    job_lines = []
    for line in str(history or "").splitlines():
        if line.startswith(RUNTIME_JOB_PREFIX):
            job_lines.append(line)
        elif line.strip():
            ordinary_lines.append(line)
    if str(note or "").strip():
        ordinary_lines.append(str(note).strip())
    retained_jobs = job_lines[-5:]

    def build() -> str:
        return "\n".join(ordinary_lines + retained_jobs)

    while ordinary_lines and len(build()) > limit:
        ordinary_lines.pop(0)
    while len(retained_jobs) > 1 and len(build()) > limit:
        retained_jobs.pop(0)
    if retained_jobs and len(build()) > limit:
        # persist_runtime_job 已保证单条标记不超过字段上限；极端情况下宁可
        # 暂不记录本次日报，也不能破坏最新 job_id 的 JSON 结构。
        return retained_jobs[-1]
    return build()[-limit:]


async def daily_feedback(campaign_id: str) -> dict:
    metrics = await sync_campaign_outcomes_and_metrics(campaign_id)
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
        "数据口径备注": _append_activity_note_preserving_runtime_jobs(
            history, note,
        ),
    })
    if metrics["action"] == "stop":
        await feishu.update_record(
            config.T_LAUNCH_CAMPAIGN, activity["record_id"], {"发送邮件授权": False},
        )
        return {**metrics, "runtime": {"stopped": True}}
    if metrics["action"] == "expand":
        inventory = await _campaign_ready_inventory(campaign_id)
        non_review_active = _non_review_active_count(
            int(inventory.get("active_participants") or 0),
            int(inventory.get("pending_review") or 0),
        )
        target = max(100, non_review_active + 100)
        runtime = await run_campaign(
            campaign_id=campaign_id, pool_target=target, queue_limit=120,
        )
        return {**metrics, "runtime": runtime}
    return {**metrics, "runtime": {"held": True}}

"""集中上稿活动运行器。

只负责三件事：追加系统可自动通过的活动参与人、生成活动草稿、
根据每日反馈决定继续扩池/保持/停止。真实发送始终由 auto_send 完成。
"""

from __future__ import annotations

import asyncio
import hashlib
import re
import time

from . import (
    config,
    draft_router,
    enrich,
    feishu,
    launch_candidate_preview,
    launch_evidence,
    launch_outreach,
    launch_participation,
)
from .feishu import ext, xrid


class LaunchRuntimeError(RuntimeError):
    pass


_LOCKS: dict[str, asyncio.Lock] = {}
LAUNCH_QUEUE_TEMPLATE_VERSION = "launch-queue-v1"


def _ids(value) -> list[str]:
    return sorted(launch_evidence._ids(value))


def _queue_key(campaign_id: str, participant_id: str) -> str:
    digest = hashlib.sha1(f"{campaign_id}|{participant_id}".encode("utf-8")).hexdigest()[:20]
    return f"launchq-{digest}"


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
                               preview: dict | None = None) -> dict:
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
    }


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
        "邮件主题": subject[:200], "邮件正文": body, "邮件语言": "en",
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

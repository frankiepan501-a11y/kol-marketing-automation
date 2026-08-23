"""SLA 超时检查 — 4 层 SLA 状态机推进

V1 寄样链路 SLA (扫"寄样订单号 != 空"的草稿):

层 1 [继承] 待审 24h 升级
  寄样阶段=待发货 AND 邮件草稿状态=待审 AND 生成时间 + 24h ≤ now
  → 重发飞书卡片 (escalation=True) + 全员 ping

层 2 +7d 内容提醒
  寄样阶段=已签收 AND 签收时间 + 7d ≤ now AND 没发过 CONTENT_REMINDER
  → 自动生 TEMPLATE_CONTENT_REMINDER 草稿 → reviewer 评分 → 路由

层 3 +30d 内容未产出软标
  寄样阶段∈{已签收, 已发货} AND (签收时间 OR 发货时间) + 30d ≤ now
  AND 主表「上稿日期」为空 (Phase 2 daemon 没扫到)
  AND 草稿「内容产出30d标记」未打
  → 主表合作状态→"未产出" + 草稿表"内容产出30d标记"=True + 飞书卡片提醒

层 4 +60d 低 ROI 软标
  寄样阶段=已签收 AND 签收时间 + 60d ≤ now AND 主表「累计订单数」<3
  AND 草稿「低ROI60d标记」未打
  → 草稿表"低ROI60d标记"=True + 主表「维护标签」加"低ROI候选" + 飞书卡片提示
"""
import datetime as dt
import os
import re, time
from collections import Counter
from zoneinfo import ZoneInfo
from . import config, feishu, draft_router, reply_drafter, brand_line_state
from .feishu import ext, xrid


SLA_HOURS_REVIEW = 24            # 层 1: ship_confirm 待审超时
SLA_DAYS_CONTENT_REMINDER = 7    # 层 2: 签收后 7 天内容提醒
SLA_DAYS_NO_CONTENT = 30         # 层 3: 30 天无内容产出软标
SLA_DAYS_LOW_ROI = 60            # 层 4: 60 天累计订单<3 软标
LOW_ROI_ORDER_THRESHOLD = 3
SLA_DAYS_SOFT_NUDGE = 12         # P4: 暖信发出 +12d 仍无上稿 → 软关怀 nudge (非催稿, 在 L3 30d 之前)
SLA_HOURS_FRANKIE_EXCEPTION = 48
P1_DRAFT_SOURCES = frozenset({
    "reply", "affiliate_quote", "ship_confirm", "tracking_followup",
})
SOURCE_LABELS = {
    "reply": "KOL 来信",
    "affiliate_quote": "报价/付费合作",
    "ship_confirm": "寄样确认邮件",
    "tracking_followup": "待补运单信息",
    "cold": "首次合作邀请",
    "followup": "未回复跟进",
    "secondary_outreach": "二次联系",
    "warm_recap": "寄样后关怀",
    "(空来源)": "其他合作邮件",
}

# 层 1c (2026-05-22 B): 已发货 → 已签收 自动推进 (按物流渠道时效假定送达)
#   背景: KOL 很少主动回"收到", 否则 L2(+7d 催稿)/L4(+60d) 永远卡在"已签收"前不触发.
#   规则: 发货时间 + 渠道时效天数 ≤ now → 自动标已签收 (签收时间 = 假定送达时刻).
#   Amazon MCF (TBA/TBC 运单 或 物流商含 Amazon/AMZN) = 7 天送达
#     → 之后 L2 在 签收时间+7d 催稿 (= 发货后 14d), 即 Frankie 定的"Amazon MCF 默认 L2+7d".
#   非 Amazon (国际/其他承运商) 暂用保守默认 14 天送达, 待补全各渠道时效表.
AMAZON_MCF_TRANSIT_DAYS = 7
DEFAULT_TRANSIT_DAYS = 14


def _carrier_transit_days(carrier: str, tracking: str) -> int:
    c = (carrier or "").lower()
    t = (tracking or "").upper()
    if "amazon" in c or "amzn" in c or t.startswith("TBA") or t.startswith("TBC"):
        return AMAZON_MCF_TRANSIT_DAYS
    return DEFAULT_TRANSIT_DAYS


async def _is_already_escalated(rec: dict) -> bool:
    """2026-05-17 A3: 改用专用字段 SLA已升级 (checkbox), 防审批意见字段 500 字截断丢 token.
    兼容: 老草稿审批意见含 [SLA-ESCALATED] 仍识别 (字段迁移脚本已跑, 但兜底防漏)"""
    f = rec["fields"]
    if f.get("SLA已升级"):
        return True
    note = ext(f.get("审批意见")) or ""
    return "[SLA-ESCALATED" in note  # 兼容老数据


async def _mark_escalated(record_id: str):
    """2026-05-17 A3: 写专用字段 SLA已升级=True, 不再往审批意见塞 token"""
    await feishu.update_record(config.T_DRAFT, record_id, {"SLA已升级": True})


# ===== 层 1: 待审草稿汇总提醒 (2026-08-23 去逐条卡片风暴) =====
def _draft_source(rec: dict) -> str:
    f = rec.get("fields") or {}
    source = (ext(f.get("邮件草稿来源")) or "").strip()
    if not source and ext(f.get("寄样阶段")) == "待发货":
        return "ship_confirm"
    return source or "(空来源)"


def _draft_age_hours(rec: dict, now_ms: int) -> int:
    try:
        generated = int((rec.get("fields") or {}).get("生成时间") or 0)
    except (TypeError, ValueError):
        generated = 0
    if not generated:
        return SLA_HOURS_REVIEW
    return max(0, int((now_ms - generated) / 3600 / 1000))


def _local_date(timestamp_ms: int) -> dt.date:
    try:
        timezone = ZoneInfo(config.KOL_SLA_TIMEZONE)
    except Exception:
        timezone = ZoneInfo("Asia/Shanghai")
    return dt.datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone).date()


def _p2_already_notified_today(rec: dict, now_ms: int) -> bool:
    try:
        sent_at = int((rec.get("fields") or {}).get("卡片发送时间") or 0)
    except (TypeError, ValueError):
        return False
    return bool(sent_at and _local_date(sent_at) == _local_date(now_ms))


def _claim_p2_daily_run(now_ms: int) -> bool:
    """Atomically claim today's P2 run, independent of any draft remaining pending."""
    day = _local_date(now_ms).isoformat()
    state_dir = config.KOL_SLA_STATE_DIR
    os.makedirs(state_dir, exist_ok=True)
    claim_path = os.path.join(state_dir, f"p2-{day}.claimed")
    try:
        fd = os.open(claim_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        return False
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(str(now_ms))
    return True


def _release_p2_daily_claim(now_ms: int) -> None:
    day = _local_date(now_ms).isoformat()
    claim_path = os.path.join(config.KOL_SLA_STATE_DIR, f"p2-{day}.claimed")
    try:
        os.unlink(claim_path)
    except FileNotFoundError:
        pass


async def _p2_marker_exists_today(now_ms: int) -> bool:
    """Bitable marker survives service restarts even after the claimed draft leaves the queue."""
    marked = await feishu.search_records(config.T_DRAFT, [
        {"field_name": "SLA已升级", "operator": "is", "value": ["true"]},
    ], field_names=["邮件草稿来源", "卡片发送时间", "SLA已升级"])
    return any(
        _draft_source(rec) not in P1_DRAFT_SOURCES and _p2_already_notified_today(rec, now_ms)
        for rec in marked
    )


def _queue_url() -> str:
    base = f"https://u1wpma3xuhr.feishu.cn/base/{config.FEISHU_APP_TOKEN}?table={config.T_DRAFT}"
    view_id = (getattr(config, "KOL_DRAFT_QUEUE_VIEW_ID", "") or "").strip()
    return f"{base}&view={view_id}" if view_id else base


def _record_url(record_id: str) -> str:
    return f"{_queue_url()}&record={record_id}"


def _source_summary(items: list) -> str:
    counts = Counter(_draft_source(rec) for rec in items)
    parts = []
    for source, count in sorted(counts.items(), key=lambda pair: (-pair[1], pair[0])):
        label = SOURCE_LABELS.get(source, source)
        parts.append(f"{label} {count} 封")
    return "｜".join(parts) if parts else "暂无"


def _top_record_lines(items: list, now_ms: int, limit: int = 5) -> str:
    ordered = sorted(items, key=lambda rec: (-_draft_age_hours(rec, now_ms), rec.get("record_id", "")))
    lines = []
    for rec in ordered[:limit]:
        rid = rec.get("record_id") or ""
        f = rec.get("fields") or {}
        subject = (ext(f.get("邮件主题")) or "无主题").replace("\n", " ")[:55]
        source = _draft_source(rec)
        age = _draft_age_hours(rec, now_ms)
        lines.append(f"- [{SOURCE_LABELS.get(source, '其他合作邮件')}｜已等 {age} 小时｜{subject}]({_record_url(rid)})")
    remaining = len(items) - len(lines)
    if remaining > 0:
        lines.append(f"- 还有 {remaining} 封，请打开页面后按「生成时间」从早到晚处理")
    return "\n".join(lines)


def build_sla_digest_card(items: list, now_ms: int, *, audience: str, level: str) -> dict:
    """Build an actionable queue-management card; this card never approves or sends email itself."""
    oldest = max((_draft_age_hours(rec, now_ms) for rec in items), default=0)
    if audience == "frankie":
        title = f"有 {len(items)} 封重要邮件超过 48 小时没处理"
        owner_label = "需要谁处理"
        owner = "独立站运营专员"
        deadline_label = "你今天要做"
        deadline = "确认负责人处理完"
        action_text = (
            "**你不用逐封审核。**请确认独立站运营专员今天处理完这些邮件。"
            "只有涉及高额付费、客户风险或特殊承诺时，才需要你决定。"
        )
        header_template = "orange"
    elif level == "P2":
        title = f"有 {len(items)} 封日常合作邮件待审核"
        owner_label = "谁来处理"
        owner = "独立站运营专员"
        deadline_label = "请在"
        deadline = "明天前处理完"
        action_text = (
            "1. 点下面按钮打开待审核邮件。\n"
            "2. 先看对方原邮件，再检查系统生成的回复。\n"
            "3. 没问题点「通过」；需要修改就先改正文；不适合回复就点「否决」或「退回重做」。"
        )
        header_template = "yellow"
    else:
        title = f"有 {len(items)} 封重要合作邮件待审核"
        owner_label = "谁来处理"
        owner = "独立站运营专员"
        deadline_label = "请在"
        deadline = "今天 4 小时内处理"
        action_text = (
            "1. 点下面按钮打开待审核邮件。\n"
            "2. 先看对方原邮件，再检查系统生成的回复。\n"
            "3. 没问题点「通过」；需要修改就先改正文；不适合回复就点「否决」或「退回重做」。\n"
            "4. **看到“待补运单信息”时，先补齐运单号和物流商，再检查正文并点「通过」。**"
        )
        header_template = "orange"

    filter_note = "系统已自动排除处理完的邮件；这里只显示等待超过 24 小时、仍需要审核的邮件。"

    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "template": header_template,
            "title": {"tag": "plain_text", "content": title},
        },
        "elements": [
            {"tag": "div", "fields": [
                {"is_short": True, "text": {"tag": "lark_md", "content": f"**{owner_label}**：{owner}"}},
                {"is_short": True, "text": {"tag": "lark_md", "content": f"**{deadline_label}**：{deadline}"}},
                {"is_short": True, "text": {"tag": "lark_md", "content": f"**一共**：{len(items)} 封"}},
                {"is_short": True, "text": {"tag": "lark_md", "content": f"**最久一封**：已等 {oldest} 小时"}},
            ]},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"**现在需要你做什么**\n{action_text}"}},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"**本次有哪些邮件**\n{_source_summary(items)}"}},
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md", "content":
                f"{filter_note}\n\n"
                "⚠️ **注意：点「通过」后，邮件会进入真实发送流程。请先看完原邮件和回复内容。**"}},
            {"tag": "div", "text": {"tag": "lark_md", "content":
                f"**建议先处理这 {min(5, len(items))} 封（等待最久）**\n{_top_record_lines(items, now_ms)}"}},
            {"tag": "action", "actions": [
                {"tag": "button", "text": {"tag": "plain_text", "content": f"去审核这 {len(items)} 封邮件"},
                 "url": _queue_url(), "type": "primary"},
            ]},
            {"tag": "note", "elements": [
                {"tag": "plain_text", "content": "这张卡只提醒你有哪些邮件要处理；实际审核在邮件草稿表完成。处理后不用回复这张卡。"},
            ]},
        ],
    }


def _is_p2_digest_hour(now_ms: int) -> bool:
    try:
        timezone = ZoneInfo(config.KOL_SLA_TIMEZONE)
    except Exception:
        timezone = ZoneInfo("Asia/Shanghai")
    local_hour = dt.datetime.fromtimestamp(now_ms / 1000, tz=timezone).hour
    return local_hour == int(config.KOL_SLA_P2_DIGEST_HOUR)


async def _send_digest(targets: list, card: dict, *, level: str) -> dict:
    sent = 0
    failed = 0
    errors = []
    message_ids = []
    for name, oid in targets:
        if not oid:
            continue
        try:
            message_id = await feishu.send_card_message("open_id", oid, card, biz="KOL", level=level)
            sent += 1
            if message_id:
                message_ids.append(message_id)
        except Exception as e:
            failed += 1
            errors.append(f"{name}: {str(e)[:120]}")
            print(f"[sla_check digest] notify {name} fail: {e}")
    return {"sent": sent, "failed": failed, "errors": errors, "message_ids": message_ids}


async def collect_sla_overdue_drafts(now_ms: int) -> dict:
    """Read-only collection and classification used by production and the Frankie-only preflight."""
    field_names = [
        "邮件主题", "邮件草稿来源", "邮件草稿状态", "对象类型", "AI评分",
        "生成时间", "寄样阶段", "审批意见", "SLA已升级", "卡片发送时间",
    ]
    waiting_review = await feishu.search_records(config.T_DRAFT, [
        {"field_name": "邮件草稿状态", "operator": "is", "value": ["待审"]},
    ], field_names=field_names)
    waiting_tracking = await feishu.search_records(config.T_DRAFT, [
        {"field_name": "邮件草稿状态", "operator": "is", "value": ["待修改"]},
        {"field_name": "邮件草稿来源", "operator": "is", "value": ["tracking_followup"]},
    ], field_names=field_names)

    all_items = waiting_review + [
        rec for rec in waiting_tracking
        if rec.get("record_id") not in {row.get("record_id") for row in waiting_review}
    ]
    overdue = [rec for rec in all_items if _draft_age_hours(rec, now_ms) >= SLA_HOURS_REVIEW]
    p1_items = [rec for rec in overdue if _draft_source(rec) in P1_DRAFT_SOURCES]
    p2_items = [rec for rec in overdue if _draft_source(rec) not in P1_DRAFT_SOURCES]
    # “每日一张”用任一 P2 记录的当日提醒时间作为持久标记。只要今天已经留痕，
    # 同小时重试、人工重跑或服务重启都不会再发第二张。
    p2_already_sent_today = any(_p2_already_notified_today(rec, now_ms) for rec in p2_items)
    p2_due_today = [] if p2_already_sent_today else list(p2_items)
    p1_over_48h = [
        rec for rec in p1_items
        if _draft_age_hours(rec, now_ms) >= SLA_HOURS_FRANKIE_EXCEPTION
    ]
    return {
        "all": all_items,
        "overdue": overdue,
        "p1": p1_items,
        "p2": p2_items,
        "p2_due_today": p2_due_today,
        "p2_already_sent_today": p2_already_sent_today,
        "p1_over_48h": p1_over_48h,
    }


async def _layer1_review_overdue(now_ms: int) -> dict:
    """Send at most one reviewer P1 digest, one Frankie 48h digest, and one daily reviewer P2 digest."""
    collected = await collect_sla_overdue_drafts(now_ms)
    all_items = collected["all"]
    overdue = collected["overdue"]
    p1_items = collected["p1"]
    p2_items = collected["p2"]
    p2_due_today = collected["p2_due_today"]
    p1_over_48h = collected["p1_over_48h"]

    frankie_targets = await feishu.resolve_notify_targets("frankie")
    if config.KOL_SLA_CARD_FRANKIE_ONLY:
        reviewer_targets = frankie_targets
    else:
        reviewer_targets = await feishu.resolve_notify_targets("ship_main")
    reviewer_delivery = {"sent": 0, "failed": 0, "errors": [], "message_ids": []}
    frankie_delivery = {"sent": 0, "failed": 0, "errors": [], "message_ids": []}
    p2_delivery = {"sent": 0, "failed": 0, "errors": [], "message_ids": []}

    if p1_items:
        reviewer_delivery = await _send_digest(
            reviewer_targets,
            build_sla_digest_card(p1_items, now_ms, audience="reviewer", level="P1"),
            level="P1",
        )
    if p1_over_48h:
        frankie_delivery = await _send_digest(
            frankie_targets,
            build_sla_digest_card(p1_over_48h, now_ms, audience="frankie", level="P1"),
            level="P1",
        )
    p2_claim_record_id = ""
    p2_claim_persisted = False
    if p2_due_today and _is_p2_digest_hour(now_ms):
        already_marked = await _p2_marker_exists_today(now_ms)
        locally_claimed = False if already_marked else _claim_p2_daily_run(now_ms)
        if not already_marked and locally_claimed:
            # 先写持久标记，再发卡。服务重启看 Bitable 标记；同实例并发看原子日期文件。
            p2_claim_record_id = min((rec.get("record_id") or "") for rec in p2_due_today)
            try:
                await feishu.update_record(config.T_DRAFT, p2_claim_record_id, {
                    "SLA已升级": True,
                    "卡片发送时间": now_ms,
                })
            except Exception as e:
                _release_p2_daily_claim(now_ms)
                p2_delivery = {
                    "sent": 0, "failed": 1,
                    "errors": [f"P2 daily claim failed: {str(e)[:120]}"], "message_ids": [],
                }
                print(f"[sla_check digest] P2 daily claim failed, skip send: {e}")
            else:
                p2_claim_persisted = True
                p2_delivery = await _send_digest(
                    reviewer_targets,
                    build_sla_digest_card(p2_due_today, now_ms, audience="reviewer", level="P2"),
                    level="P2",
                )

    reminder_timestamps_written = int(p2_claim_persisted)
    delivered_items = []
    if p2_delivery["sent"]:
        delivered_items.extend(p2_due_today)
    if delivered_items:
        seen_delivered = set()
        for rec in delivered_items:
            rid = rec.get("record_id") or ""
            if not rid or rid in seen_delivered:
                continue
            seen_delivered.add(rid)
            if rid == p2_claim_record_id:
                continue
            try:
                await feishu.update_record(config.T_DRAFT, rid, {"卡片发送时间": now_ms})
                reminder_timestamps_written += 1
            except Exception as e:
                print(f"[sla_check digest] write reminder timestamp fail: {e}")

    return {
        "layer": 1,
        "checked": len(all_items),
        "not_yet": len(all_items) - len(overdue),
        "p1_overdue": len(p1_items),
        "p1_over_48h": len(p1_over_48h),
        "p2_overdue": len(p2_items),
        "p2_due_today": len(p2_due_today),
        "reviewer_digest_sent": int(reviewer_delivery["sent"] > 0),
        "frankie_digest_sent": int(frankie_delivery["sent"] > 0),
        "p2_digest_sent": int(p2_delivery["sent"] > 0),
        "newly_marked": 0,
        "reminder_timestamps_written": reminder_timestamps_written,
        "delivery_failures": (
            reviewer_delivery["failed"] + frankie_delivery["failed"] + p2_delivery["failed"]
        ),
        "reviewer_message_ids": reviewer_delivery["message_ids"],
        "frankie_message_ids": frankie_delivery["message_ids"],
        "p2_message_ids": p2_delivery["message_ids"],
    }


# ===== 层 2: +7d 已签收无回应 → 自动生 CONTENT_REMINDER =====
async def _layer2_content_reminder(now_ms: int) -> dict:
    cutoff_ms = now_ms - SLA_DAYS_CONTENT_REMINDER * 86400 * 1000

    items = await feishu.search_records(config.T_DRAFT, [
        {"field_name": "寄样阶段", "operator": "is", "value": ["已签收"]},
    ])

    triggered = 0
    skipped = 0
    not_yet = 0

    for rec in items:
        f = rec["fields"]
        rid = rec["record_id"]

        signed_at = int(f.get("签收时间") or 0)
        if not signed_at or signed_at > cutoff_ms:
            not_yet += 1
            continue

        # 已经发过 reminder (寄样阶段已推进 OR 命中关键词含 content-reminder)
        kw = ext(f.get("命中关键词")) or ""
        note = ext(f.get("审批意见")) or ""
        if "[REMINDER-SENT]" in note or "content-reminder" in kw:
            skipped += 1
            continue

        # 标记已发(防重)
        new_note = (note + f" [REMINDER-SENT@{int(time.time())}]")[:500]
        try:
            await feishu.update_record(config.T_DRAFT, rid, {"审批意见": new_note})
        except Exception:
            pass

        # 生 CONTENT_REMINDER 草稿: 调 reply_drafter 但用新模板
        contact_type = "editor" if xrid(f.get("关联媒体人")) else "KOL"
        contact_rid = xrid(f.get("关联媒体人")) if contact_type == "editor" else xrid(f.get("关联KOL"))
        if not contact_rid:
            continue

        target_table = config.T_EDITOR if contact_type == "editor" else config.T_KOL
        try:
            contact_record = await feishu.get_record(target_table, contact_rid)
        except Exception:
            continue
        cf = contact_record["fields"]
        contact_name = ext(cf.get("媒体人姓名")) if contact_type == "editor" else ext(cf.get("账号名"))
        first = reply_drafter._first_name(contact_name)

        product_name = "the product"
        prod_rid = xrid(f.get("关联产品"))
        if prod_rid:
            try:
                prod = await feishu.get_record(config.T_PRODUCT, prod_rid)
                product_name = ext(prod["fields"].get("产品英文名")) or ext(prod["fields"].get("产品名")) or "the product"
            except Exception:
                pass

        sender_alias = ext(f.get("发送邮箱"))
        brand = config.brand_from_text(sender_alias) or "FUNLAB"  # 2026-06-26 修白牌错标
        sig = reply_drafter._sender_signature(brand)

        body = reply_drafter.TEMPLATE_CONTENT_REMINDER.format(
            first_name=first, signature=sig, product_name=product_name,
        )
        original_subject = ext(f.get("邮件主题"))

        link_field = "关联媒体人" if contact_type == "editor" else "关联KOL"
        new_fields = {
            "邮件草稿ID": f"reminder-{contact_rid[-8:]}-{int(time.time())}",
            link_field: [contact_rid],
            "邮件主题": ("Re: " + original_subject)[:200],
            "邮件正文": body,
            "邮件语言": "en",
            "邮件草稿状态": "待审",
            "邮件草稿来源": "followup",
            "对象类型": contact_type if contact_type == "KOL" else "媒体人",
            "发送邮箱": sender_alias,
            "发送人署名": "Frankie",
            "生成时间": now_ms,
            "建议发送时间": now_ms,
            "重生次数": 0,
            "收件邮箱": feishu.clean_email(ext(cf.get("邮箱")))[0] or "",
            "命中关键词": "content-reminder (sla L2 +7d)",
        }
        if prod_rid:
            new_fields["关联产品"] = [prod_rid]
        # 2026-05-17 A2: 继承父草稿关联任务 (任务台统计需要)
        task_rid = xrid(f.get("关联任务"))
        if task_rid:
            new_fields["关联任务"] = [task_rid]

        try:
            new_rid = await feishu.create_record(config.T_DRAFT, new_fields)
            print(f"[sla_check L2] reminder draft created rid={new_rid} for {contact_name}")
            try:
                await draft_router.route_draft(new_rid)
            except Exception as e:
                print(f"[sla_check L2] router fail rid={new_rid}: {e}")
            triggered += 1
        except Exception as e:
            print(f"[sla_check L2] create fail: {e}")

    return {"layer": 2, "checked": len(items), "triggered": triggered,
            "skipped": skipped, "not_yet": not_yet}


# ===== 层 3: +30d 无内容产出 → 主表软标"未产出" =====
async def _layer3_no_content_30d(now_ms: int) -> dict:
    cutoff_ms = now_ms - SLA_DAYS_NO_CONTENT * 86400 * 1000

    items = await feishu.search_records(config.T_DRAFT, [
        {"field_name": "寄样阶段", "operator": "is", "value": ["已签收", "已发货"]},
    ])

    flagged = 0
    skipped = 0
    not_yet = 0

    for rec in items:
        f = rec["fields"]
        rid = rec["record_id"]
        if f.get("内容产出30d标记"):
            skipped += 1
            continue

        ref_time = int(f.get("签收时间") or f.get("发货时间") or 0)
        if not ref_time or ref_time > cutoff_ms:
            not_yet += 1
            continue

        contact_type = "editor" if xrid(f.get("关联媒体人")) else "KOL"
        contact_rid = xrid(f.get("关联媒体人")) if contact_type == "editor" else xrid(f.get("关联KOL"))
        if not contact_rid:
            continue
        target_table = config.T_EDITOR if contact_type == "editor" else config.T_KOL

        try:
            contact = await feishu.get_record(target_table, contact_rid)
        except Exception:
            continue
        cf = contact["fields"]

        # 2026-06-17 双品牌修(#6): 按**该品牌线**草稿算"已上稿"才跳过, 不读主表混合上稿日期
        # (POWKONG 已上稿致主表上稿日期非空 → FUNLAB 线 35d 未产出被误跳漏标)
        ship_brand = config.brand_from_text(ext(f.get("发送邮箱"))) or "FUNLAB"
        try:
            _l3st = await brand_line_state.line_state(contact_rid, contact_type, ship_brand)
            if _l3st["uploaded"]:
                skipped += 1
                continue
        except Exception as _e:
            print(f"[sla_check L3] line_state fail, 回落主表上稿日期: {_e}")
            if cf.get("上稿日期"):     # 降级保守: 取不到草稿信号时回落旧口径
                skipped += 1
                continue

        contact_name = ext(cf.get("媒体人姓名")) if contact_type == "editor" else ext(cf.get("账号名"))

        # 草稿打标
        try:
            await feishu.update_record(config.T_DRAFT, rid, {"内容产出30d标记": True})
        except Exception as e:
            print(f"[sla_check L3] mark draft fail: {e}")

        # 主表合作状态软标 → "未产出" (如未存在该选项需运营在飞书加; 防 API 失败用 try)
        # 2026-06-17 双品牌(#6): 合作状态是单一 KOL 级字段, 若该 KOL 在**别的品牌线已合作** → 不降级
        # (否则 FUNLAB 线未产出会把 POWKONG 的已合作覆盖成未产出 = 数据损失)。仍打草稿标 + 发卡。
        coop_now = ext(cf.get("合作状态")) or ""
        if coop_now in ("已合作-免费", "已合作-免费(多次)", "已合作-付费"):
            print(f"[sla_check L3] {contact_rid} 别处已合作({coop_now}), 跳过 未产出 软标防降级")
        else:
            try:
                await feishu.update_record(target_table, contact_rid, {"合作状态": "未产出"})
            except Exception as e:
                print(f"[sla_check L3] master 合作状态 fail (option 可能未建): {e}")

        # 飞书卡片告警 (2026-05-31 统一字段: 加 KOL 信息块 compact)
        try:
            from . import reply_monitor
            base_url = f"https://u1wpma3xuhr.feishu.cn/base/{config.FEISHU_APP_TOKEN}?table={target_table}"
            _ctype_uni = "媒体人" if contact_type == "editor" else "KOL"
            _ci = {
                "name": contact_name,
                "stage": reply_monitor._contact_stage_label(cf) or "",
                "platform": (ext(cf.get("主要媒体")) or ext(cf.get("所属媒体"))) if contact_type == "editor" else ext(cf.get("主平台")),
                "fans": "" if contact_type == "editor" else (
                    f"{int(cf.get('粉丝数') or 0):,}" if cf.get('粉丝数') else ""),
            }
            card = {
                "header": {"template": "orange",
                           "title": {"tag": "plain_text", "content": f"📭 寄样 30 天无内容产出 — {contact_name}"}},
                "elements": [
                    feishu.build_contact_info_block(contact_info=_ci, contact_type=_ctype_uni, compact=True),
                    {"tag": "div", "text": {"tag": "lark_md",
                        "content": (f"**寄样订单**: {ext(f.get('寄样订单号'))}\n"
                                    f"**签收/发货**: {time.strftime('%Y-%m-%d', time.localtime(ref_time/1000))}\n"
                                    f"**Phase2 daemon 状态**: 未扫到上稿\n\n"
                                    f"已自动打软标「未产出」, 下次寄样降优先级。"
                                    f"如有误判 (例如 KOL 在飞书外发了内容) 请运营改主表合作状态。")}},
                    {"tag": "action", "actions": [
                        {"tag": "button", "text": {"tag": "plain_text", "content": "打开 KOL 主表"},
                         "url": base_url, "type": "primary"},
                    ]},
                ],
            }
            try:
                await feishu.send_card_message("chat_id", config.NOTIFY_CHAT_ID, card)
            except Exception:
                pass
            try:
                main_targets = (await draft_router._ship_confirm_targets())[0]
            except Exception:
                main_targets = []
            for name, oid in main_targets:  # 主审 only
                try:
                    await feishu.send_card_message("open_id", oid, card)
                except Exception:
                    pass
        except Exception as e:
            print(f"[sla_check L3] card fail: {e}")

        flagged += 1

    return {"layer": 3, "checked": len(items), "flagged": flagged,
            "skipped": skipped, "not_yet": not_yet}


# ===== 层 4: +60d 累计订单<3 → 低 ROI 软标 =====
async def _layer4_low_roi_60d(now_ms: int) -> dict:
    cutoff_ms = now_ms - SLA_DAYS_LOW_ROI * 86400 * 1000

    items = await feishu.search_records(config.T_DRAFT, [
        {"field_name": "寄样阶段", "operator": "is", "value": ["已签收"]},
    ])

    flagged = 0
    skipped = 0
    not_yet = 0

    for rec in items:
        f = rec["fields"]
        rid = rec["record_id"]
        if f.get("低ROI60d标记"):
            skipped += 1
            continue

        signed_at = int(f.get("签收时间") or 0)
        if not signed_at or signed_at > cutoff_ms:
            not_yet += 1
            continue

        contact_type = "editor" if xrid(f.get("关联媒体人")) else "KOL"
        contact_rid = xrid(f.get("关联媒体人")) if contact_type == "editor" else xrid(f.get("关联KOL"))
        if not contact_rid:
            continue
        target_table = config.T_EDITOR if contact_type == "editor" else config.T_KOL

        try:
            contact = await feishu.get_record(target_table, contact_rid)
        except Exception:
            continue
        cf = contact["fields"]

        order_count = 0
        try: order_count = int(cf.get("累计订单数") or 0)
        except (ValueError, TypeError): pass
        if order_count >= LOW_ROI_ORDER_THRESHOLD:
            skipped += 1
            continue

        contact_name = ext(cf.get("媒体人姓名")) if contact_type == "editor" else ext(cf.get("账号名"))

        try:
            await feishu.update_record(config.T_DRAFT, rid, {"低ROI60d标记": True})
        except Exception as e:
            print(f"[sla_check L4] mark draft fail: {e}")

        # 不动主表「维护标签」(单选,会覆盖运营人工标记) — 改飞书卡片让运营自决
        try:
            base_url = f"https://u1wpma3xuhr.feishu.cn/base/{config.FEISHU_APP_TOKEN}?table={target_table}"
            # 2026-05-31 统一字段: 加 KOL 信息块 compact
            from . import reply_monitor
            _ctype_uni = "媒体人" if contact_type == "editor" else "KOL"
            _ci = {
                "name": contact_name,
                "stage": reply_monitor._contact_stage_label(cf) or "",
                "platform": (ext(cf.get("主要媒体")) or ext(cf.get("所属媒体"))) if contact_type == "editor" else ext(cf.get("主平台")),
                "fans": "" if contact_type == "editor" else (
                    f"{int(cf.get('粉丝数') or 0):,}" if cf.get('粉丝数') else ""),
            }
            card = {
                "header": {"template": "yellow",
                           "title": {"tag": "plain_text", "content": f"📉 寄样 60 天累计订单<3 — {contact_name}"}},
                "elements": [
                    feishu.build_contact_info_block(contact_info=_ci, contact_type=_ctype_uni, compact=True),
                    {"tag": "div", "text": {"tag": "lark_md",
                        "content": (f"**寄样订单**: {ext(f.get('寄样订单号'))}\n"
                                    f"**累计订单数**: {order_count} (阈值<{LOW_ROI_ORDER_THRESHOLD})\n"
                                    f"**签收时间**: {time.strftime('%Y-%m-%d', time.localtime(signed_at/1000))}\n\n"
                                    f"已自动在草稿打「低ROI60d标记」。\n"
                                    f"是否要在主表把「维护标签」改成「观察」?"
                                    f"或合作状态改「不合适」? 你来定。")}},
                    {"tag": "action", "actions": [
                        {"tag": "button", "text": {"tag": "plain_text", "content": "打开主表"},
                         "url": base_url, "type": "primary"},
                    ]},
                ],
            }
            try:
                await feishu.send_card_message("chat_id", config.NOTIFY_CHAT_ID, card)
            except Exception:
                pass
            try:
                main_targets = (await draft_router._ship_confirm_targets())[0]
            except Exception:
                main_targets = []
            for name, oid in main_targets:
                try:
                    await feishu.send_card_message("open_id", oid, card)
                except Exception:
                    pass
        except Exception as e:
            print(f"[sla_check L4] card fail: {e}")

        flagged += 1

    return {"layer": 4, "checked": len(items), "flagged": flagged,
            "skipped": skipped, "not_yet": not_yet}


# ===== 层 1c: 已发货 + 渠道时效 → 自动推进已签收 (2026-05-22 B) =====
async def _layer1c_auto_sign_by_carrier(now_ms: int) -> dict:
    """KOL 很少主动回"收到" → 已签收 永远不被 write → L2/L4 dead.
    按物流渠道假定送达时效, 发货时间 + 渠道天数 ≤ now 即自动标已签收.
    纯状态推进, 不发邮件/卡片 (下游 L2 才会生成催稿草稿, 仍走 reviewer)."""
    items = await feishu.search_records(config.T_DRAFT, [
        {"field_name": "寄样阶段", "operator": "is", "value": ["已发货"]},
    ])
    advanced = 0
    not_yet = 0
    skipped = 0
    for rec in items:
        f = rec["fields"]
        rid = rec["record_id"]
        if int(f.get("签收时间") or 0):       # 已有签收时间, 别覆盖
            skipped += 1
            continue
        ship_ms = int(f.get("发货时间") or 0)
        if not ship_ms:                        # 没发货时间无法推算
            skipped += 1
            continue
        days = _carrier_transit_days(ext(f.get("物流商")), ext(f.get("运单号")))
        signed_ms = ship_ms + days * 86400 * 1000
        if signed_ms > now_ms:                 # 还没到假定送达
            not_yet += 1
            continue
        try:
            await feishu.update_record(config.T_DRAFT, rid, {
                "寄样阶段": "已签收", "签收时间": signed_ms,
            })
            advanced += 1
            print(f"[sla_check L1c] auto-sign rid={rid} carrier_days={days} signed_at={signed_ms}")
        except Exception as e:
            print(f"[sla_check L1c] update fail rid={rid}: {e}")
    return {"layer": "1c", "checked": len(items), "advanced": advanced,
            "not_yet": not_yet, "skipped": skipped}


# ===== P4 软关怀 nudge: 暖信发出 +12d 无上稿 → 一封软关怀 (寄样后 brief 重设计) =====
async def _layer_soft_nudge(now_ms: int) -> dict:
    """已签收 KOL 收到暖信(P3 brief recap)后, +SLA_DAYS_SOFT_NUDGE 天仍无上稿 → 生成一封
    "轻关怀"邮件(非催稿, 无压力)→ 强制人审 → auto_send 发出.

    与已下线的 L2 催稿区别(为什么不直接复活 L2):
      - 位置: 在暖信(P3)之后, 不替代暖信; 计时基准 = **暖信发出时间** (gate: 暖信必须已发).
      - 语气: TEMPLATE_SOFT_NUDGE 软关怀, 不问"发了没"; L2 的 TEMPLATE_CONTENT_REMINDER 还在催.
      - 闸门: 主表「上稿日期」空(没上稿才发) + 暖信已发出(brief 送达才 nudge, 防过早 nudge).
    生成草稿 source=followup(不新建单选选项避免清空风险), 身份标记 = 邮件草稿ID `nudge-` 前缀
    (auto_send 据此跳过 合作状态 倒退; 不能用命中关键词标记 — route_draft 评审后会覆盖它).
    去重: ship 草稿「审批意见」打 [NUDGE-SENT].
    """
    cutoff_ms = now_ms - SLA_DAYS_SOFT_NUDGE * 86400 * 1000

    items = await feishu.search_records(config.T_DRAFT, [
        {"field_name": "寄样阶段", "operator": "is", "value": ["已签收"]},
    ])
    if not items:
        return {"layer": "nudge", "checked": 0, "triggered": 0, "skipped": 0, "not_yet": 0}

    # 一次性拉所有"已发送暖信", 建 contact_rid -> 暖信发出时间(取最新) 映射 (= gate + 计时基准)
    warm_sent = await feishu.search_records(config.T_DRAFT, [
        {"field_name": "邮件草稿来源", "operator": "is", "value": ["warm_recap"]},
        {"field_name": "邮件草稿状态", "operator": "is", "value": ["已发送"]},
    ], field_names=["关联KOL", "关联媒体人", "发送时间", "生成时间"])
    brief_sent = {}
    for r in warm_sent:
        wf = r["fields"]
        crid = xrid(wf.get("关联KOL")) or xrid(wf.get("关联媒体人"))
        if not crid:
            continue
        st = int(wf.get("发送时间") or wf.get("生成时间") or 0)
        if st > brief_sent.get(crid, 0):
            brief_sent[crid] = st

    triggered, skipped, not_yet = 0, 0, 0

    for rec in items:
        f = rec["fields"]
        rid = rec["record_id"]

        note = ext(f.get("审批意见")) or ""
        if "[NUDGE-SENT" in note:
            skipped += 1
            continue

        contact_type = "editor" if xrid(f.get("关联媒体人")) else "KOL"
        contact_rid = xrid(f.get("关联媒体人")) if contact_type == "editor" else xrid(f.get("关联KOL"))
        if not contact_rid:
            skipped += 1
            continue

        # gate: 暖信必须已发出 (brief 送达才 nudge) + 从暖信发出时间起算 N 天
        brief_ms = brief_sent.get(contact_rid, 0)
        if not brief_ms:
            skipped += 1
            continue
        if brief_ms > cutoff_ms:        # 暖信发出还没满 N 天
            not_yet += 1
            continue

        target_table = config.T_EDITOR if contact_type == "editor" else config.T_KOL
        try:
            contact = await feishu.get_record(target_table, contact_rid)
        except Exception:
            skipped += 1
            continue
        cf = contact["fields"]

        # 已上稿 → 不 nudge (与 L3 同口径, 主表「上稿日期」非空)
        if cf.get("上稿日期"):
            skipped += 1
            continue

        contact_name = ext(cf.get("媒体人姓名")) if contact_type == "editor" else ext(cf.get("账号名"))
        first = reply_drafter._first_name(contact_name)
        email = feishu.clean_email(ext(cf.get("邮箱")))[0] or ""
        if not email:
            skipped += 1
            continue

        # 模板含 "the {product_name}" → 默认用 "product"(读作 "the product"), 不用 "the product"(会 "the the product")
        product_name = "product"
        prod_rid = xrid(f.get("关联产品"))
        if prod_rid:
            try:
                prod = await feishu.get_record(config.T_PRODUCT, prod_rid)
                product_name = ext(prod["fields"].get("产品英文名")) or ext(prod["fields"].get("产品名")) or "product"
            except Exception:
                pass

        sender_alias = ext(f.get("发送邮箱"))
        brand = config.brand_from_text(sender_alias) or "FUNLAB"  # 2026-06-26 修白牌错标
        sig = reply_drafter._sender_signature(brand)

        body = reply_drafter.TEMPLATE_SOFT_NUDGE.format(
            first_name=first, product_name=product_name, signature=sig,
        )
        original_subject = ext(f.get("邮件主题"))

        link_field = "关联媒体人" if contact_type == "editor" else "关联KOL"
        new_fields = {
            "邮件草稿ID": f"nudge-{contact_rid[-8:]}-{int(time.time())}",
            link_field: [contact_rid],
            "邮件主题": ("Re: " + original_subject)[:200],
            "邮件正文": body,
            "邮件语言": "en",
            "邮件草稿状态": "待审",
            "邮件草稿来源": "followup",
            "对象类型": "媒体人" if contact_type == "editor" else "KOL",
            "发送邮箱": sender_alias,
            "发送人署名": "Frankie",
            "生成时间": now_ms,
            "建议发送时间": now_ms,
            "重生次数": 0,
            "收件邮箱": email,
            "命中关键词": f"soft-nudge (sla +{SLA_DAYS_SOFT_NUDGE}d 暖信后无上稿)",
        }
        if prod_rid:
            new_fields["关联产品"] = [prod_rid]
        task_rid = xrid(f.get("关联任务"))
        if task_rid:
            new_fields["关联任务"] = [task_rid]

        # 先在 ship 草稿打 [NUDGE-SENT] 防重 (即便 create/route 失败也不下轮重发 → 防洪水)
        try:
            new_note = (note + f" [NUDGE-SENT@{int(time.time())}]")[:500]
            await feishu.update_record(config.T_DRAFT, rid, {"审批意见": new_note})
        except Exception as e:
            print(f"[sla_check nudge] mark fail rid={rid}: {e}")

        try:
            new_rid = await feishu.create_record(config.T_DRAFT, new_fields)
            print(f"[sla_check nudge] draft created rid={new_rid} for {contact_name}")
            try:
                await draft_router.route_draft(
                    new_rid, force_review_reason=f"soft_nudge +{SLA_DAYS_SOFT_NUDGE}d 暖信后无上稿软关怀")
            except Exception as e:
                print(f"[sla_check nudge] router fail rid={new_rid}: {e}")
            triggered += 1
        except Exception as e:
            print(f"[sla_check nudge] create fail: {e}")

    return {"layer": "nudge", "checked": len(items), "triggered": triggered,
            "skipped": skipped, "not_yet": not_yet}


async def run(now_ms: int = None) -> dict:
    """SLA layers. Production omits now_ms; tests may pin it for deterministic routing."""
    now_ms = int(now_ms if now_ms is not None else time.time() * 1000)
    results = {}
    # 2026-05-22: L1c(自动签收) + L2(content reminder 催稿) 暂时下线 — 寄样后流程重设计中.
    # 催稿放错了位置: 正确应是 已签收 → "确认收到 + brief recap" 暖信 (合一, 含卖点/追踪链接/
    # 优惠码/#ad/建议角度, 过人审), 真催稿降级成更晚更软的关怀. 见 memory kol-ship-flow-redesign.
    # A(auto_send 发出即推进已发货) + C(ship_recon 对账) 保留; L1/L3/L4 继续正常跑.
    # P4 软关怀 nudge (2026-05-29): 暖信(P3)发出 +12d 无上稿 → 软关怀(非催稿), 在 L3 30d 之前.
    for layer_fn in (_layer1_review_overdue, _layer_soft_nudge,
                      _layer3_no_content_30d, _layer4_low_roi_60d):
        try:
            r = await layer_fn(now_ms)
            results[f"layer_{r['layer']}"] = r
        except Exception as e:
            import traceback
            results[f"layer_error_{layer_fn.__name__}"] = {
                "error": str(e)[:200], "trace": traceback.format_exc()[-500:]
            }
    return results

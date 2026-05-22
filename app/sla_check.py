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
import re, time
from . import config, feishu, draft_router, reply_drafter
from .feishu import ext, xrid


SLA_HOURS_REVIEW = 24            # 层 1: ship_confirm 待审超时
SLA_DAYS_CONTENT_REMINDER = 7    # 层 2: 签收后 7 天内容提醒
SLA_DAYS_NO_CONTENT = 30         # 层 3: 30 天无内容产出软标
SLA_DAYS_LOW_ROI = 60            # 层 4: 60 天累计订单<3 软标
LOW_ROI_ORDER_THRESHOLD = 3

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


# ===== 层 1: 所有待审 24h 升级 (2026-05-15 扩大: 不再只扫寄样类) =====
async def _layer1_review_overdue(now_ms: int) -> dict:
    """
    扫所有「邮件草稿状态=待审」且生成 ≥24h 的草稿.
    旧版只扫「寄样阶段=待发货」→ reply 类待审 (千万粉丝 KOL 回复) 永远兜不到.
    新版按草稿类型分支:
      - 寄样类 (寄样阶段=待发货): 用 _build_ship_confirm_card escalation=True + ship_confirm_targets
      - 非寄样 (reply/cold/followup): 用通用红色超时升级卡 + 独立站运营专员
    """
    cutoff_ms = now_ms - SLA_HOURS_REVIEW * 3600 * 1000

    # 2026-05-17 A4: 加 field_names 减 payload (47 字段 → 13 字段)
    items = await feishu.search_records(config.T_DRAFT, [
        {"field_name": "邮件草稿状态", "operator": "is", "value": ["待审"]},
    ], field_names=[
        "邮件主题", "邮件草稿来源", "对象类型", "AI评分", "AI评分理由",
        "生成时间", "寄样阶段", "国家/地区", "收件地址 full", "关联产品",
        "审批意见", "SLA已升级",
    ])

    escalated = 0
    skipped = 0
    not_yet = 0
    base_url = f"https://u1wpma3xuhr.feishu.cn/base/{config.FEISHU_APP_TOKEN}?table={config.T_DRAFT}"

    for rec in items:
        f = rec["fields"]
        rid = rec["record_id"]

        gen_time = int(f.get("生成时间") or 0)
        if gen_time > cutoff_ms:
            not_yet += 1
            continue

        if await _is_already_escalated(rec):
            skipped += 1
            continue

        is_ship_confirm = ext(f.get("寄样阶段")) == "待发货"

        if is_ship_confirm:
            meta = {
                "country": ext(f.get("国家/地区")),
                "address": ext(f.get("收件地址 full")),
                "product_name": "the product",
            }
            prod_rid = xrid(f.get("关联产品"))
            if prod_rid:
                try:
                    prod = await feishu.get_record(config.T_PRODUCT, prod_rid)
                    meta["product_name"] = ext(prod["fields"].get("产品英文名")) or ext(prod["fields"].get("产品名")) or "the product"
                except Exception:
                    pass

            score = int(f.get("AI评分") or 0)
            summary = (ext(f.get("AI评分理由")) or "(无)")[:100]
            card = draft_router._build_ship_confirm_card(rid, rec, score, summary, meta, base_url, escalation=True)
            main, cc = await draft_router._ship_confirm_targets()
            personal_targets = main + cc
        else:
            # 非寄样类待审 24h+ → 通用红色超时升级卡
            source = ext(f.get("邮件草稿来源")) or "cold"
            contact_type = ext(f.get("对象类型")) or "KOL"
            subject = ext(f.get("邮件主题"))[:100]
            score = int(f.get("AI评分") or 0)
            summary = (ext(f.get("AI评分理由")) or "(无)")[:200]
            age_hours = int((now_ms - gen_time) / 3600 / 1000) if gen_time else 24
            card = {
                "header": {
                    "template": "red",
                    "title": {"tag": "plain_text",
                              "content": f"🚨 [SLA 超时] {source} 草稿待审 {age_hours}h — {contact_type}"},
                },
                "elements": [
                    {"tag": "div", "fields": [
                        {"is_short": True, "text": {"tag": "lark_md", "content": f"**AI 评分**: {score}/10"}},
                        {"is_short": True, "text": {"tag": "lark_md", "content": f"**已等待**: {age_hours} 小时"}},
                    ]},
                    {"tag": "div", "text": {"tag": "lark_md", "content": f"**主题**: {subject}"}},
                    {"tag": "div", "text": {"tag": "lark_md", "content": f"**评分总评**: {summary}"}},
                    {"tag": "hr"},
                    {"tag": "div", "text": {"tag": "lark_md",
                        "content": "**请立即审核**: 大 KOL 回复或商务承诺类草稿超时未审会流失转化机会"}},
                    {"tag": "action", "actions": [
                        {"tag": "button", "text": {"tag": "plain_text", "content": "打开KOL·媒体人邮件草稿"},
                         "url": base_url, "type": "primary"},
                    ]},
                ],
            }
            # 2026-05-17 A9: 改用 feishu.resolve_notify_targets helper
            personal_targets = await feishu.resolve_notify_targets("reviewer")

        success = 0
        fail = 0
        errors = []
        group_msg_id = ""
        try:
            group_msg_id = await feishu.send_card_message("chat_id", config.NOTIFY_CHAT_ID, card)
            success += 1
        except Exception as e:
            fail += 1
            errors.append(f"群: {str(e)[:80]}")
            print(f"[sla_check L1] notify chat fail: {e}")
        for name, oid in personal_targets:
            try:
                await feishu.send_card_message("open_id", oid, card)
                success += 1
            except Exception as e:
                fail += 1
                errors.append(f"{name}: {str(e)[:80]}")
                print(f"[sla_check L1] notify {name} fail: {e}")
        await feishu.mark_card_receipt(rid, success, fail, errors, group_msg_id=group_msg_id)

        try:
            await _mark_escalated(rid)
        except Exception as e:
            print(f"[sla_check L1] mark fail: {e}")
        escalated += 1

    return {"layer": 1, "checked": len(items), "escalated": escalated,
            "skipped": skipped, "not_yet": not_yet}


# ===== 层 1b: tracking_followup 第 2 封"待修改" 24h 兜底 (2026-05-21 P0-B) =====
async def _layer1b_tracking_followup_overdue(now_ms: int) -> dict:
    """24h 兜底: 第 1 封 ship_confirm 发出后, auto_send 自动建第 2 条 tracking_followup
    草稿状态=待修改 等运营回填运单号. 如果运营不回表 24h, 这条永远不发,
    KOL 拿不到运单号 → 黑洞. L1 只扫"待审"扫不到"待修改", 这层补.

    5/14 thunderstashgaming/Thao 大 KOL 之前就是踩了这个黑洞 (虽然运营最终手填了,
    但没有兜底 push 提醒, 完全靠运营记忆力). 现加这层主动 push."""
    cutoff_ms = now_ms - SLA_HOURS_REVIEW * 3600 * 1000  # 24h

    items = await feishu.search_records(config.T_DRAFT, [
        {"field_name": "邮件草稿状态", "operator": "is", "value": ["待修改"]},
        {"field_name": "邮件草稿来源", "operator": "is", "value": ["tracking_followup"]},
    ], field_names=["邮件主题", "生成时间", "对象类型",
                    "关联KOL", "关联媒体人", "审批意见"])

    pushed, skipped, not_yet = 0, 0, 0
    base_url = f"https://u1wpma3xuhr.feishu.cn/base/{config.FEISHU_APP_TOKEN}?table={config.T_DRAFT}"

    for rec in items:
        f = rec["fields"]
        rid = rec["record_id"]
        gen_time = int(f.get("生成时间") or 0)
        if gen_time > cutoff_ms:
            not_yet += 1; continue

        note = ext(f.get("审批意见")) or ""
        if "[TRACK-FOLLOWUP-PUSH]" in note:
            skipped += 1; continue

        age_hours = int((now_ms - gen_time) / 3600 / 1000) if gen_time else 24
        subject = ext(f.get("邮件主题"))[:100]

        card = {
            "header": {"template": "orange",
                "title": {"tag": "plain_text",
                    "content": f"📦 [兜底提醒] 第 2 封运单号待填 {age_hours}h"}},
            "elements": [
                {"tag": "div", "text": {"tag": "lark_md",
                    "content": f"**KOL 已收到第 1 封寄样确认邮件**, 系统已建好第 2 条 tracking_followup "
                               f"跟进草稿, 等运营回填运单号 — 已等 **{age_hours} 小时**.\n\n"
                               f"请打开草稿表, 在「运单号」「物流商」字段填值, 把「邮件草稿状态」改为 **通过** → 自动发出.\n\n"
                               f"⚠️ 不要手动改邮件正文字段 (5/15 已有错位事故)"}},
                {"tag": "div", "text": {"tag": "lark_md", "content": f"**主题**: {subject}"}},
                {"tag": "action", "actions": [
                    {"tag": "button", "text": {"tag": "plain_text", "content": "打开草稿表填运单号"},
                     "url": base_url, "type": "primary"},
                ]},
            ],
        }
        personal_targets = await feishu.resolve_notify_targets("ship_confirm")

        success, fail, errors, group_msg_id = 0, 0, [], ""
        try:
            group_msg_id = await feishu.send_card_message("chat_id", config.NOTIFY_CHAT_ID, card)
            success += 1
        except Exception as e:
            fail += 1; errors.append(f"群: {str(e)[:80]}")
            print(f"[sla_check L1b] notify chat fail: {e}")
        for name, oid in personal_targets:
            try:
                await feishu.send_card_message("open_id", oid, card)
                success += 1
            except Exception as e:
                fail += 1; errors.append(f"{name}: {str(e)[:80]}")
                print(f"[sla_check L1b] notify {name} fail: {e}")
        try:
            await feishu.mark_card_receipt(rid, success, fail, errors, group_msg_id=group_msg_id)
        except Exception:
            pass

        try:
            new_note = (note + f" [TRACK-FOLLOWUP-PUSH@{int(time.time())}]")[:500]
            await feishu.update_record(config.T_DRAFT, rid, {"审批意见": new_note})
        except Exception as e:
            print(f"[sla_check L1b] mark fail: {e}")

        pushed += 1

    return {"layer": "1b", "checked": len(items), "pushed": pushed,
            "skipped": skipped, "not_yet": not_yet}


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
        brand = "POWKONG" if "powkong" in sender_alias.lower() else "FUNLAB"
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

        # Phase 2 daemon 已扫到上稿就跳过 (主表「上稿日期」非空)
        upload_date = cf.get("上稿日期")
        if upload_date:
            skipped += 1
            continue

        contact_name = ext(cf.get("媒体人姓名")) if contact_type == "editor" else ext(cf.get("账号名"))

        # 草稿打标
        try:
            await feishu.update_record(config.T_DRAFT, rid, {"内容产出30d标记": True})
        except Exception as e:
            print(f"[sla_check L3] mark draft fail: {e}")

        # 主表合作状态软标 → "未产出" (如未存在该选项需运营在飞书加; 防 API 失败用 try)
        try:
            await feishu.update_record(target_table, contact_rid, {"合作状态": "未产出"})
        except Exception as e:
            print(f"[sla_check L3] master 合作状态 fail (option 可能未建): {e}")

        # 飞书卡片告警
        try:
            base_url = f"https://u1wpma3xuhr.feishu.cn/base/{config.FEISHU_APP_TOKEN}?table={target_table}"
            card = {
                "header": {"template": "orange",
                           "title": {"tag": "plain_text", "content": f"📭 寄样 30 天无内容产出 — {contact_name}"}},
                "elements": [
                    {"tag": "div", "text": {"tag": "lark_md",
                        "content": (f"**对象**: {contact_name} ({contact_type})\n"
                                    f"**寄样订单**: {ext(f.get('寄样订单号'))}\n"
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
            card = {
                "header": {"template": "yellow",
                           "title": {"tag": "plain_text", "content": f"📉 寄样 60 天累计订单<3 — {contact_name}"}},
                "elements": [
                    {"tag": "div", "text": {"tag": "lark_md",
                        "content": (f"**对象**: {contact_name} ({contact_type})\n"
                                    f"**寄样订单**: {ext(f.get('寄样订单号'))}\n"
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


async def run() -> dict:
    """4 层 SLA 全跑一遍 (n8n cron 每日 09:30 BJ 调用)"""
    now_ms = int(time.time() * 1000)
    results = {}
    # 2026-05-22: L1c(自动签收) + L2(content reminder 催稿) 暂时下线 — 寄样后流程重设计中.
    # 催稿放错了位置: 正确应是 已签收 → "确认收到 + brief recap" 暖信 (合一, 含卖点/追踪链接/
    # 优惠码/#ad/建议角度, 过人审), 真催稿降级成更晚更软的关怀. 见 memory kol-ship-flow-redesign.
    # A(auto_send 发出即推进已发货) + C(ship_recon 对账) 保留; L1/L1b/L3/L4 继续正常跑.
    for layer_fn in (_layer1_review_overdue, _layer1b_tracking_followup_overdue,
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

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


async def _is_already_escalated(rec: dict) -> bool:
    note = ext(rec["fields"].get("审批意见")) or ""
    return "[SLA-ESCALATED]" in note


async def _mark_escalated(record_id: str):
    rec = await feishu.get_record(config.T_DRAFT, record_id)
    note = ext(rec["fields"].get("审批意见")) or ""
    new_note = (note + f" [SLA-ESCALATED@{int(time.time())}]")[:500]
    await feishu.update_record(config.T_DRAFT, record_id, {"审批意见": new_note})


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

    items = await feishu.search_records(config.T_DRAFT, [
        {"field_name": "邮件草稿状态", "operator": "is", "value": ["待审"]},
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
            reviewers = await feishu.fetch_users_by_job_title(config.KOL_REVIEWER_JOB_TITLE)
            frankie_cc = [u for u in config.NOTIFY_USERS if u[0].startswith("潘")]
            seen = set()
            personal_targets = []
            for name, oid in reviewers + frankie_cc:
                if oid in seen:
                    continue
                seen.add(oid)
                personal_targets.append((name, oid))
            if not reviewers:
                print(f"[sla_check L1] WARN: 0 reviewers from job_title, fallback NOTIFY_USERS")
                personal_targets = config.NOTIFY_USERS

        success = 0
        fail = 0
        errors = []
        try:
            await feishu.send_card_message("chat_id", config.NOTIFY_CHAT_ID, card)
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
        await feishu.mark_card_receipt(rid, success, fail, errors)

        try:
            await _mark_escalated(rid)
        except Exception as e:
            print(f"[sla_check L1] mark fail: {e}")
        escalated += 1

    return {"layer": 1, "checked": len(items), "escalated": escalated,
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


async def run() -> dict:
    """4 层 SLA 全跑一遍 (n8n cron 每日 09:30 BJ 调用)"""
    now_ms = int(time.time() * 1000)
    results = {}
    for layer_fn in (_layer1_review_overdue, _layer2_content_reminder,
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

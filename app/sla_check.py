"""SLA 超时升级 — 扫 24h 未处理的寄样草稿,升级通知

判定:
- 草稿状态 = 待审
- 草稿来源 = reply
- 命中关键词 含 "ship-sample" (reply_drafter 写入的 ship_confirm 标志)
- 生成时间 < now - 24h

命中 → 重发飞书卡片 (标题加 🚨 SLA 超时), 同时 ping 全员 (主审 + CC)
"""
import re, time
from . import config, feishu, draft_router
from .feishu import ext, xrid


SLA_HOURS = 24


async def _is_already_escalated(rec: dict) -> bool:
    """检查是否已升级过 (避免重复 ping). 用「审批意见」字段含 [SLA-ESCALATED] 标记"""
    note = ext(rec["fields"].get("审批意见")) or ""
    return "[SLA-ESCALATED]" in note


async def _mark_escalated(record_id: str):
    """标记已升级"""
    rec = await feishu.get_record(config.T_DRAFT, record_id)
    note = ext(rec["fields"].get("审批意见")) or ""
    new_note = (note + f" [SLA-ESCALATED@{int(time.time())}]")[:500]
    await feishu.update_record(config.T_DRAFT, record_id, {"审批意见": new_note})


def _parse_address_from_highlight(highlight: str) -> dict:
    """从「匹配亮点」字段反解 ship_confirm meta
    格式: '[ship_confirm] country=US | address=Scott Stein\\n100 Montclair...'
    """
    m_country = re.search(r"country=([A-Z]{2,5})", highlight or "")
    m_addr = re.search(r"address=(.+)$", highlight or "", re.S)
    return {
        "country": (m_country.group(1) if m_country else "").strip(),
        "address": (m_addr.group(1) if m_addr else "").strip(),
    }


async def run() -> dict:
    """扫 SLA 超时草稿, 重发升级卡片"""
    cutoff_ms = int((time.time() - SLA_HOURS * 3600) * 1000)

    # 拉所有 草稿状态=待审 + 草稿来源=reply 的草稿
    items = await feishu.search_records(config.T_DRAFT, [
        {"field_name": "草稿状态", "operator": "is", "value": ["待审"]},
        {"field_name": "草稿来源", "operator": "is", "value": ["reply"]},
    ])

    escalated = 0
    skipped = 0
    not_yet = 0
    details = []

    for rec in items:
        f = rec["fields"]
        rid = rec["record_id"]

        # 仅处理 ship_confirm (命中关键词含 ship-sample)
        kw_hit = ext(f.get("命中关键词")) or ""
        if "ship-sample" not in kw_hit:
            continue

        gen_time = f.get("生成时间") or 0
        try:
            gen_time = int(gen_time)
        except (ValueError, TypeError):
            gen_time = 0
        if gen_time > cutoff_ms:
            not_yet += 1
            continue

        # 是否已升级过
        if await _is_already_escalated(rec):
            skipped += 1
            details.append({"rid": rid, "skipped": "already escalated"})
            continue

        # 重新构建卡片 (escalation=True)
        highlight = ext(f.get("匹配亮点")) or ""
        addr_meta = _parse_address_from_highlight(highlight)
        product_link_field = f.get("关联产品") or []
        prod_rid = xrid(product_link_field)
        product_name = ""
        if prod_rid:
            try:
                prod = await feishu.get_record(config.T_PRODUCT, prod_rid)
                product_name = ext(prod["fields"].get("产品名"))
            except Exception:
                pass

        meta = {
            "country": addr_meta["country"],
            "address": addr_meta["address"],
            "product_name": product_name or "the product",
        }

        score = int(f.get("AI评分") or 0)
        summary = ext(f.get("AI评分理由"))[:100] or "(无)"
        base_url = f"https://u1wpma3xuhr.feishu.cn/base/{config.FEISHU_APP_TOKEN}?table={config.T_DRAFT}"

        card = draft_router._build_ship_confirm_card(
            rid, rec, score, summary, meta, base_url, escalation=True,
        )

        # 全员通知 (主审 + CC 都收到, 升级提醒)
        main, cc = draft_router._ship_confirm_targets()
        all_targets = main + cc

        # 群也通知
        try:
            await feishu.send_card_message("chat_id", config.NOTIFY_CHAT_ID, card)
        except Exception as e:
            print(f"[sla_check] notify chat fail: {e}")
        for name, oid in all_targets:
            try:
                await feishu.send_card_message("open_id", oid, card)
            except Exception as e:
                print(f"[sla_check] notify {name} fail: {e}")

        # 标记已升级 (避免下次 cron 又重发)
        try:
            await _mark_escalated(rid)
        except Exception as e:
            print(f"[sla_check] mark escalated fail: {e}")

        escalated += 1
        details.append({"rid": rid, "escalated": True, "country": meta["country"]})

    return {
        "checked_total": len(items),
        "escalated": escalated,
        "skipped_already_escalated": skipped,
        "not_yet_24h": not_yet,
        "details": details[:10],
    }

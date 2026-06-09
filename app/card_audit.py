# -*- coding: utf-8 -*-
"""每日卡片审计 cron — 扫 >24h 未处理的 KOL 卡片汇总提醒.

为何要: 看板视图能查待办, 但运营不一定主动开看板. 每天 09:30 BJ 一张汇总卡
推到 reviewer 私聊, 列前 10 张, 每行附「📨 重发」链接(点了原卡到运营私聊底部).

逻辑:
- 扫草稿表「邮件草稿状态」∈{待审, 待修改} AND「生成时间」> 24h 前
- 每张草稿额外解析: KOL名/平台/粉丝/当前阶段(reply_monitor._contact_stage_label) + 产品
- 给每个在职 reviewer 都发汇总(老草稿没「关联运营」字段, 不能精准分组)
- 每行末尾 [📨 重发] 链接 → /card/resend-from-button → 卡片回运营私聊底部
- 抄送 Frankie 一张总数汇总卡

支持 ?dry_run=true 看会汇总几张, ?days=N 调阈值(默认 1=24h).
不发邮件; 仅 IM 卡片提醒.
"""
import os
import time
from . import config, feishu
from .feishu import ext


PENDING_STATUSES = ("待审", "待修改")  # 「通过」走 auto-send 自动发, 不算 overdue


async def run(days: float = 1.0, dry_run: bool = False, max_list: int = 10) -> dict:
    started = time.time()
    cutoff_ms = int(started * 1000 - days * 86400 * 1000)
    overdue = []
    errors = []

    for status in PENDING_STATUSES:
        try:
            items = await feishu.search_records(config.T_DRAFT, [
                {"field_name": "邮件草稿状态", "operator": "is", "value": [status]}
            ])
        except Exception as e:
            errors.append(f"search {status}: {str(e)[:100]}")
            continue
        for it in items:
            f = it.get("fields", {})
            gen_t = f.get("生成时间")
            gen_ms = 0
            if isinstance(gen_t, (int, float)):
                gen_ms = int(gen_t)
            elif isinstance(gen_t, str):
                try: gen_ms = int(gen_t)
                except (ValueError, TypeError): gen_ms = 0
            if not gen_ms or gen_ms >= cutoff_ms:
                continue
            # 解析 KOL/媒体人 + 平台 + 粉丝 + 当前阶段 (复用 reply_monitor._contact_stage_label)
            kol_name, platform, fans, stage = "", "", "", ""
            crid = feishu.xrid(f.get("关联KOL"))
            is_editor = False
            if not crid:
                crid = feishu.xrid(f.get("关联媒体人"))
                is_editor = bool(crid)
            if crid:
                try:
                    cf = (await feishu.get_record(
                        config.T_EDITOR if is_editor else config.T_KOL, crid))["fields"]
                    from . import reply_monitor  # 惰性 import 防循环
                    stage = reply_monitor._contact_stage_label(cf) or ""
                    if is_editor:
                        kol_name = ext(cf.get("媒体人姓名")) or ""
                        platform = ext(cf.get("主要媒体")) or ext(cf.get("所属媒体")) or ""
                    else:
                        kol_name = ext(cf.get("账号名")) or ""
                        platform = ext(cf.get("主平台")) or ""
                        try:
                            fans = f"{int(cf.get('粉丝数') or 0):,}"
                        except (ValueError, TypeError):
                            fans = str(cf.get("粉丝数") or "")
                except Exception:
                    pass

            # 解析产品名
            prod_name = ""
            prid = feishu.xrid(f.get("关联产品"))
            if prid:
                try:
                    pf = (await feishu.get_record(config.T_PRODUCT, prid))["fields"]
                    prod_name = ext(pf.get("产品英文名")) or ext(pf.get("产品名")) or ""
                except Exception:
                    pass

            # 品牌 + 收件邮箱 (2026-05-31 统一字段补充)
            sender = ext(f.get("发送邮箱")) or ""
            brand = config.brand_from_text(sender) or "FUNLAB"   # 2026-06-09 配置驱动(支持白牌)
            email = ext(f.get("收件邮箱")) or ""
            overdue.append({
                "rid": it["record_id"],
                "source": ext(f.get("邮件草稿来源")) or "",
                "subject": ext(f.get("邮件主题")) or "",
                "kol_name": (kol_name or "?")[:30],
                "platform": platform[:20],
                "fans": fans,
                "stage": stage,
                "product": prod_name[:40],
                "brand": brand,
                "email": email[:50],
                "status": status,
                "days_overdue": int((started * 1000 - gen_ms) / 86400000),
            })

    overdue.sort(key=lambda x: -x["days_overdue"])
    summary = {
        "ok": True, "overdue_count": len(overdue),
        "by_status": {s: sum(1 for o in overdue if o["status"] == s) for s in PENDING_STATUSES},
        "errors": errors,
        "elapsed_s": round(time.time() - started, 1),
    }

    if dry_run:
        summary["dry_run"] = True
        summary["sample"] = overdue[:5]
        return summary

    # 发卡
    try:
        reviewers = await feishu.resolve_notify_targets("reviewer")
    except Exception as e:
        summary["ok"] = False
        summary["notify_err"] = str(e)[:200]
        return summary

    sent = 0
    fail = 0
    for name, oid in reviewers:
        try:
            uid = await feishu.open_id_to_union_id(oid)
            if not uid:
                continue
            card = _build_audit_card(name, overdue, max_list)
            await feishu.send_card_via_app3("union_id", uid, card)
            sent += 1
        except Exception as e:
            fail += 1
            print(f"[card_audit] notify {name} fail: {e}")

    # 抄送 Frankie 汇总 (聪哥1号 namespace open_id, 走 send_card_message)
    try:
        fcard = _build_summary_card(overdue, sent, len(reviewers))
        await feishu.send_card_message(
            "open_id", "ou_629ce01f4bc31de078e10fcb038dbf78", fcard, biz="KOL", level="P2")
    except Exception as e:
        print(f"[card_audit] notify Frankie fail: {e}")

    summary["reviewers_total"] = len(reviewers)
    summary["reviewers_sent"] = sent
    summary["reviewers_fail"] = fail
    return summary


def _format_overdue_line(idx: int, o: dict, secret: str) -> str:
    """每行渲染(2026-05-31 标签明显化): KOL名/阶段/平台/粉丝 + 产品/品牌/收件 + 类型/等X天/状态 + [📨 重发]"""
    # 行 1: KOL 名 + 阶段 + 平台 粉丝
    head_parts = [f"**{o['kol_name']}**"]
    if o.get("stage"):
        head_parts.append(o["stage"])
    if o.get("platform"):
        plat = o["platform"]
        if o.get("fans"):
            plat += f" {o['fans']}"
        head_parts.append(plat)
    head = " · ".join(head_parts)

    # 行 2: 产品 + 品牌 (标签明显化, Frankie 反馈)
    line2_parts = []
    if o.get("product"):
        line2_parts.append(f"**产品**: {o['product']}")
    if o.get("brand"):
        line2_parts.append(f"**品牌**: {o['brand']}")
    line2 = " · ".join(line2_parts)

    # 行 3: 收件人 + 任务类型 + 等待天数 + 状态
    line3 = f"**收件**: {o.get('email') or '?'} · {o['source']} · 等 {o['days_overdue']}d · {o['status']}"

    sub = (o["subject"] or "")
    if len(sub) > 60:
        sub = sub[:60] + "..."

    resend_url = (f"https://kol-auto.zeabur.app/card/resend-from-button"
                  f"?draft_rid={o['rid']}&secret={secret}")
    lines = [f"{idx}. {head}"]
    if line2:
        lines.append(f"   {line2}")
    lines.append(f"   {line3}")
    lines.append(f"   _{sub}_")
    lines.append(f"   [📨 重发卡片到运营私聊]({resend_url})")
    return "\n".join(lines)


def _build_audit_card(reviewer_name: str, overdue: list, max_list: int = 10) -> dict:
    base_url = (f"https://u1wpma3xuhr.feishu.cn/base/{config.FEISHU_APP_TOKEN}"
                f"?table={config.T_DRAFT}")
    secret = os.environ.get("RESEND_BUTTON_SECRET", "")
    n = len(overdue)
    elements = [
        {"tag": "div", "text": {"tag": "lark_md", "content": (
            f"**Hi {reviewer_name}**, 当前 KOL 卡片任务表里有 **{n} 张** 草稿 >24h 还没处理。\n"
            "顺手清一下避免 KOL 卡死(尤其 thunderstash/Thao 类大粉丝事故)。"
        )}},
        {"tag": "hr"},
    ]
    if not overdue:
        elements.append({"tag": "div", "text": {"tag": "lark_md",
            "content": "🎉 没有待办！干净。"}})
    else:
        lines = [_format_overdue_line(i, o, secret) for i, o in enumerate(overdue[:max_list], 1)]
        elements.append({"tag": "div", "text": {"tag": "lark_md",
            "content": "\n\n".join(lines)}})
        if n > max_list:
            elements.append({"tag": "note", "elements": [{"tag": "plain_text",
                "content": f"...还有 {n - max_list} 张, 看板里看完整列表"}]})
    elements.append({"tag": "action", "actions": [
        {"tag": "button", "text": {"tag": "plain_text", "content": "📋 打开 KOL 卡片任务看板"},
         "url": base_url, "type": "primary"},
    ]})
    template = "red" if len(overdue) >= 30 else ("orange" if overdue else "green")
    return {
        "config": {"wide_screen_mode": True},
        "header": {"template": template,
                   "title": {"tag": "plain_text",
                             "content": f"⏰ KOL 卡片任务审计 · {time.strftime('%m-%d')}"}},
        "elements": elements,
    }


def _build_summary_card(overdue: list, sent: int, total: int) -> dict:
    base_url = (f"https://u1wpma3xuhr.feishu.cn/base/{config.FEISHU_APP_TOKEN}"
                f"?table={config.T_DRAFT}")
    return {
        "config": {"wide_screen_mode": True},
        "header": {"template": "blue",
                   "title": {"tag": "plain_text",
                             "content": f"🔭 KOL 卡片审计汇总 · {time.strftime('%m-%d')}"}},
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md", "content": (
                f"**全表 >24h 未处理草稿: {len(overdue)} 张**\n\n"
                f"- 已通知 reviewer: **{sent}/{total}** 人\n"
                f"- [📋 任务看板]({base_url})"
            )}},
        ]
    }

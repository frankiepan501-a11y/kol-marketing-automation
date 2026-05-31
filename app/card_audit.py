# -*- coding: utf-8 -*-
"""每日卡片审计 cron — 扫 >24h 未处理的 KOL 卡片汇总提醒.

为何要: 看板视图能查待办, 但运营不一定主动开看板. 每天 09:30 BJ 一张汇总卡
推到 reviewer 私聊, 列前 10 张 + 跳看板按钮, 兜底防漏处理.

逻辑:
- 扫草稿表「邮件草稿状态」∈{待审, 待修改} AND「生成时间」> 24h 前
- 按「关联运营」字段不精确分发(给每个在职 reviewer 都发汇总, 因老草稿没此字段)
- 抄送 Frankie 一张总数汇总卡

支持 ?dry_run=true 看会汇总几张, ?days=N 调阈值(默认 1=24h).
不发邮件; 仅 IM 卡片提醒.
"""
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
            kol_name = ""
            for rel_field in ("关联KOL", "关联媒体人"):
                rels = f.get(rel_field)
                if isinstance(rels, list) and rels:
                    first = rels[0]
                    if isinstance(first, dict):
                        kol_name = first.get("text") or first.get("name") or ""
                        if kol_name:
                            break
            overdue.append({
                "rid": it["record_id"],
                "source": ext(f.get("邮件草稿来源")) or "",
                "subject": ext(f.get("邮件主题")) or "",
                "kol_name": (kol_name or "?")[:30],
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


def _build_audit_card(reviewer_name: str, overdue: list, max_list: int = 10) -> dict:
    base_url = (f"https://u1wpma3xuhr.feishu.cn/base/{config.FEISHU_APP_TOKEN}"
                f"?table={config.T_DRAFT}")
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
        lines = []
        for i, o in enumerate(overdue[:max_list], 1):
            sub = (o["subject"] or "")[:50]
            if len(o["subject"]) > 50:
                sub += "..."
            lines.append(
                f"{i}. **{o['kol_name']}** · {o['source']} · 等 {o['days_overdue']}d · {o['status']}\n   _{sub}_"
            )
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

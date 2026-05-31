"""上稿/报道 登记卡 (2026-06-01) — 补成功事件数据 hygiene 缺口 (KOL 上稿 + 媒体人报道).

背景: 成功事件(KOL 上稿 / 媒体人报道发表)常空 → ROI/decision_feedback/完成审计对"已发布"全盲。
本模块给运营发 form 卡: 列「已寄样但成功事件空」的 KOL/媒体人, 运营看到对方发布就粘链接
→ event-hub draft_uploadreg(参数化, 从卡片 value 读字段名) → 写主表成功字段 + 合作状态。

KOL:   写 上稿日期 + 上稿链接 + 合作状态=已合作-免费 (媒体人=earned media 见 [[reference-media-relations-playbook]])
媒体人: 写 报道发表日期 + 报道链接 + 合作状态=已合作

低噪声: 只发 已寄样(上次寄样订单号非空) + 成功字段空 + (登记卡发送时间空 OR >14d), 每轮每类型 cap N 张,
按 上次寄样日期 最久优先。dedup 靠主表对应「登记卡发送时间」字段。纯写主表不发邮件 → 无 DRY-RUN 顾虑。
"""
import time
from . import config, feishu
from .feishu import ext

CARD_RESEND_DAYS = 14
PER_RUN_CAP = 8                # 每类型每轮最多 N 张

# 两类对象规格 (KOL=带货上稿 / 媒体人=earned media 报道)
SPECS = {
    "KOL": {
        "table": config.T_KOL, "ctype": "KOL", "name_field": "账号名",
        "dedup_field": "上稿登记卡发送时间",
        "date_field": "上稿日期", "link_field": "上稿链接", "coop_value": "已合作-免费",
        "reg_label": "上稿", "header": "🎬 上稿登记 — 这位达人发布了吗?",
        "input_label": "上稿链接:",
        "placeholder": "https://youtube.com/watch?v=... / tiktok.com/... / instagram.com/...",
        "desc": "如果你看到 TA 已发布内容(视频/帖/直播回放)，把链接粘下面提交 → 记上稿日期+链接, 并标「已合作-免费」(解锁 ROI 追踪)。",
        "btn": "✅ 登记上稿 (写上稿日期+链接+已合作)",
    },
    "媒体人": {
        "table": config.T_EDITOR, "ctype": "媒体人", "name_field": "媒体人姓名",
        "dedup_field": "报道登记卡发送时间",
        "date_field": "报道发表日期", "link_field": "报道链接", "coop_value": "已合作",
        "reg_label": "报道", "header": "📰 报道登记 — 这位媒体人发表了吗?",
        "input_label": "报道链接:",
        "placeholder": "https://ign.com/... / theverge.com/... / 媒体文章 URL",
        "desc": "如果你看到 TA 发表了关于我们产品的报道/评测，把链接粘下面提交 → 记报道发表日期+链接, 并标「已合作」(earned media 不带折扣码/GMV)。",
        "btn": "✅ 登记报道 (写报道发表日期+链接+已合作)",
    },
}


def _build_card(spec: dict, rid: str, contact_info: dict, email: str,
                sample_order: str, sample_date_str: str) -> dict:
    """登记 form 卡. action=draft_uploadreg 走 event-hub「Is Draft Action?」分支(handler 参数化).
    value 带 date_field/link_field/coop_value/reg_label → handler 据此写对应主表字段。"""
    base_val = {
        "action": "draft_uploadreg", "app_token": config.FEISHU_APP_TOKEN,
        "table_id": spec["table"], "record_id": rid,
        "date_field": spec["date_field"], "link_field": spec["link_field"],
        "coop_value": spec["coop_value"], "reg_label": spec["reg_label"],
    }
    return {
        "config": {"wide_screen_mode": True, "update_multi": True},
        "header": {"template": "blue", "title": {"tag": "plain_text", "content": spec["header"]}},
        "elements": [
            feishu.build_contact_info_block(
                contact_info=contact_info, product_name="", brand="",
                email=email, contact_type=spec["ctype"]),
            {"tag": "div", "text": {"tag": "lark_md",
                "content": f"📦 已寄样 (单号 {sample_order or '-'} · 寄于 {sample_date_str})，但主表**{spec['reg_label']}发表为空**。\n{spec['desc']}"}},
            {"tag": "hr"},
            {"tag": "form", "name": f"reg_{rid}", "elements": [
                {"tag": "input", "name": "upload_url", "label_position": "left",
                 "label": {"tag": "plain_text", "content": spec["input_label"]},
                 "placeholder": {"tag": "plain_text", "content": spec["placeholder"]}},
                {"tag": "button", "action_type": "form_submit", "name": "submit",
                 "text": {"tag": "plain_text", "content": spec["btn"]},
                 "type": "primary", "value": base_val},
            ]},
            {"tag": "div", "text": {"tag": "lark_md",
                "content": f"⚠️ 链接留空不写; 还没{spec['reg_label']}就忽略 (14 天后若仍未登记会再提醒一次)"}},
        ],
    }


async def _scan(spec: dict, now_ms: int) -> list:
    cutoff = now_ms - CARD_RESEND_DAYS * 86400 * 1000
    items = await feishu.search_records(
        spec["table"], [{"field_name": "上次寄样订单号", "operator": "isNotEmpty", "value": []}])
    cands = []
    for r in items:
        f = r["fields"]
        if f.get(spec["date_field"]):
            continue  # 已有成功事件
        last_card = f.get(spec["dedup_field"])
        try:
            if last_card and int(last_card) > cutoff:
                continue
        except (ValueError, TypeError):
            pass
        cands.append(r)
    cands.sort(key=lambda r: r["fields"].get("上次寄样日期") or 0)
    return cands[:PER_RUN_CAP]


async def run(dry_run: bool = False) -> dict:
    now_ms = int(time.time() * 1000)
    targets = await feishu.resolve_notify_targets("reviewer")
    unions = []
    for _nm, oid in targets:
        uid = await feishu.open_id_to_union_id(oid)
        if uid:
            unions.append(uid)

    result = {"dry_run": dry_run}
    for otype, spec in SPECS.items():
        cands = await _scan(spec, now_ms)
        if dry_run:
            preview = []
            for r in cands:
                f = r["fields"]
                try:
                    ci = await feishu.resolve_contact_info(r["record_id"], spec["ctype"])
                except Exception:
                    ci = {}
                _ = _build_card(spec, r["record_id"], ci, ext(f.get("邮箱")), ext(f.get("上次寄样订单号")), "?")
                preview.append({"rid": r["record_id"], "name": ext(f.get(spec["name_field"])),
                                "coop": ext(f.get("合作状态")), "sample_order": ext(f.get("上次寄样订单号"))})
            result[otype] = {"candidates": len(cands), "would_send": preview}
            continue
        if not unions:
            result[otype] = {"sent": 0, "candidates": len(cands), "error": "无可发卡运营"}
            continue
        sent = 0
        for r in cands:
            rid = r["record_id"]
            f = r["fields"]
            try:
                ci = await feishu.resolve_contact_info(rid, spec["ctype"])
            except Exception:
                ci = {}
            email = (feishu.clean_email(ext(f.get("邮箱")))[0] if ext(f.get("邮箱")) else "") or ""
            sd = f.get("上次寄样日期")
            sds = time.strftime("%Y-%m-%d", time.gmtime(int(sd) / 1000)) if sd else "?"
            card = _build_card(spec, rid, ci, email, ext(f.get("上次寄样订单号")), sds)
            ok_any = False
            for uid in unions:
                try:
                    if await feishu.send_card_via_app3("union_id", uid, card):
                        ok_any = True
                except Exception as e:
                    print(f"[upload_register] send fail {otype} rid={rid} uid={uid}: {e}")
            if ok_any:
                sent += 1
                try:
                    await feishu.update_record(spec["table"], rid, {spec["dedup_field"]: now_ms})
                except Exception as e:
                    print(f"[upload_register] 写发送时间失败 {otype} rid={rid}: {e}")
        result[otype] = {"sent": sent, "candidates": len(cands)}
    result["recipients"] = len(unions)
    return result

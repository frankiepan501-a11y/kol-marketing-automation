"""上稿登记卡 (2026-06-01) — 补「上稿日期」数据 hygiene 缺口.

背景: 上稿日期常空 (auto 捕获 reply_monitor.live_link_received 只在 KOL 主动邮件给链接时写;
Phase2 daemon 抓取式可能漏) → ROI(sales_attribution)/decision_feedback/late-stage 守护对"已发布"全盲
(TG_Geek 致盲根因之一)。本模块给运营发 form 卡: 列「已寄样但上稿日期空」的 KOL, 运营看到对方上稿
就粘链接 → event-hub draft_uploadreg → 写主表 上稿日期+上稿链接+合作状态=已合作-免费 → 解锁 ROI 反哺链.

低噪声: 只发 已寄样(上次寄样订单号非空) + 上稿日期空 + (上稿登记卡发送时间空 OR >14d), 每轮 cap N 张,
按 上次寄样日期 最久优先。dedup 靠主表「上稿登记卡发送时间」(fldkCL1L5x)。
卡纯写主表字段, 不发邮件 → 无 DRY-RUN 顾虑。
"""
import time
from . import config, feishu
from .feishu import ext, xrid

CARD_RESEND_DAYS = 14          # 同一 KOL 上稿登记卡 N 天内不重发
PER_RUN_CAP = 8                # 每轮最多发 N 张, 避免刷屏


def _build_upload_register_card(kol_rid: str, contact_info: dict, brand: str,
                                email: str, sample_order: str, sample_date_str: str) -> dict:
    """上稿登记 form 卡: 运营粘上稿链接 → event-hub draft_uploadreg 写主表."""
    # action 用 draft_ 前缀 → 走现有 event-hub「Is Draft Action?」分支 (handler 扩展处理),
    # table_id=T_KOL/record_id=kol_rid → handler 通用 putFields 直写主表.
    base_val = {"action": "draft_uploadreg", "app_token": config.FEISHU_APP_TOKEN,
                "table_id": config.T_KOL, "record_id": kol_rid}
    return {
        "config": {"wide_screen_mode": True, "update_multi": True},
        "header": {"template": "blue",
                   "title": {"tag": "plain_text", "content": "🎬 上稿登记 — 这位达人发布了吗?"}},
        "elements": [
            feishu.build_contact_info_block(
                contact_info=contact_info, product_name="", brand=brand,
                email=email, contact_type="KOL"),
            {"tag": "div", "text": {"tag": "lark_md",
                "content": f"📦 已寄样 (单号 {sample_order or '-'} · 寄于 {sample_date_str})，但主表**上稿日期为空**。\n"
                           f"如果你看到 TA 已发布内容(视频/帖/直播回放)，把链接粘下面提交 → 系统记上稿日期+链接, 并标「已合作-免费」(解锁 ROI 追踪)。"}},
            {"tag": "hr"},
            {"tag": "form", "name": f"upreg_{kol_rid}", "elements": [
                {"tag": "input", "name": "upload_url", "label_position": "left",
                 "label": {"tag": "plain_text", "content": "上稿链接:"},
                 "placeholder": {"tag": "plain_text", "content": "https://youtube.com/watch?v=... / tiktok.com/... / instagram.com/..."}},
                {"tag": "button", "action_type": "form_submit", "name": "submit",
                 "text": {"tag": "plain_text", "content": "✅ 登记上稿 (写上稿日期+链接+已合作)"},
                 "type": "primary", "value": base_val},
            ]},
            {"tag": "div", "text": {"tag": "lark_md",
                "content": "⚠️ 链接留空不写; 还没上稿就忽略 (14 天后若仍未登记会再提醒一次)"}},
        ],
    }


async def run(dry_run: bool = False) -> dict:
    """扫已寄样+上稿日期空+未近期发卡的 KOL → 发上稿登记卡 → 写发送时间去重.
    dry_run=True: 只列候选 + 试建卡(验无 build 错), 不发不写去重时间."""
    now_ms = int(time.time() * 1000)
    cutoff = now_ms - CARD_RESEND_DAYS * 86400 * 1000

    # 已寄样代理: 上次寄样订单号非空 (auto_send/reply_drafter ship_confirm 都写此字段)
    items = await feishu.search_records(
        config.T_KOL,
        [{"field_name": "上次寄样订单号", "operator": "isNotEmpty", "value": []}],
        field_names=["账号名", "合作状态", "上稿日期", "上次寄样订单号", "上次寄样日期",
                     "上稿登记卡发送时间", "发送邮箱", "邮箱", "主平台", "粉丝数"],
    )

    # 候选: 上稿日期空 + (上稿登记卡发送时间空 OR >14d 前)
    cands = []
    for r in items:
        f = r["fields"]
        if f.get("上稿日期"):
            continue
        last_card = f.get("上稿登记卡发送时间")
        try:
            if last_card and int(last_card) > cutoff:
                continue
        except (ValueError, TypeError):
            pass
        cands.append(r)

    # 最久寄样优先 (越早寄样越该有产出), cap PER_RUN_CAP
    cands.sort(key=lambda r: r["fields"].get("上次寄样日期") or 0)
    cands = cands[:PER_RUN_CAP]
    if not cands:
        return {"sent": 0, "candidates": 0, "msg": "无需登记的 KOL"}

    if dry_run:
        # 只列候选 + 试建一张卡验无 build 错, 不发不写
        preview = []
        for r in cands:
            f = r["fields"]
            try:
                ci = await feishu.resolve_contact_info(r["record_id"], "KOL")
            except Exception:
                ci = {}
            _ = _build_upload_register_card(r["record_id"], ci, "FUNLAB",
                                            ext(f.get("邮箱")), ext(f.get("上次寄样订单号")), "?")
            preview.append({"rid": r["record_id"], "name": ext(f.get("账号名")),
                            "coop": ext(f.get("合作状态")), "sample_order": ext(f.get("上次寄样订单号"))})
        return {"dry_run": True, "candidates": len(cands), "would_send": preview}

    # 收件人 (独立站运营专员, 职务实时查 → turnover-safe), 转 union_id
    targets = await feishu.resolve_notify_targets("reviewer")
    unions = []
    for _nm, oid in targets:
        uid = await feishu.open_id_to_union_id(oid)
        if uid:
            unions.append(uid)
    if not unions:
        return {"sent": 0, "candidates": len(cands), "error": "无可发卡运营 (resolve_notify_targets reviewer 空)"}

    sent = 0
    details = []
    for r in cands:
        rid = r["record_id"]
        f = r["fields"]
        try:
            ci = await feishu.resolve_contact_info(rid, "KOL")
        except Exception:
            ci = {}
        sender = ext(f.get("发送邮箱")) or ""
        brand = "POWKONG" if "powkong" in sender.lower() else "FUNLAB"
        email = (feishu.clean_email(ext(f.get("邮箱")))[0] if ext(f.get("邮箱")) else "") or ""
        sample_order = ext(f.get("上次寄样订单号"))
        sd = f.get("上次寄样日期")
        sample_date_str = time.strftime("%Y-%m-%d", time.gmtime(int(sd) / 1000)) if sd else "?"
        card = _build_upload_register_card(rid, ci, brand, email, sample_order, sample_date_str)
        ok_any = False
        for uid in unions:
            try:
                mid = await feishu.send_card_via_app3("union_id", uid, card)
                if mid:
                    ok_any = True
            except Exception as e:
                print(f"[upload_register] send fail rid={rid} uid={uid}: {e}")
        if ok_any:
            sent += 1
            try:
                await feishu.update_record(config.T_KOL, rid, {"上稿登记卡发送时间": now_ms})
            except Exception as e:
                print(f"[upload_register] 写发送时间失败 rid={rid}: {e}")
        details.append({"rid": rid, "name": ext(f.get("账号名")), "sent": ok_any})

    return {"sent": sent, "candidates": len(cands), "recipients": len(unions), "details": details}

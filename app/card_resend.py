# -*- coding: utf-8 -*-
"""卡片重发端点 - 看板"📨 回到飞书操作"按钮触发.

背景: 飞书 applink 不支持 openMessageId (官方文档+实测确认), 程序化拼 URL 跳到
特定消息位置不可行. 改走"撤老卡 + 重发卡到私聊底部"路径 — 重发的卡总是出现在
最底部 + 永远是最新状态(从 storage 重建).

流程:
  1. 拉草稿, 判终态(已发送/已否决/退回重生/自动通过 → 拒)
  2. 撤老卡 (DELETE /im/v1/messages/{老 msg_id}, best-effort 失败忽略)
  3. 按 source 重建对应卡 (warm_recap/ship_tracking/review_action), 复用现有 builders
  4. send_card_via_app3 重发给 operator → 拿新 msg_id
  5. 更新「卡片个人消息IDs」JSON + 「关联运营」User 字段

不发邮件; 仅 IM 卡片操作; fail-safe.
"""
import json as _json
from . import config, feishu
from .feishu import ext


TERMINAL_STATUSES = {"已发送", "已否决", "退回重生", "自动通过"}


async def _build_resend_card(draft_rid: str, rec: dict) -> dict:
    """按草稿 source/状态 重建对应卡. 复用 draft_router/warm_recap builders."""
    from . import draft_router, warm_recap, reply_monitor
    f = rec.get("fields", {})
    source = ext(f.get("邮件草稿来源")) or "cold"
    stage = ext(f.get("寄样阶段")) or ""
    contact_type = ext(f.get("对象类型")) or "KOL"
    subject = ext(f.get("邮件主题")) or ""
    base_url = f"https://u1wpma3xuhr.feishu.cn/base/{config.FEISHU_APP_TOKEN}?table={config.T_DRAFT}"

    # 解析联系人 + 产品
    prod_name = ""
    prid = feishu.xrid(f.get("关联产品"))
    if prid:
        try:
            pf = (await feishu.get_record(config.T_PRODUCT, prid))["fields"]
            prod_name = ext(pf.get("产品名")) or ext(pf.get("产品英文名")) or ""
        except Exception as e:
            print(f"[card_resend] product lookup fail: {e}")

    is_editor = bool(feishu.xrid(f.get("关联媒体人")))
    crid = feishu.xrid(f.get("关联媒体人")) if is_editor else feishu.xrid(f.get("关联KOL"))
    cf = {}
    kol_name = ""
    if crid:
        try:
            cf = (await feishu.get_record(
                config.T_EDITOR if is_editor else config.T_KOL, crid))["fields"]
            kol_name = ext(cf.get("媒体人姓名" if is_editor else "账号名")) or "?"
        except Exception as e:
            print(f"[card_resend] contact lookup fail: {e}")

    # 分发卡类型
    if source == "warm_recap":
        brief_md = ext(f.get("Per-KOL Brief")) or ""
        return warm_recap._build_warm_recap_card(draft_rid, kol_name, prod_name, subject, brief_md)

    if source == "ship_confirm" or stage == "待发货":
        recipient = ext(f.get("收件邮箱")) or contact_type
        return draft_router._build_ship_tracking_card(
            draft_rid, recipient, prod_name or "the sample", subject, "重发")

    if source == "tracking_followup":
        return draft_router._build_ship_tracking_card(
            draft_rid, kol_name or contact_type, prod_name or "the sample", subject, "运单号追加")

    # 默认 cold/reply/followup/nudge → review_action 卡
    try:
        score = int(f.get("AI评分") or 0)
    except (ValueError, TypeError):
        score = 0
    summary = ext(f.get("AI评分理由")) or ""
    reasons_text = (
        f"亮点: {ext(f.get('匹配亮点')) or '-'}\n"
        f"不足: {ext(f.get('匹配不足')) or '-'}\n"
        f"切入: {ext(f.get('建议切入点')) or '-'}"
    )
    path = ext(f.get("审核路径")) or "待人审"
    sender = ext(f.get("发送邮箱")) or ""
    brand = config.brand_from_text(sender) or "FUNLAB"  # 2026-06-26 修白牌错标

    contact_info = {}
    if cf:
        try:
            if is_editor:
                contact_info = {"name": kol_name,
                                "platform": ext(cf.get("主要媒体")) or ext(cf.get("所属媒体")) or "",
                                "fans": "", "stage": reply_monitor._contact_stage_label(cf)}
            else:
                try:
                    fans = f"{int(cf.get('粉丝数') or 0):,}"
                except (ValueError, TypeError):
                    fans = str(cf.get("粉丝数") or "")
                contact_info = {"name": kol_name,
                                "platform": ext(cf.get("主平台")) or "",
                                "fans": fans, "stage": reply_monitor._contact_stage_label(cf)}
        except Exception as e:
            print(f"[card_resend] contact_info build fail: {e}")

    return draft_router._build_review_action_card(
        draft_rid, rec, score, summary, reasons_text, path, source, contact_type,
        prod_name, brand, base_url, contact_info=contact_info)


async def run(draft_rid: str, operator_open_id: str = "",
              operator_union_id: str = "", dry_run: bool = False) -> dict:
    """重发卡片入口.

    Args:
        draft_rid: 草稿 record_id (必填)
        operator_open_id: 触发的运营 open_id (聪哥1号 或 聪哥3号 namespace 都行, 都会转 union_id)
                         若不给, 必须直接给 operator_union_id
        operator_union_id: 直接传 union_id (节省一次转换 API 调用)
        dry_run: True 仅报会做什么, 不真撤/真发

    Returns:
        {ok, status?, msg?, new_msg_id?, revoked_old?, operator_union_id?, dry_run?}
    """
    if not draft_rid:
        return {"ok": False, "msg": "缺 draft_rid"}

    # 1. 解析 operator union_id
    op_union = operator_union_id
    if not op_union and operator_open_id:
        op_union = await feishu.open_id_to_union_id(operator_open_id)
    if not op_union:
        return {"ok": False, "msg": "缺 operator union_id (open_id 转换失败或未给)"}

    # 2. 拉草稿 + 判终态
    try:
        rec = await feishu.get_record(config.T_DRAFT, draft_rid)
    except Exception as e:
        return {"ok": False, "msg": f"读草稿失败: {str(e)[:100]}"}

    f = rec.get("fields", {})
    status = ext(f.get("邮件草稿状态")) or ""
    if status in TERMINAL_STATUSES:
        return {"ok": False, "msg": f"草稿已终态({status}), 不重发", "status": status}

    # 3. 拿老卡 msg_id
    cur = ext(f.get("卡片个人消息IDs")) or ""
    try:
        mp = _json.loads(cur) if cur else {}
        if not isinstance(mp, dict):
            mp = {}
    except Exception:
        mp = {}
    old_msg_id = mp.get(op_union, "") or ""

    if dry_run:
        return {"ok": True, "dry_run": True, "status": status,
                "would_resend_to": op_union, "would_revoke": old_msg_id or None}

    # 4. 撤老卡 (best-effort)
    revoked = False
    if old_msg_id:
        try:
            await feishu.api("DELETE", f"/im/v1/messages/{old_msg_id}", which="app3")
            revoked = True
            print(f"[card_resend] revoked old msg {old_msg_id} → {op_union}")
        except Exception as e:
            # 老卡可能已撤过/超时/被删, 不影响重发
            print(f"[card_resend] revoke old fail (ignored): {e}")

    # 5. 重建卡 + 重发
    try:
        new_card = await _build_resend_card(draft_rid, rec)
    except Exception as e:
        return {"ok": False, "msg": f"重建卡失败: {str(e)[:150]}",
                "revoked_old": revoked, "status": status}

    new_msg_id = ""
    try:
        new_msg_id = await feishu.send_card_via_app3("union_id", op_union, new_card)
    except Exception as e:
        return {"ok": False, "msg": f"重发失败: {str(e)[:100]}",
                "revoked_old": revoked, "status": status}

    # 6. 更新 mids
    if new_msg_id:
        try:
            await feishu.write_card_recipients_msgids(
                draft_rid, [op_union], {op_union: new_msg_id})
        except Exception as e:
            # 写回失败不影响重发本身已生效
            print(f"[card_resend] write_msgids fail (ignored): {e}")

    return {"ok": True, "new_msg_id": new_msg_id, "revoked_old": revoked,
            "operator_union_id": op_union, "status": status}

"""草稿路由器 — 调用 reviewer + 决定下一步走向

输入: 「KOL·媒体人邮件草稿」record_id
输出: 路由结果 (写回评分字段 + 改 邮件草稿状态/审核路径 + 决定是否触发重生/通知)

路由规则 (决策 B/C):
  AI评分 ≥ 8 且 承诺命中=False → 邮件草稿状态=自动通过 / 审核路径=自动通过
  AI评分 5–7 或 承诺命中=True   → 邮件草稿状态=待审 / 审核路径=待人审 + 飞书通知
  AI评分 < 5 且 重生<2          → 邮件草稿状态=退回重生 / 重生次数+1 + 触发对应 generator 重生
  AI评分 < 5 且 重生≥2          → 邮件草稿状态=待审 / 审核路径=需人改 + 飞书通知
"""
import time
from . import config, feishu, reviewer, stage_model
from .feishu import ext


SCORE_AUTO_THRESHOLD = 8       # ≥ 此分自动通过
SCORE_RETRY_THRESHOLD = 5      # < 此分退回重生
MAX_RETRIES = 2                # 重生上限


async def route_draft(record_id: str, ship_confirm_meta: dict = None,
                       force_review_intent: str = None,
                       force_review_reason: str = None,
                       force_review_scenario: str = None,
                       skip_notify: bool = False) -> dict:
    """
    主入口: 给定草稿 record_id → 评审 + 路由 → 返回结果摘要

    Args:
        ship_confirm_meta: reply_drafter 传入的 ship_confirm 元信息 {address, country, product_name}
            存在表示这是寄样确认草稿, 通知卡片要含仓库发货建议 + 高优先级 + SLA 24h
        force_review_intent: 强制走人审的意图标记 (如 "不明意图" / "质疑/澄清" / "要报价")
            因 reviewer 给低风险模板(如 _gen_clarify_draft)评高分会自动通过 + 自动发,
            导致 KOL 被反复 spam (Ashtvn 案例: 9 封"是否需要更多 info"邮件死循环).
            存在则强制 committed=True 让草稿停"待审", 防止 _gen_clarify_draft 输出被自动发.
        skip_notify: True = 跑完评审 + 写回状态, 但不发旧聪哥1号待审卡.
            warm_recap 暖信用此跳过旧卡, 改由 warm_recap 自己发聪哥3号 form 卡(粘 UpPromote 券码),
            避免运营收到双卡(旧 1 号卡 + 新 3 号卡).
    """
    # 1. 读草稿
    rec = await feishu.get_record(config.T_DRAFT, record_id)
    f = rec["fields"]
    subject = ext(f.get("邮件主题"))
    body = ext(f.get("邮件正文"))
    contact_type = ext(f.get("对象类型")) or "KOL"
    source = ext(f.get("邮件草稿来源")) or "cold"
    sender_alias = ext(f.get("发送邮箱")) or ""
    # 从 alias 推断品牌
    if "powkong" in sender_alias.lower():
        brand = "POWKONG"
    elif "funlab" in sender_alias.lower() or "firefly" in sender_alias.lower():
        brand = "FUNLAB"
    else:
        brand = "FUNLAB"

    retries = int(f.get("重生次数") or 0)

    # 2. 调 reviewer
    result = await reviewer.review_draft(
        subject=subject, body=body, source=source,
        contact_type=contact_type, brand=brand,
    )

    score = result["score"]
    committed = result["committed"]
    hits = result["keywords_hit"]
    summary = result["summary"]
    reasons = result["reasons"]
    judge = result["ai_commitment_judge"]

    # ship_confirm 强制 committed=True (寄样涉及实物成本+物流+地址核对, 必走人审)
    if ship_confirm_meta:
        committed = True
        if "ship-sample" not in hits:
            hits = list(hits) + ["ship-sample"]

    # 不明意图 / 质疑澄清 / 要报价 / affiliate_upsell 强制 committed=True (防自动通过+自动发)
    # _gen_clarify_draft 输出"专业、无承诺"的话术会被 reviewer 给 10 分自动通过, 导致
    # KOL 每次回复 (即便只是"Lol") 都被回一封跟进邮件, 形成死循环 (Ashtvn 5/6 9 封事故)
    # affiliate_upsell 涉及合作模式邀约 + 佣金谈判 + 折扣码,必须人审 (P5.12 Steve 案例)
    if force_review_intent in ("不明意图", "质疑/澄清", "要报价", "affiliate_upsell"):
        committed = True
        intent_kw = {"不明意图": "unknown-intent", "质疑/澄清": "misspoke-correction",
                     "要报价": "quote-negotiation",
                     "affiliate_upsell": "affiliate-negotiation"}[force_review_intent]
        if intent_kw not in hits:
            hits = list(hits) + [intent_kw]

    # 2026-05-25: late-stage KOL 强制人审 — 防 reply_drafter stage-blind 给已寄样/已谈条款/已上稿
    # 的 KOL 自动发早期话术(如"要不要样品/你需要什么"). 周会 Metalfear4(已签收+直播)/PlayTopia
    # (已谈$100) 收到重复开发信事故. 详见 memory kol-stage-blind-reply-fix-2026-05-25.
    if force_review_reason:
        committed = True
        if "late-stage-relationship" not in hits:
            hits = list(hits) + ["late-stage-relationship"]

    # v4 ④b (2026-05-28): scenario_label ∈ FORCE_REVIEW_LABELS(8 高风险阶段: 合同/谈判/
    # 付款/收到视频/草稿需改/纠错/我方邮件损坏) → 强制人审. 纯加法(union 已有规则), 只增人审
    # 绝不自动发; scenario_label 错判最多多送几条人审(这些阶段本就该多审), 漏判回落上面规则.
    # 捕捉 6 粗意图(感兴趣等)漏掉的高风险回复阶段, 是 v4 防 stage-blind 误送的源头消费.
    if force_review_scenario and stage_model.is_force_review(force_review_scenario):
        committed = True
        kw = f"scenario:{force_review_scenario}"
        if kw not in hits:
            hits = list(hits) + [kw]

    # v4 ⑤ sop_gap 探测器 (2026-05-28): route_draft 完整消费 SSOT「是否人审」列 —
    # 除 ④ 的 强制人审(FORCE_REVIEW_LABELS) 外, 再接 低置信人审(LOW_CONF_REVIEW_LABELS,
    # 含 unclassified_fallback). 这样只有「否」的场景能自动通过, 其余全转人审 (纯加法 fail-safe).
    # - unclassified_fallback = playbook 没覆盖的回复 → 记 SOP 缺口 (kw=sop-gap:unmatched), 强制人审
    #   (老师框架: 无模板匹配→记gap+best-effort草稿+人审; best-effort 草稿 reply_drafter 已生成).
    # - 其它低置信(usage/brief/delay/too_expensive)= 分类不确定 → 也转人审 (kw=low-conf-review).
    # 缺口可筛: 草稿表 命中关键词 contains "sop-gap" / 场景标签="unclassified_fallback".
    if force_review_scenario and force_review_scenario in stage_model.LOW_CONF_REVIEW_LABELS:
        committed = True
        kw = ("sop-gap:unmatched" if force_review_scenario == stage_model.FALLBACK_LABEL
              else f"low-conf-review:{force_review_scenario}")
        if kw not in hits:
            hits = list(hits) + [kw]

    reasons_text = " | ".join(f"{k}:{v}" for k, v in reasons.items())[:500]
    if judge["reason"]:
        reasons_text += f" | 承诺判断:{judge['verdict']}-{judge['reason']}"
    if ship_confirm_meta:
        reasons_text = "[ship_confirm 强制人审] " + reasons_text

    # 3. 决定路由
    if score >= SCORE_AUTO_THRESHOLD and not committed:
        new_status = "自动通过"
        path = "自动通过"
        action = "auto_send"
    elif score < SCORE_RETRY_THRESHOLD and retries < MAX_RETRIES:
        new_status = "退回重生"
        path = "退回重生"
        action = "retry"
    elif score < SCORE_RETRY_THRESHOLD and retries >= MAX_RETRIES:
        new_status = "待审"
        path = "需人改"
        action = "notify_human"
    else:  # 5-7 分 或 承诺命中
        new_status = "待审"
        path = "待人审"
        action = "notify_human"

    # 4. 写回评分字段 + 改状态
    update_fields = {
        "AI评分": score,
        "AI评分理由": (summary + " | " + reasons_text)[:500],
        "承诺命中": committed,
        "命中关键词": ", ".join(hits)[:200],
        "审核路径": path,
        "邮件草稿状态": new_status,
    }
    if action == "retry":
        update_fields["重生次数"] = retries + 1

    await feishu.update_record(config.T_DRAFT, record_id, update_fields)

    # 5. 触发后续动作 (异步, 不阻塞主路由)
    if action == "notify_human" and not skip_notify:
        await _notify_human_review(record_id, rec, score, committed, summary, reasons_text, path,
                                    ship_confirm_meta=ship_confirm_meta)
    elif action == "retry":
        # 重生在调用方处理 (因为重生需要原始任务上下文)
        # router 只标状态,由 cron 或 generator 自身扫描重生标记触发重生
        pass
    # auto_send: 由 send_approved cron 扫 自动通过 状态自动发,无需此处触发

    return {
        "record_id": record_id,
        "score": score,
        "committed": committed,
        "keywords_hit": hits,
        "path": path,
        "status": new_status,
        "action": action,
        "retries_after": retries + (1 if action == "retry" else 0),
    }


async def _notify_human_review(record_id: str, rec: dict, score: int,
                               committed: bool, summary: str, reasons_text: str, path: str,
                               ship_confirm_meta: dict = None):
    """飞书 IM 通知运营审核
    ship_confirm_meta 存在 → 渲染寄样高优先级卡片 (含仓库发货建议 + SLA)
    """
    f = rec["fields"]
    subject = ext(f.get("邮件主题"))
    contact_type = ext(f.get("对象类型")) or "KOL"
    source = ext(f.get("邮件草稿来源")) or "cold"
    base_url = f"https://u1wpma3xuhr.feishu.cn/base/{config.FEISHU_APP_TOKEN}?table={config.T_DRAFT}"
    action_card = None  # 非寄样 cold/reply 待审 → 互动审核卡 (聪哥3号发负责人私聊, 卡上直接审)

    if ship_confirm_meta:
        # 解析联系人信息 (2026-05-31 统一字段: contact_info + brand + email)
        _is_ed = bool(feishu.xrid(f.get("关联媒体人")))
        _crid = feishu.xrid(f.get("关联媒体人")) if _is_ed else feishu.xrid(f.get("关联KOL"))
        _ctype = "媒体人" if _is_ed else "KOL"
        _ci = await feishu.resolve_contact_info(_crid, _ctype) if _crid else {}
        _sender = ext(f.get("发送邮箱")) or ""
        _brand = "POWKONG" if "powkong" in _sender.lower() else "FUNLAB"
        _email = ext(f.get("收件邮箱")) or ""
        card = _build_ship_confirm_card(record_id, rec, score, summary, ship_confirm_meta, base_url,
                                        contact_info=_ci, brand=_brand)
        # 寄样: 主审 (独立站运营专员) + CC (Frankie + 吴晓丹)
        main_targets, cc_targets = await _ship_confirm_targets()
        targets = main_targets + cc_targets
        # 寄样填运单号 form 卡 (负责人卡上填 运单号+物流商 即发, 无需跳表格); 群仍收 SOP 信息卡
        action_card = _build_ship_tracking_card(
            record_id, _email or contact_type,
            ship_confirm_meta.get("product_name", "") or "the sample",
            subject, "寄样确认",
            contact_info=_ci, brand=_brand, email=_email, contact_type=_ctype)
    else:
        template_color = "orange" if path == "待人审" else "red"
        title_emoji = "📝" if path == "待人审" else "⚠️"
        card = {
            "header": {
                "template": template_color,
                "title": {"tag": "plain_text", "content": f"{title_emoji} 草稿待审 ({path}) — {source} / {contact_type}"},
            },
            "elements": [
                {"tag": "div", "fields": [
                    {"is_short": True, "text": {"tag": "lark_md", "content": f"**AI 评分**: {score}/10"}},
                    {"is_short": True, "text": {"tag": "lark_md", "content": f"**承诺**: {'⚠️ 是' if committed else '否'}"}},
                ]},
                {"tag": "div", "text": {"tag": "lark_md", "content": f"**主题**: {subject[:100]}"}},
                {"tag": "div", "text": {"tag": "lark_md", "content": f"**评分总评**: {summary}"}},
                {"tag": "hr"},
                {"tag": "div", "text": {"tag": "lark_md", "content": f"**审核理由 (5 项)**\n{reasons_text[:300]}"}},
                {"tag": "action", "actions": [
                    {"tag": "button", "text": {"tag": "plain_text", "content": "打开KOL·媒体人邮件草稿"},
                     "url": base_url, "type": "primary"},
                ]},
            ],
        }
        # warm_recap 暖信: 卡片顶部加自解释操作说明 (运营无需猜, 直接照做)
        if source == "warm_recap":
            card["elements"].insert(0, {"tag": "div", "text": {"tag": "lark_md", "content": (
                "📦 **这是寄样后「确认收到 + 轻 brief」暖信**(KOL 已签收样品)— **不是催稿**。\n"
                "**你只需 2 步**:\n"
                "1️⃣ 打开下方草稿,填 **「折扣比例」**(数字,如 `15`)+ **「折扣码」**(可留空,系统按 KOL 名自动生成如 `THAO15`)\n"
                "2️⃣ 把「邮件草稿状态」改 **「通过」** → 系统自动建 Shopify 折扣码 + 替换正文 + 发出\n"
                "⚠️ **不要手动改正文里的 `[DISCOUNT_CODE]`/`[DISCOUNT_PCT]`**(系统会自动替换;改了会被拦截)\n"
                "🔗 链接已是**独立站(带折扣码追踪)**,**不要加亚马逊链接**")}})
            card["elements"].insert(1, {"tag": "hr"})
        # 2026-05-17 A9: 改用 feishu.resolve_notify_targets helper (统一决策)
        role = "needs_rewrite" if path == "需人改" else "reviewer"
        targets = await feishu.resolve_notify_targets(role)
        # 2026-05-29: cold/reply 互动审核卡 (负责人卡上直接 通过/否决/重生, 无需跳表格)
        sender_alias2 = ext(f.get("发送邮箱")) or ""
        brand2 = "POWKONG" if "powkong" in sender_alias2.lower() else "FUNLAB"
        prod_name = ""
        _prid = feishu.xrid(f.get("关联产品"))
        if _prid:
            try:
                _ppf = (await feishu.get_record(config.T_PRODUCT, _prid))["fields"]
                prod_name = ext(_ppf.get("产品名")) or ext(_ppf.get("产品英文名")) or ""
            except Exception as _e:
                print(f"[draft_router] 产品名解析失败: {_e}")
        # 2026-05-29 Frankie: 拉联系人主表 → 卡片显示 KOL 名/平台/粉丝/阶段 (运营一眼看清是谁/什么阶段)
        contact_info = {}
        try:
            _is_ed = bool(feishu.xrid(f.get("关联媒体人")))
            _crid = feishu.xrid(f.get("关联媒体人")) if _is_ed else feishu.xrid(f.get("关联KOL"))
            if _crid:
                from . import reply_monitor  # 惰性 import 防循环; _contact_stage_label 纯函数
                _ccf = (await feishu.get_record(config.T_EDITOR if _is_ed else config.T_KOL, _crid))["fields"]
                if _is_ed:
                    contact_info = {"name": ext(_ccf.get("媒体人姓名")) or "?",
                                    "platform": ext(_ccf.get("主要媒体")) or ext(_ccf.get("所属媒体")) or "",
                                    "fans": "", "stage": reply_monitor._contact_stage_label(_ccf)}
                else:
                    try:
                        _fans = f"{int(_ccf.get('粉丝数') or 0):,}"
                    except (ValueError, TypeError):
                        _fans = str(_ccf.get("粉丝数") or "")
                    contact_info = {"name": ext(_ccf.get("账号名")) or "?",
                                    "platform": ext(_ccf.get("主平台")) or "",
                                    "fans": _fans, "stage": reply_monitor._contact_stage_label(_ccf)}
        except Exception as _e:
            print(f"[draft_router] 联系人信息解析失败: {_e}")
        action_card = _build_review_action_card(record_id, rec, score, summary, reasons_text,
                                                path, source, contact_type, prod_name, brand2, base_url,
                                                contact_info=contact_info)

    # 群通知 + 个人通知, 统计成败回写草稿表「卡片发送状态/错误/时间」(2026-05-16)
    # 2026-05-17 A5: 保存群 msg_id 用于结束态 update card 标"已审"
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
        print(f"[draft_router] notify chat fail: {e}")
    _unions = []  # 收到聪哥3号互动卡的运营 union_id(写「关联运营」, 看板分组用)
    _mids = {}    # {union_id: msg_id}(写「卡片个人消息IDs」, /card/resend 撤老卡用)
    for name, oid in targets:
        try:
            if action_card is not None:
                # 互动审核卡走聪哥3号(回调到 event-hub) → 负责人 union_id 私聊, 卡上直接审
                uid = await feishu.open_id_to_union_id(oid)
                if uid:
                    msg_id = await feishu.send_card_via_app3("union_id", uid, action_card)
                    if msg_id:
                        _unions.append(uid)
                        _mids[uid] = msg_id
                else:
                    await feishu.send_card_message("open_id", oid, card)  # 拿不到 union_id 降级旧卡
            else:
                await feishu.send_card_message("open_id", oid, card)
            success += 1
        except Exception as e:
            fail += 1
            errors.append(f"{name}: {str(e)[:80]}")
            print(f"[draft_router] notify {name} fail: {e}")
    if _unions or _mids:
        await feishu.write_card_recipients_msgids(record_id, _unions, _mids)
    await feishu.mark_card_receipt(record_id, success, fail, errors, group_msg_id=group_msg_id)


# ===== ship_confirm 卡片 (V2: SOP 清单, 不查领星 API) =====
def _build_ship_confirm_card(record_id: str, rec: dict, score: int, summary: str,
                              meta: dict, base_url: str, escalation: bool = False,
                              contact_info: dict = None, brand: str = "") -> dict:
    """SHIP_CONFIRM 高优先级卡片
    Args:
        escalation: True = 24h 超时升级版 (标题加 🚨, 颜色加深, 强调超时)
        contact_info: 由 _notify_human_review 用 feishu.resolve_contact_info 解析后传入
        brand: POWKONG / FUNLAB
    """
    f = rec["fields"]
    contact_type = ext(f.get("对象类型")) or "KOL"
    subject = ext(f.get("邮件主题"))
    address = (meta.get("address") or "").strip()
    country = (meta.get("country") or "").strip().upper()
    product_name = (meta.get("product_name") or "").strip()
    email = ext(f.get("收件邮箱")) or ""

    # SLA 24h
    import time as _t
    gen_time = f.get("生成时间") or int(_t.time() * 1000)
    sla_dt_ms = gen_time + 24 * 3600 * 1000
    sla_str = _t.strftime("%Y-%m-%d %H:%M", _t.localtime(sla_dt_ms / 1000))

    if escalation:
        title = f"🚨 [SLA 超时] 寄样草稿 24h 未处理 — 立即跟进 ({contact_type})"
        emoji_lead = "🚨🚨🚨"
        color = "red"
    else:
        title = f"⚠️ 高优先级 — 寄样确认 ({contact_type}) | 24h SLA"
        emoji_lead = "⚠️"
        color = "red"

    sop_md = (
        "**📋 寄样操作 SOP** (寄样涉及 2 封邮件,分阶段)\n\n"
        "**优先级 1**: 查该国 / 该区域 **FBA 仓** 是否有库存\n"
        "  → 优先 **多渠道配送 (MCF)** 直接寄给收件人\n"
        "  → 不紧急的可走 **移除订单 (Removal Order)** 退回再寄, 成本更低\n\n"
        "**优先级 2**: 查该国 / 该区域 **海外仓** 是否有库存 → 走当地快递寄出\n\n"
        "**兜底**: **中国本地仓直发** (DHL Express / 国际快递)\n\n"
        "─────────\n\n"
        "### 📨 第 1 阶段: 寄样确认邮件 (本条草稿, 24h 内完成)\n\n"
        "1. 决定从哪个仓发货 → 创建 **MCF / 海外仓 / 国内** 订单\n"
        "2. 在本草稿正文搜 **`待填`** 找到 2 处占位符替换:\n"
        "   - `[CARRIER 待填运营修改]` → 物流商名 (USPS Ground / DHL Express / FedEx)\n"
        "   - `[ETA 待填]` → 预计到货时间 (如 3-5 business days)\n"
        "3. 把「邮件草稿状态」改为 **通过** → 系统自动发第 1 封确认邮件\n"
        "   ⚠️ 系统已加占位符校验: 还含 `待填` **不会发送**, 状态自动改回「待修改」\n"
        "4. 同步更新 KOL/媒体人 主表「合作状态」 → **已寄样**\n\n"
        "─────────\n\n"
        "### 📦 第 2 阶段: 运单号追加邮件 (24h 后, 系统自动建草稿)\n\n"
        "第 1 封发出后, 系统**自动**建第 2 条草稿 (来源=`tracking_followup`, 状态=`待修改`,\n"
        "建议发送时间 = 24h 后)。\n\n"
        "5. **24h 后**, 从 Amazon MCF / 物流系统拿到运单号 → 打开第 2 条草稿\n"
        "6. 在「运单号」「物流商」两个字段填值即可 (代码自动替换正文占位符):\n"
        "   - 「运单号」 → 实际运单号 (如 1Z999AA10123456784)\n"
        "   - 「物流商」 → 物流商 (跟第 1 封同步)\n"
        "   ⚠️ 不要手动改邮件正文字段! 改了容易错位 (5/15 已踩坑事故)\n"
        "7. 把「邮件草稿状态」改为 **通过** → 系统自动发第 2 封运单号邮件\n\n"
        "**温馨提示**: 第 2 条草稿在「KOL·媒体人邮件草稿」表里, 草稿来源=`tracking_followup` 一目了然"
    )

    return {
        "header": {
            "template": color,
            "title": {"tag": "plain_text", "content": title},
        },
        "elements": [
            feishu.build_contact_info_block(
                contact_info=contact_info, product_name=product_name, brand=brand,
                email=email, contact_type=contact_type),
            {"tag": "div", "text": {"tag": "lark_md",
                "content": f"**{emoji_lead} 对方主动给了寄送地址 + 想收 {product_name}**"}},
            {"tag": "div", "fields": [
                {"is_short": True, "text": {"tag": "lark_md", "content": f"**国家**: {country or '?'}"}},
                {"is_short": False, "text": {"tag": "lark_md",
                    "content": f"**📦 收件地址 (AI 提取自对方邮件)**\n```\n{address[:400]}\n```"}},
            ]},
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md", "content": sop_md}},
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md",
                "content": f"**⏰ SLA 截止**: {sla_str}" + (" ⚠️ **已超时!**" if escalation else "")}},
            {"tag": "div", "text": {"tag": "lark_md",
                "content": f"**📝 AI 草稿评分**: {score}/10 — {summary}"}},
            {"tag": "action", "actions": [
                {"tag": "button", "text": {"tag": "plain_text", "content": "打开此条草稿"},
                 "url": f"{base_url}", "type": "primary"},
            ]},
        ],
    }


def _build_review_action_card(record_id: str, rec: dict, score: int, summary: str,
                              reasons_text: str, path: str, source: str, contact_type: str,
                              product_name: str, brand: str, base_url: str,
                              contact_info: dict = None) -> dict:
    """cold/reply 待审互动卡 (聪哥3号发负责人私聊): 全正文 + 信息 + 通过/否决/重生/去表格 按钮.
    运营卡片上直接审核, 无需跳表格. 按钮 value 走 n8n event-hub Draft Action 分支落状态.
    contact_info (2026-05-29 Frankie): {name, platform, fans, stage} — 卡片一眼看清 KOL 是谁/什么阶段.
    """
    f = rec["fields"]
    subject = ext(f.get("邮件主题"))
    body = ext(f.get("邮件正文"))
    email = ext(f.get("收件邮箱"))
    highlight = ext(f.get("匹配亮点"))
    gap = ext(f.get("匹配不足"))
    angle = ext(f.get("建议切入点"))
    hit_kw = ext(f.get("命中关键词"))
    body_show = body if len(body) <= 1800 else body[:1800] + "\n…(正文过长已截断, 点「去表格改」看全文)"
    val = {"app_token": config.FEISHU_APP_TOKEN, "table_id": config.T_DRAFT, "record_id": record_id}
    ci = contact_info or {}
    who_label = "媒体人" if contact_type == "媒体人" else "KOL"
    elements = [
        {"tag": "div", "fields": [
            {"is_short": True, "text": {"tag": "lark_md", "content": f"**{who_label}**: {ci.get('name') or '?'}"}},
            {"is_short": True, "text": {"tag": "lark_md", "content": f"**阶段**: {ci.get('stage') or '?'}"}},
            {"is_short": True, "text": {"tag": "lark_md", "content": f"**平台**: {ci.get('platform') or '?'}"}},
            {"is_short": True, "text": {"tag": "lark_md", "content": f"**粉丝**: {ci.get('fans') or '—'}"}},
            {"is_short": True, "text": {"tag": "lark_md", "content": f"**产品**: {product_name or '?'}"}},
            {"is_short": True, "text": {"tag": "lark_md", "content": f"**品牌**: {brand}"}},
            {"is_short": True, "text": {"tag": "lark_md", "content": f"**收件人**: {email or '?'}"}},
            {"is_short": True, "text": {"tag": "lark_md", "content": f"**AI评分**: {score}/10"}},
        ]},
        {"tag": "div", "text": {"tag": "lark_md", "content": f"**评分总评**: {summary}"}},
    ]
    bits = []
    if highlight:
        bits.append(f"✅ 亮点: {highlight}")
    if gap:
        bits.append(f"⚠️ 不足: {gap}")
    if angle:
        bits.append(f"🎯 切入: {angle}")
    if bits:
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "\n".join(bits)[:600]}})
    if hit_kw:
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"**命中关键词**: {hit_kw[:150]}"}})
    elements += [
        {"tag": "hr"},
        {"tag": "div", "text": {"tag": "lark_md", "content": f"**📧 主题**\n{subject}"}},
        {"tag": "div", "text": {"tag": "lark_md", "content": f"**✉️ 正文**\n{body_show}"}},
        {"tag": "hr"},
        {"tag": "div", "text": {"tag": "lark_md", "content": "👇 **卡片上直接审核**(无需跳表格); 需大改正文才点「去表格改」"}},
        {"tag": "action", "actions": [
            {"tag": "button", "text": {"tag": "plain_text", "content": "✅ 通过"}, "type": "primary", "value": dict(val, action="draft_approve")},
            {"tag": "button", "text": {"tag": "plain_text", "content": "❌ 否决"}, "type": "danger", "value": dict(val, action="draft_reject")},
            {"tag": "button", "text": {"tag": "plain_text", "content": "🔁 退回重生"}, "type": "default", "value": dict(val, action="draft_regen")},
            {"tag": "button", "text": {"tag": "plain_text", "content": "📝 去表格改正文"}, "type": "default", "url": base_url},
        ]},
    ]
    return {
        "config": {"wide_screen_mode": True, "update_multi": True},
        "header": {
            "template": "orange" if path == "待人审" else "red",
            "title": {"tag": "plain_text", "content": f"📝 待你审核 ({path}) — {source} / {contact_type}"},
        },
        "elements": elements,
    }


def _build_ship_tracking_card(record_id: str, contact_name: str, product_name: str,
                              subject: str, stage_label: str,
                              contact_info: dict = None, brand: str = "",
                              email: str = "", contact_type: str = "KOL") -> dict:
    """寄样确认/运单号 表单卡 (聪哥3号发负责人, 卡上填即发, 无需跳表格).
    填 运单号 + 物流商 → 提交 → event-hub Draft Action(draft_tracking) 置字段+通过 →
    auto_send 用「运单号/物流商」字段自动替换正文 [TRACKING#]/[CARRIER] + 占位符闸门兜底(空不发).
    contact_info/brand/email/contact_type: 由调用方解析后传入 (2026-05-31 统一字段标准).
    """
    base_val = {"action": "draft_tracking", "app_token": config.FEISHU_APP_TOKEN,
                "table_id": config.T_DRAFT, "record_id": record_id}
    return {
        "config": {"wide_screen_mode": True, "update_multi": True},
        "header": {"template": "red", "title": {"tag": "plain_text", "content": f"📦 {stage_label} — 填运单号发样 ({contact_name})"}},
        "elements": [
            feishu.build_contact_info_block(
                contact_info=contact_info, product_name=product_name, brand=brand,
                email=email, contact_type=contact_type),
            {"tag": "div", "text": {"tag": "lark_md", "content": f"**原主题**: {subject}"}},
            {"tag": "div", "text": {"tag": "lark_md", "content": "📋 查 FBA/海外仓/国内仓库存 → 建 MCF/海外仓/国内订单 → 拿到运单号 → 下面填 → 提交即发"}},
            {"tag": "hr"},
            {"tag": "form", "name": f"ship_{record_id}", "elements": [
                {"tag": "input", "name": "tracking_no", "label_position": "left",
                 "label": {"tag": "plain_text", "content": "运单号:"},
                 "placeholder": {"tag": "plain_text", "content": "如 1Z999AA10123456784"}},
                {"tag": "input", "name": "carrier", "label_position": "left",
                 "label": {"tag": "plain_text", "content": "物流商:"},
                 "placeholder": {"tag": "plain_text", "content": "如 USPS Ground / DHL Express / FedEx"}},
                {"tag": "button", "action_type": "form_submit", "name": "submit",
                 "text": {"tag": "plain_text", "content": "✅ 确认发送 (自动替换正文运单号/物流商)"}, "type": "primary",
                 "value": base_val},
            ]},
            {"tag": "div", "text": {"tag": "lark_md", "content": "⚠️ 运单号/物流商 留空不会发(占位符闸门拦截); 不用进表格改正文, 系统自动替换"}},
        ],
    }


async def _ship_confirm_targets() -> tuple:
    """ship_confirm 通知目标分主审 + CC
    2026-05-17 A9: 改用 feishu.resolve_notify_targets helper
    Returns: (main_targets, cc_targets) 都是 [(name, open_id), ...]
    """
    main = await feishu.resolve_notify_targets("ship_main")
    cc = await feishu.resolve_notify_targets("ship_cc")
    return main, cc


async def batch_review_pending() -> dict:
    """
    扫所有 邮件草稿状态=待审 + 没有 AI评分 的记录 → 跑 reviewer
    用作兜底 cron, 防止生成器漏调 router
    """
    items = await feishu.search_records(config.T_DRAFT, [
        {"field_name": "邮件草稿状态", "operator": "is", "value": ["待审"]},
    ])
    processed = []
    for rec in items:
        if rec["fields"].get("AI评分") is not None:
            continue  # 已审过
        try:
            r = await route_draft(rec["record_id"])
            processed.append(r)
        except Exception as e:
            processed.append({"record_id": rec["record_id"], "error": str(e)[:200]})
    return {"processed": len(processed), "details": processed[:20]}

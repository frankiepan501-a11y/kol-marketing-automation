"""回复监听 - 迁移自本地 scripts/send_loop/reply_monitor.py"""
import re, time, html as html_mod
from . import config, feishu, zoho, deepseek, reply_drafter
from .feishu import ext, xrid


# ===== OOO 自动回复检测 =====
OOO_PATTERNS = [
    r"\bout\s*[-_]?\s*of\s*[-_]?\s*office\b",
    r"\bautomatic\s+reply\b",
    r"\bauto[-_\s]?reply\b",
    r"\bauto[-_\s]?response\b",
    r"\bcurrently\s+(away|out)\b",
    r"\bI('|')?ll\s+be\s+back\b",
    r"\bI('|')?ll\s+get\s+back\s+to\s+you\s+on\b",
    r"\bvacation\s+message\b",
    r"\bleave\s+notice\b",
    r"\bon\s+(annual\s+)?leave\b",
    # 中文
    r"自动回复",
    r"暂时无法回复",
    r"暂离办公室",
    r"休假中",
    r"休假回复",
    r"度假中",
]
OOO_RE = re.compile("|".join(OOO_PATTERNS), re.IGNORECASE)


def is_ooo(subject: str, body: str) -> tuple:
    """检测是否为 OOO 自动回复 → (bool, 命中片段)"""
    text = (subject or "") + "\n" + (body or "")
    m = OOO_RE.search(text[:1500])
    return (bool(m), m.group(0) if m else "")

POSITIVE = {"感兴趣", "要报价"}
INTENT_TO_STATUS_KOL = {
    "感兴趣": "洽谈中", "要报价": "洽谈中",
    "委婉拒绝": "不合适", "退订": "黑名单",
    "不明意图": None,
    "质疑/澄清": None,  # 让人审决定主表状态, 不自动推到洽谈中
}
INTENT_TO_STATUS_EDITOR = {
    "感兴趣": "洽谈中", "要报价": "洽谈中",
    "委婉拒绝": "不合适", "退订": "不合适",
    "不明意图": None,
    "质疑/澄清": None,
}
INTENT_EMOJI = {
    "感兴趣": "✅", "要报价": "💰", "委婉拒绝": "⚠️", "退订": "🛑", "不明意图": "❓",
    "质疑/澄清": "🔍",
}


def parse_email(addr: str) -> str:
    m = re.search(r'[\w.+-]+@[\w-]+\.[\w.-]+', addr or "")
    return m.group(0).lower() if m else ""


def html_to_text(s: str) -> str:
    s = re.sub(r'<br\s*/?>', '\n', s or '', flags=re.I)
    s = re.sub(r'</(p|div|h[1-6]|li)>', '\n', s, flags=re.I)
    s = re.sub(r'<[^>]+>', '', s)
    s = html_mod.unescape(s)
    return re.sub(r'\n{3,}', '\n\n', s).strip()


async def classify_intent(from_addr: str, subject: str, body: str):
    prompt = f"""你在审核一封 KOL/媒体人 回复我们 cold outreach 邮件,判断其意图并给出建议。

【回复】
From: {from_addr}
Subject: {subject}
Body (前 800 字):
{body[:800]}

【意图类型】
- 感兴趣: 主动表达兴趣或好评 + **没有保留条件** ("Sounds cool!"/"Looks awesome"/"Would love to check it out"/"Tell me more"/给地址要寄样)
  - ⚠️ 注意: "产品很酷,但我没设备测试" / "酷但我现在不做这个" / "looks great BUT I don't have a Switch" 这种**称赞 + 但保留条件**类不是感兴趣! 是委婉拒绝 (decline_reason=不匹配_条件)
- 要报价: 询问价格/佣金/合作条款 (commission/MOQ/rate card)
- 委婉拒绝: "不适合"/"暂无档期"/"not a fit"/"thanks but no" + **缺条件软拒绝** (产品好但没设备/没受众/不做该赛道)
- 退订: unsubscribe/please remove me/stop emailing
- 质疑/澄清: 对方在**纠正/反驳**我们 cold email 里的某个具体说法 (典型: "I've never made X video"/"That's not my channel"/"You have me confused with someone else"/"I don't cover that category"/"Where did you see that?")。这种回复**不是表达兴趣**,是在打脸我们对他的描述错误,必须人审 + 道歉 + 重新切入,绝不能当作"感兴趣"自动发寄样确认。
- 不明意图: out-of-office/自动回复/无法判断

【判别要点】
- 一封"我从没做过 X"/"那不是我"/"你搞错人了"类回复,即便语气客气,也是 质疑/澄清,不是 感兴趣。
- 含有 "I've never"/"I don't"/"that's not"/"you have the wrong"/"actually, I"/"to clarify"/"correction"等纠错语气强信号。
- "Tell me more" 不带纠错 = 感兴趣;"I don't make those, but tell me more about the product" = 质疑/澄清(因有纠错前置)。
- ⚠️ **称赞 + BUT 缺条件**铁律 (okamikazz 5/7 反例): KOL 说"产品很酷但我没 Switch / 我现在不做这个 / 我没这种受众" → **委婉拒绝 (decline_reason=不匹配_条件)**, 不是感兴趣。AI 之前错把 okamikazz "muito legal seu produto, uma pena que não tenho um switch para testa-lo" 归感兴趣 → 系统又自动发"sample/press kit/quick call"三选项给一个明说没 Switch 的 KOL,KOL 困惑。

【委婉拒绝细分(仅 type=委婉拒绝 时填 decline_reason 字段)】
- 不匹配_品类: KOL 不做这类产品 (例: "I focus on cooking content not gaming gear" / "wrong niche for me" / "my audience isn't into this")
  → retry_days = 0 (永久不重发)
- 不匹配_时机: 暂时忙 / 档期满 / 季节性 (例: "booked till end of Q3" / "currently on break" / "vacation till April")
  → retry_days = 30/60/90 (根据时机长短: 提到"周/月" → 30; "季度" → 60; "半年/年" → 90; 默认 60)
- 不匹配_方式: 只做付费 / 不做免费寄样 / 只做大品牌 ambassador (例: "I only work with paid sponsorships" / "no free seeding" / "only ambassador deals")
  → retry_days = 0 (转付费路径,无需重发)
- **不匹配_条件: 缺核心硬件 / 缺受众 / 暂时不做该赛道但保留未来可能** (例: "I don't have a Switch to test it" / "no audience for gaming" / "if one day I start covering Switch content I'd love to try"/ "uma pena que não tenho um switch")
  → retry_days = 180 (半年后再触达, KOL 可能买设备/转赛道)
- 不感兴趣_其他: 兜底 (例: "thanks but no" 没说原因 / "not interested")
  → retry_days = 0

返回 JSON:
{{
  "type":"感兴趣|要报价|委婉拒绝|退订|质疑/澄清|不明意图",
  "confidence":0.0-1.0,
  "summary":"一句总结",
  "key_quote":"原文 1 句",
  "suggested_action":"下一步建议",
  "decline_reason":"不匹配_品类|不匹配_时机|不匹配_方式|不匹配_条件|不感兴趣_其他 (仅 type=委婉拒绝 填,否则空)",
  "retry_days":0-180 整数 (不匹配_时机=30/60/90; 不匹配_条件=180; 其他都填 0)
}}"""
    try:
        return await deepseek.chat_json(prompt, max_tokens=500)
    except Exception as e:
        return {"type": "不明意图", "confidence": 0.0, "summary": f"API错误: {e}",
                "key_quote": "", "suggested_action": "人工查看",
                "decline_reason": "", "retry_days": 0}


async def find_kol_by_email(email: str):
    items = await feishu.search_records(config.T_KOL, [
        {"field_name": "邮箱", "operator": "contains", "value": [email.strip().lower()]}
    ])
    return items[0] if items else None


async def find_editor_by_email(email: str):
    items = await feishu.search_records(config.T_EDITOR, [
        {"field_name": "邮箱", "operator": "contains", "value": [email.strip().lower()]}
    ])
    return items[0] if items else None


async def find_contact(email: str):
    rec = await find_kol_by_email(email)
    if rec: return rec, "KOL"
    rec = await find_editor_by_email(email)
    if rec: return rec, "editor"
    return None, None


# 2026-05-17 A4 性能: process-level cache 让一次 reply_monitor cron 内多 inbox 共用一次全表扫
# TTL 5min, 跨 cron 自然过期 (cron 间隔 15min). 单次 cron 内最多 60 inbox 复用 1 次查询.
_sent_drafts_cache = {"timestamp": 0, "items": None}
_SENT_CACHE_TTL = 300


async def _get_sent_drafts():
    """拉所有 状态=已发送 草稿, 带 5min cache. 只取 link_field/来源/回复/原文/时间 等关键字段减 payload."""
    import time as _t
    if _sent_drafts_cache["items"] is not None and \
       _t.time() - _sent_drafts_cache["timestamp"] < _SENT_CACHE_TTL:
        return _sent_drafts_cache["items"]
    items = await feishu.search_records(
        config.T_DRAFT,
        [{"field_name": "邮件草稿状态", "operator": "is", "value": ["已发送"]}],
        field_names=["关联KOL", "关联媒体人", "邮件草稿来源", "是否回复",
                     "回复原文", "发送时间", "邮件主题"],
    )
    _sent_drafts_cache["items"] = items
    _sent_drafts_cache["timestamp"] = _t.time()
    return items


async def find_draft(contact_rid: str, contact_type: str):
    """找到该 contact 关联的"待监听"草稿 + 该 contact 的所有已发送草稿(供 body 去重用).
    优先取「未回复 + 发送时间最新」的草稿；都已回复时回 fallback 取最新一条。

    V3 加固方案 B (2026-05-08, 1upBinge 6 封事故根因修法):
        unreplied 候选池**排除 邮件草稿来源=reply 的草稿** — reply 草稿"未回复"语义是
        "等 KOL 对我们 reply 的再次回复", 不该作为新 inbox email 的匹配目标.
        排除 reply 后 cold/followup/tracking_followup 才是 unreplied 候选, 根因消除.
        all_matched 仍含 reply (dedup 时用).

    2026-05-17 A4: 用 _get_sent_drafts cache 减少全表扫 (一次 cron 内 60 inbox 共用 1 次查询).

    Returns: (best_draft, all_matched_drafts) 或 (None, [])
    """
    link_field = "关联媒体人" if contact_type == "editor" else "关联KOL"
    items = await _get_sent_drafts()
    matched = [r for r in items if xrid(r["fields"].get(link_field)) == contact_rid]
    if not matched:
        return None, []
    # V3-B: unreplied + fallback pool 都排除 来源=reply 草稿
    # 否则 KOL 多轮回复时, fallback 仍会选到最新的 reply 草稿 → guard 是否回复=false 不拦 → 死循环
    non_reply = [r for r in matched if ext(r["fields"].get("邮件草稿来源")) != "reply"]
    unreplied = [r for r in non_reply if not r["fields"].get("是否回复")]
    pool = unreplied if unreplied else non_reply
    if not pool:
        # 该 contact 只有 reply 草稿 (异常状态: 没 cold/followup 但已有 reply) → 拒绝处理新 inbox
        return None, matched
    pool.sort(key=lambda r: r["fields"].get("发送时间") or 0, reverse=True)
    return pool[0], matched


def build_card(contact_type: str, contact_info: dict, brand: str, intent: dict, subject: str):
    intent_type = intent.get("type", "?")
    emoji = INTENT_EMOJI.get(intent_type, "📬")
    conf = intent.get("confidence", 0)
    base_url = f"https://u1wpma3xuhr.feishu.cn/base/{config.FEISHU_APP_TOKEN}"
    target_table = config.T_EDITOR if contact_type == "editor" else config.T_KOL
    return {
        "header": {
            "template": "green" if intent_type in ("感兴趣", "要报价") else "orange" if intent_type in ("不明意图", "质疑/澄清") else "red",
            "title": {"tag": "plain_text", "content": f"{emoji} {'媒体人' if contact_type=='editor' else 'KOL'} 回复 — {intent_type}"}
        },
        "elements": [
            {"tag": "div", "fields": [
                {"is_short": True, "text": {"tag": "lark_md", "content": f"**姓名**: {contact_info['name']}"}},
                {"is_short": True, "text": {"tag": "lark_md", "content": f"**来源**: {contact_info['source']}"}},
                {"is_short": True, "text": {"tag": "lark_md", "content": f"**国家**: {contact_info['country']}"}},
                {"is_short": True, "text": {"tag": "lark_md", "content": f"**品牌**: {brand}"}},
                {"is_short": True, "text": {"tag": "lark_md", "content": f"**置信度**: {conf:.0%}"}},
                {"is_short": True, "text": {"tag": "lark_md", "content": f"**邮箱**: {contact_info['email']}"}},
            ]},
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"**📝 意图总结**\n{intent.get('summary','')}"}},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"**💬 原话**\n> {intent.get('key_quote','')[:200]}"}},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"**➡️ 建议行动**\n{intent.get('suggested_action','')}"}},
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"**原主题**: {subject}"}},
            {"tag": "action", "actions": [
                {"tag": "button", "text": {"tag": "plain_text", "content": f"打开{'媒体人' if contact_type=='editor' else 'KOL'}主表"},
                 "url": f"{base_url}?table={target_table}", "type": "primary"},
                {"tag": "button", "text": {"tag": "plain_text", "content": "打开KOL·媒体人邮件草稿"},
                 "url": f"{base_url}?table={config.T_DRAFT}", "type": "default"},
            ]},
        ]
    }


async def notify_all(card, draft_rid: str = None):
    """发卡片到群 + 全员个人. 2026-05-16: 如 draft_rid 给了, 回写发送回执到草稿表."""
    success = 0
    fail = 0
    errors = []
    try:
        await feishu.send_card_message("chat_id", config.NOTIFY_CHAT_ID, card)
        success += 1
    except Exception as e:
        fail += 1
        errors.append(f"群: {str(e)[:80]}")
        print(f"notify chat fail: {e}")
    for name, oid in config.NOTIFY_USERS:
        try:
            await feishu.send_card_message("open_id", oid, card)
            success += 1
        except Exception as e:
            fail += 1
            errors.append(f"{name}: {str(e)[:80]}")
            print(f"notify {name} fail: {e}")
    if draft_rid:
        await feishu.mark_card_receipt(draft_rid, success, fail, errors)


async def run():
    processed = 0
    results = []
    for brand in ("POWKONG", "FUNLAB"):
        alias = config.BRAND_CONFIG[brand]["alias_from"]
        try:
            msgs = await zoho.search_inbox(brand, f"to:{alias}", limit=30)
        except Exception as e:
            results.append({"brand": brand, "error": str(e)[:200]})
            continue

        for msg in msgs:
            from_addr = parse_email(msg.get("fromAddress") or msg.get("sender") or "")
            if not from_addr or alias.lower() in from_addr.lower():
                continue
            subject = msg.get("subject", "")
            msg_id = msg.get("messageId") or msg.get("summary")
            folder_id = msg.get("folderId")

            contact, ctype = await find_contact(from_addr)
            if not contact: continue

            draft, all_matched = await find_draft(contact["record_id"], ctype)
            if not draft: continue
            if draft["fields"].get("是否回复"): continue

            # 拉正文
            body_html = ""
            if folder_id:
                try: body_html = await zoho.get_message_content(brand, msg_id, folder_id)
                except Exception: pass
            email_body = html_to_text(body_html) or msg.get("summary", "") or subject

            # === V3-C: messageId-first dedup (优先), body[:200] fallback (兼容老数据) ===
            # Layer-1 hotfix (5/6 commit 848e83a) 用 body[:200] dedup 拦死循环, 但有 2 个边界:
            #   1. Zoho 渲染管线让前 200 字微变 → dedup 失效
            #   2. body 含动态时间戳/签名变量 → 同一封 email 第 2 次拉时 body 变了
            # V3-C (2026-05-08): 写"回复原文"时拼 [MID:{messageId}] token 前缀, dedup 时
            # 优先按 token 精确匹配; 兼容老数据 (没 token) 仍用 body[:200] 匹配。
            new_msg_token = f"[MID:{msg_id}]" if msg_id else ""
            new_body_key = (email_body or "")[:200].strip()
            already_seen = False
            if new_msg_token:
                already_seen = any(
                    new_msg_token in (ext(d["fields"].get("回复原文")) or "")
                    for d in all_matched
                )
            if not already_seen and new_body_key:
                # 拿出"回复原文", 剥掉可能的 [MID:xxx] / [OOO 自动回复] 前缀, 再比 200 字
                def _strip_token(s: str) -> str:
                    if s.startswith("[MID:") or s.startswith("[OOO "):
                        i = s.find("] ")
                        if i > 0:
                            return s[i + 2:]
                    return s
                already_seen = any(
                    _strip_token(ext(d["fields"].get("回复原文")) or "")[:200].strip() == new_body_key
                    for d in all_matched
                )
            if already_seen:
                print(f"[reply_monitor] dedup: skip {from_addr} (msgid={msg_id} body_head={new_body_key[:60]!r})")
                results.append({"brand": brand, "from": from_addr, "skipped": "duplicate_body"})
                continue

            # === OOO 自动回复检测 (在 AI 分类前) ===
            ooo_hit, ooo_frag = is_ooo(subject, email_body)
            if ooo_hit:
                # 标记草稿"是否回复=True" 但不分类、不通知、不生成回复草稿
                await feishu.update_record(config.T_DRAFT, draft["record_id"], {
                    "是否回复": True,
                    "回复日期": int(time.time() * 1000),
                    "回复意图": "不明意图",
                    "回复原文": (f"[OOO 自动回复] {email_body[:400]}")[:500],
                })
                # 写跟进记录但只标记不动主表合作状态
                cf = contact["fields"]
                if ctype == "editor":
                    await feishu.create_record(config.T_EDITOR_FU, {
                        "跟进摘要": f"[OOO 自动回复] {ooo_frag[:30]}",
                        "跟进日期": int(time.time() * 1000),
                        "跟进方式": "邮件",
                        "跟进内容": f"OOO 自动回复 (跳过自动回信, 等本人回来)\n命中: {ooo_frag}\n原文: {email_body[:400]}",
                        "关联媒体人": [contact["record_id"]],
                    })
                else:
                    await feishu.create_record(config.T_KOL_FU, {
                        "跟进摘要": f"[OOO 自动回复] {ooo_frag[:30]}",
                        "跟进日期": int(time.time() * 1000),
                        "跟进方式": "邮件",
                        "跟进内容": f"OOO 自动回复 (跳过自动回信, 等本人回来)\n命中: {ooo_frag}\n原文: {email_body[:400]}",
                        "关联KOL": [contact["record_id"]],
                    })
                processed += 1
                results.append({"brand": brand, "from": from_addr, "skipped": "OOO", "ooo_match": ooo_frag})
                continue  # 跳过分类/通知/生成草稿

            # 分类
            intent = await classify_intent(from_addr, subject, email_body)
            intent_type = intent.get("type", "不明意图")
            decline_reason = (intent.get("decline_reason") or "").strip()
            try:
                retry_days = int(intent.get("retry_days") or 0)
            except (ValueError, TypeError):
                retry_days = 0
            now_ms = int(time.time() * 1000)

            # 回写草稿 (V3-C: 前缀 [MID:xxx] token 让下一轮 dedup 走精确匹配)
            mid_prefix = f"[MID:{msg_id}] " if msg_id else ""
            # body 截到 460 让总长 ≤ 500 (Zoho msg_id ~19 位 + token wrap = 27 chars)
            draft_update = {
                "是否回复": True,
                "回复日期": now_ms,
                "回复意图": intent_type,
                "回复原文": (mid_prefix + (email_body or ""))[:500],
            }
            # P5.10 委婉拒绝原因分类 + 下次重发日期 (5 类含 不匹配_条件,V1.5 加)
            if intent_type == "委婉拒绝" and decline_reason in (
                "不匹配_品类", "不匹配_时机", "不匹配_方式", "不匹配_条件", "不感兴趣_其他"):
                draft_update["拒绝原因分类"] = decline_reason
                if retry_days > 0:
                    draft_update["下次重发日期"] = now_ms + retry_days * 86400 * 1000
            await feishu.update_record(config.T_DRAFT, draft["record_id"], draft_update)

            # 更新主表状态 + 跟进记录
            cf = contact["fields"]
            # 委婉拒绝 时构建主表 update payload (合作状态 + 下次重发日期)
            master_update = {}
            fu_feedback_extra = ""  # 跟进记录「客户反馈」追加结构化原因
            if ctype == "editor":
                new_status = INTENT_TO_STATUS_EDITOR.get(intent_type)
            else:
                new_status = INTENT_TO_STATUS_KOL.get(intent_type)
            if new_status:
                master_update["合作状态"] = new_status
            if intent_type == "委婉拒绝" and decline_reason:
                fu_feedback_extra = f"\n[拒绝原因: {decline_reason}]"
                if retry_days > 0:
                    master_update["下次重发日期"] = now_ms + retry_days * 86400 * 1000
                    fu_feedback_extra += f" [重发: {retry_days}d 后]"
                else:
                    fu_feedback_extra += " [重发: 永久不]"

            target_table = config.T_EDITOR if ctype == "editor" else config.T_KOL
            if master_update:
                await feishu.update_record(target_table, contact["record_id"], master_update)

            if ctype == "editor":
                await feishu.create_record(config.T_EDITOR_FU, {
                    "跟进摘要": f"[媒体人回复] {intent_type}: {intent.get('summary','')[:80]}",
                    "跟进日期": now_ms,
                    "跟进方式": "邮件",
                    "跟进内容": f"主题: {subject}\n\n意图: {intent_type} (置信度{intent.get('confidence',0):.2f}){fu_feedback_extra}\n\n原文:\n{email_body[:600]}",
                    "客户反馈": (intent.get("key_quote", "") + fu_feedback_extra)[:200],
                    "下一步行动": intent.get("suggested_action", "")[:200],
                    "关联媒体人": [contact["record_id"]],
                })
                source = ext(cf.get("主要媒体")) or ext(cf.get("所属媒体"))
                name = ext(cf.get("媒体人姓名"))
            else:
                await feishu.create_record(config.T_KOL_FU, {
                    "跟进摘要": f"[KOL回复] {intent_type}: {intent.get('summary','')[:80]}",
                    "跟进日期": now_ms,
                    "跟进方式": "邮件",
                    "跟进内容": f"主题: {subject}\n\n意图: {intent_type} (置信度{intent.get('confidence',0):.2f}){fu_feedback_extra}\n\n原文:\n{email_body[:600]}",
                    "客户反馈": (intent.get("key_quote", "") + fu_feedback_extra)[:200],
                    "下一步行动": intent.get("suggested_action", "")[:200],
                    "关联KOL": [contact["record_id"]],
                })
                source = ext(cf.get("主平台")) + f" {cf.get('粉丝数', 0):,} 粉"
                name = ext(cf.get("账号名"))

            # 通知
            contact_info = {
                "name": name, "source": source,
                "country": ext(cf.get("国家原文")) or ext(cf.get("国家")),
                "email": from_addr,
            }
            card = build_card(ctype, contact_info, brand, intent, subject)
            await notify_all(card, draft_rid=draft["record_id"])

            # === 自动生成回复草稿 (走 reviewer 自审通道) ===
            try:
                alias_for_brand = config.BRAND_CONFIG[brand]["alias_from"]
                reply_rid = await reply_drafter.draft_reply(
                    contact_record=contact,
                    contact_type=ctype,
                    brand=brand,
                    intent_type=intent_type,
                    intent_summary=intent.get("summary", ""),
                    original_subject=subject,
                    original_body=email_body,
                    sender_alias=alias_for_brand,
                    related_draft_id=draft["record_id"],
                )
                if reply_rid:
                    print(f"[reply_monitor] reply draft generated rid={reply_rid}")
            except Exception as e:
                print(f"[reply_monitor] draft_reply fail: {e}")

            processed += 1

    return {"processed": processed, "results": results}

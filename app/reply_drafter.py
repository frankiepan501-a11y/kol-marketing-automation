"""根据回复意图生成回复草稿

4 档处理:
- 退订 → 标准模板,极简,自动发
- 委婉拒绝 → 标准模板,礼貌闭环,自动发
- 要资料 → 模板 + 产品官网链接,自动发
- 要报价 → DeepSeek 生成 + 强制 committed=True (必走人审)
- 不明意图 → DeepSeek 生成澄清问题草稿

写入「外联草稿」表 (草稿来源=reply),然后调 draft_router 走自审通道。
"""
import time
from typing import Optional
from . import config, feishu, deepseek
from .feishu import ext, xrid


# ===== 模板 (中性礼貌) =====
TEMPLATE_UNSUBSCRIBE = {
    "subject_prefix": "Re: ",
    "body": (
        "Hi {first_name},\n\n"
        "Got it — I've removed you from our outreach list. No worries at all, "
        "and thanks for letting me know.\n\n"
        "Wishing you all the best with your channel/work!\n\n"
        "Best,\n{signature}"
    ),
}

TEMPLATE_DECLINE = {
    "subject_prefix": "Re: ",
    "body": (
        "Hi {first_name},\n\n"
        "Totally understand — appreciate you taking the time to reply. If anything "
        "changes down the road, feel free to drop us a line anytime.\n\n"
        "Wishing you continued success!\n\n"
        "Best,\n{signature}"
    ),
}

TEMPLATE_SEND_ASSETS = {
    "subject_prefix": "Re: ",
    "body": (
        "Hi {first_name},\n\n"
        "Thanks for your interest! Here's a quick overview of {product_name}:\n"
        "{product_link}\n\n"
        "Happy to answer any other questions you have, or jump on a quick call "
        "if that's easier.\n\n"
        "Best,\n{signature}"
    ),
}


def _first_name(full_name: str) -> str:
    if not full_name: return "there"
    parts = full_name.strip().split()
    if not parts: return "there"
    return parts[0][:30]


def _sender_signature(brand: str) -> str:
    return {
        "POWKONG": "Frankie\nPOWKONG Partnerships",
        "FUNLAB": "Frankie\nFUNLAB Creator Team",
    }.get(brand.upper(), "Frankie\nPartnerships")


async def _gen_quote_draft(contact_name: str, original_subject: str,
                            original_body: str, intent_summary: str, brand: str,
                            product_name: str, product_link: str) -> dict:
    """要报价/谈条款类 → DeepSeek 生成回复 (会被 reviewer 标 committed=True 强制人审)"""
    sig = _sender_signature(brand)
    prompt = f"""一位 KOL/编辑回复了我们的 cold email,他在询问商务条款 (报价/佣金/寄样数量等)。
你需要生成一封**专业、简洁、不做具体数字承诺**的回复草稿,等运营审核改细节后发送。

【对方回信摘要】
意图: {intent_summary}
原主题: {original_subject}
原文 (前 500 字): {original_body[:500]}

【约束】
- 80-150 词
- 主题: "Re: " + 原主题 (复用)
- **不要写具体数字** (如 10% commission / $50 unit / MOQ 500): 这些由运营手填
- 用占位符 [TBD by ops] 代替具体数字
- 称呼: Hi {_first_name(contact_name)}, (用对方名字)
- 结尾签名: {sig}
- 品牌 voice: {brand}
- 主推产品提一下: {product_name}
- 不需要附产品链接 (运营会加)

【返回 JSON】
{{"subject": "...", "body": "..."}}
"""
    try:
        r = await deepseek.chat_json(prompt, max_tokens=500, temperature=0.2)
        return {
            "subject": r.get("subject", f"Re: {original_subject}")[:200],
            "body": r.get("body", ""),
        }
    except Exception as e:
        return {
            "subject": f"Re: {original_subject}",
            "body": (f"Hi {_first_name(contact_name)},\n\n"
                     f"Thanks for the interest! Let me circle back with our team on "
                     f"specifics around [TBD by ops] and get you a clear proposal.\n\n"
                     f"Best,\n{sig}\n\n[AI 错误: {str(e)[:50]}]"),
        }


async def _gen_clarify_draft(contact_name: str, original_subject: str,
                              original_body: str, intent_summary: str, brand: str) -> dict:
    """不明意图 → DeepSeek 生成澄清问题草稿"""
    sig = _sender_signature(brand)
    prompt = f"""一位 KOL/编辑给我们 cold email 回复了一段话, 但意图模糊。
请生成一封简短回复, 礼貌追问对方实际诉求, 让对方简短确认我们下一步该怎么做。

【对方回信摘要】
意图分类: {intent_summary}
原主题: {original_subject}
原文 (前 400 字): {original_body[:400]}

【约束】
- 50-80 词
- 主题: "Re: " + 原主题
- 提 2-3 个具体选项让对方挑 (如 "Would you like a sample / press kit / quick call?")
- 称呼: Hi {_first_name(contact_name)},
- 结尾签名: {sig}
- 品牌: {brand}

【返回 JSON】
{{"subject": "...", "body": "..."}}"""
    try:
        r = await deepseek.chat_json(prompt, max_tokens=400, temperature=0.2)
        return {
            "subject": r.get("subject", f"Re: {original_subject}")[:200],
            "body": r.get("body", ""),
        }
    except Exception as e:
        return {
            "subject": f"Re: {original_subject}",
            "body": (f"Hi {_first_name(contact_name)},\n\n"
                     f"Thanks for getting back! Just to make sure I help in the right way — "
                     f"would a quick call, a sample unit, or just our press kit be most useful?\n\n"
                     f"Best,\n{sig}\n\n[AI 错误: {str(e)[:50]}]"),
        }


async def draft_reply(
    contact_record: dict,
    contact_type: str,             # KOL / editor
    brand: str,                    # POWKONG / FUNLAB
    intent_type: str,              # 感兴趣/要报价/委婉拒绝/退订/不明意图
    intent_summary: str,
    original_subject: str,
    original_body: str,
    sender_alias: str,
    related_draft_id: Optional[str] = None,
) -> Optional[str]:
    """
    生成 reply 草稿 → 写入「外联草稿」 → 调 router 走自审

    Returns:
        新建的草稿 record_id (如已生成); None 如果意图不需要回复
    """
    cf = contact_record["fields"]
    if contact_type == "editor":
        contact_name = ext(cf.get("编辑姓名"))
        link_field = "关联编辑"
    else:
        contact_name = ext(cf.get("账号名"))
        link_field = "关联KOL"

    # 默认产品占位 (从原始草稿继承 关联产品 — 暂用"主推产品"占位)
    product_name = "our latest product"
    product_link = ""
    # 试图从 related_draft 拿产品名 (如有)
    if related_draft_id:
        try:
            related = await feishu.get_record(config.T_DRAFT, related_draft_id)
            rf = related["fields"]
            prod_rid = xrid(rf.get("关联产品"))
            if prod_rid:
                prod = await feishu.get_record(config.T_PRODUCT, prod_rid)
                pf = prod["fields"]
                product_name = ext(pf.get("产品名")) or product_name
                product_link = ext(pf.get("官网链接")) or ""
        except Exception as e:
            print(f"[reply_drafter] fetch related product fail: {e}")

    sig_first = "Frankie"
    sig_full = _sender_signature(brand)
    first = _first_name(contact_name)

    # 4 档分发
    subj = ""
    body = ""
    if intent_type == "退订":
        subj = "Re: " + original_subject[:150]
        body = TEMPLATE_UNSUBSCRIBE["body"].format(first_name=first, signature=sig_full)
    elif intent_type == "委婉拒绝":
        subj = "Re: " + original_subject[:150]
        body = TEMPLATE_DECLINE["body"].format(first_name=first, signature=sig_full)
    elif intent_type == "感兴趣":
        # "感兴趣" 通常对应"要资料/产品图" → 模板自动发
        if not product_link:
            product_link = "(see attached overview)"
        subj = "Re: " + original_subject[:150]
        body = TEMPLATE_SEND_ASSETS["body"].format(
            first_name=first, signature=sig_full,
            product_name=product_name, product_link=product_link,
        )
    elif intent_type == "要报价":
        d = await _gen_quote_draft(contact_name, original_subject, original_body,
                                    intent_summary, brand, product_name, product_link)
        subj = d["subject"]
        body = d["body"]
    elif intent_type == "不明意图":
        d = await _gen_clarify_draft(contact_name, original_subject, original_body,
                                      intent_summary, brand)
        subj = d["subject"]
        body = d["body"]
    else:
        return None  # 不识别的意图,跳过

    # 写入「外联草稿」
    now_ms = int(time.time() * 1000)
    fields = {
        "草稿ID": f"reply-{contact_record['record_id'][-8:]}-{int(time.time())}",
        link_field: [contact_record["record_id"]],
        "邮件主题": subj[:200],
        "邮件正文": body,
        "邮件语言": "en",  # reply 全英
        "草稿状态": "待审",     # router 会改成 自动通过 / 待审
        "草稿来源": "reply",
        "对象类型": contact_type if contact_type == "KOL" else "编辑",
        "发送邮箱": sender_alias,
        "发送人署名": sig_first,
        "生成时间": now_ms,
        "建议发送时间": now_ms,  # reply 立即发,不调度
        "重生次数": 0,
        # 收件人
        "收件邮箱": ext(cf.get("邮箱")) or "",
    }
    rid = await feishu.create_record(config.T_DRAFT, fields)
    print(f"[reply_drafter] created draft rid={rid} intent={intent_type}")

    # 调 router (惰性 import 防循环)
    try:
        from . import draft_router
        result = await draft_router.route_draft(rid)
        print(f"[reply_drafter] router result: score={result['score']} path={result['path']}")
    except Exception as e:
        print(f"[reply_drafter] router fail: {e}")

    return rid

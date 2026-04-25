"""根据回复意图生成回复草稿

意图分流:
- 退订 → 模板, 自动发
- 委婉拒绝 → 模板, 自动发
- 感兴趣 → 调 AI 子分类:
    * ship_confirm (已含寄送地址) → 模板含 product_name, **强制人审** (committed=True)
    * send_assets (要资料/介绍) → 模板带产品链接, 自动发
    * schedule_call (想视频会议) → 模板带 calendly 链接, 自动发
    * general (泛泛感兴趣) → AI 生成开放式问题, 自动发
- 要报价 → DeepSeek 生成 + 强制 committed=True (必走人审)
- 不明意图 → DeepSeek 生成澄清问题草稿

写入「KOL·媒体人邮件草稿」表 (邮件草稿来源=reply), 然后调 draft_router 走自审通道。
"""
import time
import re
from typing import Optional
from . import config, feishu, deepseek
from .feishu import ext, xrid


# ===== 模板 (中性礼貌) =====
TEMPLATE_UNSUBSCRIBE = (
    "Hi {first_name},\n\n"
    "Got it — I've removed you from our outreach list. No worries at all, "
    "and thanks for letting me know.\n\n"
    "Wishing you all the best with your channel/work!\n\n"
    "Best,\n{signature}"
)

TEMPLATE_DECLINE = (
    "Hi {first_name},\n\n"
    "Totally understand — appreciate you taking the time to reply. If anything "
    "changes down the road, feel free to drop us a line anytime.\n\n"
    "Wishing you continued success!\n\n"
    "Best,\n{signature}"
)

TEMPLATE_SEND_ASSETS = (
    "Hi {first_name},\n\n"
    "Thanks for your interest! Here's a quick overview of {product_name}:\n"
    "{product_link}\n\n"
    "Happy to answer any other questions you have, or jump on a quick call "
    "if that's easier.\n\n"
    "Best,\n{signature}"
)

TEMPLATE_SHIP_CONFIRM = (
    "Hi {first_name},\n\n"
    "Awesome — got the address! I'm coordinating with our team to send the "
    "{product_name} your way. I'll confirm the shipping details and shoot you "
    "a tracking number once it's on its way.\n\n"
    "If you have any specific angles or formats in mind for the content, "
    "feel free to share — happy to flex on what we send.\n\n"
    "Best,\n{signature}"
)

TEMPLATE_SCHEDULE_CALL = (
    "Hi {first_name},\n\n"
    "Sounds great — happy to jump on a quick call. Here's my calendar so you "
    "can grab a slot that works for you:\n"
    "{calendly_link}\n\n"
    "Or if email's easier, just shoot me your top 2-3 questions and I'll "
    "answer in the thread.\n\n"
    "Best,\n{signature}"
)

# 默认 calendly (品牌共用,后续可分品牌)
CALENDLY_DEFAULT = "https://calendly.com/frankie-pan-funlab/30min"


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


# ===== 感兴趣子类 AI 判断 =====
async def _classify_interest(original_body: str) -> dict:
    """对方回复"感兴趣"时, 进一步细分子类.
    Returns {"sub": "ship_confirm|send_assets|schedule_call|general",
             "confidence": 0.0-1.0,
             "extracted_address": "...",  # 仅 ship_confirm 命中时填
             "reason": "..."}
    """
    prompt = f"""一位 KOL/媒体人回复了我们的 cold email, 表达了感兴趣。
请判断他的具体诉求属于以下哪一档:

【4 个子类】
- ship_confirm: 想直接收到产品实物, 通常会含**邮寄地址** (姓名+街道+城市+国家/邮编), 或明确说"please send to..."/"my address is..."
- send_assets: 想要更多产品资料 / PDF / 详细介绍 / 高清图 / 产品对比, 还没准备好接收实物
- schedule_call: 想要视频会议 / 电话沟通 / Zoom / Meet / Google call
- general: 表达了感兴趣但没具体诉求 (如 "Sounds cool, tell me more!" / "Interesting!"), 需要追问

【对方回信原文 (前 800 字)】
{original_body[:800]}

返回 JSON:
{{
  "sub": "ship_confirm|send_assets|schedule_call|general",
  "confidence": 0.0-1.0,
  "extracted_address": "如果是 ship_confirm, 把识别到的完整地址原文截出来 (姓名+街道+城市+州+邮编+国家); 否则留空",
  "country_code": "如有地址, 推断出 ISO 国家代码 (US/UK/DE/JP/CA/AU/FR/ES/IT/NL/BR/MX 等); 否则留空",
  "reason": "20 字以内说明判断依据"
}}"""
    try:
        r = await deepseek.chat_json(prompt, max_tokens=400, temperature=0.0)
        sub = r.get("sub", "general")
        if sub not in ("ship_confirm", "send_assets", "schedule_call", "general"):
            sub = "general"
        return {
            "sub": sub,
            "confidence": float(r.get("confidence", 0.5) or 0.5),
            "extracted_address": (r.get("extracted_address") or "").strip()[:500],
            "country_code": (r.get("country_code") or "").strip().upper()[:5],
            "reason": (r.get("reason") or "")[:80],
        }
    except Exception as e:
        return {"sub": "general", "confidence": 0.0, "extracted_address": "",
                "country_code": "", "reason": f"AI 错误: {str(e)[:50]}"}


async def _gen_general_interest_draft(contact_name: str, original_subject: str,
                                       original_body: str, brand: str,
                                       product_name: str, product_link: str) -> dict:
    """泛泛感兴趣 → AI 生成开放式问题草稿"""
    sig = _sender_signature(brand)
    prompt = f"""一位 KOL/媒体人回复了我们的 cold email, 说"感兴趣"但没明确诉求 (如 "Sounds cool!"/"Interested, tell me more")。
请生成一封简短回复, 礼貌追问对方实际想要什么 (产品介绍 / 寄样 / 视频会议 / 报价等), 让对方挑一项。

【对方回信原文 (前 400 字)】
{original_body[:400]}

【约束】
- 60-90 词
- 主题: "Re: " + 原主题 (复用)
- 称呼: Hi {_first_name(contact_name)},
- 产品: {product_name}
- 提 3 个具体选项让对方挑 (sample / press kit / quick call)
- 结尾签名: {sig}
- 品牌: {brand}

返回 JSON:
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
                     f"Thanks — happy you're interested! To make sure I send the right thing, "
                     f"would any of these be most useful: a sample {product_name}, our "
                     f"press kit, or a quick 15-min call?\n\n"
                     f"Best,\n{sig}\n\n[AI 错误: {str(e)[:50]}]"),
        }


async def _gen_quote_draft(contact_name: str, original_subject: str,
                            original_body: str, intent_summary: str, brand: str,
                            product_name: str, product_link: str) -> dict:
    """要报价/谈条款类 → DeepSeek 生成回复 (会被 reviewer 标 committed=True 强制人审)"""
    sig = _sender_signature(brand)
    prompt = f"""一位 KOL/媒体人回复了我们的 cold email,他在询问商务条款 (报价/佣金/寄样数量等)。
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
    prompt = f"""一位 KOL/媒体人给我们 cold email 回复了一段话, 但意图模糊。
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
    生成 reply 草稿 → 写入「KOL·媒体人邮件草稿」 → 调 router 走自审

    Returns:
        新建的草稿 record_id (如已生成); None 如果意图不需要回复
    """
    cf = contact_record["fields"]
    if contact_type == "editor":
        contact_name = ext(cf.get("媒体人姓名"))
        link_field = "关联媒体人"
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

    # 子分类元信息 (仅 ship_confirm 用到)
    sub = ""
    extracted_address = ""
    country_code = ""

    # 意图分发
    subj = ""
    body = ""
    if intent_type == "退订":
        subj = "Re: " + original_subject[:150]
        body = TEMPLATE_UNSUBSCRIBE.format(first_name=first, signature=sig_full)
    elif intent_type == "委婉拒绝":
        subj = "Re: " + original_subject[:150]
        body = TEMPLATE_DECLINE.format(first_name=first, signature=sig_full)
    elif intent_type == "感兴趣":
        # 子分类细分
        sub_info = await _classify_interest(original_body)
        sub = sub_info["sub"]
        extracted_address = sub_info["extracted_address"]
        country_code = sub_info["country_code"]

        subj = "Re: " + original_subject[:150]
        if sub == "ship_confirm":
            body = TEMPLATE_SHIP_CONFIRM.format(
                first_name=first, signature=sig_full,
                product_name=product_name,
            )
        elif sub == "schedule_call":
            body = TEMPLATE_SCHEDULE_CALL.format(
                first_name=first, signature=sig_full,
                calendly_link=CALENDLY_DEFAULT,
            )
        elif sub == "send_assets":
            body = TEMPLATE_SEND_ASSETS.format(
                first_name=first, signature=sig_full,
                product_name=product_name,
                product_link=product_link or "(I'll send the deck shortly)",
            )
        else:  # general
            d = await _gen_general_interest_draft(contact_name, original_subject,
                                                   original_body, brand,
                                                   product_name, product_link)
            subj = d["subject"]
            body = d["body"]
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

    # 写入「KOL·媒体人邮件草稿」
    now_ms = int(time.time() * 1000)
    # ship_confirm 的元信息存到「匹配亮点」字段(临时复用,后续可加专用字段)
    extras = ""
    if sub == "ship_confirm" and extracted_address:
        extras = f"[ship_confirm] country={country_code} | address={extracted_address[:300]}"

    fields = {
        "邮件草稿ID": f"reply-{contact_record['record_id'][-8:]}-{int(time.time())}",
        link_field: [contact_record["record_id"]],
        "邮件主题": subj[:200],
        "邮件正文": body,
        "邮件语言": "en",
        "邮件草稿状态": "待审",
        "邮件草稿来源": "reply",
        "对象类型": contact_type if contact_type == "KOL" else "媒体人",
        "发送邮箱": sender_alias,
        "发送人署名": sig_first,
        "生成时间": now_ms,
        "建议发送时间": now_ms,
        "重生次数": 0,
        "收件邮箱": ext(cf.get("邮箱")) or "",
    }
    if extras:
        fields["匹配亮点"] = extras[:500]   # 临时承载寄样元信息

    rid = await feishu.create_record(config.T_DRAFT, fields)
    print(f"[reply_drafter] created draft rid={rid} intent={intent_type} sub={sub}")

    # ship_confirm 强制 committed=True → 走人审 (即便 score 高)
    # 通过预先写"承诺命中=True"+"命中关键词" 让 router 必走人审分支
    if sub == "ship_confirm":
        try:
            await feishu.update_record(config.T_DRAFT, rid, {
                "承诺命中": True,
                "命中关键词": "ship-sample (subclass)",
            })
        except Exception as e:
            print(f"[reply_drafter] mark ship_confirm committed fail: {e}")

    # 调 router (惰性 import 防循环)
    try:
        from . import draft_router
        # 给 router 传 ship_confirm 元信息, 让通知卡片含发货建议
        result = await draft_router.route_draft(
            rid,
            ship_confirm_meta={"address": extracted_address, "country": country_code,
                                 "product_name": product_name} if sub == "ship_confirm" else None,
        )
        print(f"[reply_drafter] router result: score={result['score']} path={result['path']}")
    except Exception as e:
        print(f"[reply_drafter] router fail: {e}")

    return rid

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
- 质疑/澄清 → DeepSeek 生成"先承认错误再切入"草稿, **强制人审** (Ashtvn 反例)
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
    "Awesome — got the address! Your {product_name} is on its way via "
    "[CARRIER 待填运营修改] — ETA roughly [ETA 待填].\n\n"
    "I'll send the tracking number in a quick follow-up once it's assigned "
    "(typically within 24 hours of dispatch).\n\n"
    "If you have any specific angles or formats in mind for the content, "
    "feel free to share — happy to flex on what we send.\n\n"
    "Best,\n{signature}"
)

# 第 2 封追加运单号 (auto_send 在第 1 封发出后自动建草稿, 待运营填运单号)
TEMPLATE_TRACKING_FOLLOWUP = (
    "Hi {first_name},\n\n"
    "Quick update — here's the tracking number for your {product_name} sample:\n\n"
    "**[TRACKING# 待填运营修改]** (via [CARRIER 待填运营修改])\n\n"
    "Should arrive in the next [ETA 待填]. Excited to hear what you think — "
    "feel free to drop a line if anything comes up after it lands.\n\n"
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
def _is_real_address(s: str) -> bool:
    """正则验证 ship_confirm 提取的"地址"是否是真实邮寄地址(防 AI 把签名当地址).

    bug 案例 (2026-04-28 1upBinge): AI 把 "Kyle J. Beauregard, Director of Programming,
    www.wickedbinge.com" 当成地址提取出来, 触发 ship_confirm 模板"got the address!"。

    真实邮寄地址必须含:
    1. 街道门牌号 (开头 1-5 位数字 + 街道名) OR 邮编 (US 5 位 / UK 字母数字混合 / 加拿大 A1A 1A1 等)
    2. 街道关键词 (Street/St/Ave/Boulevard/Road/Rd/Lane/Drive/Way/Court/Plaza/Suite/Apt 等)
    3. 长度 >= 30 字符

    签名特征 (排除):
    - 含 URL (www./http/.com/.net/.io)
    - 含 email (@gmail/@outlook/@yahoo/@xxx.com)
    - 只有姓名+职位+网站 (Director|Manager|CEO|Founder|Editor|Producer|Programming + URL)
    """
    if not s or len(s) < 30:
        return False
    s_lower = s.lower()

    # 排除签名特征
    if any(x in s_lower for x in ["www.", "http://", "https://"]):
        return False
    # 排除职位 + 网站组合 (典型签名)
    title_kws = ["director", "manager", "ceo", "founder", "editor", "producer",
                 "programming", "marketing", "partnerships", "operations"]
    if any(t in s_lower for t in title_kws) and any(x in s_lower for x in [".com", ".net", ".org", ".io", ".tv", ".gg"]):
        return False

    # 必须含街道关键词
    street_kw_pattern = r"\b(street|st\.?|avenue|ave\.?|boulevard|blvd\.?|road|rd\.?|lane|ln\.?|drive|dr\.?|way|court|ct\.?|plaza|place|pl\.?|terrace|circle|cir\.?|parkway|pkwy|highway|hwy|suite|ste\.?|apt\.?|unit|floor|building|bldg)\b"
    has_street_kw = bool(re.search(street_kw_pattern, s_lower))

    # 邮编模式: US 5 digits / US 5+4 / UK / 加拿大 / 5位欧洲邮编
    zip_patterns = [
        r"\b\d{5}(-\d{4})?\b",                              # US ZIP
        r"\b[A-Z]\d[A-Z]\s?\d[A-Z]\d\b",                    # 加拿大
        r"\b[A-Z]{1,2}\d[A-Z\d]?\s?\d[A-Z]{2}\b",           # UK
        r"\b\d{4,5}\b",                                      # 通用 4-5 位邮编
    ]
    has_zip = any(bool(re.search(p, s)) for p in zip_patterns)

    # 街道门牌号(开头数字 + 字母街道名)
    has_street_num = bool(re.search(r"\b\d{1,5}\s+[A-Z][a-zA-Z]+", s))

    return has_street_kw and (has_zip or has_street_num)


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
- ship_confirm: **必须满足两个条件**: ① 对方明确说想要实物 / "please send to..." / "my address is..." ② **回信原文里提供了具体邮寄地址 (含街道门牌号 + 城市 + 邮编)**。
  - ✗ **签名 (姓名+职位+公司网站)** 不算地址,例: "Kyle J. Beauregard, Director of Programming, www.wickedbinge.com" 是签名,不是地址
  - ✗ 仅说 "Would love to check it out" / "Looks awesome" 没给地址 → 走 general
  - ✓ 例: "Send to: 123 Main St, Apt 4B, Brooklyn NY 11201, USA"
  - ✓ 例: "My address is 5-10-1 Shibuya, Shibuya-ku, Tokyo 150-0002, Japan"
- send_assets: 想要更多产品资料 / PDF / 详细介绍 / 高清图 / 产品对比, 还没准备好接收实物
- schedule_call: 想要视频会议 / 电话沟通 / Zoom / Meet / Google call
- general: 表达了感兴趣但没具体诉求 (如 "Sounds cool, tell me more!" / "Interesting!" / "Looks awesome, would love to check it out"), 需要追问寄送地址

【判断口诀】
- 看到 "looks awesome" / "would love to" 等没明示地址 → general (不是 ship_confirm!)
- 看到完整邮寄地址(街道+城市+邮编)→ ship_confirm
- 看到只是签名 → 不算地址, 按其他线索判断子类

【对方回信原文 (前 800 字)】
{original_body[:800]}

返回 JSON:
{{
  "sub": "ship_confirm|send_assets|schedule_call|general",
  "confidence": 0.0-1.0,
  "extracted_address": "**只有真实邮寄地址才填**(必须含街道门牌号+邮编),签名/职位/网站绝对不算; 否则留空",
  "country_code": "如有真实地址, 推断出 ISO 国家代码 (US/UK/DE/JP/CA/AU/FR/ES/IT/NL/BR/MX 等); 否则留空",
  "reason": "20 字以内说明判断依据"
}}"""
    try:
        r = await deepseek.chat_json(prompt, max_tokens=400, temperature=0.0)
        sub = r.get("sub", "general")
        if sub not in ("ship_confirm", "send_assets", "schedule_call", "general"):
            sub = "general"
        extracted_address = (r.get("extracted_address") or "").strip()[:500]

        # 后处理:即便 AI 判 ship_confirm,正则验证地址是否真实(防签名误识)
        if sub == "ship_confirm" and not _is_real_address(extracted_address):
            print(f"[reply_drafter] ship_confirm 降级 general: 地址正则校验失败 raw='{extracted_address[:120]}'")
            sub = "general"
            extracted_address = ""

        return {
            "sub": sub,
            "confidence": float(r.get("confidence", 0.5) or 0.5),
            "extracted_address": extracted_address,
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


async def _gen_misspoke_apology_draft(contact_name: str, original_subject: str,
                                        original_body: str, intent_summary: str, brand: str,
                                        product_name: str, product_link: str) -> dict:
    """质疑/澄清 → DeepSeek 生成"先承认错误再切入"草稿 (Ashtvn 反例)

    KOL 在纠正我们 cold email 里的具体说法 (e.g. "I've never made a PC gaming setup video")。
    禁止: 装没看见错误 / 直接问要不要寄样 / 双倍下注重申原描述。
    必须: 简短承认错误 / 不重复错误细节 / 用通用语气重新介绍产品价值 / 不催地址。
    """
    sig = _sender_signature(brand)
    prompt = f"""一位 KOL/媒体人回复了我们的 cold email,**纠正了我们对他的描述错误** (我们 cold email 里编造或写错了他的具体作品/视频/频道方向)。

【对方回信摘要】
意图: 质疑/澄清 - {intent_summary}
原主题: {original_subject}
原文 (前 500 字): {original_body[:500]}

【我们要做什么】
对方在用客气语气打脸我们。这是公关风险时刻。请生成一封**短、真诚、不卑不亢、不重复错误**的回复:
1. 第一句简短承认 (e.g. "Apologies for the mix-up — that's on me.")
2. 不要再次提原 cold email 里那个错误描述 (例: 如果对方说"I never made a PC gaming setup video",我们的回复**绝对不能再出现"PC gaming setup video"或类似措辞**)
3. 用一句通用话术重新介绍产品价值 (产品名: {product_name}),不绑定具体内容方向
4. **不要追问寄送地址、不要问 "would you like a sample?"** (让人审决定下一步)
5. 留一个开放钩子让对方自己挑下一步 (e.g. "If it sounds like a fit for what you actually cover, happy to share more.")

【约束】
- 70-110 词
- 主题: "Re: " + 原主题 (复用)
- 称呼: Hi {_first_name(contact_name)},
- 结尾签名: {sig}
- 品牌 voice: {brand}

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
                     f"Apologies for the mix-up — that's on me. Let me reset: {product_name} "
                     f"is something we built for the broader retro/Switch community, and I'd "
                     f"love your honest take if it sounds like a fit for what you actually cover.\n\n"
                     f"No pressure either way.\n\n"
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
    intent_type: str,              # 感兴趣/要报价/委婉拒绝/退订/质疑/澄清/不明意图
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
                # 海外营销邮件: 优先用「产品英文名」, 没填则降级用「产品名」剥前缀 + 告警
                p_en = ext(pf.get("产品英文名"))
                if p_en:
                    product_name = p_en
                else:
                    p_raw = ext(pf.get("产品名"))
                    p_clean = re.sub(r'^[A-Z]{1,4}\d{1,4}\s*[-_·]?\s*', '', p_raw).strip() or p_raw
                    m = re.match(r'^[A-Z]{2,5}[-_]?[A-Z0-9]{1,5}([-_][A-Z0-9]+)+\s*', p_clean)
                    if m: p_clean = p_clean[m.end():].strip() or p_raw
                    product_name = p_clean or product_name
                    print(f"[WARN] 产品 {prod_rid} 缺少「产品英文名」字段, 降级用中文名: {product_name}")
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
    elif intent_type == "质疑/澄清":
        d = await _gen_misspoke_apology_draft(contact_name, original_subject, original_body,
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

    # 拿 related_draft 的关联产品 record_id (用于关联到新 reply 草稿)
    related_prod_rid = None
    if related_draft_id:
        try:
            related_full = await feishu.get_record(config.T_DRAFT, related_draft_id)
            related_prod_rid = xrid(related_full["fields"].get("关联产品"))
        except Exception:
            pass

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
    if related_prod_rid:
        fields["关联产品"] = [related_prod_rid]
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

    # 质疑/澄清 强制 committed=True → 必走人审 (Ashtvn 反例)
    # KOL 在纠错, 不能让 AI 自动发 (哪怕 score 高也不行)
    if intent_type == "质疑/澄清":
        try:
            await feishu.update_record(config.T_DRAFT, rid, {
                "承诺命中": True,
                "命中关键词": "misspoke-correction (intent=质疑/澄清)",
            })
        except Exception as e:
            print(f"[reply_drafter] mark 质疑/澄清 committed fail: {e}")

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

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
    "Awesome — got the address! Your {product_name} is on its way:\n\n"
    "Tracking #: [TRACKING# 待填运营修改]\n"
    "Carrier: [CARRIER 待填运营修改]\n\n"
    "Should arrive in the next few days — feel free to drop a line "
    "once it lands.\n\n"
    "If you have any specific angles or formats in mind for the content, "
    "happy to flex on what we send.\n\n"
    "Best,\n{signature}"
)

# 2026-06-02 Fix B: 旧回复唤醒轻预热. recon 把 KOL 几个月前给过地址的旧回复翻出来 → 不直接"got the
# address, shipping!"(唐突, mrbrian 反馈), 改先确认现在是否仍感兴趣, 再寄. 不含运单号占位符(非寄样卡),
# 不预填寄样字段; 强制人审, 运营可改成寄样或继续预热.
TEMPLATE_STALE_REWARM = (
    "Hi {first_name},\n\n"
    "Circling back on the {product_name} — it's been a little while since we "
    "last connected, so I wanted to check in before sending anything over.\n\n"
    "Are you still keen to give it a try? If so, just reply to confirm and I'll "
    "get it shipped out to you right away (feel free to re-share your current "
    "address so we send to the right place).\n\n"
    "No pressure at all if the timing isn't right anymore — just let me know "
    "either way.\n\n"
    "Best,\n{signature}"
)

TEMPLATE_NEED_ADDRESS = (
    "Hi {first_name},\n\n"
    "Awesome — happy to send a {product_name} sample your way!\n\n"
    "Could you reply with your shipping details? Just need:\n"
    "- Full name\n"
    "- Street address (incl. apt/suite if any)\n"
    "- City, State/Region, ZIP\n"
    "- Country\n"
    "- Phone number for delivery\n\n"
    "Once I have that I'll get it shipped and send the tracking number as "
    "soon as it's on its way.\n\n"
    "Best,\n{signature}"
)

# P5.11 要报价场景: KOL 主动来询价 → 不直接进商务谈判, 先邀请联盟模式
# 策略: 80% KOL 看到联盟邀请会接受 (按效果分成,风险低); 拒绝才转人审给 Frankie 拍板
# 不提佣金比例 (Frankie 决策: 不引导 KOL 期待数字),不留"如果你坚持付费"退路
TEMPLATE_AFFILIATE_INVITATION_QUOTE = (
    "Hi {first_name},\n\n"
    "Thanks for reaching out about pricing!\n\n"
    "Our standard collaboration model with creators is affiliate-based rather "
    "than upfront fees: we provide you a dedicated product link + an exclusive "
    "discount code for your audience. When viewers order through your link, "
    "they get the discount and you earn commission on the sales — typically "
    "much higher upside than a flat fee for high-converting creators.\n\n"
    "For {product_name} (independent site price ${product_price}), would you "
    "be open to trying this model?\n\n"
    "If yes, I'd love to send a sample your way to start — could you reply "
    "with shipping details:\n\n"
    "- Full name\n"
    "- Street address (incl. apt/suite if any)\n"
    "- City, State/Region, ZIP\n"
    "- Country\n"
    "- Phone number for delivery\n\n"
    "Best,\n{signature}"
)

# YouTube Short-only KOL 专用 (其他平台不用此模板, 因为只有 YT normal video 能挂链接)
# 一封信 3 合 1: 接受 Short 寄样 + 提议 normal video 联盟模式 + 求地址
# 佣金比例/折扣码不在此模板写死, 等 KOL 同意进 ship_confirm 后人审定
TEMPLATE_AFFILIATE_UPSELL = (
    "Hi {first_name},\n\n"
    "A YouTube Short works great for us, and we'd be happy to send you a "
    "{product_name} sample for that.\n\n"
    "For a normal YouTube video, we usually work with creators through an "
    "affiliate commission model. We can provide you with a dedicated product "
    "link and an exclusive discount for your audience. When your viewers "
    "order through your link, they get the discount, and you earn commission "
    "from the sales.\n\n"
    "Would you be open to doing both a YouTube Short and a normal YouTube "
    "video under this setup?\n\n"
    "If that works for you, could you send over your shipping details?\n\n"
    "- Full name\n"
    "- Street address (incl. apt/suite if any)\n"
    "- City, State/Region, ZIP\n"
    "- Country\n"
    "- Phone number for delivery\n\n"
    "Best,\n{signature}"
)

# +7d 内容提醒 (寄样签收后 7 天没动静,主动 ping)
TEMPLATE_CONTENT_REMINDER = (
    "Hi {first_name},\n\n"
    "Hope the {product_name} arrived safely! Just a friendly nudge — would love "
    "to hear your first impressions when you have a chance.\n\n"
    "If you're working on a video / post around it, feel free to drop the link "
    "or a draft thumbnail my way — happy to amplify on our end too.\n\n"
    "Anything we can do to make it easier?\n\n"
    "Best,\n{signature}"
)

# P4 软关怀 nudge (寄样后 brief 重设计): 暖信(P3)发出 +N 天仍无上稿 → 一封"轻关怀"邮件.
# 与已下线的 L2 催稿(TEMPLATE_CONTENT_REMINDER, 含"friendly nudge/first impressions")的关键区别:
# 这封**不问"发了没"**, 不暗示交付义务; 主动提供帮助 + 反复强调"no rush / no pressure / 不是
# 现在也完全 OK", 是关系维护不是催稿. 无任何占位符 (auto_send 占位符闸门直接通过).
TEMPLATE_SOFT_NUDGE = (
    "Hi {first_name},\n\n"
    "Just checking in — hope you've been enjoying the {product_name}! "
    "No rush at all on anything; we know great content takes time and we'd "
    "never want to add any pressure.\n\n"
    "Mostly just wanted to make sure everything's working well for you, and to "
    "see if there's anything we can help with — extra product details, a fresh "
    "code for your audience, or answering any questions you might have.\n\n"
    "If you've already shared something we'd love a link so we can cheer you on "
    "and repost it. And if now isn't the right time, totally understood — "
    "whenever works for you works for us.\n\n"
    "Always here if you need anything.\n\n"
    "Best,\n{signature}"
)

# 第 2 封追加运单号 (auto_send 在第 1 封发出后自动建草稿, 待运营填运单号)
# 2026-05-20 P1-D 止血: 去掉 [ETA 待填] 占位符 — 运营手填 ETA 时复制粘贴错位置,
# 5/15 thunderstashgaming/Thao 千万粉 KOL 收到 "arrive in the next 2026-05-15 20:00:00".
# 写死 "in the next few days" 跟 ship_confirm 第 1 封统一, 运营少 1 项要填的 = 少 1 个错位风险.
TEMPLATE_TRACKING_FOLLOWUP = (
    "Hi {first_name},\n\n"
    "Quick update — here's the tracking number for your {product_name} sample:\n\n"
    "**[TRACKING# 待填运营修改]** (via [CARRIER 待填运营修改])\n\n"
    "Should arrive in the next few days. Excited to hear what you think — "
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

    # 街道关键词 (BUG#2 2026-05-19: 加非英文街道词 — 原表纯英文/美式,
    # 把 Cirne 巴西 "Rua…"/Robert 菲律宾 "Block…Lot…" 等合法地址判否, ship_confirm
    # 被降级 need_address → 给已给过地址的 KOL 再发一封要地址 (déjà-vu 尴尬))
    street_kw_pattern = (
        r"\b(street|st\.?|avenue|ave\.?|boulevard|blvd\.?|road|rd\.?|lane|ln\.?|"
        r"drive|dr\.?|way|court|ct\.?|plaza|place|pl\.?|terrace|circle|cir\.?|"
        r"parkway|pkwy|highway|hwy|suite|ste\.?|apt\.?|unit|floor|building|bldg|"
        # 非英文/本地化街道词: 葡/西/德/法/意/印尼/菲
        r"rua|avenida|calle|carrera|estrada|rodovia|bloco|quadra|lote|"
        r"strasse|straße|rue|via|viale|jalan|barangay|brgy|purok|sitio|"
        r"block|blk|lot|distrito|bairro|colonia)\b"
    )
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

    # BUG#2 兜底: 无已知街道词但 邮编 + 国家名 + 多行/多逗号结构 → 仍是真地址
    # (覆盖未枚举到的本地化格式; 签名极少同时含 邮编+国家+≥3 段, 不会误放行)
    country_kw = [
        "united states", "usa", "u.s.a", "united kingdom", "u.k", "canada",
        "australia", "brazil", "brasil", "philippines", "germany", "deutschland",
        "france", "spain", "españa", "italy", "italia", "netherlands",
        "mexico", "méxico", "japan", "saudi arabia", "ksa", "indonesia",
        "india", "portugal", "poland", "sweden", "norway", "ireland",
        "new zealand", "south africa", "singapore", "malaysia", "thailand",
        "vietnam", "turkey", "türkiye", "uae", "united arab emirates",
        "argentina", "chile", "colombia", "peru", "austria", "switzerland",
        "belgium", "denmark", "finland", "greece", "czech", "hungary", "romania",
    ]
    has_country = any(c in s_lower for c in country_kw)
    has_struct = s.count("\n") >= 2 or s.count(",") >= 3

    return (has_street_kw and (has_zip or has_street_num)) or \
           (has_zip and has_country and (has_street_num or has_struct))


async def _classify_interest(original_body: str) -> dict:
    """对方回复"感兴趣"时, 进一步细分子类.
    Returns {"sub": "ship_confirm|need_address|short_only|send_assets|schedule_call|general",
             "confidence": 0.0-1.0,
             "extracted_address": "...",  # 仅 ship_confirm 命中时填
             "extracted_phone": "...",  # 仅 ship_confirm 命中时填
             "reason": "..."}

    注: short_only 是 platform-agnostic 信号 (只表态 short-form). 主流程会根据
    KOL 主平台决定: YouTube → AFFILIATE_UPSELL (有 normal 长视频 upsell 价值);
    其他平台 (TikTok / IG) → 降级 need_address (无 upsell 路径).
    """
    prompt = f"""一位 KOL/媒体人回复了我们的 cold email, 表达了感兴趣。
请判断他的具体诉求属于以下哪一档:

【6 个子类】
- ship_confirm: **必须满足两个条件**: ① 对方明确说想要实物 / "please send to..." / "my address is..." ② **回信原文里提供了具体邮寄地址 (含街道门牌号 + 城市 + 邮编)**。
  - ✗ **签名 (姓名+职位+公司网站)** 不算地址,例: "Kyle J. Beauregard, Director of Programming, www.wickedbinge.com" 是签名,不是地址
  - ✓ 例: "Send to: 123 Main St, Apt 4B, Brooklyn NY 11201, USA"
  - ✓ 例: "My address is 5-10-1 Shibuya, Shibuya-ku, Tokyo 150-0002, Japan"
- need_address: **明确选了 sample 但没给地址** (典型: 我们上一封发了"sample/press kit/quick call 三选项",对方明确选了 sample 这个选项)
  - ✓ "A sample would work" / "I'll take the sample" / "Send me the sample" / "Sample please"
  - ✓ "Yes, please send" / "send it" (上下文是回复我们寄样邀请)
  - ✓ "Option 1" / "1." (回复编号选项时选 1)
  - ✓ "I would love a sample" / "happy to receive a sample"
  - 关键判别: **明确表达"要 sample 这个东西"**, 但缺地址 → need_address (不是再问选项!)
- short_only: **涉及 short-form 内容 + 不愿免费做 normal 长视频** 的两种 case:
  ① 表态只做 short-form: "I only do shorts" / "shorts only" / "I only make YouTube Shorts" / "Just shorts" / "Reels only" / "I don't do long-form"
  ② **愿意免费做 short + normal 长视频要付费 / 不愿做**: "I'd do a Short. Normal video would need a fee" / "Happy to do a Short, but for long-form I charge $XXX" / "Short is fine, normal video has a rate"
  关键判别: 触达点是 **"short 可以免费 + long-form 不能免费"**。我们对此用 affiliate 模式回应 (短免费寄样 + 长视频按佣金分成),把"付费 normal"转换成"免费但按转化分成",所以这两 case 都归 short_only,不归 quote/general。
  ⚠️ 注意: 不是"想要 sample 来拍 short" (这归 need_address)。是"表态了 short 形式偏好"。
- send_assets: 想要更多产品资料 / PDF / 详细介绍 / 高清图 / 产品对比, 还没准备好接收实物
- schedule_call: 想要视频会议 / 电话沟通 / Zoom / Meet / Google call
- general: **泛泛感兴趣无明确诉求** (如 "Sounds cool, tell me more!" / "Interesting!" / "Looks awesome, would love to check it out"), 需要追问对方想要什么

【判断口诀(关键)】
- "sample would work / I'd love a sample / send me one" 等明确选 sample 但无地址 → **need_address** (不是 general!)
- "I only do shorts / shorts only" 表态只做短形式 → **short_only** (不是 need_address)
- "looks awesome / would love to check it out" 泛泛兴趣无具体动作 → general
- 完整邮寄地址(街道+城市+邮编)→ ship_confirm
- 只是签名 → 不算地址, 按其他线索判断子类

【对方回信原文 (前 800 字)】
{original_body[:800]}

返回 JSON:
{{
  "sub": "ship_confirm|need_address|short_only|send_assets|schedule_call|general",
  "confidence": 0.0-1.0,
  "extracted_address": "**只有真实邮寄地址才填**(必须含街道门牌号+邮编),签名/职位/网站绝对不算; 否则留空",
  "country_code": "如有真实地址, 推断出 ISO 国家代码 (US/UK/DE/JP/CA/AU/FR/ES/IT/NL/BR/MX 等); 否则留空",
  "recipient_name": "**收件人姓名** (从地址首行/邮件签名抽,如 'John Smith' / 'Sarah Lee'); 没有真实地址时留空",
  "extracted_phone": "**收件电话** (从邮件中抽出,带国家区号最佳如 +1 555-123-4567 / +44 20 1234 5678; 没有则留空)",
  "reason": "20 字以内说明判断依据"
}}"""
    try:
        r = await deepseek.chat_json(prompt, max_tokens=400, temperature=0.0)
        sub = r.get("sub", "general")
        if sub not in ("ship_confirm", "need_address", "short_only", "send_assets", "schedule_call", "general"):
            sub = "general"
        extracted_address = (r.get("extracted_address") or "").strip()[:500]

        # 后处理:即便 AI 判 ship_confirm,正则验证地址是否真实(防签名误识)
        # 校验失败降级到 need_address (说明确选 sample 但地址不全), 而非 general (避免再问选项菜单)
        if sub == "ship_confirm" and not _is_real_address(extracted_address):
            print(f"[reply_drafter] ship_confirm 降级 need_address: 地址正则校验失败 raw='{extracted_address[:120]}'")
            sub = "need_address"
            extracted_address = ""

        return {
            "sub": sub,
            "confidence": float(r.get("confidence", 0.5) or 0.5),
            "extracted_address": extracted_address,
            "country_code": (r.get("country_code") or "").strip().upper()[:5],
            "recipient_name": (r.get("recipient_name") or "").strip()[:80],
            "extracted_phone": (r.get("extracted_phone") or "").strip()[:40],
            "reason": (r.get("reason") or "")[:80],
        }
    except Exception as e:
        return {"sub": "general", "confidence": 0.0, "extracted_address": "",
                "country_code": "", "recipient_name": "", "extracted_phone": "",
                "reason": f"AI 错误: {str(e)[:50]}"}


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


async def _is_late_stage_contact(cf: dict, link_field: str, contact_rid: str):
    """判断 KOL/媒体人是否已过"早期兴趣"阶段 (已寄样/已谈条款/已上稿/已合作).
    2026-05-25 stage-blind 修复: late-stage KOL 的「感兴趣」自动回复(早期话术如"要不要样品/
    你需要什么")是阶段错位 → 强制人审不自动发. 周会 Metalfear4(已签收+直播)/PlayTopia(已谈$100)事故.
    Returns (is_late: bool, reason: str)."""
    if ext(cf.get("上次寄样订单号")):
        return True, "已寄样(上次寄样订单号非空)"
    try:
        if int(cf.get("寄样次数") or 0) >= 1:
            return True, "已寄样(寄样次数≥1)"
    except (ValueError, TypeError):
        pass
    if cf.get("上稿日期"):
        return True, "已上稿"
    if ext(cf.get("合作状态")) in ("已合作-免费", "已合作-免费(多次)", "已合作-付费"):
        return True, "已合作"
    # 已发过 affiliate_quote 草稿 = 已进入条款谈判 (如 PlayTopia 已谈 $100, 主表无寄样信号)
    try:
        prior = await feishu.search_records(config.T_DRAFT, [
            {"field_name": link_field, "operator": "contains", "value": [contact_rid]},
            {"field_name": "邮件草稿来源", "operator": "is", "value": ["affiliate_quote"]},
            {"field_name": "邮件草稿状态", "operator": "is", "value": ["已发送"]},
        ])
        if prior:
            return True, "已谈条款(affiliate_quote 已发送)"
    except Exception:
        pass
    # 2026-05-29 (TG_Geek/Gameknight3227 事故): 上面 5 个信号全读「主表」字段, 但主表的寄样次数/
    # 上次寄样订单号/上稿日期/合作状态 常没回写 (TG_Geek 已寄样+已发布 review, 主表却全空+合作状态
    # =洽谈中) → guard 全 blind → 早期"想要样品吗"话术被自动外发. 单封回复的 scenario 又可能=None
    # (5/29 那封), 连 v4 scenario 强制人审也兜不住. 补: 查该 contact **草稿历史**, 任一草稿命中
    # 已过早期阶段的信号 = late-stage (不依赖主表回写 / 不依赖当封 scenario)。
    #   信号: 草稿 寄样阶段∈已发货/在途/已签收/已产出, 或 历史 场景标签 funnel∈寄样物流/brief拍摄/草稿/发布收口。
    try:
        from . import stage_model
        LATE_FUNNELS = {"寄样物流", "brief拍摄", "草稿", "发布收口"}
        LATE_SHIP = {"已发货", "在途", "已签收", "已产出"}
        hist = await feishu.search_records(config.T_DRAFT, [
            {"field_name": link_field, "operator": "contains", "value": [contact_rid]},
        ], field_names=["场景标签", "寄样阶段"])
        for d in hist:
            df = d["fields"]
            ship = ext(df.get("寄样阶段"))
            if ship in LATE_SHIP:
                return True, f"已寄样(历史草稿寄样阶段={ship})"
            scn = ext(df.get("场景标签"))
            if scn and stage_model.funnel_stage_of(scn) in LATE_FUNNELS:
                return True, f"已过早期(历史草稿场景={scn}/{stage_model.funnel_stage_of(scn)}阶段)"
    except Exception as e:
        print(f"[reply_drafter] late-stage draft-history check fail (放行): {e}")
    return False, ""


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
    scenario_label: str = "",      # v4 ④: 细分场景标签 (reply_monitor 分类得), 命中 FORCE_REVIEW_LABELS 时强制人审
    related_inbound_msg_id: str = "",  # 邮件线程化: 被回复的 KOL 入站 messageId, 落「回复目标MsgID」→ auto_send 走 action:reply
    manual_alias_review: bool = False,  # 2026-06-01: 回复发往 marketing@/frankie@(非partner@主别名)=手动高触达关系→强制人审
    stale_reply_days: int = 0,          # 2026-06-02 Fix B: 该回复 receivedTime 距今天数(0=新/未知); ≥config.STALE_REPLY_DAYS=久未互动旧回复唤醒
    inbound_intent: dict = None,        # 2026-06-03 卡片合并: reply_monitor 的入站分类 dict(type/summary/key_quote/suggested_action) → 透传给 route_draft 渲染进审核卡(替代原独立知会卡)
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
    product_link_raw = ""
    product_price = 0  # P5.11 affiliate_quote 模板需要独立站售价
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
                product_link_raw = feishu.product_url(pf)   # 官网链接优先, 缺则降级亚马逊链接(防死链)
                # P5.11: 拉「报价(USD)」字段作为独立站售价 (affiliate_quote 模板用)
                try:
                    product_price = float(pf.get("报价(USD)") or 0)
                except (ValueError, TypeError):
                    product_price = 0
        except Exception as e:
            print(f"[reply_drafter] fetch related product fail: {e}")

    # Phase 1 ROI: 给 product_link 注 UTM
    from . import utm as _utm
    product_link = _utm.make_utm_link(product_link_raw, brand, product_name, contact_name) if product_link_raw else ""
    utm_id_value = _utm.kol_utm_id(contact_name) if contact_name else ""

    sig_first = "Frankie"
    sig_full = _sender_signature(brand)
    first = _first_name(contact_name)

    # 子分类元信息 (仅 ship_confirm / affiliate_upsell 用到)
    sub = ""
    extracted_address = ""
    country_code = ""
    recipient_name = ""
    recipient_phone = ""

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
        recipient_name = sub_info.get("recipient_name", "") or contact_name
        recipient_phone = sub_info.get("extracted_phone", "")

        # short_only 路由: 仅 YouTube 主平台 → AFFILIATE_UPSELL (有 normal video upsell 价值);
        # 其他平台 (TikTok / IG) → 降级 need_address (Short / Reels 都无法挂链接,无 upsell)
        # 决策依据: Frankie 2026-05-06 — YT normal 能挂独立站链接是 YT ROI 比 TK 好的根本原因
        if sub == "short_only":
            kol_main_platform = ext(cf.get("主平台")) if contact_type != "editor" else ""
            if "youtube" in kol_main_platform.lower() or "yt" in kol_main_platform.lower():
                sub = "affiliate_upsell"
            else:
                print(f"[reply_drafter] short_only 降级 need_address (主平台={kol_main_platform}, 非 YouTube 无 upsell 路径)")
                sub = "need_address"

        # 2026-06-02 Fix B: 旧回复唤醒守卫. recon 翻出的久未互动旧回复(receivedTime≥STALE_REPLY_DAYS)
        # 即便给了地址也不直接 ship_confirm("got the address, shipping!"=唐突, mrbrian 反馈),
        # 降级 stale_rewarm(先确认现在是否还感兴趣)→ 下方强制人审 + 不预填寄样字段 + 不发寄样卡。
        if (config.STALE_REPLY_DAYS and stale_reply_days >= config.STALE_REPLY_DAYS
                and sub == "ship_confirm"):
            print(f"[reply_drafter] 旧回复 {stale_reply_days}d (≥{config.STALE_REPLY_DAYS}) → ship_confirm 降级 stale_rewarm (久未互动先预热)")
            sub = "stale_rewarm"
            extracted_address = ""   # 不走寄样字段/不预填主表

        subj = "Re: " + original_subject[:150]
        if sub == "ship_confirm":
            body = TEMPLATE_SHIP_CONFIRM.format(
                first_name=first, signature=sig_full,
                product_name=product_name,
            )
        elif sub == "affiliate_upsell":
            body = TEMPLATE_AFFILIATE_UPSELL.format(
                first_name=first, signature=sig_full,
                product_name=product_name,
            )
        elif sub == "stale_rewarm":
            body = TEMPLATE_STALE_REWARM.format(
                first_name=first, signature=sig_full,
                product_name=product_name,
            )
        elif sub == "need_address":
            body = TEMPLATE_NEED_ADDRESS.format(
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
        # P5.11: 不直接走商务谈判,先发联盟邀请 (80% KOL 接受联盟,拒绝才转人审决策)
        # 不再调用 _gen_quote_draft (DeepSeek 自由生成)→ 用固定模板, 价格从产品库报价(USD)
        subj = "Re: " + original_subject[:150]
        price_str = f"{int(product_price)}" if product_price else "TBD"
        body = TEMPLATE_AFFILIATE_INVITATION_QUOTE.format(
            first_name=first, signature=sig_full,
            product_name=product_name, product_price=price_str,
        )
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

    # 拿 related_draft 的关联产品 + 关联任务 record_id (继承到新 reply 草稿)
    # 2026-05-17 A2: 历史漏写「关联任务」导致任务台「已发送数」回写失败 (5/14 status pre-existing bug)
    related_prod_rid = None
    related_task_rid = None
    if related_draft_id:
        try:
            related_full = await feishu.get_record(config.T_DRAFT, related_draft_id)
            related_prod_rid = xrid(related_full["fields"].get("关联产品"))
            related_task_rid = xrid(related_full["fields"].get("关联任务"))
        except Exception:
            pass

    # P5.11: intent_type=要报价 → 草稿来源标 affiliate_quote (区别一般 reply,便于追踪)
    draft_source = "affiliate_quote" if intent_type == "要报价" else "reply"
    fields = {
        "邮件草稿ID": f"{'aq' if intent_type == '要报价' else 'reply'}-{contact_record['record_id'][-8:]}-{int(time.time())}",
        link_field: [contact_record["record_id"]],
        "邮件主题": subj[:200],
        "邮件正文": body,
        "邮件语言": "en",
        "邮件草稿状态": "待审",
        "邮件草稿来源": draft_source,
        "对象类型": contact_type if contact_type == "KOL" else "媒体人",
        "发送邮箱": sender_alias,
        "发送人署名": sig_first,
        "生成时间": now_ms,
        "建议发送时间": now_ms,
        "重生次数": 0,
        "收件邮箱": feishu.clean_email(ext(cf.get("邮箱")))[0] or "",
        "UTM 链接": product_link,
    }
    if related_prod_rid:
        fields["关联产品"] = [related_prod_rid]
    if related_task_rid:
        fields["关联任务"] = [related_task_rid]
    # 2026-05-29: 把入站分类得到的 场景标签 也落到这封回复草稿上 (之前只写在被回复的原草稿,
    # 导致回复草稿 场景标签=None, 审计时看不出它是对哪个阶段的回应 — TG_Geek 排查时的红鲱鱼).
    # reply_monitor 已把空 scenario 归一成 unclassified_fallback, 这里恒为有效 label。
    if scenario_label:
        fields["场景标签"] = scenario_label
    # 邮件线程化: 存被回复的 KOL 入站 messageId → auto_send 据此走 action:reply 串入原 thread.
    if related_inbound_msg_id:
        fields["回复目标MsgID"] = related_inbound_msg_id

    # ship_confirm 寄样订单字段 (12 字段,V1 寄样链路)
    if sub == "ship_confirm" and extracted_address:
        from datetime import datetime as _dt
        handle_slug = re.sub(r'[^a-zA-Z0-9]', '', contact_name or "kol").lower()[:20] or "kol"
        ship_order_id = f"SHIP-{handle_slug}-{_dt.now().strftime('%Y%m%d')}"
        fields["寄样订单号"] = ship_order_id
        fields["寄样阶段"] = "待发货"
        fields["收件姓名"] = (recipient_name or contact_name)[:80]
        fields["收件地址 full"] = extracted_address[:500]
        fields["国家/地区"] = country_code
        if recipient_phone:
            fields["收件电话"] = recipient_phone

    # affiliate_upsell 标记: 草稿打"内容形式受限"标签 (供 ROI 分析时区分 short-only KOL)
    if sub == "affiliate_upsell":
        fields["内容形式受限"] = "YouTube Short only"

    rid = await feishu.create_record(config.T_DRAFT, fields)
    print(f"[reply_drafter] created draft rid={rid} intent={intent_type} sub={sub}")

    # ship_confirm 主表回填 — **只缓存默认收件地址/电话**, 不在草稿创建时标"已寄样"。
    # 2026-06-02 修: 原来在此处就回填 寄样次数+1/上次寄样日期/上次寄样订单号 = **运营确认前预标已寄样**。
    # 若运营在寄样卡上否决(决定不寄, 如 mrbrian 被翻出的久未互动旧回复), 主表却仍显示已寄样 →
    # 污染 寄样次数 / 上次寄样订单号 / 下游 upload_register·completion 漏斗·_already_shipped 守门。
    # 正解: 寄样次数/订单号/日期 改由 auto_send 在 ship_confirm **真发出时**回填(auto_send 已有此逻辑,
    # gate 依赖的 草稿「寄样订单号」仍在上面写, 不受影响) → 否决的卡不再误标主表。
    if sub == "ship_confirm" and extracted_address:
        try:
            target_table = config.T_EDITOR if contact_type == "editor" else config.T_KOL
            backfill_payload = {"默认收件地址": extracted_address[:500]}
            if recipient_phone:
                backfill_payload["默认收件电话"] = recipient_phone
            await feishu.update_record(target_table, contact_record["record_id"], backfill_payload)
            print(f"[reply_drafter] ship_confirm draft {fields['寄样订单号']} created for {contact_name} (主表寄样字段待 auto_send 真发出时回填)")
        except Exception as e:
            print(f"[reply_drafter] backfill default address fail: {e}")

    # Phase 1 ROI: 第一次写 UTM ID 到联系人主表 (idempotent)
    if utm_id_value:
        try:
            target_table = config.T_EDITOR if contact_type == "editor" else config.T_KOL
            cur_utm = ext(cf.get("UTM ID"))
            if not cur_utm:
                await feishu.update_record(target_table, contact_record["record_id"], {"UTM ID": utm_id_value})
        except Exception as e:
            print(f"[reply_drafter] write UTM ID fail: {e}")

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
        # 强制人审 label: 优先级 sub > intent_type
        # sub == affiliate_upsell → 强制人审 (涉及佣金/折扣码谈判)
        # intent ∈ {不明意图, 质疑/澄清, 要报价} → 强制人审 (Ashtvn 死循环类防御)
        force_label = None
        if sub == "affiliate_upsell":
            force_label = "affiliate_upsell"
        elif intent_type in ("不明意图", "质疑/澄清", "要报价"):
            force_label = intent_type
        # 2026-05-25 stage-blind 修复: late-stage KOL 的「感兴趣」早期话术强制人审 (不自动发)
        force_reason = None
        if intent_type == "感兴趣":
            is_late, late_why = await _is_late_stage_contact(cf, link_field, contact_record["record_id"])
            if is_late:
                force_reason = late_why
                try:
                    await feishu.update_record(config.T_DRAFT, rid, {
                        "审批意见": (f"[阶段错位拦截] 该 KOL {late_why}, 系统按「感兴趣」生成了早期话术 "
                                     f"(sub={sub}), 已强制人审防重复开发信. 请人工换成阶段合适的回复再发。")[:500],
                    })
                except Exception:
                    pass
        # 2026-06-01: marketing@/frankie@ 回复(手动高触达关系)→强制人审, 不自动发. 保留已有 force_reason.
        if manual_alias_review and not force_reason:
            force_reason = "manual-alias:回复发往 marketing@/frankie@(人工高触达关系)→强制人审"
        # 2026-06-02 Fix B: 旧回复唤醒 → 任何意图都强制人审(防自动回复被 recon 翻出的久未互动旧邮件) + 标注先预热.
        _is_stale = bool(config.STALE_REPLY_DAYS and stale_reply_days >= config.STALE_REPLY_DAYS)
        if _is_stale:
            if not force_reason:
                force_reason = f"stale-reply:{stale_reply_days}天前旧回复(久未互动)→强制人审, 建议先轻预热确认意向再推进"
            try:
                _note = (f"[久未互动旧回复] 该回复实际是 {stale_reply_days} 天前发来的(被搜查/补登记翻出, 非近期主动)。"
                         + ("已把寄样卡降级为轻预热(先确认还感不感兴趣再寄), 别直接寄样。"
                            if sub == "stale_rewarm" else "推进/寄样前建议先发轻预热确认 KOL 现在仍有意向。"))
                await feishu.update_record(config.T_DRAFT, rid, {"审批意见": _note[:500]})
            except Exception:
                pass
        result = await draft_router.route_draft(
            rid,
            ship_confirm_meta={"address": extracted_address, "country": country_code,
                                 "product_name": product_name} if sub == "ship_confirm" else None,
            force_review_intent=force_label,
            force_review_reason=force_reason,
            force_review_scenario=scenario_label,   # v4 ④b: 高风险场景标签强制人审 (加法)
            inbound_reply=inbound_intent,           # 2026-06-03 卡片合并: 入站回复内容渲染进审核卡
        )
        print(f"[reply_drafter] router result: score={result['score']} path={result['path']}")
    except Exception as e:
        print(f"[reply_drafter] router fail: {e}")

    return rid

"""草稿自审 Reviewer

输入: 一条「KOL·媒体人邮件草稿」记录 (主题/正文/对象类型/品牌/邮件草稿来源)
输出: {
    "score": 0-10 整数,
    "reasons": {字数/主题/语气/SKU/链接 → 短理由},
    "committed": bool (是否包含商务承诺),
    "keywords_hit": ["price", "佣金", ...],  # 关键词预过命中
    "ai_commitment_judge": "yes/no/unsure" (AI 二次判结果, 仅命中关键词时调用)
}

策略 (决策 C 两层叠加):
1. 关键词正则预过 (CN+EN, ~25 词)
2. 命中关键词 → DeepSeek 二次判 "是否真有商务承诺?"
3. 不论是否命中, 都做 DeepSeek 5 项 checklist 评分
"""
import re
from typing import Dict, Any
from . import deepseek


# ===== 1. 关键词正则 (决策 A 用我建议的列表) =====
# 英文承诺词
EN_KEYWORDS = [
    "commission", "royalty", "fee", "fees", "price", "pricing", "quote",
    "moq", "paid", "payment", "sample", "free unit", "first order",
    "exclusive", "brand deal", "sponsorship", "partnership terms",
    "kickback", "bonus", "gift", "discount code", "PO", "purchase order",
]
# 中文承诺词
CN_KEYWORDS = [
    "佣金", "报价", "价格", "条款", "寄样", "样品", "免费", "首单",
    "独家", "分成", "返点", "提成", "押金", "保证金", "付款", "采购单",
]

# 词边界匹配 (英文用 \b, 中文不用)
EN_PATTERN = re.compile(r"\b(" + "|".join(re.escape(k) for k in EN_KEYWORDS) + r")\b", re.IGNORECASE)
CN_PATTERN = re.compile("(" + "|".join(re.escape(k) for k in CN_KEYWORDS) + ")")


def detect_keywords(text: str) -> list:
    """关键词预过 — 返回命中的关键词列表"""
    if not text:
        return []
    hits = set()
    for m in EN_PATTERN.findall(text):
        hits.add(m.lower())
    for m in CN_PATTERN.findall(text):
        hits.add(m)
    return sorted(hits)


# ===== 2. SKU 内部代号黑名单 (扩展用) =====
SKU_BLACKLIST_PATTERNS = [
    r"\bYM\d{2,4}\b",          # YM24, YM240 等内部代号
    r"\bSKU[-_]?[A-Z]{1,3}\d+\b",
    r"内部代号",
    r"内部 SKU",
]
SKU_BLACKLIST_RE = re.compile("|".join(SKU_BLACKLIST_PATTERNS), re.IGNORECASE)


def has_internal_sku(text: str) -> tuple:
    """检查内部 SKU 代号是否泄露 → (bool, 命中片段)"""
    if not text: return False, ""
    m = SKU_BLACKLIST_RE.search(text)
    return (bool(m), m.group(0) if m else "")


# ===== 3. AI 二次判承诺 =====
async def ai_commitment_judge(subject: str, body: str, source: str, hits: list) -> dict:
    """
    Args:
        source: cold/followup/reply
        hits: 命中的关键词列表
    Returns:
        {"verdict": "yes/no/unsure", "reason": "..."}
    """
    prompt = f"""你在审核一封 {source} 类型的对外营销邮件,判断是否包含**实质性商务承诺**。

【邮件】
Subject: {subject}
Body:
{body[:1500]}

【已命中的潜在承诺关键词】
{", ".join(hits)}

【判断准则】
"实质性承诺" = 邮件文本中明确提到下列至少一项的具体数字/条款,而非泛泛提及:
- 具体佣金/分成比例 (如 "10% commission", "8% royalty")
- 具体价格/报价 (如 "$50 per unit", "MOQ 500")
- 寄样数量/条款 (如 "free 3 units", "samples on us")
- 首单/独家/PO 承诺 (如 "first order priority", "exclusive in DACH")

如果只是**邀请讨论** ("happy to chat about pricing", "let's discuss commission")
或**通用模板词** ("partnership", "sample" 在询问语境中) → **不算实质性承诺**, 标 no。

如果不确定 → 标 unsure (按"待人审"处理)。

返回 JSON:
{{"verdict": "yes" | "no" | "unsure", "reason": "20 字以内说明"}}"""
    try:
        r = await deepseek.chat_json(prompt, max_tokens=200, temperature=0.0)
        v = r.get("verdict", "unsure").lower()
        if v not in ("yes", "no", "unsure"): v = "unsure"
        return {"verdict": v, "reason": r.get("reason", "")[:80]}
    except Exception as e:
        return {"verdict": "unsure", "reason": f"AI 错误: {str(e)[:50]}"}


# ===== 4. 5 项 checklist 评分 =====
async def ai_score(subject: str, body: str, source: str, contact_type: str, brand: str) -> dict:
    """
    5 项 checklist 评分 (每项 0-2 分, 满分 10)
    1. 字数合规
    2. 主题合规 (<40 字符 / <=7 词)
    3. 语气适配品牌 voice
    4. SKU 内部代号未泄露
    5. 链接合法 + 无硬性承诺 (除非 source=reply 且明确无承诺)
    """
    word_target = {
        "cold": "100-150 词",
        "followup": "60-100 词 (短于第一封)",
        "reply": "灵活,但需控制在 200 词内",
    }.get(source, "100-150 词")

    brand_voice = {
        "POWKONG": "直接、product-first、不啰嗦,offer 导向",
        "FUNLAB": "亲和、creator-friendly、强调玩家社区共创",
    }.get(brand.upper(), "专业且简洁")

    prompt = f"""你是一名营销邮件审核员,审核一封 {source} 邮件 ({brand} 品牌, {contact_type}).

【邮件】
Subject: {subject}
Body:
{body[:1500]}

【5 项 checklist】 (每项 0-2 分,满分 10 分)
1. 字数合规: 正文应在 {word_target} 范围内
2. 主题合规: 字符 ≤40 / 词数 ≤7 / 不堆叠表情
3. 语气适配品牌: {brand} 的 voice 是 "{brand_voice}"
4. SKU/内部代号: 不能出现 "YM24" 等内部 SKU 代号或"内部代号"等措辞
5. 链接 & 承诺: 链接必须看起来合法 (https://开头, 真实域名); 不应有硬性数字承诺 (具体佣金%/价格$),除非 source=reply 且承诺已明确无误

【返回 JSON 格式】 (每项分数 + 50 字以内理由)
{{
  "items": {{
    "字数": {{"score": 0|1|2, "reason": "..."}},
    "主题": {{"score": 0|1|2, "reason": "..."}},
    "语气": {{"score": 0|1|2, "reason": "..."}},
    "SKU": {{"score": 0|1|2, "reason": "..."}},
    "链接": {{"score": 0|1|2, "reason": "..."}}
  }},
  "total": 0-10,
  "summary": "20 字以内总评"
}}"""
    try:
        r = await deepseek.chat_json(prompt, max_tokens=600, temperature=0.0)
        return r
    except Exception as e:
        # AI 失败 → 给一个低分,转人审
        return {
            "items": {"字数": {"score": 0, "reason": f"AI 错误: {str(e)[:30]}"}},
            "total": 0,
            "summary": "AI 评分失败, 转人审",
        }


# ===== 5. 主入口 =====
async def review_draft(subject: str, body: str, source: str = "cold",
                       contact_type: str = "KOL", brand: str = "FUNLAB") -> Dict[str, Any]:
    """
    主入口: 给定草稿内容 → 返回评审结果

    Returns:
        {
            "score": int,             # 0-10
            "reasons": {dim: reason},
            "summary": str,
            "committed": bool,        # 是否有实质性商务承诺
            "keywords_hit": [str],
            "ai_commitment_judge": {"verdict": str, "reason": str},
            "internal_sku_leak": (bool, str)
        }
    """
    full_text = (subject or "") + "\n" + (body or "")

    # Layer 1: 关键词预过
    hits = detect_keywords(full_text)

    # Layer 2: 命中关键词时 AI 二次判 (没命中跳过省 token)
    commit_judge = {"verdict": "no", "reason": "未命中关键词"}
    if hits:
        commit_judge = await ai_commitment_judge(subject, body, source, hits)

    committed = commit_judge["verdict"] == "yes"

    # SKU 黑名单
    sku_leak, sku_hit = has_internal_sku(full_text)

    # 5 项打分
    scoring = await ai_score(subject, body, source, contact_type, brand)
    items = scoring.get("items", {})
    score = scoring.get("total")
    if score is None:
        # 如果 AI 没返回 total,自己求和
        score = sum((v.get("score", 0) or 0) for v in items.values())
    score = max(0, min(10, int(score)))

    # 如果 SKU 泄露 → 强制 SKU 项 0 分,总分扣 2
    if sku_leak:
        score = max(0, score - 2)
        items["SKU"] = {"score": 0, "reason": f"内部代号泄露: {sku_hit}"}

    reasons = {dim: v.get("reason", "") for dim, v in items.items()}

    return {
        "score": score,
        "reasons": reasons,
        "summary": scoring.get("summary", ""),
        "committed": committed,
        "keywords_hit": hits,
        "ai_commitment_judge": commit_judge,
        "internal_sku_leak": (sku_leak, sku_hit),
    }

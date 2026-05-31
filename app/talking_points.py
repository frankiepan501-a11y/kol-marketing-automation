# -*- coding: utf-8 -*-
"""AI 生成 brief talking points + 拍摄角度建议 (从产品卖点) → 写产品库 → 运营审批采纳.

知识库依据 (reference_kol_gifting_brief_playbook): brief = 护栏(talking points 痛点/收益 + 角度),
AI 起草、运营审 = L7 杠杆(一次/产品全 KOL 复用)。暖信 warm_recap 读产品库这两个字段拼 brief。

机制: DeepSeek 读 卖点1/2/3 + 简述 + 英文名 + 品类 → 英文 talking points(痛点/收益非参数) +
拍摄角度 → 写产品库「Talking Points」「拍摄角度建议」(标准字段, 暖信直接用) → 发卡片给运营审/改。
"""
import json, re, httpx
from . import config, feishu, deepseek, sales_attribution
from .feishu import ext, ext_url

# 框架结构库 (短视频素材库, 聪哥2号是协作者 → 同 bitable token 可跨 app 读). app_token 非 secret(CLAUDE.md 明文).
# G-A/G-B (2026-05-31): per-KOL brief 的 AI 推荐框架从这 12 个「停病药信买」变体里选。
FRAMEWORK_APP_TOKEN = "PpZIbSIuxaPa5wsNGDZcZm9Wn7t"
FRAMEWORK_TABLE_ID = "tbluWVngE93DKCdH"


def _brand_from_link(link: str) -> str:
    l = (link or "").lower()
    if "powkong" in l:
        return "POWKONG"
    if "funlab" in l:
        return "FUNLAB"
    return ""


async def _fetch_shopify_body(link: str) -> str:
    """从产品官网链接拉 Shopify 产品页正文 (body_html → 纯文本). 拉不到/非 Shopify 返回 '' (降级不阻断).
    2026-05-29 Frankie: 卖点常录不全 (食人花漏'磁吸充电'), 拉产品页给 AI → brief 要点/角度更全准。"""
    m = re.search(r"/products/([^/?#]+)", link or "")
    brand = _brand_from_link(link)
    if not m or not brand:
        return ""
    handle = m.group(1)
    try:
        tok = await sales_attribution.get_token(brand)
        shop = sales_attribution.get_shop(brand)
        async with httpx.AsyncClient(timeout=30.0) as cli:
            r = await cli.get(
                f"https://{shop}/admin/api/{sales_attribution.SHOPIFY_API_VERSION}/products.json",
                params={"handle": handle, "fields": "id,title,body_html"},
                headers={"X-Shopify-Access-Token": tok})
            r.raise_for_status()
            prods = r.json().get("products") or []
        if not prods:
            return ""
        body_html = prods[0].get("body_html") or ""
    except Exception as e:
        print(f"[talking_points] Shopify 产品页拉取失败 ({brand}/{handle}): {e}")
        return ""
    txt = re.sub(r"<[^>]+>", " ", body_html)
    txt = txt.replace("&nbsp;", " ").replace("&amp;", "&")
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt[:2000]


def _amz_clean(s: str) -> str:
    s = re.sub(r"<[^>]+>", " ", s)
    for a, b in (("&nbsp;", " "), ("&amp;", "&"), ("&#39;", "'"), ("&quot;", '"'),
                 ("&rsquo;", "'"), ("&ldquo;", '"'), ("&rdquo;", '"')):
        s = s.replace(a, b)
    return re.sub(r"\s+", " ", s).strip()


async def _fetch_amazon_listing(link: str) -> str:
    """从亚马逊 listing 页拉 标题 + 五点描述(feature-bullets) → 纯文本. 比 Shopify body_html 肥得多
    (2026-05-31 Frankie: 一个信息完整的产品页就能挖全, 替代人工填 Talking Points + 稀疏 Shopify)。
    抓不到/被反爬挡/非亚马逊链接 → 返回 '' (fail-safe 降级, 不阻断)。描述区是 A+ 图片取不到, 标题+五点已够。
    ⚠️ 生产在 Zeabur 云端(数据中心 IP), 低频(暖信个位数/天)单页 GET 通常可过; 被挡自动降级。"""
    url = (link or "").strip()
    if "amazon." not in url.lower():
        return ""
    m = re.search(r"/(?:dp|gp/product)/([A-Z0-9]{10})", url, re.I)
    if m:
        url = f"https://www.amazon.com/dp/{m.group(1)}"
    headers = {
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml",
    }
    try:
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as cli:
            r = await cli.get(url, headers=headers)
            h = r.text
    except Exception as e:
        print(f"[talking_points] 亚马逊页拉取失败 ({url}): {e}")
        return ""
    low = h.lower()
    if len(h) < 50000 or "robot check" in low or "api-services-support@amazon" in low or "validatecaptcha" in low:
        print(f"[talking_points] 亚马逊页疑似被反爬挡 (len={len(h)}) → 降级")
        return ""
    parts = []
    t = re.search(r'id="productTitle"[^>]*>(.*?)</span>', h, re.S)
    if t:
        parts.append("Title: " + _amz_clean(t.group(1))[:300])
    i = h.find('id="feature-bullets"')
    if i >= 0:
        ul = re.search(r"<ul[^>]*>(.*?)</ul>", h[i:i + 8000], re.S)
        if ul:
            bullets = []
            for b in re.findall(r'<span class="a-list-item">(.*?)</span>', ul.group(1), re.S):
                c = _amz_clean(b)
                if c and len(c) > 8 and "protection plan" not in c.lower():
                    bullets.append(c[:240])
            if bullets:
                parts.append("Bullet points:\n" + "\n".join(f"- {b}" for b in bullets[:6]))
    return "\n".join(parts)[:2500]


async def generate_for_product(prod_rid: str, overwrite: bool = False, notify: bool = True,
                               kol_rid: str = None) -> dict:
    # G-A/G-B (2026-05-31): 带 kol_rid → per-KOL 定制 brief(框架推荐+5 hooks+TikTok SEO),
    # 返回 dict 给 warm_recap 消费, **不写产品库/不发产品级卡**(那是 per-product 路径, 会被下个 KOL 覆盖)。
    if kol_rid:
        return await generate_for_kol(prod_rid, kol_rid)
    prod = await feishu.get_record(config.T_PRODUCT, prod_rid)
    pf = prod["fields"]
    name = ext(pf.get("产品英文名")) or ext(pf.get("产品名")) or "the product"
    cat = ext(pf.get("品类"))
    desc = ext(pf.get("产品简述"))
    selling = [ext(pf.get(f"卖点{i}")) for i in (1, 2, 3) if ext(pf.get(f"卖点{i}"))]
    if not selling:
        return {"ok": False, "rid": prod_rid, "skip": "无卖点可生成"}
    if ext(pf.get("Talking Points")).strip() and not overwrite:
        return {"ok": False, "rid": prod_rid, "skip": "Talking Points 已有 (overwrite=false)"}

    # 拉 Shopify 产品页正文补充卖点 (录不全的特性如磁吸充电从这里来; 拉不到则降级只用卖点)
    page_text = await _fetch_shopify_body(ext_url(pf.get("官网链接")) or "")

    page_block = (f'\nFull product page copy (seller\'s own description — MINE it for concrete features/'
                  f'benefits the audience cares about that the selling points above may have MISSED, '
                  f'e.g. magnetic charging, cooling fan, multi-device support; still reframe into '
                  f'benefit/emotion, do NOT dump specs): {page_text}\n') if page_text else ''

    prompt = f"""You are a creator-marketing copywriter. Write brief "talking points" + shooting angles that a KOL can use when posting about this product to THEIR audience.

Product: {name} ({cat})
Selling points (Chinese — translate + reframe into AUDIENCE BENEFIT/emotion, NOT spec lists): {' / '.join(selling)}
{f'Description: {desc}' if desc else ''}{page_block}
Rules:
- English only. Casual, creator-friendly, benefit/emotion-focused (NOT feature/spec dumps).
- Cover the product's most distinctive features (incl. any only found in the product page copy), reframed as audience benefits.
- talking_points: 2-3 bullets, each <=14 words — what makes it cool/useful for their audience.
- shooting_angles: 2-3 concrete content-angle ideas (<=12 words each).

Return JSON: {{"talking_points":["...","..."],"shooting_angles":["...","..."]}}"""
    out = await deepseek.chat_json(prompt, max_tokens=400)
    tp = [s.strip() for s in (out.get("talking_points") or []) if str(s).strip()][:3]
    ang = [s.strip() for s in (out.get("shooting_angles") or []) if str(s).strip()][:3]
    if not tp:
        return {"ok": False, "rid": prod_rid, "error": "AI 未产出 talking_points", "raw": out}

    await feishu.update_record(config.T_PRODUCT, prod_rid, {
        "Talking Points": "\n".join(tp),
        "拍摄角度建议": "\n".join(ang),
    })
    if notify:
        try:
            base = f"https://u1wpma3xuhr.feishu.cn/base/{config.FEISHU_APP_TOKEN}?table={config.T_PRODUCT}"
            # 2026-05-29 Frankie: 信息卡 → 聪哥3号交互卡, 卡上 ✅采纳/🔁重生 一键完成(走 event-hub
            # Is TP Action? 分支), 改文案才点去表格。value.action=tp_adopt/tp_regen + record_id=产品 rid。
            val = {"app_token": config.FEISHU_APP_TOKEN, "table_id": config.T_PRODUCT,
                   "record_id": prod_rid, "product": name}
            card = {
                "config": {"wide_screen_mode": True, "update_multi": True},
                "header": {"template": "blue", "title": {"tag": "plain_text",
                           "content": f"🧠 AI 草拟 brief 要点 — 卡上采纳/重生 ({name})"}},
                "elements": [
                    {"tag": "div", "text": {"tag": "lark_md", "content":
                        f"AI 为 **{name}** 草拟了寄样暖信用的 brief 要点(已写入产品库)。"
                        "满意点 **✅采纳**;不满意点 **🔁重新生成**;要逐条改文案点 **📝去表格改**。"}},
                    {"tag": "div", "text": {"tag": "lark_md", "content":
                        "**Talking Points**\n" + "\n".join(f"• {t}" for t in tp)}},
                    {"tag": "div", "text": {"tag": "lark_md", "content":
                        "**拍摄角度建议**\n" + "\n".join(f"• {a}" for a in ang)}},
                    {"tag": "hr"},
                    {"tag": "action", "actions": [
                        {"tag": "button", "text": {"tag": "plain_text", "content": "✅ 采纳"},
                         "type": "primary", "value": dict(val, action="tp_adopt")},
                        {"tag": "button", "text": {"tag": "plain_text", "content": "🔁 重新生成"},
                         "type": "default", "value": dict(val, action="tp_regen")},
                        {"tag": "button", "text": {"tag": "plain_text", "content": "📝 去表格改"},
                         "type": "default", "url": base},
                    ]},
                    {"tag": "note", "elements": [{"tag": "plain_text",
                        "content": "这些会自动用进该产品所有寄样暖信 brief 段(全英文)。"}]},
                ],
            }
            for nm, oid in await feishu.resolve_notify_targets("reviewer"):
                try:
                    uid = await feishu.open_id_to_union_id(oid)
                    if uid:
                        await feishu.send_card_via_app3("union_id", uid, card)
                    else:
                        await feishu.send_card_message("open_id", oid, card)  # 降级旧卡(按钮回调走不到,但能看+去表格)
                except Exception as e:
                    print(f"[talking_points] notify {nm} fail: {e}")
        except Exception as e:
            print(f"[talking_points] notify fail: {e}")
    return {"ok": True, "rid": prod_rid, "product": name, "talking_points": tp, "shooting_angles": ang,
            "shopify_page_chars": len(page_text)}


async def run(overwrite: bool = False) -> dict:
    """扫 上架状态=主推 且缺 Talking Points 的产品 → AI 生成 + 通知运营审."""
    items = await feishu.search_records(config.T_PRODUCT, [
        {"field_name": "上架状态", "operator": "is", "value": ["主推"]},
    ])
    results = []
    generated = 0
    for it in items:
        if ext(it["fields"].get("Talking Points")).strip() and not overwrite:
            continue
        try:
            r = await generate_for_product(it["record_id"], overwrite=overwrite, notify=True)
        except Exception as e:
            r = {"ok": False, "rid": it["record_id"], "error": str(e)[:150]}
        if r.get("ok"):
            generated += 1
        results.append(r)
    return {"主推产品": len(items), "生成": generated, "results": results[:20]}


# ===== G-A / G-B: per-KOL 定制 brief (framework 推荐 + 5 hook 句式 + TikTok SEO) =====
# 老师方法论 (reference_video_brief_tool_dingtalk): brief 应 per-KOL 定制(KOL 风格×产品×框架交叉),
# 不是 per-product 全员共用; AI 推荐框架 + 5 种 hook 句式(POV/疑问/否定/内心独白/测试型)。
# 输出不写产品库(会被下个 KOL 覆盖)→ 由 warm_recap 存到草稿「Per-KOL Brief」+ 暖信卡展示。

async def _fetch_frameworks() -> list:
    """拉框架结构库 12 个「停病药信买」框架 (跨 app, 聪哥2号 token). 拉不到返回 [] 降级(不阻断)."""
    try:
        path = f"/bitable/v1/apps/{FRAMEWORK_APP_TOKEN}/tables/{FRAMEWORK_TABLE_ID}/records?page_size=50"
        r = await feishu.api("GET", path)
        items = (r.get("data") or {}).get("items") or []
    except Exception as e:
        print(f"[talking_points] 框架库拉取失败: {e}")
        return []
    out = []
    for it in items:
        f = it.get("fields") or {}
        nm = ext(f.get("框架名称")).strip()
        formula = ext(f.get("短视频底层结构公式‼️")).strip()
        if not nm and not formula:
            continue
        out.append({"name": nm or formula, "formula": formula,
                    "scene": ext(f.get("适用场景")).strip()[:160]})
    return out


# 主平台 → 默认内容形式倾向 (AI 可按风格/近期标题上调; YouTube 默认长视频但可 Shorts)
_SHORT_PLATFORMS = {"TikTok", "Instagram", "Facebook"}
_LONG_PLATFORMS = {"YouTube", "Twitch"}


def _format_hint(platform: str) -> str:
    if platform in _LONG_PLATFORMS:
        return "long"
    return "short"  # TikTok/IG/FB/X/其他 默认短视频


def format_kol_brief_md(product: str, kol: str, platform: str, content_format: str,
                        frameworks: list, hooks: list, seo_keyword: str, seo_note: str,
                        title_or_caption: str, tags: list, cta: str = "") -> str:
    """把 per-KOL brief 各部分拼成人类可读文本 (存草稿「Per-KOL Brief」+ 暖信卡展示). 平台无关 + 长/短自适应."""
    is_long = (content_format == "long")
    fmt_label = "长视频" if is_long else "短视频"
    plat = platform or "?"
    lines = [f"🎬 Per-KOL Brief — {kol} × {product}  ({plat} · {fmt_label})"]
    if frameworks:
        lines.append("\n▶ 推荐视频框架:")
        lines += [f"  • {fw.get('name','')} — {fw.get('why','')}" for fw in frameworks]
    if hooks:
        hook_label = "开头 30 秒留人钩子" if is_long else "5 种 Hook 候选 (字幕/口播开头)"
        lines.append(f"\n▶ {hook_label}:")
        lines += [f"  • [{h.get('type','')}] {h.get('text','')}" for h in hooks]
    if seo_keyword:
        lines.append(f"\n▶ {plat} 核心关键词: " + seo_keyword + (f" — {seo_note}" if seo_note else ""))
    if title_or_caption:
        tc_label = "视频标题建议" if is_long else "Caption"
        lines.append(f"▶ {tc_label}: " + title_or_caption)
    if tags:
        tag_label = "标签 (Tags)" if is_long else "Hashtags"
        lines.append(f"▶ {tag_label}: " + " ".join(tags))
    if cta:
        lines.append("\n▶ 购买引导 (CTA — 让种草粉丝知道去哪买): " + cta)
    return "\n".join(lines)


async def generate_for_kol(prod_rid: str, kol_rid: str) -> dict:
    """per-KOL 定制 brief. 返回 dict (含 email_bullets 给暖信正文 + brief_md 给字段/卡片).
    失败/缺记录 → {"ok": False, ...}, 调用方(warm_recap) 降级 per-product。不写任何表。"""
    if not prod_rid or not kol_rid:
        return {"ok": False, "skip": "缺 prod_rid 或 kol_rid"}
    try:
        prod = await feishu.get_record(config.T_PRODUCT, prod_rid)
        kol = await feishu.get_record(config.T_KOL, kol_rid)
    except Exception as e:
        return {"ok": False, "skip": f"读记录失败: {e}"}
    pf = prod["fields"]
    kf = kol["fields"]
    name = ext(pf.get("产品英文名")) or ext(pf.get("产品名")) or "the product"
    cat = ext(pf.get("品类"))
    desc = ext(pf.get("产品简述"))
    selling = [ext(pf.get(f"卖点{i}")) for i in (1, 2, 3) if ext(pf.get(f"卖点{i}"))]
    tp_existing = ext(pf.get("Talking Points")).strip()

    # KOL 画像 (内容风格是多选 → ext 只取首个, 这里手动 join 全部)
    kol_name = ext(kf.get("账号名")) or "the creator"
    platform = ext(kf.get("主平台"))
    fans = int(kf.get("粉丝数", 0) or 0)
    styles_v = kf.get("内容风格")
    styles = ", ".join(str(s) for s in styles_v) if isinstance(styles_v, list) else ext(styles_v)
    ip_pref = ext(kf.get("IP喜好"))
    pub_kw = ext(kf.get("上稿匹配关键词"))
    country = ext(kf.get("国家"))
    lang = ext(kf.get("语言")) or "en"

    hint = _format_hint(platform)  # 主平台默认长/短倾向 (AI 可按风格上调)
    # 产品信息源 (肥→瘦): 亚马逊 listing(标题+五点, 最肥) → Shopify body_html(常稀疏). 都 fail-safe 降级。
    amz_text = await _fetch_amazon_listing(ext_url(pf.get("亚马逊链接")) or "")
    page_text = await _fetch_shopify_body(ext_url(pf.get("官网链接")) or "")
    frameworks = await _fetch_frameworks()
    fw_block = "\n".join(
        f'- {fw["name"]} | formula: {fw["formula"]} | when to use: {fw["scene"]}'
        for fw in frameworks) or "(framework library unavailable — propose your own structure)"
    page_block = ""
    if amz_text:
        page_block += (f'\nAmazon listing (RICHEST source — real title + bullet points; MINE it for the '
                       f'concrete features/benefits the short selling points missed, reframe as audience '
                       f'benefit, do NOT dump specs):\n{amz_text}\n')
    if page_text:
        page_block += f'\nStore product page copy (supplement): {page_text}\n'

    prompt = f"""You are a creator-marketing strategist briefing ONE specific KOL on how to post about a product to THEIR audience. Tailor everything to this creator's platform + content format + style + audience.

CREATOR:
- Handle: {kol_name}
- Main platform: {platform or "unknown"} | followers: {fans:,} | country: {country or "?"} | language: {lang}
- Content style: {styles or "unknown"}
- IP / niche interests: {ip_pref or "n/a"}
- Past content keywords: {pub_kw or "n/a"}
- Likely content format for {platform or "this platform"}: **{hint}** (default — but YOU decide: YouTube can be Shorts, IG can be long-form, gaming channels skew long reviews).

PRODUCT: {name} ({cat})
Selling points (Chinese — translate + reframe into audience benefit): {' / '.join(selling) if selling else 'n/a'}
{f'Existing generic talking points: {tp_existing}' if tp_existing else ''}{f'Description: {desc}' if desc else ''}{page_block}

VIDEO FRAMEWORK LIBRARY (pick the best fit, by exact name; the 停病药信买 logic works for both short & long, long just expands each beat):
{fw_block}

FIRST decide content_format: "short" (TikTok/Reels/Shorts, <60s, caption-hook driven) or "long" (YouTube reviews/unboxings, multi-minute, spoken intro). Then produce a brief MATCHING that format & platform.

Return JSON ONLY:
{{
  "content_format": "short" or "long",
  "recommended_frameworks": [{{"name": "<exact framework name from library>", "why": "<=18 words why it fits THIS creator+product+format"}}],
  "hooks": [{{"type": "...", "text": "..."}}],
  "seo_keyword": "<1 core SEARCH keyword for THIS platform>",
  "seo_note": "<=18 words why + WHERE to place it on {platform or "this platform"} (YouTube=title/tags; TikTok/IG/FB=caption/hashtags)",
  "title_or_caption": "<short: a ready post caption <=200 chars / long: a click-worthy video TITLE>",
  "tags": ["..."],
  "cta": "<purchase guidance: WHERE to place the buy link(s)+discount code we email, and how to prompt fans to buy on {platform or "this platform"}. Refer to the code ONLY as the literal token [CODE] and the percent ONLY as [X]% — never a made-up code>",
  "email_bullets": ["...", "..."]
}}
Rules:
- recommended_frameworks: 1-3, best first, name MUST be copied verbatim from the library.
- hooks: if content_format=="short" → EXACTLY 5 caption hooks, types in order POV / 疑问 / 否定 / 内心独白 / 测试型. If "long" → 2-3 first-30-seconds spoken retention hooks (type can be "开场钩子").
- seo_keyword: a real search term people use on {platform or "the platform"} (NOT hardcoded to TikTok).
- tags: 5-8 — hashtags (lowercase) for short platforms, or YouTube tags for long.
- cta: tell the creator WHERE to place the purchase link(s) + discount code WE WILL EMAIL THEM, and how to prompt fans, so a seeded viewer instantly knows where & how to buy. CHANNEL-AGNOSTIC — we may send a store link (with the creator's discount code), an Amazon link, or a local-store link; the creator uses whichever fit(s) their audience's region. Platform reality: YouTube → put link(s) in the description + pin a comment with the code + say it out loud; TikTok / IG / Shorts → in-video links don't work, so put link(s) in bio and say/show the discount code on screen. NEVER invent or guess a discount code or URL — the real code/link come in our email. When you must reference the code, write the literal placeholder [CODE]; for the percent write [X]% (e.g. "use code [CODE] for [X]% off"). <=45 words.
- email_bullets: 3-4 SHORT casual soft suggestions for a gifting email (optional-sounding, NOT a rigid script).
- English only. Platform-native to {platform or "the platform"} + {styles or "their"} style. No brand-logo/on-screen-text instructions. Guardrail brief, not a script."""

    try:
        out = await deepseek.chat_json(prompt, max_tokens=900, temperature=0.4)
    except Exception as e:
        return {"ok": False, "skip": f"DeepSeek 失败: {e}"}
    cf = str(out.get("content_format") or hint).strip().lower()
    if cf not in ("short", "long"):
        cf = hint
    max_hooks = 5 if cf == "short" else 3
    rf = [x for x in (out.get("recommended_frameworks") or []) if isinstance(x, dict) and x.get("name")][:3]
    hooks = [x for x in (out.get("hooks") or []) if isinstance(x, dict) and str(x.get("text") or "").strip()][:max_hooks]
    seo_keyword = str(out.get("seo_keyword") or "").strip()
    seo_note = str(out.get("seo_note") or "").strip()
    title_or_caption = str(out.get("title_or_caption") or "").strip()
    tags = [str(h).strip() for h in (out.get("tags") or []) if str(h).strip()][:8]
    cta = str(out.get("cta") or "").strip()
    email_bullets = [str(b).strip() for b in (out.get("email_bullets") or []) if str(b).strip()][:4]
    if not hooks and not rf:
        return {"ok": False, "skip": "AI 未产出 brief", "raw": out}
    brief_md = format_kol_brief_md(name, kol_name, platform, cf, rf, hooks,
                                   seo_keyword, seo_note, title_or_caption, tags, cta)
    return {"ok": True, "product": name, "kol": kol_name, "platform": platform, "content_format": cf,
            "recommended_frameworks": rf, "hooks": hooks,
            "seo_keyword": seo_keyword, "seo_note": seo_note,
            "title_or_caption": title_or_caption, "tags": tags, "cta": cta,
            "email_bullets": email_bullets, "brief_md": brief_md,
            "amazon_chars": len(amz_text), "shopify_page_chars": len(page_text),
            "frameworks_count": len(frameworks)}

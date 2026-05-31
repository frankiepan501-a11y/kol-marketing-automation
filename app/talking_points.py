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


def format_kol_brief_md(product: str, kol: str, frameworks: list, hooks: list,
                        kw: str, kw_reason: str, caption: str, hashtags: list) -> str:
    """把 per-KOL brief 各部分拼成人类可读文本 (存草稿「Per-KOL Brief」字段 + 暖信卡展示)."""
    lines = [f"🎬 Per-KOL Brief — {kol} × {product}"]
    if frameworks:
        lines.append("\n▶ 推荐视频框架:")
        lines += [f"  • {fw.get('name','')} — {fw.get('why','')}" for fw in frameworks]
    if hooks:
        lines.append("\n▶ 5 种 Hook 候选 (字幕/口播开头):")
        lines += [f"  • [{h.get('type','')}] {h.get('text','')}" for h in hooks]
    if kw:
        lines.append("\n▶ TikTok 核心关键词: " + kw + (f" — {kw_reason}" if kw_reason else ""))
    if caption:
        lines.append("▶ Caption: " + caption)
    if hashtags:
        lines.append("▶ Hashtags: " + " ".join(hashtags))
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

    page_text = await _fetch_shopify_body(ext_url(pf.get("官网链接")) or "")
    frameworks = await _fetch_frameworks()
    fw_block = "\n".join(
        f'- {fw["name"]} | formula: {fw["formula"]} | when to use: {fw["scene"]}'
        for fw in frameworks) or "(framework library unavailable — propose your own structure)"
    page_block = (f'\nSeller product page copy (mine for features the selling points missed; '
                  f'reframe as benefit, do NOT dump specs): {page_text}\n') if page_text else ''

    prompt = f"""You are a creator-marketing strategist briefing ONE specific KOL on how to post about a product to THEIR audience. Tailor everything to this creator's style + platform + audience.

CREATOR:
- Handle: {kol_name}
- Main platform: {platform or "unknown"} | followers: {fans:,} | country: {country or "?"} | language: {lang}
- Content style: {styles or "unknown"}
- IP / niche interests: {ip_pref or "n/a"}
- Past content keywords: {pub_kw or "n/a"}

PRODUCT: {name} ({cat})
Selling points (Chinese — translate + reframe into audience benefit): {' / '.join(selling) if selling else 'n/a'}
{f'Existing generic talking points: {tp_existing}' if tp_existing else ''}{f'Description: {desc}' if desc else ''}{page_block}

VIDEO FRAMEWORK LIBRARY (pick the best fit for THIS creator + product, by exact name):
{fw_block}

Produce a per-KOL brief. Return JSON ONLY:
{{
  "recommended_frameworks": [{{"name": "<exact framework name from library>", "why": "<=18 words why it fits THIS creator+product"}}],
  "hooks": [
    {{"type": "POV", "text": "pov: ... (2nd-person immersive caption hook)"}},
    {{"type": "疑问", "text": "a curiosity question hook"}},
    {{"type": "否定", "text": "a contrarian don't/stop hook"}},
    {{"type": "内心独白", "text": "an honest inner-voice hook"}},
    {{"type": "测试型", "text": "a test/challenge hook"}}
  ],
  "tiktok_keyword": "<1 core TikTok search keyword the post should target>",
  "tiktok_keyword_reason": "<=16 words why this keyword fits creator's audience",
  "caption": "<1 ready-to-post English caption, <=200 chars, includes the keyword naturally>",
  "hashtags": ["#niche", "#product", "..."],
  "email_bullets": ["...", "..."]
}}
Rules:
- recommended_frameworks: 1-3, best first, name MUST be copied verbatim from the library above.
- hooks: EXACTLY these 5 types in this order, each a ready-to-use English caption hook tailored to creator+product.
- hashtags: 5-8, lowercase, mix niche + product + reach.
- email_bullets: 3-4 SHORT casual soft suggestions for a gifting email (optional-sounding, NOT a rigid script).
- English only. Match {platform or "the platform"} + {styles or "their"} style. No brand-logo/on-screen-text instructions. Guardrail brief, not a script."""

    try:
        out = await deepseek.chat_json(prompt, max_tokens=900, temperature=0.4)
    except Exception as e:
        return {"ok": False, "skip": f"DeepSeek 失败: {e}"}
    rf = [x for x in (out.get("recommended_frameworks") or []) if isinstance(x, dict) and x.get("name")][:3]
    hooks = [x for x in (out.get("hooks") or []) if isinstance(x, dict) and str(x.get("text") or "").strip()][:5]
    kw = str(out.get("tiktok_keyword") or "").strip()
    kw_reason = str(out.get("tiktok_keyword_reason") or "").strip()
    caption = str(out.get("caption") or "").strip()
    hashtags = [str(h).strip() for h in (out.get("hashtags") or []) if str(h).strip()][:8]
    email_bullets = [str(b).strip() for b in (out.get("email_bullets") or []) if str(b).strip()][:4]
    if not hooks and not rf:
        return {"ok": False, "skip": "AI 未产出 brief", "raw": out}
    brief_md = format_kol_brief_md(name, kol_name, rf, hooks, kw, kw_reason, caption, hashtags)
    return {"ok": True, "product": name, "kol": kol_name,
            "recommended_frameworks": rf, "hooks": hooks,
            "tiktok_keyword": kw, "tiktok_keyword_reason": kw_reason,
            "caption": caption, "hashtags": hashtags,
            "email_bullets": email_bullets, "brief_md": brief_md,
            "shopify_page_chars": len(page_text), "frameworks_count": len(frameworks)}

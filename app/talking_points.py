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


async def generate_for_product(prod_rid: str, overwrite: bool = False, notify: bool = True) -> dict:
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
            card = {
                "header": {"template": "blue", "title": {"tag": "plain_text",
                           "content": f"🧠 AI 已草拟 brief 要点 — 请审核采纳 ({name})"}},
                "elements": [
                    {"tag": "div", "text": {"tag": "lark_md", "content":
                        f"AI 根据产品卖点为 **{name}** 草拟了寄样暖信用的 brief 要点(已写入产品库,**采纳就留着,要改直接在产品库改**):"}},
                    {"tag": "div", "text": {"tag": "lark_md", "content":
                        "**Talking Points**\n" + "\n".join(f"• {t}" for t in tp)}},
                    {"tag": "div", "text": {"tag": "lark_md", "content":
                        "**拍摄角度建议**\n" + "\n".join(f"• {a}" for a in ang)}},
                    {"tag": "div", "text": {"tag": "lark_md", "content":
                        "_这些会自动用进该产品的所有寄样暖信 brief 段(全英文)。_"}},
                    {"tag": "action", "actions": [{"tag": "button", "text": {"tag": "plain_text",
                        "content": "打开产品库核对/修改"}, "url": base, "type": "primary"}]},
                ],
            }
            for _, oid in await feishu.resolve_notify_targets("reviewer"):
                try:
                    await feishu.send_card_message("open_id", oid, card)
                except Exception:
                    pass
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

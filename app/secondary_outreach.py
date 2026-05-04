# -*- coding: utf-8 -*-
"""Phase 3.3 — 二次维护: 给已合作 KOL 自动发新产品 warm follow-up.

触发条件:
- 合作状态 ∈ {已合作-免费, 已合作-免费(多次), 已合作-付费}
- 「上次二次接触时间」≥ 30 天前 (或 空)
- 邮箱有效 (邮箱验真状态 ∈ {未验, 有效})

逻辑:
1. 扫 KOL 主表筛出符合条件的 KOL
2. 找本周主推产品 (产品库 上架状态=主推 + 派单就绪)
3. 调 reply_drafter 的 warm_followup 路径生草稿 (走 reviewer + auto_send 现有流水线)
4. 写 KOL 主表「上次二次接触时间」防 30 天内重复

调用: cron 每周二 09:00 BJ (避开周一主推勾选 + 09:05 cold dispatch).

本模块复用 reply_drafter 的草稿写入路径, 但 邮件草稿来源 = "secondary_outreach" 区分.
"""
import time
import re
from . import config, feishu, deepseek, draft_router
from . import utm as _utm
from .feishu import ext, xrid


SECONDARY_INTERVAL_DAYS = 30  # 距上次二次接触 ≥ 30 天


def _slugify(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^\w]+", "_", s)
    return re.sub(r"_+", "_", s).strip("_") or "unknown"


def _next_send_time(country_iso: str = "US"):
    """复用 enrich._next_send_time 的简化版 — 返回 (send_ms, desc)"""
    from .enrich import _next_send_time as _f
    return _f(country_iso or "US")


def _sender_signature(brand: str) -> str:
    return {
        "POWKONG": "Frankie\nPOWKONG Partnerships",
        "FUNLAB": "Frankie\nFUNLAB Creator Team",
    }.get(brand.upper(), "Frankie\nPartnerships")


def _first_name(s: str) -> str:
    if not s: return "there"
    return (s.strip().split() or ["there"])[0][:30]


async def _gen_warm_followup(contact_name: str, prev_brand: str, new_product: dict,
                              brand: str) -> dict:
    """DeepSeek 生 warm follow-up 邮件 (新产品 + 引用上次合作).

    new_product: 飞书产品记录 (含 产品英文名 / 卖点 / 官网链接)
    """
    pf = new_product["fields"]
    p_en = ext(pf.get("产品英文名")) or ext(pf.get("产品名"))
    p_brand = ext(pf.get("品牌"))
    p_s1 = ext(pf.get("卖点1"))
    p_s2 = ext(pf.get("卖点2"))
    p_url_raw = ext(pf.get("官网链接")) or ""

    # UTM (区分: 加 _2nd 后缀避开和首次 cold campaign 撞车)
    p_url = _utm.make_utm_link(p_url_raw, brand, p_en + " 2nd", contact_name) if p_url_raw else ""
    sig = _sender_signature(brand)
    first = _first_name(contact_name)

    prompt = f"""你给一位 **已合作过** 的海外 KOL 发 warm follow-up 邮件,推介**新产品**.

【关键约束】
- 这不是 cold email — KOL 之前已合作过 {prev_brand} 品牌的产品,关系是熟人
- 60-100 词,真人口吻,**禁正式 / 禁推销腔 / 禁 partnership 等套路词**
- 必须以 "Hey {first}," 开头
- 第 1 句引用上次合作 (e.g. "Loved working with you on the last drop —" / "Hope the {prev_brand} sample is treating you well —")
- 第 2-3 句简介新产品 (1-2 个卖点,不堆参数)
- 1 行产品链接独立段落: <p>👉 <a href="{p_url}">See it in action →</a></p>
- 软 CTA: "Want me to send one over?" / "Let me know if it's a fit and I'll ship it your way." (不催不绑)
- 结尾签名: {sig}

【新产品】
{p_en} ({p_brand})
卖点: {p_s1} / {p_s2}
官网: {p_url}

返回 JSON: {{"email_subject":"<40 字符,带 KOL 名字或场景>","email_body":"<p>Hey {first},</p><p>...</p>...<p>-- {sig.replace(chr(10), '<br>')}</p>"}}"""

    try:
        r = await deepseek.chat_json(prompt, max_tokens=600, temperature=0.3)
        return {
            "subject": (r.get("email_subject") or f"Hey {first} — got something new for you")[:200],
            "body": r.get("email_body") or "",
            "utm_url": p_url,
        }
    except Exception as e:
        return {
            "subject": f"Hey {first} — new {p_en} just dropped",
            "body": (f"<p>Hey {first},</p>"
                     f"<p>Loved working with you on the last drop. Just shipped a new {p_en} "
                     f"({p_s1}) — thought you'd dig it.</p>"
                     f"<p>👉 <a href=\"{p_url}\">See it in action →</a></p>"
                     f"<p>Want me to send one over?</p>"
                     f"<p>{sig.replace(chr(10), '<br>')}</p>"
                     f"<!-- AI 错误降级: {str(e)[:60]} -->"),
            "utm_url": p_url,
        }


async def _find_main_product():
    """找本周主推 + 派单就绪的产品 (产品库 上架状态=主推 + 4 个 checkbox 全勾)"""
    items = await feishu.search_records(config.T_PRODUCT, [
        {"field_name": "上架状态", "operator": "contains", "value": ["主推"]},
    ])
    for p in items:
        f = p["fields"]
        if (f.get("派单-库存OK") and f.get("派单-素材OK") and
            f.get("派单-文案OK") and f.get("派单-价格OK")):
            return p
    return items[0] if items else None


async def _eligible_kols():
    """筛符合二次维护的 KOL: 已合作 + 邮箱有效 + 30 天没二次接触"""
    now_ms = int(time.time() * 1000)
    eligible = []
    for status in ("已合作-免费", "已合作-免费(多次)", "已合作-付费"):
        items = await feishu.search_records(config.T_KOL, [
            {"field_name": "合作状态", "operator": "is", "value": [status]}
        ])
        for r in items:
            f = r["fields"]
            email = ext(f.get("邮箱"))
            if not email or "@" not in email or email.startswith("待补"):
                continue
            verify = ext(f.get("邮箱验真状态"))
            if verify == "风险":
                continue
            last_secondary = f.get("上次二次接触时间") or 0
            days = (now_ms - last_secondary) / 86400000 if last_secondary else 9999
            if days < SECONDARY_INTERVAL_DAYS:
                continue
            eligible.append(r)
    return eligible


async def run():
    """每周 cron: 扫已合作 KOL + 生 warm follow-up + 走 reviewer."""
    started = time.time()
    summary = {"eligible": 0, "drafts_created": 0, "errors": [], "details": []}

    product = await _find_main_product()
    if not product:
        return {"ok": False, "error": "无主推+就绪产品, 跳过本次二次维护"}
    pf = product["fields"]
    p_en = ext(pf.get("产品英文名")) or ext(pf.get("产品名"))
    brand = ext(pf.get("品牌")) or "POWKONG"
    sender_alias = "partner@powkong.com" if brand.upper() == "POWKONG" else "partner@fireflyfunlab.com"

    kols = await _eligible_kols()
    summary["eligible"] = len(kols)
    summary["product"] = p_en

    for k in kols:
        kf = k["fields"]
        kol_name = ext(kf.get("账号名"))
        kol_email = ext(kf.get("邮箱"))
        prev_brand = brand  # v1 简化: 假设上次合作也是同品牌; v2 可读跟进记录精确取

        try:
            d = await _gen_warm_followup(kol_name, prev_brand, product, brand)
        except Exception as e:
            summary["errors"].append({"rid": k["record_id"], "err": f"gen: {str(e)[:100]}"})
            continue

        send_ms, send_desc = _next_send_time(ext(kf.get("国家")) or "US")
        now_ms = int(time.time() * 1000)
        fields = {
            "邮件草稿ID": f"sec-{k['record_id'][-8:]}-{int(time.time())}",
            "关联KOL": [k["record_id"]],
            "关联产品": [product["record_id"]],
            "收件邮箱": kol_email,
            "邮件主题": d["subject"][:200],
            "邮件正文": d["body"],
            "邮件语言": "en",
            "邮件草稿状态": "待审",
            "邮件草稿来源": "secondary_outreach",
            "对象类型": "KOL",
            "发送邮箱": sender_alias,
            "发送人署名": "Frankie",
            "生成时间": now_ms,
            "建议发送时间": send_ms,
            "发送时区说明": send_desc,
            "重生次数": 0,
            "UTM 链接": d.get("utm_url", ""),
        }
        try:
            rid = await feishu.create_record(config.T_DRAFT, fields)
        except Exception as e:
            summary["errors"].append({"rid": k["record_id"], "err": f"draft: {str(e)[:100]}"})
            continue

        # 走 reviewer 自审
        try:
            route = await draft_router.route_draft(rid)
            summary["drafts_created"] += 1
            summary["details"].append({
                "kol": kol_name, "draft_rid": rid, "score": route.get("score"),
                "path": route.get("path"),
            })
        except Exception as e:
            summary["errors"].append({"draft_rid": rid, "err": f"route: {str(e)[:100]}"})

        # 写「上次二次接触时间」防 30 天内重复
        try:
            await feishu.update_record(config.T_KOL, k["record_id"],
                                       {"上次二次接触时间": now_ms})
        except Exception as e:
            summary["errors"].append({"rid": k["record_id"], "err": f"mark: {str(e)[:100]}"})

    summary["elapsed_s"] = round(time.time() - started, 1)
    return {"ok": True, **summary}

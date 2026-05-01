"""Follow-up Sequence 生成器 (D+7 第2封, D+14 第3封)"""
import re, time
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from . import config, feishu, deepseek
from .feishu import ext, xrid

FOLLOWUP_INTERVAL_DAYS = 7
STOP_STATUSES = {"已合作-免费", "已合作-免费(多次)", "已合作-付费", "不合适", "黑名单"}

# 国家时区 → 最佳发送时间
COUNTRY_TZ = {
    "US":-5,"UK":0,"DE":1,"CA":-5,"PH":8,"FR":1,"ES":1,"BR":-3,"AU":10,
    "NL":1,"IT":1,"MX":-6,"IN":5.5,"JP":9,"TH":7,"AE":4,"ID":7,"SE":1,"PT":0,
}
APAC = {"JP","TH","PH","ID","IN","AE"}


def next_send_time(country_iso: str, from_dt: datetime = None):
    from_dt = from_dt or datetime.now(timezone.utc)
    offset = COUNTRY_TZ.get(country_iso, 0)
    best_hour = 15 if country_iso in APAC else 10
    local = from_dt + timedelta(hours=offset)
    target = local.replace(hour=best_hour, minute=0, second=0, microsecond=0)
    if target <= local:
        target += timedelta(days=1)
    # 只在周二-周四发 (APAC 限周四)
    while True:
        wd = target.weekday()
        if country_iso in APAC:
            if wd == 3: break
        else:
            if wd in (1, 2, 3): break
        target += timedelta(days=1)
        target = target.replace(hour=best_hour, minute=0, second=0, microsecond=0)
    return target - timedelta(hours=offset)


async def generate_followup(round_num: int, first_draft: dict, kol: dict, product: dict,
                             brand: str, signature: str, lang: str):
    kf = kol["fields"]
    pf = product["fields"]
    # 海外营销邮件优先用「产品英文名」, 缺则降级中文剥前缀
    p_en = ext(pf.get("产品英文名"))
    if p_en:
        p_name = p_en
    else:
        p_name = re.sub(r'^[A-Z]{1,4}\d{1,4}\s*[-_·]?\s*', '', ext(pf.get("产品名"))).strip() or ext(pf.get("产品名"))
    first_subject = ext(first_draft["fields"].get("邮件主题"))
    first_body = re.sub(r'<[^>]+>', ' ', ext(first_draft["fields"].get("邮件正文")))[:400]

    # Phase 1 ROI: 给 product_url 注 UTM (与第 1 封 cold email 同 utm_content)
    from . import utm as _utm
    p_url_raw = ext(pf.get("官网链接")) or ""
    kol_handle = ext(kf.get("账号名"))
    p_url_utm = _utm.make_utm_link(p_url_raw, brand, p_name, kol_handle)

    lang_display = {"en":"English","de":"German","fr":"French","es":"Spanish","pt":"Portuguese",
                    "ja":"Japanese","it":"Italian","nl":"Dutch","sv":"Swedish"}.get(lang, "English")

    if round_num == 2:
        rules = f"""【第 2 封 Follow-up (D+7) 规则】
  ✓ 60-100 词 (比第1封短40%)
  ✓ 开头引用第1封 ("Last week I mentioned...")
  ✓ 换新角度: "for your audience" / 场景化 / 用户反馈
  ✓ 保留产品链接 (<a href="{p_url_utm}">See it in action →</a>)
  ✓ 软 CTA: "any interest at all?"
  ✗ 禁重复第1封卖点
  ✗ 禁 "just following up" 套路"""
    else:
        rules = f"""【第 3 封 Follow-up (D+14) 最后一封】
  ✓ 40-60 词, 真人口吻
  ✓ "Last check-in" / 软 close "totally understand if the timing's off"
  ✓ "should I keep you in mind for future launches?"
  ✗ 禁再介绍产品
  ✗ 禁产品链接 (soft-close)"""

    prompt = f"""你是 cold email 专家,撰写 Follow-up 第 {round_num} 封。

【上下文】
- 第 1 封已发 {7 if round_num==2 else 14} 天,无回复
- 第 1 封主题: {first_subject}
- 第 1 封摘要: {first_body[:300]}

【KOL】
账号: {ext(kf.get('账号名'))}
平台: {ext(kf.get('主平台'))}
国家: {ext(kf.get('国家原文')) or ext(kf.get('国家'))}
内容风格: {ext(kf.get('内容风格'))}

【产品】
{p_name} ({ext(pf.get('品牌'))})

{rules}

【语言】{lang_display}   【署名】{signature}

返回 JSON:
{{"email_subject":"Re: 原主题 或 新主题","email_body":"<p>...</p><p>-- {signature}</p>"}}"""

    r = await deepseek.chat_json(prompt, max_tokens=600)
    if isinstance(r, dict):
        r["_utm_url"] = p_url_utm   # Phase 1 ROI: 让 run() 写到草稿
    return r


async def run():
    now_ms = int(time.time() * 1000)

    drafts = await feishu.fetch_all_records(config.T_DRAFT)
    by_kol = defaultdict(list)
    for d in drafts:
        kid = xrid(d["fields"].get("关联KOL"))
        if kid: by_kol[kid].append(d)

    kol_map = {r["record_id"]: r for r in await feishu.fetch_all_records(config.T_KOL)}
    prod_map = {r["record_id"]: r for r in await feishu.fetch_all_records(config.T_PRODUCT)}

    stats = {"generated": 0, "skipped": 0, "errors": 0, "details": []}

    for kid, group in by_kol.items():
        first = next((d for d in group if ext(d["fields"].get("Follow-up轮次")) == "第1封"
                      and "已发" in str(ext(d["fields"].get("发送状态")))), None)
        if not first: continue

        # KOL 状态守门
        kol = kol_map.get(kid)
        if not kol: continue
        if ext(kol["fields"].get("合作状态")) in STOP_STATUSES:
            stats["skipped"] += 1; continue

        # 已回复守门
        if any(d["fields"].get("是否回复") for d in group):
            stats["skipped"] += 1; continue

        # 当前轮次
        sent_rounds = {ext(d["fields"].get("Follow-up轮次")) for d in group
                       if "已发" in str(ext(d["fields"].get("发送状态")))}
        if "第3封" in sent_rounds:
            stats["skipped"] += 1; continue
        if "第2封" in sent_rounds:
            next_round = 3
            r2 = next((d for d in group if ext(d["fields"].get("Follow-up轮次")) == "第2封"), None)
            if r2:
                sent_ms = r2["fields"].get("发送时间", 0) or 0
                if (now_ms - sent_ms) < FOLLOWUP_INTERVAL_DAYS * 86400 * 1000:
                    stats["skipped"] += 1; continue
        elif "第1封" in sent_rounds:
            next_round = 2
            sent_ms = first["fields"].get("发送时间", 0) or 0
            if (now_ms - sent_ms) < FOLLOWUP_INTERVAL_DAYS * 86400 * 1000:
                stats["skipped"] += 1; continue
        else:
            stats["skipped"] += 1; continue

        # 已存在此轮草稿跳过
        if any(ext(d["fields"].get("Follow-up轮次")) == f"第{next_round}封" for d in group):
            stats["skipped"] += 1; continue

        prod_rid = xrid(first["fields"].get("关联产品"))
        product = prod_map.get(prod_rid) if prod_rid else None
        if not product:
            stats["skipped"] += 1; continue

        sender_alias = ext(first["fields"].get("发送邮箱"))
        signature = ext(first["fields"].get("发送人署名"))
        lang = ext(first["fields"].get("邮件语言")) or "en"
        brand = "FUNLAB" if "fireflyfunlab" in sender_alias else "POWKONG"

        try:
            r = await generate_followup(next_round, first, kol, product, brand, signature, lang)
        except Exception as e:
            stats["errors"] += 1
            stats["details"].append({"kol": ext(kol["fields"].get("账号名")), "error": str(e)[:100]})
            continue

        # 建议发送时间: 上一封 + 7 天后的下一个工作日 10 AM 本地
        prev_ms = first["fields"].get("发送时间", 0) or 0
        if next_round == 3:
            r2 = next((d for d in group if ext(d["fields"].get("Follow-up轮次")) == "第2封"), None)
            if r2: prev_ms = r2["fields"].get("发送时间", 0) or prev_ms
        earliest = datetime.fromtimestamp((prev_ms / 1000) + FOLLOWUP_INTERVAL_DAYS * 86400, tz=timezone.utc)
        send_dt = next_send_time(ext(kol["fields"].get("国家")) or "US", earliest)

        try:
            sc = float(first["fields"].get("匹配度总分") or 0)
        except (ValueError, TypeError):
            sc = 0

        task_rid = xrid(first["fields"].get("关联任务"))
        fields = {
            "邮件草稿ID": f"{(task_rid or '')[:8]}-{ext(kol['fields'].get('账号名'))[:20]}-F{next_round}",
            "关联任务": [task_rid] if task_rid else [],
            "关联KOL": [kid],
            "关联产品": [prod_rid],
            "匹配度总分": sc,
            "收件邮箱": ext(first["fields"].get("收件邮箱")),
            "邮件主题": r.get("email_subject", ""),
            "邮件正文": r.get("email_body", ""),
            "邮件语言": lang,
            "邮件草稿状态": "待审",
            "邮件草稿来源": "followup",
            "对象类型": "KOL",
            "发送邮箱": sender_alias,
            "发送人署名": signature,
            "生成时间": now_ms,
            "建议发送时间": int(send_dt.timestamp() * 1000),
            "Follow-up轮次": f"第{next_round}封",
            "重生次数": 0,
            "UTM 链接": r.get("_utm_url", ""),
        }
        new_rid = await feishu.create_record(config.T_DRAFT, fields)
        stats["generated"] += 1
        detail = {"kol": ext(kol["fields"].get("账号名")), "round": next_round, "rid": new_rid}

        # === 调 router 自审 ===
        try:
            from . import draft_router
            route = await draft_router.route_draft(new_rid)
            detail["score"] = route["score"]
            detail["path"] = route["path"]
        except Exception as e:
            detail["router_err"] = str(e)[:100]
        stats["details"].append(detail)

    return stats

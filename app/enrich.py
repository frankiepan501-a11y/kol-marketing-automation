"""阶段 3+5: KOL 营销任务台触发的富化 + 打分 + 生草稿 (D2 云端版)

n8n cron 每 5 分钟扫 T_TASK_KOL 任务状态=1-待触发 + 触发=true 的任务,
对每个任务: 读关联产品 → 筛 KOL 候选 → 并发 DeepSeek 打分+生草稿 →
写「KOL·媒体人邮件草稿」 → 逐条调 draft_router 自审 → 更新任务状态。

迁移自 scripts/send_loop/enrich_and_draft.py。
"""
import re, time, asyncio, random
from typing import Optional
from . import config, feishu, deepseek, draft_router
from .feishu import ext, xrid


COUNTRY_TO_LANG = {
    "US": "en", "UK": "en", "CA": "en", "AU": "en", "PH": "en", "IN": "en",
    "DE": "de", "FR": "fr", "ES": "es", "BR": "pt", "PT": "pt",
    "JP": "ja", "IT": "it", "NL": "nl", "MX": "es", "TH": "en", "AE": "en",
    "ID": "en", "SE": "sv",
}
LANG_DISPLAY = {
    "en": "English", "de": "German", "fr": "French", "es": "Spanish",
    "pt": "Portuguese", "ja": "Japanese", "it": "Italian", "nl": "Dutch", "sv": "Swedish",
}

SIGNATURE_POOL = {
    "FUNLAB": ["Tom from FUNLAB Team", "Mia @ FUNLAB Outreach", "Alex / FUNLAB Partnership"],
    "POWKONG": ["Lisa @ POWKONG Team", "Ryan from POWKONG", "Jamie / POWKONG Partnership"],
}

# 国家时区 (复用 followup.py 的常量, 避免循环 import 这里也定义一份)
COUNTRY_TZ = {"US": -5, "UK": 0, "DE": 1, "CA": -5, "PH": 8, "FR": 1, "ES": 1,
              "BR": -3, "AU": 10, "NL": 1, "IT": 1, "MX": -6, "IN": 5.5,
              "JP": 9, "TH": 7, "AE": 4, "ID": 7, "SE": 1, "PT": 0}
APAC = {"JP", "TH", "PH", "ID", "IN", "AE"}


def _next_send_time(country_iso: str):
    """简化版调度: Tue/Wed/Thu 10AM 本地 (APAC Thu 15:00). 返回 (UTC ms, 描述)"""
    from datetime import datetime, timedelta, timezone
    now_utc = datetime.now(timezone.utc)
    offset = COUNTRY_TZ.get(country_iso, 0)
    best_hour = 15 if country_iso in APAC else 10
    local = now_utc + timedelta(hours=offset)
    target = local.replace(hour=best_hour, minute=0, second=0, microsecond=0)
    if target <= local:
        target += timedelta(days=1)
    while True:
        wd = target.weekday()
        if country_iso in APAC:
            if wd == 3: break
        else:
            if wd in (1, 2, 3): break
        target += timedelta(days=1)
        target = target.replace(hour=best_hour, minute=0, second=0, microsecond=0)
    target_utc = target - timedelta(hours=offset)
    desc = f"{['Mon','Tue','Wed','Thu','Fri','Sat','Sun'][target.weekday()]} {best_hour:02d}:00 local ({country_iso})"
    return int(target_utc.timestamp() * 1000), desc


# ===== 1. 任务扫描 =====
async def find_pending_tasks() -> list:
    """扫 KOL 营销任务台 任务状态=1-待触发 + 触发=true"""
    items = await feishu.search_records(config.T_TASK_KOL, [
        {"field_name": "任务状态", "operator": "is", "value": ["1-待触发"]},
        {"field_name": "触发", "operator": "is", "value": ["true"]},
    ])
    return items


# ===== 2. KOL 候选筛选 =====
async def filter_kols(task_fields: dict) -> list:
    """按任务条件筛选 KOL"""
    platforms_want = task_fields.get("筛选-平台") or []
    countries_want = task_fields.get("筛选-国家") or []
    styles_want = task_fields.get("筛选-内容风格") or []
    if not isinstance(platforms_want, list): platforms_want = [platforms_want]
    if not isinstance(countries_want, list): countries_want = [countries_want]
    if not isinstance(styles_want, list): styles_want = [styles_want]

    f_min = int(task_fields.get("筛选-粉丝下限") or 0)
    f_max = int(task_fields.get("筛选-粉丝上限") or 10_000_000)
    batch_limit = int(task_fields.get("批量大小") or 50)
    hard_pool = batch_limit * 10

    items = await feishu.search_records(config.T_KOL, [
        {"field_name": "合作状态", "operator": "is", "value": ["未建联"]},
        {"field_name": "邮箱", "operator": "isNotEmpty", "value": []},
    ])

    hits = []
    for rec in items:
        f = rec.get("fields", {})
        if platforms_want:
            mp = ext(f.get("主平台"))
            if mp not in platforms_want: continue
        if countries_want:
            country = ext(f.get("国家"))
            if country not in countries_want: continue
        sub = f.get("粉丝数", 0) or 0
        try: sub = int(sub)
        except (ValueError, TypeError): sub = 0
        if sub < f_min or sub > f_max: continue
        if styles_want:
            styles_raw = f.get("内容风格") or []
            if isinstance(styles_raw, str): styles_raw = [styles_raw]
            styles_list = []
            for s in styles_raw:
                if isinstance(s, dict): styles_list.append(s.get("text") or s.get("name"))
                else: styles_list.append(str(s))
            if not any(s in styles_list for s in styles_want): continue
        hits.append(rec)
        if len(hits) >= hard_pool: break

    # 排序: 腰部优先 (10w-50w 优先, 头部次之, 尾部再次)
    def sort_key(r):
        sub = r.get("fields", {}).get("粉丝数", 0) or 0
        try: sub = int(sub)
        except (ValueError, TypeError): sub = 0
        if 100_000 <= sub <= 500_000: return (3, sub)
        if sub > 500_000: return (2, -sub)
        return (1, sub)
    hits.sort(key=sort_key, reverse=True)
    return hits[:batch_limit]


# ===== 3. 单 KOL 打分 + 生草稿 =====
async def score_and_draft_one(kol_record: dict, product: dict, brand: str,
                                signature: str, threshold: int) -> dict:
    k = kol_record["fields"]
    kol_name = ext(k.get("账号名"))
    kol_platform = ext(k.get("主平台"))
    kol_country = ext(k.get("国家"))
    kol_country_cn = ext(k.get("国家原文"))
    kol_sub = k.get("粉丝数", 0) or 0
    kol_level = ext(k.get("KOL级别"))
    kol_styles = ext(k.get("内容风格"))
    kol_ip = ext(k.get("IP喜好"))
    kol_email = ext(k.get("邮箱"))
    kol_url = ext(k.get("主链接"))
    if not kol_email:
        return {"skip": "无邮箱", "kol_record_id": kol_record["record_id"]}

    pf = product["fields"]
    p_name_raw = ext(pf.get("产品名"))
    p_name = re.sub(r'^[A-Z]{1,4}\d{1,4}\s*[-_·]?\s*', '', p_name_raw).strip() or p_name_raw
    p_brand = ext(pf.get("品牌"))
    p_cat = ext(pf.get("品类"))
    p_s1 = ext(pf.get("卖点1"))
    p_s2 = ext(pf.get("卖点2"))
    p_s3 = ext(pf.get("卖点3"))
    p_url = ext(pf.get("官网链接"))
    p_price = pf.get("报价(USD)", 0)
    p_audience = ext(pf.get("目标人群"))
    p_media = ext(pf.get("媒体报道"))

    lang = COUNTRY_TO_LANG.get(kol_country, "en")
    lang_display = LANG_DISPLAY.get(lang, "English")

    prompt = f"""你是一个专业的海外 KOL 外联邮件撰稿人。请按以下 2026 年最新业内最佳实践生成邮件。

【2026 KOL 外联邮件黄金法则】(必须遵守)

📌 主题行 (<40 字符 / <=7 词): 像朋友 DM, 不像营销文案; 引用 KOL 具体内容
  ✗ 禁: "partnership"/"collaboration"/"opportunity"/"your thoughts"/"quick question"
  ✓ 例: "Piranha Plant dock for your retro setup"

📌 正文 (100-150 词):
  ✓ 第 1 句引用 KOL 具体内容 (视频/IP/风格), 禁 [xxx 占位符]
  ✓ 中段强调"为什么契合他"
  ✓ 1 行产品链接 (独立段落): <p>👉 <a href="{p_url}">{{See it in action →}}</a></p>
     - en: "See it in action →"  / de: "Sieh es live →"  / fr: "À voir en action →"
     - es: "Míralo en acción →"  / pt: "Veja em ação →"   / ja: "実物を見る →"
     - it: "Guardalo in azione →" / nl: "Zie het in actie →"
  ✓ CTA 开放式: "Would you be curious to try one? Happy to send it over, no strings attached."
  ✗ 严禁内部 SKU 代号 (YM24/PK02/FL-JC 等), 严禁 <img>, 严禁中文混杂

📌 透明度: 说清品牌/产品, 不承诺佣金, 不推销腔

📌 语言: 全文 {lang_display} (KOL 国家: {kol_country_cn or kol_country})

【任务1】打分 (5 维度,满分 100):
- 粉丝量级(0-20): 20=腰部1万-50万 / 15=头部 / 10=KOC / 5=小号 / 0=僵尸
- 内容契合(0-25): 25=近期测同类 / 18=同品类未测 / 10=泛游戏 / 0=不相关
- 地域语言(0-15): 15=母语 / 10=次要 / 5=非目标但英语 / 0=小语种
- 活跃互动(0-25): 25=近 30 天 4+条+互动率>5% / 15=1-3条 2-5% / 5=断更 / 0=半年未更
- 触达(0-15): 15=有邮箱+商务意愿 / 10=有邮箱 / 5=仅 DM / 0=无触达

【任务2】若总分 >= {threshold}, 生成主题+正文

【KOL】
账号: {kol_name} | 平台: {kol_platform} | 国家: {kol_country_cn or kol_country}
粉丝: {kol_sub:,} | 级别: {kol_level} | 风格: {kol_styles}
IP喜好: {kol_ip} | 主页: {kol_url}

【产品】
{p_name} ({p_brand} / {p_cat}) | 报价: ${p_price} USD
卖点: {p_s1} | {p_s2} | {p_s3}
官网: {p_url} | 目标人群: {p_audience}
媒体背书: {p_media or '(无)'}

【署名】{signature}

返回 JSON:
{{
  "breakdown": {{
    "粉丝量级": {{"score": 数字, "reason": "..."}},
    "内容契合": {{"score": 数字, "reason": "..."}},
    "地域语言": {{"score": 数字, "reason": "..."}},
    "活跃互动": {{"score": 数字, "reason": "..."}},
    "触达": {{"score": 数字, "reason": "..."}}
  }},
  "total_score": 总分,
  "highlights": "1-2句亮点",
  "gaps": "1-2句不足",
  "angle": "建议切入角度",
  "email_subject": "主题(若过阈值)",
  "email_body": "<p>开头</p><p>中段</p><p>CTA段</p><p>-- {signature}</p>"
}}"""

    try:
        r = await deepseek.chat_json(prompt, max_tokens=1500, temperature=0.3)
    except Exception as e:
        return {"error": str(e)[:100], "kol_record_id": kol_record["record_id"]}

    total = r.get("total_score", 0) or 0
    return {
        "kol_record_id": kol_record["record_id"],
        "kol_name": kol_name,
        "kol_email": kol_email,
        "kol_country": kol_country,
        "lang": lang,
        "total": total,
        "breakdown": r.get("breakdown", {}),
        "highlights": r.get("highlights", ""),
        "gaps": r.get("gaps", ""),
        "angle": r.get("angle", ""),
        "subject": r.get("email_subject", "") if total >= threshold else "",
        "body": r.get("email_body", "") if total >= threshold else "",
        "passed": total >= threshold,
    }


# ===== 4. 写草稿 + 调 router =====
async def write_drafts_and_route(task_rid: str, product_rid: str, brand: str,
                                  sender_alias: str, signature: str,
                                  passed_list: list) -> list:
    now_ms = int(time.time() * 1000)
    results = []
    for s in passed_list:
        if not s.get("passed"): continue
        bk = s["breakdown"]
        send_ms, send_desc = _next_send_time(s.get("kol_country", "US") or "US")
        fields = {
            "邮件草稿ID": f"{task_rid[:8]}-{s['kol_name'][:20]}",
            "关联任务": [task_rid],
            "关联KOL": [s["kol_record_id"]],
            "关联产品": [product_rid],
            "匹配度总分": s["total"],
            "粉丝量级分": bk.get("粉丝量级", {}).get("score", 0),
            "内容契合分": bk.get("内容契合", {}).get("score", 0),
            "地域语言分": bk.get("地域语言", {}).get("score", 0),
            "活跃互动分": bk.get("活跃互动", {}).get("score", 0),
            "触达分": bk.get("触达", {}).get("score", 0),
            "匹配亮点": (s.get("highlights", "") + " | 维度理由: " +
                       " / ".join(f"{k}:{v.get('reason','')[:40]}" for k, v in bk.items()))[:500],
            "匹配不足": s.get("gaps", "")[:200],
            "建议切入点": s.get("angle", "")[:200],
            "收件邮箱": s["kol_email"],
            "邮件主题": s["subject"],
            "邮件正文": s["body"],
            "邮件语言": s["lang"],
            "邮件草稿状态": "待审",
            "邮件草稿来源": "cold",
            "对象类型": "KOL",
            "发送邮箱": sender_alias,
            "发送人署名": signature,
            "生成时间": now_ms,
            "建议发送时间": send_ms,
            "发送时区说明": send_desc,
            "重生次数": 0,
        }
        try:
            rid = await feishu.create_record(config.T_DRAFT, fields)
        except Exception as e:
            results.append({"kol": s["kol_name"], "error": f"write_draft: {str(e)[:100]}"})
            continue
        # 调 router
        try:
            route = await draft_router.route_draft(rid)
            results.append({"kol": s["kol_name"], "rid": rid, "score": route["score"], "path": route["path"]})
        except Exception as e:
            results.append({"kol": s["kol_name"], "rid": rid, "router_err": str(e)[:100]})
    return results


# ===== 5. 处理一个任务 (主流程) =====
async def enrich_task(task_record: dict) -> dict:
    task_rid = task_record["record_id"]
    tf = task_record["fields"]
    task_name = ext(tf.get("任务名"))
    brand = ext(tf.get("品牌")) or "FUNLAB"
    threshold = int(tf.get("匹配度阈值") or 70)
    batch_limit = int(tf.get("批量大小") or 50)

    # 发送邮箱选项 → alias
    sender_choice = ext(tf.get("发送邮箱"))
    if "fireflyfunlab" in sender_choice or "FUNLAB" in sender_choice:
        sender_alias = "partner@fireflyfunlab.com"
    elif "powkong" in sender_choice or "POWKONG" in sender_choice:
        sender_alias = "partner@powkong.com"
    else:
        sender_alias = config.BRAND_CONFIG[brand]["alias_from"]

    signature = ext(tf.get("发送人署名")) or random.choice(SIGNATURE_POOL.get(brand, ["Frankie"]))

    # 关联产品
    prod_rid = xrid(tf.get("目标产品"))
    if not prod_rid:
        await feishu.update_record(config.T_TASK_KOL, task_rid, {
            "任务状态": "8-已取消", "备注": "未关联产品",
        })
        return {"task": task_name, "error": "无产品", "task_rid": task_rid}

    try:
        product = await feishu.get_record(config.T_PRODUCT, prod_rid)
    except Exception as e:
        await feishu.update_record(config.T_TASK_KOL, task_rid, {
            "任务状态": "8-已取消", "备注": f"读产品失败: {str(e)[:80]}",
        })
        return {"task": task_name, "error": f"读产品失败: {e}", "task_rid": task_rid}

    # 状态: 富化中
    await feishu.update_record(config.T_TASK_KOL, task_rid, {"任务状态": "3-富化中"})

    # 1. 筛 KOL
    candidates = await filter_kols(tf)
    if not candidates:
        await feishu.update_record(config.T_TASK_KOL, task_rid, {
            "任务状态": "7-已完成", "富化候选数": 0, "通过阈值数": 0, "备注": "无候选",
        })
        return {"task": task_name, "candidates": 0, "task_rid": task_rid}

    # 2. 状态: 生成草稿中 + 候选数
    await feishu.update_record(config.T_TASK_KOL, task_rid, {
        "任务状态": "4-生成草稿中", "富化候选数": len(candidates),
    })

    # 3. 并发打分+生草稿 (5 个一组,避免 DeepSeek 限速)
    sem = asyncio.Semaphore(5)
    async def _gated(kol):
        async with sem:
            return await score_and_draft_one(kol, product, brand, signature, threshold)

    scored_raw = await asyncio.gather(
        *[_gated(c) for c in candidates], return_exceptions=True,
    )
    scored = []
    for s in scored_raw:
        if isinstance(s, Exception): continue
        if isinstance(s, dict) and not s.get("error") and not s.get("skip"):
            scored.append(s)
    passed = [s for s in scored if s.get("passed")]

    # 4. 写草稿 + 调 router
    routed = await write_drafts_and_route(task_rid, prod_rid, brand, sender_alias, signature, passed)

    # 5. 状态: 草稿待审 (router 已经分了哪些自动通过/待人审/退回)
    auto_count = sum(1 for r in routed if r.get("path") == "自动通过")
    human_count = sum(1 for r in routed if r.get("path") in ("待人审", "需人改"))
    retry_count = sum(1 for r in routed if r.get("path") == "退回重生")
    await feishu.update_record(config.T_TASK_KOL, task_rid, {
        "任务状态": "5-草稿待审",
        "通过阈值数": len(passed),
        "备注": (f"自动通过 {auto_count} / 待人审 {human_count} / 退回重生 {retry_count}")[:200],
    })

    return {
        "task": task_name, "task_rid": task_rid,
        "candidates": len(candidates),
        "scored": len(scored),
        "passed": len(passed),
        "auto_pass": auto_count,
        "human_review": human_count,
        "retry": retry_count,
        "details": routed[:30],
    }


# ===== 6. 入口: 扫所有待触发任务 =====
async def run() -> dict:
    tasks = await find_pending_tasks()
    if not tasks:
        return {"processed": 0, "message": "no pending task"}

    results = []
    for t in tasks:
        try:
            r = await enrich_task(t)
            results.append(r)
        except Exception as e:
            import traceback
            results.append({
                "task_rid": t["record_id"],
                "error": str(e)[:200],
                "trace": traceback.format_exc()[-500:],
            })
    return {"processed": len(results), "results": results}

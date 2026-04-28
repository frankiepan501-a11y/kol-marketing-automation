"""阶段 3+5: KOL 营销任务台触发的富化 + 打分 + 生草稿 (v2)

变更:
- 打分本地化(scoring.score_kol),不再让 DeepSeek 算分,确定性 + 省 token
- DeepSeek 只负责生草稿(主题+正文)
- 新6维写入草稿表新字段:地区分/语言分/品类分/粉丝vs客单价分/平台分/防骚扰分

n8n cron 每 5 分钟扫 T_TASK_KOL 任务状态=2-待触发 + 触发=true 的任务,
对每个任务: 读关联产品+映射规则 → 筛 KOL 候选 → 本地打分 → 阈值过 → DeepSeek 生草稿 →
写「KOL·媒体人邮件草稿」 → 逐条调 draft_router 自审 → 更新任务状态。
"""
import re, time, asyncio, random
from . import config, feishu, deepseek, draft_router
from .feishu import ext, xrid
from .scoring import score_kol, _parse_multiselect


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

COUNTRY_TZ = {"US": -5, "UK": 0, "DE": 1, "CA": -5, "PH": 8, "FR": 1, "ES": 1,
              "BR": -3, "AU": 10, "NL": 1, "IT": 1, "MX": -6, "IN": 5.5,
              "JP": 9, "TH": 7, "AE": 4, "ID": 7, "SE": 1, "PT": 0}
APAC = {"JP", "TH", "PH", "ID", "IN", "AE"}

# 映射规则表 ID
T_MAPPING = "tblA63dLsAYTwjT8"


def _next_send_time(country_iso: str):
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
    items = await feishu.search_records(config.T_TASK_KOL, [
        {"field_name": "任务状态", "operator": "is", "value": ["2-待触发"]},
        {"field_name": "触发", "operator": "is", "value": ["true"]},
    ])
    return items


# ===== 2. 读映射规则 =====
async def get_mapping_rules(category: str, hosts: list) -> dict:
    """根据产品的品类+适配主机,读映射规则,返回:
       {expected_kol_styles: set, expected_report_cats: set,
        expected_media_types: set, want_platforms: set}"""
    rules = await feishu.search_records(T_MAPPING, [
        {"field_name": "产品品类", "operator": "is", "value": [category]},
        {"field_name": "是否启用", "operator": "is", "value": ["true"]},
    ])
    if not hosts:
        hosts = ["通用"]
    expected_styles = set()
    expected_report_cats = set()
    expected_media_types = set()
    matched_rules = 0
    for rule in rules:
        f = rule.get("fields", {})
        rule_host = ext(f.get("适配主机"))
        if rule_host in hosts or rule_host == "通用":
            expected_styles |= _parse_multiselect(f.get("KOL内容风格"))
            expected_report_cats |= _parse_multiselect(f.get("媒体人报道品类"))
            expected_media_types |= _parse_multiselect(f.get("媒体人媒体类型"))
            matched_rules += 1
    if matched_rules == 0:
        # 兜底:走通用规则
        for rule in rules:
            f = rule.get("fields", {})
            if ext(f.get("适配主机")) == "通用":
                expected_styles |= _parse_multiselect(f.get("KOL内容风格"))
                expected_report_cats |= _parse_multiselect(f.get("媒体人报道品类"))
                expected_media_types |= _parse_multiselect(f.get("媒体人媒体类型"))
                break
    return {
        "expected_styles": expected_styles,
        "expected_report_cats": expected_report_cats,
        "expected_media_types": expected_media_types,
        "matched_rules": matched_rules,
    }


# ===== 3. KOL 候选筛选 =====
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
    hard_pool = max(batch_limit * 5, 200)

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
            styles_list = list(_parse_multiselect(f.get("内容风格")))
            if not any(s in styles_list for s in styles_want): continue
        hits.append(rec)
        if len(hits) >= hard_pool: break

    return hits[:hard_pool]


# ===== 4. DeepSeek 仅生草稿(打分本地完成) =====
async def gen_draft(kol_record: dict, product: dict, brand: str,
                    signature: str, breakdown: dict, total: float) -> dict:
    k = kol_record["fields"]
    kol_name = ext(k.get("账号名"))
    kol_country = ext(k.get("国家"))
    kol_country_cn = ext(k.get("国家原文"))
    kol_sub = k.get("粉丝数", 0) or 0
    kol_styles = ext(k.get("内容风格"))
    kol_ip = ext(k.get("IP喜好"))
    kol_email = ext(k.get("邮箱"))
    kol_url = ext(k.get("主链接"))
    if not kol_email:
        return {"skip": "无邮箱"}

    pf = product["fields"]
    # 海外营销邮件优先用「产品英文名」, 缺则降级中文剥前缀
    p_en = ext(pf.get("产品英文名"))
    if p_en:
        p_name = p_en
    else:
        p_name_raw = ext(pf.get("产品名"))
        p_name = re.sub(r'^[A-Z]{1,4}\d{1,4}\s*[-_·]?\s*', '', p_name_raw).strip() or p_name_raw
        print(f"[WARN] 产品缺少「产品英文名」字段, 降级用中文剥前缀: {p_name}")
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

    # 从 breakdown 抽出亮点(高分维度)
    high_dims = sorted(breakdown.items(), key=lambda x: x[1]["score"], reverse=True)[:3]
    angle_hints = " / ".join(f"{k}:{v['reason']}" for k, v in high_dims)

    prompt = f"""你是一个专业的海外 KOL 外联邮件撰稿人。请按以下 2026 年最新业内最佳实践生成邮件。

【2026 KOL 外联邮件黄金法则】(必须遵守)

📌 主题行 (<40 字符 / <=7 词): 像朋友 DM, 不像营销文案
  ✗ 禁: "partnership"/"collaboration"/"opportunity"/"your thoughts"/"quick question"
  ✗ 禁: 通用模板主题(如 "Piranha Plant dock for your retro setup")— 任何不带KOL名字或KOL专属关键词的主题都属于"通用模板"
  ✅ 必须包含 KOL 名字(从账号名 {kol_name} 抽取) OR KOL 频道/IP 喜好的专属关键词
  ✓ 例:
    - "{kol_name}, this fits your retro corner"
    - "{kol_name}'s Switch 2 needs this dock"
    - "For your GameCube nostalgia setup"  (有具体场景,非通用)

📌 正文 (100-150 词):
  ✅ 必须以 "Hey {{KOL名字}}," 开头(从账号名提取)— 不允许 "Hey," / "Hi there," / 任何匿名开头
  ✓ 第 1 句引用 KOL 具体内容 (视频/IP/风格), 禁 [xxx 占位符]
  ✓ 中段强调"为什么契合他"(参考下方匹配亮点)
  ✓ 1 行产品链接 (独立段落): <p>👉 <a href="{p_url}">{{See it in action →}}</a></p>
     - en: "See it in action →"  / de: "Sieh es live →"  / fr: "À voir en action →"
     - es: "Míralo en acción →"  / pt: "Veja em ação →"   / ja: "実物を見る →"
     - it: "Guardalo in azione →" / nl: "Zie het in actie →"
  ✓ CTA 开放式: "Would you be curious to try one? Happy to send it over, no strings attached."
  ✗ 严禁内部 SKU 代号 (YM24/PK02/FL-JC 等), 严禁 <img>, 严禁中文混杂

📌 透明度: 说清品牌/产品, 不承诺佣金, 不推销腔
📌 语言: 全文 {lang_display} (KOL 国家: {kol_country_cn or kol_country})

【KOL】
账号: {kol_name} | 国家: {kol_country_cn or kol_country}
粉丝: {kol_sub:,} | 风格: {kol_styles} | IP喜好: {kol_ip}
主页: {kol_url}

【产品】
{p_name} ({p_brand} / {p_cat}) | 报价: ${p_price} USD
卖点: {p_s1} | {p_s2} | {p_s3}
官网: {p_url} | 目标人群: {p_audience}
媒体背书: {p_media or '(无)'}

【匹配亮点】(系统已确认 {total:.0f} 分,基于以下维度)
{angle_hints}

【署名】{signature}

返回 JSON:
{{
  "email_subject": "主题",
  "email_body": "<p>开头</p><p>中段</p><p>CTA段</p><p>-- {signature}</p>",
  "highlights": "1句话总结这位 KOL 与产品的契合点",
  "angle": "建议切入角度(英文,1句)"
}}"""

    try:
        r = await deepseek.chat_json(prompt, max_tokens=1000, temperature=0.4)
    except Exception as e:
        return {"error": f"deepseek: {str(e)[:100]}"}

    return {
        "subject": r.get("email_subject", ""),
        "body": r.get("email_body", ""),
        "highlights": r.get("highlights", ""),
        "angle": r.get("angle", ""),
    }


# ===== 5. 单 KOL: 本地打分 + 过阈值再生草稿 =====
async def score_and_draft_one(kol_record: dict, product: dict, brand: str,
                                signature: str, threshold: float,
                                expected_styles: set, want_platforms: set) -> dict:
    k = kol_record["fields"]
    kol_name = ext(k.get("账号名"))
    kol_email = ext(k.get("邮箱"))
    kol_country = ext(k.get("国家"))
    if not kol_email:
        return {"skip": "无邮箱", "kol_record_id": kol_record["record_id"]}

    # 本地打分
    total, breakdown = score_kol(k, product["fields"], expected_styles, want_platforms)
    lang = COUNTRY_TO_LANG.get(kol_country, "en")

    out = {
        "kol_record_id": kol_record["record_id"],
        "kol_name": kol_name,
        "kol_email": kol_email,
        "kol_country": kol_country,
        "lang": lang,
        "total": total,
        "breakdown": breakdown,
        "passed": total >= threshold,
    }
    if not out["passed"]:
        return out

    # 过阈值才调 DeepSeek 生草稿
    draft = await gen_draft(kol_record, product, brand, signature, breakdown, total)
    if "error" in draft or "skip" in draft:
        out["error"] = draft.get("error") or draft.get("skip")
        out["passed"] = False
        return out
    out.update({
        "subject": draft["subject"],
        "body": draft["body"],
        "highlights": draft["highlights"],
        "angle": draft["angle"],
    })
    return out


# ===== 6. 写草稿 + 调 router =====
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
            # 新 6 维分(对应 scoring.score_kol 输出)
            "地区分": bk.get("地区", {}).get("score", 0),
            "语言分": bk.get("语言", {}).get("score", 0),
            "品类分": bk.get("品类", {}).get("score", 0),
            "粉丝vs客单价分": bk.get("粉丝vs客单价", {}).get("score", 0),
            "平台分": bk.get("平台", {}).get("score", 0),
            "防骚扰分": bk.get("防骚扰", {}).get("score", 0),
            "匹配亮点": (s.get("highlights", "") + " | 维度: " +
                       " / ".join(f"{k}:{v.get('reason','')[:40]}" for k, v in bk.items()))[:500],
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
        try:
            route = await draft_router.route_draft(rid)
            results.append({"kol": s["kol_name"], "rid": rid, "score": route["score"], "path": route["path"]})
        except Exception as e:
            results.append({"kol": s["kol_name"], "rid": rid, "router_err": str(e)[:100]})
    return results


# ===== 7. 处理一个任务(主流程) =====
async def enrich_task(task_record: dict) -> dict:
    task_rid = task_record["record_id"]
    tf = task_record["fields"]
    task_name = ext(tf.get("任务名"))
    brand = ext(tf.get("品牌")) or "FUNLAB"
    threshold = float(tf.get("匹配度阈值") or 80)
    batch_limit = int(tf.get("批量大小") or 50)

    sender_choice = ext(tf.get("发送邮箱"))
    if "fireflyfunlab" in sender_choice or "FUNLAB" in sender_choice:
        sender_alias = "partner@fireflyfunlab.com"
    elif "powkong" in sender_choice or "POWKONG" in sender_choice:
        sender_alias = "partner@powkong.com"
    else:
        sender_alias = config.BRAND_CONFIG[brand]["alias_from"]

    signature = ext(tf.get("发送人署名")) or random.choice(SIGNATURE_POOL.get(brand, ["Frankie"]))

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

    # 读映射规则
    p_cat = ext(product["fields"].get("品类"))
    p_hosts = list(_parse_multiselect(product["fields"].get("适配主机")))
    mapping = await get_mapping_rules(p_cat, p_hosts)

    # 任务的筛选-平台 → set
    want_platforms = set()
    plats = tf.get("筛选-平台") or []
    if not isinstance(plats, list): plats = [plats]
    for p in plats:
        if isinstance(p, dict): want_platforms.add(p.get("text") or p.get("name") or "")
        else: want_platforms.add(str(p))

    await feishu.update_record(config.T_TASK_KOL, task_rid, {"任务状态": "3-富化中"})

    candidates = await filter_kols(tf)
    if not candidates:
        await feishu.update_record(config.T_TASK_KOL, task_rid, {
            "任务状态": "7-已完成", "富化候选数": 0, "通过阈值数": 0, "备注": "无候选",
        })
        return {"task": task_name, "candidates": 0, "task_rid": task_rid}

    await feishu.update_record(config.T_TASK_KOL, task_rid, {
        "任务状态": "4-生成草稿中", "富化候选数": len(candidates),
    })

    # 第一轮:本地打分(不调 DeepSeek)
    scored_local = []
    for kol in candidates:
        total, bk = score_kol(kol["fields"], product["fields"],
                                mapping["expected_styles"], want_platforms)
        scored_local.append({"kol": kol, "total": total, "breakdown": bk})

    # 排序取 batch_limit 内通过阈值的
    scored_local.sort(key=lambda x: x["total"], reverse=True)
    top_pass = [x for x in scored_local if x["total"] >= threshold][:batch_limit]

    # 第二轮:DeepSeek 仅对过阈值的生草稿
    sem = asyncio.Semaphore(5)
    async def _gated(item):
        async with sem:
            kol = item["kol"]
            return await score_and_draft_one(
                kol, product, brand, signature, threshold,
                mapping["expected_styles"], want_platforms,
            )

    scored_raw = await asyncio.gather(
        *[_gated(item) for item in top_pass], return_exceptions=True,
    )
    scored = []
    for s in scored_raw:
        if isinstance(s, Exception): continue
        if isinstance(s, dict) and not s.get("error") and not s.get("skip"):
            scored.append(s)
    passed = [s for s in scored if s.get("passed")]

    # 写草稿 + 调 router
    routed = await write_drafts_and_route(task_rid, prod_rid, brand, sender_alias, signature, passed)

    auto_count = sum(1 for r in routed if r.get("path") == "自动通过")
    human_count = sum(1 for r in routed if r.get("path") in ("待人审", "需人改"))
    retry_count = sum(1 for r in routed if r.get("path") == "退回重生")
    await feishu.update_record(config.T_TASK_KOL, task_rid, {
        "任务状态": "5-草稿待审",
        "通过阈值数": len(passed),
        "备注": (f"映射规则{mapping['matched_rules']} / 自动通过 {auto_count} / 待人审 {human_count} / 退回 {retry_count}")[:200],
    })

    return {
        "task": task_name, "task_rid": task_rid,
        "candidates": len(candidates),
        "local_pass": len(top_pass),
        "deepseek_ok": len(scored),
        "passed": len(passed),
        "auto_pass": auto_count,
        "human_review": human_count,
        "retry": retry_count,
    }


# ===== 8. 入口 =====
async def run() -> dict:
    tasks = await find_pending_tasks()
    if not tasks:
        return {"processed": 0, "message": "no pending KOL task"}

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

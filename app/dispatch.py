"""每日派单调度器 (#2 自动化每日营销任务派单)

每日 09:05 北京时间 n8n cron 触发:
1. 读产品库:上架状态=主推 + 4 个就绪 checkbox 全勾
2. 按品牌分组,按"品牌每日上限 / 该品牌主推产品数"分配批量大小
3. 对每个产品:读品类映射 → 在 KOL 任务台建任务 → 触发=true
4. enrich-task cron 5 分钟内自然接管(本地打分 → 过阈值生草稿)

媒体人派单 v2 后做。
"""
import time
from datetime import datetime
from . import config, feishu, product_naming
from .feishu import ext
from .scoring import _parse_multiselect


T_MAPPING = "tblA63dLsAYTwjT8"
DEFAULT_BRAND_LIMIT = 80  # 兜底:产品库未填"品牌每日上限"时

# IP 合规闸开关. 2026-06-03 Frankie 先停用"产品级一刀切"闸(待改为 KOL 级限制).
# True = 恢复产品级拦截(非合规品整个跳过派单)。
IP_GATE_ENABLED = False

# 销售国家(allowlist) → 市场语言(中文名, 对齐任务台「筛选-语言」MultiSelect 选项).
# 设计: 硬筛维度是"语言"而非"KOL 国家"——英语红人(US/UK/CA/AU)受众互通英语市场,
# 用国家硬筛会误杀(澳洲英语 YouTuber 受众覆盖美国)。语言筛保住英语池, 同时挡掉
# "卖不到的语言"(如食人花不卖日本 → 日语 KOL 被排除)。销售国家留空 → 不派生语言 →
# 不筛(选填, 不阻断派单), 边界 case 由人审兜底。
COUNTRY_TO_LANGS = {
    "US": ["英语"], "UK": ["英语"], "CA": ["英语"], "AU": ["英语"],
    "PH": ["英语"], "IN": ["英语"], "AE": ["英语"], "TH": ["英语"], "ID": ["英语"],
    "DE": ["德语"], "FR": ["法语"], "ES": ["西班牙语"], "MX": ["西班牙语"],
    "IT": ["意大利语"], "NL": ["荷兰语"], "BR": ["葡萄牙语"], "PT": ["葡萄牙语"],
    "JP": ["日语"], "SE": ["瑞典语"],
}


CATEGORY_PLATFORMS = {
    # 品类 → 推荐筛选平台(为空=不限,enrich 会全平台候选)
    "手柄": ["YouTube", "Instagram", "TikTok"],
    "收纳包": ["YouTube", "Instagram", "TikTok"],
    "充电底座": ["YouTube", "Instagram"],
    "Switch底座": ["YouTube", "Instagram"],
    "掌机": ["YouTube", "Twitch"],
    "键盘": ["YouTube", "Twitch", "Instagram"],
    "耳机": ["YouTube", "Instagram"],
    "RGB灯饰": ["Instagram", "TikTok", "YouTube"],
    "通用配件": ["YouTube", "Instagram", "TikTok"],
    "手柄配件(扳机/摇杆/面壳)": ["YouTube", "Twitch"],
}


def _fans_range_for_price(price: float) -> tuple:
    """按产品客单价决定 KOL 粉丝筛选下限"""
    if price <= 30:
        return (5_000, 0)  # 0 = 不限上限
    if price <= 100:
        return (50_000, 0)
    return (500_000, 0)


async def fetch_main_push_products() -> list:
    """读产品库主推 + 派单就绪 5 项全勾 (2026-05-26 加「展示链接OK」防戴夫死链事故重演)"""
    items = await feishu.search_records(config.T_PRODUCT, [
        {"field_name": "上架状态", "operator": "is", "value": ["主推"]},
        {"field_name": "派单-库存OK", "operator": "is", "value": ["true"]},
        {"field_name": "派单-素材OK", "operator": "is", "value": ["true"]},
        {"field_name": "派单-文案OK", "operator": "is", "value": ["true"]},
        {"field_name": "派单-价格OK", "operator": "is", "value": ["true"]},
        {"field_name": "派单-展示链接OK", "operator": "is", "value": ["true"]},
    ])
    return items


async def fetch_mapping_for_product(category: str, hosts: list) -> dict:
    """读映射表,聚合品类×适配主机的:
       - KOL 内容风格 (派 KOL 任务用)
       - 媒体人报道品类 / 媒体人媒体类型 (派 editor 任务用)"""
    rules = await feishu.search_records(T_MAPPING, [
        {"field_name": "产品品类", "operator": "is", "value": [category]},
        {"field_name": "是否启用", "operator": "is", "value": ["true"]},
    ])
    if not hosts:
        hosts = ["通用"]
    expected_styles = set()
    expected_report_cats = set()
    expected_media_types = set()
    matched = 0
    for rule in rules:
        f = rule.get("fields", {})
        rule_host = ext(f.get("适配主机"))
        if rule_host in hosts or rule_host == "通用":
            expected_styles |= _parse_multiselect(f.get("KOL内容风格"))
            expected_report_cats |= _parse_multiselect(f.get("媒体人报道品类"))
            expected_media_types |= _parse_multiselect(f.get("媒体人媒体类型"))
            matched += 1
    return {
        "expected_styles": list(expected_styles),
        "expected_report_cats": list(expected_report_cats),
        "expected_media_types": list(expected_media_types),
        "matched_rules": matched,
    }


async def create_kol_task(product: dict, batch_size: int, mapping: dict) -> dict:
    """在 KOL 任务台建一条派单任务,触发=true"""
    pf = product["fields"]
    p_name = ext(pf.get("产品名"))
    p_brand = ext(pf.get("品牌"))
    p_cat = ext(pf.get("品类"))
    try:
        p_price = float(pf.get("报价(USD)", 0) or 0)
    except (ValueError, TypeError):
        p_price = 0

    today = datetime.now().strftime("%Y%m%d")
    task_name = f"派单-{today}-{p_brand}-{p_name[:30]}"

    fans_min, fans_max = _fans_range_for_price(p_price)
    platforms = CATEGORY_PLATFORMS.get(p_cat, [])
    # 销售国家 → 市场语言(硬筛维度). 留空 → sell_langs=[] → enrich 不按语言筛(选填不阻断).
    sell_countries = list(_parse_multiselect(pf.get("销售国家")))
    sell_langs = sorted({lg for c in sell_countries for lg in COUNTRY_TO_LANGS.get(c, [])})
    sender_choice = "FUNLAB邮箱(@funlabswitch.com)" if p_brand == "FUNLAB" else "POWKONG邮箱(@powkong.com)"

    fields = {
        "任务名": task_name,
        "品牌": p_brand,
        "目标产品": [product["record_id"]],
        "筛选-平台": platforms,
        "筛选-语言": sell_langs,
        "筛选-内容风格": mapping["expected_styles"],
        "筛选-粉丝下限": fans_min,
        "筛选-粉丝上限": fans_max if fans_max else 100_000_000,
        "发送邮箱": sender_choice,
        "批量大小": batch_size,
        "匹配度阈值": 80,
        "任务状态": "2-待触发",
        "触发": True,
        "备注": f"自动派单 / 映射规则{mapping['matched_rules']}行",
    }

    rid = await feishu.create_record(config.T_TASK_KOL, fields)
    return {
        "task_rid": rid, "task_name": task_name, "brand": p_brand,
        "product": p_name, "category": p_cat, "batch_size": batch_size,
        "platforms": platforms, "expected_styles": mapping["expected_styles"],
        "matched_rules": mapping["matched_rules"],
    }


# 媒体人派单参数 (池子 302, 比 KOL 1061 小, 配额自然减)
EDITOR_DAILY_LIMIT = 30  # 每个产品每天最多派 30 个媒体人候选 (跑出来后过阈值的更少)


async def create_editor_task(product: dict, mapping: dict) -> dict:
    """在媒体人任务台建一条派单任务,触发=true.
    阈值先用 70 占位,Phase A 后跑分数分布定型.
    筛选条件: 媒体类型 + 报道品类 (从映射表), 不限国家/语言 (媒体人池小, 让 score_editor 自己扣分)"""
    pf = product["fields"]
    p_name = ext(pf.get("产品名"))
    p_brand = ext(pf.get("品牌"))
    p_cat = ext(pf.get("品类"))

    today = datetime.now().strftime("%Y%m%d")
    task_name = f"PR派单-{today}-{p_brand}-{p_name[:28]}"
    sender_choice = "FUNLAB邮箱(@fireflyfunlab.com)" if p_brand == "FUNLAB" else "POWKONG邮箱(@powkong.com)"

    fields = {
        "任务名": task_name,
        "品牌": p_brand,
        "目标产品": [product["record_id"]],
        "筛选-媒体类型": mapping["expected_media_types"],
        "筛选-报道品类": mapping["expected_report_cats"],
        "发送邮箱": sender_choice,
        "人数上限": EDITOR_DAILY_LIMIT,
        "匹配度阈值": 75,
        "任务状态": "2-待触发",
        "触发": True,
        "备注": f"自动派单(媒体人) / 映射规则{mapping['matched_rules']}行",
    }
    rid = await feishu.create_record(config.T_TASK_EDITOR, fields)
    return {
        "task_rid": rid, "task_name": task_name, "brand": p_brand,
        "product": p_name, "category": p_cat,
        "limit": EDITOR_DAILY_LIMIT,
        "expected_report_cats": mapping["expected_report_cats"],
        "expected_media_types": mapping["expected_media_types"],
        "matched_rules": mapping["matched_rules"],
        "type": "editor",
    }


async def _alert_ip_blocked(blocked: list):
    """IP 合规拦截告警 → 通知群 + Frankie。blocked = [(产品名, IP状态), ...]"""
    lines = "\n".join(f"- {name} · {status or '空(待审)'}" for name, status in blocked[:20])
    card = {
        "header": {"template": "orange",
                   "title": {"tag": "plain_text", "content": f"IP 合规拦截 {len(blocked)} 个产品未派单"}},
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md",
                "content": ("以下 FUNLAB 主推产品「IP合规状态」非合规, 已**跳过派单**"
                            "(可走非 Funlab 渠道, 不挂 FUNLAB):\n" + lines)}},
            {"tag": "div", "text": {"tag": "lark_md",
                "content": "如属误判 → 去 FUNLAB 产品库改「IP合规状态」为 合规-无IP / 合规-已授权 即恢复派单。"}},
        ],
    }
    try:
        await feishu.send_card_message("chat_id", config.NOTIFY_CHAT_ID, card, biz="KOL", level="P1")
        for name, oid in config.NOTIFY_USERS:
            if name.startswith("潘"):     # 只发 Frankie
                try:
                    await feishu.send_card_message("open_id", oid, card, biz="KOL", level="P1")
                except Exception:
                    pass
    except Exception as e:
        print(f"[dispatch] IP 拦截告警发送失败: {e}")


async def run() -> dict:
    """主入口:读主推产品 → 按品牌分配额度 → 建任务"""
    products = await fetch_main_push_products()
    if not products:
        return {"dispatched": 0, "message": "no main-push product ready"}

    # 按品牌分组 + 读"品牌每日上限"(从产品库任意一条产品读,假设运营在某个产品填了)
    by_brand = {}
    brand_limit = {}
    for p in products:
        pf = p["fields"]
        b = ext(pf.get("品牌"))
        if not b:
            continue
        by_brand.setdefault(b, []).append(p)
        try:
            lim = float(pf.get("品牌每日上限", 0) or 0)
        except (ValueError, TypeError):
            lim = 0
        if lim > 0:
            brand_limit[b] = max(brand_limit.get(b, 0), int(lim))

    results = []
    ip_blocked = []   # IP 合规拦截清单 → 循环后统一告警
    for brand, prods in by_brand.items():
        daily_limit = brand_limit.get(brand, DEFAULT_BRAND_LIMIT)
        per_product = max(10, daily_limit // max(1, len(prods)))

        for product in prods:
            pf = product["fields"]
            # IP 合规闸 (产品级一刀切) — 2026-06-03 Frankie 先停用(IP_GATE_ENABLED=False), 待改 KOL 级限制
            if IP_GATE_ENABLED:
                allowed, ip_status, ip_reason = await product_naming.ip_compliance_gate(pf)
                if not allowed:
                    pn = ext(pf.get("产品名"))
                    results.append({"skipped": pn, "reason": f"IP合规拦截: {ip_reason}", "ip_blocked": True})
                    ip_blocked.append((pn, ip_status))
                    continue
            p_cat = ext(pf.get("品类"))
            p_hosts = list(_parse_multiselect(pf.get("适配主机")))
            mapping = await fetch_mapping_for_product(p_cat, p_hosts)

            if not mapping["expected_styles"]:
                results.append({
                    "skipped": ext(pf.get("产品名")),
                    "reason": f"映射表无规则: 品类={p_cat} 主机={p_hosts}"
                })
                continue

            try:
                r = await create_kol_task(product, per_product, mapping)
                r["brand_limit"] = daily_limit
                results.append(r)
            except Exception as e:
                results.append({
                    "error": str(e)[:200],
                    "product": ext(pf.get("产品名")),
                })

            # 媒体人派单 (产品库勾选「派单-需要媒体人」才建)
            need_editor = bool(pf.get("派单-需要媒体人"))
            if need_editor:
                if not (mapping["expected_report_cats"] or mapping["expected_media_types"]):
                    results.append({
                        "skipped_editor": ext(pf.get("产品名")),
                        "reason": "映射表无媒体人规则(报道品类+媒体类型均空)",
                    })
                else:
                    try:
                        re = await create_editor_task(product, mapping)
                        results.append(re)
                    except Exception as e:
                        results.append({
                            "error": f"editor: {str(e)[:200]}",
                            "product": ext(pf.get("产品名")),
                        })

    if ip_blocked:
        await _alert_ip_blocked(ip_blocked)

    n_kol = sum(1 for r in results if r.get("task_rid") and r.get("type") != "editor")
    n_editor = sum(1 for r in results if r.get("type") == "editor")
    return {
        "dispatched": n_kol + n_editor,  # backward compat
        "dispatched_kol": n_kol,
        "dispatched_editor": n_editor,
        "ip_blocked": len(ip_blocked),
        "ip_blocked_products": [n for n, _ in ip_blocked][:20],
        "skipped": sum(1 for r in results if r.get("skipped") or r.get("skipped_editor")),
        "errors": sum(1 for r in results if r.get("error")),
        "by_brand": {b: len(ps) for b, ps in by_brand.items()},
        "brand_limits": brand_limit,
        "results": results[:60],
    }

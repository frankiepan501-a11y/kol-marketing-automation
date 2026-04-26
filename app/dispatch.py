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
from . import config, feishu
from .feishu import ext
from .scoring import _parse_multiselect


T_MAPPING = "tblA63dLsAYTwjT8"
DEFAULT_BRAND_LIMIT = 80  # 兜底:产品库未填"品牌每日上限"时

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
    """读产品库主推 + 派单就绪 4 项全勾"""
    items = await feishu.search_records(config.T_PRODUCT, [
        {"field_name": "上架状态", "operator": "is", "value": ["主推"]},
        {"field_name": "派单-库存OK", "operator": "is", "value": ["true"]},
        {"field_name": "派单-素材OK", "operator": "is", "value": ["true"]},
        {"field_name": "派单-文案OK", "operator": "is", "value": ["true"]},
        {"field_name": "派单-价格OK", "operator": "is", "value": ["true"]},
    ])
    return items


async def fetch_mapping_for_product(category: str, hosts: list) -> dict:
    """读映射表,聚合品类×适配主机的所有 KOL 内容风格"""
    rules = await feishu.search_records(T_MAPPING, [
        {"field_name": "产品品类", "operator": "is", "value": [category]},
        {"field_name": "是否启用", "operator": "is", "value": ["true"]},
    ])
    if not hosts:
        hosts = ["通用"]
    expected_styles = set()
    matched = 0
    for rule in rules:
        f = rule.get("fields", {})
        rule_host = ext(f.get("适配主机"))
        if rule_host in hosts or rule_host == "通用":
            expected_styles |= _parse_multiselect(f.get("KOL内容风格"))
            matched += 1
    return {"expected_styles": list(expected_styles), "matched_rules": matched}


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
    sender_choice = "FUNLAB邮箱(@funlabswitch.com)" if p_brand == "FUNLAB" else "POWKONG邮箱(@powkong.com)"

    fields = {
        "任务名": task_name,
        "品牌": p_brand,
        "目标产品": [product["record_id"]],
        "筛选-平台": platforms,
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
    for brand, prods in by_brand.items():
        daily_limit = brand_limit.get(brand, DEFAULT_BRAND_LIMIT)
        per_product = max(10, daily_limit // max(1, len(prods)))

        for product in prods:
            pf = product["fields"]
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

    return {
        "dispatched": sum(1 for r in results if r.get("task_rid")),
        "skipped": sum(1 for r in results if r.get("skipped")),
        "errors": sum(1 for r in results if r.get("error")),
        "by_brand": {b: len(ps) for b, ps in by_brand.items()},
        "brand_limits": brand_limit,
        "results": results[:30],
    }

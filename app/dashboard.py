"""数据看板聚合 (KOL + 编辑 双对象)"""
import re, time
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from . import config, feishu
from .feishu import ext, xrid

POSITIVE = {"感兴趣", "要报价"}
NEGATIVE = {"委婉拒绝", "退订"}
BJ_TZ = timezone(timedelta(hours=8))
DASH_DAILY_RETENTION_DAYS = 30


def score_band(s):
    try: s = float(s)
    except (ValueError, TypeError): return None
    if s >= 90: return "90-100分"
    if s >= 80: return "80-89分"
    if s >= 70: return "70-79分"
    if s >= 50: return "50-69分"
    return "<50分"


def multi_vals(f):
    if not f: return []
    if isinstance(f, list):
        out = []
        for x in f:
            if isinstance(x, dict):
                v = x.get("text") or x.get("name")
                if v: out.append(v)
            else: out.append(str(x))
        return out
    return [str(f)]


def _snapshot_day(value):
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / 1000, BJ_TZ).date()
    if isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=BJ_TZ)
            return parsed.astimezone(BJ_TZ).date()
        except ValueError:
            return None
    return None


def plan_snapshot_retention(existing, now, daily_days=DASH_DAILY_RETENTION_DAYS):
    """Keep 30 days of all dimensions; older history keeps weekly/monthly overview checkpoints."""
    today = now.astimezone(BJ_TZ).date()
    cutoff = today - timedelta(days=daily_days - 1)
    dated = []
    invalid = 0
    for record in existing:
        day = _snapshot_day((record.get("fields") or {}).get("统计日期"))
        if day is None:
            invalid += 1
            continue
        dated.append((record, day))

    older_days = {day for _, day in dated if day < cutoff}
    week_latest = {}
    month_latest = {}
    for day in older_days:
        iso = day.isocalendar()
        week_key = (iso.year, iso.week)
        month_key = (day.year, day.month)
        week_latest[week_key] = max(day, week_latest.get(week_key, day))
        month_latest[month_key] = max(day, month_latest.get(month_key, day))
    weekly_dates = set(week_latest.values())
    monthly_dates = set(month_latest.values())

    historical_delete_ids = []
    today_delete_ids = []
    kept_daily_detail = 0
    kept_weekly_overview = 0
    kept_monthly_overview = 0
    for record, day in dated:
        record_id = record.get("record_id")
        if day == today:
            if record_id:
                today_delete_ids.append(record_id)
            continue
        if day >= cutoff:
            kept_daily_detail += 1
            continue
        is_overview = ext((record.get("fields") or {}).get("维度类型")) == "总览"
        keep_weekly = is_overview and day in weekly_dates
        keep_monthly = is_overview and day in monthly_dates
        if keep_weekly or keep_monthly:
            kept_weekly_overview += int(keep_weekly)
            kept_monthly_overview += int(keep_monthly and not keep_weekly)
        elif record_id:
            historical_delete_ids.append(record_id)
    return {
        "cutoff_date": cutoff.isoformat(),
        "historical_delete_ids": historical_delete_ids,
        "today_delete_ids": today_delete_ids,
        "invalid_date_records": invalid,
        "kept_daily_detail": kept_daily_detail,
        "kept_weekly_overview": kept_weekly_overview,
        "kept_monthly_overview": kept_monthly_overview,
    }


async def _delete_dashboard_records(record_ids):
    for i in range(0, len(record_ids), 500):
        await feishu.api("POST",
            f"/bitable/v1/apps/{config.FEISHU_APP_TOKEN}/tables/{config.T_DASH}/records/batch_delete",
            {"records": record_ids[i:i+500]})


async def cleanup_retention(commit=False):
    """Preview or apply historical cleanup without replacing today's snapshot."""
    now = datetime.now(BJ_TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    existing = await feishu.fetch_all_records(config.T_DASH)
    retention = plan_snapshot_retention(existing, now)
    historical_ids = retention["historical_delete_ids"]
    if commit:
        await _delete_dashboard_records(historical_ids)
    return {
        "commit": commit,
        "records_before": len(existing),
        "cutoff_date": retention["cutoff_date"],
        "planned_historical_delete": len(historical_ids),
        "deleted": len(historical_ids) if commit else 0,
        "expected_records_after": len(existing) - (len(historical_ids) if commit else 0),
        "today_records_untouched": len(retention["today_delete_ids"]),
        "invalid_date_records": retention["invalid_date_records"],
        "kept_daily_detail": retention["kept_daily_detail"],
        "kept_weekly_overview": retention["kept_weekly_overview"],
        "kept_monthly_overview": retention["kept_monthly_overview"],
    }


async def run():
    now = datetime.now(BJ_TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    today_ms = int(now.timestamp() * 1000)

    drafts = await feishu.fetch_all_records(config.T_DRAFT)
    drafts = [d for d in drafts if ext(d["fields"].get("发送状态")) in ("已发", "已发送")]
    kol_drafts = [d for d in drafts if ext(d["fields"].get("对象类型")) != "媒体人"]
    editor_drafts = [d for d in drafts if ext(d["fields"].get("对象类型")) == "媒体人"]

    kol_recs = await feishu.fetch_all_records(config.T_KOL)
    kol_map = {r["record_id"]: r for r in kol_recs}

    ed_recs = await feishu.fetch_all_records(config.T_EDITOR)
    ed_map = {r["record_id"]: r for r in ed_recs}

    prod_recs = await feishu.fetch_all_records(config.T_PRODUCT)
    prod_map = {r["record_id"]: ext(r["fields"].get("产品名")) for r in prod_recs}

    def enrich_kol(d):
        f = d["fields"]
        kid = xrid(f.get("关联KOL"))
        pid = xrid(f.get("关联产品"))
        km = kol_map.get(kid, {}).get("fields", {}) if kid else {}
        note = ext(km.get("迁移备注"))
        m = re.search(r"抓取关键词\s*[:：]\s*([^|]+?)(?:\||$)", note)
        kw = m.group(1).strip() if m else "(未知)"
        sender = ext(f.get("发送邮箱"))
        brand = config.brand_from_text(sender) or "?"   # 2026-06-09 配置驱动(支持白牌)
        try: sc = float(f.get("匹配度总分") or 0)
        except (ValueError, TypeError): sc = 0
        return {
            "obj": "KOL", "brand": brand,
            "country": ext(km.get("国家")) or "未知",
            "keyword": kw,
            "product": prod_map.get(pid, "(未知)") if pid else "(未知)",
            "signature": ext(f.get("发送人署名")),
            "score": sc, "score_band": score_band(sc),
            "language": ext(f.get("邮件语言")),
            "replied": bool(f.get("是否回复")),
            "intent": ext(f.get("回复意图")),
        }

    def enrich_editor(d):
        f = d["fields"]
        eid = xrid(f.get("关联媒体人"))
        pid = xrid(f.get("关联产品"))
        em = ed_map.get(eid, {}).get("fields", {}) if eid else {}
        sender = ext(f.get("发送邮箱"))
        brand = config.brand_from_text(sender) or "?"   # 2026-06-09 配置驱动(支持白牌)
        try: sc = float(f.get("匹配度总分") or 0)
        except (ValueError, TypeError): sc = 0
        return {
            "obj": "媒体人", "brand": brand,
            "country": ext(em.get("国家")) or "未知",
            "product": prod_map.get(pid, "(未知)") if pid else "(未知)",
            "signature": ext(f.get("发送人署名")),
            "score": sc, "score_band": score_band(sc),
            "language": ext(f.get("邮件语言")),
            "replied": bool(f.get("是否回复")),
            "intent": ext(f.get("回复意图")),
            "main_media": ext(em.get("主要媒体")),
            "media_group": ext(em.get("媒体集团")),
            "media_type": ext(em.get("媒体类型")),
            "categories": multi_vals(em.get("报道品类")),
        }

    enriched_kol = [enrich_kol(d) for d in kol_drafts]
    enriched_ed = [enrich_editor(d) for d in editor_drafts]

    # 2026-05-22 Bug1 修复: 回写任务台「已发送数/回复数/感兴趣数」(5/14 看板数据缺失).
    # 任务台 schema 早有这 3 个字段但从无代码写 → 运营看板一直空.
    # 按草稿「关联任务」分组重算 (drafts 已过滤=已发送), 幂等无 race; KOL/媒体人 分写各自任务台.
    task_stats = defaultdict(lambda: {"sent": 0, "replied": 0, "interested": 0})
    for d in drafts:
        f = d["fields"]
        task_rid = xrid(f.get("关联任务"))
        if not task_rid:
            continue
        obj = "媒体人" if ext(f.get("对象类型")) == "媒体人" else "KOL"
        st = task_stats[(obj, task_rid)]
        st["sent"] += 1
        if bool(f.get("是否回复")):
            st["replied"] += 1
        if ext(f.get("回复意图")) in POSITIVE:
            st["interested"] += 1
    task_written = 0
    for (obj, task_rid), st in task_stats.items():
        table = config.T_TASK_EDITOR if obj == "媒体人" else config.T_TASK_KOL
        try:
            await feishu.update_record(table, task_rid, {
                "已发送数": st["sent"], "回复数": st["replied"], "感兴趣数": st["interested"],
            })
            task_written += 1
        except Exception as e:
            print(f"[dashboard] 任务台计数回写失败 task={task_rid}: {e}")

    def agg(records):
        n = len(records)
        if n == 0: return None
        replied = sum(1 for r in records if r["replied"])
        positive = sum(1 for r in records if r["replied"] and r["intent"] in POSITIVE)
        negative = sum(1 for r in records if r["replied"] and r["intent"] in NEGATIVE)
        avg = sum(r["score"] or 0 for r in records) / n
        return {
            "send": n, "replied": replied, "positive": positive, "negative": negative,
            "reply_rate": replied/n, "positive_rate": positive/n, "avg_score": avg,
            "sample_hint": "样本<10, 仅供参考" if n < 10 else "",
        }

    snapshots = []
    def add(obj, dim_type, dim_value, brand, recs):
        a = agg(recs)
        if not a: return
        snapshots.append({
            "对象类型": obj, "维度类型": dim_type, "维度值": dim_value, "品牌": brand,
            "统计日期": today_ms,
            "发送量": a["send"], "回复数": a["replied"],
            "正向回复数": a["positive"], "负面回复数": a["negative"],
            "回复率": round(a["reply_rate"], 4),
            "正向回复率": round(a["positive_rate"], 4),
            "平均匹配度分": round(a["avg_score"], 1),
            "样本量提示": a["sample_hint"],
        })

    # KOL 维度
    add("KOL", "总览", "全部 KOL", "全部", enriched_kol)
    for b in config.BRAND_CONFIG:   # 2026-06-09 配置驱动: 含白牌品牌维度
        add("KOL", "总览", f"品牌={b}", b, [r for r in enriched_kol if r["brand"]==b])
    by_kw = defaultdict(list)
    for r in enriched_kol: by_kw[r["keyword"]].append(r)
    for k, rs in by_kw.items(): add("KOL", "关键词", k, "全部", rs)
    by_c = defaultdict(list)
    for r in enriched_kol: by_c[r["country"]].append(r)
    for c, rs in by_c.items(): add("KOL", "国家", c, "全部", rs)
    by_sb = defaultdict(list)
    for r in enriched_kol:
        if r["score_band"]: by_sb[r["score_band"]].append(r)
    for s, rs in sorted(by_sb.items()): add("KOL", "匹配度段", s, "全部", rs)

    # 编辑维度
    add("媒体人", "总览", "全部 媒体人", "全部", enriched_ed)
    for b in config.BRAND_CONFIG:   # 2026-06-09 配置驱动: 含白牌品牌维度
        add("媒体人", "总览", f"品牌={b}", b, [r for r in enriched_ed if r["brand"]==b])
    by_media = defaultdict(list)
    for r in enriched_ed:
        if r["main_media"]: by_media[r["main_media"]].append(r)
    for m, rs in by_media.items(): add("媒体人", "主要媒体", m, "全部", rs)
    by_group = defaultdict(list)
    for r in enriched_ed:
        if r["media_group"]: by_group[r["media_group"]].append(r)
    for g, rs in by_group.items(): add("媒体人", "媒体集团", g, "全部", rs)
    by_type = defaultdict(list)
    for r in enriched_ed:
        if r["media_type"]: by_type[r["media_type"]].append(r)
    for t, rs in by_type.items(): add("媒体人", "媒体类型", t, "全部", rs)
    by_cat = defaultdict(list)
    for r in enriched_ed:
        for c in r["categories"]: by_cat[c].append(r)
    for c, rs in by_cat.items(): add("媒体人", "报道品类", c, "全部", rs)
    by_c = defaultdict(list)
    for r in enriched_ed: by_c[r["country"]].append(r)
    for c, rs in by_c.items(): add("媒体人", "国家", c, "全部", rs)

    # 全局对比
    add("全部", "总览", "全 KOL+编辑", "全部", enriched_kol + enriched_ed)

    # 容量治理：近 30 天保留完整维度；更早仅保留每周/月最后一次的总览行。
    # 先删历史冗余，再创建今日快照，最后删旧的今日快照；即使创建失败也不会丢掉今日旧快照。
    existing = await feishu.fetch_all_records(config.T_DASH)
    retention = plan_snapshot_retention(existing, now)
    await _delete_dashboard_records(retention["historical_delete_ids"])

    # 写新快照
    records = [{"fields": s} for s in snapshots]
    for i in range(0, len(records), 500):
        await feishu.api("POST",
            f"/bitable/v1/apps/{config.FEISHU_APP_TOKEN}/tables/{config.T_DASH}/records/batch_create",
            {"records": records[i:i+500]})
    await _delete_dashboard_records(retention["today_delete_ids"])

    return {"snapshots": len(snapshots), "kol_drafts": len(kol_drafts), "editor_drafts": len(editor_drafts),
            "tasks_stat_written": task_written,
            "retention": {
                "cutoff_date": retention["cutoff_date"],
                "historical_deleted": len(retention["historical_delete_ids"]),
                "today_replaced": len(retention["today_delete_ids"]),
                "invalid_date_records": retention["invalid_date_records"],
                "kept_daily_detail": retention["kept_daily_detail"],
                "kept_weekly_overview": retention["kept_weekly_overview"],
                "kept_monthly_overview": retention["kept_monthly_overview"],
            }}

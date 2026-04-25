"""数据看板聚合 (KOL + 编辑 双对象)"""
import re, time
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from . import config, feishu
from .feishu import ext, xrid

POSITIVE = {"感兴趣", "要报价"}
NEGATIVE = {"委婉拒绝", "退订"}


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


async def run():
    now = datetime.now(timezone(timedelta(hours=8))).replace(hour=0, minute=0, second=0, microsecond=0)
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
        brand = "FUNLAB" if "fireflyfunlab" in sender else ("POWKONG" if "powkong" in sender else "?")
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
        brand = "FUNLAB" if "fireflyfunlab" in sender else ("POWKONG" if "powkong" in sender else "?")
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
    for b in ("FUNLAB", "POWKONG"):
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
    for b in ("FUNLAB", "POWKONG"):
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

    # 删今日旧快照
    existing = await feishu.fetch_all_records(config.T_DASH)
    del_ids = [r["record_id"] for r in existing
               if isinstance(r["fields"].get("统计日期"), (int, float))
               and abs(r["fields"]["统计日期"] - today_ms) < 86400000]
    if del_ids:
        for i in range(0, len(del_ids), 500):
            await feishu.api("POST",
                f"/bitable/v1/apps/{config.FEISHU_APP_TOKEN}/tables/{config.T_DASH}/records/batch_delete",
                {"records": del_ids[i:i+500]})

    # 写新快照
    records = [{"fields": s} for s in snapshots]
    for i in range(0, len(records), 500):
        await feishu.api("POST",
            f"/bitable/v1/apps/{config.FEISHU_APP_TOKEN}/tables/{config.T_DASH}/records/batch_create",
            {"records": records[i:i+500]})

    return {"snapshots": len(snapshots), "kol_drafts": len(kol_drafts), "editor_drafts": len(editor_drafts)}

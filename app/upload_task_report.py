"""KOL 上稿 × 任务进度 周报 (2026-06-05) — 按产品审计任务完成情况.

每周一 → 飞书卡片 digest(运营群+Frankie 私聊) + 写一行/产品到 bitable「KOL上稿任务周报」
留档表(可点链接/排序/留快照, ROI 列预留, 收口后自动填)。

每产品全漏斗: 候选 → 过阈 → 发信(发信度) → 回复(回复率) → 寄样 → 上稿(上稿率) + 覆盖率 + 加权进度 + 卡点 + Top 上稿 KOL(链接)。

口径(Frankie 定):
- 发信度 = 已发信 / 通过阈值数 (任务执行彻底度, 显示封顶 100%)
- 覆盖率 = 已发信 unique KOL / 适配池 (适配池 = KOL 池中 内容风格∩任务筛选风格, 平台命中或无主战场)
- 上稿率 = 上稿数 / 寄样数 (寄样里产出多少)
- 进度 = 加权(派单10% + 发信25% + 回复15% + 寄样25% + 上稿25%, 各乘转化率封顶1)
- Top 上稿 KOL 排序 = 累计GMV desc → 粉丝数 desc; ROI(累计GMV/订单)已在 KOL 池现成, 收口后自动有数。

纯读 3 表 + 写留档表 + 发飞书卡, 不发邮件 → 无 DRY-RUN 顾虑。?dry_run=true 只算+发卡不写留档表。
"""
import time
from . import config, feishu
from .feishu import ext

T_REPORT = config.T_UPLOAD_REPORT


def _xids(v):
    """link 字段提取全部 record_ids (feishu.xrid 只返回首个; 这里要全部)."""
    out = []
    if isinstance(v, list):
        for u in v:
            if isinstance(u, dict):
                out += u.get("record_ids") or u.get("link_record_ids") or []
    elif isinstance(v, dict):
        out += v.get("record_ids") or v.get("link_record_ids") or []
    return out


def _i(v):
    try:
        return int(float(ext(v) or 0))
    except (ValueError, TypeError):
        return 0


def _f(v):
    try:
        return float(ext(v) or 0)
    except (ValueError, TypeError):
        return 0.0


def _mset(v):
    if isinstance(v, list):
        return set(ext(x) if isinstance(x, dict) else str(x) for x in v if x)
    if isinstance(v, str):
        return set(s.strip() for s in v.split(",") if s.strip())
    return set()


def _adapt_pool(kols: list, styles: set, platforms: set) -> int:
    """适配池: KOL 池中 内容风格∩任务筛选风格 ≠∅ (平台命中或该 KOL 无主战场)。"""
    if not styles:
        return 0
    n = 0
    for kf in kols:
        if _mset(kf.get("内容风格")) & styles:
            kplat = _mset(kf.get("主平台")) | _mset(kf.get("其他平台"))
            if not platforms or (kplat & platforms) or not kplat:
                n += 1
    return n


async def run(dry_run: bool = False, notify: bool = True, frankie_only: bool = False) -> dict:
    tasks = await feishu.fetch_all_records(config.T_TASK_KOL)
    drafts = await feishu.fetch_all_records(config.T_DRAFT)
    kol_recs = await feishu.fetch_all_records(config.T_KOL)
    kol_by_id = {r["record_id"]: r["fields"] for r in kol_recs}
    kol_fields = [r["fields"] for r in kol_recs]

    # draft: 产品rid -> set(kol_rid) 全部 / 已发信 / 已回复 (全去重独立 KOL)
    prod_kols, prod_sent_kols, prod_reply_kols = {}, {}, {}
    for d in drafts:
        f = d["fields"]
        pids = _xids(f.get("关联产品")); kids = _xids(f.get("关联KOL"))
        sent = ext(f.get("发送状态")) in ("已发送", "成功", "已发")
        replied = bool(f.get("是否回复"))   # 权威回复信号(同 completion_report)
        for p in pids:
            prod_kols.setdefault(p, set()).update(kids)
            if sent:
                prod_sent_kols.setdefault(p, set()).update(kids)
            if replied:
                prod_reply_kols.setdefault(p, set()).update(kids)

    # 任务按 目标产品 聚合
    from collections import defaultdict
    agg = defaultdict(lambda: {"cand": 0, "pass": 0, "sent": 0, "reply": 0, "interest": 0,
                               "styles": set(), "platforms": set(), "ops": set(), "tasks": 0,
                               "pname": "", "prid": None})
    for tk in tasks:
        f = tk["fields"]
        pids = _xids(f.get("目标产品"))
        prid = pids[0] if pids else None
        pname = ext(f.get("目标产品")) or ext(f.get("任务名"))
        if not prid and not pname:
            continue
        g = agg[prid or pname]
        g["prid"] = prid
        if not g["pname"]:
            g["pname"] = pname
        g["tasks"] += 1
        g["cand"] += _i(f.get("富化候选数")); g["pass"] += _i(f.get("通过阈值数"))
        g["sent"] += _i(f.get("已发送数")); g["reply"] += _i(f.get("回复数")); g["interest"] += _i(f.get("感兴趣数"))
        g["styles"] |= _mset(f.get("筛选-内容风格")); g["platforms"] |= _mset(f.get("筛选-平台"))
        if ext(f.get("负责运营")):
            g["ops"].add(ext(f.get("负责运营")))

    now = time.time() + 8 * 3600
    week = time.strftime("%Y-W%W", time.localtime(now))
    now_ms = int(time.time() * 1000)
    rows = []
    for key, g in agg.items():
        prid = g["prid"]
        if not g["pname"] or not prid:
            continue
        # ── 全去重独立 KOL 口径 ──
        kol_set = prod_kols.get(prid, set())
        sent_u = len(prod_sent_kols.get(prid, set()))            # 已发信 unique
        reply_u = len(kol_set & prod_reply_kols.get(prid, set()))  # 已回复 unique
        shipped = posted = cooperated = 0
        posters = []
        gmv_sum = orders_sum = 0.0
        for kid in kol_set:
            kf = kol_by_id.get(kid)
            if not kf:
                continue
            if ext(kf.get("上次寄样订单号")) or _i(kf.get("寄样次数")) >= 1:
                shipped += 1
            if ext(kf.get("合作状态")).startswith("已合作"):
                cooperated += 1
            if kf.get("上稿日期"):
                posted += 1
                posters.append(kf)
                gmv_sum += _f(kf.get("累计GMV")); orders_sum += _i(kf.get("累计订单数"))
        pool = _adapt_pool(kol_fields, g["styles"], g["platforms"])
        coverage = (sent_u / pool) if pool else 0.0              # 覆盖率 = 已发信unique / 适配池
        reply_rate = (reply_u / sent_u) if sent_u else 0.0       # 回复率 = 回复unique / 发信unique
        post_rate = (posted / shipped) if shipped else 0.0       # 上稿率 = 上稿 / 寄样
        exec_rate = min(g["sent"] / (g["pass"] or 1), 1.0)       # 派单执行率(累计已发/过阈, 留档参考)
        # 加权进度 = 纯转化深度(逐级转化, 不含覆盖率; 覆盖率单独看). Frankie 选 B 口径。
        c_reply = min(reply_u / sent_u, 1) if sent_u else 0       # 回复/发信
        c_ship = min(shipped / reply_u, 1) if reply_u else 0      # 寄样/回复
        c_post = min(posted / shipped, 1) if shipped else 0       # 上稿/寄样
        prog = (0.10 * (1 if sent_u else 0) + 0.25 * c_reply + 0.30 * c_ship + 0.35 * c_post) * 100
        nolink = sum(1 for kf in posters if not ext(kf.get("上稿链接")))
        flags = []
        if reply_rate < 0.03 and sent_u >= 20:
            flags.append("回复率<3%")
        if shipped > 0 and posted == 0:
            flags.append("寄样后断层")
        if pool and coverage < 0.1:
            flags.append(f"覆盖率<10%(欠派单,池{pool})")
        if nolink > 0:
            flags.append(f"{nolink}个上稿缺链接")
        # Top 上稿 KOL
        posters.sort(key=lambda kf: (_f(kf.get("累计GMV")), _f(kf.get("粉丝数"))), reverse=True)
        top_lines = []
        for kf in posters[:5]:
            fans = _f(kf.get("粉丝数")) / 10000
            link = ext(kf.get("上稿链接")) or "(无链接)"
            gmv = _f(kf.get("累计GMV")); od = _i(kf.get("累计订单数"))
            roi = f" ｜GMV${gmv:.0f}/单{od}" if (gmv or od) else " ｜GMV待ROI"
            top_lines.append(f"{ext(kf.get('账号名'))}({ext(kf.get('主平台'))},{fans:.0f}万){roi} {link}")
        rows.append({
            "pname": g["pname"], "tasks": g["tasks"], "cand": g["cand"], "pass": g["pass"],
            "sent_u": sent_u, "reply_u": reply_u, "shipped": shipped, "posted": posted,
            "cooperated": cooperated, "pool": pool, "exec_rate": exec_rate, "coverage": coverage,
            "reply_rate": reply_rate, "post_rate": post_rate, "prog": prog, "flags": flags,
            "top_lines": top_lines, "ops": "、".join(sorted(g["ops"])) or "-",
            "gmv": gmv_sum, "orders": int(orders_sum),
        })
    rows.sort(key=lambda r: -r["sent_u"])

    # 写留档表
    written = 0
    if not dry_run:
        for r in rows:
            try:
                await feishu.create_record(T_REPORT, {
                    "产品·周": f"{r['pname'][:28]} · {week}", "对象类型": "KOL", "统计周": week,
                    "任务数": r["tasks"], "候选数": r["cand"], "过阈数": r["pass"], "发信数": r["sent_u"],
                    "回复数": r["reply_u"], "寄样数": r["shipped"], "上稿数": r["posted"], "已合作数": r["cooperated"],
                    "适配池": r["pool"], "发信度%": round(r["exec_rate"] * 100), "覆盖率%": round(r["coverage"] * 100),
                    "回复率%": round(r["reply_rate"] * 100), "上稿率%": round(r["post_rate"] * 100),
                    "进度%": round(r["prog"]), "卡点": " / ".join(r["flags"]) or "—",
                    "Top上稿KOL+链接": "\n".join(r["top_lines"]) or "—",
                    "累计GMV": round(r["gmv"]), "累计订单数": r["orders"], "负责运营": r["ops"],
                    "生成时间": now_ms,
                })
                written += 1
            except Exception as e:
                print(f"[upload_task_report] 写留档失败 {r['pname']}: {e}")

    # 飞书卡片 digest
    card = _build_card(rows, week)
    sent_n = await _notify(card, frankie_only=frankie_only) if notify else 0
    return {"dry_run": dry_run, "products": len(rows), "written": written, "notified": sent_n,
            "rows": [{k: r[k] for k in ("pname", "sent_u", "reply_u", "shipped", "posted", "pool", "coverage", "prog", "flags")} for r in rows]}


def _build_card(rows: list, week: str) -> dict:
    els = []
    for r in rows:
        funnel = (f"适配池 **{r['pool']}** → 发信 **{r['sent_u']}**(覆盖{r['coverage']*100:.0f}%) "
                  f"→ 回复 **{r['reply_u']}**({r['reply_rate']*100:.0f}%) → 寄样 **{r['shipped']}** "
                  f"→ 上稿 **{r['posted']}**({r['post_rate']*100:.0f}%) → 已合作 **{r['cooperated']}**")
        metrics = f"进度 **{r['prog']:.0f}%** ｜ 运营 {r['ops']}"
        content = f"**📦 {r['pname'][:30]}**（{r['tasks']}批派单）\n🔻 {funnel}\n📊 {metrics}"
        if r["flags"]:
            content += f"\n⚠️ 卡点: {' / '.join(r['flags'])}"
        if r["top_lines"]:
            content += "\n🎬 Top上稿: " + " ／ ".join(t.split(" ")[0] + " " + t.split(" ")[-1] for t in r["top_lines"][:3])
        els.append({"tag": "div", "text": {"tag": "lark_md", "content": content}})
        els.append({"tag": "hr"})
    els.append({"tag": "div", "text": {"tag": "lark_md", "content":
        f"📋 完整明细+可点链接+ROI: [上稿任务周报表](https://u1wpma3xuhr.feishu.cn/base/{config.FEISHU_APP_TOKEN}?table={T_REPORT})\n"
        "_全为去重独立 KOL 口径 ｜ 覆盖率=发信/适配池(单独看) ｜ 进度=逐级转化深度(回复/发信×寄样/回复×上稿/寄样加权,不含覆盖) ｜ GMV待ROI收口_"}})
    return {
        "config": {"wide_screen_mode": True},
        "header": {"template": "green", "title": {"tag": "plain_text", "content": f"🟡 [KOL·P2] 上稿×任务进度周报 · {week}"}},
        "elements": els,
    }


async def _notify(card, frankie_only: bool = False) -> int:
    sent = 0
    if not frankie_only:
        try:
            await feishu.send_card_message("chat_id", config.NOTIFY_CHAT_ID, card)
            sent += 1
        except Exception as e:
            print(f"[upload_task_report] 群发送失败: {e}")
    for name, oid in config.NOTIFY_USERS:
        if "Frankie" not in name and "潘志聪" not in name:
            continue
        try:
            await feishu.send_card_message("open_id", oid, card)
            sent += 1
        except Exception as e:
            print(f"[upload_task_report] {name} 发送失败: {e}")
    return sent

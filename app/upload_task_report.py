"""上稿/报道 × 任务进度 周报 (2026-06-05) — 按产品审计任务完成情况. KOL + 媒体人两端.

每周一 → 飞书卡片 digest(运营群+Frankie 私聊) + 写一行/产品到 bitable 留档表「KOL上稿任务周报」
`tblHrlzTeSIhOjCY`(可点链接/排序/快照, 对象类型=KOL/媒体人, ROI 列预留收口后自动填)。

口径(Frankie 定, 全去重独立 KOL/媒体人):
- 漏斗 = 适配池 → 发信(unique) → 回复(draft「是否回复」unique) → 寄样 → 成功(KOL:上稿/媒体人:报道发表) → 已合作
- 覆盖率 = 发信unique / 适配池(池中 风格∩任务筛选, 平台/媒体类型命中或无)
- 成功率 = 成功 / 寄样
- 进度 = 纯转化深度(进入漏斗10% + 回复/发信25% + 寄样/回复30% + 成功/寄样35%, 不含覆盖, 覆盖单独列)
- 本周建议动作 + 在哪调 = 规则驱动处方(诊断→处方, 运营照单执行)
- KOL 有 GMV(收口后填); 媒体人=earned media 无 GMV, Top 按报道发表日期排。

纯读 + 写留档表 + 发飞书卡, 不发邮件 → 无 DRY-RUN 顾虑。?dry_run 不写表 / ?notify=false 不发卡 / ?frankie_only 只发 Frankie。
"""
import time
from . import config, feishu
from .feishu import ext

T_REPORT = config.T_UPLOAD_REPORT

# ── 两端 spec (KOL / 媒体人), 差异集中在此 ──
SPECS = [
    {"key": "KOL", "emoji": "🎮", "task": config.T_TASK_KOL, "pool": config.T_KOL,
     "link": "关联KOL", "name": "账号名", "obj": "KOL",
     "date": "上稿日期", "link_field": "上稿链接", "success": "上稿",
     "style_pool": "内容风格", "style_task": "筛选-内容风格",
     "plat_pool": ["主平台", "其他平台"], "plat_task": "筛选-平台",
     "media_disp": "主平台", "fans": "粉丝数", "has_gmv": True,
     "guide_vol": "产品库调高该品牌「品牌每日上限」(默认80,各主推产品平分)+保持「主推」；想集中给单品→暂时少勾同品牌其他主推",
     "guide_match": "品类映射规则表 tblA63dLsAYTwjT8 改「KOL内容风格」让派给更对的人(即时生效)；开发信模板要改→找技术/Claude",
     "guide_link": "收到上稿登记卡时粘链接，或去达人频道找补"},
    {"key": "媒体人", "emoji": "📰", "task": config.T_TASK_EDITOR, "pool": config.T_EDITOR,
     "link": "关联媒体人", "name": "媒体人姓名", "obj": "媒体人",
     "date": "报道发表日期", "link_field": "报道链接", "success": "报道",
     "style_pool": "报道品类", "style_task": "筛选-报道品类",
     "plat_pool": ["媒体类型"], "plat_task": "筛选-媒体类型",
     "media_disp": "所属媒体", "fans": None, "has_gmv": False,
     "guide_vol": "编辑任务「人数上限」(默认30)调高 + 产品库勾「派单-需要媒体人」保持派发",
     "guide_match": "品类映射规则表 tblA63dLsAYTwjT8 改「媒体人报道品类/媒体类型」(即时生效)；PR pitch 模板要改→找技术/Claude",
     "guide_link": "收到报道登记卡时粘链接，或去媒体站点找补"},
]
GUIDE_SHIP = "非发信问题：确认暖信(warm_recap)已发+寄样后brief质量，必要时人工催稿"


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


def _advise(spec, cov, rr, sent, ship, post, nolink):
    """据漏斗信号给(本周建议动作, 在哪调). 阈值 Frankie 定: 覆盖低<10%; 回复好≥20%/差<5%; 样本门槛."""
    succ = spec["success"]
    if ship > 0 and post == 0:
        return (f"📦 寄样后断层——查寄样后 brief/暖信(非发信问题)", GUIDE_SHIP)
    if cov < 0.10 and rr >= 0.20 and sent >= 10:
        return ("🔥 转化好但严重欠量——加大派单(量×2-3)", spec["guide_vol"])
    if rr < 0.05 and sent >= 20:
        return ("🔧 回复率过低——别加量，先改选品匹配/开发信模板", spec["guide_match"])
    if cov < 0.10 and sent >= 10:
        return ("📈 覆盖偏低，可适度加派单(先确认回复率够)", spec["guide_vol"])
    if nolink > 0:
        return (f"🔗 补 {nolink} 个{succ}缺链接；其余维持", spec["guide_link"])
    return ("✅ 转化健康——维持，加量复制", spec["guide_vol"] if rr >= 0.10 else spec["guide_match"])


def _adapt_pool(pool_fields, styles, platforms, spec):
    """适配池: 池中 风格字段∩任务筛选风格 ≠∅ (平台/媒体类型命中或该对象无)。"""
    if not styles:
        return 0
    n = 0
    for kf in pool_fields:
        if _mset(kf.get(spec["style_pool"])) & styles:
            kplat = set()
            for pf in spec["plat_pool"]:
                kplat |= _mset(kf.get(pf))
            if not platforms or (kplat & platforms) or not kplat:
                n += 1
    return n


async def _compute_spec(spec, drafts, week):
    """算单端(KOL 或 媒体人)的按产品 rows."""
    tasks = await feishu.fetch_all_records(spec["task"])
    pool_recs = await feishu.fetch_all_records(spec["pool"])
    by_id = {r["record_id"]: r["fields"] for r in pool_recs}
    pool_fields = [r["fields"] for r in pool_recs]

    prod_obj, prod_sent, prod_reply = {}, {}, {}
    for d in drafts:
        f = d["fields"]
        kids = _xids(f.get(spec["link"]))
        if not kids:
            continue
        pids = _xids(f.get("关联产品"))
        sent = ext(f.get("发送状态")) in ("已发送", "成功", "已发")
        replied = bool(f.get("是否回复"))
        for p in pids:
            prod_obj.setdefault(p, set()).update(kids)
            if sent:
                prod_sent.setdefault(p, set()).update(kids)
            if replied:
                prod_reply.setdefault(p, set()).update(kids)

    from collections import defaultdict
    agg = defaultdict(lambda: {"cand": 0, "pass": 0, "sent": 0, "reply": 0, "tasks": 0,
                               "styles": set(), "platforms": set(), "ops": set(), "pname": "", "prid": None})
    for tk in tasks:
        f = tk["fields"]
        pids = _xids(f.get("目标产品"))
        prid = pids[0] if pids else None
        pname = ext(f.get("目标产品")) or ext(f.get("任务名"))
        if not prid:
            continue
        g = agg[prid]
        g["prid"] = prid
        if not g["pname"]:
            g["pname"] = pname
        g["tasks"] += 1
        g["cand"] += _i(f.get("富化候选数")); g["pass"] += _i(f.get("通过阈值数"))
        g["sent"] += _i(f.get("已发送数")); g["reply"] += _i(f.get("回复数"))
        g["styles"] |= _mset(f.get(spec["style_task"])); g["platforms"] |= _mset(f.get(spec["plat_task"]))
        if ext(f.get("负责运营")):
            g["ops"].add(ext(f.get("负责运营")))

    rows = []
    for prid, g in agg.items():
        if not g["pname"]:
            continue
        obj_set = prod_obj.get(prid, set())
        sent_u = len(prod_sent.get(prid, set()))
        reply_u = len(obj_set & prod_reply.get(prid, set()))
        shipped = posted = cooperated = 0
        posters = []
        gmv_sum = orders_sum = 0.0
        for kid in obj_set:
            kf = by_id.get(kid)
            if not kf:
                continue
            if ext(kf.get("上次寄样订单号")) or _i(kf.get("寄样次数")) >= 1:
                shipped += 1
            if ext(kf.get("合作状态")).startswith("已合作"):
                cooperated += 1
            if kf.get(spec["date"]):
                posted += 1
                posters.append(kf)
                if spec["has_gmv"]:
                    gmv_sum += _f(kf.get("累计GMV")); orders_sum += _i(kf.get("累计订单数"))
        pool = _adapt_pool(pool_fields, g["styles"], g["platforms"], spec)
        coverage = (sent_u / pool) if pool else 0.0
        reply_rate = (reply_u / sent_u) if sent_u else 0.0
        post_rate = (posted / shipped) if shipped else 0.0
        exec_rate = min(g["sent"] / (g["pass"] or 1), 1.0)
        c_reply = min(reply_u / sent_u, 1) if sent_u else 0
        c_ship = min(shipped / reply_u, 1) if reply_u else 0
        c_post = min(posted / shipped, 1) if shipped else 0
        prog = (0.10 * (1 if sent_u else 0) + 0.25 * c_reply + 0.30 * c_ship + 0.35 * c_post) * 100
        nolink = sum(1 for kf in posters if not ext(kf.get(spec["link_field"])))
        flags = []
        if reply_rate < 0.03 and sent_u >= 20:
            flags.append("回复率<3%")
        if shipped > 0 and posted == 0:
            flags.append("寄样后断层")
        if pool and coverage < 0.1:
            flags.append(f"覆盖率<10%(欠派单,池{pool})")
        if nolink > 0:
            flags.append(f"{nolink}个{spec['success']}缺链接")
        advice, where = _advise(spec, coverage, reply_rate, sent_u, shipped, posted, nolink)
        # Top: KOL 按 GMV→粉丝; 媒体人 按 报道发表日期 desc
        if spec["has_gmv"]:
            posters.sort(key=lambda kf: (_f(kf.get("累计GMV")), _f(kf.get("粉丝数"))), reverse=True)
        else:
            posters.sort(key=lambda kf: _i(kf.get(spec["date"])), reverse=True)
        top_lines = []
        for kf in posters[:5]:
            link = ext(kf.get(spec["link_field"])) or "(无链接)"
            media = ext(kf.get(spec["media_disp"]))
            if spec["has_gmv"]:
                fans = _f(kf.get(spec["fans"])) / 10000
                gmv = _f(kf.get("累计GMV")); od = _i(kf.get("累计订单数"))
                roi = f" ｜GMV${gmv:.0f}/单{od}" if (gmv or od) else " ｜GMV待ROI"
                top_lines.append(f"{ext(kf.get(spec['name']))}({media},{fans:.0f}万){roi} {link}")
            else:
                top_lines.append(f"{ext(kf.get(spec['name']))}({media}) {link}")
        rows.append({
            "spec": spec["key"], "emoji": spec["emoji"], "success": spec["success"],
            "pname": g["pname"], "tasks": g["tasks"], "cand": g["cand"], "pass": g["pass"],
            "sent_u": sent_u, "reply_u": reply_u, "shipped": shipped, "posted": posted,
            "cooperated": cooperated, "pool": pool, "exec_rate": exec_rate, "coverage": coverage,
            "reply_rate": reply_rate, "post_rate": post_rate, "prog": prog, "flags": flags,
            "top_lines": top_lines, "ops": "、".join(sorted(g["ops"])) or "-",
            "gmv": gmv_sum, "orders": int(orders_sum), "advice": advice, "where": where,
        })
    rows.sort(key=lambda r: -r["sent_u"])
    return rows


async def run(dry_run: bool = False, notify: bool = True, frankie_only: bool = False) -> dict:
    now = time.time() + 8 * 3600
    week = time.strftime("%Y-W%W", time.localtime(now))
    now_ms = int(time.time() * 1000)
    drafts = await feishu.fetch_all_records(config.T_DRAFT)

    all_rows = []
    for spec in SPECS:
        all_rows += await _compute_spec(spec, drafts, week)

    written = 0
    if not dry_run:
        for r in all_rows:
            try:
                await feishu.create_record(T_REPORT, {
                    "产品·周": f"{r['pname'][:26]} · {week}", "对象类型": r["spec"], "统计周": week,
                    "任务数": r["tasks"], "候选数": r["cand"], "过阈数": r["pass"], "发信数": r["sent_u"],
                    "回复数": r["reply_u"], "寄样数": r["shipped"], "上稿数": r["posted"], "已合作数": r["cooperated"],
                    "适配池": r["pool"], "发信度%": round(r["exec_rate"] * 100), "覆盖率%": round(r["coverage"] * 100),
                    "回复率%": round(r["reply_rate"] * 100), "上稿率%": round(r["post_rate"] * 100),
                    "进度%": round(r["prog"]), "卡点": " / ".join(r["flags"]) or "—",
                    "本周建议动作": r["advice"], "在哪调·操作指引": r["where"],
                    "Top上稿KOL+链接": "\n".join(r["top_lines"]) or "—",
                    "累计GMV": round(r["gmv"]), "累计订单数": r["orders"], "负责运营": r["ops"],
                    "生成时间": now_ms,
                })
                written += 1
            except Exception as e:
                print(f"[upload_task_report] 写留档失败 {r['pname']}: {e}")

    card = _build_card(all_rows, week)
    sent_n = await _notify(card, frankie_only=frankie_only) if notify else 0
    return {"dry_run": dry_run, "products": len(all_rows), "written": written, "notified": sent_n,
            "rows": [{k: r[k] for k in ("spec", "pname", "sent_u", "reply_u", "shipped", "posted",
                                        "pool", "coverage", "prog", "advice")} for r in all_rows]}


def _build_card(rows: list, week: str) -> dict:
    els = []
    last_spec = None
    for r in rows:
        if r["spec"] != last_spec:
            els.append({"tag": "div", "text": {"tag": "lark_md", "content": f"**{r['emoji']} {r['spec']}端**"}})
            last_spec = r["spec"]
        funnel = (f"适配池 **{r['pool']}** → 发信 **{r['sent_u']}**(覆盖{r['coverage']*100:.0f}%) "
                  f"→ 回复 **{r['reply_u']}**({r['reply_rate']*100:.0f}%) → 寄样 **{r['shipped']}** "
                  f"→ {r['success']} **{r['posted']}**({r['post_rate']*100:.0f}%) → 已合作 **{r['cooperated']}**")
        content = (f"**📦 {r['pname'][:28]}**（{r['tasks']}批派单）\n🔻 {funnel}\n"
                   f"📊 进度 **{r['prog']:.0f}%** ｜ 运营 {r['ops']}")
        if r["flags"]:
            content += f"\n⚠️ 卡点: {' / '.join(r['flags'])}"
        content += f"\n🎯 **本周建议**: {r['advice']}\n　📍 在哪调: {r['where']}"
        if r["top_lines"]:
            content += f"\n🎬 Top{r['success']}: " + " ／ ".join(t.split(" ")[0] + " " + t.split(" ")[-1] for t in r["top_lines"][:3])
        els.append({"tag": "div", "text": {"tag": "lark_md", "content": content}})
        els.append({"tag": "hr"})
    els.append({"tag": "div", "text": {"tag": "lark_md", "content":
        f"📋 完整明细+可点链接+ROI: [上稿任务周报表](https://u1wpma3xuhr.feishu.cn/base/{config.FEISHU_APP_TOKEN}?table={T_REPORT})\n"
        "_全为去重独立口径 ｜ 覆盖率=发信/适配池(单独看) ｜ 进度=逐级转化深度(不含覆盖) ｜ 成功:KOL上稿/媒体人报道发表 ｜ GMV待ROI收口_"}})
    return {
        "config": {"wide_screen_mode": True},
        "header": {"template": "green", "title": {"tag": "plain_text", "content": f"🟡 [KOL·P2] 上稿/报道×任务进度周报 · {week}"}},
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

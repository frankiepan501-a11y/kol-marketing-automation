"""KOL + 媒体人 任务完成情况周报 (2026-06-01) — Frankie 问"没有完成情况审计/什么算结束"的答案.

每周一 → 飞书运营群 + Frankie 私聊。一张卡看 KOL + 媒体人 两端全局: 漏斗转化 + 终态分布 + 卡点清单。
终态阈值 Frankie 定: 无回应=末次发信 +N天(KOL 14 ≈首触D+28 / 媒体人 7, 编辑跟进≤1次判更快) / 寄样未产出=签收 +60d。

终态 5 类(KOL/媒体人共用骨架, 成功口径不同 — 见 [[reference-media-relations-playbook]]):
  ✅ 成功结束   = 成功字段非空 (KOL:上稿日期 / 媒体人:报道发表日期) 或 合作状态=已合作*
  ❌ 关闭-拒绝  = 合作状态∈{不合适,黑名单} 或 邮箱验真状态=无效
  ⏳ 关闭-无回应 = 合作状态∈awaiting(KOL:待回复 / 媒体人:建联中) + 末次发信 +N天
  🟡 关闭-寄样未产出 = 已寄样 + 成功字段空 + 签收+60d
  🔄 进行中     = 其余已建联
未发过信(未建联) 不计入任务统计。纯读 + 发卡, 不发邮件 / 不写主表。
"""
import time
from . import config, feishu
from .feishu import ext, xrid

D60 = 60 * 86400 * 1000

# KOL=带货上稿(affiliate) / 媒体人=earned media 报道. 成功口径+无回应阈值不同(方法论 reference-media-relations-playbook)
SPECS = [
    {"label": "KOL", "emoji": "🎮", "table": config.T_KOL, "link": "关联KOL", "name": "账号名",
     "date": "上稿日期", "awaiting": ("待回复", ""), "noreply_days": 14,
     "success": "上稿", "extra_funnel": ()},
    {"label": "媒体人", "emoji": "📰", "table": config.T_EDITOR, "link": "关联媒体人", "name": "媒体人姓名",
     "date": "报道发表日期", "awaiting": ("建联中", ""), "noreply_days": 7,
     "success": "报道", "extra_funnel": ("样品评估",)},
]


def _int(v):
    try:
        return int(v or 0)
    except (ValueError, TypeError):
        return 0


def _classify(cf: dict, last_send_ms: int, now_ms: int, spec: dict) -> str:
    coop = ext(cf.get("合作状态")) or ""
    verify = ext(cf.get("邮箱验真状态")) or ""
    posted = bool(cf.get(spec["date"]))
    shipped = bool(ext(cf.get("上次寄样订单号"))) or _int(cf.get("寄样次数")) >= 1
    if posted or coop.startswith("已合作"):
        return "成功"
    if coop in ("不合适", "黑名单") or verify == "无效":
        return "拒绝"
    if shipped and cf.get("上次寄样日期"):
        try:
            if now_ms - int(cf.get("上次寄样日期")) > D60:
                return "寄样未产出"
        except (ValueError, TypeError):
            pass
    if coop in spec["awaiting"] and last_send_ms and (now_ms - last_send_ms > spec["noreply_days"] * 86400 * 1000):
        return "无回应"
    if coop in ("待回复", "建联中", "洽谈中", "样品评估") or shipped:
        return "进行中"
    return "未建联"


async def _compute(spec: dict, drafts: list, now_ms: int) -> dict:
    last_send, replied = {}, set()
    for d in drafts:
        f = d["fields"]
        crid = xrid(f.get(spec["link"]))
        if not crid:
            continue
        if ext(f.get("发送状态")) in ("已发", "已发送"):
            ts = _int(f.get("发送时间"))
            if ts > last_send.get(crid, 0):
                last_send[crid] = ts
        if f.get("是否回复"):
            replied.add(crid)
    rows = await feishu.fetch_all_records(spec["table"])
    states = {k: [] for k in ("成功", "拒绝", "无回应", "寄样未产出", "进行中", "未建联")}
    funnel = {"engaged": 0, "replied": 0, "洽谈": 0, "已寄样": 0, "已发布": 0, "已合作": 0}
    for r in rows:
        crid, cf = r["record_id"], r["fields"]
        ls = last_send.get(crid, 0)
        st = _classify(cf, ls, now_ms, spec)
        states[st].append((ext(cf.get(spec["name"])) or "?", cf, ls))
        if ls <= 0 and st == "未建联":
            continue
        funnel["engaged"] += 1
        if crid in replied:
            funnel["replied"] += 1
        coop = ext(cf.get("合作状态")) or ""
        if coop == "洽谈中" or coop in spec["extra_funnel"]:
            funnel["洽谈"] += 1
        if ext(cf.get("上次寄样订单号")) or _int(cf.get("寄样次数")) >= 1:
            funnel["已寄样"] += 1
        if cf.get(spec["date"]):
            funnel["已发布"] += 1
        if coop.startswith("已合作"):
            funnel["已合作"] += 1
    stuck_ship, pending_noreply = [], []
    for nm, cf, ls in states["寄样未产出"]:
        sd = cf.get("上次寄样日期")
        stuck_ship.append(f"{nm}({int((now_ms-int(sd))/86400000)}d)" if sd else nm)
    for nm, cf, ls in states["无回应"]:
        pending_noreply.append(f"{nm}({int((now_ms-ls)/86400000)}d)" if ls else nm)
    eng = max(funnel["engaged"], 1)
    return {
        "funnel": funnel, "terminal": {k: len(v) for k, v in states.items()},
        "reply_rate": funnel["replied"] / eng * 100, "post_rate": funnel["已发布"] / eng * 100,
        "stuck_ship": stuck_ship, "pending_noreply": pending_noreply,
    }


def _section(spec: dict, c: dict) -> list:
    fn = c["funnel"]
    n = c["terminal"]
    cap_word = "洽谈/样评" if spec["label"] == "媒体人" else "洽谈"
    funnel_line = (f"建联 **{fn['engaged']}** → 回复 **{fn['replied']}** ({c['reply_rate']:.0f}%) "
                   f"→ {cap_word} **{fn['洽谈']}** → 已寄样 **{fn['已寄样']}** "
                   f"→ {spec['success']} **{fn['已发布']}** ({c['post_rate']:.0f}%) → 已合作 **{fn['已合作']}**")
    terminal_line = (f"✅ 成功 **{n['成功']}**　❌ 拒绝 **{n['拒绝']}**　⏳ 无回应 **{n['无回应']}**　"
                     f"🟡 寄样未产出 **{n['寄样未产出']}**　🔄 进行中 **{n['进行中']}**")
    ss = c["stuck_ship"]; pn = c["pending_noreply"]
    s1 = "、".join(ss[:10]) + (f" …共{len(ss)}" if len(ss) > 10 else "") if ss else "无"
    s2 = "、".join(pn[:10]) + (f" …共{len(pn)}" if len(pn) > 10 else "") if pn else "无"
    return [
        {"tag": "div", "text": {"tag": "lark_md", "content": f"**{spec['emoji']} {spec['label']}端**\n🔻 {funnel_line}\n🏁 {terminal_line}"}},
        {"tag": "div", "text": {"tag": "lark_md", "content": f"　🟡 寄样>60d未{spec['success']}: {s1}\n　⏳ 已判无回应(末次发信+{spec['noreply_days']}d): {s2}"}},
    ]


async def run(dry_run: bool = False) -> dict:
    now_ms = int(time.time() * 1000)
    drafts = await feishu.fetch_all_records(config.T_DRAFT)
    computed = {}
    elements = []
    for spec in SPECS:
        c = await _compute(spec, drafts, now_ms)
        computed[spec["label"]] = {
            "funnel": c["funnel"], "terminal": c["terminal"],
            "reply_rate": round(c["reply_rate"], 1), "post_rate": round(c["post_rate"], 1),
            "stuck_ship": len(c["stuck_ship"]), "pending_noreply": len(c["pending_noreply"]),
        }
        elements += _section(spec, c)
        elements.append({"tag": "hr"})
    today = time.strftime("%Y-%m-%d", time.localtime(time.time() + 8 * 3600))
    elements.append({"tag": "div", "text": {"tag": "lark_md", "content":
        "_口径: 成功=KOL上稿/媒体人报道发表; 无回应=末次发信+14d(KOL)/+7d(媒体人); 寄样未产出=签收+60d。仅统计已发信。媒体人=earned media 无 GMV/折扣码归因。_"}})
    card = {
        "config": {"wide_screen_mode": True},
        "header": {"template": "blue",
                   "title": {"tag": "plain_text", "content": f"🟡 [KOL·P2] KOL+媒体人 任务完成情况周报 · {today}"}},
        "elements": elements,
    }
    sent = 0 if dry_run else await _notify(card)
    return {"dry_run": dry_run, "report": computed, "notified": sent}


async def _notify(card) -> int:
    """Frankie 定: 运营群 + Frankie 私聊。运营在群里看(不重复私聊), 私聊只给 Frankie。"""
    sent = 0
    try:
        await feishu.send_card_message("chat_id", config.NOTIFY_CHAT_ID, card)
        sent += 1
    except Exception as e:
        print(f"[completion_report] 群发送失败: {e}")
    for name, oid in config.NOTIFY_USERS:
        if "Frankie" not in name and "潘志聪" not in name:
            continue
        try:
            await feishu.send_card_message("open_id", oid, card)
            sent += 1
        except Exception as e:
            print(f"[completion_report] {name} 发送失败: {e}")
    return sent

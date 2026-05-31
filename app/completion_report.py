"""KOL 任务完成情况周报 (2026-06-01) — Frankie 问"没有完成情况审计/什么算结束"的答案.

每周一 → 飞书运营群 + Frankie 私聊。一张卡看全局: 漏斗转化 + 终态分布 + 卡点清单,
替代逐个翻 KOL。终态阈值 Frankie 定 (2026-06-01): 无回应=末次发信 +14d(≈首触 D+28) / 寄样未产出=签收 +60d。

KOL 个体终态 5 类:
  ✅ 成功结束   = 上稿日期非空
  ❌ 关闭-拒绝  = 合作状态∈{不合适,黑名单} 或 邮箱验真状态=无效
  ⏳ 关闭-无回应 = 合作状态=待回复 + 末次发信 +14d 仍无回复
  🟡 关闭-寄样未产出 = 已寄样 + 上稿日期空 + 签收(上次寄样日期)+60d
  🔄 进行中     = 其余已建联 (洽谈中/已寄样在等/刚发信在等)
未发过信(未建联) 不计入任务统计。

纯读 + 发卡, 不发邮件 / 不写主表 → 无 DRY-RUN 顾虑。
"""
import time
from . import config, feishu
from .feishu import ext, xrid

D14 = 14 * 86400 * 1000
D60 = 60 * 86400 * 1000


def _int(v):
    try:
        return int(v or 0)
    except (ValueError, TypeError):
        return 0


def _classify(cf: dict, last_send_ms: int, now_ms: int) -> str:
    coop = ext(cf.get("合作状态")) or ""
    verify = ext(cf.get("邮箱验真状态")) or ""
    posted = bool(cf.get("上稿日期"))
    shipped = bool(ext(cf.get("上次寄样订单号"))) or _int(cf.get("寄样次数")) >= 1
    ship_date = cf.get("上次寄样日期")
    if posted:
        return "成功"
    if coop in ("不合适", "黑名单") or verify == "无效":
        return "拒绝"
    if shipped and ship_date:
        try:
            if now_ms - int(ship_date) > D60:
                return "寄样未产出"
        except (ValueError, TypeError):
            pass
    if coop in ("待回复", "") and last_send_ms and (now_ms - last_send_ms > D14):
        return "无回应"
    if coop in ("待回复", "洽谈中") or shipped:
        return "进行中"
    if coop.startswith("已合作"):
        return "成功"   # 已合作但上稿日期空(数据缺) — 视为成功(有合作)
    return "未建联"


async def run(dry_run: bool = False) -> dict:
    now_ms = int(time.time() * 1000)

    # 1. 拉所有已发送草稿 → 每 KOL 的 末次发信时间 + 是否回复过
    drafts = await feishu.fetch_all_records(config.T_DRAFT)
    last_send = {}    # kol_rid → max 发送时间
    replied = set()   # kol_rid 有任一草稿 是否回复=true
    for d in drafts:
        f = d["fields"]
        krid = xrid(f.get("关联KOL"))
        if not krid:
            continue
        if ext(f.get("发送状态")) in ("已发", "已发送"):
            ts = f.get("发送时间")
            try:
                ts = int(ts) if ts else 0
            except (ValueError, TypeError):
                ts = 0
            if ts > last_send.get(krid, 0):
                last_send[krid] = ts
        if f.get("是否回复"):
            replied.add(krid)

    # 2. 拉所有 KOL → 分类
    kols = await feishu.fetch_all_records(config.T_KOL)
    states = {"成功": [], "拒绝": [], "无回应": [], "寄样未产出": [], "进行中": [], "未建联": []}
    funnel = {"engaged": 0, "replied": 0, "洽谈": 0, "已寄样": 0, "已上稿": 0, "已合作": 0}
    for k in kols:
        krid = k["record_id"]
        cf = k["fields"]
        ls = last_send.get(krid, 0)
        engaged = ls > 0  # 发过至少 1 封 = 建联
        st = _classify(cf, ls, now_ms)
        states[st].append((ext(cf.get("账号名")) or "?", cf, ls))
        if not engaged and st == "未建联":
            continue  # 真未建联(没发过信)不计漏斗
        funnel["engaged"] += 1
        if krid in replied:
            funnel["replied"] += 1
        coop = ext(cf.get("合作状态")) or ""
        if coop == "洽谈中":
            funnel["洽谈"] += 1
        if ext(cf.get("上次寄样订单号")) or _int(cf.get("寄样次数")) >= 1:
            funnel["已寄样"] += 1
        if cf.get("上稿日期"):
            funnel["已上稿"] += 1
        if coop.startswith("已合作"):
            funnel["已合作"] += 1

    # 3. 卡点清单
    stuck_ship = []      # 寄样未产出 (>60d)
    for nm, cf, ls in states["寄样未产出"]:
        sd = cf.get("上次寄样日期")
        days = int((now_ms - int(sd)) / 86400000) if sd else 0
        stuck_ship.append(f"{nm}({days}d)")
    pending_noreply = []  # 即将/已判无回应
    for nm, cf, ls in states["无回应"]:
        days = int((now_ms - ls) / 86400000) if ls else 0
        pending_noreply.append(f"{nm}({days}d无回复)")

    eng = max(funnel["engaged"], 1)
    reply_rate = funnel["replied"] / eng * 100
    post_rate = funnel["已上稿"] / eng * 100

    card = _build_card(funnel, states, stuck_ship, pending_noreply, reply_rate, post_rate)
    sent = 0 if dry_run else await _notify(card)
    return {
        "dry_run": dry_run,
        "funnel": funnel,
        "terminal": {k: len(v) for k, v in states.items()},
        "reply_rate": round(reply_rate, 1), "post_rate": round(post_rate, 1),
        "stuck_ship": len(stuck_ship), "pending_noreply": len(pending_noreply),
        "notified": sent,
    }


def _build_card(funnel, states, stuck_ship, pending_noreply, reply_rate, post_rate) -> dict:
    today = time.strftime("%Y-%m-%d", time.localtime(time.time() + 8 * 3600))  # BJ
    n = {k: len(v) for k, v in states.items()}
    funnel_line = (f"建联(已发信) **{funnel['engaged']}** → 回复 **{funnel['replied']}** ({reply_rate:.0f}%) "
                   f"→ 洽谈 **{funnel['洽谈']}** → 已寄样 **{funnel['已寄样']}** "
                   f"→ 上稿 **{funnel['已上稿']}** ({post_rate:.0f}%) → 已合作 **{funnel['已合作']}**")
    terminal_line = (f"✅ 成功 **{n['成功']}**　❌ 拒绝 **{n['拒绝']}**　⏳ 无回应 **{n['无回应']}**　"
                     f"🟡 寄样未产出 **{n['寄样未产出']}**　🔄 进行中 **{n['进行中']}**")
    stuck1 = "、".join(stuck_ship[:12]) + (f" …共{len(stuck_ship)}" if len(stuck_ship) > 12 else "") if stuck_ship else "无"
    stuck2 = "、".join(pending_noreply[:12]) + (f" …共{len(pending_noreply)}" if len(pending_noreply) > 12 else "") if pending_noreply else "无"
    elements = [
        {"tag": "div", "text": {"tag": "lark_md", "content": f"**🔻 漏斗转化**\n{funnel_line}"}},
        {"tag": "div", "text": {"tag": "lark_md", "content": f"**🏁 终态分布**\n{terminal_line}"}},
        {"tag": "hr"},
        {"tag": "div", "text": {"tag": "lark_md", "content": f"**🟡 寄样>60天未上稿 (该跟进/判结束)**\n{stuck1}"}},
        {"tag": "div", "text": {"tag": "lark_md", "content": f"**⏳ 已判无回应 (末次发信+14d 无回复)**\n{stuck2}"}},
        {"tag": "hr"},
        {"tag": "div", "text": {"tag": "lark_md", "content": "_口径: 成功=已上稿 / 无回应=末次发信+14d(≈首触 D+28) / 寄样未产出=签收+60d。仅统计已发信 KOL。_"}},
    ]
    return {
        "config": {"wide_screen_mode": True},
        "header": {"template": "blue",
                   "title": {"tag": "plain_text", "content": f"🟡 [KOL·P2] KOL 任务完成情况周报 · {today}"}},
        "elements": elements,
    }


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
            continue  # 私聊只发 Frankie
        try:
            await feishu.send_card_message("open_id", oid, card)
            sent += 1
        except Exception as e:
            print(f"[completion_report] {name} 发送失败: {e}")
    return sent

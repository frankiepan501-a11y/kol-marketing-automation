# -*- coding: utf-8 -*-
"""洽谈中 stall 自动检测 (2026-06-18).

痛点(阿烨人肉盘点 11 个建联中断, 里面 7 个其实不用动=白干): KOL 一旦回复进入「洽谈中」,
cold followup 就停了, 系统**没有针对"洽谈中后冷下来"的自动跟进**。
本模块: 自动找出「合作状态=洽谈中 + 有历史草稿 + 无待发/待审草稿 + 距最后发信 ≥ STALL_DAYS」的温线索,
发数字卡给运营(独立站运营专员)+Frankie 提醒。**检测自动化, 跟进仍人工**(温线索个性化, 不自动发催稿信)。

不算停滞(自动排除, 防白盘):
  - 有待审/待修改/已通过待发 草稿 → 系统/运营即将处理(如回信卡在限速队列)。
  - 距最后发信 < STALL_DAYS (最近刚发过/刚维护过, 在等对方)。
  - 合作状态 不合适/黑名单 (enrich 已硬排除)。
weekly cron。
"""
import time
from . import config, feishu, draft_router
from .feishu import ext, xrid

STALL_DAYS = int(config.env("NEGOTIATION_STALL_DAYS", "10"))
MAX_CARD = 25   # 卡片最多列这么多, 超出只提示总数

_PENDING_STATUS = {"待审", "待修改"}


def _is_pending(d: dict) -> bool:
    """该草稿=有待办action(系统/运营即将处理) → 该 KOL 不算停滞。"""
    f = d["fields"]
    st = ext(f.get("邮件草稿状态")) or ""
    ss = ext(f.get("发送状态")) or ""
    if st in _PENDING_STATUS:
        return True
    if st in ("通过", "自动通过") and ss in ("", "未发"):
        return True   # 已审过排队待发(如回信卡限速队列)
    return False


def detect_stalls(kols: list, drafts_by_kol: dict, now_ms: int) -> list:
    """纯函数(可单测): 返回停滞洽谈中 KOL 列表(按静默天数降序由调用方排)。
    停滞 = 有历史草稿 + 无待办草稿 + 距最后发信 ≥ STALL_DAYS; 或 洽谈中但 0 草稿(数据异常)。"""
    cutoff = now_ms - STALL_DAYS * 86400000
    out = []
    for k in kols:
        rid = k["record_id"]
        ds = drafts_by_kol.get(rid, [])
        if not ds:
            out.append({"rid": rid, "kf": k["fields"], "days": -1,
                        "note": "洽谈中但无草稿(数据对不上, 先核对)", "prid": None})
            continue
        if any(_is_pending(d) for d in ds):
            continue
        last_ms = 0
        last_replied = False
        last_prid = None
        for d in ds:
            f = d["fields"]
            if ext(f.get("发送状态")) != "已发":
                continue
            try:
                t = int(f.get("发送时间") or 0)
            except (ValueError, TypeError):
                t = 0
            if t > last_ms:
                last_ms = t
                last_replied = bool(f.get("是否回复"))
                last_prid = xrid(f.get("关联产品"))
        if not last_ms or last_ms > cutoff:
            continue   # 最近发过, 在等对方, 不算停滞
        days = int((now_ms - last_ms) / 86400000)
        note = "我们欠回复(对方已回未接)" if last_replied else "对方欠回复(我们已发, 对方静默)"
        out.append({"rid": rid, "kf": k["fields"], "days": days, "note": note, "prid": last_prid})
    return out


async def run(dry_run: bool = False) -> dict:
    now_ms = int(time.time() * 1000)
    kols = await feishu.search_records(config.T_KOL, [
        {"field_name": "合作状态", "operator": "is", "value": ["洽谈中"]},
    ], field_names=["账号名", "邮箱", "合作状态", "主平台", "粉丝数"])
    # 性能: 用 search(page_size 500 + field_names) 而非 fetch_all_records(全字段 100/页, 慢到 HTTP 超时)
    all_drafts = await feishu.search_records(config.T_DRAFT, [], field_names=[
        "关联KOL", "邮件草稿状态", "发送状态", "发送时间", "是否回复", "关联产品"])
    by_kol = {}
    for d in all_drafts:
        kid = xrid(d["fields"].get("关联KOL"))
        if kid:
            by_kol.setdefault(kid, []).append(d)

    stalls = detect_stalls(kols, by_kol, now_ms)
    stalls.sort(key=lambda s: -(s["days"] if s["days"] is not None else 0))

    # 产品名解析 (缓存)
    pcache = {}
    for s in stalls:
        prid = s.get("prid")
        if prid and prid not in pcache:
            try:
                pf = (await feishu.get_record(config.T_PRODUCT, prid))["fields"]
                pcache[prid] = ext(pf.get("产品名")) or ext(pf.get("产品英文名")) or ""
            except Exception:
                pcache[prid] = ""

    result = {"ok": True, "stalled": len(stalls), "stall_days": STALL_DAYS,
              "details": [{"name": ext(s["kf"].get("账号名")), "email": ext(s["kf"].get("邮箱")),
                           "days": s["days"], "note": s["note"]} for s in stalls[:MAX_CARD]]}
    if dry_run:
        return result
    if not stalls:
        return {"ok": True, "stalled": 0, "msg": f"无停滞洽谈中 KOL (阈值 {STALL_DAYS} 天)"}

    # 卡片
    rows = []
    for s in stalls[:MAX_CARD]:
        kf = s["kf"]
        name = ext(kf.get("账号名")) or "?"
        email = ext(kf.get("邮箱")) or "?"
        prod = pcache.get(s.get("prid"), "") or "?"
        plat = ext(kf.get("主平台")) or ""
        try:
            fans = f"{int(kf.get('粉丝数') or 0):,}"
        except (ValueError, TypeError):
            fans = ""
        dtxt = "无草稿" if s["days"] == -1 else f"静默 {s['days']} 天"
        rows.append(f"**{name}** · {email}\n　{prod} · {dtxt} · {s['note']}" +
                    (f" · {plat} {fans}粉" if plat else ""))
    more = f"\n\n…另有 {len(stalls) - MAX_CARD} 个未列出" if len(stalls) > MAX_CARD else ""
    base_url = f"https://u1wpma3xuhr.feishu.cn/base/{config.FEISHU_APP_TOKEN}?table={config.T_KOL}"
    card = {
        "config": {"wide_screen_mode": True},
        "header": {"template": "orange",
                   "title": {"tag": "plain_text", "content": f"💬 洽谈中线索停滞 — {len(stalls)} 个待人工跟进 (静默 ≥ {STALL_DAYS} 天)"}},
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md", "content":
                "这些洽谈中的 KOL 回复后冷下来了、系统无待发草稿。**请人工判断要不要个性化跟进**(温线索, 别群发模板)。\n"
                "_已自动排除: 有待发/待审回信的(系统会处理)、最近刚发过的、已标不合适的。_"}},
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md", "content": "\n\n".join(rows) + more}},
            {"tag": "action", "actions": [
                {"tag": "button", "text": {"tag": "plain_text", "content": "打开 KOL 主表"},
                 "url": base_url, "type": "primary"}]},
        ],
    }
    try:
        main, _cc = await draft_router._ship_confirm_targets()
    except Exception:
        main = []
    sent_to = []
    targets = list(main) + [("Frankie", "ou_629ce01f4bc31de078e10fcb038dbf78")]
    seen = set()
    for name, oid in targets:
        if not oid or oid in seen:
            continue
        seen.add(oid)
        try:
            await feishu.send_card_message("open_id", oid, card)
            sent_to.append(name)
        except Exception as e:
            print(f"[negotiation_stall] send fail {name}: {e}")
    result["sent_to"] = sent_to
    return result

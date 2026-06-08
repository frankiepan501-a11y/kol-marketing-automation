"""手动发送补登记 (2026-06-01) — 把系统外手动发的 KOL/媒体人邮件登记进系统.

背景(Scott Stein 事故根因): 手动发的 pitch 没有草稿记录 → reply_monitor.find_draft 找不到草稿 →
`if not draft: continue` 直接跳过整封回复 → 对方积极回复(Scott: "would love to check this out"+给地址)
被系统完全丢弃。本模块周期性扫 Zoho 发件箱, 对"我们真发过但系统无草稿记录"的池内联系人补建一条
「已发送」草稿(来源=cold, 邮件草稿ID manual- 前缀) → ① 跟进进度可见 ② reply_monitor 能处理其回复
③ 完成漏斗把手动发的也算进 engaged。

幂等: 联系人已有任一「已发」草稿 → 视为已追踪, 跳过(补建一条即够)。退信地址(验真=无效)不补。
followup 只对有「Follow-up轮次」的草稿生成跟进 → manual 草稿不会触发二次发信。纯读 Zoho + 写 bitable, 不发邮件。
"""
import re, time
from . import config, feishu, zoho
from .feishu import ext, xrid

SENT_SCAN_LIMIT = 200   # 每品牌扫最近 N 封发件箱


def _emails(s):
    return [e.lower() for e in re.findall(r'[\w.+-]+@[\w-]+\.[\w.-]+', s or "")]


async def run(dry_run: bool = False) -> dict:
    now_ms = int(time.time() * 1000)

    # 1. 池子 email→{rid,type,coop,verify}
    pool = {}
    for tbl, ctype, name_f in ((config.T_KOL, "KOL", "账号名"), (config.T_EDITOR, "媒体人", "媒体人姓名")):
        for r in await feishu.fetch_all_records(tbl):
            f = r["fields"]
            em = ext(f.get("邮箱")).strip().lower()
            if em and "@" in em:
                pool.setdefault(em, {"rid": r["record_id"], "ctype": ctype, "name": ext(f.get(name_f)),
                                     "coop": ext(f.get("合作状态")) or "未建联",
                                     "verify": ext(f.get("邮箱验真状态")) or ""})

    # 2. 已追踪集合: 有任一「已发」草稿的联系人 rid
    tracked = set()
    for d in await feishu.fetch_all_records(config.T_DRAFT):
        f = d["fields"]
        if ext(f.get("发送状态")) in ("已发", "已发送"):
            rid = xrid(f.get("关联KOL")) or xrid(f.get("关联媒体人"))
            if rid:
                tracked.add(rid)

    # 3. 扫两品牌发件箱, 找"发过但无草稿"的池内联系人
    found = {}   # email→{first sent msg}
    for brand in config.BRAND_CONFIG:   # 2026-06-08 配置驱动: 含白牌发件箱补登记
        try:
            sent = await zoho.list_sent_messages(brand, limit=SENT_SCAN_LIMIT)
            msgs = sent.get("messages") or [] if isinstance(sent, dict) else []
        except Exception as e:
            print(f"[manual_send_recon] {brand} list_sent fail: {e}")
            continue
        for m in msgs:
            ts = 0
            try:
                ts = int(m.get("sentDateInGMT") or m.get("receivedTime") or 0)
            except (ValueError, TypeError):
                ts = 0
            subj = m.get("subject", "")
            for e in _emails(m.get("toAddress", "")):
                if e in pool and e not in found:
                    found[e] = {"brand": brand, "ts": ts, "subject": subj}

    # 4. 候选 = 池内 + 发过 + 未追踪(无草稿) + 验真≠无效
    cands = []
    for e, msg in found.items():
        p = pool[e]
        if p["rid"] in tracked:
            continue   # 已有草稿, 系统已追踪
        if p["verify"] == "无效":
            continue   # 退信地址不补
        cands.append((e, p, msg))

    if dry_run:
        return {"dry_run": True, "candidates": len(cands),
                "would_register": [{"name": p["name"], "email": e, "type": p["ctype"],
                                    "coop": p["coop"], "subject": msg["subject"][:50]} for e, p, msg in cands[:30]]}

    registered = 0
    details = []
    for e, p, msg in cands:
        rid, ctype = p["rid"], p["ctype"]
        link_field = "关联媒体人" if ctype == "媒体人" else "关联KOL"
        alias = config.BRAND_CONFIG[msg["brand"]]["alias_from"]
        ts = msg["ts"] or now_ms
        # 补建「已发送」草稿 (来源=cold, manual- 前缀; followup 不碰[无 Follow-up轮次]; auto_send 不重发[已发送])
        try:
            await feishu.create_record(config.T_DRAFT, {
                "邮件草稿ID": f"manual-{rid[-8:]}-{int(ts/1000)}",
                link_field: [rid],
                "邮件主题": (msg["subject"] or "(手动发送)")[:200],
                "邮件正文": "(手动发送邮件, 系统补登记占位 — 正文见 Zoho 发件箱; 此记录让 reply_monitor 能处理对方回复)",
                "邮件草稿状态": "已发送", "发送状态": "已发", "发送时间": ts,
                "邮件草稿来源": "cold", "对象类型": ctype,
                "收件邮箱": e, "发送邮箱": alias, "生成时间": now_ms,
            })
        except Exception as ex:
            print(f"[manual_send_recon] 建草稿失败 {e}: {ex}")
            continue
        # 合作状态 未建联→建联中 (不降级)
        if p["coop"] == "未建联":
            try:
                await feishu.update_record(config.T_KOL if ctype == "KOL" else config.T_EDITOR, rid, {"合作状态": "建联中"})
            except Exception as ex:
                print(f"[manual_send_recon] 更新合作状态失败 {e}: {ex}")
        # 跟进记录
        fu_tbl = config.T_KOL_FU if ctype == "KOL" else config.T_EDITOR_FU
        fu_link = "关联KOL" if ctype == "KOL" else "关联媒体人"
        try:
            await feishu.create_record(fu_tbl, {
                "跟进摘要": "[手动发送补登记] " + (msg["subject"] or "")[:60],
                "跟进日期": ts, "跟进方式": "邮件",
                "跟进内容": f"Zoho 发件箱审计发现手动发送给 {e}(主题: {msg['subject'][:80]}), 系统补登记追踪 → 对方回复后 reply_monitor 可正常处理。",
                fu_link: [rid],
            })
        except Exception as ex:
            print(f"[manual_send_recon] 跟进记录失败 {e}: {ex}")
        registered += 1
        details.append({"name": p["name"], "email": e, "type": ctype})
        tracked.add(rid)

    return {"registered": registered, "candidates": len(cands), "details": details[:30]}

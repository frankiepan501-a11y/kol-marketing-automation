# -*- coding: utf-8 -*-
"""KOL 同邮箱去重 gate (2026-06-04, 防持续脏数据).

背景: KOL 库有多个写入路径(影响者同步 Flow3 / 陈翔宇本地 scraper daemon / 手动), 同一创作者的
名字变体(如 spawnpoiiint/spawnpoiint, nani.gg 系列)被各自推成多条同邮箱 KOL → reply/warm_recap
重复弹卡、dispatch 同日重复 cold。一次性清理(34条)治标; 本模块是**源头无关的周期 gate**(治本):
每周扫同邮箱重复, 保留最有进展的1条, 弃用其余(迁移备注+清邮箱+合作状态=不合适, **可逆**)。

🔒 安全护栏(可无人值守自动跑): **只自动弃用"无任何寄样+无上稿+合作状态非已合作"的重复**。
若同组有 2+ 条"活跃"记录(真冲突, 弃用会丢真实活动)→ **整组跳过 + 飞书告警 Frankie 人工处理**,
绝不自动弃用有寄样/上稿/已合作的记录。
"""
import time
from . import config, feishu
from .feishu import ext

# 合作状态进展排序 (保留分高者)
_COOP_RANK = {
    "已合作-付费": 6, "已合作-免费(多次)": 5, "已合作-免费": 4,
    "洽谈中": 3, "样品评估": 3, "待回复": 2, "建联中": 2,
    "不合适": 1, "黑名单": 1, "未建联": 0, "空": 0, "": 0,
}
# "活跃"记录 = 有真实价值不能自动弃用 (有寄样/上稿/已合作)
_ACTIVE_COOP = {"已合作-付费", "已合作-免费(多次)", "已合作-免费"}


def _gi(f, k):
    try:
        return int(f.get(k) or 0)
    except (ValueError, TypeError):
        return 0


def _rec(r):
    f = r["fields"]
    coop = ext(f.get("合作状态")) or "空"
    ship = _gi(f, "寄样次数") or (1 if ext(f.get("上次寄样订单号")) else 0)
    upload = 1 if ext(f.get("上稿日期")) else 0
    return {"rid": r["record_id"], "name": ext(f.get("账号名")),
            "coop": coop, "ship": ship, "upload": upload}


def _score(x):
    return (10 if x["upload"] else 0) + x["ship"] * 3 + _COOP_RANK.get(x["coop"], 0)


def _is_active(x):
    """有真实价值, 不能被自动弃用."""
    return x["ship"] >= 1 or x["upload"] or x["coop"] in _ACTIVE_COOP


async def run(dry_run: bool = False) -> dict:
    """扫同邮箱重复 → 安全弃用无活动重复; 真冲突跳过+告警. 周 cron 调."""
    kol = await feishu.fetch_all_records(config.T_KOL)
    groups = {}
    for r in kol:
        e = ext(r["fields"].get("邮箱")).strip().lower()
        if "@" in e:
            groups.setdefault(e, []).append(_rec(r))
    dups = {e: rs for e, rs in groups.items() if len(rs) > 1}

    today = time.strftime("%Y-%m-%d", time.localtime())
    deprecated, conflicts, plan = [], [], []
    for e, rs in dups.items():
        rs2 = sorted(rs, key=_score, reverse=True)
        keep, drop = rs2[0], rs2[1:]
        active_drops = [d for d in drop if _is_active(d)]
        if active_drops:
            # 真冲突: 弃用方含活跃记录 → 整组不动, 告警人工
            conflicts.append({"email": e, "keep": keep["name"],
                              "drops": [f"{d['name']}[{d['coop']}|寄{d['ship']}|稿{d['upload']}]" for d in drop]})
            continue
        for d in drop:
            plan.append((d["rid"], d["name"], e, keep["name"], keep["rid"]))

    if not dry_run:
        for rid, nm, e, kn, krid in plan:
            note = f"[重复弃用 {today}] 已并入真身 {kn}({krid}); 原邮箱={e}"
            try:
                await feishu.update_record(config.T_KOL, rid, {
                    "迁移备注": note, "邮箱": "", "合作状态": "不合适"})
                deprecated.append({"rid": rid, "name": nm, "merged_into": kn, "email": e})
            except Exception as ex:
                print(f"[kol_dedup] 弃用失败 {nm}: {str(ex)[:80]}")
    else:
        deprecated = [{"rid": rid, "name": nm, "merged_into": kn, "email": e} for rid, nm, e, kn, krid in plan]

    # 告警: 真冲突需人工 (只在非 dry_run + 有冲突时发, 避免噪声)
    if conflicts and not dry_run:
        try:
            lines = "\n".join(f"• {c['email']}: 留 {c['keep']} | 弃 " + ", ".join(c["drops"]) for c in conflicts[:15])
            card = {
                "header": {"template": "orange",
                           "title": {"tag": "plain_text", "content": f"⚠️ KOL 重复需人工去重 ({len(conflicts)} 组)"}},
                "elements": [
                    {"tag": "div", "text": {"tag": "lark_md", "content":
                        (f"自动去重已清 {len(deprecated)} 条无活动重复。下面 {len(conflicts)} 组**弃用方也有寄样/上稿/已合作**, "
                         "自动去重不敢动(怕丢真实活动), 需你人工决定保留哪条:\n\n" + lines)[:1800]}},
                ],
            }
            await feishu.send_card_message("chat_id", config.NOTIFY_CHAT_ID, card, biz="AUDIT")
            for name, oid in config.NOTIFY_USERS:
                if name.startswith("潘"):
                    try:
                        await feishu.send_card_message("open_id", oid, card, biz="AUDIT")
                    except Exception:
                        pass
        except Exception as ex:
            print(f"[kol_dedup] 冲突告警发送失败: {str(ex)[:80]}")

    return {"重复组": len(dups), "自动弃用": len(deprecated),
            "真冲突需人工": len(conflicts), "deprecated": deprecated[:30], "conflicts": conflicts[:30]}

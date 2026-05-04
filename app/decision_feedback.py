# -*- coding: utf-8 -*-
"""Phase 3.2 — 决策反哺: 根据 Phase 3 销售归因结果, 自动升降级 KOL 合作状态.

升降级规则 (硬编码 v1, 未来挪到飞书规则表):

| 当前状态                | 触发条件                          | 升/降到              |
|--------------------------|------------------------------------|----------------------|
| 已合作-免费              | 累计 GMV ≥ $100 OR 订单 ≥ 2       | 已合作-免费(多次)    |
| 已合作-免费(多次)        | 累计 GMV ≥ $300 OR 订单 ≥ 5       | 已合作-付费          |
| 已合作-免费              | 寄样后 30 天 GMV = 0 + 0 上稿     | 不合适 (冷藏)        |
| 已合作-付费              | 任意                              | 维持 (人审才降级)    |
| 已合作-免费(多次)        | GMV = 0 60 天                     | 维持 (60 天宽限期)   |

每次升降级写「决策反哺日志」字段记录: 时间 + 原因 + 旧状态→新状态.

调用: cron 每日 09:35 BJ (Phase 3 销售归因 09:30 后 5 分钟跑).
"""
import time
from typing import Optional
from . import config, feishu
from .feishu import ext


# === 阈值参数 (硬编码 v1) ===
TIER_FREE_TO_RECURRING_GMV = 100.0
TIER_FREE_TO_RECURRING_ORDERS = 2
TIER_RECURRING_TO_PAID_GMV = 300.0
TIER_RECURRING_TO_PAID_ORDERS = 5
COLD_DAYS_AFTER_SAMPLE = 30  # 寄样后 30 天 0 GMV 0 上稿 → 冷藏


def _decide_action(cur_status: str, gmv: float, orders: int,
                   has_publish: bool, days_since_sample: int) -> Optional[dict]:
    """决策核心: 返回 {to_status, reason} 或 None (维持)"""
    if cur_status == "已合作-免费":
        if gmv >= TIER_RECURRING_TO_PAID_GMV or orders >= TIER_RECURRING_TO_PAID_ORDERS:
            # 跳级: 免费 → 付费 (单笔订单大或多次复购)
            return {"to": "已合作-付费",
                    "reason": f"跳级升付费: GMV=${gmv:.2f} 订单数={orders} 触发付费阈值"}
        if gmv >= TIER_FREE_TO_RECURRING_GMV or orders >= TIER_FREE_TO_RECURRING_ORDERS:
            return {"to": "已合作-免费(多次)",
                    "reason": f"升多次免费: GMV=${gmv:.2f} 订单数={orders}"}
        # v1 暂不主动冷藏 — 需要"上次状态变更时间"字段作精确寄样时间 proxy
        # 否则用入库日期会误降所有历史迁移 KOL. v2 加字段后启用下面这段:
        # if days_since_sample >= COLD_DAYS_AFTER_SAMPLE and gmv == 0 and not has_publish:
        #     return {"to": "不合适",
        #             "reason": f"冷藏: 寄样 {days_since_sample} 天 GMV=0 + 0 上稿"}
        return None

    if cur_status == "已合作-免费(多次)":
        if gmv >= TIER_RECURRING_TO_PAID_GMV or orders >= TIER_RECURRING_TO_PAID_ORDERS:
            return {"to": "已合作-付费",
                    "reason": f"升付费: GMV=${gmv:.2f} 订单数={orders}"}
        return None  # 60 天宽限,这一档不主动冷藏

    if cur_status == "已合作-付费":
        return None  # 维持 (Frankie 人审才降级)

    return None


def _gmv_of(f: dict) -> float:
    try: return float(f.get("累计GMV") or 0)
    except (ValueError, TypeError): return 0


def _orders_of(f: dict) -> int:
    try: return int(f.get("累计订单数") or 0)
    except (ValueError, TypeError): return 0


def _days_since(ms: Optional[int]) -> int:
    if not ms: return 99999
    return int((time.time() * 1000 - ms) / 86400000)


async def run():
    """每日 cron: 扫所有「已合作」KOL/媒体人, 按规则升降级."""
    started = time.time()
    summary = {"matched": 0, "upgraded_to_recurring": 0, "upgraded_to_paid": 0,
               "downgraded_to_unfit": 0, "no_change": 0, "details": [], "errors": []}

    for table_id, label in [(config.T_KOL, "KOL"), (config.T_EDITOR, "Editor")]:
        # 拉所有「已合作-免费」 / 「已合作-免费(多次)」 / 「已合作-付费」
        for status in ("已合作-免费", "已合作-免费(多次)", "已合作-付费"):
            try:
                items = await feishu.search_records(table_id, [
                    {"field_name": "合作状态", "operator": "is", "value": [status]}
                ])
            except Exception as e:
                summary["errors"].append({"label": label, "status": status, "err": str(e)[:100]})
                continue

            for r in items:
                f = r["fields"]
                gmv = _gmv_of(f)
                orders = _orders_of(f)
                last_order_ms = f.get("上次订单日期") or 0
                publish_ms = f.get("上稿日期") or 0
                # 寄样时间 proxy: 用「上稿监测时间」首次扫描时间, 或 KOL 入库后 30 天
                sample_proxy_ms = publish_ms or f.get("入库日期") or 0
                days_since_sample = _days_since(sample_proxy_ms)

                action = _decide_action(status, gmv, orders, bool(publish_ms), days_since_sample)
                if not action:
                    summary["no_change"] += 1
                    continue

                # 拼日志: 「[YYYY-MM-DD HH:MM] 旧状态→新状态: 原因」
                ts_str = time.strftime("%Y-%m-%d %H:%M", time.gmtime(time.time() + 8*3600))
                log_line = f"[{ts_str}] {status} → {action['to']}: {action['reason']}"
                cur_log = ext(f.get("决策反哺日志")) or ""
                new_log = (cur_log + "\n" + log_line) if cur_log else log_line
                if len(new_log) > 1500:
                    new_log = new_log[-1500:]  # 截断防爆

                update_fields = {
                    "合作状态": action["to"],
                    "决策反哺日志": new_log,
                }
                try:
                    await feishu.update_record(table_id, r["record_id"], update_fields)
                    summary["matched"] += 1
                    if action["to"] == "已合作-免费(多次)":
                        summary["upgraded_to_recurring"] += 1
                    elif action["to"] == "已合作-付费":
                        summary["upgraded_to_paid"] += 1
                    elif action["to"] == "不合适":
                        summary["downgraded_to_unfit"] += 1
                    summary["details"].append({
                        "label": label, "rid": r["record_id"],
                        "name": ext(f.get("账号名")) or ext(f.get("媒体人姓名")),
                        "from": status, "to": action["to"],
                        "gmv": gmv, "orders": orders, "reason": action["reason"],
                    })
                except Exception as e:
                    summary["errors"].append({"rid": r["record_id"], "err": str(e)[:100]})

    summary["elapsed_s"] = round(time.time() - started, 1)
    return {"ok": True, **summary}

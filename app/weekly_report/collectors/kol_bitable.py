"""KOL/媒体人 Bitable collector.

数据源 (复用 app.feishu + app.config):
  - KOL 营销库 (config.FEISHU_APP_TOKEN, 聪哥 1 号 app)
    - T_TASK_KOL / T_TASK_EDITOR: 营销任务台
    - T_DRAFT: 草稿/发送日志 (双端共用, 对象类型字段区分)
  - 选题池 (单独 base CPvwbGznza5L4ZsgBG8cULcinne, 用 feishu.api 直调任意 path)
    - tblMjoCLnikzKuA2: 关键词候选池

输出 collected.kol.data:
{
  "kol": {
    "tasks_created": int,
    "qualified": int,        # AI评分 ≥ 80 的草稿数 (本周内)
    "sent": int,             # 发送状态=已发
    "replied": int,          # 是否回复=True
    "intent_replies": int,   # 回复意图 in [感兴趣, 要报价]
    "decline": int,          # 回复意图 in [委婉拒绝, 退订]
    "interest_rate": float,  # = intent / sent
    "reply_rate": float,     # = replied / sent
    "decline_rate": float,
  },
  "editor": {
    同上, 阈值 ≥ 75
  },
  "topic_pool": {
    "candidates_total": int,
    "by_status": {"待选题": int, "已写 PK": int, "已写 FL": int, "已写双站": int, "不适合": int},
    "new_this_week": int,
    "consumed_this_week": int,  # 本周状态从「待选题」变「已写*」的
    "low_stock_alert": bool,    # 待选题 < 10
  },
}
"""
import asyncio
import datetime
import logging
from collections import defaultdict

log = logging.getLogger("weekly_report.kol_bitable")

# 选题池
TOPIC_POOL_APP = "CPvwbGznza5L4ZsgBG8cULcinne"
TOPIC_POOL_TABLE = "tblMjoCLnikzKuA2"

# 阈值
KOL_THRESHOLD = 80
EDITOR_THRESHOLD = 75


def _ext(f):
    """飞书 field unwrap (复制自 app.feishu.ext, 避免 import 触发 config env 检查)."""
    if f is None:
        return ""
    if isinstance(f, list):
        if not f:
            return ""
        if isinstance(f[0], dict):
            return f[0].get("text") or f[0].get("link") or f[0].get("name") or ""
        return str(f[0])
    if isinstance(f, dict):
        return f.get("text") or f.get("link") or ""
    return f or ""


def _ts_ms(d: datetime.date, end: bool = False) -> int:
    """date → ms timestamp. end=True 取 23:59:59."""
    if end:
        dt = datetime.datetime.combine(d, datetime.time(23, 59, 59))
    else:
        dt = datetime.datetime.combine(d, datetime.time(0, 0, 0))
    return int(dt.timestamp() * 1000)


def _in_window(record: dict, start_ms: int, end_ms: int) -> bool:
    ct = record.get("created_time") or 0
    return start_ms <= ct <= end_ms


def _calc_drafts(drafts: list, obj_type: str, threshold: int, start_ms: int, end_ms: int) -> dict:
    """从草稿表 records 里统计指定对象类型的本周指标."""
    in_week = [d for d in drafts if _in_window(d, start_ms, end_ms)]
    typed = [d for d in in_week
             if (_ext(d.get("fields", {}).get("对象类型")) or "KOL") == obj_type]

    qualified = sum(1 for d in typed if (d["fields"].get("AI评分") or 0) >= threshold)
    sent = sum(1 for d in typed if _ext(d["fields"].get("发送状态")) == "已发")
    replied = sum(1 for d in typed if d["fields"].get("是否回复"))
    intent = sum(1 for d in typed if _ext(d["fields"].get("回复意图")) in ("感兴趣", "要报价"))
    decline = sum(1 for d in typed if _ext(d["fields"].get("回复意图")) in ("委婉拒绝", "退订"))

    return {
        "qualified": qualified,
        "sent": sent,
        "replied": replied,
        "intent_replies": intent,
        "decline": decline,
        "interest_rate": round(intent / max(sent, 1), 4),
        "reply_rate": round(replied / max(sent, 1), 4),
        "decline_rate": round(decline / max(sent, 1), 4),
    }


def _calc_tasks(tasks: list, start_ms: int, end_ms: int) -> int:
    """统计本周新建任务数."""
    return sum(1 for t in tasks if _in_window(t, start_ms, end_ms))


async def _fetch_topic_pool() -> list:
    """选题池在独立 base, 用 feishu.api 直调."""
    from app import feishu
    items = []
    page_token = ""
    while True:
        path = f"/bitable/v1/apps/{TOPIC_POOL_APP}/tables/{TOPIC_POOL_TABLE}/records?page_size=100"
        if page_token:
            path += f"&page_token={page_token}"
        r = await feishu.api("GET", path)
        d = r.get("data") or {}
        items.extend(d.get("items") or [])
        if not d.get("has_more"):
            break
        page_token = d.get("page_token", "")
        if not page_token:
            break
    return items


def _calc_topic_pool(items: list, start_ms: int, end_ms: int) -> dict:
    by_status = defaultdict(int)
    consumed_this_week = 0
    new_this_week = 0
    for it in items:
        f = it.get("fields", {})
        status = _ext(f.get("状态")) or "(空)"
        by_status[status] += 1

        # 本周新增 (按 created_time)
        if _in_window(it, start_ms, end_ms):
            new_this_week += 1

        # 本周消费 = last_modified_time 在本周 + 状态非「待选题」
        # 飞书 record 有 last_modified_time 字段
        lmt = it.get("last_modified_time") or 0
        if start_ms <= lmt <= end_ms and status not in ("待选题", "(空)"):
            consumed_this_week += 1

    waiting = by_status.get("待选题", 0)
    return {
        "candidates_total": len(items),
        "by_status": dict(by_status),
        "new_this_week": new_this_week,
        "consumed_this_week": consumed_this_week,
        "low_stock_alert": waiting < 10,
        "waiting": waiting,
    }


async def collect(start_date, end_date) -> dict:
    log.info("kol_bitable.collect %s ~ %s", start_date, end_date)
    start_ms = _ts_ms(start_date)
    end_ms = _ts_ms(end_date, end=True)

    try:
        from app import feishu, config
        # 并发拉 3 张 KOL 营销库表 + 选题池
        drafts, task_kol, task_editor, topic_pool = await asyncio.gather(
            feishu.fetch_all_records(config.T_DRAFT),
            feishu.fetch_all_records(config.T_TASK_KOL),
            feishu.fetch_all_records(config.T_TASK_EDITOR),
            _fetch_topic_pool(),
            return_exceptions=True,
        )

        result = {"window": f"{start_date}~{end_date}"}

        # KOL
        if isinstance(drafts, Exception) or isinstance(task_kol, Exception):
            log.warning("kol fetch partial failure")
            result["kol"] = {
                "error": f"drafts={type(drafts).__name__ if isinstance(drafts, Exception) else 'ok'} "
                         f"task_kol={type(task_kol).__name__ if isinstance(task_kol, Exception) else 'ok'}"
            }
        else:
            result["kol"] = {
                "tasks_created": _calc_tasks(task_kol, start_ms, end_ms),
                **_calc_drafts(drafts, "KOL", KOL_THRESHOLD, start_ms, end_ms),
            }

        # Editor
        if isinstance(drafts, Exception) or isinstance(task_editor, Exception):
            result["editor"] = {
                "error": f"drafts={type(drafts).__name__ if isinstance(drafts, Exception) else 'ok'} "
                         f"task_editor={type(task_editor).__name__ if isinstance(task_editor, Exception) else 'ok'}"
            }
        else:
            result["editor"] = {
                "tasks_created": _calc_tasks(task_editor, start_ms, end_ms),
                **_calc_drafts(drafts, "媒体人", EDITOR_THRESHOLD, start_ms, end_ms),
            }

        # Topic pool
        if isinstance(topic_pool, Exception):
            result["topic_pool"] = {"error": f"{type(topic_pool).__name__}: {topic_pool}"}
        else:
            result["topic_pool"] = _calc_topic_pool(topic_pool, start_ms, end_ms)

        return {"status": "ok", "data": result}

    except Exception as e:
        log.exception("kol_bitable collect outer fail")
        return {"status": "error", "error": f"{type(e).__name__}: {e}", "data": {}}


if __name__ == "__main__":
    import datetime as dt
    today = dt.date.today()
    last_sun = today - dt.timedelta(days=today.weekday() + 1)
    last_mon = last_sun - dt.timedelta(days=6)
    print(asyncio.run(collect(last_mon, last_sun)))

"""KOL 关键词开发自动派工 — 周一/四 09:00 BJ 自动从词库表抽 5 个关键词建爬虫任务

链路:
1. 查 KOL 开发关键词词库 (T_KEYWORD) 状态=启用, 按"上次跑日期"升序 (优先抽最久未跑的)
2. 抽前 N 个 (默认 5)
3. 在爬虫任务台 (T_CRAWL_TASK) 建 KOL-YouTube 任务
4. 更新词库表"上次跑日期"+"累计跑次数"
5. 飞书私聊 Frankie 汇报

n8n cron: 周一+周四 09:00 BJ → POST /kol-keyword-cron/run
"""
import time, os, json
from . import config, feishu


T_KEYWORD = os.environ.get("T_KEYWORD", "tblDjyE5ZkXnzf2r")
T_CRAWL_TASK = os.environ.get("T_CRAWL_TASK", "tblQnLHnBa1RjJUE")

DAILY_LIMIT = int(os.environ.get("KW_DAILY_LIMIT", "5"))
PER_KW_CHANNEL_LIMIT = int(os.environ.get("KW_PER_KW_CHANNEL_LIMIT", "50"))

FRANKIE_OPEN_ID = "ou_629ce01f4bc31de078e10fcb038dbf78"


async def run():
    # 1. 查 启用关键词
    items = await feishu.search_records(T_KEYWORD, [
        {"field_name": "状态", "operator": "is", "value": ["启用"]},
    ])
    if not items:
        return {"ok": False, "error": "no enabled keywords in T_KEYWORD"}

    # 2. 按"上次跑日期"升序 (空值=0 排最前, 最久未跑的优先)
    def _sort_key(it):
        v = it["fields"].get("上次跑日期", 0) or 0
        if not isinstance(v, (int, float)): v = 0
        return v
    items.sort(key=_sort_key)
    top_n = items[:DAILY_LIMIT]

    today_str = time.strftime("%Y-%m-%d")
    today_ms = int(time.time() * 1000)

    created, failed = [], []
    for kw_rec in top_n:
        kf = kw_rec["fields"]
        kw = feishu.ext(kf.get("关键词")).strip()
        if not kw:
            continue
        try:
            await feishu.create_record(T_CRAWL_TASK, {
                "任务名": f"[自动] YT KOL - {kw} - {today_str}",
                "爬虫类型": "KOL-YouTube",
                "关键词列表": kw,
                "每批数量上限": PER_KW_CHANNEL_LIMIT,
                "任务状态": "1-待触发",
            })
        except Exception as e:
            failed.append({"keyword": kw, "stage": "create_task", "error": str(e)[:150]})
            continue

        cur = kf.get("累计跑次数", 0) or 0
        if not isinstance(cur, (int, float)): cur = 0
        try:
            await feishu.update_record(T_KEYWORD, kw_rec["record_id"], {
                "上次跑日期": today_ms,
                "累计跑次数": int(cur) + 1,
            })
        except Exception as e:
            failed.append({"keyword": kw, "stage": "update_keyword", "error": str(e)[:150]})

        created.append(kw)

    # 5. 飞书私聊 Frankie
    if created or failed:
        lines = [f"📣 KOL 关键词开发自动派工 ({today_str})", ""]
        if created:
            lines.append(f"✅ 已建 {len(created)} 条 YT 爬虫任务, 陈翔宇电脑 daemon 5min 内拾起执行:")
            lines.append("")
            for k in created:
                lines.append(f"• {k}")
        if failed:
            lines.append("")
            lines.append(f"⚠️ {len(failed)} 条失败:")
            for f in failed:
                lines.append(f"• [{f['stage']}] {f['keyword']}: {f['error']}")
        text = "\n".join(lines)
        try:
            await feishu.api("POST", "/im/v1/messages?receive_id_type=open_id",
                             {"receive_id": FRANKIE_OPEN_ID,
                              "msg_type": "text",
                              "content": json.dumps({"text": text}, ensure_ascii=False)},
                             which="notify")
        except Exception as e:
            print(f"[keyword_cron] 飞书通知失败: {e}")

    return {
        "ok": True,
        "created": len(created),
        "keywords": created,
        "failed": failed,
    }

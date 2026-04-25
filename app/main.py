"""KOL 营销自动化 Webhook 服务
部署在 Zeabur, 由 n8n cron / webhook 触发
"""
from fastapi import FastAPI, Header, HTTPException
from . import config, reply_monitor, dashboard, followup, enrich, auto_send, draft_router

app = FastAPI(title="KOL Marketing Automation", version="0.2")


def _check_auth(auth: str):
    if not auth or not auth.startswith("Bearer "):
        raise HTTPException(401, "Missing Bearer token")
    if auth[7:] != config.INTERNAL_TOKEN:
        raise HTTPException(401, "Invalid token")


@app.get("/")
async def root():
    return {"service": "kol-marketing-automation", "status": "up"}


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/reply-monitor/run")
async def run_reply_monitor(authorization: str = Header(default="")):
    """扫 partner@ 收件箱新回复 → DeepSeek 分类 → 更新数据库 → 飞书通知 + 生成回复草稿"""
    _check_auth(authorization)
    try:
        result = await reply_monitor.run()
        return {"ok": True, **result}
    except Exception as e:
        import traceback
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()[-1000:]}


@app.post("/dashboard/refresh")
async def run_dashboard(authorization: str = Header(default="")):
    """每日 9:00 刷新 KOL+编辑 营销数据看板"""
    _check_auth(authorization)
    try:
        result = await dashboard.run()
        return {"ok": True, **result}
    except Exception as e:
        import traceback
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()[-1000:]}


@app.post("/followup/generate")
async def run_followup(authorization: str = Header(default="")):
    """每日 10:00 扫无回复草稿 → 生成 D+7 第2封 / D+14 第3封 → 调 reviewer"""
    _check_auth(authorization)
    try:
        result = await followup.run()
        return {"ok": True, **result}
    except Exception as e:
        import traceback
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()[-1000:]}


@app.post("/enrich-task/run")
async def run_enrich_task(authorization: str = Header(default="")):
    """每 5 分钟扫 KOL 营销任务台 待触发任务 → 富化打分 + 生草稿 + 调 reviewer"""
    _check_auth(authorization)
    try:
        result = await enrich.run()
        return {"ok": True, **result}
    except Exception as e:
        import traceback
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()[-1500:]}


@app.post("/auto-send/run")
async def run_auto_send(authorization: str = Header(default="")):
    """每 10 分钟扫 自动通过/通过 状态草稿 → Zoho 双品牌发送 + 限速"""
    _check_auth(authorization)
    try:
        result = await auto_send.run()
        return {"ok": True, **result}
    except Exception as e:
        import traceback
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()[-1000:]}


@app.post("/reviewer/scan")
async def run_reviewer_scan(authorization: str = Header(default="")):
    """兜底: 扫所有 待审 + 没 AI评分 的草稿, 跑 reviewer 自审 (防止生成器漏调)"""
    _check_auth(authorization)
    try:
        result = await draft_router.batch_review_pending()
        return {"ok": True, **result}
    except Exception as e:
        import traceback
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()[-1000:]}


@app.post("/reviewer/run-one")
async def run_reviewer_one(authorization: str = Header(default=""),
                            record_id: str = ""):
    """单条草稿强制重审 (人工触发, 调试用)"""
    _check_auth(authorization)
    if not record_id:
        return {"ok": False, "error": "missing record_id"}
    try:
        result = await draft_router.route_draft(record_id)
        return {"ok": True, **result}
    except Exception as e:
        import traceback
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()[-500:]}

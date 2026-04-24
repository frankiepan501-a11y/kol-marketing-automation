"""KOL 营销自动化 Webhook 服务
部署在 Zeabur,由 n8n cron / webhook 触发
"""
from fastapi import FastAPI, Header, HTTPException
from . import config, reply_monitor, dashboard, followup

app = FastAPI(title="KOL Marketing Automation", version="0.1")


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
    """扫 partner@ 收件箱新回复 → DeepSeek 分类 → 更新数据库 → 飞书通知"""
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
    """每日 10:00 扫无回复草稿 → 生成 D+7 第2封 / D+14 第3封"""
    _check_auth(authorization)
    try:
        result = await followup.run()
        return {"ok": True, **result}
    except Exception as e:
        import traceback
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()[-1000:]}


# TODO 下一步: /send-approved/run, /enrich-kol-task, /enrich-editor-task

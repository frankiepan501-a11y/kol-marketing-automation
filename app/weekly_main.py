"""DTC Weekly Report 独立 FastAPI 入口.

部署到 Zeabur service `dtc-weekly` (与 kol-automation service 完全独立, env 命名空间隔离).

启动: ENTRY=app.weekly_main:app 在 Dockerfile.

设计依据 (双框架):
- 之前混在 kol-automation service 导致 env 命名空间混乱, 41 个 env 看不清边界, 是伪 L8 杠杆
- 拆独立 service 后:
  * 业务边界 = service 边界 (KOL 派单 vs DTC 周报)
  * env 互不影响 (改一个不会污染另一个)
  * 故障隔离 (长任务/重启互不波及)

复用同 GitHub repo + 同 Dockerfile, 只是 entry 不同. 代码资产 100% 复用.
"""
from fastapi import FastAPI, Header, HTTPException

from . import config
from . import weekly_report

app = FastAPI(title="DTC Weekly Report", version="0.3")


def _check_auth(auth: str):
    if not auth or not auth.startswith("Bearer "):
        raise HTTPException(401, "Missing Bearer token")
    if auth[7:] != config.INTERNAL_TOKEN:
        raise HTTPException(401, "Invalid token")


@app.get("/")
async def root():
    return {"service": "dtc-weekly-report", "status": "up", "version": "0.3"}


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/weekly-report/run")
async def run_weekly_report(authorization: str = Header(default=""),
                             dry_run: bool = False,
                             async_mode: bool = True):
    """每周一 08:00 BJ n8n cron 触发. 全自动生成双品牌运营周报.

    - ?dry_run=true 跳过飞书发布, 返回数据预览 + collectors 状态 + gaps
    - ?async_mode=true (默认) fire-and-forget 后台跑 (避开 Zeabur 165s 网关 timeout)
    """
    import asyncio
    _check_auth(authorization)
    if async_mode and not dry_run:
        asyncio.create_task(weekly_report.main.run(dry_run=False))
        return {"ok": True, "started": "background",
                "msg": "weekly report run started, will push to feishu when done (~60s)"}
    try:
        result = await weekly_report.main.run(dry_run=dry_run)
        return {"ok": True, **result}
    except Exception as e:
        import traceback
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()[-1500:]}


@app.post("/weekly-report/dry-run")
async def weekly_report_dry_run(authorization: str = Header(default="")):
    """显式 dry-run 接口: 同步跑 collectors + integrator + renderer, 不发飞书. 用于调试."""
    _check_auth(authorization)
    try:
        result = await weekly_report.main.run(dry_run=True)
        return {"ok": True, **result}
    except Exception as e:
        import traceback
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()[-1500:]}

from __future__ import annotations

import logging
import os
import threading
import uuid
from datetime import datetime, timezone
from typing import Any, Literal

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

from .clients import FeishuClient, YouTubeClient
from .collector import IncrementalCollector
from .constants import BASE_TOKEN, CONFIG_READ_FIELDS, DEFAULT_PLATFORM, TABLES
from .core import BEIJING, is_youtube_video_id, scalar
from .job_status import durable_job_snapshot_many, finished_status
from .daily import DailyReporter, NYXI_CONFIG_ID, format_report
from .quota import DailySearchBudget

BUILD_VERSION = os.environ.get("BUILD_VERSION", "dev")
COMMIT_ENABLED = os.environ.get("COMMIT_ENABLED", "0") == "1"
SERVICE_AUTH_TOKEN = os.environ.get("SERVICE_AUTH_TOKEN", "")

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("socialecho-youtube-incremental")

app = FastAPI(title="SocialEcho YouTube Incremental", version=BUILD_VERSION)
_lock = threading.Lock()
_jobs: dict[str, dict[str, Any]] = {}


class RunRequest(BaseModel):
    brand: str | None = None
    platform: Literal["YouTube"] = DEFAULT_PLATFORM
    config_record_id: str | None = None
    mode: Literal["preview", "commit"] = "commit"
    force: bool = False


class BackfillRequest(BaseModel):
    brand: str | None = None
    platform: Literal["YouTube"] = DEFAULT_PLATFORM
    config_record_id: str | None = None
    mode: Literal["preview", "commit"] = "preview"
    window_days: int = 7
    force: bool = False


class ReplayRequest(BaseModel):
    mode: Literal["preview", "commit"] = "preview"
    brand: str | None = None
    platform: Literal["YouTube"] = DEFAULT_PLATFORM
    config_record_id: str | None = None


def _authorized(authorization: str | None) -> None:
    if not SERVICE_AUTH_TOKEN:
        raise HTTPException(status_code=503, detail="service auth is not configured")
    if authorization != f"Bearer {SERVICE_AUTH_TOKEN}":
        raise HTTPException(status_code=401, detail="unauthorized")


def _execute(job_id: str, request: RunRequest) -> None:
    started = datetime.now(timezone.utc)
    _jobs[job_id] = {
        "job_id": job_id,
        "status": "running",
        "mode": request.mode,
        "started_at": started.isoformat(),
    }
    collector: IncrementalCollector | None = None
    try:
        collector = IncrementalCollector(FeishuClient(), YouTubeClient())
        if request.config_record_id or request.brand:
            result = collector.run(
                now=started,
                commit=request.mode == "commit",
                force=request.force,
                job_id=job_id,
                config_record_id=request.config_record_id,
                brand=request.brand,
                platform=request.platform,
            )
        else:
            result = collector.run_many(
                now=started,
                commit=request.mode == "commit",
                force=request.force,
                job_id=job_id,
                platform=request.platform,
            )
        _jobs[job_id] = {
            "job_id": job_id,
            "status": result.get("status", "completed"),
            "started_at": started.isoformat(),
            "finished_at": datetime.now(timezone.utc).isoformat(),
            **result,
        }
        logger.info(
            "job complete id=%s mode=%s status=%s new=%s updated=%s reason=%s",
            job_id,
            request.mode,
            result.get("status"),
            result.get("new_posts", 0),
            result.get("updated_existing", 0),
            result.get("reason", ""),
        )
    except Exception as error:
        logger.exception("job failed id=%s type=%s", job_id, type(error).__name__)
        if request.mode == "commit" and collector is not None and request.config_record_id:
            try:
                collector.mark_failure(
                    error,
                    config_record_id=request.config_record_id,
                    job_id=job_id,
                )
            except Exception:
                logger.exception("failed to record failure state id=%s", job_id)
        _jobs[job_id] = {
            "job_id": job_id,
            "status": "failed",
            "mode": request.mode,
            "started_at": started.isoformat(),
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "error_type": type(error).__name__,
        }
    finally:
        _lock.release()


def _execute_backfill(job_id: str, request: BackfillRequest) -> None:
    started = datetime.now(timezone.utc)
    _jobs[job_id] = {
        "job_id": job_id,
        "status": "running",
        "mode": request.mode,
        "operation": "backfill",
        "started_at": started.isoformat(),
    }
    collector: IncrementalCollector | None = None
    resolved_config_record_id = request.config_record_id
    try:
        collector = IncrementalCollector(FeishuClient(), YouTubeClient())
        if request.mode == "commit" and not resolved_config_record_id:
            resolved_config_record_id = str(
                collector._config(brand=request.brand, platform=request.platform).get("_record_id") or ""
            )
        result = collector.backfill(
            now=started,
            commit=request.mode == "commit",
            window_days=request.window_days,
            force=request.force,
            job_id=job_id,
            config_record_id=request.config_record_id,
            brand=request.brand,
            platform=request.platform,
        )
        _jobs[job_id] = {
            "job_id": job_id,
            "status": result.get("status", "completed"),
            "operation": "backfill",
            "started_at": started.isoformat(),
            "finished_at": datetime.now(timezone.utc).isoformat(),
            **result,
        }
        logger.info(
            "backfill complete id=%s mode=%s brand=%s window=%s/%s new=%s next=%s",
            job_id,
            request.mode,
            request.brand or request.config_record_id or "auto",
            result.get("window_start"),
            result.get("window_end"),
            result.get("new_posts", 0),
            result.get("next_end", ""),
        )
    except Exception as error:
        logger.exception("backfill failed id=%s type=%s", job_id, type(error).__name__)
        if request.mode == "commit" and collector is not None and resolved_config_record_id:
            try:
                collector.mark_backfill_failure(
                    error,
                    config_record_id=resolved_config_record_id,
                    job_id=job_id,
                )
            except Exception:
                logger.exception("failed to record backfill failure id=%s", job_id)
        _jobs[job_id] = {
            "job_id": job_id,
            "status": "failed",
            "mode": request.mode,
            "operation": "backfill",
            "started_at": started.isoformat(),
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "error_type": type(error).__name__,
            "error_message": str(error)[:500],
        }
    finally:
        _lock.release()


def _execute_daily(job_id: str) -> None:
    started = datetime.now(timezone.utc)
    _jobs[job_id] = {"job_id": job_id, "status": "running", "operation": "daily", "started_at": started.isoformat()}
    nyxi: dict[str, Any] = {"status": "failed", "job_id": job_id}
    weekly: dict[str, Any] | None = None
    report: dict[str, Any] | None = None
    try:
        feishu = FeishuClient()
        reporter = DailyReporter(feishu)
        existing = reporter.begin(started)
        if existing.get("state") == "sent":
            _jobs[job_id] = {
                "job_id": job_id, "status": "completed", "operation": "daily",
                "reason": "already_reported", "message_id": existing.get("message_id"),
                "finished_at": datetime.now(timezone.utc).isoformat(),
            }
            return
        if existing.get("state") == "sending":
            raise RuntimeError("previous message outcome is unknown; reconcile the group before retry")
        collector = IncrementalCollector(feishu, YouTubeClient())
        try:
            nyxi = collector.run(
                now=started, commit=True, force=True, job_id=job_id,
                config_record_id=NYXI_CONFIG_ID, brand="NYXI",
            )
            nyxi["job_id"] = job_id
            if started.astimezone(BEIJING).weekday() == 0:
                try:
                    weekly = collector.refresh_older(
                        now=started, commit=True, config_record_id=NYXI_CONFIG_ID
                    )
                except Exception as error:
                    weekly = {"status": "failed", "error_type": type(error).__name__}
        except Exception as error:
            logger.exception("daily NYXI failed id=%s", job_id)
            nyxi = {"status": "failed", "job_id": job_id, "error_type": type(error).__name__}
            try:
                collector.mark_failure(error, config_record_id=NYXI_CONFIG_ID, job_id=job_id)
            except Exception:
                logger.exception("failed to record NYXI failure id=%s", job_id)
        posts = reporter.today_posts(started, existing) if nyxi["status"] == "completed" else []
        known_channels = reporter.known_channels_before(existing) if nyxi["status"] == "completed" else set()
        backfill = {"status": "not_started", "message": "历史补采尚未启动"}
        if nyxi["status"] != "completed":
            backfill = {"status": "nyxi_failed", "message": "NYXI 失败，未启动历史补采"}
        else:
            try:
                budget = DailySearchBudget.from_nyxi(nyxi)
                backfill = collector.backfill_budgeted(
                    now=started, job_id=job_id, budget=budget, brand="8BitDo",
                )
            except Exception as error:
                logger.exception("daily 8BitDo backfill failed id=%s", job_id)
                backfill = {"status": "failed", "message": f"历史补采失败（{type(error).__name__}）；游标保留"}
        text = format_report(
            started, nyxi=nyxi, backfill=backfill, posts=posts,
            weekly=weekly, known_channel_ids=known_channels,
        )
        report = reporter.send_once(started, text, job_id=job_id)
        result_status = "completed" if nyxi["status"] == "completed" else "failed"
        _jobs[job_id] = {
            "job_id": job_id, "status": result_status, "operation": "daily",
            "started_at": started.isoformat(), "finished_at": datetime.now(timezone.utc).isoformat(),
            "nyxi": nyxi, "weekly": weekly, "backfill": backfill, "report": report,
        }
    except Exception as error:
        logger.exception("daily job failed id=%s", job_id)
        _jobs[job_id] = {
            "job_id": job_id, "status": "failed", "operation": "daily",
            "started_at": started.isoformat(), "finished_at": datetime.now(timezone.utc).isoformat(),
            "error_type": type(error).__name__, "error_message": str(error)[:300],
            "nyxi": nyxi, "weekly": weekly, "report": report,
        }
    finally:
        _lock.release()


@app.get("/health")
def health() -> dict[str, Any]:
    return {"ok": True, "version": BUILD_VERSION, "commit_enabled": COMMIT_ENABLED}


@app.get("/admin/version")
def version() -> dict[str, Any]:
    return {"service": "socialecho-youtube-incremental", "version": BUILD_VERSION}


@app.get("/status")
def durable_status(
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    """Read the durable run summary stored in Feishu, surviving service restarts."""
    _authorized(authorization)
    configs = FeishuClient().list_records(
        BASE_TOKEN, TABLES["keyword_config"], field_names=CONFIG_READ_FIELDS
    )
    enabled = [
        row
        for row in configs
        if scalar(row.get("启用")) is True and scalar(row.get("平台")) == DEFAULT_PLATFORM
    ]
    return {
        "ok": True,
        "service": "socialecho-youtube-incremental",
        "version": BUILD_VERSION,
        "config_count": len(enabled),
        "configs": [
            {
                "config_record_id": row.get("_record_id"),
                "brand": row.get("竞品品牌"),
                "platform": row.get("平台"),
                "run_status": row.get("运行状态"),
                "last_success_at": row.get("最近成功采集时间"),
                "waterline": row.get("最近采集水位"),
                "last_new_posts": row.get("最近新增帖子数"),
                "candidate_new_kols": row.get("最近新增KOL候选数"),
                "summary": row.get("YouTube历史进度"),
                "error_summary": row.get("错误摘要"),
            }
            for row in enabled
        ],
    }


@app.post("/run")
def run(
    request: RunRequest,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    _authorized(authorization)
    if request.mode == "commit" and not COMMIT_ENABLED:
        raise HTTPException(status_code=409, detail="commit mode is disabled")
    if not _lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="a collection job is already running")
    job_id = f"ytinc-{uuid.uuid4().hex[:12]}"
    thread = threading.Thread(target=_execute, args=(job_id, request), daemon=True)
    thread.start()
    return {
        "ok": True,
        "accepted": True,
        "job_id": job_id,
        "status_url": f"/runs/{job_id}",
        "mode": request.mode,
    }


@app.post("/backfill")
def backfill(
    request: BackfillRequest,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    """Queue one bounded newest-to-oldest YouTube history window."""
    _authorized(authorization)
    if not request.brand and not request.config_record_id:
        raise HTTPException(status_code=422, detail="brand or config_record_id is required")
    if request.window_days < 1 or request.window_days > 31:
        raise HTTPException(status_code=422, detail="window_days must be between 1 and 31")
    if request.mode == "commit" and not COMMIT_ENABLED:
        raise HTTPException(status_code=409, detail="commit mode is disabled")
    if not _lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="a collection job is already running")
    job_id = f"ytbackfill-{uuid.uuid4().hex[:12]}"
    thread = threading.Thread(target=_execute_backfill, args=(job_id, request), daemon=True)
    thread.start()
    return {
        "ok": True,
        "accepted": True,
        "job_id": job_id,
        "status_url": f"/runs/{job_id}",
        "mode": request.mode,
        "operation": "backfill",
        "window_days": request.window_days,
    }


@app.post("/daily")
def daily(authorization: str | None = Header(default=None)) -> dict[str, Any]:
    _authorized(authorization)
    if not COMMIT_ENABLED:
        raise HTTPException(status_code=409, detail="commit mode is disabled")
    if not _lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="a collection job is already running")
    job_id = f"ytdaily-{uuid.uuid4().hex[:12]}"
    threading.Thread(target=_execute_daily, args=(job_id,), daemon=True).start()
    return {"ok": True, "accepted": True, "job_id": job_id, "status_url": f"/runs/{job_id}"}


@app.post("/report/test")
def test_report(authorization: str | None = Header(default=None)) -> dict[str, Any]:
    """Send one fixed, clearly labelled channel test per Beijing business day."""
    _authorized(authorization)
    if not COMMIT_ENABLED:
        raise HTTPException(status_code=409, detail="commit mode is disabled")
    now = datetime.now(timezone.utc)
    date = now.astimezone(BEIJING).date().isoformat()
    reporter = DailyReporter(FeishuClient())
    return reporter.send_once(
        now,
        f"🟢 [KOL·P3] NYXI YouTube 日报通道测试 · {date}\n这是一条测试消息，不代表采集结果。",
        job_id=f"report-test-{date}", key=f"{date}|NYXI日报测试",
    )


@app.get("/runs/{job_id}")
def status(job_id: str, authorization: str | None = Header(default=None)) -> dict[str, Any]:
    _authorized(authorization)
    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")
    return job


@app.get("/runs/{job_id}/assert")
def assert_finished(
    job_id: str, authorization: str | None = Header(default=None)
) -> dict[str, Any]:
    """Return 2xx only after a successful completion or an intentional skip."""
    _authorized(authorization)
    job = _jobs.get(job_id)
    if not job and job_id.startswith("ytdaily-"):
        raise HTTPException(status_code=404, detail="daily job lost before final verification")
    if not job:
        configs = FeishuClient().list_records(
            BASE_TOKEN, TABLES["keyword_config"], field_names=CONFIG_READ_FIELDS
        )
        job = durable_job_snapshot_many(job_id, configs)
    status_code, payload = finished_status(job)
    if status_code != 200:
        raise HTTPException(status_code=status_code, detail=payload["detail"])
    return payload


@app.post("/replay/{video_id}")
def replay(
    video_id: str,
    request: ReplayRequest,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    _authorized(authorization)
    if not is_youtube_video_id(video_id):
        raise HTTPException(status_code=422, detail="invalid YouTube video id")
    if request.mode == "commit" and not COMMIT_ENABLED:
        raise HTTPException(status_code=409, detail="commit mode is disabled")
    if not _lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="a collection job is already running")
    try:
        return IncrementalCollector(FeishuClient(), YouTubeClient()).replay_video(
            video_id,
            now=datetime.now(timezone.utc),
            commit=request.mode == "commit",
            config_record_id=request.config_record_id,
            brand=request.brand,
            platform=request.platform,
        )
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error)) from None
    finally:
        _lock.release()

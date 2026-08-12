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
from .constants import BASE_TOKEN, CONFIG_RECORD_ID, TABLES
from .core import is_youtube_video_id

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
    brand: Literal["NYXI"] = "NYXI"
    platform: Literal["YouTube"] = "YouTube"
    mode: Literal["preview", "commit"] = "commit"
    force: bool = False


class ReplayRequest(BaseModel):
    mode: Literal["preview", "commit"] = "preview"


def finished_status(job: dict[str, Any] | None) -> tuple[int, dict[str, Any]]:
    """Map an in-memory job to an HTTP status without requiring FastAPI in tests."""
    if not job:
        return 404, {"detail": "job not found"}
    if job.get("status") == "running":
        return 409, {"detail": "job is still running"}
    if job.get("status") == "failed":
        return 500, {
            "detail": {
                "job_id": job.get("job_id"),
                "error_type": job.get("error_type"),
            }
        }
    return 200, job


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
        result = collector.run(
            now=started,
            commit=request.mode == "commit",
            force=request.force,
            job_id=job_id,
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
        if request.mode == "commit" and collector is not None:
            try:
                collector.mark_failure(error, job_id=job_id)
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
    config = FeishuClient().get_record(
        BASE_TOKEN, TABLES["keyword_config"], CONFIG_RECORD_ID
    )
    return {
        "ok": True,
        "service": "socialecho-youtube-incremental",
        "version": BUILD_VERSION,
        "run_status": config.get("运行状态"),
        "last_success_at": config.get("最近成功采集时间"),
        "waterline": config.get("最近采集水位"),
        "last_new_posts": config.get("最近新增帖子数"),
        "candidate_new_kols": config.get("最近新增KOL候选数"),
        "summary": config.get("YouTube历史进度"),
        "error_summary": config.get("错误摘要"),
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
        )
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error)) from None
    finally:
        _lock.release()

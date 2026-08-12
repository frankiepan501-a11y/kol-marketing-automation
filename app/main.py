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
                collector.mark_failure(error)
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
    if not job:
        raise HTTPException(status_code=404, detail="job not found")
    if job.get("status") == "running":
        raise HTTPException(status_code=409, detail="job is still running")
    if job.get("status") == "failed":
        raise HTTPException(status_code=500, detail={"job_id": job_id, "error_type": job.get("error_type")})
    return job

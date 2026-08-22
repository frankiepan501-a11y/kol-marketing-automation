from __future__ import annotations

from typing import Any


def finished_status(job: dict[str, Any] | None) -> tuple[int, dict[str, Any]]:
    """Map a job to an HTTP status without requiring the web framework."""
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


def durable_job_snapshot(
    job_id: str, config: dict[str, Any]
) -> dict[str, Any] | None:
    """Recover one job's terminal state from Feishu after a service restart."""
    summary = str(config.get("YouTube历史进度") or "")
    if f"job={job_id}；" not in summary:
        return None
    common = {"job_id": job_id, "durable": True, "summary": summary}
    if summary.startswith("云端增量完成；"):
        return {**common, "status": "completed", "ok": True}
    if summary.startswith("云端增量跳过；"):
        return {**common, "status": "skipped", "ok": True}
    if summary.startswith("云端增量失败；"):
        return {
            **common,
            "status": "failed",
            "error_type": str(config.get("错误摘要") or "durable_failure"),
        }
    if summary.startswith("云端增量运行中；"):
        return {**common, "status": "running"}
    return None


def durable_job_snapshot_many(
    job_id: str, configs: list[dict[str, Any]]
) -> dict[str, Any] | None:
    """Aggregate per-config summaries for a multi-brand run after restart."""
    snapshots = [
        snapshot
        for config in configs
        if (snapshot := durable_job_snapshot(job_id, config)) is not None
    ]
    if not snapshots:
        return None
    if any(snapshot.get("status") == "failed" for snapshot in snapshots):
        failed = next(snapshot for snapshot in snapshots if snapshot.get("status") == "failed")
        return {
            "job_id": job_id,
            "status": "failed",
            "error_type": failed.get("error_type", "durable_failure"),
            "durable": True,
            "config_count": len(snapshots),
        }
    if any(snapshot.get("status") == "running" for snapshot in snapshots):
        return {
            "job_id": job_id,
            "status": "running",
            "durable": True,
            "config_count": len(snapshots),
        }
    return {
        "job_id": job_id,
        "status": "completed",
        "ok": True,
        "durable": True,
        "config_count": len(snapshots),
        "summaries": [snapshot.get("summary", "") for snapshot in snapshots],
    }

"""Cloud controller for KOL media archive jobs and YouTube metrics."""

from __future__ import annotations

import asyncio
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from hashlib import sha1
import uuid

import httpx

from . import config, feishu, media_archive


WORK_FIELDS = [
    "发布平台", "作品链接", "平台作品ID", "作品名称", "同源作品组",
    "关联KOL", "关联产品", "品牌", "上稿日期", "归档状态",
    "归档文件名", "归档文件链接", "飞书file_token", "允许自动归档",
    "自动处理状态", "归档任务ID", "同源主素材", "重试次数",
    "处理终端", "处理开始时间", "处理完成时间", "失败环节", "失败原因",
    "平台最高分辨率", "视频分辨率", "视频帧率", "视频码率", "视频编码",
    "音频编码", "视频时长(秒)", "文件大小(字节)", "文件SHA256", "清晰度检查",
    "播放量", "点赞量", "评论数", "数据更新时间", "数据抓取状态",
    "下次数据抓取时间", "数据抓取失败原因",
]

BRAND_FOLDER_TOKENS = {
    "FUNLAB": "QpRbfffaclLjOkd2FNDc18QOnvb",
    "POWKONG": "AYvPfcGhLlews5dClAjcUkQ8nMb",
    "白牌": "MqpCfOXPhl2U20dDAdWc6MKcneg",
}

ARCHIVE_RESULT_FIELDS = {
    "归档状态", "自动处理状态", "归档文件名", "归档文件链接", "飞书file_token",
    "平台最高分辨率", "视频分辨率", "视频帧率", "视频码率", "视频编码",
    "音频编码", "视频时长(秒)", "文件大小(字节)", "文件SHA256", "清晰度检查",
    "处理终端", "处理完成时间", "失败环节", "失败原因",
}


def _now_ms(now: datetime | None = None) -> int:
    value = now or datetime.now(timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp() * 1000)


def _run_id(prefix: str) -> str:
    return f"{prefix}-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"


def _archive_job_id(group_key: str) -> str:
    digest = sha1(group_key.encode("utf-8")).hexdigest()[:12]
    return f"archive-{digest}"


async def _update_status(record_id: str, fields: dict) -> None:
    """Write select fields separately from data fields to avoid select clearing."""
    select_names = {"归档状态", "自动处理状态", "失败环节", "数据抓取状态", "清晰度检查"}
    data_fields = {
        key: value for key, value in fields.items()
        if key not in select_names and value is not None
    }
    select_fields = {
        key: value for key, value in fields.items()
        if key in select_names and value is not None
    }
    if data_fields:
        await feishu.update_record(config.T_UPLOAD_WORK, record_id, data_fields)
    for key, value in select_fields.items():
        await feishu.update_record(config.T_UPLOAD_WORK, record_id, {key: value})


async def scan(commit: bool = False, refresh_metrics: bool = True) -> dict:
    records = await feishu.fetch_all_records(config.T_UPLOAD_WORK, field_names=WORK_FIELDS, page_size=200)
    plans = media_archive.plan_archive_groups(records, max_retries=config.MEDIA_ARCHIVE_MAX_RETRIES)
    validations = [media_archive.validate_work_row(record) for record in records]
    invalid_enabled = [
        result for result, record in zip(validations, records)
        if (record.get("fields") or {}).get("允许自动归档") and not result.valid
    ]
    queued = [plan for plan in plans if plan.action == "queue_download"]
    propagated = [plan for plan in plans if plan.action == "propagate_existing"]

    if commit:
        for result in invalid_enabled:
            await _update_status(result.record_id, {
                "自动处理状态": "失败",
                "失败环节": "格式检查",
                "失败原因": "；".join(result.errors),
            })

        by_id = {str(record.get("record_id") or ""): record for record in records}
        for plan in queued:
            job_id = _archive_job_id(plan.group_key)
            await _update_status(plan.master_record_id, {
                "归档任务ID": job_id,
                "同源主素材": True,
                "自动处理状态": "待执行",
                "归档状态": "待下载",
                "失败环节": None,
                "失败原因": "",
            })
            for follower_id in plan.follower_record_ids:
                await _update_status(follower_id, {
                    "归档任务ID": job_id,
                    "同源主素材": False,
                    "自动处理状态": "等待同源母版",
                })

        copy_fields = [
            "归档文件名", "归档文件链接", "飞书file_token", "平台最高分辨率",
            "视频分辨率", "视频帧率", "视频码率", "视频编码", "音频编码",
            "视频时长(秒)", "文件大小(字节)", "文件SHA256", "清晰度检查",
            "处理终端", "处理完成时间",
        ]
        for plan in propagated:
            master_fields = (by_id.get(plan.master_record_id) or {}).get("fields") or {}
            payload = {name: master_fields[name] for name in copy_fields if master_fields.get(name) not in (None, "")}
            payload.update({"归档状态": "已归档", "自动处理状态": "已完成"})
            for follower_id in plan.follower_record_ids:
                follower_fields = (by_id.get(follower_id) or {}).get("fields") or {}
                if media_archive.field_text(follower_fields.get("归档状态")) != "已归档":
                    await _update_status(follower_id, payload)

    metrics_result = None
    if refresh_metrics:
        metrics_result = await refresh_youtube_metrics(commit=commit)

    return {
        "run_id": _run_id("media-archive-scan"),
        "commit": commit,
        "records": len(records),
        "queued_groups": len(queued),
        "propagate_groups": len(propagated),
        "invalid_enabled": len(invalid_enabled),
        "plans": [asdict(plan) for plan in plans],
        "invalid_samples": [asdict(result) for result in invalid_enabled[:10]],
        "metrics": metrics_result,
    }


async def fetch_youtube_videos(video_ids: list[str]) -> dict[str, dict]:
    if not config.YOUTUBE_DATA_API_KEY:
        raise RuntimeError("YOUTUBE_DATA_API_KEY is not configured")
    found: dict[str, dict] = {}
    async with httpx.AsyncClient(timeout=30.0) as client:
        for start in range(0, len(video_ids), 50):
            chunk = video_ids[start:start + 50]
            response = await client.get(
                "https://www.googleapis.com/youtube/v3/videos",
                params={
                    "part": "snippet,statistics,status",
                    "id": ",".join(chunk),
                    "key": config.YOUTUBE_DATA_API_KEY,
                },
            )
            response.raise_for_status()
            for item in (response.json().get("items") or []):
                found[str(item.get("id") or "")] = item
    return found


def _snapshot_days_by_work(records: list[dict]) -> dict[str, set[int]]:
    result: dict[str, set[int]] = {}
    for record in records:
        fields = record.get("fields") or {}
        linked = media_archive.linked_record_ids(fields.get("关联作品"))
        try:
            day = int(float(media_archive.field_text(fields.get("上稿后天数")) or 0))
        except ValueError:
            continue
        for record_id in linked:
            result.setdefault(record_id, set()).add(day)
    return result


async def refresh_youtube_metrics(commit: bool = False,
                                  now: datetime | None = None,
                                  record_id: str = "") -> dict:
    now_dt = now or datetime.now(timezone.utc)
    now_ms = _now_ms(now_dt)
    target_record_id = record_id
    records = await feishu.fetch_all_records(config.T_UPLOAD_WORK, field_names=WORK_FIELDS, page_size=200)
    snapshots = []
    if config.T_MEDIA_ARCHIVE_SNAPSHOT:
        snapshots = await feishu.fetch_all_records(
            config.T_MEDIA_ARCHIVE_SNAPSHOT,
            field_names=["关联作品", "上稿后天数"],
            page_size=200,
        )
    captured = _snapshot_days_by_work(snapshots)

    youtube_rows: list[tuple[dict, str]] = []
    for record in records:
        if target_record_id and str(record.get("record_id") or "") != target_record_id:
            continue
        fields = record.get("fields") or {}
        if media_archive.field_text(fields.get("发布平台")) != "YouTube":
            continue
        video_id = media_archive.field_text(fields.get("平台作品ID"))
        if not video_id:
            validation = media_archive.validate_work_row(record)
            video_id = validation.platform_id
        if video_id:
            youtube_rows.append((record, video_id))

    videos = await fetch_youtube_videos(list(dict.fromkeys(video_id for _, video_id in youtube_rows))) if youtube_rows else {}
    updated = 0
    created = 0
    missing = 0
    errors: list[dict] = []
    run_id = _run_id("youtube-metrics")
    batch_mode = commit and not target_record_id
    data_updates: list[dict] = []
    status_updates: list[dict] = []
    snapshot_rows: list[dict] = []
    for record, video_id in youtube_rows:
        record_id = str(record.get("record_id") or "")
        item = videos.get(video_id)
        if not item:
            missing += 1
            if commit:
                reason = "YouTube未返回该视频；可能已删除、设为私密或ID无效"
                if batch_mode:
                    data_updates.append({
                        "record_id": record_id,
                        "fields": {"数据抓取失败原因": reason},
                    })
                    status_updates.append({
                        "record_id": record_id,
                        "fields": {"数据抓取状态": "抓取失败"},
                    })
                else:
                    await _update_status(record_id, {
                        "数据抓取状态": "抓取失败",
                        "数据抓取失败原因": reason,
                    })
            continue
        mapped = media_archive.map_youtube_video(item)
        latest = {key: value for key, value in mapped.items() if value is not None}
        latest.update({
            "数据更新时间": now_ms,
            "数据抓取失败原因": "",
        })

        published_ms = mapped.get("上稿日期")
        due_day = None
        next_at_ms = None
        if published_ms:
            published_dt = datetime.fromtimestamp(published_ms / 1000, tz=timezone.utc)
            due_day = media_archive.due_metric_milestone(
                published_dt, now_dt, captured.get(record_id, set()),
            )
            captured_after = set(captured.get(record_id, set()))
            if due_day is not None:
                captured_after.add(due_day)
            next_day = media_archive.next_future_metric_milestone(
                published_dt, now_dt, captured_after,
            )
            if next_day is not None:
                next_at_ms = int((published_dt + timedelta(days=next_day)).timestamp() * 1000)
        if next_at_ms:
            latest["下次数据抓取时间"] = next_at_ms

        if commit:
            if batch_mode:
                data_updates.append({"record_id": record_id, "fields": latest})
                status_updates.append({
                    "record_id": record_id,
                    "fields": {"数据抓取状态": "已更新"},
                })
            else:
                await feishu.update_record(config.T_UPLOAD_WORK, record_id, latest)
                await _update_status(record_id, {"数据抓取状态": "已更新"})
            if due_day is not None and config.T_MEDIA_ARCHIVE_SNAPSHOT:
                snapshot_fields = {
                    "快照名称": f"{video_id}-D{due_day}-{now_dt:%Y%m%d}",
                    "关联作品": [record_id],
                    "平台作品ID": video_id,
                    "采集时间": now_ms,
                    "上稿后天数": due_day,
                    "播放量": mapped.get("播放量"),
                    "点赞量": mapped.get("点赞量"),
                    "评论数": mapped.get("评论数"),
                    "数据源": "YouTube Data API",
                    "运行ID": run_id,
                }
                snapshot_fields = {key: value for key, value in snapshot_fields.items() if value is not None}
                if batch_mode:
                    snapshot_rows.append(snapshot_fields)
                else:
                    await feishu.create_record(config.T_MEDIA_ARCHIVE_SNAPSHOT, snapshot_fields)
                    created += 1
        updated += 1

    if batch_mode:
        if data_updates:
            await feishu.batch_update_records(config.T_UPLOAD_WORK, data_updates)
        if status_updates:
            await feishu.batch_update_records(config.T_UPLOAD_WORK, status_updates)
        if snapshot_rows:
            await feishu.batch_create_records(config.T_MEDIA_ARCHIVE_SNAPSHOT, snapshot_rows)
            created += len(snapshot_rows)

    return {
        "run_id": run_id,
        "commit": commit,
        "youtube_rows": len(youtube_rows),
        "updated": updated,
        "snapshots_created": created,
        "missing": missing,
        "errors": errors,
    }


def _same_relation(record: dict, kol_ids: tuple[str, ...], product_ids: tuple[str, ...]) -> bool:
    fields = record.get("fields") or {}
    return (
        frozenset(media_archive.linked_record_ids(fields.get("关联KOL"))) == frozenset(kol_ids)
        and frozenset(media_archive.linked_record_ids(fields.get("关联产品"))) == frozenset(product_ids)
    )


def _next_sequence(records: list[dict], kol_ids: tuple[str, ...],
                   product_ids: tuple[str, ...]) -> int:
    maximum = 0
    for record in records:
        if not _same_relation(record, kol_ids, product_ids):
            continue
        filename = media_archive.field_text((record.get("fields") or {}).get("归档文件名"))
        match = __import__("re").search(r"-(\d+)\.[^.]+$", filename)
        if match:
            maximum = max(maximum, int(match.group(1)))
    return maximum + 1


async def claim_job(worker_id: str, commit: bool = True,
                    record_id: str = "") -> dict | None:
    records = await feishu.fetch_all_records(config.T_UPLOAD_WORK, field_names=WORK_FIELDS, page_size=200)
    candidates = []
    for record in records:
        fields = record.get("fields") or {}
        if record_id and str(record.get("record_id") or "") != record_id:
            continue
        if not fields.get("允许自动归档"):
            continue
        if media_archive.field_text(fields.get("自动处理状态")) != "待执行":
            continue
        validation = media_archive.validate_work_row(record)
        if validation.valid:
            candidates.append((record, validation))
    if not candidates:
        return None
    record, validation = sorted(candidates, key=lambda pair: str(pair[0].get("record_id") or ""))[0]
    fields = record.get("fields") or {}
    target_id = str(record.get("record_id") or "")
    kol_ids = media_archive.linked_record_ids(fields.get("关联KOL"))
    product_ids = media_archive.linked_record_ids(fields.get("关联产品"))
    kol = await feishu.get_record(config.T_KOL, kol_ids[0])
    product = await feishu.get_record(config.T_PRODUCT, product_ids[0])
    kol_fields = kol.get("fields") or {}
    product_fields = product.get("fields") or {}
    kol_name = media_archive.field_text(
        kol_fields.get("账号名") or kol_fields.get("KOL") or kol_fields.get("媒体人")
    )
    product_name = media_archive.field_text(
        product_fields.get("素材归档名") or product_fields.get("SKU") or product_fields.get("产品名")
    )
    filename = media_archive.field_text(fields.get("归档文件名"))
    if not filename:
        filename = media_archive.build_archive_filename(
            validation.platform,
            kol_name,
            product_name,
            _next_sequence(records, kol_ids, product_ids),
            "mp4",
        )
    brand = media_archive.field_text(fields.get("品牌"))
    brand_folder_token = BRAND_FOLDER_TOKENS.get(brand)
    if not brand_folder_token:
        raise RuntimeError(f"unsupported archive brand: {brand}")
    retry_count = int(float(media_archive.field_text(fields.get("重试次数")) or 0)) + 1
    started_at_ms = _now_ms()
    job_id = media_archive.field_text(fields.get("归档任务ID")) or _archive_job_id(
        media_archive.field_text(fields.get("同源作品组")) or target_id
    )
    if commit:
        await _update_status(target_id, {
            "自动处理状态": "处理中",
            "处理终端": worker_id,
            "处理开始时间": started_at_ms,
            "重试次数": retry_count,
            "归档任务ID": job_id,
            "归档文件名": filename,
            "失败原因": "",
        })
    return {
        "job_id": job_id,
        "record_id": target_id,
        "source_group": media_archive.field_text(fields.get("同源作品组")) or target_id,
        "platform": validation.platform,
        "platform_id": validation.platform_id,
        "url": validation.url,
        "filename": filename,
        "brand": brand,
        "brand_folder_token": brand_folder_token,
        "product_folder_name": product_name,
        "retry_count": retry_count,
        "worker_id": worker_id,
    }


async def complete_job(record_id: str, source_group: str,
                       job_id: str, result_fields: dict,
                       commit: bool = True) -> dict:
    unexpected = sorted(set(result_fields) - ARCHIVE_RESULT_FIELDS)
    if unexpected:
        raise ValueError(f"unexpected result fields: {', '.join(unexpected)}")
    if not str(result_fields.get("飞书file_token") or ""):
        raise ValueError("missing 飞书file_token")
    link = media_archive.field_text(result_fields.get("归档文件链接"))
    if "/file/" not in link:
        raise ValueError("归档文件链接必须是具体/file/直链")
    records = await feishu.fetch_all_records(config.T_UPLOAD_WORK, field_names=WORK_FIELDS, page_size=200)
    anchor = next(
        (record for record in records if str(record.get("record_id") or "") == record_id),
        None,
    )
    if not anchor:
        raise ValueError("archive anchor record does not exist")
    anchor_fields = anchor.get("fields") or {}
    expected_job_id = media_archive.field_text(anchor_fields.get("归档任务ID"))
    if not expected_job_id or expected_job_id != job_id:
        raise ValueError("job_id does not match the active archive job")
    if media_archive.field_text(anchor_fields.get("自动处理状态")) != "处理中":
        raise ValueError("archive anchor is not processing")
    anchor_group = media_archive.field_text(anchor_fields.get("同源作品组")) or record_id
    if anchor_group != source_group:
        raise ValueError("source_group does not match archive anchor")
    anchor_kol_ids = media_archive.linked_record_ids(anchor_fields.get("关联KOL"))
    anchor_product_ids = media_archive.linked_record_ids(anchor_fields.get("关联产品"))
    targets = [
        record for record in records
        if (media_archive.field_text((record.get("fields") or {}).get("同源作品组")) or str(record.get("record_id") or "")) == source_group
        and _same_relation(record, anchor_kol_ids, anchor_product_ids)
    ]
    if not targets:
        targets = [record for record in records if str(record.get("record_id") or "") == record_id]
    status_fields = {
        "归档状态": result_fields.get("归档状态") or "已归档",
        "自动处理状态": result_fields.get("自动处理状态") or "已完成",
    }
    if commit:
        for target in targets:
            target_id = str(target.get("record_id") or "")
            await _update_status(target_id, {**result_fields, **status_fields})
    return {"commit": commit, "updated_records": len(targets), "record_ids": [t.get("record_id") for t in targets]}


async def fail_job(record_id: str, job_id: str, stage: str, error: str,
                   commit: bool = True) -> dict:
    record = await feishu.get_record(config.T_UPLOAD_WORK, record_id)
    fields = record.get("fields") or {}
    if media_archive.field_text(fields.get("归档任务ID")) != job_id:
        raise ValueError("job_id does not match the active archive job")
    if media_archive.field_text(fields.get("自动处理状态")) != "处理中":
        raise ValueError("archive anchor is not processing")
    retries = int(float(media_archive.field_text(fields.get("重试次数")) or 0))
    exhausted = retries >= config.MEDIA_ARCHIVE_MAX_RETRIES
    payload = {
        "失败环节": stage,
        "失败原因": str(error)[:2000],
        "自动处理状态": "失败" if exhausted else "待执行",
        "归档状态": "下载失败" if exhausted else "待下载",
    }
    if commit:
        await _update_status(record_id, payload)
    return {"commit": commit, "record_id": record_id, "retries": retries, "exhausted": exhausted}


async def replay_job(record_id: str, commit: bool = False) -> dict:
    record = await feishu.get_record(config.T_UPLOAD_WORK, record_id)
    validation = media_archive.validate_work_row(record)
    result = {
        "commit": commit,
        "record_id": record_id,
        "valid": validation.valid,
        "platform": validation.platform,
        "platform_id": validation.platform_id,
        "errors": list(validation.errors),
    }
    if commit:
        if not validation.valid:
            raise ValueError("；".join(validation.errors))
        await _update_status(record_id, {
            "允许自动归档": True,
            "自动处理状态": "待执行",
            "归档状态": "待下载",
            "失败环节": None,
            "失败原因": "",
        })
    return result


async def heartbeat(worker_id: str, version: str = "", host: str = "",
                    status: str = "在线",
                    queue_scanned: int | None = None, claimed: int | None = None,
                    succeeded: int | None = None, failed: int | None = None,
                    last_error: str | None = None,
                    last_record_id: str | None = None,
                    commit: bool = True) -> dict:
    fields = {
        "Worker ID": worker_id,
        "最后心跳": _now_ms(),
    }
    optional_fields = {
        "版本": version or None,
        "主机": host or None,
        "扫描数": queue_scanned,
        "领取数": claimed,
        "成功数": succeeded,
        "失败数": failed,
        "最后错误": str(last_error)[:2000] if last_error is not None else None,
        "最后记录ID": last_record_id,
    }
    fields.update({key: value for key, value in optional_fields.items() if value is not None})
    if not config.T_MEDIA_ARCHIVE_WORKER:
        return {"commit": False, "persisted": False, "reason": "worker table not configured"}
    records = await feishu.fetch_all_records(
        config.T_MEDIA_ARCHIVE_WORKER,
        field_names=["Worker ID"],
        page_size=200,
    )
    existing = next(
        (record for record in records
         if media_archive.field_text((record.get("fields") or {}).get("Worker ID")) == worker_id),
        None,
    )
    if commit:
        if existing:
            await feishu.update_record(config.T_MEDIA_ARCHIVE_WORKER, existing["record_id"], fields)
            record_id = existing["record_id"]
        else:
            record_id = await feishu.create_record(config.T_MEDIA_ARCHIVE_WORKER, fields)
        await feishu.update_record(
            config.T_MEDIA_ARCHIVE_WORKER, record_id, {"状态": status},
        )
    else:
        record_id = (existing or {}).get("record_id")
    return {"commit": commit, "persisted": commit, "record_id": record_id, "worker_id": worker_id}


async def audit_worker_health(now: datetime | None = None,
                              stale_minutes: int = 10) -> dict:
    """Audit local-worker liveness from the cloud-side tables.

    An offline terminal is actionable only when archive work is waiting or in
    progress.  This keeps the dead-man check outside the PC it supervises and
    avoids alerting Frankie when the queue is intentionally empty.
    """
    now_ms = _now_ms(now)
    stale_after_ms = max(1, int(stale_minutes)) * 60 * 1000
    works = await feishu.fetch_all_records(
        config.T_UPLOAD_WORK,
        field_names=[
            "允许自动归档", "自动处理状态", "归档任务ID", "处理开始时间",
        ],
        page_size=200,
    )
    workers = await feishu.fetch_all_records(
        config.T_MEDIA_ARCHIVE_WORKER,
        field_names=["Worker ID", "最后心跳", "状态", "版本", "主机", "最后错误", "最后记录ID"],
        page_size=200,
    ) if config.T_MEDIA_ARCHIVE_WORKER else []

    active_states = {"待执行", "等待同源母版", "处理中"}
    active = [
        record for record in works
        if (record.get("fields") or {}).get("允许自动归档")
        and media_archive.field_text((record.get("fields") or {}).get("自动处理状态")) in active_states
    ]
    worker_summaries = []
    fresh_workers = 0
    for record in workers:
        fields = record.get("fields") or {}
        try:
            heartbeat_ms = int(float(media_archive.field_text(fields.get("最后心跳")) or 0))
        except ValueError:
            heartbeat_ms = 0
        age_ms = max(0, now_ms - heartbeat_ms) if heartbeat_ms else None
        fresh = (
            age_ms is not None
            and age_ms <= stale_after_ms
            and media_archive.field_text(fields.get("状态")) in {"在线", "忙碌"}
        )
        fresh_workers += int(fresh)
        worker_summaries.append({
            "worker_id": media_archive.field_text(fields.get("Worker ID")),
            "status": media_archive.field_text(fields.get("状态")),
            "heartbeat_age_seconds": round(age_ms / 1000) if age_ms is not None else None,
            "fresh": fresh,
            "version": media_archive.field_text(fields.get("版本")),
            "host": media_archive.field_text(fields.get("主机")),
            "last_error": media_archive.field_text(fields.get("最后错误")),
            "last_record_id": media_archive.field_text(fields.get("最后记录ID")),
        })

    issues = []
    if active and fresh_workers == 0:
        issues.append({
            "code": "NO_FRESH_WORKER",
            "level": "P1",
            "message": f"有{len(active)}条素材任务等待或处理中，但没有{stale_minutes}分钟内在线的归档终端",
        })
    return {
        "run_id": _run_id("media-archive-worker-audit"),
        "ok": not issues,
        "active_queue": len(active),
        "fresh_workers": fresh_workers,
        "workers": worker_summaries,
        "issues": issues,
    }

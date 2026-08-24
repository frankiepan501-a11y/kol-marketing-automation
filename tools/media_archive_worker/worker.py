"""Deterministic local worker primitives for KOL media archiving."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
import json
from pathlib import Path
import re
import time


@dataclass(frozen=True)
class QualityReport:
    valid: bool
    result: str
    width: int
    height: int
    fps: float
    video_codec: str
    audio_codec: str
    duration_seconds: float
    bitrate: int
    file_size: int
    errors: tuple[str, ...]


def build_ytdlp_command(executable: str, url: str, output_template: Path,
                        proxy: str = "", cookies_browser: str = "",
                        cookies_file: str = "", video_format_id: str = "",
                        audio_format_id: str = "") -> list[str]:
    if video_format_id and audio_format_id:
        format_selector = f"{video_format_id}+{audio_format_id}/{video_format_id}/best"
    elif video_format_id:
        format_selector = f"{video_format_id}/best"
    else:
        format_selector = "bestvideo*+bestaudio/best"
    command = [
        executable,
        "--no-playlist",
        "--newline",
        "--write-info-json",
        "--no-clean-info-json",
        "-f", format_selector,
        "--merge-output-format", "mp4",
        "--output", str(output_template),
    ]
    if proxy:
        command.extend(["--proxy", proxy])
    if cookies_file:
        command.extend(["--cookies", cookies_file])
    elif cookies_browser:
        command.extend(["--cookies-from-browser", cookies_browser])
    command.append(url)
    return command


def _to_int(value) -> int:
    try:
        return int(float(value or 0))
    except (TypeError, ValueError):
        return 0


def _to_float(value) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _fps(value) -> float:
    text = str(value or "0")
    if "/" in text:
        numerator, denominator = text.split("/", 1)
        denominator_f = _to_float(denominator)
        return _to_float(numerator) / denominator_f if denominator_f else 0.0
    return _to_float(text)


def assess_quality(probe: dict, expected_width: int = 0,
                   expected_height: int = 0,
                   expected_fps: float = 0.0) -> QualityReport:
    streams = probe.get("streams") or []
    video = next((stream for stream in streams if stream.get("codec_type") == "video"), None)
    audio = next((stream for stream in streams if stream.get("codec_type") == "audio"), None)
    fmt = probe.get("format") or {}
    errors: list[str] = []
    if not video:
        errors.append("缺少视频轨")
    if not audio:
        errors.append("缺少音频轨")

    width = _to_int((video or {}).get("width"))
    height = _to_int((video or {}).get("height"))
    duration = _to_float(fmt.get("duration") or (video or {}).get("duration"))
    file_size = _to_int(fmt.get("size"))
    if width <= 0 or height <= 0:
        errors.append("无法读取视频分辨率")
    if duration <= 0:
        errors.append("无法读取视频时长")
    if file_size <= 0:
        errors.append("无法读取文件大小")

    valid = not errors
    actual_fps = _fps((video or {}).get("avg_frame_rate") or (video or {}).get("r_frame_rate"))
    below_expected = (
        valid and expected_width > 0 and expected_height > 0
        and (width < expected_width or height < expected_height)
    ) or (valid and expected_fps > 0 and actual_fps + 0.5 < expected_fps)
    result = "检查失败" if not valid else ("低于平台最高档" if below_expected else "最高可用画质")
    bitrate = _to_int((video or {}).get("bit_rate") or fmt.get("bit_rate"))

    return QualityReport(
        valid=valid,
        result=result,
        width=width,
        height=height,
        fps=actual_fps,
        video_codec=str((video or {}).get("codec_name") or ""),
        audio_codec=str((audio or {}).get("codec_name") or ""),
        duration_seconds=duration,
        bitrate=bitrate,
        file_size=file_size,
        errors=tuple(errors),
    )


def file_sha256(path: Path, chunk_size: int = 4 * 1024 * 1024) -> str:
    digest = sha256()
    with Path(path).open("rb") as stream:
        while chunk := stream.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest().upper()


def build_success_fields(media_file: Path, report: QualityReport,
                         expected_width: int, expected_height: int,
                         file_token: str, worker_id: str,
                         completed_at_ms: int) -> dict:
    path = Path(media_file)
    file_url = f"https://u1wpma3xuhr.feishu.cn/file/{file_token}"
    return {
        "归档状态": "已归档",
        "自动处理状态": "已完成",
        "归档文件名": path.name,
        "归档文件链接": {"link": file_url, "text": path.name},
        "飞书file_token": file_token,
        "平台最高分辨率": f"{int(expected_width)}×{int(expected_height)}",
        "视频分辨率": f"{report.width}×{report.height}",
        "视频帧率": round(report.fps, 3),
        "视频码率": report.bitrate,
        "视频编码": report.video_codec,
        "音频编码": report.audio_codec,
        "视频时长(秒)": round(report.duration_seconds, 3),
        "文件大小(字节)": report.file_size,
        "文件SHA256": file_sha256(path),
        "清晰度检查": report.result,
        "处理终端": worker_id,
        "处理完成时间": completed_at_ms,
        "失败环节": None,
        "失败原因": "",
    }


async def process_job(job: dict, work_root: Path, toolchain, drive, controller,
                      completed_at_ms: int | None = None) -> dict:
    """Run one public archive job through the five user-visible stages.

    Adapters are injected so tests exercise the workflow boundary without
    coupling to subprocess or network internals.
    """
    job_dir = Path(work_root) / str(job.get("job_id") or job.get("record_id") or "unknown")
    qa_dir = job_dir / "qa"
    job_dir.mkdir(parents=True, exist_ok=True)
    qa_dir.mkdir(parents=True, exist_ok=True)
    stage = "格式检查"
    try:
        format_info = await toolchain.inspect_formats(job, job_dir)
        expected_width = int(format_info.get("expected_width") or 0)
        expected_height = int(format_info.get("expected_height") or 0)
        expected_fps = float(format_info.get("expected_fps") or 0)

        stage = "高画质下载"
        media_file = Path(await toolchain.download(job, job_dir))

        stage = "清晰度检查"
        report = await toolchain.inspect_file(
            media_file, expected_width, expected_height, expected_fps, qa_dir,
        )
        if not report.valid:
            raise RuntimeError("；".join(report.errors) or "media quality inspection failed")

        stage = "飞书上传"
        folder_token = await drive.ensure_product_folder(
            job["brand_folder_token"], job["product_folder_name"],
        )

        stage = "飞书上传"
        receipt_path = job_dir / "upload-receipt.json"
        media_sha256 = file_sha256(media_file)
        receipt = {}
        if receipt_path.exists():
            try:
                receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                receipt = {}
        if (
            receipt.get("filename") == job["filename"]
            and receipt.get("sha256") == media_sha256
            and receipt.get("file_token")
        ):
            file_token = str(receipt["file_token"])
        else:
            file_token = await drive.upload(media_file, folder_token, job["filename"])
            if file_token:
                receipt_path.write_text(json.dumps({
                    "filename": job["filename"],
                    "sha256": media_sha256,
                    "file_token": file_token,
                }, ensure_ascii=False, indent=2), encoding="utf-8")
        if not file_token:
            raise RuntimeError("Feishu upload returned no file_token")

        stage = "表格回填"
        result_fields = build_success_fields(
            media_file=media_file,
            report=report,
            expected_width=expected_width,
            expected_height=expected_height,
            file_token=file_token,
            worker_id=str(job.get("worker_id") or ""),
            completed_at_ms=completed_at_ms or int(time.time() * 1000),
        )
        await controller.complete(job, result_fields)
        return {
            "ok": True,
            "job_id": job.get("job_id"),
            "record_id": job.get("record_id"),
            "file_token": file_token,
            "result_fields": result_fields,
        }
    except Exception as exc:
        await controller.fail(job, stage, str(exc))
        return {
            "ok": False,
            "job_id": job.get("job_id"),
            "record_id": job.get("record_id"),
            "failed_stage": stage,
            "error": str(exc),
        }

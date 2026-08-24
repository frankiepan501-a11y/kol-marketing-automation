"""Local runtime adapters for the KOL media archive worker.

The cloud controller owns queue state and Base writes.  This module stays on a
Windows worker because TikTok/Instagram access and multi-GB media downloads are
more reliable from the office network.  It never stores credentials in code.
"""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import socket
import sys
from typing import Any

import httpx

from . import worker


WORKER_VERSION = "0.2.4"


def parse_cli_json(text: str) -> dict:
    """Extract the complete JSON response even when a CLI prints progress first."""
    decoder = json.JSONDecoder()
    values: list[tuple[int, dict]] = []
    for index, character in enumerate(text or ""):
        if character != "{":
            continue
        try:
            value, end = decoder.raw_decode(text[index:])
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            values.append((end, value))
    if not values:
        raise RuntimeError(f"command returned no JSON object: {(text or '')[-500:]}")
    # Nested objects also decode successfully when scanning from every ``{``.
    # The outer CLI envelope is the widest decoded object, not the last one.
    return max(values, key=lambda item: item[0])[1]


def _number(value: Any) -> int:
    try:
        return max(0, int(float(value or 0)))
    except (TypeError, ValueError):
        return 0


def summarize_available_formats(metadata: dict) -> dict:
    """Summarize the platform's largest visible video track without setting a floor."""
    formats = metadata.get("formats") or []
    all_video_formats = [
        item for item in formats
        if str(item.get("vcodec") or "none").lower() != "none"
        and _number(item.get("width")) > 0 and _number(item.get("height")) > 0
    ]
    video_only_formats = [
        item for item in all_video_formats
        if str(item.get("acodec") or "none").lower() == "none"
    ]
    video_formats = video_only_formats or all_video_formats
    best_video = max(
        video_formats,
        key=lambda item: (
            _number(item.get("width")) * _number(item.get("height")),
            _number(item.get("fps")),
            _number(item.get("tbr")),
        ),
        default={},
    )
    audio_formats = [
        item for item in formats
        if str(item.get("acodec") or "none").lower() != "none"
        and str(item.get("vcodec") or "none").lower() == "none"
    ] if str(best_video.get("acodec") or "none").lower() == "none" else []
    best_audio = max(
        audio_formats,
        key=lambda item: (
            _number(item.get("abr")),
            _number(item.get("asr")),
            _number(item.get("tbr")),
            _number(item.get("filesize") or item.get("filesize_approx")),
        ),
        default={},
    )
    video_bytes = _number(best_video.get("filesize") or best_video.get("filesize_approx"))
    audio_bytes = _number(best_audio.get("filesize") or best_audio.get("filesize_approx"))
    return {
        "expected_width": _number(best_video.get("width") or metadata.get("width")),
        "expected_height": _number(best_video.get("height") or metadata.get("height")),
        "expected_fps": best_video.get("fps") or metadata.get("fps") or 0,
        "video_format_id": str(best_video.get("format_id") or ""),
        "audio_format_id": str(best_audio.get("format_id") or ""),
        "estimated_download_bytes": video_bytes + audio_bytes,
        "platform_title": str(metadata.get("title") or ""),
        "extractor": str(metadata.get("extractor_key") or metadata.get("extractor") or ""),
    }


async def _run_process(args: list[str], cwd: Path | None = None) -> str:
    process = await asyncio.create_subprocess_exec(
        *args,
        cwd=str(cwd) if cwd else None,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    out_text = stdout.decode("utf-8", errors="replace")
    err_text = stderr.decode("utf-8", errors="replace")
    if process.returncode:
        detail = (err_text or out_text)[-4000:]
        raise RuntimeError(f"command failed ({process.returncode}): {detail}")
    return out_text


class SubprocessToolchain:
    def __init__(self, ytdlp: str = "yt-dlp", ffprobe: str = "ffprobe",
                 ffmpeg: str = "ffmpeg", proxy: str = "",
                 cookies_browser: str = "", cookies_file: str = "",
                 js_runtime: str = ""):
        self.ytdlp = ytdlp
        self.ffprobe = ffprobe
        self.ffmpeg = ffmpeg
        self.proxy = proxy
        self.cookies_browser = cookies_browser
        self.cookies_file = cookies_file
        self.js_runtime = js_runtime

    def _access_args(self) -> list[str]:
        args: list[str] = []
        if self.proxy:
            args.extend(["--proxy", self.proxy])
        if self.js_runtime:
            args.extend(["--js-runtimes", self.js_runtime])
        if self.cookies_file:
            args.extend(["--cookies", self.cookies_file])
        elif self.cookies_browser:
            args.extend(["--cookies-from-browser", self.cookies_browser])
        return args

    async def inspect_formats(self, job: dict, job_dir: Path) -> dict:
        command = [
            self.ytdlp, "--no-playlist", "--skip-download", "--dump-single-json",
            "--no-warnings", *self._access_args(), str(job["url"]),
        ]
        output = await _run_process(command, cwd=job_dir)
        metadata = parse_cli_json(output)
        summary = summarize_available_formats(metadata)
        if summary["expected_width"] <= 0 or summary["expected_height"] <= 0:
            raise RuntimeError("平台格式清单没有可用的视频分辨率")
        job["_format_info"] = summary
        (job_dir / "format-audit.json").write_text(
            json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8",
        )
        return summary

    async def download(self, job: dict, job_dir: Path) -> Path:
        target = job_dir / str(job["filename"])
        if target.exists() and target.stat().st_size > 0:
            return target
        estimated = _number((job.get("_format_info") or {}).get("estimated_download_bytes"))
        reserve = max(1024 ** 3, int(estimated * 1.3))
        free = shutil.disk_usage(job_dir).free
        if free < reserve:
            raise RuntimeError(f"磁盘空间不足：可用{free}字节，预计至少需要{reserve}字节")
        command = worker.build_ytdlp_command(
            executable=self.ytdlp,
            url=str(job["url"]),
            output_template=target,
            proxy=self.proxy,
            cookies_browser="" if self.cookies_file else self.cookies_browser,
            cookies_file=self.cookies_file,
            video_format_id=str((job.get("_format_info") or {}).get("video_format_id") or ""),
            audio_format_id=str((job.get("_format_info") or {}).get("audio_format_id") or ""),
            ffmpeg_location=self.ffmpeg,
            js_runtime=self.js_runtime,
        )
        await _run_process(command, cwd=job_dir)
        if target.exists() and target.stat().st_size > 0:
            return target
        candidates = sorted(job_dir.glob(f"{target.stem}.*"), key=lambda path: path.stat().st_size, reverse=True)
        media_candidates = [path for path in candidates if path.suffix.lower() not in {".json", ".part", ".ytdl"}]
        if not media_candidates:
            raise RuntimeError("下载命令完成但没有找到媒体文件")
        return media_candidates[0]

    async def _probe(self, media_file: Path) -> dict:
        output = await _run_process([
            self.ffprobe, "-v", "error", "-show_streams", "-show_format",
            "-of", "json", str(media_file),
        ])
        probe = parse_cli_json(output)
        probe.setdefault("format", {})["size"] = media_file.stat().st_size
        return probe

    async def _decode_window(self, media_file: Path, start: float | None = None,
                             seconds: float | None = None) -> None:
        command = [self.ffmpeg, "-v", "error"]
        if start is not None:
            command.extend(["-ss", f"{max(0.0, start):.3f}"])
        command.extend(["-i", str(media_file), "-map", "0:v:0"])
        if seconds is not None:
            command.extend(["-t", f"{seconds:.3f}"])
        command.extend(["-f", "null", "-"])
        await _run_process(command)

    async def _write_qa_frames(self, media_file: Path, duration: float,
                               qa_dir: Path) -> None:
        positions = [0.0, max(0.0, duration / 2), max(0.0, duration - 0.5)]
        labels = ["start", "middle", "end"]
        for label, position in zip(labels, positions):
            destination = qa_dir / f"{label}.jpg"
            await _run_process([
                self.ffmpeg, "-v", "error", "-ss", f"{position:.3f}",
                "-i", str(media_file), "-frames:v", "1", "-q:v", "2",
                "-y", str(destination),
            ])
            if not destination.exists() or destination.stat().st_size <= 0:
                raise RuntimeError(f"无法生成{label}清晰度检查帧")

    async def inspect_file(self, media_file: Path, expected_width: int,
                           expected_height: int, expected_fps: float,
                           qa_dir: Path) -> worker.QualityReport:
        probe = await self._probe(media_file)
        report = worker.assess_quality(
            probe, expected_width, expected_height, expected_fps,
        )
        if not report.valid:
            return report
        if report.duration_seconds <= 30 * 60:
            await self._decode_window(media_file)
        else:
            for position in (0.0, report.duration_seconds / 2, max(0.0, report.duration_seconds - 30)):
                await self._decode_window(media_file, start=position, seconds=30)
        await self._write_qa_frames(media_file, report.duration_seconds, qa_dir)
        (qa_dir / "ffprobe.json").write_text(
            json.dumps(probe, ensure_ascii=False, indent=2), encoding="utf-8",
        )
        return report


def _find_file_items(value: Any) -> list[dict]:
    if isinstance(value, dict):
        for key in ("files", "items"):
            items = value.get(key)
            if isinstance(items, list) and all(isinstance(item, dict) for item in items):
                return items
        for child in value.values():
            found = _find_file_items(child)
            if found:
                return found
    return []


def _find_token(value: Any) -> str:
    if isinstance(value, dict):
        for key in ("file_token", "folder_token", "token"):
            if value.get(key):
                return str(value[key])
        for child in value.values():
            token = _find_token(child)
            if token:
                return token
    return ""


class LarkCliDrive:
    def __init__(self, executable: str = "lark-cli", profile: str = ""):
        self.executable = executable
        self.profile = profile

    async def _run(self, *args: str, cwd: Path | None = None) -> dict:
        command = [self.executable, *args, "--as", "bot", "--format", "json"]
        if self.profile:
            command.extend(["--profile", self.profile])
        return parse_cli_json(await _run_process(command, cwd=cwd))

    async def _list(self, folder_token: str) -> list[dict]:
        result = await self._run(
            "drive", "files", "list", "--folder-token", folder_token,
            "--page-all", "--page-size", "200",
        )
        return _find_file_items(result)

    async def ensure_product_folder(self, brand_folder_token: str,
                                    product_folder_name: str) -> str:
        for item in await self._list(brand_folder_token):
            if str(item.get("name") or "") == product_folder_name and str(item.get("type") or "") == "folder":
                return _find_token(item)
        result = await self._run(
            "drive", "+create-folder", "--folder-token", brand_folder_token,
            "--name", product_folder_name,
        )
        token = _find_token(result)
        if not token:
            raise RuntimeError("飞书创建产品文件夹后未返回folder_token")
        return token

    async def upload(self, media_file: Path, folder_token: str, filename: str) -> str:
        for item in await self._list(folder_token):
            if str(item.get("name") or "") == filename and str(item.get("type") or "") == "file":
                remote_size = _number(item.get("size") or item.get("file_size"))
                local_size = media_file.stat().st_size
                if remote_size <= 0:
                    raise RuntimeError(f"飞书同名文件缺少大小信息，拒绝盲目复用：{filename}")
                if remote_size != local_size:
                    raise RuntimeError(
                        f"飞书同名文件大小不一致，拒绝覆盖或误复用：{filename} "
                        f"(云盘{remote_size}字节，本地{local_size}字节)"
                    )
                return _find_token(item)
        result = await self._run(
            "drive", "+upload", "--file", f"./{media_file.name}",
            "--folder-token", folder_token, "--name", filename,
            cwd=media_file.parent,
        )
        token = _find_token(result)
        if not token:
            raise RuntimeError("飞书上传完成后未返回file_token")
        return token


class ControllerClient:
    def __init__(self, base_url: str, token: str,
                 client: httpx.AsyncClient | None = None):
        if not base_url or not token:
            raise ValueError("MEDIA_ARCHIVE_CONTROLLER_URL and INTERNAL_TOKEN are required")
        self.base_url = base_url.rstrip("/")
        self._owns_client = client is None
        self.client = client or httpx.AsyncClient(
            base_url=self.base_url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=httpx.Timeout(60.0, connect=20.0),
        )

    async def _post(self, path: str, payload: dict) -> dict:
        response = await self.client.post(path, json=payload)
        response.raise_for_status()
        result = response.json()
        if not result.get("ok"):
            raise RuntimeError(str(result))
        return result

    async def claim(self, worker_id: str, record_id: str = "") -> dict | None:
        result = await self._post("/media-archive/worker/claim", {
            "worker_id": worker_id, "record_id": record_id,
        })
        return result.get("job")

    async def complete(self, job: dict, result_fields: dict) -> dict:
        return await self._post("/media-archive/worker/complete", {
            "job_id": job["job_id"],
            "record_id": job["record_id"],
            "source_group": job["source_group"],
            "result_fields": result_fields,
        })

    async def fail(self, job: dict, stage: str, error: str) -> dict:
        return await self._post("/media-archive/worker/fail", {
            "job_id": job["job_id"], "record_id": job["record_id"],
            "stage": stage, "error": error,
        })

    async def heartbeat(self, **payload) -> dict:
        return await self._post("/media-archive/worker/heartbeat", payload)

    async def close(self) -> None:
        if self._owns_client:
            await self.client.aclose()


@dataclass(frozen=True)
class Settings:
    controller_url: str
    internal_token: str
    worker_id: str
    work_root: Path
    poll_seconds: int
    heartbeat_seconds: int
    ytdlp: str
    ffprobe: str
    ffmpeg: str
    lark_cli: str
    lark_profile: str
    proxy: str
    cookies_browser: str
    cookies_file: str
    js_runtime: str

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            controller_url=os.environ.get("MEDIA_ARCHIVE_CONTROLLER_URL", "").strip(),
            internal_token=os.environ.get("INTERNAL_TOKEN", "").strip(),
            worker_id=os.environ.get("MEDIA_ARCHIVE_WORKER_ID", socket.gethostname()).strip(),
            work_root=Path(os.environ.get("MEDIA_ARCHIVE_WORK_ROOT", "D:/kol_media_archive/work")),
            poll_seconds=max(30, int(os.environ.get("MEDIA_ARCHIVE_POLL_SECONDS", "120"))),
            heartbeat_seconds=max(
                30,
                min(300, int(os.environ.get("MEDIA_ARCHIVE_HEARTBEAT_SECONDS", "120"))),
            ),
            ytdlp=os.environ.get("YTDLP_BIN", "yt-dlp").strip(),
            ffprobe=os.environ.get("FFPROBE_BIN", "ffprobe").strip(),
            ffmpeg=os.environ.get("FFMPEG_BIN", "ffmpeg").strip(),
            lark_cli=os.environ.get("LARK_CLI_BIN", "lark-cli").strip(),
            lark_profile=os.environ.get("LARK_CLI_PROFILE", "").strip(),
            proxy=os.environ.get("KOL_SCRAPER_PROXY", "").strip(),
            cookies_browser=os.environ.get("MEDIA_ARCHIVE_COOKIES_BROWSER", "").strip(),
            cookies_file=os.environ.get("MEDIA_ARCHIVE_COOKIES_FILE", "").strip(),
            js_runtime=os.environ.get("YTDLP_JS_RUNTIME", "").strip(),
        )


def _append_log(path: Path, event: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "time": datetime.now(timezone.utc).isoformat(),
        **event,
    }
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(record, ensure_ascii=False) + "\n")


async def busy_heartbeat_loop(controller: ControllerClient, settings: Settings,
                              job: dict, stop_event: asyncio.Event,
                              log_path: Path) -> None:
    """Keep the cloud-side dead-man record fresh during long media jobs."""
    while not stop_event.is_set():
        try:
            await controller.heartbeat(
                worker_id=settings.worker_id,
                version=WORKER_VERSION,
                host=socket.gethostname(),
                status="忙碌",
                last_record_id=str(job.get("record_id") or ""),
            )
        except Exception as exc:
            _append_log(log_path, {
                "ok": False,
                "event": "busy_heartbeat_failed",
                "record_id": str(job.get("record_id") or ""),
                "error": str(exc),
            })
        try:
            await asyncio.wait_for(
                stop_event.wait(), timeout=settings.heartbeat_seconds,
            )
        except asyncio.TimeoutError:
            continue


async def run_once(settings: Settings, record_id: str = "") -> dict:
    settings.work_root.mkdir(parents=True, exist_ok=True)
    log_path = settings.work_root / "logs" / f"worker-{datetime.now():%Y%m%d}.jsonl"
    controller = ControllerClient(settings.controller_url, settings.internal_token)
    toolchain = SubprocessToolchain(
        ytdlp=settings.ytdlp, ffprobe=settings.ffprobe, ffmpeg=settings.ffmpeg,
        proxy=settings.proxy, cookies_browser=settings.cookies_browser,
        cookies_file=settings.cookies_file, js_runtime=settings.js_runtime,
    )
    drive = LarkCliDrive(settings.lark_cli, settings.lark_profile)
    try:
        await controller.heartbeat(
            worker_id=settings.worker_id, version=WORKER_VERSION,
            host=socket.gethostname(), status="在线", queue_scanned=1,
        )
        job = await controller.claim(settings.worker_id, record_id=record_id)
        if not job:
            result = {"ok": True, "idle": True, "worker_id": settings.worker_id}
            _append_log(log_path, result)
            return result
        stop_event = asyncio.Event()
        heartbeat_task = asyncio.create_task(
            busy_heartbeat_loop(controller, settings, job, stop_event, log_path),
        )
        try:
            result = await worker.process_job(
                job=job, work_root=settings.work_root, toolchain=toolchain,
                drive=drive, controller=controller,
            )
        finally:
            stop_event.set()
            await heartbeat_task
        _append_log(log_path, result)
        await controller.heartbeat(
            worker_id=settings.worker_id, version=WORKER_VERSION,
            host=socket.gethostname(), status="在线",
            claimed=1, succeeded=1 if result.get("ok") else 0,
            failed=0 if result.get("ok") else 1,
            last_error=str(result.get("error") or ""),
            last_record_id=str(result.get("record_id") or ""),
        )
        return result
    finally:
        await controller.close()


async def run_daemon(settings: Settings) -> None:
    while True:
        try:
            await run_once(settings)
        except Exception as exc:
            log_path = settings.work_root / "logs" / f"worker-{datetime.now():%Y%m%d}.jsonl"
            _append_log(log_path, {"ok": False, "daemon_error": str(exc)})
        await asyncio.sleep(settings.poll_seconds)


async def probe_environment(settings: Settings) -> dict:
    """Read-only dependency, Drive access and controller health check."""
    versions = {}
    for name, command in {
        "yt_dlp": [settings.ytdlp, "--version"],
        "ffmpeg": [settings.ffmpeg, "-version"],
        "ffprobe": [settings.ffprobe, "-version"],
    }.items():
        output = await _run_process(command)
        versions[name] = (output.splitlines() or [""])[0].strip()
    if settings.js_runtime:
        runtime_name, separator, runtime_path = settings.js_runtime.partition(":")
        executable = runtime_path if separator and runtime_path else runtime_name
        output = await _run_process([executable, "--version"])
        versions["js_runtime"] = (output.splitlines() or [""])[0].strip()

    whoami = parse_cli_json(await _run_process([
        settings.lark_cli, "whoami", "--as", "bot",
        *(["--profile", settings.lark_profile] if settings.lark_profile else []),
    ]))
    drive = LarkCliDrive(settings.lark_cli, settings.lark_profile)
    video_root_items = await drive._list("PoOqfBHf8lq2e8dOawHc98x2nne")
    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.get(f"{settings.controller_url.rstrip('/')}/health")
        response.raise_for_status()
        controller_health = response.json()
    return {
        "ok": True,
        "worker_id": settings.worker_id,
        "host": socket.gethostname(),
        "versions": versions,
        "lark_identity": {
            "identity": whoami.get("identity") or whoami.get("as"),
            "app_id": whoami.get("app_id") or whoami.get("appId"),
            "profile": whoami.get("profile"),
        },
        "video_root_visible_items": len(video_root_items),
        "controller_health": controller_health,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="KOL media archive local worker")
    parser.add_argument("--once", action="store_true", help="claim at most one job and exit")
    parser.add_argument("--probe", action="store_true", help="read-only dependency/access check")
    parser.add_argument("--record-id", default="", help="replay one explicit Base record")
    args = parser.parse_args(argv)
    settings = Settings.from_env()
    if args.probe:
        result = asyncio.run(probe_environment(settings))
        print(json.dumps(result, ensure_ascii=False))
    elif args.once or args.record_id:
        result = asyncio.run(run_once(settings, record_id=args.record_id))
        print(json.dumps(result, ensure_ascii=False))
    else:
        asyncio.run(run_daemon(settings))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

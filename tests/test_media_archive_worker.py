from pathlib import Path
import asyncio

from tools.media_archive_worker import worker


def test_ytdlp_command_requests_best_tracks_without_quality_recompression(tmp_path):
    command = worker.build_ytdlp_command(
        executable="yt-dlp.exe",
        url="https://www.instagram.com/reel/ABC123/",
        output_template=tmp_path / "IG-Creator-Product-01.%(ext)s",
        proxy="http://127.0.0.1:7890",
        cookies_browser="chrome",
    )

    assert command[:2] == ["yt-dlp.exe", "--no-playlist"]
    assert ["-f", "bestvideo*+bestaudio/best"] == command[command.index("-f"):command.index("-f") + 2]
    assert "--merge-output-format" in command
    assert "mp4" in command
    assert command[command.index("--proxy") + 1] == "http://127.0.0.1:7890"
    assert command[command.index("--cookies-from-browser") + 1] == "chrome"
    assert "--recode-video" not in command


def test_ytdlp_command_locks_the_audited_video_and_audio_formats(tmp_path):
    command = worker.build_ytdlp_command(
        executable="yt-dlp.exe",
        url="https://youtu.be/nUkwNTRFJBc",
        output_template=tmp_path / "YT-Creator-Product-01.mp4",
        video_format_id="v1440",
        audio_format_id="a-best",
    )

    assert command[command.index("-f") + 1] == "v1440+a-best/v1440/best"
    assert "--recode-video" not in command


def test_quality_report_accepts_low_resolution_when_it_is_platform_best():
    probe = {
        "format": {"duration": "12.5", "size": "1048576", "bit_rate": "1800000"},
        "streams": [
            {"codec_type": "video", "codec_name": "h264", "width": 720, "height": 1280,
             "avg_frame_rate": "30/1", "bit_rate": "1600000"},
            {"codec_type": "audio", "codec_name": "aac", "bit_rate": "128000"},
        ],
    }

    report = worker.assess_quality(probe, expected_width=720, expected_height=1280)

    assert report.valid is True
    assert report.result == "最高可用画质"
    assert report.width == 720
    assert report.height == 1280
    assert report.errors == ()


def test_quality_report_records_below_platform_max_but_does_not_block_archive():
    probe = {
        "format": {"duration": "12.5", "size": "1048576"},
        "streams": [
            {"codec_type": "video", "codec_name": "h264", "width": 720, "height": 1280,
             "avg_frame_rate": "30000/1001"},
            {"codec_type": "audio", "codec_name": "aac"},
        ],
    }

    report = worker.assess_quality(probe, expected_width=1080, expected_height=1920)

    assert report.valid is True
    assert report.result == "低于平台最高档"
    assert round(report.fps, 2) == 29.97


def test_quality_report_detects_lower_frame_rate_at_the_same_resolution():
    probe = {
        "format": {"duration": "12.5", "size": "1048576"},
        "streams": [
            {"codec_type": "video", "codec_name": "h264", "width": 1080,
             "height": 1920, "avg_frame_rate": "30/1"},
            {"codec_type": "audio", "codec_name": "aac"},
        ],
    }

    report = worker.assess_quality(
        probe, expected_width=1080, expected_height=1920, expected_fps=60,
    )

    assert report.valid is True
    assert report.result == "低于平台最高档"


def test_quality_report_blocks_files_without_audio_track():
    probe = {
        "format": {"duration": "12.5", "size": "1048576"},
        "streams": [
            {"codec_type": "video", "codec_name": "h264", "width": 1080, "height": 1920,
             "avg_frame_rate": "30/1"},
        ],
    }

    report = worker.assess_quality(probe, expected_width=1080, expected_height=1920)

    assert report.valid is False
    assert report.result == "检查失败"
    assert "缺少音频轨" in report.errors


def test_worker_result_fields_include_direct_file_url_and_audit_evidence(tmp_path):
    media_file = tmp_path / "YT-Amrie47-YS11-5-戴夫-01.mp4"
    media_file.write_bytes(b"known media bytes")
    report = worker.QualityReport(
        valid=True,
        result="最高可用画质",
        width=1440,
        height=2560,
        fps=60.0,
        video_codec="av1",
        audio_codec="aac",
        duration_seconds=175.87,
        bitrate=9700000,
        file_size=media_file.stat().st_size,
        errors=(),
    )

    fields = worker.build_success_fields(
        media_file=media_file,
        report=report,
        expected_width=1440,
        expected_height=2560,
        file_token="file-token-123",
        worker_id="old-terminal-grey",
        completed_at_ms=1787583600000,
    )

    assert fields["归档状态"] == "已归档"
    assert fields["自动处理状态"] == "已完成"
    assert fields["归档文件链接"]["link"] == "https://u1wpma3xuhr.feishu.cn/file/file-token-123"
    assert fields["飞书file_token"] == "file-token-123"
    assert fields["视频分辨率"] == "1440×2560"
    assert fields["平台最高分辨率"] == "1440×2560"
    assert fields["文件SHA256"]
    assert fields["处理终端"] == "old-terminal-grey"


def test_process_job_runs_download_check_upload_and_complete(tmp_path):
    class FakeToolchain:
        async def inspect_formats(self, job, job_dir):
            return {"expected_width": 1080, "expected_height": 1920, "expected_fps": 30}

        async def download(self, job, job_dir):
            path = job_dir / job["filename"]
            path.write_bytes(b"downloaded highest quality media")
            return path

        async def inspect_file(self, media_file, expected_width, expected_height, expected_fps, qa_dir):
            return worker.QualityReport(
                valid=True, result="最高可用画质", width=1080, height=1920, fps=30.0,
                video_codec="h264", audio_codec="aac", duration_seconds=10.0,
                bitrate=1000000, file_size=media_file.stat().st_size, errors=(),
            )

    class FakeDrive:
        async def ensure_product_folder(self, brand_folder_token, product_folder_name):
            return "folder-token"

        async def upload(self, media_file, folder_token, filename):
            return "file-token"

    class FakeController:
        def __init__(self):
            self.completed = None
            self.failed = None

        async def complete(self, job, result_fields):
            self.completed = (job, result_fields)
            return {"ok": True}

        async def fail(self, job, stage, error):
            self.failed = (job, stage, error)

    controller = FakeController()
    job = {
        "job_id": "archive-1",
        "record_id": "rec-1",
        "source_group": "source-1",
        "filename": "YT-Creator-Product-01.mp4",
        "url": "https://youtu.be/nUkwNTRFJBc",
        "brand_folder_token": "brand-folder",
        "product_folder_name": "Product",
        "worker_id": "old-terminal-grey",
    }

    result = asyncio.run(worker.process_job(
        job=job,
        work_root=tmp_path,
        toolchain=FakeToolchain(),
        drive=FakeDrive(),
        controller=controller,
        completed_at_ms=1787583600000,
    ))

    assert result["ok"] is True
    assert controller.failed is None
    assert controller.completed[1]["飞书file_token"] == "file-token"
    assert controller.completed[1]["归档文件名"] == "YT-Creator-Product-01.mp4"


def test_process_job_reports_the_exact_failure_stage(tmp_path):
    class FailingToolchain:
        async def inspect_formats(self, job, job_dir):
            raise RuntimeError("login required")

    class FakeController:
        def __init__(self):
            self.failed = None

        async def fail(self, job, stage, error):
            self.failed = (stage, error)

    controller = FakeController()
    result = asyncio.run(worker.process_job(
        job={"job_id": "archive-1", "record_id": "rec-1", "worker_id": "grey"},
        work_root=tmp_path,
        toolchain=FailingToolchain(),
        drive=object(),
        controller=controller,
    ))

    assert result["ok"] is False
    assert controller.failed[0] == "格式检查"
    assert "login required" in controller.failed[1]


def test_process_job_reuses_verified_upload_receipt_after_callback_failure(tmp_path):
    class FakeToolchain:
        async def inspect_formats(self, job, job_dir):
            return {"expected_width": 720, "expected_height": 1280, "expected_fps": 30}

        async def download(self, job, job_dir):
            path = job_dir / job["filename"]
            path.write_bytes(b"same-media-on-retry")
            return path

        async def inspect_file(self, media_file, expected_width, expected_height, expected_fps, qa_dir):
            return worker.QualityReport(
                valid=True, result="最高可用画质", width=720, height=1280, fps=30,
                video_codec="h264", audio_codec="aac", duration_seconds=10,
                bitrate=1000, file_size=media_file.stat().st_size, errors=(),
            )

    class FakeDrive:
        def __init__(self):
            self.uploads = 0

        async def ensure_product_folder(self, brand_folder_token, product_folder_name):
            return "folder-token"

        async def upload(self, media_file, folder_token, filename):
            self.uploads += 1
            return "file-token"

    class FlakyController:
        def __init__(self):
            self.completions = 0

        async def complete(self, job, result_fields):
            self.completions += 1
            if self.completions == 1:
                raise RuntimeError("callback timeout")

        async def fail(self, job, stage, error):
            return None

    job = {
        "job_id": "archive-retry",
        "record_id": "rec-1",
        "source_group": "source-1",
        "filename": "YT-Creator-Product-01.mp4",
        "url": "https://youtu.be/nUkwNTRFJBc",
        "brand_folder_token": "brand-folder",
        "product_folder_name": "Product",
        "worker_id": "old-terminal-grey",
    }
    drive = FakeDrive()
    controller = FlakyController()

    first = asyncio.run(worker.process_job(
        job=dict(job), work_root=tmp_path, toolchain=FakeToolchain(),
        drive=drive, controller=controller,
    ))
    second = asyncio.run(worker.process_job(
        job=dict(job), work_root=tmp_path, toolchain=FakeToolchain(),
        drive=drive, controller=controller,
    ))

    assert first["ok"] is False
    assert first["failed_stage"] == "表格回填"
    assert second["ok"] is True
    assert drive.uploads == 1

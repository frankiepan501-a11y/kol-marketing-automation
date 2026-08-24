import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

from tools.media_archive_worker import runtime


def test_busy_heartbeat_runs_while_a_job_is_processing(tmp_path):
    calls = []
    stop_event = asyncio.Event()

    class FakeController:
        async def heartbeat(self, **payload):
            calls.append(payload)
            stop_event.set()
            return {"ok": True}

    settings = SimpleNamespace(
        worker_id="old-terminal-grey",
        heartbeat_seconds=30,
    )
    job = {"record_id": "rec-1"}

    asyncio.run(runtime.busy_heartbeat_loop(
        FakeController(), settings, job, stop_event,
        tmp_path / "worker.jsonl",
    ))

    assert calls == [{
        "worker_id": "old-terminal-grey",
        "version": runtime.WORKER_VERSION,
        "host": runtime.socket.gethostname(),
        "status": "忙碌",
        "last_record_id": "rec-1",
    }]


def test_best_available_summary_uses_largest_video_and_audio_size():
    metadata = {
        "formats": [
            {"format_id": "a1", "vcodec": "none", "acodec": "opus", "filesize": 2_000_000},
            {"format_id": "v720", "vcodec": "h264", "acodec": "none", "width": 720,
             "height": 1280, "filesize": 20_000_000},
            {"format_id": "v1440", "vcodec": "av1", "acodec": "none", "width": 1440,
             "height": 2560, "filesize_approx": 200_000_000},
        ],
    }

    summary = runtime.summarize_available_formats(metadata)

    assert summary["expected_width"] == 1440
    assert summary["expected_height"] == 2560
    assert summary["estimated_download_bytes"] == 202_000_000
    assert summary["video_format_id"] == "v1440"


def test_best_available_summary_does_not_add_a_second_audio_track_to_combined_format():
    metadata = {
        "formats": [
            {"format_id": "combined", "vcodec": "h264", "acodec": "aac",
             "width": 1080, "height": 1920, "fps": 60, "filesize": 30_000_000},
            {"format_id": "a1", "vcodec": "none", "acodec": "opus", "abr": 128},
        ],
    }

    summary = runtime.summarize_available_formats(metadata)

    assert summary["video_format_id"] == "combined"
    assert summary["audio_format_id"] == ""


def test_parse_cli_json_accepts_a_progress_line_before_json():
    value = runtime.parse_cli_json('uploading file: video.mp4\n{"file_token":"file123"}\n')

    assert value["file_token"] == "file123"


def test_parse_cli_json_keeps_the_outer_response_when_it_contains_nested_objects():
    text = (
        '[page 1] fetching...\n'
        '{"ok":true,"identity":{"type":"bot"},'
        '"data":{"files":[{"name":"FUNLAB","type":"folder","token":"folder-1"}]},'
        '"_notice":{"risk":"read"}}\n'
    )

    value = runtime.parse_cli_json(text)

    assert value["ok"] is True
    assert value["data"]["files"][0]["token"] == "folder-1"


def test_drive_upload_reuses_exact_existing_file_without_second_upload(tmp_path):
    media_file = tmp_path / "YT-KOL-Product-01.mp4"
    media_file.write_bytes(b"video")

    class FakeCliDrive(runtime.LarkCliDrive):
        def __init__(self):
            super().__init__(executable="lark-cli")
            self.commands = []

        async def _run(self, *args):
            self.commands.append(args)
            return {"files": [{
                "name": media_file.name, "token": "existing-file", "type": "file",
                "size": media_file.stat().st_size,
            }]}

    drive = FakeCliDrive()
    token = asyncio.run(drive.upload(media_file, "folder-1", media_file.name))

    assert token == "existing-file"
    assert len(drive.commands) == 1
    assert drive.commands[0][:3] == ("drive", "files", "list")


def test_drive_upload_rejects_an_existing_same_name_with_a_different_size(tmp_path):
    media_file = tmp_path / "YT-KOL-Product-01.mp4"
    media_file.write_bytes(b"correct-video")

    class FakeCliDrive(runtime.LarkCliDrive):
        async def _run(self, *args):
            return {"files": [{
                "name": media_file.name, "token": "wrong-file", "type": "file", "size": 3,
            }]}

    drive = FakeCliDrive()
    try:
        asyncio.run(drive.upload(media_file, "folder-1", media_file.name))
    except RuntimeError as exc:
        assert "同名文件大小不一致" in str(exc)
    else:
        raise AssertionError("same-name size mismatch must not be reused")


def test_controller_client_sends_completion_for_one_source_group(monkeypatch):
    calls = []

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"ok": True, "updated_records": 2}

    class FakeClient:
        async def post(self, path, **kwargs):
            calls.append((path, kwargs))
            return FakeResponse()

        async def aclose(self):
            return None

    client = runtime.ControllerClient(
        base_url="https://controller.example", token="secret", client=FakeClient(),
    )
    job = {"job_id": "archive-1", "record_id": "rec-1", "source_group": "source-1"}
    result = asyncio.run(client.complete(job, {"飞书file_token": "file-1"}))

    assert result["updated_records"] == 2
    assert calls[0][0] == "/media-archive/worker/complete"
    assert calls[0][1]["json"]["source_group"] == "source-1"
    assert calls[0][1]["json"]["job_id"] == "archive-1"


def test_controller_client_sends_job_id_with_failure():
    calls = []

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"ok": True, "exhausted": False}

    class FakeClient:
        async def post(self, path, **kwargs):
            calls.append((path, kwargs))
            return FakeResponse()

    client = runtime.ControllerClient(
        base_url="https://controller.example", token="secret", client=FakeClient(),
    )
    job = {"job_id": "archive-1", "record_id": "rec-1"}

    asyncio.run(client.fail(job, "高画质下载", "network error"))

    assert calls[0][0] == "/media-archive/worker/fail"
    assert calls[0][1]["json"]["job_id"] == "archive-1"

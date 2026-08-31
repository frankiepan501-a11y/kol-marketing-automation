import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from app import kol_email_repair


def row(record_id="k1", *, name="Jane Smith", email="jane@example.com",
        status="无效", profile="https://www.youtube.com/@janesmith"):
    return {"record_id": record_id, "fields": {
        "账号名": name, "邮箱": email, "邮箱验真状态": status,
        "主链接": {"link": profile},
    }}


class KolEmailRepairTests(unittest.TestCase):
    def test_dry_run_only_plans_snov_valid_result(self):
        with patch.object(
            kol_email_repair.feishu, "fetch_all_records", new=AsyncMock(return_value=[]),
        ), patch.object(
            kol_email_repair.feishu, "get_record", new=AsyncMock(return_value=row()),
        ), patch.object(
            kol_email_repair.snov, "find_email",
            new=AsyncMock(return_value={"status": "valid", "email": "jane@example.com"}),
        ), patch.object(
            kol_email_repair.feishu, "update_record", new=AsyncMock(),
        ) as update:
            result = asyncio.run(kol_email_repair.run_email_repair(["k1"], dry_run=True))

        self.assertEqual(0, result["writes"])
        self.assertEqual(1, result["by_status"]["would_write_valid"])
        update.assert_not_awaited()

    def test_missing_email_uses_youtube_evidence_then_validates_before_write(self):
        initial = row(email="", status="未验")
        written = row(email="jane@example.com", status="有效")
        get_record = AsyncMock(side_effect=[initial, initial, written])
        with patch.object(
            kol_email_repair.feishu, "fetch_all_records", new=AsyncMock(return_value=[]),
        ), patch.object(
            kol_email_repair.feishu, "get_record", new=get_record,
        ), patch.object(
            kol_email_repair.relabel, "fetch_youtube_public_profile",
            new=AsyncMock(return_value={"email": "jane@example.com"}),
        ), patch.object(
            kol_email_repair.snov, "find_email",
            new=AsyncMock(return_value={"status": "valid", "email": "jane@example.com"}),
        ), patch.object(
            kol_email_repair.feishu, "update_record", new=AsyncMock(),
        ) as update:
            result = asyncio.run(kol_email_repair.run_email_repair(["k1"], dry_run=False))

        self.assertEqual(1, result["writes"])
        self.assertEqual(1, result["by_status"]["written_valid"])
        self.assertEqual(
            {"邮箱": "jane@example.com", "邮箱验真状态": "有效"},
            update.await_args.args[2],
        )

    def test_unknown_verification_never_writes(self):
        with patch.object(
            kol_email_repair.feishu, "fetch_all_records", new=AsyncMock(return_value=[]),
        ), patch.object(
            kol_email_repair.feishu, "get_record", new=AsyncMock(return_value=row()),
        ), patch.object(
            kol_email_repair.snov, "find_email",
            new=AsyncMock(return_value={"status": "unknown", "email": "jane@example.com"}),
        ), patch.object(
            kol_email_repair.feishu, "update_record", new=AsyncMock(),
        ) as update:
            result = asyncio.run(kol_email_repair.run_email_repair(["k1"], dry_run=False))

        self.assertEqual(0, result["writes"])
        self.assertEqual(1, result["by_status"]["verification_unknown"])
        update.assert_not_awaited()

    def test_duplicate_owner_blocks_write(self):
        ownership = [row("other", email="jane@example.com")]
        with patch.object(
            kol_email_repair.feishu, "fetch_all_records", new=AsyncMock(return_value=ownership),
        ), patch.object(
            kol_email_repair.feishu, "get_record", new=AsyncMock(return_value=row()),
        ), patch.object(
            kol_email_repair.snov, "find_email",
            new=AsyncMock(return_value={"status": "valid", "email": "jane@example.com"}),
        ), patch.object(
            kol_email_repair.feishu, "update_record", new=AsyncMock(),
        ) as update:
            result = asyncio.run(kol_email_repair.run_email_repair(["k1"], dry_run=False))

        self.assertEqual(0, result["writes"])
        self.assertEqual(1, result["by_status"]["duplicate_email_owner"])
        update.assert_not_awaited()

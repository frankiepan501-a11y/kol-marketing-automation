import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from app import kol_public_source_backfill


class KolPublicSourceBackfillTests(unittest.TestCase):
    def test_trial_selection_requires_missing_public_source_but_not_missing_email(self):
        rows = [
            {"record_id": "yt1", "fields": {
                "主平台": "YouTube", "主链接": {"link": "https://youtube.com/@one"},
                "邮箱": "", "聚合页URL": "", "其他链接": "",
            }},
            {"record_id": "has-email", "fields": {
                "主平台": "YouTube", "主链接": {"link": "https://youtube.com/@two"},
                "邮箱": "two@example.test", "聚合页URL": "", "其他链接": "",
            }},
            {"record_id": "complete-links", "fields": {
                "主平台": "Instagram", "主链接": {"link": "https://instagram.com/three"},
                "邮箱": "", "聚合页URL": {"link": "https://linktr.ee/three"},
                "其他链接": "https://three.example/",
            }},
        ]

        self.assertEqual(
            ["yt1", "has-email"],
            kol_public_source_backfill.select_trial_record_ids(rows, limit=10),
        )

    def test_trial_selection_round_robins_supported_platforms(self):
        rows = []
        for platform, prefix, host in (
            ("YouTube", "y", "youtube.com/@"),
            ("Instagram", "i", "instagram.com/"),
            ("TikTok", "t", "tiktok.com/@"),
        ):
            for index in range(3):
                rows.append({"record_id": f"{prefix}{index}", "fields": {
                    "主平台": platform,
                    "主链接": {"link": f"https://{host}{prefix}{index}"},
                    "邮箱": "", "聚合页URL": "", "其他链接": "",
                }})

        self.assertEqual(
            ["y0", "i0", "t0", "y1", "i1", "t1"],
            kol_public_source_backfill.select_trial_record_ids(rows, limit=6),
        )

    def test_plan_never_overwrites_existing_public_source_fields(self):
        fields = {
            "聚合页URL": {"link": "https://linktr.ee/existing"},
            "其他链接": {"link": "https://existing.example/"},
        }
        candidates = [
            {"url": "https://beacons.ai/new", "kind": "aggregate"},
            {"url": "https://new.example/", "kind": "website"},
        ]

        self.assertEqual(
            {},
            kol_public_source_backfill.plan_public_source_fields(
                fields, candidates, field_types={"聚合页URL": 15, "其他链接": 15},
            ),
        )

    def test_plan_fills_only_empty_target_fields_and_formats_url_fields(self):
        candidates = [
            {"url": "https://beacons.ai/new", "kind": "aggregate"},
            {"url": "https://new.example/", "kind": "website"},
        ]

        self.assertEqual({
            "聚合页URL": {"link": "https://beacons.ai/new", "text": "beacons.ai"},
            "其他链接": {"link": "https://new.example/", "text": "new.example"},
        }, kol_public_source_backfill.plan_public_source_fields(
            {"聚合页URL": "", "其他链接": ""},
            candidates,
            field_types={"聚合页URL": 15, "其他链接": 15},
        ))

    def test_plan_uses_plain_text_when_master_field_is_text_type(self):
        self.assertEqual({
            "其他链接": "https://new.example/",
        }, kol_public_source_backfill.plan_public_source_fields(
            {"其他链接": ""},
            [{"url": "https://new.example/", "kind": "website"}],
            field_types={"其他链接": 1},
        ))

    def test_processing_error_aborts_all_production_writes(self):
        good = {"record_id": "good", "fields": {
            "主平台": "YouTube", "主链接": {"link": "https://youtube.com/@good"},
            "聚合页URL": "", "其他链接": "",
        }}
        get_record = AsyncMock(side_effect=[good, RuntimeError("temporary")])
        with patch.object(
            kol_public_source_backfill.feishu, "get_record", new=get_record,
        ), patch.object(
            kol_public_source_backfill.kol_email_sources,
            "discover_public_landing_page_candidates",
            new=AsyncMock(return_value=[{
                "url": "https://linktr.ee/good", "kind": "aggregate",
            }]),
        ), patch.object(
            kol_public_source_backfill.feishu, "update_record", new=AsyncMock(),
        ) as update:
            result = asyncio.run(kol_public_source_backfill.run_public_source_backfill(
                ["good", "bad"], field_types={"聚合页URL": 15, "其他链接": 1},
                dry_run=False,
            ))

        self.assertFalse(result["safe_to_continue"])
        self.assertEqual("inspection_error", result["abort_reason"])
        self.assertEqual(0, result["writes"])
        self.assertEqual(1, result["by_status"]["processing_error"])
        self.assertEqual(1, result["by_status"]["not_written_after_failure"])
        update.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()

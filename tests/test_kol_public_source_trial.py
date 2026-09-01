import argparse
import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from scripts import run_kol_public_source_trial as trial


class KolPublicSourceTrialTests(unittest.TestCase):
    def test_main_passes_public_source_handoff_to_email_stage(self):
        with tempfile.TemporaryDirectory() as tmp:
            ids_file = Path(tmp) / "ids.json"
            ids_file.write_text(json.dumps(["k1"]), encoding="utf-8")
            args = argparse.Namespace(
                limit=1,
                ids_file=str(ids_file),
                evidence_file="",
                commit_links=False,
                commit_emails=False,
            )
            handoff = {
                "k1": {"聚合页URL": {"link": "https://linktr.ee/new-source"}},
            }
            links = {
                "dry_run": True, "requested": 1, "processed": 1, "writes": 0,
                "safe_to_continue": True, "abort_reason": "",
                "by_status": {"would_write_public_source": 1},
                "handoff_fields": handoff,
                "results": [{
                    "record_id": "k1", "status": "would_write_public_source",
                }],
            }
            emails = {
                "dry_run": True, "requested": 1, "processed": 1, "writes": 0,
                "safe_to_continue": True, "abort_reason": "",
                "by_status": {"would_write_public_contact": 1},
                "results": [{
                    "record_id": "k1", "status": "would_write_public_contact",
                }],
            }
            with patch.object(
                trial, "_field_types",
                new=AsyncMock(return_value={"聚合页URL": 15, "其他链接": 1}),
            ), patch.object(
                trial.kol_public_source_backfill, "run_public_source_backfill",
                new=AsyncMock(return_value=links),
            ) as source_backfill, patch.object(
                trial.kol_email_repair, "run_email_repair",
                new=AsyncMock(return_value=emails),
            ) as email_repair:
                asyncio.run(trial.main(args))

        self.assertTrue(source_backfill.await_args.kwargs["include_handoff_fields"])
        self.assertEqual(handoff, email_repair.await_args.kwargs["source_overrides"])

    def test_selects_next_empty_email_public_source_batch_with_exclusions(self):
        def item(record_id, platform, *, email="", aggregate="", other=""):
            return {"record_id": record_id, "fields": {
                "主平台": platform,
                "邮箱": email,
                "聚合页URL": {"link": aggregate} if aggregate else None,
                "其他链接": other,
            }}

        records = [
            item("ig-old", "Instagram", aggregate="https://linktr.ee/old"),
            item("ig-new", "Instagram", aggregate="https://linktr.ee/new"),
            item("tt-new", "TikTok", other="https://creator.example"),
            item("yt-email", "YouTube", email="known@example.com",
                 aggregate="https://beacons.ai/known"),
            item("x-missing-source", "X"),
            item("x-new", "X", aggregate="https://beacons.ai/new"),
        ]

        selected = trial._select_empty_email_public_source_record_ids(
            records, limit=3, excluded_ids={"ig-old"},
        )

        self.assertEqual(["ig-new", "tt-new", "x-new"], selected)

    def test_public_source_failure_skips_email_stage(self):
        with tempfile.TemporaryDirectory() as tmp:
            ids_file = Path(tmp) / "ids.json"
            ids_file.write_text(json.dumps(["k1", "k2"]), encoding="utf-8")
            args = argparse.Namespace(
                limit=30,
                ids_file=str(ids_file),
                evidence_file="",
                commit_links=True,
                commit_emails=True,
            )
            unsafe_links = {
                "dry_run": False,
                "requested": 2,
                "processed": 2,
                "writes": 0,
                "safe_to_continue": False,
                "abort_reason": "readback_mismatch",
                "by_status": {"readback_mismatch": 1},
                "results": [],
            }
            with patch.object(
                trial, "_field_types",
                new=AsyncMock(return_value={"聚合页URL": 15, "其他链接": 1}),
            ), patch.object(
                trial.kol_public_source_backfill, "run_public_source_backfill",
                new=AsyncMock(return_value=unsafe_links),
            ), patch.object(
                trial.kol_email_repair, "run_email_repair", new=AsyncMock(),
            ) as email_repair:
                result = asyncio.run(trial.main(args))

        email_repair.assert_not_awaited()
        self.assertEqual(0, result["emails"]["processed"])
        self.assertEqual(
            2,
            result["emails"]["by_status"]["skipped_after_public_source_failure"],
        )
        self.assertFalse(result["links"]["safe_to_continue"])

    def test_evidence_keeps_safe_per_record_email_status(self):
        with tempfile.TemporaryDirectory() as tmp:
            ids_file = Path(tmp) / "ids.json"
            evidence_file = Path(tmp) / "evidence.json"
            ids_file.write_text(json.dumps(["k1"]), encoding="utf-8")
            args = argparse.Namespace(
                limit=1,
                ids_file=str(ids_file),
                evidence_file=str(evidence_file),
                commit_links=False,
                commit_emails=False,
            )
            links = {
                "dry_run": True, "requested": 1, "processed": 1, "writes": 0,
                "safe_to_continue": True, "abort_reason": "",
                "by_status": {"no_new_public_source": 1},
                "results": [{"record_id": "k1", "status": "no_new_public_source"}],
            }
            emails = {
                "dry_run": True, "requested": 1, "processed": 1, "writes": 0,
                "by_status": {"would_write_public_contact": 1},
                "results": [{
                    "record_id": "k1",
                    "status": "would_write_public_contact",
                    "email_fingerprint": "not-written-to-evidence",
                }],
            }
            with patch.object(
                trial, "_field_types",
                new=AsyncMock(return_value={"聚合页URL": 15, "其他链接": 1}),
            ), patch.object(
                trial.kol_public_source_backfill, "run_public_source_backfill",
                new=AsyncMock(return_value=links),
            ), patch.object(
                trial.kol_email_repair, "run_email_repair",
                new=AsyncMock(return_value=emails),
            ):
                asyncio.run(trial.main(args))

            evidence = json.loads(evidence_file.read_text(encoding="utf-8"))

        self.assertEqual(
            [{"record_id": "k1", "status": "would_write_public_contact"}],
            evidence["email_record_status"],
        )
        self.assertNotIn("email_fingerprint", evidence["email_record_status"][0])


if __name__ == "__main__":
    unittest.main()

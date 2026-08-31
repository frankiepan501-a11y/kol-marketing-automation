import argparse
import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from scripts import run_kol_public_source_trial as trial


class KolPublicSourceTrialTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()

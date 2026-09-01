import asyncio
import json
import unittest
from unittest.mock import AsyncMock, patch

from scripts import replay_kol_public_email as replay


class KolPublicEmailReplayTests(unittest.TestCase):
    def test_cli_rejects_page_budget_above_four(self):
        with self.assertRaises(SystemExit):
            replay.parse_args(["--record-id", "rec1", "--max-pages", "8"])

    def test_single_record_replay_is_read_only_and_redacts_contact_values(self):
        record = {
            "record_id": "rec1",
            "fields": {"账号名": "Creator", "邮箱": ""},
        }
        discovery = {
            "candidates": [{
                "email": "creator@example.com",
                "source": "master_aggregate",
                "source_url": "https://linktr.ee/creator",
            }],
            "trace": [{
                "stage": "public_page",
                "source": "master_aggregate",
                "source_kind": "aggregate",
                "host": "linktr.ee",
                "url_fingerprint": "abc123",
                "status": "ok",
                "contact_pages_found": 1,
                "linked_pages_found": 1,
                "email_candidates_found": 1,
                "url": "https://linktr.ee/creator",
                "email": "creator@example.com",
            }],
        }
        inspection = {
            "record_id": "rec1",
            "status": "verified_valid",
            "planned_fields": {
                "邮箱": "creator@example.com",
                "邮箱验真状态": "有效",
            },
            "source_url": "https://linktr.ee/creator",
            "candidate_count": 1,
        }
        update = AsyncMock()
        with patch.object(
            replay.feishu, "fetch_all_records", new=AsyncMock(return_value=[record]),
        ) as fetch_records, patch.object(
            replay.feishu, "get_record", new=AsyncMock(),
        ) as get_record, patch.object(
            replay.kol_email_sources,
            "discover_public_email_candidates_with_trace",
            new=AsyncMock(return_value=discovery),
        ) as discover, patch.object(
            replay.kol_email_repair, "inspect_record",
            new=AsyncMock(return_value=inspection),
        ) as inspect, patch.object(
            replay.feishu, "update_record", new=update,
        ):
            result = asyncio.run(replay.replay_record("rec1", max_pages=4))

        self.assertEqual({
            "record_id": "rec1",
            "mode": "read_only",
            "write_attempted": False,
            "public_candidate_count": 1,
            "verification_status": "verified_valid",
            "verification_candidate_count": 1,
            "public_trace": [{
                "stage": "public_page",
                "source": "master_aggregate",
                "source_kind": "aggregate",
                "host": "linktr.ee",
                "url_fingerprint": "abc123",
                "status": "ok",
                "contact_pages_found": 1,
                "linked_pages_found": 1,
                "email_candidates_found": 1,
            }],
        }, result)
        serialized = json.dumps(result, ensure_ascii=False)
        self.assertNotIn("creator@example.com", serialized)
        self.assertNotIn("https://linktr.ee/creator", serialized)
        fetch_records.assert_awaited_once_with(
            replay.config.T_KOL,
            field_names=replay.REPLAY_FIELDS,
            page_size=500,
        )
        get_record.assert_not_awaited()
        discover.assert_awaited_once_with(record["fields"], max_pages=4)
        inspect.assert_awaited_once_with(
            record, candidates=discovery["candidates"],
        )
        update.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()

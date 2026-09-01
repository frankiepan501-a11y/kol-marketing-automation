import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from app import kol_email_repair, snov


def row(record_id="k1", *, name="Jane Smith", email="jane@example.com",
        status="无效", profile="https://www.youtube.com/@janesmith"):
    return {"record_id": record_id, "fields": {
        "账号名": name, "邮箱": email, "邮箱验真状态": status,
        "主链接": {"link": profile},
    }}


class KolEmailRepairTests(unittest.TestCase):
    def test_public_contact_candidate_is_accepted_without_snov(self):
        candidate = {
            "email": "business@creator.example",
            "source": "master_other_contact",
            "source_url": "https://creator.example/contact",
        }
        with patch.object(
            snov, "find_email",
            new=AsyncMock(side_effect=AssertionError("public contact must not call Snov")),
        ) as finder:
            result = asyncio.run(kol_email_repair.inspect_record(
                row(email="", status="未验", name="CreatorHandle"),
                candidates=[candidate],
            ))

        self.assertEqual("public_contact_found", result["status"])
        self.assertEqual(
            {"邮箱": "business@creator.example", "邮箱验真状态": "未验"},
            result["planned_fields"],
        )
        finder.assert_not_awaited()

    def test_source_overrides_are_used_by_email_discovery_in_same_run(self):
        initial = row(email="", status="未验")

        async def discover(fields):
            self.assertEqual(
                "https://linktr.ee/new-source",
                kol_email_repair.feishu.ext_url(fields.get("聚合页URL")),
            )
            return [{
                "email": "business@creator.example",
                "source": "master_aggregate",
                "source_url": "https://linktr.ee/new-source",
            }]

        with patch.object(
            kol_email_repair.feishu, "fetch_all_records",
            new=AsyncMock(return_value=[initial]),
        ), patch.object(
            kol_email_repair.kol_email_sources, "discover_public_email_candidates",
            new=AsyncMock(side_effect=discover),
        ):
            result = asyncio.run(kol_email_repair.run_email_repair(
                ["k1"], dry_run=True,
                source_overrides={
                    "k1": {"聚合页URL": {"link": "https://linktr.ee/new-source"}},
                },
            ))

        self.assertEqual(1, result["by_status"]["would_write_public_contact"])

    def test_inspect_record_accepts_pre_discovered_candidates_for_single_replay(self):
        discover = AsyncMock(side_effect=AssertionError("must not rediscover"))
        with patch.object(
            kol_email_repair.kol_email_sources, "discover_public_email_candidates",
            new=discover,
        ):
            result = asyncio.run(kol_email_repair.inspect_record(
                row(email="", status="未验"),
                candidates=[{
                    "email": "jane@example.com",
                    "source": "master_aggregate",
                    "source_url": "https://linktr.ee/jane",
                }],
            ))

        self.assertEqual("public_contact_found", result["status"])
        self.assertEqual(1, result["candidate_count"])
        discover.assert_not_awaited()

    def test_dry_run_uses_one_bulk_read_and_never_calls_single_record_endpoint(self):
        initial = row(email="", status="未验")
        bulk_read = AsyncMock(return_value=[initial])
        single_read = AsyncMock(side_effect=RuntimeError("1254607 Data not ready"))
        with patch.object(
            kol_email_repair.feishu, "fetch_all_records", new=bulk_read,
        ), patch.object(
            kol_email_repair.feishu, "get_record", new=single_read,
        ), patch.object(
            kol_email_repair.kol_email_sources, "discover_public_email_candidates",
            new=AsyncMock(return_value=[]),
        ), patch.object(
            kol_email_repair.feishu, "update_record", new=AsyncMock(),
        ) as update:
            result = asyncio.run(kol_email_repair.run_email_repair(
                ["k1"], dry_run=True,
            ))

        self.assertTrue(result["safe_to_continue"])
        self.assertEqual(1, result["by_status"]["no_public_email"])
        self.assertEqual(0, result["writes"])
        bulk_read.assert_awaited_once_with(
            kol_email_repair.config.T_KOL,
            field_names=kol_email_repair.BULK_READ_FIELDS,
            page_size=500,
        )
        single_read.assert_not_awaited()
        update.assert_not_awaited()

    def test_dry_run_plans_public_contact_as_unverified(self):
        initial = row(email="", status="未验")
        with patch.object(
            kol_email_repair.feishu, "fetch_all_records",
            new=AsyncMock(return_value=[initial]),
        ), patch.object(
            kol_email_repair.kol_email_sources, "discover_public_email_candidates",
            new=AsyncMock(return_value=[{
                "email": "jane@example.com", "source": "youtube_about",
            }]),
        ), patch.object(
            kol_email_repair.feishu, "update_record", new=AsyncMock(),
        ) as update:
            result = asyncio.run(kol_email_repair.run_email_repair(["k1"], dry_run=True))

        self.assertEqual(0, result["writes"])
        self.assertEqual(1, result["by_status"]["would_write_public_contact"])
        self.assertEqual(
            {"邮箱": "<public-contact>", "邮箱验真状态": "未验"},
            result["results"][0]["planned_fields"],
        )
        update.assert_not_awaited()

    def test_existing_email_is_never_rediscovered_or_planned(self):
        with patch.object(
            kol_email_repair.kol_email_sources, "discover_public_email_candidates",
            new=AsyncMock(),
        ) as discover:
            result = asyncio.run(kol_email_repair.inspect_record(row()))

        self.assertEqual("existing_email_skipped", result["status"])
        self.assertEqual({}, result["planned_fields"])
        discover.assert_not_awaited()

    def test_existing_invalid_email_text_is_also_never_overwritten(self):
        with patch.object(
            kol_email_repair.kol_email_sources, "discover_public_email_candidates",
            new=AsyncMock(),
        ) as discover:
            result = asyncio.run(kol_email_repair.inspect_record(
                row(email="not-an-email", status="未验"),
            ))

        self.assertEqual("existing_email_skipped", result["status"])
        self.assertEqual({}, result["planned_fields"])
        discover.assert_not_awaited()

    def test_inspection_error_aborts_all_email_writes(self):
        good = row("good", email="", status="未验")
        bad = row("bad", email="", status="未验")
        with patch.object(
            kol_email_repair.kol_email_sources, "discover_public_email_candidates",
            new=AsyncMock(side_effect=[
                [{"email": "jane@example.com", "source": "youtube_about"}],
                RuntimeError("temporary"),
            ]),
        ), patch.object(
            kol_email_repair.feishu, "fetch_all_records",
            new=AsyncMock(return_value=[good, bad]),
        ), patch.object(
            kol_email_repair.feishu, "update_record", new=AsyncMock(),
        ) as update:
            result = asyncio.run(kol_email_repair.run_email_repair(
                ["good", "bad"], dry_run=False,
            ))

        self.assertFalse(result["safe_to_continue"])
        self.assertEqual("inspection_error", result["abort_reason"])
        self.assertEqual(0, result["writes"])
        self.assertEqual(1, result["by_status"]["processing_error"])
        self.assertEqual(1, result["by_status"]["not_written_after_failure"])
        update.assert_not_awaited()

    def test_missing_email_uses_public_evidence_and_writes_unverified(self):
        initial = row(email="", status="未验")
        written = row(email="jane@example.com", status="未验")
        get_record = AsyncMock(side_effect=[initial, written])
        with patch.object(
            kol_email_repair.feishu, "fetch_all_records",
            new=AsyncMock(side_effect=[[initial], [initial]]),
        ), patch.object(
            kol_email_repair.feishu, "get_record", new=get_record,
        ), patch.object(
            kol_email_repair.kol_email_sources, "discover_public_email_candidates",
            new=AsyncMock(return_value=[{
                "email": "jane@example.com", "source": "youtube_about",
            }]),
        ), patch.object(
            kol_email_repair.feishu, "update_record", new=AsyncMock(),
        ) as update:
            result = asyncio.run(kol_email_repair.run_email_repair(["k1"], dry_run=False))

        self.assertEqual(1, result["writes"])
        self.assertEqual(1, result["by_status"]["written_public_contact"])
        self.assertEqual(
            {"邮箱": "jane@example.com", "邮箱验真状态": "未验"},
            update.await_args.args[2],
        )

    def test_source_fields_changed_after_inspection_abort_before_write(self):
        initial = row(email="", status="未验")
        initial["fields"]["其他链接"] = "https://creator.example/contact"
        changed = row(email="", status="未验")
        changed["fields"]["其他链接"] = "https://creator.example/updated"
        with patch.object(
            kol_email_repair.feishu, "fetch_all_records",
            new=AsyncMock(side_effect=[[initial], [initial]]),
        ), patch.object(
            kol_email_repair.feishu, "get_record", new=AsyncMock(return_value=changed),
        ), patch.object(
            kol_email_repair.kol_email_sources, "discover_public_email_candidates",
            new=AsyncMock(return_value=[{
                "email": "jane@example.com", "source": "public_contact",
            }]),
        ), patch.object(
            kol_email_repair.feishu, "update_record", new=AsyncMock(),
        ) as update:
            result = asyncio.run(kol_email_repair.run_email_repair(
                ["k1"], dry_run=False,
            ))

        self.assertFalse(result["safe_to_continue"])
        self.assertEqual("concurrent_change", result["abort_reason"])
        self.assertEqual(0, result["writes"])
        self.assertEqual(1, result["by_status"]["concurrent_change"])
        update.assert_not_awaited()

    def test_new_duplicate_owner_after_inspection_blocks_first_write(self):
        initial = row(email="", status="未验")
        late_owner = row("other", email="jane@example.com", status="有效")
        bulk_read = AsyncMock(side_effect=[[initial], [initial, late_owner]])
        with patch.object(
            kol_email_repair.feishu, "fetch_all_records", new=bulk_read,
        ), patch.object(
            kol_email_repair.feishu, "get_record", new=AsyncMock(),
        ) as get_record, patch.object(
            kol_email_repair.kol_email_sources, "discover_public_email_candidates",
            new=AsyncMock(return_value=[{
                "email": "jane@example.com", "source": "youtube_about",
            }]),
        ), patch.object(
            kol_email_repair.feishu, "update_record", new=AsyncMock(),
        ) as update:
            result = asyncio.run(kol_email_repair.run_email_repair(
                ["k1"], dry_run=False,
            ))

        self.assertTrue(result["safe_to_continue"])
        self.assertEqual(0, result["writes"])
        self.assertEqual(1, result["by_status"]["duplicate_email_owner"])
        self.assertEqual(2, bulk_read.await_count)
        get_record.assert_not_awaited()
        update.assert_not_awaited()

    def test_first_clean_public_candidate_is_used(self):
        initial = row(email="", status="未验")
        with patch.object(
            kol_email_repair.kol_email_sources, "discover_public_email_candidates",
            new=AsyncMock(return_value=[
                {"email": "hello@creator.com", "source": "public_website"},
                {"email": "jane@creator.com", "source": "public_contact"},
            ]),
        ):
            result = asyncio.run(kol_email_repair.inspect_record(initial))

        self.assertEqual("public_contact_found", result["status"])
        self.assertEqual("public_website", result["source"])
        self.assertEqual("hello@creator.com", result["planned_fields"]["邮箱"])

    def test_single_token_kol_name_does_not_block_public_contact(self):
        initial = row(email="", status="未验", name="GamerTag")
        with patch.object(
            kol_email_repair.kol_email_sources, "discover_public_email_candidates",
            new=AsyncMock(return_value=[{
                "email": "hello@example.com", "source": "public_contact",
            }]),
        ):
            result = asyncio.run(kol_email_repair.inspect_record(initial))

        self.assertEqual("public_contact_found", result["status"])
        self.assertEqual("hello@example.com", result["planned_fields"]["邮箱"])

    def test_duplicate_owner_blocks_write(self):
        initial = row(email="", status="未验")
        ownership = [row("other", email="jane@example.com")]
        with patch.object(
            kol_email_repair.feishu, "fetch_all_records",
            new=AsyncMock(return_value=[initial, *ownership]),
        ), patch.object(
            kol_email_repair.kol_email_sources, "discover_public_email_candidates",
            new=AsyncMock(return_value=[{
                "email": "jane@example.com", "source": "youtube_about",
            }]),
        ), patch.object(
            kol_email_repair.feishu, "update_record", new=AsyncMock(),
        ) as update:
            result = asyncio.run(kol_email_repair.run_email_repair(["k1"], dry_run=False))

        self.assertEqual(0, result["writes"])
        self.assertEqual(1, result["by_status"]["duplicate_email_owner"])
        update.assert_not_awaited()

    def test_duplicate_lookup_reads_stable_email_owner_index(self):
        initial = row(email="", status="未验")
        search = AsyncMock(return_value=[initial])
        with patch.object(
            kol_email_repair.feishu, "fetch_all_records", new=search,
        ), patch.object(
            kol_email_repair.kol_email_sources, "discover_public_email_candidates",
            new=AsyncMock(return_value=[{
                "email": "jane@example.com", "source": "youtube_about",
            }]),
        ):
            asyncio.run(kol_email_repair.run_email_repair(["k1"], dry_run=True))

        search.assert_awaited_once_with(
            kol_email_repair.config.T_KOL,
            field_names=kol_email_repair.BULK_READ_FIELDS, page_size=500,
        )

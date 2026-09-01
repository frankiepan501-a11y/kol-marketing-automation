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
    def test_inspect_record_accepts_pre_discovered_candidates_for_single_replay(self):
        discover = AsyncMock(side_effect=AssertionError("must not rediscover"))
        with patch.object(
            kol_email_repair.kol_email_sources, "discover_public_email_candidates",
            new=discover,
        ), patch.object(
            kol_email_repair.snov, "find_email",
            new=AsyncMock(return_value={"status": "not_found"}),
        ):
            result = asyncio.run(kol_email_repair.inspect_record(
                row(email="", status="未验"),
                candidates=[{
                    "email": "jane@example.com",
                    "source": "master_aggregate",
                    "source_url": "https://linktr.ee/jane",
                }],
            ))

        self.assertEqual("verification_not_found", result["status"])
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
            field_names=[
                "账号名", "邮箱", "邮箱验真状态", "主链接", "聚合页URL", "其他链接",
            ],
            page_size=500,
        )
        single_read.assert_not_awaited()
        update.assert_not_awaited()

    def test_dry_run_only_plans_snov_valid_result(self):
        initial = row(email="", status="未验")
        with patch.object(
            kol_email_repair.feishu, "fetch_all_records", new=AsyncMock(return_value=[initial]),
        ), patch.object(
            kol_email_repair.kol_email_sources, "discover_public_email_candidates",
            new=AsyncMock(return_value=[{
                "email": "jane@example.com", "source": "youtube_about",
            }]),
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

    def test_existing_email_is_never_reverified_or_planned(self):
        with patch.object(
            kol_email_repair.kol_email_sources, "discover_public_email_candidates",
            new=AsyncMock(),
        ) as discover, patch.object(
            kol_email_repair.snov, "find_email", new=AsyncMock(),
        ) as verifier:
            result = asyncio.run(kol_email_repair.inspect_record(row()))

        self.assertEqual("existing_email_skipped", result["status"])
        self.assertEqual({}, result["planned_fields"])
        discover.assert_not_awaited()
        verifier.assert_not_awaited()

    def test_existing_invalid_email_text_is_also_never_overwritten(self):
        with patch.object(
            kol_email_repair.kol_email_sources, "discover_public_email_candidates",
            new=AsyncMock(),
        ) as discover, patch.object(
            kol_email_repair.snov, "find_email", new=AsyncMock(),
        ) as verifier:
            result = asyncio.run(kol_email_repair.inspect_record(
                row(email="not-an-email", status="未验"),
            ))

        self.assertEqual("existing_email_skipped", result["status"])
        self.assertEqual({}, result["planned_fields"])
        discover.assert_not_awaited()
        verifier.assert_not_awaited()

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
            kol_email_repair.snov, "find_email",
            new=AsyncMock(return_value={"status": "valid", "email": "jane@example.com"}),
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

    def test_missing_email_uses_youtube_evidence_then_validates_before_write(self):
        initial = row(email="", status="未验")
        written = row(email="jane@example.com", status="有效")
        get_record = AsyncMock(side_effect=[initial, written])
        with patch.object(
            kol_email_repair.feishu, "fetch_all_records", new=AsyncMock(return_value=[initial]),
        ), patch.object(
            kol_email_repair.feishu, "get_record", new=get_record,
        ), patch.object(
            kol_email_repair.kol_email_sources, "discover_public_email_candidates",
            new=AsyncMock(return_value=[{
                "email": "jane@example.com", "source": "youtube_about",
            }]),
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
            kol_email_repair.snov, "find_email",
            new=AsyncMock(return_value={"status": "valid", "email": "jane@example.com"}),
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
            kol_email_repair.snov, "find_email",
            new=AsyncMock(return_value={"status": "valid", "email": "jane@example.com"}),
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
        self.assertEqual(["邮箱"], bulk_read.await_args_list[1].kwargs["field_names"])
        get_record.assert_not_awaited()
        update.assert_not_awaited()

    def test_multiple_public_candidates_stop_only_on_explicit_valid(self):
        initial = row(email="", status="未验")
        with patch.object(
            kol_email_repair.kol_email_sources, "discover_public_email_candidates",
            new=AsyncMock(return_value=[
                {"email": "hello@creator.com", "source": "public_website"},
                {"email": "jane@creator.com", "source": "public_contact"},
            ]),
        ), patch.object(
            kol_email_repair.snov, "find_email", new=AsyncMock(side_effect=[
                {"status": "unknown", "email": "hello@creator.com"},
                {"status": "valid", "email": "jane@creator.com"},
            ]),
        ) as finder:
            result = asyncio.run(kol_email_repair.inspect_record(initial))

        self.assertEqual("verified_valid", result["status"])
        self.assertEqual("public_contact", result["source"])
        self.assertEqual("jane@creator.com", result["planned_fields"]["邮箱"])
        self.assertEqual(2, finder.await_count)

    def test_unknown_verification_never_writes(self):
        initial = row(email="", status="未验")
        with patch.object(
            kol_email_repair.feishu, "fetch_all_records", new=AsyncMock(return_value=[initial]),
        ), patch.object(
            kol_email_repair.kol_email_sources, "discover_public_email_candidates",
            new=AsyncMock(return_value=[{
                "email": "jane@example.com", "source": "youtube_about",
            }]),
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

    def test_insufficient_kol_name_is_not_reported_as_provider_unavailable(self):
        initial = row(email="", status="未验", name="GamerTag")
        with patch.object(
            kol_email_repair.kol_email_sources, "discover_public_email_candidates",
            new=AsyncMock(return_value=[{
                "email": "hello@example.com", "source": "public_contact",
            }]),
        ), patch.object(
            kol_email_repair.snov, "find_email",
            new=AsyncMock(return_value={
                "status": "unavailable", "email": None, "raw": "name/domain 不足",
            }),
        ):
            result = asyncio.run(kol_email_repair.inspect_record(initial))

        self.assertEqual("verification_input_insufficient", result["status"])
        self.assertEqual({}, result["planned_fields"])

    def test_oauth_failure_is_reported_as_verifier_auth_unavailable(self):
        initial = row(email="", status="未验")
        with patch.object(
            kol_email_repair.kol_email_sources, "discover_public_email_candidates",
            new=AsyncMock(return_value=[{
                "email": "jane@example.com", "source": "public_contact",
            }]),
        ), patch.object(
            kol_email_repair.snov, "find_email",
            new=AsyncMock(return_value={
                "status": "unavailable", "email": None,
                "raw": "oauth: 401 invalid_client",
            }),
        ):
            result = asyncio.run(kol_email_repair.inspect_record(initial))

        self.assertEqual("verification_auth_unavailable", result["status"])
        self.assertEqual({}, result["planned_fields"])

    def test_finder_http_failure_keeps_safe_status_code(self):
        initial = row(email="", status="未验")
        with patch.object(
            kol_email_repair.kol_email_sources, "discover_public_email_candidates",
            new=AsyncMock(return_value=[{
                "email": "jane@example.com", "source": "public_contact",
            }]),
        ), patch.object(
            kol_email_repair.snov, "find_email",
            new=AsyncMock(return_value={
                "status": "unavailable", "email": None,
                "raw": "finder: Client error '403 Forbidden' for url",
            }),
        ):
            result = asyncio.run(kol_email_repair.inspect_record(initial))

        self.assertEqual("verification_provider_http_403", result["status"])
        self.assertEqual({}, result["planned_fields"])

    def test_finder_network_failure_is_reported_without_raw_detail(self):
        initial = row(email="", status="未验")
        with patch.object(
            kol_email_repair.kol_email_sources, "discover_public_email_candidates",
            new=AsyncMock(return_value=[{
                "email": "jane@example.com", "source": "public_contact",
            }]),
        ), patch.object(
            kol_email_repair.snov, "find_email",
            new=AsyncMock(return_value={
                "status": "unavailable", "email": None,
                "raw": "finder: ConnectError: connection reset",
            }),
        ):
            result = asyncio.run(kol_email_repair.inspect_record(initial))

        self.assertEqual(
            "verification_provider_network_unavailable", result["status"],
        )
        self.assertEqual({}, result["planned_fields"])

    def test_not_valid_verification_is_reported_without_write(self):
        initial = row(email="", status="未验")
        with patch.object(
            kol_email_repair.feishu, "fetch_all_records", new=AsyncMock(return_value=[initial]),
        ), patch.object(
            kol_email_repair.kol_email_sources, "discover_public_email_candidates",
            new=AsyncMock(return_value=[{
                "email": "jane@example.com", "source": "youtube_about",
            }]),
        ), patch.object(
            kol_email_repair.snov, "find_email",
            new=AsyncMock(return_value={"status": "not_valid", "email": "jane@example.com"}),
        ), patch.object(
            kol_email_repair.feishu, "update_record", new=AsyncMock(),
        ) as update:
            result = asyncio.run(kol_email_repair.run_email_repair(["k1"], dry_run=False))

        self.assertEqual(0, result["writes"])
        self.assertEqual(1, result["by_status"]["verification_not_valid"])
        update.assert_not_awaited()

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
            kol_email_repair.snov, "find_email",
            new=AsyncMock(return_value={"status": "valid", "email": "jane@example.com"}),
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
        ), patch.object(
            kol_email_repair.snov, "find_email",
            new=AsyncMock(return_value={"status": "valid", "email": "jane@example.com"}),
        ):
            asyncio.run(kol_email_repair.run_email_repair(["k1"], dry_run=True))

        search.assert_awaited_once_with(
            kol_email_repair.config.T_KOL,
            field_names=kol_email_repair.BULK_READ_FIELDS, page_size=500,
        )

    def test_exact_public_email_must_match_finder_result(self):
        initial = row(email="", status="未验", name="Arcade Leo")
        with patch.object(
            kol_email_repair.kol_email_sources, "discover_public_email_candidates",
            new=AsyncMock(return_value=[{
                "email": "business@arcadeleo.com", "source": "youtube_about",
            }]),
        ), patch.object(
            kol_email_repair.snov, "find_email",
            new=AsyncMock(return_value={
                "status": "valid", "email": "business@arcadeleo.com",
            }),
        ) as verifier:
            result = asyncio.run(kol_email_repair.inspect_record(initial))

        self.assertEqual("verified_valid", result["status"])
        self.assertEqual("business@arcadeleo.com", result["planned_fields"]["邮箱"])
        verifier.assert_awaited_once_with("Arcade Leo", "arcadeleo.com")

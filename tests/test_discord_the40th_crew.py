import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from app import discord_the40th_crew as crew
from app import discord_tester_routes as routes


def answers(**overrides):
    values = {
        "country": "US",
        "age_confirmed": True,
        "devices": ["Switch 2"],
        "game_preferences": ["open-world"],
        "truth_confirmed": True,
        "privacy_confirmed": True,
        "can_complete_14_day_test": True,
        "controller_tasks_confirmed": True,
        "cardcase_opt_in": True,
        "disclosure_confirmed": True,
        "content_authorization_confirmed": True,
        "content_authorization_version": "the40th-content-v1",
        "application_source": "Discord",
        "future_test_contact_opt_in": False,
    }
    values.update(overrides)
    return values


def record(user_id, *, controller_at=None, cardcase_at=None, withdrawn=False):
    return {
        "record_id": f"rec-{int(user_id):04d}",
        "discord_user_id": str(user_id),
        "application_id": crew.application_id(str(user_id)),
        "campaign_id": crew.CAMPAIGN_ID,
        "controller_eligible": controller_at is not None,
        "cardcase_eligible": cardcase_at is not None,
        "first_controller_eligible_at": controller_at,
        "first_cardcase_eligible_at": cardcase_at,
        "withdrawn_at": 1 if withdrawn else None,
    }


class EligibilityTests(unittest.TestCase):
    def test_controller_decline_does_not_remove_cardcase_eligibility(self):
        result = crew.evaluate_eligibility(
            answers(
                can_complete_14_day_test=False,
                controller_tasks_confirmed=False,
                disclosure_confirmed=False,
                content_authorization_confirmed=False,
            )
        )
        self.assertTrue(result.base_eligible)
        self.assertFalse(result.controller_eligible)
        self.assertTrue(result.cardcase_eligible)

    def test_only_five_markets_switch_devices_and_adults_are_base_eligible(self):
        for change in (
            {"country": "UK"},
            {"age_confirmed": False},
            {"devices": ["Steam Deck"]},
            {"truth_confirmed": False},
            {"privacy_confirmed": False},
        ):
            with self.subTest(change=change):
                self.assertFalse(crew.evaluate_eligibility(answers(**change)).base_eligible)

    def test_form_payload_has_no_purchase_prime_or_contact_fields(self):
        serialized = str(crew.step1_modal()) + str(crew.step2_modal("signed-state"))
        forbidden = ("Amazon", "Prime", "purchase", "email", "phone", "address")
        for value in forbidden:
            with self.subTest(value=value):
                self.assertNotIn(value.casefold(), serialized.casefold())


class SubmissionMergeTests(unittest.TestCase):
    def test_resubmission_preserves_first_times_and_application_id(self):
        first = crew.merge_submission(
            existing=None,
            discord_user_id="42",
            discord_username="player",
            submitted_answers=answers(content_authorization_confirmed=False),
            now_ms=1000,
        )
        second = crew.merge_submission(
            existing=first,
            discord_user_id="42",
            discord_username="renamed-player",
            submitted_answers=answers(content_authorization_confirmed=True),
            now_ms=5000,
        )
        third = crew.merge_submission(
            existing=second,
            discord_user_id="42",
            discord_username="renamed-again",
            submitted_answers=answers(content_authorization_confirmed=True),
            now_ms=9000,
        )
        self.assertEqual(1000, third["first_submitted_at"])
        self.assertEqual(1000, third["first_valid_submitted_at"])
        self.assertEqual(5000, third["first_controller_eligible_at"])
        self.assertEqual(1000, third["first_cardcase_eligible_at"])
        self.assertEqual(first["application_id"], third["application_id"])
        self.assertEqual(9000, third["updated_at"])

    def test_resubmission_does_not_regress_lifecycle_status_or_make_on_time_user_late(self):
        first = crew.merge_submission(
            existing=None, discord_user_id="42", discord_username="player",
            submitted_answers=answers(), now_ms=crew.SUBMISSION_CUTOFF_MS - 1000,
        )
        first.update({
            "application_status": "confirmed",
            "controller_status": "selected",
            "cardcase_status": "reserve",
        })
        updated = crew.merge_submission(
            existing=first, discord_user_id="42", discord_username="player",
            submitted_answers=answers(game_preferences=["Zelda"]),
            now_ms=crew.SUBMISSION_CUTOFF_MS + 1000,
        )
        self.assertEqual("confirmed", updated["application_status"])
        self.assertEqual("selected", updated["controller_status"])
        self.assertEqual("reserve", updated["cardcase_status"])
        self.assertFalse(updated["late_submission"])

    def test_authorization_and_future_contact_changes_keep_audit_evidence(self):
        first = crew.merge_submission(
            existing=None, discord_user_id="42", discord_username="player",
            submitted_answers=answers(future_test_contact_opt_in=True), now_ms=1000,
        )
        revoked = crew.merge_submission(
            existing=first, discord_user_id="42", discord_username="player",
            submitted_answers=answers(
                content_authorization_confirmed=False,
                future_test_contact_opt_in=False,
            ), now_ms=2000,
        )
        restored = crew.merge_submission(
            existing=revoked, discord_user_id="42", discord_username="player",
            submitted_answers=answers(future_test_contact_opt_in=True), now_ms=3000,
        )
        self.assertEqual(1000, restored["content_authorization_consented_at"])
        self.assertIsNone(restored["content_authorization_revoked_at"])
        self.assertEqual(3000, restored["content_authorization_last_granted_at"])
        self.assertIn("1000|consented", restored["content_authorization_event_log"])
        self.assertIn("2000|declined_or_withdrawn", restored["content_authorization_event_log"])
        self.assertIsNone(restored["future_test_contact_revoked_at"])
        self.assertIn("3000|opted_in", restored["future_test_contact_event_log"])

    def test_withdrawn_user_cannot_rejoin_current_campaign(self):
        current = crew.merge_submission(
            existing=None,
            discord_user_id="42",
            discord_username="player",
            submitted_answers=answers(),
            now_ms=1000,
        )
        current["withdrawn_at"] = 2000
        with self.assertRaisesRegex(ValueError, "withdrawn"):
            crew.merge_submission(
                existing=current,
                discord_user_id="42",
                discord_username="player",
                submitted_answers=answers(),
                now_ms=3000,
            )

    def test_cutoff_is_an_exclusive_upper_bound(self):
        before = crew.merge_submission(
            existing=None,
            discord_user_id="1",
            discord_username="before",
            submitted_answers=answers(),
            now_ms=crew.SUBMISSION_CUTOFF_MS - 1,
        )
        exact = crew.merge_submission(
            existing=None,
            discord_user_id="2",
            discord_username="exact",
            submitted_answers=answers(),
            now_ms=crew.SUBMISSION_CUTOFF_MS,
        )
        self.assertFalse(before["late_submission"])
        self.assertTrue(exact["late_submission"])
        self.assertTrue(exact["controller_late_submission"])
        self.assertTrue(exact["cardcase_late_submission"])

    def test_bitable_mapping_excludes_legacy_purchase_fields(self):
        merged = crew.merge_submission(
            existing=None,
            discord_user_id="42",
            discord_username="player",
            submitted_answers=answers(),
            now_ms=1000,
        )
        fields = crew.to_bitable_fields(merged)
        self.assertEqual("42", fields["Discord用户ID"])
        self.assertEqual(crew.CAMPAIGN_ID, fields["活动批次"])
        serialized = str(fields).casefold()
        for forbidden in ("amazon", "prime", "购买凭证", "购买过funlab"):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden.casefold(), serialized)


class AllocationTests(unittest.TestCase):
    def test_allocates_independent_controller_and_cardcase_tracks(self):
        rows = [record(i, controller_at=i, cardcase_at=i) for i in range(1, 76)]
        allocation = crew.allocate(rows)
        self.assertEqual([str(i) for i in range(1, 21)], allocation.controller_selected)
        self.assertEqual([str(i) for i in range(21, 31)], allocation.controller_reserve)
        self.assertEqual([str(i) for i in range(21, 61)], allocation.cardcase_selected)
        self.assertEqual([str(i) for i in range(61, 71)], allocation.cardcase_reserve)
        self.assertTrue(set(allocation.controller_selected).isdisjoint(allocation.cardcase_selected))

    def test_same_millisecond_uses_stable_record_id(self):
        rows = [record(3, controller_at=100, cardcase_at=None),
                record(1, controller_at=100, cardcase_at=None),
                record(2, controller_at=100, cardcase_at=None)]
        self.assertEqual(["1", "2", "3"], crew.allocate(rows).controller_selected)

    def test_late_and_withdrawn_records_do_not_enter_allocations(self):
        rows = [
            record(1, controller_at=crew.SUBMISSION_CUTOFF_MS - 1, cardcase_at=None),
            record(2, controller_at=crew.SUBMISSION_CUTOFF_MS, cardcase_at=None),
            record(3, controller_at=3, cardcase_at=None, withdrawn=True),
        ]
        self.assertEqual(["1"], crew.allocate(rows).controller_selected)

    def test_preallocation_withdrawal_does_not_turn_initial_allocation_into_a_rerun(self):
        rows = [record(i, controller_at=i, cardcase_at=i) for i in range(1, 76)]
        rows[0]["withdrawn_at"] = 999
        rows[0]["controller_status"] = "withdrawn_or_expired"
        rows[0]["cardcase_status"] = "withdrawn_or_expired"
        allocation = crew.allocate(rows)
        self.assertEqual(20, len(allocation.controller_selected))
        self.assertEqual(40, len(allocation.cardcase_selected))

    def test_promotion_removes_cardcase_and_backfills_next_reserve(self):
        rows = [record(i, controller_at=i, cardcase_at=i) for i in range(1, 72)]
        baseline = crew.allocate(rows)
        promoted = crew.promote_controller(
            baseline,
            discord_user_id="21",
            invited_at_ms=crew.PROMOTION_INVITE_CUTOFF_MS - 1,
            replacing_user_id="1",
        )
        self.assertIn("21", promoted.controller_selected)
        self.assertNotIn("1", promoted.controller_selected)
        self.assertNotIn("21", promoted.cardcase_selected)
        self.assertIn("61", promoted.cardcase_selected)
        self.assertNotIn("61", promoted.cardcase_reserve)

    def test_promotion_invitation_cutoff_is_exclusive(self):
        allocation = crew.allocate([record(i, controller_at=i, cardcase_at=i) for i in range(1, 72)])
        with self.assertRaisesRegex(ValueError, "cutoff"):
            crew.promote_controller(
                allocation,
                discord_user_id="21",
                invited_at_ms=crew.PROMOTION_INVITE_CUTOFF_MS,
                replacing_user_id="1",
            )

    def test_allocation_updates_write_independent_statuses_and_ranks(self):
        rows = [record(i, controller_at=i, cardcase_at=i) for i in range(1, 72)]
        updates = crew.allocation_updates(rows)
        self.assertEqual("selected", updates["1"]["controller_status"])
        self.assertEqual("eligible_unallocated", updates["1"]["cardcase_status"])
        self.assertEqual("reserve", updates["21"]["controller_status"])
        self.assertEqual(1, updates["21"]["controller_reserve_rank"])
        self.assertEqual("selected", updates["21"]["cardcase_status"])
        self.assertEqual("reserve", updates["61"]["cardcase_status"])
        self.assertEqual(1, updates["61"]["cardcase_reserve_rank"])

    def test_allocation_updates_mark_late_and_withdrawn_instead_of_eligible(self):
        late = record(1, controller_at=crew.SUBMISSION_CUTOFF_MS, cardcase_at=crew.SUBMISSION_CUTOFF_MS)
        withdrawn = record(2, controller_at=1, cardcase_at=1, withdrawn=True)
        updates = crew.allocation_updates([late, withdrawn])
        for user_id in ("1", "2"):
            self.assertEqual("withdrawn_or_expired", updates[user_id]["controller_status"])
            self.assertEqual("withdrawn_or_expired", updates[user_id]["cardcase_status"])

    def test_rerun_reports_vacancy_without_silently_promoting_a_reserve(self):
        rows = [record(i, controller_at=i, cardcase_at=i) for i in range(1, 76)]
        first_updates = crew.allocation_updates(rows)
        for row in rows:
            row.update(first_updates[row["discord_user_id"]])
        rows[0]["withdrawn_at"] = 999
        rows[0]["controller_status"] = "withdrawn_or_expired"
        updates = crew.allocation_updates(rows, rerun=True)
        controller_selected = {
            user_id for user_id, fields in updates.items()
            if fields["controller_status"] in crew._SELECTED_TRACK_STATUSES
        }
        cardcase_selected = {
            user_id for user_id, fields in updates.items()
            if fields["cardcase_status"] in crew._SELECTED_TRACK_STATUSES
        }
        self.assertEqual(19, len(controller_selected))
        self.assertEqual(40, len(cardcase_selected))
        self.assertEqual("reserve", updates["21"]["controller_status"])
        self.assertNotIn("21", controller_selected)
        self.assertTrue(controller_selected.isdisjoint(cardcase_selected))
        for fields in updates.values():
            if fields["controller_status"] == "reserve":
                self.assertIsInstance(fields["controller_reserve_rank"], int)
            if fields["cardcase_status"] == "reserve":
                self.assertIsInstance(fields["cardcase_reserve_rank"], int)

    def test_promoted_cardcase_evidence_survives_allocation_rerun(self):
        rows = [record(i, controller_at=i, cardcase_at=i) for i in range(1, 76)]
        first_updates = crew.allocation_updates(rows)
        for row in rows:
            row.update(first_updates[row["discord_user_id"]])
        rows[0]["withdrawn_at"] = 999
        rows[0]["controller_status"] = "withdrawn_or_expired"
        rows[20]["controller_status"] = "promoted_to_first20"
        rows[20]["cardcase_status"] = "promoted_to_first20"
        rows[20]["cardcase_reserve_rank"] = None
        updates = crew.allocation_updates(rows, rerun=True)
        self.assertEqual("promoted_to_first20", updates["21"]["controller_status"])
        self.assertEqual("promoted_to_first20", updates["21"]["cardcase_status"])

    def test_duplicate_user_or_application_fails_closed_before_allocation(self):
        original = record(1, controller_at=1, cardcase_at=1)
        duplicate = dict(original, record_id="rec-duplicate")
        with self.assertRaisesRegex(RuntimeError, "duplicate.*manual review"):
            crew.allocation_updates([original, duplicate])

    def test_confirmation_deadline_is_72_hours(self):
        self.assertEqual(1000 + 72 * 60 * 60 * 1000, crew.confirmation_due_at(1000))


class ActionRetryTests(unittest.TestCase):
    def test_action_key_is_stable_and_versioned(self):
        self.assertEqual(
            "FUNLAB-THE40TH-CREW-2026:42:add_the_first_20:v1",
            crew.action_idempotency_key("42", "add_the_first_20", "v1"),
        )

    def test_retryable_errors_use_five_delays_then_stop(self):
        for attempt, expected_delay in enumerate((60, 300, 1800, 7200, 21600), start=1):
            decision = crew.retry_decision("discord_5xx", attempt_count=attempt)
            self.assertTrue(decision.retryable)
            self.assertEqual(expected_delay, decision.delay_seconds)
            self.assertFalse(decision.manual_action_required)
        exhausted = crew.retry_decision("discord_5xx", attempt_count=6)
        self.assertFalse(exhausted.retryable)
        self.assertTrue(exhausted.manual_action_required)

    def test_nonretryable_error_requires_manual_action_immediately(self):
        decision = crew.retry_decision("missing_role", attempt_count=1)
        self.assertFalse(decision.retryable)
        self.assertTrue(decision.manual_action_required)
        self.assertEqual(0, decision.delay_seconds)

    def test_action_lifecycle_is_pending_then_applied(self):
        action = crew.new_action("42", "add_the_first_20", "v1", now_ms=1000)
        self.assertEqual("pending", action.status)
        self.assertEqual(0, action.retry_count)
        applied = crew.mark_action_applied(action, now_ms=2000, evidence="role-readback:present")
        self.assertEqual("applied", applied.status)
        self.assertEqual("role-readback:present", applied.evidence)

    def test_action_failure_schedules_only_one_record_and_manual_requeue_resets_gate(self):
        action = crew.new_action("42", "send_result_dm", "v1", now_ms=1000)
        failed = crew.mark_action_failed(action, "discord_429", now_ms=2000, retry_after_seconds=90)
        self.assertEqual("failed", failed.status)
        self.assertEqual(1, failed.retry_count)
        self.assertEqual(2000 + 90 * 1000, failed.next_retry_at)
        self.assertFalse(failed.manual_action_required)
        manual = crew.mark_action_failed(failed, "missing_role", now_ms=3000)
        self.assertTrue(manual.manual_action_required)
        requeued = crew.requeue_action(manual, now_ms=4000)
        self.assertEqual("pending", requeued.status)
        self.assertFalse(requeued.manual_action_required)
        self.assertEqual(0, requeued.next_retry_at)


class InteractionTests(unittest.IsolatedAsyncioTestCase):
    async def test_complete_two_step_flow_writes_before_success_message(self):
        ledger = FakeLedger()
        notices = []
        dm_sender = AsyncMock(return_value="sent")

        start = await crew.build_interaction_outcome(
            component_payload("the40th_apply_start.Instagram"),
            signing_secret="secret",
            ledger=ledger,
            completion_notifier=notices.append,
            direct_message_sender=dm_sender,
            now_ms=1000,
        )
        self.assertEqual(5, start.response["type"])
        await start.work()
        dm_sender.assert_awaited_once()
        self.assertIn(crew.THE40TH_PRIVACY_MARKER, dm_sender.await_args.args[1]["content"])
        self.assertIn("12 months after campaign close", dm_sender.await_args.args[1]["content"])
        self.assertIn("90 days", dm_sender.await_args.args[1]["content"])
        self.assertIn("privacy notice", notices.pop().casefold())

        dm_button_id = dm_sender.await_args.args[1]["components"][0]["components"][0]["custom_id"]

        opened_first = await crew.build_interaction_outcome(
            component_payload(dm_button_id),
            signing_secret="secret",
            ledger=ledger,
            completion_notifier=notices.append,
            now_ms=1500,
        )
        self.assertEqual(9, opened_first.response["type"])

        step1 = await crew.build_interaction_outcome(
            modal_payload(
                opened_first.response["data"]["custom_id"],
                country="US",
                age_confirmed="yes",
                devices="Switch 2",
                game_preferences="open-world;action-adventure",
                can_complete_14_day_test="yes",
            ),
            signing_secret="secret",
            ledger=ledger,
            completion_notifier=notices.append,
            now_ms=2000,
        )
        continue_id = step1.response["data"]["components"][0]["components"][0]["custom_id"]
        self.assertLessEqual(len(continue_id), 100)
        opened = await crew.build_interaction_outcome(
            component_payload(continue_id),
            signing_secret="secret",
            ledger=ledger,
            completion_notifier=notices.append,
            now_ms=3000,
        )
        self.assertEqual(9, opened.response["type"])

        step2_id = opened.response["data"]["custom_id"]
        submitted = await crew.build_interaction_outcome(
            modal_payload(
                step2_id,
                controller_tasks_confirmed="yes",
                cardcase_opt_in="yes",
                disclosure_confirmed="yes",
                content_authorization_confirmed="yes",
                future_test_contact="yes",
            ),
            signing_secret="secret",
            ledger=ledger,
            completion_notifier=notices.append,
            now_ms=4000,
        )
        self.assertEqual(5, submitted.response["type"])
        self.assertEqual([], notices)
        self.assertEqual([], ledger.saved)
        await submitted.work()
        self.assertEqual(1, len(ledger.saved))
        self.assertIn("successfully", notices[0].casefold())
        self.assertIn(crew.application_id("42"), notices[0])
        self.assertEqual("Instagram", ledger.saved[0]["application_source"])
        self.assertTrue(ledger.saved[0]["future_test_contact_opt_in"])

    async def test_private_form_token_is_user_bound_and_expires(self):
        token = crew._privacy_form_custom_id(
            discord_user_id="42", source="Discord", expires_at_ms=2000, secret="secret"
        )
        wrong_user = await crew.build_interaction_outcome(
            component_payload(token) | {"member": {"user": {"id": "99", "username": "other"}}},
            signing_secret="secret", ledger=FakeLedger(), now_ms=1000,
        )
        expired = await crew.build_interaction_outcome(
            component_payload(token), signing_secret="secret", ledger=FakeLedger(), now_ms=2000,
        )
        self.assertEqual(4, wrong_user.response["type"])
        self.assertEqual(4, expired.response["type"])

    async def test_failed_write_never_reports_submission_success(self):
        ledger = FakeLedger(fail=True)
        notices = []
        outcome = await completed_submission_outcome(ledger, notices)
        await outcome.work()
        self.assertNotIn("successfully", notices[0].casefold())
        self.assertIn("not saved", notices[0].casefold())

    async def test_saved_application_is_not_reported_unsaved_when_notice_delivery_fails(self):
        ledger = FakeLedger()

        async def broken_notice(_message):
            raise RuntimeError("Discord webhook unavailable")

        outcome = await completed_submission_outcome(ledger, broken_notice)
        await outcome.work()
        self.assertEqual(1, len(ledger.saved))

    async def test_public_start_fails_closed_without_private_dm_sender(self):
        outcome = await crew.build_interaction_outcome(
            component_payload("the40th_apply_start"),
            signing_secret="secret",
            ledger=FakeLedger(),
            now_ms=1000,
        )
        self.assertEqual(4, outcome.response["type"])
        self.assertIn("private", outcome.response["data"]["content"].casefold())


class LedgerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.claim_store = crew.ApplicationClaimStore(Path(self.tempdir.name) / "claims.sqlite3")

    def tearDown(self):
        self.tempdir.cleanup()

    async def test_existing_record_is_updated_and_first_times_are_preserved(self):
        api = AsyncMock(side_effect=[
            {
                "code": 0,
                "data": {"items": [{
                    "record_id": "rec-existing",
                    "fields": {
                        "Discord用户ID": "42",
                        "Discord用户名": "old-name",
                        "活动批次": crew.CAMPAIGN_ID,
                        "application_id": crew.application_id("42"),
                        "first_submitted_at": 1000,
                        "first_valid_submitted_at": 1000,
                        "first_cardcase_eligible_at": 1000,
                    },
                }]},
            },
            {"code": 0, "data": {}},
        ])
        with patch("app.feishu.api", api):
            record_id, merged = await crew.The40thCrewLedger(claim_store=self.claim_store).save_submission(
                discord_user_id="42",
                discord_username="new-name",
                submitted_answers=answers(content_authorization_confirmed=True),
                now_ms=5000,
            )
        self.assertEqual("rec-existing", record_id)
        self.assertEqual(crew.application_id("42"), merged["application_id"])
        self.assertEqual(1000, merged["first_valid_submitted_at"])
        self.assertEqual(5000, merged["first_controller_eligible_at"])
        self.assertEqual("PUT", api.await_args_list[-1].args[0])
        self.assertEqual(1000, api.await_args_list[-1].args[2]["fields"]["first_cardcase_eligible_at"])

    async def test_duplicate_records_fail_closed(self):
        api = AsyncMock(return_value={
            "code": 0,
            "data": {"items": [
                {"record_id": "a", "fields": {"application_id": crew.application_id("42")}},
                {"record_id": "b", "fields": {"application_id": crew.application_id("42")}},
            ]},
        })
        with patch("app.feishu.api", api):
            with self.assertRaisesRegex(RuntimeError, "duplicate"):
                await crew.The40thCrewLedger(claim_store=self.claim_store).save_submission(
                    discord_user_id="42",
                    discord_username="player",
                    submitted_answers=answers(),
                    now_ms=5000,
                )

    async def test_allocation_preview_is_read_only_and_commit_is_explicit(self):
        ledger = crew.The40thCrewLedger(claim_store=self.claim_store)
        rows = [record(i, controller_at=i, cardcase_at=i) for i in range(1, 32)]
        writes = AsyncMock()
        with patch.object(ledger, "list_campaign_applications", AsyncMock(return_value=rows)), \
             patch.object(ledger, "update_application", writes):
            preview = await ledger.apply_allocation(commit=False)
            writes.assert_not_awaited()
            self.assertEqual("preview", preview["mode"])
            self.assertEqual(20, preview["controller_selected_count"])
            committed = await ledger.apply_allocation(commit=True)
            self.assertEqual("commit", committed["mode"])
            self.assertEqual(len(rows), writes.await_count)

    async def test_interrupted_initial_allocation_replays_the_persisted_snapshot(self):
        ledger = crew.The40thCrewLedger(claim_store=self.claim_store)
        rows = [record(i, controller_at=i, cardcase_at=i) for i in range(1, 76)]
        attempts = 0

        async def fail_after_first(_record_id, _fields):
            nonlocal attempts
            attempts += 1
            if attempts == 2:
                raise RuntimeError("simulated write interruption")

        with patch.object(ledger, "list_campaign_applications", AsyncMock(return_value=rows)), \
             patch.object(ledger, "update_application", side_effect=fail_after_first):
            with self.assertRaisesRegex(RuntimeError, "interruption"):
                await ledger.apply_allocation(commit=True)
        self.assertEqual("pending", self.claim_store.allocation_state(crew.CAMPAIGN_ID)["status"])

        writes = AsyncMock()
        with patch.object(ledger, "list_campaign_applications", AsyncMock(return_value=rows)), \
             patch.object(ledger, "update_application", writes):
            recovered = await ledger.apply_allocation(commit=True)
        self.assertEqual(len(rows), writes.await_count)
        self.assertEqual(20, recovered["controller_selected_count"])
        self.assertEqual(40, recovered["cardcase_selected_count"])
        self.assertEqual("finalized", self.claim_store.allocation_state(crew.CAMPAIGN_ID)["status"])

    async def test_pending_replay_keeps_a_new_withdrawal_final_and_reports_vacancy(self):
        ledger = crew.The40thCrewLedger(claim_store=self.claim_store)
        rows = [record(i, controller_at=i, cardcase_at=i) for i in range(1, 76)]
        snapshot = crew.allocation_updates(rows)
        self.claim_store.begin_allocation(crew.CAMPAIGN_ID, snapshot)
        rows[0]["withdrawn_at"] = 999
        rows[0]["controller_status"] = "withdrawn_or_expired"
        rows[0]["cardcase_status"] = "withdrawn_or_expired"
        writes = AsyncMock()
        with patch.object(ledger, "list_campaign_applications", AsyncMock(return_value=rows)), \
             patch.object(ledger, "update_application", writes):
            recovered = await ledger.apply_allocation(commit=True)
        first_write = next(
            call for call in writes.await_args_list if call.args[0] == rows[0]["record_id"]
        )
        self.assertEqual("withdrawn_or_expired", first_write.args[1]["controller_status"])
        self.assertEqual("withdrawn_or_expired", first_write.args[1]["cardcase_status"])
        self.assertEqual(1, recovered["controller_vacancy_count"])
        self.assertTrue(recovered["promotion_required"])

    async def test_pending_replay_never_downgrades_later_operational_statuses(self):
        for status in ("confirmed", "testing", "deliverables_complete"):
            with self.subTest(status=status):
                claim_store = crew.ApplicationClaimStore(
                    Path(self.tempdir.name) / f"claims-{status}.sqlite3"
                )
                ledger = crew.The40thCrewLedger(claim_store=claim_store)
                rows = [record(i, controller_at=i, cardcase_at=i) for i in range(1, 76)]
                snapshot = crew.allocation_updates(rows)
                claim_store.begin_allocation(crew.CAMPAIGN_ID, snapshot)
                rows[0]["controller_status"] = status
                rows[0]["cardcase_status"] = status
                writes = AsyncMock()
                with patch.object(
                    ledger, "list_campaign_applications", AsyncMock(return_value=rows)
                ), patch.object(ledger, "update_application", writes):
                    await ledger.apply_allocation(commit=True)
                first_write = next(
                    call for call in writes.await_args_list if call.args[0] == rows[0]["record_id"]
                )
                self.assertEqual(status, first_write.args[1]["controller_status"])
                self.assertEqual(status, first_write.args[1]["cardcase_status"])

    async def test_pending_replay_fails_closed_before_writes_for_duplicate_or_missing_users(self):
        for scenario in ("duplicate", "missing"):
            with self.subTest(scenario=scenario):
                claim_store = crew.ApplicationClaimStore(
                    Path(self.tempdir.name) / f"claims-{scenario}.sqlite3"
                )
                ledger = crew.The40thCrewLedger(claim_store=claim_store)
                rows = [record(i, controller_at=i, cardcase_at=i) for i in range(1, 76)]
                claim_store.begin_allocation(crew.CAMPAIGN_ID, crew.allocation_updates(rows))
                if scenario == "duplicate":
                    duplicate = dict(rows[0])
                    duplicate["record_id"] = "rec-duplicate"
                    current_rows = rows + [duplicate]
                    expected = "duplicate"
                else:
                    current_rows = rows[1:]
                    expected = "records are missing"
                writes = AsyncMock()
                with patch.object(
                    ledger,
                    "list_campaign_applications",
                    AsyncMock(return_value=current_rows),
                ), patch.object(ledger, "update_application", writes):
                    with self.assertRaisesRegex(RuntimeError, expected):
                        await ledger.apply_allocation(commit=True)
                writes.assert_not_awaited()

    async def test_confirmed_places_count_as_occupied_and_do_not_create_false_vacancies(self):
        ledger = crew.The40thCrewLedger(claim_store=self.claim_store)
        rows = [record(i, controller_at=i, cardcase_at=i) for i in range(1, 76)]
        first_updates = crew.allocation_updates(rows)
        for row in rows:
            row.update(first_updates[row["discord_user_id"]])
        rows[0]["controller_status"] = "confirmed"
        rows[20]["cardcase_status"] = "confirmed"
        self.claim_store.begin_allocation(crew.CAMPAIGN_ID, first_updates)
        self.claim_store.finalize_allocation(crew.CAMPAIGN_ID)
        with patch.object(ledger, "list_campaign_applications", AsyncMock(return_value=rows)):
            preview = await ledger.apply_allocation(commit=False)
        self.assertEqual(20, preview["controller_selected_count"])
        self.assertEqual(40, preview["cardcase_selected_count"])
        self.assertEqual(0, preview["controller_vacancy_count"])
        self.assertEqual(0, preview["cardcase_vacancy_count"])
        self.assertFalse(preview["promotion_required"])

    def test_claim_store_allows_only_one_live_creator_and_recovers_stale_lease(self):
        app_id = crew.application_id("42")
        first = self.claim_store.acquire(app_id, owner_token="one", now_ms=1000)
        concurrent = self.claim_store.acquire(app_id, owner_token="two", now_ms=1001)
        recovered = self.claim_store.acquire(
            app_id, owner_token="two", now_ms=1000 + crew._CLAIM_LEASE_MS,
        )
        self.assertEqual((True, ""), first)
        self.assertEqual((False, ""), concurrent)
        self.assertEqual((True, ""), recovered)
        self.claim_store.bind(app_id, owner_token="two", record_id="rec-42", now_ms=200000)
        self.assertEqual(
            (False, "rec-42"),
            self.claim_store.acquire(app_id, owner_token="three", now_ms=200001),
        )

    def test_claim_renewal_extends_lease_from_the_current_clock(self):
        app_id = crew.application_id("99")
        self.assertEqual(
            (True, ""), self.claim_store.acquire(app_id, owner_token="one", now_ms=1000),
        )
        self.claim_store.renew(
            app_id, owner_token="one", now_ms=1000 + crew._CLAIM_LEASE_MS - 1,
        )
        self.assertEqual(
            (False, ""),
            self.claim_store.acquire(
                app_id, owner_token="two", now_ms=1000 + crew._CLAIM_LEASE_MS + 1,
            ),
        )


class PrivacyDmRouteTests(unittest.IsolatedAsyncioTestCase):
    async def test_expired_notice_does_not_block_a_fresh_private_form_link(self):
        direct_bot_id = routes.direct_campaign.BOT_USER_ID
        old_message = {
            "id": "old",
            "author": {"id": direct_bot_id},
            "content": crew.THE40TH_PRIVACY_MARKER,
            "components": [{"components": [{
                "custom_id": "the40th_form_start.Discord.1.expired-signature",
            }]}],
        }
        new_payload = crew.privacy_dm_payload(
            "Discord", "the40th_form_start.Discord.2000000.fresh-signature",
        )
        sent = {}

        async def discord_request(method, path, *, body=None):
            if path == "/users/@me/channels":
                return {"id": "dm-1"}
            if path.endswith("messages?limit=100"):
                return [old_message]
            if method == "POST" and path.endswith("/messages"):
                sent.update(body or {})
                return {"id": "new"}
            if path.endswith("/messages/new"):
                return {"id": "new", "author": {"id": direct_bot_id}, **sent}
            raise AssertionError((method, path))

        with patch.object(routes, "_discord_request", side_effect=discord_request), \
             patch.object(routes.time, "time", return_value=1000):
            status = await routes._send_the40th_privacy_dm("42", new_payload)
        self.assertEqual(routes.direct_campaign.DirectMessageStatus.SENT, status)
        self.assertIn("nonce", sent)


class AllocationRouteTests(unittest.IsolatedAsyncioTestCase):
    async def test_preview_endpoint_is_available_without_write_gate(self):
        request = AsyncMock()
        request.json.return_value = {"commit": False}
        result = {"ok": True, "mode": "preview"}
        with patch.object(routes, "_check_internal_auth") as auth, \
             patch.object(crew.The40thCrewLedger, "apply_allocation", AsyncMock(return_value=result)) as apply:
            response = await routes.run_the40th_allocation(request, authorization="Bearer internal")
        self.assertEqual(result, response)
        auth.assert_called_once_with("Bearer internal")
        apply.assert_awaited_once_with(commit=False)

    async def test_commit_endpoint_fails_closed_until_environment_gate_is_enabled(self):
        request = AsyncMock()
        request.json.return_value = {"commit": True}
        with patch.object(routes, "_check_internal_auth"), \
             patch.dict("os.environ", {"DISCORD_THE40TH_ALLOCATION_WRITES_ENABLED": "0"}):
            with self.assertRaisesRegex(Exception, "allocation writes are not enabled"):
                await routes.run_the40th_allocation(request, authorization="Bearer internal")


class FakeLedger:
    def __init__(self, *, fail=False):
        self.fail = fail
        self.saved = []

    async def save_submission(self, *, discord_user_id, discord_username, submitted_answers, now_ms):
        if self.fail:
            raise RuntimeError("temporary Feishu failure")
        self.saved.append(dict(submitted_answers))
        return "rec-42", crew.merge_submission(
            existing=None,
            discord_user_id=discord_user_id,
            discord_username=discord_username,
            submitted_answers=submitted_answers,
            now_ms=now_ms,
        )


def component_payload(custom_id):
    return {
        "type": 3,
        "data": {"custom_id": custom_id},
        "member": {"user": {"id": "42", "username": "player"}},
    }


def modal_payload(custom_id, **values):
    return {
        "type": 5,
        "data": {
            "custom_id": custom_id,
            "components": [
                {"component": {"custom_id": key, "values": str(value).split(";")}}
                for key, value in values.items()
            ],
        },
        "member": {"user": {"id": "42", "username": "player"}},
    }


async def completed_submission_outcome(ledger, notices):
    notifier = notices.append if hasattr(notices, "append") else notices
    step1 = await crew.build_interaction_outcome(
        modal_payload(
            "the40th_apply_v1_step1.Discord",
            country="US",
            age_confirmed="yes",
            devices="Switch 2",
            game_preferences="open-world",
            can_complete_14_day_test="yes",
        ),
        signing_secret="secret",
        ledger=ledger,
        completion_notifier=notifier,
        now_ms=2000,
    )
    continue_id = step1.response["data"]["components"][0]["components"][0]["custom_id"]
    opened = await crew.build_interaction_outcome(
        component_payload(continue_id),
        signing_secret="secret",
        ledger=ledger,
        completion_notifier=notifier,
        now_ms=3000,
    )
    return await crew.build_interaction_outcome(
        modal_payload(
            opened.response["data"]["custom_id"],
            controller_tasks_confirmed="yes",
            cardcase_opt_in="yes",
            disclosure_confirmed="yes",
            content_authorization_confirmed="yes",
            future_test_contact="no",
        ),
        signing_secret="secret",
        ledger=ledger,
        completion_notifier=notifier,
        now_ms=4000,
    )


if __name__ == "__main__":
    unittest.main()

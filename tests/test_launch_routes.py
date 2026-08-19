import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException

from app import main


class FakeRequest:
    def __init__(self, payload):
        self.payload = payload

    async def json(self):
        return self.payload


class LaunchRouteTests(unittest.TestCase):
    def test_write_route_requires_internal_auth(self):
        with self.assertRaises(HTTPException) as ctx:
            asyncio.run(main.launch_evidence_start(FakeRequest({}), authorization=""))
        self.assertEqual(ctx.exception.status_code, 401)

    def test_evidence_switch_defaults_to_denied(self):
        with patch.object(main.config, "INTERNAL_TOKEN", "secret"), \
             patch.object(main.config, "LAUNCH_EVIDENCE_ENABLED", False), \
             patch.object(main.launch_evidence, "start_analysis", new=AsyncMock()) as start:
            with self.assertRaises(HTTPException) as ctx:
                asyncio.run(main.launch_evidence_start(
                    FakeRequest({"campaign_id": "c1", "expected_config_version": 1}),
                    authorization="Bearer secret",
                ))
        self.assertEqual(ctx.exception.status_code, 403)
        start.assert_not_awaited()

    def test_not_found_conflict_and_validation_have_stable_status_codes(self):
        cases = [
            (main.launch_evidence.EvidenceNotFoundError("missing"), 404),
            (main.launch_evidence.EvidenceVersionConflict("old"), 409),
            (main.launch_evidence.EvidenceValidationError("bad state"), 422),
        ]
        for error, expected in cases:
            with self.subTest(expected=expected), \
                 patch.object(main.config, "INTERNAL_TOKEN", "secret"), \
                 patch.object(main.config, "LAUNCH_EVIDENCE_ENABLED", True), \
                 patch.object(main.launch_evidence, "start_analysis", new=AsyncMock(side_effect=error)):
                with self.assertRaises(HTTPException) as ctx:
                    asyncio.run(main.launch_evidence_start(
                        FakeRequest({"campaign_id": "c1", "expected_config_version": 1}),
                        authorization="Bearer secret",
                    ))
                self.assertEqual(ctx.exception.status_code, expected)

    def test_preview_accepts_campaign_without_product_and_stays_read_only(self):
        result = {"read_only": True, "writes": 0, "campaign_id": "c1"}
        with patch.object(main.config, "INTERNAL_TOKEN", "secret"), \
             patch.object(main.launch_candidate_preview, "preview_candidates", new=AsyncMock(return_value=result)) as preview:
            response = asyncio.run(main.launch_candidates_preview(
                authorization="Bearer secret", product_id="", campaign_id="c1",
                object_type="KOL", limit=20,
            ))
        self.assertTrue(response["read_only"])
        self.assertEqual(response["writes"], 0)
        preview.assert_awaited_once_with("", object_type="KOL", limit=20, campaign_id="c1")

    def test_participant_switch_denies_before_any_write(self):
        with patch.object(main.config, "INTERNAL_TOKEN", "secret"), \
             patch.object(main.config, "LAUNCH_PARTICIPATION_WRITE_ENABLED", False), \
             patch.object(main.launch_participation, "lock_participants", new=AsyncMock()) as lock:
            with self.assertRaises(HTTPException) as ctx:
                asyncio.run(main.launch_participants_lock(
                    FakeRequest({"campaign_id": "c1"}), authorization="Bearer secret",
                ))
        self.assertEqual(ctx.exception.status_code, 403)
        lock.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()

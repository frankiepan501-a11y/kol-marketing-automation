import os
import unittest
from unittest.mock import AsyncMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app import x_history


class XHistoryProbeTests(unittest.TestCase):
    def setUp(self):
        self.old_token = x_history.config.INTERNAL_TOKEN
        x_history.config.INTERNAL_TOKEN = "unit-token"
        app = FastAPI()
        app.include_router(x_history.router)
        self.client = TestClient(app)

    def tearDown(self):
        x_history.config.INTERNAL_TOKEN = self.old_token

    def test_probe_requires_internal_bearer(self):
        self.assertEqual(401, self.client.get("/x-history/probe").status_code)
        self.assertEqual(
            401,
            self.client.get(
                "/x-history/probe",
                headers={"Authorization": "Bearer wrong"},
            ).status_code,
        )

    @patch("app.x_history._x_search_all", new_callable=AsyncMock)
    def test_probe_reports_full_archive_supported_without_exposing_posts(self, search_all):
        search_all.return_value = {
            "data": [{"id": "123", "text": "secret test body"}],
            "meta": {"result_count": 1, "next_token": "private-page-token"},
        }
        response = self.client.get(
            "/x-history/probe",
            headers={"Authorization": "Bearer unit-token"},
        )
        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertTrue(body["ok"])
        self.assertTrue(body["full_archive_supported"])
        self.assertEqual(1, body["result_count"])
        self.assertNotIn("secret test body", str(body))
        self.assertNotIn("private-page-token", str(body))

    @patch("app.x_history._x_search_all", new_callable=AsyncMock)
    def test_probe_classifies_permission_error_and_redacts_token(self, search_all):
        search_all.side_effect = x_history.XApiError(
            status_code=403,
            category="full_archive_not_authorized",
            message="Bearer x-secret-token is not allowed",
        )
        response = self.client.get(
            "/x-history/probe",
            headers={"Authorization": "Bearer unit-token"},
        )
        self.assertEqual(200, response.status_code)
        body = response.json()
        self.assertFalse(body["ok"])
        self.assertFalse(body["full_archive_supported"])
        self.assertEqual("full_archive_not_authorized", body["reason"])
        self.assertNotIn("x-secret-token", str(body))


if __name__ == "__main__":
    unittest.main()

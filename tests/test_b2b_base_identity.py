import time
import unittest
from unittest.mock import patch

from app import feishu


class _TokenResponse:
    def raise_for_status(self):
        return None

    def json(self):
        return {
            "code": 0,
            "tenant_access_token": "external-base-token",
            "expire": 3600,
        }


class _TokenClient:
    def __init__(self):
        self.posts = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def post(self, url, *, json, timeout):
        self.posts.append({"url": url, "json": json, "timeout": timeout})
        return _TokenResponse()


class B2BBaseIdentityTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        feishu._tokens.clear()

    def tearDown(self):
        feishu._tokens.clear()

    async def test_b2b_base_uses_only_external_assistant_credentials_and_own_cache_key(self):
        client = _TokenClient()
        feishu._tokens["b2b_assistant"] = ("cached-card-token", time.time() + 3600)

        with (
            patch.object(feishu.config, "FEISHU_B2B_ASSISTANT_APP_ID", "external-app-id"),
            patch.object(feishu.config, "FEISHU_B2B_ASSISTANT_APP_SECRET", "external-app-secret"),
            patch.object(feishu.config, "FEISHU_NOTIFY_APP_ID", "notify-app-id"),
            patch.object(feishu.config, "FEISHU_NOTIFY_APP_SECRET", "notify-app-secret"),
            patch.object(feishu.config, "FEISHU_BITABLE_APP_ID", "shared-base-app-id"),
            patch.object(feishu.config, "FEISHU_BITABLE_APP_SECRET", "shared-base-app-secret"),
            patch.object(feishu.httpx, "AsyncClient", return_value=client),
        ):
            result = await feishu.token("b2b_base")

        self.assertEqual("external-base-token", result)
        self.assertEqual(1, len(client.posts))
        self.assertEqual(
            {"app_id": "external-app-id", "app_secret": "external-app-secret"},
            client.posts[0]["json"],
        )
        self.assertEqual("external-base-token", feishu._tokens["b2b_base"][0])
        self.assertEqual("cached-card-token", feishu._tokens["b2b_assistant"][0])


if __name__ == "__main__":
    unittest.main()

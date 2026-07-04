import os
import unittest

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app import invest


class InvestAssistantTest(unittest.TestCase):
    def setUp(self):
        self.old_token = invest.config.INTERNAL_TOKEN
        invest.config.INTERNAL_TOKEN = "unit-token"
        invest._jobs.clear()
        self.old_env = {k: os.environ.get(k) for k in (
            "X_BEARER_TOKEN",
            "TWITTER_BEARER_TOKEN",
            "FEISHU_INVEST_ASSISTANT_APP_ID",
            "FEISHU_INVEST_ASSISTANT_APP_SECRET",
            "INVEST_NOTIFY_UNION_ID",
        )}
        for key in self.old_env:
            os.environ.pop(key, None)
        app = FastAPI()
        app.include_router(invest.router)
        self.client = TestClient(app)

    def tearDown(self):
        invest._jobs.clear()
        invest.config.INTERNAL_TOKEN = self.old_token
        for key, value in self.old_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def test_config_check_requires_bearer(self):
        self.assertEqual(401, self.client.get("/invest/config-check").status_code)
        self.assertEqual(
            401,
            self.client.get(
                "/invest/config-check",
                headers={"Authorization": "Bearer wrong"},
            ).status_code,
        )

    def test_config_check_does_not_expose_secret_values(self):
        os.environ["X_BEARER_TOKEN"] = "x-secret"
        res = self.client.get(
            "/invest/config-check",
            headers={"Authorization": "Bearer unit-token"},
        )
        self.assertEqual(200, res.status_code)
        body = res.json()
        self.assertTrue(body["ok"])
        self.assertTrue(body["configured"]["X_BEARER_TOKEN"])
        self.assertNotIn("x-secret", str(body))

    def test_daily_run_reports_missing_x_token(self):
        res = self.client.post(
            "/invest/daily/run?dry_run=true&notify=false",
            headers={"Authorization": "Bearer unit-token"},
        )
        self.assertEqual(200, res.status_code)
        body = res.json()
        self.assertFalse(body["ok"])
        self.assertEqual("missing_config", body["error_type"])
        self.assertIn("X_BEARER_TOKEN", body["error"])

    def test_daily_run_async_returns_job_id(self):
        res = self.client.post(
            "/invest/daily/run?dry_run=true&notify=false&async_mode=true",
            headers={"Authorization": "Bearer unit-token"},
        )
        self.assertEqual(200, res.status_code)
        body = res.json()
        self.assertTrue(body["ok"])
        self.assertTrue(body["accepted"])
        self.assertFalse(body["already_running"])
        self.assertTrue(body["job_id"].startswith("invest-"))

        status = self.client.get(
            f"/invest/jobs/{body['job_id']}",
            headers={"Authorization": "Bearer unit-token"},
        )
        self.assertEqual(200, status.status_code)
        self.assertIn(status.json()["status"], ("running", "success", "error"))

    def test_job_status_requires_bearer(self):
        self.assertEqual(401, self.client.get("/invest/jobs/missing").status_code)
        self.assertEqual(
            401,
            self.client.get(
                "/invest/jobs/missing",
                headers={"Authorization": "Bearer wrong"},
            ).status_code,
        )

    def test_extract_json_accepts_fenced_json(self):
        parsed = invest._extract_json('```json\n{"summary":"ok","a_share_candidates":[]}\n```')
        self.assertEqual("ok", parsed["summary"])
        self.assertEqual([], parsed["a_share_candidates"])

    def test_card_contains_candidate_and_disclaimer(self):
        posts = [{
            "id": "123",
            "created_at": "2026-07-03T00:00:00Z",
            "url": "https://x.com/aleabitoreddit/status/123",
            "text": "AI semiconductor supply chain test post",
            "metrics": {"like_count": 10},
        }]
        analysis = {
            "summary": "AI supply chain signal",
            "themes": ["AI", "semiconductor"],
            "us_tickers": [{"ticker": "NVDA", "reason": "AI compute"}],
            "a_share_candidates": [{
                "code": "300000",
                "name": "测试公司",
                "action": "观察",
                "confidence": 60,
                "reason": "产业链映射测试",
                "risks": ["需人工复核"],
            }],
            "follow_up": ["check earnings"],
        }
        card = invest._format_card(posts, analysis, lookback_hours=30)
        text = str(card)
        self.assertIn("300000", text)
        self.assertIn("测试公司", text)
        self.assertIn("非投资建议", text)
        self.assertIn("https://x.com/aleabitoreddit/status/123", text)

    def test_invalid_placeholder_code_is_removed(self):
        analysis = {
            "a_share_candidates": [{
                "code": "688XXX",
                "name": "占位代码公司",
                "action": "加入候选",
                "confidence": 80,
                "risks": [],
            }],
        }
        cleaned = invest._normalize_analysis(analysis)
        candidate = cleaned["a_share_candidates"][0]
        self.assertEqual("", candidate["code"])
        self.assertIn("需人工核对代码", "；".join(candidate["risks"]))

        card = invest._format_card([{
            "id": "123",
            "created_at": "2026-07-03T00:00:00Z",
            "url": "https://x.com/aleabitoreddit/status/123",
            "text": "test",
            "metrics": {},
        }], cleaned, lookback_hours=30)
        text = str(card)
        self.assertNotIn("688XXX", text)
        self.assertIn("代码待核对", text)


if __name__ == "__main__":
    unittest.main()

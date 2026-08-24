import asyncio
import unittest
from unittest.mock import patch

from app import deepseek, invest
from app.weekly_report import integrator


class _Response:
    status_code = 200
    text = ""

    def raise_for_status(self):
        return None

    def json(self):
        return {"choices": [{"message": {"content": '{"ok": true}'}}]}


class _PaymentRequiredResponse(_Response):
    status_code = 402

    def raise_for_status(self):
        request = deepseek.httpx.Request("POST", "https://api.deepseek.com/chat/completions")
        response = deepseek.httpx.Response(402, request=request)
        raise deepseek.httpx.HTTPStatusError(
            "payment required", request=request, response=response,
        )


class _UnauthorizedResponse(_PaymentRequiredResponse):
    status_code = 401


class _Client:
    def __init__(self, captured, *args, **kwargs):
        self.captured = captured

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def post(self, url, **kwargs):
        self.captured.append((url, kwargs))
        return _Response()


class _PaymentRequiredClient(_Client):
    async def post(self, url, **kwargs):
        self.captured.append((url, kwargs))
        return _PaymentRequiredResponse()


class _UnauthorizedClient(_Client):
    async def post(self, url, **kwargs):
        self.captured.append((url, kwargs))
        return _UnauthorizedResponse()


class DeepSeekKeyIsolationTests(unittest.TestCase):
    def test_kol_client_uses_only_kol_key(self):
        captured = []
        factory = lambda *a, **kw: _Client(captured, *a, **kw)
        with patch.object(deepseek.config, "KOL_DEEPSEEK_API_KEY", "kol-key"), \
             patch.object(deepseek.config, "MANUAL_TOOLS_DEEPSEEK_API_KEY", "manual-key"), \
             patch.object(deepseek.httpx, "AsyncClient", factory):
            result = asyncio.run(deepseek.chat_json("hello"))

        self.assertEqual({"ok": True}, result)
        self.assertEqual("Bearer kol-key", captured[0][1]["headers"]["Authorization"])

    def test_invest_client_uses_only_manual_tools_key(self):
        captured = []
        factory = lambda *a, **kw: _Client(captured, *a, **kw)
        with patch.object(invest.config, "KOL_DEEPSEEK_API_KEY", "kol-key"), \
             patch.object(invest.config, "MANUAL_TOOLS_DEEPSEEK_API_KEY", "manual-key"), \
             patch.object(invest.httpx, "AsyncClient", factory):
            result = asyncio.run(invest._call_deepseek("system", "user"))

        self.assertEqual('{"ok": true}', result)
        self.assertEqual("Bearer manual-key", captured[0][1]["headers"]["Authorization"])

    def test_missing_kol_key_fails_before_network(self):
        with patch.object(deepseek.config, "KOL_DEEPSEEK_API_KEY", ""):
            with self.assertRaisesRegex(RuntimeError, "missing KOL_DEEPSEEK_API_KEY"):
                asyncio.run(deepseek.chat_json("hello"))

    def test_kol_client_marks_402_as_terminal_provider_failure(self):
        captured = []
        factory = lambda *a, **kw: _PaymentRequiredClient(captured, *a, **kw)
        with patch.object(deepseek.config, "KOL_DEEPSEEK_API_KEY", "kol-key"), \
             patch.object(deepseek.httpx, "AsyncClient", factory):
            with self.assertRaises(deepseek.DeepSeekTerminalError) as caught:
                asyncio.run(deepseek.chat_json("hello"))

        self.assertEqual(402, caught.exception.status_code)
        self.assertEqual(1, len(captured))

    def test_kol_client_marks_401_as_terminal_provider_failure(self):
        captured = []
        factory = lambda *a, **kw: _UnauthorizedClient(captured, *a, **kw)
        with patch.object(deepseek.config, "KOL_DEEPSEEK_API_KEY", "kol-key"), \
             patch.object(deepseek.httpx, "AsyncClient", factory):
            with self.assertRaises(deepseek.DeepSeekTerminalError) as caught:
                asyncio.run(deepseek.chat_json("hello"))

        self.assertEqual(401, caught.exception.status_code)
        self.assertEqual(1, len(captured))

    def test_dtc_weekly_uses_only_dedicated_key(self):
        captured = []
        factory = lambda *a, **kw: _Client(captured, *a, **kw)
        with patch.dict(
            "os.environ",
            {
                "DTC_WEEKLY_DEEPSEEK_API_KEY": "dtc-key",
                "KOL_DEEPSEEK_API_KEY": "kol-key",
                "DEEPSEEK_API_KEY": "legacy-key",
            },
            clear=False,
        ), patch.object(integrator.httpx, "AsyncClient", factory):
            result = asyncio.run(integrator._call_llm("system", "user"))

        self.assertEqual('{"ok": true}', result)
        self.assertEqual("Bearer dtc-key", captured[0][1]["headers"]["Authorization"])

    def test_dtc_weekly_missing_dedicated_key_fails_before_network(self):
        with patch.dict(
            "os.environ",
            {"KOL_DEEPSEEK_API_KEY": "kol-key", "DEEPSEEK_API_KEY": "legacy-key"},
            clear=True,
        ):
            with self.assertRaisesRegex(RuntimeError, "DTC_WEEKLY_DEEPSEEK_API_KEY"):
                asyncio.run(integrator._call_llm("system", "user"))


if __name__ == "__main__":
    unittest.main()

import asyncio
import unittest
from unittest.mock import patch

from app import deepseek, invest


class _Response:
    status_code = 200
    text = ""

    def raise_for_status(self):
        return None

    def json(self):
        return {"choices": [{"message": {"content": '{"ok": true}'}}]}


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


if __name__ == "__main__":
    unittest.main()

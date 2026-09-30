import asyncio
import os
import unittest
from unittest.mock import AsyncMock, patch

os.environ.setdefault('INTERNAL_TOKEN', 'test-token')
from app import feishu, config


class RoleQueryRetryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        feishu._job_title_cache.clear()
        feishu._role_resolution_alerted_at.clear()

    async def lookup(self, responses):
        client = AsyncMock()
        client.get.side_effect = responses
        client.__aenter__.return_value = client
        with patch.object(feishu.httpx, 'AsyncClient', return_value=client), \
             patch.object(feishu, 'token', AsyncMock(return_value='test')), \
             patch('asyncio.sleep', new=AsyncMock()), \
             patch.object(config, 'KOL_CONTACT_DEPARTMENT_IDS', ['dept']), \
             patch.object(feishu, 'send_card_message', new=AsyncMock()) as send:
            try:
                result = await feishu.resolve_notify_targets('ship_main', job_title='商务BD专员')
                return result, client, send, None
            except RuntimeError as exc:
                return None, client, send, exc

    @staticmethod
    def response(code=0, items=None, status=200):
        return feishu.httpx.Response(status, json={'code': code, 'data': {
            'items': items or [], 'has_more': False}})

    async def test_40003_then_success_returns_owner_without_alert(self):
        person = {'name': '测试BD', 'union_id': 'on_test', 'job_title': '商务BD专员',
                  'status': {'is_activated': True}}
        result, client, send, error = await self.lookup([
            self.response(40003), self.response(items=[person])])
        self.assertIsNone(error)
        self.assertEqual(result, [('测试BD', 'on_test')])
        self.assertEqual(client.get.await_count, 2)
        send.assert_not_awaited()

    async def test_exhausted_internal_error_is_not_no_employee(self):
        result, client, send, error = await self.lookup([self.response(40003)] * 3)
        self.assertIsNotNone(error)
        self.assertEqual(client.get.await_count, 3)
        self.assertEqual(feishu._job_title_cache, {})
        body = str(send.call_args_list)
        self.assertIn('通讯录查询失败', body)
        self.assertNotIn('未找到在职职务', body)
        self.assertIn('40003', body)

    async def test_permission_error_no_retry(self):
        _, client, send, error = await self.lookup([self.response(40004)])
        self.assertIsNotNone(error)
        self.assertEqual(client.get.await_count, 1)
        self.assertIn('通讯录查询失败', str(send.call_args_list))

    async def test_success_with_no_staff_keeps_no_staff_alert(self):
        _, client, send, error = await self.lookup([self.response()])
        self.assertIsNotNone(error)
        self.assertEqual(client.get.await_count, 1)
        self.assertIn('未找到在职职务', str(send.call_args_list))

    async def test_network_timeout_retries_bounded(self):
        _, client, _, error = await self.lookup([feishu.httpx.ReadTimeout('timeout')] * 3)
        self.assertIsNotNone(error)
        self.assertEqual(client.get.await_count, 3)


if __name__ == '__main__':
    unittest.main()

import unittest
import io
from urllib.error import HTTPError
from unittest.mock import patch
from app.clients import FeishuClient
from app.clients import ApiError
from app.diagnostics import safe_failure
from app.job_status import finished_status

class ErrorDiagnosticsTests(unittest.TestCase):
    def test_http_read_retry_and_write_safety(self):
        for method, expected_calls in [('GET', 3), ('POST', 1)]:
            with self.subTest(method=method):
                client = FeishuClient('test', 'test')
                client._token = 'test'
                def fail(*args, **kwargs):
                    raise HTTPError('https://example.invalid', 400, 'test', {},
                                    io.BytesIO(b'{"code":1254607,"msg":"test"}'))
                with patch('urllib.request.urlopen', side_effect=fail) as request, patch('app.clients.time.sleep'):
                    with self.assertRaises(ApiError):
                        client.request(method, '/test')
                    self.assertEqual(request.call_count, expected_calls)

    def test_transient_read_retries_same_page(self):
        client = FeishuClient('test', 'test')
        client._token = 'test'
        with patch('app.clients._json_request', side_effect=[{'code':1254607}, {'code':0,'data':{}}]) as request, patch('app.clients.time.sleep'):
            self.assertEqual(client.request('GET', '/test'), {'code':0,'data':{}})
            self.assertEqual(request.call_count, 2)

    def test_write_is_never_retried(self):
        client = FeishuClient('test', 'test')
        client._token = 'test'
        with patch('app.clients._json_request', return_value={'code':1254607}) as request:
            with self.assertRaises(ApiError): client.request('POST', '/test')
            self.assertEqual(request.call_count, 1)

    def test_api_code_survives_without_message_or_metadata(self):
        error = ApiError('feishu', '1254607', 'secret-url-token', metadata={'key': 'secret'})
        result = safe_failure(error)
        self.assertEqual(result['error_code'], '1254607')
        self.assertNotIn('secret', str(result))

    def test_unexpected_details_are_not_serialized(self):
        self.assertEqual(safe_failure(RuntimeError('secret')), {'error_type': 'RuntimeError'})

    def test_daily_nested_failure_is_visible_to_assert(self):
        status, payload = finished_status({'status':'failed', 'job_id':'test', 'nyxi':{
            'error_type':'ApiError', 'error_service':'feishu', 'error_code':'1254607'}})
        self.assertEqual(status, 500)
        self.assertEqual(payload['detail']['error_code'], '1254607')

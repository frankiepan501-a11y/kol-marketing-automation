import unittest
from app.clients import ApiError
from app.diagnostics import safe_failure
from app.job_status import finished_status

class ErrorDiagnosticsTests(unittest.TestCase):
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

import hashlib
import json
from pathlib import Path
import sys
import os
import tempfile
import unittest
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import coolify_watchdog as w


class WatchdogTests(unittest.TestCase):
    def setUp(self):
        guard = patch.object(w, "RECIPIENT_SHA256", hashlib.sha256(b"test-recipient").hexdigest())
        guard.start()
        self.addCleanup(guard.stop)
        env = patch.dict(w.os.environ, {"FEISHU_NOTIFY_OPEN_ID": "test-recipient"})
        env.start()
        self.addCleanup(env.stop)

    def test_two_failures_then_dedup_then_recovery(self):
        bad = [{'host': 'host', 'failure': 'TimeoutError'}]
        first, due = w.transition({}, bad, 10000)
        self.assertFalse(due)
        second, due = w.transition(first, bad, 10600)
        self.assertEqual(len(due), 1)
        second['host'].update(sent_at=10600, sent_fingerprint=due[0][1])
        third, due = w.transition(second, bad, 11200)
        self.assertFalse(due)
        recovered, due = w.transition(third, [{'host': 'host'}], 11800)
        self.assertFalse(due)
        self.assertNotIn('sent_fingerprint', recovered['host'])
        self.assertEqual(recovered['host']['count'], 0)

    def test_stale_failure_not_consecutive(self):
        state, due = w.transition({'host': {'count': 10, 'checked': 1}},
                                 [{'host': 'host', 'failure': 'TimeoutError'}], 10000)
        self.assertEqual(state['host']['count'], 1)
        self.assertFalse(due)

    def test_certificate_warning_escalates(self):
        state, due = w.transition({}, [{'host': 'host', 'certificate': 'warning'}], 10000)
        self.assertEqual(len(due), 1)
        state['host'].update(sent_at=10000, sent_fingerprint=due[0][1])
        _, due = w.transition(state, [{'host': 'host', 'certificate': 'critical'}], 10600)
        self.assertEqual(len(due), 1)

    def test_sender_guard_and_readback(self):
        with patch.dict(w.os.environ, {'FEISHU_NOTIFY_APP_ID': 'wrong'}):
            with self.assertRaises(RuntimeError):
                w.notify('test', 'id')
        calls = []
        def fake(path, payload=None, token=None):
            calls.append((path, payload))
            if path.startswith('auth/'):
                return {'tenant_access_token': 'test-only'}
            if payload:
                self.assertEqual(payload['receive_id'], 'test-recipient')
                self.assertEqual(payload['msg_type'], 'text')
                return {'data': {'message_id': 'test-mid'}}
            return {'data': {'items': [{'body': {'content': calls[1][1]['content']}}]}}
        with patch.dict(w.os.environ, {'FEISHU_NOTIFY_APP_ID': w.APP, 'FEISHU_NOTIFY_APP_SECRET': 'test-only'}), patch.object(w, 'api', side_effect=fake):
            self.assertEqual(w.notify('simulation', 'incident', True), 'test-mid')
        self.assertEqual(len(calls), 3)

    def test_wrong_readback_fails(self):
        replies = [{'tenant_access_token': 'test-only'}, {'data': {'message_id': 'test-mid'}},
                   {'data': {'items': [{'body': {'content': json.dumps({'text': 'wrong'})}}]}}]
        with patch.dict(w.os.environ, {'FEISHU_NOTIFY_APP_ID': w.APP, 'FEISHU_NOTIFY_APP_SECRET': 'test-only'}), patch.object(w, 'api', side_effect=replies):
            with self.assertRaises(RuntimeError):
                w.notify('test', 'id')

    def test_sent_receipt_persisted_before_readback_failure(self):
        old = Path.cwd()
        with tempfile.TemporaryDirectory() as folder:
            os.chdir(folder)
            try:
                def simulated_send(body, incident, test, on_sent):
                    on_sent('saved-mid', 'digest')
                    raise RuntimeError('readback unavailable')
                probe = {'host': w.HOSTS[0], 'certificate': 'warning'}
                with patch.dict(w.os.environ, {'WATCHDOG_DRY_RUN': 'false', 'WATCHDOG_TEST': 'false'}), patch.object(w, 'probe', return_value=probe), patch.object(w, 'notify', side_effect=simulated_send):
                    with self.assertRaises(RuntimeError):
                        w.main()
                saved = json.loads(Path('.coolify-watchdog-state/state.json').read_text())
                self.assertEqual(saved[w.HOSTS[0]]['pending_readback']['message_id'], 'saved-mid')
                with patch.dict(w.os.environ, {'WATCHDOG_DRY_RUN': 'false', 'WATCHDOG_TEST': 'false'}), patch.object(w, 'notification_token', return_value='test-only'), patch.object(w, 'verify_message', side_effect=RuntimeError('still unavailable')), patch.object(w, 'notify') as send:
                    with self.assertRaises(RuntimeError):
                        w.main()
                    send.assert_not_called()
            finally:
                os.chdir(old)


if __name__ == '__main__':
    unittest.main()

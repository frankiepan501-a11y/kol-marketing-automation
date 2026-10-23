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
    def test_internal_fresh_stale_future_and_issue(self):
        data = {'checked_at': '2026-10-23T14:00:00+00:00', 'ok': True, 'issues': []}
        now = w.datetime.datetime.fromisoformat(data['checked_at']).timestamp()
        self.assertIsNone(w.validate_status(data, now + 120))
        self.assertEqual(w.validate_status(data, now + 601), 'STATUS_STALE')
        self.assertEqual(w.validate_status(data, now - 61), 'STATUS_FUTURE')
        data.update(ok=False, issues=['disk_usage'])
        self.assertEqual(w.validate_status(data, now), 'INTERNAL:disk_usage')

    def test_internal_rejects_extra_fields_and_bad_types(self):
        good = {'checked_at': '2026-10-23T14:00:00Z', 'ok': True, 'issues': []}
        for bad in [dict(good, containers={}), dict(good, ok='true'),
                    dict(good, ok=False), dict(good, issues=['secret']),
                    dict(good, checked_at='2026-10-23T14:00:00'),
                    dict(good, ok=False, issues=['disk_usage', 'disk_usage'])]:
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                w.validate_status(bad, 0)

    def test_internal_two_failures_dedup_and_recovery(self):
        bad = [{'host': w.STATUS_URL, 'failure': 'INTERNAL:disk_usage'}]
        state, due = w.transition({}, bad, 10000)
        self.assertFalse(due)
        state, due = w.transition(state, bad, 10600)
        self.assertEqual(len(due), 1)
        state[w.STATUS_URL].update(sent_at=10600, sent_fingerprint=due[0][1])
        state, due = w.transition(state, bad, 11200)
        self.assertFalse(due)
        state, due = w.transition(state, [{'host': w.STATUS_URL}], 11800)
        self.assertNotIn('sent_fingerprint', state[w.STATUS_URL])

    def test_stale_status_alerts_immediately_and_deduplicates(self):
        bad = [{'host': w.STATUS_URL, 'failure': 'STATUS_STALE'}]
        state, due = w.transition({}, bad, 10000)
        self.assertEqual(len(due), 1)
        state[w.STATUS_URL].update(sent_at=10000, sent_fingerprint=due[0][1])
        _, due = w.transition(state, bad, 10600)
        self.assertFalse(due)

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

    def test_env_whitespace_preserves_identity_guard(self):
        with patch.dict(w.os.environ, {'FEISHU_NOTIFY_APP_ID': ' ' + w.APP + '\n', 'FEISHU_NOTIFY_APP_SECRET': ' test-only\n'}), patch.object(w, 'api', return_value={'tenant_access_token': 'test-token'}) as api:
            self.assertEqual(w.notification_token(), 'test-token')
            self.assertEqual(api.call_args.args[1], {'app_id': w.APP, 'app_secret': 'test-only'})
        with patch.dict(w.os.environ, {'FEISHU_NOTIFY_APP_ID': ' wrong\n'}), patch.object(w, 'api') as api:
            with self.assertRaises(RuntimeError):
                w.notification_token()
            api.assert_not_called()

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
                with patch.dict(w.os.environ, {'WATCHDOG_DRY_RUN': 'false', 'WATCHDOG_TEST': 'false'}), patch.object(w, 'probe', return_value=probe), patch.object(w, 'probe_internal', return_value={'host': w.STATUS_URL}), patch.object(w, 'notify', side_effect=simulated_send):
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

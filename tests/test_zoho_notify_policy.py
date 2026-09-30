import asyncio
import sys
import types
import unittest
from contextlib import ExitStack
from unittest.mock import AsyncMock, MagicMock, patch

from app import zoho


class NotifyPolicyTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(patch.dict('os.environ', {'EMAIL_DRY_RUN_TO': 'test@example.invalid'}))
        self.access = self.stack.enter_context(patch.object(zoho, 'access', AsyncMock(return_value='test-token')))
        self.stack.enter_context(patch.object(zoho.config, 'BRAND_CONFIG', {'FUNLAB': {'account_id': 'test-account'}}))
        self.client = MagicMock()
        self.client.__aenter__ = AsyncMock(return_value=self.client)
        self.client.__aexit__ = AsyncMock(return_value=None)
        self.client.get = AsyncMock()
        self.stack.enter_context(patch.object(zoho.httpx, 'AsyncClient', return_value=self.client))
        self.stack.enter_context(patch.object(zoho.asyncio, 'sleep', AsyncMock()))
        self.notifications = types.ModuleType('app.feishu')
        self.notifications.resolve_notify_targets = AsyncMock(return_value=[('reviewer', 'test-user')])
        self.notifications.send_card_message = AsyncMock()
        import app
        self.stack.enter_context(patch.dict(sys.modules, {'app.feishu': self.notifications}))
        self.stack.enter_context(patch.object(app, 'feishu', self.notifications, create=True))

    def response(self, body='x' * 100, status=200):
        self.client.get.return_value = MagicMock(status_code=status)
        self.client.get.return_value.json.return_value = {'data': {'content': body}}

    async def test_silent_normal_still_reads_content(self):
        self.response()
        result = await zoho.verify_sent_after('FUNLAB', 'm1', 'sent', 100, notify=False)
        self.assertEqual(result['status'], 'verified')
        self.client.get.assert_awaited_once()
        self.notifications.resolve_notify_targets.assert_not_awaited()

    async def test_silent_truncation_returns_failure_without_notification(self):
        self.response('short')
        result = await zoho.verify_sent_after('FUNLAB', 'm1', 'sent', 100, notify=False)
        self.assertEqual(result['status'], 'truncated')
        self.notifications.resolve_notify_targets.assert_not_awaited()
        self.notifications.send_card_message.assert_not_awaited()

    async def test_default_truncation_preserves_existing_notification(self):
        self.response('short')
        result = await zoho.verify_sent_after('FUNLAB', 'm1', 'sent', 100)
        self.assertEqual(result['status'], 'truncated')
        self.notifications.send_card_message.assert_awaited_once()

    async def test_unavailable_content_is_not_success(self):
        self.response(status=404)
        result = await zoho.verify_sent_after('FUNLAB', 'm1', 'sent', 100, notify=False)
        self.assertEqual(result['status'], 'unverified')
        self.notifications.resolve_notify_targets.assert_not_awaited()

    async def test_network_error_is_not_success(self):
        self.client.get.side_effect = TimeoutError('test')
        result = await zoho.verify_sent_after('FUNLAB', 'm1', 'sent', 100, notify=False)
        self.assertEqual(result['status'], 'unverified')
        self.notifications.resolve_notify_targets.assert_not_awaited()

    def setup_send(self):
        for name, value in [('_get_folder_ids', ('draft', 'sent')), ('create_draft', 'draft-id'),
                            ('get_draft_body', '<p>' + 'x' * 100 + '</p>'),
                            ('get_draft_subject', 'test subject'), ('delete_draft', None),
                            ('_send_now', 'accepted-id')]:
            setattr(self, name, self.stack.enter_context(patch.object(zoho, name, AsyncMock(return_value=value))))
        self.validate = self.stack.enter_context(patch.object(zoho, '_validate_draft'))

    async def test_silent_send_waits_and_retains_accepted_id_on_failure(self):
        self.setup_send()
        self.response('short')
        with self.assertRaises(zoho.SentVerificationError) as caught:
            await zoho.send_email('FUNLAB', 'person@example.invalid', 'test subject', 'x' * 100, notify=False)
        self.assertEqual(caught.exception.message_id, 'accepted-id')
        self.assertTrue(caught.exception.send_accepted)
        self._send_now.assert_awaited_once()
        self.validate.assert_called_once()
        self.notifications.send_card_message.assert_not_awaited()

    async def test_silent_send_success_keeps_draft_and_sent_checks(self):
        self.setup_send()
        self.response('x' * 1000)
        mid = await zoho.send_email('FUNLAB', 'person@example.invalid', 'test subject', 'x' * 100, notify=False)
        self.assertEqual(mid, 'accepted-id')
        self.get_draft_body.assert_awaited_once()
        self.delete_draft.assert_awaited_once()
        self.client.get.assert_awaited_once()
        self.assertEqual(self._send_now.await_args.args[1], 'test@example.invalid')

    async def test_draft_failure_prevents_real_send(self):
        self.setup_send()
        self.validate.side_effect = zoho.DraftValidationError('test failure')
        with self.assertRaises(zoho.DraftValidationError):
            await zoho.send_email('FUNLAB', 'person@example.invalid', 'test subject', 'x' * 100, notify=False)
        self._send_now.assert_not_awaited()
        self.delete_draft.assert_awaited_once()

    async def test_cancel_after_acceptance_keeps_receipt(self):
        self.setup_send()
        with patch.object(zoho, 'verify_sent_after', AsyncMock(side_effect=asyncio.CancelledError)):
            with self.assertRaises(zoho.SentVerificationError) as caught:
                await zoho.send_email('FUNLAB', 'person@example.invalid', 'test', 'x' * 100, notify=False)
        self.assertEqual(caught.exception.message_id, 'accepted-id')
        self.assertTrue(caught.exception.send_accepted)
        self.assertEqual(caught.exception.verification['error_type'], 'CancelledError')
        self._send_now.assert_awaited_once()

    async def test_default_send_retains_background_check_and_cleanup(self):
        self.setup_send()
        self.response('x' * 1000)
        mid = await zoho.send_email('FUNLAB', 'person@example.invalid', 'test', 'x' * 100)
        self.assertEqual(mid, 'accepted-id')
        pending = list(zoho._pending_verify_tasks)
        self.assertEqual(len(pending), 1)
        await asyncio.gather(*pending)
        # Done callbacks execute before gather returns.
        self.assertFalse(zoho._pending_verify_tasks)
        self.client.get.assert_awaited_once()

    async def test_concurrent_calls_do_not_share_notification_policy(self):
        self.response('short')
        await asyncio.gather(
            zoho.verify_sent_after('FUNLAB', 'quiet', 'sent', 100, notify=False),
            zoho.verify_sent_after('FUNLAB', 'default', 'sent', 100))
        self.notifications.send_card_message.assert_awaited_once()
        self.assertIn('default', str(self.notifications.send_card_message.await_args))

    async def test_bad_policy_rejected_before_any_api(self):
        with self.assertRaises(ValueError):
            await zoho.send_email('FUNLAB', 'person@example.invalid', 'test', 'x' * 100, notify='false')
        self.access.assert_not_awaited()


    async def test_receipt_saved_before_verification(self):
        self.setup_send()
        events = []
        async def persist(mid):
            events.append(("persist", mid))
        async def verify(*args, **kwargs):
            self.assertEqual(events, [("persist", "accepted-id")])
            return {"status": "verified"}
        with patch.object(zoho, "verify_sent_after", AsyncMock(side_effect=verify)):
            await zoho.send_email("FUNLAB", "person@example.invalid", "test", "x" * 100,
                                  notify=False, on_accepted=persist)

    async def test_receipt_failure_or_cancel_preserves_accepted_id(self):
        self.setup_send()
        for error in (RuntimeError("store unavailable"), asyncio.CancelledError()):
            with self.subTest(error=type(error).__name__), \
                 patch.object(zoho, "verify_sent_after", AsyncMock()) as verify:
                with self.assertRaises(zoho.SentVerificationError) as caught:
                    await zoho.send_email("FUNLAB", "person@example.invalid", "test", "x" * 100,
                                          notify=False, on_accepted=AsyncMock(side_effect=error))
                self.assertEqual(caught.exception.message_id, "accepted-id")
                self.assertFalse(caught.exception.verification["receipt_persisted"])
                verify.assert_not_awaited()

    async def test_silent_reply_timeout_never_falls_back_to_second_send(self):
        self.setup_send()
        with patch.dict("os.environ", {"EMAIL_DRY_RUN_TO": ""}), \
             patch.object(zoho, "_send_reply", AsyncMock(side_effect=TimeoutError("unknown result"))):
            with self.assertRaises(TimeoutError):
                await zoho.send_email("FUNLAB", "person@example.invalid", "test", "x" * 100,
                                      reply_to_msg_id="inbound", notify=False)
        self._send_now.assert_not_awaited()

    async def test_default_cannot_use_receipt_hook(self):
        with self.assertRaises(ValueError):
            await zoho.send_email("FUNLAB", "person@example.invalid", "test", "x" * 100,
                                  on_accepted=AsyncMock())
        self.access.assert_not_awaited()


    async def test_sync_receipt_callback_rejected_before_send(self):
        self.setup_send()
        with self.assertRaises(ValueError):
            await zoho.send_email("FUNLAB", "person@example.invalid", "test", "x" * 100,
                                  notify=False, on_accepted=lambda mid: None)
        self._send_now.assert_not_awaited()

if __name__ == '__main__':
    unittest.main()

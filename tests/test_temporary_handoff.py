import asyncio
import unittest
from unittest.mock import AsyncMock, patch
from app import config, cs_dispatch, feishu, sla_check


class TemporaryHandoffTests(unittest.TestCase):
    def test_failed_legacy_lookup_does_not_claim_or_mark_legacy_p2(self):
        rows = [{'record_id':'bd', 'fields':{'邮件草稿来源':'warm_recap'}},
                {'record_id':'cold', 'fields':{'邮件草稿来源':'cold'}}]
        with patch.object(feishu, 'resolve_partnership_targets', new=AsyncMock(return_value=[('BD','on_bd')])), patch.object(
            feishu, 'resolve_notify_targets', new=AsyncMock(side_effect=RuntimeError('no old role'))), patch.object(
            sla_check, '_p2_marker_exists_today', new=AsyncMock(return_value=False)), patch.object(
            sla_check, '_claim_p2_daily_run', return_value=True) as claim, patch.object(
            feishu, 'update_record', new=AsyncMock()) as update, patch.object(
            sla_check, '_send_digest', new=AsyncMock(return_value={'sent':1,'failed':0,'errors':[],'message_ids':['om_bd']})), patch.object(
            sla_check, 'build_sla_digest_card', return_value={}):
            result=asyncio.run(sla_check._send_p2_handoff(rows, 123))
            self.assertEqual(result['delivered_record_ids'], ['bd'])
            self.assertEqual(result['claim_record_ids'], ['bd'])
            self.assertEqual(result['failed'], 1)
            claim.assert_called_once_with(123, 'existing')
            self.assertEqual(update.await_args.args[1], 'bd')

    def test_p2_group_claims_are_independent(self):
        import tempfile
        with tempfile.TemporaryDirectory() as directory, patch.object(config, 'KOL_SLA_STATE_DIR', directory):
            stamp=1790500000000
            self.assertTrue(sla_check._claim_p2_daily_run(stamp, 'existing'))
            self.assertTrue(sla_check._claim_p2_daily_run(stamp, 'legacy'))
            self.assertFalse(sla_check._claim_p2_daily_run(stamp, 'existing'))
            sla_check._release_p2_daily_claim(stamp, 'legacy')
            self.assertTrue(sla_check._claim_p2_daily_run(stamp, 'legacy'))
            self.assertFalse(sla_check._claim_p2_daily_run(stamp, 'existing'))

    def test_persistent_marker_is_scoped_after_restart(self):
        stamp=1790500000000
        row={'fields':{'邮件草稿来源':'followup','邮件草稿ID':'nudge-test',
                       '卡片发送时间':stamp,'SLA已升级':True}}
        with patch.object(feishu, 'search_records', new=AsyncMock(return_value=[row])):
            self.assertTrue(asyncio.run(sla_check._p2_marker_exists_today(stamp, 'existing')))
            self.assertFalse(asyncio.run(sla_check._p2_marker_exists_today(stamp, 'legacy')))

    def test_mixed_sla_separates_new_and_existing_work(self):
        rows = [{'fields':{'邮件草稿来源':s}} for s in ('cold', 'reply')]
        delivered = {'sent':1,'failed':0,'errors':[],'message_ids':['om_test']}
        with patch.object(config, 'KOL_PARTNERSHIP_JOB_TITLE', '商务BD专员'), patch.object(
            config, 'KOL_SLA_CARD_FRANKIE_ONLY', False), patch.object(
            feishu, 'resolve_partnership_targets', new=AsyncMock(return_value=[('BD','on_bd')])), patch.object(
            feishu, 'resolve_notify_targets', new=AsyncMock(return_value=[('old','on_old')])), patch.object(
            sla_check, 'build_sla_digest_card', side_effect=lambda items,*a,**k: items), patch.object(
            sla_check, '_send_digest', new=AsyncMock(return_value=delivered)) as sender:
            asyncio.run(sla_check._send_routed_review_digest(rows,0,[],'P2'))
            self.assertEqual(sender.await_args_list[0].args, ([('BD','on_bd')], [rows[1]]))
            self.assertEqual(sender.await_args_list[1].args, ([('old','on_old')], [rows[0]]))

    def test_draft_scope_excludes_prospecting(self):
        for source in ('cold', 'followup', 'secondary', '', 'unknown'):
            self.assertFalse(feishu.is_existing_partnership_draft({'邮件草稿来源': source}))
        for source in ('reply', 'affiliate_quote', 'ship_confirm', 'tracking_followup', 'warm_recap'):
            self.assertTrue(feishu.is_existing_partnership_draft({'邮件草稿来源': source}))
        for prefix in ('reminder-', 'nudge-'):
            self.assertTrue(feishu.is_existing_partnership_draft(
                {'邮件草稿来源':'followup', '邮件草稿ID':prefix+'test'}))

    def test_partner_stage_defaults_to_frankie(self):
        with patch.object(config, 'KOL_PARTNERSHIP_JOB_TITLE', '商务BD专员'), patch.object(
            config, 'KOL_PARTNERSHIP_FRANKIE_ONLY', True), patch.object(
            feishu, 'resolve_notify_targets', new=AsyncMock(return_value=[])) as resolve:
            asyncio.run(feishu.resolve_partnership_targets())
            resolve.assert_awaited_once_with('frankie')

    def test_partner_live_queries_separate_title(self):
        with patch.object(config, 'KOL_PARTNERSHIP_JOB_TITLE', '商务BD专员'), patch.object(
            config, 'KOL_PARTNERSHIP_FRANKIE_ONLY', False), patch.object(
            feishu, 'resolve_notify_targets', new=AsyncMock(return_value=[])) as resolve:
            asyncio.run(feishu.resolve_partnership_targets('ship_main'))
            resolve.assert_awaited_once_with('ship_main', job_title='商务BD专员')

    def test_cold_keeps_old_route_even_when_partner_enabled(self):
        with patch.object(config, 'KOL_PARTNERSHIP_JOB_TITLE', '商务BD专员'), patch.object(
            feishu, 'resolve_notify_targets', new=AsyncMock(return_value=[])) as resolve:
            asyncio.run(feishu.resolve_draft_notify_targets('reviewer', {'邮件草稿来源':'cold'}))
            resolve.assert_awaited_once_with('reviewer')

    def test_site_override_checks_active_identity_and_stages(self):
        user = {'name':'陈翔宇', 'union_id':'on_chen', 'status':{'is_activated':True}}
        with patch.object(cs_dispatch, 'TEMP_SITE_OPERATOR', '陈翔宇'), patch.object(
            cs_dispatch, 'TEMP_SITE_FRANKIE_ONLY', True), patch.object(
            feishu, 'api', new=AsyncMock(return_value={'data':{'user':user}})):
            targets, route = asyncio.run(cs_dispatch._resolve_targets('独立站运营专员'))
            self.assertEqual(targets, [('Frankie', cs_dispatch.OBSERVE_UNION)])
            self.assertEqual(route, 'temporary_site_frankie_test')
            user['status']['is_resigned'] = True
            targets, route = asyncio.run(cs_dispatch._resolve_targets('独立站运营专员'))
            self.assertEqual(targets, [])
            self.assertEqual(route, 'temporary_site_blocked:identity_inactive_or_mismatch')

    def test_site_live_and_other_platform_unchanged(self):
        user = {'name':'陈翔宇', 'union_id':'on_chen', 'status':{'is_activated':True}}
        with patch.object(cs_dispatch, 'TEMP_SITE_OPERATOR', '陈翔宇'), patch.object(
            cs_dispatch, 'TEMP_SITE_FRANKIE_ONLY', False), patch.object(
            feishu, 'api', new=AsyncMock(return_value={'data':{'user':user}})):
            targets, route = asyncio.run(cs_dispatch._resolve_targets('独立站运营专员'))
            self.assertEqual(targets, [('陈翔宇', 'on_chen')])
            self.assertEqual(route, 'temporary_site_operator')
            _, route = asyncio.run(cs_dispatch._resolve_targets('陈翔宇'))
            self.assertEqual(route, 'assigned_operator')

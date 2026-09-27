import asyncio
import unittest
from unittest.mock import AsyncMock, patch
from app import config, cs_dispatch, feishu, sla_check


class TemporaryHandoffTests(unittest.TestCase):
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
            self.assertEqual(route, 'job_title_no_active_user')

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

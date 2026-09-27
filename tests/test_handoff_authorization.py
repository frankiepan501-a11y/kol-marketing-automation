import asyncio
import unittest
from unittest.mock import AsyncMock, patch
from app import config, feishu, cs_dispatch, handoff_authorization as gate


class HandoffAuthorizationTests(unittest.IsolatedAsyncioTestCase):
    async def test_kol_current_owner_only_and_namespace(self):
        data={'card_action':{'action':'draft_reject','record_id':'test','_delivery_identity':'kol_assistant'},'sender_open_id':'ou_test'}
        with patch.object(config,'KOL_PARTNERSHIP_JOB_TITLE','商务BD专员'), patch.object(
            feishu,'get_record',new=AsyncMock(return_value={'fields':{'邮件草稿来源':'reply'}})), patch.object(
            feishu,'resolve_partnership_targets',new=AsyncMock(return_value=[('BD','on_bd')])), patch.object(
            feishu,'open_id_to_union_id',new=AsyncMock(return_value='on_old')) as identity:
            self.assertFalse((await gate.authorize_kol(data))['allowed'])
            identity.assert_awaited_with('ou_test',which='kol_assistant')
            identity.return_value='on_bd'
            self.assertTrue((await gate.authorize_kol(data))['allowed'])

    async def test_kol_cold_unchanged_and_unknown_record_denied(self):
        data={'card_action':{'action':'draft_approve','record_id':'test'}}
        with patch.object(config,'KOL_PARTNERSHIP_JOB_TITLE','商务BD专员'), patch.object(
            feishu,'get_record',new=AsyncMock(return_value={'fields':{'邮件草稿来源':'cold'}})) as get:
            self.assertEqual(await gate.authorize_kol(data),{'allowed':True,'scoped':False})
            get.side_effect=RuntimeError('offline')
            self.assertFalse((await gate.authorize_kol(data))['allowed'])

    async def test_cross_table_denied_without_read(self):
        data={'card_action':{'action':'draft_approve','record_id':'test','table_id':'unrelated'}}
        with patch.object(config,'KOL_PARTNERSHIP_JOB_TITLE','商务BD专员'),patch.object(feishu,'get_record',new=AsyncMock()) as get:
            self.assertFalse((await gate.authorize_kol(data))['allowed'])
            get.assert_not_awaited()

    async def test_cs_departed_cannot_mutate_or_rebuild_cards(self):
        e={'operator':{'union_id':'on_departed'},'action':{'value':{'act':'escalate','rid':'test'}}}
        with patch.object(cs_dispatch,'TEMP_SITE_OPERATOR','陈翔宇'),patch.object(
            feishu,'api',new=AsyncMock(return_value={'data':{'record':{'fields':{'销售平台':'独立站','状态':'待回'}}}})) as api,patch.object(
            cs_dispatch,'_resolve_targets',new=AsyncMock(return_value=([('陈翔宇','on_chen')],''))),patch.object(
            cs_dispatch,'_spawn') as spawn:
            result=await cs_dispatch.handle_callback(e)
            self.assertTrue(result['handoff_denied'])
            await cs_dispatch._show_callback_error(e,result)
            self.assertEqual(api.await_count,1)
            spawn.assert_not_called()

    async def test_cs_valid_owner_duplicate_has_no_business_write(self):
        e={'operator':{'union_id':'on_chen'},'action':{'value':{'act':'escalate','rid':'test'}}}
        def close_task(coro): coro.close()
        with patch.object(cs_dispatch,'TEMP_SITE_OPERATOR','陈翔宇'),patch.object(
            feishu,'api',new=AsyncMock(return_value={'data':{'record':{'fields':{'销售平台':'独立站','状态':'已升级'}}}})) as api,patch.object(
            cs_dispatch,'_resolve_targets',new=AsyncMock(return_value=([('陈翔宇','on_chen')],''))),patch.object(
            cs_dispatch,'_spawn',side_effect=close_task):
            result=await cs_dispatch.handle_callback(e)
            self.assertIn('无需重复',result['toast']['content'])
            self.assertEqual(api.await_count,1)

    async def test_denial_patches_only_original_card_without_actions(self):
        with patch.object(cs_dispatch,'_update_card',new=AsyncMock(return_value=True)) as update,patch.object(
            feishu,'api',new=AsyncMock()) as api:
            await cs_dispatch._show_callback_error({'context':{'open_message_id':'om_old'}},{'handoff_denied':True})
            self.assertEqual(update.await_args.args[0],'om_old')
            self.assertNotIn('actions',str(update.await_args.args[1]))
            api.assert_not_awaited()

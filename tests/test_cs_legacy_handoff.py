import asyncio
import copy
import unittest
from unittest.mock import AsyncMock, Mock, patch
from app import cs_dispatch as cs


class LegacyHandoffTests(unittest.IsolatedAsyncioTestCase):
    def shapes(self):
        action={'value':{'act':'escalate','rid':'test-legacy'}}
        return [{'open_id':'ou_chen','action':action},
                {'operator':{'open_id':'ou_chen','union_id':'on_chen'},'action':action}]

    async def test_both_shapes_resolve_same_owner(self):
        response=Mock()
        response.json.return_value={'code':0,'data':{'user':{'union_id':'on_chen'}}}
        client=AsyncMock()
        client.__aenter__.return_value=client
        client.get.return_value=response
        def discard(coro): coro.close()
        with patch.object(cs,'TEMP_SITE_OPERATOR','陈翔宇'), patch.object(cs.feishu,'api',AsyncMock(return_value={'data':{'record':{'fields':{'销售平台':'独立站','状态':'已升级'}}}})), patch.object(cs,'_resolve_targets',AsyncMock(return_value=([('陈翔宇','on_chen')],''))), patch.object(cs,'_token',AsyncMock(return_value='test')), patch.object(cs.httpx,'AsyncClient',return_value=client), patch.object(cs,'_spawn',side_effect=discard):
            for event in self.shapes():
                result=await cs.handle_callback(event)
                self.assertFalse(result.get('handoff_denied'),result)
            self.assertIn('ou_chen',client.get.await_args.args[0])

    def test_normalization_is_nonmutating_and_preserves_operator(self):
        event={'open_id':'ou_legacy','operator':{'union_id':'on_current'}}
        before=copy.deepcopy(event)
        self.assertEqual(cs.normalize_callback_event(event)['operator'],event['operator'])
        self.assertEqual(event,before)
        self.assertEqual(cs.normalize_callback_event({'event':{'open_id':'ou_legacy'}})['operator'],{'open_id':'ou_legacy'})

    async def test_dual_delivery_claims_once_in_either_order(self):
        for reverse in (False,True):
            events=self.shapes()[::(-1 if reverse else 1)]
            for e in events:e['action']={'value':{'act':'send_reply','rid':'test-dual'}}
            tasks=[]
            def save(coro): tasks.append(asyncio.create_task(coro))
            handler=AsyncMock(return_value={'toast':{'type':'success','content':'fixture only'}})
            with patch.object(cs,'CS_REPLY_LIVE',True),patch.object(cs,'_callback_fast_inflight',set()),patch.object(cs,'_inflight',set()),patch.object(cs,'_recent_seen',return_value=False),patch.object(cs,'_spawn',side_effect=save),patch.object(cs,'handle_callback',handler):
                await cs.handle_callback_fast(events[0])
                await cs.handle_callback_fast(events[1])
                await asyncio.gather(*tasks)
                self.assertEqual(handler.await_count,1)
                self.assertEqual(handler.await_args.args[0]['operator']['open_id'],'ou_chen')

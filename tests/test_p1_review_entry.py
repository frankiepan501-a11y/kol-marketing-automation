import asyncio
import json
import unittest
from unittest.mock import AsyncMock, patch
import test_sla_digest  # supplies isolated test config
from app import draft_router, sla_check, feishu


class ReviewEntryTests(unittest.TestCase):
    def row(self, rid='rec_one', **extra):
        return {'record_id': rid, 'fields': {'邮件草稿状态':'待审', '邮件草稿来源':'cold',
            'AI评分':7, 'AI评分理由':'[hybrid-ai-exception] fixed human review',
            '邮件正文':'Review body', '审核路径':'待人审', **extra}}

    def test_scored_exception_is_not_bulk_repaired_or_rescored(self):
        notify = AsyncMock(return_value={'action_delivered':True})
        route = AsyncMock()
        with patch.object(feishu,'search_records',AsyncMock(return_value=[self.row()])), \
             patch.object(draft_router,'_notify_human_review',notify), \
             patch.object(draft_router,'route_draft',route):
            asyncio.run(draft_router.batch_review_pending())
        notify.assert_not_awaited()
        route.assert_not_awaited()

    def test_existing_card_and_other_records_are_not_resent(self):
        rows=[self.row('old', **{'卡片个人消息IDs':'{"on_x":"om_old"}'})]+[self.row(str(i)) for i in range(3)]
        notify=AsyncMock(return_value={'action_delivered':True})
        with patch.object(feishu,'search_records',AsyncMock(return_value=rows)), patch.object(draft_router,'_notify_human_review',notify):
            asyncio.run(draft_router.batch_review_pending())
        notify.assert_not_awaited()

    def test_cold_reminder_uses_configured_role_and_card_instructions(self):
        with patch.object(sla_check.config,'KOL_REVIEWER_JOB_TITLE','商务BD专员'):
            card=sla_check.build_sla_digest_card([self.row()],1790745084346,audience='reviewer',level='P2')
        text=json.dumps(card,ensure_ascii=False)
        self.assertIn('商务BD专员',text)
        self.assertNotIn('独立站运营专员',text)
        self.assertNotIn('实际审核在邮件草稿表完成',text)
        self.assertIn('审核卡',text)

    def test_review_card_does_not_require_table_edit_permission(self):
        card=draft_router._build_review_action_card('rec_test',self.row(),7,'summary','','待人审','cold','KOL','product','FUNLAB','https://example.com')
        text=json.dumps(card,ensure_ascii=False)
        self.assertNotIn('去表格改',text)
        self.assertIn('退回重生',text)
        self.assertIn('record=rec_test',text)

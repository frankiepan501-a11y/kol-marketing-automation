import unittest

from app import draft_regen


class DraftRegenDurableIdempotencyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.original_get = draft_regen.feishu.get_record
        self.original_search = draft_regen.feishu.search_records
        self.original_update = draft_regen.feishu.update_record
        self.original_chat = draft_regen.deepseek.chat_json

    async def asyncTearDown(self):
        draft_regen.feishu.get_record = self.original_get
        draft_regen.feishu.search_records = self.original_search
        draft_regen.feishu.update_record = self.original_update
        draft_regen.deepseek.chat_json = self.original_chat

    async def test_existing_regenerated_draft_is_reused_after_process_restart(self):
        updates = []

        async def fake_get(table_id, record_id):
            return {
                "record_id": record_id,
                "fields": {
                    "邮件草稿状态": "待审",
                    "重生次数": 0,
                    "邮件草稿ID": "draft-old",
                },
            }

        async def fake_search(table_id, conditions, field_names=None):
            self.assertEqual("邮件草稿ID", conditions[0]["field_name"])
            self.assertEqual(["draft-old-rg1"], conditions[0]["value"])
            return [{
                "record_id": "rec_existing",
                "fields": {
                    "邮件草稿ID": "draft-old-rg1",
                    "邮件草稿状态": "待审",
                    "卡片群消息ID": "om_existing",
                },
            }]

        async def fake_update(table_id, record_id, fields):
            updates.append((record_id, fields))

        async def fail_if_model_called(*args, **kwargs):
            raise AssertionError("durable idempotency must reuse the existing draft")

        draft_regen.feishu.get_record = fake_get
        draft_regen.feishu.search_records = fake_search
        draft_regen.feishu.update_record = fake_update
        draft_regen.deepseek.chat_json = fail_if_model_called

        result = await draft_regen.regen_draft("rec_old", feedback="warmer")

        self.assertTrue(result["ok"])
        self.assertTrue(result["reused"])
        self.assertEqual("rec_existing", result["new_rid"])
        self.assertEqual("已否决", updates[0][1]["邮件草稿状态"])


if __name__ == "__main__":
    unittest.main()

import unittest

from app import draft_regen, draft_router


class DraftRegenDurableIdempotencyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.original_get = draft_regen.feishu.get_record
        self.original_search = draft_regen.feishu.search_records
        self.original_update = draft_regen.feishu.update_record
        self.original_create = draft_regen.feishu.create_record
        self.original_chat = draft_regen.deepseek.chat_json
        self.original_route = draft_router.route_draft

    async def asyncTearDown(self):
        draft_regen.feishu.get_record = self.original_get
        draft_regen.feishu.search_records = self.original_search
        draft_regen.feishu.update_record = self.original_update
        draft_regen.feishu.create_record = self.original_create
        draft_regen.deepseek.chat_json = self.original_chat
        draft_router.route_draft = self.original_route

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

    async def test_denied_old_draft_reuses_existing_replacement_and_repairs_missing_card(self):
        routed = []

        async def fake_get(table_id, record_id):
            return {
                "record_id": record_id,
                "fields": {
                    "邮件草稿状态": "已否决",
                    "重生次数": 0,
                    "邮件草稿ID": "draft-old",
                },
            }

        async def fake_search(table_id, conditions, field_names=None):
            return [{
                "record_id": "rec_existing",
                "fields": {
                    "邮件草稿ID": "draft-old-rg1",
                    "邮件草稿状态": "待审",
                },
            }]

        async def fake_update(table_id, record_id, fields):
            return None

        async def fake_route(record_id, force_review_reason=""):
            routed.append(record_id)
            return {"routed": True}

        async def fail_if_model_called(*args, **kwargs):
            raise AssertionError("restart recovery must not call DeepSeek again")

        draft_regen.feishu.get_record = fake_get
        draft_regen.feishu.search_records = fake_search
        draft_regen.feishu.update_record = fake_update
        draft_regen.deepseek.chat_json = fail_if_model_called
        draft_router.route_draft = fake_route

        result = await draft_regen.regen_draft("rec_old")

        self.assertTrue(result["ok"])
        self.assertTrue(result["reused"])
        self.assertEqual(["rec_existing"], routed)

    async def test_reused_replacement_returns_failure_when_review_card_routing_fails(self):
        async def fake_get(table_id, record_id):
            return {
                "record_id": record_id,
                "fields": {
                    "邮件草稿状态": "已否决",
                    "重生次数": 0,
                    "邮件草稿ID": "draft-old",
                },
            }

        async def fake_search(table_id, conditions, field_names=None):
            return [{
                "record_id": "rec_existing",
                "fields": {"邮件草稿ID": "draft-old-rg1", "邮件草稿状态": "待审"},
            }]

        async def fake_update(table_id, record_id, fields):
            return None

        async def fail_route(record_id, force_review_reason=""):
            raise RuntimeError("card delivery unavailable")

        draft_regen.feishu.get_record = fake_get
        draft_regen.feishu.search_records = fake_search
        draft_regen.feishu.update_record = fake_update
        draft_router.route_draft = fail_route

        result = await draft_regen.regen_draft("rec_old")

        self.assertFalse(result["ok"])
        self.assertEqual("rec_existing", result["new_rid"])
        self.assertIn("card delivery unavailable", result["error"])

    async def test_new_replacement_returns_failure_when_review_card_routing_fails(self):
        async def fake_get(table_id, record_id):
            return {
                "record_id": record_id,
                "fields": {
                    "邮件草稿状态": "待审",
                    "重生次数": 0,
                    "邮件草稿ID": "draft-old",
                    "邮件主题": "Old subject",
                    "邮件正文": "Old body",
                },
            }

        async def fake_search(table_id, conditions, field_names=None):
            return []

        async def fake_update(table_id, record_id, fields):
            return None

        async def fake_create(table_id, fields):
            return "rec_new"

        async def fake_chat(*args, **kwargs):
            return {
                "subject": "Better subject",
                "body": "A complete replacement draft body that is comfortably longer than forty characters.",
            }

        async def fail_route(record_id, force_review_reason=""):
            raise RuntimeError("card delivery unavailable")

        draft_regen.feishu.get_record = fake_get
        draft_regen.feishu.search_records = fake_search
        draft_regen.feishu.update_record = fake_update
        draft_regen.feishu.create_record = fake_create
        draft_regen.deepseek.chat_json = fake_chat
        draft_router.route_draft = fail_route

        result = await draft_regen.regen_draft("rec_old")

        self.assertFalse(result["ok"])
        self.assertEqual("rec_new", result["new_rid"])
        self.assertIn("card delivery unavailable", result["error"])


if __name__ == "__main__":
    unittest.main()

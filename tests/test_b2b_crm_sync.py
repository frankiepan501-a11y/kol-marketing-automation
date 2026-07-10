import unittest

from app import b2b_crm_sync


class B2BCrmSyncTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._orig_find = b2b_crm_sync._find_customer_match
        self._orig_get = b2b_crm_sync._get_record
        self._orig_create = b2b_crm_sync._create_record
        self._orig_update = b2b_crm_sync._update_record
        self.created = []
        self.updated = []

        async def fake_create(table_id, fields):
            rid = "rec_customer" if table_id == b2b_crm_sync.B2B_CUSTOMER_TABLE else f"rec_follow_{len(self.created)}"
            self.created.append((table_id, rid, fields))
            return rid

        async def fake_update(table_id, record_id, fields):
            self.updated.append((table_id, record_id, fields))

        b2b_crm_sync._create_record = fake_create
        b2b_crm_sync._update_record = fake_update

    async def asyncTearDown(self):
        b2b_crm_sync._find_customer_match = self._orig_find
        b2b_crm_sync._get_record = self._orig_get
        b2b_crm_sync._create_record = self._orig_create
        b2b_crm_sync._update_record = self._orig_update

    async def test_outreach_sent_creates_customer_and_followup(self):
        async def fake_find(**kwargs):
            return None, "created"

        b2b_crm_sync._find_customer_match = fake_find
        result = await b2b_crm_sync.sync_outreach_sent(
            {
                "record_id": "rec_queue",
                "company": "Cenega",
                "email": "marek@cenega.pl",
                "owner": "吴晓丹",
                "sender": "silvia.wu@powkong.com",
                "subject": "switch 2 accessories",
            },
            {"国家/地区": "Poland", "公司类型": "分销商", "主营类目": "video games distribution", "AI建议等级": "B-可开发"},
            message_id="<msg-1@powkong.com>",
            batch_id="batch-1",
            sent_at_ms=1783318147000,
        )

        self.assertTrue(result["ok"])
        self.assertEqual("rec_customer", result["customer_record_id"])
        self.assertTrue(result["customer_created"])
        self.assertEqual(2, len(self.created))
        customer_fields = self.created[0][2]
        self.assertEqual("Cenega", customer_fields["公司名称"])
        self.assertEqual("已发开发邮件", customer_fields["合作状态"])
        self.assertEqual("领英", customer_fields["客户来源"])
        follow_fields = self.created[1][2]
        self.assertEqual(["rec_customer"], follow_fields["关联客户"])
        self.assertEqual("邮件", follow_fields["跟进方式"])
        self.assertIn("<msg-1@powkong.com>", follow_fields["跟进内容"])

    async def test_inbound_reply_skips_same_message_id(self):
        result = await b2b_crm_sync.sync_inbound_reply(
            {
                "record_id": "rec_customer",
                "last_in_at": "2026-07-06T10:00:00+08:00",
                "last_in_message_id": "<same@example.com>",
            },
            {"fields": {"最后来信Message-ID": "<same@example.com>"}},
        )

        self.assertEqual("same_message_id", result["skipped"])
        self.assertEqual([], self.created)
        self.assertEqual([], self.updated)

    async def test_mail_receipt_creates_followup_and_updates_customer_log(self):
        async def fake_get(table_id, record_id):
            self.assertEqual(b2b_crm_sync.B2B_CUSTOMER_TABLE, table_id)
            return {"record_id": record_id, "fields": {"公司名称": "Cenega", "合作状态": "已发开发邮件", "跟进日志": ""}}

        b2b_crm_sync._get_record = fake_get
        result = await b2b_crm_sync.sync_mail_receipt_to_customer(
            {
                "fields": {
                    "关联CRM客户": [{"record_ids": ["rec_customer"]}],
                    "客户/域名": "Cenega",
                    "最后来信主题": "Re: switch 2 accessories",
                    "外部邮箱": "marek@cenega.pl",
                    "最后来信Message-ID": "<reply@example.com>",
                }
            },
            receipt_type="已邮件回复",
            actor="吴晓丹",
            note="已回复产品资料",
            channels=[],
        )

        self.assertTrue(result["ok"])
        self.assertEqual("rec_customer", result["customer_record_id"])
        self.assertEqual(1, len(self.updated))
        update_fields = self.updated[0][2]
        self.assertEqual("初步建联", update_fields["合作状态"])
        self.assertIn("已回复产品资料", update_fields["跟进日志"])
        self.assertEqual(1, len(self.created))
        self.assertEqual("邮件", self.created[0][2]["跟进方式"])

    async def test_linkedin_contact_left_keeps_customer_open(self):
        async def fake_find(**kwargs):
            return {
                "record_id": "rec_customer",
                "fields": {
                    "公司名称": "Extra Stores",
                    "合作状态": "未联系",
                    "跟进日志": "",
                },
            }, "company"

        b2b_crm_sync._find_customer_match = fake_find
        result = await b2b_crm_sync.sync_linkedin_contact_left(
            "rec_lead",
            {
                "公司名称": "Extra Stores",
                "联系人姓名": "Wael Abuzaid",
                "职位": "Finance & Business Development Director",
                "跟进人": "冼浩华",
                "国家/地区": "Saudi Arabia",
                "LinkedIn联系人页": {"link": "https://www.linkedin.com/in/wael", "text": "LinkedIn"},
            },
            actor="冼浩华",
            note="LinkedIn显示5月离职",
        )

        self.assertTrue(result["ok"])
        self.assertEqual("rec_customer", result["customer_record_id"])
        self.assertFalse(result["customer_created"])
        self.assertEqual(1, len(self.updated))
        update_fields = self.updated[0][2]
        self.assertNotIn("合作状态", update_fields)
        self.assertIn("Wael Abuzaid", update_fields["跟进日志"])
        self.assertIn("已离职", update_fields["跟进日志"])
        self.assertEqual(1, len(self.created))
        follow_fields = self.created[0][2]
        self.assertEqual("LinkedIn", follow_fields["跟进方式"])
        self.assertEqual("联系人已离职", follow_fields["客户反馈"])
        self.assertIn("重新找", follow_fields["下一步行动"])

    async def test_linkedin_contact_left_is_idempotent_for_same_lead(self):
        async def fake_find(**kwargs):
            return {
                "record_id": "rec_customer",
                "fields": {
                    "公司名称": "Extra Stores",
                    "合作状态": "未联系",
                    "跟进日志": "2026-07-10 11:43 冼浩华 [LinkedIn] LinkedIn核验：联系人 Wael Abuzaid / Finance & Business Development Director 已离职或不在当前客户公司；客户公司仍保留开发价值，下一步重新找采购/BD/Category/Product 相关联系人。",
                    "跟进人": "冼浩华",
                    "开发人": "冼浩华",
                    "客户来源": "领英",
                },
            }, "company"

        b2b_crm_sync._find_customer_match = fake_find
        result = await b2b_crm_sync.sync_linkedin_contact_left(
            "rec_lead",
            {
                "公司名称": "Extra Stores",
                "联系人姓名": "Wael Abuzaid",
                "职位": "Finance & Business Development Director",
                "跟进人": "冼浩华",
            },
            actor="冼浩华",
        )

        self.assertTrue(result["ok"])
        self.assertTrue(result["already_logged"])
        self.assertEqual("", result["followup_record_id"])
        self.assertEqual([], self.created)
        self.assertEqual([], self.updated)


if __name__ == "__main__":
    unittest.main()

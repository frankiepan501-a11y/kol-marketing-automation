import unittest

from app import b2b_assistant, b2b_linkedin_daily_card


def _walk(obj):
    if isinstance(obj, dict):
        yield obj
        for value in obj.values():
            yield from _walk(value)
    elif isinstance(obj, list):
        for item in obj:
            yield from _walk(item)


class B2BLinkedInContactLeftTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._orig_get = b2b_assistant._get_record
        self._orig_update = b2b_assistant._update_record
        self._orig_send = b2b_assistant._send_reply
        self._orig_operator = b2b_assistant._operator_name
        self._orig_crm = b2b_assistant.b2b_crm_sync.sync_linkedin_contact_left
        self.updated = []

        async def fake_get(table_id, record_id):
            return {
                "record_id": record_id,
                "fields": {
                    "公司名称": "Extra Stores",
                    "联系人姓名": "Wael Abuzaid",
                    "职位": "Finance & Business Development Director",
                    "开发状态": "待开发",
                    "触达状态": "待触达",
                    "跟进人": "冼浩华",
                    "备注": "",
                },
            }

        async def fake_update(table_id, record_id, fields):
            self.updated.append((table_id, record_id, fields))

        async def fake_send(payload, text):
            return {"message_id": "om_test", "text": text}

        async def fake_operator(open_id):
            return "冼浩华"

        async def fake_crm(lead_record_id, lead_fields, *, actor, note=""):
            return {
                "ok": True,
                "customer_record_id": "rec_customer",
                "customer_created": False,
                "matched_by": "company",
                "followup_record_id": "rec_follow",
            }

        b2b_assistant._get_record = fake_get
        b2b_assistant._update_record = fake_update
        b2b_assistant._send_reply = fake_send
        b2b_assistant._operator_name = fake_operator
        b2b_assistant.b2b_crm_sync.sync_linkedin_contact_left = fake_crm

    async def asyncTearDown(self):
        b2b_assistant._get_record = self._orig_get
        b2b_assistant._update_record = self._orig_update
        b2b_assistant._send_reply = self._orig_send
        b2b_assistant._operator_name = self._orig_operator
        b2b_assistant.b2b_crm_sync.sync_linkedin_contact_left = self._orig_crm

    async def test_receipt_marks_contact_left_and_links_crm(self):
        result = await b2b_assistant._handle_linkedin_receipt(
            {
                "sender_open_id": "ou_ahua",
                "card_action": {
                    "record_id": "rec_lead",
                    "action": "linkedin_contact_left",
                    "company": "Extra Stores",
                },
                "card_form_value": {"linkedin_note": "LinkedIn显示5月离职"},
            }
        )

        self.assertTrue(result["ok"])
        self.assertEqual("linkedin_contact_left", result["action"])
        self.assertEqual(1, len(self.updated))
        fields = self.updated[0][2]
        self.assertEqual("联系人已离职", fields["开发状态"])
        self.assertEqual("联系人失效", fields["触达状态"])
        self.assertEqual("联系人已离职", fields["触达验证结果"])
        self.assertEqual("rec_customer", fields["CRM记录ID"])
        self.assertEqual(["rec_customer"], fields["关联CRM客户"])
        self.assertIn("LinkedIn显示5月离职", fields["备注"])
        self.assertIn("CRM同步：已记录联系人离职", result["reply"])

    def test_daily_card_includes_contact_left_button(self):
        card = b2b_linkedin_daily_card.build_card(
            [
                {
                    "record_id": "rec_lead",
                    "company": "Extra Stores",
                    "contact": "Wael Abuzaid",
                    "position": "Finance & Business Development Director",
                    "country": "Saudi Arabia",
                    "company_type": "零售商",
                    "linkedin_company": "",
                    "linkedin_profile": "https://www.linkedin.com/in/wael",
                    "website": "https://www.extra.com",
                    "url": "https://u1wpma3xuhr.feishu.cn/base/x?record=rec_lead",
                    "grade": "A-优先开发",
                    "score": 84,
                    "dev_status": "待开发",
                    "reach_status": "待触达",
                    "assignment_reason": "非指定国家平均派发",
                    "reason": "符合 B2B 相似客户开发逻辑",
                    "connect_copy": "Hi Wael, open to connect?",
                    "message_copy": "Thanks for connecting.",
                }
            ],
            owner_name="冼浩华",
        )

        buttons = [node for node in _walk(card) if node.get("tag") == "button"]
        contact_left = [
            b for b in buttons
            if ((b.get("text") or {}).get("content") == "👤 联系人已离职")
        ]
        self.assertEqual(1, len(contact_left))
        self.assertEqual("linkedin_contact_left", contact_left[0]["value"]["action"])

    async def test_daily_card_can_resend_specific_record(self):
        orig_get_record = b2b_linkedin_daily_card._get_record

        async def fake_get_record(record_id):
            return {
                "record_id": record_id,
                "fields": {
                    "线索名称": "Extra Stores - Wael Abuzaid",
                    "公司名称": "Extra Stores",
                    "联系人姓名": "Wael Abuzaid",
                    "职位": "Finance & Business Development Director",
                    "国家/地区": "Saudi Arabia",
                    "公司类型": "零售商",
                    "AI建议等级": "A-优先开发",
                    "AI开发评分": 84,
                    "开发状态": "联系人已离职",
                    "触达状态": "联系人失效",
                    "跟进人": "冼浩华",
                },
            }

        b2b_linkedin_daily_card._get_record = fake_get_record
        try:
            result = await b2b_linkedin_daily_card.run(
                commit=False,
                notify=False,
                record_id="rec_lead",
                owner="冼浩华",
            )
        finally:
            b2b_linkedin_daily_card._get_record = orig_get_record

        self.assertEqual("rec_lead", result["record_id"])
        self.assertEqual({"冼浩华": 1}, result["groups"])
        self.assertEqual("Extra Stores", result["preview"]["冼浩华"][0]["company"])
        self.assertEqual(1, result["assignment_stats"]["direct_record"])


if __name__ == "__main__":
    unittest.main()

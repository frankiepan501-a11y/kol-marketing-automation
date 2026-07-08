import json
import unittest

from app import cs_dispatch, cs_ingest


def base_msg(body: str, subj: str = "Firefly Pro Controller issue"):
    return {
        "id": "<inbound-1@example.com>",
        "id_prefix": "CSF",
        "frm": "pennrican1@gmail.com",
        "subj": subj,
        "body": body,
        "channel": "邮箱",
        "brand_default": "FUNLAB",
        "received_ms": 1783480000000,
        "in_reply_to": "",
        "references": "",
    }


class CsInfoRequestTests(unittest.TestCase):
    def setUp(self):
        self._old_observe = cs_dispatch.OBSERVE
        cs_dispatch.OBSERVE = False

    def tearDown(self):
        cs_dispatch.OBSERVE = self._old_observe

    def test_amazon_without_order_or_site_waits_for_customer_info(self):
        msg = base_msg(
            "I bought this on Amazon and the XR and XL buttons activate by themselves. "
            "I may leave a review if this is not fixed."
        )
        fields = cs_ingest._to_fields(msg, {
            "is_cs": True,
            "is_amazon": True,
            "brand": "FUNLAB",
            "platform": "未知",
            "complaint_type": "产品",
            "product": "Firefly Pro Controller",
            "order_no": "",
            "language": "EN",
            "summary": "客户反馈 Firefly Pro 手柄按键自动触发，未提供订单号和站点。",
            "confidence": "AI起草人工审",
            "draft_reply": "bad draft",
        }, resources=[])

        self.assertEqual("待客户补充", fields["状态"])
        self.assertEqual("待定·领星反查站点", fields["分配运营"])
        self.assertIn("缺订单号", fields["信息缺口"])
        self.assertIn("缺国家站点", fields["信息缺口"])
        self.assertIn("order number", fields["AI草稿"])
        self.assertIn("marketplace/country", fields["AI草稿"])
        self.assertNotIn("free replacement", fields["AI草稿"].lower())
        self.assertNotIn("refund", fields["AI草稿"].lower())

    def test_amazon_site_hint_routes_but_marks_order_missing(self):
        msg = base_msg("I purchased it from Amazon Mexico and the controller is defective.")
        fields = cs_ingest._to_fields(msg, {
            "is_cs": True,
            "is_amazon": True,
            "brand": "FUNLAB",
            "complaint_type": "产品",
            "product": "Firefly Pro Controller",
            "order_no": "",
            "language": "EN",
            "summary": "客户反馈产品故障，提供 Amazon Mexico 站点但未提供订单号。",
            "confidence": "AI起草人工审",
            "draft_reply": "Please share a video of the issue.",
        }, amz_override=("亚马逊-墨西哥", "陈翔宇", "site_hint"), resources=[])

        self.assertEqual("待派", fields["状态"])
        self.assertEqual("亚马逊-墨西哥", fields["销售平台"])
        self.assertEqual("陈翔宇", fields["分配运营"])
        self.assertIn("缺订单号", fields["信息缺口"])

    def test_waiting_ticket_match_uses_outbound_message_id(self):
        incoming = base_msg("My order number is 123-4567890-1234567.", "Re: Firefly issue")
        incoming["id"] = "<customer-reply@example.com>"
        incoming["in_reply_to"] = "<outbound-info-request@funlabswitch.com>"
        waiting = [{
            "record_id": "rec_wait",
            "fields": {
                "状态": "待客户补充",
                "客户标识": "pennrican1@gmail.com",
                "线程ID": "<inbound-1@example.com>",
                "最近出站Message-ID": "<outbound-info-request@funlabswitch.com>",
            },
        }]

        matched = cs_ingest._match_waiting_info_ticket(incoming, waiting)
        self.assertEqual("rec_wait", matched["record_id"])

    def test_card_renders_handoff_context_for_operator(self):
        fields = {
            "工单ID": "CSF-<inbound-1@example.com>",
            "客户标识": "pennrican1@gmail.com",
            "品牌": ["FUNLAB"],
            "产品": "Firefly Pro Controller",
            "销售平台": ["亚马逊-墨西哥"],
            "渠道": ["邮箱"],
            "客诉类型": ["产品"],
            "AI置信度": ["AI起草人工审"],
            "分配运营": "陈翔宇",
            "客诉摘要": "客户反馈按键自动触发。",
            "AI草稿": "Dear Luis,\n\nPlease share your order number.",
            "信息缺口": "缺订单号",
            "沟通历史摘要": "首封问题: XR/XL 自动触发。\n系统补询: 已同线程发送给客户。",
            "最近客户补充": "I bought it from Amazon Mexico.",
            "最近出站Message-ID": "<outbound-info-request@funlabswitch.com>",
        }

        card = cs_dispatch._build_card("rec_context", fields, resources=[])
        rendered = json.dumps(card, ensure_ascii=False)
        self.assertIn("接手上下文", rendered)
        self.assertIn("客户补充", rendered)
        self.assertIn("缺订单号", rendered)
        self.assertIn("outbound-info-request", rendered)

    def test_info_request_safety_gate_rejects_commitment_words(self):
        self.assertEqual("", cs_ingest._info_request_is_safe(
            cs_ingest._info_request_reply({"品牌": "FUNLAB", "产品": "Firefly Pro Controller"})
        ))
        self.assertEqual("free replacement", cs_ingest._info_request_is_safe(
            "We will send a free replacement after you reply."
        ))


if __name__ == "__main__":
    unittest.main()

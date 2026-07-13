import json
import unittest
from email.message import EmailMessage

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

    def test_email_attachment_extraction_keeps_customer_image(self):
        msg = EmailMessage()
        msg["Subject"] = "Controller issue"
        msg.set_content("Please see attached image.")
        msg.add_attachment(b"fake-image-bytes", maintype="image", subtype="jpeg", filename="issue.jpg")

        attachments = cs_ingest._extract_email_attachments(msg)

        self.assertEqual(1, len(attachments))
        self.assertEqual("issue.jpg", attachments[0]["filename"])
        self.assertEqual("图片", attachments[0]["kind"])
        self.assertEqual(b"fake-image-bytes", attachments[0]["bytes"])

    def test_email_link_extraction_keeps_customer_video_url(self):
        msg = EmailMessage()
        msg["Subject"] = "Controller issue"
        msg.set_content("Plain fallback")
        msg.add_alternative(
            '<p>Video proof: <a href="https://cdn.shopify.com/s/files/1/abc/issue.mp4">issue</a></p>',
            subtype="html",
        )

        attachments = cs_ingest._extract_email_attachments(msg)

        self.assertEqual(1, len(attachments))
        self.assertEqual("链接", attachments[0]["kind"])
        self.assertEqual("https://cdn.shopify.com/s/files/1/abc/issue.mp4", attachments[0]["url"])

    def test_card_renders_customer_evidence_context(self):
        fields = {
            "工单ID": "CSF-<inbound-2@example.com>",
            "客户标识": "mailer@shopify.com",
            "品牌": ["FUNLAB"],
            "产品": "controller",
            "销售平台": ["独立站"],
            "渠道": ["邮箱"],
            "客诉类型": ["产品"],
            "AI置信度": ["AI起草人工审"],
            "分配运营": "陈翔宇",
            "客诉摘要": "客户提供图片和视频反馈按键问题。",
            "AI草稿": "Dear customer, thank you.",
            "客户附件状态": ["已保存"],
            "客户附件数量": 2,
            "客户附件JSON": json.dumps([
                {"filename": "issue.jpg", "kind": "图片", "size": 1024, "file_token": "boxcn_img"},
                {"filename": "issue.mp4", "kind": "视频", "size": 2048, "file_token": "boxcn_vid"},
            ], ensure_ascii=False),
            "客户附件摘要": "客户原始证据附件: 已保存 2 个",
        }

        card = cs_dispatch._build_card("rec_evidence", fields, resources=[])
        rendered = json.dumps(card, ensure_ascii=False)
        self.assertIn("客户证据附件", rendered)
        self.assertIn("issue.jpg", rendered)
        self.assertIn("issue.mp4", rendered)
        self.assertIn("打开工单记录", rendered)

    def test_card_renders_no_customer_evidence_checked(self):
        fields = {
            "工单ID": "CSF-<inbound-3@example.com>",
            "客户标识": "mailer@shopify.com",
            "品牌": ["FUNLAB"],
            "产品": "controller",
            "销售平台": ["亚马逊-加拿大"],
            "渠道": ["邮箱"],
            "客诉类型": ["产品"],
            "AI置信度": ["AI起草人工审"],
            "分配运营": "陈翔宇",
            "客诉摘要": "客户反馈 turbo switch glitching。",
            "AI草稿": "Dear Aisha, thank you.",
            "客户附件状态": ["无附件"],
            "客户附件数量": 0,
            "客户附件JSON": "[]",
            "客户附件摘要": "未检测到客户图片/视频/PDF附件。",
        }

        card = cs_dispatch._build_card("rec_no_evidence", fields, resources=[])
        rendered = json.dumps(card, ensure_ascii=False)
        self.assertIn("客户证据附件", rendered)
        self.assertIn("无附件", rendered)
        self.assertIn("未检测到客户图片/视频/PDF附件", rendered)


if __name__ == "__main__":
    unittest.main()

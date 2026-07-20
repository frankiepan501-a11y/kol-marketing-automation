import json
import unittest

from app import cs_dispatch


class CsDispatchCardTests(unittest.TestCase):
    def setUp(self):
        self._old_observe = cs_dispatch.OBSERVE
        cs_dispatch.OBSERVE = False

    def tearDown(self):
        cs_dispatch.OBSERVE = self._old_observe

    def test_email_message_id_is_not_rendered_in_card_title_or_body(self):
        fields = {
            "工单ID": "CSF-<CAFYLfD+G=ssqGGMQ7ZpP8-DCcv3kngR63=fWaxBgfZrDEDLmfg@mail.gmail.com>",
            "客户标识": "pennrican1@gmail.com",
            "品牌": ["FUNLAB"],
            "产品": "Firefly Pro Controller",
            "销售平台": ["未知"],
            "渠道": ["邮箱"],
            "客诉类型": ["产品"],
            "AI置信度": ["AI起草人工审"],
            "分配运营": "待定·领星反查站点",
            "客诉摘要": "XR and XL buttons activate on their own.",
            "AI草稿": "Dear Luis,\n\nWe can help with a replacement.",
        }

        card = cs_dispatch._build_card("rec_test", fields, resources=[])
        rendered = json.dumps(card, ensure_ascii=False)
        title = card["header"]["title"]["content"]

        self.assertIn("[客服·待判责]", title)
        self.assertIn("FUNLAB", title)
        self.assertIn("Firefly Pro Controller", title)
        self.assertNotIn("CAFYLfD", rendered)
        self.assertNotIn("mail.gmail.com", rendered)
        self.assertIn("CSF · rec_test", rendered)
        self.assertIn("兜底发给 Frankie 判定站点/负责人", rendered)

    def test_known_operator_card_remains_normal_waiting_reply(self):
        fields = {
            "工单ID": "CSF-<raw@mail.example.com>",
            "客户标识": "customer@example.com",
            "品牌": ["FUNLAB"],
            "产品": "FF05A controller",
            "销售平台": ["亚马逊-墨西哥"],
            "渠道": ["邮箱"],
            "客诉类型": ["产品"],
            "AI置信度": ["AI起草人工审"],
            "分配运营": "陈翔宇",
            "客诉摘要": "Need firmware.",
            "AI草稿": "Hello,\n\nPlease use the official firmware link.",
        }

        card = cs_dispatch._build_card("rec_known", fields, resources=[])
        rendered = json.dumps(card, ensure_ascii=False)

        self.assertIn("[客服·待回]", card["header"]["title"]["content"])
        self.assertNotIn("兜底发给 Frankie", rendered)

    def test_custom_reply_input_splits_2000_chars_across_two_fields(self):
        fields = {
            "工单ID": "CSF-<raw@mail.example.com>",
            "客户标识": "customer@example.com",
            "品牌": ["FUNLAB"],
            "产品": "FF01 controller",
            "销售平台": ["亚马逊-美国"],
            "渠道": ["邮箱"],
            "客诉类型": ["产品"],
            "AI置信度": ["AI起草人工审"],
            "分配运营": "陈翔宇",
            "客诉摘要": "Need firmware.",
            "AI草稿": "Hello," + "x" * 2500,
        }

        card = cs_dispatch._build_card("rec_len", fields, resources=[])
        form = next(e for e in card["elements"] if e.get("tag") == "form")
        primary = next(e for e in form["elements"] if e.get("name") == "custom_reply")
        extra = next(e for e in form["elements"] if e.get("name") == "custom_reply_extra")

        self.assertEqual(1000, primary["max_length"])
        self.assertEqual(1000, extra["max_length"])
        self.assertIn("第1段", primary["label"]["content"])
        self.assertIn("总计≤2000字", extra["label"]["content"])
        rendered = json.dumps(card, ensure_ascii=False)
        self.assertIn("Hello," + "x" * 1994, rendered)
        self.assertNotIn("Hello," + "x" * 1995, rendered)


if __name__ == "__main__":
    unittest.main()

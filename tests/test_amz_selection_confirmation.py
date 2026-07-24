import asyncio
import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import amz_assistant
from app import amz_selection_confirmation as sel


class AmzSelectionConfirmationTests(unittest.TestCase):
    def _candidate(self, rid="rec1"):
        record = {
            "record_id": rid,
            "fields": {
                "ASIN": "B0CH1817WW",
                "候选标题": "Dreame L20 Ultra replacement filter",
                "产品中文名": "Dreame L20 Ultra 扫地机替换滤网",
                "Amazon链接": {"link": "https://www.amazon.de/dp/B0CH1817WW", "text": "Listing"},
                "样本ASIN主图URL": {"link": "https://m.media-amazon.com/images/I/41Bum-N615L._AC_.jpg", "text": "Image"},
                "包装尺寸": "12.9,5.5,3.6",
                "商品重量g": "50",
                "套装件数": "2",
                "套装内容": "2个替换滤网；采购需按Amazon主图核对滤网尺寸和适配型号 Dreame L20 Ultra",
                "采购成本RMB": 4,
                "1688供应商链接": {"link": "https://detail.1688.com/offer/test.html", "text": "1688"},
                "采购回填状态": "已回填",
                "三方案推荐履约": "FBA头程-经济线",
                "FBA€": "2.75",
                "佣金€": "3.9",
                "A-采购前可用毛利RMB": "128.38",
                "A-采购前毛利率%": "58.1",
                "A-物流成本RMB": "0.74",
                "A-货运比": "0",
                "A-毛利RMB": "124.38",
                "A-毛利率%": "56.3",
                "B-采购前可用毛利RMB": "127.14",
                "B-采购前毛利率%": "57.5",
                "B-物流成本RMB": "1.98",
                "B-货运比": "0.01",
                "B-毛利RMB": "123.14",
                "B-毛利率%": "55.7",
                "C-采购前可用毛利RMB": "121.44",
                "C-采购前毛利率%": "55.0",
                "C-物流成本RMB": "31.05",
                "C-货运比": "0.17",
                "C-毛利RMB": "117.44",
                "C-毛利率%": "53.2",
                "财务闸结论": "通过",
                "合规闸结论": "Go",
                "当前状态": "待50件验证",
                "综合结论": "50件验证",
                "下一步动作": "发起50件验证",
                "侵权风险说明": "品牌词/IP：Dreame；只能写兼容/适配关系。",
                "DE样本竞品售价": 25.99,
                "DE竞品中位价": 24.99,
                "DE竞品均价": 26.5,
                "DE竞品平均月销量": 100,
                "DE类目新品平均月销量": 50,
                "UK样本竞品售价": 21.99,
                "UK竞品中位价": 20.99,
                "UK竞品平均月销量": 80,
                "UK类目新品平均月销量": 40,
                "FR样本竞品售价": 26.99,
                "FR竞品中位价": 25.99,
                "FR竞品平均月销量": 70,
                "FR类目新品平均月销量": 35,
                "IT样本竞品售价": 24.99,
                "IT竞品中位价": 23.99,
                "IT竞品平均月销量": 50,
                "IT类目新品平均月销量": 25,
                "ES样本竞品售价": 24.99,
                "ES竞品中位价": 23.99,
                "ES竞品平均月销量": 60,
                "ES类目新品平均月销量": 30,
                "DE本土号毛利率%": "62.1",
            },
        }
        candidate = sel._candidate_from_record(record)
        candidate["image_key"] = "img_test_key"
        return candidate

    def test_purchase_quantity_formula_uses_competitor_and_new_product_sales(self):
        self.assertEqual(80, sel.reference_monthly_sales(100, 50))

        qty, note = sel.suggest_purchase_qty(
            competitor_avg_monthly_sales=100,
            category_new_avg_monthly_sales=50,
            decision="Go",
            coverage_days=30,
        )
        self.assertEqual(10, qty)
        self.assertIn("参考月销80", note)
        self.assertIn("入场系数12%", note)

        qty, _ = sel.suggest_purchase_qty(
            competitor_avg_monthly_sales=100,
            category_new_avg_monthly_sales=50,
            decision="条件推进",
            coverage_days=21,
        )
        self.assertEqual(5, qty)

        qty, note = sel.suggest_purchase_qty(
            competitor_avg_monthly_sales="",
            category_new_avg_monthly_sales="",
            decision="Go",
            coverage_days=30,
        )
        self.assertIsNone(qty)
        self.assertEqual("需补竞品月销量和类目新品月销量", note)

    def test_build_card_has_prices_quantities_links_and_decision_buttons(self):
        candidate = self._candidate()
        card = sel.build_selection_confirmation_card([candidate], "batch-test")
        rendered = json.dumps(card, ensure_ascii=False)

        self.assertIn("欧洲站选品结果确认", rendered)
        self.assertIn("不是上架验证卡，也不是固定50件试销卡", rendered)
        self.assertIn("竞品售价、建议售价与各站采购量", rendered)
        self.assertIn("DE: 竞品价 €25.99", rendered)
        self.assertIn("UK: 竞品价 £21.99", rendered)
        self.assertIn("建议采购 10件", rendered)
        self.assertIn("三渠道对比", rendered)
        self.assertIn("回款/投入分析", rendered)
        self.assertIn("打开 Listing", rendered)
        self.assertIn("查看主图原图", rendered)
        self.assertIn("打开候选表记录", rendered)
        self.assertIn("打开1688供应商", rendered)
        for action in sel.DECISION_ACTIONS:
            self.assertIn(action, rendered)
        self.assertNotIn('"tag": "form"', rendered)
        self.assertNotIn("form_submit", rendered)
        self.assertEqual([], sel.validate_selection_confirmation_card(card, [candidate]))

    def test_process_callback_updates_decision_and_patches_original_card(self):
        candidate = self._candidate()
        writes = []
        patches = []

        original_get = sel._get_candidate
        original_get_many = sel._get_candidates_by_ids
        original_update = sel._update_candidate
        original_prepare = sel._prepare_card_images
        original_patch = sel.amz_assistant.update_card

        async def fake_get(record_id):
            return dict(candidate)

        async def fake_get_many(record_ids):
            return [dict(candidate)]

        async def fake_update(record_id, fields):
            writes.append((record_id, fields))

        async def fake_prepare(candidates):
            return None

        async def fake_patch(message_id, card):
            patches.append((message_id, card))
            return True

        event = {
            "action": {
                "value": {
                    "source": sel.SOURCE,
                    "action": sel.ACTION_GO,
                    "record_id": "rec1",
                    "batch_id": "batch-test",
                    "card_record_ids": ["rec1"],
                    "system_decision": "Go",
                    "suggested_total_qty": 10,
                }
            },
            "context": {"open_message_id": "om_test"},
            "operator": {"name": "tester"},
        }

        try:
            sel._get_candidate = fake_get
            sel._get_candidates_by_ids = fake_get_many
            sel._update_candidate = fake_update
            sel._prepare_card_images = fake_prepare
            sel.amz_assistant.update_card = fake_patch
            result = asyncio.run(sel._process_callback(event))
        finally:
            sel._get_candidate = original_get
            sel._get_candidates_by_ids = original_get_many
            sel._update_candidate = original_update
            sel._prepare_card_images = original_prepare
            sel.amz_assistant.update_card = original_patch

        self.assertEqual("success", result["toast"]["type"])
        self.assertEqual("rec1", writes[0][0])
        self.assertEqual("待采购确认", writes[0][1]["当前状态"])
        self.assertEqual("Go", writes[0][1]["综合结论"])
        self.assertIn("进入采购阶段", writes[0][1]["下一步动作"])
        self.assertIn("建议采购总量=10件", writes[0][1]["人审备注"])
        self.assertEqual("om_test", patches[0][0])

    def test_amz_assistant_routes_selection_callbacks(self):
        original_handler = sel.handle_callback
        original_token = amz_assistant.VERIFICATION_TOKEN
        calls = []

        async def fake_handler(event):
            calls.append(event)
            return {"toast": {"type": "success", "content": "routed"}}

        payload = {
            "header": {"event_type": "card.action.trigger"},
            "event": {
                "action": {"value": {"action": sel.ACTION_HOLD, "record_id": "rec1"}},
                "context": {"open_message_id": "om_test"},
            },
        }
        try:
            sel.handle_callback = fake_handler
            amz_assistant.VERIFICATION_TOKEN = ""
            result = asyncio.run(amz_assistant.handle_feishu_callback(payload))
        finally:
            sel.handle_callback = original_handler
            amz_assistant.VERIFICATION_TOKEN = original_token

        self.assertEqual("routed", result["toast"]["content"])
        self.assertEqual(1, len(calls))


if __name__ == "__main__":
    unittest.main()

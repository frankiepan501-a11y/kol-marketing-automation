import asyncio
import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import amz_procurement_preview as preview


class AmzProcurementPreviewTests(unittest.TestCase):
    def _record(self, rid="rec1", asin="B0CH1817WW", decision="Go", qty=150, cost=4, current_status="待采购确认"):
        return {
            "record_id": rid,
            "fields": {
                "ASIN": asin,
                "候选标题": "Dreame L20 Ultra replacement filter",
                "产品中文名": "Dreame L20 Ultra 扫地机替换滤网",
                "Amazon链接": {"link": f"https://www.amazon.de/dp/{asin}", "text": "Listing"},
                "样本ASIN主图URL": {"link": f"https://m.media-amazon.com/images/I/{asin}.jpg", "text": "Image"},
                "包装尺寸": "12.9,5.5,3.6",
                "商品重量g": "50",
                "套装件数": "2",
                "套装内容": "2个替换滤网；采购需按Amazon主图核对滤网尺寸和适配型号",
                "采购成本RMB": cost,
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
                "当前状态": current_status,
                "综合结论": decision,
                "下一步动作": "进入采购阶段：采购复核MOQ/交期/同款后下单",
                "侵权风险说明": "品牌词/IP：Dreame；只能写兼容/适配关系。",
                "人审备注": f"2026-07-29 tester: 选品结果确认={decision}; 系统建议={decision}; 建议采购总量={qty}件; 批次=batch-old.",
                "DE样本竞品售价": 25.99,
                "DE竞品平均月销量": 100,
                "DE类目新品平均月销量": 50,
            },
        }

    def _candidate(self, **kwargs):
        item = preview._candidate_from_record(self._record(**kwargs))
        item["image_key"] = "img_test_key"
        return item

    def test_build_preview_card_has_routes_and_no_callback_controls(self):
        candidates = [
            self._candidate(rid="rec_go", decision="Go", qty=150),
            self._candidate(rid="rec_cond", asin="B0CSCXSHPQ", decision="条件推进", qty=15, cost=40.1, current_status="待采购复核"),
            self._candidate(rid="rec_hold", asin="B0CNRH4GRJ", decision="暂缓", qty=0, cost=20, current_status="暂缓"),
        ]
        card = preview.build_procurement_preview_card(candidates, "batch-preview")
        rendered = json.dumps(card, ensure_ascii=False)

        self.assertIn("采购阶段预览", rendered)
        self.assertIn("待 Frankie 确认", rendered)
        self.assertIn("不写采购阶段触发表", rendered)
        self.assertIn("不发采购部", rendered)
        self.assertIn("直接入采购 1 个", rendered)
        self.assertIn("条件采购复核 1 个", rendered)
        self.assertIn("暂缓不发采购 1 个", rendered)
        self.assertIn("采购部下一步", rendered)
        self.assertIn("采购成本（单套）", rendered)
        self.assertIn("套装件数（每套内含）", rendered)
        self.assertIn("三渠道对比", rendered)
        self.assertIn("A FBA经济线（推荐）", rendered)
        self.assertNotIn("B FBA快速线（推荐）", rendered)
        self.assertIn("请在聊天里回复", rendered)
        self.assertIn("打开 Listing", rendered)
        self.assertIn("查看主图原图", rendered)
        self.assertIn("打开候选表记录", rendered)
        self.assertIn("打开1688供应商", rendered)
        self.assertNotIn('"tag": "form"', rendered)
        self.assertNotIn("form_submit", rendered)
        self.assertNotIn('"value": {"source":', rendered)
        self.assertEqual([], preview.validate_procurement_preview_card(card, candidates))

    def test_suggested_qty_prefers_confirmed_review_note(self):
        candidate = self._candidate(qty=60)
        self.assertEqual(60, candidate["suggested_procurement_qty"])

    def test_send_preview_card_dry_run_does_not_write_or_send(self):
        original_get_many = preview._get_candidates_by_ids

        async def fake_get_many(record_ids):
            return [self._candidate(rid=record_ids[0], decision="Go", qty=150)]

        try:
            preview._get_candidates_by_ids = fake_get_many
            result = asyncio.run(preview.send_procurement_preview_card(mode="dry_run", record_ids=["rec1"]))
        finally:
            preview._get_candidates_by_ids = original_get_many

        self.assertTrue(result["ok"])
        self.assertEqual("passed", result["card_selftest"])
        self.assertEqual([], result["would_write"])
        self.assertFalse(result["would_send_to_procurement"])
        self.assertIn("card", result)


if __name__ == "__main__":
    unittest.main()

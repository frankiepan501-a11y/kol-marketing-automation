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
        self.assertIn("本卡目的", rendered)
        self.assertIn("正式发采购部时只包含", rendered)
        self.assertIn("采购部收到后要做", rendered)
        self.assertIn("不会发给采购部", rendered)
        self.assertIn("暂缓/淘汰只留档", rendered)
        self.assertIn("下一步", rendered)
        self.assertIn("正式采购产品 2 个", rendered)
        self.assertIn("直接入采购 1 个", rendered)
        self.assertIn("条件采购复核 1 个", rendered)
        self.assertIn("暂缓/淘汰不发采购 1 个", rendered)
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
        self.assertNotIn("暂缓不发采购｜", rendered)
        self.assertNotIn("B0CNRH4GRJ", rendered)
        self.assertNotIn('"tag": "form"', rendered)
        self.assertNotIn("form_submit", rendered)
        self.assertNotIn('"value": {"source":', rendered)
        self.assertEqual([], preview.validate_procurement_preview_card(card, candidates))

    def test_hold_and_reject_products_are_summary_only(self):
        candidates = [
            self._candidate(rid="rec_hold", asin="B0HOLD", decision="暂缓", qty=0, current_status="暂缓"),
            self._candidate(rid="rec_reject", asin="B0REJECT", decision="淘汰", qty=0, current_status="淘汰"),
        ]
        card = preview.build_procurement_preview_card(candidates, "batch-preview")
        rendered = json.dumps(card, ensure_ascii=False)

        self.assertIn("正式采购产品 0 个", rendered)
        self.assertIn("暂缓/淘汰不发采购 2 个", rendered)
        self.assertIn("本批没有需要采购部处理的产品", rendered)
        self.assertNotIn("暂缓不发采购｜", rendered)
        self.assertNotIn("淘汰归档｜", rendered)
        self.assertNotIn("B0HOLD", rendered)
        self.assertNotIn("B0REJECT", rendered)
        self.assertEqual([], preview.validate_procurement_preview_card(card, candidates))

    def test_procurement_audience_has_execution_wording(self):
        candidates = [
            self._candidate(rid="rec_go", decision="Go", qty=150),
            self._candidate(rid="rec_cond", asin="B0CSCXSHPQ", decision="条件推进", qty=15, cost=40.1, current_status="待采购复核"),
            self._candidate(rid="rec_hold", asin="B0HOLD", decision="暂缓", qty=0, current_status="暂缓"),
        ]
        card = preview.build_procurement_preview_card(candidates, "batch-proc", audience="procurement")
        rendered = json.dumps(card, ensure_ascii=False)

        self.assertIn("采购复核回填", rendered)
        self.assertIn("采购复核回填卡", rendered)
        self.assertIn("本卡只发需要采购部复核", rendered)
        self.assertIn("暂缓/淘汰不出现在产品区块", rendered)
        self.assertIn("提交本产品复核", rendered)
        self.assertIn("同款确认", rendered)
        self.assertIn("MOQ", rendered)
        self.assertIn("阶梯价（选填）", rendered)
        self.assertIn("交期", rendered)
        self.assertIn("箱规/尺寸重量（选填）", rendered)
        self.assertIn("必填：同款、MOQ、交期、现货/库存、供应商结论、采购建议", rendered)
        self.assertIn("可先提交，系统会带到下一步待补资料", rendered)
        self.assertIn("供应商结论", rendered)
        self.assertIn("采购建议", rendered)
        self.assertIn("是否有现货库存", rendered)
        self.assertIn("库存数", rendered)
        self.assertIn("不是 ERP 新品录入", rendered)
        self.assertIn("不会自动下单", rendered)
        self.assertIn("ERP 新品录入放在最终采购确认之后", rendered)
        self.assertIn("条件未满足前不要下单", rendered)
        self.assertNotIn("待 Frankie 确认", rendered)
        self.assertNotIn("给 Frankie 确认", rendered)
        self.assertNotIn("请在聊天里回复是否按本口径", rendered)
        self.assertNotIn("暂缓不发采购｜", rendered)
        self.assertNotIn("B0HOLD", rendered)
        self.assertEqual([], preview.validate_procurement_preview_card(card, candidates, audience="procurement"))

    def test_completed_procurement_review_card_has_no_active_form(self):
        candidate = self._candidate(rid="rec_done", decision="Go", qty=150)
        candidate["procurement_review_status"] = "已提交"
        candidate["procurement_review"] = {
            "采购复核回填": "已提交",
            "同款确认": "同款可采购",
            "MOQ": "50套",
            "阶梯价": "50套=4",
            "交期": "现货1-2天",
            "箱规尺寸重量": "外箱50套/6kg",
            "现货": "有现货",
            "库存数": "300套",
            "供应商结论": "供应商可用",
            "采购建议": "可采购",
        }
        card = preview.build_procurement_preview_card([candidate], "batch-done", audience="procurement")
        rendered = json.dumps(card, ensure_ascii=False)

        self.assertIn("已全部复核", rendered)
        self.assertIn("采购复核已提交", rendered)
        self.assertIn("同款确认: 同款可采购", rendered)
        self.assertNotIn("proc_review_form_rec_done", rendered)
        self.assertNotIn("form_submit", rendered)
        self.assertEqual([], preview.validate_procurement_preview_card(card, [candidate], audience="procurement"))

    def test_procurement_audience_requires_explicit_approval(self):
        original_get_many = preview._get_candidates_by_ids

        async def fake_get_many(record_ids):
            return [self._candidate(rid=record_ids[0], decision="Go", qty=150)]

        try:
            preview._get_candidates_by_ids = fake_get_many
            with self.assertRaises(ValueError):
                asyncio.run(
                    preview.send_procurement_preview_card(
                        mode="dry_run",
                        record_ids=["rec1"],
                        frankie_only=False,
                        audience="procurement",
                    )
                )
        finally:
            preview._get_candidates_by_ids = original_get_many

    def test_procurement_audience_allows_frankie_only_review_sample(self):
        original_get_many = preview._get_candidates_by_ids

        async def fake_get_many(record_ids):
            return [self._candidate(rid=record_ids[0], decision="Go", qty=150)]

        try:
            preview._get_candidates_by_ids = fake_get_many
            result = asyncio.run(
                preview.send_procurement_preview_card(
                    mode="dry_run",
                    record_ids=["rec1"],
                    frankie_only=True,
                    audience="procurement",
                )
            )
        finally:
            preview._get_candidates_by_ids = original_get_many

        self.assertTrue(result["ok"])
        self.assertTrue(result["frankie_only"])
        self.assertFalse(result["would_send_to_procurement"])
        rendered = json.dumps(result["card"], ensure_ascii=False)
        self.assertIn("提交本产品复核", rendered)

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

    def test_send_procurement_audience_dry_run_marks_procurement_send(self):
        original_get_many = preview._get_candidates_by_ids

        async def fake_get_many(record_ids):
            return [self._candidate(rid=record_ids[0], decision="Go", qty=150)]

        try:
            preview._get_candidates_by_ids = fake_get_many
            result = asyncio.run(
                preview.send_procurement_preview_card(
                    mode="dry_run",
                    record_ids=["rec1"],
                    frankie_only=False,
                    audience="procurement",
                    procurement_approved=True,
                )
            )
        finally:
            preview._get_candidates_by_ids = original_get_many

        self.assertTrue(result["ok"])
        self.assertEqual("procurement", result["audience"])
        self.assertFalse(result["frankie_only"])
        self.assertTrue(result["would_send_to_procurement"])
        self.assertEqual("passed", result["card_selftest"])

    def test_procurement_review_allows_optional_tiers_and_carton(self):
        review = {
            "same": "同款可采购",
            "moq": "2套",
            "tiers": "",
            "leadtime": "7天",
            "carton": "",
            "stock": "有现货",
            "stock_qty": "20套",
            "supplier": "供应商可用",
            "suggestion": "可采购",
            "note": "1688链接已填，阶梯价和箱规后补",
        }

        self.assertEqual("", preview._validate_review(review))
        self.assertEqual("阶梯价、箱规/尺寸重量", preview._optional_gap_text(review))
        self.assertIn("后续待补=阶梯价、箱规/尺寸重量", preview._review_summary(review))
        self.assertIn("可进入最终采购确认", preview._review_next_action(review))
        self.assertIn("ERP新品和物流复算前必须补齐", preview._review_next_action(review))

    def test_procurement_review_callback_writes_current_product_and_patches_card(self):
        candidate = self._candidate(rid="rec1", decision="Go", qty=150)
        updates = {}
        patched = {}
        original_get = preview._get_candidate
        original_update = preview._update_candidate
        original_get_many = preview._get_candidates_by_ids
        original_prepare = preview._prepare_card_images
        original_update_card = preview.amz_assistant.update_card

        async def fake_get(record_id):
            return dict(candidate)

        async def fake_update(record_id, fields):
            updates["record_id"] = record_id
            updates.update(fields)

        async def fake_get_many(record_ids):
            item = dict(candidate)
            if updates.get("人审备注"):
                item["review_note"] = updates["人审备注"]
                item["procurement_review"] = preview._latest_procurement_review(updates["人审备注"])
                item["procurement_review_status"] = "已提交"
            return [item]

        async def fake_prepare(candidates):
            return None

        async def fake_update_card(message_id, card):
            patched["message_id"] = message_id
            patched["card"] = card
            return True

        event = {
            "open_message_id": "om_proc_review_test",
            "operator": {"name": "郭嘉美"},
            "action": {
                "value": {
                    "action": preview.ACTION_REVIEW_SUBMIT,
                    "record_id": "rec1",
                    "batch_id": "batch-review",
                    "card_record_ids": ["rec1"],
                },
                "form_value": {
                    "proc_review_same_rec1": "同款可采购",
                    "proc_review_moq_rec1": "50套",
                    "proc_review_tiers_rec1": "50套=4；100套=3.8",
                    "proc_review_leadtime_rec1": "现货1-2天",
                    "proc_review_carton_rec1": "单套12.9*5.5*3.6cm/50g；外箱50套/6kg",
                    "proc_review_stock_rec1": "有现货",
                    "proc_review_stock_qty_rec1": "300套",
                    "proc_review_supplier_rec1": "供应商可用",
                    "proc_review_suggestion_rec1": "可采购",
                    "proc_review_note_rec1": "需确认无Logo，按2件套报价",
                },
            },
        }
        try:
            preview._get_candidate = fake_get
            preview._update_candidate = fake_update
            preview._get_candidates_by_ids = fake_get_many
            preview._prepare_card_images = fake_prepare
            preview.amz_assistant.update_card = fake_update_card
            result = asyncio.run(preview._process_callback(event))
        finally:
            preview._get_candidate = original_get
            preview._update_candidate = original_update
            preview._get_candidates_by_ids = original_get_many
            preview._prepare_card_images = original_prepare
            preview.amz_assistant.update_card = original_update_card

        self.assertIn("本产品采购复核已提交", (result.get("toast") or {}).get("content", ""))
        self.assertEqual("rec1", updates["record_id"])
        self.assertIn("采购复核已提交", updates["采购备注"])
        self.assertIn("同款确认=同款可采购", updates["采购备注"])
        self.assertIn("MOQ=50套", updates["人审备注"])
        self.assertIn("阶梯价=50套=4；100套=3.8", updates["人审备注"])
        self.assertIn("待Frankie/运营最终确认采购量", updates["下一步动作"])
        self.assertEqual("om_proc_review_test", patched["message_id"])
        rendered = json.dumps(patched["card"], ensure_ascii=False)
        self.assertIn("采购复核已提交", rendered)
        self.assertNotIn("proc_review_form_rec1", rendered)


if __name__ == "__main__":
    unittest.main()

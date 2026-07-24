import asyncio
import json
import unittest

from app import amz_validation50 as val50


class AmzValidation50Tests(unittest.TestCase):
    def _candidate(self, rid="rec1", validation_status="未开始", gate="Go"):
        return {
            "record_id": rid,
            "asin": "B0CH1817WW",
            "site": "DE",
            "title": "Dreame L20 Ultra replacement filter",
            "cn_name": "Dreame L20 Ultra 扫地机替换滤网",
            "amazon_url": "https://www.amazon.de/dp/B0CH1817WW",
            "image_url": "https://m.media-amazon.com/images/I/41Bum-N615L._AC_.jpg",
            "image_key": "img_test_key",
            "package_size": "12.9,5.5,3.6",
            "weight_g": "50",
            "set_count": "2",
            "set_content": "2个替换滤网；采购需按Amazon主图核对滤网尺寸和适配型号 Dreame L20 Ultra",
            "quote_status": "已回填",
            "quote_cost": 4,
            "supplier_link": "https://detail.1688.com/offer/test.html",
            "fulfillment": "FBA头程-经济线",
            "fba_fee_eur": "2.75",
            "commission_eur": "3.9",
            "channels": [
                {
                    "code": "A",
                    "label": "FBA经济线",
                    "aliases": ["FBA头程-经济线", "经济线"],
                    "logistics_rmb": "0.74",
                    "freight_ratio": "0",
                    "margin_rmb": "124.38",
                    "margin_rate": "56.3",
                },
                {
                    "code": "B",
                    "label": "FBA快速线",
                    "aliases": ["FBA头程-快速线", "快速线"],
                    "logistics_rmb": "1.98",
                    "freight_ratio": "0.01",
                    "margin_rmb": "123.14",
                    "margin_rate": "55.7",
                },
                {
                    "code": "C",
                    "label": "FBM-4PX",
                    "aliases": ["FBM", "4PX", "自发货"],
                    "logistics_rmb": "31.05",
                    "freight_ratio": "0.14",
                    "margin_rmb": "117.44",
                    "margin_rate": "53.2",
                },
            ],
            "finance_gate": "通过",
            "compliance_gate": gate,
            "ip_risk": "中",
            "risk_note": (
                "自动风险扫描：中风险 / 60分 / 60分内快速通过，问题点留档\n"
                "问题点：\n"
                "- [中] 品牌词/IP: 识别到兼容品牌词：Dreame 建议: 只能写兼容/适配关系。\n"
                "- [低] EU/GPSR: 欧洲站上架前需要准备 GPSR 责任人。"
            ),
            "current_status": "待50件验证",
            "overall_decision": "50件验证",
            "next_action": "发起50件验证",
            "validation_status": validation_status,
            "review_note": "previous note",
        }

    def test_build_card_has_context_links_and_no_forms(self):
        card = val50.build_validation50_card([self._candidate()], "batch-test", qty=50)
        rendered = json.dumps(card, ensure_ascii=False)

        self.assertIn("AMZ·P0", rendered)
        self.assertIn("德国站50件验证启动", rendered)
        self.assertIn("50件验证要看", rendered)
        self.assertIn("三渠道经济性", rendered)
        self.assertIn("A FBA经济线（推荐）", rendered)
        self.assertIn("50件粗算", rendered)
        self.assertIn("237.0 RMB", rendered)
        self.assertIn("打开 Listing", rendered)
        self.assertIn("查看主图原图", rendered)
        self.assertIn("打开候选表记录", rendered)
        self.assertIn("打开1688供应商", rendered)
        self.assertIn('"tag": "img"', rendered)
        self.assertNotIn('"tag": "form"', rendered)
        self.assertNotIn("form_submit", rendered)
        self.assertEqual([], val50.validate_validation50_card(card, [self._candidate()]))

    def test_eligibility_requires_go_status_and_procurement_cost(self):
        ok, reason = val50._eligible(self._candidate())
        self.assertTrue(ok)
        self.assertEqual("ok", reason)

        bad = self._candidate(gate="暂缓")
        ok, reason = val50._eligible(bad)
        self.assertFalse(ok)
        self.assertEqual("合规闸未Go", reason)

        bad = self._candidate()
        bad["quote_cost"] = None
        ok, reason = val50._eligible(bad)
        self.assertFalse(ok)
        self.assertEqual("采购成本未回填", reason)

    def test_start_fields_mark_validation_in_progress_and_keep_note(self):
        fields = val50._build_start_fields(self._candidate(), "batch-test", 50)

        self.assertEqual("进行中", fields["50件验证状态"])
        self.assertEqual("待50件验证", fields["当前状态"])
        self.assertEqual("50件验证", fields["综合结论"])
        self.assertEqual("发起50件验证", fields["下一步动作"])
        self.assertIn("previous note", fields["人审备注"])
        self.assertIn("进入50件验证", fields["人审备注"])

    def test_dry_run_returns_would_update_without_writing_or_sending(self):
        original_get_many = val50._get_candidates_by_ids
        original_update = val50._update_candidate
        original_send = val50.amz_assistant.send_card_to_union
        writes = []
        sends = []

        async def fake_get_many(record_ids):
            return [self._candidate(record_ids[0])]

        async def fake_update(record_id, fields):
            writes.append((record_id, fields))

        async def fake_send(union_id, card):
            sends.append((union_id, card))
            return "om_unexpected"

        try:
            val50._get_candidates_by_ids = fake_get_many
            val50._update_candidate = fake_update
            val50.amz_assistant.send_card_to_union = fake_send
            result = asyncio.run(val50.start_validation50(mode="dry_run", record_ids=["rec1"], frankie_only=True))
        finally:
            val50._get_candidates_by_ids = original_get_many
            val50._update_candidate = original_update
            val50.amz_assistant.send_card_to_union = original_send

        self.assertEqual(1, result["eligible_count"])
        self.assertEqual([], writes)
        self.assertEqual([], sends)
        self.assertEqual("进行中", result["would_update"][0]["fields"]["50件验证状态"])

    def test_commit_updates_eligible_and_sends_start_card(self):
        original_get_many = val50._get_candidates_by_ids
        original_update = val50._update_candidate
        original_prepare = val50._prepare_card_images
        original_send = val50.amz_assistant.send_card_to_union
        writes = []
        sends = []

        async def fake_get_many(record_ids):
            return [self._candidate(record_ids[0]), self._candidate(record_ids[1], validation_status="进行中")]

        async def fake_update(record_id, fields):
            writes.append((record_id, fields))

        async def fake_prepare(candidates):
            return None

        async def fake_send(union_id, card):
            sends.append((union_id, card))
            return "om_val50"

        try:
            val50._get_candidates_by_ids = fake_get_many
            val50._update_candidate = fake_update
            val50._prepare_card_images = fake_prepare
            val50.amz_assistant.send_card_to_union = fake_send
            result = asyncio.run(val50.start_validation50(
                mode="commit",
                record_ids=["rec1", "rec2"],
                frankie_only=True,
                batch_id="batch-test",
            ))
        finally:
            val50._get_candidates_by_ids = original_get_many
            val50._update_candidate = original_update
            val50._prepare_card_images = original_prepare
            val50.amz_assistant.send_card_to_union = original_send

        self.assertEqual(1, result["eligible_count"])
        self.assertEqual(1, result["skipped_count"])
        self.assertEqual(["rec1"], result["updated_record_ids"])
        self.assertEqual("进行中", writes[0][1]["50件验证状态"])
        self.assertEqual(["om_val50"], result["message_ids"])
        self.assertEqual(1, len(sends))


if __name__ == "__main__":
    unittest.main()


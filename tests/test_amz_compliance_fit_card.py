import asyncio
import json
import unittest

from app import amz_assistant
from app import amz_compliance_fit_card as fit


class AmzComplianceFitCardTests(unittest.TestCase):
    def _candidate(self, rid="rec1", gate="待核", title=None):
        return {
            "record_id": rid,
            "asin": "B0CH1817WW",
            "title": title or "Dreame L20 Ultra replacement filter",
            "cn_name": "Dreame L20 Ultra 扫地机替换滤网",
            "amazon_url": "https://www.amazon.de/dp/B0CH1817WW",
            "image_url": "https://m.media-amazon.com/images/I/41Bum-N615L._AC_.jpg",
            "image_key": "img_test_key",
            "package_size": "12.9,5.5,3.6",
            "weight_g": "50",
            "set_count": "2",
            "set_content": "2个替换滤网；采购需核对适配型号 Dreame L20 Ultra",
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
                    "margin_rmb": "99.16",
                    "margin_rate": "53.3",
                },
                {
                    "code": "B",
                    "label": "FBA快速线",
                    "aliases": ["FBA头程-快速线", "快速线"],
                    "logistics_rmb": "1.98",
                    "freight_ratio": "0.01",
                    "margin_rmb": "97.93",
                    "margin_rate": "52.6",
                },
                {
                    "code": "C",
                    "label": "FBM-4PX",
                    "aliases": ["FBM", "4PX", "自发货"],
                    "logistics_rmb": "31.05",
                    "freight_ratio": "0.17",
                    "margin_rmb": "92.23",
                    "margin_rate": "49.5",
                },
            ],
            "current_status": "待合规核查",
            "overall_decision": "50件验证",
            "finance_gate": "财务通过",
            "compliance_gate": gate,
            "ip_risk": "低" if gate == "Go" else "待核",
            "risk_note": "已处理" if gate == "Go" else "",
            "data_gaps": ["认证"],
            "next_action": "发起50件验证" if gate == "Go" else "查合规/型号适配",
        }

    def test_scan_detects_compatibility_brand_and_gpsr_risks(self):
        scan = fit.scan_candidate(self._candidate())

        rendered = json.dumps(scan, ensure_ascii=False)
        self.assertEqual("review_required", scan["decision"])
        self.assertEqual("中", scan["level"])
        self.assertIn("Dreame", rendered)
        self.assertIn("品牌词/IP", rendered)
        self.assertIn("EU/GPSR", rendered)
        self.assertIn("不是法律结论", json.dumps(fit.build_fit_card([self._candidate()], "batch-test"), ensure_ascii=False))

    def test_low_signal_product_can_auto_pass_with_gpsr_reminder(self):
        candidate = self._candidate(title="Generic replacement dust filter")
        candidate["cn_name"] = "通用替换滤网"
        candidate["set_content"] = "2 filters, no brand model wording"
        scan = fit.scan_candidate(candidate)

        self.assertEqual("auto_pass", scan["decision"])
        self.assertEqual("低", scan["level"])

    def test_risk_feedback_card_has_independent_forms_and_no_manual_audit_controls(self):
        candidates = [self._candidate("rec1"), self._candidate("rec2")]
        fit._attach_risk_scans(candidates)
        card = fit.build_fit_card(candidates, "batch-test")
        rendered = json.dumps(card, ensure_ascii=False)

        self.assertIn("AMZ·P0", rendered)
        self.assertIn("自动风险扫描结果", rendered)
        self.assertIn("自动发现的问题点", rendered)
        self.assertIn("risk_action_rec1", rendered)
        self.assertIn("risk_action_rec2", rendered)
        self.assertIn("risk_note_rec1", rendered)
        self.assertIn(fit.ACTION_SUBMIT, rendered)
        self.assertIn("打开 Listing", rendered)
        self.assertIn("查看主图原图", rendered)
        self.assertIn("打开候选表记录", rendered)
        self.assertIn("打开1688供应商", rendered)
        self.assertIn('"tag": "img"', rendered)
        self.assertIn("select_static", rendered)
        self.assertIn("三渠道毛利", rendered)
        self.assertIn("GPSR", rendered)
        self.assertIn("人只处理系统发现的例外", rendered)
        self.assertNotIn("fit_result_rec1", rendered)
        self.assertNotIn("选择IP/外观风险", rendered)
        self.assertEqual([], fit.validate_fit_card(card, candidates))

    def test_completed_product_renders_without_form(self):
        candidate = self._candidate("rec1", "Go")
        fit._attach_risk_scans([candidate])
        card = fit.build_fit_card([candidate], "batch-test")
        rendered = json.dumps(card, ensure_ascii=False)

        self.assertIn("自动风险处理已完成", rendered)
        self.assertNotIn("risk_action_rec1", rendered)

    def test_amz_assistant_dispatches_fit_action(self):
        original_handler = fit.handle_callback
        calls = []

        async def fake_handle(event):
            calls.append(event)
            return {"toast": {"type": "success", "content": "ok"}}

        try:
            fit.handle_callback = fake_handle
            result = asyncio.run(amz_assistant.handle_feishu_callback({
                "schema": "2.0",
                "header": {"event_type": "card.action.trigger"},
                "event": {
                    "operator": {"union_id": "on_operator"},
                    "context": {"open_message_id": "om_card"},
                    "action": {"value": {"action": fit.ACTION_SUBMIT, "record_id": "rec1"}},
                },
            }))
        finally:
            fit.handle_callback = original_handler

        self.assertEqual("success", result["toast"]["type"])
        self.assertEqual(1, len(calls))
        self.assertEqual("om_card", calls[0]["context"]["open_message_id"])

    def test_legacy_manual_card_action_is_disabled(self):
        event = {
            "action": {
                "value": {"action": fit.ACTION_LEGACY_SUBMIT, "record_id": "rec1"},
                "form_value": {"fit_result_rec1": "Go", "fit_iprisk_rec1": "低"},
            }
        }
        result = asyncio.run(fit.handle_callback(event))
        self.assertEqual("error", result["toast"]["type"])
        self.assertIn("旧人工核查卡已停用", result["toast"]["content"])

    def test_handle_callback_validates_required_feedback_fields_before_spawn(self):
        event = {
            "action": {
                "value": {"action": fit.ACTION_SUBMIT, "record_id": "rec1"},
                "form_value": {"risk_action_rec1": "", "risk_note_rec1": ""},
            }
        }
        result = asyncio.run(fit.handle_callback(event))
        self.assertEqual("error", result["toast"]["type"])
        self.assertIn("请选择如何处理系统建议", result["toast"]["content"])

        event["action"]["form_value"] = {"risk_action_rec1": "标记系统误报", "risk_note_rec1": ""}
        result = asyncio.run(fit.handle_callback(event))
        self.assertEqual("error", result["toast"]["type"])
        self.assertIn("必须填写处理备注", result["toast"]["content"])

    def test_extract_form_values_accepts_nested_and_list_payloads(self):
        nested = fit._extract_form_values({
            "action": {
                "form_value": {
                    "risk_feedback_form_rec1": {
                        "risk_action_rec1": {"value": "确认系统建议"},
                        "risk_note_rec1": {"input_value": "按系统建议处理"},
                    }
                }
            }
        })
        self.assertEqual("确认系统建议", fit._form_value(nested, "rec1", "action"))
        self.assertEqual("按系统建议处理", fit._form_value(nested, "rec1", "note"))

        listed = fit._extract_form_values({
            "action": {
                "input_values": [
                    {"name": "risk_action_rec1", "value": "升级合规复核"},
                    {"name": "risk_note_rec1", "input_value": "外观相似度需要人工看"},
                ]
            }
        })
        self.assertEqual("升级合规复核", fit._form_value(listed, "rec1", "action"))
        self.assertEqual("外观相似度需要人工看", fit._form_value(listed, "rec1", "note"))

    def test_handle_callback_fast_ack_spawns_background(self):
        original_spawn = fit._spawn
        original_recent = dict(fit._recent_callbacks)
        spawned = []

        def fake_spawn(coro):
            spawned.append(coro)
            coro.close()

        try:
            fit._recent_callbacks.clear()
            fit._spawn = fake_spawn
            result = asyncio.run(fit.handle_callback({
                "action": {
                    "value": {"action": fit.ACTION_SUBMIT, "record_id": "rec1"},
                    "form_value": {"risk_action_rec1": "确认系统建议", "risk_note_rec1": "按系统建议处理"},
                }
            }))
        finally:
            fit._spawn = original_spawn
            fit._recent_callbacks.clear()
            fit._recent_callbacks.update(original_recent)

        self.assertEqual("success", result["toast"]["type"])
        self.assertIn("已收到", result["toast"]["content"])
        self.assertEqual(1, len(spawned))

    def test_duplicate_callback_does_not_retry_when_already_written(self):
        original_spawn = fit._spawn
        original_recent = dict(fit._recent_callbacks)
        original_get = fit._get_candidate
        spawned = []
        event = {
            "action": {
                "value": {"action": fit.ACTION_SUBMIT, "record_id": "rec1"},
                "form_value": {"risk_action_rec1": "确认系统建议", "risk_note_rec1": "按系统建议处理"},
            }
        }
        key = fit._callback_key("rec1", event["action"]["form_value"])

        def fake_spawn(coro):
            spawned.append(coro)
            coro.close()

        async def fake_get(record_id):
            return self._candidate(record_id, gate="Go")

        try:
            fit._recent_callbacks.clear()
            fit._recent_callbacks[key] = 9999999999.0
            fit._spawn = fake_spawn
            fit._get_candidate = fake_get
            result = asyncio.run(fit.handle_callback(event))
        finally:
            fit._spawn = original_spawn
            fit._get_candidate = original_get
            fit._recent_callbacks.clear()
            fit._recent_callbacks.update(original_recent)

        self.assertEqual("success", result["toast"]["type"])
        self.assertIn("已核查", result["toast"]["content"])
        self.assertEqual([], spawned)

    def test_process_callback_confirm_system_suggestion_updates_current_record_and_patches_card(self):
        original_get = fit._get_candidate
        original_update = fit._update_candidate
        original_get_many = fit._get_candidates_by_ids
        original_patch = fit.amz_assistant.update_card
        original_prepare = fit._prepare_card_images
        updates = []
        patches = []

        async def fake_get(record_id):
            return self._candidate(record_id)

        async def fake_update(record_id, fields):
            updates.append((record_id, fields))

        async def fake_get_many(record_ids):
            return [self._candidate(rid) for rid in record_ids]

        async def fake_patch(message_id, card):
            patches.append((message_id, card))
            return True

        async def fake_prepare(candidates):
            return None

        try:
            fit._get_candidate = fake_get
            fit._update_candidate = fake_update
            fit._get_candidates_by_ids = fake_get_many
            fit.amz_assistant.update_card = fake_patch
            fit._prepare_card_images = fake_prepare
            result = asyncio.run(fit._process_callback({
                "context": {"open_message_id": "om_fit"},
                "operator": {"union_id": "on_operator"},
                "action": {
                    "value": {
                        "action": fit.ACTION_SUBMIT,
                        "record_id": "rec1",
                        "batch_id": "batch-test",
                        "card_record_ids": ["rec1", "rec2"],
                    },
                    "form_value": {"risk_action_rec1": "确认系统建议", "risk_note_rec1": "按系统建议处理"},
                },
            }))
        finally:
            fit._get_candidate = original_get
            fit._update_candidate = original_update
            fit._get_candidates_by_ids = original_get_many
            fit.amz_assistant.update_card = original_patch
            fit._prepare_card_images = original_prepare

        self.assertEqual("success", result["toast"]["type"])
        self.assertEqual("rec1", updates[0][0])
        self.assertEqual("暂缓", updates[0][1]["合规闸结论"])
        self.assertEqual("中", updates[0][1]["IP/外观风险"])
        self.assertEqual("待合规核查", updates[0][1]["当前状态"])
        self.assertIn("自动风险扫描", updates[0][1]["侵权风险说明"])
        self.assertEqual(1, len(patches))
        rendered = json.dumps(patches[0][1], ensure_ascii=False)
        self.assertIn("自动风险处理已完成", rendered)
        self.assertIn("risk_action_rec2", rendered)

    def test_send_fit_card_can_send_to_gray_recipients_when_enabled(self):
        original_frankie_only = fit.FRANKIE_ONLY
        original_get_many = fit._get_candidates_by_ids
        original_update = fit._update_candidate
        original_send_union = fit.amz_assistant.send_card_to_union
        original_send_chat = fit.amz_assistant.send_card_to_chat
        sent = []

        async def fake_get_many(record_ids):
            return [self._candidate(record_ids[0])]

        async def fake_send_union(union_id, card):
            sent.append(("union", union_id, card))
            return "om_union"

        async def fake_send_chat(chat_id, card):
            sent.append(("chat", chat_id, card))
            return "om_chat"

        async def fake_update(record_id, fields):
            return None

        try:
            fit.FRANKIE_ONLY = False
            fit._get_candidates_by_ids = fake_get_many
            fit._update_candidate = fake_update
            fit.amz_assistant.send_card_to_union = fake_send_union
            fit.amz_assistant.send_card_to_chat = fake_send_chat
            result = asyncio.run(fit.send_fit_card(
                mode="commit",
                record_ids=["rec1"],
                frankie_only=False,
                gray_union_ids=["on_ops"],
                gray_chat_ids=["oc_ops"],
            ))
        finally:
            fit.FRANKIE_ONLY = original_frankie_only
            fit._get_candidates_by_ids = original_get_many
            fit._update_candidate = original_update
            fit.amz_assistant.send_card_to_union = original_send_union
            fit.amz_assistant.send_card_to_chat = original_send_chat

        self.assertFalse(result["frankie_only"])
        self.assertEqual(["om_chat", "om_union"], result["message_ids"])
        self.assertEqual(("chat", "oc_ops"), sent[0][:2])
        self.assertEqual(("union", "on_ops"), sent[1][:2])


if __name__ == "__main__":
    unittest.main()

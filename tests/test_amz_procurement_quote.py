import asyncio
import json
import unittest

from app import amz_assistant
from app import amz_procurement_quote as quote


class AmzProcurementQuoteTests(unittest.TestCase):
    def _candidate(self, rid="rec1", status="待回填"):
        return {
            "record_id": rid,
            "asin": "B0CH1817WW",
            "title": "Dreame L20 Ultra replacement filter",
            "cn_name": "Dreame L20 Ultra 扫地机替换滤网",
            "amazon_url": "https://www.amazon.de/dp/B0CH1817WW",
            "image_url": "https://m.media-amazon.com/images/I/41Bum-N615L._AC_.jpg",
            "image_key": "img_test_key",
            "package_size": "12.9,5.5,3.6",
            "weight_g": "50",
            "set_count": "",
            "set_content": "替换滤网；具体件数待采购核对",
            "quote_status": status,
            "quote_cost": 18.5 if status == "已回填" else None,
            "supplier_link": "https://detail.1688.com/offer/test.html" if status == "已回填" else "",
            "fulfillment": "FBA头程-经济线",
            "fba_fee_eur": "3.04",
            "commission_eur": "2.39",
            "channels": [
                {
                    "code": "A",
                    "label": "FBA经济线",
                    "aliases": ["FBA头程-经济线", "经济线"],
                    "pre_margin_rmb": "92.23",
                    "pre_margin_rate": "49.5",
                    "logistics_rmb": "31.05",
                    "freight_ratio": "0.17",
                    "margin_rmb": "73.73",
                    "margin_rate": "39.5",
                },
                {
                    "code": "B",
                    "label": "FBA快速线",
                    "aliases": ["FBA头程-快速线", "快速线"],
                    "pre_margin_rmb": "84.86",
                    "pre_margin_rate": "45.6",
                    "logistics_rmb": "38.42",
                    "freight_ratio": "0.21",
                    "margin_rmb": "66.36",
                    "margin_rate": "35.6",
                },
                {
                    "code": "C",
                    "label": "FBM-4PX",
                    "aliases": ["FBM", "4PX", "自发货"],
                    "pre_margin_rmb": "50.12",
                    "pre_margin_rate": "37.1",
                    "logistics_rmb": "43.15",
                    "freight_ratio": "0.32",
                    "margin_rmb": "31.62",
                    "margin_rate": "23.4",
                },
            ],
            "pre_margin_rmb": "92.23",
            "pre_margin_rate": "49.5",
            "logistics_rmb": "31.05",
            "freight_ratio": "0.17",
            "batch_id": "AMZ-DE-PROCQ-20260723-P0",
        }

    def test_quote_card_has_independent_form_per_product(self):
        card = quote.build_quote_card([self._candidate("rec1"), self._candidate("rec2")], "batch-test")
        rendered = json.dumps(card, ensure_ascii=False)

        self.assertIn("AMZ·P0", rendered)
        self.assertIn("proc_cost_rec1", rendered)
        self.assertIn("proc_cost_rec2", rendered)
        self.assertIn("proc_link_rec1", rendered)
        self.assertIn("proc_link_rec2", rendered)
        self.assertIn(quote.ACTION_SUBMIT, rendered)
        self.assertIn("打开 Listing", rendered)
        self.assertIn("查看主图原图", rendered)
        self.assertIn("打开候选表记录", rendered)
        self.assertIn('"tag": "img"', rendered)
        self.assertIn("img_test_key", rendered)
        self.assertIn("三渠道对比", rendered)
        self.assertIn("A FBA经济线（推荐）", rendered)
        self.assertIn("B FBA快速线", rendered)
        self.assertIn("C FBM-4PX", rendered)
        self.assertIn("采购后 73.73 RMB / 39.5%", rendered)
        self.assertIn("FBA配送费", rendered)
        self.assertIn("佣金", rendered)
        self.assertIn("提交只更新当前产品", rendered)
        self.assertEqual([], quote.validate_quote_card(card, [self._candidate("rec1"), self._candidate("rec2")]))

    def test_completed_product_renders_without_input(self):
        card = quote.build_quote_card([self._candidate("rec1", "已回填")], "batch-test")
        rendered = json.dumps(card, ensure_ascii=False)

        self.assertIn("采购已回填", rendered)
        self.assertIn("18.5 RMB", rendered)
        self.assertNotIn("proc_cost_rec1", rendered)

    def test_candidate_time_label_formats_feishu_milliseconds(self):
        item = quote._candidate_from_record({
            "record_id": "rec1",
            "fields": {
                "ASIN": "B0CH1817WW",
                "采购回填状态": "已回填",
                "采购成本RMB": 18.5,
                "采购回填时间": 1784781360000,
            },
        })

        self.assertEqual("2026-07-23 12:36", item["quote_time"])

    def test_amz_assistant_dispatches_procurement_action(self):
        original_handler = quote.handle_callback
        calls = []

        async def fake_handle(event):
            calls.append(event)
            return {"toast": {"type": "success", "content": "ok"}}

        try:
            quote.handle_callback = fake_handle
            result = asyncio.run(amz_assistant.handle_feishu_callback({
                "schema": "2.0",
                "header": {"event_type": "card.action.trigger"},
                "event": {
                    "operator": {"union_id": "on_operator"},
                    "context": {"open_message_id": "om_card"},
                    "action": {"value": {"action": quote.ACTION_SUBMIT, "record_id": "rec1"}},
                },
            }))
        finally:
            quote.handle_callback = original_handler

        self.assertEqual("success", result["toast"]["type"])
        self.assertEqual(1, len(calls))
        self.assertEqual("om_card", calls[0]["context"]["open_message_id"])

    def test_handle_callback_validates_cost_and_link_before_spawn(self):
        event = {
            "action": {
                "value": {"action": quote.ACTION_SUBMIT, "record_id": "rec1"},
                "form_value": {"proc_cost_rec1": "0", "proc_link_rec1": "https://detail.1688.com/offer/test.html"},
            }
        }
        result = asyncio.run(quote.handle_callback(event))
        self.assertEqual("error", result["toast"]["type"])
        self.assertIn("大于0", result["toast"]["content"])

        event["action"]["form_value"] = {"proc_cost_rec1": "18.5", "proc_link_rec1": "detail.1688.com/offer/test.html"}
        result = asyncio.run(quote.handle_callback(event))
        self.assertEqual("error", result["toast"]["type"])
        self.assertIn("1688供应商链接", result["toast"]["content"])

    def test_extract_action_flattens_nested_form_value(self):
        action, value, form = quote._extract_action({
            "action": {
                "value": {"action": quote.ACTION_SUBMIT, "record_id": "rec1"},
                "form_value": {
                    "proc_quote_form_rec1": {
                        "proc_cost_rec1": {"value": "18.5"},
                        "proc_link_rec1": {"value": "https://detail.1688.com/offer/test.html"},
                        "proc_note_rec1": {"value": "MOQ 100"},
                    }
                },
            }
        })

        self.assertEqual(quote.ACTION_SUBMIT, action)
        self.assertEqual("rec1", value["record_id"])
        self.assertEqual("18.5", quote._form_value(form, "rec1", "cost"))
        self.assertEqual("https://detail.1688.com/offer/test.html", quote._form_value(form, "rec1", "link"))
        self.assertEqual("MOQ 100", quote._form_value(form, "rec1", "note"))

    def test_extract_action_flattens_input_values_list(self):
        action, _, form = quote._extract_action({
            "action": {
                "value": json.dumps({"action": quote.ACTION_SUBMIT, "record_id": "rec1"}, ensure_ascii=False),
                "input_values": [
                    {"name": "proc_cost_rec1", "value": "18.5"},
                    {"name": "proc_link_rec1", "value": {"text": "https://detail.1688.com/offer/test.html"}},
                    {"name": "proc_note_rec1", "input_value": "颜色要核对"},
                ],
            }
        })

        self.assertEqual(quote.ACTION_SUBMIT, action)
        self.assertEqual("18.5", quote._form_value(form, "rec1", "cost"))
        self.assertEqual("https://detail.1688.com/offer/test.html", quote._form_value(form, "rec1", "link"))
        self.assertEqual("颜色要核对", quote._form_value(form, "rec1", "note"))

    def test_extract_action_flattens_event_card_form_value_json(self):
        _, _, form = quote._extract_action({
            "action": {"value": {"action": quote.ACTION_SUBMIT, "record_id": "rec1"}},
            "card_form_value": json.dumps({
                "proc_quote_form_rec1": {
                    "proc_cost_rec1": "18.5",
                    "proc_link_rec1": "https://detail.1688.com/offer/test.html",
                }
            }, ensure_ascii=False),
        })

        self.assertEqual("18.5", quote._form_value(form, "rec1", "cost"))
        self.assertEqual("https://detail.1688.com/offer/test.html", quote._form_value(form, "rec1", "link"))

    def test_handle_callback_fast_ack_spawns_background(self):
        original_spawn = quote._spawn
        original_recent = dict(quote._recent_callbacks)
        spawned = []

        def fake_spawn(coro):
            spawned.append(coro)
            coro.close()

        try:
            quote._recent_callbacks.clear()
            quote._spawn = fake_spawn
            result = asyncio.run(quote.handle_callback({
                "action": {
                    "value": {"action": quote.ACTION_SUBMIT, "record_id": "rec1"},
                    "form_value": {
                        "proc_cost_rec1": "18.5",
                        "proc_link_rec1": "https://detail.1688.com/offer/test.html",
                    },
                }
            }))
        finally:
            quote._spawn = original_spawn
            quote._recent_callbacks.clear()
            quote._recent_callbacks.update(original_recent)

        self.assertEqual("success", result["toast"]["type"])
        self.assertIn("已收到", result["toast"]["content"])
        self.assertEqual(1, len(spawned))

    def test_duplicate_callback_retries_when_record_not_written(self):
        original_spawn = quote._spawn
        original_recent = dict(quote._recent_callbacks)
        original_get = quote._get_candidate
        spawned = []

        event = {
            "action": {
                "value": {"action": quote.ACTION_SUBMIT, "record_id": "rec1"},
                "form_value": {
                    "proc_cost_rec1": "18.5",
                    "proc_link_rec1": "https://detail.1688.com/offer/test.html",
                },
            }
        }
        key = quote._callback_key("rec1", event["action"]["form_value"])

        def fake_spawn(coro):
            spawned.append(coro)
            coro.close()

        async def fake_get(record_id):
            return self._candidate(record_id, status="待回填")

        try:
            quote._recent_callbacks.clear()
            quote._recent_callbacks[key] = 9999999999.0
            quote._spawn = fake_spawn
            quote._get_candidate = fake_get
            result = asyncio.run(quote.handle_callback(event))
        finally:
            quote._spawn = original_spawn
            quote._get_candidate = original_get
            quote._recent_callbacks.clear()
            quote._recent_callbacks.update(original_recent)

        self.assertEqual("success", result["toast"]["type"])
        self.assertIn("重新收到", result["toast"]["content"])
        self.assertEqual(1, len(spawned))

    def test_duplicate_callback_does_not_retry_when_record_already_written(self):
        original_spawn = quote._spawn
        original_recent = dict(quote._recent_callbacks)
        original_get = quote._get_candidate
        spawned = []

        event = {
            "action": {
                "value": {"action": quote.ACTION_SUBMIT, "record_id": "rec1"},
                "form_value": {
                    "proc_cost_rec1": "18.5",
                    "proc_link_rec1": "https://detail.1688.com/offer/test.html",
                },
            }
        }
        key = quote._callback_key("rec1", event["action"]["form_value"])

        def fake_spawn(coro):
            spawned.append(coro)
            coro.close()

        async def fake_get(record_id):
            return self._candidate(record_id, status="已回填")

        try:
            quote._recent_callbacks.clear()
            quote._recent_callbacks[key] = 9999999999.0
            quote._spawn = fake_spawn
            quote._get_candidate = fake_get
            result = asyncio.run(quote.handle_callback(event))
        finally:
            quote._spawn = original_spawn
            quote._get_candidate = original_get
            quote._recent_callbacks.clear()
            quote._recent_callbacks.update(original_recent)

        self.assertEqual("success", result["toast"]["type"])
        self.assertIn("已回填", result["toast"]["content"])
        self.assertEqual([], spawned)

    def test_process_callback_updates_only_current_record_and_patches_card(self):
        original_get = quote._get_candidate
        original_update = quote._update_candidate
        original_get_many = quote._get_candidates_by_ids
        original_patch = quote.amz_assistant.update_card
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

        try:
            quote._get_candidate = fake_get
            quote._update_candidate = fake_update
            quote._get_candidates_by_ids = fake_get_many
            quote.amz_assistant.update_card = fake_patch
            result = asyncio.run(quote._process_callback({
                "context": {"open_message_id": "om_proc"},
                "operator": {"union_id": "on_operator"},
                "action": {
                    "value": {
                        "action": quote.ACTION_SUBMIT,
                        "record_id": "rec1",
                        "batch_id": "batch-test",
                        "card_record_ids": ["rec1", "rec2"],
                    },
                    "form_value": {
                        "proc_cost_rec1": "18.5",
                        "proc_link_rec1": "https://detail.1688.com/offer/test.html",
                        "proc_note_rec1": "MOQ 100",
                    },
                },
            }))
        finally:
            quote._get_candidate = original_get
            quote._update_candidate = original_update
            quote._get_candidates_by_ids = original_get_many
            quote.amz_assistant.update_card = original_patch

        self.assertEqual("success", result["toast"]["type"])
        self.assertEqual("rec1", updates[0][0])
        self.assertEqual(18.5, updates[0][1]["采购成本RMB"])
        self.assertEqual("已回填", updates[0][1]["采购回填状态"])
        self.assertEqual(
            {"link": "https://detail.1688.com/offer/test.html", "text": "https://detail.1688.com/offer/test.html"},
            updates[0][1]["1688供应商链接"],
        )
        self.assertEqual(
            {"link": "https://detail.1688.com/offer/test.html", "text": "https://detail.1688.com/offer/test.html"},
            updates[0][1]["采购链接"],
        )
        self.assertEqual(1, len(patches))
        rendered = json.dumps(patches[0][1], ensure_ascii=False)
        self.assertIn("采购已回填", rendered)
        self.assertIn("proc_cost_rec2", rendered)

    def test_process_callback_accepts_nested_form_payload(self):
        original_get = quote._get_candidate
        original_update = quote._update_candidate
        original_get_many = quote._get_candidates_by_ids
        original_patch = quote.amz_assistant.update_card
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

        try:
            quote._get_candidate = fake_get
            quote._update_candidate = fake_update
            quote._get_candidates_by_ids = fake_get_many
            quote.amz_assistant.update_card = fake_patch
            result = asyncio.run(quote._process_callback({
                "context": {"open_message_id": "om_proc"},
                "operator": {"union_id": "on_operator"},
                "action": {
                    "value": {
                        "action": quote.ACTION_SUBMIT,
                        "record_id": "rec1",
                        "batch_id": "batch-test",
                        "card_record_ids": ["rec1", "rec2"],
                    },
                    "form_value": {
                        "proc_quote_form_rec1": {
                            "proc_cost_rec1": {"value": "19.6"},
                            "proc_link_rec1": {"value": "https://detail.1688.com/offer/nested.html"},
                            "proc_note_rec1": {"value": "嵌套表单"},
                        }
                    },
                },
            }))
        finally:
            quote._get_candidate = original_get
            quote._update_candidate = original_update
            quote._get_candidates_by_ids = original_get_many
            quote.amz_assistant.update_card = original_patch

        self.assertEqual("success", result["toast"]["type"])
        self.assertEqual("rec1", updates[0][0])
        self.assertEqual(19.6, updates[0][1]["采购成本RMB"])
        self.assertEqual(
            {"link": "https://detail.1688.com/offer/nested.html", "text": "https://detail.1688.com/offer/nested.html"},
            updates[0][1]["1688供应商链接"],
        )
        self.assertEqual(1, len(patches))

    def test_url_cell_preserves_bare_1688_offer_link(self):
        url = "https://detail.1688.com/offer/6150807684"
        self.assertEqual({"link": url, "text": url}, quote._url_cell(url))

    def test_update_candidate_raises_on_feishu_business_error(self):
        original_api = quote.feishu.api
        calls = []

        async def fake_api(method, path, body=None, which="bitable"):
            calls.append((method, path, body, which))
            return {"code": 1254030, "msg": "permission denied"}

        try:
            quote.feishu.api = fake_api
            with self.assertRaisesRegex(RuntimeError, "code=1254030"):
                asyncio.run(quote._update_candidate("rec1", {"采购回填状态": "已回填"}))
        finally:
            quote.feishu.api = original_api

        self.assertEqual("PUT", calls[0][0])
        self.assertEqual(quote.FEISHU_API_WHICH, calls[0][3])

    def test_send_quote_card_can_send_to_gray_recipients_when_enabled(self):
        original_frankie_only = quote.FRANKIE_ONLY
        original_get_many = quote._get_candidates_by_ids
        original_update = quote._update_candidate
        original_send_union = quote.amz_assistant.send_card_to_union
        original_send_chat = quote.amz_assistant.send_card_to_chat
        sent = []
        updates = []

        async def fake_get_many(record_ids):
            return [self._candidate(record_ids[0])]

        async def fake_update(record_id, fields):
            updates.append((record_id, fields))

        async def fake_send_union(union_id, card):
            sent.append(("union", union_id, card))
            return "om_union"

        async def fake_send_chat(chat_id, card):
            sent.append(("chat", chat_id, card))
            return "om_chat"

        try:
            quote.FRANKIE_ONLY = False
            quote._get_candidates_by_ids = fake_get_many
            quote._update_candidate = fake_update
            quote.amz_assistant.send_card_to_union = fake_send_union
            quote.amz_assistant.send_card_to_chat = fake_send_chat
            result = asyncio.run(quote.send_quote_card(
                mode="commit",
                record_ids=["rec1"],
                frankie_only=False,
                gray_union_ids=["on_purchase"],
                gray_chat_ids=["oc_purchase"],
            ))
        finally:
            quote.FRANKIE_ONLY = original_frankie_only
            quote._get_candidates_by_ids = original_get_many
            quote._update_candidate = original_update
            quote.amz_assistant.send_card_to_union = original_send_union
            quote.amz_assistant.send_card_to_chat = original_send_chat

        self.assertFalse(result["frankie_only"])
        self.assertEqual(["om_chat", "om_union"], result["message_ids"])
        self.assertEqual("om_chat,om_union", updates[0][1]["采购卡片消息ID"])
        self.assertEqual(("chat", "oc_purchase"), sent[0][:2])
        self.assertEqual(("union", "on_purchase"), sent[1][:2])

    def test_prepare_card_images_uploads_image_url_to_key(self):
        original_download = quote._download_image
        original_upload = quote.amz_assistant.upload_image_for_card
        original_cache = dict(quote._image_key_cache)

        async def fake_download(url):
            return b"fake-image-bytes" * 20, "image/jpeg"

        async def fake_upload(data, filename, content_type):
            self.assertIn("amz_B0CH1817WW", filename)
            self.assertEqual("image/jpeg", content_type)
            self.assertGreater(len(data), 100)
            return "img_uploaded_key"

        try:
            quote._image_key_cache.clear()
            quote._download_image = fake_download
            quote.amz_assistant.upload_image_for_card = fake_upload
            candidate = self._candidate("rec1")
            candidate.pop("image_key", None)
            asyncio.run(quote._prepare_card_images([candidate]))
        finally:
            quote._download_image = original_download
            quote.amz_assistant.upload_image_for_card = original_upload
            quote._image_key_cache.clear()
            quote._image_key_cache.update(original_cache)

        self.assertEqual("img_uploaded_key", candidate["image_key"])


if __name__ == "__main__":
    unittest.main()

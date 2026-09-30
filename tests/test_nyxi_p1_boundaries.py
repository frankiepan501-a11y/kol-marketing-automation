import copy
import unittest
from unittest.mock import AsyncMock, patch
from app import (ship_recon, manual_send_recon, auto_send, config, feishu, nyxi_trial, enrich, reply_monitor, reply_drafter,
                 sla_check, warm_recap, secondary_outreach, upload_register,
                 completion_report, upload_task_report)


def contact(owned=True):
    return {"record_id": "kol1", "fields": {
        "账号名": "test", "邮箱": "person@example.invalid", "合作状态": "未建联",
        "迁移备注": nyxi_trial.CONTACT_MARKER if owned else ""}}


def draft(owned=True, brand="FUNLAB"):
    return {"record_id": "draft1", "fields": {
        "邮件草稿ID": nyxi_trial.DRAFT_PREFIX + "one" if owned else "old-one",
        "关联KOL": {"record_ids": ["kol1"]},
        "发送邮箱": config.BRAND_CONFIG[brand]["alias_from"], "发送时间": 1,
        "寄样阶段": "已签收", "签收时间": 1}}


class BoundaryTests(unittest.IsolatedAsyncioTestCase):
    def test_reservation_preserves_notes_and_denies_legacy_history(self):
        c = contact(False)
        c["fields"]["迁移备注"] = "original note"
        patch_fields = nyxi_trial.reservation_patch(c, [], [])
        self.assertTrue(patch_fields["迁移备注"].startswith("original note"))
        self.assertEqual(patch_fields["触达路由状态"], "待核对")
        with self.assertRaises(ValueError):
            nyxi_trial.reservation_patch(c, [draft(False)], [])
        with self.assertRaisesRegex(ValueError, "prior activity"):
            nyxi_trial.reservation_patch(c, [], [{"fields": {"参与状态": "已取消"}}])
        for field, value in (("合作状态", "待回复"), ("邮箱验真状态", "无效"),
                             ("上稿日期", 1), ("上次寄样日期", 1), ("上次寄样订单号", "ship"),
                             ("触达路由状态", "沿用原线程")):
            bad = copy.deepcopy(c)
            bad["fields"][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                nyxi_trial.reservation_patch(bad, [], [])

    def test_master_reservation_blocks_cold_even_when_route_corrupted(self):
        c = contact()
        for value in ("", "可新开发", "待核对"):
            c["fields"]["触达路由状态"] = value
            self.assertFalse(enrich._allows_new_cold_outreach(c["fields"]))
        self.assertTrue(enrich._allows_new_cold_outreach(contact(False)["fields"]))

    async def test_real_projection_preserves_marker_and_brand_selection(self):
        rows = [draft(), draft(False, "POWKONG")]
        rows[1]["record_id"] = "old-powkong"
        async def projected(_table, _filter, field_names):
            return [{"record_id": r["record_id"], "fields": {k:v for k,v in r["fields"].items()
                     if k in field_names}} for r in rows]
        reply_monitor._sent_drafts_cache["items"] = None
        with patch.object(feishu, "search_records", AsyncMock(side_effect=projected)):
            nyxi, _ = await reply_monitor.find_draft("kol1", "KOL", brand="FUNLAB")
            old, _ = await reply_monitor.find_draft("kol1", "KOL", brand="POWKONG")
        reply_monitor._sent_drafts_cache["items"] = None
        self.assertTrue(nyxi_trial.owns_draft(nyxi))
        self.assertEqual(old["record_id"], "old-powkong")
        self.assertFalse(nyxi_trial.owns_draft(old))

    async def test_reserved_contact_inbox_held_before_sent_draft_search(self):
        with patch.dict(config.BRAND_CONFIG, {"FUNLAB": {"alias_from":"partner@brand.invalid", "domain":"brand.invalid"}}, clear=True), \
             patch.object(reply_monitor.zoho, "list_inbox", AsyncMock(return_value=[{"fromAddress":"person@example.invalid", "messageId":"m"}])), \
             patch.object(reply_monitor, "find_contact", AsyncMock(return_value=(contact(), "KOL"))), \
             patch.object(reply_monitor, "find_draft", AsyncMock()) as find, \
             patch.object(feishu, "update_record", AsyncMock()) as write:
            result = await reply_monitor.run()
        self.assertEqual(result["results"][0]["skipped"], "nyxi_session_owned")
        find.assert_not_awaited()
        write.assert_not_awaited()

    async def test_fulfillment_sla_all_layers_do_not_mutate_owned_drafts(self):
        for fn in (sla_check._layer2_content_reminder, sla_check._layer3_no_content_30d,
                   sla_check._layer4_low_roi_60d, sla_check._layer1c_auto_sign_by_carrier,
                   sla_check._layer_soft_nudge):
            with self.subTest(fn=fn.__name__), \
                 patch.object(feishu, "search_records", AsyncMock(return_value=[draft()])), \
                 patch.object(feishu, "update_record", AsyncMock()) as update, \
                 patch.object(feishu, "create_record", AsyncMock()) as create, \
                 patch.object(feishu, "send_card_message", AsyncMock()) as card:
                result = await fn(2000000000000)
                self.assertEqual(result["skipped"], 1)
                for m in (update, create, card):
                    m.assert_not_awaited()

    async def test_warm_recap_and_direct_reply_held(self):
        with patch.object(feishu, "create_record", AsyncMock()) as create:
            result = await warm_recap.build_for_ship_draft(draft())
            self.assertTrue(result["skipped"])
            reply = await reply_drafter.draft_reply(contact(), "KOL", "FUNLAB", "感兴趣", "", "subject", "body", "partner@brand.invalid")
            self.assertIsNone(reply)
        create.assert_not_awaited()

    async def test_secondary_and_upload_candidates_preserve_old(self):
        old = contact(False)
        old["record_id"] = "old"
        old["fields"].update({"上次寄样日期": 1, "上次寄样订单号": "ship1"})
        with patch.object(feishu, "search_records", AsyncMock(side_effect=[[contact(), old], [], []])):
            result = await secondary_outreach._eligible_kols()
        self.assertEqual([r["record_id"] for r in result], ["old"])
        spec = next(iter(upload_register.SPECS.values()))
        with patch.object(feishu, "search_records", AsyncMock(return_value=[contact(), old])):
            result = await upload_register._scan(spec, 2000000000000)
        self.assertEqual([r["record_id"] for r in result], ["old"])

    async def test_completion_report_only_legacy_relationship(self):
        owned, old = contact(), contact(False)
        old["record_id"] = "old"
        owned["fields"]["合作状态"] = "已合作-免费"
        old["fields"]["合作状态"] = "已合作-免费"
        spec = completion_report.SPECS[0]
        result = await completion_report._compute(spec, [draft()], [owned, old], 2000000000000)
        self.assertEqual(result["funnel"]["engaged"], 1)
        self.assertIn("迁移备注", completion_report._contact_fields(spec))
        self.assertIn("邮件草稿ID", completion_report.DRAFT_FIELDS)

    async def test_receipt_persist_and_readback_without_new_enums(self):
        row = draft()
        async def get(*args):return copy.deepcopy(row)
        async def update(_table, _rid, fields):row["fields"].update(fields)
        with patch.object(feishu, "get_record", AsyncMock(side_effect=get)), \
             patch.object(feishu, "update_record", AsyncMock(side_effect=update)) as write:
            result = await nyxi_trial.persist_accepted_receipt("draft1", "FUNLAB", "accepted1")
            await nyxi_trial.persist_accepted_receipt("draft1", "FUNLAB", "accepted1")
            with self.assertRaises(ValueError):
                await nyxi_trial.persist_accepted_receipt("draft1", "FUNLAB", "other")
        self.assertEqual(write.await_count, 1)
        self.assertEqual(set(write.await_args.args[2]), {"发送错误"})
        self.assertTrue(result["do_not_resend"])
        self.assertIn("accepted1", row["fields"]["发送错误"])

    async def test_receipt_readback_failure_is_not_success(self):
        with patch.object(feishu, "get_record", AsyncMock(return_value=draft())), \
             patch.object(feishu, "update_record", AsyncMock()):
            with self.assertRaises(RuntimeError):
                await nyxi_trial.persist_accepted_receipt("draft1", "FUNLAB", "accepted1")

    async def test_contact_lookup_failure_cannot_bypass_ownership(self):
        row = draft(False)
        row["fields"].update({"邮件草稿来源": "reply", "收件邮箱": "person@example.invalid",
                               "邮件主题": "test subject", "邮件正文": "x" * 100})
        with patch.object(feishu, "get_record", AsyncMock(side_effect=TimeoutError())), \
             patch.object(auto_send.zoho, "send_email", AsyncMock()) as send:
            result = await auto_send.send_one(row)
        self.assertEqual(result["reason"], "contact_gate_unavailable")
        send.assert_not_awaited()

    async def test_reserved_contact_blocks_unmarked_stale_draft(self):
        row = draft(False)
        row["fields"].update({"邮件草稿来源": "reply", "收件邮箱": "person@example.invalid",
                               "邮件主题": "test subject", "邮件正文": "x" * 100})
        with patch.object(feishu, "get_record", AsyncMock(return_value=contact())), \
             patch.object(auto_send.zoho, "send_email", AsyncMock()) as send:
            result = await auto_send.send_one(row)
        self.assertEqual(result["reason"], "nyxi_session_owned")
        send.assert_not_awaited()

    async def test_ship_reconciliation_does_not_reclassify_owned_draft(self):
        with patch.object(feishu, "search_records", AsyncMock(return_value=[draft()])), \
             patch.object(feishu, "update_record", AsyncMock()) as update, \
             patch.object(ship_recon, "_list_sent_paged", AsyncMock()) as sent:
            result = await ship_recon.run()
        self.assertEqual(result["details"][0]["result"], "nyxi_session_owned")
        update.assert_not_awaited()
        sent.assert_not_awaited()

    async def test_manual_reconciliation_cannot_create_unowned_draft(self):
        with patch.object(feishu, "fetch_all_records", AsyncMock(side_effect=[[contact()], [], []])), \
             patch.object(manual_send_recon.zoho, "list_sent_messages", AsyncMock(return_value={"messages":[{"toAddress":"person@example.invalid"}]})), \
             patch.object(feishu, "create_record", AsyncMock()) as create:
            result = await manual_send_recon.run(dry_run=True)
        self.assertEqual(result["candidates"], 0)
        create.assert_not_awaited()

    async def test_product_report_excludes_owned_counts_and_keeps_old(self):
        owned, old = contact(), contact(False)
        old["record_id"] = "old"
        d1, d2 = draft(), draft(False)
        d2["fields"]["关联KOL"] = {"record_ids": ["old"]}
        for d in (d1,d2):
            d["fields"].update({"关联产品": {"record_ids": ["prod"]}, "发送状态":"已发"})
        for c in (owned, old):
            c["fields"]["内容风格"] = ["hardware"]
        task = {"record_id":"task", "fields":{"目标产品":{"record_ids":["prod"]},"任务名":"product", "筛选-内容风格":["hardware"]}}
        with patch.object(feishu, "fetch_all_records", AsyncMock(side_effect=[[task], [owned,old]])):
            result = await upload_task_report._compute_spec(upload_task_report.SPECS[0], [d1,d2], "week")
        self.assertEqual(result[0]["sent_u"], 1)
        self.assertEqual(result[0]["pool"], 1)

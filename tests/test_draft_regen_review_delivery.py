import unittest
from unittest import mock

from app import draft_router


class DraftRegenReviewDeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_pending_scan_preserves_regenerated_draft_human_review(self):
        for score in (2, 10):
            for draft_id, expected in (("reply-example-rg1", "notify_human"),
                                       ("reply-example-rg1-rg2", "notify_human"),
                                       ("reply-example", "auto_send" if score == 10 else "retry")):
                with self.subTest(score=score, draft_id=draft_id):
                    rec = {"record_id": "rec_new", "fields": {
                        "邮件草稿ID": draft_id, "邮件草稿状态": "待审",
                        "邮件主题": "Test", "邮件正文": "Test body", "重生次数": 1,
                    }}
                    review = {"score": score, "committed": False, "keywords_hit": [],
                              "summary": "test", "reasons": {},
                              "ai_commitment_judge": {"reason": "", "verdict": "none"}}
                    update = mock.AsyncMock()
                    notify = mock.AsyncMock()
                    with mock.patch.object(draft_router.feishu, "search_records", new=mock.AsyncMock(return_value=[rec])), \
                         mock.patch.object(draft_router.feishu, "get_record", new=mock.AsyncMock(return_value=rec)), \
                         mock.patch.object(draft_router.feishu, "update_record", new=update), \
                         mock.patch.object(draft_router.reviewer, "review_draft", new=mock.AsyncMock(return_value=review)), \
                         mock.patch.object(draft_router, "_notify_human_review", new=notify):
                        result = await draft_router.batch_review_pending()
                    self.assertEqual(expected, result["details"][0]["action"])
                    if expected == "notify_human":
                        self.assertEqual("待审", update.call_args.args[2]["邮件草稿状态"])
                        self.assertEqual("待人审", update.call_args.args[2]["审核路径"])
                        self.assertNotIn("重生次数", update.call_args.args[2])
                        notify.assert_awaited_once()

    async def test_force_review_reason_overrides_low_score_retry_route(self):
        rec = {
            "record_id": "rec_new",
            "fields": {
                "邮件主题": "A subject",
                "邮件正文": "A sufficiently complete draft body for review.",
                "邮件草稿来源": "cold",
                "对象类型": "KOL",
                "重生次数": 1,
            },
        }
        review = {
            "score": 2,
            "committed": False,
            "keywords_hit": [],
            "summary": "needs work",
            "reasons": {},
            "ai_commitment_judge": {"reason": "", "verdict": "none"},
        }
        updates = []

        async def fake_update(table_id, record_id, fields):
            updates.append(fields)

        notify = mock.AsyncMock(return_value={"action_delivered": True})
        with mock.patch.object(draft_router.feishu, "get_record", new=mock.AsyncMock(return_value=rec)), \
             mock.patch.object(draft_router.reviewer, "review_draft", new=mock.AsyncMock(return_value=review)), \
             mock.patch.object(draft_router.feishu, "update_record", new=fake_update), \
             mock.patch.object(draft_router, "_notify_human_review", new=notify):
            result = await draft_router.route_draft(
                "rec_new", force_review_reason="regenerated draft must be reviewed"
            )

        self.assertEqual("notify_human", result["action"])
        self.assertEqual("待审", result["status"])
        self.assertEqual("待人审", result["path"])
        self.assertEqual("待审", updates[0]["邮件草稿状态"])
        notify.assert_awaited_once()

    async def test_zero_app3_action_cards_raises_delivery_failure(self):
        rec = {
            "record_id": "rec_new",
            "fields": {
                "邮件主题": "A subject",
                "邮件正文": "A sufficiently complete draft body for review.",
                "邮件草稿来源": "cold",
                "对象类型": "KOL",
            },
        }
        async_noop = mock.AsyncMock(return_value=None)
        with mock.patch.object(
            draft_router.feishu, "resolve_notify_targets",
            new=mock.AsyncMock(return_value=[("Reviewer", "ou_reviewer")]),
        ), mock.patch.object(
            draft_router.feishu, "send_card_message",
            new=mock.AsyncMock(return_value="om_group"),
        ), mock.patch.object(
            draft_router.feishu, "open_id_to_union_id",
            new=mock.AsyncMock(return_value="on_reviewer"),
        ), mock.patch.object(
            draft_router.feishu, "send_card_via_app3",
            new=mock.AsyncMock(return_value=""),
        ), mock.patch.object(
            draft_router.feishu, "write_card_recipients_msgids", new=async_noop,
        ), mock.patch.object(
            draft_router.feishu, "mark_card_receipt", new=async_noop,
        ):
            with self.assertRaisesRegex(RuntimeError, "0 App3 review cards delivered"):
                await draft_router._notify_human_review(
                    "rec_new", rec, 6, True, "review", "reason", "待人审"
                )


if __name__ == "__main__":
    unittest.main()

import unittest
from unittest.mock import AsyncMock, patch

from app import reply_drafter, reply_monitor


def row(rid, subject, product, sent, replied=False):
    return {"record_id": rid, "fields": {
        "关联KOL": {"record_ids": ["switchplay"]},
        "发送邮箱": "partner@fireflyfunlab.com",
        "邮件主题": subject,
        "关联产品": {"record_ids": [product]},
        "发送时间": sent,
        "是否回复": replied,
    }}


class SwitchPlayStageReplyTests(unittest.IsolatedAsyncioTestCase):
    async def test_same_thread_beats_unreplied_other_product(self):
        records = [
            row("unrelated", "SwitchPlay Gaming, ceci est pour vous", "YS43", 20),
            row("same-thread", "Re: FUNLAB x SwitchPlay — Switch 2 Pro Controller", "KAKARIKO", 10, True),
        ]
        with patch.object(reply_monitor, "_get_sent_drafts", AsyncMock(return_value=records)):
            chosen, _ = await reply_monitor.find_draft(
                "switchplay", "KOL", brand="FUNLAB",
                subject="Re: Re: Re: FUNLAB x SwitchPlay — Switch 2 Pro Controller",
            )
        self.assertEqual("same-thread", chosen["record_id"])

    def test_sample_received_during_editing_cannot_request_address(self):
        body = "Yes, I have received it. I recorded the unboxing video last night and am editing it."
        self.assertEqual("sample_received_editing", reply_drafter._received_stage_sub(body, True))
        self.assertNotIn("shipping details", reply_drafter.TEMPLATE_SAMPLE_RECEIVED_EDITING.lower())
        self.assertNotIn("on its way", reply_drafter.TEMPLATE_SAMPLE_RECEIVED_EDITING.lower())


if __name__ == "__main__":
    unittest.main()

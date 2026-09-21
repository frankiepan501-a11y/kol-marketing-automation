import asyncio
import datetime
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("INTERNAL_TOKEN", "test-token")

from app import feishu
from app.weekly_report import publisher


class WeeklyReportRoleRoutingTests(unittest.TestCase):
    def test_weekly_recipients_keep_frankie_and_resolve_every_active_operator_by_job_title(self):
        operators = [("张佳烨", "on_zhang"), ("叶星", "on_ye")]
        with patch.object(
            feishu,
            "fetch_users_by_job_title",
            new=AsyncMock(return_value=operators),
        ) as lookup:
            recipients = asyncio.run(publisher.resolve_weekly_recipients())

        lookup.assert_awaited_once_with(
            "独立站运营专员", which="notify", department_ids=[]
        )
        self.assertEqual(
            [
                ("Frankie", "open_id", publisher.FRANKIE_OPEN_ID),
                ("张佳烨", "union_id", "on_zhang"),
                ("叶星", "union_id", "on_ye"),
            ],
            recipients,
        )

    def test_weekly_recipients_fail_before_publish_when_job_title_has_no_active_operator(self):
        with patch.object(
            feishu,
            "fetch_users_by_job_title",
            new=AsyncMock(return_value=[]),
        ):
            with self.assertRaisesRegex(RuntimeError, "独立站运营专员"):
                asyncio.run(publisher.resolve_weekly_recipients())

    def test_publish_sends_to_resolved_recipient_types(self):
        recipients = [
            ("Frankie", "open_id", "ou_frankie"),
            ("张佳烨", "union_id", "on_zhang"),
            ("叶星", "union_id", "on_ye"),
        ]
        sender = AsyncMock(return_value=(True, ""))
        with patch.object(
            publisher, "resolve_weekly_recipients", new=AsyncMock(return_value=recipients)
        ), patch.object(
            publisher, "_write_history_bitable", new=AsyncMock(return_value={"record_id": "rec1"})
        ), patch.object(publisher, "_send_card", new=sender), patch.object(
            publisher, "DEFAULT_PARENT_NODE", ""
        ):
            result = asyncio.run(
                publisher.publish(
                    "",
                    "# report",
                    {},
                    datetime.date(2026, 9, 14),
                    datetime.date(2026, 9, 20),
                    [],
                )
            )

        self.assertEqual(
            [("open_id", "ou_frankie"), ("union_id", "on_zhang"), ("union_id", "on_ye")],
            [(call.args[0], call.args[1]) for call in sender.await_args_list],
        )
        notify = next(item for item in result["actions"] if item["step"] == "notify")
        self.assertEqual(["Frankie", "张佳烨", "叶星"], [r["name"] for r in notify["recipients"]])


if __name__ == "__main__":
    unittest.main()

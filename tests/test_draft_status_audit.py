import unittest

from app.draft_status_audit import classify_issue


def rec(fields):
    return {"record_id": "rec_test", "fields": fields}


class DraftStatusAuditTest(unittest.TestCase):
    def test_ignores_unsent_ready_draft(self):
        issue = classify_issue(rec({
            "邮件草稿状态": "通过",
            "发送状态": "",
        }))
        self.assertIsNone(issue)

    def test_ignores_consistent_sent_draft(self):
        issue = classify_issue(rec({
            "邮件草稿状态": "已发送",
            "发送状态": "已发",
            "发送时间": 1770000000000,
        }))
        self.assertIsNone(issue)

    def test_passed_sent_draft_is_auto_fixable(self):
        issue = classify_issue(rec({
            "邮件草稿ID": "draft-1",
            "邮件草稿状态": "通过",
            "发送状态": "已发",
            "发送时间": 1770000000000,
        }))
        self.assertIsNotNone(issue)
        self.assertTrue(issue["auto_fixable"])
        self.assertEqual(issue["reason"], "safe_autofix")

    def test_rejected_sent_draft_is_report_only(self):
        issue = classify_issue(rec({
            "邮件草稿ID": "draft-2",
            "邮件草稿状态": "已否决",
            "发送状态": "已发",
            "发送时间": 1770000000000,
        }))
        self.assertIsNotNone(issue)
        self.assertFalse(issue["auto_fixable"])
        self.assertEqual(issue["reason"], "manual_review_status")

    def test_missing_send_time_is_report_only(self):
        issue = classify_issue(rec({
            "邮件草稿ID": "draft-3",
            "邮件草稿状态": "通过",
            "发送状态": "已发",
        }))
        self.assertIsNotNone(issue)
        self.assertFalse(issue["auto_fixable"])
        self.assertEqual(issue["reason"], "missing_send_time")


if __name__ == "__main__":
    unittest.main()

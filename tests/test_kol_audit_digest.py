import unittest

from app.kol_audit_digest import _duplicate_actionable, _status_actionable


class KolAuditDigestTest(unittest.TestCase):
    def test_clean_audits_are_not_actionable(self):
        status = {"fixed_count": 0, "update_error_count": 0, "report_only_count": 0}
        dup = {"fixed_count": 0, "update_error_count": 0, "auto_fixable_count": 0}
        self.assertEqual(0, _status_actionable(status, notify_report_only=False))
        self.assertEqual(0, _duplicate_actionable(dup, dry_run=False))

    def test_auto_fixed_status_is_actionable(self):
        status = {"fixed_count": 2, "update_error_count": 0, "report_only_count": 0}
        self.assertEqual(2, _status_actionable(status, notify_report_only=False))

    def test_report_only_is_optional(self):
        status = {"fixed_count": 0, "update_error_count": 0, "report_only_count": 2}
        self.assertEqual(0, _status_actionable(status, notify_report_only=False))
        self.assertEqual(2, _status_actionable(status, notify_report_only=True))

    def test_duplicate_dry_run_counts_pending(self):
        dup = {"fixed_count": 0, "update_error_count": 0, "auto_fixable_count": 3}
        self.assertEqual(3, _duplicate_actionable(dup, dry_run=True))
        self.assertEqual(0, _duplicate_actionable(dup, dry_run=False))

    def test_duplicate_auto_fix_counts_fixed(self):
        dup = {"fixed_count": 4, "update_error_count": 0, "auto_fixable_count": 4}
        self.assertEqual(4, _duplicate_actionable(dup, dry_run=False))


if __name__ == "__main__":
    unittest.main()

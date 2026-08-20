import unittest

from app import feishu


class CleanEmailTests(unittest.TestCase):
    def test_prefers_clean_duplicate_over_scraped_greeting_suffix(self):
        raw = (
            "TotallyTubularJonathan@incharacteragency.comHowdy, "
            "TotallyTubularJonathan@incharacteragency.com, "
            "TotallyTubularJonathan@incharacteragency.com...tap"
        )

        email, reason = feishu.clean_email(raw)

        self.assertEqual(email, "totallytubularjonathan@incharacteragency.com")
        self.assertIn("干净版本", reason)

    def test_rejects_single_email_with_scraped_greeting_suffix(self):
        email, reason = feishu.clean_email("creator@example.comHowdy")

        self.assertEqual(email, "")
        self.assertIn("疑似粘连", reason)

    def test_keeps_first_mailbox_when_multiple_people_are_listed(self):
        email, reason = feishu.clean_email("first@example.com; second@example.net")

        self.assertEqual(email, "first@example.com")
        self.assertIn("2 个邮箱", reason)


if __name__ == "__main__":
    unittest.main()

import unittest

from app.draft_duplicate_audit import build_duplicate_groups, plan_auto_denials


def rec(rid, **fields):
    return {"record_id": rid, "fields": fields}


def link(rid):
    return {"link_record_ids": [rid]}


class DraftDuplicateAuditTest(unittest.TestCase):
    def test_sent_record_denies_ready_duplicate(self):
        records = [
            rec("sent", 邮件草稿ID="draft-1", 邮件草稿状态="已发送", 发送状态="已发",
                邮件草稿来源="cold", 关联KOL=link("kol1"), 关联产品=link("prod1"),
                发送邮箱="partner@fireflyfunlab.com"),
            rec("ready", 邮件草稿ID="draft-1", 邮件草稿状态="自动通过", 发送状态="",
                邮件草稿来源="cold", 关联KOL=link("kol1"), 关联产品=link("prod1"),
                发送邮箱="partner@fireflyfunlab.com"),
        ]
        plan = plan_auto_denials(build_duplicate_groups(records))
        self.assertIn("ready", plan)
        self.assertNotIn("sent", plan)
        self.assertIn("已有已发记录", plan["ready"]["reason"])

    def test_multiple_ready_keeps_earliest(self):
        records = [
            rec("r2", 邮件草稿ID="draft-2", 邮件草稿状态="通过", 发送状态="",
                邮件草稿来源="cold", 关联KOL=link("kol2"), 关联产品=link("prod2"),
                发送邮箱="partner@powkong.com", 生成时间=200),
            rec("r1", 邮件草稿ID="draft-2", 邮件草稿状态="通过", 发送状态="",
                邮件草稿来源="cold", 关联KOL=link("kol2"), 关联产品=link("prod2"),
                发送邮箱="partner@powkong.com", 生成时间=100),
            rec("r3", 邮件草稿ID="draft-2", 邮件草稿状态="自动通过", 发送状态="未发",
                邮件草稿来源="cold", 关联KOL=link("kol2"), 关联产品=link("prod2"),
                发送邮箱="partner@powkong.com", 生成时间=300),
        ]
        plan = plan_auto_denials(build_duplicate_groups(records))
        self.assertNotIn("r1", plan)
        self.assertIn("r2", plan)
        self.assertIn("r3", plan)

    def test_cold_business_key_dedups_different_draft_ids(self):
        records = [
            rec("keep", 邮件草稿ID="a", 邮件草稿状态="通过", 发送状态="",
                邮件草稿来源="cold", 关联KOL=link("kol3"), 关联产品=link("prod3"),
                发送邮箱="partner@fireflyfunlab.com", 生成时间=100),
            rec("deny", 邮件草稿ID="b", 邮件草稿状态="通过", 发送状态="",
                邮件草稿来源="cold", 关联KOL=link("kol3"), 关联产品=link("prod3"),
                发送邮箱="partner@fireflyfunlab.com", 生成时间=200),
        ]
        plan = plan_auto_denials(build_duplicate_groups(records))
        self.assertNotIn("keep", plan)
        self.assertIn("deny", plan)
        self.assertIn("同一联系人×产品×品牌", plan["deny"]["reason"])

    def test_reply_is_not_business_key_deduped(self):
        records = [
            rec("reply1", 邮件草稿ID="reply-a", 邮件草稿状态="通过", 发送状态="",
                邮件草稿来源="reply", 关联KOL=link("kol4"), 关联产品=link("prod4"),
                发送邮箱="partner@powkong.com", 生成时间=100),
            rec("reply2", 邮件草稿ID="reply-b", 邮件草稿状态="通过", 发送状态="",
                邮件草稿来源="reply", 关联KOL=link("kol4"), 关联产品=link("prod4"),
                发送邮箱="partner@powkong.com", 生成时间=200),
        ]
        plan = plan_auto_denials(build_duplicate_groups(records))
        self.assertEqual({}, plan)

    def test_chained_duplicates_do_not_over_deny(self):
        records = [
            # A and B collide by cold key, so B should be denied.
            rec("a", 邮件草稿ID="draft-a", 邮件草稿状态="通过", 发送状态="",
                邮件草稿来源="cold", 关联KOL=link("kol5"), 关联产品=link("prod5"),
                发送邮箱="partner@fireflyfunlab.com", 生成时间=100),
            rec("b", 邮件草稿ID="draft-b", 邮件草稿状态="通过", 发送状态="",
                邮件草稿来源="cold", 关联KOL=link("kol5"), 关联产品=link("prod5"),
                发送邮箱="partner@fireflyfunlab.com", 生成时间=200),
            # B and C collide by draft ID. Because B is denied, C remains the
            # first kept record for draft-b and should not be denied.
            rec("c", 邮件草稿ID="draft-b", 邮件草稿状态="通过", 发送状态="",
                邮件草稿来源="cold", 关联KOL=link("kol6"), 关联产品=link("prod6"),
                发送邮箱="partner@fireflyfunlab.com", 生成时间=300),
        ]
        plan = plan_auto_denials(build_duplicate_groups(records))
        self.assertEqual({"b"}, set(plan))


if __name__ == "__main__":
    unittest.main()

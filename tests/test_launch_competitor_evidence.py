import unittest

from app import launch_competitor_evidence as evidence


class LaunchCompetitorEvidenceTests(unittest.TestCase):
    def test_long_term_and_p75_match_is_level_a(self):
        contact = {
            "record_id": "kol1",
            "fields": {"主平台": "YouTube", "账号名": "Creator One"},
        }
        posts = []
        for index, views in enumerate([100, 200, 300, 400, 500, 600, 700, 800], start=1):
            posts.append({
                "record_id": f"post{index}",
                "fields": {
                    "平台": "YouTube", "内容类型": "长视频", "曝光量": views,
                    "发布时间": 1_700_000_000_000 + index * 1000,
                    "人工复核状态": "已确认", "相关性": "相关", "合作信号": "明确合作",
                    "关联KOL": ["other"],
                },
            })
        posts[0]["fields"].update({"关联KOL": ["kol1"], "发布时间": 1_690_000_000_000})
        posts[7]["fields"].update({"关联KOL": ["kol1"], "发布时间": 1_700_000_000_000})

        result = evidence.rank_contact_evidence(contact, posts, base_score=100)

        self.assertEqual("A", result["evidence_level"])
        self.assertEqual(3100, result["final_priority"])
        self.assertTrue(result["long_term"])
        self.assertTrue(result["high_performance"])
        self.assertEqual(600, result["p75_thresholds"]["YouTube|长视频"])

    def test_display_name_similarity_does_not_match_identity(self):
        contact = {
            "record_id": "kol1",
            "fields": {"主平台": "YouTube", "账号名": "Game Review Pro"},
        }
        post = {
            "record_id": "post1",
            "fields": {
                "平台": "YouTube", "内容类型": "长视频", "曝光量": 999999,
                "人工复核状态": "已确认", "相关性": "相关", "合作信号": "明确合作",
                "作者名称": "Game Reviews Professional",
            },
        }

        result = evidence.rank_contact_evidence(contact, [post], base_score=88)

        self.assertEqual("无加分", result["evidence_level"])
        self.assertEqual(88, result["final_priority"])
        self.assertEqual([], result["matched_post_ids"])


if __name__ == "__main__":
    unittest.main()

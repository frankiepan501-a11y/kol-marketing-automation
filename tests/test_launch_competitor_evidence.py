import unittest

from app import launch_competitor_evidence as evidence


class LaunchCompetitorEvidenceTests(unittest.TestCase):
    def test_nyxi_non_official_post_is_rule_inferred_partner_evidence(self):
        fields = {
            "竞品品牌": "NYXI", "平台": "YouTube",
            "KOL平台ID": "UC-creator", "KOL账号Handle": "creator-one",
            "人工复核状态": "待复核", "相关性": "疑似", "合作信号": "待分析",
        }

        self.assertEqual("rule_inferred_non_official", evidence.evidence_basis(fields))
        self.assertFalse(evidence.is_nyxi_official_post(fields))

    def test_nyxi_official_channels_are_not_partner_evidence(self):
        for fields in (
            {
                "竞品品牌": "NYXI", "KOL平台ID": "UCIY4yC2qUCPcM7ws-xTARYg",
                "人工复核状态": "已确认", "相关性": "相关", "合作信号": "明确合作",
            },
            {"竞品品牌": "NYXI", "KOL账号Handle": "@NyxiGaming"},
            {"竞品品牌": "NYXI", "采集来源": ["YouTube官方频道"]},
        ):
            self.assertTrue(evidence.is_nyxi_official_post(fields))
            self.assertEqual("", evidence.evidence_basis(fields))

    def test_other_brand_still_requires_manual_confirmation(self):
        fields = {
            "竞品品牌": "Other", "人工复核状态": "待复核",
            "相关性": "相关", "合作信号": "待分析",
        }

        self.assertEqual("", evidence.evidence_basis(fields))

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
        self.assertEqual(["kol_record:kol1"], result["stable_identity_keys"])

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

    def test_matches_current_base_creator_id_fields_without_relation(self):
        contact = {
            "record_id": "kol1",
            "fields": {
                "主平台": "YouTube", "账号名": "Creator One",
                "YouTube频道ID": "UC-current-schema",
                "主链接": "https://youtube.com/@creatorone",
            },
        }
        post = {
            "record_id": "post1",
            "fields": {
                "平台": "YouTube", "内容类型": "评测", "曝光量": 50000,
                "人工复核状态": "已确认", "相关性": "相关", "合作信号": "明确合作",
                "KOL平台ID": "UC-current-schema",
                "KOL主页URL": "https://youtube.com/@creatorone",
                "KOL账号Handle": "creatorone",
            },
        }

        result = evidence.rank_contact_evidence(contact, [post], base_score=88)

        self.assertEqual("C", result["evidence_level"])
        self.assertEqual(["platform_creator_id"], result["identity_paths"])
        self.assertEqual(["youtube|creator:UC-current-schema"], result["stable_identity_keys"])

    def test_rule_inferred_post_keeps_provenance_in_rank_output(self):
        contact = {
            "record_id": "kol1",
            "fields": {"主平台": "YouTube", "YouTube频道ID": "UC-creator"},
        }
        post = {
            "record_id": "post1",
            "fields": {
                "竞品品牌": "NYXI", "平台": "YouTube", "内容类型": "评测",
                "曝光量": 1000, "KOL平台ID": "UC-creator",
                "人工复核状态": "待复核", "相关性": "疑似", "合作信号": "待分析",
            },
        }

        result = evidence.rank_contact_evidence(contact, [post], base_score=80)

        self.assertEqual("C", result["evidence_level"])
        self.assertEqual("rule_inferred_non_official", result["evidence_posts"][0]["evidence_basis"])


if __name__ == "__main__":
    unittest.main()

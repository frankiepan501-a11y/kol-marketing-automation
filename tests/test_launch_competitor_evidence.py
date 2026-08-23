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

    def test_rank_output_uses_url_link_instead_of_display_text(self):
        contact = {
            "record_id": "kol1",
            "fields": {"主平台": "YouTube", "YouTube频道ID": "UC-creator"},
        }
        post = {
            "record_id": "post1",
            "fields": {
                "竞品品牌": "NYXI", "平台": "YouTube", "内容类型": "评测",
                "曝光量": 1000, "KOL平台ID": "UC-creator",
                "帖子URL": {"text": "查看帖子", "link": "https://youtube.com/watch?v=post1"},
            },
        }

        result = evidence.rank_contact_evidence(contact, [post], base_score=80)

        self.assertEqual(
            "https://youtube.com/watch?v=post1",
            result["evidence_posts"][0]["post_url"],
        )

    def test_indexed_rank_matches_legacy_rank_and_reports_coverage(self):
        contacts = [{
            "record_id": "kol1",
            "fields": {
                "主平台": "YouTube", "YouTube频道ID": "UC-one",
                "账号名": "Creator One", "主链接": "https://youtube.com/@creatorone",
            },
        }]
        posts = [
            {"record_id": "post1", "fields": {
                "竞品品牌": "NYXI", "平台": "YouTube", "内容类型": "评测",
                "KOL平台ID": "UC-one", "KOL账号Handle": "creatorone",
                "KOL主页URL": "https://youtube.com/@creatorone", "曝光量": 100,
            }},
            {"record_id": "post2", "fields": {
                "竞品品牌": "NYXI", "平台": "YouTube", "内容类型": "评测",
                "KOL平台ID": "UC-two", "KOL账号Handle": "creatortwo",
                "KOL主页URL": "https://youtube.com/@creatortwo", "曝光量": 200,
            }},
            {"record_id": "official", "fields": {
                "竞品品牌": "NYXI", "平台": "YouTube", "内容类型": "评测",
                "KOL平台ID": "UCIY4yC2qUCPcM7ws-xTARYg", "KOL账号Handle": "nyxigaming",
                "曝光量": 999,
            }},
        ]

        index = evidence.build_evidence_index(posts)
        indexed = evidence.rank_contact_evidence_from_index(
            contacts[0], index, base_score=80,
        )
        legacy = evidence.rank_contact_evidence(contacts[0], posts, base_score=80)
        coverage = evidence.summarize_evidence_coverage(index, contacts)

        self.assertEqual(legacy["matched_post_ids"], indexed["matched_post_ids"])
        self.assertEqual(3, coverage["linked_posts_total"])
        self.assertEqual(2, coverage["valid_partner_posts"])
        self.assertEqual(1, coverage["official_excluded"])
        self.assertEqual(2, coverage["distinct_authors"])
        self.assertEqual(1, coverage["matched_contacts"])
        self.assertEqual(1, coverage["matched_authors"])
        self.assertEqual(1, coverage["unmatched_authors"])

    def test_profile_url_identity_uses_link_not_display_text(self):
        contact = {"record_id": "kol1", "fields": {
            "主平台": "YouTube",
            "主链接": {"text": "打开达人主页", "link": "https://youtube.com/@creatorone"},
        }}
        post = {"record_id": "post1", "fields": {
            "竞品品牌": "NYXI", "平台": "YouTube", "内容类型": "评测",
            "KOL主页URL": {"text": "来源主页", "link": "https://youtube.com/@creatorone"},
            "曝光量": 100,
        }}

        result = evidence.rank_contact_evidence(contact, [post], base_score=80)

        self.assertEqual(["profile_url"], result["identity_paths"])
        self.assertEqual(
            ["youtube|url:https://youtube.com/@creatorone"],
            result["stable_identity_keys"],
        )

    def test_profile_url_identity_accepts_lark_cli_markdown_link(self):
        contact = {"record_id": "kol1", "fields": {
            "主平台": "YouTube", "主链接": "[主页](https://youtube.com/@creatorone)",
        }}
        post = {"record_id": "post1", "fields": {
            "竞品品牌": "NYXI", "平台": "YouTube", "内容类型": "评测",
            "KOL主页URL": "[主页](https://youtube.com/@creatorone)", "曝光量": 100,
        }}

        result = evidence.rank_contact_evidence(contact, [post], base_score=80)

        self.assertEqual(["profile_url"], result["identity_paths"])

    def test_coverage_merges_partial_aliases_for_same_author(self):
        posts = [
            {"record_id": "post1", "fields": {
                "竞品品牌": "NYXI", "平台": "YouTube", "内容类型": "评测",
                "KOL平台ID": "UC-one", "KOL主页URL": "https://youtube.com/@creatorone",
            }},
            {"record_id": "post2", "fields": {
                "竞品品牌": "NYXI", "平台": "YouTube", "内容类型": "评测",
                "KOL主页URL": "https://youtube.com/@creatorone",
            }},
        ]

        coverage = evidence.summarize_evidence_coverage(
            evidence.build_evidence_index(posts), [],
        )

        self.assertEqual(1, coverage["distinct_authors"])

    def test_unmatched_author_sample_is_ranked_and_never_write_ready(self):
        posts = []
        for index, views in enumerate([100, 200, 300, 400, 500, 600, 700, 800], start=1):
            posts.append({"record_id": f"pool{index}", "fields": {
                "竞品品牌": "NYXI", "平台": "YouTube", "内容类型": "评测",
                "KOL平台ID": f"UC-pool-{index}", "KOL账号Handle": f"pool-{index}",
                "KOL主页URL": f"https://youtube.com/@pool-{index}",
                "曝光量": views, "发布时间": 1_700_000_000_000 + index * 1000,
            }})
        posts.extend([
            {"record_id": "strong-old", "fields": {
                "竞品品牌": "NYXI", "平台": "YouTube", "内容类型": "评测",
                "KOL平台ID": "UC-strong", "KOL账号Handle": "strong",
                "KOL账号名": "Strong Creator",
                "KOL主页URL": "https://youtube.com/@strong", "曝光量": 100,
                "发布时间": 1_690_000_000_000,
                "帖子URL": "https://youtube.com/watch?v=old",
            }},
            {"record_id": "strong-new", "fields": {
                "竞品品牌": "NYXI", "平台": "YouTube", "内容类型": "评测",
                "KOL平台ID": "UC-strong", "KOL账号Handle": "strong",
                "KOL账号名": "Strong Creator",
                "KOL主页URL": "https://youtube.com/@strong", "曝光量": 900,
                "发布时间": 1_700_000_000_000,
                "帖子URL": "https://youtube.com/watch?v=new",
            }},
            {"record_id": "matched", "fields": {
                "竞品品牌": "NYXI", "平台": "YouTube", "内容类型": "评测",
                "KOL平台ID": "UC-matched", "KOL账号Handle": "matched",
                "KOL主页URL": "https://youtube.com/@matched", "曝光量": 9999,
            }},
            {"record_id": "official", "fields": {
                "竞品品牌": "NYXI", "平台": "YouTube", "内容类型": "评测",
                "KOL平台ID": "UCIY4yC2qUCPcM7ws-xTARYg", "KOL账号Handle": "nyxigaming",
                "曝光量": 99999,
            }},
        ])
        contacts = [{"record_id": "kol1", "fields": {
            "主平台": "YouTube", "YouTube频道ID": "UC-matched",
            "主链接": "https://youtube.com/@matched",
        }}]

        result = evidence.rank_unmatched_author_candidates(
            evidence.build_evidence_index(posts), contacts, limit=20,
        )

        self.assertEqual(9, result["unmatched_authors"])
        self.assertEqual("UC-strong", result["candidates"][0]["creator_id"])
        self.assertEqual("A", result["candidates"][0]["evidence_level"])
        self.assertEqual(2, result["candidates"][0]["post_count"])
        self.assertEqual("https://youtube.com/@strong", result["candidates"][0]["profile_url"])
        self.assertEqual("needs_profile_enrichment", result["candidates"][0]["promotion_status"])
        self.assertFalse(result["candidates"][0]["eligible_for_master_write"])
        self.assertNotIn("UC-matched", {row["creator_id"] for row in result["candidates"]})
        self.assertNotIn(
            "UCIY4yC2qUCPcM7ws-xTARYg",
            {row["creator_id"] for row in result["candidates"]},
        )

    def test_author_prewrite_gate_fails_closed_until_every_business_gate_passes(self):
        incomplete = evidence.author_prewrite_gate(
            {
                "platform": "YouTube", "country": "US", "language": "en",
                "email": "", "content_text": "Nintendo Switch controller review",
                "is_official": False,
            },
            target_countries={"US", "DE", "ES"}, target_languages={"en", "de", "es"},
            semantic_cues={"nintendo", "switch", "controller", "dave the diver"},
        )
        passed = evidence.author_prewrite_gate(
            {
                "platform": "YouTube", "country": "US", "language": "en",
                "email": "creator@example.com",
                "content_text": "Dave the Diver Nintendo Switch controller review",
                "is_official": False,
            },
            target_countries={"US", "DE", "ES"}, target_languages={"en", "de", "es"},
            semantic_cues={"nintendo", "switch", "controller", "dave the diver"},
        )

        self.assertFalse(incomplete["passed"])
        self.assertIn("missing_valid_email", incomplete["reason_codes"])
        self.assertTrue(passed["passed"])
        self.assertEqual([], passed["reason_codes"])

    def test_relation_ids_accept_lark_cli_direct_id_shape(self):
        posts = [{"record_id": "post1", "fields": {
            "竞品品牌": "NYXI", "平台": "YouTube", "内容类型": "评测",
            "KOL平台ID": "UC-linked", "KOL账号Handle": "linked",
            "关联KOL": [{"id": "kol1", "text": "Linked Creator"}],
        }}]

        result = evidence.rank_unmatched_author_candidates(
            evidence.build_evidence_index(posts),
            [{"record_id": "kol1", "fields": {"主平台": "YouTube"}}],
            limit=20,
        )

        self.assertEqual(0, result["unmatched_authors"])
        self.assertEqual([], result["candidates"])

    def test_markdown_urls_are_returned_as_clickable_raw_urls(self):
        posts = [{"record_id": "post1", "fields": {
            "竞品品牌": "NYXI", "平台": "YouTube", "内容类型": "评测",
            "KOL平台ID": "UC-new", "KOL账号Handle": "new",
            "KOL主页URL": "[主页](https://youtube.com/@NewCreator)",
            "帖子URL": "[证据](https://youtube.com/watch?v=AbC123)",
        }}]

        result = evidence.rank_unmatched_author_candidates(
            evidence.build_evidence_index(posts), [], limit=20,
        )

        candidate = result["candidates"][0]
        self.assertEqual("https://youtube.com/@NewCreator", candidate["profile_url"])
        self.assertEqual(
            "https://youtube.com/watch?v=AbC123",
            candidate["evidence_posts"][0]["post_url"],
        )

    def test_timestamp_accepts_seconds_and_iso_strings(self):
        self.assertEqual(1_700_000_000_000, evidence._timestamp(1_700_000_000))
        self.assertEqual(
            1_700_000_000_000,
            evidence._timestamp("2023-11-14T22:13:20+00:00"),
        )


if __name__ == "__main__":
    unittest.main()

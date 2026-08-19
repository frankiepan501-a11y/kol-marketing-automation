import unittest

from scripts import attach_nyxi_full_evidence_user as attach


class AttachNyxiFullEvidenceUserTests(unittest.TestCase):
    def test_write_payload_preserves_events_and_uses_raw_api_value_shapes(self):
        fields = {
            "关联竞品帖子": [{"record_ids": ["sample1", "sample2"]}],
            "关联竞品营销事件": [{"record_ids": ["evt1", "evt2"]}],
        }
        metadata = {
            "ranking_version": "evidence-v4", "config_version": 2,
            "total_posts": 2988, "chunks": 50, "official_excluded": 435,
        }

        result = attach.activity_write_fields(fields, metadata, snapshot_ms=123456)

        self.assertEqual(["sample1", "sample2"], result["关联竞品帖子"])
        self.assertEqual(["evt1", "evt2"], result["关联竞品营销事件"])
        self.assertEqual(123456, result["证据快照时间"])
        self.assertTrue(result["证据等待/变更说明"].startswith(attach.SNAPSHOT_PREFIX))

    def test_split_2988_posts_into_50_bounded_chunks(self):
        record_ids = [f"post{index:04d}" for index in range(2988)]

        chunks = attach.split_chunks(record_ids)

        self.assertEqual(50, len(chunks))
        self.assertTrue(all(len(chunk) == 60 for chunk in chunks[:-1]))
        self.assertEqual(48, len(chunks[-1]))
        self.assertEqual(record_ids, [record_id for chunk in chunks for record_id in chunk])

    def test_author_components_merge_partial_identity_aliases(self):
        rows = [
            {"record_id": "post1", "fields": {
                "平台": ["YouTube"], "KOL平台ID": "UC-one",
                "KOL主页URL": "[主页](https://youtube.com/@creatorone)",
            }},
            {"record_id": "post2", "fields": {
                "平台": ["YouTube"],
                "KOL主页URL": "[主页](https://youtube.com/@creatorone)",
            }},
        ]

        components, _ = attach.canonical_author_components(rows)

        self.assertEqual(1, len(components))

    def test_activity_readback_requires_every_committed_field(self):
        metadata = {
            "ranking_version": "evidence-v4", "config_version": 2,
            "total_posts": 2988, "chunks": 50, "official_excluded": 435,
        }
        expected = attach.activity_write_fields(
            {
                "关联竞品帖子": [{"record_ids": ["post1"]}],
                "关联竞品营销事件": [{"record_ids": ["evt1"]}],
            },
            metadata, snapshot_ms=123456,
        )
        actual = dict(expected)
        actual["关联竞品帖子"] = [{"record_ids": ["post1"]}]
        actual["关联竞品营销事件"] = [{"record_ids": ["evt1"]}]

        self.assertTrue(attach.activity_fields_match(actual, expected))
        for field, bad_value in (
            ("竞品证据模式", "不使用竞品证据"),
            ("竞品分析状态", "配置无效"),
            ("竞品品牌", "OTHER"),
            ("证据配置版本", 1),
            ("证据排序版本", "evidence-v3"),
            ("证据快照时间", 999),
            ("证据等待/变更说明", "wrong"),
        ):
            changed = dict(actual)
            changed[field] = bad_value
            self.assertFalse(attach.activity_fields_match(changed, expected), field)

    def test_snapshot_rows_require_exact_chunks_and_closed_external_action(self):
        partner_ids = [f"post{index:03d}" for index in range(65)]
        result = {
            "official_posts_excluded": 435,
            "distinct_partner_authors": 10,
            "matched_authors": 4,
            "matched_master_kols": 5,
            "unmatched_authors": 6,
        }
        metadata = attach.snapshot_metadata(result, partner_ids)
        chunks = attach.split_chunks(partner_ids)
        rows = []
        for index, chunk in enumerate(chunks, start=1):
            rows.append({
                "record_id": f"node{index}",
                "fields": attach.snapshot_node_fields(
                    index=index, total=len(chunks), post_ids=chunk, metadata=metadata,
                ),
            })

        self.assertEqual(["node1", "node2"], attach.verify_snapshot_rows(rows, partner_ids, metadata))
        rows[0]["fields"]["允许外部动作"] = True
        with self.assertRaises(RuntimeError):
            attach.verify_snapshot_rows(rows, partner_ids, metadata)

    def test_partial_snapshot_can_resume_only_when_existing_chunks_match(self):
        partner_ids = [f"post{index:03d}" for index in range(65)]
        result = {
            "official_posts_excluded": 435,
            "distinct_partner_authors": 10,
            "matched_authors": 4,
            "matched_master_kols": 5,
            "unmatched_authors": 6,
        }
        metadata = attach.snapshot_metadata(result, partner_ids)
        first_chunk = attach.snapshot_node_fields(
            index=1, total=2, post_ids=partner_ids[:60], metadata=metadata,
        )
        rows = [{"record_id": "node1", "fields": first_chunk}]

        self.assertEqual(
            {"evidence-v4-chunk-001"},
            attach.verify_snapshot_subset(rows, partner_ids, metadata),
        )
        rows[0]["fields"]["待确认竞品帖子"] = partner_ids[1:61]
        with self.assertRaises(RuntimeError):
            attach.verify_snapshot_subset(rows, partner_ids, metadata)


if __name__ == "__main__":
    unittest.main()

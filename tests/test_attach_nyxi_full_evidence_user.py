import unittest

from scripts import attach_nyxi_full_evidence_user as attach


class AttachNyxiFullEvidenceUserTests(unittest.TestCase):
    def test_write_payload_preserves_events_and_uses_raw_api_value_shapes(self):
        fields = {"关联竞品营销事件": [{"record_ids": ["evt1", "evt2"]}]}

        result = attach.activity_write_fields(fields, ["post1", "post2"], snapshot_ms=123456)

        self.assertEqual(["post1", "post2"], result["关联竞品帖子"])
        self.assertEqual(["evt1", "evt2"], result["关联竞品营销事件"])
        self.assertEqual(123456, result["证据快照时间"])

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
        expected = attach.activity_write_fields(
            {"关联竞品营销事件": [{"record_ids": ["evt1"]}]},
            ["post1"], snapshot_ms=123456,
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


if __name__ == "__main__":
    unittest.main()

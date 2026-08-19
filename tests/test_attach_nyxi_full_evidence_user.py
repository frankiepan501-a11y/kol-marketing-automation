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


if __name__ == "__main__":
    unittest.main()

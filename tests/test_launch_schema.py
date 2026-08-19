import unittest

from scripts import apply_launch_evidence_schema as schema


class LaunchSchemaTests(unittest.TestCase):
    def test_node_schema_covers_every_runtime_written_field(self):
        names = {field["field_name"] for field in schema.NODE_FIELDS}
        self.assertTrue({
            "节点状态", "节点阻塞说明", "竞品品牌", "目标证据配置版本",
            "待确认竞品帖子", "待确认竞品事件", "调查提交版本",
            "调查提交时间", "调查提交说明",
        }.issubset(names))

    def test_same_name_and_type_is_reused(self):
        desired = [schema.text("活动ID"), schema.number("版本")]
        diff = schema.diff_fields([
            {"field_name": "活动ID", "type": 1},
            {"field_name": "版本", "type": 2},
        ], desired)
        self.assertEqual([], diff["missing"])
        self.assertEqual(["活动ID", "版本"], diff["reused"])

    def test_same_name_different_type_stops_batch(self):
        diff = schema.diff_fields(
            [{"field_name": "版本", "type": 1}], [schema.number("版本")],
        )
        with self.assertRaises(schema.SchemaConflict):
            schema.validate_no_conflicts({"activity": diff})

    def test_second_dry_run_has_no_missing_fields(self):
        existing = [dict(item) for item in schema.NODE_FIELDS]
        first = schema.diff_fields([], schema.NODE_FIELDS)
        second = schema.diff_fields(existing, schema.NODE_FIELDS)
        self.assertEqual(len(schema.NODE_FIELDS), len(first["missing"]))
        self.assertEqual([], second["missing"])

    def test_participant_primary_field_is_first_and_text(self):
        self.assertEqual("参与记录ID", schema.PARTICIPANT_FIELDS[0]["field_name"])
        self.assertEqual(1, schema.PARTICIPANT_FIELDS[0]["type"])

    def test_activity_schema_has_campaign_specific_market_scope(self):
        by_name = {field["field_name"]: field for field in schema.ACTIVITY_FIELDS}
        self.assertEqual(4, by_name["活动目标国家"]["type"])
        self.assertEqual(4, by_name["活动目标语言"]["type"])

    def test_participant_schema_has_actionable_review_fields(self):
        names = {field["field_name"] for field in schema.PARTICIPANT_FIELDS}
        self.assertTrue({
            "达人主页", "主平台快照", "国家快照", "语言快照", "粉丝数快照",
            "内容与活跃摘要", "内容数据更新时间", "历史关系与触达摘要",
            "竞品证据摘要", "主证据帖子", "系统审核分流", "系统审核说明",
            "审核结论", "审核原因", "审核人", "审核时间",
            "活动分池",
        }.issubset(names))

    def test_participant_relation_cardinality_uses_base_v3_default(self):
        by_name = {field["field_name"]: field for field in schema.PARTICIPANT_FIELDS}
        for name in ("关联活动", "关联KOL", "关联媒体人"):
            self.assertNotIn("multiple", by_name[name]["property"])

    def test_existing_participant_table_rejects_wrong_primary(self):
        with self.assertRaises(schema.SchemaConflict):
            schema.validate_participant_primary([
                {"field_name": "记录名称", "type": 1, "is_primary": True},
                {"field_name": "参与记录ID", "type": 1, "is_primary": False},
            ])

    def test_relation_target_drift_is_a_conflict(self):
        desired = [schema.relation("关联活动", "expected")]
        current = [{"field_name": "关联活动", "type": 18,
                    "property": {"table_id": "wrong", "multiple": False}}]
        diff = schema.diff_fields(current, desired)
        self.assertEqual("关联目标表不一致", diff["conflicts"][0]["reason"])


if __name__ == "__main__":
    unittest.main()

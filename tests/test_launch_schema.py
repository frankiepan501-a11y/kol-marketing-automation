import unittest

from scripts import apply_launch_evidence_schema as schema


class LaunchSchemaTests(unittest.TestCase):
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

    def test_existing_participant_table_rejects_wrong_primary(self):
        with self.assertRaises(schema.SchemaConflict):
            schema.validate_participant_primary([
                {"field_name": "记录名称", "type": 1, "is_primary": True},
                {"field_name": "参与记录ID", "type": 1, "is_primary": False},
            ])

    def test_relation_target_drift_is_a_conflict(self):
        desired = [schema.relation("关联活动", "expected", multiple=False)]
        current = [{"field_name": "关联活动", "type": 18,
                    "property": {"table_id": "wrong", "multiple": False}}]
        diff = schema.diff_fields(current, desired)
        self.assertEqual("关联目标表或是否多选不一致", diff["conflicts"][0]["reason"])


if __name__ == "__main__":
    unittest.main()

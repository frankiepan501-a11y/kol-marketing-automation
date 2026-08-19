import unittest

from scripts.apply_launch_product_lock import (
    PIRANHA_CANONICAL,
    LOCK_RECORDS,
    validate_field_schema,
    validate_record_identity,
    validate_written_locks,
)


class LaunchProductLockMigrationTests(unittest.TestCase):
    def test_identity_preflight_accepts_expected_record(self):
        validate_record_identity(PIRANHA_CANONICAL, {
            "record_id": PIRANHA_CANONICAL,
            "fields": {"品牌": "POWKONG", "SKU": "PK02-S2", "产品名": "YM24食人花-二代"},
        })

    def test_identity_preflight_rejects_record_id_drift(self):
        with self.assertRaisesRegex(RuntimeError, "产品身份校验失败"):
            validate_record_identity(PIRANHA_CANONICAL, {
                "record_id": PIRANHA_CANONICAL,
                "fields": {"品牌": "POWKONG", "SKU": "OTHER", "产品名": "YM24食人花-二代"},
            })

    def test_schema_preflight_rejects_wrong_existing_type(self):
        fields = {
            "产品名": {"type": 1},
            "品牌": {"type": 3},
            "SKU": {"type": 1},
            "派单模式": {"type": 1},
        }
        with self.assertRaisesRegex(RuntimeError, "字段类型冲突"):
            validate_field_schema(fields)

    def test_written_lock_validation_rejects_partial_write(self):
        records = {}
        for record_id, expected in LOCK_RECORDS.items():
            identity = {
                PIRANHA_CANONICAL: ("POWKONG", "PK02-S2", "YM24食人花-二代"),
                "recvqD87uSM1Fh": ("POWKONG", "PK02-S3", "YM24-食人花2代"),
                "recvkJOoCsNb1s": ("FUNLAB", "FF05A-04", "戴夫联名 Switch 2 手柄"),
            }[record_id]
            records[record_id] = {"record_id": record_id, "fields": {
                "品牌": identity[0], "SKU": identity[1], "产品名": identity[2], **expected,
            }}
        records[PIRANHA_CANONICAL]["fields"]["活动主记录"] = False

        with self.assertRaisesRegex(RuntimeError, "写后校验失败"):
            validate_written_locks(records)


if __name__ == "__main__":
    unittest.main()

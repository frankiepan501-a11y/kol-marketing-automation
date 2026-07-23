import asyncio
import json
import os
import urllib.parse
import unittest


for key in [
    "FEISHU_NOTIFY_APP_ID",
    "FEISHU_NOTIFY_APP_SECRET",
    "FEISHU_APP3_ID",
    "FEISHU_APP3_SECRET",
    "FEISHU_APP_TOKEN",
    "T_KOL",
    "T_EDITOR",
    "T_DRAFT",
    "T_KOL_FU",
    "T_EDITOR_FU",
    "T_DASH",
    "T_PRODUCT",
    "T_TASK_KOL",
    "T_TASK_EDITOR",
    "SNOV_CLIENT_ID",
    "SNOV_CLIENT_SECRET",
    "INTERNAL_TOKEN",
]:
    os.environ.setdefault(key, "test")

from app import feishu  # noqa: E402


class FeishuFetchAllRecordsTests(unittest.TestCase):
    def test_fetch_all_records_projects_fields_and_uses_500_page_size(self):
        paths = []

        async def fake_api(method, path, body=None, which="bitable"):
            paths.append(path)
            if len(paths) == 1:
                return {
                    "data": {
                        "items": [{"record_id": "rec1", "fields": {}}],
                        "has_more": True,
                        "page_token": "recNext",
                    }
                }
            return {
                "data": {
                    "items": [{"record_id": "rec2", "fields": {}}],
                    "has_more": False,
                }
            }

        original_api = feishu.api
        feishu.api = fake_api
        try:
            rows = asyncio.run(
                feishu.fetch_all_records(
                    "tblTest",
                    field_names=["发送状态", "邮件草稿ID"],
                    page_size=999,
                )
            )
        finally:
            feishu.api = original_api

        self.assertEqual([r["record_id"] for r in rows], ["rec1", "rec2"])
        self.assertEqual(len(paths), 2)

        first_qs = urllib.parse.parse_qs(urllib.parse.urlparse(paths[0]).query)
        self.assertEqual(first_qs["page_size"], ["500"])
        self.assertEqual(json.loads(first_qs["field_names"][0]), ["发送状态", "邮件草稿ID"])
        self.assertNotIn("page_token", first_qs)

        second_qs = urllib.parse.parse_qs(urllib.parse.urlparse(paths[1]).query)
        self.assertEqual(second_qs["page_token"], ["recNext"])
        self.assertEqual(json.loads(second_qs["field_names"][0]), ["发送状态", "邮件草稿ID"])


if __name__ == "__main__":
    unittest.main()

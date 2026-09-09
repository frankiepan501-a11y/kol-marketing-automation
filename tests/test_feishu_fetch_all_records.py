import asyncio
import json
import os
import sys
import urllib.parse
import unittest
from unittest.mock import AsyncMock, patch


REPO_ROOT = os.environ.get("KOL_TEST_REPO_ROOT") or os.path.join(
    os.path.dirname(__file__), ".."
)
sys.path.insert(0, os.path.abspath(REPO_ROOT))

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
    def test_kol_recovery_does_not_leak_into_synchronous_or_other_task_reads(self):
        failure = feishu.FeishuAPIError(method="GET", path="/records",
            status_code=400, feishu_code=1254607, feishu_msg="Data not ready")

        async def exercise():
            entered = asyncio.Event()
            release = asyncio.Event()

            async def background_scope():
                with feishu.kol_background_read_recovery():
                    entered.set()
                    await release.wait()

            background = asyncio.create_task(background_scope())
            await entered.wait()
            try:
                with self.assertRaises(feishu.FeishuReadError):
                    await feishu.fetch_all_records("kol-test")
            finally:
                release.set()
                await background
            with self.assertRaises(feishu.FeishuReadError):
                await feishu.fetch_all_records("kol-test")

        with patch.object(feishu.config, "T_KOL", "kol-test"), \
             patch.object(feishu, "api", new=AsyncMock(side_effect=failure)) as api, \
             patch("asyncio.sleep", new=AsyncMock()) as sleep:
            asyncio.run(exercise())
        self.assertEqual(2, api.await_count)
        sleep.assert_not_awaited()

    def test_kol_recovery_is_bounded_and_never_returns_partial_rows(self):
        failure = feishu.FeishuAPIError(method="GET", path="/records",
            status_code=400, feishu_code=1254607, feishu_msg="Data not ready")
        first = {"data": {"items": [{"record_id": "rec1"}],
                          "has_more": True, "page_token": "next"}}
        with patch.object(feishu.config, "T_KOL", "kol-test"), \
             patch.object(feishu, "api", new=AsyncMock(side_effect=[first, failure, failure, failure])) as api, \
             patch("asyncio.sleep", new=AsyncMock()) as sleep:
            with feishu.kol_background_read_recovery(), self.assertRaises(feishu.FeishuReadError):
                asyncio.run(feishu.fetch_all_records("kol-test"))
        self.assertEqual(4, api.await_count)
        self.assertEqual([30, 60], [call.args[0] for call in sleep.await_args_list])

    def test_other_tables_do_not_receive_kol_recovery(self):
        failure = feishu.FeishuAPIError(method="GET", path="/records",
            status_code=400, feishu_code=1254607, feishu_msg="Data not ready")
        with patch.object(feishu, "api", new=AsyncMock(side_effect=failure)) as api, \
             patch("asyncio.sleep", new=AsyncMock()) as sleep:
            with feishu.kol_background_read_recovery(), self.assertRaises(feishu.FeishuReadError):
                asyncio.run(feishu.fetch_all_records("unrelated-table"))
        self.assertEqual(1, api.await_count)
        sleep.assert_not_awaited()

    def test_kol_transient_later_page_recovers_without_losing_first_page(self):
        calls = []

        async def fake_api(method, path, **kwargs):
            calls.append(path)
            if len(calls) == 1:
                return {"code": 0, "data": {"items": [{"record_id": "rec1"}],
                        "has_more": True, "page_token": "next"}}
            if len(calls) == 2:
                raise feishu.FeishuAPIError(method=method, path=path,
                    status_code=400, feishu_code=1254607, feishu_msg="Data not ready")
            return {"code": 0, "data": {"items": [{"record_id": "rec2"}], "has_more": False}}

        with patch.object(feishu.config, "T_KOL", "kol-test"), \
             patch.object(feishu, "api", new=fake_api), \
             patch("asyncio.sleep", new=AsyncMock()):
            with feishu.kol_background_read_recovery():
                rows = asyncio.run(feishu.fetch_all_records("kol-test", page_size=500))
        self.assertEqual(["rec1", "rec2"], [row["record_id"] for row in rows])
        self.assertEqual(calls[1], calls[2])

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

    def test_fetch_all_records_can_request_persistent_automatic_timestamps(self):
        paths = []

        async def fake_api(method, path, body=None, which="bitable"):
            paths.append(path)
            return {"data": {"items": [], "has_more": False}}

        original_api = feishu.api
        feishu.api = fake_api
        try:
            asyncio.run(feishu.fetch_all_records(
                "tblTest", automatic_fields=True,
            ))
        finally:
            feishu.api = original_api

        query = urllib.parse.parse_qs(urllib.parse.urlparse(paths[0]).query)
        self.assertEqual(["true"], query["automatic_fields"])

    def test_fetch_all_records_restarts_with_smaller_page_after_http_400(self):
        calls = []

        async def fake_api(method, path, body=None, which="bitable"):
            query = urllib.parse.parse_qs(urllib.parse.urlparse(path).query)
            calls.append((query["page_size"][0], query.get("page_token", [""])[0]))
            page_size = int(query["page_size"][0])
            page_token = query.get("page_token", [""])[0]
            if page_size == 500 and page_token:
                raise feishu.FeishuAPIError(
                    method="GET",
                    path=path,
                    status_code=400,
                    feishu_code=1254000,
                    feishu_msg="page_size exceeds limit",
                )
            if not page_token:
                return {
                    "data": {
                        "items": [{"record_id": f"rec-{page_size}-1", "fields": {}}],
                        "has_more": True,
                        "page_token": "next/+==",
                    }
                }
            return {
                "data": {
                    "items": [{"record_id": f"rec-{page_size}-2", "fields": {}}],
                    "has_more": False,
                }
            }

        original_api = feishu.api
        feishu.api = fake_api
        try:
            rows = asyncio.run(feishu.fetch_all_records("tblTest", page_size=500))
        finally:
            feishu.api = original_api

        self.assertEqual(
            [r["record_id"] for r in rows],
            ["rec-200-1", "rec-200-2"],
        )
        self.assertEqual(
            calls,
            [("500", ""), ("500", "next/+=="), ("200", ""), ("200", "next/+==")],
        )

    def test_fetch_all_records_fails_loudly_if_has_more_has_no_page_token(self):
        async def fake_api(method, path, body=None, which="bitable"):
            return {
                "data": {
                    "items": [{"record_id": "rec1", "fields": {}}],
                    "has_more": True,
                }
            }

        original_api = feishu.api
        feishu.api = fake_api
        try:
            with self.assertRaisesRegex(
                feishu.FeishuPaginationError,
                r"table=tblTest.*page=1.*page_size=500.*page_token=<missing>",
            ):
                asyncio.run(feishu.fetch_all_records("tblTest", page_size=500))
        finally:
            feishu.api = original_api

    def test_fetch_all_records_reports_final_http_400_context(self):
        page_sizes = []

        async def fake_api(method, path, body=None, which="bitable"):
            query = urllib.parse.parse_qs(urllib.parse.urlparse(path).query)
            page_sizes.append(int(query["page_size"][0]))
            raise feishu.FeishuAPIError(
                method="GET",
                path=path,
                status_code=400,
                feishu_code=1254000,
                feishu_msg="page_size exceeds limit",
            )

        original_api = feishu.api
        feishu.api = fake_api
        try:
            with self.assertRaisesRegex(
                feishu.FeishuReadError,
                r"table=tblTest.*page=1.*page_size=50.*status=400.*code=1254000",
            ):
                asyncio.run(feishu.fetch_all_records("tblTest", page_size=500))
        finally:
            feishu.api = original_api

        self.assertEqual(page_sizes, [500, 200, 100, 50])

    def test_fetch_all_records_does_not_hide_unrelated_http_400(self):
        page_sizes = []

        async def fake_api(method, path, body=None, which="bitable"):
            query = urllib.parse.parse_qs(urllib.parse.urlparse(path).query)
            page_sizes.append(int(query["page_size"][0]))
            raise feishu.FeishuAPIError(
                method="GET",
                path=path,
                status_code=400,
                feishu_code=1254000,
                feishu_msg="invalid field_names",
            )

        original_api = feishu.api
        feishu.api = fake_api
        try:
            with self.assertRaisesRegex(
                feishu.FeishuReadError,
                r"table=tblTest.*page_size=500.*msg=invalid field_names",
            ):
                asyncio.run(feishu.fetch_all_records("tblTest", page_size=500))
        finally:
            feishu.api = original_api

        self.assertEqual(page_sizes, [500])

    def test_fetch_all_records_fails_loudly_on_repeated_page_token(self):
        async def fake_api(method, path, body=None, which="bitable"):
            return {
                "data": {
                    "items": [],
                    "has_more": True,
                    "page_token": "same-token",
                }
            }

        original_api = feishu.api
        feishu.api = fake_api
        try:
            with self.assertRaisesRegex(
                feishu.FeishuPaginationError,
                r"repeated a token.*page=2.*page_token='same-token'",
            ):
                asyncio.run(feishu.fetch_all_records("tblTest", page_size=500))
        finally:
            feishu.api = original_api

    def test_fetch_all_records_fails_loudly_on_duplicate_record(self):
        async def fake_api(method, path, body=None, which="bitable"):
            query = urllib.parse.parse_qs(urllib.parse.urlparse(path).query)
            if "page_token" not in query:
                return {
                    "data": {
                        "items": [{"record_id": "rec1", "fields": {}}],
                        "has_more": True,
                        "page_token": "next",
                    }
                }
            return {
                "data": {
                    "items": [{"record_id": "rec1", "fields": {}}],
                    "has_more": False,
                }
            }

        original_api = feishu.api
        feishu.api = fake_api
        try:
            with self.assertRaisesRegex(
                feishu.FeishuPaginationError,
                r"repeated a record.*page=2.*record_id='rec1'",
            ):
                asyncio.run(feishu.fetch_all_records("tblTest", page_size=500))
        finally:
            feishu.api = original_api


if __name__ == "__main__":
    unittest.main()

import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from app import relabel


DAY_MS = 86_400_000
NOW_MS = 1_777_000_000_000


class RelabelProfileTests(unittest.TestCase):
    def test_recent_video_page_returns_titles_and_publish_dates(self):
        html = r'''
        {"lockupMetadataViewModel":{"title":{"content":"Switch 2 Dock Review"},
        "publishedTimeText":{"simpleText":"2 days ago"}}}
        {"lockupMetadataViewModel":{"title":{"content":"Mario Kart Setup"},
        "metadata":{"content":"3 weeks ago"}}}
        {"lockupMetadataViewModel":{"title":{"content":"Old PC Build"},
        "publishedTimeText":{"simpleText":"5 months ago"}}}
        '''

        videos = relabel.parse_recent_video_page(html, now_ms=NOW_MS, limit=10)

        self.assertEqual(
            ["Switch 2 Dock Review", "Mario Kart Setup", "Old PC Build"],
            [video["title"] for video in videos],
        )
        self.assertEqual(NOW_MS - 2 * DAY_MS, videos[0]["published_at"])
        self.assertEqual(NOW_MS - 21 * DAY_MS, videos[1]["published_at"])
        self.assertEqual(NOW_MS - 150 * DAY_MS, videos[2]["published_at"])

    def test_successful_profile_plan_fills_all_launch_screening_fields(self):
        fields = {
            "合作状态": "未建联",
            "内容风格": ["游戏", "科技测评", "UNBOX"],
            "IP喜好": "Nintendo, Switch 2, Mario",
        }
        videos = [
            {"title": "Best Nintendo Switch 2 dock review", "published_at": NOW_MS - 2 * DAY_MS},
            {"title": "Mario Kart controller setup", "published_at": NOW_MS - 20 * DAY_MS},
            {"title": "Old Steam Deck video", "published_at": NOW_MS - 120 * DAY_MS},
        ]
        classification = {
            "type": "KOL",
            "styles": ["游戏", "科技测评", "UNBOX"],
            "ip_tags": ["Nintendo", "Switch 2", "Mario"],
            "content_vertical": "游戏硬件评测",
            "ecosystems": ["Switch", "Switch 2"],
        }

        update = relabel.plan_profile_update(
            fields, videos, classification, now_ms=NOW_MS,
        )

        self.assertEqual("v2", update["标签版本"])
        self.assertEqual("游戏硬件评测", update["内容垂类"])
        self.assertEqual(["Switch", "Switch 2"], update["主机生态"])
        self.assertEqual(NOW_MS - 2 * DAY_MS, update["最近发布日"])
        self.assertEqual(2, update["近90天发布数"])
        self.assertEqual("有效", update["资料可用状态"])
        self.assertEqual(NOW_MS, update["资料核实时间"])
        self.assertEqual("可新开发", update["触达路由状态"])

    def test_manual_verification_is_preserved_while_machine_fields_refresh(self):
        manual_at = NOW_MS - 10 * DAY_MS
        fields = {
            "合作状态": "未建联",
            "资料可用状态": "人工核实有效",
            "资料核实时间": manual_at,
            "内容风格": ["游戏"],
            "IP喜好": "Nintendo Switch",
        }
        videos = [
            {"title": "Nintendo Switch game", "published_at": NOW_MS - DAY_MS},
            {"title": "Mario news", "published_at": NOW_MS - 7 * DAY_MS},
            {"title": "Zelda review", "published_at": NOW_MS - 14 * DAY_MS},
        ]

        update = relabel.plan_profile_update(
            fields,
            videos,
            {"type": "KOL", "styles": ["游戏"], "ip_tags": ["Nintendo", "Switch"]},
            now_ms=NOW_MS,
        )

        self.assertEqual("人工核实有效", update["资料可用状态"])
        self.assertEqual(manual_at, update["资料核实时间"])

    def test_deterministic_fallback_builds_nintendo_profile_without_model(self):
        result = relabel.deterministic_profile_classification(
            name="MarioBricks", description="",
            recent_titles=[
                "Mario Kart World - Full Game",
                "Splatoon Raiders - Final Boss",
                "DK Bananza DLC Event",
            ],
        )

        self.assertEqual("KOL", result["type"])
        self.assertEqual("deterministic_fallback", result["classification_source"])
        self.assertEqual("主机游戏", result["content_vertical"])
        self.assertIn("Switch", result["ecosystems"])
        self.assertIn("Mario", result["ip_tags"])

    def test_deterministic_fallback_blocks_exact_official_or_media_identity(self):
        publisher = relabel.deterministic_profile_classification(
            name="Nintendo", description="", recent_titles=["Nintendo Direct"],
        )
        media = relabel.deterministic_profile_classification(
            name="IGN", description="", recent_titles=["Switch 2 Review"],
        )

        self.assertEqual("游戏厂商", publisher["type"])
        self.assertEqual("媒体", media["type"])

    def test_touch_route_is_relationship_based_and_deterministic(self):
        self.assertEqual("可新开发", relabel.touch_route_for_status("未建联"))
        self.assertEqual("沿用原线程", relabel.touch_route_for_status("洽谈中"))
        self.assertEqual("禁止新开发", relabel.touch_route_for_status("黑名单"))
        self.assertEqual("待核对", relabel.touch_route_for_status(""))

    def test_profile_write_failure_is_visible_and_not_counted(self):
        record = {"record_id": "kol1", "fields": {
            "账号名": "No Link", "合作状态": "未建联", "主链接": None,
        }}

        with patch.object(
            relabel.feishu, "get_record", new=AsyncMock(return_value=record),
        ), patch.object(
            relabel.feishu, "update_record", new=AsyncMock(side_effect=RuntimeError("boom")),
        ):
            result = asyncio.run(relabel.run_profile_records(["kol1"], dry_run=False))

        self.assertEqual(0, result["writes"])
        self.assertEqual(1, result["by_status"]["write_fail"])
        self.assertEqual("no_channel_id", result["results"][0]["intended_status"])

    def test_one_processing_error_does_not_abort_other_records(self):
        good = {"record_id": "good", "fields": {
            "账号名": "No Link", "合作状态": "未建联", "主链接": None,
        }}

        async def get_record(_table, record_id):
            if record_id == "bad":
                raise RuntimeError("missing")
            return good

        with patch.object(relabel.feishu, "get_record", side_effect=get_record):
            result = asyncio.run(relabel.run_profile_records(
                ["bad", "good"], dry_run=True,
            ))

        self.assertEqual(2, result["processed"])
        self.assertEqual(1, result["by_status"]["processing_error"])
        self.assertEqual(1, result["by_status"]["no_channel_id"])


if __name__ == "__main__":
    unittest.main()

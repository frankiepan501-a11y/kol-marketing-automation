import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from app import relabel
from app.enrich_model_guard import EnrichModelBudget


DAY_MS = 86_400_000
NOW_MS = 1_777_000_000_000


class RelabelProfileTests(unittest.TestCase):
    def test_x_profile_parser_uses_explicit_location_post_language_and_public_email(self):
        result = relabel.parse_x_public_profile({
            "id": "42", "name": "Reviewer", "username": "reviewer",
            "description": "Switch hardware reviews. Business: hello@example.com",
            "location": "Berlin, Germany", "url": "https://example.com",
            "public_metrics": {"followers_count": 12345},
        }, [
            {"id": "1", "text": "Controller review", "lang": "de"},
            {"id": "2", "text": "Switch setup", "lang": "de"},
            {"id": "3", "text": "Gaming news", "lang": "en"},
        ])

        self.assertTrue(result["retrieved"])
        self.assertEqual("DE", result["country"])
        self.assertEqual("de", result["language"])
        self.assertEqual("hello@example.com", result["email"])
        self.assertEqual(12345, result["followers"])

    def test_x_profile_parser_keeps_ambiguous_location_and_tied_language_unknown(self):
        result = relabel.parse_x_public_profile({
            "id": "43", "name": "Reviewer", "username": "reviewer",
            "description": "Gaming every day", "location": "Bay Area",
        }, [
            {"id": "1", "text": "One", "lang": "en"},
            {"id": "2", "text": "Zwei", "lang": "de"},
        ])

        self.assertEqual("", result["country"])
        self.assertEqual("", result["language"])
        self.assertEqual("", result["email"])

    def test_x_profile_parser_does_not_ignore_non_target_language_majority(self):
        result = relabel.parse_x_public_profile({
            "id": "44", "username": "reviewer", "location": "United States",
        }, [
            *({"id": str(index), "text": "投稿", "lang": "ja"} for index in range(9)),
            {"id": "10", "text": "Controller", "lang": "en"},
        ])

        self.assertEqual("", result["language"])

    def test_youtube_about_parser_reads_only_public_country_and_email(self):
        html = r'''<script>{"aboutChannelRenderer":{"metadata":{
        "aboutChannelViewModel":{"description":"For business: creator@example.com",
        "country":"加拿大","canonicalChannelUrl":"https://www.youtube.com/@Creator",
        "channelId":"UC123","subscriberCountText":"19.3K subscribers"}}}}</script>'''

        result = relabel.parse_youtube_about_page(html)

        self.assertTrue(result["retrieved"])
        self.assertEqual("CA", result["country"])
        self.assertEqual("creator@example.com", result["email"])
        self.assertEqual("UC123", result["channel_id"])
        self.assertEqual(19300, result["followers"])

    def test_public_count_parser_supports_plain_and_abbreviated_counts(self):
        self.assertEqual(19300, relabel._parse_public_count("19.3K subscribers"))
        self.assertEqual(1_200_000, relabel._parse_public_count("1.2M subscribers"))
        self.assertEqual(12345, relabel._parse_public_count("12,345 subscribers"))
        self.assertEqual(0, relabel._parse_public_count("Subscribers hidden"))

    def test_youtube_about_parser_keeps_unknown_country_and_missing_email_blocked(self):
        html = r'''{"aboutChannelViewModel":{"description":"Gaming videos weekly",
        "country":"Unknownland","channelId":"UC999"}}'''

        result = relabel.parse_youtube_about_page(html)

        self.assertTrue(result["retrieved"])
        self.assertEqual("", result["country"])
        self.assertEqual("", result["email"])

    def test_youtube_about_parser_chooses_populated_model_when_page_contains_empty_duplicate(self):
        html = r'''{"aboutChannelViewModel":{}}
        {"aboutChannelViewModel":{"description":"Business: real@example.com",
        "country":"United States","channelId":"UC-real"}}'''

        result = relabel.parse_youtube_about_page(html)

        self.assertEqual("US", result["country"])
        self.assertEqual("real@example.com", result["email"])
        self.assertEqual("UC-real", result["channel_id"])

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

    def test_controlled_import_route_stays_pending_during_profile_refresh(self):
        fields = {
            "合作状态": "未建联", "触达路由状态": "待核对",
            "迁移备注": "[CONTROLLED_IMPORT] campaign=c1",
        }
        videos = [{"title": "Switch controller review", "published_at": NOW_MS - DAY_MS}]

        update = relabel.plan_profile_update(
            fields, videos,
            {"type": "KOL", "styles": ["游戏"], "ip_tags": ["Switch"]},
            now_ms=NOW_MS,
        )

        self.assertEqual("待核对", update["触达路由状态"])

    def test_controlled_import_route_stays_pending_when_channel_id_is_missing(self):
        record = {"record_id": "kol1", "fields": {
            "账号名": "Controlled", "合作状态": "未建联", "主链接": None,
            "触达路由状态": "待核对", "迁移备注": "[CONTROLLED_IMPORT] campaign=c1",
        }}

        result = asyncio.run(relabel.relabel_one_kol(record, dry_run=True, now_ms=NOW_MS))

        self.assertEqual("no_channel_id", result["intended_status"])
        self.assertEqual("待核对", result["planned_fields"]["触达路由状态"])

    def test_controlled_import_route_stays_pending_when_scrape_fails(self):
        record = {"record_id": "kol1", "fields": {
            "账号名": "Controlled", "合作状态": "未建联",
            "主链接": {"link": "https://youtube.com/channel/UC123"},
            "触达路由状态": "待核对", "迁移备注": "[CONTROLLED_IMPORT] campaign=c1",
        }}
        with patch.object(
            relabel, "fetch_recent_videos", new=AsyncMock(return_value=[]),
        ):
            result = asyncio.run(relabel.relabel_one_kol(
                record, dry_run=True, now_ms=NOW_MS,
            ))

        self.assertEqual("scrape_fail", result["intended_status"])
        self.assertEqual("待核对", result["planned_fields"]["触达路由状态"])

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
        self.assertNotIn("硬件改装", result["styles"])

    def test_deterministic_fallback_does_not_label_toy_channel_from_two_game_hits(self):
        result = relabel.deterministic_profile_classification(
            name="EL GUERRERO DEL JUGUETE", description="",
            recent_titles=[
                "Unique Ghostbusters molds and official Miniland gem",
                "Airgam Monsters figures that terrified a generation",
                "Factory-sealed Nintendo box with six unused games",
                "The rarest 70s comics",
                "The rarest My Pet Monster in the world",
                "Toy hunter travels the world",
                "Toy treasure hunters in Spain",
                "Banned figures from the 90s",
                "Collecting stories",
                "Rare collectible video games",
            ],
        )

        self.assertEqual("其他", result["content_vertical"])
        self.assertEqual(["未知"], result["ecosystems"])
        self.assertEqual([], result["ip_tags"])
        self.assertTrue(result["clear_profile_tags"])
        self.assertIn("2/10", result["reason"])

    def test_low_support_machine_profile_clears_stale_machine_tags(self):
        fields = {
            "合作状态": "未建联",
            "内容风格": ["游戏"],
            "IP喜好": "Nintendo, Switch",
        }
        videos = [
            {"title": "Rare toy collection", "published_at": NOW_MS - DAY_MS},
            {"title": "Vintage comics", "published_at": NOW_MS - 7 * DAY_MS},
            {"title": "Nintendo sealed box", "published_at": NOW_MS - 14 * DAY_MS},
        ]
        classification = relabel.deterministic_profile_classification(
            name="Toy Channel", description="",
            recent_titles=[video["title"] for video in videos],
        )

        update = relabel.plan_profile_update(
            fields, videos, classification, now_ms=NOW_MS,
        )

        self.assertEqual([], update["内容风格"])
        self.assertEqual("", update["IP喜好"])
        self.assertEqual("其他", update["内容垂类"])
        self.assertEqual(["未知"], update["主机生态"])

    def test_low_support_machine_profile_preserves_fresh_manual_tags(self):
        manual_at = NOW_MS - 10 * DAY_MS
        fields = {
            "合作状态": "未建联",
            "资料可用状态": "人工核实有效",
            "资料核实时间": manual_at,
            "内容风格": ["游戏"],
            "IP喜好": "Nintendo",
        }
        videos = [
            {"title": "Rare toy collection", "published_at": NOW_MS - DAY_MS},
            {"title": "Vintage comics", "published_at": NOW_MS - 7 * DAY_MS},
            {"title": "Nintendo sealed box", "published_at": NOW_MS - 14 * DAY_MS},
        ]
        classification = relabel.deterministic_profile_classification(
            name="Manually Checked Channel", description="",
            recent_titles=[video["title"] for video in videos],
        )

        update = relabel.plan_profile_update(
            fields, videos, classification, now_ms=NOW_MS,
        )

        self.assertNotIn("内容风格", update)
        self.assertNotIn("IP喜好", update)
        self.assertEqual("人工核实有效", update["资料可用状态"])

    def test_deterministic_fallback_blocks_exact_official_or_media_identity(self):
        publisher = relabel.deterministic_profile_classification(
            name="Nintendo", description="", recent_titles=["Nintendo Direct"],
        )
        media = relabel.deterministic_profile_classification(
            name="IGN", description="", recent_titles=["Switch 2 Review"],
        )

        self.assertEqual("游戏厂商", publisher["type"])
        self.assertEqual("媒体", media["type"])

    def test_empty_model_profile_counts_as_failure_and_uses_deterministic_fallback(self):
        with tempfile.TemporaryDirectory() as tmp:
            budget = EnrichModelBudget(
                per_task=2, per_run=2, daily=10, failure_threshold=2,
                state_path=Path(tmp) / "budget.json",
            )
            with patch.object(
                relabel.deepseek, "chat_json", new=AsyncMock(return_value={}),
            ):
                result = asyncio.run(relabel.classify_v2(
                    "Switch Reviewer", "reviewer", "Nintendo hardware",
                    12000, ["Switch 2 Dock Review"], model_budget=budget,
                ))

        self.assertEqual("deterministic_fallback", result["classification_source"])
        self.assertEqual("model_error", result["model_fallback_reason"])
        self.assertEqual(1, budget.consecutive_failures)

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

    def test_deterministic_profile_mode_never_calls_deepseek_or_writes(self):
        record = {"record_id": "kol1", "fields": {
            "账号名": "Switch Reviewer", "合作状态": "未建联",
            "主链接": {"link": "https://youtube.com/channel/UC123"},
            "YouTube频道ID": "UC123", "粉丝数": 12000,
        }}
        videos = [
            {"title": "Nintendo Switch 2 Dock Review", "published_at": NOW_MS - DAY_MS},
            {"title": "Mario Kart Controller Setup", "published_at": NOW_MS - 2 * DAY_MS},
            {"title": "Zelda Hardware Comparison", "published_at": NOW_MS - 3 * DAY_MS},
        ]

        with patch.object(
            relabel.feishu, "get_record", new=AsyncMock(return_value=record),
        ), patch.object(
            relabel, "fetch_recent_videos", new=AsyncMock(return_value=videos),
        ), patch.object(
            relabel.deepseek, "chat_json", new=AsyncMock(),
        ) as model, patch.object(
            relabel.feishu, "update_record", new=AsyncMock(),
        ) as write:
            result = asyncio.run(relabel.run_profile_records(
                ["kol1"], dry_run=True, classification_mode="deterministic",
            ))

        self.assertEqual("deterministic", result["classification_mode"])
        self.assertEqual(0, result["model_calls"])
        self.assertEqual(0, result["writes"])
        self.assertEqual("deterministic_fallback", result["results"][0]["classification_source"])
        model.assert_not_awaited()
        write.assert_not_awaited()

    def test_launch_profile_refresh_caps_model_calls_and_falls_back_deterministically(self):
        records = {
            record_id: {"record_id": record_id, "fields": {
                "账号名": f"Reviewer {record_id}", "合作状态": "未建联",
                "主链接": {"link": f"https://youtube.com/channel/UC{index:022d}"},
                "粉丝数": 12000,
            }}
            for index, record_id in enumerate(("kol1", "kol2", "kol3"), start=1)
        }
        videos = [
            {"title": "Nintendo Switch 2 Dock Review", "published_at": NOW_MS - DAY_MS},
            {"title": "Mario Kart Controller Setup", "published_at": NOW_MS - 2 * DAY_MS},
            {"title": "Zelda Hardware Comparison", "published_at": NOW_MS - 3 * DAY_MS},
        ]
        model_result = {
            "type": "KOL", "confidence": 0.9, "styles": ["游戏"],
            "ip_tags": ["Switch 2"], "country_guess": "US",
            "content_vertical": "游戏硬件评测", "ecosystems": ["Switch 2"],
            "reason": "grounded",
        }

        with tempfile.TemporaryDirectory() as tmp:
            budget = EnrichModelBudget(
                per_task=1, per_run=1, daily=10, failure_threshold=2,
                state_path=Path(tmp) / "launch-budget.json",
            )
            with patch.object(
                relabel.feishu, "get_record",
                new=AsyncMock(side_effect=lambda _table, record_id: records[record_id]),
            ), patch.object(
                relabel, "fetch_recent_videos", new=AsyncMock(return_value=videos),
            ), patch.object(
                relabel.deepseek, "chat_json", new=AsyncMock(return_value=model_result),
            ) as model:
                result = asyncio.run(relabel.run_profile_records(
                    ["kol1", "kol2", "kol3"], dry_run=True,
                    classification_mode="deepseek", model_budget=budget,
                ))

        self.assertEqual(1, result["model_calls"])
        self.assertEqual(1, model.await_count)
        self.assertEqual(2, sum(
            item.get("classification_source") == "deterministic_fallback"
            for item in result["results"]
        ))
        self.assertEqual(1, result["model_budget"]["run_calls"])


if __name__ == "__main__":
    unittest.main()

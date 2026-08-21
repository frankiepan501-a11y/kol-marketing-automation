import asyncio
import unittest
from collections import Counter
from unittest.mock import AsyncMock, patch

from app import keyword_supply


class KeywordSupplyBrazilTests(unittest.TestCase):
    def test_piranha_uses_curated_fallback_when_dynamic_generation_is_unavailable(self):
        used = [
            {"fields": {
                "任务名": "[活动补池:campaign1] YT KOL - " + word,
                "关键词列表": word, "爬虫类型": "KOL-YouTube",
                "任务状态": "3-已完成", "筛选-语言": ["en"],
            }}
            for word in keyword_supply._CAMPAIGN_KEYWORDS["piranha"]["en"]
        ]
        activity = {"fields": {"活动目标语言": ["en"]}}
        product = {"fields": {"产品英文名": "POWKONG Piranha Plant 2 Dock"}}

        with patch.object(
            keyword_supply.feishu, "fetch_all_records", new=AsyncMock(return_value=used),
        ), patch.object(
            keyword_supply.deepseek, "chat_json",
            new=AsyncMock(side_effect=RuntimeError("402 Payment Required")),
        ), patch.object(
            keyword_supply.feishu, "create_record", new=AsyncMock(return_value="task1"),
        ) as create_record:
            result = asyncio.run(keyword_supply.ensure_campaign_supply(
                campaign_id="campaign1", activity=activity, product=product,
                required_candidates=250, dry_run=False,
            ))

        self.assertTrue(result["ok"])
        self.assertEqual(5, result["created"])
        self.assertEqual("curated_fallback", result["keyword_source"])
        self.assertIn("402 Payment Required", result["generation_warning"])
        self.assertEqual("", result["generation_error"])
        self.assertEqual(5, create_record.await_count)

    def test_campaign_supply_generates_more_targeted_keywords_after_seed_list_is_exhausted(self):
        used = [
            {"fields": {
                "任务名": "[活动补池:campaign1] YT KOL - " + word,
                "关键词列表": word, "爬虫类型": "KOL-YouTube",
                "任务状态": "3-已完成", "筛选-语言": ["en"],
            }}
            for word in keyword_supply._CAMPAIGN_KEYWORDS["piranha"]["en"]
        ]
        activity = {"fields": {"活动目标语言": ["en"]}}
        product = {"fields": {"产品英文名": "POWKONG Piranha Plant 2 Dock"}}

        with patch.object(
            keyword_supply.feishu, "fetch_all_records", new=AsyncMock(return_value=used),
        ), patch.object(
            keyword_supply.deepseek, "chat_json", new=AsyncMock(return_value={
                "keywords": [
                    "mario switch collection showcase", "nintendo desk setup review",
                    "super mario collector room tour", "switch accessories setup channel",
                ],
            }),
        ) as generate, patch.object(
            keyword_supply.feishu, "create_record", new=AsyncMock(return_value="task1"),
        ) as create_record:
            result = asyncio.run(keyword_supply.ensure_campaign_supply(
                campaign_id="campaign1", activity=activity, product=product,
                required_candidates=200, dry_run=False,
            ))

        self.assertTrue(result["ok"])
        self.assertEqual(4, result["created"])
        self.assertEqual("dynamic", result["keyword_source"])
        generate.assert_awaited_once()
        self.assertEqual(4, create_record.await_count)

    def test_campaign_supply_is_deterministic_idempotent_and_target_language_only(self):
        rows = [{"fields": {
            "任务名": "[活动补池:campaign1] YT KOL - dave the diver gameplay",
            "关键词列表": "dave the diver gameplay", "爬虫类型": "KOL-YouTube",
            "任务状态": "1-待触发", "筛选-语言": ["en"],
        }}]
        activity = {"fields": {"活动目标语言": ["en", "de", "es"]}}
        product = {"fields": {"产品英文名": "FUNLAB Dave the Diver Controller"}}

        with patch.object(
            keyword_supply.feishu, "fetch_all_records", new=AsyncMock(return_value=rows),
        ), patch.object(
            keyword_supply.feishu, "create_record", new=AsyncMock(return_value="task1"),
        ) as create_record:
            result = asyncio.run(keyword_supply.ensure_campaign_supply(
                campaign_id="campaign1", activity=activity, product=product,
                required_candidates=150, dry_run=False,
            ))

        self.assertTrue(result["ok"])
        self.assertGreater(result["created"], 0)
        created_fields = [call.args[1] for call in create_record.await_args_list]
        self.assertTrue(all("[活动补池:campaign1]" in f["任务名"] for f in created_fields))
        self.assertTrue(all(f["关键词列表"] != "dave the diver gameplay" for f in created_fields))
        self.assertTrue(all(f["筛选-语言"][0] in {"en", "de", "es"} for f in created_fields))

    def test_dave_does_not_use_piranha_dynamic_keyword_generation(self):
        used = [
            {"fields": {
                "任务名": "[活动补池:campaign1] YT KOL - " + word,
                "关键词列表": word, "爬虫类型": "KOL-YouTube",
                "任务状态": "3-已完成", "筛选-语言": ["en"],
            }}
            for word in keyword_supply._CAMPAIGN_KEYWORDS["dave"]["en"]
        ]
        activity = {"fields": {"活动目标语言": ["en"]}}
        product = {"fields": {"产品英文名": "FUNLAB Dave the Diver Controller"}}

        with patch.object(
            keyword_supply.feishu, "fetch_all_records", new=AsyncMock(return_value=used),
        ), patch.object(
            keyword_supply.deepseek, "chat_json", new=AsyncMock(),
        ) as generate, patch.object(
            keyword_supply.feishu, "create_record", new=AsyncMock(),
        ):
            result = asyncio.run(keyword_supply.ensure_campaign_supply(
                campaign_id="campaign1", activity=activity, product=product,
                required_candidates=200, dry_run=False,
            ))

        generate.assert_not_awaited()
        self.assertEqual("none", result["keyword_source"])
        self.assertGreater(result["shortfall_tasks"], 0)

    def test_stale_pending_campaign_tasks_do_not_count_as_active_supply(self):
        old_ms = int(keyword_supply.time.time() * 1000) - keyword_supply.DISCOVERY_ACTIVE_TTL_MS - 1
        rows = [{"fields": {
            "任务名": "[活动补池:campaign1] YT KOL - old task",
            "关键词列表": "old task", "任务状态": "1-待触发",
            "创建日期": old_ms,
        }}]
        activity = {"fields": {"活动目标语言": ["en"]}}
        product = {"fields": {"产品英文名": "POWKONG Piranha Plant 2 Dock"}}

        with patch.object(
            keyword_supply.feishu, "fetch_all_records", new=AsyncMock(return_value=rows),
        ), patch.object(
            keyword_supply.deepseek, "chat_json",
            new=AsyncMock(return_value={"keywords": [
                "mario switch collection showcase",
                "nintendo desk setup review",
                "super mario collector room tour",
            ]}),
        ), patch.object(
            keyword_supply.feishu, "create_record", new=AsyncMock(return_value="task1"),
        ):
            result = asyncio.run(keyword_supply.ensure_campaign_supply(
                campaign_id="campaign1", activity=activity, product=product,
                required_candidates=120, dry_run=False,
            ))

        self.assertEqual(0, result["active_pending_before"])
        self.assertEqual(1, result["stale_pending_before"])
        self.assertGreater(result["created"], 0)

    def test_market_configuration_includes_portuguese_brazil(self):
        portuguese = [m for m in keyword_supply.MARKETS if m["lang"] == "pt"]

        self.assertEqual(1, len(portuguese))
        self.assertIn("BR", portuguese[0]["countries"])

    def test_brasil_suffix_does_not_make_english_phrase_portuguese(self):
        market = next(m for m in keyword_supply.MARKETS if m["lang"] == "pt")

        self.assertFalse(keyword_supply._is_localized("cozy gaming room brasil", market))
        self.assertTrue(keyword_supply._is_localized("quarto gamer zelda brasil", market))

    def test_portuguese_dry_run_generates_without_writing(self):
        portuguese_market = [m for m in keyword_supply.MARKETS if m["lang"] == "pt"]
        self.assertEqual(1, len(portuguese_market))

        with patch.object(keyword_supply, "MARKETS", portuguese_market), \
             patch.object(keyword_supply, "_load", new=AsyncMock(return_value=(set(), Counter({"pt": 5})))), \
             patch.object(
                 keyword_supply.deepseek,
                 "chat_json",
                 new=AsyncMock(side_effect=[
                     {"keywords": ["zelda cozy gamer desk makeover"]},
                     {"keywords": ["quarto gamer zelda brasil"]},
                 ]),
             ) as chat_json, \
             patch.object(keyword_supply.feishu, "create_record", new=AsyncMock()) as create_record:
            result = asyncio.run(keyword_supply.run(dry_run=True))

        self.assertEqual(["quarto gamer zelda brasil"], result["markets"]["pt"]["would_add"])
        self.assertEqual(2, chat_json.await_count)
        create_record.assert_not_awaited()

    def test_two_english_batches_report_market_error(self):
        portuguese_market = [m for m in keyword_supply.MARKETS if m["lang"] == "pt"]

        with patch.object(keyword_supply, "MARKETS", portuguese_market), \
             patch.object(keyword_supply, "_load", new=AsyncMock(return_value=(set(), Counter({"pt": 5})))), \
             patch.object(
                 keyword_supply.deepseek,
                 "chat_json",
                 new=AsyncMock(side_effect=[
                     {"keywords": ["cozy gaming room brasil"]},
                     {"keywords": ["best nintendo games brasil"]},
                 ]),
             ), \
             patch.object(keyword_supply.feishu, "create_record", new=AsyncMock()) as create_record:
            result = asyncio.run(keyword_supply.run(dry_run=True))

        self.assertFalse(result["ok"])
        self.assertIn("error", result["markets"]["pt"])
        create_record.assert_not_awaited()

    def test_write_failure_makes_top_level_not_ok(self):
        portuguese_market = [m for m in keyword_supply.MARKETS if m["lang"] == "pt"]

        with patch.object(keyword_supply, "MARKETS", portuguese_market), \
             patch.object(keyword_supply, "_load", new=AsyncMock(return_value=(set(), Counter({"pt": 5})))), \
             patch.object(
                 keyword_supply.deepseek,
                 "chat_json",
                 new=AsyncMock(return_value={"keywords": ["quarto gamer zelda brasil"]}),
             ), \
             patch.object(
                 keyword_supply.feishu,
                 "create_record",
                 new=AsyncMock(side_effect=RuntimeError("Feishu unavailable")),
             ):
            result = asyncio.run(keyword_supply.run(dry_run=False))

        self.assertFalse(result["ok"])
        self.assertEqual(0, result["markets"]["pt"]["added"])
        self.assertTrue(result["markets"]["pt"]["errors"])


if __name__ == "__main__":
    unittest.main()

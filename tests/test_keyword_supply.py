import asyncio
import tempfile
import unittest
from collections import Counter
from pathlib import Path
from unittest.mock import AsyncMock, patch

from app import keyword_supply
from app.enrich_model_guard import EnrichModelBudget


class KeywordSupplyBrazilTests(unittest.TestCase):
    def test_external_youtube_history_key_normalizes_unicode_case_space_and_country_order(self):
        left = keyword_supply._discovery_history_key(
            "  ＮＹＸＩ   Switch Controller  ", ["UK", "US"], "EN",
        )
        right = keyword_supply._discovery_history_key(
            "nyxi switch controller", ["US", "UK"], "en",
        )

        self.assertEqual(left, right)

    def test_piranha_exhausted_seeds_use_traceable_seven_layer_deterministic_pool(self):
        old_words = []
        for groups in (
            keyword_supply._CAMPAIGN_KEYWORDS["piranha"],
            keyword_supply._CAMPAIGN_FALLBACK_KEYWORDS["piranha"],
        ):
            old_words.extend(groups.get("en", []))
        used = [{"fields": {
            "任务名": "[活动补池:campaign1] YT KOL - " + word,
            "关键词列表": word, "爬虫类型": "KOL-YouTube",
            "任务状态": "3-已完成", "筛选-语言": ["en"],
        }} for word in old_words]
        activity = {"fields": {
            "活动目标语言": ["en"], "竞品证据模式": "不使用",
            "竞品分析状态": "不适用", "竞品品牌": "NYXI",
        }}
        product = {"fields": {
            "产品英文名": "POWKONG Piranha Plant 2 Dock",
            "品类": "底座", "适配IP": ["Super Mario", "Piranha Plant"],
            "适配主机": ["Switch 2"],
        }}

        with patch.object(
            keyword_supply.feishu, "fetch_all_records", new=AsyncMock(return_value=used),
        ), patch.object(
            keyword_supply.deepseek, "chat_json", new=AsyncMock(),
        ) as generate, patch.object(
            keyword_supply.feishu, "create_record", new=AsyncMock(return_value="task1"),
        ) as create_record:
            result = asyncio.run(keyword_supply.ensure_campaign_supply(
                campaign_id="campaign1", activity=activity, product=product,
                required_candidates=300, max_tasks=6, dry_run=False,
            ))

        self.assertEqual(6, result["created"])
        self.assertEqual("seven_layer_deterministic", result["keyword_source"])
        self.assertEqual(6, len({item["source"] for item in result["keywords"]}))
        self.assertTrue(all(len(item["axes"]) >= 3 for item in result["keywords"]))
        self.assertTrue(all("nyxi" not in item["keyword"] for item in result["keywords"]))
        self.assertTrue(all(
            "[词源:" in call.args[1]["任务名"]
            for call in create_record.await_args_list
        ))
        generate.assert_not_awaited()

    def test_piranha_created_tasks_use_activity_country_scope(self):
        activity = {"fields": {
            "活动目标语言": ["en"], "活动目标国家": ["US"],
            "竞品证据模式": "不使用", "竞品分析状态": "不适用",
        }}
        product = {"fields": {
            "产品英文名": "POWKONG Piranha Plant 2 Dock",
            "品类": "底座", "适配IP": ["Piranha Plant"],
            "适配主机": ["Switch 2"],
        }}

        with patch.object(
            keyword_supply.feishu, "fetch_all_records",
            new=AsyncMock(return_value=[]),
        ), patch.object(
            keyword_supply.deepseek, "chat_json", new=AsyncMock(),
        ) as model, patch.object(
            keyword_supply.feishu, "create_record",
            new=AsyncMock(return_value="task1"),
        ) as create_record:
            result = asyncio.run(keyword_supply.ensure_campaign_supply(
                campaign_id="campaign1", activity=activity, product=product,
                required_candidates=50, max_tasks=1, dry_run=False,
                allow_ai=False, volume_priority=True,
            ))

        self.assertEqual(1, result["created"])
        self.assertEqual(
            ["US"], create_record.await_args.args[1]["筛选-国家"],
        )
        model.assert_not_awaited()

    def test_piranha_competitor_layer_is_only_enabled_by_activity_evidence_mode(self):
        base_fields = {
            "活动目标语言": ["en"], "竞品品牌": "OtherBrand",
        }
        product_fields = {
            "产品英文名": "POWKONG Piranha Plant 2 Dock",
            "品类": "底座", "适配IP": ["Super Mario"], "适配主机": ["Switch 2"],
        }
        disabled = keyword_supply._piranha_seven_layer_candidates(
            activity_fields={**base_fields, "竞品证据模式": "不使用",
                             "竞品分析状态": "不适用"},
            product_fields=product_fields, languages=["en"],
            existing_keywords=set(), limit=20,
        )
        enabled = keyword_supply._piranha_seven_layer_candidates(
            activity_fields={**base_fields, "竞品证据模式": keyword_supply.launch_evidence.MODE_REUSE,
                             "竞品分析状态": "已就绪"},
            product_fields=product_fields, languages=["en"],
            existing_keywords=set(), limit=20,
        )

        self.assertNotIn("competitor", {item["source"] for item in disabled})
        self.assertIn("competitor", {item["source"] for item in enabled})

    def test_zero_model_mode_reports_keyword_shortfall_without_calling_deepseek(self):
        exhausted = set(keyword_supply._CAMPAIGN_KEYWORDS["piranha"]["en"])
        exhausted.update(keyword_supply._CAMPAIGN_FALLBACK_KEYWORDS["piranha"]["en"])
        activity = {"fields": {
            "活动目标语言": ["en"], "竞品证据模式": "不使用",
            "竞品分析状态": "不适用",
        }}
        product = {"fields": {
            "产品英文名": "POWKONG Piranha Plant 2 Dock",
            "品类": "底座", "适配IP": ["Super Mario", "Piranha Plant"],
            "适配主机": ["Switch 2"],
        }}
        exhausted.update(
            item["keyword"] for item in keyword_supply._piranha_seven_layer_candidates(
                activity_fields=activity["fields"], product_fields=product["fields"],
                languages=["en"], existing_keywords=exhausted, limit=1000,
            )
        )
        used = [{"fields": {
            "任务名": "[活动补池:campaign1] YT KOL - " + word,
            "关键词列表": word, "爬虫类型": "KOL-YouTube",
            "任务状态": "3-已完成", "筛选-语言": ["en"],
        }} for word in exhausted]
        with patch.object(
            keyword_supply.feishu, "fetch_all_records", new=AsyncMock(return_value=used),
        ), patch.object(
            keyword_supply.deepseek, "chat_json", new=AsyncMock(),
        ) as model, patch.object(
            keyword_supply.feishu, "create_record", new=AsyncMock(),
        ) as write:
            result = asyncio.run(keyword_supply.ensure_campaign_supply(
                campaign_id="campaign1", activity=activity, product=product,
                required_candidates=200, dry_run=True, allow_ai=False,
            ))

        self.assertEqual(0, result["model_calls"])
        self.assertGreater(result["shortfall_tasks"], 0)
        self.assertEqual("fixed_keywords_exhausted", result["skipped"])
        model.assert_not_awaited()
        write.assert_not_awaited()

    def test_piranha_uses_curated_fallback_when_dynamic_generation_is_unavailable(self):
        activity = {"fields": {"活动目标语言": ["en"]}}
        product = {"fields": {"产品英文名": "POWKONG Piranha Plant 2 Dock"}}
        fixed = list(keyword_supply._CAMPAIGN_KEYWORDS["piranha"]["en"])
        seven_layer = keyword_supply._piranha_seven_layer_candidates(
            activity_fields=activity["fields"], product_fields=product["fields"],
            languages=["en"], existing_keywords=set(fixed), limit=100,
        )
        used = [
            {"fields": {
                "任务名": "[活动补池:campaign1] YT KOL - " + word,
                "关键词列表": word, "爬虫类型": "KOL-YouTube",
                "任务状态": "3-已完成", "筛选-语言": ["en"],
            }}
            for word in fixed + [item["keyword"] for item in seven_layer]
        ]

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

    def test_launch_keyword_generation_respects_shared_budget_and_uses_curated_fallback(self):
        activity = {"fields": {"活动目标语言": ["en"]}}
        product = {"fields": {"产品英文名": "POWKONG Piranha Plant 2 Dock"}}
        fixed = list(keyword_supply._CAMPAIGN_KEYWORDS["piranha"]["en"])
        seven_layer = keyword_supply._piranha_seven_layer_candidates(
            activity_fields=activity["fields"], product_fields=product["fields"],
            languages=["en"], existing_keywords=set(fixed), limit=100,
        )
        used = [{"fields": {
            "任务名": "[活动补池:campaign1] YT KOL - " + word,
            "关键词列表": word, "爬虫类型": "KOL-YouTube",
            "任务状态": "3-已完成", "筛选-语言": ["en"],
        }} for word in fixed + [item["keyword"] for item in seven_layer]]

        with tempfile.TemporaryDirectory() as tmp:
            budget = EnrichModelBudget(
                per_task=2, per_run=0, daily=10, failure_threshold=2,
                state_path=Path(tmp) / "launch-budget.json",
            )
            with patch.object(
                keyword_supply.feishu, "fetch_all_records", new=AsyncMock(return_value=used),
            ), patch.object(
                keyword_supply.deepseek, "chat_json", new=AsyncMock(),
            ) as model, patch.object(
                keyword_supply.feishu, "create_record", new=AsyncMock(return_value="task1"),
            ) as create_record:
                result = asyncio.run(keyword_supply.ensure_campaign_supply(
                    campaign_id="campaign1", activity=activity, product=product,
                    required_candidates=250, dry_run=False, model_budget=budget,
                ))

        self.assertTrue(result["ok"])
        self.assertEqual(5, result["created"])
        self.assertEqual("curated_fallback", result["keyword_source"])
        self.assertEqual("run_budget_exhausted", result["generation_warning"])
        self.assertEqual(0, result["model_calls"])
        self.assertEqual(0, result["model_budget"]["run_calls"])
        model.assert_not_awaited()
        self.assertEqual(5, create_record.await_count)

    def test_campaign_supply_generates_more_targeted_keywords_after_seed_list_is_exhausted(self):
        activity = {"fields": {"活动目标语言": ["en"]}}
        product = {"fields": {"产品英文名": "POWKONG Piranha Plant 2 Dock"}}
        fixed = list(keyword_supply._CAMPAIGN_KEYWORDS["piranha"]["en"])
        seven_layer = keyword_supply._piranha_seven_layer_candidates(
            activity_fields=activity["fields"], product_fields=product["fields"],
            languages=["en"], existing_keywords=set(fixed), limit=100,
        )
        used = [
            {"fields": {
                "任务名": "[活动补池:campaign1] YT KOL - " + word,
                "关键词列表": word, "爬虫类型": "KOL-YouTube",
                "任务状态": "3-已完成", "筛选-语言": ["en"],
            }}
            for word in fixed + [item["keyword"] for item in seven_layer]
        ]

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
            "任务名": (
                "[活动补池:campaign1][词源:legacy_fixed] "
                "YT KOL - dave the diver gameplay"
            ),
            "关键词列表": "dave the diver gameplay", "爬虫类型": "KOL-YouTube",
            "任务状态": "1-待触发", "筛选-语言": ["en"],
            "筛选-国家": ["US", "UK", "CA"],
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

    def test_dave_builds_diverse_traceable_keywords_after_fixed_seeds_are_exhausted(self):
        used = [
            {"fields": {
                "任务名": "[活动补池:campaign1] YT KOL - " + word,
                "关键词列表": word, "爬虫类型": "KOL-YouTube",
                "任务状态": "3-已完成", "筛选-语言": ["en"],
            }}
            for word in keyword_supply._CAMPAIGN_KEYWORDS["dave"]["en"]
        ]
        activity = {"fields": {
            "活动目标语言": ["en", "de", "es"],
            "活动目标国家": ["US", "UK", "DE", "ES"],
            "竞品证据模式": "引用历史证据", "竞品分析状态": "已就绪",
            "竞品品牌": "NYXI",
        }}
        product = {"fields": {
            "产品英文名": "FUNLAB Dave the Diver Controller",
            "适配IP": ["Dave the Diver"], "适配主机": ["Switch 2", "Switch", "PC"],
            "品类": "手柄",
            "目标人群": "Dave fans, indie game players, Switch 2 players",
            "对标关键词": "Dave the Diver; controller; indie game controller",
        }}

        with patch.object(
            keyword_supply.feishu, "fetch_all_records", new=AsyncMock(return_value=used),
        ), patch.object(
            keyword_supply.deepseek, "chat_json", new=AsyncMock(),
        ) as generate, patch.object(
            keyword_supply.feishu, "create_record", new=AsyncMock(),
        ):
            result = asyncio.run(keyword_supply.ensure_campaign_supply(
                campaign_id="campaign1", activity=activity, product=product,
                required_candidates=200, dry_run=True, max_tasks=4,
                structured_pilot=True,
            ))

        generate.assert_not_awaited()
        self.assertEqual(4, result["would_create"])
        self.assertEqual(0, result["shortfall_tasks"])
        self.assertFalse(result["quality_filters_lowered"])
        sources = {item["source"] for item in result["keywords"]}
        self.assertGreaterEqual(len(sources), 3)
        self.assertIn("competitor", sources)
        self.assertIn("ip_theme", sources)
        self.assertIn("platform", sources)
        self.assertTrue(all(len(item["axes"]) >= 2 for item in result["keywords"]))
        competitor = next(item for item in result["keywords"] if item["source"] == "competitor")
        self.assertEqual("引用历史证据", competitor["evidence_mode"])
        languages = [item["language"] for item in result["keywords"]]
        self.assertGreaterEqual(languages.count("en"), 2)
        self.assertIn("de", languages)
        self.assertIn("es", languages)

    def test_dave_no_competitor_mode_never_leaks_stale_competitor_brand(self):
        activity = {"fields": {
            "活动目标语言": ["en", "de", "es"],
            "活动目标国家": ["US", "UK", "DE", "ES"],
            "竞品证据模式": "不使用竞品证据", "竞品分析状态": "不适用",
            "竞品品牌": "NYXI",
        }}
        product = {"fields": {
            "产品英文名": "FUNLAB Dave the Diver Controller",
            "适配IP": ["Dave the Diver"], "适配主机": ["Switch 2"],
            "品类": "手柄", "目标人群": "indie game players",
        }}

        with patch.object(
            keyword_supply.feishu, "fetch_all_records", new=AsyncMock(return_value=[]),
        ), patch.object(
            keyword_supply.feishu, "create_record", new=AsyncMock(),
        ) as create_record:
            result = asyncio.run(keyword_supply.ensure_campaign_supply(
                campaign_id="campaign1", activity=activity, product=product,
                required_candidates=200, dry_run=True, max_tasks=4,
                structured_pilot=True,
            ))

        self.assertEqual(4, result["would_create"])
        self.assertNotIn("competitor", {item["source"] for item in result["keywords"]})
        self.assertTrue(all("nyxi" not in item["keyword"] for item in result["keywords"]))
        create_record.assert_not_awaited()

    def test_autonomous_dave_reuses_old_external_youtube_keys_after_seven_days(self):
        now_ms = 1_800_000_000_000
        old_seconds = (now_ms - keyword_supply.DISCOVERY_REUSE_WINDOW_MS - 1) // 1000
        activity = {"fields": {
            "活动目标语言": ["en", "de", "es"],
            "活动目标国家": ["US", "UK", "DE", "ES"],
            "竞品证据模式": "引用历史证据", "竞品分析状态": "已就绪",
            "竞品品牌": "NYXI",
        }}
        product = {"fields": {
            "产品英文名": "FUNLAB Dave the Diver Controller",
            "适配IP": ["Dave the Diver"], "适配主机": ["Switch 2"],
            "品类": "手柄", "目标人群": "indie game players",
        }}
        structured = keyword_supply._dave_structured_candidates(
            activity_fields=activity["fields"], product_fields=product["fields"],
            languages=["en", "de", "es"], existing_keywords=set(),
            pilot_version="v2",
        )
        language_countries = {
            "en": ["US", "UK"], "de": ["DE"], "es": ["ES"],
        }
        used = []
        for language in ("en", "de", "es"):
            for word in keyword_supply._CAMPAIGN_KEYWORDS["dave"][language]:
                used.append({"last_modified_time": old_seconds, "fields": {
                    "任务名": (
                        f"[活动补池:campaign1][词源:legacy_fixed] YT KOL - {word}"
                    ),
                    "关键词列表": word, "任务状态": "3-已完成",
                    "筛选-语言": [language], "筛选-国家": language_countries[language],
                    "执行日志": "待入库: 4\n其中有邮箱: 2",
                }})
        for item in structured:
            used.append({"last_modified_time": old_seconds, "fields": {
                "任务名": (
                    f"[活动补池:campaign1][词源:{item['source']}] "
                    f"YT KOL - {item['keyword']}"
                ),
                "关键词列表": item["keyword"], "任务状态": "3-已完成",
                "筛选-语言": [item["language"]],
                "筛选-国家": language_countries[item["language"]],
                "执行日志": "待入库: 4\n其中有邮箱: 2",
            }})

        with patch.object(keyword_supply.time, "time", return_value=now_ms / 1000), \
             patch.object(
                 keyword_supply.feishu, "fetch_all_records",
                 new=AsyncMock(return_value=used),
             ), patch.object(
                 keyword_supply.deepseek, "chat_json", new=AsyncMock(),
             ) as model:
            result = asyncio.run(keyword_supply.ensure_campaign_supply(
                campaign_id="campaign1", activity=activity, product=product,
                required_candidates=200, max_tasks=4, dry_run=True,
            ))

        self.assertEqual(4, result["would_create"])
        self.assertEqual(0, result["shortfall_tasks"])
        self.assertGreaterEqual(len({item["source"] for item in result["keywords"]}), 3)
        self.assertGreater(result["keyword_history_state"]["reusable_after_ttl"], 0)
        self.assertEqual(4, result["external_youtube_discovery"]["would_create"])
        self.assertIn("completed_tasks", result["external_youtube_outcome"])
        self.assertFalse(result["quality_filters_lowered"])
        model.assert_not_awaited()

    def test_autonomous_piranha_reuses_seven_layer_keys_without_ai_after_seven_days(self):
        now_ms = 1_800_000_000_000
        old_seconds = (now_ms - keyword_supply.DISCOVERY_REUSE_WINDOW_MS - 1) // 1000
        activity = {"fields": {
            "活动目标语言": ["en", "de", "es"],
            "活动目标国家": ["US", "UK", "DE", "ES"],
            "竞品证据模式": "不使用竞品证据", "竞品分析状态": "不适用",
        }}
        product = {"fields": {
            "产品英文名": "POWKONG Piranha Plant 2 Dock",
            "适配IP": ["Piranha Plant"], "适配主机": ["Switch 2"],
            "品类": "底座",
        }}
        countries = {"en": ["US", "UK"], "de": ["DE"], "es": ["ES"]}
        deterministic = keyword_supply._piranha_seven_layer_candidates(
            activity_fields=activity["fields"], product_fields=product["fields"],
            languages=["en", "de", "es"], existing_keywords=set(), limit=100,
        )
        rows = []
        for language in ("en", "de", "es"):
            for word in keyword_supply._CAMPAIGN_KEYWORDS["piranha"][language]:
                rows.append({"last_modified_time": old_seconds, "fields": {
                    "任务名": (
                        f"[活动补池:campaign1][词源:legacy_fixed] YT KOL - {word}"
                    ),
                    "关键词列表": word, "任务状态": "3-已完成",
                    "筛选-语言": [language], "筛选-国家": countries[language],
                    "执行日志": "原始发现: 4\n其中有邮箱: 2",
                }})
        for item in deterministic:
            rows.append({"last_modified_time": old_seconds, "fields": {
                "任务名": (
                    f"[活动补池:campaign1][词源:{item['source']}] "
                    f"YT KOL - {item['keyword']}"
                ),
                "关键词列表": item["keyword"], "任务状态": "3-已完成",
                "筛选-语言": [item["language"]],
                "筛选-国家": countries[item["language"]],
                "执行日志": "原始发现: 4\n其中有邮箱: 2",
            }})

        with tempfile.TemporaryDirectory() as tmp:
            budget = EnrichModelBudget(
                per_task=2, per_run=0, daily=0, failure_threshold=2,
                state_path=Path(tmp) / "launch-budget.json",
            )
            with patch.object(keyword_supply.time, "time", return_value=now_ms / 1000), \
                 patch.object(
                     keyword_supply.feishu, "fetch_all_records",
                     new=AsyncMock(return_value=rows),
                 ), patch.object(
                     keyword_supply.deepseek, "chat_json", new=AsyncMock(),
                 ) as model:
                result = asyncio.run(keyword_supply.ensure_campaign_supply(
                    campaign_id="campaign1", activity=activity, product=product,
                    required_candidates=200, max_tasks=4, dry_run=True,
                    model_budget=budget, volume_priority=True,
                ))

        self.assertEqual(4, result["would_create"])
        self.assertEqual(0, result["shortfall_tasks"])
        self.assertGreater(result["keyword_history_state"]["reusable_after_ttl"], 0)
        self.assertGreaterEqual(len({item["source"] for item in result["keywords"]}), 3)
        self.assertFalse(result["quality_filters_lowered"])
        self.assertNotIn("competitor", {item["source"] for item in result["keywords"]})
        model.assert_not_awaited()

    def test_autonomous_dave_blocks_recent_same_key_but_not_other_market_key(self):
        now_ms = 1_800_000_000_000
        recent_seconds = (now_ms - 60_000) // 1000
        word = keyword_supply._CAMPAIGN_KEYWORDS["dave"]["en"][0]
        rows = [{"last_modified_time": recent_seconds, "fields": {
            "任务名": f"[活动补池:campaign1] YT KOL - {word}",
            "关键词列表": word, "任务状态": "3-已完成",
            "筛选-语言": ["de"], "筛选-国家": ["DE"],
            "执行日志": "待入库: 2\n其中有邮箱: 1",
        }}]
        activity = {"fields": {
            "活动目标语言": ["en"], "活动目标国家": ["US"],
        }}
        product = {"fields": {
            "产品英文名": "FUNLAB Dave the Diver Controller",
            "适配IP": ["Dave the Diver"], "适配主机": ["Switch 2"],
            "品类": "手柄",
        }}

        with patch.object(keyword_supply.time, "time", return_value=now_ms / 1000), \
             patch.object(
                 keyword_supply.feishu, "fetch_all_records",
                 new=AsyncMock(return_value=rows),
             ):
            result = asyncio.run(keyword_supply.ensure_campaign_supply(
                campaign_id="campaign1", activity=activity, product=product,
                required_candidates=150, max_tasks=3, dry_run=True,
            ))

        self.assertIn(word, {item["keyword"] for item in result["keywords"]})
        self.assertEqual(0, result["keyword_history_state"]["recently_executed"])

    def test_autonomous_dave_never_duplicates_an_active_external_youtube_key(self):
        now_ms = 1_800_000_000_000
        word = keyword_supply._CAMPAIGN_KEYWORDS["dave"]["en"][0]
        rows = [{"last_modified_time": (now_ms - 30 * 86_400_000) // 1000,
                 "fields": {
            "任务名": f"[活动补池:campaign1] YT KOL - {word}",
            "关键词列表": word, "任务状态": "1-待触发",
            "筛选-语言": ["en"], "筛选-国家": ["US"],
            "创建日期": now_ms - 30 * 86_400_000,
        }}]
        activity = {"fields": {
            "活动目标语言": ["en"], "活动目标国家": ["US"],
        }}
        product = {"fields": {
            "产品英文名": "FUNLAB Dave the Diver Controller",
            "适配IP": ["Dave the Diver"], "适配主机": ["Switch 2"],
            "品类": "手柄",
        }}

        with patch.object(keyword_supply.time, "time", return_value=now_ms / 1000), \
             patch.object(
                 keyword_supply.feishu, "fetch_all_records",
                 new=AsyncMock(return_value=rows),
             ):
            result = asyncio.run(keyword_supply.ensure_campaign_supply(
                campaign_id="campaign1", activity=activity, product=product,
                required_candidates=150, max_tasks=3, dry_run=True,
            ))

        self.assertNotIn(word, {item["keyword"] for item in result["keywords"]})
        self.assertGreater(result["keyword_history_state"]["active_duplicate"], 0)

    def test_autonomous_dave_source_zero_twice_cools_only_that_campaign_source(self):
        now_ms = 1_800_000_000_000
        recent_seconds = (now_ms - 60_000) // 1000
        activity = {"fields": {
            "活动目标语言": ["en", "de", "es"],
            "活动目标国家": ["US", "UK", "DE", "ES"],
            "竞品证据模式": "引用历史证据", "竞品分析状态": "已就绪",
            "竞品品牌": "NYXI",
        }}
        product = {"fields": {
            "产品英文名": "FUNLAB Dave the Diver Controller",
            "适配IP": ["Dave the Diver"], "适配主机": ["Switch 2"],
            "品类": "手柄",
        }}
        rows = [{"last_modified_time": recent_seconds - index * 30,
                 "fields": {
            "任务名": (
                f"[活动补池:campaign1][词源:competitor] "
                f"YT KOL - zero result {index}"
            ),
            "关键词列表": f"zero result {index}", "任务状态": "3-已完成",
            "筛选-语言": ["en"], "筛选-国家": ["US", "UK"],
            "执行日志": "待入库: 0\n其中有邮箱: 0",
        }} for index in range(2)]

        with patch.object(keyword_supply.time, "time", return_value=now_ms / 1000), \
             patch.object(
                 keyword_supply.feishu, "fetch_all_records",
                 new=AsyncMock(return_value=rows),
             ):
            result = asyncio.run(keyword_supply.ensure_campaign_supply(
                campaign_id="campaign1", activity=activity, product=product,
                required_candidates=200, max_tasks=9, dry_run=True,
                volume_priority=True,
            ))

        self.assertEqual("cooldown", result["source_health"]["competitor"]["mode"])
        self.assertNotIn("competitor", {item["source"] for item in result["keywords"]})
        self.assertGreater(result["keyword_history_state"]["source_cooling"], 0)

    def test_positive_email_without_internal_attribution_stays_in_probe_mode(self):
        now_ms = 1_800_000_000_000
        recent_seconds = (now_ms - 60_000) // 1000
        activity = {"fields": {
            "活动目标语言": ["en"], "活动目标国家": ["US"],
            "竞品证据模式": "引用历史证据", "竞品分析状态": "已就绪",
            "竞品品牌": "NYXI",
        }}
        product = {"fields": {
            "产品英文名": "FUNLAB Dave the Diver Controller",
            "适配IP": ["Dave the Diver"], "适配主机": ["Switch 2"],
            "品类": "手柄",
        }}
        rows = [{"last_modified_time": recent_seconds - index * 30,
                 "fields": {
            "任务名": (
                f"[活动补池:campaign1][词源:competitor] "
                f"YT KOL - pending outcome {index}"
            ),
            "关键词列表": f"pending outcome {index}", "任务状态": "3-已完成",
            "筛选-语言": ["en"], "筛选-国家": ["US"],
            "执行日志": (
                "待入库: 4\n其中有邮箱: 2"
                if index == 0 else "待入库: 0\n其中有邮箱: 0"
            ),
        }} for index in range(3)]

        with patch.object(keyword_supply.time, "time", return_value=now_ms / 1000), \
             patch.object(
                 keyword_supply.feishu, "fetch_all_records",
                 new=AsyncMock(return_value=rows),
             ):
            result = asyncio.run(keyword_supply.ensure_campaign_supply(
                campaign_id="campaign1", activity=activity, product=product,
                required_candidates=200, max_tasks=4, dry_run=True,
            ))

        health = result["source_health"]["competitor"]
        self.assertEqual("signal_missing", health["mode"])
        self.assertEqual(1, health["pending_or_unknown"])
        self.assertNotEqual("cooldown", health["mode"])
        self.assertLessEqual(
            sum(item["source"] == "competitor" for item in result["keywords"]), 1,
        )

    def test_source_outcome_with_usable_candidate_restores_healthy_rotation(self):
        now_ms = 1_800_000_000_000
        recent_seconds = (now_ms - 60_000) // 1000
        rows = [{"record_id": f"task{index}",
                 "last_modified_time": recent_seconds - index * 30, "fields": {
            "任务名": (
                f"[活动补池:campaign1][词源:competitor] "
                f"YT KOL - pending outcome {index}"
            ),
            "关键词列表": f"pending outcome {index}", "任务状态": "3-已完成",
            "筛选-语言": ["en"], "筛选-国家": ["US"],
            "执行日志": "待入库: 4\n其中有邮箱: 2",
        }} for index in range(2)]
        activity = {"fields": {
            "活动目标语言": ["en"], "活动目标国家": ["US"],
            "竞品证据模式": "引用历史证据", "竞品分析状态": "已就绪",
            "竞品品牌": "NYXI",
        }}
        product = {"fields": {
            "产品英文名": "FUNLAB Dave the Diver Controller",
            "适配IP": ["Dave the Diver"], "适配主机": ["Switch 2"],
            "品类": "手柄",
        }}

        with patch.object(keyword_supply.time, "time", return_value=now_ms / 1000), \
             patch.object(
                 keyword_supply.feishu, "fetch_all_records",
                 new=AsyncMock(return_value=rows),
             ):
            result = asyncio.run(keyword_supply.ensure_campaign_supply(
                campaign_id="campaign1", activity=activity, product=product,
                required_candidates=200, max_tasks=4, dry_run=True,
                source_outcomes={"by_task": {
                    "task0": {
                        "source": "competitor", "keyword": "pending outcome 0",
                        "discovered": 1, "auto_approved": 1, "operator_review": 0,
                    },
                }},
            ))

        self.assertEqual("healthy", result["source_health"]["competitor"]["mode"])

    def test_historical_source_aggregate_cannot_override_two_zero_rounds(self):
        now_ms = 1_800_000_000_000
        recent_seconds = (now_ms - 60_000) // 1000
        rows = [{"record_id": f"zero{index}",
                 "last_modified_time": recent_seconds - index * 30, "fields": {
            "任务名": (
                f"[活动补池:campaign1][词源:competitor] YT KOL - zero {index}"
            ),
            "关键词列表": f"zero {index}", "任务状态": "3-已完成",
            "筛选-语言": ["en"], "筛选-国家": ["US"],
            "执行日志": "待入库: 0\n其中有邮箱: 0",
        }} for index in range(2)]

        health = keyword_supply._source_health(
            rows, prefix="[活动补池:campaign1]", now_ms=now_ms,
            source_outcomes={
                "by_source": {"competitor": {"auto_approved": 99}},
                "by_task": {},
            },
        )

        self.assertEqual("cooldown", health["competitor"]["mode"])

    def test_history_key_requires_complete_dimensions_and_completed_status(self):
        now_ms = 1_800_000_000_000
        recent_seconds = (now_ms - 60_000) // 1000
        word = keyword_supply._CAMPAIGN_KEYWORDS["dave"]["en"][0]
        dirty_rows = [
            {"last_modified_time": recent_seconds, "fields": {
                "任务名": "[活动补池:campaign1] missing dimensions",
                "关键词列表": word, "任务状态": "3-已完成",
                "筛选-国家": [], "筛选-语言": [],
                "执行日志": "待入库: 0\n其中有邮箱: 0",
            }},
            {"last_modified_time": recent_seconds, "fields": {
                "任务名": "[活动补池:campaign1] cancelled",
                "关键词列表": word, "任务状态": "8-已取消",
                "筛选-国家": ["US"], "筛选-语言": ["en"],
            }},
        ]

        state = keyword_supply._history_key_state(
            dirty_rows, keyword=word, countries=["US"], language="en", now_ms=now_ms,
        )

        self.assertEqual("unseen", state["state"])

    def test_existing_active_source_probe_blocks_another_probe_across_runs(self):
        now_ms = 1_800_000_000_000
        old_seconds = (now_ms - 3 * 60 * 60 * 1000) // 1000
        rows = [{"last_modified_time": old_seconds - index * 30, "fields": {
            "任务名": (
                f"[活动补池:campaign1][词源:competitor] YT KOL - zero {index}"
            ),
            "关键词列表": f"zero {index}", "任务状态": "3-已完成",
            "筛选-国家": ["US"], "筛选-语言": ["en"],
            "执行日志": "待入库: 0\n其中有邮箱: 0",
        }} for index in range(2)]
        rows.append({"last_modified_time": now_ms // 1000, "fields": {
            "任务名": "[活动补池:campaign1][词源:competitor] YT KOL - active probe",
            "关键词列表": "active probe", "任务状态": "2-运行中",
            "筛选-国家": ["US"], "筛选-语言": ["en"],
        }})
        activity = {"fields": {
            "活动目标语言": ["en"], "活动目标国家": ["US"],
            "竞品证据模式": "引用历史证据", "竞品分析状态": "已就绪",
            "竞品品牌": "NYXI",
        }}
        product = {"fields": {
            "产品英文名": "FUNLAB Dave the Diver Controller",
            "适配IP": ["Dave the Diver"], "适配主机": ["Switch 2"],
            "品类": "手柄",
        }}

        with patch.object(keyword_supply.time, "time", return_value=now_ms / 1000), \
             patch.object(
                 keyword_supply.feishu, "fetch_all_records",
                 new=AsyncMock(return_value=rows),
             ):
            result = asyncio.run(keyword_supply.ensure_campaign_supply(
                campaign_id="campaign1", activity=activity, product=product,
                required_candidates=200, max_tasks=9, dry_run=True,
            ))

        self.assertEqual("probe", result["source_health"]["competitor"]["mode"])
        self.assertNotIn("competitor", {item["source"] for item in result["keywords"]})

    def test_missing_source_signal_fails_closed_to_one_campaign_probe(self):
        now_ms = 1_800_000_000_000
        rows = [{"last_modified_time": (now_ms - 60_000) // 1000, "fields": {
            "任务名": "[活动补池:campaign1] YT KOL - old untagged",
            "关键词列表": "old untagged", "任务状态": "3-已完成",
            "筛选-国家": ["US"], "筛选-语言": ["en"],
            "执行日志": "待入库: 0\n其中有邮箱: 0",
        }}]
        activity = {"fields": {
            "活动目标语言": ["en"], "活动目标国家": ["US"],
        }}
        product = {"fields": {
            "产品英文名": "FUNLAB Dave the Diver Controller",
            "适配IP": ["Dave the Diver"], "适配主机": ["Switch 2"],
            "品类": "手柄",
        }}

        with patch.object(keyword_supply.time, "time", return_value=now_ms / 1000), \
             patch.object(
                 keyword_supply.feishu, "fetch_all_records",
                 new=AsyncMock(return_value=rows),
             ):
            result = asyncio.run(keyword_supply.ensure_campaign_supply(
                campaign_id="campaign1", activity=activity, product=product,
                required_candidates=200, max_tasks=9, dry_run=True,
            ))

        self.assertEqual("source_signal_unavailable", result["skipped"])
        self.assertEqual(1, result["would_create"])

    def test_source_signal_scan_does_not_ignore_older_completed_missing_task(self):
        now_ms = 1_800_000_000_000
        rows = [
            {"record_id": f"complete-{index}",
             "last_modified_time": (now_ms - index * 60_000) // 1000,
             "fields": {
                 "任务名": (
                     f"[活动补池:campaign1][词源:competitor] complete {index}"
                 ),
                 "关键词列表": f"complete {index}", "任务状态": "3-已完成",
                 "执行日志": "原始发现: 2\n其中有邮箱: 1",
             }}
            for index in (1, 2)
        ]
        rows.append({
            "record_id": "older-missing-source",
            "last_modified_time": (now_ms - 3 * 60_000) // 1000,
            "fields": {
                "任务名": "[活动补池:campaign1] old completed task",
                "关键词列表": "old completed task", "任务状态": "3-已完成",
                "执行日志": "原始发现: 2\n其中有邮箱: 1",
            },
        })

        signal = keyword_supply._campaign_source_signal(
            rows, prefix="[活动补池:campaign1]",
        )

        self.assertFalse(signal["available"])
        self.assertEqual(1, signal["missing_tasks"])
        self.assertEqual("older-missing-source", signal["details"][0]["record_id"])

    def test_reusable_keyword_prefers_positive_then_unknown_then_zero(self):
        positive_word, zero_word = keyword_supply._CAMPAIGN_KEYWORDS["dave"]["en"][:2]
        ordered = keyword_supply._round_robin_sources([
            {"source": "legacy_fixed", "keyword": zero_word,
             "outcome_rank": 2, "last_completed_ms": 10},
            {"source": "legacy_fixed", "keyword": "unknown",
             "outcome_rank": 1, "last_completed_ms": 5},
            {"source": "legacy_fixed", "keyword": positive_word,
             "outcome_rank": 0, "last_completed_ms": 20},
        ], {
            "legacy_fixed": {"last_completed_ms": 20},
        })

        self.assertEqual([positive_word, "unknown", zero_word], [x["keyword"] for x in ordered])

    def test_external_outcome_does_not_call_new_plus_updated_raw_discovered(self):
        rows = [{"fields": {
            "任务名": "[活动补池:campaign1][词源:platform] task",
            "任务状态": "3-已完成", "实际产出-新增": 3, "实际产出-更新": 35,
            "执行日志": "其中有邮箱: 0",
        }}]
        outcome = keyword_supply._external_youtube_outcome(
            rows, prefix="[活动补池:campaign1]",
        )

        self.assertEqual(0, outcome["raw_discovered"])
        self.assertEqual(38, outcome["records_written"])
        self.assertEqual(3, outcome["new_records"])

    def test_real_supply_calls_for_same_campaign_share_one_write_lock(self):
        state = {"active": 0, "max_active": 0}

        async def fake_unlocked(**_kwargs):
            state["active"] += 1
            state["max_active"] = max(state["max_active"], state["active"])
            await asyncio.sleep(0.01)
            state["active"] -= 1
            return {"ok": True}

        async def scenario():
            common = {
                "campaign_id": "campaign1", "activity": {}, "product": {},
                "required_candidates": 1, "dry_run": False,
            }
            return await asyncio.gather(
                keyword_supply.ensure_campaign_supply(**common),
                keyword_supply.ensure_campaign_supply(**common),
            )

        with patch.object(
            keyword_supply, "_ensure_campaign_supply_unlocked", new=fake_unlocked,
        ):
            asyncio.run(scenario())

        self.assertEqual(1, state["max_active"])

    def test_new_competitor_investigation_is_not_used_before_evidence_is_ready(self):
        activity = {"fields": {
            "活动目标语言": ["en", "de", "es"],
            "活动目标国家": ["US", "UK", "DE", "ES"],
            "竞品证据模式": "发起新分析", "竞品分析状态": "分析中",
            "竞品品牌": "OtherBrand",
        }}
        product = {"fields": {
            "产品英文名": "FUNLAB Dave the Diver Controller",
            "适配IP": ["Dave the Diver"], "适配主机": ["Switch 2"],
            "品类": "手柄",
        }}

        with patch.object(
            keyword_supply.feishu, "fetch_all_records", new=AsyncMock(return_value=[]),
        ):
            result = asyncio.run(keyword_supply.ensure_campaign_supply(
                campaign_id="campaign1", activity=activity, product=product,
                required_candidates=200, dry_run=True, max_tasks=4,
                structured_pilot=True,
            ))

        self.assertNotIn("competitor", {item["source"] for item in result["keywords"]})
        self.assertTrue(all(
            "otherbrand" not in item["keyword"] for item in result["keywords"]
        ))

    def test_pilot_preview_reports_target_countries_not_covered_by_selected_languages(self):
        activity = {"fields": {
            "活动目标语言": ["de", "es"],
            "活动目标国家": ["DE", "ES", "FR"],
            "竞品证据模式": "不使用竞品证据", "竞品分析状态": "不适用",
        }}
        product = {"fields": {
            "产品英文名": "FUNLAB Dave the Diver Controller",
            "适配IP": ["Dave the Diver"], "适配主机": ["Switch 2"],
            "品类": "手柄",
        }}

        with patch.object(
            keyword_supply.feishu, "fetch_all_records", new=AsyncMock(return_value=[]),
        ):
            result = asyncio.run(keyword_supply.ensure_campaign_supply(
                campaign_id="campaign1", activity=activity, product=product,
                required_candidates=200, dry_run=True, max_tasks=4,
                structured_pilot=True,
            ))

        self.assertEqual(["FR"], result["uncovered_target_countries"])

    def test_campaign_keyword_task_persists_source_tag_without_changing_filters(self):
        activity = {"fields": {
            "活动目标语言": ["en", "de", "es"],
            "活动目标国家": ["US", "UK", "DE", "FR", "ES", "IT", "NL", "SE", "PT"],
            "竞品证据模式": "引用历史证据", "竞品分析状态": "已就绪",
            "竞品品牌": "NYXI",
        }}
        product = {"fields": {
            "产品英文名": "FUNLAB Dave the Diver Controller",
            "适配IP": ["Dave the Diver"], "适配主机": ["Switch 2"], "品类": "手柄",
        }}

        with patch.object(
            keyword_supply.feishu, "fetch_all_records", new=AsyncMock(return_value=[]),
        ), patch.object(
            keyword_supply.feishu, "create_record", new=AsyncMock(return_value="task1"),
        ) as create_record:
            result = asyncio.run(keyword_supply.ensure_campaign_supply(
                campaign_id="campaign1", activity=activity, product=product,
                required_candidates=200, dry_run=False, max_tasks=4,
                structured_pilot=True,
            ))

        self.assertEqual(4, result["created"])
        for call in create_record.await_args_list:
            fields = call.args[1]
            self.assertRegex(
                fields["任务名"],
                r"^\[活动补池:campaign1\]\[灰度:dave-keyword-v1\]\[词源:[a-z_]+\]",
            )
            self.assertEqual(50, fields["每批数量上限"])
            self.assertEqual("1-待触发", fields["任务状态"])
            self.assertTrue(fields["触发"])
            self.assertNotIn("CA", fields["筛选-国家"])
            expected = {
                "en": ["US", "UK", "FR", "IT", "NL", "SE", "PT"],
                "de": ["DE"], "es": ["ES"],
            }
            self.assertEqual(expected[fields["筛选-语言"][0]], fields["筛选-国家"])
        self.assertEqual([], result["uncovered_target_countries"])

    def test_autonomous_dave_supply_keeps_legacy_keywords_and_adds_source_trace(self):
        activity = {"fields": {
            "活动目标语言": ["en"], "活动目标国家": ["US", "UK"],
        }}
        product = {"fields": {"产品英文名": "FUNLAB Dave the Diver Controller"}}

        with patch.object(
            keyword_supply.feishu, "fetch_all_records", new=AsyncMock(return_value=[]),
        ), patch.object(
            keyword_supply.feishu, "create_record", new=AsyncMock(return_value="task1"),
        ) as create_record:
            result = asyncio.run(keyword_supply.ensure_campaign_supply(
                campaign_id="campaign1", activity=activity, product=product,
                required_candidates=150, dry_run=False,
            ))

        self.assertEqual("deterministic", result["keyword_source"])
        self.assertGreater(result["created"], 0)
        self.assertTrue(all(
            "[灰度:" not in call.args[1]["任务名"]
            and "[词源:legacy_fixed]" in call.args[1]["任务名"]
            for call in create_record.await_args_list
        ))
        self.assertTrue(all(
            call.args[1]["筛选-国家"] == ["US", "UK"]
            for call in create_record.await_args_list
        ))

    def test_dave_pilot_retries_only_the_missing_records_after_partial_write(self):
        rows = [{"fields": {
            "任务名": (
                "[活动补池:campaign1][灰度:dave-keyword-v1]"
                "[词源:ip_theme] YT KOL - dave the diver review"
            ),
            "关键词列表": "dave the diver review", "任务状态": "3-已完成",
        }}]
        activity = {"fields": {
            "活动目标语言": ["en", "de", "es"],
            "活动目标国家": ["US", "UK", "DE", "ES"],
        }}
        product = {"fields": {"产品英文名": "FUNLAB Dave the Diver Controller"}}

        with patch.object(
            keyword_supply.feishu, "fetch_all_records", new=AsyncMock(return_value=rows),
        ), patch.object(
            keyword_supply.feishu, "create_record", new=AsyncMock(return_value="task2"),
        ) as create_record:
            result = asyncio.run(keyword_supply.ensure_campaign_supply(
                campaign_id="campaign1", activity=activity, product=product,
                required_candidates=200, dry_run=False, max_tasks=4,
                structured_pilot=True,
            ))

        self.assertEqual(3, result["created"])
        self.assertEqual(3, create_record.await_count)

    def test_dave_pilot_stops_after_four_records_exist(self):
        rows = [{"fields": {
            "任务名": (
                "[活动补池:campaign1][灰度:dave-keyword-v1]"
                f"[词源:platform] YT KOL - pilot word {index}"
            ),
            "关键词列表": f"pilot word {index}", "任务状态": "3-已完成",
        }} for index in range(4)]
        activity = {"fields": {
            "活动目标语言": ["en", "de", "es"],
            "活动目标国家": ["US", "UK", "DE", "ES"],
        }}
        product = {"fields": {"产品英文名": "FUNLAB Dave the Diver Controller"}}

        with patch.object(
            keyword_supply.feishu, "fetch_all_records", new=AsyncMock(return_value=rows),
        ), patch.object(
            keyword_supply.feishu, "create_record", new=AsyncMock(),
        ) as create_record:
            result = asyncio.run(keyword_supply.ensure_campaign_supply(
                campaign_id="campaign1", activity=activity, product=product,
                required_candidates=200, dry_run=False, max_tasks=4,
                structured_pilot=True,
            ))

        self.assertEqual("pilot_already_created", result["skipped"])
        self.assertEqual(0, result["created"])
        create_record.assert_not_awaited()

    def test_dave_v2_uses_three_axis_terms_and_ignores_completed_v1_batch(self):
        rows = [{"fields": {
            "任务名": (
                "[活动补池:campaign1][灰度:dave-keyword-v1]"
                f"[词源:platform] YT KOL - v1 word {index}"
            ),
            "关键词列表": f"v1 word {index}", "任务状态": "3-已完成",
        }} for index in range(4)]
        activity = {"fields": {
            "活动目标语言": ["en", "de", "es"],
            "活动目标国家": ["US", "UK", "DE", "ES"],
            "竞品证据模式": "引用历史证据", "竞品分析状态": "已就绪",
            "竞品品牌": "NYXI",
        }}
        product = {"fields": {
            "产品英文名": "FUNLAB Dave the Diver Controller",
            "适配IP": ["Dave the Diver"], "适配主机": ["Switch 2"],
            "品类": "手柄",
        }}

        with patch.object(
            keyword_supply.feishu, "fetch_all_records", new=AsyncMock(return_value=rows),
        ):
            result = asyncio.run(keyword_supply.ensure_campaign_supply(
                campaign_id="campaign1", activity=activity, product=product,
                required_candidates=200, dry_run=True, max_tasks=4,
                structured_pilot=True, pilot_version="v2",
            ))

        self.assertEqual("structured_v2", result["keyword_source"])
        self.assertEqual(4, result["would_create"])
        words = {item["keyword"] for item in result["keywords"]}
        self.assertIn("nyxi switch controller review", words)
        self.assertIn("dave the diver nintendo switch gameplay review", words)
        self.assertIn("nintendo switch 2 controller gaming test deutsch", words)
        self.assertIn("mando nintendo switch 2 review gaming españa", words)
        ip_item = next(item for item in result["keywords"] if item["source"] == "ip_theme")
        self.assertEqual(["ip_theme", "platform", "content_format"], ip_item["axes"])

    def test_discovery_item_blocks_official_and_multilingual_purchase_intent(self):
        blocked = [
            ("en", "official controller store"),
            ("en", "officially reviewed controller deals"),
            ("de", "controller günstig kaufen"),
            ("de", "offizieller controller angebote preise"),
            ("es", "comprar mando oferta"),
            ("es", "mandos oficiales precios ofertas"),
        ]
        for language, word in blocked:
            with self.subTest(word=word):
                self.assertIsNone(keyword_supply._discovery_item(
                    language=language, keyword=word, source="platform", reason="test",
                ))

    def test_run_campaign_pilot_is_scoped_to_active_locked_dave_activity(self):
        active = {"fields": {
            "活动ID": keyword_supply.DAVE_KEYWORD_PILOT_CAMPAIGN_ID,
            "运行模式": "正式运行", "状态": "正式执行中",
            "产品主记录ID": "product1",
        }}
        locked_product = {"fields": {
            "产品英文名": "FUNLAB Dave the Diver Controller",
            "派单模式": "活动专用",
        }}
        with patch.object(
            keyword_supply.launch_evidence, "get_activity", new=AsyncMock(return_value=active),
        ), patch.object(
            keyword_supply.feishu, "get_record", new=AsyncMock(return_value=locked_product),
        ), patch.object(
            keyword_supply, "ensure_campaign_supply",
            new=AsyncMock(return_value={"ok": True, "created": 0, "would_create": 4}),
        ) as ensure:
            result = asyncio.run(keyword_supply.run_campaign_pilot(
                campaign_id=keyword_supply.DAVE_KEYWORD_PILOT_CAMPAIGN_ID,
                dry_run=True,
            ))

        self.assertTrue(result["read_only"])
        ensure.assert_awaited_once()
        self.assertTrue(ensure.await_args.kwargs["structured_pilot"])

        with patch.object(
            keyword_supply.launch_evidence, "get_activity", new=AsyncMock(return_value=active),
        ):
            with self.assertRaisesRegex(ValueError, "只允许戴夫活动"):
                asyncio.run(keyword_supply.run_campaign_pilot(
                    campaign_id="another-campaign", dry_run=True,
                ))

        inactive = {"fields": {**active["fields"], "状态": "已暂停"}}
        with patch.object(
            keyword_supply.launch_evidence, "get_activity", new=AsyncMock(return_value=inactive),
        ):
            with self.assertRaisesRegex(ValueError, "不是正式运行"):
                asyncio.run(keyword_supply.run_campaign_pilot(
                    campaign_id=keyword_supply.DAVE_KEYWORD_PILOT_CAMPAIGN_ID,
                    dry_run=True,
                ))

        unlocked_product = {"fields": {**locked_product["fields"], "派单模式": "常规"}}
        with patch.object(
            keyword_supply.launch_evidence, "get_activity", new=AsyncMock(return_value=active),
        ), patch.object(
            keyword_supply.feishu, "get_record", new=AsyncMock(return_value=unlocked_product),
        ):
            with self.assertRaisesRegex(ValueError, "未处于活动专用锁"):
                asyncio.run(keyword_supply.run_campaign_pilot(
                    campaign_id=keyword_supply.DAVE_KEYWORD_PILOT_CAMPAIGN_ID,
                    dry_run=True,
                ))

    def test_concurrent_real_pilot_submissions_create_at_most_four_tasks(self):
        active = {"fields": {
            "活动ID": keyword_supply.DAVE_KEYWORD_PILOT_CAMPAIGN_ID,
            "运行模式": "正式运行", "状态": "正式执行中",
            "产品主记录ID": "product1", "活动目标语言": ["en", "de", "es"],
            "活动目标国家": ["US", "UK", "DE", "ES"],
            "竞品证据模式": "不使用竞品证据", "竞品分析状态": "不适用",
        }}
        product = {"fields": {
            "产品英文名": "FUNLAB Dave the Diver Controller",
            "派单模式": "活动专用", "适配IP": ["Dave the Diver"],
            "适配主机": ["Switch 2"], "品类": "手柄",
        }}
        crawler_rows = []

        async def fetch_rows(_table):
            return list(crawler_rows)

        async def create_row(_table, fields):
            crawler_rows.append({"record_id": f"task{len(crawler_rows) + 1}",
                                 "fields": dict(fields)})
            await asyncio.sleep(0)
            return crawler_rows[-1]["record_id"]

        async def exercise():
            return await asyncio.gather(*[
                keyword_supply.run_campaign_pilot(
                    campaign_id=keyword_supply.DAVE_KEYWORD_PILOT_CAMPAIGN_ID,
                    dry_run=False,
                ) for _ in range(2)
            ])

        with patch.object(
            keyword_supply.launch_evidence, "get_activity", new=AsyncMock(return_value=active),
        ), patch.object(
            keyword_supply.feishu, "get_record", new=AsyncMock(return_value=product),
        ), patch.object(
            keyword_supply.feishu, "fetch_all_records", new=AsyncMock(side_effect=fetch_rows),
        ), patch.object(
            keyword_supply.feishu, "create_record", new=AsyncMock(side_effect=create_row),
        ):
            results = asyncio.run(exercise())

        self.assertEqual(4, len(crawler_rows))
        self.assertEqual(4, sum(item["writes"] for item in results))
        self.assertIn("pilot_already_created", {item.get("skipped") for item in results})

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

    def test_real_running_status_counts_as_active_campaign_supply(self):
        now_ms = int(keyword_supply.time.time() * 1000)
        rows = [{"fields": {
            "任务名": "[活动补池:campaign1] YT KOL - running probe",
            "关键词列表": "running probe", "任务状态": "2-运行中",
            "创建日期": now_ms, "筛选-语言": ["en"],
        }}]
        activity = {"fields": {"活动目标语言": ["en"]}}
        product = {"fields": {"产品英文名": "POWKONG Piranha Plant 2 Dock"}}

        with patch.object(
            keyword_supply.feishu, "fetch_all_records", new=AsyncMock(return_value=rows),
        ), patch.object(
            keyword_supply.deepseek, "chat_json",
            new=AsyncMock(return_value={"keywords": ["nintendo collector channel"]}),
        ), patch.object(
            keyword_supply.feishu, "create_record", new=AsyncMock(return_value="task1"),
        ):
            result = asyncio.run(keyword_supply.ensure_campaign_supply(
                campaign_id="campaign1", activity=activity, product=product,
                required_candidates=1, dry_run=True,
            ))

        self.assertEqual(1, result["active_pending_before"])
        self.assertEqual(2, result["would_create"])

    def test_piranha_low_yield_recent_tasks_enter_cooldown_without_lowering_filters(self):
        now_ms = int(keyword_supply.time.time() * 1000)
        rows = [{"fields": {
            "任务名": f"[活动补池:campaign1] YT KOL - low yield {index}",
            "关键词列表": f"low yield {index}", "任务状态": "3-已完成",
            "创建日期": now_ms - index * 60_000,
            "实际产出-新增": 0,
            "执行日志": "待入库: 0\n其中有邮箱: 0",
            "筛选-语言": ["en"],
        }} for index in range(12)]
        activity = {"fields": {"活动目标语言": ["en"]}}
        product = {"fields": {"产品英文名": "POWKONG Piranha Plant 2 Dock"}}

        with patch.object(
            keyword_supply.feishu, "fetch_all_records", new=AsyncMock(return_value=rows),
        ), patch.object(
            keyword_supply.deepseek, "chat_json", new=AsyncMock(),
        ) as generate, patch.object(
            keyword_supply.feishu, "create_record", new=AsyncMock(),
        ) as create_record:
            result = asyncio.run(keyword_supply.ensure_campaign_supply(
                campaign_id="campaign1", activity=activity, product=product,
                required_candidates=300, approved_candidates=1, dry_run=False,
            ))

        self.assertTrue(result["ok"])
        self.assertEqual("quality_cooldown", result["skipped"])
        self.assertEqual("cooldown", result["quality_gate"]["mode"])
        self.assertEqual(0, result["created"])
        self.assertFalse(result["quality_filters_lowered"])
        generate.assert_not_awaited()
        create_record.assert_not_awaited()

    def test_volume_priority_keeps_discovery_running_during_quality_cooldown(self):
        now_ms = int(keyword_supply.time.time() * 1000)
        rows = [{"fields": {
            "任务名": f"[活动补池:campaign1] YT KOL - low yield {index}",
            "关键词列表": f"low yield {index}", "任务状态": "3-已完成",
            "创建日期": now_ms - index * 60_000,
            "实际产出-新增": 0,
            "执行日志": "待入库: 0\n其中有邮箱: 0",
            "筛选-语言": ["en"],
        }} for index in range(12)]
        activity = {"fields": {"活动目标语言": ["en"]}}
        product = {"fields": {"产品英文名": "POWKONG Piranha Plant 2 Dock"}}

        with patch.object(
            keyword_supply.feishu, "fetch_all_records", new=AsyncMock(return_value=rows),
        ), patch.object(
            keyword_supply.deepseek, "chat_json", new=AsyncMock(return_value={"keywords": []}),
        ), patch.object(
            keyword_supply.feishu, "create_record", new=AsyncMock(),
        ):
            result = asyncio.run(keyword_supply.ensure_campaign_supply(
                campaign_id="campaign1", activity=activity, product=product,
                required_candidates=300, approved_candidates=1, dry_run=True,
                volume_priority=True,
            ))

        self.assertEqual("cooldown", result["quality_gate"]["mode"])
        self.assertTrue(result["volume_priority"])
        self.assertTrue(result["quality_cooldown_overridden"])
        self.assertEqual(6, result["target_tasks"])
        self.assertGreater(result["would_create"], 0)
        self.assertFalse(result["quality_filters_lowered"])

    def test_piranha_low_yield_after_cooldown_allows_only_one_probe_task(self):
        now_ms = int(keyword_supply.time.time() * 1000)
        old_ms = now_ms - keyword_supply.LOW_YIELD_PROBE_COOLDOWN_MS - 1
        rows = [{"fields": {
            "任务名": f"[活动补池:campaign1] YT KOL - exhausted {index}",
            "关键词列表": f"exhausted {index}", "任务状态": "3-已完成",
            "创建日期": old_ms - index * 60_000,
            "实际产出-新增": 0,
            "执行日志": "待入库: 0\n其中有邮箱: 0",
            "筛选-语言": ["en"],
        }} for index in range(12)]
        activity = {"fields": {"活动目标语言": ["en"]}}
        product = {"fields": {"产品英文名": "POWKONG Piranha Plant 2 Dock"}}

        with patch.object(
            keyword_supply.feishu, "fetch_all_records", new=AsyncMock(return_value=rows),
        ), patch.object(
            keyword_supply.deepseek, "chat_json", new=AsyncMock(return_value={
                "keywords": [{"language": "en", "keyword": "nintendo collector review"}],
            }),
        ), patch.object(
            keyword_supply.feishu, "create_record", new=AsyncMock(),
        ):
            result = asyncio.run(keyword_supply.ensure_campaign_supply(
                campaign_id="campaign1", activity=activity, product=product,
                required_candidates=300, approved_candidates=1, dry_run=True,
            ))

        self.assertEqual("slow_probe", result["quality_gate"]["mode"])
        self.assertEqual(1, result["target_tasks"])
        self.assertLessEqual(result["would_create"], 1)
        self.assertFalse(result["quality_filters_lowered"])

    def test_piranha_missing_email_signal_fails_closed_instead_of_expanding(self):
        now_ms = int(keyword_supply.time.time() * 1000)
        rows = [{"fields": {
            "任务名": f"[活动补池:campaign1] YT KOL - changed log {index}",
            "关键词列表": f"changed log {index}", "任务状态": "3-已完成",
            "创建日期": now_ms - index * 60_000,
            "实际产出-新增": 0,
            "执行日志": "crawler output format changed",
            "筛选-语言": ["en"],
        }} for index in range(12)]
        activity = {"fields": {"活动目标语言": ["en"]}}
        product = {"fields": {"产品英文名": "POWKONG Piranha Plant 2 Dock"}}

        with patch.object(
            keyword_supply.feishu, "fetch_all_records", new=AsyncMock(return_value=rows),
        ), patch.object(
            keyword_supply.feishu, "create_record", new=AsyncMock(),
        ) as create_record:
            result = asyncio.run(keyword_supply.ensure_campaign_supply(
                campaign_id="campaign1", activity=activity, product=product,
                required_candidates=300, approved_candidates=0, dry_run=False,
            ))

        self.assertEqual("quality_cooldown", result["skipped"])
        self.assertIn(
            "email_yield_signal_unavailable", result["quality_gate"]["reasons"],
        )
        self.assertEqual(0.0, result["quality_gate"]["email_signal_coverage"])
        create_record.assert_not_awaited()

    def test_piranha_partial_email_signal_coverage_also_fails_closed(self):
        now_ms = int(keyword_supply.time.time() * 1000)
        rows = [{"fields": {
            "任务名": f"[活动补池:campaign1] YT KOL - partial log {index}",
            "关键词列表": f"partial log {index}", "任务状态": "3-已完成",
            "创建日期": now_ms - index * 60_000,
            "执行日志": (
                "其中有邮箱: 3" if index < 6 else "new unstructured log"
            ),
            "筛选-语言": ["en"],
        }} for index in range(12)]
        activity = {"fields": {"活动目标语言": ["en"]}}
        product = {"fields": {"产品英文名": "POWKONG Piranha Plant 2 Dock"}}

        with patch.object(
            keyword_supply.feishu, "fetch_all_records", new=AsyncMock(return_value=rows),
        ), patch.object(
            keyword_supply.feishu, "create_record", new=AsyncMock(),
        ) as create_record:
            result = asyncio.run(keyword_supply.ensure_campaign_supply(
                campaign_id="campaign1", activity=activity, product=product,
                required_candidates=300, approved_candidates=50, dry_run=False,
            ))

        self.assertEqual("quality_cooldown", result["skipped"])
        self.assertEqual(0.5, result["quality_gate"]["email_signal_coverage"])
        self.assertIn(
            "email_yield_signal_unavailable", result["quality_gate"]["reasons"],
        )
        create_record.assert_not_awaited()

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

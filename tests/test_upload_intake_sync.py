import asyncio
from unittest.mock import AsyncMock

from app import upload_intake_sync as intake


def _source(record_id="src-1", *, urls="https://youtu.be/Video123", kol="Creator",
            email="creator@example.com", product="YS11-5-戴夫", brand="FUNLAB",
            created_time=1_000):
    return {
        "record_id": record_id,
        "created_time": created_time,
        "fields": {
            "上稿平台链接": urls,
            "KOL": kol,
            "邮箱": email,
            "产品": product,
            "品牌": brand,
        },
    }


def _kol(record_id="kol-1", *, name="Creator", email="creator@example.com"):
    return {"record_id": record_id, "fields": {"账号名": name, "邮箱": email}}


def _product(record_id="product-1", *, name="戴夫联名 Switch 2 手柄",
             sku="FF05A-04", archive="YS11-5-戴夫", brand="FUNLAB"):
    return {
        "record_id": record_id,
        "fields": {
            "产品名": name,
            "SKU": sku,
            "老库ERP SKU": sku,
            "素材归档名": archive,
            "品牌": brand,
        },
    }


def _work(record_id="work-1", *, source_id="src-1", url="https://www.youtube.com/watch?v=Video123",
          platform="YouTube", platform_id="Video123", archive_status="待下载",
          file_url="", filename=""):
    return {
        "record_id": record_id,
        "fields": {
            "来源记录ID": source_id,
            "作品链接": url,
            "发布平台": platform,
            "平台作品ID": platform_id,
            "归档状态": archive_status,
            "归档文件链接": file_url,
            "归档文件名": filename,
        },
    }


def _readback(record_id="work-new", source_id="src-1"):
    return {
        "record_id": record_id,
        "fields": {
            "作品名称": "YT-Creator-YS11-5-戴夫-Video123",
            "同源作品组": f"Creator｜YS11-5-戴夫｜{source_id.split('#', 1)[0]}",
            "来源记录ID": source_id,
            "作品链接": "https://www.youtube.com/watch?v=Video123",
            "发布平台": "YouTube",
            "关联KOL": [{"record_ids": ["kol-1"]}],
            "关联产品": [{"record_ids": ["product-1"]}],
            "品牌": "FUNLAB",
            "归档状态": "待下载",
            "内容类型": "长视频",
            "数据抓取状态": "待抓取",
            "素材可复用": False,
            "作品状态": "正常",
            "平台作品ID": "Video123",
            "迁移状态": "新流程",
            "允许自动归档": False,
        },
    }


def test_extracts_markdown_urls_and_classifies_content_without_tracking_queries():
    value = (
        "[YouTube](https://www.youtube.com/shorts/AbC_123?si=track)； "
        "https://www.instagram.com/p/Post987/?igsh=track；"
    )

    parsed = [intake.parse_public_url(url) for url in intake.extract_urls(value)]

    assert [(item.platform, item.platform_id, item.content_type) for item in parsed] == [
        ("YouTube", "AbC_123", "短视频"),
        ("Instagram", "Post987", "图文"),
    ]
    assert parsed[0].normalized_url == "https://www.youtube.com/shorts/AbC_123"
    assert parsed[1].normalized_url == "https://www.instagram.com/p/Post987"


def test_tiktok_normalization_keeps_the_creator_path_for_a_downloadable_url():
    parsed = intake.parse_public_url(
        "https://www.tiktok.com/@ninkevdo/video/7635113337541364999?is_from_webapp=1"
    )

    assert parsed.platform == "TikTok"
    assert parsed.platform_id == "7635113337541364999"
    assert parsed.normalized_url == (
        "https://www.tiktok.com/@ninkevdo/video/7635113337541364999"
    )


def test_tiktok_short_link_keeps_its_resolvable_host():
    parsed = intake.parse_public_url("https://vm.tiktok.com/ZMShortCode/?share=1")

    assert intake.is_public_work_url(parsed) is True
    assert parsed.normalized_url == "https://vm.tiktok.com/ZMShortCode"


def test_empty_youtu_be_url_does_not_raise():
    parsed = intake.parse_public_url("https://youtu.be/")

    assert parsed.platform == "YouTube"
    assert parsed.platform_id == ""


def test_profile_or_empty_platform_urls_never_create_work_rows():
    sources = [
        _source("src-empty", urls="https://youtu.be/"),
        _source("src-profile", urls="https://www.instagram.com/creator/"),
    ]

    result = intake.plan_sync(sources, [], [_kol()], [_product()])

    assert result["counts"]["create"] == 0
    assert result["counts"]["manual_review"] == 2
    assert all("格式无效" in "；".join(item["reasons"]) for item in result["items"])


def test_twitch_profile_amazon_live_home_and_empty_fb_watch_are_not_works():
    urls = [
        "https://www.twitch.tv/creator",
        "https://www.amazon.com/live",
        "https://fb.watch/",
    ]

    assert all(not intake.is_public_work_url(intake.parse_public_url(url)) for url in urls)


def test_single_source_plans_one_disabled_work_row_with_unique_relations():
    result = intake.plan_sync(
        [_source()],
        [],
        [_kol()],
        [_product()],
    )

    assert result["counts"] == {"create": 1, "existing": 0, "manual_review": 0, "ignored": 0}
    item = result["items"][0]
    assert item["action"] == "create"
    assert item["source_key"] == "src-1"
    fields = item["fields"]
    assert fields["关联KOL"] == ["kol-1"]
    assert fields["关联产品"] == ["product-1"]
    assert fields["允许自动归档"] is False
    assert fields["发布平台"] == "YouTube"
    assert fields["作品链接"] == {
        "link": "https://www.youtube.com/watch?v=Video123",
        "text": "https://www.youtube.com/watch?v=Video123",
    }
    assert fields["数据抓取状态"] == "待抓取"
    assert fields["归档状态"] == "待下载"
    assert fields["迁移状态"] == "新流程"


def test_existing_platform_id_prevents_duplicate_even_when_url_shape_changes():
    existing = _work(url="[video](https://www.youtube.com/watch?v=Video123)")
    source = _source(record_id="src-new", urls="https://youtu.be/Video123?feature=shared")

    result = intake.plan_sync([source], [existing], [_kol()], [_product()])

    assert result["counts"]["existing"] == 1
    assert result["items"][0]["existing_record_id"] == "work-1"


def test_existing_work_wins_before_relation_matching_so_history_is_not_sent_to_review():
    existing = _work(url="https://www.youtube.com/watch?v=Video123")
    source = _source(kol="[Unknown](https://youtube.com/@unknown)", email="", product="未建产品")

    result = intake.plan_sync([source], [existing], [], [])

    assert result["counts"]["existing"] == 1
    assert result["counts"]["manual_review"] == 0


def test_markdown_link_display_name_can_match_a_kol_when_email_is_empty():
    source = _source(kol="[Creator](https://youtube.com/@creator)", email="")

    result = intake.plan_sync([source], [], [_kol()], [_product()])

    assert result["counts"]["create"] == 1
    assert result["items"][0]["fields"]["关联KOL"] == ["kol-1"]


def test_youtube_community_post_is_not_sent_to_the_video_download_queue():
    parsed = intake.parse_public_url(
        "http://youtube.com/post/UgkxwBt2X_5Ysm3aOQO0La8_088gGvBD-l0Q?si=tracking"
    )

    assert parsed.platform == "YouTube"
    assert parsed.content_type == "帖子"
    assert parsed.normalized_url == "https://www.youtube.com/post/UgkxwBt2X_5Ysm3aOQO0La8_088gGvBD-l0Q"


def test_two_links_on_same_platform_require_review_instead_of_guessing_same_content():
    source = _source(
        urls=(
            "https://www.youtube.com/watch?v=Video123\n"
            "https://www.youtube.com/watch?v=Other456"
        )
    )

    result = intake.plan_sync([source], [], [_kol()], [_product()])

    assert result["counts"]["manual_review"] == 2
    assert all("同一平台含多个作品" in "；".join(item["reasons"]) for item in result["items"])


def test_multiple_distinct_platforms_share_one_source_group_but_keep_one_row_each():
    source = _source(
        urls=(
            "https://www.youtube.com/shorts/Video123\n"
            "https://www.tiktok.com/@creator/video/7635113337541364999"
        )
    )

    result = intake.plan_sync([source], [], [_kol()], [_product()])

    creates = [item for item in result["items"] if item["action"] == "create"]
    assert len(creates) == 2
    assert {item["source_key"] for item in creates} == {"src-1#YT", "src-1#TK"}
    assert len({item["fields"]["同源作品组"] for item in creates}) == 1


def test_new_platform_inherits_the_existing_source_group():
    existing = _work(source_id="src-1#YT")
    existing["fields"]["同源作品组"] = "historical-group"
    source = _source(urls=(
        "https://www.youtube.com/watch?v=Video123\n"
        "https://www.tiktok.com/@creator/video/7635113337541364999"
    ))

    result = intake.plan_sync([source], [existing], [_kol()], [_product()])

    new_item = next(item for item in result["items"] if item["action"] == "create")
    assert new_item["fields"]["同源作品组"] == "historical-group"


def test_same_work_in_two_source_rows_is_created_only_once_per_batch():
    sources = [
        _source("src-1", urls="https://youtu.be/Video123", created_time=1),
        _source("src-2", urls="https://www.youtube.com/watch?v=Video123", created_time=2),
    ]

    result = intake.plan_sync(sources, [], [_kol()], [_product()])

    assert result["counts"]["create"] == 1
    assert result["counts"]["manual_review"] == 1
    duplicate = next(item for item in result["items"] if item["action"] == "manual_review")
    assert "本批与另一来源记录重复作品" in duplicate["reasons"]


def test_missing_source_brand_never_matches_a_product():
    result = intake.plan_sync(
        [_source(brand="")], [], [_kol()], [_product()],
    )

    assert result["counts"]["create"] == 0
    assert "品牌缺失" in result["items"][0]["reasons"]


def test_ambiguous_product_or_kol_never_creates_a_work_row():
    result = intake.plan_sync(
        [_source(product="2代食人花")],
        [],
        [_kol(), _kol("kol-2")],
        [
            _product("product-1", name="YM24食人花-二代", sku="PK02-S2", archive="食人花2代", brand="POWKONG"),
            _product("product-2", name="YM24-食人花2代", sku="PK02-S3", archive="食人花2代", brand="POWKONG"),
        ],
    )

    assert result["counts"]["create"] == 0
    assert result["counts"]["manual_review"] == 1
    reasons = "；".join(result["items"][0]["reasons"])
    assert "KOL匹配不唯一" in reasons
    assert "产品未唯一匹配" in reasons


def test_created_time_cutoff_keeps_historical_backlog_out_of_scheduled_commit():
    result = intake.plan_sync(
        [_source(record_id="old", created_time=999), _source(record_id="new", created_time=1_001)],
        [],
        [_kol()],
        [_product()],
        created_not_before_ms=1_000,
    )

    by_source = {item["source_record_id"]: item for item in result["items"]}
    assert by_source["old"]["action"] == "ignored"
    assert "早于灰度起点" in by_source["old"]["reasons"]
    assert by_source["new"]["action"] == "create"


def test_source_backfill_waits_for_all_siblings_and_requires_one_direct_file():
    archived = _work(
        "work-yt", source_id="src-1#YT", archive_status="已归档",
        file_url="https://u1wpma3xuhr.feishu.cn/file/file-one",
        filename="YT-Creator-YS11-5-戴夫-01.mp4",
    )
    pending = _work(
        "work-tk", source_id="src-1#TK", platform="TikTok", platform_id="123",
        url="https://www.tiktok.com/@creator/video/123",
        archive_status="待下载",
    )

    source_fields = {"上稿平台链接": (
        "https://www.youtube.com/watch?v=Video123\n"
        "https://www.tiktok.com/@creator/video/123"
    )}
    waiting = intake.plan_source_backfill("src-1", source_fields, [archived, pending])
    ready = intake.plan_source_backfill(
        "src-1", source_fields, [archived, pending],
        overrides={
            "work-tk": {
                "归档状态": "已归档",
                "归档文件链接": "https://u1wpma3xuhr.feishu.cn/file/file-one",
                "归档文件名": "YT-Creator-YS11-5-戴夫-01.mp4",
            }
        },
    )

    assert waiting["ready"] is False
    assert waiting["reason"] == "source_has_pending_works"
    assert ready == {
        "ready": True,
        "source_record_id": "src-1",
        "fields": {
            "飞书云盘链接": {
                "link": "https://u1wpma3xuhr.feishu.cn/file/file-one",
                "text": "YT-Creator-YS11-5-戴夫-01.mp4",
            },
            "素材情况": "已下载",
        },
        "reason": "",
    }


def test_source_backfill_waits_when_an_intake_url_has_no_work_row_yet():
    archived = _work(
        "work-yt", source_id="src-1#YT", archive_status="已归档",
        file_url="https://u1wpma3xuhr.feishu.cn/file/file-one",
        filename="YT-Creator-YS11-5-戴夫-01.mp4",
    )
    source_fields = {"上稿平台链接": (
        "https://www.youtube.com/watch?v=Video123\n"
        "https://www.tiktok.com/@creator/video/123"
    )}

    result = intake.plan_source_backfill("src-1", source_fields, [archived])

    assert result["ready"] is False
    assert result["reason"] == "source_has_unrepresented_or_invalid_urls"


def test_source_backfill_requires_a_direct_file_on_every_archived_sibling():
    with_file = _work(
        "work-yt", source_id="src-1#YT", archive_status="已归档",
        file_url="https://u1wpma3xuhr.feishu.cn/file/file-one",
        filename="video.mp4",
    )
    missing_file = _work(
        "work-tk", source_id="src-1#TK", platform="TikTok", platform_id="123",
        url="https://www.tiktok.com/@creator/video/123", archive_status="已归档",
    )

    result = intake.plan_source_backfill(
        "src-1",
        {"上稿平台链接": (
            "https://www.youtube.com/watch?v=Video123\n"
            "https://www.tiktok.com/@creator/video/123"
        )},
        [with_file, missing_file],
    )

    assert result["ready"] is False
    assert result["reason"] == "source_archive_file_missing"


def test_unsupported_video_is_not_reported_as_graphic_content():
    twitch = _work(
        "work-twitch", source_id="src-1", platform="Twitch", platform_id="123",
        url="https://www.twitch.tv/videos/123", archive_status="无需下载",
    )
    twitch["fields"]["内容类型"] = "直播"

    result = intake.plan_source_backfill(
        "src-1", {"上稿平台链接": "https://www.twitch.tv/videos/123"}, [twitch],
    )

    assert result["ready"] is True
    assert result["fields"] == {"素材情况": "下载不了"}


def test_sync_commit_creates_only_selected_source_and_reads_it_back(monkeypatch):
    monkeypatch.setattr(intake.config, "T_UPLOAD_INTAKE", "source-table")
    monkeypatch.setattr(intake.config, "T_UPLOAD_WORK", "work-table")
    monkeypatch.setattr(intake.config, "T_KOL", "kol-table")
    monkeypatch.setattr(intake.config, "T_PRODUCT", "product-table")
    monkeypatch.setattr(
        intake.feishu,
        "fetch_all_records",
        AsyncMock(side_effect=[[_source()], [], [_kol()], [_product()], []]),
    )
    create = AsyncMock(return_value="work-new")
    update = AsyncMock(return_value={})
    monkeypatch.setattr(intake.feishu, "create_record", create)
    monkeypatch.setattr(intake.feishu, "update_record", update)
    monkeypatch.setattr(
        intake.feishu,
        "get_record",
        AsyncMock(return_value=_readback()),
    )

    result = asyncio.run(intake.sync(commit=True, source_record_id="src-1", max_creates=1))

    assert result["created"] == 1
    assert result["created_record_ids"] == ["work-new"]
    assert create.await_args.args[0] == "work-table"
    assert create.await_args.args[1]["允许自动归档"] is False
    assert create.await_args.args[1]["作品链接"] == {
        "link": "https://www.youtube.com/watch?v=Video123",
        "text": "https://www.youtube.com/watch?v=Video123",
    }
    assert all(call.args[0] == "work-table" for call in update.await_args_list)


def test_critical_readback_requires_brand_group_and_all_system_states():
    planned = intake.plan_sync([_source()], [], [_kol()], [_product()])["items"][0]["fields"]
    incomplete = _readback()
    incomplete["fields"].pop("品牌")

    assert intake._critical_readback_ok(incomplete, planned) is False


def test_sync_rechecks_the_work_table_immediately_before_create(monkeypatch):
    monkeypatch.setattr(intake.config, "T_UPLOAD_INTAKE", "source-table")
    monkeypatch.setattr(intake.config, "T_UPLOAD_WORK", "work-table")
    monkeypatch.setattr(intake.config, "T_KOL", "kol-table")
    monkeypatch.setattr(intake.config, "T_PRODUCT", "product-table")
    concurrent = _work("work-concurrent", source_id="another-source")
    monkeypatch.setattr(
        intake.feishu,
        "fetch_all_records",
        AsyncMock(side_effect=[[_source()], [], [_kol()], [_product()], [concurrent]]),
    )
    create = AsyncMock()
    monkeypatch.setattr(intake.feishu, "create_record", create)

    result = asyncio.run(intake.sync(commit=True, source_record_id="src-1", max_creates=1))

    create.assert_not_awaited()
    assert result["deduped_after_recheck"] == 1


def test_sync_recovers_a_row_when_create_lands_but_response_is_uncertain(monkeypatch):
    monkeypatch.setattr(intake.config, "T_UPLOAD_INTAKE", "source-table")
    monkeypatch.setattr(intake.config, "T_UPLOAD_WORK", "work-table")
    monkeypatch.setattr(intake.config, "T_KOL", "kol-table")
    monkeypatch.setattr(intake.config, "T_PRODUCT", "product-table")
    partial = _work("work-recovered", source_id="src-1")
    partial["fields"]["运营备注"] = intake.SYNC_NOTE
    fetch = AsyncMock(side_effect=[[_source()], [], [_kol()], [_product()], [], [partial]])
    monkeypatch.setattr(intake.feishu, "fetch_all_records", fetch)
    create = AsyncMock(side_effect=RuntimeError("response lost"))
    monkeypatch.setattr(intake.feishu, "create_record", create)
    monkeypatch.setattr(intake.feishu, "update_record", AsyncMock(return_value={}))
    monkeypatch.setattr(
        intake.feishu,
        "get_record",
        AsyncMock(return_value=_readback("work-recovered")),
    )

    result = asyncio.run(intake.sync(commit=True, source_record_id="src-1", max_creates=1))

    create.assert_awaited_once()
    assert result["created_record_ids"] == ["work-recovered"]


def test_sync_repairs_only_an_exact_row_owned_by_the_synchronizer(monkeypatch):
    monkeypatch.setattr(intake.config, "T_UPLOAD_INTAKE", "source-table")
    monkeypatch.setattr(intake.config, "T_UPLOAD_WORK", "work-table")
    monkeypatch.setattr(intake.config, "T_KOL", "kol-table")
    monkeypatch.setattr(intake.config, "T_PRODUCT", "product-table")
    partial = _work("work-partial", source_id="src-1")
    partial["fields"].update({
        "运营备注": intake.SYNC_NOTE,
        "作品链接": "",
        "关联KOL": [],
        "关联产品": [],
    })
    monkeypatch.setattr(
        intake.feishu,
        "fetch_all_records",
        AsyncMock(side_effect=[[_source()], [partial], [_kol()], [_product()]]),
    )
    create = AsyncMock()
    update = AsyncMock(return_value={})
    monkeypatch.setattr(intake.feishu, "create_record", create)
    monkeypatch.setattr(intake.feishu, "update_record", update)
    monkeypatch.setattr(
        intake.feishu,
        "get_record",
        AsyncMock(return_value=_readback("work-partial")),
    )

    result = asyncio.run(intake.sync(
        commit=True,
        source_record_id="",
        max_creates=1,
        created_not_before_ms=500,
        allow_batch=True,
    ))

    create.assert_not_awaited()
    assert result["repaired_record_ids"] == ["work-partial"]
    assert any("关联KOL" in call.args[2] for call in update.await_args_list)
    assert any("作品链接" in call.args[2] for call in update.await_args_list)


def test_sync_does_not_repair_or_clear_relations_when_source_matching_is_ambiguous(monkeypatch):
    monkeypatch.setattr(intake.config, "T_UPLOAD_INTAKE", "source-table")
    monkeypatch.setattr(intake.config, "T_UPLOAD_WORK", "work-table")
    monkeypatch.setattr(intake.config, "T_KOL", "kol-table")
    monkeypatch.setattr(intake.config, "T_PRODUCT", "product-table")
    partial = _work("work-partial", source_id="src-1")
    partial["fields"]["运营备注"] = intake.SYNC_NOTE
    ambiguous_source = _source(kol="Unknown", email="")
    monkeypatch.setattr(
        intake.feishu,
        "fetch_all_records",
        AsyncMock(side_effect=[[ambiguous_source], [partial], [], [_product()]]),
    )
    update = AsyncMock()
    monkeypatch.setattr(intake.feishu, "update_record", update)

    result = asyncio.run(intake.sync(
        commit=True,
        source_record_id="",
        max_creates=1,
        created_not_before_ms=500,
        allow_batch=True,
    ))

    assert result["repaired_record_ids"] == []
    update.assert_not_awaited()


def test_sync_does_not_roll_back_operator_approval_or_archive_lifecycle(monkeypatch):
    monkeypatch.setattr(intake.config, "T_UPLOAD_INTAKE", "source-table")
    monkeypatch.setattr(intake.config, "T_UPLOAD_WORK", "work-table")
    monkeypatch.setattr(intake.config, "T_KOL", "kol-table")
    monkeypatch.setattr(intake.config, "T_PRODUCT", "product-table")
    planned = intake.plan_sync([_source()], [], [_kol()], [_product()])["items"][0]["fields"]
    completed = _readback("work-completed")
    completed["fields"].update({
        "运营备注": intake.SYNC_NOTE,
        "允许自动归档": True,
        "归档状态": "已归档",
        "数据抓取状态": "已更新",
        "素材可复用": True,
    })
    monkeypatch.setattr(
        intake.feishu,
        "fetch_all_records",
        AsyncMock(side_effect=[[_source()], [completed], [_kol()], [_product()]]),
    )
    update = AsyncMock()
    monkeypatch.setattr(intake.feishu, "update_record", update)

    result = asyncio.run(intake.sync(
        commit=True,
        source_record_id="",
        max_creates=1,
        created_not_before_ms=500,
        allow_batch=True,
    ))

    assert intake._critical_readback_ok(completed, planned) is False
    assert intake._missing_initial_fields(completed, planned) == {}
    assert result["repaired_record_ids"] == []
    update.assert_not_awaited()


def test_plan_blocks_repair_when_source_key_and_url_identity_hit_different_rows():
    partial = _work("work-partial", source_id="src-1", url="", platform_id="")
    partial["fields"]["运营备注"] = intake.SYNC_NOTE
    duplicate = _work("work-duplicate", source_id="other-source")

    plan = intake.plan_sync([_source()], [partial, duplicate], [_kol()], [_product()])

    assert plan["items"][0]["action"] == "manual_review"
    assert "平台作品身份已命中多条作品行" in plan["items"][0]["reasons"]


def test_plan_blocks_repair_when_identity_already_has_multiple_rows_regardless_of_order():
    partial = _work("work-partial", source_id="src-1", url="")
    partial["fields"]["运营备注"] = intake.SYNC_NOTE
    duplicate = _work("work-duplicate", source_id="other-source")

    plan = intake.plan_sync([_source()], [partial, duplicate], [_kol()], [_product()])

    assert plan["items"][0]["action"] == "manual_review"
    assert "平台作品身份已命中多条作品行" in plan["items"][0]["reasons"]


def test_plan_blocks_empty_url_repair_when_existing_platform_identity_disagrees():
    partial = _work(
        "work-partial",
        source_id="src-1",
        url="",
        platform="TikTok",
        platform_id="Different123",
    )
    partial["fields"]["运营备注"] = intake.SYNC_NOTE

    plan = intake.plan_sync([_source()], [partial], [_kol()], [_product()])

    assert plan["items"][0]["action"] == "manual_review"
    assert "来源记录ID已有平台与当前链接不一致" in plan["items"][0]["reasons"]


def test_plan_deduplicates_short_url_even_when_existing_row_has_a_manual_platform_id():
    short_url = "https://vm.tiktok.com/ZMShortCode"
    existing = _work(
        "work-existing",
        source_id="other-source",
        url=short_url,
        platform="TikTok",
        platform_id="ResolvedPlatformId",
    )
    source = _source(urls=short_url)

    plan = intake.plan_sync([source], [existing], [_kol()], [_product()])

    assert plan["items"][0]["action"] == "existing"
    assert plan["items"][0]["existing_record_id"] == "work-existing"


def test_plan_follows_partial_platform_id_to_find_a_second_duplicate_row():
    short_url = "https://vm.tiktok.com/ZMShortCode"
    platform_id = "7635113337541364999"
    partial = _work(
        "work-partial",
        source_id="src-1",
        url="",
        platform="TikTok",
        platform_id=platform_id,
    )
    partial["fields"]["运营备注"] = intake.SYNC_NOTE
    duplicate = _work(
        "work-duplicate",
        source_id="other-source",
        url=f"https://www.tiktok.com/@creator/video/{platform_id}",
        platform="TikTok",
        platform_id=platform_id,
    )
    source = _source(urls=short_url)

    plan = intake.plan_sync([source], [partial, duplicate], [_kol()], [_product()])

    assert plan["items"][0]["action"] == "manual_review"
    assert "平台作品身份已命中多条作品行" in plan["items"][0]["reasons"]

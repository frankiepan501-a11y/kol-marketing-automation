import asyncio
from unittest.mock import AsyncMock

import pytest

from app import feishu


def test_batch_update_rejects_partial_success(monkeypatch):
    monkeypatch.setattr(feishu, "api", AsyncMock(return_value={
        "data": {"records": [{"record_id": "rec-1"}]},
    }))

    with pytest.raises(RuntimeError, match="requested=2 returned=1"):
        asyncio.run(feishu.batch_update_records("tbl-work", [
            {"record_id": "rec-1", "fields": {"播放量": 10}},
            {"record_id": "rec-2", "fields": {"播放量": 20}},
        ]))


def test_batch_update_rejects_wrong_record_id(monkeypatch):
    monkeypatch.setattr(feishu, "api", AsyncMock(return_value={
        "data": {"records": [
            {"record_id": "rec-1"},
            {"record_id": "rec-other"},
        ]},
    }))

    with pytest.raises(RuntimeError, match="incomplete"):
        asyncio.run(feishu.batch_update_records("tbl-work", [
            {"record_id": "rec-1", "fields": {"播放量": 10}},
            {"record_id": "rec-2", "fields": {"播放量": 20}},
        ]))


def test_batch_create_rejects_partial_success(monkeypatch):
    monkeypatch.setattr(feishu, "api", AsyncMock(return_value={
        "data": {"records": [{"record_id": "snap-1"}]},
    }))

    with pytest.raises(RuntimeError, match="requested=2 returned=1"):
        asyncio.run(feishu.batch_create_records("tbl-snapshot", [
            {"快照名称": "one"},
            {"快照名称": "two"},
        ]))


def test_batch_helpers_reject_business_error_codes(monkeypatch):
    monkeypatch.setattr(feishu, "api", AsyncMock(return_value={
        "code": 1254000,
        "msg": "bad request",
        "data": {"records": [{"record_id": "rec-1"}]},
    }))

    with pytest.raises(RuntimeError, match="code=1254000"):
        asyncio.run(feishu.batch_update_records("tbl-work", [
            {"record_id": "rec-1", "fields": {"播放量": 10}},
        ]))


def test_batch_helpers_accept_complete_responses(monkeypatch):
    api = AsyncMock(side_effect=[
        {"data": {"records": [{"record_id": "rec-2"}, {"record_id": "rec-1"}]}},
        {"data": {"records": [{"record_id": "snap-1"}, {"record_id": "snap-2"}]}},
    ])
    monkeypatch.setattr(feishu, "api", api)

    updated = asyncio.run(feishu.batch_update_records("tbl-work", [
        {"record_id": "rec-1", "fields": {"播放量": 10}},
        {"record_id": "rec-2", "fields": {"播放量": 20}},
    ]))
    created = asyncio.run(feishu.batch_create_records("tbl-snapshot", [
        {"快照名称": "one"},
        {"快照名称": "two"},
    ]))

    assert {item["record_id"] for item in updated} == {"rec-1", "rec-2"}
    assert len(created) == 2
    assert api.await_args_list[1].kwargs["retry_transient"] is False

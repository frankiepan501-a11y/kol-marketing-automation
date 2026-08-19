"""集中上稿活动参与记录和名单版本提交。"""

from __future__ import annotations

import asyncio
import json
import re
import time
from collections import defaultdict

from . import config, feishu, launch_candidate_preview, launch_evidence
from .feishu import ext


class ParticipantValidationError(ValueError):
    """名单、活动状态或候选资格不合法。"""


class ParticipantVersionConflict(RuntimeError):
    """排序版本或恢复批次不匹配。"""


class ParticipantRetryableError(RuntimeError):
    """写入失败但已经完整回滚，可以带失败批次重试。"""


class ParticipantManualReviewError(RuntimeError):
    """数据存在重复或回滚不完整，必须人工修复。"""


_LOCKS: dict[str, asyncio.Lock] = {}
_FIELDS = {
    "KOL": {
        "version": "KOL已锁定名单版本",
        "block": "KOL名单阻塞代码",
        "failed_batch": "KOL失败锁定批次ID",
        "pending": "KOL阻塞待处理记录",
        "link": "关联KOL",
    },
    "媒体人": {
        "version": "媒体人已锁定名单版本",
        "block": "媒体人名单阻塞代码",
        "failed_batch": "媒体人失败锁定批次ID",
        "pending": "媒体人阻塞待处理记录",
        "link": "关联媒体人",
    },
}
_ELIGIBLE_DECISIONS = {"eligible_new_cold", "reactivation_same_thread"}
_PRE_OUTREACH_STATES = {"锁定准备中", "已入围"}


def participant_key(campaign_id: str, product_family_id: str,
                    object_type: str, contact_id: str) -> str:
    return "|".join((campaign_id, product_family_id, object_type, contact_id))


def _link_ids(value) -> list[str]:
    return sorted(launch_evidence._ids(value))


def _contact_id(fields: dict, field_map: dict) -> str:
    linked = _link_ids(fields.get(field_map["link"]))
    return (linked[0] if linked else ext(fields.get("联系人记录ID"))).strip()


def _participant_unique_key(fields: dict) -> str:
    return (ext(fields.get("参与记录ID")) or ext(fields.get("活动参与唯一键"))).strip()


def _assert_readback(actual: dict, expected: dict) -> None:
    fields = actual.get("fields") or {}
    for name, value in expected.items():
        if isinstance(value, list):
            if _link_ids(fields.get(name)) != sorted(value):
                raise ParticipantManualReviewError(f"参与记录回读不一致: {name}")
        elif ext(fields.get(name)) != ext(value):
            raise ParticipantManualReviewError(f"参与记录回读不一致: {name}")


def _history(value) -> list[dict]:
    if not value:
        return []
    if isinstance(value, list):
        return [x for x in value if isinstance(x, dict)]
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        raise ParticipantValidationError("排序快照历史不是有效 JSON")
    if not isinstance(parsed, list) or not all(isinstance(x, dict) for x in parsed):
        raise ParticipantValidationError("排序快照历史必须是 JSON 数组")
    return parsed


def _snapshot(candidate: dict, ranking_version: str) -> dict:
    return {
        "ranking_version": ranking_version,
        "base_score": candidate.get("score"),
        "final_priority": candidate.get("final_priority"),
        "evidence_level": candidate.get("evidence_level", "无加分"),
        "ranking_group": candidate.get("evidence_level", "无加分"),
        "identity_paths": candidate.get("identity_paths") or [],
        "post_ids": candidate.get("matched_post_ids") or [],
        "evidence_posts": candidate.get("evidence_posts") or [],
        "duplicate_touch_decision": candidate.get("decision"),
        "selection_reason": _selection_reason(candidate),
        "saved_at": int(time.time() * 1000),
    }


def _selection_reason(candidate: dict) -> str:
    decision = candidate.get("decision") or ""
    level = candidate.get("evidence_level") or "无加分"
    paths = ",".join(candidate.get("identity_paths") or [])
    reason = f"重复触达预检={decision}；竞品证据等级={level}"
    if paths:
        reason += f"；身份匹配={paths}"
    return reason


def _ranking_fields(candidate: dict, ranking_version: str) -> dict:
    return {
        "进入方式": (
            "同线程激活" if candidate.get("decision") == "reactivation_same_thread"
            else "新开发"
        ),
        "基础评分快照": candidate.get("score") or 0,
        "竞品证据等级": candidate.get("evidence_level") or "无加分",
        "关联竞品帖子": candidate.get("matched_post_ids") or [],
        "最终优先级": candidate.get("final_priority") or candidate.get("score") or 0,
        "选择原因": _selection_reason(candidate),
        "排序版本": ranking_version,
    }


def _with_snapshot(fields: dict, candidate: dict, ranking_version: str) -> str:
    history = _history(fields.get("排序快照历史"))
    if len(history) >= 10:
        raise ParticipantValidationError("排序快照历史已达 10 个版本，需先人工归档")
    history.append(_snapshot(candidate, ranking_version))
    return json.dumps(history, ensure_ascii=False, separators=(",", ":"))


async def _set_block(activity: dict, field_map: dict, *, code: str,
                     failed_batch: str, pending_ids: list[str]) -> None:
    await feishu.update_record(config.T_LAUNCH_CAMPAIGN, activity["record_id"], {
        field_map["block"]: code,
        field_map["failed_batch"]: failed_batch,
        field_map["pending"]: ",".join(sorted(set(pending_ids))),
    })


def _validate_gate(fields: dict, field_map: dict, *, recovery_of_batch_id: str) -> None:
    if not bool(fields.get("名单锁定授权")):
        raise ParticipantValidationError("名单锁定授权未开启")
    mode = ext(fields.get("竞品证据模式"))
    status = ext(fields.get("竞品分析状态"))
    if mode not in launch_evidence.VALID_MODES:
        raise ParticipantValidationError("竞品证据模式未配置")
    if mode in {launch_evidence.MODE_NEW, launch_evidence.MODE_REUSE} and status != "已就绪":
        raise ParticipantValidationError("竞品证据尚未就绪")
    if mode == launch_evidence.MODE_NONE and status != "不适用":
        raise ParticipantValidationError("不使用竞品证据时状态必须为不适用")
    block = ext(fields.get(field_map["block"]))
    if not block:
        if recovery_of_batch_id:
            raise ParticipantVersionConflict("活动当前无可恢复失败批次")
        return
    if block == "LOCK_BATCH_RETRYABLE":
        failed = ext(fields.get(field_map["failed_batch"]))
        if not recovery_of_batch_id or recovery_of_batch_id != failed:
            raise ParticipantVersionConflict("recovery_of_batch_id 与失败批次不匹配")
        return
    raise ParticipantManualReviewError(f"该对象类型处于人工阻塞: {block}")


async def _participants(campaign_id: str, product_family_id: str,
                        object_type: str) -> list[dict]:
    return await feishu.search_records(config.T_LAUNCH_PARTICIPANT, [
        {"field_name": "活动ID", "operator": "is", "value": [campaign_id]},
        {"field_name": "产品家族ID", "operator": "is", "value": [product_family_id]},
        {"field_name": "对象类型", "operator": "is", "value": [object_type]},
    ])


async def lock_participants(
    *, campaign_id: str, product_family_id: str, object_type: str,
    contact_ids: list[str], expected_ranking_version: str, lock_batch_id: str,
    recovery_of_batch_id: str = "",
) -> dict:
    if not config.LAUNCH_PARTICIPATION_WRITE_ENABLED:
        raise ParticipantValidationError("LAUNCH_PARTICIPATION_WRITE_ENABLED 未开启")
    if not config.T_LAUNCH_PARTICIPANT:
        raise ParticipantValidationError("T_LAUNCH_PARTICIPANT 未配置")
    if object_type not in _FIELDS:
        raise ParticipantValidationError("object_type must be KOL or 媒体人")
    contacts = list(dict.fromkeys(contact_ids or []))
    if not contacts or len(contacts) > 100 or len(contacts) != len(contact_ids or []):
        raise ParticipantValidationError("contact_ids 必须为 1-100 个不重复联系人")
    if not re.fullmatch(r"[A-Za-z0-9._-]{6,80}", lock_batch_id or ""):
        raise ParticipantValidationError("lock_batch_id 格式不合法")

    lock = _LOCKS.setdefault(campaign_id, asyncio.Lock())
    async with lock:
        activity = await launch_evidence.get_activity(campaign_id)
        activity_fields = activity.get("fields") or {}
        if ext(activity_fields.get("产品主记录ID")) != product_family_id:
            raise ParticipantValidationError("product_family_id 与活动产品主记录不一致")
        if ext(activity_fields.get("证据排序版本")) != expected_ranking_version:
            raise ParticipantVersionConflict("证据排序版本已变化，请刷新名单")
        field_map = _FIELDS[object_type]
        _validate_gate(
            activity_fields, field_map, recovery_of_batch_id=recovery_of_batch_id,
        )

        preview = await launch_candidate_preview.preview_candidates(
            product_family_id, object_type=object_type, campaign_id=campaign_id,
            internal_full=True,
        )
        candidate_map = {x.get("contact_id", ""): x for x in preview.get("candidates") or []}
        invalid = [cid for cid in contacts if (
            cid not in candidate_map
            or candidate_map[cid].get("decision") not in _ELIGIBLE_DECISIONS
        )]
        if invalid:
            raise ParticipantValidationError(
                "名单包含当前不合格联系人: " + ",".join(invalid)
            )

        existing = list(await _participants(campaign_id, product_family_id, object_type))
        by_key: dict[str, list[dict]] = defaultdict(list)
        for record in existing:
            record_fields = record.get("fields") or {}
            contact_id = _contact_id(record_fields, field_map)
            key = _participant_unique_key(record_fields) or participant_key(
                campaign_id, product_family_id, object_type, contact_id,
            )
            by_key[key].append(record)
        duplicates = [key for key, rows in by_key.items() if key and len(rows) > 1]
        if duplicates:
            await _set_block(
                activity, field_map, code="DUPLICATE_PARTICIPANT_MANUAL",
                failed_batch=lock_batch_id,
                pending_ids=[r["record_id"] for key in duplicates for r in by_key[key]],
            )
            raise ParticipantManualReviewError("参与记录ID存在重复记录")

        by_contact = {
            _contact_id(row.get("fields") or {}, field_map): row
            for row in existing
        }
        old_version = ext(activity_fields.get(field_map["version"]))
        changed_existing: list[tuple[dict, dict]] = []
        created_ids: list[str] = []
        selected_record_ids: list[str] = []

        # 先把整批业务规则和快照容量校验完，再做第一笔写入。
        # 这样后续某条业务校验失败时，不会让前面的联系人提前换版本。
        prepared = []
        for contact_id in contacts:
            candidate = candidate_map[contact_id]
            record = by_contact.get(contact_id)
            if record:
                fields = record.get("fields") or {}
                if ext(fields.get("参与状态")) == "已取消" and ext(fields.get("取消原因代码")) != "锁定失败":
                    raise ParticipantValidationError(
                        f"已取消联系人不能自动恢复: {contact_id}"
                    )
                old = {
                    name: fields.get(name) for name in (
                        "参与记录ID", "参与状态", "名单版本", "锁定批次ID",
                        "取消原因代码", "排序快照历史", "进入方式", "基础评分快照",
                        "竞品证据等级", "关联竞品帖子", "最终优先级", "选择原因", "排序版本",
                    )
                }
                update = {
                    "参与记录ID": participant_key(
                        campaign_id, product_family_id, object_type, contact_id,
                    ),
                    "参与状态": "已入围", "名单版本": expected_ranking_version,
                    "锁定批次ID": lock_batch_id, "取消原因代码": "",
                    "排序快照历史": _with_snapshot(fields, candidate, expected_ranking_version),
                    **_ranking_fields(candidate, expected_ranking_version),
                }
                prepared.append(("update", contact_id, record, old, update))
            else:
                key = participant_key(campaign_id, product_family_id, object_type, contact_id)
                fields = {
                    "参与记录ID": key,
                    "活动ID": campaign_id,
                    "关联活动": [activity["record_id"]],
                    "产品家族ID": product_family_id,
                    "对象类型": object_type,
                    field_map["link"]: [contact_id],
                    **_ranking_fields(candidate, expected_ranking_version),
                    "参与状态": "已入围",
                    "名单版本": expected_ranking_version,
                    "锁定批次ID": lock_batch_id,
                    "取消原因代码": "",
                    "排序快照历史": _with_snapshot({}, candidate, expected_ranking_version),
                }
                prepared.append(("create", contact_id, None, None, fields))

        activity_commit_started = False
        commit_confirmed = False
        try:
            for action, contact_id, record, old, fields in prepared:
                if action == "update":
                    changed_existing.append((record, old))
                    await feishu.update_record(config.T_LAUNCH_PARTICIPANT, record["record_id"], fields)
                    _assert_readback(
                        await feishu.get_record(config.T_LAUNCH_PARTICIPANT, record["record_id"]),
                        fields,
                    )
                    selected_record_ids.append(record["record_id"])
                else:
                    record_id = await feishu.create_record(config.T_LAUNCH_PARTICIPANT, fields)
                    created_ids.append(record_id)
                    selected_record_ids.append(record_id)
                    _assert_readback(
                        await feishu.get_record(config.T_LAUNCH_PARTICIPANT, record_id),
                        fields,
                    )

            activity_commit_started = True
            await feishu.update_record(config.T_LAUNCH_CAMPAIGN, activity["record_id"], {
                field_map["version"]: expected_ranking_version,
                field_map["block"]: "",
                field_map["failed_batch"]: "",
                field_map["pending"]: "",
            })
            commit_confirmed = True
        except Exception as exc:
            if activity_commit_started:
                try:
                    refreshed = await launch_evidence.get_activity(campaign_id)
                    commit_confirmed = (
                        ext((refreshed.get("fields") or {}).get(field_map["version"]))
                        == expected_ranking_version
                    )
                except Exception:
                    commit_confirmed = False
            if commit_confirmed:
                pass
            else:
                rollback_failed = []
                for record, old in reversed(changed_existing):
                    try:
                        await feishu.update_record(
                            config.T_LAUNCH_PARTICIPANT, record["record_id"], old,
                        )
                    except Exception:
                        rollback_failed.append(record["record_id"])
                for record_id in created_ids:
                    try:
                        await feishu.update_record(config.T_LAUNCH_PARTICIPANT, record_id, {
                            "参与状态": "已取消", "取消原因代码": "锁定失败",
                        })
                    except Exception:
                        rollback_failed.append(record_id)
                if rollback_failed:
                    await _set_block(
                        activity, field_map, code="LOCK_BATCH_MANUAL_REVIEW",
                        failed_batch=lock_batch_id, pending_ids=rollback_failed,
                    )
                    raise ParticipantManualReviewError("名单写入失败且回滚不完整") from exc
                await _set_block(
                    activity, field_map, code="LOCK_BATCH_RETRYABLE",
                    failed_batch=lock_batch_id, pending_ids=[],
                )
                raise ParticipantRetryableError("名单写入失败，已完整回滚，可重试") from exc

        cancelled = []
        try:
            selected = set(contacts)
            for record in existing:
                fields = record.get("fields") or {}
                contact_id = _contact_id(fields, field_map)
                if (
                    contact_id not in selected
                    and ext(fields.get("名单版本")) == old_version
                    and ext(fields.get("参与状态")) in _PRE_OUTREACH_STATES
                ):
                    await feishu.update_record(config.T_LAUNCH_PARTICIPANT, record["record_id"], {
                        "参与状态": "已取消", "取消原因代码": "不再符合",
                    })
                    cancelled.append(record["record_id"])
        except Exception as exc:
            pending = [record.get("record_id", "") for record in existing if record.get("record_id")]
            await _set_block(
                activity, field_map, code="LOCK_BATCH_MANUAL_REVIEW",
                failed_batch=lock_batch_id, pending_ids=pending,
            )
            raise ParticipantManualReviewError("新名单已提交，但旧名单清理不完整") from exc

        return {
            "campaign_id": campaign_id,
            "object_type": object_type,
            "ranking_version": expected_ranking_version,
            "lock_batch_id": lock_batch_id,
            "created": len(created_ids),
            "reused": len(contacts) - len(created_ids),
            "cancelled": len(cancelled),
            "participant_record_ids": selected_record_ids,
        }

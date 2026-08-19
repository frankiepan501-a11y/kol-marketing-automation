"""集中上稿活动的竞品证据配置与状态转换。

模块只负责活动控制字段。候选排序和活动参与名单分别由其他模块处理。
"""

from __future__ import annotations

import asyncio
import time

from . import config, feishu
from .feishu import ext


MODE_NEW = "发起新分析"
MODE_REUSE = "引用历史证据"
MODE_NONE = "不使用竞品证据"
VALID_MODES = {MODE_NEW, MODE_REUSE, MODE_NONE}


class EvidenceValidationError(ValueError):
    """证据模式或字段组合不合法。"""


class EvidenceNotFoundError(LookupError):
    """活动、帖子或事件不存在。"""


class EvidenceVersionConflict(RuntimeError):
    """调用方使用了旧配置版本。"""


_ACTIVITY_LOCKS: dict[str, asyncio.Lock] = {}


def _ids(value) -> list[str]:
    if isinstance(value, dict):
        return list(value.get("link_record_ids") or value.get("record_ids") or [])
    if isinstance(value, list):
        out = []
        for item in value:
            if isinstance(item, str):
                out.append(item)
            elif isinstance(item, dict):
                out.extend(item.get("link_record_ids") or item.get("record_ids") or [])
        return out
    return []


def _record_brand(fields: dict) -> str:
    return (ext(fields.get("竞品品牌")) or ext(fields.get("品牌")) or ext(fields.get("竞品"))).strip()


async def get_activity(campaign_id: str) -> dict:
    rows = await feishu.search_records(config.T_LAUNCH_CAMPAIGN, [{
        "field_name": "活动ID", "operator": "is", "value": [campaign_id],
    }])
    if not rows:
        raise EvidenceNotFoundError(f"活动不存在: {campaign_id}")
    if len(rows) != 1:
        raise EvidenceValidationError(f"活动ID不唯一: {campaign_id}")
    return rows[0]


async def _validate_linked_records(
    *, competitor_brand: str, post_record_ids: list[str], event_record_ids: list[str]
) -> tuple[list[dict], list[dict]]:
    posts = []
    for record_id in post_record_ids:
        try:
            record = await feishu.get_record(config.T_COMPETITOR_POST, record_id)
        except Exception as exc:
            raise EvidenceNotFoundError(f"竞品帖子不存在: {record_id}") from exc
        fields = record.get("fields") or {}
        if _record_brand(fields).upper() != competitor_brand.upper():
            raise EvidenceValidationError(f"竞品帖子品牌不匹配: {record_id}")
        if ext(fields.get("人工复核状态")) != "已确认" or ext(fields.get("相关性")) != "相关":
            raise EvidenceValidationError(f"竞品帖子尚未确认或不相关: {record_id}")
        posts.append(record)

    events = []
    for record_id in event_record_ids:
        try:
            record = await feishu.get_record(config.T_COMPETITOR_EVENT, record_id)
        except Exception as exc:
            raise EvidenceNotFoundError(f"竞品营销事件不存在: {record_id}") from exc
        fields = record.get("fields") or {}
        if _record_brand(fields).upper() != competitor_brand.upper():
            raise EvidenceValidationError(f"竞品营销事件品牌不匹配: {record_id}")
        event_review_status = (
            ext(fields.get("人工确认状态")) or ext(fields.get("人工复核状态"))
        )
        if event_review_status != "已确认":
            raise EvidenceValidationError(f"竞品营销事件尚未确认: {record_id}")
        events.append(record)
    return posts, events


def build_config_target(
    *,
    mode: str,
    competitor_brand: str = "",
    post_record_ids: list[str] | None = None,
    event_record_ids: list[str] | None = None,
    change_reason: str = "",
) -> dict:
    """把一种证据选择标准化成需要一次性写入活动表的完整字段组合。"""
    if mode not in VALID_MODES:
        raise EvidenceValidationError(f"未知竞品证据模式: {mode}")

    posts = list(dict.fromkeys(post_record_ids or []))
    events = list(dict.fromkeys(event_record_ids or []))
    brand = (competitor_brand or "").strip()

    if mode == MODE_NONE:
        return {
            "竞品证据模式": MODE_NONE,
            "竞品分析状态": "不适用",
            "竞品品牌": "",
            "关联竞品帖子": [],
            "关联竞品营销事件": [],
            "证据等待/变更说明": (change_reason or "").strip(),
        }

    if not brand:
        raise EvidenceValidationError("竞品品牌必填")
    if mode == MODE_NEW:
        if posts or events:
            raise EvidenceValidationError("发起新分析时不能预填帖子或事件")
        status = "待分析"
    else:
        if not posts:
            raise EvidenceValidationError("引用历史证据至少需要一条竞品帖子")
        status = "已就绪"

    return {
        "竞品证据模式": mode,
        "竞品分析状态": status,
        "竞品品牌": brand,
        "关联竞品帖子": posts,
        "关联竞品营销事件": events,
        "证据快照时间": None,
        "证据排序版本": "",
        "证据等待/变更说明": (change_reason or "").strip(),
    }


async def configure_evidence(
    *,
    campaign_id: str,
    mode: str,
    competitor_brand: str = "",
    post_record_ids: list[str] | None = None,
    event_record_ids: list[str] | None = None,
    change_reason: str = "",
    expected_config_version: int,
) -> dict:
    """配置一种证据模式；活动记录更新是唯一提交点。"""
    if not config.LAUNCH_EVIDENCE_ENABLED:
        raise EvidenceValidationError("LAUNCH_EVIDENCE_ENABLED 未开启")
    lock = _ACTIVITY_LOCKS.setdefault(campaign_id, asyncio.Lock())
    async with lock:
        activity = await get_activity(campaign_id)
        current = int((activity.get("fields") or {}).get("证据配置版本") or 0)
        if current != int(expected_config_version):
            raise EvidenceVersionConflict(f"证据配置版本冲突: current={current}")

        target = build_config_target(
            mode=mode,
            competitor_brand=competitor_brand,
            post_record_ids=post_record_ids,
            event_record_ids=event_record_ids,
            change_reason=change_reason,
        )
        previous_mode = ext((activity.get("fields") or {}).get("竞品证据模式"))
        if (
            mode == MODE_NONE and previous_mode in {MODE_NEW, MODE_REUSE}
            and not (change_reason or "").strip()
        ):
            raise EvidenceValidationError("切换为不使用竞品证据时必须填写变更说明")
        if mode == MODE_REUSE:
            await _validate_linked_records(
                competitor_brand=target["竞品品牌"],
                post_record_ids=target["关联竞品帖子"],
                event_record_ids=target["关联竞品营销事件"],
            )

        research_node = None
        if mode == MODE_NEW:
            research_node = await _ensure_research_node(
                campaign_id=campaign_id, competitor_brand=target["竞品品牌"],
            )

        new_version = current + 1
        target["证据配置版本"] = new_version
        if mode in {MODE_REUSE, MODE_NONE}:
            target["证据快照时间"] = int(time.time() * 1000)
            prefix = "evidence" if mode == MODE_REUSE else "base"
            target["证据排序版本"] = f"{prefix}-v{new_version}"
        try:
            await feishu.update_record(
                config.T_LAUNCH_CAMPAIGN, activity["record_id"], target,
            )
        except Exception:
            if research_node:
                await feishu.update_record(config.T_LAUNCH_NODE, research_node["record_id"], {
                    "节点状态": "已阻塞",
                    "节点阻塞说明": "活动配置提交失败；本次调查节点不生效",
                })
            raise
        return {
            "campaign_id": campaign_id,
            "activity_record_id": activity["record_id"],
            "mode": mode,
            "status": target["竞品分析状态"],
            "config_version": new_version,
            "ranking_version": target.get("证据排序版本", ""),
        }


async def _transition_simple(
    *, campaign_id: str, expected_config_version: int,
    expected_status: str, target_status: str,
) -> dict:
    if not config.LAUNCH_EVIDENCE_ENABLED:
        raise EvidenceValidationError("LAUNCH_EVIDENCE_ENABLED 未开启")
    lock = _ACTIVITY_LOCKS.setdefault(campaign_id, asyncio.Lock())
    async with lock:
        activity = await get_activity(campaign_id)
        fields = activity.get("fields") or {}
        current = int(fields.get("证据配置版本") or 0)
        if current != int(expected_config_version):
            raise EvidenceVersionConflict(f"证据配置版本冲突: current={current}")
        if ext(fields.get("竞品证据模式")) != MODE_NEW:
            raise EvidenceValidationError("只有发起新分析模式允许该状态转换")
        status = ext(fields.get("竞品分析状态"))
        if status != expected_status:
            raise EvidenceValidationError(
                f"非法状态转换: {status or '空'} -> {target_status}"
            )
        new_version = current + 1
        await feishu.update_record(config.T_LAUNCH_CAMPAIGN, activity["record_id"], {
            "竞品分析状态": target_status,
            "证据配置版本": new_version,
        })
        return {
            "campaign_id": campaign_id,
            "activity_record_id": activity["record_id"],
            "status": target_status,
            "config_version": new_version,
        }


async def start_analysis(*, campaign_id: str, expected_config_version: int) -> dict:
    return await _transition_simple(
        campaign_id=campaign_id,
        expected_config_version=expected_config_version,
        expected_status="待分析",
        target_status="分析中",
    )


async def _find_research_nodes(campaign_id: str) -> list[dict]:
    return await feishu.search_records(config.T_LAUNCH_NODE, [
        {"field_name": "活动ID", "operator": "is", "value": [campaign_id]},
        {"field_name": "节点代码", "operator": "is", "value": ["competitor_research"]},
    ])


async def _ensure_research_node(*, campaign_id: str, competitor_brand: str) -> dict:
    rows = await _find_research_nodes(campaign_id)
    if len(rows) > 1:
        raise EvidenceValidationError(f"competitor_research 节点不唯一: {campaign_id}")
    if rows:
        return rows[0]
    record_id = await feishu.create_record(config.T_LAUNCH_NODE, {
        "节点代码": "competitor_research",
        "活动ID": campaign_id,
        "节点名称": "竞品合作证据调查",
        "节点状态": "待执行",
        "竞品品牌": competitor_brand,
    })
    return {"record_id": record_id, "fields": {
        "节点代码": "competitor_research", "活动ID": campaign_id,
    }}


async def _get_research_node(campaign_id: str) -> dict:
    rows = await _find_research_nodes(campaign_id)
    if not rows:
        raise EvidenceNotFoundError(f"活动缺少 competitor_research 节点: {campaign_id}")
    if len(rows) != 1:
        raise EvidenceValidationError(f"competitor_research 节点不唯一: {campaign_id}")
    return rows[0]


async def _validate_candidate_records(
    *, competitor_brand: str, post_record_ids: list[str], event_record_ids: list[str]
) -> None:
    for table_id, label, record_ids in (
        (config.T_COMPETITOR_POST, "竞品帖子", post_record_ids),
        (config.T_COMPETITOR_EVENT, "竞品营销事件", event_record_ids),
    ):
        for record_id in record_ids:
            try:
                record = await feishu.get_record(table_id, record_id)
            except Exception as exc:
                raise EvidenceNotFoundError(f"{label}不存在: {record_id}") from exc
            if _record_brand(record.get("fields") or {}).upper() != competitor_brand.upper():
                raise EvidenceValidationError(f"{label}品牌不匹配: {record_id}")


def _require_new_analysis_state(
    fields: dict, *, expected_config_version: int, expected_status: str,
) -> int:
    current = int(fields.get("证据配置版本") or 0)
    if current != int(expected_config_version):
        raise EvidenceVersionConflict(f"证据配置版本冲突: current={current}")
    if ext(fields.get("竞品证据模式")) != MODE_NEW:
        raise EvidenceValidationError("只有发起新分析模式允许该状态转换")
    status = ext(fields.get("竞品分析状态"))
    if status != expected_status:
        raise EvidenceValidationError(f"非法状态转换: {status or '空'}")
    return current


async def submit_analysis(
    *, campaign_id: str, candidate_post_ids: list[str],
    candidate_event_ids: list[str] | None = None, submission_note: str = "",
    expected_config_version: int,
) -> dict:
    if not config.LAUNCH_EVIDENCE_ENABLED:
        raise EvidenceValidationError("LAUNCH_EVIDENCE_ENABLED 未开启")
    posts = list(dict.fromkeys(candidate_post_ids or []))
    events = list(dict.fromkeys(candidate_event_ids or []))
    if not posts:
        raise EvidenceValidationError("调查提交至少需要一条候选帖子")
    lock = _ACTIVITY_LOCKS.setdefault(campaign_id, asyncio.Lock())
    async with lock:
        activity = await get_activity(campaign_id)
        fields = activity.get("fields") or {}
        current = _require_new_analysis_state(
            fields, expected_config_version=expected_config_version,
            expected_status="分析中",
        )
        brand = ext(fields.get("竞品品牌")).strip()
        await _validate_candidate_records(
            competitor_brand=brand, post_record_ids=posts, event_record_ids=events,
        )
        node = await _get_research_node(campaign_id)
        new_version = current + 1
        node_fields = {
            "待确认竞品帖子": posts,
            "待确认竞品事件": events,
            "调查提交版本": new_version,
            "调查提交时间": int(time.time() * 1000),
            "调查提交说明": (submission_note or "").strip(),
            "节点状态": "待活动提交",
        }
        await feishu.update_record(config.T_LAUNCH_NODE, node["record_id"], node_fields)
        try:
            await feishu.update_record(config.T_LAUNCH_CAMPAIGN, activity["record_id"], {
                "竞品分析状态": "待人工确认", "证据配置版本": new_version,
            })
        except Exception:
            await feishu.update_record(config.T_LAUNCH_NODE, node["record_id"], {
                "节点状态": "已阻塞",
                "节点阻塞说明": "活动版本提交失败；本次候选不生效",
            })
            raise
        await feishu.update_record(config.T_LAUNCH_NODE, node["record_id"], {
            "节点状态": "待人工确认",
        })
        return {
            "campaign_id": campaign_id, "status": "待人工确认",
            "config_version": new_version, "candidate_posts": posts,
            "candidate_events": events,
        }


async def confirm_analysis(
    *, campaign_id: str, confirmed_post_ids: list[str],
    confirmed_event_ids: list[str] | None = None,
    expected_config_version: int,
) -> dict:
    if not config.LAUNCH_EVIDENCE_ENABLED:
        raise EvidenceValidationError("LAUNCH_EVIDENCE_ENABLED 未开启")
    posts = list(dict.fromkeys(confirmed_post_ids or []))
    events = list(dict.fromkeys(confirmed_event_ids or []))
    if not posts:
        raise EvidenceValidationError("人工确认至少需要一条帖子")
    lock = _ACTIVITY_LOCKS.setdefault(campaign_id, asyncio.Lock())
    async with lock:
        activity = await get_activity(campaign_id)
        fields = activity.get("fields") or {}
        current = _require_new_analysis_state(
            fields, expected_config_version=expected_config_version,
            expected_status="待人工确认",
        )
        node = await _get_research_node(campaign_id)
        node_fields = node.get("fields") or {}
        if int(node_fields.get("调查提交版本") or -1) != current:
            raise EvidenceVersionConflict("调查节点提交版本不是活动当前版本")
        candidate_posts = set(_ids(node_fields.get("待确认竞品帖子")))
        candidate_events = set(_ids(node_fields.get("待确认竞品事件")))
        if not set(posts).issubset(candidate_posts) or not set(events).issubset(candidate_events):
            raise EvidenceValidationError("只能确认当前调查提交集合的子集")
        brand = ext(fields.get("竞品品牌")).strip()
        await _validate_linked_records(
            competitor_brand=brand, post_record_ids=posts, event_record_ids=events,
        )
        new_version = current + 1
        ranking_version = f"evidence-v{new_version}"
        await feishu.update_record(config.T_LAUNCH_NODE, node["record_id"], {
            "节点状态": "待活动提交", "目标证据配置版本": new_version,
        })
        try:
            await feishu.update_record(config.T_LAUNCH_CAMPAIGN, activity["record_id"], {
                "竞品分析状态": "已就绪",
                "关联竞品帖子": posts,
                "关联竞品营销事件": events,
                "证据快照时间": int(time.time() * 1000),
                "证据排序版本": ranking_version,
                "证据配置版本": new_version,
            })
        except Exception:
            await feishu.update_record(config.T_LAUNCH_NODE, node["record_id"], {
                "节点状态": "已阻塞",
                "节点阻塞说明": "活动确认版本提交失败；本次确认不生效",
            })
            raise
        await feishu.update_record(config.T_LAUNCH_NODE, node["record_id"], {
            "节点状态": "已确认",
        })
        return {
            "campaign_id": campaign_id, "status": "已就绪",
            "config_version": new_version, "ranking_version": ranking_version,
            "confirmed_posts": posts, "confirmed_events": events,
        }


async def retry_analysis(*, campaign_id: str, expected_config_version: int) -> dict:
    return await _transition_simple(
        campaign_id=campaign_id,
        expected_config_version=expected_config_version,
        expected_status="失败",
        target_status="待分析",
    )

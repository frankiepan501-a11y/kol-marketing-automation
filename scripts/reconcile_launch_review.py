"""审核后整理活动名单：只保留新开发池，历史关系分池留档并补位。

默认 dry-run。只有显式 --commit 才会短暂打开活动名单锁，且 finally 中关闭。
本脚本不创建任务、草稿、卡片、邮件、寄样或付款记录。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app import config, launch_candidate_preview, launch_evidence, launch_participation
from app.feishu import ext


def _contact_id(fields: dict) -> str:
    ids = launch_evidence._ids(fields.get("关联KOL"))
    return (sorted(ids)[0] if ids else ext(fields.get("联系人记录ID"))).strip()


async def _set_activity_gate(activity: dict, enabled: bool, *, ranking_version: str = "") -> None:
    fields = {"名单锁定授权": bool(enabled)}
    if ranking_version:
        fields["证据排序版本"] = ranking_version
    await launch_participation._update_and_confirm(
        config.T_LAUNCH_CAMPAIGN, activity["record_id"],
        fields,
    )


async def _close_activity_gate(activity: dict, *, ranking_version: str) -> None:
    last_error = None
    for delay in (0, 2, 5):
        if delay:
            await asyncio.sleep(delay)
        try:
            await _set_activity_gate(activity, False, ranking_version=ranking_version)
            return
        except Exception as exc:  # closing the production gate is worth bounded retries
            last_error = exc
    raise RuntimeError("活动名单锁未能自动关闭") from last_error


async def run(args) -> dict:
    activity = await launch_evidence.get_activity(args.campaign_id)
    participants = await launch_participation._participants(
        args.campaign_id, args.product_id, "KOL",
    )
    preview = await launch_candidate_preview.preview_candidates(
        campaign_id=args.campaign_id, object_type="KOL", limit=500, internal_full=True,
    )
    plan = launch_participation.plan_review_backfill(
        participants, preview["candidates"],
        field_map={"link": "关联KOL"}, target_count=args.target_count,
    )
    candidate_by_id = {x.get("contact_id", ""): x for x in preview["candidates"]}
    result = {
        "mode": "commit" if args.commit else "dry_run",
        "campaign_id": args.campaign_id,
        "ranking_version": args.ranking_version,
        "target_count": args.target_count,
        "preview_summary": preview["summary"],
        **plan,
        "selected": [
            {
                "contact_id": cid,
                "name": (candidate_by_id.get(cid) or {}).get("name", ""),
                "country": (candidate_by_id.get(cid) or {}).get("country", ""),
                "language": (candidate_by_id.get(cid) or {}).get("language", ""),
                "review_decision": (candidate_by_id.get(cid) or {}).get("review_decision", ""),
            }
            for cid in plan["selected_contact_ids"]
        ],
        "writes": 0,
    }
    if plan["shortfall_count"]:
        raise RuntimeError(f"合规新开发候选不足，还缺 {plan['shortfall_count']} 人")
    if not args.commit:
        return result

    config.LAUNCH_PARTICIPATION_WRITE_ENABLED = True
    gate_open = False
    lock_succeeded = False
    old_ranking_version = ext((activity.get("fields") or {}).get("证据排序版本"))
    try:
        await _set_activity_gate(activity, True, ranking_version=args.ranking_version)
        gate_open = True
        lock_result = await launch_participation.lock_participants(
            campaign_id=args.campaign_id,
            product_family_id=args.product_id,
            object_type="KOL",
            contact_ids=plan["selected_contact_ids"],
            expected_ranking_version=args.ranking_version,
            lock_batch_id=args.lock_batch_id,
            _preview_result=preview,
        )
        lock_succeeded = True

        pool_by_contact = {}
        for cid in plan["human_excluded_contact_ids"]:
            pool_by_contact[cid] = ("已排除", "运营取消")
        for cid in plan["existing_pipeline_contact_ids"]:
            pool_by_contact[cid] = ("现有流程贡献池", "不再符合")
        for cid in plan["republish_contact_ids"]:
            pool_by_contact[cid] = ("二次发布池", "不再符合")
        for cid in plan["other_outflow_contact_ids"]:
            pool_by_contact[cid] = ("已排除", "不再符合")

        annotated = 0
        for row in participants:
            cid = _contact_id(row.get("fields") or {})
            if cid not in pool_by_contact:
                continue
            pool, cancel_code = pool_by_contact[cid]
            await launch_participation._update_and_confirm(
                config.T_LAUNCH_PARTICIPANT, row["record_id"],
                {"活动分池": pool, "取消原因代码": cancel_code},
            )
            annotated += 1
        result.update({
            "writes": len(plan["selected_contact_ids"]) + annotated + 2,
            "lock_result": lock_result,
            "annotated_outflows": annotated,
        })
    finally:
        try:
            if gate_open:
                await _close_activity_gate(
                    activity,
                    ranking_version=(
                        args.ranking_version if lock_succeeded else old_ranking_version
                    ),
                )
        finally:
            config.LAUNCH_PARTICIPATION_WRITE_ENABLED = False

    current = await launch_participation._participants(
        args.campaign_id, args.product_id, "KOL",
    )
    active = [
        row for row in current
        if ext((row.get("fields") or {}).get("参与状态")) == "已入围"
    ]
    bad_active = [
        row.get("record_id", "") for row in active
        if ext((row.get("fields") or {}).get("活动分池")) != "新开发池"
        or ext((row.get("fields") or {}).get("进入方式")) != "新开发"
    ]
    external_links = sum(
        bool(launch_evidence._ids((row.get("fields") or {}).get(name)))
        for row in active
        for name in ("关联KOL任务", "关联媒体人任务", "关联邮件草稿")
    )
    refreshed_activity = await launch_evidence.get_activity(args.campaign_id)
    auth_fields = refreshed_activity.get("fields") or {}
    verification = {
        "active_count": len(active),
        "bad_active_record_ids": bad_active,
        "linked_tasks_or_drafts": external_links,
        "list_lock_authorized": bool(auth_fields.get("名单锁定授权")),
        "email_authorized": bool(auth_fields.get("发送邮件授权")),
        "sample_authorized": bool(auth_fields.get("样品寄送授权")),
        "payment_authorized": bool(auth_fields.get("付费承诺授权")),
        "reserve_authorized": bool(auth_fields.get("储备金释放授权")),
    }
    result["verification"] = verification
    if (
        len(active) != args.target_count or bad_active or external_links
        or any(verification[name] for name in (
            "list_lock_authorized", "email_authorized", "sample_authorized",
            "payment_authorized", "reserve_authorized",
        ))
    ):
        raise RuntimeError("写后安全核对失败: " + json.dumps(verification, ensure_ascii=False))
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--campaign-id", required=True)
    parser.add_argument("--product-id", required=True)
    parser.add_argument("--target-count", type=int, default=20)
    parser.add_argument("--ranking-version", default="evidence-v3")
    parser.add_argument("--lock-batch-id", default="p0-three-pool-v3-20260819a")
    parser.add_argument("--commit", action="store_true")
    args = parser.parse_args()
    print(json.dumps(asyncio.run(run(args)), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

"""Dave竞品证据作者受控导入。

只创建KOL主表与“待审核”活动参与记录。它不创建草稿，也不调用发送器；
统一发送中心只有在运营把参与记录改为“通过”后才可能继续处理。
"""
from __future__ import annotations

import asyncio
import time
from urllib.parse import urlparse

from . import (
    config, feishu, launch_candidate_preview as preview, launch_evidence,
    launch_participation,
)
from .feishu import ext


DAVE_CAMPAIGN_ID = preview.DAVE_EVIDENCE_AUTHOR_PILOT_CAMPAIGN_ID
DAVE_VERIFIED_SOURCE_JOB_ID = "launchruntime-c84194113d49"
DAVE_EXPECTED_HANDLES = {"mekelkasanova", "itsdadmode", "professorshario"}
DAVE_LOCKED_AUTHORS = {
    "mekelkasanova": "youtube|handle:mekelkasanova",
    "itsdadmode": "youtube|handle:itsdadmode",
    "professorshario": "youtube|handle:professorshario",
}
IMPORT_VERSION = "dave-evidence-author-controlled-v1"
_IMPORT_LOCKS: dict[str, asyncio.Lock] = {}


class ControlledImportError(RuntimeError):
    pass


def _handle(candidate: dict) -> str:
    return str(candidate.get("handle") or "").strip().lstrip("@").casefold()


def validate_locked_seed(candidate: dict) -> str:
    """锁定P0样本身份；拒绝用同一handle夹带另一频道ID或主页。"""
    handle = _handle(candidate)
    expected_key = DAVE_LOCKED_AUTHORS.get(handle)
    platform = str(candidate.get("platform") or "").strip().casefold()
    author_key = str(candidate.get("author_key") or "").strip().casefold()
    creator_id = str(candidate.get("creator_id") or "").strip()
    profile_url = str(candidate.get("profile_url") or "").strip()
    parsed_url = urlparse(profile_url)
    host = parsed_url.netloc.casefold().removeprefix("www.")
    path_handle = parsed_url.path.rstrip("/").split("/")[-1].lstrip("@").casefold()
    if (
        not expected_key or platform != "youtube" or author_key != expected_key
        or creator_id or host != "youtube.com" or path_handle != handle
    ):
        raise ControlledImportError(f"{handle or 'unknown'}不属于锁定的P0身份快照")
    return handle


def controlled_marker(campaign_id: str, author_key: str) -> str:
    return (
        f"[CONTROLLED_IMPORT] campaign={campaign_id}; author_key={author_key}; "
        f"version={IMPORT_VERSION}; no_auto_email=true"
    )


def participant_key(campaign_id: str, product_id: str, kol_id: str) -> str:
    return launch_participation.participant_key(campaign_id, product_id, "KOL", kol_id)


def _post_ids(candidate: dict) -> list[str]:
    values = list(candidate.get("matched_post_ids") or [])
    values.extend(
        str(post.get("record_id") or "")
        for post in candidate.get("evidence_posts") or []
        if isinstance(post, dict)
    )
    return list(dict.fromkeys(value for value in values if value))[:100]


def _master_fields(candidate: dict, campaign_id: str, source_job_id: str) -> dict:
    email = str(candidate.get("_verified_email") or "").strip().lower()
    if not email:
        raise ControlledImportError("受控导入缺少已通过写前闸的公开商务邮箱")
    profile_url = str(
        candidate.get("public_profile_url") or candidate.get("profile_url") or ""
    ).strip()
    if not profile_url:
        raise ControlledImportError("受控导入缺少公开主页")
    author_key = str(candidate.get("author_key") or "").strip()
    fields = {
        "账号名": str(candidate.get("name") or candidate.get("handle") or "").strip(),
        "邮箱": email,
        # 这里只证明邮箱公开且格式有效，不冒充已做投递验真。
        "邮箱验真状态": "未验",
        "国家": str(candidate.get("country") or "").strip(),
        "国家原文": str(candidate.get("country_raw") or "").strip(),
        "语言": str(candidate.get("language") or "").strip(),
        "主平台": "X" if str(candidate.get("platform") or "").lower() in {"x", "twitter"}
        else "YouTube",
        "主链接": {"link": profile_url, "text": "打开达人主页"},
        "粉丝数": int(candidate.get("followers") or 0),
        "发现来源": "SocialEcho竞品监控",
        "合作状态": "未建联",
        # 主表仍保持待核对，防日常派单把受控导入对象提前当成普通新开发。
        "触达路由状态": "待核对",
        "资料可用状态": "有效",
        "资料核实时间": int(time.time() * 1000),
        "合作竞品": "NYXI",
        "迁移备注": (
            controlled_marker(campaign_id, author_key)
            + f"; source_job={source_job_id}"
        )[:1000],
    }
    creator_id = str(candidate.get("creator_id") or "").strip()
    if fields["主平台"] == "YouTube" and creator_id.startswith("UC"):
        fields["YouTube频道ID"] = creator_id
    post_ids = _post_ids(candidate)
    if post_ids:
        fields["竞品帖子证据"] = post_ids
    return fields


def _participant_fields(
    candidate: dict, *, campaign_id: str, activity_id: str, product_id: str,
    kol_id: str, ranking_version: str, source_job_id: str,
) -> dict:
    enriched = {
        **candidate,
        "profile_url": str(
            candidate.get("public_profile_url") or candidate.get("profile_url") or ""
        ),
        "score": float(candidate.get("evidence_strength_score") or 0),
        "final_priority": float(candidate.get("evidence_strength_score") or 0),
        "evidence_level": str(candidate.get("evidence_level") or "竞品作者证据"),
        "matched_post_ids": _post_ids(candidate),
        "review_route": "KOL运营审核",
        "review_decision": "待审核",
        "review_instruction": (
            "系统已通过国家、语言、内容相关性、非官方身份、公开商务邮箱、"
            "双主表重复和戴夫历史评测预检。运营只需打开主页复核当前内容质量；"
            "在改为“通过”前不会生成开发信。"
        ),
        "content_summary": (
            f"公开主页已读；国家={candidate.get('country') or '未知'}；"
            f"语言={candidate.get('language') or '未知'}；"
            f"粉丝={int(candidate.get('followers') or 0)}"
        ),
        "relationship_summary": "双主表身份和邮箱预检无既有关系",
        "evidence_summary": (
            f"NYXI公开帖子作者；source_job={source_job_id}；"
            f"证据帖子={len(_post_ids(candidate))}条"
        ),
        "primary_evidence_url": str(
            next((post.get("post_url") or post.get("url") for post in
                  candidate.get("evidence_posts") or [] if isinstance(post, dict)), "")
        ),
    }
    ranking_fields = launch_participation._ranking_fields(enriched, ranking_version)
    ranking_fields.update({
        "审核结论": "待审核", "审核原因": "", "审核人": None,
        "审核时间": None, "系统审核分流": "KOL运营审核",
    })
    return {
        "参与记录ID": participant_key(campaign_id, product_id, kol_id),
        "活动ID": campaign_id,
        "关联活动": [activity_id],
        "产品家族ID": product_id,
        "对象类型": "KOL",
        "关联KOL": [kol_id],
        **ranking_fields,
        "参与状态": "已入围",
        "名单版本": ranking_version,
        "锁定批次ID": f"controlled-{int(time.time())}",
        "取消原因代码": "",
        "排序快照历史": launch_participation._with_snapshot(
            {}, enriched, ranking_version,
        ),
    }


def _controlled_master(candidate: dict, kols: list[dict], campaign_id: str) -> dict | None:
    author_key = str(candidate.get("author_key") or "").strip()
    marker = controlled_marker(campaign_id, author_key)
    owners = preview._identity_owners(candidate, kols, [])
    owner_ids = {owner.get("record_id") for owner in owners}
    matches = [
        row for row in kols
        if row.get("record_id") in owner_ids
        and marker in ext((row.get("fields") or {}).get("迁移备注"))
    ]
    if len(matches) > 1:
        raise ControlledImportError("受控导入幂等标记命中多条KOL主表记录")
    return matches[0] if matches else None


async def _drafts_for_kol(kol_id: str) -> list[dict]:
    rows = await feishu.fetch_all_records(
        config.T_DRAFT,
        field_names=["邮件草稿ID", "邮件草稿状态", "发送状态", "关联KOL"],
    )
    return [
        row for row in rows
        if kol_id in preview._link_ids((row.get("fields") or {}).get("关联KOL"))
    ]


async def _readback_master_safety(kol_id: str, expected_email: str,
                                  campaign_id: str, author_key: str) -> dict:
    """单选字段可能静默丢失；逐字段补写并再次回读，受控标记同时让常规筛选失败关闭。"""
    required = {
        "合作状态": "未建联",
        "触达路由状态": "待核对",
        "资料可用状态": "有效",
    }
    master = await feishu.get_record(config.T_KOL, kol_id)
    fields = master.get("fields") or {}
    marker = controlled_marker(campaign_id, author_key)
    if marker not in ext(fields.get("迁移备注")):
        raise ControlledImportError("KOL主表创建后幂等标记回读失败")
    if feishu.clean_email(ext(fields.get("邮箱")))[0] != expected_email:
        raise ControlledImportError("KOL主表创建后邮箱回读不一致")
    missing = {
        field: value for field, value in required.items()
        if ext(fields.get(field)) != value
    }
    for field, value in missing.items():
        await feishu.update_record(config.T_KOL, kol_id, {field: value})
    if missing:
        master = await feishu.get_record(config.T_KOL, kol_id)
        fields = master.get("fields") or {}
    failed = [field for field, value in required.items() if ext(fields.get(field)) != value]
    if failed:
        raise ControlledImportError(
            "KOL主表安全状态回读失败: " + ",".join(failed)
        )
    return master


async def _participants_by_unique_key_strong(unique_key: str) -> list[dict]:
    """用records list作事实回读，避免search索引延迟造成重复创建。"""
    rows = await feishu.fetch_all_records(
        config.T_LAUNCH_PARTICIPANT,
        field_names=[
            "参与记录ID", "审核结论", "参与状态", "关联KOL", "关联邮件草稿",
        ],
    )
    return [
        row for row in rows
        if ext((row.get("fields") or {}).get("参与记录ID")) == unique_key
    ]


async def audit_controlled_import_progress(campaign_id: str) -> dict:
    """服务重启或异常后，从业务事实表重建受控导入进度，不依赖内存任务。"""
    activity = await launch_evidence.get_activity(campaign_id)
    product_id = preview._activity_product_id(activity.get("fields") or {})
    kols, _ = await preview._load_evidence_identity_contacts()
    results = []
    for handle, author_key in sorted(DAVE_LOCKED_AUTHORS.items()):
        candidate = {
            "author_key": author_key, "platform": "YouTube", "handle": handle,
            "profile_url": f"https://youtube.com/@{handle}",
        }
        master = _controlled_master(candidate, kols, campaign_id)
        if not master:
            continue
        kol_id = master.get("record_id")
        participants = await _participants_by_unique_key_strong(
            participant_key(campaign_id, product_id, kol_id),
        ) if product_id else []
        drafts = await _drafts_for_kol(kol_id)
        results.append({
            "handle": handle, "kol_id": kol_id,
            "participant_ids": [row.get("record_id") for row in participants],
            "participant_count": len(participants),
            "review_statuses": [
                ext((row.get("fields") or {}).get("审核结论")) for row in participants
            ],
            "draft_count": len(drafts),
        })
    return {
        "campaign_id": campaign_id, "durable_audit": True,
        "imported": len(results),
        "participation_records": sum(row["participant_count"] for row in results),
        "draft_count": sum(row["draft_count"] for row in results),
        "drafts_created": 0, "emails_sent": 0, "results": results,
    }


def _remaining_block_reasons(candidate: dict, controlled: dict | None) -> list[str]:
    reasons = list(candidate.get("write_block_reasons") or [])
    if not controlled:
        return reasons
    kol_id = controlled.get("record_id")
    identity_owner_ids = {
        owner.get("record_id") for owner in candidate.get("existing_identity_owners") or []
    }
    email_owner_ids = {
        owner.get("record_id") for owner in candidate.get("existing_email_owners") or []
    }
    allowed = set()
    if identity_owner_ids == {kol_id}:
        allowed.add("creator_identity_already_in_kol_or_media_master")
    if email_owner_ids == {kol_id}:
        allowed.add("email_already_in_kol_or_media_master")
    return [reason for reason in reasons if reason not in allowed]


async def _commit_selected(
    *, campaign_id: str, source_job_id: str, selected: list[dict],
    activity: dict, product_id: str, ranking_version: str,
) -> dict:
    kols, editors = await preview._load_evidence_identity_contacts()
    results = []
    writes = 0
    for candidate in selected:
        controlled = _controlled_master(candidate, kols, campaign_id)
        fresh_identity = preview._identity_owners(candidate, kols, editors)
        email = str(candidate.get("_verified_email") or "").strip().lower()
        fresh_email_owners = (
            preview._email_owner_index(kols, editors).get(email, []) if email else []
        )
        reasons = _remaining_block_reasons(candidate, controlled)
        if controlled:
            fresh_identity_ids = {
                owner.get("record_id") for owner in fresh_identity
            }
            fresh_email_owner_ids = {
                owner.get("record_id") for owner in fresh_email_owners
            }
            if fresh_identity_ids != {controlled.get("record_id")}:
                reasons.append("creator_identity_changed_before_write")
            if fresh_email_owner_ids != {controlled.get("record_id")}:
                reasons.append("email_identity_changed_before_write")
            if reasons:
                raise ControlledImportError(
                    f"{_handle(candidate)}重跑硬闸未通过: {','.join(dict.fromkeys(reasons))}"
                )
            kol_id = controlled["record_id"]
            master_action = "reused"
        else:
            if fresh_identity:
                reasons.append("creator_identity_changed_before_write")
            if fresh_email_owners:
                reasons.append("email_identity_changed_before_write")
            if reasons or not candidate.get("eligible_for_master_write"):
                raise ControlledImportError(
                    f"{_handle(candidate)}写前条件变化: {','.join(dict.fromkeys(reasons))}"
                )
            kol_id = await feishu.create_record(
                config.T_KOL, _master_fields(candidate, campaign_id, source_job_id),
            )
            writes += 1
            master_action = "created"
            master = await _readback_master_safety(
                kol_id, email, campaign_id,
                str(candidate.get("author_key") or ""),
            )
            kols.append(master)

        unique_key = participant_key(campaign_id, product_id, kol_id)
        existing = await _participants_by_unique_key_strong(unique_key)
        if len(existing) > 1:
            raise ControlledImportError("活动参与记录唯一键重复")
        if existing:
            participant = existing[0]
            participant_action = "reused"
        else:
            participant_id = await feishu.create_record(
                config.T_LAUNCH_PARTICIPANT,
                _participant_fields(
                    candidate, campaign_id=campaign_id,
                    activity_id=activity["record_id"], product_id=product_id,
                    kol_id=kol_id, ranking_version=ranking_version,
                    source_job_id=source_job_id,
                ),
            )
            writes += 1
            participant_action = "created"
            participant = await feishu.get_record(
                config.T_LAUNCH_PARTICIPANT, participant_id,
            )
            if ext((participant.get("fields") or {}).get("参与记录ID")) != unique_key:
                raise ControlledImportError("活动参与记录创建后主键回读失败")
            readback = await _participants_by_unique_key_strong(unique_key)
            if len(readback) != 1 or readback[0].get("record_id") != participant_id:
                raise ControlledImportError("活动参与记录创建后唯一键回读失败")
        participant_fields = participant.get("fields") or {}
        if ext(participant_fields.get("审核结论")) != "待审核":
            raise ControlledImportError("受控参与记录未保持待审核")
        if preview._link_ids(participant_fields.get("关联邮件草稿")):
            raise ControlledImportError("受控参与记录意外关联邮件草稿")
        drafts = await _drafts_for_kol(kol_id)
        if drafts:
            raise ControlledImportError("受控导入对象已存在邮件草稿，禁止报告成功")
        results.append({
            "handle": _handle(candidate), "kol_id": kol_id,
            "participant_id": participant.get("record_id"),
            "master_action": master_action, "participant_action": participant_action,
            "review_status": "待审核", "draft_count": 0,
        })

    return {
        "read_only": False, "campaign_id": campaign_id, "source_job_id": source_job_id,
        "planned": len(selected), "imported": len(results), "writes": writes,
        "master_writes": sum(row["master_action"] == "created" for row in results),
        "participation_writes": sum(
            row["participant_action"] == "created" for row in results
        ),
        "drafts_created": 0, "emails_sent": 0, "results": results,
        "guard": "全部参与记录保持待审核且无草稿；未调用任何发送器",
    }


async def run_controlled_import(
    *, campaign_id: str, seed_candidates: list[dict], source_job_id: str,
    expected_handles: list[str], commit: bool = False,
) -> dict:
    if campaign_id != DAVE_CAMPAIGN_ID:
        raise ControlledImportError("当前受控导入只允许Dave灰度活动")
    expected = {str(value or "").strip().lstrip("@").casefold() for value in expected_handles}
    seed_handles = {validate_locked_seed(candidate) for candidate in seed_candidates}
    if not expected or expected != seed_handles or not expected.issubset(DAVE_EXPECTED_HANDLES):
        raise ControlledImportError("受控导入样本不属于锁定的P0作者")
    enrichment = await preview.enrich_unmatched_evidence_authors(
        campaign_id=campaign_id, limit=len(seed_candidates),
        seed_candidates=seed_candidates, source_job_id=source_job_id,
        _include_verified_email=True,
    )
    by_handle = {_handle(candidate): candidate for candidate in enrichment.get("candidates") or []}
    missing = sorted(expected - set(by_handle))
    if missing:
        raise ControlledImportError(f"锁定作者样本缺失: {','.join(missing)}")
    selected = [by_handle[handle] for handle in sorted(expected)]
    activity = await launch_evidence.get_activity(campaign_id)
    activity_fields = activity.get("fields") or {}
    product_id = preview._activity_product_id(activity_fields)
    ranking_version = ext(activity_fields.get("证据排序版本"))
    if not product_id or not ranking_version:
        raise ControlledImportError("活动缺少产品或排序版本")
    if ranking_version != str(enrichment.get("ranking_version") or ""):
        raise ControlledImportError("活动排序版本与公开资料预检版本不一致")

    # 预演也要识别本活动之前已经受控导入的同一作者；只豁免“命中自己”
    # 产生的身份/邮箱重复，其余当前硬闸仍须重新通过。
    current_kols, _ = await preview._load_evidence_identity_contacts()
    preview_rows = []
    for candidate in selected:
        controlled = _controlled_master(candidate, current_kols, campaign_id)
        reasons = _remaining_block_reasons(candidate, controlled)
        preview_rows.append({
            "handle": _handle(candidate),
            "reusable": bool(controlled) and not reasons,
            "reasons": reasons,
        })
    blocked = [row for row in preview_rows if row["reasons"]]
    if not commit:
        return {
            "read_only": True, "campaign_id": campaign_id, "source_job_id": source_job_id,
            "planned": len(selected) - len(blocked), "blocked": blocked,
            "reusable": sum(row["reusable"] for row in preview_rows),
            "writes": 0, "master_writes": 0, "participation_writes": 0,
            "drafts_created": 0, "emails_sent": 0,
            "guard": "只计划KOL主表＋待审核参与记录；不创建草稿、不调用发送器",
        }

    lock = _IMPORT_LOCKS.setdefault(campaign_id, asyncio.Lock())
    async with lock:
        # 锁内再次读取活动版本，避免预检后活动配置被运营修改。
        current_activity = await launch_evidence.get_activity(campaign_id)
        current_fields = current_activity.get("fields") or {}
        if (
            preview._activity_product_id(current_fields) != product_id
            or ext(current_fields.get("证据排序版本")) != ranking_version
        ):
            raise ControlledImportError("活动配置在写入前发生变化，请重新预演")
        return await _commit_selected(
            campaign_id=campaign_id, source_job_id=source_job_id, selected=selected,
            activity=current_activity, product_id=product_id,
            ranking_version=ranking_version,
        )

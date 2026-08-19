"""集中上稿活动：单个已审 KOL 的一次性真实开发信放行。"""

from __future__ import annotations

import asyncio
import html
import os
import re
import time
from urllib.parse import urlsplit

from . import (
    auto_send,
    config,
    enrich,
    feishu,
    launch_candidate_preview,
    launch_email_preflight,
    launch_evidence,
    zoho,
)
from .feishu import ext


class OutreachValidationError(RuntimeError):
    """真实发送门槛不满足。"""


class OutreachRawValidationError(RuntimeError):
    """邮件已经调用发送，但 Zoho 发件箱 raw 校验没有通过。"""


_LOCKS: dict[str, asyncio.Lock] = {}


def require_real_one_only(confirm: str) -> None:
    if (os.environ.get("EMAIL_DRY_RUN_TO") or "").strip():
        raise OutreachValidationError("EMAIL_DRY_RUN_TO 仍开启（DRY-RUN）；禁止真实发送")
    if confirm != "REAL_ONE_ONLY":
        raise OutreachValidationError("必须显式传 confirm=REAL_ONE_ONLY")


def _link_ids(value) -> set[str]:
    if isinstance(value, dict):
        return set(value.get("link_record_ids") or value.get("record_ids") or [])
    if isinstance(value, list):
        out = set()
        for item in value:
            if isinstance(item, str):
                out.add(item)
            elif isinstance(item, dict):
                out.update(item.get("link_record_ids") or item.get("record_ids") or [])
        return out
    return set()


def _canonical_profile(url: str) -> str:
    value = str(url or "").strip()
    if not value:
        return ""
    if "://" not in value:
        value = "https://" + value
    parsed = urlsplit(value)
    host = parsed.netloc.lower().removeprefix("www.")
    path = re.sub(r"/+", "/", parsed.path).rstrip("/").lower()
    return f"{host}{path}"


def _profile_identity_matches(participant_url: str, expected_url: str,
                              kol: dict | None = None) -> bool:
    if _canonical_profile(participant_url) == _canonical_profile(expected_url):
        return True
    # 飞书历史快照可能保存 /channel/UC...，用户审核时打开的是 /@handle。
    # 只有“参与记录 channel ID = KOL 主表 channel ID”且“@handle = KOL 账号名”才视为同一人。
    pf = urlsplit(participant_url if "://" in participant_url else "https://" + participant_url)
    ef = urlsplit(expected_url if "://" in expected_url else "https://" + expected_url)
    participant_match = re.fullmatch(r"/channel/(UC[A-Za-z0-9_-]{20,})/?", pf.path, re.I)
    expected_match = re.fullmatch(r"/@([^/]+)/?", ef.path)
    kf = (kol or {}).get("fields") or {}
    channel_id = ext(kf.get("YouTube频道ID")).strip()
    account_name = ext(kf.get("账号名")).strip().lstrip("@").lower()
    return bool(
        pf.netloc.lower().removeprefix("www.") == "youtube.com"
        and ef.netloc.lower().removeprefix("www.") == "youtube.com"
        and participant_match and expected_match
        and participant_match.group(1).lower() == channel_id.lower()
        and expected_match.group(1).lower() == account_name
    )


def validate_participant_gate(activity: dict, participant: dict, *, campaign_id: str,
                              product_id: str, contact_id: str,
                              expected_profile_url: str,
                              expected_ranking_version: str,
                              kol: dict | None = None) -> None:
    af = activity.get("fields") or {}
    pf = participant.get("fields") or {}
    checks = {
        "活动ID": ext(af.get("活动ID")) == campaign_id and ext(pf.get("活动ID")) == campaign_id,
        "产品": ext(af.get("产品主记录ID")) == product_id and ext(pf.get("产品家族ID")) == product_id,
        "对象类型": ext(pf.get("对象类型")) == "KOL",
        "联系人": contact_id in _link_ids(pf.get("关联KOL")),
        "参与状态": ext(pf.get("参与状态")) == "已入围",
        "审核结论": ext(pf.get("审核结论")) == "通过",
        "进入方式": ext(pf.get("进入方式")) == "新开发",
        "活动分池": ext(pf.get("活动分池")) == "新开发池",
        "名单版本": (
            ext(af.get("KOL已锁定名单版本")) == expected_ranking_version
            and ext(pf.get("名单版本")) == expected_ranking_version
            and ext(pf.get("排序版本")) == expected_ranking_version
        ),
        "名单阻塞": not ext(af.get("KOL名单阻塞代码")),
        "达人主页": _profile_identity_matches(
            feishu.ext_url(pf.get("达人主页")), expected_profile_url, kol,
        ),
    }
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise OutreachValidationError("真实发送门槛未通过: " + ", ".join(failed))


async def _find_release_draft(nonce: str) -> dict | None:
    draft_key = f"launch-{nonce}"
    rows = await feishu.search_records(config.T_DRAFT, [
        {"field_name": "邮件草稿ID", "operator": "is", "value": [draft_key]},
    ], field_names=["邮件草稿ID", "邮件草稿状态", "发送状态", "发送时间", "审批意见"])
    exact = [row for row in rows if ext((row.get("fields") or {}).get("邮件草稿ID")) == draft_key]
    if len(exact) > 1:
        raise OutreachValidationError("一次性放行 nonce 对应多条草稿，已停止发送")
    return exact[0] if exact else None


def _real_raw_validation(*, message: dict, raw_body: str, expected_to: str,
                         expected_subject: str, expected_body: str,
                         product: dict, brand: str, nonce: str) -> dict:
    product_fields = product.get("fields") or {}
    product_name = ext(product_fields.get("产品英文名")).strip()
    normalized = (raw_body or "").replace("&amp;", "&")
    raw_text = html.unescape(re.sub(r"<[^>]+>", "", normalized)).strip()
    expected_text = html.unescape(re.sub(r"<[^>]+>", "", expected_body or "")).strip()
    actual_to, _ = feishu.clean_email(message.get("toAddress") or "")
    actual_from, _ = feishu.clean_email(message.get("fromAddress") or "")
    expected_from, _ = feishu.clean_email(config.BRAND_CONFIG[brand]["alias_from"])
    links = re.findall(r'href=["\']([^"\']+)', expected_body or "", re.I)
    identity_rules = launch_email_preflight._identity_rules(product_fields, product_name)
    checks = {
        "recipient_matches_approved_kol": actual_to == expected_to.lower(),
        "sender_matches_brand": bool(expected_from and actual_from == expected_from),
        "subject_preserved": (message.get("subject") or "").strip() == expected_subject.strip(),
        "body_not_truncated": len(raw_text) >= max(50, int(len(expected_text) * 0.7)),
        "html_rendered": bool(re.search(r"<(p|div|br|a|strong)[\s>/]", raw_body or "", re.I)),
        "product_identity_present": launch_email_preflight._product_identity_present(raw_text, identity_rules),
        "all_links_present": bool(links) and all(url in normalized for url in links),
        "placeholder_free": not any(
            marker in (message.get("subject") or "") + raw_text
            for marker in launch_email_preflight.PLACEHOLDER_MARKERS
        ),
        "one_time_nonce_present": f"launch-release:{nonce}" in normalized,
    }
    return {
        "passed": all(checks.values()), "checks": checks,
        "raw_text_length": len(raw_text), "expected_text_length": len(expected_text),
        "subject": message.get("subject") or "",
    }


async def validate_sent_raw(*, brand: str, message_id: str, expected_to: str,
                            expected_subject: str, expected_body: str,
                            product: dict, nonce: str) -> dict:
    _, sent_folder_id = await zoho._get_folder_ids(brand)
    deadline = time.monotonic() + 55
    hit = None
    raw_body = ""
    while time.monotonic() < deadline:
        sent = await zoho.list_sent_messages(brand, limit=100)
        hit = next((m for m in sent.get("messages", [])
                    if str(m.get("messageId") or "") == str(message_id)), None)
        if hit:
            raw_body = await zoho.get_message_content(brand, str(message_id), sent_folder_id)
        if hit and raw_body:
            break
        await asyncio.sleep(5)
    if not hit or not raw_body:
        raise OutreachRawValidationError(
            "真实邮件在 55 秒内未完整出现在 Zoho 发件箱；该 nonce 已消费，禁止自动补发"
        )
    result = _real_raw_validation(
        message=hit, raw_body=raw_body, expected_to=expected_to,
        expected_subject=expected_subject, expected_body=expected_body,
        product=product, brand=brand, nonce=nonce,
    )
    if not result["passed"]:
        raise OutreachRawValidationError(
            f"真实邮件 raw 校验失败；禁止自动补发: {result['checks']}"
        )
    return result


async def _set_activity_email_gate(activity_record_id: str, value: bool) -> None:
    updated = await feishu.update_record(
        config.T_LAUNCH_CAMPAIGN, activity_record_id, {"发送邮件授权": value},
    )
    fields = updated.get("fields") if isinstance(updated, dict) else None
    if isinstance(fields, dict) and bool(fields.get("发送邮件授权")) != value:
        raise OutreachValidationError("活动发送邮件授权写后回读不一致")


async def _fast_precheck(*, kol: dict, product: dict, product_id: str,
                         contact_id: str, brand: str) -> dict:
    """只读取本次联系人相关记录，执行与全池预览相同的重复触达检查。"""
    email, reason = feishu.clean_email(ext((kol.get("fields") or {}).get("邮箱")))
    if not email:
        raise OutreachValidationError(f"KOL 邮箱不可发送: {reason}")

    draft_fields = launch_candidate_preview.DRAFT_FIELDS
    product_fields = product.get("fields") or {}
    canonical_id = ext(product_fields.get("活动主记录ID")).strip() or product_id
    merge_key = ext(product_fields.get("活动归并键")).strip()

    async def exact_email_owners(table_id: str, object_type: str) -> set[tuple[str, str]]:
        rows = await feishu.search_records(table_id, [
            {"field_name": "邮箱", "operator": "contains", "value": [email]},
        ], field_names=["邮箱"])
        owners = set()
        for row in rows:
            row_email, _ = feishu.clean_email(ext((row.get("fields") or {}).get("邮箱")))
            if row_email == email:
                owners.add((object_type, row.get("record_id", "")))
        return owners

    product_filter = (
        {"field_name": "活动归并键", "operator": "is", "value": [merge_key]}
        if merge_key else
        {"field_name": "活动主记录ID", "operator": "is", "value": [canonical_id]}
    )
    drafts_by_contact, drafts_by_email, kol_owners, editor_owners, family_rows = await asyncio.gather(
        feishu.search_records(config.T_DRAFT, [
            {"field_name": "关联KOL", "operator": "contains", "value": [contact_id]},
        ], field_names=draft_fields),
        feishu.search_records(config.T_DRAFT, [
            {"field_name": "收件邮箱", "operator": "contains", "value": [email]},
        ], field_names=draft_fields),
        exact_email_owners(config.T_KOL, "KOL"),
        exact_email_owners(config.T_EDITOR, "媒体人"),
        feishu.search_records(
            config.T_PRODUCT, [product_filter],
            field_names=["活动主记录ID", "活动归并键"],
        ),
    )

    drafts = []
    seen_drafts = set()
    for draft in drafts_by_contact + drafts_by_email:
        draft_email, _ = feishu.clean_email(ext((draft.get("fields") or {}).get("收件邮箱")))
        linked_to_contact = contact_id in _link_ids((draft.get("fields") or {}).get("关联KOL"))
        if not linked_to_contact and draft_email != email:
            continue
        draft_id = draft.get("record_id", "")
        if draft_id not in seen_drafts:
            seen_drafts.add(draft_id)
            drafts.append(draft)

    family_ids = {product_id, canonical_id}
    for row in family_rows:
        fields = row.get("fields") or {}
        same_canonical = (ext(fields.get("活动主记录ID")).strip() or row.get("record_id", "")) == canonical_id
        same_merge_key = bool(merge_key and ext(fields.get("活动归并键")).strip() == merge_key)
        if same_canonical or same_merge_key:
            family_ids.add(row.get("record_id", ""))
    family_ids.discard("")

    return launch_candidate_preview.precheck_contact(
        kol, object_type="KOL", brand=brand, product_ids=family_ids,
        drafts=drafts, email_owners={email: kol_owners | editor_owners},
        now_ms=int(time.time() * 1000),
    )


async def send_one_real(*, campaign_id: str, participant_record_id: str,
                        product_id: str, contact_id: str, brand: str,
                        expected_profile_url: str, expected_ranking_version: str,
                        nonce: str, approved_by: str, confirm: str) -> dict:
    require_real_one_only(confirm)
    if not re.fullmatch(r"[A-Za-z0-9._-]{8,64}", nonce or ""):
        raise OutreachValidationError("nonce 必须是 8-64 位字母、数字、点、横线或下划线")
    if not approved_by.strip():
        raise OutreachValidationError("approved_by required")
    brand = (brand or "").upper()
    if brand not in config.BRAND_CONFIG:
        raise OutreachValidationError(f"unknown brand: {brand}")

    lock = _LOCKS.setdefault(campaign_id, asyncio.Lock())
    async with lock:
        existing = await _find_release_draft(nonce)
        if existing:
            # 无论上次是成功、失败还是结果不确定，都只能人工回查，绝不再次调用发送。
            return {
                "ok": ext((existing.get("fields") or {}).get("邮件草稿状态")) == "已发送",
                "reused": True, "resent": False, "draft_id": existing.get("record_id"),
                "status": ext((existing.get("fields") or {}).get("邮件草稿状态")),
                "message": "一次性放行已存在；系统未重发",
            }

        activity = await launch_evidence.get_activity(campaign_id)
        participant = await feishu.get_record(config.T_LAUNCH_PARTICIPANT, participant_record_id)
        kol = await feishu.get_record(config.T_KOL, contact_id)
        validate_participant_gate(
            activity, participant, campaign_id=campaign_id, product_id=product_id,
            contact_id=contact_id, expected_profile_url=expected_profile_url,
            expected_ranking_version=expected_ranking_version, kol=kol,
        )
        if bool((activity.get("fields") or {}).get("发送邮件授权")):
            raise OutreachValidationError("活动全局发送授权已处于开启状态；需先查明原因，禁止叠加放行")
        paused = auto_send.pause_state().get("paused_brands") or {}
        if brand in paused:
            raise OutreachValidationError(f"{brand} Zoho 通道已暂停: {paused[brand]}")

        product = await feishu.get_record(config.T_PRODUCT, product_id)
        product_fields = product.get("fields") or {}
        if config.brand_from_text(ext(product_fields.get("品牌"))) != brand:
            raise OutreachValidationError("产品品牌与发件品牌不一致")
        if ext(product_fields.get("派单模式")) != "活动专用":
            raise OutreachValidationError("产品不是活动专用锁状态，禁止走活动单人放行")
        email, reason = feishu.clean_email(ext((kol.get("fields") or {}).get("邮箱")))
        if not email:
            raise OutreachValidationError(f"KOL 邮箱不可发送: {reason}")

        precheck = await _fast_precheck(
            kol=kol, product=product, product_id=product_id,
            contact_id=contact_id, brand=brand,
        )
        if precheck.get("decision") != "eligible_new_cold":
            raise OutreachValidationError(
                "全局重复触达预检未通过: "
                + str(precheck.get("decision") or "unknown")
                + " / " + "; ".join(precheck.get("reasons") or [])
            )

        score = float((participant.get("fields") or {}).get("基础评分快照") or 0)
        breakdown = {
            "活动人工审核": {
                "score": score,
                "reason": "已通过活动名单审核；竞品证据和地区语言条件已在锁定快照中确认",
            },
        }
        signature = "Tom from FUNLAB Team" if brand == "FUNLAB" else "Lisa @ POWKONG Team"
        generated = await enrich.gen_draft(kol, product, brand, signature, breakdown, score)
        if generated.get("error") or generated.get("skip"):
            raise OutreachValidationError(
                "开发信生成失败: " + str(generated.get("error") or generated.get("skip"))
            )
        subject = str(generated.get("subject") or "").strip()
        body = str(generated.get("body") or "").strip()
        if not subject or len(re.sub(r"<[^>]+>", "", body)) < 50:
            raise OutreachValidationError("开发信主题为空或正文过短")
        body = body + (
            f'<span style="display:none;font-size:0;color:transparent">'
            f"launch-release:{nonce}</span>"
        )
        now_ms = int(time.time() * 1000)
        approval_note = (
            f"[一次性真实灰度授权] campaign={campaign_id}; participant={participant_record_id}; "
            f"approved_by={approved_by}; nonce={nonce}; 仅此 KOL，禁止自动重发"
        )
        draft_fields = {
            "邮件草稿ID": f"launch-{nonce}", "关联KOL": [contact_id], "关联产品": [product_id],
            "匹配度总分": score, "匹配亮点": str(generated.get("highlights") or "")[:500],
            "建议切入点": str(generated.get("angle") or "")[:200], "收件邮箱": email,
            "邮件主题": subject[:200], "邮件正文": body, "邮件语言": "en",
            "邮件草稿状态": "待审", "邮件草稿来源": "cold", "对象类型": "KOL",
            "发送邮箱": config.BRAND_CONFIG[brand]["sender_label"], "发送人署名": signature,
            "生成时间": now_ms, "建议发送时间": now_ms, "发送时区说明": "活动单人灰度立即发送",
            "重生次数": 0, "UTM 链接": generated.get("utm_url") or "", "审批意见": approval_note[:500],
        }
        try:
            draft_id = await feishu.create_record(config.T_DRAFT, draft_fields)
        except Exception as exc:
            found = await _find_release_draft(nonce)
            if not found:
                raise OutreachValidationError("一次性放行草稿创建结果不确定；已停止自动重试") from exc
            draft_id = found["record_id"]
            return {"ok": False, "reused": True, "resent": False, "draft_id": draft_id,
                    "message": "草稿创建结果不确定；系统未发送"}
        await feishu.update_record(
            config.T_LAUNCH_PARTICIPANT, participant_record_id, {"关联邮件草稿": [draft_id]},
        )
        draft_record = await feishu.get_record(config.T_DRAFT, draft_id)

        gate_opened = False
        try:
            await _set_activity_email_gate(activity["record_id"], True)
            gate_opened = True
            sent = await auto_send.send_one(
                draft_record, activity_release=auto_send._LAUNCH_ACTIVITY_RELEASE,
            )
            if not sent.get("ok"):
                raise OutreachValidationError("真实发送失败: " + str(sent.get("error") or "unknown"))
            validation = await validate_sent_raw(
                brand=brand, message_id=str(sent.get("msg_id") or ""), expected_to=email,
                expected_subject=subject, expected_body=body, product=product, nonce=nonce,
            )
            await feishu.update_record(config.T_DRAFT, draft_id, {
                "审批意见": (approval_note + f" | raw=9/9通过; Zoho消息ID={sent.get('msg_id')}")[:500],
            })
            return {
                "ok": True, "reused": False, "resent": False, "campaign_id": campaign_id,
                "participant_id": participant_record_id, "draft_id": draft_id,
                "contact_id": contact_id, "recipient": email, "brand": brand,
                "message_id": sent.get("msg_id"), "subject": subject, "validation": validation,
            }
        except OutreachRawValidationError:
            await feishu.update_record(config.T_DRAFT, draft_id, {
                "审批意见": (approval_note + " | raw校验失败/超时；邮件可能已发，禁止自动补发")[:500],
            })
            raise
        finally:
            if gate_opened:
                await _set_activity_email_gate(activity["record_id"], False)

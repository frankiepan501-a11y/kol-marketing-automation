"""集中上稿活动首次真实开发信前的测试邮箱 raw 内容校验。"""

from __future__ import annotations

import asyncio
import hashlib
import html
import os
import re
import time

from . import config, feishu, zoho
from .feishu import ext


PLACEHOLDER_MARKERS = (
    "[TBD", "[CARRIER", "[TRACKING#", "[ETA", "[ADDRESS", "[PRICE",
    "[QUANTITY", "[xxx", "[XXX", "待填",
)
DEFAULT_TEST_EMAIL_ALLOWLIST = {"frankiepan501@gmail.com"}
_RUN_LOCKS: dict[tuple[str, str], asyncio.Lock] = {}
_RUN_STATES: dict[tuple[str, str], dict] = {}


def _monotonic() -> float:
    return time.monotonic()


class RawValidationError(RuntimeError):
    """Zoho 已发送箱的实际内容未通过放行校验。"""


def _test_email_allowlist() -> set[str]:
    configured = os.environ.get("EMAIL_TEST_ALLOWLIST", "")
    values = {x.strip().lower() for x in configured.split(",") if x.strip()}
    return values or DEFAULT_TEST_EMAIL_ALLOWLIST


def require_test_mode(confirm: str) -> str:
    target_raw = (os.environ.get("EMAIL_DRY_RUN_TO") or "").strip()
    if not target_raw:
        raise RuntimeError("EMAIL_DRY_RUN_TO 未设置；禁止执行邮件测试")
    if confirm != "TEST_ONLY":
        raise RuntimeError("必须显式传 confirm=TEST_ONLY")
    clean, reason = feishu.clean_email(target_raw)
    if not clean:
        raise RuntimeError(f"EMAIL_DRY_RUN_TO 不是有效邮箱: {reason}")
    if target_raw.lower() != clean:
        raise RuntimeError("EMAIL_DRY_RUN_TO 必须是单个规范邮箱，禁止姓名、多邮箱或附加文本")
    if clean not in _test_email_allowlist():
        raise RuntimeError("EMAIL_DRY_RUN_TO 不在独立测试邮箱白名单；禁止发送")
    return clean


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


def _strip_html(value: str) -> str:
    return html.unescape(re.sub(r"<[^>]+>", "", value or "").replace("&nbsp;", " ")).strip()


def _text_values(value) -> list[str]:
    if isinstance(value, list):
        return [str(x).strip() for x in value if isinstance(x, str) and str(x).strip()]
    text = str(ext(value) or "").strip()
    return [text] if text else []


def _identity_rules(product_fields: dict, product_name: str) -> dict:
    ip_markers = [x for x in _text_values(product_fields.get("适配IP"))
                  if re.search(r"[A-Za-z]", x) and len(x.split()) >= 2]
    keyword = ext(product_fields.get("主关键词(英文)"))
    stop = {"the", "and", "for", "with", "edition", "official", "switch", "controller", "dock"}
    keyword_tokens = [x.lower() for x in re.findall(r"[A-Za-z]{3,}", str(keyword or ""))
                      if x.lower() not in stop]
    product_type_tokens = sorted(set(re.findall(
        r"\b(controller|dock|charger|case|headset|keyboard|grip|joy-?con|accessory)\b",
        str(product_name or "").lower(),
    )))
    return {
        "exact_name": product_name,
        "ip_markers": ip_markers,
        "keyword_tokens": sorted(set(keyword_tokens)),
        "product_type_tokens": product_type_tokens,
    }


def _product_identity_present(raw_text: str, rules: dict) -> bool:
    text = raw_text.lower()
    exact = (rules.get("exact_name") or "").lower()
    if exact and exact in text:
        return True
    ip_ok = any(marker.lower() in text for marker in rules.get("ip_markers") or [])
    keyword_tokens = rules.get("keyword_tokens") or []
    keyword_hits = sum(1 for token in keyword_tokens if token in text)
    product_type_ok = any(
        re.search(rf"\b{re.escape(token)}\b", text)
        for token in rules.get("product_type_tokens") or []
    )
    # Some valid product keywords collapse to one distinctive token after generic
    # words (Switch/controller/dock/etc.) are removed. Requiring two hits made
    # those products impossible to validate even when the licensed IP matched.
    required_hits = min(2, len(keyword_tokens))
    return bool(ip_ok and (
        product_type_ok or (required_hits and keyword_hits >= required_hits)
    ))


def build_test_email(product: dict, draft: dict, brand: str, run_key: str) -> dict:
    """读取真实 cold 草稿，不另造一套测试模板。"""
    product_fields = product.get("fields") or {}
    draft_fields = draft.get("fields") or {}
    product_name = ext(product_fields.get("产品英文名")).strip()
    if not product_name:
        raise RuntimeError("产品缺少产品英文名，测试邮件已阻止")
    if ext(draft_fields.get("邮件草稿来源")) != "cold":
        raise RuntimeError("只能校验真实 cold 开发信草稿")
    if product.get("record_id") not in _link_ids(draft_fields.get("关联产品")):
        raise RuntimeError("草稿未关联到本次产品，测试已阻止")
    subject = ext(draft_fields.get("邮件主题")).strip()
    body = ext(draft_fields.get("邮件正文")).strip()
    if not subject or len(_strip_html(body)) < 50:
        raise RuntimeError("真实开发信草稿主题为空或正文过短，测试已阻止")
    if not re.fullmatch(r"[A-Za-z0-9._-]{8,64}", run_key or ""):
        raise RuntimeError("run_key 必须是 8-64 位字母、数字、点、横线或下划线")
    links = [url for _, url in feishu.product_links(product_fields)]
    if not links:
        raise RuntimeError("产品缺少亚马逊链接/官网链接，测试邮件已阻止")
    return {
        "subject": f"[Launch preflight:{run_key}] {subject}",
        "original_subject": subject,
        "body": body,
        "product_name": product_name,
        "product_identity_rules": _identity_rules(product_fields, product_name),
        "links": links,
        "brand": brand,
        "from_address": config.BRAND_CONFIG[brand]["alias_from"],
    }


def validate_raw_content(*, raw_subject: str, raw_body: str, actual_to: str,
                         actual_from: str, expected_to: str, expected: dict) -> dict:
    normalized_body = (raw_body or "").replace("&amp;", "&")
    raw_text = _strip_html(normalized_body)
    expected_text = _strip_html(expected.get("body") or "")
    clean_actual_to, _ = feishu.clean_email(actual_to)
    clean_actual_from, _ = feishu.clean_email(actual_from)
    clean_expected_from, _ = feishu.clean_email(expected.get("from_address") or "")
    checks = {
        "recipient_is_test_mailbox": clean_actual_to == expected_to.lower(),
        "sender_matches_product_brand": bool(clean_expected_from and clean_actual_from == clean_expected_from),
        "subject_preserved": expected.get("subject", "") in (raw_subject or ""),
        "dry_run_subject_prefix": "[DRY-RUN" in (raw_subject or ""),
        "body_not_truncated": len(raw_text) >= max(50, int(len(expected_text) * 0.7)),
        "html_rendered": bool(re.search(r"<(p|div|br|a|strong)[\s>/]", raw_body or "", re.I)),
        "product_identity_present": _product_identity_present(
            raw_text, expected.get("product_identity_rules") or {}
        ),
        "all_links_present": all(url in normalized_body for url in expected.get("links") or []),
        "placeholder_free": not any(marker in (raw_subject or "") + raw_text for marker in PLACEHOLDER_MARKERS),
    }
    return {
        "passed": all(checks.values()), "checks": checks,
        "raw_text_length": len(raw_text), "expected_text_length": len(expected_text),
    }


def _find_sent(messages: list[dict], expected: dict, target: str, run_key: str) -> dict | None:
    expected_from, _ = feishu.clean_email(expected.get("from_address") or "")
    marker = f"[Launch preflight:{run_key}]"
    for message in messages:
        subject = message.get("subject") or ""
        actual_to, _ = feishu.clean_email(message.get("toAddress") or "")
        actual_from, _ = feishu.clean_email(message.get("fromAddress") or "")
        if (marker in subject and actual_to == target
                and actual_from == expected_from):
            return message
    return None


async def _raw_for_message(brand: str, message: dict, sent_folder_id: str) -> str:
    message_id = str(message.get("messageId") or "")
    return await zoho.get_message_content(brand, message_id, sent_folder_id) if message_id else ""


async def send_and_validate(product_id: str, draft_id: str, brand: str, *, confirm: str,
                            run_key: str) -> dict:
    target = require_test_mode(confirm)
    brand = (brand or "").upper()
    if brand not in config.BRAND_CONFIG:
        raise ValueError(f"unknown brand: {brand}")
    product = await feishu.get_record(config.T_PRODUCT, product_id)
    product_brand = config.brand_from_text(ext((product.get("fields") or {}).get("品牌")))
    if product_brand != brand:
        raise RuntimeError(f"产品品牌={product_brand or '未识别'}，不能使用 {brand} 邮箱测试")
    draft = await feishu.get_record(config.T_DRAFT, draft_id)
    draft_brand = config.brand_from_text(ext((draft.get("fields") or {}).get("发送邮箱")))
    if draft_brand != brand:
        raise RuntimeError(f"草稿发件品牌={draft_brand or '未识别'}，不能作为 {brand} 测试样本")
    expected = build_test_email(product, draft, brand, run_key)
    fingerprint = hashlib.sha256(
        f"{product_id}|{draft_id}|{expected['subject']}|{expected['body']}".encode("utf-8")
    ).hexdigest()
    state_key = (brand, run_key)
    lock = _RUN_LOCKS.setdefault(state_key, asyncio.Lock())
    async with lock:
        state = _RUN_STATES.get(state_key)
        if state and state.get("fingerprint") != fingerprint:
            raise RuntimeError("run_key 已绑定另一份产品/草稿；禁止复用")

        _, sent_folder_id = await zoho._get_folder_ids(brand)
        sent = await zoho.list_sent_messages(brand, limit=100)
        existing = _find_sent(sent.get("messages", []), expected, target, run_key)
        reused = bool(existing or state)
        message_id = str((existing or {}).get("messageId") or (state or {}).get("message_id") or "")
        if not state:
            # 在任何发送 await 之前占位。同实例并发或超时重试只能回查，不能再次发送。
            state = {"fingerprint": fingerprint, "message_id": "", "claimed_at": time.time()}
            _RUN_STATES[state_key] = state
        if not existing and not message_id and not reused:
            try:
                message_id = await zoho.send_email(
                    brand, "launch-preflight@example.invalid", expected["subject"], expected["body"]
                )
                state["message_id"] = message_id
            except Exception as exc:
                # 发送结果可能不确定，保留 claim；同 run_key 后续只回查，绝不补发。
                raise RawValidationError("测试发送结果不确定；已锁定 run_key，禁止自动重发") from exc

        raw_body = ""
        hit = existing
        deadline = _monotonic() + 50
        while _monotonic() < deadline:
            sent = await zoho.list_sent_messages(brand, limit=100)
            hit = next((m for m in sent.get("messages", []) if message_id and str(m.get("messageId")) == str(message_id)), None)
            hit = hit or _find_sent(sent.get("messages", []), expected, target, run_key)
            if hit:
                raw_body = await _raw_for_message(brand, hit, sent_folder_id)
            if hit and raw_body:
                break
            await asyncio.sleep(5)
        if not hit or not raw_body:
            raise RawValidationError("测试邮件在 50 秒内未完整出现；run_key 已锁定，只允许回查，禁止自动补发")

        validation = validate_raw_content(
            raw_subject=hit.get("subject") or "", raw_body=raw_body,
            actual_to=hit.get("toAddress") or "", actual_from=hit.get("fromAddress") or "",
            expected_to=target, expected=expected,
        )
        if not validation["passed"]:
            raise RawValidationError(f"测试邮箱 raw 内容校验失败: {validation['checks']}")
    return {
        "ok": True, "test_only": True, "brand": brand, "product_id": product_id,
        "draft_id": draft_id, "run_key": run_key, "message_id": message_id,
        "recipient": target, "subject": hit.get("subject") or "", "reused": reused,
        "validation": validation, "production_draft_rows_written": 0,
    }

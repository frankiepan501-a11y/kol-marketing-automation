"""集中上稿活动首次真实开发信前的测试邮箱 raw 内容校验。"""

from __future__ import annotations

import asyncio
import html
import os
import re

from . import config, feishu, zoho
from .feishu import ext


PLACEHOLDER_MARKERS = (
    "[TBD", "[CARRIER", "[TRACKING#", "[ETA", "[ADDRESS", "[PRICE",
    "[QUANTITY", "[xxx", "[XXX", "待填",
)


def require_test_mode(confirm: str) -> str:
    target = (os.environ.get("EMAIL_DRY_RUN_TO") or "").strip()
    if not target:
        raise RuntimeError("EMAIL_DRY_RUN_TO 未设置；禁止执行邮件测试")
    if confirm != "TEST_ONLY":
        raise RuntimeError("必须显式传 confirm=TEST_ONLY")
    clean, reason = feishu.clean_email(target)
    if not clean:
        raise RuntimeError(f"EMAIL_DRY_RUN_TO 不是有效邮箱: {reason}")
    return clean


def build_test_email(product: dict, brand: str) -> dict:
    fields = product.get("fields") or {}
    product_name = ext(fields.get("产品英文名")).strip()
    if not product_name:
        raise RuntimeError("产品缺少产品英文名，测试邮件已阻止")
    links = [url for _, url in feishu.product_links(fields)]
    if not links:
        raise RuntimeError("产品缺少亚马逊链接/官网链接，测试邮件已阻止")
    link_html = "".join(
        f'<p><a href="{html.escape(url, quote=True)}">Product page {idx}</a></p>'
        for idx, url in enumerate(links, start=1)
    )
    subject = f"[Launch preflight] {product_name}"
    body = (
        f"<p>Hey Frankie,</p><p>This is the controlled preflight for "
        f"<strong>{html.escape(product_name)}</strong> from {html.escape(brand)}. "
        "It verifies the same Zoho draft, HTML rendering, sending and sent-folder raw-content path "
        "that will protect the first real launch outreach.</p>"
        + link_html
        + "<p>No KOL or media contact receives this message. -- Launch preflight</p>"
    )
    return {"subject": subject, "body": body, "product_name": product_name, "links": links}


def _strip_html(value: str) -> str:
    return html.unescape(re.sub(r"<[^>]+>", "", value or "").replace("&nbsp;", " ")).strip()


def validate_raw_content(*, raw_subject: str, raw_body: str, actual_to: str,
                         expected_to: str, expected: dict) -> dict:
    normalized_body = (raw_body or "").replace("&amp;", "&")
    raw_text = _strip_html(normalized_body)
    expected_text = _strip_html(expected.get("body") or "")
    clean_actual_to, _ = feishu.clean_email(actual_to)
    checks = {
        "recipient_is_test_mailbox": clean_actual_to == expected_to.lower(),
        "subject_preserved": expected.get("subject", "") in (raw_subject or ""),
        "dry_run_subject_prefix": "[DRY-RUN" in (raw_subject or ""),
        "body_not_truncated": len(raw_text) >= max(50, int(len(expected_text) * 0.7)),
        "html_rendered": bool(re.search(r"<(p|div|br|a|strong)[\s>/]", raw_body or "", re.I)),
        "product_name_present": expected.get("product_name", "") in raw_text,
        "all_links_present": all(url in normalized_body for url in expected.get("links") or []),
        "placeholder_free": not any(marker in (raw_subject or "") + raw_text for marker in PLACEHOLDER_MARKERS),
    }
    return {
        "passed": all(checks.values()), "checks": checks,
        "raw_text_length": len(raw_text), "expected_text_length": len(expected_text),
    }


async def send_and_validate(product_id: str, brand: str, *, confirm: str) -> dict:
    target = require_test_mode(confirm)
    brand = (brand or "").upper()
    if brand not in config.BRAND_CONFIG:
        raise ValueError(f"unknown brand: {brand}")
    product = await feishu.get_record(config.T_PRODUCT, product_id)
    expected = build_test_email(product, brand)
    # 原始收件人使用保留域名；zoho.send_email 必须由 EMAIL_DRY_RUN_TO 重定向到测试邮箱。
    message_id = await zoho.send_email(
        brand, "launch-preflight@example.invalid", expected["subject"], expected["body"]
    )

    drafts_folder_id, sent_folder_id = await zoho._get_folder_ids(brand)
    del drafts_folder_id
    raw_body = ""
    raw_subject = ""
    actual_to = ""
    for attempt in range(6):
        raw_body = await zoho.get_message_content(brand, message_id, sent_folder_id)
        sent = await zoho.list_sent_messages(brand, limit=30)
        hit = next((m for m in sent.get("messages", []) if str(m.get("messageId")) == str(message_id)), None)
        if hit:
            raw_subject = hit.get("subject") or ""
            actual_to = hit.get("toAddress") or ""
        if raw_body and hit:
            break
        if attempt < 5:
            await asyncio.sleep(2)

    validation = validate_raw_content(
        raw_subject=raw_subject, raw_body=raw_body, actual_to=actual_to,
        expected_to=target, expected=expected,
    )
    if not validation["passed"]:
        raise RuntimeError(f"测试邮箱 raw 内容校验失败: {validation['checks']}")
    return {
        "ok": True, "test_only": True, "brand": brand, "product_id": product_id,
        "message_id": message_id, "recipient": target,
        "subject": raw_subject, "validation": validation,
        "production_draft_rows_written": 0,
    }

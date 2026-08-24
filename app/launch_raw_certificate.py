"""活动邮件 Raw 验证证书的兼容序列化格式。"""

from __future__ import annotations

import json


SCHEMA_VERSION = 2


def _decode(raw_value) -> dict[str, dict]:
    if isinstance(raw_value, str):
        try:
            payload = json.loads(raw_value or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}
    elif isinstance(raw_value, dict):
        payload = raw_value
    else:
        return {}
    if not isinstance(payload, dict):
        return {}

    certificates = payload.get("certificates")
    if isinstance(certificates, dict):
        return {
            str(template_version): dict(certificate)
            for template_version, certificate in certificates.items()
            if isinstance(certificate, dict)
            and certificate.get("template_version") == str(template_version)
        }

    template_version = str(payload.get("template_version") or "").strip()
    if template_version:
        return {template_version: dict(payload)}
    return {}


def select(raw_value, template_version: str) -> dict:
    """读取指定模板证书；同时兼容旧版单证书JSON。"""
    return dict(_decode(raw_value).get(template_version) or {})


def merge(raw_value, certificate: dict) -> str:
    """只更新当前模板证书，保留同一活动的其他模板证书。"""
    template_version = str(certificate.get("template_version") or "").strip()
    if not template_version:
        raise ValueError("Raw证书缺少template_version")
    certificates = _decode(raw_value)
    certificates[template_version] = dict(certificate)
    return json.dumps(
        {"schema_version": SCHEMA_VERSION, "certificates": certificates},
        ensure_ascii=False,
        separators=(",", ":"),
    )

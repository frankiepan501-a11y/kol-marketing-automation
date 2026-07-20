"""Customer-service official resource resolver.

This module keeps customer-facing URLs deterministic. The CS drafter may use
only resources returned here; it must not invent firmware files, placeholder
links, or attachment claims.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import time
from html import unescape
from typing import Callable, Iterable

import httpx

from . import feishu

CS_APP = os.environ.get("CS_TICKET_APP_TOKEN", "J2fibLgBZaLGTNsQOPHcQXLonZe")
T_TICKET = os.environ.get("CS_TICKET_TABLE_ID", "tblAhXMA9uDbGEMS")
T_RESOURCE = os.environ.get("CS_RESOURCE_TABLE_ID", "tblY3HNzoPPxqQPg")
WRITEBACK_TICKET_FIELDS = (os.environ.get("CS_RESOURCE_WRITEBACK_FIELDS", "1") or "1") == "1"

UPGRADE_FIRMWARE_URL = "https://funlabswitch.com/pages/upgrade-firmware"
FIRMWARE_MANUAL_URL = "https://funlabswitch.com/pages/firmware-update-process"
HOW_TO_VIDEO_URL = "https://funlabswitch.com/pages/how-to-video"
FIREFLY_YOUTUBE_PLAYLIST = "https://www.youtube.com/playlist?list=PLwaEVgYNxSXxVeNPMUGM3Jwk5GDcxb1Zo"

RESOURCE_FIELD_NAMES = [
    "资源键", "品牌", "系列", "型号", "资源类型", "问题标签", "标题",
    "公开 URL", "来源页面", "适用条件 JSON", "语言", "状态",
    "last_checked", "content_hash",
]

ACTIVE_STATUS = "已上线"
RESOURCE_TYPE_LABELS = {
    "firmware_download": "固件下载",
    "firmware_manual": "升级手册",
    "firmware_guide_page": "升级说明页",
    "how_to_video_page": "指引视频页",
    "how_to_video": "指引视频",
    "youtube_playlist": "YouTube合集",
}

MODEL_SERIES = {
    "FF01": "Firefly",
    "FF05": "Luminex",
}

FIRMWARE_VERSION_ROWS = {
    "V203": {
        "series": "Firefly",
        "model": "FF01",
        "current_versions": ["V180", "V191", "V199"],
        "target_version": "V203",
        "issue_tags": ["firmware", "one-key wake-up"],
    },
    "V204": {
        "series": "Firefly",
        "model": "FF01",
        "current_versions": ["V192", "V198"],
        "target_version": "V204",
        "issue_tags": ["firmware", "one-key wake-up"],
    },
    "V454": {
        "series": "Luminex",
        "model": "FF05",
        "current_versions": ["V412", "V447", "V453"],
        "target_version": "V454",
        "issue_tags": ["firmware", "rumble", "one-key wake-up"],
    },
    "V459": {
        "series": "Luminex",
        "model": "FF05",
        "current_versions": ["V417", "V432", "V444"],
        "target_version": "V459",
        "issue_tags": ["firmware", "rumble", "one-key wake-up"],
    },
    "V189": {
        "series": "Firefly",
        "model": "FF01",
        "current_versions": [],
        "target_version": "V189",
        "issue_tags": ["firmware", "abxy"],
    },
    "V190": {
        "series": "Firefly",
        "model": "FF01",
        "current_versions": [],
        "target_version": "V190",
        "issue_tags": ["firmware", "abxy"],
    },
    "V198": {
        "series": "Firefly",
        "model": "FF01",
        "current_versions": [],
        "target_version": "V198",
        "issue_tags": ["firmware", "abxy"],
    },
    "V600": {
        "series": "Firefly-Z",
        "model": "Firefly-Z",
        "current_versions": [],
        "target_version": "V600",
        "issue_tags": ["firmware", "factory"],
    },
    "V603": {
        "series": "Firefly-Z",
        "model": "Firefly-Z",
        "current_versions": [],
        "target_version": "V603",
        "issue_tags": ["firmware", "factory"],
    },
}


def _now_ms() -> int:
    return int(time.time() * 1000)


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _strip_tags(s: str) -> str:
    s = re.sub(r"<[^>]+>", " ", s or "")
    return _norm(unescape(s))


def _hash(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()[:16]


def _resource_key(resource: dict) -> str:
    raw = "|".join([
        resource.get("brand", ""),
        resource.get("series", ""),
        resource.get("model", ""),
        resource.get("resource_type", ""),
        resource.get("title", ""),
        resource.get("url", ""),
    ])
    return _hash(raw)


def _resource(
    *,
    brand: str = "FUNLAB",
    series: str = "",
    model: str = "",
    resource_type: str,
    title: str,
    url: str,
    source_page: str,
    conditions: dict | None = None,
    issue_tags: list[str] | None = None,
    language: str = "EN",
    status: str = ACTIVE_STATUS,
    content: str = "",
    last_checked: int | None = None,
) -> dict:
    r = {
        "brand": brand,
        "series": series,
        "model": model,
        "resource_type": resource_type,
        "issue_tags": issue_tags or [],
        "title": title,
        "url": url,
        "source_page": source_page,
        "conditions": conditions or {},
        "language": language,
        "status": status,
        "last_checked": last_checked or _now_ms(),
        "content_hash": _hash(content or (title + url + json.dumps(conditions or {}, sort_keys=True))),
    }
    r["resource_key"] = _resource_key(r)
    return r


def parse_upgrade_firmware_html(html: str, source_url: str = UPGRADE_FIRMWARE_URL) -> list[dict]:
    """Extract official firmware download rows from the FUNLAB firmware page."""
    resources: list[dict] = [
        _resource(
            resource_type="firmware_guide_page",
            title="FUNLAB Upgrade Firmware page",
            url=source_url,
            source_page=source_url,
            conditions={"purpose": "firmware upgrade steps and download table"},
            issue_tags=["firmware", "upgrade"],
            content=html,
        )
    ]
    seen = {resources[0]["resource_key"]}
    anchor_re = re.compile(r"<a\b[^>]*href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>", re.I | re.S)
    for href, label_html in anchor_re.findall(html or ""):
        label = _strip_tags(label_html)
        m = re.search(r"\b(V\d{3,4})\b", label, re.I)
        if not m or "drive.google.com" not in href:
            continue
        version = m.group(1).upper()
        meta = FIRMWARE_VERSION_ROWS.get(version)
        if not meta:
            continue
        r = _resource(
            series=meta["series"],
            model=meta["model"],
            resource_type="firmware_download",
            title=f"FUNLAB {meta['series']} {meta['model']} firmware {version}",
            url=unescape(href),
            source_page=source_url,
            conditions={
                "current_versions": meta.get("current_versions", []),
                "target_version": meta["target_version"],
                "version_selector_required": bool(meta.get("current_versions")),
            },
            issue_tags=meta.get("issue_tags", []),
            content=label + href,
        )
        if r["resource_key"] not in seen:
            seen.add(r["resource_key"])
            resources.append(r)
    return resources


def parse_firmware_manual_html(html: str, source_url: str = FIRMWARE_MANUAL_URL) -> list[dict]:
    resources: list[dict] = []
    anchor_re = re.compile(r"<a\b[^>]*href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>", re.I | re.S)
    for href, label_html in anchor_re.findall(html or ""):
        label = _strip_tags(label_html).lower()
        if "drive.google.com" not in href:
            continue
        if "english" in label:
            lang = "EN"
            title = "FUNLAB firmware update manual - English"
        elif "chinese" in label:
            lang = "ZH"
            title = "FUNLAB firmware update manual - Chinese"
        else:
            continue
        resources.append(_resource(
            resource_type="firmware_manual",
            title=title,
            url=unescape(href),
            source_page=source_url,
            conditions={"purpose": "firmware update manual"},
            issue_tags=["firmware", "manual", "upgrade"],
            language=lang,
            content=label + href,
        ))
    return _dedupe(resources)


def _series_from_heading(heading: str) -> str:
    h = (heading or "").lower()
    if "luminex" in h:
        return "Luminex"
    if "firefly" in h:
        return "Firefly"
    if "luminous" in h:
        return "Luminous"
    if "luminpad" in h:
        return "Luminpad"
    if "lumingrip" in h:
        return "Lumingrip"
    return ""


def parse_how_to_video_html(html: str, source_url: str = HOW_TO_VIDEO_URL) -> list[dict]:
    """Extract Shopline self-hosted how-to videos from Video-data-* JSON blocks."""
    resources = [
        _resource(
            resource_type="how_to_video_page",
            title="FUNLAB How-to Video page",
            url=source_url,
            source_page=source_url,
            conditions={"purpose": "series-level how-to video directory"},
            issue_tags=["how-to", "video"],
            content=html,
        )
    ]
    script_re = re.compile(
        r"<script\b[^>]*id=[\"']Video-data-[^\"']+[\"'][^>]*>(.*?)</script>",
        re.I | re.S,
    )
    for m in script_re.finditer(html or ""):
        raw = unescape(m.group(1) or "").strip()
        try:
            data = json.loads(raw)
        except Exception:
            continue
        url = data.get("video_url") or ""
        title = _norm(data.get("sub_title") or "")
        if not url or not title or title.lower() == "example title":
            continue
        prefix = html[:m.start()]
        h2 = re.findall(r"<h[123]\b[^>]*>(.*?)</h[123]>", prefix, re.I | re.S)
        heading = _strip_tags(h2[-1]) if h2 else ""
        series = _series_from_heading(heading)
        resources.append(_resource(
            series=series,
            model="",
            resource_type="how_to_video",
            title=f"{series + ' - ' if series else ''}{title}",
            url=unescape(url),
            source_page=source_url,
            conditions={"section": heading, "topic": title},
            issue_tags=["how-to", "video", _topic_tag(title)],
            content=raw,
        ))
    return _dedupe(resources)


def youtube_playlist_resource() -> dict:
    return _resource(
        series="Firefly",
        model="FF01",
        resource_type="youtube_playlist",
        title="FUNLAB Firefly How-to video playlist",
        url=FIREFLY_YOUTUBE_PLAYLIST,
        source_page=FIREFLY_YOUTUBE_PLAYLIST,
        conditions={"series_scope": "Firefly only"},
        issue_tags=["how-to", "video", "firefly"],
        content=FIREFLY_YOUTUBE_PLAYLIST,
    )


def builtin_resources() -> list[dict]:
    """Known-good resources verified from official pages.

    This fallback lets dispatch/validation stay useful before the Bitable truth
    table is created or while the indexer is unavailable.
    """
    return [
        _resource(
            resource_type="firmware_guide_page",
            title="FUNLAB Upgrade Firmware page",
            url=UPGRADE_FIRMWARE_URL,
            source_page=UPGRADE_FIRMWARE_URL,
            conditions={"purpose": "firmware upgrade steps and download table"},
            issue_tags=["firmware", "upgrade"],
        ),
        _resource(
            series="Luminex",
            model="FF05",
            resource_type="firmware_download",
            title="FUNLAB Luminex FF05 firmware V454",
            url="https://drive.google.com/drive/folders/12Pj09f83wBIdCce2hHEkhVdQuqxZYehW?usp=sharing",
            source_page=UPGRADE_FIRMWARE_URL,
            conditions={"current_versions": ["V412", "V447", "V453"], "target_version": "V454", "version_selector_required": True},
            issue_tags=["firmware", "rumble", "one-key wake-up"],
        ),
        _resource(
            series="Luminex",
            model="FF05",
            resource_type="firmware_download",
            title="FUNLAB Luminex FF05 firmware V459",
            url="https://drive.google.com/drive/folders/1J_WkY5mKiUrttYUQFzNlpInJUsmBDMJZ?usp=sharing",
            source_page=UPGRADE_FIRMWARE_URL,
            conditions={"current_versions": ["V417", "V432", "V444"], "target_version": "V459", "version_selector_required": True},
            issue_tags=["firmware", "rumble", "one-key wake-up"],
        ),
        _resource(
            resource_type="firmware_manual",
            title="FUNLAB firmware update manual - English",
            url="https://drive.google.com/drive/folders/10ZQLVbKGgmzGP-BP7kvtUlf9HN3g-rnN?usp=sharing",
            source_page=FIRMWARE_MANUAL_URL,
            conditions={"purpose": "firmware update manual"},
            issue_tags=["firmware", "manual", "upgrade"],
            language="EN",
        ),
        _resource(
            resource_type="firmware_manual",
            title="FUNLAB firmware update manual - Chinese",
            url="https://drive.google.com/drive/folders/1z5wfyPppT6x7qMH8bJGByD82K970aBhb?usp=sharing",
            source_page=FIRMWARE_MANUAL_URL,
            conditions={"purpose": "firmware update manual"},
            issue_tags=["firmware", "manual", "upgrade"],
            language="ZH",
        ),
        _resource(
            resource_type="how_to_video_page",
            title="FUNLAB How-to Video page",
            url=HOW_TO_VIDEO_URL,
            source_page=HOW_TO_VIDEO_URL,
            conditions={"purpose": "series-level how-to video directory"},
            issue_tags=["how-to", "video"],
        ),
        youtube_playlist_resource(),
    ]


def _topic_tag(title: str) -> str:
    s = (title or "").lower()
    if "connect" in s:
        return "connect"
    if "turbo" in s:
        return "turbo"
    if "vibration" in s or "motor" in s:
        return "vibration"
    if "mapping" in s:
        return "mapping"
    if "lighting" in s:
        return "lighting"
    return "how-to"


def _dedupe(resources: Iterable[dict]) -> list[dict]:
    out: list[dict] = []
    seen: set[str] = set()
    for r in resources:
        key = r.get("resource_key") or _resource_key(r)
        r["resource_key"] = key
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


async def _fetch(url: str) -> str:
    async with httpx.AsyncClient(timeout=45.0, headers={"User-Agent": "Mozilla/5.0 cs-resource-indexer"}) as c:
        r = await c.get(url)
        r.raise_for_status()
        return r.text


async def index_official_resources(fetcher: Callable[[str], object] | None = None) -> list[dict]:
    """Fetch and parse official FUNLAB resource pages."""
    fetcher = fetcher or _fetch
    resources: list[dict] = []
    fetched = {
        UPGRADE_FIRMWARE_URL: await fetcher(UPGRADE_FIRMWARE_URL),
        FIRMWARE_MANUAL_URL: await fetcher(FIRMWARE_MANUAL_URL),
        HOW_TO_VIDEO_URL: await fetcher(HOW_TO_VIDEO_URL),
    }
    resources.extend(parse_upgrade_firmware_html(fetched[UPGRADE_FIRMWARE_URL]))
    resources.extend(parse_firmware_manual_html(fetched[FIRMWARE_MANUAL_URL]))
    resources.extend(parse_how_to_video_html(fetched[HOW_TO_VIDEO_URL]))
    resources.append(youtube_playlist_resource())
    return _dedupe(resources)


def _field_text(v) -> str:
    if v is None:
        return ""
    if isinstance(v, dict):
        return str(v.get("text") or v.get("name") or v.get("link") or "")
    if isinstance(v, list):
        parts = []
        for item in v:
            parts.append(_field_text(item))
        return "".join(parts)
    return str(v)


def _field_url(v) -> str:
    if isinstance(v, dict):
        return str(v.get("link") or v.get("text") or "")
    if isinstance(v, list) and v:
        return _field_url(v[0])
    return _field_text(v)


def _parse_json_obj(s: str) -> dict:
    try:
        obj = json.loads(s or "{}")
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def resource_from_fields(fields: dict, record_id: str = "") -> dict:
    r = {
        "record_id": record_id,
        "resource_key": _field_text(fields.get("资源键")),
        "brand": _field_text(fields.get("品牌")),
        "series": _field_text(fields.get("系列")),
        "model": _field_text(fields.get("型号")),
        "resource_type": _field_text(fields.get("资源类型")),
        "issue_tags": [x.strip() for x in re.split(r"[,，/ ]+", _field_text(fields.get("问题标签"))) if x.strip()],
        "title": _field_text(fields.get("标题")),
        "url": _field_url(fields.get("公开 URL")),
        "source_page": _field_url(fields.get("来源页面")),
        "conditions": _parse_json_obj(_field_text(fields.get("适用条件 JSON"))),
        "language": _field_text(fields.get("语言")) or "EN",
        "status": _field_text(fields.get("状态")),
        "last_checked": fields.get("last_checked") or 0,
        "content_hash": _field_text(fields.get("content_hash")),
    }
    if not r["resource_key"]:
        r["resource_key"] = _resource_key(r)
    return r


def resource_to_fields(resource: dict) -> dict:
    return {
        "资源键": resource.get("resource_key") or _resource_key(resource),
        "品牌": resource.get("brand", ""),
        "系列": resource.get("series", ""),
        "型号": resource.get("model", ""),
        "资源类型": resource.get("resource_type", ""),
        "问题标签": ", ".join(resource.get("issue_tags") or []),
        "标题": resource.get("title", ""),
        "公开 URL": resource.get("url", ""),
        "来源页面": resource.get("source_page", ""),
        "适用条件 JSON": json.dumps(resource.get("conditions") or {}, ensure_ascii=False, sort_keys=True),
        "语言": resource.get("language", "EN"),
        "状态": resource.get("status", ACTIVE_STATUS),
        "last_checked": resource.get("last_checked") or _now_ms(),
        "content_hash": resource.get("content_hash", ""),
    }


async def load_table_resources() -> list[dict]:
    if not T_RESOURCE:
        return []
    rows = []
    page = ""
    while True:
        path = f"/bitable/v1/apps/{CS_APP}/tables/{T_RESOURCE}/records/search?page_size=500"
        if page:
            path += f"&page_token={page}"
        body = {"field_names": RESOURCE_FIELD_NAMES}
        d = await feishu.api("POST", path, body, which="notify")
        data = d.get("data") or {}
        for item in data.get("items") or []:
            rows.append(resource_from_fields(item.get("fields") or {}, item.get("record_id", "")))
        if not data.get("has_more"):
            break
        page = data.get("page_token", "")
        if not page:
            break
    return rows


async def active_resources() -> list[dict]:
    try:
        rows = await load_table_resources()
        active = [r for r in rows if (r.get("status") or ACTIVE_STATUS) == ACTIVE_STATUS and r.get("url")]
        if active:
            return active
    except Exception as e:
        print(f"[cs_resources.active_resources] fallback builtin: {e}")
    return builtin_resources()


async def upsert_resources(resources: list[dict], dry_run: bool = True) -> dict:
    if not T_RESOURCE:
        return {"ok": False, "error": "CS_RESOURCE_TABLE_ID not set", "planned": len(resources)}
    existing = {r["resource_key"]: r for r in await load_table_resources()}
    planned_create, planned_update = [], []
    for r in resources:
        key = r.get("resource_key") or _resource_key(r)
        r["resource_key"] = key
        if key in existing:
            planned_update.append(r)
        else:
            planned_create.append(r)
    if dry_run:
        return {"ok": True, "dry_run": True, "create": len(planned_create), "update": len(planned_update)}
    for r in planned_create:
        await feishu.api(
            "POST",
            f"/bitable/v1/apps/{CS_APP}/tables/{T_RESOURCE}/records",
            {"fields": resource_to_fields(r)},
            which="notify",
        )
    for r in planned_update:
        await feishu.api(
            "PUT",
            f"/bitable/v1/apps/{CS_APP}/tables/{T_RESOURCE}/records/{existing[r['resource_key']]['record_id']}",
            {"fields": resource_to_fields(r)},
            which="notify",
        )
    return {"ok": True, "dry_run": False, "create": len(planned_create), "update": len(planned_update)}


async def run_index(commit: bool = False) -> dict:
    resources = await index_official_resources()
    write = await upsert_resources(resources, dry_run=not commit) if T_RESOURCE else {
        "ok": False,
        "error": "CS_RESOURCE_TABLE_ID not set; parsed only",
    }
    return {
        "parsed": len(resources),
        "commit": commit,
        "write": write,
        "samples": [
            {
                "type": r.get("resource_type"),
                "series": r.get("series"),
                "model": r.get("model"),
                "title": r.get("title"),
                "url": r.get("url"),
            }
            for r in resources[:12]
        ],
    }


def _infer_model(text: str) -> str:
    s = (text or "").upper()
    m = re.search(r"\b(FF0[15])[A-Z0-9-]*\b", s)
    if m:
        return m.group(1)
    if "LUMINEX" in s:
        return "FF05"
    if "FIREFLY" in s:
        return "FF01"
    return ""


def _infer_series(text: str, model: str = "") -> str:
    s = (text or "").lower()
    if model in MODEL_SERIES:
        return MODEL_SERIES[model]
    return _series_from_heading(s)


def _infer_current_version(text: str) -> str:
    versions = [v.upper() for v in re.findall(r"\bV\d{3,4}\b", text or "", re.I)]
    if not versions:
        return ""
    # If the customer mentions several versions, treat the first one as context;
    # dispatch card will still show ambiguity when it cannot map exactly.
    return versions[0]


def _infer_needs(text: str, brand: str, model: str, series: str) -> list[str]:
    s = (text or "").lower()
    needs: list[str] = []
    firmware_terms = any(k in s for k in ["firmware", "固件", "update", "upgrade", "升级"])
    vibration_terms = any(k in s for k in ["vibrat", "rumble", "震动", "马达"])
    reset_terms = any(k in s for k in ["factory reset", "reset", "重置"])
    howto_terms = any(k in s for k in ["how to", "connect", "pair", "bluetooth", "turbo", "mapping", "nfc", "连接", "配对"])
    if brand == "FUNLAB" and (firmware_terms or (model == "FF05" and (vibration_terms or reset_terms))):
        needs.extend(["firmware_download", "firmware_manual", "how_to_video"])
    elif brand == "FUNLAB" and howto_terms:
        needs.append("how_to_video")
    # Keep order stable and unique.
    return list(dict.fromkeys(needs))


def _resource_matches_base(r: dict, brand: str, model: str, series: str, resource_type: str) -> bool:
    if resource_type != r.get("resource_type"):
        return False
    if brand and r.get("brand") and r["brand"] != brand:
        return False
    if model and r.get("model") and r["model"] != model:
        return False
    if series and r.get("series") and r["series"].lower() != series.lower():
        return False
    return True


def resolve_for_ticket(fields: dict, resources: list[dict] | None = None) -> dict:
    text = "\n".join([
        _field_text(fields.get("品牌")),
        _field_text(fields.get("产品")),
        _field_text(fields.get("客诉摘要")),
        _field_text(fields.get("原文")),
    ])
    brand = _field_text(fields.get("品牌")) or ("FUNLAB" if "funlab" in text.lower() else "")
    model = _infer_model(text)
    series = _infer_series(text, model)
    current_version = _infer_current_version(text)
    needs = _infer_needs(text, brand, model, series)
    resources = resources if resources is not None else builtin_resources()
    active = [r for r in resources if (r.get("status") or ACTIVE_STATUS) == ACTIVE_STATUS and r.get("url")]
    matches: list[dict] = []
    missing: list[str] = []
    ambiguities: list[str] = []

    if "firmware_download" in needs:
        candidates = [
            r for r in active
            if _resource_matches_base(r, brand, model, series, "firmware_download")
        ]
        if current_version:
            exact = [
                r for r in candidates
                if current_version in [v.upper() for v in (r.get("conditions") or {}).get("current_versions", [])]
            ]
            if exact:
                matches.extend(exact)
            elif candidates:
                ambiguities.append(
                    f"当前版本 {current_version} 未命中已知固件条件，请客户截图确认。"
                )
                matches.extend(candidates)
            else:
                missing.append("固件下载")
        elif len(candidates) > 1:
            ambiguities.append("缺少当前固件版本；必须按升级工具显示的版本让客户选择对应下载。")
            matches.extend(candidates)
        elif candidates:
            matches.extend(candidates)
        else:
            missing.append("固件下载")

    if "firmware_manual" in needs:
        manuals = [r for r in active if r.get("resource_type") == "firmware_manual" and r.get("brand") in ("", brand)]
        guide_pages = [r for r in active if r.get("resource_type") == "firmware_guide_page" and r.get("brand") in ("", brand)]
        if manuals or guide_pages:
            matches.extend(guide_pages[:1])
            # Prefer English first, keep Chinese as supporting resource if present.
            matches.extend(sorted(manuals, key=lambda r: 0 if r.get("language") == "EN" else 1))
        else:
            missing.append("升级手册")

    if "how_to_video" in needs:
        pages = [r for r in active if r.get("resource_type") == "how_to_video_page" and r.get("brand") in ("", brand)]
        videos = [
            r for r in active
            if r.get("resource_type") == "how_to_video"
            and (not series or not r.get("series") or r.get("series", "").lower() == series.lower())
        ]
        playlists = [
            r for r in active
            if r.get("resource_type") == "youtube_playlist"
            and (not series or r.get("series", "").lower() == series.lower())
        ]
        if pages:
            matches.extend(pages[:1])
        if videos:
            # For email/card readability use the first few exact-series videos.
            matches.extend(videos[:3])
        elif playlists:
            matches.extend(playlists[:1])
        elif not pages:
            missing.append("指引视频")

    matches = _dedupe(matches)
    if missing:
        status = "缺资源"
    elif ambiguities:
        status = "有歧义"
    elif needs:
        status = "已命中"
    else:
        status = "无需资源"
    return {
        "brand": brand,
        "series": series,
        "model": model,
        "current_version": current_version,
        "needs": needs,
        "status": status,
        "matches": matches,
        "missing": missing,
        "ambiguities": ambiguities,
    }


def ticket_resource_fields(context: dict) -> dict:
    return {
        "资源状态": context.get("status", ""),
        "资源需求JSON": json.dumps({
            "brand": context.get("brand"),
            "series": context.get("series"),
            "model": context.get("model"),
            "current_version": context.get("current_version"),
            "needs": context.get("needs"),
            "missing": context.get("missing"),
            "ambiguities": context.get("ambiguities"),
        }, ensure_ascii=False),
        "资源命中JSON": json.dumps([
            {
                "resource_key": r.get("resource_key"),
                "resource_type": r.get("resource_type"),
                "title": r.get("title"),
                "url": r.get("url"),
                "source_page": r.get("source_page"),
                "conditions": r.get("conditions"),
                "series": r.get("series"),
                "model": r.get("model"),
            }
            for r in context.get("matches") or []
        ], ensure_ascii=False),
    }


def _link_md(title: str, url: str) -> str:
    safe_title = (title or url or "").replace("[", "(").replace("]", ")")
    return f"[{safe_title}]({url})" if url else safe_title


def format_card_block(context: dict) -> str:
    status = context.get("status")
    if status == "无需资源":
        return ""
    lines = [f"**🔗 官方资源状态:** {status}"]
    if context.get("brand") or context.get("series") or context.get("model"):
        lines.append(
            f"**匹配对象:** {context.get('brand') or '-'} / {context.get('series') or '-'} / {context.get('model') or '-'}"
        )
    if context.get("current_version"):
        lines.append(f"**识别到当前版本:** {context['current_version']}")
    for msg in context.get("ambiguities") or []:
        lines.append(f"⚠️ {msg}")
    for msg in context.get("missing") or []:
        lines.append(f"❌ 缺少资源：{msg}")
    for r in context.get("matches") or []:
        cond = r.get("conditions") or {}
        suffix = ""
        if cond.get("current_versions"):
            suffix = f"（适用当前版本: {', '.join(cond['current_versions'])} → {cond.get('target_version', '')}）"
        elif r.get("series"):
            suffix = f"（{r.get('series')}）"
        lines.append(f"- {RESOURCE_TYPE_LABELS.get(r.get('resource_type'), r.get('resource_type'))}: {_link_md(r.get('title'), r.get('url'))} {suffix}")
    return "\n".join(lines)


def prompt_context(context: dict) -> str:
    if context.get("status") == "无需资源":
        return ""
    lines = [
        "OFFICIAL_RESOURCE_CONTEXT:",
        "Use only the URLs below. Do not invent links. Do not say any file is attached.",
        f"Resource status: {context.get('status')}",
    ]
    for msg in context.get("ambiguities") or []:
        lines.append(f"Ambiguity: {msg}")
    for msg in context.get("missing") or []:
        lines.append(f"Missing: {msg}")
    for r in context.get("matches") or []:
        lines.append(json.dumps({
            "type": r.get("resource_type"),
            "title": r.get("title"),
            "url": r.get("url"),
            "conditions": r.get("conditions"),
            "series": r.get("series"),
            "model": r.get("model"),
        }, ensure_ascii=False))
    return "\n".join(lines)


def _first_name(customer: str) -> str:
    email = _field_text(customer)
    local = email.split("@", 1)[0]
    part = re.split(r"[._\-\s]+", local)[0]
    return part[:1].upper() + part[1:] if part and part.isalpha() else ""


def build_resource_reply(fields: dict, context: dict) -> str:
    """Build deterministic reply for firmware cases where official URLs matter."""
    if "firmware_download" not in (context.get("needs") or []):
        return ""
    firmware = [r for r in context.get("matches") or [] if r.get("resource_type") == "firmware_download"]
    if not firmware:
        return ""
    name = _first_name(fields.get("客户标识")) or "there"
    lines = [
        f"Hello {name},",
        "",
        "Thank you for the update. Since the factory reset did not resolve the vibration issue, please use the official FUNLAB firmware package that matches the current firmware version shown in the upgrade tool.",
        "",
    ]
    if len(firmware) > 1:
        lines.append("Please choose the download according to the current version number shown by the upgrade program:")
    else:
        lines.append("Official firmware download:")
    for r in firmware:
        cond = r.get("conditions") or {}
        versions = cond.get("current_versions") or []
        target = cond.get("target_version") or ""
        if versions:
            lines.append(f"- If the current version shows {', '.join(versions)}, download {target}: {r['url']}")
        else:
            lines.append(f"- {r.get('title')}: {r['url']}")
    lines.extend(["", "Upgrade resources:"])
    added = set()
    for r in context.get("matches") or []:
        if r.get("resource_type") == "firmware_download":
            continue
        url = r.get("url", "")
        if not url or url in added:
            continue
        added.add(url)
        lines.append(f"- {r.get('title')}: {url}")
    lines.extend([
        "",
        "Please do not install a firmware package if the version shown by the upgrade tool is different from the listed applicable versions. If you see V411 or any other version not listed above, please send us a screenshot of the upgrade tool before proceeding.",
        "",
        "Best regards,",
        "FUNLAB Support Team",
    ])
    return "\n".join(lines)


FALSE_ATTACHMENT_RE = re.compile(
    r"\b(attached|attachment|enclosed)\b|find\s+the\s+firmware\s+file\s+attached|file\s+attached",
    re.I,
)
PLACEHOLDER_URL_RE = re.compile(r"(\[link\]|\[url\]|\(link\)|<link>|https?://example\.com)", re.I)
REPLY_URL_RE = re.compile(r"https?://[^\s<>'\")]+", re.I)


def _canonical_url_key(url: str) -> str:
    raw = unescape((url or "").strip()).rstrip(".,;])")
    m = re.search(r"drive\.google\.com/drive/folders/([^/?#]+)", raw, re.I)
    if m:
        return "gdrive-folder:" + m.group(1)
    return re.sub(r"[?#].*$", "", raw).rstrip("/").lower()


def _reply_url_keys(text: str) -> set[str]:
    return {_canonical_url_key(url) for url in REPLY_URL_RE.findall(text or "")}


def _matched_urls_present(required_urls: list[str], text: str) -> list[str]:
    keys = _reply_url_keys(text)
    present = []
    for url in required_urls:
        if url and (_canonical_url_key(url) in keys or url in text):
            present.append(url)
    return present


def _must_include_all_firmware_urls(context: dict, firmware_urls: list[str]) -> bool:
    return (
        context.get("model") == "FF05"
        and not context.get("current_version")
        and len([u for u in firmware_urls if u]) > 1
    )


def validate_reply_for_ticket(reply: str, fields: dict, resources: list[dict] | None = None) -> str:
    """Return blocking reason, or empty string if safe enough to send."""
    text = reply or ""
    if FALSE_ATTACHMENT_RE.search(text):
        return "回复声称有附件，但当前客服发送链路没有附件 manifest，不能发。"
    if PLACEHOLDER_URL_RE.search(text):
        return "回复含占位链接，请替换成真实官方 URL。"
    context = resolve_for_ticket(fields, resources=resources)
    if context.get("status") == "缺资源":
        return "该工单需要官方资源，但资源真相源缺失，不能默认发送。"
    if "firmware_download" in (context.get("needs") or []):
        firmware_urls = [r.get("url") for r in context.get("matches") or [] if r.get("resource_type") == "firmware_download"]
        firmware_urls = [u for u in firmware_urls if u]
        present_urls = _matched_urls_present(firmware_urls, text)
        if firmware_urls and not present_urls:
            return "回复涉及固件下载，但没有包含命中的官方固件 URL。"
        if _must_include_all_firmware_urls(context, firmware_urls):
            present_keys = {_canonical_url_key(u) for u in present_urls}
            missing_urls = [u for u in firmware_urls if _canonical_url_key(u) not in present_keys]
            if missing_urls:
                return "回复涉及多个候选固件下载，但没有包含全部需客户选择的官方固件 URL。"
    return ""

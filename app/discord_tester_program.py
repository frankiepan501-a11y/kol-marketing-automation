"""FUNLAB Discord private product tester program.

This module owns the deterministic Discord interaction flow.  Public
applications stay inside Discord; proof and shipping details use signed web
forms and are never requested in a Discord message.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import time
import unicodedata
from dataclasses import dataclass
from typing import Awaitable, Callable, Optional


Work = Callable[[], Awaitable[None]]
CompletionNotifier = Callable[[str], Awaitable[None]]
_drafts: dict[str, tuple[float, dict]] = {}
_DRAFT_TTL_SECONDS = 30 * 60
_HOUR_CANONICAL = {"2-5": "2–5", "6-10": "6–10"}
_PC_SCORING_HOURS = {*_HOUR_CANONICAL.values(), "10+"}


def _require_feishu_ok(result: dict, action: str) -> dict:
    code = result.get("code", 0)
    if code not in (0, None):
        raise RuntimeError(f"Feishu {action} failed: code={code} msg={result.get('msg', '')}")
    return result


@dataclass
class InteractionOutcome:
    response: dict
    work: Optional[Work] = None


class DiscordTesterLedger:
    """Durable application ledger backed by the existing Feishu Bitable app."""

    def __init__(self, *, base_token: str = "KINabIENjak8fRsB6AHcIDALntc",
                 table_id: str = "tblt8oRYMtaa8B4v"):
        self.base_token = os.environ.get("DISCORD_TESTER_APPLICATION_BASE_TOKEN", base_token)
        self.table_id = os.environ.get("DISCORD_TESTER_APPLICATION_TABLE_ID", table_id)

    async def save_application(self, fields: dict) -> str:
        from . import feishu

        search = {
            "filter": {
                "conjunction": "and",
                "conditions": [
                    {"field_name": "Discord用户ID", "operator": "is", "value": [fields["Discord用户ID"]]},
                    {"field_name": "活动批次", "operator": "is", "value": [fields["活动批次"]]},
                ],
            },
            "page_size": 20,
        }
        path = f"/bitable/v1/apps/{self.base_token}/tables/{self.table_id}/records"
        found = _require_feishu_ok(
            await feishu.api("POST", f"{path}/search", search, which="bitable"),
            "application search",
        )
        items = ((found.get("data") or {}).get("items") or [])
        if items:
            record_id = items[0]["record_id"]
            _require_feishu_ok(
                await feishu.api("PUT", f"{path}/{record_id}", {"fields": fields}, which="bitable"),
                "application update",
            )
            return record_id
        created = _require_feishu_ok(
            await feishu.api("POST", path, {"fields": fields}, which="bitable"),
            "application create",
        )
        record = (created.get("data") or {}).get("record") or {}
        record_id = record.get("record_id") or (created.get("data") or {}).get("record_id") or ""
        if not record_id:
            raise RuntimeError("Feishu application create returned no record_id")
        return record_id

    async def update_application(self, record_id: str, fields: dict) -> None:
        from . import feishu
        path = f"/bitable/v1/apps/{self.base_token}/tables/{self.table_id}/records/{record_id}"
        _require_feishu_ok(
            await feishu.api("PUT", path, {"fields": fields}, which="bitable"),
            "application update",
        )

    async def get_application(self, record_id: str) -> dict:
        from . import feishu
        path = f"/bitable/v1/apps/{self.base_token}/tables/{self.table_id}/records/{record_id}"
        result = _require_feishu_ok(await feishu.api("GET", path, which="bitable"), "application read")
        record = (result.get("data") or {}).get("record") or {}
        return record.get("fields") or {}

    async def list_applications(self) -> list[dict]:
        from . import feishu
        path = f"/bitable/v1/apps/{self.base_token}/tables/{self.table_id}/records"
        items: list[dict] = []
        page_token = ""
        while True:
            query = "?page_size=500" + (f"&page_token={page_token}" if page_token else "")
            result = _require_feishu_ok(
                await feishu.api("GET", path + query, which="bitable"),
                "application list",
            )
            data = result.get("data") or {}
            items.extend(data.get("items") or [])
            if not data.get("has_more") or not data.get("page_token"):
                return items
            page_token = str(data["page_token"])


class DiscordTesterFeedbackLedger:
    def __init__(self, *, base_token: str = "KINabIENjak8fRsB6AHcIDALntc",
                 table_id: str = "tblVwzVNXVGu6ef5"):
        self.base_token = os.environ.get("DISCORD_TESTER_APPLICATION_BASE_TOKEN", base_token)
        self.table_id = os.environ.get("DISCORD_TESTER_FEEDBACK_TABLE_ID", table_id)

    async def create_feedback(self, fields: dict) -> str:
        from . import feishu
        path = f"/bitable/v1/apps/{self.base_token}/tables/{self.table_id}/records"
        created = _require_feishu_ok(
            await feishu.api("POST", path, {"fields": fields}, which="bitable"),
            "feedback create",
        )
        record = (created.get("data") or {}).get("record") or {}
        record_id = record.get("record_id") or (created.get("data") or {}).get("record_id") or ""
        if not record_id:
            raise RuntimeError("Feishu feedback create returned no record_id")
        return record_id

    async def update_feedback(self, record_id: str, fields: dict) -> None:
        from . import feishu
        path = f"/bitable/v1/apps/{self.base_token}/tables/{self.table_id}/records/{record_id}"
        _require_feishu_ok(
            await feishu.api("PUT", path, {"fields": fields}, which="bitable"),
            "feedback update",
        )


def _text_input(custom_id: str, label: str, *, placeholder: str = "", max_length: int = 200) -> dict:
    component = {
        "type": 4,
        "custom_id": custom_id,
        "label": label,
        "style": 1,
        "required": True,
        "max_length": max_length,
    }
    if placeholder:
        component["placeholder"] = placeholder
    return {"type": 1, "components": [component]}


def _step1_modal() -> dict:
    return {
        "type": 9,
        "data": {
            "custom_id": "tester_apply_v2_step1",
            "title": "Step 1 Of 2 — Eligibility",
            "components": [
                _text_input("country", "Country Or Region", placeholder="United States, Canada, Mexico, UK, DE, FR, IT, ES"),
                _text_input("age", "Are You 18 Or Older?", placeholder="Type YES"),
                _text_input("devices", "Devices You Actively Use", placeholder="Switch / Switch 2 / Steam Deck / PC Steam"),
                _text_input("amazon_24m", "Amazon Video Games Purchase In 24 Months?", placeholder="Type YES or NO"),
                _text_input("commit", "14-Day Test, Privacy And Rules?", placeholder="Type YES"),
            ],
        },
    }


def _values(data: dict) -> dict[str, str]:
    out: dict[str, str] = {}

    def walk(node):
        if isinstance(node, dict):
            cid = node.get("custom_id")
            if cid and "value" in node:
                out[str(cid)] = str(node.get("value") or "").strip()
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    walk(data.get("components") or [])
    return out


def _yes(value: str) -> bool:
    return value.strip().casefold() in {"yes", "y", "true", "1", "i agree"}


def _country_code(value: str) -> str:
    key = "".join(ch for ch in value.casefold() if ch.isalnum())
    aliases = {
        "us": "US", "usa": "US", "unitedstates": "US", "unitedstatesofamerica": "US",
        "ca": "CA", "canada": "CA", "mx": "MX", "mexico": "MX",
        "uk": "UK", "unitedkingdom": "UK", "greatbritain": "UK",
        "de": "DE", "germany": "DE", "fr": "FR", "france": "FR",
        "it": "IT", "italy": "IT", "es": "ES", "spain": "ES",
    }
    return aliases.get(key, "")


def _device_mask(value: str) -> int:
    text = value.casefold()
    mask = 0
    if "switch 2" in text or "switch2" in text:
        mask |= 2
    # Keep Switch 1 and Switch 2 distinct in the ledger while allowing either
    # device to satisfy the Nintendo Switch eligibility requirement.
    switch_1_text = text.replace("switch 2", "").replace("switch2", "")
    if "switch" in switch_1_text:
        mask |= 1
    if "steam deck" in text or "steamdeck" in text:
        mask |= 4
    non_deck_text = text.replace("steam deck", "").replace("steamdeck", "")
    if "pc" in non_deck_text or "steam" in non_deck_text:
        mask |= 8
    return mask


def _sign_state(state: str, secret: str) -> str:
    digest = hmac.new(secret.encode("utf-8"), state.encode("ascii"), hashlib.sha256).digest()[:8]
    sig = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
    return f"{state}.{sig}"


def _verify_state(token: str, secret: str) -> str:
    try:
        state, supplied = token.rsplit(".", 1)
    except ValueError:
        return ""
    expected = _sign_state(state, secret).rsplit(".", 1)[1]
    return state if hmac.compare_digest(supplied, expected) else ""


def issue_form_token(kind: str, record_id: str, discord_user_id: str,
                     ttl_seconds: int, secret: str) -> str:
    claims = {
        "kind": kind,
        "record_id": record_id,
        "discord_user_id": discord_user_id,
        "exp": int(time.time()) + max(60, int(ttl_seconds)),
    }
    raw = json.dumps(claims, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
    payload = base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")
    return _sign_state(payload, secret)


def read_form_token(token: str, expected_kind: str, secret: str, *, now: int | None = None) -> dict:
    payload = _verify_state(token, secret)
    if not payload:
        return {}
    try:
        raw = base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4))
        claims = json.loads(raw)
        current = int(time.time() if now is None else now)
        if claims.get("kind") != expected_kind or int(claims.get("exp") or 0) < current:
            return {}
        if not claims.get("record_id") or not claims.get("discord_user_id"):
            return {}
        return claims
    except (TypeError, ValueError, UnicodeError, json.JSONDecodeError):
        return {}


def _form_platforms(value: str) -> list[str]:
    mask = _device_mask(value)
    out = []
    if mask & 1:
        out.append("Switch 1")
    if mask & 2:
        out.append("Switch 2")
    if mask & 4:
        out.append("Steam Deck")
    if mask & 8:
        out.append("PC / Steam")
    return out


def build_form_writes(kind: str, form: dict[str, str], claims: dict) -> tuple[dict, dict]:
    """Map one secure form submission to the application and feedback ledgers."""
    now_ms = int(time.time() * 1000)
    app: dict = {"最近更新时间": now_ms}
    feedback: dict = {}
    if kind == "verification":
        app.update({
            "核验邮箱": form.get("email", "")[:300],
            "核验状态": "待核验",
            "报名状态": "待核验",
            "核验资料删除日": now_ms + 30 * 24 * 60 * 60 * 1000,
        })
    elif kind == "shipping":
        address = "\n".join(part for part in [
            form.get("address_line_1", ""), form.get("address_line_2", ""),
            ", ".join(part for part in [form.get("city", ""), form.get("region", ""), form.get("postal_code", "")] if part),
            form.get("country", ""), form.get("carrier_notes", ""),
        ] if part)
        app.update({
            "收件姓名": form.get("legal_name", "")[:300],
            "联系电话": form.get("phone", "")[:100],
            "核验邮箱": form.get("email", "")[:300],
            "收件地址": address[:5000],
            "配送状态": "待发货",
        })
        feedback = {}
    else:
        type_map = {
            "receipt": "收货确认", "checkpoint1": "Checkpoint 1", "checkpoint2": "Checkpoint 2",
            "final": "Final", "emergency": "紧急安全上报", "logistics": "物流异常",
        }
        feedback = {
            "报名记录ID": claims.get("record_id", ""),
            "Discord用户ID": claims.get("discord_user_id", ""),
            "反馈类型": type_map[kind],
            "反馈摘要": (form.get("summary") or form.get("notes") or form.get("condition") or "")[:5000],
            "复现步骤": form.get("steps", "")[:5000],
            "发生频率": form.get("frequency", "")[:1000],
            "测试游戏与时长": form.get("games_hours", "")[:2000],
            "处理状态": "新提交",
            "提交时间": now_ms,
        }
        platforms = _form_platforms(form.get("platforms", ""))
        if platforms:
            feedback["测试平台"] = platforms
        if kind == "receipt":
            feedback["反馈摘要"] = "\n".join(filter(None, [
                f"Package: {form.get('condition', '')}",
                f"Contents: {form.get('contents', '')}",
                f"Instructions: {form.get('instructions', '')}",
                f"Setup: {form.get('setup', '')}",
                f"First impressions: {form.get('first_impressions', '')}",
                form.get("notes", ""),
            ]))[:5000]
            receipt_platforms = _form_platforms(form.get("platforms", ""))
            if receipt_platforms:
                feedback["测试平台"] = receipt_platforms
            condition = " ".join(form.get(key, "") for key in (
                "condition", "contents", "instructions", "setup", "first_impressions", "notes",
            )).casefold()
            blocked = any(term in condition for term in (
                "damage", "damaged", "crush", "cracked", "broken", "missing", "wrong",
                "unsafe", "leak", "swollen", "smoke", "破损", "损坏", "缺少", "错货", "不安全",
            ))
            if blocked:
                app.update({"签收确认": False, "配送状态": "破损或异常", "测试进度": "暂停"})
                feedback["严重度"] = "P1-阻断测试"
            else:
                app.update({"签收确认": True, "配送状态": "已签收", "测试进度": "已签收"})
        elif kind == "checkpoint1":
            app["测试进度"] = "第一阶段完成"
            feedback["严重度"] = "P2-重要问题"
        elif kind == "checkpoint2":
            app["测试进度"] = "第二阶段完成"
            feedback["严重度"] = "P2-重要问题"
        elif kind == "final":
            app["测试进度"] = "最终反馈完成"
            feedback["严重度"] = "P3-建议"
        elif kind == "emergency":
            app["问题与异常"] = "P0：立即停用；" + form.get("summary", "")[:4500]
            feedback["严重度"] = "P0-立即停用"
        elif kind == "logistics":
            app.update({"配送状态": "破损或丢件", "问题与异常": form.get("notes", "")[:5000]})
            feedback["严重度"] = "P1-阻断测试"
    return app, feedback


def status_allows_form(kind: str, status: str) -> bool:
    normalized = status.strip().casefold()
    verification = {"shortlisted", "need verification", "已入围", "待核验"}
    selected = {
        "selected", "已入选", "待发货", "已发货", "已签收", "测试中", "active", "completed", "已完成",
    }
    if kind == "verification":
        return normalized in verification
    return normalized in selected


def retention_clear_fields(scope: str) -> dict:
    if scope == "verification":
        return {"购买凭证": [], "核验状态": "凭证已删除", "核验资料删除日": None}
    common = {
        "Discord用户ID": "", "Discord用户名": "", "核验邮箱": "", "购买凭证": [],
        "收件姓名": "", "收件地址": "", "联系电话": "",
    }
    if scope == "unselected":
        return {**common, "报名状态": "未入选资料已去标识", "未入选资料删除日": None}
    if scope == "selected":
        return {**common, "报名状态": "活动资料已去标识", "配送资料删除日": None}
    raise ValueError("scope must be verification, unselected, or selected")


def retention_date_field(scope: str) -> str:
    fields = {
        "verification": "核验资料删除日",
        "unselected": "未入选资料删除日",
        "selected": "配送资料删除日",
    }
    if scope not in fields:
        raise ValueError("scope must be verification, unselected, or selected")
    return fields[scope]


def _step1_state(values: dict[str, str]) -> tuple[str, str]:
    country = _country_code(values.get("country", ""))
    devices = _device_mask(values.get("devices", ""))
    if not _yes(values.get("age", "")):
        return "", "Applicants must be at least 18 years old."
    if not country:
        return "", "The current test is not available in your country or region."
    if not devices & 3:
        return "", "You must actively use a Nintendo Switch or Switch 2."
    if not _yes(values.get("amazon_24m", "")):
        return "", "Recent Amazon Video Games purchase experience is required."
    if not _yes(values.get("commit", "")):
        return "", "The 14-day test and confidentiality commitment are required."
    return f"2-{country}-{devices:x}", ""


def _continue_button(custom_id: str, label: str) -> dict:
    return {
        "type": 4,
        "data": {
            "content": "Eligibility confirmed. Continue when you are ready.",
            "flags": 64,
            "components": [{
                "type": 1,
                "components": [{"type": 2, "style": 1, "label": label, "custom_id": custom_id}],
            }],
        },
    }


def _step2_modal(token: str) -> dict:
    return {
        "type": 9,
        "data": {
            "custom_id": f"tester_apply_v2_step2.{token}",
            "title": "Step 2 Of 2 — Match And Preferences",
            "components": [
                _text_input("purchase_profile", "Amazon, FUNLAB And Prime Profile", placeholder="COUNT=4-6; FUNLAB=YES; PRIME=YES", max_length=200),
                _text_input("play_profile", "Weekly Play Profile", placeholder="SWITCH=6-10; PC=2-5; CROSS=YES", max_length=200),
                _text_input("favorite_ips", "Favorite Game IPs Or Franchises", placeholder="Pokémon; Zelda; Mario (max 3)", max_length=240),
                _text_input("usage", "Games, Platforms And Controllers", placeholder="Games + platform + controller examples", max_length=1500),
                _text_input("priorities", "What Matters Most In Gaming Accessories?", placeholder="Comfort; low latency; durability (max 3)", max_length=180),
            ],
        },
    }


def _error_response(content: str, *, restart: bool = True) -> InteractionOutcome:
    data: dict = {"content": content, "flags": 64}
    if restart:
        data["components"] = [{
            "type": 1,
            "components": [{
                "type": 2, "style": 1, "label": "Restart Application",
                "custom_id": "tester_apply_start",
            }],
        }]
    return InteractionOutcome({"type": 4, "data": data})


def _parse_pair_text(value: str) -> dict[str, str]:
    out: dict[str, str] = {}
    normalized = unicodedata.normalize("NFKC", value).translate(str.maketrans({"，": ";", "；": ";", ",": ";"}))
    for part in normalized.split(";"):
        if "=" not in part:
            continue
        key, item = part.split("=", 1)
        out[key.strip().casefold()] = item.strip()
    return out


def _limited_list(value: str, label: str, *, item_max: int, total_max: int) -> tuple[str, str]:
    normalized = unicodedata.normalize("NFKC", value).translate(str.maketrans({"，": ";", "；": ";", ",": ";"}))
    items: list[str] = []
    seen: set[str] = set()
    for raw in normalized.split(";"):
        item = raw.strip()
        if not item:
            continue
        if len(item) > item_max:
            return "", f"Each {label} entry must be {item_max} characters or fewer."
        key = item.casefold()
        if key not in seen:
            seen.add(key)
            items.append(item)
    if not items:
        return "", f"Enter at least one {label} entry."
    if len(items) > 3:
        return "", f"Enter up to 3 {label} entries."
    result = "; ".join(items)
    if len(result) > total_max:
        return "", f"The {label} answer is too long."
    return result, ""


def _step2_values(values: dict[str, str]) -> tuple[dict, str]:
    purchase = _parse_pair_text(values.get("purchase_profile", ""))
    purchase_count = purchase.get("count", "").replace("–", "-").replace(" ", "")
    aliases = {"1": "1", "2-3": "2–3", "4-6": "4–6", "7+": "7+"}
    if purchase_count not in aliases:
        return {}, "Use COUNT=1, COUNT=2-3, COUNT=4-6, or COUNT=7+."
    if purchase.get("funlab", "").casefold() not in {"yes", "no"}:
        return {}, "Use FUNLAB=YES or FUNLAB=NO."
    prime_raw = purchase.get("prime", "").casefold()
    if prime_raw not in {"yes", "no", "prefer not", "prefer not to say"}:
        return {}, "Use PRIME=YES, PRIME=NO, or PRIME=PREFER NOT."
    play = _parse_pair_text(values.get("play_profile", ""))
    switch_hours = play.get("switch", "").upper()
    pc_hours = play.get("pc", "").upper()
    valid_switch = {"UNDER 2", "2-5", "6-10", "11-20", "20+"}
    valid_pc = {"0", "UNDER 2", "2-5", "6-10", "10+"}
    if switch_hours not in valid_switch or pc_hours not in valid_pc:
        return {}, "Use the play-time examples shown in the field."
    cross_raw = play.get("cross", "").casefold()
    if cross_raw not in {"yes", "no"}:
        return {}, "Use CROSS=YES or CROSS=NO."
    favorite_ips, error = _limited_list(
        values.get("favorite_ips", ""), "game IP", item_max=80, total_max=240
    )
    if error:
        return {}, error
    priorities, error = _limited_list(
        values.get("priorities", ""), "accessory priority", item_max=60, total_max=180
    )
    if error:
        return {}, error
    usage = unicodedata.normalize("NFKC", values.get("usage", "")).strip()
    if not usage:
        return {}, "Describe the games, platforms, and controllers you use."
    if len(usage) > 1500:
        return {}, "The games, platforms, and controllers answer is too long."
    return {
        "purchase_count": aliases[purchase_count],
        "funlab": purchase["funlab"].casefold() == "yes",
        "prime": "不愿透露" if prime_raw.startswith("prefer") else ("是" if prime_raw == "yes" else "否"),
        "switch_hours": "Under 2" if switch_hours == "UNDER 2" else _HOUR_CANONICAL.get(switch_hours, switch_hours),
        "pc_hours": "Under 2" if pc_hours == "UNDER 2" else _HOUR_CANONICAL.get(pc_hours, pc_hours),
        "cross": cross_raw == "yes",
        "favorite_ips": favorite_ips,
        "usage": usage,
        "priorities": priorities,
    }, ""


def _store_draft(data: dict) -> str:
    now = time.time()
    for key, (expires, _) in list(_drafts.items()):
        if expires <= now:
            _drafts.pop(key, None)
    draft_id = secrets.token_urlsafe(8)
    _drafts[draft_id] = (now + _DRAFT_TTL_SECONDS, data)
    return draft_id


def _load_draft(draft_id: str) -> dict | None:
    item = _drafts.get(draft_id)
    if not item or item[0] <= time.time():
        _drafts.pop(draft_id, None)
        return None
    return item[1]


def _step1_fields(state: str) -> tuple[str, list[str]]:
    try:
        version, country, mask_hex = state.split("-", 2)
        if version != "2":
            raise ValueError
        mask = int(mask_hex, 16)
    except (TypeError, ValueError):
        return "", []
    devices = []
    if mask & 1:
        devices.append("Switch 1")
    if mask & 2:
        devices.append("Switch 2")
    if mask & 4:
        devices.append("Steam Deck")
    if mask & 8:
        devices.append("PC / Steam")
    return country, devices


def _provisional_score(devices: list[str], step2: dict) -> int:
    purchase_points = {"1": 6, "2–3": 9, "4–6": 12, "7+": 15}.get(step2["purchase_count"], 0)
    device_points = 5
    if "Switch 2" in devices:
        device_points += 7
    if "Steam Deck" in devices:
        device_points += 5
    if "PC / Steam" in devices and step2["pc_hours"] in _PC_SCORING_HOURS:
        device_points += 5
    if step2["cross"]:
        device_points += 3
    # Reliability: four points for the 14-day commitment, three for a complete form.
    return purchase_points + min(25, device_points) + 7


def _infer_route(devices: list[str], pc_hours: str) -> str:
    if "Steam Deck" in devices:
        return "Switch + Steam Deck"
    if "PC / Steam" in devices and pc_hours in _PC_SCORING_HOURS:
        return "Switch + PC Steam"
    if "Switch 2" in devices:
        return "Switch 2"
    return "Switch"


def _application_fields(payload: dict, step1_state: str, step2: dict) -> tuple[dict, str]:
    country, devices = _step1_fields(step1_state)
    if not country or not devices:
        return {}, "The application draft is invalid. Please start again."
    route = _infer_route(devices, step2["pc_hours"])
    user = ((payload.get("member") or {}).get("user") or payload.get("user") or {})
    discord_id = str(user.get("id") or "")
    if not discord_id:
        return {}, "Discord user identity is missing. Please try again in the FUNLAB server."
    now_ms = int(time.time() * 1000)
    score = _provisional_score(devices, step2)
    fields = {
        "Discord用户ID": discord_id,
        "Discord用户名": str(user.get("global_name") or user.get("username") or "")[:200],
        "活动批次": "FUNLAB-PRIVATE-TEST-2026-08",
        "报名状态": "已提交",
        "国家或地区": country,
        "年满18岁": True,
        "设备": devices,
        "近24个月Amazon游戏品类购买": "是",
        "购买过FUNLAB": "是" if step2["funlab"] else "否",
        "Amazon Prime": step2["prime"],
        "Amazon购买次数": step2["purchase_count"],
        "Amazon购买品类": "",
        "每周Switch游戏时长": step2["switch_hours"],
        "每周PC手柄时长": step2["pc_hours"],
        "游戏与手柄使用经验": step2["usage"],
        "拟测试场景": step2["usage"],
        "喜爱游戏IP": step2["favorite_ips"],
        "配件关注点": step2["priorities"],
        "断连问题回答": "",
        "功能测试回答": "",
        "申请理由": "",
        "主测试路线": route,
        "承诺完成测试": True,
        "同意保密": True,
        "理解非抽奖且不要求评价": True,
        "可选加入Tester Alumni": False,
        "报名时间": now_ms,
        "最近更新时间": now_ms,
        "筛选分数": score,
        "客群类型": "品牌老客户" if step2["funlab"] else "新品类用户",
        "筛选结论": f"客观预评分{score}/47；其余53分由FUNLAB与Prime核验、IP匹配、配件关注点匹配及Discord历史人工评分。",
        "核验状态": "未开始",
        "配送状态": "未收集",
        "测试进度": "未开始",
    }
    return fields, ""


async def build_interaction_outcome(payload: dict, *, signing_secret: str = "",
                                    ledger: Optional[DiscordTesterLedger] = None,
                                    completion_notifier: Optional[CompletionNotifier] = None) -> InteractionOutcome:
    """Build a Discord response through the public interaction boundary."""
    interaction_type = int(payload.get("type") or 0)
    if interaction_type == 1:  # Discord endpoint verification ping
        return InteractionOutcome({"type": 1})

    data = payload.get("data") or {}
    custom_id = str(data.get("custom_id") or "")
    if interaction_type == 3 and custom_id == "tester_apply_start":
        return InteractionOutcome(_step1_modal())

    legacy_ids = (
        "tester_apply_step1", "tester_apply_step2.", "tester_apply_step3.",
        "tester_apply_continue2.", "tester_apply_continue3.",
    )
    if custom_id == legacy_ids[0] or custom_id.startswith(legacy_ids[1:]):
        return _error_response(
            "The application form has been updated. Please return to the recruitment message and start again."
        )

    if interaction_type == 5 and custom_id == "tester_apply_v2_step1":
        state, error = _step1_state(_values(data))
        if error:
            return _error_response(f"This application is not eligible: {error}")
        draft_id = _store_draft({"step1": state})
        token = _sign_state(f"d-{draft_id}", signing_secret or "development-only")
        return InteractionOutcome(_continue_button(
            f"tester_apply_v2_continue2.{token}", "Continue To Final Step"
        ))

    if interaction_type == 3 and custom_id.startswith("tester_apply_v2_continue2."):
        token = custom_id.removeprefix("tester_apply_v2_continue2.")
        state = _verify_state(token, signing_secret or "development-only")
        draft_id = state.removeprefix("d-") if state.startswith("d-") else ""
        if not draft_id or not _load_draft(draft_id):
            return _error_response("Form expired or invalid. Please start again.")
        return InteractionOutcome(_step2_modal(token))

    if interaction_type == 5 and custom_id.startswith("tester_apply_v2_step2."):
        token = custom_id.removeprefix("tester_apply_v2_step2.")
        state = _verify_state(token, signing_secret or "development-only")
        draft_id = state.removeprefix("d-") if state.startswith("d-") else ""
        draft = _load_draft(draft_id) if draft_id else None
        if not draft:
            return _error_response("Form expired or invalid. Please start again.")
        step2, error = _step2_values(_values(data))
        if error:
            return _error_response(error)
        fields, error = _application_fields(payload, draft.get("step1", ""), step2)
        if error:
            return _error_response(error)
        active_ledger = ledger or DiscordTesterLedger()

        async def save_and_notify() -> None:
            try:
                record_id = await active_ledger.save_application(fields)
            except Exception:
                message = ("We could not save your application. No application was recorded. "
                           "Please try again shortly or contact marketing@fireflyfunlab.com.")
            else:
                _drafts.pop(draft_id, None)
                message = (f"Application received. Your application ID is `{record_id}`. "
                           "Do not send proof or shipping details in Discord.")
            if completion_notifier:
                await completion_notifier(message)

        return InteractionOutcome(
            {"type": 5, "data": {"flags": 64}},
            work=save_and_notify,
        )

    return InteractionOutcome({
        "type": 4,
        "data": {"content": "This application action is no longer available.", "flags": 64},
    })

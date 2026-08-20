"""HTTP routes for the existing FUN Bot product-tester program."""
from __future__ import annotations

import json
import os
from html import escape
from urllib.parse import urlencode

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
import httpx
from fastapi import APIRouter, BackgroundTasks, Header, HTTPException, Request
from fastapi.responses import HTMLResponse

from . import discord_tester_program


router = APIRouter(prefix="/discord/tester", tags=["discord-tester"])
FORM_KINDS = {"verification", "shipping", "receipt", "checkpoint1", "checkpoint2", "final", "emergency", "logistics"}
DISCORD_API = "https://discord.com/api/v10"

_STYLE = """
body{font-family:Arial,sans-serif;background:#0B0B10;color:#f4f0ff;margin:0;padding:24px}
main{max-width:720px;margin:auto;background:#17121f;padding:28px;border:1px solid #44205d;border-radius:16px}
h1{color:#c979ff}label{display:block;margin:18px 0 6px;font-weight:700}
input,textarea,select{width:100%;box-sizing:border-box;padding:12px;border-radius:8px;border:1px solid #5e4770;background:#0f0c14;color:#fff}
textarea{min-height:110px}button{margin-top:22px;background:#9900FF;color:white;border:0;border-radius:8px;padding:13px 22px;font-weight:700}
.warn{background:#3a141c;border-left:4px solid #ff4567;padding:14px}.note{color:#c9bbd5;font-size:14px}.check{width:auto}
"""


def _input(name: str, label: str, *, required: bool = True, kind: str = "text") -> str:
    req = " required" if required else ""
    return f'<label for="{name}">{escape(label)}</label><input id="{name}" name="{name}" type="{kind}"{req}>'


def _textarea(name: str, label: str, *, required: bool = True) -> str:
    req = " required" if required else ""
    return f'<label for="{name}">{escape(label)}</label><textarea id="{name}" name="{name}"{req}></textarea>'


def form_html(kind: str, token: str) -> str:
    if kind not in FORM_KINDS:
        raise ValueError("unsupported form kind")
    title = {
        "verification": "Shortlisted Applicant Verification",
        "shipping": "Selected Tester Shipping Details",
        "receipt": "Sample Receipt Confirmation",
        "checkpoint1": "Testing Checkpoint 1",
        "checkpoint2": "Testing Checkpoint 2",
        "final": "Final Product Test Feedback",
        "emergency": "Urgent Safety Report",
        "logistics": "Shipment Problem Report",
    }[kind]
    intro = ""
    fields = ""
    if kind == "verification":
        intro = '<div class="warn"><strong>Redact before uploading:</strong> hide order numbers, full name, address, payment details, and unrelated purchases.</div>'
        fields = (_input("email", "Email used for verification", kind="email")
                  + _input("amazon_proof", "Amazon Video Games purchase proof", kind="file")
                  + _input("funlab_proof", "FUNLAB purchase proof (only if claimed)", required=False, kind="file")
                  + _input("prime_proof", "Active Prime proof (only if claimed)", required=False, kind="file")
                  + '<label><input class="check" name="redacted" type="checkbox" required> I confirm the files are redacted and genuine.</label>')
    elif kind == "shipping":
        intro = '<p class="note">Only designated logistics personnel may access this information.</p>'
        fields = (_input("legal_name", "Full legal name for delivery")
                  + _input("phone", "Phone number for carrier", kind="tel")
                  + _input("email", "Email for delivery updates", kind="email")
                  + _input("address_line_1", "Address line 1")
                  + _input("address_line_2", "Address line 2", required=False)
                  + _input("city", "City") + _input("region", "State / Province / Region")
                  + _input("postal_code", "Postal code") + _input("country", "Country")
                  + _textarea("carrier_notes", "Local carrier instructions", required=False)
                  + '<label><input class="check" name="accurate" type="checkbox" required> I confirm this delivery information is accurate.</label>')
    elif kind == "receipt":
        fields = (_input("condition", "Package condition", kind="text")
                  + _textarea("notes", "Missing items, damage, or delivery notes", required=False)
                  + '<label><input class="check" name="received" type="checkbox" required> I confirm that I received the sample.</label>')
    elif kind in {"checkpoint1", "checkpoint2", "final"}:
        fields = (_input("platforms", "Platforms tested (comma separated)")
                  + _input("games_hours", "Games tested and approximate hours")
                  + _textarea("summary", "What worked well and what did not?")
                  + _textarea("steps", "For any issue: reproduction steps, expected result, and actual result")
                  + _input("frequency", "Issue frequency, or NONE"))
    elif kind == "emergency":
        intro = '<div class="warn"><strong>Stop using and safely disconnect the product immediately.</strong> Do not reproduce overheating, odor, swelling, smoke, shock, damage, or injury risk.</div>'
        fields = (_input("platforms", "Platform in use") + _textarea("summary", "What happened?")
                  + _textarea("steps", "What happened immediately before the issue?")
                  + _input("frequency", "One-time or repeated?")
                  + _input("evidence", "Optional evidence file", required=False, kind="file"))
    else:
        fields = (_input("condition", "Problem type (lost, damaged, delayed, other)")
                  + _textarea("notes", "Describe the shipment problem"))
    return (f'<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width">'
            f'<title>FUNLAB — {escape(title)}</title><style>{_STYLE}</style></head><body><main>'
            f'<p>FUNLAB Private Product Test</p><h1>{escape(title)}</h1>{intro}'
            f'<form method="post" enctype="multipart/form-data"><input type="hidden" name="token" value="{escape(token)}">'
            f'{fields}<button type="submit">Submit Securely</button></form>'
            f'<p class="note">Privacy contact: marketing@fireflyfunlab.com</p></main></body></html>')


def discord_setup_plan() -> dict:
    return {
        "prelaunch_private": True,
        "roles": ["Tester Interest", "Tester Selected", "Tester Active", "Official Product Tester", "Tester Alumni"],
        "channels": ["tester-announcements", "tester-faq", "tester-questions", "start-here", "tester-lounge",
                     "tester-feedback", "known-issues", "weekly-check-in"],
        "actions": ["create_missing_roles", "create_hidden_prelaunch_category", "create_missing_channels",
                    "configure_interactions_endpoint", "send_staff_rehearsal"],
    }


def rehearsal_message_payload() -> dict:
    return {
        "content": ("🧪 **STAFF TEST — FUNLAB Private Product Testing Application**\n"
                    "This message is in the hidden prelaunch area. It is not the public recruitment post.\n"
                    "Click below to test the 3-step application. Do not use real shipping details or proof files."),
        "components": [{"type": 1, "components": [{
            "type": 2, "style": 1, "label": "Test Application Flow", "custom_id": "tester_apply_start",
        }]}],
        "allowed_mentions": {"parse": []},
    }


def _discord_headers() -> dict:
    token = os.environ.get("DISCORD_BOT_TOKEN", "")
    if not token:
        raise HTTPException(503, "FUN Bot token is not configured")
    return {"Authorization": f"Bot {token}", "User-Agent": "FUNLAB-Tester-Program/1.0"}


async def _discord_request(method: str, path: str, *, body: dict | None = None):
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.request(method, f"{DISCORD_API}{path}", headers=_discord_headers(), json=body)
    if response.status_code >= 400:
        raise HTTPException(502, f"Discord API failed at {path}: HTTP {response.status_code}")
    return response.json() if response.content else {}


@router.post("/admin/setup")
async def setup_discord_prelaunch(request: Request, authorization: str = Header(default=""), commit: bool = False):
    _check_internal_auth(authorization)
    plan = discord_setup_plan()
    if not commit:
        return {"ok": True, "mode": "preview", "plan": plan}

    guild_id = os.environ.get("DISCORD_FUNLAB_GUILD_ID", "1009762946437619742")
    roles = await _discord_request("GET", f"/guilds/{guild_id}/roles")
    role_by_name = {role.get("name"): role for role in roles}
    created_roles = []
    for name in plan["roles"]:
        if name not in role_by_name:
            role = await _discord_request("POST", f"/guilds/{guild_id}/roles", body={
                "name": name, "mentionable": False, "hoist": False,
            })
            role_by_name[name] = role
            created_roles.append(role.get("id"))

    channels = await _discord_request("GET", f"/guilds/{guild_id}/channels")
    category_name = "🧪 PRODUCT TEST (PRELAUNCH)"
    category = next((item for item in channels if item.get("type") == 4 and item.get("name") == category_name), None)
    if not category:
        category = await _discord_request("POST", f"/guilds/{guild_id}/channels", body={
            "name": category_name,
            "type": 4,
            "permission_overwrites": [{"id": guild_id, "type": 0, "deny": "1024", "allow": "0"}],
        })
        channels.append(category)

    existing = {item.get("name"): item for item in channels if item.get("parent_id") == category.get("id")}
    created_channels = []
    channel_specs = {
        "tester-announcements": {"type": 0, "topic": "Prelaunch staff rehearsal; public recruitment has not been published."},
        "tester-faq": {"type": 0, "topic": "Tester FAQ and policy links."},
        "tester-questions": {"type": 0, "topic": "Public questions after launch. Never upload order or address information."},
        "start-here": {"type": 0, "topic": "Private tester rules, schedule, and safety instructions."},
        "tester-lounge": {"type": 0, "topic": "Private tester discussion; formal issues go to the Forum."},
        "tester-feedback": {"type": 15, "topic": "One issue per post; P0 safety reports use the urgent form.",
                            "available_tags": [{"name": name, "moderated": False} for name in
                                               ["Switch", "Switch 2", "Steam Deck", "PC Steam", "Connection", "Controls", "Compatibility", "Stability", "Instructions", "Suggestion", "Need Info", "Confirmed", "Planned", "Resolved"]]},
        "known-issues": {"type": 0, "topic": "Team-published known issues and status."},
        "weekly-check-in": {"type": 0, "topic": "Testing checkpoint reminders and secure form links."},
    }
    for name, spec in channel_specs.items():
        if name in existing:
            continue
        payload = {"name": name, "parent_id": category["id"], **spec}
        item = await _discord_request("POST", f"/guilds/{guild_id}/channels", body=payload)
        existing[name] = item
        created_channels.append(item.get("id"))

    base = str(request.base_url).rstrip("/")
    endpoint_status = "not_attempted"
    try:
        await _discord_request("PATCH", "/applications/@me", body={
            "interactions_endpoint_url": f"{base}/discord/tester/interactions",
        })
        endpoint_status = "configured"
    except HTTPException as exc:
        endpoint_status = f"failed:{exc.status_code}"

    rehearsal = await _discord_request("POST", f"/channels/{existing['tester-announcements']['id']}/messages",
                                       body=rehearsal_message_payload())
    return {"ok": True, "mode": "commit", "prelaunch_private": True,
            "created_role_ids": created_roles, "created_channel_ids": created_channels,
            "category_id": category.get("id"), "rehearsal_message_id": rehearsal.get("id"),
            "interactions_endpoint": endpoint_status, "public_announcement_sent": False}


@router.get("/forms/{kind}", response_class=HTMLResponse)
async def get_secure_form(kind: str, token: str):
    secret = os.environ.get("DISCORD_TESTER_SIGNING_SECRET", "")
    claims = discord_tester_program.read_form_token(token, kind, secret)
    if kind not in FORM_KINDS or not claims:
        raise HTTPException(403, "This secure form link is invalid or expired")
    return HTMLResponse(form_html(kind, token))


@router.get("/privacy", response_class=HTMLResponse)
async def privacy_notice():
    return HTMLResponse(f"""<!doctype html><html><head><meta charset='utf-8'><style>{_STYLE}</style></head><body><main>
<h1>FUNLAB Product Testing Privacy Notice</h1>
<p>We collect only the information needed to assess applications, verify eligibility, deliver samples, operate the private test, resolve safety or support cases, and analyze de-identified feedback.</p>
<p>The public Discord application does not request an address, phone number, payment details, order screenshots, or Prime screenshots. Only shortlisted applicants receive a limited verification form; only selected testers receive a shipping form.</p>
<ul><li>Unselected application data: deleted within 30 days after selection is finalized.</li><li>Original verification files: deleted within 30 days after verification.</li><li>Selected testers' identifiable contact and shipping data: deleted or de-identified within 90 days after the program ends, except an open loss, replacement, legal, fraud, or safety case.</li><li>De-identified product feedback may be retained for product improvement.</li></ul>
<p>Request access, correction, withdrawal, or deletion at <strong>marketing@fireflyfunlab.com</strong>.</p>
</main></body></html>""")


@router.get("/rules", response_class=HTMLResponse)
async def tester_rules():
    return HTMLResponse(f"""<!doctype html><html><head><meta charset='utf-8'><style>{_STYLE}</style></head><body><main>
<h1>FUNLAB Product Tester Rules</h1>
<p>This is a structured product test, not a giveaway, sweepstakes, co-creation program, or review incentive. Selection is not random.</p>
<p>Honest positive and negative feedback are equally welcome. No Amazon review, positive review, five-star rating, or public post is required.</p>
<p>Keep unreleased product details and private test content confidential until FUNLAB allows sharing. Complete all required checkpoints to keep the sample permanently.</p>
<p>Fraud, duplicate claims, harassment, resale before release, or leaks may result in disqualification, access removal, and a return request.</p>
<div class='warn'><strong>Safety:</strong> Stop using and safely disconnect the product for overheating, unusual odor, battery swelling, smoke, electric shock, device or property damage, or injury risk. Use the urgent safety form; do not reproduce a dangerous issue.</div>
</main></body></html>""")


def _check_internal_auth(authorization: str) -> None:
    from . import config
    if not authorization.startswith("Bearer ") or authorization[7:] != config.INTERNAL_TOKEN:
        raise HTTPException(401, "Invalid internal token")


@router.post("/admin/invitations")
async def create_secure_invitation(request: Request, authorization: str = Header(default="")):
    _check_internal_auth(authorization)
    body = await request.json()
    kind = str(body.get("kind") or "")
    record_id = str(body.get("record_id") or "")
    discord_user_id = str(body.get("discord_user_id") or "")
    ttl_hours = min(24 * 30, max(1, int(body.get("ttl_hours") or 48)))
    if kind not in FORM_KINDS or not record_id or not discord_user_id:
        raise HTTPException(400, "kind, record_id, and discord_user_id are required")
    secret = os.environ.get("DISCORD_TESTER_SIGNING_SECRET", "")
    if not secret:
        raise HTTPException(503, "Secure form signing is not configured")
    token = discord_tester_program.issue_form_token(kind, record_id, discord_user_id, ttl_hours * 3600, secret)
    base = str(request.base_url).rstrip("/")
    return {"ok": True, "kind": kind, "expires_in_hours": ttl_hours,
            "url": f"{base}/discord/tester/forms/{kind}?{urlencode({'token': token})}"}


async def _upload_bitable_file(upload, *, parent_node: str) -> dict:
    from . import feishu
    filename = os.path.basename(str(getattr(upload, "filename", "upload.bin")))[:180] or "upload.bin"
    data = await upload.read()
    if not data or len(data) > 8 * 1024 * 1024:
        raise HTTPException(400, f"{filename}: file must be between 1 byte and 8 MB")
    content_type = str(getattr(upload, "content_type", "application/octet-stream") or "application/octet-stream")
    token = await feishu.token("bitable")
    async with httpx.AsyncClient(timeout=90.0) as client:
        response = await client.post(
            "https://open.feishu.cn/open-apis/drive/v1/medias/upload_all",
            headers={"Authorization": f"Bearer {token}"},
            data={"file_name": filename, "parent_type": "bitable_file", "parent_node": parent_node, "size": str(len(data))},
            files={"file": (filename, data, content_type)},
        )
    if response.status_code >= 400:
        raise HTTPException(502, "Secure file storage failed")
    file_token = ((response.json().get("data") or {}).get("file_token") or "")
    if not file_token:
        raise HTTPException(502, "Secure file storage returned no file token")
    return {"file_token": file_token}


@router.post("/forms/{kind}", response_class=HTMLResponse)
async def submit_secure_form(kind: str, request: Request):
    if kind not in FORM_KINDS:
        raise HTTPException(404, "Unknown form")
    form_data = await request.form()
    token = str(form_data.get("token") or request.query_params.get("token") or "")
    secret = os.environ.get("DISCORD_TESTER_SIGNING_SECRET", "")
    claims = discord_tester_program.read_form_token(token, kind, secret)
    if not claims:
        raise HTTPException(403, "This secure form link is invalid or expired")
    text_fields = {key: str(value) for key, value in form_data.multi_items() if isinstance(value, str)}
    if kind == "verification" and "redacted" not in text_fields:
        raise HTTPException(400, "You must confirm that proof files are redacted and genuine")
    amazon_proof = form_data.get("amazon_proof")
    if kind == "verification" and not getattr(amazon_proof, "filename", ""):
        raise HTTPException(400, "Amazon purchase proof is required")
    if kind == "shipping" and "accurate" not in text_fields:
        raise HTTPException(400, "You must confirm that the shipping information is accurate")
    app_fields, feedback_fields = discord_tester_program.build_form_writes(kind, text_fields, claims)
    app_ledger = discord_tester_program.DiscordTesterLedger()
    feedback_ledger = discord_tester_program.DiscordTesterFeedbackLedger()

    uploads = [value for _, value in form_data.multi_items()
               if not isinstance(value, str) and getattr(value, "filename", "")]
    attachment_values = []
    for upload in uploads:
        attachment_values.append(await _upload_bitable_file(upload, parent_node=app_ledger.base_token))
    if kind == "verification":
        app_fields["购买凭证"] = attachment_values

    await app_ledger.update_application(claims["record_id"], app_fields)
    feedback_id = ""
    if feedback_fields:
        feedback_id = await feedback_ledger.create_feedback(feedback_fields)
        if attachment_values and kind == "emergency":
            await feedback_ledger.update_feedback(feedback_id, {"附件": attachment_values})
    special = ("<div class='warn'><strong>Stop using the product.</strong> FUNLAB has received your urgent report. Do not reproduce the issue.</div>"
               if kind == "emergency" else "")
    return HTMLResponse(f"<!doctype html><html><head><meta charset='utf-8'><style>{_STYLE}</style></head><body><main>"
                        f"<h1>Submission Received</h1>{special}<p>Your information was saved securely.</p>"
                        f"<p class='note'>Reference: {escape(feedback_id or claims['record_id'])}</p>"
                        f"<p>You may close this page.</p></main></body></html>")


def verify_discord_signature(public_key_hex: str, signature_hex: str,
                             timestamp: str, body: bytes) -> bool:
    """Verify Discord's Ed25519 request signature against the exact raw body."""
    if not all((public_key_hex, signature_hex, timestamp)):
        return False
    try:
        key = Ed25519PublicKey.from_public_bytes(bytes.fromhex(public_key_hex))
        key.verify(bytes.fromhex(signature_hex), timestamp.encode("ascii") + body)
        return True
    except (ValueError, InvalidSignature, UnicodeError):
        return False


async def _edit_original_interaction(application_id: str, interaction_token: str,
                                     content: str) -> None:
    if not application_id or not interaction_token:
        return
    url = f"{DISCORD_API}/webhooks/{application_id}/{interaction_token}/messages/@original"
    async with httpx.AsyncClient(timeout=15.0) as client:
        response = await client.patch(url, json={"content": content, "components": []})
    response.raise_for_status()


@router.post("/interactions")
async def discord_interactions(request: Request, background_tasks: BackgroundTasks):
    body = await request.body()
    signature = request.headers.get("X-Signature-Ed25519", "")
    timestamp = request.headers.get("X-Signature-Timestamp", "")
    public_key = os.environ.get("DISCORD_APPLICATION_PUBLIC_KEY", "")
    if not verify_discord_signature(public_key, signature, timestamp, body):
        raise HTTPException(401, "Invalid Discord request signature")
    try:
        payload = json.loads(body)
    except (TypeError, ValueError):
        raise HTTPException(400, "Invalid JSON")
    application_id = str(payload.get("application_id") or "")
    interaction_token = str(payload.get("token") or "")

    async def notify_completion(message: str) -> None:
        await _edit_original_interaction(application_id, interaction_token, message)

    outcome = await discord_tester_program.build_interaction_outcome(
        payload,
        signing_secret=os.environ.get("DISCORD_TESTER_SIGNING_SECRET", ""),
        completion_notifier=notify_completion,
    )
    if outcome.work:
        background_tasks.add_task(outcome.work)
    return outcome.response

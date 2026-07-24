"""客服助手 v0 — 邮箱采集 → AI 分类/路由 → 写客服工单台

源: ① Powkong support@powkong.com (Zoho API)  ② Funlab support@funlabswitch.com (网易企业邮箱 IMAP)
只采集+分类+写工单台(只读观察), 不发卡、不回客户。所有凭据走 env(public 仓铁律)。
设计稿: memory `cs-channel-apiization-2026-06-24`。

分类/路由规则 v1 (Frankie 2026-06-25 封板; 2026-07-24 补 Walmart):
- 真实客户: 亚马逊单(订单号 3-7-7) → 站点待领星反查(不自动派站点, 防误派);
  Walmart/沃尔玛 → 林明坚; 美客多单 → 梁俊辉; 独立站 → 张佳烨.
- 无订单号且分不清是客户还是分销商 → 默认当客户; 有平台线索则按平台路由, 否则独立站兜底.
- 非客户: 供应商/B2B/合作 → 标记推 B2B 群; 营销/SEO/平台通知/垃圾 → 忽略归档.
- 置信度: 操作咨询=AI直答 / 质量补发=AI起草人工审 / 投诉升级·退款=必须人工.
"""
import asyncio
import html
import json
import os
import re
import time
import httpx
from email.message import EmailMessage
from email.utils import formataddr, formatdate, make_msgid, parseaddr
from . import deepseek, feishu, cs_resources

# ---- 资源 (非 secret, 可 env 覆盖) ----
CS_APP_TOKEN = os.environ.get("CS_TICKET_APP_TOKEN", "J2fibLgBZaLGTNsQOPHcQXLonZe")
T_TICKET = os.environ.get("CS_TICKET_TABLE_ID", "tblAhXMA9uDbGEMS")
POWKONG_INBOX_FID = os.environ.get("ZOHO_POWKONG_CS_INBOX_FID", "7855434000000008014")
B2B_GROUP = os.environ.get("CS_B2B_GROUP_CHAT_ID", "oc_2e878553984592d7396401fdd6a37d61")

# ---- Zoho POWKONG_CS (env, secret) ----
ZCID = os.environ.get("ZOHO_POWKONG_CS_CLIENT_ID", "")
ZSEC = os.environ.get("ZOHO_POWKONG_CS_CLIENT_SECRET", "")
ZRT = os.environ.get("ZOHO_POWKONG_CS_REFRESH_TOKEN", "")
ZACC = os.environ.get("ZOHO_POWKONG_CS_ACCOUNT_ID", "")
ZREGION = os.environ.get("ZOHO_REGION", ".com")

# ---- 网易 FUNLAB_CS (env, secret) ----
NE_USER = os.environ.get("NETEASE_FUNLAB_CS_USER", "")
NE_CODE = os.environ.get("NETEASE_FUNLAB_CS_AUTHCODE", "")
NE_IMAP = os.environ.get("NETEASE_IMAP_HOST", "imap.qiye.163.com")
NE_SMTP = os.environ.get("NETEASE_SMTP_HOST", "smtp.qiye.163.com")
ZOHO_CS_FROM = os.environ.get("ZOHO_POWKONG_CS_FROM", "support@powkong.com")

# 自动补询客户信息。默认关闭真发，避免部署后采集 cron 立刻给真实客户发信。
CS_INFO_REQUEST_LIVE = (os.environ.get("CS_INFO_REQUEST_LIVE", "0") or "0") == "1"
CS_INFO_REQUEST_DRY_RUN_TO = (os.environ.get("CS_INFO_REQUEST_DRY_RUN_TO", "")
                              or os.environ.get("CS_REPLY_DRY_RUN_TO", "") or "").strip()
CS_INFO_REQUEST_MAX = int(os.environ.get("CS_INFO_REQUEST_MAX", "2") or "2")

# 客户原始证据附件。P0 只做原件透传: 图片/视频/PDF/常见压缩包写入工单附件字段,
# 不让 AI 摘要替代原始文件。飞书 Bitable 单附件上传当前按 20MB 上限处理。
CS_EVIDENCE_ATTACHMENT_FIELD = os.environ.get("CS_EVIDENCE_ATTACHMENT_FIELD", "客户证据附件")
CS_ATTACHMENT_MAX_MB = float(os.environ.get("CS_ATTACHMENT_MAX_MB", "20") or "20")
CS_ATTACHMENT_MAX_BYTES = int(CS_ATTACHMENT_MAX_MB * 1024 * 1024)
CS_ATTACHMENT_ALLOWED_PREFIXES = tuple(
    x.strip().lower() for x in os.environ.get(
        "CS_ATTACHMENT_ALLOWED_PREFIXES",
        "image/,video/,application/pdf,application/zip,application/x-zip-compressed",
    ).split(",") if x.strip()
)

# ---- Discord FUN Bot (token=env secret; 频道 id 非 secret 给默认值) ----
# v0 只接 FUNLAB 公开 #support-center (FUN Bot 可读)。私有工单(MEE6)待官号 2FA 授权后补。
# Zeabur 东京可直连 Discord API, 无需代理。
DISCORD_BOT_TOKEN = os.environ.get("DISCORD_BOT_TOKEN", "")
DC_SUPPORT_CHAN = os.environ.get("DISCORD_FUNLAB_SUPPORT_CHANNEL_ID", "1012184626640470089")
DC_GUILD = os.environ.get("DISCORD_FUNLAB_GUILD_ID", "1009762946437619742")

PLATFORM_OPTS = ["亚马逊-美国", "亚马逊-墨西哥", "亚马逊-加拿大", "亚马逊-日本",
                 "亚马逊-英国", "亚马逊-欧洲", "独立站", "美客多", "沃尔玛", "TikTok", "未知"]
TYPE_OPTS = ["物流", "产品", "退换货", "售后", "投诉升级"]
LANG_OPTS = ["EN", "中文", "德", "法", "西", "葡", "日", "其他"]
CONF_OPTS = ["AI直答", "AI起草人工审", "必须人工"]
AMZ_ORDER_RE = re.compile(r"\d{3}-\d{7}-\d{7}")
STATUS_WAIT_INFO = "待客户补充"

# ---- 领星反查 (亚马逊订单号 → sid → 店铺 country → 运营) ----
LX_PROXY_URL = os.environ.get("LINGXING_PROXY_URL", "")
LX_PROXY_TOKEN = os.environ.get("LINGXING_PROXY_TOKEN", "")
# country → (销售平台选项, 运营). 巴西/澳洲等未映射 → 兜底待人工。
COUNTRY_MAP = {
    "美国": ("亚马逊-美国", "黄奕纯"),
    "加拿大": ("亚马逊-加拿大", "陈翔宇"),
    "墨西哥": ("亚马逊-墨西哥", "陈翔宇"),
    "日本": ("亚马逊-日本", "陈翔宇"),
    "英国": ("亚马逊-英国", "林明坚"),
    "德国": ("亚马逊-欧洲", "林明坚"), "法国": ("亚马逊-欧洲", "林明坚"),
    "西班牙": ("亚马逊-欧洲", "林明坚"), "意大利": ("亚马逊-欧洲", "林明坚"),
    "荷兰": ("亚马逊-欧洲", "林明坚"), "比利时": ("亚马逊-欧洲", "林明坚"),
    "波兰": ("亚马逊-欧洲", "林明坚"), "瑞典": ("亚马逊-欧洲", "林明坚"),
    "爱尔兰": ("亚马逊-欧洲", "林明坚"), "土耳其": ("亚马逊-欧洲", "林明坚"),
}
_seller_cache = {"map": {}, "ts": 0.0}
_SELLER_TTL = 3600


def _strip_html(s: str) -> str:
    # Preserve href targets before dropping tags; Shopify/contact-form uploads are
    # often links in HTML rather than MIME attachments.
    s = re.sub(r'<a\b[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>',
               lambda m: f" {re.sub(r'<[^>]+>', ' ', m.group(2) or '').strip()} {m.group(1)} ",
               s or "", flags=re.I | re.S)
    s = re.sub(r"<[^>]+>", " ", s or "")
    return re.sub(r"\s+", " ", html.unescape(s).replace("&nbsp;", " ")).strip()


def _field_text(v) -> str:
    if v is None:
        return ""
    if isinstance(v, str):
        return v.strip()
    if isinstance(v, (int, float)):
        return str(int(v) if isinstance(v, float) and v.is_integer() else v)
    if isinstance(v, list):
        parts = []
        for x in v:
            if isinstance(x, dict):
                parts.append(str(x.get("text") or x.get("name") or x.get("link") or ""))
            else:
                parts.append(str(x))
        return " ".join(p for p in parts if p).strip()
    if isinstance(v, dict):
        return str(v.get("text") or v.get("name") or v.get("link") or "").strip()
    return str(v).strip()


def _customer_email(v: str) -> str:
    return (parseaddr(v or "")[1] or v or "").strip().lower()


def _safe_filename(name: str, fallback: str = "customer-evidence.bin") -> str:
    raw = (name or "").strip().replace("\\", "_").replace("/", "_")
    raw = re.sub(r"[\x00-\x1f<>:\"|?*]+", "_", raw)
    raw = re.sub(r"\s+", " ", raw).strip(" .")
    return (raw or fallback)[:180]


def _attachment_allowed(content_type: str, filename: str = "") -> bool:
    ct = (content_type or "application/octet-stream").lower()
    if any(ct.startswith(prefix) for prefix in CS_ATTACHMENT_ALLOWED_PREFIXES):
        return True
    ext = os.path.splitext((filename or "").lower())[1]
    return ext in {".jpg", ".jpeg", ".png", ".gif", ".webp", ".heic", ".heif",
                   ".mp4", ".mov", ".avi", ".m4v", ".pdf", ".zip"}


def _attachment_kind(content_type: str, filename: str = "") -> str:
    ct = (content_type or "").lower()
    if ct == "text/uri-list":
        return "链接"
    if ct.startswith("image/"):
        return "图片"
    if ct.startswith("video/"):
        return "视频"
    if ct == "application/pdf" or filename.lower().endswith(".pdf"):
        return "PDF"
    return "文件"


_URL_RE = re.compile(r"https?://[^\s<>'\")]+", re.I)


def _filename_from_url(url: str, idx: int) -> str:
    try:
        path = url.split("?", 1)[0].rstrip("/")
        name = path.rsplit("/", 1)[-1]
        return _safe_filename(name or f"customer-evidence-link-{idx}.url")
    except Exception:
        return f"customer-evidence-link-{idx}.url"


def _looks_like_evidence_url(url: str) -> bool:
    low = (url or "").lower()
    if any(x in low for x in ["cdn.shopify.com", "shopifycdn", "/uploads/", "drive.google.com"]):
        return True
    return bool(re.search(r"\.(jpg|jpeg|png|gif|webp|heic|heif|mp4|mov|m4v|avi|pdf|zip)(\?|$)", low))


def _extract_evidence_links(*texts: str) -> list[dict]:
    seen, out = set(), []
    for text in texts:
        for url in _URL_RE.findall(text or ""):
            url = html.unescape(url).rstrip(".,;]")
            if url in seen or not _looks_like_evidence_url(url):
                continue
            seen.add(url)
            idx = len(out) + 1
            filename = _filename_from_url(url, idx)
            out.append({"filename": filename, "content_type": "text/uri-list", "size": 0,
                        "kind": "链接", "source": "email-link", "url": url,
                        "skipped_reason": "外部证据链接，已在卡片保留 URL"})
    return out


def _public_attachment_meta(att: dict) -> dict:
    return {k: v for k, v in att.items() if k not in {"bytes", "raw"}}


def _attachments_json(attachments: list[dict]) -> str:
    safe = [_public_attachment_meta(a) for a in attachments]
    return json.dumps(safe, ensure_ascii=False)[:5000]


def _attachments_summary(attachments: list[dict]) -> str:
    if not attachments:
        return "未检测到客户图片/视频/PDF附件。"
    saved = [a for a in attachments if a.get("file_token")]
    links = [a for a in attachments if a.get("url") and not a.get("file_token")]
    skipped = [a for a in attachments if a.get("skipped_reason") and not a.get("url")]
    failed = [a for a in attachments if a.get("upload_error")]
    lines = [f"客户原始证据: 已保存附件 {len(saved)} 个"
             + (f"，提取链接 {len(links)} 个" if links else "")
             + (f"，跳过 {len(skipped)} 个" if skipped else "")
             + (f"，失败 {len(failed)} 个" if failed else "")]
    for idx, a in enumerate(attachments[:8], 1):
        size_mb = (int(a.get("size") or 0) / 1024 / 1024)
        if a.get("file_token"):
            state = "已保存"
        elif a.get("url"):
            state = "链接"
        else:
            state = "跳过" if a.get("skipped_reason") else "待保存"
        reason = a.get("skipped_reason") or a.get("upload_error") or ""
        lines.append(f"{idx}. [{a.get('kind') or '文件'}] {a.get('filename') or '-'} · {size_mb:.2f}MB · {state}"
                     + (f" · {reason}" if reason else ""))
    if len(attachments) > 8:
        lines.append(f"... 另有 {len(attachments) - 8} 个附件未在摘要中展开。")
    return "\n".join(lines)[:5000]


def _attachment_status(attachments: list[dict]) -> str:
    if not attachments:
        return "无附件"
    saved = sum(1 for a in attachments if a.get("file_token"))
    linked = sum(1 for a in attachments if a.get("url") and not a.get("file_token"))
    failed_or_skipped = sum(1 for a in attachments if (a.get("skipped_reason") and not a.get("url")) or a.get("upload_error"))
    if saved and not failed_or_skipped:
        return "已保存"
    if saved and (failed_or_skipped or linked):
        return "部分跳过"
    if linked and not failed_or_skipped:
        return "部分跳过"
    return "保存失败" if failed_or_skipped else "无附件"


def _attachment_base_fields(attachments: list[dict]) -> dict:
    return {
        "客户附件数量": sum(1 for a in attachments if a.get("file_token")),
        "客户附件状态": _attachment_status(attachments),
        "客户附件摘要": _attachments_summary(attachments),
        "客户附件JSON": _attachments_json(attachments),
    }


async def _upload_bitable_attachment(filename: str, content_type: str, data: bytes) -> str:
    tok = await feishu.token("notify")
    safe_name = _safe_filename(filename)
    form = {
        "file_name": safe_name,
        "parent_type": "bitable_file",
        "parent_node": CS_APP_TOKEN,
        "size": str(len(data)),
    }
    files = {"file": (safe_name, data, content_type or "application/octet-stream")}
    async with httpx.AsyncClient(timeout=90.0) as c:
        r = await c.post("https://open.feishu.cn/open-apis/drive/v1/medias/upload_all",
                         headers={"Authorization": f"Bearer {tok}"},
                         data=form, files=files)
        if r.status_code >= 400:
            raise Exception(f"Feishu upload {r.status_code}: {r.text[:240]}")
        d = r.json()
    token = (d.get("data") or {}).get("file_token")
    if not token:
        raise Exception(f"Feishu upload no file_token: {str(d)[:240]}")
    return token


async def _save_attachments_to_ticket(record_id: str, attachments: list[dict],
                                      existing_fields: dict | None = None,
                                      dry_run: bool = False) -> dict:
    """Upload customer evidence to the ticket record attachment field.

    Returns fields that should be written to the ticket. This function keeps
    bytes out of Bitable JSON and only stores original files in the attachment
    field plus small metadata.
    """
    if not attachments:
        fields = _attachment_base_fields([])
        if not dry_run and record_id:
            await feishu.api("PUT",
                             f"/bitable/v1/apps/{CS_APP_TOKEN}/tables/{T_TICKET}/records/{record_id}",
                             {"fields": fields}, which="notify")
        return fields

    result = []
    attachment_values = []
    existing_fields = existing_fields or {}
    existing_attach = existing_fields.get(CS_EVIDENCE_ATTACHMENT_FIELD)
    if isinstance(existing_attach, list):
        for item in existing_attach:
            if isinstance(item, dict) and item.get("file_token"):
                attachment_values.append({"file_token": item["file_token"]})

    for att in attachments:
        meta = _public_attachment_meta(att)
        data = att.get("bytes") or b""
        if att.get("skipped_reason"):
            result.append(meta)
            continue
        if not data:
            meta["skipped_reason"] = "空附件或未下载到二进制"
            result.append(meta)
            continue
        if dry_run:
            meta["dry_run"] = True
            result.append(meta)
            continue
        try:
            token = await _upload_bitable_attachment(att.get("filename") or "customer-evidence.bin",
                                                     att.get("content_type") or "application/octet-stream",
                                                     data)
            meta["file_token"] = token
            attachment_values.append({"file_token": token})
        except Exception as e:
            meta["upload_error"] = str(e)[:240]
        result.append(meta)

    fields = _attachment_base_fields(result)
    if attachment_values:
        fields[CS_EVIDENCE_ATTACHMENT_FIELD] = attachment_values
    if not dry_run:
        await feishu.api("PUT", f"/bitable/v1/apps/{CS_APP_TOKEN}/tables/{T_TICKET}/records/{record_id}",
                         {"fields": fields}, which="notify")
    return fields


# ===== 领星反查 (亚马逊订单号 → 站点/运营) =====
async def _lx_proxy(method: str, path: str, params: dict) -> dict:
    async with httpx.AsyncClient(timeout=40.0) as c:
        r = await c.post(LX_PROXY_URL,
                         headers={"Authorization": f"Bearer {LX_PROXY_TOKEN}",
                                  "Content-Type": "application/json"},
                         json={"method": method, "path": path, "params": params})
        r.raise_for_status()
        return r.json()


async def _get_sid_country() -> dict:
    if _seller_cache["map"] and (time.time() - _seller_cache["ts"] < _SELLER_TTL):
        return _seller_cache["map"]
    rows = (await _lx_proxy("GET", "/erp/sc/data/seller/lists", {})).get("data") or []
    m = {str(x.get("sid")): x.get("country") for x in rows if x.get("sid")}
    if m:
        _seller_cache["map"], _seller_cache["ts"] = m, time.time()
    return m


async def _lookup_amazon_route(order_id: str):
    """亚马逊订单号 → (销售平台, 运营)。查不到/未映射(巴西/澳洲等) → (None, None)。"""
    if not (order_id and LX_PROXY_URL and LX_PROXY_TOKEN):
        return None, None
    try:
        data = (await _lx_proxy("POST", "/erp/sc/data/mws/orderDetail",
                                {"order_id": order_id})).get("data") or []
        row = data[0] if isinstance(data, list) and data else (data if isinstance(data, dict) else None)
        if not row:
            return None, None
        country = (await _get_sid_country()).get(str(row.get("sid") or ""))
        return COUNTRY_MAP.get(country, (None, None))
    except Exception:
        return None, None


# ===== 源 ① Powkong (Zoho) =====
async def _ztoken() -> str:
    async with httpx.AsyncClient(timeout=30.0) as c:
        r = await c.post(f"https://accounts.zoho{ZREGION}/oauth/v2/token",
                         data={"refresh_token": ZRT, "client_id": ZCID,
                               "client_secret": ZSEC, "grant_type": "refresh_token"})
        r.raise_for_status()
        return r.json()["access_token"]


async def _zget(url: str, tok: str) -> dict:
    async with httpx.AsyncClient(timeout=40.0) as c:
        r = await c.get(url, headers={"Authorization": f"Zoho-oauthtoken {tok}"})
        r.raise_for_status()
        return r.json()


async def _zget_bytes(url: str, tok: str) -> tuple[bytes, str]:
    async with httpx.AsyncClient(timeout=90.0) as c:
        r = await c.get(url, headers={"Authorization": f"Zoho-oauthtoken {tok}"})
        r.raise_for_status()
        return r.content, r.headers.get("content-type", "application/octet-stream").split(";")[0]


async def _fetch_zoho_attachments(tok: str, folder_id: str, message_id: str) -> list[dict]:
    """Best-effort Zoho Mail attachment extraction.

    Zoho returns attachment descriptors under slightly different shapes across
    accounts. Keep parsing defensive and only persist customer evidence-like
    files.
    """
    try:
        info = await _zget(
            f"https://mail.zoho.com/api/accounts/{ZACC}/folders/{folder_id}"
            f"/messages/{message_id}/attachmentinfo", tok)
    except Exception:
        return []
    raw = info.get("data") or info.get("attachments") or []
    if isinstance(raw, dict):
        raw = raw.get("attachments") or raw.get("attachmentInfo") or raw.get("data") or []
    out = []
    for item in raw if isinstance(raw, list) else []:
        if not isinstance(item, dict):
            continue
        aid = item.get("attachmentId") or item.get("id") or item.get("storeName")
        filename = _safe_filename(item.get("attachmentName") or item.get("fileName") or item.get("name") or str(aid or "attachment"))
        content_type = (item.get("contentType") or item.get("mimeType") or "application/octet-stream").split(";")[0]
        try:
            size = int(item.get("attachmentSize") or item.get("size") or 0)
        except Exception:
            size = 0
        meta = {"filename": filename, "content_type": content_type, "size": size,
                "kind": _attachment_kind(content_type, filename), "source": "zoho"}
        if not aid:
            meta["skipped_reason"] = "Zoho 未返回 attachmentId"
        elif not _attachment_allowed(content_type, filename):
            meta["skipped_reason"] = f"非证据类型: {content_type}"
        elif size and size > CS_ATTACHMENT_MAX_BYTES:
            meta["skipped_reason"] = f"超过 {CS_ATTACHMENT_MAX_MB:g}MB 上限"
        if not meta.get("skipped_reason"):
            try:
                data, real_ct = await _zget_bytes(
                    f"https://mail.zoho.com/api/accounts/{ZACC}/folders/{folder_id}"
                    f"/messages/{message_id}/attachments/{aid}", tok)
                if len(data) > CS_ATTACHMENT_MAX_BYTES:
                    meta["skipped_reason"] = f"超过 {CS_ATTACHMENT_MAX_MB:g}MB 上限"
                else:
                    meta["bytes"] = data
                    meta["size"] = len(data)
                    meta["content_type"] = real_ct or content_type
                    meta["kind"] = _attachment_kind(meta["content_type"], filename)
            except Exception as e:
                meta["skipped_reason"] = f"Zoho 下载失败: {str(e)[:120]}"
        out.append(meta)
    return out


async def _fetch_powkong(limit: int) -> list:
    if not (ZCID and ZRT and ZACC):
        return []
    tok = await _ztoken()
    listing = await _zget(
        f"https://mail.zoho.com/api/accounts/{ZACC}/messages/view"
        f"?folderId={POWKONG_INBOX_FID}&limit={limit}&start=0", tok)
    out = []
    for m in (listing.get("data") or []):
        mid = m.get("messageId")
        if not mid:
            continue
        try:
            content = await _zget(
                f"https://mail.zoho.com/api/accounts/{ZACC}/folders/{POWKONG_INBOX_FID}"
                f"/messages/{mid}/content", tok)
            d = content.get("data")
            body = _strip_html(d.get("content", "") if isinstance(d, dict) else "")
        except Exception:
            body = ""
        attachments = await _fetch_zoho_attachments(tok, POWKONG_INBOX_FID, mid)
        attachments.extend(_extract_evidence_links(body))
        out.append({"id": mid, "id_prefix": "CSP", "frm": m.get("fromAddress", ""),
                    "subj": m.get("subject", ""), "received_ms": int(m.get("receivedTime") or 0),
                    "body": body, "channel": "邮箱", "brand_default": "POWKONG",
                    "in_reply_to": m.get("inReplyTo") or m.get("inReplyToHeader") or "",
                    "references": m.get("references") or "",
                    "mail_thread_id": m.get("threadId") or "",
                    "attachments": attachments})
    return out


async def _fetch_powkong_one(message_id: str) -> dict:
    if not (ZCID and ZRT and ZACC and message_id):
        return {}
    tok = await _ztoken()
    body = ""
    try:
        content = await _zget(
            f"https://mail.zoho.com/api/accounts/{ZACC}/folders/{POWKONG_INBOX_FID}"
            f"/messages/{message_id}/content", tok)
        d = content.get("data")
        body = _strip_html(d.get("content", "") if isinstance(d, dict) else "")
    except Exception:
        body = ""
    attachments = await _fetch_zoho_attachments(tok, POWKONG_INBOX_FID, message_id)
    attachments.extend(_extract_evidence_links(body))
    return {"id": message_id, "id_prefix": "CSP", "frm": "", "subj": "",
            "received_ms": 0, "body": body, "channel": "邮箱", "brand_default": "POWKONG",
            "in_reply_to": "", "references": "", "mail_thread_id": "",
            "attachments": attachments}


# ===== 源 ② Funlab (网易 IMAP, 同步, 跑在线程里) =====
def _extract_body(msg) -> str:
    try:
        if msg.is_multipart():
            plain, html = "", ""
            for part in msg.walk():
                ct = part.get_content_type()
                if part.get("Content-Disposition", "").startswith("attachment"):
                    continue
                try:
                    payload = part.get_payload(decode=True)
                    if not payload:
                        continue
                    txt = payload.decode(part.get_content_charset() or "utf-8", "replace")
                except Exception:
                    continue
                if ct == "text/plain" and not plain:
                    plain = txt
                elif ct == "text/html" and not html:
                    html = txt
            return (plain or _strip_html(html))
        payload = msg.get_payload(decode=True)
        txt = payload.decode(msg.get_content_charset() or "utf-8", "replace") if payload else ""
        return txt if msg.get_content_type() == "text/plain" else _strip_html(txt)
    except Exception:
        return ""


def _decode_email_filename(raw: str) -> str:
    try:
        from email.header import decode_header, make_header
        return str(make_header(decode_header(raw or "")))
    except Exception:
        return raw or ""


def _email_text_parts_for_links(msg) -> list[str]:
    texts = []
    try:
        parts = msg.walk() if msg.is_multipart() else [msg]
        for part in parts:
            if part.is_multipart():
                continue
            ct = part.get_content_type()
            if ct not in ("text/plain", "text/html"):
                continue
            if (part.get("Content-Disposition") or "").lower().startswith("attachment"):
                continue
            try:
                payload = part.get_payload(decode=True)
                if not payload:
                    continue
                txt = payload.decode(part.get_content_charset() or "utf-8", "replace")
            except Exception:
                continue
            texts.append(txt if ct == "text/plain" else _strip_html(txt))
    except Exception:
        return []
    return texts


def _extract_email_attachments(msg) -> list[dict]:
    out = []
    try:
        parts = msg.walk() if msg.is_multipart() else [msg]
        idx = 0
        for part in parts:
            if part.is_multipart():
                continue
            filename = _decode_email_filename(part.get_filename() or "")
            disp = (part.get("Content-Disposition") or "").lower()
            ct = (part.get_content_type() or "application/octet-stream").split(";")[0]
            # 只保留明确附件或带文件名的图片/视频/PDF；无文件名的 inline logo 暂不采。
            if not filename:
                continue
            if "attachment" not in disp and not _attachment_allowed(ct, filename):
                continue
            idx += 1
            safe_name = _safe_filename(filename, f"customer-evidence-{idx}.bin")
            meta = {"filename": safe_name, "content_type": ct, "size": 0,
                    "kind": _attachment_kind(ct, safe_name), "source": "netease-imap"}
            if not _attachment_allowed(ct, safe_name):
                meta["skipped_reason"] = f"非证据类型: {ct}"
                out.append(meta)
                continue
            try:
                data = part.get_payload(decode=True) or b""
            except Exception:
                data = b""
            meta["size"] = len(data)
            if not data:
                meta["skipped_reason"] = "空附件"
            elif len(data) > CS_ATTACHMENT_MAX_BYTES:
                meta["skipped_reason"] = f"超过 {CS_ATTACHMENT_MAX_MB:g}MB 上限"
            else:
                meta["bytes"] = data
            out.append(meta)
        out.extend(_extract_evidence_links(*_email_text_parts_for_links(msg)))
    except Exception as e:
        out.append({"filename": "attachment-parse-error", "content_type": "application/octet-stream",
                    "size": 0, "kind": "文件", "source": "netease-imap",
                    "skipped_reason": f"解析失败: {str(e)[:120]}"})
    return out


def _fetch_funlab_sync(limit: int) -> list:
    import imaplib, ssl, email
    from email.header import decode_header, make_header
    from email.utils import parsedate_to_datetime, parseaddr
    if not (NE_USER and NE_CODE):
        return []
    out = []
    conn = imaplib.IMAP4_SSL(NE_IMAP, 993, ssl_context=ssl.create_default_context(), timeout=30)
    try:
        conn.login(NE_USER, NE_CODE)
        # 网易必须发 ID 命令, 否则 SELECT 报 Unsafe Login
        imaplib.Commands["ID"] = ("AUTH", "SELECTED")
        conn._simple_command(
            "ID", '("name" "funlab-cs" "version" "1.0" "vendor" "python" "contact" "%s")' % NE_USER)
        conn.select("INBOX", readonly=True)
        typ, data = conn.search(None, "ALL")
        ids = data[0].split()
        for mid in ids[-limit:][::-1]:
            typ, d = conn.fetch(mid, "(BODY.PEEK[])")
            if not d or not d[0]:
                continue
            msg = email.message_from_bytes(d[0][1])
            try:
                subj = str(make_header(decode_header(msg.get("Subject", ""))))
            except Exception:
                subj = msg.get("Subject", "")
            frm = parseaddr(msg.get("From", ""))[1] or msg.get("From", "")
            msgid = (msg.get("Message-ID", "") or "").strip() or f"netease-{mid.decode()}"
            try:
                received_ms = int(parsedate_to_datetime(msg.get("Date")).timestamp() * 1000)
            except Exception:
                received_ms = 0
            out.append({"id": msgid, "id_prefix": "CSF", "frm": frm, "subj": subj,
                        "received_ms": received_ms, "body": _extract_body(msg)[:8000],
                        "channel": "邮箱", "brand_default": "FUNLAB",
                        "in_reply_to": (msg.get("In-Reply-To", "") or "").strip(),
                        "references": (msg.get("References", "") or "").strip(),
                        "mail_thread_id": "",
                        "attachments": _extract_email_attachments(msg)})
    finally:
        try:
            conn.logout()
        except Exception:
            pass
    return out


async def _fetch_funlab(limit: int) -> list:
    return await asyncio.to_thread(_fetch_funlab_sync, limit)


def _fetch_funlab_one_sync(message_id: str, scan_limit: int = 500) -> dict:
    import imaplib, ssl, email
    from email.header import decode_header, make_header
    from email.utils import parsedate_to_datetime, parseaddr
    if not (NE_USER and NE_CODE and message_id):
        return {}
    conn = imaplib.IMAP4_SSL(NE_IMAP, 993, ssl_context=ssl.create_default_context(), timeout=30)
    try:
        conn.login(NE_USER, NE_CODE)
        imaplib.Commands["ID"] = ("AUTH", "SELECTED")
        conn._simple_command(
            "ID", '("name" "funlab-cs-backfill" "version" "1.0" "vendor" "python" "contact" "%s")' % NE_USER)
        conn.select("INBOX", readonly=True)
        candidates = []
        if message_id.startswith("<") and message_id.endswith(">"):
            try:
                typ, data = conn.search(None, "HEADER", "Message-ID", f'"{message_id}"')
                if typ == "OK" and data and data[0]:
                    candidates.extend(data[0].split())
            except Exception:
                pass
        if not candidates:
            typ, data = conn.search(None, "ALL")
            ids = (data[0].split() if data and data[0] else [])[-scan_limit:][::-1]
            candidates.extend(ids)
        for mid in candidates:
            typ, d = conn.fetch(mid, "(BODY.PEEK[])")
            if not d or not d[0]:
                continue
            msg = email.message_from_bytes(d[0][1])
            msgid = (msg.get("Message-ID", "") or "").strip() or f"netease-{mid.decode()}"
            if msgid != message_id and f"netease-{mid.decode()}" != message_id:
                continue
            try:
                subj = str(make_header(decode_header(msg.get("Subject", ""))))
            except Exception:
                subj = msg.get("Subject", "")
            frm = parseaddr(msg.get("From", ""))[1] or msg.get("From", "")
            try:
                received_ms = int(parsedate_to_datetime(msg.get("Date")).timestamp() * 1000)
            except Exception:
                received_ms = 0
            return {"id": msgid, "id_prefix": "CSF", "frm": frm, "subj": subj,
                    "received_ms": received_ms, "body": _extract_body(msg)[:8000],
                    "channel": "邮箱", "brand_default": "FUNLAB",
                    "in_reply_to": (msg.get("In-Reply-To", "") or "").strip(),
                    "references": (msg.get("References", "") or "").strip(),
                    "mail_thread_id": "",
                    "attachments": _extract_email_attachments(msg)}
    finally:
        try:
            conn.logout()
        except Exception:
            pass
    return {}


async def _fetch_funlab_one(message_id: str, scan_limit: int = 500) -> dict:
    return await asyncio.to_thread(_fetch_funlab_one_sync, message_id, scan_limit)


# ===== 源 ③ Discord (FUN Bot REST: 公开 #support-center + 私有工单 MEE6) =====
def _dc_hdr():
    return {"Authorization": f"Bot {DISCORD_BOT_TOKEN}", "User-Agent": "DiscordBot (cs,1.0)"}


def _dc_ts(m: dict) -> int:
    from datetime import datetime
    try:
        return int(datetime.fromisoformat(
            (m.get("timestamp", "") or "").replace("Z", "+00:00")).timestamp() * 1000)
    except Exception:
        return 0


def _dc_name(au: dict) -> str:
    return au.get("global_name") or au.get("username") or str(au.get("id", ""))


async def _fetch_discord(limit: int) -> list:
    """降噪聚合: 工单频道→1工单1条(合并客户全部消息); 公开频道→合并连续同人消息+跳碎片。"""
    if not DISCORD_BOT_TOKEN:
        return []
    out, per = [], min(int(limit), 50)
    async with httpx.AsyncClient(timeout=30.0) as c:
        targets = []  # (channel_id, ticket_name)
        if DC_SUPPORT_CHAN:
            targets.append((DC_SUPPORT_CHAN, ""))
        if DC_GUILD:  # 枚举 🔧SUPPORT 分类下的 MEE6 工单频道(#N-name)
            try:
                gr = await c.get(f"https://discord.com/api/v10/guilds/{DC_GUILD}/channels", headers=_dc_hdr())
                if gr.status_code == 200:
                    chans = gr.json()
                    cats = {x["id"] for x in chans if x.get("type") == 4
                            and ("SUPPORT" in (x.get("name", "").upper()) or "🔧" in x.get("name", ""))}
                    for x in chans:
                        if (x.get("type") == 0 and x.get("parent_id") in cats
                                and re.match(r"^\d+-", x.get("name", ""))):
                            targets.append((x["id"], x.get("name", "")))
            except Exception:
                pass
        for cid, tname in targets:
            try:
                r = await c.get(f"https://discord.com/api/v10/channels/{cid}/messages?limit={per}", headers=_dc_hdr())
                if r.status_code != 200:
                    continue
                msgs = r.json()
            except Exception:
                continue
            hm = [m for m in (msgs if isinstance(msgs, list) else [])
                  if not (m.get("author") or {}).get("bot") and (m.get("content") or "").strip()]
            hm.reverse()  # API 返回最新在前 → 转成时间正序
            if not hm:
                continue
            if tname:
                # 工单频道: 整票客户消息聚合成 1 条工单
                body = "\n".join((m.get("content") or "").strip() for m in hm)[:6000]
                au = hm[0].get("author") or {}
                out.append({"id": f"ticket-{cid}", "id_prefix": "CSDT",
                            "frm": f"{_dc_name(au)} (Discord·工单{tname})", "subj": "",
                            "received_ms": _dc_ts(hm[-1]), "body": body,
                            "channel": "Discord", "brand_default": "FUNLAB"})
            else:
                # 公开频道: 合并连续同一作者的消息为 1 条, 跳过纯碎片
                groups = []
                for m in hm:
                    aid = (m.get("author") or {}).get("id")
                    if groups and groups[-1][0] == aid:
                        groups[-1][1].append(m)
                    else:
                        groups.append([aid, [m]])
                for _aid, grp in groups:
                    body = "\n".join((g.get("content") or "").strip() for g in grp)[:6000]
                    if len(body) < 12 and "?" not in body:   # 纯寒暄/碎片("ok"/"thanks")跳过
                        continue
                    au = grp[0].get("author") or {}
                    out.append({"id": grp[0].get("id"), "id_prefix": "CSD",
                                "frm": f"{_dc_name(au)} (Discord)", "subj": "",
                                "received_ms": _dc_ts(grp[-1]), "body": body,
                                "channel": "Discord", "brand_default": "FUNLAB"})
    return out


# ===== 去重 =====
async def _existing_thread_ids() -> set:
    ids, page = set(), ""
    while True:
        path = (f"/bitable/v1/apps/{CS_APP_TOKEN}/tables/{T_TICKET}"
                f"/records?page_size=200" + (f"&page_token={page}" if page else ""))
        d = await feishu.api("GET", path, which="notify")
        data = d.get("data", {})
        for it in data.get("items", []):
            v = it.get("fields", {}).get("线程ID")
            if isinstance(v, list) and v:
                v = v[0].get("text") if isinstance(v[0], dict) else v[0]
            if v:
                ids.add(str(v).strip())
        if data.get("has_more"):
            page = data.get("page_token", "")
        else:
            break
    return ids


async def _waiting_info_tickets() -> list:
    """Rows waiting for customer order/site supplement."""
    body = {"filter": {"conjunction": "and", "conditions": [
        {"field_name": "状态", "operator": "is", "value": [STATUS_WAIT_INFO]}]},
        "page_size": 200}
    d = await feishu.api("POST", f"/bitable/v1/apps/{CS_APP_TOKEN}/tables/{T_TICKET}/records/search",
                         body, which="notify")
    return d.get("data", {}).get("items", []) or []


def _message_tokens(msg: dict) -> set:
    raw = " ".join([msg.get("id", ""), msg.get("in_reply_to", ""), msg.get("references", "")])
    return {x.strip() for x in re.findall(r"<[^>]+>|[A-Za-z0-9_.+=/-]{8,}", raw) if x.strip()}


def _ticket_tokens(f: dict) -> set:
    vals = [_field_text(f.get("线程ID")), _field_text(f.get("最近出站Message-ID")),
            _field_text(f.get("工单ID"))]
    raw = " ".join(vals)
    return {x.strip() for x in re.findall(r"<[^>]+>|[A-Za-z0-9_.+=/-]{8,}", raw) if x.strip()}


def _seen_in_history(f: dict, msg_id: str) -> bool:
    return bool(msg_id and msg_id in _field_text(f.get("沟通历史摘要")))


def _match_waiting_info_ticket(msg: dict, waiting: list) -> dict | None:
    """Find an existing wait-info ticket for a customer's later email reply."""
    msg_tokens = _message_tokens(msg)
    sender = _customer_email(msg.get("frm"))
    same_sender = []
    for row in waiting:
        f = row.get("fields", {}) or {}
        if msg_tokens and (msg_tokens & _ticket_tokens(f)):
            return row
        if sender and sender == _customer_email(_field_text(f.get("客户标识"))):
            same_sender.append(row)
    # If one customer has exactly one pending supplement ticket, treat the new
    # email as the continuation even if the provider did not expose headers.
    return same_sender[0] if len(same_sender) == 1 else None


def _marketplace_hint(text: str):
    t = (text or "").lower()
    patterns = [
        (r"\b(walmart|relay\.walmart\.com|marketplace\.walmart\.com)\b|沃尔玛", ("沃尔玛", "林明坚")),
        (r"\b(amazon\s*(us|usa)|usa|united states|amazon\.com|america)\b|美国", ("亚马逊-美国", "黄奕纯")),
        (r"\b(amazon\s*mx|amazon\s*mexico|mexico|amazon\.com\.mx)\b|墨西哥", ("亚马逊-墨西哥", "陈翔宇")),
        (r"\b(amazon\s*ca|amazon\s*canada|canada|amazon\.ca)\b|加拿大", ("亚马逊-加拿大", "陈翔宇")),
        (r"\b(amazon\s*jp|amazon\s*japan|japan|amazon\.co\.jp)\b|日本", ("亚马逊-日本", "陈翔宇")),
        (r"\b(amazon\s*uk|uk amazon|united kingdom|amazon\.co\.uk|britain)\b|英国", ("亚马逊-英国", "林明坚")),
        (r"\b(amazon\s*eu|europe|germany|france|spain|italy|amazon\.de|amazon\.fr|amazon\.es|amazon\.it)\b|欧洲|德国|法国|西班牙|意大利",
         ("亚马逊-欧洲", "林明坚")),
    ]
    for pat, route in patterns:
        if re.search(pat, t, re.I):
            return route
    return None, None


def _is_walmart_ticket(msg: dict, c: dict | None = None) -> bool:
    c = c or {}
    text = "\n".join([
        msg.get("frm", ""),
        msg.get("subj", ""),
        msg.get("body", ""),
        str(c.get("platform") or ""),
        str(c.get("summary") or ""),
    ])
    platform, _operator = _marketplace_hint(text)
    return platform == "沃尔玛"


def _amazon_info_gaps(order_no: str, platform: str, route_basis: str = "") -> list:
    gaps = []
    if not order_no:
        gaps.append("缺订单号")
    if not platform or platform == "未知":
        gaps.append("缺国家站点")
    if order_no and (not platform or platform == "未知"):
        gaps.append("领星未命中")
    if route_basis == "site_hint" and not order_no:
        gaps.append("订单号仍缺")
    return list(dict.fromkeys(gaps))


def _info_request_reply(f: dict, missing: str = "") -> str:
    product = _field_text(f.get("产品")) or "controller"
    first = "there"
    customer = _field_text(f.get("客户标识"))
    if customer and "@" not in customer:
        first = customer.split()[0].strip(" ,") or first
    return (
        f"Dear {first},\n\n"
        f"Thank you for reaching out, and I'm sorry for the trouble with your {product}.\n\n"
        "To help us arrange the correct support as quickly as possible, could you please reply "
        "with your order number and the Amazon marketplace/country where you purchased it, "
        "such as Amazon US, Mexico, Canada, UK, EU, or Japan?\n\n"
        "If you cannot find the order number, you can also send a screenshot of the order details "
        "or let us know the purchase country/site.\n\n"
        "Once we have this information, we'll route your case to the right support team and help "
        "you with the next step.\n\n"
        "Best regards,\n"
        f"{_field_text(f.get('品牌')) or 'FUNLAB'} Support Team"
    )


def _info_request_is_safe(reply: str) -> str:
    low = (reply or "").lower()
    banned = ["free replacement", "refund", "ship a new", "we will replace", "we'll replace",
              "attached", "[link]", "tbd", "待确认", "待填"]
    for kw in banned:
        if kw in low:
            return kw
    return ""


def _to_html(body: str) -> str:
    paras = [p.strip() for p in (body or "").split("\n\n") if p.strip()]
    return "".join(f"<p>{p.replace(chr(10), '<br>')}</p>" for p in paras)


def _orig_subject_from_msg(msg: dict) -> str:
    subj = (msg.get("subj") or "your message").strip()
    subj = re.sub(r"^\s*(re|fwd|fw)\s*:\s*", "", subj, flags=re.I).strip()
    return "Re: " + (subj or "your message")


async def _zoho_send_reply(to_addr: str, subject: str, html: str, reply_to_msgid: str = "") -> str:
    tok = await _ztoken()
    base = f"https://mail.zoho.com/api/accounts/{ZACC}/messages"
    url = f"{base}/{reply_to_msgid}" if reply_to_msgid else base
    payload = {"fromAddress": ZOHO_CS_FROM, "toAddress": to_addr, "subject": subject,
               "content": html, "mailFormat": "html"}
    if reply_to_msgid:
        payload["action"] = "reply"
    async with httpx.AsyncClient(timeout=45.0) as c:
        r = await c.post(url, json=payload, headers={"Authorization": f"Zoho-oauthtoken {tok}"})
        d = r.json()
    if (d.get("status", {}) or {}).get("code") != 200:
        raise Exception(f"Zoho info request send fail: {str(d)[:300]}")
    return (d.get("data", {}) or {}).get("messageId", "")


def _netease_send_sync(to_addr: str, subject: str, html: str, in_reply_to: str = "") -> str:
    import smtplib, ssl
    msg = EmailMessage()
    msg_id = make_msgid(domain="funlabswitch.com")
    msg["From"] = formataddr(("FUNLAB Support", NE_USER))
    msg["To"] = to_addr
    msg["Subject"] = subject
    msg["Date"] = formatdate(localtime=True)
    msg["Message-ID"] = msg_id
    if in_reply_to:
        msg["In-Reply-To"] = in_reply_to
        msg["References"] = in_reply_to
    plain = re.sub(r"<[^>]+>", "", html).replace("&nbsp;", " ").strip()
    msg.set_content(plain or " ", subtype="plain", charset="utf-8")
    msg.add_alternative(html, subtype="html", charset="utf-8")
    with smtplib.SMTP_SSL(NE_SMTP, 465, context=ssl.create_default_context(), timeout=30) as s:
        s.login(NE_USER, NE_CODE)
        s.send_message(msg)
    return msg_id


async def _netease_send(to_addr: str, subject: str, html: str, in_reply_to: str = "") -> str:
    return await asyncio.to_thread(_netease_send_sync, to_addr, subject, html, in_reply_to)


async def _send_info_request(msg: dict, fields: dict, reply: str) -> tuple[str, str]:
    """Send or dry-run the info request. Returns (mode, outbound_message_id)."""
    hit = _info_request_is_safe(reply)
    if hit:
        raise ValueError(f"补询模板含禁止承诺/占位符: {hit}")
    html = _to_html(reply)
    subject = _orig_subject_from_msg(msg)
    customer = _customer_email(fields.get("客户标识", ""))
    prefix = msg.get("id_prefix")
    if CS_INFO_REQUEST_DRY_RUN_TO:
        target = f"{prefix}:{customer}"
        banner = (f'<div style="background:#fff3cd;padding:8px;border:1px solid #ffc107;margin-bottom:12px">'
                  f'<strong>CS INFO REQUEST DRY-RUN</strong> — 本应发往 <code>{target}</code>，真客户不会收到。</div>')
        mid = await _zoho_send_reply(CS_INFO_REQUEST_DRY_RUN_TO,
                                     f"[CS-INFO-DRY-RUN→{target}] {subject}", banner + html, "")
        return "dry_run", mid
    if not CS_INFO_REQUEST_LIVE:
        return "disabled", ""
    if prefix == "CSP":
        return "live", await _zoho_send_reply(customer, subject, html, msg.get("id", ""))
    if prefix == "CSF":
        return "live", await _netease_send(customer, subject, html, msg.get("id", ""))
    return "unsupported", ""


# ===== 分类 =====
CLASSIFY_PROMPT = """你是跨境电商(游戏配件 POWKONG/FUNLAB)客服分诊AI。判断这封邮件输出JSON。
规则:
1. 真实客户(客诉/咨询/售后)→is_cs=true:
   - 订单号是亚马逊格式(3位-7位-7位数字) → is_amazon=true(站点稍后由领星定);
   - 明确提到 Amazon/亚马逊/差评/review 但没有订单号 → is_amazon=true, platform=未知, draft_reply 只请求订单号和国家站点;
   - Walmart/沃尔玛/relay.walmart.com → platform=沃尔玛;
   - 美客多订单 → platform=美客多;
   - 其余非平台客诉(独立站如PK+数字) → platform=独立站(兜底);
   - 无订单号且分不清是客户还是分销商 → 默认当客户; 有平台线索按平台, 否则 platform=独立站.
2. 供应商/B2B/合作/分销 询盘 → is_cs=false, route=B2B群.
3. 营销推广/SEO外链/平台系统通知/纯垃圾 → is_cs=false, route=忽略.
4. 纯寒暄/致谢/确认收到/无实际问题或诉求的对话碎片(尤其 Discord 闲聊) → is_cs=false, route=忽略.
   Discord 降噪: 只有 "same issue" / "did anyone solve this" / "any update" / "thanks" 等跟帖碎片,
   且没有订单号、产品型号、个人故障细节、明确售后诉求时, 不要单独建客服工单; 只有能看出独立客户诉求时才 is_cs=true.

【公司售后政策 v1 (2026-06-26 Frankie 拍板) — draft_reply 必须严格遵守, 禁止"待确认/TBD/占位"】
- 质保期: 统一 12 个月(霍尔摇杆款可称 18 个月)。
- 缺陷处理: 默认免费补发/换新(不默认退款); 要客户提供故障视频/照片确认; 不要求寄回坏件;
  $50+ 手柄或可疑重复索赔 → 要求销毁视频(剪线/壳体写字); <$20 配件凭照片即补。
- 运费: 质保内缺陷 → 公司全担; 非缺陷(客户原因/不喜欢) → 客户担; Amazon 30 天内按平台规则。
- 退款 vs 补发: 默认补发; 退款只在 客户坚持退/补发后仍故障/缺货/Amazon窗口内要退/物流确认丢件不愿等。
【已发货+客户称未收到 或 要求退款 — 先查物流定性, draft 不直接承诺退款, 要引导查物流/分情况】
  ① 在途未超正常时效 → 不退, 告知最新物流+预计送达, 请客户再等;
  ② 物流停滞/明显超时/查无更新 → 判丢件 → 补发或退款(公司担);
  ③ 显示已投递/已签收但客户称没收到 → 不直接退, 引导查门口/邻居/快递柜+联系承运商查投递点; $50+或可疑重复→需核实防欺诈;
  ④ 地址错误/被退回 → 联系客户核对地址后重发。
draft_reply 用英文自然体现以上, 给客户清晰下一步(如缺陷:"within the 12-month warranty we'll ship a free replacement, no need to return the faulty unit, we cover shipping — please share a short video/photo of the issue"; 未收到:先说会核实物流/给下一步, 不承诺退款)。
【置信度 confidence — 收紧: 只有红线才"必须人工", 不要因"涉及退款/换货"就标必须人工】
- 必须人工 = 仅红线: 单笔退款>$150 / 法律威胁 / Amazon A-to-z 或差评要挟 / 疑似欺诈(重复索赔) / 政策完全未覆盖的全新情况;
- AI直答 = 操作类咨询(物流查询/确认地址/固件使用指导);
- AI起草人工审 = 质量补发/换货/未发货按树处理/质保内 且 金额≤$150 的常规客诉(运营审核草稿后自己发, 不升级)。
字段:
is_cs(bool), is_amazon(bool), route(B2B群/忽略/空),
brand(FUNLAB或POWKONG, 据产品判断, 不确定用给定的默认品牌),
platform(沃尔玛/美客多/独立站/未知 四选一; 亚马逊单填未知),
complaint_type(物流/产品/退换货/售后/投诉升级, 非客服留空),
product, order_no, language(EN/中文/德/法/西/葡/日/其他),
summary(一句中文摘要), confidence(AI直答/AI起草人工审/必须人工),
draft_reply(给客户的英文回复草稿, 非客服留空), reason(一句中文).
只输出JSON。"""


async def _classify(msg: dict) -> dict:
    prompt = (CLASSIFY_PROMPT
              + f"\n\n[此邮箱默认品牌:{msg['brand_default']}]\n发件人:{msg['frm']}\n"
              + f"主题:{msg['subj']}\n正文:{msg['body'][:1800]}")
    return await deepseek.chat_json(prompt, max_tokens=900, temperature=0.2)


def _pick(v, opts, default=None):
    return v if v in opts else default


def _to_fields(msg: dict, c: dict, amz_override=None, resources: list | None = None) -> dict:
    order_no = (c.get("order_no") or "").strip()
    is_amazon = bool(c.get("is_amazon")) or bool(AMZ_ORDER_RE.search(order_no))
    is_walmart = _is_walmart_ticket(msg, c)
    is_cs = bool(c.get("is_cs"))
    summary = (c.get("summary") or "").strip()
    info_gaps, route_basis = [], ""

    if not is_cs:
        route = c.get("route") or "忽略"
        platform, operator, status = "未知", "", "归档非客服"
        summary = f"[→{route}] {summary}" if route else summary
    else:
        status = "待派"
        if is_walmart:
            platform, operator = "沃尔玛", "林明坚"
        elif is_amazon:
            if amz_override and amz_override[0]:
                platform, operator = amz_override[0], amz_override[1]  # 领星/文本命中真实站点
                route_basis = amz_override[2] if len(amz_override) > 2 else "order_lookup"
            else:
                platform, operator = "未知", "待定·领星反查站点"
            info_gaps = _amazon_info_gaps(order_no, platform, route_basis)
            if platform == "未知":
                status = STATUS_WAIT_INFO
                summary = f"[亚马逊单·需客户补订单号/站点] {summary}"
            elif info_gaps:
                summary = f"[亚马逊单·{'/'.join(info_gaps)}] {summary}"
        elif c.get("platform") == "美客多":
            platform, operator = "美客多", "梁俊辉"
        else:
            platform, operator = "独立站", "张佳烨"

    brand = _pick(c.get("brand"), ["FUNLAB", "POWKONG"], msg["brand_default"])
    fields = {
        "工单ID": f"{msg['id_prefix']}-{msg['id']}"[:200],
        "入站时间": int(msg.get("received_ms") or 0),
        "渠道": msg["channel"],
        "品牌": brand,
        "销售平台": _pick(platform, PLATFORM_OPTS, "未知"),
        "产品": (c.get("product") or "")[:200],
        "客户标识": msg["frm"],
        "订单号": order_no,
        "客诉摘要": summary[:500],
        "原文": (msg["subj"] + "\n\n" + msg["body"])[:8000],
        "语种": _pick(c.get("language"), LANG_OPTS, "其他"),
        "AI置信度": _pick(c.get("confidence"), CONF_OPTS, "必须人工"),
        "AI草稿": (c.get("draft_reply") or "")[:5000],
        "分配运营": operator,
        "状态": status,
        "线程ID": msg["id"],
    }
    if info_gaps:
        fields["信息缺口"] = " / ".join(info_gaps)
        fields["沟通历史摘要"] = (f"首封问题: {summary[:260]}\n"
                            f"路由依据: {route_basis or '待客户补充'}\n"
                            f"仍缺字段: {' / '.join(info_gaps)}")[:5000]
    if not (msg.get("attachments") or []):
        fields.update(_attachment_base_fields([]))
    if status == STATUS_WAIT_INFO:
        fields["AI草稿"] = _info_request_reply(fields, fields.get("信息缺口", ""))[:5000]
        fields["补充信息次数"] = 0
    ct = _pick(c.get("complaint_type"), TYPE_OPTS, None)
    if ct:
        fields["客诉类型"] = ct
    ctx = cs_resources.resolve_for_ticket(fields, resources=resources)
    resource_reply = cs_resources.build_resource_reply(fields, ctx)
    if resource_reply and status != STATUS_WAIT_INFO:
        fields["AI草稿"] = resource_reply[:5000]
    if cs_resources.WRITEBACK_TICKET_FIELDS:
        fields.update(cs_resources.ticket_resource_fields(ctx))
    return fields


def _info_send_update(fields: dict, mode: str, outbound_msg_id: str = "") -> dict:
    now = int(time.time() * 1000)
    old_hist = _field_text(fields.get("沟通历史摘要"))
    if mode == "live":
        note = "系统补询: 已同线程发送给客户。"
        count = int(float(_field_text(fields.get("补充信息次数")) or 0)) + 1
    elif mode == "dry_run":
        note = f"系统补询: DRY-RUN 已发测试邮箱 {CS_INFO_REQUEST_DRY_RUN_TO}，真客户未收到。"
        count = int(float(_field_text(fields.get("补充信息次数")) or 0))
    elif mode == "disabled":
        note = "系统补询: CS_INFO_REQUEST_LIVE=0，未自动发给客户。"
        count = int(float(_field_text(fields.get("补充信息次数")) or 0))
    else:
        note = f"系统补询: 未发送，mode={mode}。"
        count = int(float(_field_text(fields.get("补充信息次数")) or 0))
    update = {
        "补充信息请求时间": now,
        "补充信息次数": count,
        "沟通历史摘要": (old_hist + "\n" + note).strip()[:5000],
    }
    if mode == "live" and outbound_msg_id:
        update["最近出站Message-ID"] = outbound_msg_id[:1000]
    return update


async def _reroute_from_supplement(msg: dict, f: dict) -> tuple[str, str, str, str]:
    text = "\n".join([msg.get("subj", ""), msg.get("body", ""), _field_text(f.get("最近客户补充"))])
    order = _field_text(f.get("订单号"))
    mo = AMZ_ORDER_RE.search(text)
    if mo:
        order = mo.group(0)
    platform, operator, basis = "", "", ""
    if order:
        platform, operator = await _lookup_amazon_route(order)
        basis = "order_lookup" if platform else ""
    if not platform:
        platform, operator = _marketplace_hint(text)
        basis = "site_hint" if platform else basis
    return order, platform or "", operator or "", basis


async def _operator_draft_after_supplement(msg: dict, f: dict, order_no: str,
                                           platform: str, resources: list | None) -> tuple[str, str]:
    original = _field_text(f.get("原文"))
    body = (original + "\n\n[Customer supplement]\n" + (msg.get("body") or ""))[:8000]
    synth = {
        "brand_default": _field_text(f.get("品牌")) or "FUNLAB",
        "frm": _field_text(f.get("客户标识")) or msg.get("frm", ""),
        "subj": _orig_subject_from_msg(msg),
        "body": body,
    }
    try:
        c = await _classify(synth)
    except Exception:
        c = {}
    draft = (c.get("draft_reply") or _field_text(f.get("AI草稿")) or "").strip()
    summary = (c.get("summary") or _field_text(f.get("客诉摘要")) or "").strip()
    tmp = dict(f)
    tmp.update({"订单号": order_no, "销售平台": platform, "AI草稿": draft, "客诉摘要": summary})
    ctx = cs_resources.resolve_for_ticket(tmp, resources=resources)
    resource_reply = cs_resources.build_resource_reply(tmp, ctx)
    if resource_reply:
        draft = resource_reply
    return summary[:500], draft[:5000]


async def _handle_waiting_info_reply(row: dict, msg: dict, resources: list | None,
                                     dry_run: bool = False) -> dict:
    rid = row.get("record_id")
    f = row.get("fields", {}) or {}
    if not rid:
        return {"action": "wait_reply_skip", "reason": "missing_record_id"}
    if _seen_in_history(f, msg.get("id", "")):
        return {"action": "wait_reply_skip", "reason": "already_seen", "record_id": rid}

    supplement = (msg.get("body") or "")[:3000]
    old_hist = _field_text(f.get("沟通历史摘要"))
    count = int(float(_field_text(f.get("补充信息次数")) or 0))
    order_no, platform, operator, basis = await _reroute_from_supplement(msg, f)
    gaps = _amazon_info_gaps(order_no, platform, basis)
    common = {
        "最近客户补充": supplement[:5000],
        "沟通历史摘要": (old_hist + f"\n客户补充({msg.get('id','')[:120]}): {supplement[:800]}").strip()[:5000],
    }

    if platform and operator:
        summary, draft = await _operator_draft_after_supplement(msg, f, order_no, platform, resources)
        update = {
            **common,
            "订单号": order_no,
            "销售平台": platform,
            "分配运营": operator,
            "状态": "待派",
            "信息缺口": " / ".join(gaps),
            "客诉摘要": summary or _field_text(f.get("客诉摘要")),
            "AI草稿": draft or _field_text(f.get("AI草稿")),
            "卡片消息ID": "",
        }
        update["沟通历史摘要"] = (update["沟通历史摘要"]
                             + f"\n重路由: {platform} / {operator} / 依据={basis or 'unknown'}"
                             + (f" / 仍缺: {' / '.join(gaps)}" if gaps else "")).strip()[:5000]
        if not dry_run:
            await feishu.api("PUT", f"/bitable/v1/apps/{CS_APP_TOKEN}/tables/{T_TICKET}/records/{rid}",
                             {"fields": update}, which="notify")
        return {"action": "wait_reply_rerouted", "record_id": rid, "platform": platform,
                "operator": operator, "dry_run": dry_run}

    if count < CS_INFO_REQUEST_MAX:
        tmp = dict(f)
        tmp.update(common)
        reply = _info_request_reply(tmp, _field_text(f.get("信息缺口")))
        update = {**common, "AI草稿": reply[:5000], "信息缺口": " / ".join(gaps or ["缺订单号", "缺国家站点"])}
        if not dry_run:
            mode, outbound = await _send_info_request(msg, tmp, reply)
            update.update(_info_send_update(tmp, mode, outbound))
            await feishu.api("PUT", f"/bitable/v1/apps/{CS_APP_TOKEN}/tables/{T_TICKET}/records/{rid}",
                             {"fields": update}, which="notify")
        return {"action": "wait_reply_asked_again", "record_id": rid, "dry_run": dry_run}

    update = {
        **common,
        "订单号": order_no,
        "销售平台": "未知",
        "分配运营": "待定·客户补充仍不足",
        "状态": "待派",
        "信息缺口": " / ".join(gaps or ["缺订单号", "缺国家站点"]),
        "卡片消息ID": "",
    }
    update["沟通历史摘要"] = (update["沟通历史摘要"]
                         + "\n自动补询已达上限，仍无法判定站点/订单，转待判责卡。")[:5000]
    if not dry_run:
        await feishu.api("PUT", f"/bitable/v1/apps/{CS_APP_TOKEN}/tables/{T_TICKET}/records/{rid}",
                         {"fields": update}, which="notify")
    return {"action": "wait_reply_escalate_to_triage", "record_id": rid, "dry_run": dry_run}


async def backfill_evidence(record_id: str, dry_run: bool = False, scan_limit: int = 500) -> dict:
    """Re-read the original mailbox message for an existing ticket and attach evidence files."""
    if not record_id:
        return {"ok": False, "error": "missing record_id"}
    rec = await feishu.api("GET", f"/bitable/v1/apps/{CS_APP_TOKEN}/tables/{T_TICKET}/records/{record_id}",
                           which="notify")
    f = ((rec.get("data") or {}).get("record") or {}).get("fields", {}) or {}
    ticket = _field_text(f.get("工单ID"))
    thread_id = _field_text(f.get("线程ID"))
    prefix = (ticket.split("-", 1)[0] if "-" in ticket else "").upper()
    msg = {}
    if prefix == "CSF":
        msg = await _fetch_funlab_one(thread_id, scan_limit=scan_limit)
    elif prefix == "CSP":
        msg = await _fetch_powkong_one(thread_id)
    else:
        return {"ok": False, "record_id": record_id, "error": f"unsupported ticket prefix: {prefix or ticket}"}

    attachments = msg.get("attachments") or []
    if dry_run:
        fields = _attachment_base_fields([_public_attachment_meta(a) for a in attachments])
        return {"ok": True, "record_id": record_id, "dry_run": True,
                "thread_id": thread_id, "found_message": bool(msg),
                "attachment_count": len(attachments), "fields": fields}
    fields = await _save_attachments_to_ticket(record_id, attachments, existing_fields=f, dry_run=False)
    return {"ok": True, "record_id": record_id, "thread_id": thread_id,
            "found_message": bool(msg), "attachment_count": len(attachments),
            "saved_count": fields.get("客户附件数量", 0),
            "status": fields.get("客户附件状态"),
            "summary": fields.get("客户附件摘要", "")[:1000]}


# ===== 主入口 =====
async def run(source: str = "all", limit: int = 20, dry_run: bool = False) -> dict:
    src_err = {}
    msgs = []
    if source in ("all", "powkong"):
        try:
            msgs += await _fetch_powkong(limit)
        except Exception as e:
            src_err["powkong"] = str(e)[:200]
    if source in ("all", "funlab"):
        try:
            msgs += await _fetch_funlab(limit)
        except Exception as e:
            src_err["funlab"] = str(e)[:200]
    if source in ("all", "discord"):
        try:
            msgs += await _fetch_discord(limit)
        except Exception as e:
            src_err["discord"] = str(e)[:200]

    try:
        resources = await cs_resources.active_resources()
    except Exception:
        resources = cs_resources.builtin_resources()
    existing = await _existing_thread_ids()
    waiting = await _waiting_info_tickets()
    new_cnt, skip_cnt, err_cnt = 0, 0, 0
    samples = []
    for m in msgs:
        if not m.get("id") or m["id"] in existing:
            skip_cnt += 1
            continue
        waiting_match = _match_waiting_info_ticket(m, waiting)
        if waiting_match:
            try:
                result = await _handle_waiting_info_reply(waiting_match, m, resources, dry_run=dry_run)
                if (m.get("attachments") or []) and result.get("record_id") and not dry_run:
                    await _save_attachments_to_ticket(result["record_id"], m.get("attachments") or [],
                                                      existing_fields=(waiting_match.get("fields") or {}),
                                                      dry_run=False)
                if len(samples) < 14:
                    samples.append({"from": m["frm"][:26], "状态": "客户补充归并",
                                    "动作": result.get("action"), "record": result.get("record_id", ""),
                                    "附件": len(m.get("attachments") or [])})
                new_cnt += 1
            except Exception:
                err_cnt += 1
            continue
        try:
            c = await _classify(m)
        except Exception:
            err_cnt += 1
            continue
        # 亚马逊客诉 → 领星反查真实站点 → 对应运营(订单号格式判不出站点)
        amz_override = None
        if c.get("is_cs"):
            mo = AMZ_ORDER_RE.search(c.get("order_no") or "")
            if mo:
                p, op = await _lookup_amazon_route(mo.group(0))
                if p:
                    amz_override = (p, op, "order_lookup")
            if not amz_override and (c.get("is_amazon") or "amazon" in (m.get("body", "") + m.get("subj", "")).lower() or "亚马逊" in (m.get("body", "") + m.get("subj", ""))):
                p, op = _marketplace_hint(m.get("body", "") + "\n" + m.get("subj", ""))
                if p:
                    amz_override = (p, op, "site_hint")
        fields = _to_fields(m, c, amz_override, resources=resources)
        if fields.get("状态") == STATUS_WAIT_INFO and not dry_run:
            mode, outbound = await _send_info_request(m, fields, fields.get("AI草稿", ""))
            fields.update(_info_send_update(fields, mode, outbound))
        if len(samples) < 14:
            samples.append({"渠道品牌": f"{fields['品牌']}", "from": m["frm"][:26],
                            "is_cs": c.get("is_cs"), "平台": fields["销售平台"],
                            "运营": fields["分配运营"], "状态": fields["状态"],
                            "附件": len(m.get("attachments") or []),
                            "摘要": fields["客诉摘要"][:48]})
        if not dry_run:
            created = await feishu.api(
                "POST", f"/bitable/v1/apps/{CS_APP_TOKEN}/tables/{T_TICKET}/records",
                {"fields": fields}, which="notify")
            rid = (((created.get("data") or {}).get("record") or {}).get("record_id")
                   or ((created.get("data") or {}).get("record_id") or ""))
            if rid and (m.get("attachments") or []):
                await _save_attachments_to_ticket(rid, m.get("attachments") or [], dry_run=False)
        new_cnt += 1

    return {"sources": source, "fetched": len(msgs), "new": new_cnt, "skipped": skip_cnt,
            "errors": err_cnt, "source_errors": src_err, "dry_run": dry_run, "samples": samples}

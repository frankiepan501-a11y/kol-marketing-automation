"""Zoho Mail API - 双品牌 token 管理"""
import httpx, time, urllib.parse
from . import config

_access = {}  # brand → (token, expiry_ts)


async def refresh_access(brand: str):
    cfg = config.BRAND_CONFIG[brand]
    async with httpx.AsyncClient(timeout=30.0) as cli:
        r = await cli.post(
            f"https://accounts.zoho{config.ZOHO_REGION}/oauth/v2/token",
            data={
                "refresh_token": cfg["refresh_token"],
                "client_id": cfg["client_id"],
                "client_secret": cfg["client_secret"],
                "grant_type": "refresh_token",
            },
        )
        r.raise_for_status()
        d = r.json()
        if "access_token" not in d:
            raise Exception(f"Zoho refresh failed: {d}")
        _access[brand] = (d["access_token"], time.time() + 3300)  # 55 min cache
        return d["access_token"]


async def access(brand: str):
    cached = _access.get(brand)
    if cached and cached[1] > time.time():
        return cached[0]
    return await refresh_access(brand)


def _ensure_html(body: str) -> str:
    """如果 body 是纯文本 (没有 <p>/<br>/<div> 等 HTML 标签), 自动转 HTML:
    - **xxx** → <strong>xxx</strong> (markdown bold)
    - 段落用 <p>...</p> 包裹
    - 单换行 \\n → <br>
    """
    import re
    if not body:
        return ""
    # 已经是 HTML 跳过
    if re.search(r"<(p|div|br|h[1-6]|li|strong|em|a)[\s>/]", body, re.I):
        return body
    # markdown 加粗
    s = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", body)
    # 按双换行切段, 段内单换行用 <br>
    paragraphs = [p.strip() for p in s.split("\n\n") if p.strip()]
    return "".join(f"<p>{p.replace(chr(10), '<br>')}</p>" for p in paragraphs)


async def send_email(brand: str, to_addr: str, subject: str, body: str):
    """发送邮件 — 自动 plaintext→HTML + DRY-RUN 重定向支持

    DRY-RUN: 如果 env `EMAIL_DRY_RUN_TO` 有值, 自动把 to 改成此邮箱,
    主题前加 [DRY-RUN→{真实 to}], 防止改代码时误发到真客户。
    """
    import os
    cfg = config.BRAND_CONFIG[brand]
    tok = await access(brand)
    html_body = _ensure_html(body)

    real_to = to_addr
    dry_run_to = os.environ.get("EMAIL_DRY_RUN_TO", "").strip()
    if dry_run_to:
        to_addr = dry_run_to
        subject = f"[DRY-RUN→{real_to}] {subject}"
        html_body = (
            f"<div style=\"background:#fff3cd;padding:8px;border:1px solid #ffc107;margin-bottom:12px\">"
            f"<strong>⚠️ DRY-RUN MODE</strong> — 这封邮件本来要发给 <code>{real_to}</code>, "
            f"但 EMAIL_DRY_RUN_TO env 已设置为 <code>{dry_run_to}</code>, 真客户不会收到。</div>"
            + html_body
        )
        print(f"[zoho.send_email DRY-RUN] {real_to} → {to_addr}")

    async with httpx.AsyncClient(timeout=45.0) as cli:
        r = await cli.post(
            f"https://mail.zoho.com/api/accounts/{cfg['account_id']}/messages",
            json={
                "fromAddress": cfg["alias_from"],
                "toAddress": to_addr,
                "subject": subject,
                "content": html_body,
                "mailFormat": "html",
            },
            headers={"Authorization": f"Zoho-oauthtoken {tok}"},
        )
        r.raise_for_status()
        d = r.json()
        if d.get("status", {}).get("code") != 200:
            raise Exception(f"Send fail: {d}")
        return d["data"].get("messageId")


async def search_inbox(brand: str, search_key: str, limit: int = 30):
    """search_key 如 'to:partner@powkong.com'"""
    cfg = config.BRAND_CONFIG[brand]
    tok = await access(brand)
    params = urllib.parse.quote(search_key)
    async with httpx.AsyncClient(timeout=30.0) as cli:
        r = await cli.get(
            f"https://mail.zoho.com/api/accounts/{cfg['account_id']}/messages/search?searchKey={params}&limit={limit}",
            headers={"Authorization": f"Zoho-oauthtoken {tok}"},
        )
        r.raise_for_status()
        return r.json().get("data") or []


async def test_send(brand: str, to_addr: str, subject: str = "[Test] Zoho OAuth check", body: str = "<p>Test email — please ignore.</p>"):
    """测试发邮件 (验证 send_email API 是否能工作, 不依赖 folders scope)"""
    return await send_email(brand, to_addr, subject, body)


async def list_folders(brand: str):
    """列出账户所有 folder, 用于找 sent folder id"""
    cfg = config.BRAND_CONFIG[brand]
    tok = await access(brand)
    async with httpx.AsyncClient(timeout=30.0) as cli:
        r = await cli.get(
            f"https://mail.zoho.com/api/accounts/{cfg['account_id']}/folders",
            headers={"Authorization": f"Zoho-oauthtoken {tok}"},
        )
        r.raise_for_status()
        return r.json().get("data") or []


async def list_sent_messages(brand: str, limit: int = 30):
    """列出 sent folder 最近发出的邮件"""
    folders = await list_folders(brand)
    sent_folder = None
    for f in folders:
        ftype = (f.get("folderType") or "").lower()
        fname = (f.get("folderName") or "").lower()
        if ftype == "sent" or fname in ("sent", "sent items", "已发送"):
            sent_folder = f
            break
    if not sent_folder:
        return {"error": "no sent folder", "folders": [f.get("folderName") for f in folders]}
    cfg = config.BRAND_CONFIG[brand]
    tok = await access(brand)
    fid = sent_folder["folderId"]
    async with httpx.AsyncClient(timeout=30.0) as cli:
        r = await cli.get(
            f"https://mail.zoho.com/api/accounts/{cfg['account_id']}/messages/view?folderId={fid}&limit={limit}&start=0",
            headers={"Authorization": f"Zoho-oauthtoken {tok}"},
        )
        r.raise_for_status()
        return {"sent_folder": sent_folder, "messages": r.json().get("data") or []}


async def get_message_content(brand: str, msg_id: str, folder_id: str):
    cfg = config.BRAND_CONFIG[brand]
    tok = await access(brand)
    async with httpx.AsyncClient(timeout=30.0) as cli:
        r = await cli.get(
            f"https://mail.zoho.com/api/accounts/{cfg['account_id']}/folders/{folder_id}/messages/{msg_id}/content",
            headers={"Authorization": f"Zoho-oauthtoken {tok}"},
        )
        if r.status_code != 200:
            return ""
        return r.json().get("data", {}).get("content", "")

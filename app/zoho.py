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


async def send_email(brand: str, to_addr: str, subject: str, html_body: str):
    cfg = config.BRAND_CONFIG[brand]
    tok = await access(brand)
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

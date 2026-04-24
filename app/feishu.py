"""飞书 API 封装 - 双 App token 管理"""
import httpx
import time
from . import config

_tokens = {}  # {"bitable": (token, expiry_ts), "notify": ...}


async def _refresh_token(which: str):
    if which == "bitable":
        aid, sec = config.FEISHU_BITABLE_APP_ID, config.FEISHU_BITABLE_APP_SECRET
    else:
        aid, sec = config.FEISHU_NOTIFY_APP_ID, config.FEISHU_NOTIFY_APP_SECRET
    async with httpx.AsyncClient() as cli:
        r = await cli.post(
            "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
            json={"app_id": aid, "app_secret": sec},
            timeout=30.0,
        )
        r.raise_for_status()
        data = r.json()
        tok = data["tenant_access_token"]
        # Feishu token expires in ~2h, cache 90 min
        _tokens[which] = (tok, time.time() + 5400)
        return tok


async def token(which: str = "bitable"):
    cached = _tokens.get(which)
    if cached and cached[1] > time.time():
        return cached[0]
    return await _refresh_token(which)


async def api(method: str, path: str, body=None, which: str = "bitable"):
    tok = await token(which)
    url = f"https://open.feishu.cn/open-apis{path}"
    headers = {"Authorization": f"Bearer {tok}", "Content-Type": "application/json; charset=utf-8"}
    async with httpx.AsyncClient(timeout=60.0) as cli:
        r = await cli.request(method, url, json=body, headers=headers)
        if r.status_code >= 400:
            raise Exception(f"{method} {path} → {r.status_code}: {r.text[:300]}")
        return r.json()


# ===== Helpers =====
def ext(f):
    if f is None: return ""
    if isinstance(f, list):
        if not f: return ""
        if isinstance(f[0], dict): return f[0].get("text") or f[0].get("link") or f[0].get("name") or ""
        return str(f[0])
    if isinstance(f, dict): return f.get("text") or f.get("link") or ""
    return f or ""


def xrid(f):
    if not f: return None
    if isinstance(f, dict):
        ids = f.get("link_record_ids") or f.get("record_ids")
        if ids and isinstance(ids, list) and ids: return ids[0]
    if isinstance(f, list) and f:
        x = f[0]
        if isinstance(x, dict):
            ids = x.get("record_ids") or x.get("link_record_ids")
            if ids and isinstance(ids, list) and ids: return ids[0]
    return None


async def fetch_all_records(table_id: str):
    items = []
    page_token = ""
    while True:
        path = f"/bitable/v1/apps/{config.FEISHU_APP_TOKEN}/tables/{table_id}/records?page_size=100"
        if page_token: path += f"&page_token={page_token}"
        r = await api("GET", path)
        d = r.get("data") or {}
        items.extend(d.get("items") or [])
        if not d.get("has_more"): break
        page_token = d.get("page_token", "")
        if not page_token: break
    return items


async def search_records(table_id: str, filters: list):
    """filters: [{"field_name":..., "operator":..., "value": [...]}, ...]"""
    items = []
    page_token = ""
    while True:
        path = f"/bitable/v1/apps/{config.FEISHU_APP_TOKEN}/tables/{table_id}/records/search?page_size=100"
        if page_token: path += f"&page_token={page_token}"
        body = {"filter": {"conjunction": "and", "conditions": filters}} if filters else {}
        r = await api("POST", path, body)
        d = r.get("data") or {}
        items.extend(d.get("items") or [])
        if not d.get("has_more"): break
        page_token = d.get("page_token", "")
        if not page_token: break
    return items


async def get_record(table_id: str, record_id: str):
    r = await api("GET", f"/bitable/v1/apps/{config.FEISHU_APP_TOKEN}/tables/{table_id}/records/{record_id}")
    return r["data"]["record"]


async def update_record(table_id: str, record_id: str, fields: dict):
    await api("PUT", f"/bitable/v1/apps/{config.FEISHU_APP_TOKEN}/tables/{table_id}/records/{record_id}",
              {"fields": fields})


async def create_record(table_id: str, fields: dict):
    r = await api("POST", f"/bitable/v1/apps/{config.FEISHU_APP_TOKEN}/tables/{table_id}/records",
                  {"fields": fields})
    return r["data"]["record"]["record_id"]


async def send_card_message(receive_type: str, receive_id: str, card: dict):
    """用聪哥分身1号发飞书互动卡片"""
    import json
    body = {
        "receive_id": receive_id,
        "msg_type": "interactive",
        "content": json.dumps(card, ensure_ascii=False),
    }
    await api("POST", f"/im/v1/messages?receive_id_type={receive_type}", body, which="notify")

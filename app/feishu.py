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
        # 用飞书返回的真实 expire (实测 ~3822-4100s = 63-68min), 提前 5min 刷新
        # 修复 2026-05-16: 旧版写死 5400s (90min) > 实际有效期 → 偶发 99991663
        expire = int(data.get("expire") or 3600) - 300
        _tokens[which] = (tok, time.time() + max(60, expire))
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
    """从飞书字段值抽取文本.

    🚨 5/8 root cause fix: 飞书 search/get API 对**多段文本**字段返回:
      [{"text":"Hi CTA,\\n", "type":"text"}, {"text":"\\n"}, {"text":"段 2..."}, ...]
    历史只取 [0].text → 拿到 8 字符 "Hi CTA,\\n" → 发出去 KOL 收到 8 字符空白邮件.
    受害事故: 4/26 Scott Stein (CNET) / 5/6+5/8 ctatechdesk / 5/8 mafastudios+gameknight3227.
    之前 memory zoho-mailformat-html-pitfall 误归因 Zoho HTML, 真正根因是 ext() bug.

    修复: array of dict 拼接所有 segment 的 text 字段.
    单选/链接字段不受影响 (链接通常 1 个 element, 单选直接是 string).
    """
    if f is None: return ""
    if isinstance(f, list):
        if not f: return ""
        if isinstance(f[0], dict):
            parts = []
            for item in f:
                if not isinstance(item, dict): continue
                t = item.get("text") or item.get("link") or item.get("name")
                if t: parts.append(t)
            return "".join(parts) if parts else ""
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


# ===== 按职务实时查在职员工 (聪哥1号 contact:contact:readonly) =====
# 遵守 feishu-people-as-source-of-truth 铁律: 不硬编码 open_id, 按职务实时查飞书人事
# 缓存 1h: 避免每次发卡片都拉一遍部门列表 (大约 7-10 个部门 + 每部门 1 次 user list)
_job_title_cache = {}  # {title: (timestamp, [(name, open_id), ...])}
_JOB_TITLE_TTL = 3600


async def fetch_users_by_job_title(title: str):
    """按职务名拿当前在职员工 [(name, open_id), ...].
    用聪哥1号 (notify app) contact API, 已开通 contact:contact:readonly.
    1h 缓存. 失败时返回空列表 (调用方应有降级路径).
    """
    cached = _job_title_cache.get(title)
    if cached and (time.time() - cached[0]) < _JOB_TITLE_TTL:
        return cached[1]

    tok = await token("notify")
    results = []
    try:
        # 1. 列所有顶级部门 (fetch_child=true 拿全树)
        async with httpx.AsyncClient(timeout=30.0) as cli:
            r = await cli.get(
                "https://open.feishu.cn/open-apis/contact/v3/departments",
                params={"page_size": 50, "fetch_child": "true",
                        "parent_department_id": "0",
                        "department_id_type": "open_department_id"},
                headers={"Authorization": f"Bearer {tok}"},
            )
            r.raise_for_status()
            depts = (r.json().get("data") or {}).get("items") or []

            # 2. 按部门列用户 (含 job_title + status)
            seen = set()
            for d in depts:
                dept_id = d.get("open_department_id")
                if not dept_id:
                    continue
                page_token = ""
                while True:
                    params = {"department_id": dept_id, "page_size": 50,
                              "user_id_type": "open_id",
                              "department_id_type": "open_department_id"}
                    if page_token:
                        params["page_token"] = page_token
                    ur = await cli.get(
                        "https://open.feishu.cn/open-apis/contact/v3/users",
                        params=params,
                        headers={"Authorization": f"Bearer {tok}"},
                    )
                    if ur.status_code >= 400:
                        break
                    ud = ur.json()
                    if ud.get("code") != 0:
                        break
                    items = (ud.get("data") or {}).get("items") or []
                    for u in items:
                        oid = u.get("open_id")
                        if not oid or oid in seen:
                            continue
                        seen.add(oid)
                        if u.get("job_title") != title:
                            continue
                        s = u.get("status") or {}
                        is_active = (s.get("is_activated") and
                                     not s.get("is_resigned") and
                                     not s.get("is_frozen"))
                        if is_active:
                            results.append((u.get("name", ""), oid))
                    if not (ud.get("data") or {}).get("has_more"):
                        break
                    page_token = (ud.get("data") or {}).get("page_token") or ""
                    if not page_token:
                        break
    except Exception as e:
        print(f"[feishu.fetch_users_by_job_title] {title} err: {e}")
        # 失败不缓存, 下次重试
        return []

    _job_title_cache[title] = (time.time(), results)
    return results

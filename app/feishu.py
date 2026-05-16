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


import re as _re
# RFC 5322-lite, 实际飞书/Zoho 都用这种简化校验
_EMAIL_RE = _re.compile(r'[\w.+-]+@[\w-]+(?:\.[\w-]+)+')

def clean_email(raw: str) -> tuple:
    """从 KOL 主表「邮箱」字段抽出一个能发送的邮箱.

    处理场景:
    - 单个干净邮箱 → 原样返回
    - 多邮箱换行/分号/逗号分隔 (e.g. "a@x.com\\nb@y.com") → 取第一个
    - 含 "dm" / "待补" / 中文等非邮箱字符 → 返回 ("", reason)
    - 含 @ 但无有效域名 (e.g. "@username") → 返回 ("", reason)

    Returns: (clean_email_or_empty, reason_if_skipped)
        - clean_email 非空 = 可发
        - reason 非空 = 跳过原因 (写进发送错误字段方便运营定位)
    """
    if not raw:
        return "", "邮箱字段为空"
    raw = str(raw).strip()
    matches = _EMAIL_RE.findall(raw)
    if not matches:
        return "", f"未找到有效邮箱: {raw[:60]!r}"
    first = matches[0].lower()
    if len(matches) > 1:
        # 多邮箱: 用第一个 + 写原因供运营追溯
        return first, f"原始字段含 {len(matches)} 个邮箱, 已自动选第一个: {first} (全部: {matches})"
    return first, ""


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


async def search_records(table_id: str, filters: list, field_names: list = None):
    """filters: [{"field_name":..., "operator":..., "value": [...]}, ...]
    field_names: 只拉这些字段, 减少 payload (2026-05-17 A4 性能优化, 可选)"""
    items = []
    page_token = ""
    while True:
        path = f"/bitable/v1/apps/{config.FEISHU_APP_TOKEN}/tables/{table_id}/records/search?page_size=500"
        if page_token: path += f"&page_token={page_token}"
        body = {}
        if filters:
            body["filter"] = {"conjunction": "and", "conditions": filters}
        if field_names:
            body["field_names"] = field_names
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


async def send_card_message(receive_type: str, receive_id: str, card: dict) -> str:
    """用聪哥分身1号发飞书互动卡片. 2026-05-17 返回 message_id (供 A5 后续 update).
    返回空字符串 = 拿不到 msg_id (不影响主流程)
    """
    import json
    body = {
        "receive_id": receive_id,
        "msg_type": "interactive",
        "content": json.dumps(card, ensure_ascii=False),
    }
    resp = await api("POST", f"/im/v1/messages?receive_id_type={receive_type}", body, which="notify")
    return (resp.get("data") or {}).get("message_id") or ""


async def update_card_message(message_id: str, new_card: dict) -> bool:
    """PATCH 互动卡片消息 (2026-05-17 A5 用, 标"已审" 重渲染卡片)
    成功返 True, 失败返 False (静默 print, 不阻塞主流程)
    """
    import json
    if not message_id:
        return False
    try:
        body = {"content": json.dumps(new_card, ensure_ascii=False)}
        await api("PATCH", f"/im/v1/messages/{message_id}", body, which="notify")
        return True
    except Exception as e:
        print(f"[feishu.update_card_message] {message_id} fail: {e}")
        return False


async def mark_card_resolved(draft_rid: str, result_label: str, table_id: str = None):
    """草稿状态结束 (已发送/已否决/退回重生) 时, 把原群卡片标题前缀加 [✅已审-xxx]
    让群里其他 reviewer 不会再点开作废卡片. 2026-05-17 A5.

    Args:
        draft_rid: 草稿记录 id
        result_label: "已发送" / "已否决" / "已重生" 等结束态描述
        table_id: 默认 config.T_DRAFT
    """
    from . import config
    table_id = table_id or config.T_DRAFT
    try:
        rec = await get_record(table_id, draft_rid)
    except Exception as e:
        print(f"[mark_card_resolved] get_record fail: {e}")
        return

    f = rec["fields"]
    msg_id = ext(f.get("卡片群消息ID"))
    if not msg_id:
        return  # 没存群 msg_id, 不需要 update
    if f.get("卡片已标记已审"):
        return  # 已经标过, 防重复 update

    subject = ext(f.get("邮件主题"))[:80]
    source = ext(f.get("邮件草稿来源")) or "cold"
    contact_type = ext(f.get("对象类型")) or "KOL"

    # 简化版"已审"卡片 (灰色, 防误点)
    resolved_card = {
        "header": {
            "template": "grey",
            "title": {"tag": "plain_text",
                      "content": f"✅ [已审-{result_label}] {source} / {contact_type} 草稿已处理"},
        },
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md",
                "content": f"**主题**: {subject}\n\n**结果**: {result_label}\n\n_此卡片已作废, 无需再审_"}},
        ],
    }
    ok = await update_card_message(msg_id, resolved_card)
    if ok:
        try:
            await update_record(table_id, draft_rid, {"卡片已标记已审": True})
        except Exception:
            pass


async def mark_card_receipt(draft_rid: str, success_count: int, fail_count: int,
                             errors: list, group_msg_id: str = "", table_id: str = None):
    """回写卡片发送状态到草稿表 (T_DRAFT) — 让运营从飞书看哪些卡片发成功了.

    Args:
        draft_rid: 草稿记录 id
        success_count: 发成功的卡片数 (含群通知 + 个人通知)
        fail_count: 失败数
        errors: [str, ...] 各 target 的错误描述, 截断到 300 字
        group_msg_id: 群卡片消息 id (2026-05-17 A5, 用于状态结束时 update card)
        table_id: 默认 config.T_DRAFT, 测试时可注入
    """
    import time as _t
    from . import config
    table_id = table_id or config.T_DRAFT
    if success_count == 0 and fail_count > 0:
        status = "失败"
    elif success_count > 0 and fail_count > 0:
        status = "部分成功"
    elif success_count > 0:
        status = "已发送"
    else:
        return  # 没发任何卡片, 不写
    fields = {
        "卡片发送状态": status,
        "卡片发送时间": int(_t.time() * 1000),
    }
    if errors:
        fields["卡片发送错误"] = (" | ".join(errors))[:500]
    if group_msg_id:
        fields["卡片群消息ID"] = group_msg_id
    try:
        await update_record(table_id, draft_rid, fields)
    except Exception as e:
        # 回写本身失败不该影响主流程, 但 print 出来
        print(f"[feishu.mark_card_receipt] rid={draft_rid} fail: {e}")


# ===== 按职务实时查在职员工 (聪哥1号 contact:contact:readonly) =====
# 遵守 feishu-people-as-source-of-truth 铁律: 不硬编码 open_id, 按职务实时查飞书人事
# 缓存 1h: 避免每次发卡片都拉一遍部门列表 (大约 7-10 个部门 + 每部门 1 次 user list)
_job_title_cache = {}  # {title: (timestamp, [(name, open_id), ...])}
_JOB_TITLE_TTL = 3600


async def resolve_notify_targets(role: str) -> list:
    """统一草稿通知 targets 决策 (2026-05-17 A9 抽 helper, 消除 draft_router/sla_check 重复).

    role:
      - "reviewer": 待审草稿主审 → 独立站运营专员 + Frankie CC (含去重)
      - "needs_rewrite": 需人改 → NOTIFY_USERS 全员 (Frankie 必收防质量异常漏看)
      - "ship_main": 寄样确认主审 → 独立站运营专员
      - "ship_cc": 寄样 CC → Frankie + 吴晓丹

    所有 role 在职务查询失败时降级到 NOTIFY_USERS 关键字过滤, 防 contact API 故障漏告警.
    """
    from . import config
    if role == "needs_rewrite":
        return list(config.NOTIFY_USERS)

    if role == "ship_cc":
        return [u for u in config.NOTIFY_USERS
                if u[0].startswith("潘") or "晓丹" in u[0]]

    # reviewer / ship_main 都用职务实时查
    by_title = await fetch_users_by_job_title(config.KOL_REVIEWER_JOB_TITLE)
    if not by_title:
        # 降级: 用 NOTIFY_USERS 关键字过滤 "独立站"
        print(f"[resolve_notify_targets] WARN: job_title={config.KOL_REVIEWER_JOB_TITLE!r} returned empty, fallback")
        by_title = [u for u in config.NOTIFY_USERS if "独立站" in u[0]]

    if role == "ship_main":
        return by_title

    if role == "reviewer":
        # 独立站运营专员 + Frankie CC, 去重
        frankie_cc = [u for u in config.NOTIFY_USERS if u[0].startswith("潘")]
        seen = set()
        merged = []
        for name, oid in by_title + frankie_cc:
            if oid in seen: continue
            seen.add(oid)
            merged.append((name, oid))
        return merged

    raise ValueError(f"unknown role: {role!r}")


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

"""飞书 API 封装 - 双 App token 管理"""
import httpx
import time
from . import config

_tokens = {}  # {"bitable": (token, expiry_ts), "notify": ...}


async def _refresh_token(which: str):
    if which == "bitable":
        aid, sec = config.FEISHU_BITABLE_APP_ID, config.FEISHU_BITABLE_APP_SECRET
    elif which == "app3":
        aid, sec = config.FEISHU_APP3_ID, config.FEISHU_APP3_SECRET
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
    """飞书 API 调用. 5xx 自动重试 2 次 (5s + 10s 指数退避).

    2026-05-21 加 5xx retry: 飞书 Bitable API 偶发 code=2200 Internal Error / 502 / 503,
    实战 5/21 15:30 触发 1 次 endpoint 告警, 下次 cron 15min 后自然恢复.
    加 in-call retry 避免 cron 周期等待, 同时减少 endpoint 告警噪音.

    重试范围: HTTP 500/502/503/504 (服务端瞬态) + 网络层错误 → 重试 2 次 (5s+10s).
    飞书 code 1254607 "Data not ready" (Bitable 异步索引瞬态, 幂等读) → 重试 3 次 (5s+10s+20s),
    比 5xx 多 1 档: 5/29 + 6/4 各撞 1 次, 6/4 那次持续 >15s 撞穿原 2 档触发误报告警 → 单独拉长.
    不重试其他 4xx (auth/permission/业务错误).
    """
    import asyncio
    tok = await token(which)
    url = f"https://open.feishu.cn/open-apis{path}"
    headers = {"Authorization": f"Bearer {tok}", "Content-Type": "application/json; charset=utf-8"}
    # 重试档位: 5xx/网络用前 2 档 (5s+10s); 飞书 1254607 用全部 3 档 (5s+10s+20s ≈35s 窗口).
    # 2026-06-04: 1254607 是 Bitable 异步索引瞬态(幂等读), 偶发持续 >15s 撞穿 2 档 → 误报告警.
    # 单独拉长 1254607 重试不动 5xx/网络逻辑 (后者多为非幂等写, 不宜盲目多试).
    retry_delays = [5, 10, 20]
    max_transient = 2  # 5xx / 网络瞬态的重试次数 (前 2 档)
    last_exc = None
    for attempt in range(len(retry_delays) + 1):  # 0=首次, 1..3=最多 3 次重试
        try:
            async with httpx.AsyncClient(timeout=60.0) as cli:
                r = await cli.request(method, url, json=body, headers=headers)
                if 500 <= r.status_code < 600 and attempt < max_transient:
                    # 5xx 重试
                    print(f"[feishu.api] {method} {path[:80]} → {r.status_code} retry {attempt+1}/{max_transient} in {retry_delays[attempt]}s")
                    await asyncio.sleep(retry_delays[attempt])
                    continue
                # 飞书 1254607 "Data not ready"(数据未就绪, 异步索引瞬态) → 比 5xx 多 1 档重试
                if r.status_code >= 400 and attempt < len(retry_delays):
                    try:
                        _fcode = r.json().get("code")
                    except Exception:
                        _fcode = None
                    if _fcode == 1254607:
                        print(f"[feishu.api] {method} {path[:80]} → 飞书1254607 数据未就绪 retry {attempt+1}/{len(retry_delays)} in {retry_delays[attempt]}s")
                        await asyncio.sleep(retry_delays[attempt])
                        continue
                if r.status_code >= 400:
                    raise Exception(f"{method} {path} → {r.status_code}: {r.text[:300]}")
                return r.json()
        except (httpx.TimeoutException, httpx.NetworkError) as e:
            # 网络层瞬态错误也重试 (同 5xx, 前 2 档)
            last_exc = e
            if attempt < max_transient:
                print(f"[feishu.api] {method} {path[:80]} network err retry {attempt+1}/{max_transient} in {retry_delays[attempt]}s: {e}")
                await asyncio.sleep(retry_delays[attempt])
                continue
            raise
    # 重试 2 次仍 5xx, 最后一次抛
    if last_exc:
        raise last_exc
    raise Exception(f"{method} {path} → exhausted retries")


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


def ext_url(f):
    """URL 字段取真实链接(link 优先), 不取显示文本(text). 2026-05-27 戴夫事故根因:
    ext() 对 URL 字段先取 text → 运营/我把 text 填成中文标签 → 标签被当链接 + UTM 发出去成死链.
    专门读 URL 字段(官网链接等)用此函数, 不能用 ext()."""
    if isinstance(f, dict):
        return f.get("link") or f.get("text") or ""
    if isinstance(f, list) and f and isinstance(f[0], dict):
        return f[0].get("link") or f[0].get("text") or ""
    return ext(f)


def product_url(fields):
    """产品主对外链接(单条, 用于 UTM 字段/ROI 归因): 「官网链接」优先, 缺则降级「亚马逊链接」.
    2026-06-02: 运营改用填亚马逊链接(信息更丰富), 官网链接可空 → 防死链 gate 据此放宽.
    两者都是 URL 字段, ext_url 取的 link 必为完整 URL → 不会死链 (亚马逊即使 text 显示 ASIN, link 仍是 https://www.amazon.com/dp/...)."""
    return ext_url(fields.get("官网链接")) or ext_url(fields.get("亚马逊链接"))


def product_links(fields):
    """产品对外链接列表(多条, 用于邮件正文同时展示): 任务栏填了几个就返回几个.
    2026-06-02 Frankie: 亚马逊 + 独立站两条都填则邮件里都放, 只填一条就放一条.
    Returns ordered [(kind, url_raw), ...]; kind ∈ {'amazon','site'}; 都空则 []."""
    out = []
    amz = ext_url(fields.get("亚马逊链接"))
    site = ext_url(fields.get("官网链接"))
    if amz: out.append(("amazon", amz))
    if site: out.append(("site", site))
    return out


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


# ===== 飞书通知统一格式 (Phase 1, 2026-05-22) =====
# 镜像 ~/scripts/_lib/feishu_title.py + memory reference_feishu_notification_rules.md.
# 云端无该 helper 文件, 故在此内联实现. 改格式规则时两边必须同步.
_LEVEL_EMOJI = {"P0": "🔴", "P1": "🟠", "P2": "🟡", "P3": "🟢"}


def format_title_str(biz: str = "KOL", level: str = "P1", title: str = "", suffix: str = "") -> str:
    """返回统一格式标题字符串 {emoji} [{biz}·{level}] {title} · {suffix}.
    text 消息首行 + card 标题共用, 保证两种 msg_type 格式一致.
    """
    emoji = _LEVEL_EMOJI.get(level, "🟠")
    head = f"{emoji} [{biz}·{level}] {title}".rstrip()
    return f"{head} · {suffix}" if suffix else head


def _format_card_title(card: dict, biz: str = "KOL", level: str = "P1") -> dict:
    """给 card.header.title.content 加统一前缀 {emoji} [{biz}·{level}].
    ⚠️ P0 系统铁律: 标题格式化绝不能影响卡片送达 — 任何异常都 try/except 吞掉, 回退原 card.
    幂等: content 已以优先级 emoji + '[' 开头则跳过 (防重发重复加前缀).
    """
    try:
        header = card.get("header") or {}
        title = header.get("title") or {}
        orig = title.get("content", "") or ""
        if orig[:1] in ("🔴", "🟠", "🟡", "🟢") and "[" in orig[:8]:
            return card  # 已有统一前缀, 幂等跳过
        title["tag"] = "plain_text"
        title["content"] = format_title_str(biz, level, orig)
        header["title"] = title
        card["header"] = header
    except Exception as e:
        print(f"[_format_card_title] skip (non-fatal): {e}")
    return card


async def send_card_message(receive_type: str, receive_id: str, card: dict,
                            biz: str = "KOL", level: str = "P1") -> str:
    """用聪哥分身1号发飞书互动卡片. 2026-05-17 返回 message_id (供 A5 后续 update).
    返回空字符串 = 拿不到 msg_id (不影响主流程)
    2026-05-22 Phase 1: 自动给卡片标题加 {emoji} [{biz}·{level}] 统一前缀.
      - KOL 业务通知 (草稿/SLA/寄样/回复) 默认 biz=KOL level=P1
      - 系统告警 (main endpoint / zoho health) 调用方传 biz=AUDIT
      - 标题格式化失败回退原 card, 不影响送达
    """
    import json
    card = _format_card_title(card, biz, level)
    body = {
        "receive_id": receive_id,
        "msg_type": "interactive",
        "content": json.dumps(card, ensure_ascii=False),
    }
    resp = await api("POST", f"/im/v1/messages?receive_id_type={receive_type}", body, which="notify")
    return (resp.get("data") or {}).get("message_id") or ""


# ===== 聪哥3号 交互卡 (warm_recap 暖信卡: 运营粘 UpPromote 券码) =====
# 卡片必须用聪哥3号(app3)发: card.action.trigger 回调只回到发卡 app, 且 n8n event-hub
# YjTXaoWAcy89xZpT 订阅的是 3 号. open_id 各 App 不互通, 故用 union_id 收件.
_union_cache = {}  # open_id(聪哥1号 namespace) -> union_id(跨 app 稳定)


async def open_id_to_union_id(open_id: str) -> str:
    """聪哥1号 contact API 把 open_id 换成 union_id (跨 app 稳定, 供聪哥3号发卡用).
    1h 进程缓存; 查不到返回空字符串 (调用方应跳过该 target).
    """
    if not open_id:
        return ""
    cached = _union_cache.get(open_id)
    if cached:
        return cached
    try:
        r = await api("GET", f"/contact/v3/users/{open_id}?user_id_type=open_id", which="notify")
        uid = (((r.get("data") or {}).get("user") or {}).get("union_id")) or ""
        if uid:
            _union_cache[open_id] = uid
        return uid
    except Exception as e:
        print(f"[feishu.open_id_to_union_id] {open_id} fail: {e}")
        return ""


async def send_card_via_app3(receive_type: str, receive_id: str, card: dict) -> str:
    """用聪哥3号发交互卡 (form 卡, 回调走 n8n event-hub). receive_type 通常 union_id.
    返回 message_id (失败抛异常, 调用方 catch).
    不套 _format_card_title — warm_recap 卡自带 header, 不需要 KOL 业务前缀.
    """
    import json
    body = {
        "receive_id": receive_id,
        "msg_type": "interactive",
        "content": json.dumps(card, ensure_ascii=False),
    }
    resp = await api("POST", f"/im/v1/messages?receive_id_type={receive_type}", body, which="app3")
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


async def write_card_recipients_msgids(draft_rid: str,
                                        recipients_union_ids: list,
                                        msgids: dict,
                                        table_id: str = None):
    """卡片下发后, 把通知到的运营 + 各自私聊卡 message_id 写回草稿表.
    供"卡片任务看板"按运营人分组(关联运营) + "重发卡片"端点按 union_id 撤老卡(卡片个人消息IDs).

    Args:
        draft_rid: 草稿记录 id
        recipients_union_ids: 真正收到聪哥3号互动卡的运营 union_id 列表, 写「关联运营」User 字段
        msgids: {union_id: msg_id, ...}, merge 进「卡片个人消息IDs」JSON 文本字段
        table_id: 默认 config.T_DRAFT

    幂等: msgids merge 不清老的; 「关联运营」每次发卡覆盖(当时快照).
    fail-safe: 任何子步骤失败 print 不 raise, 不影响主流程.
    """
    import json as _json
    from . import config
    table_id = table_id or config.T_DRAFT
    if not draft_rid:
        return
    fields = {}
    if recipients_union_ids:
        fields["关联运营"] = [{"id": uid} for uid in recipients_union_ids if uid]
    if msgids:
        try:
            rec = await get_record(table_id, draft_rid)
            cur = ext(rec.get("fields", {}).get("卡片个人消息IDs")) or ""
            try:
                mp = _json.loads(cur) if cur else {}
                if not isinstance(mp, dict):
                    mp = {}
            except Exception:
                mp = {}
            for k, v in msgids.items():
                if k and v:
                    mp[k] = v
            fields["卡片个人消息IDs"] = _json.dumps(mp, ensure_ascii=False)
        except Exception as e:
            print(f"[feishu.write_card_recipients_msgids] read fail rid={draft_rid}: {e}")
            try:
                fields["卡片个人消息IDs"] = _json.dumps(
                    {k: v for k, v in msgids.items() if k and v}, ensure_ascii=False)
            except Exception:
                pass
    if not fields:
        return
    try:
        # user_id_type=union_id 让飞书按 union_id 解析 User 字段(跨 app namespace 通用)
        path = (f"/bitable/v1/apps/{config.FEISHU_APP_TOKEN}/tables/{table_id}"
                f"/records/{draft_rid}?user_id_type=union_id")
        await api("PUT", path, {"fields": fields})
    except Exception as e:
        print(f"[feishu.write_card_recipients_msgids] put fail rid={draft_rid}: {e}")


# ===== KOL/媒体人通用信息块 (2026-05-31 统一标准: 所有操作类卡顶部都用此 block) =====
# 字段标准: KOL名/媒体人姓名 + 阶段(_contact_stage_label) + 平台 + 粉丝 + 产品 + 品牌 + 收件邮箱
# 各卡按用途取舍: 操作类全字段 / SLA/退信告警类 compact=True 省产品/品牌/邮箱

async def resolve_contact_info(contact_rid: str, contact_type: str = "KOL") -> dict:
    """拉 KOL/媒体人主表 → {name, stage, platform, fans}.
    contact_type: "KOL" 或 "媒体人". fail-safe: 失败返 {}(调用方显示 '?')."""
    if not contact_rid:
        return {}
    from . import config, reply_monitor  # 惰性 import 防循环
    try:
        t = config.T_EDITOR if contact_type == "媒体人" else config.T_KOL
        cf = (await get_record(t, contact_rid)).get("fields", {})
        out = {"stage": reply_monitor._contact_stage_label(cf) or ""}
        if contact_type == "媒体人":
            out["name"] = ext(cf.get("媒体人姓名")) or "?"
            out["platform"] = ext(cf.get("主要媒体")) or ext(cf.get("所属媒体")) or ""
            out["fans"] = ""
        else:
            out["name"] = ext(cf.get("账号名")) or "?"
            out["platform"] = ext(cf.get("主平台")) or ""
            try:
                out["fans"] = f"{int(cf.get('粉丝数') or 0):,}"
            except (ValueError, TypeError):
                out["fans"] = str(cf.get("粉丝数") or "")
        return out
    except Exception as e:
        print(f"[resolve_contact_info] {contact_rid} fail: {e}")
        return {}


def build_contact_info_block(contact_info: dict = None,
                              product_name: str = "",
                              brand: str = "",
                              email: str = "",
                              contact_type: str = "KOL",
                              include_email: bool = True,
                              compact: bool = False) -> dict:
    """统一 KOL/媒体人信息字段块 (返 div.fields element).
    contact_info: {name, stage, platform, fans} — 由 resolve_contact_info 解析.
    compact=True: 仅 4 字段(name/stage/platform/fans) — SLA 提醒/退信告警用.
    include_email=False: 不显示收件邮箱(SLA 用).
    调用方插到卡 elements 头部."""
    ci = contact_info or {}
    who = "媒体人" if contact_type == "媒体人" else "KOL"
    fields = [
        {"is_short": True, "text": {"tag": "lark_md", "content": f"**{who}**: {ci.get('name') or '?'}"}},
        {"is_short": True, "text": {"tag": "lark_md", "content": f"**阶段**: {ci.get('stage') or '?'}"}},
        {"is_short": True, "text": {"tag": "lark_md", "content": f"**平台**: {ci.get('platform') or '?'}"}},
        {"is_short": True, "text": {"tag": "lark_md", "content": f"**粉丝**: {ci.get('fans') or '—'}"}},
    ]
    if not compact:
        if product_name:
            fields.append({"is_short": True, "text": {"tag": "lark_md", "content": f"**产品**: {product_name}"}})
        if brand:
            fields.append({"is_short": True, "text": {"tag": "lark_md", "content": f"**品牌**: {brand}"}})
        if include_email and email:
            fields.append({"is_short": True, "text": {"tag": "lark_md", "content": f"**收件**: {email}"}})
    return {"tag": "div", "fields": fields}


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

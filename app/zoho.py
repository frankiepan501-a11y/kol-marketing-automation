"""Zoho Mail API - 双品牌 token 管理 + V2 Draft 沙盒验证

V2 (2026-05-08): send_email 路径加 3 层防御 — 防 ext() 类 bug 再发空白邮件
  layer-1: plain body 短于 50 字符 → 直接拒发 (不调 Zoho)
  layer-2: 创建 Zoho draft → 拉 raw → 校验 body 长度/占位符/主题 → 通过才删 draft 真发
  layer-3: 真发后 30s 后台抽检 sent folder raw → 长度异常发飞书告警

调用方零改动: send_email 签名不变, 验证失败 raise DraftValidationError, auto_send.py 现有
except 已能处理为"发送失败" (写 发送状态=失败 + 发送错误 + 邮件草稿状态=发送失败)。
"""
import httpx, time, urllib.parse, re, asyncio, os
from email import message_from_string
from email.header import decode_header
from . import config

_access = {}              # brand → (token, expiry_ts)
_folder_ids_cache = {}    # brand → {"drafts": id, "sent": id, "fetched_at": ts}
_FOLDER_CACHE_TTL = 3600  # 1 小时
_pending_verify_tasks = set()  # 防 fire-and-forget asyncio task 被 GC


class DraftValidationError(Exception):
    """V2 layer-2 校验失败 — body 截断 / 占位符未替换 / 主题缺失"""
    pass


# ===== OAuth token =====
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
        _access[brand] = (d["access_token"], time.time() + 3300)  # 55 min
        return d["access_token"]


async def access(brand: str):
    cached = _access.get(brand)
    if cached and cached[1] > time.time():
        return cached[0]
    return await refresh_access(brand)


# ===== HTML 工具 =====
def _ensure_html(body: str) -> str:
    """如果 body 是纯文本 (没有 <p>/<br>/<div> 等 HTML 标签), 自动转 HTML:
    - **xxx** → <strong>xxx</strong> (markdown bold)
    - 段落用 <p>...</p> 包裹
    - 单换行 \\n → <br>
    """
    if not body:
        return ""
    if re.search(r"<(p|div|br|h[1-6]|li|strong|em|a)[\s>/]", body, re.I):
        return body
    s = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", body)
    paragraphs = [p.strip() for p in s.split("\n\n") if p.strip()]
    return "".join(f"<p>{p.replace(chr(10), '<br>')}</p>" for p in paragraphs)


def _strip_html(s: str) -> str:
    """HTML → 纯文本(留空格), 用于长度对比"""
    if not s:
        return ""
    return re.sub(r"<[^>]+>", "", s).replace("&nbsp;", " ").strip()


# ===== Folder ID 缓存 (Drafts / Sent) =====
async def list_accounts(brand: str) -> list:
    """诊断(只读): 列该 brand token 可访问的全部 Zoho 账号 + 每个的 sendMailDetails(合法发件地址),
    用于排查发送 500(fromAddress 非合法 send-as 身份 / account_id 不匹配)。"""
    tok = await access(brand)
    async with httpx.AsyncClient(timeout=30.0) as cli:
        r = await cli.get("https://mail.zoho.com/api/accounts",
                          headers={"Authorization": f"Zoho-oauthtoken {tok}"})
        r.raise_for_status()
        return r.json().get("data") or []


async def raw_send_probe(brand: str, to_addr: str) -> dict:
    """诊断(只发1封): 原始发送 POST 不 raise, 返回 Zoho 真实 status+body(看 500 具体原因)。
    payload 与 _send_now 完全一致, 忠实复现失败。"""
    cfg = config.BRAND_CONFIG[brand]
    tok = await access(brand)
    async with httpx.AsyncClient(timeout=30.0) as cli:
        r = await cli.post(
            f"https://mail.zoho.com/api/accounts/{cfg['account_id']}/messages",
            json={"fromAddress": cfg["alias_from"], "toAddress": to_addr,
                  "subject": "[probe] send debug", "content": "<p>probe</p>", "mailFormat": "html"},
            headers={"Authorization": f"Zoho-oauthtoken {tok}"},
        )
        return {"status": r.status_code, "body": r.text[:900]}


async def _list_folders_raw(brand: str) -> list:
    cfg = config.BRAND_CONFIG[brand]
    tok = await access(brand)
    async with httpx.AsyncClient(timeout=30.0) as cli:
        r = await cli.get(
            f"https://mail.zoho.com/api/accounts/{cfg['account_id']}/folders",
            headers={"Authorization": f"Zoho-oauthtoken {tok}"},
        )
        r.raise_for_status()
        return r.json().get("data") or []


async def _get_folder_ids(brand: str) -> tuple:
    """返 (drafts_folder_id, sent_folder_id), 缓存 1 小时"""
    cached = _folder_ids_cache.get(brand)
    if cached and (time.time() - cached["fetched_at"] < _FOLDER_CACHE_TTL):
        return cached["drafts"], cached["sent"]

    folders = await _list_folders_raw(brand)
    drafts_id = None
    sent_id = None
    for f in folders:
        ftype = (f.get("folderType") or "").lower()
        fname = (f.get("folderName") or "").lower()
        if ftype == "drafts" or fname in ("drafts", "草稿", "草稿箱"):
            drafts_id = f.get("folderId")
        elif ftype == "sent" or fname in ("sent", "sent items", "已发送"):
            sent_id = f.get("folderId")

    if not drafts_id or not sent_id:
        raise Exception(
            f"Zoho folders missing: drafts={drafts_id} sent={sent_id} "
            f"(found: {[f.get('folderName') for f in folders]})"
        )
    _folder_ids_cache[brand] = {
        "drafts": drafts_id, "sent": sent_id, "fetched_at": time.time(),
    }
    return drafts_id, sent_id


# ===== V2 Draft 沙盒 =====
async def create_draft(brand: str, to_addr: str, subject: str, html_body: str) -> str:
    """POST /messages with mode=draft → 返 draft messageId.

    Zoho 收到 mode=draft 时会跑完整渲染管线 (HTML 转换/MIME 编码), 拉 raw 看到的
    就是真发送时会渲染的内容 — 这正是检测 ext() bug 截断的关键信号。
    """
    cfg = config.BRAND_CONFIG[brand]
    tok = await access(brand)
    async with httpx.AsyncClient(timeout=45.0) as cli:
        r = await cli.post(
            f"https://mail.zoho.com/api/accounts/{cfg['account_id']}/messages",
            json={
                "mode": "draft",
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
            raise Exception(f"Create draft fail: {d}")
        mid = d.get("data", {}).get("messageId")
        if not mid:
            raise Exception(f"Create draft no messageId: {d}")
        return mid


async def get_draft_body(brand: str, drafts_fid: str, draft_id: str) -> str:
    """拿 draft body raw HTML — Zoho 渲染管线已跑完的真实内容"""
    cfg = config.BRAND_CONFIG[brand]
    tok = await access(brand)
    async with httpx.AsyncClient(timeout=30.0) as cli:
        r = await cli.get(
            f"https://mail.zoho.com/api/accounts/{cfg['account_id']}"
            f"/folders/{drafts_fid}/messages/{draft_id}/content",
            headers={"Authorization": f"Zoho-oauthtoken {tok}"},
        )
        if r.status_code != 200:
            raise Exception(f"Get draft body fail: {r.status_code} {r.text[:200]}")
        return r.json().get("data", {}).get("content", "") or ""


async def get_draft_subject(brand: str, drafts_fid: str, draft_id: str) -> str:
    """从 RFC822 header 解析 Subject — 验证主题没被渲染管线吃掉"""
    cfg = config.BRAND_CONFIG[brand]
    tok = await access(brand)
    async with httpx.AsyncClient(timeout=30.0) as cli:
        r = await cli.get(
            f"https://mail.zoho.com/api/accounts/{cfg['account_id']}"
            f"/folders/{drafts_fid}/messages/{draft_id}/header",
            headers={"Authorization": f"Zoho-oauthtoken {tok}"},
        )
        if r.status_code != 200:
            raise Exception(f"Get draft header fail: {r.status_code} {r.text[:200]}")
        raw_header = r.json().get("data", {}).get("headerContent", "") or ""
        # 解析 RFC822 — Subject 可能 RFC2047 编码 =?UTF-8?Q?...?=
        msg = message_from_string(raw_header)
        s = msg.get("Subject", "") or ""
        # decode_header 处理多 part 编码
        decoded = ""
        for part, enc in decode_header(s):
            if isinstance(part, bytes):
                try:
                    decoded += part.decode(enc or "utf-8", errors="replace")
                except Exception:
                    decoded += part.decode("utf-8", errors="replace")
            else:
                decoded += part
        return decoded.strip()


async def delete_draft(brand: str, drafts_fid: str, draft_id: str) -> None:
    """删 draft — 验证失败/通过都要清理, 不污染收件人 Drafts box"""
    cfg = config.BRAND_CONFIG[brand]
    tok = await access(brand)
    async with httpx.AsyncClient(timeout=30.0) as cli:
        try:
            await cli.delete(
                f"https://mail.zoho.com/api/accounts/{cfg['account_id']}"
                f"/folders/{drafts_fid}/messages/{draft_id}",
                headers={"Authorization": f"Zoho-oauthtoken {tok}"},
            )
        except Exception as e:
            # 删 draft 失败不影响主流程, 只 log
            print(f"[zoho.delete_draft WARN] brand={brand} draft={draft_id} err={e}")


# ===== V2 layer-2 校验 =====
# 占位符黑名单与 auto_send.py PLACEHOLDER_KEYWORDS 同步, 但在 zoho 层做二次兜底
# (auto_send 已做一次, 这里防 ext() bug 让占位符在 raw body 里"复活")
_PLACEHOLDER_BLACKLIST = [
    "[TRACKING#", "[CARRIER", "[TBD", "[ETA",
    "[ADDRESS", "[PRICE", "[QUANTITY", "[xxx", "[XXX", "待填",
]


def _validate_draft(raw_body: str, raw_subject: str,
                    expected_html: str, expected_subject: str) -> None:
    """3 项校验, 任一 fail 抛 DraftValidationError 含原因"""
    raw_text = _strip_html(raw_body)
    expected_text = _strip_html(expected_html)
    expected_text_len = len(expected_text)
    raw_text_len = len(raw_text)

    # ① body 长度 — 防 ext() bug 截断到几字符
    # 阈值 0.7: 允许 Zoho 渲染加点 banner/quote 减点空白, 但不能截断 30%+
    if expected_text_len >= 50 and raw_text_len < expected_text_len * 0.7:
        raise DraftValidationError(
            f"body 截断: raw={raw_text_len} chars, expected≈{expected_text_len} "
            f"(ratio={raw_text_len/max(expected_text_len,1):.2f}); "
            f"raw_text_head='{raw_text[:80]}'"
        )
    # 极端短: expected 本身就 < 50 字符 (接近 ext() bug 症状)
    if expected_text_len < 50:
        raise DraftValidationError(
            f"body 过短: expected={expected_text_len} chars (< 50 阈值, 疑似 ext() multi-segment bug); "
            f"head='{expected_text[:80]}'"
        )

    # ② 占位符 — 模板里的 [TRACKING#/[CARRIER/待填 等不应出现在 raw body
    for kw in _PLACEHOLDER_BLACKLIST:
        if kw in raw_text:
            raise DraftValidationError(
                f"占位符未替换: 命中 '{kw}' in raw body; head='{raw_text[:120]}'"
            )

    # ③ 主题 — 防主题被渲染管线吃掉 / 模板占位符没替换
    raw_sub_clean = (raw_subject or "").strip()
    if not raw_sub_clean:
        raise DraftValidationError(
            f"主题缺失: raw subject 为空 (expected='{expected_subject}')"
        )
    if len(raw_sub_clean) < 5:
        raise DraftValidationError(
            f"主题过短: raw='{raw_sub_clean}' < 5 chars (expected='{expected_subject}')"
        )
    for kw in _PLACEHOLDER_BLACKLIST:
        if kw in raw_sub_clean:
            raise DraftValidationError(
                f"主题含占位符: 命中 '{kw}' in subject='{raw_sub_clean}'"
            )
    # 主题不允许实质性截断 (允许 Zoho 加 RE:/FWD: 等前缀, 但不能比传入短 > 30%)
    expected_sub_len = len((expected_subject or "").strip())
    if expected_sub_len >= 5 and len(raw_sub_clean) < expected_sub_len * 0.7:
        raise DraftValidationError(
            f"主题截断: raw='{raw_sub_clean}' ({len(raw_sub_clean)} chars) "
            f"vs expected='{expected_subject}' ({expected_sub_len} chars)"
        )


# ===== V2 layer-3: 30s 后 sent folder 抽检 =====
async def verify_sent_after(brand: str, msg_id: str, sent_fid: str,
                             expected_text_len: int, delay: int = 30):
    """后台 task: sleep N 秒 → 拉 sent folder raw → 长度对比 → 异常发飞书告警.

    失败不阻塞主流程, 只飞书告警. 双重保险防 draft-vs-send 渲染差异漏网。
    """
    try:
        await asyncio.sleep(delay)
        cfg = config.BRAND_CONFIG[brand]
        tok = await access(brand)
        async with httpx.AsyncClient(timeout=30.0) as cli:
            r = await cli.get(
                f"https://mail.zoho.com/api/accounts/{cfg['account_id']}"
                f"/folders/{sent_fid}/messages/{msg_id}/content",
                headers={"Authorization": f"Zoho-oauthtoken {tok}"},
            )
            if r.status_code != 200:
                # 30s 后 sent folder 还查不到很常见 (Zoho 索引延迟), 不告警
                print(f"[zoho.verify_sent WARN] brand={brand} msg={msg_id} "
                      f"sent folder lookup {r.status_code} (索引延迟, 跳过)")
                return
            sent_body = r.json().get("data", {}).get("content", "") or ""
            sent_text_len = len(_strip_html(sent_body))
            if expected_text_len >= 50 and sent_text_len < expected_text_len * 0.7:
                # 真异常 — draft 通过但 sent 截断 → 飞书告警
                ratio = sent_text_len / max(expected_text_len, 1)
                msg = (
                    f"**⚠️ Zoho sent 抽检异常**\n"
                    f"brand=`{brand}` msg_id=`{msg_id}`\n"
                    f"sent body={sent_text_len} chars, expected≈{expected_text_len} "
                    f"(ratio={ratio:.2f})\n"
                    f"draft 验证通过但 sent 截断 — 可能渲染管线分歧, 立即查看"
                )
                print(f"[zoho.verify_sent ALERT] {msg}")
                card = {
                    "header": {
                        "title": {"tag": "plain_text", "content": "Zoho sent 抽检异常"},
                        "template": "red",
                    },
                    "elements": [{"tag": "div", "text": {"tag": "lark_md", "content": msg}}],
                }
                try:
                    from . import feishu
                    for _, oid in config.NOTIFY_USERS:
                        try:
                            await feishu.send_card_message("open_id", oid, card, biz="AUDIT")
                        except Exception as e:
                            print(f"[zoho.verify_sent feishu alert fail oid={oid}] {e}")
                except Exception as e:
                    print(f"[zoho.verify_sent feishu import fail] {e}")
            else:
                print(f"[zoho.verify_sent OK] brand={brand} msg={msg_id} "
                      f"sent={sent_text_len} expected={expected_text_len}")
    except Exception as e:
        print(f"[zoho.verify_sent ERROR] brand={brand} msg={msg_id} err={e}")


# ===== 真发 (内部, 走现有 messages POST) =====
async def _send_now(brand: str, to_addr: str, subject: str, html_body: str) -> str:
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


# ===== 线程化回复 (action:reply) =====
# POST /messages/{被回复邮件 messageId} + action:"reply" → Zoho 自动加 In-Reply-To/References
# header, 落进同一 thread. 实测 (C:/tmp/zoho_reply_thread_test.py): M2 threadId==M1. 解决 KOL
# 换主题脱离原 thread 的碎片化 + 上下文丢失 (重复开发信帮凶). 只在草稿带「回复目标MsgID」时走此路径,
# cold 首封 / followup (无入站 msgId) 仍走 _send_now 新邮件.
async def _send_reply(brand: str, orig_msg_id: str, to_addr: str,
                      subject: str, html_body: str) -> str:
    cfg = config.BRAND_CONFIG[brand]
    tok = await access(brand)
    async with httpx.AsyncClient(timeout=45.0) as cli:
        r = await cli.post(
            f"https://mail.zoho.com/api/accounts/{cfg['account_id']}/messages/{orig_msg_id}",
            json={
                "fromAddress": cfg["alias_from"],
                "toAddress": to_addr,
                "action": "reply",
                "subject": subject,
                "content": html_body,
                "mailFormat": "html",
            },
            headers={"Authorization": f"Zoho-oauthtoken {tok}"},
        )
        r.raise_for_status()
        d = r.json()
        if d.get("status", {}).get("code") != 200:
            raise Exception(f"Send reply fail: {d}")
        return d["data"].get("messageId")


# ===== 主入口 — 签名向后兼容 (新增可选 reply_to_msg_id, 默认 None = 行为不变) =====
async def send_email(brand: str, to_addr: str, subject: str, body: str,
                     reply_to_msg_id: str = None):
    """发送邮件 — V2: layer-1 短 body 拒发 + layer-2 draft 沙盒验证 + layer-3 30s sent 抽检.

    DRY-RUN: 如果 env `EMAIL_DRY_RUN_TO` 有值, 自动把 to 改成此邮箱,
    主题前加 [DRY-RUN→{真实 to}], 防止改代码时误发到真客户.

    校验失败抛 DraftValidationError, auto_send 现有 except 已能处理为"发送失败".
    """
    html_body = _ensure_html(body)

    real_to = to_addr
    dry_run_to = os.environ.get("EMAIL_DRY_RUN_TO", "").strip()
    if dry_run_to:
        to_addr = dry_run_to
        subject = f"[DRY-RUN→{real_to}] {subject}"
        # dry-run 改 to 后 action:reply 串入真实 KOL thread 语义错乱 (memory 已识别隐患④),
        # 且测试目的是验内容渲染非线程. 强制降级新邮件, 保证 dry-run 纯净.
        reply_to_msg_id = None
        html_body = (
            f"<div style=\"background:#fff3cd;padding:8px;border:1px solid #ffc107;margin-bottom:12px\">"
            f"<strong>⚠️ DRY-RUN MODE</strong> — 这封邮件本来要发给 <code>{real_to}</code>, "
            f"但 EMAIL_DRY_RUN_TO env 已设置为 <code>{dry_run_to}</code>, 真客户不会收到。</div>"
            + html_body
        )
        print(f"[zoho.send_email DRY-RUN] {real_to} → {to_addr}")

    # === Layer-1: 不调 Zoho 直接拒发, 防 ext() bug 浪费 API quota ===
    plain_len = len(_strip_html(html_body))
    if plain_len < 50:
        raise DraftValidationError(
            f"layer-1 短 body 拒发: plain={plain_len} chars < 50 (疑似 ext() bug); "
            f"head='{_strip_html(html_body)[:80]}'"
        )

    # === Layer-2: Draft 沙盒验证 ===
    drafts_fid, sent_fid = await _get_folder_ids(brand)
    draft_id = await create_draft(brand, to_addr, subject, html_body)
    try:
        raw_body = await get_draft_body(brand, drafts_fid, draft_id)
        raw_subject = await get_draft_subject(brand, drafts_fid, draft_id)
        _validate_draft(raw_body, raw_subject, html_body, subject)
    except Exception:
        # 验证失败/任何拉取异常 — 必须删 draft 不污染收件箱
        await delete_draft(brand, drafts_fid, draft_id)
        raise
    # 验证通过 — 删 draft 后真发
    await delete_draft(brand, drafts_fid, draft_id)

    # 带 reply_to_msg_id → 走 action:reply 串入原 thread; 否则新邮件. 沙盒验证/dry-run 已对二者一致.
    if reply_to_msg_id:
        try:
            msg_id = await _send_reply(brand, reply_to_msg_id, to_addr, subject, html_body)
        except Exception as e:
            # reply 端点失败 (orig msgId 失效/跨账户等) → 降级新邮件, 保证邮件仍发出 (线程化是增强非必需)
            print(f"[zoho.send_email] _send_reply fail, 降级 _send_now: orig={reply_to_msg_id} err={str(e)[:160]}")
            msg_id = await _send_now(brand, to_addr, subject, html_body)
    else:
        msg_id = await _send_now(brand, to_addr, subject, html_body)

    # === Layer-3: 30s 后台抽检 sent folder (非阻塞) ===
    expected_text_len = len(_strip_html(html_body))
    task = asyncio.create_task(
        verify_sent_after(brand, msg_id, sent_fid, expected_text_len, delay=30)
    )
    _pending_verify_tasks.add(task)
    task.add_done_callback(_pending_verify_tasks.discard)

    return msg_id


# ===== 兼容: 既有调用 (search_inbox / list_folders / list_sent_messages / get_message_content / test_send) =====
async def search_inbox(brand: str, search_key: str, limit: int = 30):
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


# 2026-06-01 方案B: 扫整个账户收件箱(所有非 sent/draft/spam/trash 文件夹), 不按 to:别名过滤.
# 根治多内部外联别名(partner/marketing/frankie/sibyl.guo/goya.li...)的回复被漏接 —
# reply_monitor 由 find_contact 池门控过滤非 KOL 邮件, 任意我方地址收到的 KOL 回复都能捕获.
_SKIP_FOLDERS = {"sent", "sent items", "已发送", "drafts", "outbox", "templates", "spam", "trash", "草稿", "垃圾"}


async def list_inbox(brand: str, per_folder: int = 60):
    """返回账户收件类文件夹最近 per_folder 封消息 (含 fromAddress/toAddress/messageId/subject/folderId/summary/receivedTime)."""
    cfg = config.BRAND_CONFIG[brand]
    tok = await access(brand)
    aid = cfg["account_id"]
    out = []
    async with httpx.AsyncClient(timeout=45.0) as cli:
        fr = await cli.get(f"https://mail.zoho.com/api/accounts/{aid}/folders",
                           headers={"Authorization": f"Zoho-oauthtoken {tok}"})
        fr.raise_for_status()
        for f in (fr.json().get("data") or []):
            fname = (f.get("folderName") or "").lower()
            ftype = (f.get("folderType") or "").lower()
            if ftype == "sent" or fname in _SKIP_FOLDERS:
                continue
            try:
                r = await cli.get(
                    f"https://mail.zoho.com/api/accounts/{aid}/messages/view?folderId={f['folderId']}&limit={per_folder}&start=1",
                    headers={"Authorization": f"Zoho-oauthtoken {tok}"})
                r.raise_for_status()
                out += (r.json().get("data") or [])
            except Exception:
                continue
    return out


async def test_send(brand: str, to_addr: str, subject: str = "[Test] Zoho OAuth check",
                     body: str = "<p>Test email — please ignore. This message exists only to verify Zoho OAuth + send pipeline. Padding to satisfy V2 layer-1 length floor.</p>"):
    return await send_email(brand, to_addr, subject, body)


async def list_folders(brand: str):
    return await _list_folders_raw(brand)


async def list_sent_messages(brand: str, limit: int = 30):
    folders = await _list_folders_raw(brand)
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

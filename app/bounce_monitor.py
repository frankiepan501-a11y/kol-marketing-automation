"""退信处理器 (v4 email_bounced 独立 pipeline, 不走分类器).

扫 partner@ 收件箱里 mailer-daemon 的**硬退信** → 解析死地址 → 精确匹配 KOL/编辑联系人
→ 标「邮箱验真状态=无效」+ 飞书通知 + 跟进记录. 下游 auto_send / secondary_outreach
honor「无效」停发, 不再浪费发送到证实不存在的地址 (90 天 53 封退信, 47 硬退多为编辑猜测地址).

FP 防护 (误把好邮箱标死 = 永久停发真 KOL, 必须严):
  1. 只处理 **硬退** (permanent / address not found / user unknown / 550…), 软退(临时/满箱/超额)跳过不动.
  2. 死地址必须**精确等于**某联系人的「邮箱」(find_contact 精确匹配), 防误attribution.
  3. 「邮箱验真状态」是可逆单选, 运营可改回; 每次新标都飞书通知留人.
  4. 幂等: 联系人已=无效 则跳过(不重复标/不重复通知), 天然去重无需记 messageId.
"""
import re
from . import config, feishu, zoho
from .feishu import ext
from .reply_monitor import find_contact, parse_email

# 硬退信号 (permanent failure) — 命中任一即视为硬退
HARD_MARKERS = [
    "permanent error", "permanent failure", "address not found",
    "couldn't be found", "could not be found", "couldn t be found",
    "does not exist", "doesn't exist", "no such user", "no such address",
    "user unknown", "unknown user", "recipient address rejected",
    "mailbox unavailable", "no mailbox", "account that you tried to reach does not exist",
    "550 ", "551 ", "553 ", "5.1.1", "5.1.10", "5.0.0",
    "address couldn", "recipient not found",
]
# 软退信号 (临时, 不能标无效) — 仅当无任何硬信号时用于排除
SOFT_MARKERS = [
    "temporarily", "temporary failure", "try again", "mailbox full",
    "over quota", "quota exceeded", "deferred", "delayed", "4.2.2", "4.7.",
    "greylist", "rate limited", "too many",
]
# 退信主题/发件人特征
BOUNCE_SUBJECTS = [
    "undelivered", "delivery status notification", "delivery failure",
    "returned to sender", "mail delivery", "failure notice", "delivery has failed",
    "undeliverable", "mail delivery failed",
]


def is_bounce_msg(from_addr: str, subject: str) -> bool:
    fa = (from_addr or "").lower()
    sj = (subject or "").lower()
    if "mailer-daemon" in fa or "postmaster" in fa:
        return True
    return any(k in sj for k in BOUNCE_SUBJECTS)


def is_hard_bounce(subject: str, body: str) -> tuple:
    """(是否硬退, 命中片段). 硬信号优先于软信号; 无硬信号则不算硬退(保守)."""
    text = ((subject or "") + "\n" + (body or "")).lower()
    hit = next((m for m in HARD_MARKERS if m in text), None)
    if hit:
        return True, hit
    return False, ""


def extract_dead_addresses(body: str, exclude: set) -> list:
    """从退信正文提取候选死地址. 先试定向模式, 再全量兜底; 去掉我方/系统地址."""
    text = body or ""
    cands = []
    # 定向模式 (Google / 通用)
    for pat in [
        r"deliver(?:ed)?\s+to[:\s]+<?([\w.+\-]+@[\w.\-]+)>?",
        r"to\s+<([\w.+\-]+@[\w.\-]+)>",
        r"final-recipient:\s*rfc822;\s*([\w.+\-]+@[\w.\-]+)",
        r"recipient address rejected[:\s]+([\w.+\-]+@[\w.\-]+)",
        r"<([\w.+\-]+@[\w.\-]+)>:?\s*(?:host|550|user|recipient)",
    ]:
        for m in re.finditer(pat, text, re.IGNORECASE):
            cands.append(m.group(1).lower())
    # 全量兜底
    for m in re.finditer(r"[\w.+\-]+@[\w.\-]+\.[\w.\-]+", text):
        cands.append(m.group(0).lower())
    # 去重保序 + 排除我方/系统
    seen, out = set(), []
    for e in cands:
        e = e.strip(".,;:<>()[]\"' ").lower()
        if not e or e in seen:
            continue
        seen.add(e)
        if any(x in e for x in exclude) or "mailer-daemon" in e or "postmaster" in e \
           or e.endswith("zoho.com") or e.endswith("googlemail.com") or e.endswith("google.com"):
            continue
        out.append(e)
    return out


def _build_card(contact_type: str, name: str, dead_email: str, brand: str, reason: str) -> dict:
    base_url = f"https://u1wpma3xuhr.feishu.cn/base/{config.FEISHU_APP_TOKEN}"
    t = config.T_EDITOR if contact_type == "editor" else config.T_KOL
    return {
        "header": {"template": "red",
                   "title": {"tag": "plain_text",
                             "content": f"📭 退信 — {'媒体人' if contact_type=='editor' else 'KOL'} 邮箱已标无效"}},
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md",
                "content": (f"**{name}** 的邮箱 `{dead_email}` 硬退信, 已标「邮箱验真状态=无效」并停止再发。\n"
                            f"**品牌**: {brand}　|　**退信类型**: 硬退 (永久)\n"
                            f"**命中**: {reason}")}},
            {"tag": "div", "text": {"tag": "lark_md",
                "content": "⚠️ 多为猜测的地址 (编辑 `{fi}{last}@domain`)。如确认是误判, 在主表把「邮箱验真状态」改回即可恢复发送。"}},
            {"tag": "action", "actions": [
                {"tag": "button", "text": {"tag": "plain_text", "content": "打开主表核对"},
                 "url": f"{base_url}?table={t}", "type": "primary"}]},
        ],
    }


async def run(dry_run: bool = False) -> dict:
    """dry_run=True: 只报"会标哪些联系人无效", 不真写/不通知 (首跑核对匹配正确性, 防 FP)."""
    import time
    processed = 0
    marked = 0
    results = []
    for brand in ("POWKONG", "FUNLAB"):
        alias = config.BRAND_CONFIG[brand]["alias_from"]
        exclude = {alias.lower(), "powkong.com", "fireflyfunlab.com", "funlab"}
        try:
            msgs = await zoho.search_inbox(brand, f"to:{alias}", limit=50)
        except Exception as e:
            results.append({"brand": brand, "error": str(e)[:200]})
            continue
        for msg in msgs:
            from_addr = (msg.get("fromAddress") or msg.get("sender") or "")
            subject = msg.get("subject", "")
            if not is_bounce_msg(from_addr, subject):
                continue
            processed += 1
            msg_id = msg.get("messageId") or msg.get("summary")
            folder_id = msg.get("folderId")
            body = ""
            if folder_id and msg_id:
                try:
                    body = await zoho.get_message_content(brand, msg_id, folder_id)
                except Exception:
                    pass
            full = (msg.get("summary", "") or "") + "\n" + (body or "")
            hard, hit = is_hard_bounce(subject, full)
            if not hard:
                results.append({"brand": brand, "subject": subject[:50], "skip": "soft/unknown bounce"})
                continue
            dead_addrs = extract_dead_addresses(full, exclude)
            matched_any = False
            for dead in dead_addrs:
                contact, ctype = await find_contact(dead)
                if not contact:
                    continue
                matched_any = True
                cf = contact["fields"]
                cur = ext(cf.get("邮箱验真状态"))
                if cur == "无效":
                    results.append({"brand": brand, "dead": dead, "skip": "already 无效 (dedup)"})
                    break
                target_table = config.T_EDITOR if ctype == "editor" else config.T_KOL
                name = ext(cf.get("媒体人姓名")) if ctype == "editor" else ext(cf.get("账号名"))
                if dry_run:
                    marked += 1
                    results.append({"brand": brand, "dead": dead, "contact": name, "type": ctype,
                                    "would_mark": "无效", "cur_status": cur or "(空)", "hit": hit})
                    break
                try:
                    await feishu.update_record(target_table, contact["record_id"], {"邮箱验真状态": "无效"})
                except Exception as e:
                    results.append({"brand": brand, "dead": dead, "error": f"mark fail: {str(e)[:80]}"})
                    break
                # 跟进记录留痕
                fu_table = config.T_EDITOR_FU if ctype == "editor" else config.T_KOL_FU
                link_field = "关联媒体人" if ctype == "editor" else "关联KOL"
                try:
                    await feishu.create_record(fu_table, {
                        "跟进摘要": f"[退信] {dead} 硬退, 标无效停发",
                        "跟进日期": int(time.time() * 1000),
                        "跟进方式": "邮件",
                        "跟进内容": f"mailer-daemon 硬退信 (品牌 {brand})\n命中: {hit}\n主题: {subject[:120]}",
                        link_field: [contact["record_id"]],
                    })
                except Exception as e:
                    print(f"[bounce_monitor] 跟进记录 fail: {e}")
                # 飞书通知 (群)
                try:
                    card = _build_card(ctype, name, dead, brand, hit)
                    await feishu.send_card_message("chat_id", config.NOTIFY_CHAT_ID, card)
                except Exception as e:
                    print(f"[bounce_monitor] notify fail: {e}")
                marked += 1
                results.append({"brand": brand, "dead": dead, "contact": name, "type": ctype, "marked": "无效"})
                break  # 一封退信标一个联系人就够
            if not matched_any:
                results.append({"brand": brand, "subject": subject[:50], "skip": "硬退但未匹配到联系人(地址已变更/非我方发)"})
    return {"processed_bounces": processed, "marked_invalid": marked, "results": results[:30]}

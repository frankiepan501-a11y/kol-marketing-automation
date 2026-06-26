"""寄样状态对账 (2026-05-22 C) — 用 Zoho 发件箱 ground truth 核对 bitable 寄样阶段.

把 2026-05-22 手工审计 (memory kol-ship-recon-2026-05-22) 沉淀成 recurring job:
  扫"寄样阶段=待发货 AND 邮件草稿状态=已发送"的草稿 (= 系统说发了但状态没推进),
  去 Zoho 发件箱**独立验证**确实发出了 ship-confirm/tracking 邮件 + 抽真实运单号,
  验证通过才回填 已发货 + 运单号 + 物流商 + 发货时间.

为何独立查 Zoho 而不直接信 bitable "已发送"字段:
  审计 dead-man-switch 铁律 — 审计要独立于被审计对象, 不信系统自报状态.
  auto_send.py 的 A 修复 (发出即推进) 覆盖未来正常路径; 本 recon 是兜底审计,
  catch 手工发送 / A 失效 / 历史积压 等一切"发了但卡住"的情况.
"""
import re, time, httpx
from . import config, feishu, zoho
from .feishu import ext, xrid

# 发件箱分页上限 (覆盖近期发送; 卡死草稿都是近期的)
_SENT_SCAN_MAX = 600
_SENT_PAGE = 200


def _strip(html: str) -> str:
    t = re.sub(r"<[^>]+>", " ", html or "")
    t = re.sub(r"&nbsp;", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def _extract_tracking(body: str):
    """从已发出的寄样邮件正文抽 (运单号, 物流商). 抽不到返回 (None, None)."""
    txt = _strip(body)
    if "tracking #" not in txt.lower() and "on its way" not in txt.lower():
        return None, None
    tn = None
    m = re.search(r"Tracking\s*#?\s*:?\s*([A-Z0-9]{8,})", txt, re.I)
    if m:
        tn = m.group(1).strip()
    carrier = None
    mc = re.search(r"Carrier\s*:?\s*([A-Za-z][A-Za-z .]{2,30}?)\s*(?:Should|Tracking|Best|$)", txt)
    if mc:
        cand = mc.group(1).strip()
        # carrier 字段历史会被 datetime 污染 (P1-D bug) → 数字/日期则丢弃
        if not re.search(r"\d{2}:\d{2}|\d{4}-\d{2}", cand):
            carrier = cand
    # 运单号是 TBA/TBC 前缀 → Amazon MCF, 即使 carrier 字段脏也能推断
    if not carrier and tn and tn.upper().startswith(("TBA", "TBC")):
        carrier = "Amazon"
    return tn, carrier


async def _list_sent_paged(brand: str):
    """分页拉发件箱 (zoho.list_sent_messages 写死 limit=30 不够, 这里复用 access+folder 自己翻页)."""
    cfg = config.BRAND_CONFIG[brand]
    folders = await zoho._list_folders_raw(brand)
    sent = None
    for f in folders:
        if (f.get("folderType") or "").lower() == "sent" or (f.get("folderName") or "").lower() in ("sent", "已发送"):
            sent = f
            break
    if not sent:
        return None, []
    fid = sent["folderId"]
    tok = await zoho.access(brand)
    out = []
    start = 1
    async with httpx.AsyncClient(timeout=40.0) as cli:
        while start <= _SENT_SCAN_MAX:
            r = await cli.get(
                f"https://mail.zoho.com/api/accounts/{cfg['account_id']}/messages/view"
                f"?folderId={fid}&limit={_SENT_PAGE}&start={start}",
                headers={"Authorization": f"Zoho-oauthtoken {tok}"},
            )
            if r.status_code != 200:
                break
            batch = r.json().get("data") or []
            if not batch:
                break
            out.extend(batch)
            if len(batch) < _SENT_PAGE:
                break
            start += _SENT_PAGE
    return fid, out


async def run() -> dict:
    """对账主流程. dry_recon=只报不写? 默认直接回填 (纯状态字段, 不发邮件)."""
    # 1. 找"系统说已发送但寄样阶段还卡待发货"的草稿
    stuck = await feishu.search_records(config.T_DRAFT, [
        {"field_name": "寄样阶段", "operator": "is", "value": ["待发货"]},
        {"field_name": "邮件草稿状态", "operator": "is", "value": ["已发送"]},
    ])
    if not stuck:
        return {"stuck": 0, "reconciled": 0, "unverified": 0, "details": []}

    # 2. 预拉两个品牌发件箱 (按需)
    sent_cache = {}  # brand -> (fid, [msgs])

    reconciled = 0
    unverified = 0
    details = []
    for rec in stuck:
        f = rec["fields"]
        rid = rec["record_id"]
        # 取联系人邮箱
        ctype = "editor" if xrid(f.get("关联媒体人")) else "KOL"
        crid = xrid(f.get("关联媒体人")) if ctype == "editor" else xrid(f.get("关联KOL"))
        kol_email = ""
        kol_name = ""
        if crid:
            try:
                cr = await feishu.get_record(config.T_EDITOR if ctype == "editor" else config.T_KOL, crid)
                cf = cr["fields"]
                kol_name = ext(cf.get("媒体人姓名")) if ctype == "editor" else ext(cf.get("账号名"))
                kol_email = (feishu.clean_email(ext(cf.get("邮箱")))[0] or "").lower()
            except Exception:
                pass
        if not kol_email:
            kol_email = (feishu.clean_email(ext(f.get("收件邮箱")))[0] or "").lower()
        if not kol_email:
            unverified += 1
            details.append({"rid": rid, "kol": kol_name, "result": "no_email"})
            continue

        brand = config.brand_from_text(ext(f.get("发送邮箱")) or "") or "FUNLAB"  # 2026-06-26 修白牌错标
        if brand not in sent_cache:
            sent_cache[brand] = await _list_sent_paged(brand)
        fid, msgs = sent_cache[brand]

        # 3. 发件箱里找发给该 KOL 的寄样邮件 (含 Tracking #/on its way)
        matched = [m for m in msgs if kol_email in (m.get("toAddress") or "").lower()]
        ship_tn, ship_carrier, ship_ms = None, None, 0
        for m in sorted(matched, key=lambda x: int(x.get("sentDateInGMT") or 0)):
            try:
                body = await zoho.get_message_content(brand, m.get("messageId"), fid)
            except Exception:
                continue
            tn, carrier = _extract_tracking(body)
            if tn or "on its way" in _strip(body).lower():
                ship_tn = tn or ship_tn
                ship_carrier = carrier or ship_carrier
                ship_ms = int(m.get("sentDateInGMT") or 0) or ship_ms
                # 不 break: 后续邮件可能补更完整的运单号

        if not ship_ms:
            unverified += 1
            details.append({"rid": rid, "kol": kol_name, "email": kol_email,
                             "result": "no_ship_email_in_sent"})
            continue

        # 4. 验证通过 → 回填. 文本字段 + 单选分两次 PUT (单选独立防清空铁律).
        text_fields = {"发货时间": ship_ms}
        if ship_tn:
            text_fields["运单号"] = ship_tn
        if ship_carrier:
            text_fields["物流商"] = ship_carrier
        try:
            await feishu.update_record(config.T_DRAFT, rid, text_fields)
            await feishu.update_record(config.T_DRAFT, rid, {"寄样阶段": "已发货"})
            reconciled += 1
            details.append({"rid": rid, "kol": kol_name, "tracking": ship_tn,
                            "carrier": ship_carrier, "result": "reconciled"})
            print(f"[ship_recon] {kol_name} → 已发货 tn={ship_tn} carrier={ship_carrier}")
        except Exception as e:
            unverified += 1
            details.append({"rid": rid, "kol": kol_name, "result": f"update_fail:{e}"})

    return {"stuck": len(stuck), "reconciled": reconciled,
            "unverified": unverified, "details": details}

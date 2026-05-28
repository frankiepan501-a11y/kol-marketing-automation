"""自动发送 (云端版, 替代 send_approved.py)

n8n cron 每 10 分钟触发 → 扫「KOL·媒体人邮件草稿」状态=自动通过 OR 通过 + 发送状态=未发
+ 建议发送时间 ≤ now → Zoho 发送 + 限速 + 跨品牌交叉

发完:
- 草稿: 状态=已发送, 发送状态=已发, 发送时间, 邮件草稿状态=已发送
- KOL: 合作状态 未建联→待回复
- 编辑: 合作状态 未建联→建联中
- 跟进记录表: 新增一条
"""
import re, time, asyncio, random
from . import config, feishu, zoho
from .feishu import ext, xrid


# 限速: 每个品牌每小时 40 封, 每次 cron 扫描最多发 N 封
RATE_PER_RUN = 20         # 每次 cron 跑最多 20 封 (n8n 每 10min 触发 = 每小时 ~120 封,但被分到 2 个品牌 + 限速节奏)
MIN_DELAY = 3             # 云端 delay 比本地短 (3-10s 而不是 30-90s, n8n 单次执行 ≤5min)
MAX_DELAY = 10
PER_BRAND_PER_RUN = 10    # 单品牌单次最多 10 封


def _brand_from_alias(alias: str) -> str:
    s = (alias or "").lower()
    if "powkong" in s: return "POWKONG"
    if "fireflyfunlab" in s or "funlab" in s: return "FUNLAB"
    return "FUNLAB"


# ===== 发送前占位符校验 =====
# 任何模板里的"待填"占位符, 发送前必须删干净, 否则阻止发送
PLACEHOLDER_KEYWORDS = [
    "待填", "[TBD", "[CARRIER", "[TRACKING#", "[ETA",
    "[ADDRESS", "[PRICE", "[QUANTITY", "[xxx", "[XXX",
    "[DISCOUNT_CODE", "[DISCOUNT_PCT",   # warm_recap 暖信折扣占位符: 运营没填折扣比例则不发
]


def has_unfilled_placeholder(subject: str, body: str) -> tuple:
    """检查 subject + body 是否还含未填写的占位符 → (bool, 命中的关键词)"""
    text = (subject or "") + "\n" + (body or "")
    for kw in PLACEHOLDER_KEYWORDS:
        if kw in text:
            return True, kw
    return False, ""


# ===== 1. 扫 ready 草稿 =====
async def scan_ready() -> tuple:
    """
    返回 (ready_list, scheduled_later_count, already_sent_count)
    ready 条件: 邮件草稿状态∈{自动通过, 通过} + 发送状态∈{None, 未发} + 建议发送时间 ≤ now
    """
    items_auto = await feishu.search_records(config.T_DRAFT, [
        {"field_name": "邮件草稿状态", "operator": "is", "value": ["自动通过"]},
    ])
    items_pass = await feishu.search_records(config.T_DRAFT, [
        {"field_name": "邮件草稿状态", "operator": "is", "value": ["通过"]},
    ])
    seen = set()
    items = []
    for r in items_auto + items_pass:
        rid = r["record_id"]
        if rid in seen: continue
        seen.add(rid)
        items.append(r)

    # follow-up 守门: 拉所有按 KOL 分组的草稿
    all_drafts_by_kol = {}
    all_recs = await feishu.fetch_all_records(config.T_DRAFT)
    for rec in all_recs:
        kid = xrid(rec["fields"].get("关联KOL"))
        if kid: all_drafts_by_kol.setdefault(kid, []).append(rec)

    now_ms = int(time.time() * 1000)
    ready = []
    scheduled_later = 0
    already_sent = 0
    skip_followup = 0

    for rec in items:
        f = rec["fields"]
        send_status = ext(f.get("发送状态"))
        if send_status and send_status not in ("未发", ""):
            already_sent += 1
            continue

        # follow-up 守门: KOL 已回复则把这封 follow-up 标"已否决"
        round_num = ext(f.get("Follow-up轮次"))
        if round_num in ("第2封", "第3封"):
            kol_rid = xrid(f.get("关联KOL"))
            if kol_rid and any(d["fields"].get("是否回复") for d in all_drafts_by_kol.get(kol_rid, [])):
                try:
                    await feishu.update_record(config.T_DRAFT, rec["record_id"], {
                        "邮件草稿状态": "已否决", "审批意见": "KOL 已回复, 跳过此 follow-up",
                    })
                except Exception as e:
                    print(f"[auto_send] mark 已否决 fail: {e}")
                skip_followup += 1
                continue

        target_ms = f.get("建议发送时间")
        if target_ms:
            try:
                t = int(target_ms)
                if t > now_ms:
                    scheduled_later += 1
                    continue
            except (ValueError, TypeError):
                pass

        ready.append(rec)

    return ready, scheduled_later, already_sent + skip_followup


# ===== 2. 发一封 =====
async def send_one(rec: dict) -> dict:
    f = rec["fields"]
    rid = rec["record_id"]
    raw_to = ext(f.get("收件邮箱"))
    subject = ext(f.get("邮件主题"))
    body_html = ext(f.get("邮件正文"))
    sender_alias = ext(f.get("发送邮箱"))
    brand = _brand_from_alias(sender_alias)

    # 严格邮箱清洗 (2026-05-16): 历史 multi-email 换行 (SwitchUp/techbymidas) 含 @
    # 通过旧 "@" not in 校验 → Zoho 500; "dm"/"待补" 也不带 @
    to_email, clean_reason = feishu.clean_email(raw_to)
    if not to_email:
        await feishu.update_record(config.T_DRAFT, rid, {
            "发送状态": "失败",
            "发送错误": f"邮箱格式错误: {clean_reason}",
            "邮件草稿状态": "发送失败",
        })
        return {"rid": rid, "ok": False, "error": f"bad email: {raw_to[:60]}"}
    # 多邮箱选第一个时把原因写回审批意见, 方便运营追溯
    if clean_reason:
        try:
            old_note = ext(f.get("审批意见"))
            await feishu.update_record(config.T_DRAFT, rid, {
                "审批意见": (old_note + " | " + clean_reason)[:500],
            })
        except Exception:
            pass

    # v4 email_bounced 闸: 关联联系人「邮箱验真状态=无效」(bounce_monitor 标的硬退死地址) → 停发.
    # 唯一发送 chokepoint, 覆盖 cold/followup/reply/tracking 所有来源; 可逆(运营改回状态即恢复).
    _link_rid = xrid(f.get("关联媒体人")) or xrid(f.get("关联KOL"))
    _link_tbl = config.T_EDITOR if xrid(f.get("关联媒体人")) else config.T_KOL
    if _link_rid:
        try:
            _c = await feishu.get_record(_link_tbl, _link_rid)
            if ext(_c["fields"].get("邮箱验真状态")) == "无效":
                await feishu.update_record(config.T_DRAFT, rid, {
                    "发送状态": "失败",
                    "发送错误": "联系人邮箱验真状态=无效(硬退信), 已停发",
                    "邮件草稿状态": "已否决",
                    "审批意见": ("[退信停发] 该联系人邮箱已被退信处理器标「无效」(硬退). "
                                 "如误判, 在主表把「邮箱验真状态」改回有效/未验后此草稿可重新发。")[:500],
                })
                return {"rid": rid, "ok": False, "error": "contact email 无效 (bounced), skipped"}
        except Exception as e:
            print(f"[auto_send] 邮箱验真状态 gate check fail (放行): {e}")

    # ship_confirm 寄样邮件: 自动用「运单号/物流商」字段值替换正文占位符
    # 张佳烨在草稿表填这两个字段(2 秒动作),无需进正文改文本
    tracking_no = ext(f.get("运单号"))
    carrier = ext(f.get("物流商"))
    if tracking_no:
        body_html = body_html.replace("[TRACKING# 待填运营修改]", tracking_no)
        body_html = body_html.replace("[TRACKING#待填运营修改]", tracking_no)
        body_html = body_html.replace("[TRACKING# 待填]", tracking_no)
    if carrier:
        body_html = body_html.replace("[CARRIER 待填运营修改]", carrier)
        body_html = body_html.replace("[CARRIER待填运营修改]", carrier)
        body_html = body_html.replace("[CARRIER 待填]", carrier)

    # warm_recap 暖信 (P3): 硬门 — 必须 ①「折扣比例」已填 ②正文占位符未被手改, 系统才能插真实 Shopify 码。
    # 防运营手改正文(如把 [DISCOUNT_CODE] 改成 "{KOL名}")+折扣比例空 → 发出半成品暖信(341万粉 Thao 风险)。
    source_field = ext(f.get("邮件草稿来源"))
    if source_field == "warm_recap":
        try:
            _pct_raw = f.get("折扣比例")
            _pct = (float(_pct_raw) / 100.0) if _pct_raw not in (None, "") else 0
        except (ValueError, TypeError):
            _pct = 0
        if _pct <= 0:
            await feishu.update_record(config.T_DRAFT, rid, {
                "邮件草稿状态": "待修改", "审核路径": "需人改",
                "审批意见": "[暖信待填] 请先在草稿填「折扣比例」(数字, 如 15) 再点通过; 「折扣码」可留空(系统按 KOL 名自动生成)。",
            })
            return {"rid": rid, "ok": False, "error": "warm_recap: 折扣比例未填"}
        if "[DISCOUNT_CODE]" not in body_html or "[DISCOUNT_PCT]" not in body_html:
            await feishu.update_record(config.T_DRAFT, rid, {
                "邮件草稿状态": "待修改", "审核路径": "需人改",
                "审批意见": "[暖信占位符被改] 请勿手动改正文里的 [DISCOUNT_CODE]/[DISCOUNT_PCT] —— 系统会按你填的折扣比例/折扣码自动替换。请恢复这两个占位符(或重新生成暖信)再点通过。",
            })
            return {"rid": rid, "ok": False, "error": "warm_recap: 占位符被手改, 拒发"}
        from . import shopify_discount
        _is_ed = bool(xrid(f.get("关联媒体人")))
        _crid = xrid(f.get("关联媒体人")) or xrid(f.get("关联KOL"))
        _handle = ""
        if _crid:
            try:
                _mrec = await feishu.get_record(config.T_EDITOR if _is_ed else config.T_KOL, _crid)
                _handle = ext(_mrec["fields"].get("媒体人姓名") if _is_ed else _mrec["fields"].get("账号名"))
            except Exception:
                pass
        _rc = await shopify_discount.resolve_send_code(brand, _handle or "kol", ext(f.get("折扣码")), _pct)
        if _rc.get("ok") and _rc.get("code"):
            _code = _rc["code"]
            body_html = body_html.replace("[DISCOUNT_CODE]", _code).replace("[DISCOUNT_PCT]", str(int(round(_pct * 100))))
            try:
                await feishu.update_record(config.T_DRAFT, rid, {"折扣码": _code})
                if _crid:
                    await feishu.update_record(config.T_EDITOR if _is_ed else config.T_KOL, _crid, {"折扣码": _code})
            except Exception as e:
                print(f"[auto_send] 回写折扣码失败 {rid}: {e}")
        else:
            await feishu.update_record(config.T_DRAFT, rid, {
                "邮件草稿状态": "待修改", "审核路径": "需人改",
                "审批意见": f"[暖信折扣码生成失败] {str(_rc.get('error'))[:280]}, 请检查 Shopify 折扣码后重试",
            })
            return {"rid": rid, "ok": False, "error": f"discount create failed: {_rc.get('error')}"}

    # 发送前 body 长度 sanity check (V1 最小防御, 防 feishu.ext() multi-segment bug 类再触发)
    # 5/8 ctatechdesk 事故根因: 草稿表 body 是 multi-segment array, ext() 只拿 [0].text 几字符
    # → KOL 收到空白邮件. 修了 ext() 后, 这层 sanity check 是兜底保险.
    plain_body = re.sub(r'<[^>]+>', '', body_html or '').strip()
    if len(plain_body) < 50:
        await feishu.update_record(config.T_DRAFT, rid, {
            "邮件草稿状态": "待修改",
            "审核路径": "需人改",
            "审批意见": f"[发送前 sanity check] body 仅 {len(plain_body)} 纯文本字符, 疑似截断 bug, 拒发. 检查草稿正文 + feishu.ext() 是否拼接所有 segments.",
        })
        return {"rid": rid, "ok": False, "error": f"body too short ({len(plain_body)} chars), suspected truncation"}

    # 发送前占位符校验: 防止"[运单号待填]"等没换就发出去
    has_ph, ph_kw = has_unfilled_placeholder(subject, body_html)
    if has_ph:
        await feishu.update_record(config.T_DRAFT, rid, {
            "邮件草稿状态": "待修改",
            "审核路径": "需人改",
            "审批意见": f"[占位符未替换] 命中 '{ph_kw}', 请运营把模板里的占位符全替换成真实信息",
        })
        return {"rid": rid, "ok": False, "error": f"unfilled placeholder: {ph_kw}"}

    try:
        msg_id = await zoho.send_email(brand, to_email, subject, body_html)
    except Exception as e:
        err = str(e)[:500]
        await feishu.update_record(config.T_DRAFT, rid, {
            "发送状态": "失败", "发送错误": err, "邮件草稿状态": "发送失败",
        })
        return {"rid": rid, "ok": False, "error": err}

    # 更新草稿
    now_ms = int(time.time() * 1000)
    update_payload = {
        "发送状态": "已发",
        "发送时间": now_ms,
        "邮件草稿状态": "已发送",
    }
    # ship_confirm 寄样邮件: 发出后推进寄样阶段 待发货 → 已发货, 写发货时间
    # 2026-05-22 (A): 不再要求运单号 — 运单号在第 2 封 tracking_followup 才填,
    # 旧条件 (寄样订单号 AND tracking_no) 让第 1 封 ship_confirm 永远卡"待发货"
    # (实证 5 个 KOL 卡死, 见 memory kol-ship-recon-2026-05-22).
    # 只要有寄样订单号(=ship_confirm 已批准发出)就推进; 只从 待发货/空 推进, 不覆盖已签收.
    if ext(f.get("寄样订单号")) and ext(f.get("寄样阶段")) in ("", "待发货"):
        update_payload["寄样阶段"] = "已发货"
        update_payload["发货时间"] = now_ms
    await feishu.update_record(config.T_DRAFT, rid, update_payload)

    # 2026-05-17 A5: 发送成功 → 标"已审"群卡片 (防多人审同张卡 race)
    try:
        await feishu.mark_card_resolved(rid, "已发送")
    except Exception as e:
        print(f"[auto_send] mark_card_resolved fail: {e}")

    # 按对象类型 + 跟进
    obj_type = ext(f.get("对象类型"))
    source = ext(f.get("邮件草稿来源"))    # cold / followup / reply / tracking_followup
    body_text = re.sub(r'<[^>]+>', '', body_html or '').replace('&nbsp;', ' ').strip()[:500]
    signature = ext(f.get("发送人署名"))
    follow_content = f"发件: {sender_alias} ({signature})\n主题: {subject}\n\n{body_text}"

    # 跟进摘要前缀 (区分 cold/followup/reply/tracking_followup)
    if source == "tracking_followup":
        prefix = "[运单号追加]"
    elif source == "warm_recap":
        prefix = "[寄样暖信]"
    elif source == "reply":
        prefix = "[回复发出]"
    elif source == "followup":
        prefix = f"[Follow-up {ext(f.get('Follow-up轮次')) or ''}]"
    elif obj_type == "媒体人":
        prefix = "[PR pitch]"
    else:
        prefix = "[冷开发信]"

    if obj_type == "媒体人":
        editor_rid = xrid(f.get("关联媒体人"))
        if editor_rid:
            # 状态变更只在 cold/followup 类型 (reply 已被 reply_monitor 改成洽谈中,不能覆盖)
            if source in ("", "cold", "followup", None):
                try:
                    await feishu.update_record(config.T_EDITOR, editor_rid, {"合作状态": "建联中"})
                except Exception as e:
                    print(f"[auto_send] update editor status: {e}")
            try:
                await feishu.create_record(config.T_EDITOR_FU, {
                    "跟进摘要": f"{prefix} {subject[:80]}",
                    "跟进日期": int(time.time() * 1000),
                    "跟进方式": "邮件",
                    "跟进内容": follow_content,
                    "关联媒体人": [editor_rid],
                })
            except Exception as e:
                print(f"[auto_send] editor follow: {e}")
    else:
        kol_rid = xrid(f.get("关联KOL"))
        if kol_rid:
            if source in ("", "cold", "followup", None):
                try:
                    await feishu.update_record(config.T_KOL, kol_rid, {"合作状态": "待回复"})
                except Exception as e:
                    print(f"[auto_send] update kol status: {e}")
            try:
                await feishu.create_record(config.T_KOL_FU, {
                    "跟进摘要": f"{prefix} {subject[:80]}",
                    "跟进日期": int(time.time() * 1000),
                    "跟进方式": "邮件",
                    "跟进内容": follow_content,
                    "关联KOL": [kol_rid],
                })
            except Exception as e:
                print(f"[auto_send] kol follow: {e}")

    # ship_confirm 第一封发出后, 自动建第 2 条 tracking_followup 草稿
    # 判断: 草稿来源=reply + 命中关键词含 ship-sample (ship_confirm 标志)
    kw_hit = ext(f.get("命中关键词")) or ""
    if source == "reply" and "ship-sample" in kw_hit:
        try:
            await _create_tracking_followup_draft(rec, sender_alias, signature)
        except Exception as e:
            print(f"[auto_send] create tracking_followup fail: {e}")

    return {"rid": rid, "ok": True, "msg_id": msg_id, "to": to_email, "brand": brand}


async def _create_tracking_followup_draft(parent_rec: dict, sender_alias: str, signature: str):
    """ship_confirm 第 1 封发出后,自动建第 2 条 tracking_followup 草稿
    24h 后建议发送, 等运营从 Amazon 拿到运单号填占位符再点通过
    """
    from . import reply_drafter
    pf = parent_rec["fields"]
    obj_type = ext(pf.get("对象类型"))
    parent_subject = ext(pf.get("邮件主题"))
    to_email = ext(pf.get("收件邮箱"))
    parent_rid = parent_rec["record_id"]

    # 拿对方姓名 + 产品名
    contact_name = "there"
    product_name = "the sample"
    if obj_type == "媒体人":
        editor_rid = xrid(pf.get("关联媒体人"))
        if editor_rid:
            try:
                ed = await feishu.get_record(config.T_EDITOR, editor_rid)
                contact_name = ext(ed["fields"].get("媒体人姓名")) or contact_name
            except Exception: pass
        link_field = "关联媒体人"
        link_rid = editor_rid
    else:
        kol_rid = xrid(pf.get("关联KOL"))
        if kol_rid:
            try:
                k = await feishu.get_record(config.T_KOL, kol_rid)
                contact_name = ext(k["fields"].get("账号名")) or contact_name
            except Exception: pass
        link_field = "关联KOL"
        link_rid = kol_rid

    prod_rid = xrid(pf.get("关联产品"))
    if prod_rid:
        try:
            p = await feishu.get_record(config.T_PRODUCT, prod_rid)
            ppf = p["fields"]
            # 优先「产品英文名」, 缺则降级中文剥前缀
            p_en = ext(ppf.get("产品英文名"))
            if p_en:
                product_name = p_en
            else:
                p_raw = ext(ppf.get("产品名"))
                p_clean = re.sub(r'^[A-Z]{1,4}\d{1,4}\s*[-_·]?\s*', '', p_raw).strip() or p_raw
                product_name = p_clean
                print(f"[WARN] 产品 {prod_rid} 缺少「产品英文名」, 降级用 {product_name}")
        except Exception: pass

    # 第 2 封模板
    first = contact_name.strip().split()[0][:30] if contact_name else "there"
    body = reply_drafter.TEMPLATE_TRACKING_FOLLOWUP.format(
        first_name=first,
        product_name=product_name,
        signature=reply_drafter._sender_signature(
            "POWKONG" if "powkong" in (sender_alias or "").lower() else "FUNLAB"
        ),
    )
    subj = parent_subject if parent_subject.startswith("Re:") else f"Re: {parent_subject}"

    now_ms = int(time.time() * 1000)
    schedule_ms = now_ms + 24 * 3600 * 1000  # +24h

    fields = {
        "邮件草稿ID": f"track-{parent_rid[-8:]}-{int(time.time())}",
        "邮件主题": subj[:200],
        "邮件正文": body,
        "邮件语言": "en",
        "邮件草稿状态": "待修改",   # 待运营 24h 后填运单号
        "邮件草稿来源": "tracking_followup",
        "对象类型": obj_type or "KOL",
        "发送邮箱": sender_alias,
        "发送人署名": ext(pf.get("发送人署名")) or "Frankie",
        "收件邮箱": to_email,
        "生成时间": now_ms,
        "建议发送时间": schedule_ms,
        "重生次数": 0,
        "审批意见": f"[等运单号 24h] 父草稿 rid={parent_rid}, 24h 后从 Amazon MCF 拿到运单号填进去再改'通过'",
    }
    if link_rid:
        fields[link_field] = [link_rid]
    if prod_rid:
        fields["关联产品"] = [prod_rid]
    # 2026-05-17 A2: 从父草稿继承关联任务 (任务台「已发送数」统计需要)
    parent_task_rid = xrid(pf.get("关联任务"))
    if parent_task_rid:
        fields["关联任务"] = [parent_task_rid]

    new_rid = await feishu.create_record(config.T_DRAFT, fields)
    print(f"[auto_send] created tracking_followup draft rid={new_rid} (schedule +24h)")


# ===== 3. 主入口 =====
async def run() -> dict:
    ready, scheduled_later, skipped = await scan_ready()
    if not ready:
        return {"sent": 0, "fail": 0, "scheduled_later": scheduled_later, "skipped": skipped, "msg": "no ready drafts"}

    # 按品牌分组
    by_brand = {"POWKONG": [], "FUNLAB": []}
    for r in ready:
        b = _brand_from_alias(ext(r["fields"].get("发送邮箱")))
        by_brand[b].append(r)

    # 限制每品牌每次最多 PER_BRAND_PER_RUN
    for b in by_brand:
        by_brand[b] = by_brand[b][:PER_BRAND_PER_RUN]

    # 交叉队列
    queue = []
    max_per = max(len(v) for v in by_brand.values()) if by_brand else 0
    for i in range(max_per):
        for b in ("POWKONG", "FUNLAB"):
            if i < len(by_brand[b]):
                queue.append(by_brand[b][i])
    queue = queue[:RATE_PER_RUN]  # 全局上限

    results = []
    sent = 0
    fail = 0

    for i, rec in enumerate(queue, 1):
        r = await send_one(rec)
        results.append(r)
        if r["ok"]: sent += 1
        else: fail += 1
        # 间隔
        if i < len(queue):
            await asyncio.sleep(random.randint(MIN_DELAY, MAX_DELAY))

    return {
        "sent": sent, "fail": fail,
        "scheduled_later": scheduled_later, "skipped": skipped,
        "queue_size": len(queue),
        "details": results[:10],
    }

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
from . import config, feishu, zoho, coop_status
from .feishu import ext, xrid


# 2026-06-04: 邮箱域名退信率守卫 — 算各域名历史「无效」率, 高退信域名(猜测格式系统性错)集合.
# 5min cache (一次 cron 内多草稿共用一次全表扫). 数据来自主表 邮箱验真状态=无效.
# 2026-06-10: per-table cache, 泛化到 KOL + editor 两端 (editor/kol wrapper 各自阈值).
_bad_dom_cache = {}   # table_id -> {"ts": float, "doms": set}
_BAD_DOM_TTL = 300


async def _bad_domains(table_id: str, min_inv: int, rate: float) -> set:
    """返回某主表历史退信率高的邮箱域名集合 (无效数≥min_inv 且 无效率≥rate). 带 5min per-table cache."""
    c = _bad_dom_cache.get(table_id)
    if c and c["doms"] is not None and time.time() - c["ts"] < _BAD_DOM_TTL:
        return c["doms"]
    bad = set()
    try:
        recs = await feishu.fetch_all_records(table_id)
        total, invalid = {}, {}
        for r in recs:
            e = ext(r["fields"].get("邮箱")).strip().lower()
            if "@" not in e:
                continue
            d = e.split("@", 1)[1]
            total[d] = total.get(d, 0) + 1
            if ext(r["fields"].get("邮箱验真状态")) == "无效":
                invalid[d] = invalid.get(d, 0) + 1
        for d, inv in invalid.items():
            if inv >= min_inv and inv / max(total.get(d, 1), 1) >= rate:
                bad.add(d)
    except Exception as e:
        print(f"[auto_send] _bad_domains({table_id}) 计算失败 (放行): {e}")
        return set()
    _bad_dom_cache[table_id] = {"ts": time.time(), "doms": bad}
    return bad


async def _editor_bad_domains() -> set:
    return await _bad_domains(config.T_EDITOR, config.EDITOR_DOMAIN_BOUNCE_MIN, config.EDITOR_DOMAIN_BOUNCE_RATE)


async def _kol_bad_domains() -> set:
    return await _bad_domains(config.T_KOL, config.KOL_DOMAIN_BOUNCE_MIN, config.KOL_DOMAIN_BOUNCE_RATE)


# 限速: 每个品牌每小时 40 封, 每次 cron 扫描最多发 N 封
# 2026-06-17 限速闸 (Zoho 账号级封号恢复后防再撞 50封/h·150封/天 限速; 全 env 可调, 恢复头几天调更低如 30)
#   小时级 = PER_BRAND_PER_RUN × cron频率(10min→6run/h): 6×6 = 36/h/品牌 < 50 ✓
#   天级   = SEND_DAILY_CAP 滚动24h已发硬上限 < 150 留 buffer
RATE_PER_RUN = int(config.env("SEND_RATE_PER_RUN", "12"))           # 每次 cron 全局上限
PER_BRAND_PER_RUN = int(config.env("SEND_PER_BRAND_PER_RUN", "6"))  # 单品牌单次上限 (→ ~36/h/品牌)
SEND_DAILY_CAP = int(config.env("SEND_DAILY_CAP", "120"))           # 单品牌滚动 24h 上限 (<150)
PAUSE_THRESHOLD = int(config.env("SEND_PAUSE_THRESHOLD", "3"))      # 连续通道错误数 → 自动暂停
# 2026-06-18 回信预留: cold 每天最多 SEND_DAILY_CAP - REPLY_RESERVE, 留 REPLY_RESERVE 给时间敏感
# (reply/ship/quote)防被 cold 吃光额度饿住。**动态**: 只为"当前真有待发回信"预留, 没回信全释放给 cold(不浪费)。
REPLY_RESERVE = int(config.env("SEND_REPLY_RESERVE", "30"))
MIN_DELAY = 3             # 云端 delay 比本地短 (3-10s 而不是 30-90s, n8n 单次执行 ≤5min)
MAX_DELAY = 10

# ===== 自动暂停 (通道挂时止血, 防 Zoho 再被封; 2026-06-17 改 per-brand: 一品牌挂不连累其他) =====
_paused_brands = {}        # brand -> reason; 该品牌连续通道错误自动暂停
_pause_alerted = set()     # 已告警品牌 (去重)

# ===== DRY-RUN 守卫 (2026-06-18, 防本 session 根因事故复发) =====
# EMAIL_DRY_RUN_TO 有值=有人在测邮件 → run() 拒绝跑全表, 防"DRY-RUN+全表 auto-send"
# 误把真草稿标已发送(→真 KOL 永久漏发)。测邮件用隔离方式(单条合成/纯函数), 不碰全表。
_dryrun_alerted = False    # 一次性提醒去重 (DRY-RUN 清掉后重置 → 下次再设会重新提醒)


async def _dryrun_alert_once(dry_to: str):
    """DRY-RUN 开着挡住生产发送 → 一次性飞书提醒 Frankie(防忘记关 env 致发送长期停)。"""
    global _dryrun_alerted
    if _dryrun_alerted:
        return
    _dryrun_alerted = True
    card = {
        "config": {"wide_screen_mode": True},
        "header": {"template": "yellow",
                   "title": {"tag": "plain_text", "content": "🟡 [KOL] DRY-RUN 开着 · 生产发送已暂停"}},
        "elements": [{"tag": "div", "text": {"tag": "lark_md", "content": (
            f"检测到 `EMAIL_DRY_RUN_TO={dry_to}` → auto_send 已**拒绝全表发送**(防误污染真草稿)。\n\n"
            "• 这是改邮件代码时的测试态, 正常。\n"
            "• **测完记得删掉这个 Zeabur env** → 生产发送才会恢复。\n"
            "• 期间测邮件用隔离方式(单条合成草稿/纯函数), 别调全表 /auto-send/run。")}}],
    }
    try:
        await feishu.send_card_message("open_id", "ou_629ce01f4bc31de078e10fcb038dbf78", card)
    except Exception as e:
        print(f"[auto_send DRY-RUN alert fail] {e}")

# 时间敏感(优先发, 配额先给它们): KOL 回信/寄样/报价/暖信/运单跟进; nudge-前缀也算
_HIGH_PRIORITY_SRC = {"reply", "ship_confirm", "affiliate_quote", "warm_recap", "tracking_followup"}
_COLD_SRC = {"cold", "followup"}     # 非时间敏感(批量 cold + 跟进), 受 cold 天级上限(留预留给回信)


def _is_priority(rec: dict) -> bool:
    """时间敏感草稿(回信/寄样/报价/暖信/运单跟进/nudge) → 不受 cold 上限, 有专属预留。"""
    f = rec["fields"]
    src = ext(f.get("邮件草稿来源")) or ""
    did = ext(f.get("邮件草稿ID")) or ""
    return (src in _HIGH_PRIORITY_SRC) or did.startswith("nudge-")


def _select_brand_drafts(prio: list, cold: list, sent_24h_b: int, cold_sent_24h_b: int) -> tuple:
    """纯函数(可单测): 选该品牌本轮要发的草稿。
    时间敏感(prio)用全部总剩余额度(优先发, 永不被 cold 饿住);
    cold 受 cold天级上限 = SEND_DAILY_CAP - reserve, reserve=min(REPLY_RESERVE, 当前待发回信数)
    → 没回信待发 reserve=0, cold 用满 SEND_DAILY_CAP(不浪费预留)。
    返回 (merged 列表[回信在前, 截到 PER_BRAND_PER_RUN], info dict)。"""
    total_remaining = max(0, SEND_DAILY_CAP - sent_24h_b)
    reserve = min(REPLY_RESERVE, len(prio))                   # 动态: 只为当前待发回信预留
    cold_ceiling = max(0, SEND_DAILY_CAP - reserve)
    cold_remaining = max(0, cold_ceiling - cold_sent_24h_b)
    take_prio = prio[:total_remaining]
    cold_room = max(0, total_remaining - len(take_prio))      # 总额度里留给 cold 的
    take_cold = cold[:min(cold_remaining, cold_room)]
    merged = (take_prio + take_cold)[:PER_BRAND_PER_RUN]
    info = {"sent_24h": sent_24h_b, "cold_sent_24h": cold_sent_24h_b,
            "total_remaining": total_remaining, "reserve": reserve,
            "cold_remaining": cold_remaining, "prio_pending": len(prio),
            "cold_pending": len(cold), "take": len(merged)}
    return merged, info
# Zoho 通道级错误特征 (区别于单收件人 bad email / 占位符等 — 那些不触发暂停)
_CHANNEL_ERR_SIGNS = ("server error '5", "500", "550", "unusual sending", "exceeded", "blocked", "429", "too many")


def _is_channel_error(err: str) -> bool:
    e = (err or "").lower()
    return any(s in e for s in _CHANNEL_ERR_SIGNS)


def _draft_priority(rec: dict) -> tuple:
    """排序键: 时间敏感(reply/ship/quote..)优先, 同级按建议发送时间最旧优先。"""
    f = rec["fields"]
    src = ext(f.get("邮件草稿来源")) or ""
    did = ext(f.get("邮件草稿ID")) or ""
    is_high = (src in _HIGH_PRIORITY_SRC) or did.startswith("nudge-")
    try:
        sched = int(f.get("建议发送时间") or 0)
    except (ValueError, TypeError):
        sched = 0
    return (0 if is_high else 1, sched)


def clear_pause(brand: str = None):
    """人工解除暂停 (确认该品牌 Zoho 可发后调 /auto-send/resume[?brand=X]); 不传 brand 解除全部。"""
    global _paused_brands, _pause_alerted
    if brand:
        _paused_brands.pop(brand, None)
        _pause_alerted.discard(brand)
    else:
        _paused_brands = {}
        _pause_alerted = set()


def pause_state() -> dict:
    return {"paused": bool(_paused_brands), "paused_brands": dict(_paused_brands)}


async def _trigger_pause(brand: str, reason: str):
    """某品牌连续通道错误 → 暂停**该品牌**(不连累其他品牌) + P0 飞书告警 (运营群 + Frankie, 每品牌只发 1 次)。"""
    _paused_brands[brand] = reason
    print(f"[auto_send PAUSE] brand={brand} {reason}")
    if brand in _pause_alerted:
        return
    _pause_alerted.add(brand)
    card = {
        "config": {"wide_screen_mode": True},
        "header": {"template": "red",
                   "title": {"tag": "plain_text", "content": f"🔴 [KOL·P0] {brand} 邮件发送通道异常 · 已自动暂停"}},
        "elements": [{"tag": "div", "text": {"tag": "lark_md", "content": (
            f"**已自动暂停 {brand} 的 KOL 邮件发送**（防 Zoho 账号再被限流封锁）。其他品牌不受影响。\n\n"
            f"**原因**: {reason}\n\n"
            "• 该品牌审批的卡片会**正常排队**，通道恢复后自动补发——不用重复点。\n"
            f"• 通道恢复(Zoho 能发)后，调 `/auto-send/resume?brand={brand}` 解除。")}}],
    }
    try:
        await feishu.send_card_message("chat_id", config.NOTIFY_CHAT_ID, card)
    except Exception as e:
        print(f"[auto_send PAUSE] 群告警失败: {e}")
    for _name, _oid in config.NOTIFY_USERS:
        try:
            await feishu.send_card_message("open_id", _oid, card)
        except Exception as e:
            print(f"[auto_send PAUSE] 私聊告警失败 {_oid}: {e}")


def _brand_from_alias(alias: str) -> str:
    # 2026-06-08 配置驱动(支持白牌); 无匹配兜底 FUNLAB(沿用原默认)
    return config.brand_from_text(alias) or "FUNLAB"


# ===== 发送前占位符校验 =====
# 任何模板里的"待填"占位符, 发送前必须删干净, 否则阻止发送
PLACEHOLDER_KEYWORDS = [
    "待填", "[TBD", "[CARRIER", "[TRACKING#", "[ETA",
    "[ADDRESS", "[PRICE", "[QUANTITY", "[xxx", "[XXX",
    "[DISCOUNT_CODE", "[DISCOUNT_PCT",   # warm_recap 暖信折扣占位符: 运营没填折扣比例则不发
    "[PURCHASE_LINKS",   # ship_confirm 各国追踪购买短链: 运营没粘则不发
]


def format_purchase_links(raw: str) -> str:
    """把运营粘的"标签 链接"文本(空格/逗号/换行/分号分隔, 标签可多词可中文)解析成
    '• 标签: 链接' 清单, 每条独立一行。标签按运营原样保留(支持 Amazon US / Walmart US / 美国 等
    任意平台×国家×独立站组合)。识别不出标签的链接给默认 'Link N'; 完全无链接 → 原样返回(兜底)。
    2026-06-17 接入: 解决运营多站点多链接不知怎么填 + 原样塞进邮件一坨乱的问题。"""
    if not raw or not raw.strip():
        return raw or ""
    tokens = re.split(r'[\s,;，；、\n\r]+', raw.strip())

    def _is_url(t: str) -> bool:
        t = t.strip().rstrip('.,)，。')
        if t.lower().startswith(('http://', 'https://')):
            return True
        return bool(re.match(r'^[\w-]+(\.[\w-]+)+(/\S*)?$', t)) and '.' in t

    items = []
    label = []
    for tok in tokens:
        if not tok:
            continue
        if _is_url(tok):
            lbl = " ".join(label).strip().rstrip(':：').strip()
            items.append((lbl or f"Link {len(items) + 1}", tok))
            label = []
        else:
            label.append(tok)
    if not items:
        return raw.strip()
    return "\n".join(f"• {lbl}: {url}" for lbl, url in items)


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

    # 滚动 24h 已发计数 (按品牌, 供 run() 天级限速闸; 复用上面 all_recs 零额外查询)
    day_ago = now_ms - 86400000
    sent_24h = {b: 0 for b in config.BRAND_CONFIG}
    cold_sent_24h = {b: 0 for b in config.BRAND_CONFIG}     # 其中 cold/followup 已发数 (供回信预留闸)
    for rec in all_recs:
        ff = rec["fields"]
        if ext(ff.get("发送状态")) != "已发":
            continue
        try:
            st = int(ff.get("发送时间") or 0)
        except (ValueError, TypeError):
            continue
        if st >= day_ago:
            b = _brand_from_alias(ext(ff.get("发送邮箱")))
            if b in sent_24h:
                sent_24h[b] += 1
                if (ext(ff.get("邮件草稿来源")) or "") in _COLD_SRC:
                    cold_sent_24h[b] += 1

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

    return ready, scheduled_later, already_sent + skip_followup, sent_24h, cold_sent_24h


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

    # 2026-06-10: A 类 MCN/聚合域名静态黑名单 — 整域作废地址(频道名@代投域名硬拼, 实测整域退信),
    # 退 1 次即拉黑(不等退信率攒够). 与下方动态退信率守卫互补(那个针对真实大媒体域名)。
    # 仅拦 cold(已建联/回复的邮箱已被对方确认有效, 不卡); 命中即否决草稿 + 标联系人「无效」(可逆)。
    if ext(f.get("邮件草稿来源")) == "cold" and "@" in to_email:
        _adom = to_email.split("@", 1)[1].lower()
        if _adom in config.AGGREGATOR_BLOCK_DOMAINS:
            try:
                await feishu.update_record(config.T_DRAFT, rid, {
                    "发送状态": "失败",
                    "发送错误": f"邮箱域名 {_adom} 属 MCN/聚合代投域名(地址按频道名硬拼, 整域作废), 停发",
                    "邮件草稿状态": "已否决",
                    "审批意见": (f"[聚合域名停发] {_adom} 是 MCN/营销平台聚合域名, 邮箱多为'频道名@该域名'"
                                 "猜测拼凑, 非达人本人邮箱(实测整域退信)。请找达人本人邮箱(主页/媒体页/"
                                 "其他渠道)再发。如确认此邮箱有效, 改「邮件草稿状态」重发。")[:500],
                })
                if _link_rid:
                    try:
                        await feishu.update_record(_link_tbl, _link_rid, {"邮箱验真状态": "无效"})
                    except Exception:
                        pass
            except Exception as e:
                print(f"[auto_send] 聚合域名黑名单 update fail: {e}")
            return {"rid": rid, "ok": False, "error": f"aggregator domain {_adom}, skipped"}

    # 2026-06-04: 编辑邮箱域名退信率守卫 — 仅对媒体人 cold (猜测邮箱). 该域名历史高退信(格式系统性
    # 猜错, 如 engadget/vox 33-50%)→ 不发(猜准是浪费), 标'域名高退信-需人工找邮箱/PR inbox'。
    # 仅 cold (回复/已建联的不卡, 那些邮箱已被对方确认有效)。多数中小媒体(0%退信)不在 bad 集→不受影响。
    if ext(f.get("对象类型")) == "媒体人" and ext(f.get("邮件草稿来源")) == "cold" and "@" in to_email:
        try:
            _bad = await _editor_bad_domains()
            _dom = to_email.split("@", 1)[1]
            # Snov 验为 valid 的邮箱(编辑「邮箱验真状态=有效」)放行 — 已确认可送达, 不受历史域名退信率拦
            _snov_ok = False
            if _dom in _bad:
                _link_ed_chk = xrid(f.get("关联媒体人"))
                if _link_ed_chk:
                    try:
                        _ed_chk = await feishu.get_record(config.T_EDITOR, _link_ed_chk)
                        if ext(_ed_chk["fields"].get("邮箱验真状态")) == "有效":
                            _snov_ok = True
                    except Exception:
                        pass
            if _dom in _bad and not _snov_ok:
                await feishu.update_record(config.T_DRAFT, rid, {
                    "发送状态": "失败",
                    "发送错误": f"编辑邮箱域名 {_dom} 历史高退信(猜测格式系统性错), 停发",
                    "邮件草稿状态": "已否决",
                    "审批意见": (f"[域名高退信停发] {_dom} 的编辑邮箱靠猜测({'{fi}{last}@'}), 实测高退信"
                                 "(个人邮箱 PR-gated 猜不到)。建议人工找真实邮箱或改投该媒体 press@/tips@ PR 收件箱。"
                                 "如已确认邮箱有效, 改「邮件草稿状态」重发。")[:500],
                })
                _link_ed = xrid(f.get("关联媒体人"))
                if _link_ed:
                    try:
                        await feishu.update_record(config.T_EDITOR, _link_ed, {"邮箱验真状态": "风险"})
                    except Exception:
                        pass
                return {"rid": rid, "ok": False, "error": f"editor domain {_dom} high-bounce, skipped"}
        except Exception as e:
            print(f"[auto_send] 编辑域名退信守卫 fail (放行): {e}")

    # 2026-06-10: KOL 端邮箱域名退信率守卫 (泛化 editor 守卫). KOL cold 邮箱来自爬虫/聚合平台,
    # 同样系统性退信. 仅 cold; 联系人「邮箱验真状态=有效」(已确认可送达)放行不卡。
    # 比 editor 块保守: 只否决当前草稿, 不改联系人状态(域名高退信≠该地址必死, 避免误标真人无效永久停发)。
    if ext(f.get("对象类型")) == "KOL" and ext(f.get("邮件草稿来源")) == "cold" and "@" in to_email:
        try:
            _kbad = await _kol_bad_domains()
            _kdom = to_email.split("@", 1)[1].lower()
            _kok = False
            if _kdom in _kbad and _link_rid:
                try:
                    _kc = await feishu.get_record(config.T_KOL, _link_rid)
                    if ext(_kc["fields"].get("邮箱验真状态")) == "有效":
                        _kok = True
                except Exception:
                    pass
            if _kdom in _kbad and not _kok:
                await feishu.update_record(config.T_DRAFT, rid, {
                    "发送状态": "失败",
                    "发送错误": f"KOL 邮箱域名 {_kdom} 历史高退信(疑聚合/猜测地址), 停发",
                    "邮件草稿状态": "已否决",
                    "审批意见": (f"[域名高退信停发] {_kdom} 的 KOL 邮箱历史高退信率(疑似聚合/猜测地址)。"
                                 "请找达人本人真实邮箱再发。如确认有效, 改「邮件草稿状态」重发。")[:500],
                })
                return {"rid": rid, "ok": False, "error": f"KOL domain {_kdom} high-bounce, skipped"}
        except Exception as e:
            print(f"[auto_send] KOL 域名退信守卫 fail (放行): {e}")

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

    # 折扣占位符替换 + 硬门 (warm_recap 暖信 + ship_confirm 发货确认 共用, 2026-06-16 购买链接前移到发货确认):
    # 正文带 [DISCOUNT_CODE]/[DISCOUNT_PCT] → 必须 ①「折扣比例」已填 ②占位符未被手改 ③「折扣码」(运营在飞书
    # 卡片粘的 UpPromote 券码)非空。任一不过 → 改"待修改"拒发 (防发出半成品 / [DISCOUNT_CODE] 泄漏给真 KOL)。
    # 不自动建 Shopify 码 — 券码 = UpPromote 真相源(带佣金追踪)。ship_confirm 草稿 = reply/affiliate_quote
    # 来源 + 有寄样订单号 + 阶段待发货 (排除 tracking_followup 第2封, 它来源=tracking_followup 不带券码占位符)。
    source_field = ext(f.get("邮件草稿来源"))
    # 2026-06-24 修阿烨「填了运单号/券码却发不出去」: 原条件加了 寄样阶段∈{空,待发货},
    # 但寄样确认邮件本就在"已发货"前后发, 阶段=已发货/非寄样邮件 的草稿被整段占位符替换跳过 →
    # [DISCOUNT_CODE]/[PURCHASE_LINKS] 留在正文 → 末尾闸门拦回"待修改"→永远发不出。
    # 寄样订单号+来源 已足够界定寄样确认, 下游"字段没填即拦待修改"已防半成品, 故去掉阶段门(不按标签卡)。
    _is_ship_confirm = (source_field in ("reply", "affiliate_quote")
                        and bool(ext(f.get("寄样订单号"))))
    if source_field == "warm_recap" or _is_ship_confirm:
        _lbl = "暖信" if source_field == "warm_recap" else "发货确认"
        try:
            _pct_raw = f.get("折扣比例")
            _pct = (float(_pct_raw) / 100.0) if _pct_raw not in (None, "") else 0
        except (ValueError, TypeError):
            _pct = 0
        if _pct <= 0:
            await feishu.update_record(config.T_DRAFT, rid, {
                "邮件草稿状态": "待修改", "审核路径": "需人改",
                "审批意见": f"[{_lbl}待填] 请在飞书卡片填「折扣%」(数字, 如 10) + 粘 UpPromote 券码后再提交。",
            })
            return {"rid": rid, "ok": False, "error": f"{_lbl}: 折扣比例未填"}
        if "[DISCOUNT_CODE]" not in body_html or "[DISCOUNT_PCT]" not in body_html:
            await feishu.update_record(config.T_DRAFT, rid, {
                "邮件草稿状态": "待修改", "审核路径": "需人改",
                "审批意见": f"[{_lbl}占位符被改] 请勿手动改正文里的 [DISCOUNT_CODE]/[DISCOUNT_PCT] —— 系统会用你粘的 UpPromote 券码 + 折扣% 自动替换。请恢复这两个占位符(或重新生成)再走卡片提交。",
            })
            return {"rid": rid, "ok": False, "error": f"{_lbl}: 占位符被手改, 拒发"}
        _code = (ext(f.get("折扣码")) or "").strip()
        if not _code:
            await feishu.update_record(config.T_DRAFT, rid, {
                "邮件草稿状态": "待修改", "审核路径": "需人改",
                "审批意见": f"[{_lbl}待填券码] 请在飞书卡片粘 UpPromote 券码再提交 —— 「折扣码」不能为空(它是 UpPromote 佣金追踪真相源)。",
            })
            return {"rid": rid, "ok": False, "error": f"{_lbl}: 折扣码(UpPromote 券码)未填"}
        # 用运营粘的 UpPromote 券码替换占位符 (不再调 Shopify 自建码)
        body_html = body_html.replace("[DISCOUNT_CODE]", _code).replace("[DISCOUNT_PCT]", str(int(round(_pct * 100))))
        # 回写券码到 KOL/编辑主表缓存 (供 sales_attribution 折扣码→KOL 归因)
        _is_ed = bool(xrid(f.get("关联媒体人")))
        _crid = xrid(f.get("关联媒体人")) or xrid(f.get("关联KOL"))
        if _crid:
            try:
                await feishu.update_record(config.T_EDITOR if _is_ed else config.T_KOL, _crid, {"折扣码": _code})
            except Exception as e:
                print(f"[auto_send] 回写券码到主表失败 {rid}: {e}")
        # 购买短链 (2026-06-16 双轨): 正文带 [PURCHASE_LINKS] → 要求草稿「购买短链」非空(运营粘各国追踪短链)。
        # warm_recap 模板无此占位符不触发; ship_confirm 模板有 → 没填则拦"待修改"防 [PURCHASE_LINKS] 泄漏。
        if "[PURCHASE_LINKS]" in body_html:
            _links = (ext(f.get("购买短链")) or "").strip()
            if not _links:
                await feishu.update_record(config.T_DRAFT, rid, {
                    "邮件草稿状态": "待修改", "审核路径": "需人改",
                    "审批意见": f"[{_lbl}待填购买短链] 卡片「购买短链」栏填: 标签+链接, 多条空格或换行隔开(如 '美国 amzn.to/x  澳洲 amzn.to/y  独立站 powkong.com/...'), 只1条也行, 系统自动排成清单。",
                })
                return {"rid": rid, "ok": False, "error": f"{_lbl}: 购买短链未填"}
            # 2026-06-17: 解析成"• 标签: 链接"清单(不再原样塞), 运营多站点多链接随意粘格式都规整
            body_html = body_html.replace("[PURCHASE_LINKS]", format_purchase_links(_links))

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

    # 邮件线程化: 草稿带「回复目标MsgID」(reply/affiliate_quote/tracking_followup 等延续对话) →
    # 走 action:reply 串入原 thread; cold/followup 无此值 → 新邮件. dry-run 时 zoho 内部强制降级新邮件.
    reply_to_msg_id = ext(f.get("回复目标MsgID")) or None
    try:
        msg_id = await zoho.send_email(brand, to_email, subject, body_html,
                                       reply_to_msg_id=reply_to_msg_id)
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
        # 2026-05-29 数据 hygiene: 寄样确认发出即把"已寄样"真值回写主表. 幂等(次数 max≥1 / 订单号·日期仅空时写),
        # 与 tracking_followup 第2封/reply_drafter 创建时回写不冲突. 否则主表 寄样次数/上次寄样订单号 恒空
        # → 看板/评分/late-stage 守护对"已寄样"全盲(TG_Geek 主表全空根因之一).
        try:
            _crid = xrid(f.get("关联媒体人")) or xrid(f.get("关联KOL"))
            _ctbl = config.T_EDITOR if xrid(f.get("关联媒体人")) else config.T_KOL
            if _crid:
                _cf = (await feishu.get_record(_ctbl, _crid))["fields"]
                _m = {}
                try:
                    if int(_cf.get("寄样次数") or 0) < 1:
                        _m["寄样次数"] = 1
                except (ValueError, TypeError):
                    _m["寄样次数"] = 1
                if not ext(_cf.get("上次寄样订单号")):
                    _m["上次寄样订单号"] = ext(f.get("寄样订单号"))
                if not _cf.get("上次寄样日期"):
                    _m["上次寄样日期"] = now_ms
                if _m:
                    await feishu.update_record(_ctbl, _crid, _m)
        except Exception as e:
            print(f"[auto_send] 寄样主表回写失败 rid={rid}: {e}")
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

    # P4 软关怀 nudge 复用 source=followup, 但目标是已签收(深漏斗)KOL — 不能把 合作状态 倒回
    # "待回复"/"建联中". 用「邮件草稿ID」nudge- 前缀识别 → 跳过下面的状态重置 (其余 followup 不变).
    # ⚠️ 不能用「命中关键词」识别: route_draft 评审后会用 reviewer hits **覆盖**命中关键词
    # (实测被覆盖成 "sample, late-stage-relationship"), 而「邮件草稿ID」route_draft 不动 = 稳定标记.
    kw_hit = ext(f.get("命中关键词")) or ""
    _is_soft_nudge = ext(f.get("邮件草稿ID")).startswith("nudge-")

    if obj_type == "媒体人":
        editor_rid = xrid(f.get("关联媒体人"))
        if editor_rid:
            # 状态变更只在 cold/followup 类型 (reply 已被 reply_monitor 改成洽谈中,不能覆盖)
            if source in ("", "cold", "followup", None) and not _is_soft_nudge:
                try:
                    _ec = await feishu.get_record(config.T_EDITOR, editor_rid)
                    _adv = coop_status.advance_coop_status(ext(_ec["fields"].get("合作状态")) or "", "建联中")
                    if _adv:  # 单调前进守卫: 已合作/更后阶段不被发信后置打回建联中
                        await feishu.update_record(config.T_EDITOR, editor_rid, {"合作状态": _adv})
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
            if source in ("", "cold", "followup", None) and not _is_soft_nudge:
                try:
                    _kc = await feishu.get_record(config.T_KOL, kol_rid)
                    _adv = coop_status.advance_coop_status(ext(_kc["fields"].get("合作状态")) or "", "待回复")
                    if _adv:  # 单调前进守卫: 已合作/更后阶段不被发信后置打回待回复
                        await feishu.update_record(config.T_KOL, kol_rid, {"合作状态": _adv})
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

    # 🚨 2026-06-01 Frankie 拍板「1 步制」(Nintendo Games 重复事故): ship_confirm 卡已收运单号+物流商并发进确认
    #   邮件 = 一步到位。**不再自动建第 2 条 tracking_followup**(原 2 步制让运营填一次运单号又被第2张卡问一次=重复混淆)。
    #   `_create_tracking_followup_draft` 保留为 dead code(不调用), 历史 tracking_followup 草稿照常处理, 不再新建。
    #   若日后要回 2 步制(ship_confirm 不带运单号"只说在路上"+ 单独运单号卡), 去 TEMPLATE_SHIP_CONFIRM 运单号占位符再恢复此处。

    return {"rid": rid, "ok": True, "msg_id": msg_id, "to": to_email, "brand": brand}


async def _create_tracking_followup_draft(parent_rec: dict, sender_alias: str, signature: str):
    """ship_confirm 第 1 封发出后,自动建第 2 条 tracking_followup 草稿
    24h 后建议发送, 等运营从 Amazon 拿到运单号填占位符再点通过
    """
    from . import reply_drafter
    parent_rid = parent_rec["record_id"]
    # 幂等守卫 (2026-06-01): 防同一 ship_confirm 被 auto_send 重复处理→建多条 tracking_followup→重复运单号卡/邮件.
    # 标记 = 邮件草稿ID 前缀 track-{parent8}-。已存在则跳过(不重复建草稿/不重复发卡)。
    _p8 = parent_rid[-8:]
    try:
        _exist = await feishu.search_records(config.T_DRAFT, [
            {"field_name": "邮件草稿来源", "operator": "is", "value": ["tracking_followup"]},
            {"field_name": "邮件草稿ID", "operator": "contains", "value": [f"track-{_p8}-"]},
        ])
        if _exist:
            print(f"[auto_send] tracking_followup 已存在(parent={parent_rid}), 跳过重复创建 (幂等守卫)")
            return
    except Exception as _e:
        print(f"[auto_send] tracking_followup 幂等查重失败(继续创建): {_e}")
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
    # 邮件线程化: 第 2 封运单号追加继承父草稿的「回复目标MsgID」, 与 ship_confirm 第 1 封串同一 thread.
    parent_thread_mid = ext(pf.get("回复目标MsgID"))
    if parent_thread_mid:
        fields["回复目标MsgID"] = parent_thread_mid

    new_rid = await feishu.create_record(config.T_DRAFT, fields)
    print(f"[auto_send] created tracking_followup draft rid={new_rid} (schedule +24h)")

    # 运单号 form 卡: 发负责人(独立站运营专员)私聊 → 卡上填 运单号+物流商 即发, 无需进表格
    try:
        from .draft_router import _build_ship_tracking_card
        # 2026-05-31 统一字段: 解析 contact_info + brand + email 传给 builder
        _is_ed = bool(feishu.xrid(pf.get("关联媒体人")))
        _crid = feishu.xrid(pf.get("关联媒体人")) if _is_ed else feishu.xrid(pf.get("关联KOL"))
        _ctype = "媒体人" if _is_ed else "KOL"
        _ci = await feishu.resolve_contact_info(_crid, _ctype) if _crid else {}
        _sender = ext(pf.get("发送邮箱")) or ""
        _brand = "POWKONG" if "powkong" in _sender.lower() else "FUNLAB"
        _email = ext(pf.get("收件邮箱")) or ""
        track_card = _build_ship_tracking_card(
            new_rid, contact_name, product_name, subj, "运单号追加",
            contact_info=_ci, brand=_brand, email=_email, contact_type=_ctype)
        _unions = []  # 看板「关联运营」 + /card/resend 撤老卡用
        _mids = {}
        for _nm, _oid in await feishu.resolve_notify_targets("reviewer"):
            uid = await feishu.open_id_to_union_id(_oid)
            if uid:
                msg_id = await feishu.send_card_via_app3("union_id", uid, track_card)
                if msg_id:
                    _unions.append(uid)
                    _mids[uid] = msg_id
        if _unions or _mids:
            await feishu.write_card_recipients_msgids(new_rid, _unions, _mids)
    except Exception as e:
        print(f"[auto_send] tracking_followup 运单号卡发送失败: {e}")


# ===== 3. 主入口 =====
async def run() -> dict:
    # 2026-06-18 DRY-RUN 守卫: EMAIL_DRY_RUN_TO 有值(有人在测邮件) → 拒绝跑全表,
    # 防"DRY-RUN+全表 auto-send"误把真草稿标已发送(→真 KOL 永久漏发, 本 session 事故根因)。
    global _dryrun_alerted
    _dry = (config.env("EMAIL_DRY_RUN_TO", "") or "").strip()
    if _dry:
        await _dryrun_alert_once(_dry)
        return {"sent": 0, "fail": 0, "skipped_dryrun": True, "dry_run_to": _dry,
                "msg": f"DRY-RUN active(EMAIL_DRY_RUN_TO={_dry}) → 全表自动发送已拒绝(防污染真草稿)。"
                       "测邮件用隔离方式(单条合成/纯函数), 测完删此 env 恢复生产发送。"}
    _dryrun_alerted = False     # DRY-RUN 已清 → 重置提醒, 下次再设会重新提醒

    ready, scheduled_later, skipped, sent_24h, cold_sent_24h = await scan_ready()
    if not ready:
        return {"sent": 0, "fail": 0, "scheduled_later": scheduled_later, "skipped": skipped, "msg": "no ready drafts"}

    # 按品牌分组 (2026-06-08 配置驱动: 含白牌, 否则白牌草稿 KeyError 崩)
    by_brand = {b: [] for b in config.BRAND_CONFIG}
    for r in ready:
        b = _brand_from_alias(ext(r["fields"].get("发送邮箱")))
        by_brand[b].append(r)

    # per-brand 自动暂停: 已暂停品牌跳过 (其草稿排队等解除, 不连累其他品牌)
    for b in list(by_brand):
        if b in _paused_brands:
            by_brand[b] = []

    # 限速闸 (2026-06-17 天级上限 + 2026-06-18 回信动态预留):
    #   时间敏感(reply/ship/quote..) 可用全部 daily_remaining(优先发, 永不被 cold 饿住);
    #   cold/followup 受 cold 上限 = SEND_DAILY_CAP - reserve, reserve=min(REPLY_RESERVE, 当前待发回信数)
    #   → 没回信待发则 reserve=0, cold 可用满 SEND_DAILY_CAP(不浪费预留)。
    caps_info = {}
    for b in by_brand:
        prio = sorted([d for d in by_brand[b] if _is_priority(d)], key=_draft_priority)
        cold = sorted([d for d in by_brand[b] if not _is_priority(d)], key=_draft_priority)
        by_brand[b], caps_info[b] = _select_brand_drafts(
            prio, cold, sent_24h.get(b, 0), cold_sent_24h.get(b, 0))

    # 交叉队列
    queue = []
    max_per = max((len(v) for v in by_brand.values()), default=0)
    for i in range(max_per):
        for b in by_brand:
            if i < len(by_brand[b]):
                queue.append(by_brand[b][i])
    queue = queue[:RATE_PER_RUN]  # 全局上限

    results = []
    sent = 0
    fail = 0
    consec = {}     # brand -> 连续 Zoho 通道错误数; 达阈值只暂停该品牌

    for i, rec in enumerate(queue, 1):
        b = _brand_from_alias(ext(rec["fields"].get("发送邮箱")))
        if b in _paused_brands:
            continue     # 本轮内该品牌已被暂停 → 跳过其剩余草稿
        r = await send_one(rec)
        results.append(r)
        if r["ok"]:
            sent += 1
            consec[b] = 0
        else:
            fail += 1
            if _is_channel_error(r.get("error", "")):
                consec[b] = consec.get(b, 0) + 1
                if consec[b] >= PAUSE_THRESHOLD:
                    await _trigger_pause(b, f"连续 {consec[b]} 次 Zoho 通道错误: {r.get('error','')[:140]}")
                    # 不 break: 其他品牌继续发, 只该品牌暂停
            else:
                consec[b] = 0     # 单收件人错误(bad email/占位符)不算通道挂
        # 间隔
        if i < len(queue):
            await asyncio.sleep(random.randint(MIN_DELAY, MAX_DELAY))

    return {
        "sent": sent, "fail": fail,
        "scheduled_later": scheduled_later, "skipped": skipped,
        "queue_size": len(queue),
        "caps": caps_info,
        "paused_brands": dict(_paused_brands),
        "details": results[:10],
    }

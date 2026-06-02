"""退回重生 方案A (2026-06-02): 让「退回重生」真正重新生成一版更贴运营要求+KOL阶段的草稿.

背景: 原 draft_router 把低分草稿标 邮件草稿状态=退回重生 + 重生次数+1, 注释说"由 generator
扫描触发重生", 但**实际从无代码扫描/重生** → 自动+手动重生都只标状态就卡住(orphan):
enrich Layer2 把该 KOL 当"已有草稿"永久排除 / card_audit 当终态不提醒 / 报告不算 → KOL 掉坑,
而卡片还显示"系统将重新生成"误导运营 (张佳烨 2026-06-02 发现).

方案A: 自包含重生 prompt, 喂 3 信号让 DeepSeek 针对性改进, 而非换个说法重 roll:
  ① 上一版被否草稿 + AI评分理由 (学习, 勿重复同样问题)
  ② 运营自定义「重生方向」(form 卡输入, 最高优先指令)
  ③ KOL 当前阶段 (合作状态/寄样/上稿 → 禁早期冷开发话术, 治 stage-blind)
重生 = 新草稿 + 旧草稿置「已否决」(superseded) + 新草稿**强制人审重新走卡**(运营点了重生就是要看新版, 不自动发).
cold / reply 同一函数处理. 重生上限 MAX_MANUAL_REGEN 防 runaway → 转「需人改」走表格.
"""
import time
import json
from . import config, feishu, deepseek
from .feishu import ext, xrid

MAX_MANUAL_REGEN = 3  # 重生次数到此 → 不再重生, 转「需人改」

# 重生时从旧草稿复制到新草稿的结构性字段 (link 字段单独处理)
_COPY_TEXT_FIELDS = ["收件邮箱", "收件姓名", "收件地址 full", "收件电话", "发送邮箱",
                     "发送人署名", "发送时区说明", "建议发送时间", "邮件语言",
                     "UTM 链接", "回复目标MsgID", "对象类型", "邮件草稿来源", "国家/地区"]
_COPY_LINK_FIELDS = ["关联KOL", "关联媒体人", "关联产品", "关联任务", "关联运营"]


def _link_ids(v) -> list:
    """link 字段值 → record_id 列表 (写入用)."""
    out = []
    for x in (v or []):
        if isinstance(x, dict):
            out += (x.get("record_ids") or ([x["record_id"]] if x.get("record_id") else []))
    return out


def _build_regen_prompt(old_subject: str, old_body: str, score_reason: str, feedback: str,
                        stage: str, source: str, contact_name: str, product_name: str,
                        product_sells: str, lang_display: str, is_reply: bool) -> str:
    kind = "回复邮件 (reply)" if is_reply else "外联开发信 (cold outreach)"
    fb_block = (f"\n【运营的重生方向 — 最高优先级, 必须按此改】\n{feedback}\n" if feedback.strip()
                else "\n【运营未填具体方向 — 依据下面的评分理由 + 阶段自行改进】\n")
    return f"""你是资深海外 KOL 外联邮件撰稿人。运营**否决了上一版草稿并要求重新生成一版**。
请针对性改进，**不要只换个说法重写**，要真正解决问题 + 贴合该达人当前阶段。

【这是一封】{kind}，发给达人: {contact_name}，产品: {product_name}
产品卖点参考: {product_sells}
语言: 用 {lang_display} 写。

【上一版(已被否决, 勿照抄, 勿重复同样问题)】
主题: {old_subject}
正文:
{old_body}

【上一版被否/低分的原因】
{score_reason or "(无评分理由, 见运营方向)"}
{fb_block}
【该达人当前阶段 — 必须贴合, 禁止阶段错位】
{stage}
⚠️ 如果该达人已建联/已寄样/已合作/已上稿, **绝对禁止**用"要不要样品/初次合作邀请"等早期冷开发话术;
按其真实阶段写(如已寄样→确认收到+内容沟通; 已上稿→道谢+二次合作)。若是早期未建联才用开发信话术。

【输出要求】
- 主题 < 40 字符 / <= 7 词, 含达人名字或其专属关键词, 不用 collaboration/partnership/opportunity 等营销词
- 正文 80-160 词, 像朋友 DM 不像群发模板, 自然、有针对性
- 严格输出 JSON: {{"subject": "...", "body": "..."}} (body 用 \\n 分段, 不要 HTML)
"""


async def regen_draft(record_id: str, feedback: str = "") -> dict:
    """重生一条草稿. 返回 {ok, new_rid?, route?, skip?/error?}."""
    try:
        rec = await feishu.get_record(config.T_DRAFT, record_id)
    except Exception as e:
        return {"ok": False, "error": f"get draft fail: {e}"}
    f = rec["fields"]
    status = ext(f.get("邮件草稿状态"))
    # 幂等: 已被替代/终态(非退回重生/待审/待修改) 跳过. 已否决也跳(避免重复重生).
    if status in ("已发送", "已否决", "自动通过", "通过"):
        return {"ok": False, "skip": f"草稿终态={status}, 跳过重生"}

    retries = int(f.get("重生次数") or 0)
    source = ext(f.get("邮件草稿来源")) or "cold"
    is_reply = source in ("reply", "tracking_followup", "affiliate_quote", "warm_recap")
    old_subject = ext(f.get("邮件主题"))
    old_body = ext(f.get("邮件正文"))
    score_reason = ext(f.get("AI评分理由"))
    ctype = ext(f.get("对象类型"))
    link_field = "关联媒体人" if ctype == "媒体人" else "关联KOL"
    contact_ids = _link_ids(f.get(link_field))
    contact_table = config.T_EDITOR if ctype == "媒体人" else config.T_KOL

    # 重生次数上限 → 转需人改 (不再重生, 防 runaway)
    if retries >= MAX_MANUAL_REGEN:
        await feishu.update_record(config.T_DRAFT, record_id, {
            "邮件草稿状态": "待审", "审核路径": "需人改",
            "审批意见": f"[重生已达上限 {retries} 次] 请直接在表格里人工修改正文后发, 不再自动重生。"[:500],
        })
        return {"ok": False, "skip": f"重生次数={retries} 已达上限 {MAX_MANUAL_REGEN}, 转需人改"}

    # KOL 当前阶段 (③ stage-aware)
    stage = "(未知阶段)"
    cf = {}
    if contact_ids:
        try:
            crec = await feishu.get_record(contact_table, contact_ids[0])
            cf = crec["fields"]
            from .reply_monitor import _contact_stage_label
            stage = _contact_stage_label(cf) if ctype != "媒体人" else (ext(cf.get("合作状态")) or "媒体人")
        except Exception as e:
            print(f"[regen] 取联系人阶段失败 {contact_ids}: {e}")
    contact_name = (ext(cf.get("账号名")) or ext(cf.get("媒体人姓名")) or ext(f.get("收件姓名")) or "there")

    # 产品信息
    product_name = ""; product_sells = ""
    prod_ids = _link_ids(f.get("关联产品"))
    if prod_ids:
        try:
            pf = (await feishu.get_record(config.T_PRODUCT, prod_ids[0]))["fields"]
            product_name = ext(pf.get("产品英文名")) or ext(pf.get("产品名"))
            product_sells = " / ".join(x for x in [ext(pf.get("卖点1")), ext(pf.get("卖点2")), ext(pf.get("卖点3"))] if x)
        except Exception as e:
            print(f"[regen] 取产品失败 {prod_ids}: {e}")
    lang_display = {"en": "English", "fr": "French", "de": "German", "es": "Spanish",
                    "pt": "Portuguese", "it": "Italian"}.get(ext(f.get("邮件语言")) or "en", "English")

    # ① ② ③ 三信号 prompt → DeepSeek
    prompt = _build_regen_prompt(old_subject, old_body, score_reason, feedback or "",
                                 stage, source, contact_name, product_name, product_sells,
                                 lang_display, is_reply)
    try:
        gen = await deepseek.chat_json(prompt, max_tokens=600, temperature=0.4)
    except Exception as e:
        return {"ok": False, "error": f"deepseek fail: {e}"}
    new_subject = (gen.get("subject") or "").strip()
    new_body = (gen.get("body") or "").strip()
    if not new_subject or len(new_body) < 40:
        return {"ok": False, "error": f"重生输出过短 subj={new_subject!r} body_len={len(new_body)}"}

    # 建新草稿: 复制结构字段 + 新正文 + 重生次数+1 + 待审占位(route_draft 会改)
    new_fields = {}
    for fld in _COPY_TEXT_FIELDS:
        v = ext(f.get(fld))
        if v: new_fields[fld] = v
    for fld in _COPY_LINK_FIELDS:
        ids = _link_ids(f.get(fld))
        if ids: new_fields[fld] = ids
    new_fields.update({
        "邮件主题": new_subject,
        "邮件正文": new_body.replace("\\n", "\n"),
        "邮件草稿状态": "待审",
        "重生次数": retries + 1,
        "生成时间": int(time.time() * 1000),
        "邮件草稿ID": (ext(f.get("邮件草稿ID")) or record_id[:12]) + f"-rg{retries+1}",
        "AI评分理由": f"[重生 #{retries+1}] 上一版({record_id})被否, 已按"
                     + ("运营方向+" if (feedback or '').strip() else "")
                     + "评分理由+当前阶段重写",
    })
    try:
        new_rid = await feishu.create_record(config.T_DRAFT, new_fields)  # 返回 record_id 字符串
    except Exception as e:
        return {"ok": False, "error": f"create new draft fail: {e}"}
    if not new_rid:
        return {"ok": False, "error": "create new draft 无 record_id"}

    # 旧草稿 → 已否决 (superseded)
    try:
        await feishu.update_record(config.T_DRAFT, record_id, {
            "邮件草稿状态": "已否决",
            "审批意见": f"[重生替代 → 新草稿 {new_rid}] 运营点退回重生"
                       + (f", 方向: {feedback[:120]}" if (feedback or '').strip() else "(未填方向)"),
        })
    except Exception as e:
        print(f"[regen] 旧草稿置否决失败 {record_id}: {e}")

    # 新草稿走 route_draft, 强制人审 (运营点了重生=要看新版, 不自动发)
    from . import draft_router
    try:
        route = await draft_router.route_draft(
            new_rid, force_review_reason=f"[重生#{retries+1}] 运营请求重生的新版, 请审")
    except Exception as e:
        route = {"error": str(e)[:120]}
    print(f"[regen] {record_id} → 新草稿 {new_rid} (重生#{retries+1}, fb={'有' if (feedback or '').strip() else '无'}) route={route}")
    return {"ok": True, "old_rid": record_id, "new_rid": new_rid, "retries": retries + 1, "route": route}


async def regen_scan() -> dict:
    """cron 兜底: 扫所有 邮件草稿状态=退回重生 的草稿 → 重生 (修自动路径 + 漏网的手动).
    feedback 取草稿「审批意见」(运营若在表格写了方向)."""
    items = await feishu.search_records(config.T_DRAFT, [
        {"field_name": "邮件草稿状态", "operator": "is", "value": ["退回重生"]},
    ])
    results = []
    for rec in items:
        rid = rec["record_id"]
        fb = ext(rec["fields"].get("审批意见")) or ""
        # 审批意见里若是系统标记([重生替代...] 等) 不当 feedback
        if fb.startswith("[") or fb.startswith("【"):
            fb = ""
        try:
            r = await regen_draft(rid, feedback=fb)
        except Exception as e:
            r = {"ok": False, "rid": rid, "error": str(e)[:120]}
        results.append(r)
    return {"扫到退回重生": len(items), "重生": sum(1 for r in results if r.get("ok")), "details": results[:20]}

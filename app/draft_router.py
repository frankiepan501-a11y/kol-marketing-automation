"""草稿路由器 — 调用 reviewer + 决定下一步走向

输入: 「外联草稿」record_id
输出: 路由结果 (写回评分字段 + 改 草稿状态/审核路径 + 决定是否触发重生/通知)

路由规则 (决策 B/C):
  AI评分 ≥ 8 且 承诺命中=False → 草稿状态=自动通过 / 审核路径=自动通过
  AI评分 5–7 或 承诺命中=True   → 草稿状态=待审 / 审核路径=待人审 + 飞书通知
  AI评分 < 5 且 重生<2          → 草稿状态=退回重生 / 重生次数+1 + 触发对应 generator 重生
  AI评分 < 5 且 重生≥2          → 草稿状态=待审 / 审核路径=需人改 + 飞书通知
"""
import time
from . import config, feishu, reviewer
from .feishu import ext


SCORE_AUTO_THRESHOLD = 8       # ≥ 此分自动通过
SCORE_RETRY_THRESHOLD = 5      # < 此分退回重生
MAX_RETRIES = 2                # 重生上限


async def route_draft(record_id: str, ship_confirm_meta: dict = None) -> dict:
    """
    主入口: 给定草稿 record_id → 评审 + 路由 → 返回结果摘要

    Args:
        ship_confirm_meta: reply_drafter 传入的 ship_confirm 元信息 {address, country, product_name}
            存在表示这是寄样确认草稿, 通知卡片要含仓库发货建议 + 高优先级 + SLA 24h
    """
    # 1. 读草稿
    rec = await feishu.get_record(config.T_DRAFT, record_id)
    f = rec["fields"]
    subject = ext(f.get("邮件主题"))
    body = ext(f.get("邮件正文"))
    contact_type = ext(f.get("对象类型")) or "KOL"
    source = ext(f.get("草稿来源")) or "cold"
    sender_alias = ext(f.get("发送邮箱")) or ""
    # 从 alias 推断品牌
    if "powkong" in sender_alias.lower():
        brand = "POWKONG"
    elif "funlab" in sender_alias.lower() or "firefly" in sender_alias.lower():
        brand = "FUNLAB"
    else:
        brand = "FUNLAB"

    retries = int(f.get("重生次数") or 0)

    # 2. 调 reviewer
    result = await reviewer.review_draft(
        subject=subject, body=body, source=source,
        contact_type=contact_type, brand=brand,
    )

    score = result["score"]
    committed = result["committed"]
    hits = result["keywords_hit"]
    summary = result["summary"]
    reasons = result["reasons"]
    judge = result["ai_commitment_judge"]

    reasons_text = " | ".join(f"{k}:{v}" for k, v in reasons.items())[:500]
    if judge["reason"]:
        reasons_text += f" | 承诺判断:{judge['verdict']}-{judge['reason']}"

    # 3. 决定路由
    if score >= SCORE_AUTO_THRESHOLD and not committed:
        new_status = "自动通过"
        path = "自动通过"
        action = "auto_send"
    elif score < SCORE_RETRY_THRESHOLD and retries < MAX_RETRIES:
        new_status = "退回重生"
        path = "退回重生"
        action = "retry"
    elif score < SCORE_RETRY_THRESHOLD and retries >= MAX_RETRIES:
        new_status = "待审"
        path = "需人改"
        action = "notify_human"
    else:  # 5-7 分 或 承诺命中
        new_status = "待审"
        path = "待人审"
        action = "notify_human"

    # 4. 写回评分字段 + 改状态
    update_fields = {
        "AI评分": score,
        "AI评分理由": (summary + " | " + reasons_text)[:500],
        "承诺命中": committed,
        "命中关键词": ", ".join(hits)[:200],
        "审核路径": path,
        "草稿状态": new_status,
    }
    if action == "retry":
        update_fields["重生次数"] = retries + 1

    await feishu.update_record(config.T_DRAFT, record_id, update_fields)

    # 5. 触发后续动作 (异步, 不阻塞主路由)
    if action == "notify_human":
        await _notify_human_review(record_id, rec, score, committed, summary, reasons_text, path,
                                    ship_confirm_meta=ship_confirm_meta)
    elif action == "retry":
        # 重生在调用方处理 (因为重生需要原始任务上下文)
        # router 只标状态,由 cron 或 generator 自身扫描重生标记触发重生
        pass
    # auto_send: 由 send_approved cron 扫 自动通过 状态自动发,无需此处触发

    return {
        "record_id": record_id,
        "score": score,
        "committed": committed,
        "keywords_hit": hits,
        "path": path,
        "status": new_status,
        "action": action,
        "retries_after": retries + (1 if action == "retry" else 0),
    }


async def _notify_human_review(record_id: str, rec: dict, score: int,
                               committed: bool, summary: str, reasons_text: str, path: str,
                               ship_confirm_meta: dict = None):
    """飞书 IM 通知运营审核
    ship_confirm_meta 存在 → 渲染寄样高优先级卡片 (含仓库发货建议 + SLA)
    """
    f = rec["fields"]
    subject = ext(f.get("邮件主题"))
    contact_type = ext(f.get("对象类型")) or "KOL"
    source = ext(f.get("草稿来源")) or "cold"
    base_url = f"https://u1wpma3xuhr.feishu.cn/base/{config.FEISHU_APP_TOKEN}?table={config.T_DRAFT}"

    if ship_confirm_meta:
        card = _build_ship_confirm_card(record_id, rec, score, summary, ship_confirm_meta, base_url)
        # 寄样高优先级 → 群 + 全员通知 (Frankie + 4 个运营)
        targets = config.NOTIFY_USERS
    else:
        template_color = "orange" if path == "待人审" else "red"
        title_emoji = "📝" if path == "待人审" else "⚠️"
        card = {
            "header": {
                "template": template_color,
                "title": {"tag": "plain_text", "content": f"{title_emoji} 草稿待审 ({path}) — {source} / {contact_type}"},
            },
            "elements": [
                {"tag": "div", "fields": [
                    {"is_short": True, "text": {"tag": "lark_md", "content": f"**AI 评分**: {score}/10"}},
                    {"is_short": True, "text": {"tag": "lark_md", "content": f"**承诺**: {'⚠️ 是' if committed else '否'}"}},
                ]},
                {"tag": "div", "text": {"tag": "lark_md", "content": f"**主题**: {subject[:100]}"}},
                {"tag": "div", "text": {"tag": "lark_md", "content": f"**评分总评**: {summary}"}},
                {"tag": "hr"},
                {"tag": "div", "text": {"tag": "lark_md", "content": f"**审核理由 (5 项)**\n{reasons_text[:300]}"}},
                {"tag": "action", "actions": [
                    {"tag": "button", "text": {"tag": "plain_text", "content": "打开外联草稿"},
                     "url": base_url, "type": "primary"},
                ]},
            ],
        }
        # 个人通知 (Frankie 一定要,需人改时全员)
        targets = [u for u in config.NOTIFY_USERS if u[0].startswith("潘")]
        if path == "需人改":
            targets = config.NOTIFY_USERS

    # 群通知
    try:
        await feishu.send_card_message("chat_id", config.NOTIFY_CHAT_ID, card)
    except Exception as e:
        print(f"[draft_router] notify chat fail: {e}")
    # 个人通知
    for name, oid in targets:
        try:
            await feishu.send_card_message("open_id", oid, card)
        except Exception as e:
            print(f"[draft_router] notify {name} fail: {e}")


# ===== 仓库推荐 (V1: 静态映射, V2 接领星 API) =====
# 国家 → 优先发货仓库类型 (按优先级)
# 优先级: FBA 仓 > 海外仓 > 中国本地仓
COUNTRY_TO_WAREHOUSE = {
    # 北美
    "US":  ["美国 FBA 仓", "美国海外仓 (Powkong/Funlab US)"],
    "CA":  ["加拿大 FBA 仓", "美国 FBA 仓 (跨境快递)"],
    "MX":  ["墨西哥 FBA 仓", "美国 FBA 仓 (DHL 跨境)"],
    # 欧洲 (EFN 共享)
    "UK":  ["英国 FBA 仓", "欧洲海外仓 (DE)"],
    "DE":  ["德国 FBA 仓 (EFN)", "欧洲海外仓 (DE)"],
    "FR":  ["法国 FBA 仓 (EFN)", "德国 FBA 仓 (EFN 共享)"],
    "ES":  ["西班牙 FBA 仓 (EFN)", "德国 FBA 仓 (EFN 共享)"],
    "IT":  ["意大利 FBA 仓 (EFN)", "德国 FBA 仓 (EFN 共享)"],
    "NL":  ["荷兰 FBA 仓 (EFN)", "德国 FBA 仓 (EFN 共享)"],
    "PT":  ["欧洲海外仓 (DE/ES)", "德国 FBA 仓 (EFN 共享)"],
    "SE":  ["瑞典 FBA 仓", "德国 FBA 仓 (EFN 共享)"],
    # 日澳
    "JP":  ["日本 FBA 仓", "日本海外仓"],
    "AU":  ["澳洲 FBA 仓", "中国本地仓直发 (DHL Express)"],
    # 拉美
    "BR":  ["巴西海外仓", "中国本地仓直发 (DHL Express)"],
    # 亚太其他
    "IN":  ["印度 FBA 仓", "中国本地仓直发"],
    "TH":  ["中国本地仓直发", "新加坡海外仓 (中转)"],
    "PH":  ["中国本地仓直发"],
    "ID":  ["中国本地仓直发", "新加坡海外仓 (中转)"],
    "AE":  ["阿联酋海外仓", "中国本地仓直发 (Aramex)"],
}


def _recommend_warehouses(country_code: str) -> list:
    """根据国家给出仓库推荐顺序; 找不到返回兜底"""
    if not country_code:
        return ["⚠️ 未识别国家, 请运营手动从领星 ERP 确认仓库"]
    return COUNTRY_TO_WAREHOUSE.get(country_code.upper()) or [
        f"⚠️ {country_code} 暂无映射, 中国本地仓直发 (DHL Express)",
        "请运营在领星 ERP 确认是否有当地海外仓",
    ]


def _build_ship_confirm_card(record_id: str, rec: dict, score: int, summary: str,
                              meta: dict, base_url: str) -> dict:
    """SHIP_CONFIRM 高优先级卡片"""
    f = rec["fields"]
    contact_type = ext(f.get("对象类型")) or "KOL"
    subject = ext(f.get("邮件主题"))
    address = (meta.get("address") or "").strip()
    country = (meta.get("country") or "").strip().upper()
    product_name = (meta.get("product_name") or "").strip()

    warehouses = _recommend_warehouses(country)
    wh_md = "\n".join(f"  {i+1}. {w}" for i, w in enumerate(warehouses))

    # SLA 24h
    import time as _t
    sla_dt = _t.strftime("%Y-%m-%d %H:%M", _t.localtime(_t.time() + 24 * 3600))

    return {
        "header": {
            "template": "red",
            "title": {"tag": "plain_text",
                      "content": f"⚠️ 高优先级 — 寄样确认 ({contact_type}) | 24h SLA"},
        },
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md",
                "content": f"**👤 对方主动给了寄送地址 + 想收 {product_name}**"}},
            {"tag": "hr"},
            {"tag": "div", "fields": [
                {"is_short": True, "text": {"tag": "lark_md", "content": f"**国家**: {country or '?'}"}},
                {"is_short": True, "text": {"tag": "lark_md", "content": f"**产品**: {product_name}"}},
                {"is_short": False, "text": {"tag": "lark_md", "content": f"**📦 收件地址 (AI 提取)**\n```\n{address[:400]}\n```"}},
            ]},
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md",
                "content": f"**🚚 仓库发货建议** (优先级排序, 优先海外仓/FBA 减少时效)\n{wh_md}\n\n_请到领星 ERP 确认对应仓库的库存 + 仓库负责人, 走寄样审批流程_"}},
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md",
                "content": f"**⏰ SLA**: 必须在 **{sla_dt}** 之前完成: 库存确认 → 物流 owner 指派 → 草稿审核通过 → 系统自动发回信"}},
            {"tag": "div", "text": {"tag": "lark_md",
                "content": f"**📝 AI 草稿评分**: {score}/10 — {summary}"}},
            {"tag": "action", "actions": [
                {"tag": "button", "text": {"tag": "plain_text", "content": "打开此条草稿"},
                 "url": f"{base_url}", "type": "primary"},
                {"tag": "button", "text": {"tag": "plain_text", "content": "领星 ERP 仓库管理"},
                 "url": "https://erp.lingxing.com", "type": "default"},
            ]},
        ],
    }


async def batch_review_pending() -> dict:
    """
    扫所有 草稿状态=待审 + 没有 AI评分 的记录 → 跑 reviewer
    用作兜底 cron, 防止生成器漏调 router
    """
    items = await feishu.search_records(config.T_DRAFT, [
        {"field_name": "草稿状态", "operator": "is", "value": ["待审"]},
    ])
    processed = []
    for rec in items:
        if rec["fields"].get("AI评分") is not None:
            continue  # 已审过
        try:
            r = await route_draft(rec["record_id"])
            processed.append(r)
        except Exception as e:
            processed.append({"record_id": rec["record_id"], "error": str(e)[:200]})
    return {"processed": len(processed), "details": processed[:20]}

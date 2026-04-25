"""KOL 营销自动化 Webhook 服务
部署在 Zeabur, 由 n8n cron / webhook 触发
"""
from fastapi import FastAPI, Header, HTTPException
from . import config, reply_monitor, dashboard, followup, enrich, auto_send, draft_router, sla_check

app = FastAPI(title="KOL Marketing Automation", version="0.2")


def _check_auth(auth: str):
    if not auth or not auth.startswith("Bearer "):
        raise HTTPException(401, "Missing Bearer token")
    if auth[7:] != config.INTERNAL_TOKEN:
        raise HTTPException(401, "Invalid token")


@app.get("/")
async def root():
    return {"service": "kol-marketing-automation", "status": "up"}


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/reply-monitor/run")
async def run_reply_monitor(authorization: str = Header(default="")):
    """扫 partner@ 收件箱新回复 → DeepSeek 分类 → 更新数据库 → 飞书通知 + 生成回复草稿"""
    _check_auth(authorization)
    try:
        result = await reply_monitor.run()
        return {"ok": True, **result}
    except Exception as e:
        import traceback
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()[-1000:]}


@app.post("/dashboard/refresh")
async def run_dashboard(authorization: str = Header(default="")):
    """每日 9:00 刷新 KOL+编辑 营销数据看板"""
    _check_auth(authorization)
    try:
        result = await dashboard.run()
        return {"ok": True, **result}
    except Exception as e:
        import traceback
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()[-1000:]}


@app.post("/followup/generate")
async def run_followup(authorization: str = Header(default="")):
    """每日 10:00 扫无回复草稿 → 生成 D+7 第2封 / D+14 第3封 → 调 reviewer"""
    _check_auth(authorization)
    try:
        result = await followup.run()
        return {"ok": True, **result}
    except Exception as e:
        import traceback
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()[-1000:]}


@app.post("/enrich-task/run")
async def run_enrich_task(authorization: str = Header(default="")):
    """每 5 分钟扫 KOL 营销任务台 待触发任务 → 富化打分 + 生草稿 + 调 reviewer"""
    _check_auth(authorization)
    try:
        result = await enrich.run()
        return {"ok": True, **result}
    except Exception as e:
        import traceback
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()[-1500:]}


@app.post("/auto-send/run")
async def run_auto_send(authorization: str = Header(default="")):
    """每 10 分钟扫 自动通过/通过 状态草稿 → Zoho 双品牌发送 + 限速"""
    _check_auth(authorization)
    try:
        result = await auto_send.run()
        return {"ok": True, **result}
    except Exception as e:
        import traceback
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()[-1000:]}


@app.post("/reviewer/scan")
async def run_reviewer_scan(authorization: str = Header(default="")):
    """兜底: 扫所有 待审 + 没 AI评分 的草稿, 跑 reviewer 自审 (防止生成器漏调)"""
    _check_auth(authorization)
    try:
        result = await draft_router.batch_review_pending()
        return {"ok": True, **result}
    except Exception as e:
        import traceback
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()[-1000:]}


@app.post("/sla/check")
async def run_sla_check(authorization: str = Header(default="")):
    """每 6h 扫 ship_confirm 草稿超 24h 未处理 → 升级通知"""
    _check_auth(authorization)
    try:
        result = await sla_check.run()
        return {"ok": True, **result}
    except Exception as e:
        import traceback
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()[-1000:]}


@app.post("/reviewer/run-one")
async def run_reviewer_one(authorization: str = Header(default=""),
                            record_id: str = ""):
    """单条草稿强制重审 (人工触发, 调试用)"""
    _check_auth(authorization)
    if not record_id:
        return {"ok": False, "error": "missing record_id"}
    try:
        result = await draft_router.route_draft(record_id)
        return {"ok": True, **result}
    except Exception as e:
        import traceback
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()[-500:]}


from fastapi import Body
from . import reply_drafter, reply_monitor
from .reply_drafter import _classify_interest, _gen_general_interest_draft, _gen_quote_draft, _gen_clarify_draft
from .reply_drafter import (
    TEMPLATE_UNSUBSCRIBE, TEMPLATE_DECLINE, TEMPLATE_SEND_ASSETS,
    TEMPLATE_SHIP_CONFIRM, TEMPLATE_SCHEDULE_CALL, CALENDLY_DEFAULT,
    _first_name, _sender_signature,
)


@app.post("/reply-drafter/dry-run")
async def reply_drafter_dry_run(authorization: str = Header(default=""),
                                  payload: dict = Body(default={})):
    """Dry-run 测试 reply_drafter — 给输入回复, 返回生成的草稿 (不写飞书)
    Payload:
      {
        "intent_type": "感兴趣|要报价|委婉拒绝|退订|不明意图",
        "intent_summary": "...",
        "original_subject": "...",
        "original_body": "...",
        "contact_name": "Scott Stein",
        "brand": "POWKONG|FUNLAB",
        "product_name": "Piranha Plant Switch 2 Dock",
        "product_link": "https://...",
        "is_editor": true|false
      }
    返回: {ok, ooo_check, sub_classify(if 感兴趣), subject, body, would_route}
    """
    _check_auth(authorization)
    p = payload or {}
    intent_type = p.get("intent_type", "感兴趣")
    intent_summary = p.get("intent_summary", "")
    original_subject = p.get("original_subject", "")
    original_body = p.get("original_body", "")
    contact_name = p.get("contact_name", "there")
    brand = p.get("brand", "FUNLAB")
    product_name = p.get("product_name", "our latest product")
    product_link = p.get("product_link", "")

    sig_full = _sender_signature(brand)
    first = _first_name(contact_name)

    # OOO 预检
    ooo_hit, ooo_frag = reply_monitor.is_ooo(original_subject, original_body)
    result = {"ooo_check": {"hit": ooo_hit, "match": ooo_frag}}
    if ooo_hit:
        result["action"] = "skip (OOO auto-reply, no draft generated)"
        return {"ok": True, **result}

    sub_info = None
    subj = ""
    body = ""
    if intent_type == "退订":
        subj = "Re: " + original_subject[:150]
        body = TEMPLATE_UNSUBSCRIBE.format(first_name=first, signature=sig_full)
    elif intent_type == "委婉拒绝":
        subj = "Re: " + original_subject[:150]
        body = TEMPLATE_DECLINE.format(first_name=first, signature=sig_full)
    elif intent_type == "感兴趣":
        sub_info = await _classify_interest(original_body)
        subj = "Re: " + original_subject[:150]
        if sub_info["sub"] == "ship_confirm":
            body = TEMPLATE_SHIP_CONFIRM.format(first_name=first, signature=sig_full,
                                                  product_name=product_name)
        elif sub_info["sub"] == "schedule_call":
            body = TEMPLATE_SCHEDULE_CALL.format(first_name=first, signature=sig_full,
                                                   calendly_link=CALENDLY_DEFAULT)
        elif sub_info["sub"] == "send_assets":
            body = TEMPLATE_SEND_ASSETS.format(first_name=first, signature=sig_full,
                                                 product_name=product_name,
                                                 product_link=product_link or "(I'll send the deck shortly)")
        else:  # general
            d = await _gen_general_interest_draft(contact_name, original_subject, original_body,
                                                    brand, product_name, product_link)
            subj = d["subject"]; body = d["body"]
    elif intent_type == "要报价":
        d = await _gen_quote_draft(contact_name, original_subject, original_body,
                                    intent_summary, brand, product_name, product_link)
        subj = d["subject"]; body = d["body"]
    elif intent_type == "不明意图":
        d = await _gen_clarify_draft(contact_name, original_subject, original_body,
                                      intent_summary, brand)
        subj = d["subject"]; body = d["body"]

    # 计算 would_route (走 reviewer 评分)
    from . import reviewer
    contact_type = "editor" if p.get("is_editor") else "KOL"
    review_result = await reviewer.review_draft(subj, body, source="reply",
                                                  contact_type=contact_type, brand=brand)
    score = review_result["score"]
    committed = review_result["committed"]

    # ship_confirm 强制 committed=True
    forced_commit = False
    if sub_info and sub_info["sub"] == "ship_confirm":
        committed = True
        forced_commit = True

    # 路由决策
    if score >= 8 and not committed:
        path = "自动通过 (会自动发)"
    elif score < 5:
        path = "退回重生"
    else:
        path = "待人审 (高优先级 + SLA 24h)" if forced_commit else "待人审"

    result.update({
        "intent_type": intent_type,
        "sub_classify": sub_info,
        "subject": subj,
        "body": body,
        "review": {
            "score": score,
            "committed": committed,
            "forced_commit_by_ship": forced_commit,
            "keywords_hit": review_result["keywords_hit"],
            "summary": review_result["summary"],
        },
        "would_route": path,
    })
    return {"ok": True, **result}

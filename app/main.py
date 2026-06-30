"""KOL 营销自动化 Webhook 服务
部署在 Zeabur, 由 n8n cron / webhook 触发
"""
import asyncio
import os
import time
import traceback as _tb
from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from . import config, reply_monitor, dashboard, followup, enrich, enrich_editor, auto_send, draft_router, sla_check, dispatch, relabel, keyword_cron, feishu, ship_recon, draft_cleanup, bounce_monitor, shopify_discount, warm_recap, talking_points, draft_regen, kol_dedup, keyword_supply, draft_status_audit, draft_duplicate_audit, kol_audit_digest
from . import weekly_report  # P0 周报模块, 设计方案 https://u1wpma3xuhr.feishu.cn/wiki/QeQMw2peBiJcIdkKBI2c1tBbnLe
from . import cs_ingest  # 客服助手 v0: Powkong 邮箱采集→分类→工单台 (memory cs-channel-apiization-2026-06-24)
from . import cs_dispatch  # 客服助手 v0: 工单台待派 → 派单卡片(观察期全发 Frankie)

app = FastAPI(title="KOL Marketing Automation", version="0.2")

# Endpoint 失败告警 dedup: {endpoint: last_alert_ts} (60 min 内同 endpoint 只告 1 次)
_alert_last = {}
_ALERT_COOLDOWN = 3600


def _check_auth(auth: str):
    if not auth or not auth.startswith("Bearer "):
        raise HTTPException(401, "Missing Bearer token")
    if auth[7:] != config.INTERNAL_TOKEN:
        raise HTTPException(401, "Invalid token")


async def _alert_endpoint_failure(endpoint: str, error: str, trace: str = ""):
    """n8n cron 触发的 endpoint 失败时, 发飞书卡片告警给 Frankie.
    Dedup: 同 endpoint 60min 内只发 1 次, 防 cron 5min 跑一次轰炸 Frankie.

    2026-05-17 加入 (Bug A8): 替代每个 n8n workflow 加 OnError node.
    """
    now = time.time()
    last = _alert_last.get(endpoint, 0)
    if now - last < _ALERT_COOLDOWN:
        return  # 冷却期内, 跳过
    _alert_last[endpoint] = now

    transient = "1254607" in (error or "") or "Data not ready" in (error or "") or "1254607" in (trace or "")
    level = "P2" if transient else "P1"
    template = "yellow" if transient else "red"
    impact = "本轮 endpoint 未完成；下次 cron 会再跑。若连续出现再查 Zeabur/飞书。"
    if endpoint == "/auto-send/run":
        impact = "本轮 auto-send 可能跳过，已通过 1h 冷却避免重复刷屏；下次 cron 会再尝试。"

    card = {
        "header": {
            "template": template,
            "title": {"tag": "plain_text", "content": f"KOL 发信链运行异常 · {endpoint}"},
        },
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md",
                "content": (
                    "**类型**: endpoint 运行失败（不是草稿状态审计）\n"
                    f"**影响**: {impact}\n"
                    f"**错误**: {(error or '')[:300]}\n"
                    "**冷却**: 同 endpoint 1h 内只告 1 次"
                )}},
            {"tag": "div", "text": {"tag": "lark_md",
                "content": f"**Trace 末段**:\n```\n{trace[-400:] if trace else '(无)'}\n```"}},
        ],
    }
    try:
        # 2026-06-08 不进群(Frankie #4)。端点失败=infra 故障, 运营无法处理 → 保持只私聊 Frankie
        # (退信/重复才给 Frankie+运营; 此处沿用原"防其他人误以为要处理"设计)。
        for name, oid in config.NOTIFY_USERS:
            if name.startswith("潘"):
                try: await feishu.send_card_message("open_id", oid, card, biz="AUDIT", level=level)
                except Exception: pass
    except Exception as e:
        print(f"[_alert_endpoint_failure] {endpoint} self-alert fail: {e}")


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
        tr = _tb.format_exc()[-1000:]
        await _alert_endpoint_failure("/reply-monitor/run", str(e), tr)
        return {"ok": False, "error": str(e), "trace": tr}


@app.post("/keyword-supply/run")
async def run_keyword_supply(authorization: str = Header(default=""), dry_run: bool = False):
    """自动关键词供给: YouTube 待触发队列<水位时 DeepSeek 生受众/IP/主题向长尾词补进爬虫任务台,
    让 daemon 持续有词抓(消除关键词断供的脉冲式产出)。?dry_run=true 只生成不写表。"""
    _check_auth(authorization)
    try:
        result = await keyword_supply.run(dry_run=dry_run)
        return {"dry_run": dry_run, **result}
    except Exception as e:
        tr = _tb.format_exc()[-1000:]
        await _alert_endpoint_failure("/keyword-supply/run", str(e), tr)
        return {"ok": False, "error": str(e), "trace": tr}


@app.post("/bounce-monitor/run")
async def run_bounce_monitor(authorization: str = Header(default=""), dry_run: bool = False):
    """扫 partner@ 收件箱 mailer-daemon 硬退信 → 标联系人「邮箱验真状态=无效」+ 停发 (v4 email_bounced).
    ?dry_run=true: 只报会标哪些联系人, 不真写/不通知 (首跑核对匹配正确性)."""
    _check_auth(authorization)
    try:
        result = await bounce_monitor.run(dry_run=dry_run)
        return {"ok": True, "dry_run": dry_run, **result}
    except Exception as e:
        tr = _tb.format_exc()[-1000:]
        await _alert_endpoint_failure("/bounce-monitor/run", str(e), tr)
        return {"ok": False, "error": str(e), "trace": tr}


@app.post("/talking-points/run")
async def run_talking_points(authorization: str = Header(default=""),
                             product_rid: str = "", kol_rid: str = "", overwrite: bool = False):
    """AI 生成 brief talking points + 拍摄角度(从产品卖点)→ 写产品库 → 通知运营审。
    ?product_rid=单个产品; 不传则扫上架状态=主推 缺 Talking Points 的产品。?overwrite=true 覆盖已有。
    ?product_rid=&kol_rid= 同时给 → 只读返回 per-KOL 定制 brief(框架+5 hooks+TikTok SEO, 不写表, 供验证)。"""
    _check_auth(authorization)
    try:
        if product_rid and kol_rid:
            return {"ok2": True, **(await talking_points.generate_for_kol(product_rid, kol_rid))}
        if product_rid:
            return {"ok2": True, **(await talking_points.generate_for_product(product_rid, overwrite=overwrite, notify=True))}
        return {"ok": True, **(await talking_points.run(overwrite=overwrite))}
    except Exception as e:
        tr = _tb.format_exc()[-1000:]
        return {"ok": False, "error": str(e), "trace": tr}


@app.post("/draft/regen")
async def run_draft_regen(record_id: str = Query(...), feedback: str = Query(""),
                          authorization: str = Header(default="")):
    """退回重生 方案A: 给指定草稿真重生一版(3信号: 上一版+评分理由 / 运营方向feedback / 当前阶段),
    旧草稿置已否决, 新草稿强制人审重新走卡。n8n 卡片「退回重生」按钮调此端点。"""
    _check_auth(authorization)
    try:
        result = await draft_regen.regen_draft(record_id, feedback=feedback or "")
        return {"ok": True, **result}
    except Exception as e:
        tr = _tb.format_exc()[-1000:]
        return {"ok": False, "error": str(e), "trace": tr}


@app.post("/draft/regen-scan/run")
async def run_draft_regen_scan(authorization: str = Header(default="")):
    """cron 兜底: 扫 邮件草稿状态=退回重生 的草稿 → 重生 (修自动路径 + 漏网手动)。"""
    _check_auth(authorization)
    try:
        result = await draft_regen.regen_scan()
        return {"ok": True, **result}
    except Exception as e:
        tr = _tb.format_exc()[-1000:]
        await _alert_endpoint_failure("/draft/regen-scan/run", str(e), tr)
        return {"ok": False, "error": str(e), "trace": tr}


@app.post("/warm-recap/run")
async def run_warm_recap(authorization: str = Header(default="")):
    """P3 寄样后暖信: 扫 寄样阶段=已签收 → 给还没暖信的 KOL 生成"确认收到+brief recap"暖信草稿(强制人审, 运营填折扣)"""
    _check_auth(authorization)
    try:
        result = await warm_recap.run()
        return {"ok": True, **result}
    except Exception as e:
        tr = _tb.format_exc()[-1000:]
        await _alert_endpoint_failure("/warm-recap/run", str(e), tr)
        return {"ok": False, "error": str(e), "trace": tr}


@app.post("/cs/ingest")
async def run_cs_ingest(authorization: str = Header(default=""),
                        source: str = "all", limit: int = 20, dry_run: bool = False):
    """客服助手 v0: 拉客服邮箱(Powkong=Zoho / Funlab=网易IMAP) → DeepSeek 分类/路由 → 写工单台(只读观察).
    ?source=all|powkong|funlab / ?limit=N / ?dry_run=true 只分类不写表(返回 samples)."""
    _check_auth(authorization)
    try:
        result = await cs_ingest.run(source=source, limit=limit, dry_run=dry_run)
        return {"ok": True, **result}
    except Exception as e:
        tr = _tb.format_exc()[-1000:]
        await _alert_endpoint_failure("/cs/ingest", str(e), tr)
        return {"ok": False, "error": str(e), "trace": tr}


@app.post("/cs/dispatch")
async def run_cs_dispatch(authorization: str = Header(default=""), limit: int = 10, rids: str = ""):
    """客服助手 v0: 工单台待派 → 派单卡片. 观察期(CS_DISPATCH_OBSERVE=1)全部发 Frankie 校准.
    ?rids=rid1,rid2 定向派指定工单(审计后精确放行, 避开未审计渠道)."""
    _check_auth(authorization)
    try:
        result = await cs_dispatch.run(limit=limit, rids=rids)
        return {"ok": True, **result}
    except Exception as e:
        tr = _tb.format_exc()[-1000:]
        await _alert_endpoint_failure("/cs/dispatch", str(e), tr)
        return {"ok": False, "error": str(e), "trace": tr}


@app.post("/cs/callback")
async def cs_callback(request: Request, authorization: str = Header(default="")):
    """客服卡片按钮回调(经 n8n cs-assistant-callback 转发). 返回 toast 给操作人即时反馈."""
    _check_auth(authorization)
    try:
        payload = await request.json()
        event = payload.get("event", payload)
        return await cs_dispatch.handle_callback(event)
    except Exception as e:
        return {"toast": {"type": "error", "content": "处理失败，请稍后重试"}}


@app.post("/kol-discount/selftest")
async def run_kol_discount_selftest(authorization: str = Header(default=""), brand: str = "FUNLAB"):
    """P2 云端 smoke: 建一次性 Shopify 测试折扣码 → 删除. 验证 SHOPIFY_* env + 鉴权 + GraphQL 通."""
    _check_auth(authorization)
    try:
        return {"ok": True, **(await shopify_discount.selftest(brand))}
    except Exception as e:
        tr = _tb.format_exc()[-1000:]
        return {"ok": False, "error": str(e), "trace": tr}


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


@app.post("/dispatch/run")
async def run_dispatch(authorization: str = Header(default="")):
    """每日 09:05 北京 派单调度: 读主推+派单就绪产品 → 按品牌分配额度 → 在 KOL 任务台建任务 → 触发 enrich-task"""
    _check_auth(authorization)
    try:
        result = await dispatch.run()
        return {"ok": True, **result}
    except Exception as e:
        tr = _tb.format_exc()[-1500:]
        await _alert_endpoint_failure("/dispatch/run", str(e), tr)
        return {"ok": False, "error": str(e), "trace": tr}


@app.post("/enrich-task/run")
async def run_enrich_task(authorization: str = Header(default="")):
    """每 5 分钟扫 KOL 营销任务台 待触发任务 → 富化打分 + 生草稿 + 调 reviewer"""
    _check_auth(authorization)
    try:
        result = await enrich.run()
        return {"ok": True, **result}
    except Exception as e:
        tr = _tb.format_exc()[-1500:]
        await _alert_endpoint_failure("/enrich-task/run", str(e), tr)
        return {"ok": False, "error": str(e), "trace": tr}


@app.post("/enrich-task-editor/run")
async def run_enrich_task_editor(authorization: str = Header(default="")):
    """每 5 分钟扫 媒体人营销任务台 待触发任务 → score_editor 6 维 + DeepSeek 生 PR pitch + 调 reviewer"""
    _check_auth(authorization)
    try:
        result = await enrich_editor.run()
        return {"ok": True, **result}
    except Exception as e:
        tr = _tb.format_exc()[-1500:]
        await _alert_endpoint_failure("/enrich-task-editor/run", str(e), tr)
        return {"ok": False, "error": str(e), "trace": tr}


@app.post("/kol-keyword-cron/run")
async def run_kol_keyword_cron(authorization: str = Header(default="")):
    """周一/四 09:00 BJ 自动从词库抽 5 关键词建 YT 爬虫任务 (KOL 持续开发)"""
    _check_auth(authorization)
    try:
        result = await keyword_cron.run()
        return result
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
        tr = _tb.format_exc()[-1000:]
        await _alert_endpoint_failure("/auto-send/run", str(e), tr)
        return {"ok": False, "error": str(e), "trace": tr}


@app.post("/zoho/send-debug")
async def zoho_send_debug(authorization: str = Header(default=""),
                          brand: str = "白牌", to: str = "frankiepan501@gmail.com"):
    """诊断(只发1封): 抓 Zoho 发送的真实 status+body(看 500 具体原因)"""
    _check_auth(authorization)
    from . import zoho
    try:
        return {"ok": True, "brand": brand, **(await zoho.raw_send_probe(brand, to))}
    except Exception as e:
        return {"ok": False, "error": str(e), "trace": _tb.format_exc()[-500:]}


@app.get("/zoho/accounts")
async def zoho_accounts(authorization: str = Header(default=""), brand: str = "白牌"):
    """诊断(只读): 列该 brand Zoho token 可访问账号 + 合法发件地址, 排查发送500。
    对比 config 的 account_id / alias_from 是否与 Zoho 实际一致。"""
    _check_auth(authorization)
    from . import zoho
    try:
        accts = await zoho.list_accounts(brand)
        cfg = config.BRAND_CONFIG.get(brand, {})
        out = []
        for a in accts:
            out.append({
                "accountId": a.get("accountId"),
                "primaryEmailAddress": a.get("primaryEmailAddress"),
                "accountDisplayName": a.get("accountDisplayName"),
                "incomingBlocked": a.get("incomingBlocked"),
                "outgoingBlocked": a.get("outgoingBlocked"),
                "sendMailDetails": [{"fromAddress": s.get("fromAddress"),
                                     "displayName": s.get("displayName"),
                                     "validated": s.get("validated"),
                                     "default": s.get("sendMailId") and s.get("default")}
                                    for s in (a.get("sendMailDetails") or [])],
            })
        return {"ok": True, "brand": brand,
                "config": {"account_id": cfg.get("account_id"), "alias_from": cfg.get("alias_from")},
                "zoho_accounts": out}
    except Exception as e:
        return {"ok": False, "error": str(e), "trace": _tb.format_exc()[-500:]}


@app.post("/negotiation-stall/run")
async def run_negotiation_stall(authorization: str = Header(default=""), dry_run: bool = False,
                                max_days: int = None):
    """洽谈中 stall 自动检测 (2026-06-18 weekly cron): 找回复后冷下来的温线索发卡给运营人工跟进。
    ?dry_run=true 只算不发卡; ?max_days=0 列全部(首张卡), 不传=用 60 天上限(死线索不列)。"""
    _check_auth(authorization)
    from . import negotiation_stall
    try:
        result = await negotiation_stall.run(dry_run=dry_run, max_days=max_days)
        return {"ok": True, **result}
    except Exception as e:
        tr = _tb.format_exc()[-1000:]
        await _alert_endpoint_failure("/negotiation-stall/run", str(e), tr)
        return {"ok": False, "error": str(e), "trace": tr}


@app.get("/auto-send/status")
async def auto_send_status(authorization: str = Header(default="")):
    """查发送通道暂停状态 + 限速闸配置 (2026-06-17; 验证 env / 监控用)"""
    _check_auth(authorization)
    import os as _os
    _dry = (_os.environ.get("EMAIL_DRY_RUN_TO", "") or "").strip()
    return {"ok": True, **auto_send.pause_state(),
            "dry_run_active": bool(_dry), "dry_run_to": _dry or None,
            "caps": {"RATE_PER_RUN": auto_send.RATE_PER_RUN,
                     "PER_BRAND_PER_RUN": auto_send.PER_BRAND_PER_RUN,
                     "SEND_DAILY_CAP": auto_send.SEND_DAILY_CAP,
                     "REPLY_RESERVE": auto_send.REPLY_RESERVE,
                     "PAUSE_THRESHOLD": auto_send.PAUSE_THRESHOLD}}


@app.post("/auto-send/resume")
async def resume_auto_send(authorization: str = Header(default=""), brand: str = ""):
    """人工解除自动暂停 (确认 Zoho 可发后调; ?brand=POWKONG 只解该品牌, 不传解全部)"""
    _check_auth(authorization)
    auto_send.clear_pause(brand or None)
    return {"ok": True, "resumed": brand or "ALL", **auto_send.pause_state()}


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


@app.post("/sales-attribution/run")
async def run_sales_attribution(authorization: str = Header(default="")):
    """Phase 3 ROI 闭环: Shopify 双店拉订单 + UTM 归因 + 写飞书 KOL 主表"""
    _check_auth(authorization)
    from . import sales_attribution
    try:
        return await sales_attribution.run()
    except Exception as e:
        import traceback
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()[-1000:]}


@app.get("/amazon/oauth/start")
async def amazon_oauth_start(secret: str = Query(default="")):
    """一次性: 返回 LWA 授权 URL (点开授权 → 回调拿 refresh_token)。secret=INTERNAL_TOKEN 防滥用。
    前置: 已配 AMZ_ADS_CLIENT_ID + 在 LWA 应用注册了 redirect_uri (= AMZ_OAUTH_REDIRECT_URI)。"""
    if secret != config.INTERNAL_TOKEN:
        raise HTTPException(401, "bad secret")
    from urllib.parse import urlencode
    cid = config.AMZ_ADS_CLIENT_ID
    if not cid:
        return {"ok": False, "error": "AMZ_ADS_CLIENT_ID 未配 (先建 LWA 应用并配 client_id/secret)"}
    state = (config.INTERNAL_TOKEN or "x")[:16]
    q = urlencode({
        "client_id": cid,
        "scope": "profile advertising::campaign_management",  # profile=拿 /v2/profiles, 缺则 bad-scope
        "response_type": "code",
        "redirect_uri": config.AMZ_OAUTH_REDIRECT_URI,
        "state": state,
    })
    return {"ok": True, "authorize_url": f"https://www.amazon.com/ap/oa?{q}",
            "redirect_uri": config.AMZ_OAUTH_REDIRECT_URI,
            "note": "登录申请 Amazon Ads API 的那个 Amazon 账号 → 授权 → 回调会把 refresh_token 写进服务日志"}


@app.get("/amazon/oauth/callback")
async def amazon_oauth_callback(code: str = Query(default=""), state: str = Query(default=""),
                                error: str = Query(default="")):
    """LWA 授权回调: code → refresh_token。⚠️ 不在浏览器明文回显 token (防泄漏),
    完整 refresh_token 写进**服务日志** (Zeabur runtime logs), 由开发者取后配进 AMZ_ADS_REFRESH_TOKEN env。"""
    if error:
        return HTMLResponse(f"<h3>授权失败</h3><p>{error}</p>", status_code=400)
    expect_state = (config.INTERNAL_TOKEN or "x")[:16]
    if state != expect_state:
        return HTMLResponse("<h3>state 不匹配, 拒绝</h3>", status_code=403)
    if not code:
        return HTMLResponse("<h3>缺 code</h3>", status_code=400)
    from . import amazon_attribution
    try:
        d = await amazon_attribution.exchange_code_for_refresh_token(
            code, config.AMZ_OAUTH_REDIRECT_URI)
    except Exception as e:
        return HTMLResponse(f"<h3>换 token 异常</h3><p>{str(e)[:300]}</p>", status_code=500)
    rt = d.get("refresh_token")
    if not rt:
        print(f"[amazon_oauth] exchange FAIL: {d}")
        return HTMLResponse(f"<h3>换 token 失败</h3><pre>{d}</pre>", status_code=400)
    # 完整 token 进服务日志 + 写容器临时文件 (供 /amazon/oauth/peek bearer 取); 浏览器只回掩码
    print(f"[amazon_oauth] REFRESH_TOKEN_OK len={len(rt)} value={rt}")
    try:
        with open("/tmp/amz_rt.txt", "w") as _fh:
            _fh.write(rt)
    except Exception as _e:
        print(f"[amazon_oauth] write /tmp/amz_rt.txt fail: {_e}")
    masked = rt[:8] + "..." + rt[-6:]
    return HTMLResponse(
        "<h3>✅ 授权成功</h3>"
        f"<p>refresh_token 已写入服务日志 (掩码 {masked})。</p>"
        "<p>请通知开发者从 Zeabur 日志取出, 配进 AMZ_ADS_REFRESH_TOKEN env。此页可关闭。</p>")


@app.get("/amazon/oauth/peek")
async def amazon_oauth_peek(authorization: str = Header(default="")):
    """一次性: 取出 callback 捕获的 refresh_token (开发者配进 AMZ_ADS_REFRESH_TOKEN env 后即弃)。"""
    _check_auth(authorization)
    import os as _os
    try:
        rt = open("/tmp/amz_rt.txt").read().strip()
    except Exception:
        return {"ok": False, "msg": "无捕获 token (本次部署后重新点一次授权链接)"}
    return {"ok": True, "len": len(rt), "rt": rt}


@app.post("/amazon-attribution/selftest")
async def amazon_attr_selftest(authorization: str = Header(default="")):
    """凭据到位后 smoke: 刷 token + 列 profiles + 列 advertisers (找 US/POWKONG profileId)。不写数据。"""
    _check_auth(authorization)
    from . import amazon_attribution
    try:
        return await amazon_attribution.selftest()
    except Exception as e:
        return {"ok": False, "error": str(e), "trace": _tb.format_exc()[-1000:]}


@app.post("/amazon-ads/sp-ping")
async def amazon_ads_sp_ping(authorization: str = Header(default="")):
    """只读 smoke: 证明 campaign_management scope 能调 SP 广告管理 (POST /sp/campaigns/list)。不写广告。"""
    _check_auth(authorization)
    from . import amazon_attribution
    try:
        return await amazon_attribution.sp_ping()
    except Exception as e:
        return {"ok": False, "error": str(e), "trace": _tb.format_exc()[-1000:]}


@app.post("/card/resend")
async def run_card_resend(authorization: str = Header(default=""),
                          draft_rid: str = "", operator_open_id: str = "",
                          operator_union_id: str = "", dry_run: bool = False):
    """卡片任务看板"📨 回到飞书操作"按钮触发: 撤老卡 + 重发卡到运营私聊底部.
    飞书 applink 不支持跳特定消息(实测+官方文档确认), 改走重发路径."""
    _check_auth(authorization)
    from . import card_resend
    try:
        return await card_resend.run(draft_rid=draft_rid,
                                      operator_open_id=operator_open_id,
                                      operator_union_id=operator_union_id,
                                      dry_run=dry_run)
    except Exception as e:
        import traceback
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()[-1000:]}


@app.post("/card/audit-overdue/run")
async def run_card_audit(authorization: str = Header(default=""),
                          days: float = 1.0, dry_run: bool = False, max_list: int = 10):
    """每日 09:30 BJ cron: 扫 >N 天未处理待办草稿汇总, 发提醒卡给 reviewer + Frankie.
    ?dry_run=true 看会汇总几张不真发; ?days=N 调阈值(默认 1=24h); ?max_list=N 卡里列前 N 张(默认 10)."""
    _check_auth(authorization)
    from . import card_audit
    try:
        return await card_audit.run(days=days, dry_run=dry_run, max_list=max_list)
    except Exception as e:
        import traceback
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()[-1000:]}


@app.post("/draft-status-audit/run")
async def run_draft_status_audit(authorization: str = Header(default=""),
                                 dry_run: bool = True,
                                 auto_fix: bool = False,
                                 notify: bool = False,
                                 notify_report_only: bool = False,
                                 sample_limit: int = 20):
    """草稿状态一致性审计.

    检查: 发送状态=已发/已发送 但 邮件草稿状态!=已发送。
    默认 dry_run=true 只报不写; auto_fix=true 且 dry_run=false 时, 仅把
    空/通过/自动通过 + 有发送时间 的安全记录回填为 已发送。不会触发邮件发送。
    """
    _check_auth(authorization)
    try:
        return {"ok": True, **(await draft_status_audit.run(
            dry_run=dry_run,
            auto_fix=auto_fix,
            notify=notify,
            notify_report_only=notify_report_only,
            sample_limit=sample_limit))}
    except Exception as e:
        tr = _tb.format_exc()[-1000:]
        await _alert_endpoint_failure("/draft-status-audit/run", str(e), tr)
        return {"ok": False, "error": str(e), "trace": tr}


@app.post("/draft-duplicate-audit/run")
async def run_draft_duplicate_audit(authorization: str = Header(default=""),
                                    dry_run: bool = True,
                                    auto_fix: bool = False,
                                    notify: bool = False,
                                    notify_report_only: bool = False,
                                    sample_limit: int = 20):
    """重复草稿审计.

    检查: 同一邮件草稿ID重复, 或 cold/followup 同一联系人×产品×品牌重复。
    默认 dry_run=true 只报不写; auto_fix=true 且 dry_run=false 时, 仅把多余的
    通过/自动通过 + 未发 草稿改成 已否决。不会触发邮件发送。
    """
    _check_auth(authorization)
    try:
        return {"ok": True, **(await draft_duplicate_audit.run(
            dry_run=dry_run,
            auto_fix=auto_fix,
            notify=notify,
            notify_report_only=notify_report_only,
            sample_limit=sample_limit))}
    except Exception as e:
        tr = _tb.format_exc()[-1000:]
        await _alert_endpoint_failure("/draft-duplicate-audit/run", str(e), tr)
        return {"ok": False, "error": str(e), "trace": tr}


@app.post("/kol-audit/digest/run")
async def run_kol_audit_digest(authorization: str = Header(default=""),
                               dry_run: bool = False,
                               auto_fix: bool = True,
                               notify: bool = True,
                               notify_clean: bool = False,
                               notify_report_only: bool = False,
                               sample_limit: int = 5):
    """KOL 发信链统一审计摘要.

    同时跑草稿状态一致性 + 重复草稿审计。默认只在有异常/自动修复/写入失败时发一张卡；
    无异常静默。不会触发邮件发送。
    """
    _check_auth(authorization)
    try:
        return {"ok": True, **(await kol_audit_digest.run(
            dry_run=dry_run,
            auto_fix=auto_fix,
            notify=notify,
            notify_clean=notify_clean,
            notify_report_only=notify_report_only,
            sample_limit=sample_limit))}
    except Exception as e:
        tr = _tb.format_exc()[-1000:]
        await _alert_endpoint_failure("/kol-audit/digest/run", str(e), tr)
        return {"ok": False, "error": str(e), "trace": tr}


@app.post("/manual-send-recon/run")
async def run_manual_send_recon(authorization: str = Header(default=""), dry_run: bool = False):
    """手动发送补登记: 扫 Zoho 发件箱, 对池内'发过但无草稿'联系人补建已发送草稿+建联中+跟进记录.
    解决手动发→无草稿→reply_monitor 跳过回复的盲区(Scott Stein 根因)。?dry_run=true 只列不写。纯读 Zoho 写 bitable 不发邮件。"""
    _check_auth(authorization)
    from . import manual_send_recon
    try:
        return {"ok": True, **(await manual_send_recon.run(dry_run=dry_run))}
    except Exception as e:
        import traceback
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()[-1000:]}


@app.post("/completion-report/run")
async def run_completion_report(authorization: str = Header(default=""), dry_run: bool = False):
    """KOL 任务完成情况周报: 漏斗转化 + 5 类终态分布 + 卡点清单 → 飞书运营群 + Frankie 私聊.
    终态: 成功=已上稿/无回应=末次发信+14d/寄样未产出=签收+60d。纯读不发邮件不写主表。?dry_run=true 只算不发卡。"""
    _check_auth(authorization)
    from . import completion_report
    try:
        return {"ok": True, **(await completion_report.run(dry_run=dry_run))}
    except Exception as e:
        import traceback
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()[-1000:]}


@app.post("/upload-register/scan")
async def run_upload_register(authorization: str = Header(default=""), dry_run: bool = False):
    """上稿登记卡: 扫已寄样+上稿日期空+未近期发卡的 KOL → 发 form 卡给运营登记上稿链接.
    补「上稿日期」数据 hygiene 缺口 (解锁 ROI/decision_feedback)。纯写主表不发邮件。
    ?dry_run=true 只列候选不发卡。"""
    _check_auth(authorization)
    from . import upload_register
    try:
        return {"ok": True, **(await upload_register.run(dry_run=dry_run))}
    except Exception as e:
        import traceback
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()[-1000:]}


@app.get("/card/resend-from-button")
async def resend_from_button(draft_rid: str = "", secret: str = ""):
    """飞书 bitable 按钮"打开链接"触发: 拉草稿「关联运营」每人重发卡.
    无 Bearer auth(浏览器 GET 不便带 header), 用 query secret 校验.
    返回小 HTML 自动 close 2.5s, 运营回飞书私聊看新卡."""
    expected_secret = os.environ.get("RESEND_BUTTON_SECRET", "")
    if not draft_rid or not secret or not expected_secret or secret != expected_secret:
        return HTMLResponse("<h3>❌ 参数错误或未授权</h3>", status_code=400)
    from . import card_resend, feishu
    try:
        # user_id_type=union_id 让 User 字段 id 返 union_id (跟 write 侧一致)
        path = (f"/bitable/v1/apps/{config.FEISHU_APP_TOKEN}/tables/{config.T_DRAFT}"
                f"/records/{draft_rid}?user_id_type=union_id")
        r = await feishu.api("GET", path)
        f = (r.get("data") or {}).get("record", {}).get("fields", {})
        union_ids = []
        lr = f.get("关联运营")
        if isinstance(lr, list):
            for u in lr:
                if isinstance(u, dict):
                    uid = u.get("id") or ""
                    if uid:
                        union_ids.append(uid)
        ok_count = 0
        details = []
        if union_ids:
            # 新草稿: 「关联运营」已填, 按 union_id 重发给原运营
            for uid in union_ids:
                res = await card_resend.run(draft_rid=draft_rid, operator_union_id=uid)
                if res.get("ok"):
                    ok_count += 1
                details.append(f"{uid[:12]}: {res.get('msg') or 'ok'}")
            target_n = len(union_ids)
        else:
            # 老草稿(retrofit 前)「关联运营」为空 → fallback 到当前在职独立站运营专员
            # (resolve_notify_targets 职务实时查, turnover-safe), 用 open_id 重发
            targets = await feishu.resolve_notify_targets("reviewer")
            if not targets:
                return HTMLResponse(
                    "<h3>⚠️ 此草稿无关联运营, 且未查到在职运营</h3>"
                    "<p>解决: 人工去草稿表打开, 或联系 Frankie 检查职务配置。</p>",
                    status_code=200)
            for name, oid in targets:
                res = await card_resend.run(draft_rid=draft_rid, operator_open_id=oid)
                if res.get("ok"):
                    ok_count += 1
                details.append(f"{name}: {res.get('msg') or 'ok'}")
            target_n = len(targets)
        if ok_count == 0:
            # 一张都没重发(通常=草稿已终态/已处理过) → 不自动关, 显式说明原因, 防"以为坏了"
            html = f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>重发</title></head>
<body style="font:16px/1.6 system-ui,sans-serif;padding:40px;text-align:center;color:#333;">
<div style="font-size:48px;">ℹ️</div>
<h2>这张卡无需重发</h2>
<p>多半是该草稿已处理过(已发送/已否决/已通过)，看板上是旧行。</p>
<p style="color:#999;font-size:13px;">原因: {"<br>".join(details) or "无可重发对象"}</p>
<p style="color:#bbb;font-size:12px;">(此页不会自动关闭，看完手动关即可)</p>
</body></html>"""
            return HTMLResponse(html)
        html = f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>重发</title></head>
<body style="font:16px/1.6 system-ui,sans-serif;padding:40px;text-align:center;color:#333;">
<div style="font-size:48px;">✅</div>
<h2>已重发卡片 {ok_count}/{target_n} 位运营</h2>
<p>请回飞书私聊底部查看新卡</p>
<p style="color:#999;font-size:12px;">{"<br>".join(details)}</p>
<script>setTimeout(()=>window.close(),2500);</script>
</body></html>"""
        return HTMLResponse(html)
    except Exception as e:
        import traceback
        return HTMLResponse(
            f"<h3>❌ 错误</h3><pre>{str(e)[:200]}</pre>",
            status_code=500)


@app.post("/upload-task-report/run")
async def run_upload_task_report(authorization: str = Header(default=""), dry_run: bool = False,
                                 notify: bool = True, frankie_only: bool = False):
    """KOL 上稿×任务进度 周报(按产品): 飞书卡片 digest + 写留档表.
    ?dry_run=true 不写留档表; ?notify=false 不发卡; ?frankie_only=true 卡只发 Frankie 不进群(审格式用)."""
    _check_auth(authorization)
    from . import upload_task_report
    try:
        return await upload_task_report.run(dry_run=dry_run, notify=notify, frankie_only=frankie_only)
    except Exception as e:
        import traceback
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()[-1200:]}


@app.post("/decision-feedback/run")
async def run_decision_feedback(authorization: str = Header(default="")):
    """Phase 3.2 决策反哺: 据 GMV/订单/上稿 自动升降级 KOL 合作状态"""
    _check_auth(authorization)
    from . import decision_feedback
    try:
        return await decision_feedback.run()
    except Exception as e:
        import traceback
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()[-1000:]}


@app.post("/secondary-outreach/run")
async def run_secondary_outreach(authorization: str = Header(default=""),
                                   limit: int = 0, async_mode: bool = True):
    """Phase 3.3 二次维护: 给已合作 KOL 自动生新产品 warm follow-up.
    - ?limit=5 仅跑前 5 (smoke test)
    - ?async_mode=true (默认) 立即 ack 后台跑 (避开 Zeabur 165s 网关 timeout)
    - ?async_mode=false 同步等结果 (仅 limit ≤ 10 时用)"""
    _check_auth(authorization)
    from . import secondary_outreach
    if async_mode:
        import asyncio
        asyncio.create_task(secondary_outreach.run(limit=limit))
        return {"ok": True, "started": "background", "limit": limit,
                "msg": "查飞书草稿表 邮件草稿来源=secondary_outreach 看进度, ~14sec/KOL"}
    try:
        return await secondary_outreach.run(limit=limit)
    except Exception as e:
        import traceback
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()[-1000:]}


@app.post("/relabel/kol-test")
async def run_relabel_kol_test(authorization: str = Header(default=""), limit: int = 10):
    """A 阶段验证: 重打前 N 个 KOL 标签 (基于近期视频标题). D3=c 云端反爬命中率测试."""
    _check_auth(authorization)
    try:
        result = await relabel.run_kol_test(limit=limit)
        return {"ok": True, **result}
    except Exception as e:
        import traceback
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()[-1500:]}


@app.post("/zoho/test-send")
async def zoho_test_send(authorization: str = Header(default=""),
                          brand: str = "POWKONG", to: str = "frankiepan501@gmail.com"):
    """发一封测试邮件验证 Zoho OAuth send_email 链路 (不依赖 folders scope)"""
    _check_auth(authorization)
    from . import zoho
    try:
        msg_id = await zoho.test_send(brand, to)
        return {"ok": True, "msg_id": msg_id, "brand": brand, "to": to}
    except Exception as e:
        import traceback
        return {"ok": False, "error": str(e)[:300], "trace": traceback.format_exc()[-500:]}


@app.post("/zoho/health")
async def zoho_health(authorization: str = Header(default="")):
    """2026-05-17 A7: Zoho OAuth daily smoke test (轻量, 不发邮件).
    验 POWKONG + FUNLAB 各自的 list_folders (需 folders.ALL scope) →
    任一品牌 401 时自动调 _alert_endpoint_failure 发飞书告警给 Frankie.
    建议 n8n cron 每日 09:30 BJ 跑一次, OAuth scope 静默失效从 72h 缩短到 24h 发现.
    """
    _check_auth(authorization)
    from . import zoho
    results = {}
    has_fail = False
    for brand in config.BRAND_CONFIG.keys():   # 2026-06-08 配置驱动: 含白牌健康检查
        try:
            folders = await zoho.list_folders(brand)
            results[brand] = {"ok": True, "folder_count": len(folders)}
        except Exception as e:
            results[brand] = {"ok": False, "error": str(e)[:200]}
            has_fail = True
    if has_fail:
        msg = " | ".join(f"{b}: {r.get('error', 'ok')}" for b, r in results.items() if not r["ok"])
        await _alert_endpoint_failure("/zoho/health", f"Zoho OAuth 失效: {msg}", "")
    return {"ok": not has_fail, "results": results}


@app.post("/kol/dedup/run")
async def run_kol_dedup(authorization: str = Header(default=""), dry_run: bool = False):
    """2026-06-04: KOL 同邮箱去重 gate (周 cron). 保留最有进展记录, 弃用无活动重复(可逆);
    同组有 2+ 活跃记录(有寄样/上稿/已合作)的真冲突 → 跳过+飞书告警 Frankie 人工。?dry_run 只算不写。"""
    _check_auth(authorization)
    try:
        result = await kol_dedup.run(dry_run=dry_run)
        return {"ok": True, "dry_run": dry_run, **result}
    except Exception as e:
        tr = _tb.format_exc()[-800:]
        await _alert_endpoint_failure("/kol/dedup/run", str(e), tr)
        return {"ok": False, "error": str(e), "trace": tr}


@app.post("/deepseek/balance-check")
async def deepseek_balance_check(authorization: str = Header(default="")):
    """2026-06-04: DeepSeek 余额预警 (dead-man-switch). 查余额, 低于阈值或不可用 → 飞书告警 Frankie.
    根因: DeepSeek 欠费 → enrich/reply_drafter/regen/talking_points 全 402 静默停摆(张佳烨 6/4 踩到)。
    建议 n8n cron 每日 09:00 BJ 跑一次。纯监控不发邮件。"""
    _check_auth(authorization)
    import httpx
    try:
        async with httpx.AsyncClient(timeout=20.0) as cli:
            r = await cli.get("https://api.deepseek.com/user/balance",
                              headers={"Authorization": f"Bearer {config.DEEPSEEK_API_KEY}"})
            r.raise_for_status()
            d = r.json()
        avail = bool(d.get("is_available"))
        infos = d.get("balance_infos") or [{}]
        bal = float(infos[0].get("total_balance") or 0)
        cur = infos[0].get("currency", "CNY")
        thr = config.DEEPSEEK_BALANCE_ALERT_THRESHOLD
        low = (not avail) or (bal < thr)
        if low:
            card = {
                "header": {"template": "red",
                           "title": {"tag": "plain_text", "content": "🚨 DeepSeek 余额不足 — 请充值"}},
                "elements": [
                    {"tag": "div", "text": {"tag": "lark_md", "content":
                        (f"**当前余额**: {bal} {cur}　**可用**: {'是' if avail else '否 ⚠️'}\n"
                         f"**阈值**: {thr} {cur}\n\n"
                         "⚠️ DeepSeek 是 KOL 全链 AI 生成依赖(冷开发信/回复分类/退回重生/talking points)。"
                         "**余额耗尽会整条静默 402 停摆**。请尽快充值: https://platform.deepseek.com")}},
                ],
            }
            try:
                await feishu.send_card_message("chat_id", config.NOTIFY_CHAT_ID, card, biz="AUDIT")
                for name, oid in config.NOTIFY_USERS:
                    if name.startswith("潘"):  # 只私聊 Frankie (只有他能充值)
                        try: await feishu.send_card_message("open_id", oid, card, biz="AUDIT")
                        except Exception: pass
            except Exception as e:
                print(f"[deepseek-balance] alert send fail: {e}")
        return {"ok": True, "balance": bal, "currency": cur, "available": avail,
                "threshold": thr, "low": low, "alerted": low}
    except Exception as e:
        tr = _tb.format_exc()[-800:]
        await _alert_endpoint_failure("/deepseek/balance-check", str(e), tr)
        return {"ok": False, "error": str(e), "trace": tr}


@app.get("/zoho/sent-check")
async def zoho_sent_check(authorization: str = Header(default=""),
                          brand: str = "POWKONG", to: str = ""):
    """查 Zoho sent folder, 可选过滤 to 邮箱 (调试用)"""
    _check_auth(authorization)
    from . import zoho
    try:
        result = await zoho.list_sent_messages(brand, limit=30)
        if to and "messages" in result:
            filtered = []
            for m in result["messages"]:
                if to.lower() in (m.get("toAddress") or "").lower():
                    filtered.append({
                        "subject": m.get("subject"),
                        "to": m.get("toAddress"),
                        "from": m.get("fromAddress"),
                        "sentDateInGMT": m.get("sentDateInGMT"),
                        "messageId": m.get("messageId"),
                        "summary": (m.get("summary") or "")[:200],
                    })
            return {"ok": True, "matched": len(filtered), "results": filtered}
        return {"ok": True, **result}
    except Exception as e:
        import traceback
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()[-500:]}


@app.post("/sla/check")
async def run_sla_check(authorization: str = Header(default="")):
    """每 6h 扫 ship_confirm 草稿超 24h 未处理 → 升级通知"""
    _check_auth(authorization)
    try:
        result = await sla_check.run()
        return {"ok": True, **result}
    except Exception as e:
        tr = _tb.format_exc()[-1000:]
        await _alert_endpoint_failure("/sla/check", str(e), tr)
        return {"ok": False, "error": str(e), "trace": tr}


@app.post("/draft-cleanup/run")
async def run_draft_cleanup(authorization: str = Header(default=""), days: int = 30):
    """草稿归档清理 (2026-05-27): 删 N 天前的「已否决/发送失败」草稿, 硬保护其他状态.
    dedup 跳过这俩状态→删了不影响防重/ROI. 建议周 cron."""
    _check_auth(authorization)
    try:
        result = await draft_cleanup.run(days=days)
        return {"ok": True, **result}
    except Exception as e:
        tr = _tb.format_exc()[-1000:]
        await _alert_endpoint_failure("/draft-cleanup/run", str(e), tr)
        return {"ok": False, "error": str(e), "trace": tr}


@app.post("/ship-recon/run")
async def run_ship_recon(authorization: str = Header(default="")):
    """寄样状态对账 (2026-05-22 C): 用 Zoho 发件箱 ground truth 核对 bitable 寄样阶段,
    自动回填"发了但卡待发货"的草稿. 纯状态字段写, 不发邮件. 建议日 cron."""
    _check_auth(authorization)
    try:
        result = await ship_recon.run()
        return {"ok": True, **result}
    except Exception as e:
        tr = _tb.format_exc()[-1000:]
        await _alert_endpoint_failure("/ship-recon/run", str(e), tr)
        return {"ok": False, "error": str(e), "trace": tr}


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
from .reply_drafter import _classify_interest, _gen_general_interest_draft, _gen_quote_draft, _gen_clarify_draft, _gen_misspoke_apology_draft
from .reply_drafter import (
    TEMPLATE_UNSUBSCRIBE, TEMPLATE_DECLINE, TEMPLATE_SEND_ASSETS,
    TEMPLATE_SHIP_CONFIRM, TEMPLATE_NEED_ADDRESS, TEMPLATE_AFFILIATE_UPSELL,
    TEMPLATE_AFFILIATE_INVITATION_QUOTE, TEMPLATE_SCHEDULE_CALL, CALENDLY_DEFAULT,
    _first_name, _sender_signature,
)


@app.post("/reply-drafter/backfill")
async def backfill_reply_for_existing(
    authorization: str = Header(default=""),
    record_id: str = "",
):
    """对已有回复但未生成草稿的旧记录,补跑 reply_drafter pipeline.
    用于在新 reply_drafter 上线前回过的邮件做 backfill.

    流程: 读草稿 → 提取回复内容 → OOO 检测 → 调 reply_drafter.draft_reply()
    """
    _check_auth(authorization)
    if not record_id:
        return {"ok": False, "error": "missing record_id"}

    from . import feishu, reply_monitor, reply_drafter
    from .feishu import ext, xrid

    try:
        rec = await feishu.get_record(config.T_DRAFT, record_id)
        f = rec["fields"]

        if not f.get("是否回复"):
            return {"ok": False, "error": "this draft has no reply yet (是否回复=False)"}

        intent_type = ext(f.get("回复意图")) or "不明意图"
        original_body = ext(f.get("回复原文")) or ""
        original_subject = ext(f.get("邮件主题")) or ""
        sender_alias = ext(f.get("发送邮箱")) or ""

        # OOO 检测 (跟 reply_monitor 一致)
        ooo_hit, ooo_match = reply_monitor.is_ooo(original_subject, original_body)
        if ooo_hit:
            return {"ok": True, "skipped": "OOO_AUTO_REPLY", "ooo_match": ooo_match,
                    "msg": "OOO 自动回复 - 不生成草稿"}

        # 推断品牌
        if "powkong" in sender_alias.lower():
            brand = "POWKONG"
        else:
            brand = "FUNLAB"

        # 找联系人 (草稿关联 KOL 或 媒体人)
        contact_record = None
        contact_type = None
        editor_rid = xrid(f.get("关联媒体人"))
        if editor_rid:
            contact_record = await feishu.get_record(config.T_EDITOR, editor_rid)
            contact_type = "editor"
        else:
            kol_rid = xrid(f.get("关联KOL"))
            if kol_rid:
                contact_record = await feishu.get_record(config.T_KOL, kol_rid)
                contact_type = "KOL"

        if not contact_record:
            return {"ok": False, "error": "no linked contact (no 关联媒体人 or 关联KOL)"}

        # 调 reply_drafter
        new_rid = await reply_drafter.draft_reply(
            contact_record=contact_record,
            contact_type=contact_type,
            brand=brand,
            intent_type=intent_type,
            intent_summary=f"[Backfill] 历史已分类意图: {intent_type}",
            original_subject=original_subject,
            original_body=original_body,
            sender_alias=sender_alias,
            related_draft_id=record_id,
        )
        return {"ok": True, "new_draft_rid": new_rid, "intent_type": intent_type,
                "contact_type": contact_type, "brand": brand}
    except Exception as e:
        import traceback
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()[-700:]}


@app.post("/reply-drafter/dry-run")
async def reply_drafter_dry_run(authorization: str = Header(default=""),
                                  payload: dict = Body(default={})):
    """Dry-run 测试 reply_drafter — 给输入回复, 返回生成的草稿 (不写飞书)
    Payload:
      {
        "intent_type": "感兴趣|要报价|委婉拒绝|退订|质疑/澄清|不明意图",
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
    product_link_raw = p.get("product_link", "")
    # Phase 1 ROI: dry-run 也注 UTM (与生产路径一致)
    from .utm import make_utm_link as _make_utm
    product_link = _make_utm(product_link_raw, brand, product_name, contact_name) if product_link_raw else ""

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
        elif sub_info["sub"] == "need_address":
            body = TEMPLATE_NEED_ADDRESS.format(first_name=first, signature=sig_full,
                                                  product_name=product_name)
        elif sub_info["sub"] == "short_only" or sub_info["sub"] == "affiliate_upsell":
            # dry-run 端点不知道 KOL 主平台,默认 affiliate_upsell 模板 (生产路径在 reply_drafter
            # 主流程会用 KOL 主平台判断 YT vs 其他, 这里仅用于运营预览模板效果)
            body = TEMPLATE_AFFILIATE_UPSELL.format(first_name=first, signature=sig_full,
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
        # P5.11: 改用 affiliate_invitation 固定模板,不再 DeepSeek 自由生成
        # dry-run 默认 product_price=50,生产路径从产品库报价(USD)取
        product_price = p.get("product_price", 50)
        price_str = f"{int(product_price)}" if product_price else "TBD"
        subj = "Re: " + original_subject[:150]
        body = TEMPLATE_AFFILIATE_INVITATION_QUOTE.format(
            first_name=first, signature=sig_full,
            product_name=product_name, product_price=price_str,
        )
    elif intent_type == "质疑/澄清":
        d = await _gen_misspoke_apology_draft(contact_name, original_subject, original_body,
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
    # 质疑/澄清 强制 committed=True (Ashtvn 反例 — KOL 在打脸我们)
    if intent_type == "质疑/澄清":
        committed = True
        forced_commit = True
    # 同步生产 router 强制人审规则: 不明意图 / 要报价 / affiliate_upsell / short_only
    # 防 dry-run endpoint 显示与生产路径不一致 (避免再误导调试)
    if intent_type in ("不明意图", "要报价"):
        committed = True
        forced_commit = True
    if sub_info and sub_info.get("sub") in ("affiliate_upsell", "short_only"):
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


@app.post("/weekly-report/run")
async def run_weekly_report(authorization: str = Header(default=""),
                              dry_run: bool = False,
                              async_mode: bool = True):
    """每周一 08:00 BJ 触发: 双品牌运营周报全自动生成 (12 sections + Lighthouse + 双框架 KPI).

    设计方案: https://u1wpma3xuhr.feishu.cn/wiki/QeQMw2peBiJcIdkKBI2c1tBbnLe
    - ?dry_run=true 跳过飞书发布, 返回 markdown 预览 + 缺口列表
    - ?async_mode=true (默认) fire-and-forget 后台跑 (避开 Zeabur 165s 网关 timeout)
    - ?async_mode=false 同步等结果 (仅 dry_run 调试用)
    """
    _check_auth(authorization)
    if async_mode and not dry_run:
        asyncio.create_task(weekly_report.main.run(dry_run=False))
        return {"ok": True, "started": "background",
                "msg": "weekly report run started, will push to feishu when done (~60s)"}
    try:
        result = await weekly_report.main.run(dry_run=dry_run)
        return {"ok": True, **result}
    except Exception as e:
        import traceback
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()[-1000:]}


@app.post("/weekly-report/dry-run")
async def weekly_report_dry_run(authorization: str = Header(default="")):
    """显式 dry-run 接口: 同步跑 collectors + integrator + renderer, 不发飞书. 用于调试."""
    _check_auth(authorization)
    try:
        result = await weekly_report.main.run(dry_run=True)
        return {"ok": True, **result}
    except Exception as e:
        import traceback
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()[-1500:]}

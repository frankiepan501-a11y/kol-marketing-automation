"""Zoho 客服邮箱客诉 collector.

⭐ 双框架核心 KPI - 客诉判断 SOP 改造效果跟踪.

数据源: Zoho Mail API (service@ 客服邮箱, 独立于现有 partner@ 营销邮箱)
- service@powkong.com (env ZOHO_PK_SERVICE_REFRESH_TOKEN)
- service@funlabswitch.com (env ZOHO_FL_SERVICE_REFRESH_TOKEN)

⚠️ Phase 2 上线前提: Frankie 需配 service@ 邮箱独立 OAuth (与 partner@ 不同 refresh token)
   未配前 collector 返回 status=error + 数据缺口段, 不影响其他 collectors

输出 collected.complaints.data:
{
  "powkong": {
    "total": int,           # 本周客诉邮件数
    "resolved": int,
    "pending": int,
    "avg_first_response_hours": float,
    "by_type": {            # DeepSeek 自动分类
      "物流问题": int, "产品问题": int, "退换货请求": int,
      "售后咨询": int, "其他": int,
    },
    "frankie_escalated": int,  # ⭐ Frankie 升级率信号 (邮件含 escalation 标签或转发给 frankie@)
    "escalation_rate": float,   # frankie_escalated / total
  },
  "funlab": {同上},
}
"""
import asyncio
import datetime
import json
import logging
import os

import httpx

log = logging.getLogger("weekly_report.zoho_complaints")

ZOHO_REGION = os.environ.get("ZOHO_REGION", ".com")  # .com / .com.cn / .eu
TIMEOUT = 30.0


def _get_creds(brand: str) -> dict:
    """读 service@ OAuth 凭证. 与 partner@ 独立."""
    if brand == "POWKONG":
        cid = os.environ.get("ZOHO_PK_SERVICE_CLIENT_ID") or os.environ.get("ZOHO_POWKONG_CLIENT_ID")
        secret = os.environ.get("ZOHO_PK_SERVICE_CLIENT_SECRET") or os.environ.get("ZOHO_POWKONG_CLIENT_SECRET")
        rtok = os.environ.get("ZOHO_PK_SERVICE_REFRESH_TOKEN")
        acct = os.environ.get("ZOHO_PK_SERVICE_ACCOUNT_ID")
        alias = os.environ.get("ZOHO_PK_SERVICE_ALIAS", "service@powkong.com")
    else:
        cid = os.environ.get("ZOHO_FL_SERVICE_CLIENT_ID") or os.environ.get("ZOHO_FUNLAB_CLIENT_ID")
        secret = os.environ.get("ZOHO_FL_SERVICE_CLIENT_SECRET") or os.environ.get("ZOHO_FUNLAB_CLIENT_SECRET")
        rtok = os.environ.get("ZOHO_FL_SERVICE_REFRESH_TOKEN")
        acct = os.environ.get("ZOHO_FL_SERVICE_ACCOUNT_ID")
        alias = os.environ.get("ZOHO_FL_SERVICE_ALIAS", "service@funlabswitch.com")
    if not all([cid, secret, rtok, acct]):
        raise RuntimeError(f"Zoho service@ env 未配齐 (brand={brand}). "
                           "需要: ZOHO_{PK,FL}_SERVICE_REFRESH_TOKEN + ACCOUNT_ID + (CLIENT_ID/SECRET 可复用 partner)")
    return {"client_id": cid, "client_secret": secret, "refresh_token": rtok,
            "account_id": acct, "alias": alias}


async def _refresh_access(creds: dict) -> str:
    async with httpx.AsyncClient(timeout=TIMEOUT) as cli:
        r = await cli.post(f"https://accounts.zoho{ZOHO_REGION}/oauth/v2/token",
                           data={"refresh_token": creds["refresh_token"],
                                 "client_id": creds["client_id"],
                                 "client_secret": creds["client_secret"],
                                 "grant_type": "refresh_token"})
        r.raise_for_status()
        return r.json()["access_token"]


async def _search_inbox(creds: dict, search_key: str, limit: int = 200, sort_after_ms: int = 0):
    """Zoho Mail messages search.

    Zoho /messages/search 的 searchKey 用 RFC3164 风格: subject:xxx OR from:xxx ... .
    返回里 receivedTime 是 epoch ms.
    """
    tok = await _refresh_access(creds)
    url = f"https://mail.zoho{ZOHO_REGION}/api/accounts/{creds['account_id']}/messages/search"
    params = {"searchKey": search_key, "limit": str(limit), "start": "1"}
    async with httpx.AsyncClient(timeout=TIMEOUT) as cli:
        r = await cli.get(url, headers={"Authorization": f"Zoho-oauthtoken {tok}"},
                           params=params)
        r.raise_for_status()
        data = r.json()
    msgs = data.get("data") or []
    if sort_after_ms:
        msgs = [m for m in msgs if int(m.get("receivedTime") or 0) >= sort_after_ms]
    return msgs


async def _classify_complaints(messages: list) -> dict:
    """用 DeepSeek 把邮件主题/正文摘要分到 5 类.

    Phase 2.1 简化版: 用关键词字典初步分类 (不调 DeepSeek 节省成本 + 减少超时).
    Phase 2.2 升级: 改成 DeepSeek batch 分类 (一次性 prompt 含全部 messages, 返回 JSON 数组).
    """
    KEYWORDS = {
        "物流问题": ["shipping", "delivery", "delayed", "lost", "tracking", "carrier", "package", "freight",
                     "物流", "快递", "派送", "丢失", "签收"],
        "产品问题": ["broken", "defect", "not working", "quality", "wrong item", "missing", "damaged",
                     "故障", "质量", "损坏", "不能用"],
        "退换货请求": ["refund", "return", "exchange", "cancel order", "money back",
                       "退款", "退货", "换货", "取消订单"],
        "售后咨询": ["question", "how to", "warranty", "support", "inquiry",
                     "咨询", "怎么", "保修", "请问"],
    }

    by_type = {k: 0 for k in KEYWORDS}
    by_type["其他"] = 0
    for m in messages:
        text = ((m.get("subject") or "") + " " + (m.get("summary") or "")).lower()
        matched = False
        for tname, kws in KEYWORDS.items():
            if any(kw in text for kw in kws):
                by_type[tname] += 1
                matched = True
                break
        if not matched:
            by_type["其他"] += 1
    return by_type


def _avg_first_response_hours(messages: list) -> float:
    """简化估算: 每封含 receivedTime 的邮件假设 next 回复时间 = sentTime (Zoho 不直接给 thread 数据).

    实际上需要 Zoho /threads/<thread_id>/messages 拉 thread 内每条 + 比对入站/出站. 简化为 stub.
    Phase 3 升级: 真按 thread 算.
    """
    return 0.0  # placeholder


def _count_escalated(messages: list) -> int:
    """Frankie 升级 = 邮件 cc / forwarded 到 frankiepan501@ 或主题含 [Escalation]."""
    cnt = 0
    for m in messages:
        text = ((m.get("subject") or "") + " " + (m.get("toAddress") or "") + " " +
                (m.get("ccAddress") or "")).lower()
        if "frankiepan" in text or "[escalation]" in text or "升级" in text:
            cnt += 1
    return cnt


async def _fetch_brand(brand: str, start_date, end_date) -> dict:
    creds = _get_creds(brand)
    start_ms = int(datetime.datetime.combine(start_date, datetime.time.min).timestamp() * 1000)
    end_ms = int(datetime.datetime.combine(end_date + datetime.timedelta(days=1), datetime.time.min).timestamp() * 1000)

    # Zoho search filter: 所有 inbox 邮件 (然后内存过滤本周)
    messages = await _search_inbox(creds, search_key="entire:in", limit=200, sort_after_ms=start_ms)
    week_msgs = [m for m in messages if int(m.get("receivedTime") or 0) < end_ms]

    by_type = await _classify_complaints(week_msgs)
    escalated = _count_escalated(week_msgs)
    total = len(week_msgs)

    return {
        "total": total,
        "resolved": 0,    # Phase 3: 按 thread 状态判断
        "pending": total,  # 简化: 全算待跟进
        "avg_first_response_hours": _avg_first_response_hours(week_msgs),
        "by_type": by_type,
        "frankie_escalated": escalated,
        "escalation_rate": round(escalated / max(total, 1), 4),
    }


async def collect(start_date, end_date) -> dict:
    log.info("zoho_complaints.collect %s ~ %s", start_date, end_date)
    try:
        pk, fl = await asyncio.gather(
            _fetch_brand("POWKONG", start_date, end_date),
            _fetch_brand("FUNLAB", start_date, end_date),
            return_exceptions=True,
        )
        if isinstance(pk, Exception):
            log.warning("complaints PK fetch fail: %s", pk)
            pk = {"error": f"{type(pk).__name__}: {pk}"}
        if isinstance(fl, Exception):
            log.warning("complaints FL fetch fail: %s", fl)
            fl = {"error": f"{type(fl).__name__}: {fl}"}
        return {"status": "ok", "data": {"powkong": pk, "funlab": fl,
                                          "window": f"{start_date}~{end_date}"}}
    except Exception as e:
        log.exception("zoho_complaints collect outer fail")
        return {"status": "error", "error": f"{type(e).__name__}: {e}", "data": {}}


if __name__ == "__main__":
    import datetime as dt
    today = dt.date.today()
    last_sun = today - dt.timedelta(days=today.weekday() + 1)
    last_mon = last_sun - dt.timedelta(days=6)
    print(asyncio.run(collect(last_mon, last_sun)))

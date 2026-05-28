# -*- coding: utf-8 -*-
"""P2: Shopify Admin API 自建 KOL 折扣码 (寄样后 brief 重设计).

每个寄样 KOL 一个唯一折扣码(% off), 用于①暖信里给受众优惠 ②sales_attribution 按码→KOL 归因 GMV。
自生成唯一码(handle-based), 不依赖外部联盟工具。鉴权复用 sales_attribution(POWKONG client_credentials / FUNLAB shpat)。

幂等: ensure_kol_discount 先看 KOL/编辑主表「折扣码」, 有则复用不重复建。
"""
import os
import re
import httpx
from . import config, feishu
from .feishu import ext
from .sales_attribution import get_token, get_shop, SHOPIFY_API_VERSION

# KOL 折扣码默认折扣 (受众用); 可用 env KOL_DISCOUNT_PCT 覆盖 (0.15 = 15%)
KOL_DISCOUNT_PCT = float(os.environ.get("KOL_DISCOUNT_PCT", "0.15"))

_CREATE = """
mutation($i: DiscountCodeBasicInput!) {
  discountCodeBasicCreate(basicCodeDiscount: $i) {
    codeDiscountNode { id codeDiscount { ... on DiscountCodeBasic {
      title status codes(first: 1) { nodes { code } } } } }
    userErrors { field message code }
  }
}
"""
_DELETE = "mutation($id: ID!) { discountCodeDelete(id: $id) { deletedCodeDiscountId userErrors { message } } }"


def _sanitize(handle: str, maxlen: int = 12) -> str:
    s = re.sub(r"[^A-Za-z0-9]", "", handle or "").upper()[:maxlen]
    return s or "KOL"


def make_code(handle: str, pct: float = None, suffix: str = "") -> str:
    """KOL handle → 折扣码, 如 'Mario Plays' + 15% → 'MARIOPLAYS15'."""
    p = int(round((pct if pct is not None else KOL_DISCOUNT_PCT) * 100))
    return f"{_sanitize(handle)}{p}{suffix}"


async def _gql(brand: str, query: str, variables: dict = None) -> dict:
    tok = await get_token(brand)
    shop = get_shop(brand)
    async with httpx.AsyncClient(timeout=30.0) as c:
        r = await c.post(
            f"https://{shop}/admin/api/{SHOPIFY_API_VERSION}/graphql.json",
            headers={"X-Shopify-Access-Token": tok, "Content-Type": "application/json"},
            json={"query": query, "variables": variables or {}},
        )
        r.raise_for_status()
        d = r.json()
    if d.get("errors"):
        raise RuntimeError(f"Shopify GraphQL error: {d['errors']}")
    return d["data"]


async def _create_once(brand: str, code: str, pct: float, title: str) -> dict:
    import time
    inp = {
        "title": title or f"KOL {code}",
        "code": code,
        "startsAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "customerSelection": {"all": True},
        "customerGets": {"value": {"percentage": pct}, "items": {"all": True}},
        "appliesOncePerCustomer": True,
    }
    res = (await _gql(brand, _CREATE, {"i": inp}))["discountCodeBasicCreate"]
    errs = res.get("userErrors") or []
    node = res.get("codeDiscountNode") or {}
    return {"errors": errs, "gid": node.get("id"), "code": code}


async def create_discount(brand: str, handle: str, pct: float = None, title: str = None) -> dict:
    """建唯一折扣码; 撞码自动加 2 位后缀重试 (最多 3 次). Returns {ok, code, gid, error}."""
    pct = pct if pct is not None else KOL_DISCOUNT_PCT
    for attempt in range(3):
        suffix = "" if attempt == 0 else str(__import__("random").randint(10, 99))
        code = make_code(handle, pct, suffix)
        r = await _create_once(brand, code, pct, title or f"KOL {handle}")
        if r["gid"]:
            return {"ok": True, "code": r["code"], "gid": r["gid"], "error": None}
        # 撞码 (TAKEN/已存在) → 换后缀重试; 其它错直接返回
        msgs = " ".join((e.get("code", "") + e.get("message", "")) for e in r["errors"]).lower()
        if "taken" not in msgs and "exist" not in msgs:
            return {"ok": False, "code": code, "gid": None, "error": r["errors"]}
    return {"ok": False, "code": code, "gid": None, "error": "code collision after 3 tries"}


async def create_discount_with_code(brand: str, code: str, pct: float, title: str = None) -> dict:
    """用**指定**折扣码建码(运营自定义码). 已存在(taken)视为可用复用. Returns {ok, code, reused, error}."""
    r = await _create_once(brand, code, pct, title or f"KOL {code}")
    if r["gid"]:
        return {"ok": True, "code": code, "reused": False, "error": None}
    msgs = " ".join((e.get("code", "") + e.get("message", "")) for e in r["errors"]).lower()
    if "taken" in msgs or "exist" in msgs:
        return {"ok": True, "code": code, "reused": True, "error": None}  # 已存在=可用
    return {"ok": False, "code": code, "reused": False, "error": r["errors"]}


async def resolve_send_code(brand: str, handle: str, desired_code: str, pct: float) -> dict:
    """发暖信前确定最终折扣码: 运营填了 desired_code→用它(taken 复用); 没填→按 handle+pct 自动生成.
    Returns {ok, code, error}. pct 为小数 (0.15)."""
    dc = (desired_code or "").strip().upper()
    if dc:
        r = await create_discount_with_code(brand, re.sub(r"[^A-Za-z0-9]", "", dc) or dc, pct)
        return {"ok": r["ok"], "code": r.get("code"), "error": r.get("error")}
    r = await create_discount(brand, handle, pct)
    return {"ok": r["ok"], "code": r.get("code"), "error": r.get("error")}


async def delete_discount(brand: str, gid: str) -> dict:
    res = (await _gql(brand, _DELETE, {"id": gid}))["discountCodeDelete"]
    return {"deleted": res.get("deletedCodeDiscountId"), "errors": res.get("userErrors")}


async def ensure_kol_discount(brand: str, contact_rid: str, contact_type: str,
                              handle: str, draft_rid: str = None, pct: float = None) -> dict:
    """幂等: KOL/编辑主表已有「折扣码」则复用; 否则建码 + 写回主表(+草稿). Returns {ok, code, reused, error}."""
    master_tbl = config.T_EDITOR if contact_type == "editor" else config.T_KOL
    try:
        rec = await feishu.get_record(master_tbl, contact_rid)
        existing = ext(rec["fields"].get("折扣码")).strip()
    except Exception as e:
        existing = ""
    if existing:
        if draft_rid:
            try:
                await feishu.update_record(config.T_DRAFT, draft_rid, {"折扣码": existing})
            except Exception:
                pass
        return {"ok": True, "code": existing, "reused": True, "error": None}

    r = await create_discount(brand, handle, pct)
    if not r["ok"]:
        return {"ok": False, "code": r["code"], "reused": False, "error": r["error"]}
    # 写回主表缓存 + 草稿 per-sample
    try:
        await feishu.update_record(master_tbl, contact_rid, {"折扣码": r["code"]})
    except Exception as e:
        print(f"[shopify_discount] 写主表折扣码失败 {contact_rid}: {e}")
    if draft_rid:
        try:
            await feishu.update_record(config.T_DRAFT, draft_rid, {"折扣码": r["code"]})
        except Exception:
            pass
    return {"ok": True, "code": r["code"], "reused": False, "error": None}


async def selftest(brand: str = "FUNLAB") -> dict:
    """云端 smoke: 建一个一次性测试码 → 删除. 验证 env + 鉴权 + GraphQL 通."""
    import time
    code = f"ZZSELFTEST{int(time.time()) % 100000}"
    r = await _create_once(brand, code, 0.05, f"SELFTEST {code}")
    out = {"brand": brand, "test_code": code, "create_errors": r["errors"], "gid": r["gid"]}
    if r["gid"]:
        d = await delete_discount(brand, r["gid"])
        out["deleted"] = d["deleted"]
    return out

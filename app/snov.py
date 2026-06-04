"""Snov.io Email Finder — 编辑(媒体人)真邮箱解析, 替代 {fi}{last}@域名 猜测.

2026-06-04: 编辑邮箱靠猜测大媒体退信 33-66%(根因=编辑流动/邮箱不存在). Snov finder
按 名+域名 返回真实邮箱 + 内联 emailStatus(valid/unknown). 仅用于媒体人 cold.

设计:
- finder 返回 emailStatus=valid → 用真邮箱(可能纠正), 标编辑「邮箱验真状态=有效」让域名守卫放行.
- finder 找到但非 valid(unknown) → 用找到的邮箱发, 退信由 bounce_monitor 回标(Frankie 拍板,
  不增加人工审核). 不改编辑记录(域名守卫照常治理).
- 找不到/API 不可用 → 降级现状(猜测 + 域名守卫). 纯加法 fail-safe.

凭证走 Zeabur env SNOV_CLIENT_ID/SNOV_CLIENT_SECRET, 不硬编(repo 公开).
独立 verifier 端点(get-emails-verification-status)该套餐 403 无权限 → 不用, finder 内联状态即验证.
"""
import time
import httpx

from . import config

_OAUTH = "https://api.snov.io/v1/oauth/access_token"
_FINDER = "https://api.snov.io/v1/get-emails-from-names"

_token = {"value": None, "ts": 0.0}
_TOKEN_TTL = 3000  # JWT exp = iat+3600, 留余量


async def _get_token() -> str:
    if _token["value"] and time.time() - _token["ts"] < _TOKEN_TTL:
        return _token["value"]
    cid = config.SNOV_CLIENT_ID
    secret = config.SNOV_CLIENT_SECRET
    if not cid or not secret:
        raise RuntimeError("SNOV_CLIENT_ID/SECRET 未配置")
    async with httpx.AsyncClient(timeout=30.0) as cli:
        r = await cli.post(_OAUTH, data={
            "grant_type": "client_credentials",
            "client_id": cid,
            "client_secret": secret,
        })
        r.raise_for_status()
        d = r.json()
    tok = d.get("access_token")
    if not tok:
        raise RuntimeError(f"Snov OAuth 无 access_token: {str(d)[:200]}")
    _token["value"] = tok
    _token["ts"] = time.time()
    return tok


def _split_name(full: str):
    """全名拆 first/last. 单 token 无法拆 → None(Snov 需要 first+last+domain)."""
    parts = [p for p in str(full or "").replace(",", " ").split() if p]
    if len(parts) < 2:
        return None, None
    return parts[0], parts[-1]


async def find_email(full_name: str, domain: str) -> dict:
    """按 名+域名 查真邮箱.

    返回 {status, email, raw}:
      status ∈ valid | unknown | not_found | unavailable
      email  = 找到的邮箱(valid/unknown 时); 否则 None
    """
    first, last = _split_name(full_name)
    domain = (domain or "").strip().lower()
    if not first or not last or not domain:
        return {"status": "unavailable", "email": None, "raw": "name/domain 不足"}

    try:
        token = await _get_token()
    except Exception as e:
        return {"status": "unavailable", "email": None, "raw": f"oauth: {str(e)[:120]}"}

    args = {"access_token": token, "firstName": first, "lastName": last, "domain": domain}
    deadline = time.time() + 28
    try:
        async with httpx.AsyncClient(timeout=30.0) as cli:
            # 首次提交 + 轮询 (Snov finder 异步: in_progress → complete)
            d = None
            while time.time() < deadline:
                r = await cli.post(_FINDER, data=args)
                r.raise_for_status()
                d = r.json()
                ident = (d.get("status") or {}).get("identifier")
                if ident == "complete":
                    break
                import asyncio
                await asyncio.sleep(3)
    except Exception as e:
        return {"status": "unavailable", "email": None, "raw": f"finder: {str(e)[:120]}"}

    if not d or (d.get("status") or {}).get("identifier") != "complete":
        return {"status": "unavailable", "email": None, "raw": "finder 超时未 complete"}

    data = d.get("data")
    emails = (data.get("emails") if isinstance(data, dict) else None) or []
    emails = [em for em in emails if isinstance(em, dict) and em.get("email")]
    if not emails:
        return {"status": "not_found", "email": None, "raw": "无匹配邮箱"}

    # 取首个; 优先 valid
    best = None
    for em in emails:
        if str(em.get("emailStatus", "")).lower() == "valid":
            best = em
            break
    if best is None:
        best = emails[0]
    em_status = str(best.get("emailStatus", "")).lower()
    return {
        "status": "valid" if em_status == "valid" else "unknown",
        "email": best.get("email"),
        "raw": em_status,
    }

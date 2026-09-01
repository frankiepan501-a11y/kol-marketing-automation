"""Bounded discovery of emails explicitly published by a KOL.

This module discovers evidence only.  It never guesses an address and never
decides deliverability.  ``kol_email_repair`` may accept an address explicitly
published on the KOL's owned public sources as a contact email, while retaining
the unverified status and downstream bounce/domain safeguards.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import html
import ipaddress
import json
import re
import secrets
import socket
from urllib.parse import parse_qs, unquote, urljoin, urlparse

import httpx

from . import feishu, relabel


_EMAIL_RE = re.compile(
    r"(?i)(?<![\w.+-])([\w.+-]+@[\w.-]+\.[a-z]{2,})(?![\w.-])",
)
_URL_RE = re.compile(r"https?://[^\s<>\"']+", re.IGNORECASE)
_HREF_RE = re.compile(r"(?is)<a\b[^>]*\bhref\s*=\s*(['\"])(.*?)\1")
_ANCHOR_RE = re.compile(r"(?is)<a\b(?P<attrs>[^>]*)>(?P<label>.*?)</a>")
_HREF_ATTR_RE = re.compile(r"(?is)\bhref\s*=\s*(['\"])(.*?)\1")
_RESERVED_DOMAINS = {"example.com", "example.net", "example.org"}
_SOCIAL_HOSTS = (
    "youtube.com", "youtu.be", "instagram.com", "tiktok.com", "twitter.com",
    "x.com", "facebook.com", "threads.net",
)
_AGGREGATOR_HOSTS = (
    "linktr.ee", "beacons.ai", "bio.link", "lnk.bio", "solo.to",
    "campsite.bio", "allmylinks.com", "linkin.bio", "bio.site",
    "carrd.co", "hoo.be", "stan.store", "milkshake.app", "direct.me",
)
_SEARCH_HOSTS = (
    "google.com", "bing.com", "yahoo.com", "duckduckgo.com", "baidu.com",
)
_PROXY_FAKE_NETWORK = ipaddress.ip_network("198.18.0.0/15")
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; KOLPublicContactAudit/1.0)",
    "Accept": "text/html,text/plain;q=0.9,*/*;q=0.1",
}
_CONTACT_TOKENS = ("contact", "business", "collab", "sponsor", "brand", "press", "media")
_CONTACT_LINK_TOKENS = (
    "contact", "business", "collab", "sponsor", "press", "media",
    "work with", "work-with", "work_with", "get in touch", "get-in-touch",
    "email",
)
_CONTACT_LABEL_TOKENS = (
    "business", "collab", "collaboration", "sponsor", "partnership",
    "press", "media", "work with", "work-with", "work_with",
    "get in touch", "get-in-touch", "inquiry", "inquiries", "contact",
)
_NON_KOL_CONTACT_TOKENS = (
    "customer support", "platform support", "help center", "privacy",
    "terms of service", "webmaster", "copyright", "abuse report",
)
_DISCOVERY_SIGNING_KEY = secrets.token_bytes(32)


def _flatten(value) -> str:
    if isinstance(value, dict):
        return "\n".join(_flatten(item) for item in value.values())
    if isinstance(value, list):
        return "\n".join(_flatten(item) for item in value)
    return str(value or "")


def _clean_url(value: str, *, base_url: str = "") -> str:
    raw = html.unescape(str(value or "").strip())
    if base_url:
        raw = urljoin(base_url, raw)
    parsed = urlparse(raw)
    if parsed.netloc.casefold().endswith("youtube.com") and parsed.path == "/redirect":
        raw = unquote((parse_qs(parsed.query).get("q") or [""])[0])
        parsed = urlparse(raw)
    if parsed.netloc.casefold() == "l.instagram.com":
        raw = unquote((parse_qs(parsed.query).get("u") or [""])[0])
        parsed = urlparse(raw)
    if parsed.scheme.casefold() not in {"http", "https"} or not parsed.hostname:
        return ""
    if parsed.username or parsed.password:
        return ""
    return raw


def _urls_from_value(value) -> list[str]:
    text = _flatten(value)
    urls = []
    for match in _URL_RE.findall(text):
        cleaned = _clean_url(match.rstrip(".,);]"))
        if cleaned and cleaned not in urls:
            urls.append(cleaned)
    return urls


def extract_public_emails(source: str) -> list[str]:
    """Return only syntactically clean, explicitly present public addresses."""
    decoded = html.unescape(str(source or ""))
    emails = []
    for match in _EMAIL_RE.findall(decoded):
        cleaned, _ = feishu.clean_email(match.split("?", 1)[0])
        if not cleaned:
            continue
        domain = cleaned.rsplit("@", 1)[1].casefold()
        if domain in _RESERVED_DOMAINS or domain.endswith(".invalid"):
            continue
        if cleaned not in emails:
            emails.append(cleaned)
    return emails


def _visible_text(source: str) -> str:
    text = re.sub(r"(?is)<(?:script|style)\b.*?</(?:script|style)>", " ", str(source or ""))
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    return " ".join(html.unescape(text).split())


def _explicit_contact_context(value: str) -> bool:
    context = " ".join(str(value or "").casefold().split())
    if any(token in context for token in _NON_KOL_CONTACT_TOKENS):
        return False
    return any(token in context for token in _CONTACT_LABEL_TOKENS)


def extract_explicit_contact_emails(source: str, page_url: str) -> list[str]:
    """Return addresses presented as creator/business contact information.

    An address merely present in a footer is not enough.  On link-in-bio roots
    it needs an explicit business/collaboration/contact label.  On a dedicated
    contact page, a mailto link or visible address is itself bounded evidence.
    """
    decoded = html.unescape(str(source or ""))
    dedicated_contact_page = (
        not _is_aggregator(page_url)
        and _is_contact_evidence_page(page_url, "")
    )
    accepted = []

    def belongs_to_aggregator_platform(email_address: str) -> bool:
        if not _is_aggregator(page_url):
            return False
        page_host = _site_host(urlparse(page_url).hostname or "")
        email_domain = _site_host(email_address.rsplit("@", 1)[-1])
        return email_domain == page_host

    for match in _ANCHOR_RE.finditer(decoded):
        href = _HREF_ATTR_RE.search(match.group("attrs") or "")
        raw_href = html.unescape(href.group(2) if href else "").strip()
        if not raw_href.casefold().startswith("mailto:"):
            continue
        label = _visible_text(match.group("label") or "")
        address_text = unquote(raw_href[7:]).split("?", 1)[0]
        for email_address in extract_public_emails(address_text):
            if belongs_to_aggregator_platform(email_address):
                continue
            local_part = email_address.split("@", 1)[0].replace(".", " ").replace("_", " ")
            if not (
                dedicated_contact_page
                or _explicit_contact_context(label)
                or _explicit_contact_context(local_part)
                or label.casefold().strip() == "email"
            ):
                continue
            if email_address not in accepted:
                accepted.append(email_address)

    visible = _visible_text(decoded)
    for match in _EMAIL_RE.finditer(visible):
        email_address = extract_public_emails(match.group(1))
        if not email_address:
            continue
        if belongs_to_aggregator_platform(email_address[0]):
            continue
        start = max(0, match.start() - 120)
        end = min(len(visible), match.end() + 120)
        context = visible[start:end]
        if not (dedicated_contact_page or _explicit_contact_context(context)):
            continue
        if any(token in context.casefold() for token in _NON_KOL_CONTACT_TOKENS):
            continue
        if email_address[0] not in accepted:
            accepted.append(email_address[0])
    return accepted


def master_source_urls(fields: dict) -> list[dict]:
    """Read only the Base fields explicitly intended as extra public sources."""
    results = []
    for field, source in (("聚合页URL", "master_aggregate"), ("其他链接", "master_other")):
        for url in _urls_from_value(fields.get(field)):
            if not _is_search_url(url) and not any(item["url"] == url for item in results):
                results.append({"url": url, "source": source})
    return results


def _hrefs(source: str, base_url: str) -> list[str]:
    results = []
    for _, raw in _HREF_RE.findall(str(source or "")):
        cleaned = _clean_url(raw, base_url=base_url)
        if cleaned and cleaned not in results:
            results.append(cleaned)
    return results


def _anchors(source: str, base_url: str) -> list[tuple[str, str]]:
    results = []
    for match in _ANCHOR_RE.finditer(str(source or "")):
        href = _HREF_ATTR_RE.search(match.group("attrs") or "")
        if not href:
            continue
        cleaned = _clean_url(href.group(2), base_url=base_url)
        if not cleaned:
            continue
        label = html.unescape(re.sub(r"(?is)<[^>]+>", " ", match.group("label") or ""))
        label = " ".join(label.split())
        if not any(item[0] == cleaned for item in results):
            results.append((cleaned, label))
    return results


def _site_host(value: str) -> str:
    host = (value or "").casefold().rstrip(".")
    return host[4:] if host.startswith("www.") else host


def contact_page_urls(source: str, base_url: str, *, limit: int = 2) -> list[str]:
    base = urlparse(base_url)
    candidates = []
    for order, (url, label) in enumerate(_anchors(source, base_url)):
        parsed = urlparse(url)
        if _site_host(parsed.hostname or "") != _site_host(base.hostname or "") or url == base_url:
            continue
        if _is_search_url(url):
            continue
        target = f"{parsed.path} {label}".casefold()
        rank = next((index for index, token in enumerate(
            _CONTACT_LINK_TOKENS
        ) if token in target), None)
        if rank is not None:
            candidates.append((rank, order, url))
    return [url for _, _, url in sorted(candidates)[:limit]]


def _is_social(url: str) -> bool:
    host = (urlparse(url).hostname or "").casefold()
    return any(host == value or host.endswith(f".{value}") for value in _SOCIAL_HOSTS)


def _is_aggregator(url: str) -> bool:
    host = (urlparse(url).hostname or "").casefold()
    return any(host == value or host.endswith(f".{value}") for value in _AGGREGATOR_HOSTS)


def _is_search_url(url: str) -> bool:
    host = (urlparse(url).hostname or "").casefold()
    return any(host == value or host.endswith(f".{value}") for value in _SEARCH_HOSTS)


def classify_public_source_url(url: str) -> str:
    """Classify an explicit public profile link without guessing its purpose."""
    cleaned = _clean_url(url)
    host = (urlparse(cleaned).hostname or "").casefold()
    if (
        not cleaned or _is_social(cleaned) or _is_search_url(cleaned)
        or host == "localhost" or host.endswith(".local")
    ):
        return ""
    try:
        if not ipaddress.ip_address(host).is_global:
            return ""
    except ValueError:
        pass
    if not host:
        return ""
    if _is_aggregator(cleaned):
        return "aggregate"
    return "website"


def _balanced_json_value(source: str, marker: str) -> list[str]:
    """Return JSON object/array fragments following an exact key marker."""
    values = []
    cursor = 0
    while True:
        marker_index = str(source or "").find(marker, cursor)
        if marker_index < 0:
            break
        colon = marker_index + len(marker)
        while colon < len(source) and source[colon].isspace():
            colon += 1
        if colon >= len(source) or source[colon] != ":":
            cursor = marker_index + len(marker)
            continue
        start = colon + 1
        while start < len(source) and source[start].isspace():
            start += 1
        if start >= len(source) or source[start] not in "[{":
            cursor = marker_index + len(marker)
            continue
        opening = source[start]
        closing = "]" if opening == "[" else "}"
        depth = 0
        quoted = False
        escaped = False
        end = -1
        for index in range(start, len(source)):
            char = source[index]
            if quoted:
                if escaped:
                    escaped = False
                elif char == "\\":
                    escaped = True
                elif char == '"':
                    quoted = False
                continue
            if char == '"':
                quoted = True
            elif char == opening:
                depth += 1
            elif char == closing:
                depth -= 1
                if depth == 0:
                    end = index + 1
                    break
        if end > start:
            values.append(source[start:end])
            cursor = end
        else:
            cursor = start + 1
    return values


def _json_link_values(value) -> list[str]:
    results = []
    if isinstance(value, dict):
        for key, nested in value.items():
            if key in {"url", "link", "external_url"} and isinstance(nested, str):
                results.append(nested)
            else:
                results.extend(_json_link_values(nested))
    elif isinstance(value, list):
        for nested in value:
            results.extend(_json_link_values(nested))
    return results


def social_profile_external_urls(source: str, profile_url: str) -> list[str]:
    """Read only explicit Instagram/TikTok bio-link fields from public HTML."""
    host = (urlparse(profile_url).hostname or "").casefold()
    fragments = []
    if host == "instagram.com" or host.endswith(".instagram.com"):
        fragments.extend(_balanced_json_value(source, '"bio_links"'))
    elif host == "tiktok.com" or host.endswith(".tiktok.com"):
        fragments.extend(_balanced_json_value(source, '"bioLink"'))
        fragments.extend(_balanced_json_value(source, '"bio_link"'))
    else:
        return []

    values = []
    for fragment in fragments:
        try:
            values.extend(_json_link_values(json.loads(fragment)))
        except json.JSONDecodeError:
            continue
    results = []
    for value in values:
        cleaned = _clean_url(value)
        if cleaned and not _is_social(cleaned) and cleaned not in results:
            results.append(cleaned)
    return results


def _is_contact_evidence_page(url: str, source: str) -> bool:
    """Require an explicit contact context before accepting a website email.

    A valid address in a generic footer can belong to customer support, privacy,
    careers or a web vendor.  Official Link-in-bio pages and explicit contact /
    business / press pages are the bounded evidence accepted by this repair.
    """
    if _is_search_url(url):
        return False
    parsed = urlparse(url)
    context = parsed.path.casefold()
    return _is_aggregator(url) or any(
        token in context for token in _CONTACT_LINK_TOKENS
    )


def _trusted_public_source_roots(fields: dict) -> set[str]:
    roots = {
        item["url"] for item in master_source_urls(fields) if item.get("url")
    }
    profile_url = _clean_url(feishu.ext_url(fields.get("主链接")))
    if profile_url:
        roots.add(profile_url)
    return roots


def _candidate_proof_payload(candidate: dict) -> bytes:
    payload = {
        key: str(candidate.get(key) or "")
        for key in (
            "email", "source", "source_url", "provenance_url", "evidence_kind",
        )
    }
    return json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
    ).encode("utf-8")


def _make_candidate(*, email: str, source: str, source_url: str,
                    provenance_url: str, evidence_kind: str) -> dict:
    candidate = {
        "email": email,
        "source": source,
        "source_url": source_url,
        "provenance_url": provenance_url,
        "evidence_kind": evidence_kind,
    }
    candidate["discovery_proof"] = hmac.new(
        _DISCOVERY_SIGNING_KEY,
        _candidate_proof_payload(candidate),
        hashlib.sha256,
    ).hexdigest()
    return candidate


def is_trusted_public_contact_candidate(candidate: dict, fields: dict) -> bool:
    """Fail closed unless the candidate carries owned, explicit provenance."""
    expected_proof = hmac.new(
        _DISCOVERY_SIGNING_KEY,
        _candidate_proof_payload(candidate),
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(
        str(candidate.get("discovery_proof") or ""), expected_proof,
    ):
        return False
    source = str(candidate.get("source") or "")
    evidence_kind = str(candidate.get("evidence_kind") or "")
    profile_url = _clean_url(feishu.ext_url(fields.get("主链接")))
    source_url = _clean_url(candidate.get("source_url") or "")
    provenance_url = _clean_url(candidate.get("provenance_url") or "")

    trusted_roots = _trusted_public_source_roots(fields)
    if not provenance_url or provenance_url not in trusted_roots:
        return False
    if evidence_kind == "youtube_about":
        return (
            source == "youtube_about"
            and provenance_url == profile_url
            and source_url == profile_url
            and bool(_youtube_lookup_value(profile_url))
        )
    if evidence_kind == "owned_aggregator":
        return bool(source_url and _is_aggregator(source_url))
    if evidence_kind == "explicit_contact_page":
        return bool(source_url and _is_contact_evidence_page(source_url, ""))
    return False


def _aggregator_external_urls(source: str, base_url: str, *, limit: int = 2) -> list[str]:
    base_host = _site_host(urlparse(base_url).hostname or "")
    candidates = []
    for order, (url, label) in enumerate(_anchors(source, base_url)):
        host = (urlparse(url).hostname or "").casefold()
        if not host or _site_host(host) == base_host or _is_social(url):
            continue
        if _is_search_url(url):
            continue
        target = f"{urlparse(url).path} {label}".casefold()
        contact_rank = next((index for index, token in enumerate(
            _CONTACT_LINK_TOKENS
        ) if token in target), None)
        if contact_rank is not None:
            rank = contact_rank
        elif any(token in target for token in (
            "official", "website", "homepage", "my site", "our site",
        )):
            rank = 100
        else:
            rank = 200
        if not any(item[2] == url for item in candidates):
            candidates.append((rank, order, url))
    return [url for _, _, url in sorted(candidates)[:limit]]


def _safe_page_trace(url: str, source: str, status: str, *,
                     contact_pages_found: int = 0,
                     linked_pages_found: int = 0,
                     email_candidates_found: int = 0) -> dict:
    cleaned = _clean_url(url)
    host = (urlparse(cleaned).hostname or "").casefold()
    return {
        "stage": "public_page",
        "source": source,
        "source_kind": classify_public_source_url(cleaned) or "unknown",
        "host": host,
        "url_fingerprint": hashlib.sha256(
            cleaned.encode("utf-8")
        ).hexdigest()[:12] if cleaned else "",
        "status": status,
        "contact_pages_found": int(contact_pages_found),
        "linked_pages_found": int(linked_pages_found),
        "email_candidates_found": int(email_candidates_found),
    }


async def _public_host(host: str, *, allow_proxy_fake_dns: bool = False) -> bool:
    try:
        direct = ipaddress.ip_address(host)
        return direct.is_global
    except ValueError:
        pass
    try:
        infos = await asyncio.get_running_loop().run_in_executor(
            None, lambda: socket.getaddrinfo(host, None, type=socket.SOCK_STREAM),
        )
    except OSError:
        return False
    addresses = {item[4][0] for item in infos}
    parsed_addresses = [ipaddress.ip_address(value) for value in addresses]
    if (
        allow_proxy_fake_dns
        and parsed_addresses
        and all(address in _PROXY_FAKE_NETWORK for address in parsed_addresses)
    ):
        # Clash fake-IP mode maps public hostnames into 198.18.0.0/15.  This
        # exception is only enabled for the explicit link-in-bio allowlist;
        # arbitrary sites, direct IPs and redirect targets remain fail-closed.
        return True
    return bool(parsed_addresses) and all(address.is_global for address in parsed_addresses)


async def fetch_public_page(url: str, *, max_bytes: int = 1_000_000,
                            allow_social: bool = False) -> dict:
    """Fetch a small public HTML/text page while rejecting local/private targets."""
    current = _clean_url(url)
    if not current or (_is_social(current) and not allow_social):
        return {"ok": False, "reason": "unsupported_url"}
    async with httpx.AsyncClient(timeout=12.0, follow_redirects=False, headers=_HEADERS) as client:
        for _ in range(4):
            parsed = urlparse(current)
            if _is_social(current) and not allow_social:
                return {"ok": False, "reason": "unsupported_url"}
            if not await _public_host(
                parsed.hostname or "",
                allow_proxy_fake_dns=(
                    _is_aggregator(current) or (allow_social and _is_social(current))
                ),
            ):
                return {"ok": False, "reason": "non_public_host"}
            try:
                async with client.stream("GET", current) as response:
                    if response.status_code in {301, 302, 303, 307, 308}:
                        target = _clean_url(response.headers.get("location", ""), base_url=current)
                        if not target:
                            return {"ok": False, "reason": "bad_redirect"}
                        current = target
                        continue
                    if response.status_code != 200:
                        return {"ok": False, "reason": f"http_{response.status_code}"}
                    content_type = response.headers.get("content-type", "").casefold()
                    if not any(kind in content_type for kind in ("text/html", "text/plain", "application/xhtml")):
                        return {"ok": False, "reason": "unsupported_content"}
                    body = bytearray()
                    async for chunk in response.aiter_bytes():
                        body.extend(chunk)
                        if len(body) > max_bytes:
                            return {"ok": False, "reason": "body_too_large"}
                    return {
                        "ok": True, "url": current,
                        "text": bytes(body).decode(response.encoding or "utf-8", errors="replace"),
                    }
            except (httpx.HTTPError, OSError):
                return {"ok": False, "reason": "fetch_error"}
    return {"ok": False, "reason": "redirect_limit"}


def _youtube_lookup_value(profile_url: str) -> str:
    parsed = urlparse(str(profile_url or "").strip())
    if "youtube.com" not in (parsed.hostname or "").casefold():
        return ""
    path = parsed.path.strip("/")
    if path.startswith("@"):
        return path.split("/", 1)[0]
    if path.startswith("channel/"):
        return path.split("/", 1)[1].split("/", 1)[0]
    return ""


def _append_public_source_candidate(results: list[dict], url: str, source: str) -> None:
    cleaned = _clean_url(url)
    kind = classify_public_source_url(cleaned)
    if not kind or any(item["url"] == cleaned for item in results):
        return
    results.append({"url": cleaned, "kind": kind, "source": source})


async def discover_public_landing_page_candidates(
    fields: dict, *, max_aggregator_pages: int = 2,
) -> list[dict]:
    """Return explicit public landing pages attached to the KOL profile.

    The function reads only links published by the profile itself.  It does not
    search the web, synthesize a domain, or infer a URL from the account name.
    """
    results: list[dict] = []
    profile_url = str(feishu.ext_url(fields.get("主链接")) or "").strip()
    profile_kind = classify_public_source_url(profile_url)
    if profile_kind:
        _append_public_source_candidate(results, profile_url, "primary_profile")

    youtube_value = _youtube_lookup_value(profile_url)
    if youtube_value:
        profile = await relabel.fetch_youtube_public_profile(youtube_value)
        for item in profile.get("external_links") or []:
            value = (item or {}).get("url") if isinstance(item, dict) else item
            _append_public_source_candidate(results, value, "youtube_external")
    elif profile_url and _is_social(profile_url):
        social_page = await fetch_public_page(
            profile_url, max_bytes=2_000_000, allow_social=True,
        )
        if social_page.get("ok"):
            host = urlparse(profile_url).netloc.casefold()
            source = (
                "instagram_external" if "instagram.com" in host
                else "tiktok_external" if "tiktok.com" in host
                else "social_external"
            )
            for value in social_profile_external_urls(social_page["text"], profile_url):
                _append_public_source_candidate(results, value, source)

    aggregate_items = [item for item in results if item["kind"] == "aggregate"]
    for item in aggregate_items[:max(0, int(max_aggregator_pages))]:
        page = await fetch_public_page(item["url"])
        if not page.get("ok"):
            continue
        for value in _aggregator_external_urls(page["text"], page["url"], limit=2):
            _append_public_source_candidate(results, value, f"{item['source']}_linked")
    return results


async def discover_public_email_candidates_with_trace(
    fields: dict, *, max_pages: int = 4,
) -> dict:
    """Discover candidates and safe page-level diagnostics for one KOL.

    Trace entries intentionally omit full URLs, page bodies and email values so
    a replay report can be shared without copying KOL business data.
    """
    candidates = []
    queue = [
        {**item, "root_url": item["url"]}
        for item in master_source_urls(fields)
    ]
    queued_urls = {item["url"] for item in queue}
    trace = []

    def enqueue(url: str, source: str, *, root_url: str,
                front: bool = False) -> None:
        cleaned = _clean_url(url)
        if not cleaned or _is_search_url(cleaned) or cleaned in queued_urls:
            return
        queued_urls.add(cleaned)
        root = _clean_url(root_url)
        if not root:
            return
        item = {"url": cleaned, "source": source, "root_url": root}
        if front:
            queue.insert(0, item)
        else:
            queue.append(item)

    profile_url = str(feishu.ext_url(fields.get("主链接")) or "").strip()
    if _is_aggregator(profile_url):
        enqueue(profile_url, "primary_aggregate", root_url=profile_url)
    youtube_value = _youtube_lookup_value(profile_url)
    if youtube_value:
        profile = await relabel.fetch_youtube_public_profile(youtube_value)
        profile_candidate_count = 0
        for email_value in profile.get("emails") or ([profile.get("email")] if profile.get("email") else []):
            for email_address in extract_public_emails(email_value):
                candidates.append(_make_candidate(
                    email=email_address,
                    source="youtube_about",
                    source_url=profile_url,
                    provenance_url=profile_url,
                    evidence_kind="youtube_about",
                ))
                profile_candidate_count += 1
        for item in profile.get("external_links") or []:
            url = _clean_url((item or {}).get("url") if isinstance(item, dict) else item)
            if url and not _is_social(url):
                enqueue(url, "youtube_external", root_url=profile_url)
        trace.append({
            "stage": "social_profile",
            "source": "youtube_about",
            "source_kind": "social_profile",
            "host": (urlparse(profile_url).hostname or "").casefold(),
            "url_fingerprint": hashlib.sha256(
                profile_url.encode("utf-8")
            ).hexdigest()[:12] if profile_url else "",
            "status": "profile_loaded",
            "contact_pages_found": 0,
            "linked_pages_found": len(profile.get("external_links") or []),
            "email_candidates_found": profile_candidate_count,
        })
    elif profile_url and _is_social(profile_url):
        social_page = await fetch_public_page(
            profile_url, max_bytes=2_000_000, allow_social=True,
        )
        if social_page.get("ok"):
            source_name = (
                "instagram_external" if "instagram.com" in urlparse(profile_url).netloc.casefold()
                else "tiktok_external" if "tiktok.com" in urlparse(profile_url).netloc.casefold()
                else "social_external"
            )
            external_urls = social_profile_external_urls(social_page["text"], profile_url)
            for url in external_urls:
                enqueue(url, source_name, root_url=profile_url)
            trace.append({
                "stage": "social_profile",
                "source": source_name,
                "source_kind": "social_profile",
                "host": (urlparse(profile_url).hostname or "").casefold(),
                "url_fingerprint": hashlib.sha256(
                    profile_url.encode("utf-8")
                ).hexdigest()[:12],
                "status": "fetched",
                "contact_pages_found": 0,
                "linked_pages_found": len(external_urls),
                "email_candidates_found": 0,
            })
        else:
            event = _safe_page_trace(
                profile_url, "social_profile", social_page.get("reason") or "fetch_error",
            )
            event["stage"] = "social_profile"
            event["source_kind"] = "social_profile"
            trace.append(event)

    seen_urls = set()
    fetched = 0
    page_limit = max(1, min(int(max_pages), 4))
    while queue and fetched < page_limit:
        item = queue.pop(0)
        url = item["url"]
        if url in seen_urls:
            continue
        seen_urls.add(url)
        page = await fetch_public_page(url)
        fetched += 1
        if not page.get("ok"):
            trace.append(_safe_page_trace(
                url, item["source"], page.get("reason") or "fetch_error",
            ))
            continue
        final_url = page["url"]
        page_text = page["text"]
        page_candidates = []
        if _is_contact_evidence_page(final_url, item["source"]):
            for email_address in extract_explicit_contact_emails(page_text, final_url):
                candidate = _make_candidate(
                    email=email_address,
                    source=item["source"],
                    source_url=final_url,
                    provenance_url=item["root_url"],
                    evidence_kind=(
                        "owned_aggregator" if _is_aggregator(final_url)
                        else "explicit_contact_page"
                    ),
                )
                candidates.append(candidate)
                page_candidates.append(candidate)
        contact_urls = contact_page_urls(page_text, final_url, limit=2)
        linked_urls = []
        if _is_aggregator(final_url):
            linked_urls = _aggregator_external_urls(page_text, final_url, limit=2)
        trace.append(_safe_page_trace(
            final_url,
            item["source"],
            "fetched",
            contact_pages_found=len(contact_urls),
            linked_pages_found=len(linked_urls),
            email_candidates_found=len(page_candidates),
        ))

        # Follow explicit contact pages and aggregator links before unrelated
        # seed URLs.  This spends the existing page budget on the strongest
        # evidence instead of increasing network volume or timeout exposure.
        priority_items = [
            (value, f"{item['source']}_contact") for value in contact_urls
        ] + [
            (value, f"{item['source']}_linked") for value in linked_urls
        ]
        for value, source in reversed(priority_items):
            enqueue(value, source, root_url=item["root_url"], front=True)

    if queue:
        for pending in queue:
            trace.append(_safe_page_trace(
                pending["url"], pending["source"], "skipped_page_limit",
            ))
        trace.append({
            "stage": "page_budget",
            "status": "page_limit_reached",
            "remaining_pages": len(queue),
            "page_limit": page_limit,
        })

    deduped = []
    for item in candidates:
        key = item["email"].casefold()
        if not any(existing["email"].casefold() == key for existing in deduped):
            deduped.append(item)
    return {"candidates": deduped, "trace": trace}


async def discover_public_email_candidates(fields: dict, *, max_pages: int = 4) -> list[dict]:
    """Discover bounded public candidates; no inference, pattern generation or write."""
    result = await discover_public_email_candidates_with_trace(
        fields, max_pages=max_pages,
    )
    return result["candidates"]

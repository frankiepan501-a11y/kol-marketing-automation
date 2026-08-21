"""集中上稿活动的承诺/实际上稿事实回填。

只接受两类可核验事实：
1. 达人回复正文里同时出现明确发布动作和具体日期；
2. 达人回复已被分类为 live_link_received 且正文含公开内容链接，
   或 KOL 主表已有上稿日期+链接且只能唯一归属一个正式活动参与记录。

“感兴趣 / 要报价 / 洽谈中”不会被推断成承诺日期。
"""

from __future__ import annotations

import html
import re
import time
from datetime import datetime, timezone
from urllib.parse import urlparse

from . import config, feishu, launch_evidence
from .feishu import ext, ext_url


DAY_MS = 24 * 60 * 60 * 1000
ACTIVE_STATES = {"已入围", "锁定准备中"}
SENT_STATES = {"已发", "已发送"}
SOCIAL_HOSTS = {
    "youtube.com", "www.youtube.com", "youtu.be", "m.youtube.com",
    "tiktok.com", "www.tiktok.com", "instagram.com", "www.instagram.com",
    "facebook.com", "www.facebook.com", "x.com", "www.x.com",
    "twitter.com", "www.twitter.com", "twitch.tv", "www.twitch.tv",
}
OWN_HOST_MARKERS = ("powkong", "funlab", "amazon.", "amzn.")

_MONTHS = {
    "jan": 1, "january": 1, "januar": 1, "enero": 1,
    "feb": 2, "february": 2, "februar": 2, "febrero": 2,
    "mar": 3, "march": 3, "märz": 3, "marz": 3, "marzo": 3,
    "apr": 4, "april": 4, "abril": 4,
    "may": 5, "mai": 5, "mayo": 5,
    "jun": 6, "june": 6, "juni": 6, "junio": 6,
    "jul": 7, "july": 7, "juli": 7, "julio": 7,
    "aug": 8, "august": 8, "agosto": 8,
    "sep": 9, "sept": 9, "september": 9, "septiembre": 9,
    "oct": 10, "october": 10, "oktober": 10, "octubre": 10,
    "nov": 11, "november": 11, "noviembre": 11,
    "dec": 12, "december": 12, "dezember": 12, "diciembre": 12,
}
_MONTH_PATTERN = "|".join(sorted((re.escape(x) for x in _MONTHS), key=len, reverse=True))
_COMMITMENT_PATTERNS = (
    re.compile(
        r"\b(?:i|we)\s+(?:can|will|plan\s+to|expect\s+to|intend\s+to|aim\s+to|"
        r"should\s+be\s+able\s+to)\s+(?:post|publish|upload|share|release)\b",
        re.I,
    ),
    re.compile(
        r"\b(?:ich|wir)\s+(?:kann|können|werde|werden|plane|planen).{0,35}"
        r"\b(?:posten|veröffentlichen|hochladen|teilen)\b",
        re.I,
    ),
    re.compile(
        r"\b(?:yo|nosotros|puedo|podemos|publicaré|publicaremos|planeo|planeamos).{0,35}"
        r"\b(?:publicar|subir|compartir)\b",
        re.I,
    ),
)
_URL_RE = re.compile(r"https?://[^\s<>\"']+", re.I)


def _ids(value) -> list[str]:
    return sorted(launch_evidence._ids(value))


def _ts(value) -> int:
    try:
        return max(0, int(float(value or 0)))
    except (TypeError, ValueError):
        return 0


def _unquoted_reply_text(value: str) -> str:
    text = html.unescape(str(value or ""))
    text = re.sub(r"(?is)<blockquote\b.*?</blockquote>", "", text)
    text = re.split(
        r"(?im)^\s*(?:on .{0,160} wrote:|-----original message-----|from:\s.+)$",
        text,
        maxsplit=1,
    )[0]
    text = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith(">"))
    return re.sub(r"(?s)<[^>]+>", " ", text).strip()


def _date_ms(year: int, month: int, day: int) -> int:
    try:
        return int(datetime(year, month, day, tzinfo=timezone.utc).timestamp() * 1000)
    except (TypeError, ValueError):
        return 0


def extract_explicit_commitment(
    value: str,
    *,
    default_year: int,
    min_ts: int = 0,
    max_ts: int = 0,
) -> int:
    """从未引用的本轮回复中提取“明确发布动作+具体日期”。"""
    text = _unquoted_reply_text(value)
    action_matches = [
        match for pattern in _COMMITMENT_PATTERNS for match in pattern.finditer(text)
    ]
    if not text or not action_matches:
        return 0

    candidates: list[int] = []

    def same_clause(date_match: re.Match) -> bool:
        for action_match in action_matches:
            left = min(action_match.end(), date_match.end())
            right = max(action_match.start(), date_match.start())
            if right - left > 120:
                continue
            if not re.search(r"[.!?;\n]", text[left:right]):
                return True
        return False

    for match in re.finditer(r"\b(20\d{2})[-/.](\d{1,2})[-/.](\d{1,2})\b", text):
        if same_clause(match):
            candidates.append(_date_ms(
                int(match.group(1)), int(match.group(2)), int(match.group(3)),
            ))
    for match in re.finditer(
        rf"\b({_MONTH_PATTERN})\.?\s+(\d{{1,2}})(?:st|nd|rd|th)?(?:,?\s+(20\d{{2}}))?\b",
        text,
        re.I,
    ):
        if same_clause(match):
            candidates.append(_date_ms(
                int(match.group(3) or default_year),
                _MONTHS[match.group(1).casefold()],
                int(match.group(2)),
            ))
    for match in re.finditer(
        rf"\b(\d{{1,2}})(?:st|nd|rd|th)?\.?\s+(?:de\s+)?({_MONTH_PATTERN})"
        rf"(?:\.?\s+(20\d{{2}}))?\b",
        text,
        re.I,
    ):
        if same_clause(match):
            candidates.append(_date_ms(
                int(match.group(3) or default_year),
                _MONTHS[match.group(2).casefold()],
                int(match.group(1)),
            ))
    valid = [
        item for item in candidates
        if item and (not min_ts or item >= min_ts) and (not max_ts or item <= max_ts)
    ]
    return min(valid) if valid else 0


def extract_publication_url(value: str, *, object_type: str = "KOL") -> str:
    text = _unquoted_reply_text(value)
    for raw in _URL_RE.findall(text):
        url = raw.rstrip(".,;:!?)]}")
        host = (urlparse(url).hostname or "").casefold()
        if not host or any(marker in host for marker in OWN_HOST_MARKERS):
            continue
        if object_type == "KOL" and host not in SOCIAL_HOSTS:
            continue
        return url
    return ""


def _eligible_participant(row: dict, draft_map: dict[str, dict]) -> bool:
    fields = row.get("fields") or {}
    if ext(fields.get("参与状态")) not in ACTIVE_STATES:
        return False
    if ext(fields.get("审核结论")) != "通过":
        return False
    return any(
        ext((draft_map.get(draft_id) or {}).get("fields", {}).get("发送状态")) in SENT_STATES
        for draft_id in _ids(fields.get("关联邮件草稿"))
    )


def _default_year(activity_fields: dict) -> int:
    for field_name in ("窗口开始", "窗口结束"):
        value = _ts(activity_fields.get(field_name))
        if value:
            return datetime.fromtimestamp(value / 1000, tz=timezone.utc).year
    return datetime.now(timezone.utc).year


async def reconcile_campaign(campaign_id: str, *, dry_run: bool = True) -> dict:
    if not config.T_LAUNCH_PARTICIPANT:
        raise RuntimeError("T_LAUNCH_PARTICIPANT 未配置")
    activity = await launch_evidence.get_activity(campaign_id)
    activity_fields = activity.get("fields") or {}
    all_participants = await feishu.fetch_all_records(
        config.T_LAUNCH_PARTICIPANT,
        field_names=[
            "活动ID", "对象类型", "参与状态", "审核结论", "关联KOL", "关联邮件草稿",
            "承诺上稿时间", "实际上稿时间", "上稿链接",
        ],
        page_size=500,
    )
    participants = [
        row for row in all_participants
        if ext((row.get("fields") or {}).get("活动ID")) == campaign_id
    ]
    drafts = await feishu.fetch_all_records(
        config.T_DRAFT,
        field_names=[
            "发送状态", "发送时间", "是否回复", "回复日期", "回复原文", "场景标签",
        ],
        page_size=500,
    )
    draft_map = {row.get("record_id"): row for row in drafts if row.get("record_id")}
    uploaded_kols = await feishu.search_records(
        config.T_KOL,
        [{"field_name": "上稿日期", "operator": "isNotEmpty", "value": []}],
        field_names=["上稿日期", "上稿链接"],
    )
    uploaded_by_id = {
        row.get("record_id"): row for row in uploaded_kols if row.get("record_id")
    }

    eligible_by_kol: dict[str, list[dict]] = {}
    for row in all_participants:
        if not _eligible_participant(row, draft_map):
            continue
        fields = row.get("fields") or {}
        if _ts(fields.get("实际上稿时间")):
            continue
        for kol_id in _ids(fields.get("关联KOL")):
            eligible_by_kol.setdefault(kol_id, []).append(row)

    default_year = _default_year(activity_fields)
    window_end = _ts(activity_fields.get("窗口结束"))
    planned, errors = [], []
    ambiguous_manual_uploads = 0
    missing_live_links = 0
    for participant in participants:
        participant_id = participant.get("record_id")
        fields = participant.get("fields") or {}
        if not participant_id or not _eligible_participant(participant, draft_map):
            continue
        draft_rows = [
            draft_map[draft_id] for draft_id in _ids(fields.get("关联邮件草稿"))
            if draft_id in draft_map
            and ext((draft_map[draft_id].get("fields") or {}).get("发送状态")) in SENT_STATES
        ]
        draft_rows.sort(
            key=lambda row: _ts((row.get("fields") or {}).get("回复日期")), reverse=True,
        )
        update = {}
        commitment_source = ""
        actual_source = ""

        if not _ts(fields.get("承诺上稿时间")):
            for draft in draft_rows:
                draft_fields = draft.get("fields") or {}
                if not draft_fields.get("是否回复"):
                    continue
                reply_ts = _ts(draft_fields.get("回复日期"))
                commitment = extract_explicit_commitment(
                    ext(draft_fields.get("回复原文")),
                    default_year=default_year,
                    min_ts=max(0, reply_ts - DAY_MS),
                    max_ts=(window_end + 90 * DAY_MS) if window_end else 0,
                )
                if commitment:
                    update["承诺上稿时间"] = commitment
                    commitment_source = "explicit_reply_date"
                    break

        if not _ts(fields.get("实际上稿时间")):
            object_type = ext(fields.get("对象类型")) or "KOL"
            for draft in draft_rows:
                draft_fields = draft.get("fields") or {}
                if ext(draft_fields.get("场景标签")) != "live_link_received":
                    continue
                link = extract_publication_url(
                    ext(draft_fields.get("回复原文")), object_type=object_type,
                )
                if not link:
                    missing_live_links += 1
                    continue
                update["实际上稿时间"] = _ts(draft_fields.get("回复日期")) or int(time.time() * 1000)
                update["上稿链接"] = {"link": link, "text": "打开上稿内容"}
                actual_source = "reply_live_link"
                break

        if not _ts(fields.get("实际上稿时间")) and "实际上稿时间" not in update:
            for kol_id in _ids(fields.get("关联KOL")):
                uploaded = uploaded_by_id.get(kol_id)
                if not uploaded:
                    continue
                candidates = eligible_by_kol.get(kol_id) or []
                if len(candidates) != 1 or candidates[0].get("record_id") != participant_id:
                    ambiguous_manual_uploads += 1
                    continue
                upload_fields = uploaded.get("fields") or {}
                upload_ts = _ts(upload_fields.get("上稿日期"))
                upload_link = ext_url(upload_fields.get("上稿链接")) or ext(
                    upload_fields.get("上稿链接")
                )
                sent_ts = min(
                    (_ts((draft.get("fields") or {}).get("发送时间")) for draft in draft_rows),
                    default=0,
                )
                if not upload_ts or not upload_link or (sent_ts and upload_ts < sent_ts):
                    continue
                update["实际上稿时间"] = upload_ts
                update["上稿链接"] = {"link": upload_link, "text": "打开上稿内容"}
                actual_source = "unique_main_table_upload"
                break

        if not update:
            continue
        planned.append({
            "participant_id": participant_id,
            "fields": update,
            "commitment_source": commitment_source,
            "actual_source": actual_source,
        })
        if dry_run:
            continue
        try:
            await feishu.update_record(
                config.T_LAUNCH_PARTICIPANT, participant_id, update,
            )
        except Exception as exc:
            errors.append({"participant_id": participant_id, "error": str(exc)[:160]})

    successful = len(planned) if dry_run else len(planned) - len(errors)
    return {
        "campaign_id": campaign_id,
        "dry_run": dry_run,
        "participants_scanned": len(participants),
        "updates_planned": len(planned),
        "updates_written": successful,
        "commitments_written": sum(bool(item["fields"].get("承诺上稿时间")) for item in planned)
        if dry_run else sum(
            bool(item["fields"].get("承诺上稿时间"))
            and not any(error["participant_id"] == item["participant_id"] for error in errors)
            for item in planned
        ),
        "actuals_written": sum(bool(item["fields"].get("实际上稿时间")) for item in planned)
        if dry_run else sum(
            bool(item["fields"].get("实际上稿时间"))
            and not any(error["participant_id"] == item["participant_id"] for error in errors)
            for item in planned
        ),
        "ambiguous_manual_uploads": ambiguous_manual_uploads,
        "missing_live_links": missing_live_links,
        "planned": planned,
        "errors": errors,
    }

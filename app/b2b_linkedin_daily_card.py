"""B2B LinkedIn daily development card.

This module only prepares and sends Feishu interactive cards. It does not
operate LinkedIn pages, send connection requests, or send private messages.
Those steps remain manual and are confirmed by card receipts handled by
app.b2b_assistant.
"""
import json
import os
from collections import Counter
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

from . import b2b_linkedin_auto_pool, feishu

BJ = timezone(timedelta(hours=8))

B2B_APP_TOKEN = os.environ.get("B2B_CUSTOMER_APP_TOKEN", "E1kkbx1tVaJvQGsKf94cJG88nzb")
B2B_LINKEDIN_TABLE = os.environ.get("B2B_LINKEDIN_TABLE", "tblN8XszEatuTJgP")
B2B_LINKEDIN_VIEW = os.environ.get("B2B_LINKEDIN_VIEW", "vew9f7zQ7s")
B2B_GROUP_CHAT_ID = os.environ.get("B2B_GROUP_CHAT_ID", "oc_2e878553984592d7396401fdd6a37d61")
B2B_LINKEDIN_FRANKIE_EMAIL = os.environ.get("B2B_LINKEDIN_FRANKIE_EMAIL", "398459272@qq.com")
DEFAULT_DISPATCH_OWNERS = ["吴晓丹", "冼浩华", "李桐欣"]

LINKEDIN_FIELD_NAMES = [
    "线索名称",
    "公司名称",
    "线索来源",
    "开发状态",
    "触达状态",
    "公司官网",
    "LinkedIn公司页",
    "LinkedIn联系人页",
    "联系人姓名",
    "职位",
    "国家/地区",
    "公司类型",
    "主力渠道",
    "代理竞品",
    "主营类目",
    "AI开发评分",
    "ICP匹配",
    "AI建议等级",
    "AI开发理由",
    "推荐连接语",
    "推荐私信",
    "推荐开发信",
    "跟进人",
    "下一步行动",
    "CRM匹配状态",
    "邮箱",
    "邮箱验真状态",
    "备注",
]

SUMMARY_FIELD_NAMES = [
    "线索名称",
    "公司名称",
    "线索来源",
    "开发状态",
    "触达状态",
    "公司官网",
    "LinkedIn公司页",
    "LinkedIn联系人页",
    "联系人姓名",
    "职位",
    "国家/地区",
    "公司类型",
    "主力渠道",
    "代理竞品",
    "主营类目",
    "AI开发评分",
    "ICP匹配",
    "AI建议等级",
    "AI开发理由",
    "跟进人",
    "CRM匹配状态",
    "邮箱",
    "邮箱验真状态",
    "创建批次",
    "Snov查询状态",
    "备注",
]

GRADE_ORDER = {
    "A-优先开发": 0,
    "A": 0,
    "B-可开发": 1,
    "B": 1,
    "C-低优先": 2,
    "C": 2,
}


def _text(value) -> str:
    return str(feishu.ext(value) or "").strip()


def _url(value) -> str:
    return str(feishu.ext_url(value) or "").strip()


def _score(value) -> float:
    try:
        return float(value or 0)
    except Exception:
        try:
            return float(_text(value) or 0)
        except Exception:
            return 0.0


def _clip(value: str, limit: int = 360) -> str:
    value = (value or "").strip()
    if len(value) <= limit:
        return value
    return value[: limit - 1].rstrip() + "…"


def _record_url(record_id: str) -> str:
    return f"https://u1wpma3xuhr.feishu.cn/base/{B2B_APP_TOKEN}?table={B2B_LINKEDIN_TABLE}&view={B2B_LINKEDIN_VIEW}&record={record_id}"


def _card_action(record_id: str, action: str, company: str) -> dict:
    return {
        "route": "linkedin_lead_receipt",
        "action": action,
        "record_id": record_id,
        "lead_record_id": record_id,
        "company": company,
        "source": "linkedin_daily_card",
        "app_token": B2B_APP_TOKEN,
        "table_id": B2B_LINKEDIN_TABLE,
    }


async def _list_records(*, field_names: list[str] | None = None, automatic_fields: bool = False) -> list[dict]:
    items: list[dict] = []
    page_token = ""
    encoded_fields = ""
    if field_names:
        encoded_fields = "&field_names=" + quote(json.dumps(field_names, ensure_ascii=False), safe="")
    auto = "&automatic_fields=true" if automatic_fields else ""
    while True:
        path = f"/bitable/v1/apps/{B2B_APP_TOKEN}/tables/{B2B_LINKEDIN_TABLE}/records?page_size=500{auto}{encoded_fields}"
        if page_token:
            path += "&page_token=" + quote(page_token, safe="")
        resp = await feishu.api("GET", path, which="bitable")
        data = resp.get("data") or {}
        items.extend(data.get("items") or [])
        if not data.get("has_more"):
            break
        page_token = data.get("page_token") or ""
        if not page_token:
            break
    return items


async def _update_record(record_id: str, fields: dict) -> None:
    await feishu.api(
        "PUT",
        f"/bitable/v1/apps/{B2B_APP_TOKEN}/tables/{B2B_LINKEDIN_TABLE}/records/{record_id}",
        {"fields": fields},
        which="bitable",
    )


def _row_from_record(rec: dict) -> dict:
    fields = rec.get("fields") or {}
    record_id = rec.get("record_id") or ""
    company = _text(fields.get("公司名称"))
    title = _text(fields.get("线索名称")) or company
    linkedin_profile = _url(fields.get("LinkedIn联系人页"))
    linkedin_company = _url(fields.get("LinkedIn公司页"))
    return {
        "record_id": record_id,
        "title": title,
        "company": company or title or record_id,
        "source": _text(fields.get("线索来源")),
        "dev_status": _text(fields.get("开发状态")),
        "reach_status": _text(fields.get("触达状态")),
        "website": _url(fields.get("公司官网")),
        "linkedin": linkedin_profile or linkedin_company,
        "linkedin_company": linkedin_company,
        "linkedin_profile": linkedin_profile,
        "contact": _text(fields.get("联系人姓名")),
        "position": _text(fields.get("职位")),
        "country": _text(fields.get("国家/地区")),
        "company_type": _text(fields.get("公司类型")),
        "channels": _text(fields.get("主力渠道")),
        "competitors": _text(fields.get("代理竞品")),
        "category": _text(fields.get("主营类目")),
        "score": _score(fields.get("AI开发评分")),
        "icp": _text(fields.get("ICP匹配")),
        "grade": _text(fields.get("AI建议等级")),
        "reason": _text(fields.get("AI开发理由")),
        "connect_copy": _text(fields.get("推荐连接语")),
        "message_copy": _text(fields.get("推荐私信")),
        "email_copy": _text(fields.get("推荐开发信")),
        "owner": _text(fields.get("跟进人")) or "未分配",
        "next_action": _text(fields.get("下一步行动")),
        "crm_match": _text(fields.get("CRM匹配状态")),
        "email": _text(fields.get("邮箱")),
        "email_status": _text(fields.get("邮箱验真状态")),
        "batch": _text(fields.get("创建批次")),
        "snov_status": _text(fields.get("Snov查询状态")),
        "note": _text(fields.get("备注")),
        "url": _record_url(record_id),
        "created_time": int(rec.get("created_time") or 0),
    }


def _is_test_row(row: dict) -> bool:
    probe = " ".join([row.get("title", ""), row.get("company", ""), row.get("note", "")])
    return "样张测试" in probe or "__test__" in probe.lower()


def _eligible(row: dict, *, include_test: bool = False) -> bool:
    if not include_test and _is_test_row(row):
        return False
    if row["dev_status"] not in {"", "待开发"}:
        return False
    if row["reach_status"] not in {"", "待触达"}:
        return False
    if row["crm_match"] and row["crm_match"] != "新线索":
        return False
    if row["icp"] == "否":
        return False
    return True


def _grade_rank(value: str) -> int:
    value = value or ""
    for key, rank in GRADE_ORDER.items():
        if key in value:
            return rank
    return 9


async def _eligible_rows(*, include_test: bool = False) -> list[dict]:
    rows = [_row_from_record(rec) for rec in await _list_records(field_names=LINKEDIN_FIELD_NAMES)]
    rows = [row for row in rows if _eligible(row, include_test=include_test)]
    rows.sort(key=lambda r: (_grade_rank(r["grade"]), -r["score"], r["owner"], r["company"]))
    return rows


def _dispatch_owners() -> list[str]:
    raw = os.environ.get("B2B_LINKEDIN_DISPATCH_OWNERS", "").strip()
    if not raw:
        return list(DEFAULT_DISPATCH_OWNERS)
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            owners = [str(x).strip() for x in parsed if str(x).strip()]
            return owners or list(DEFAULT_DISPATCH_OWNERS)
    except Exception:
        pass
    owners = [x.strip() for x in raw.split(",") if x.strip()]
    return owners or list(DEFAULT_DISPATCH_OWNERS)


def _country_rule_owner(country: str) -> tuple[str, str]:
    compact = (country or "").lower().replace(" ", "").replace("　", "")
    if any(token in compact for token in ["台湾", "台灣", "taiwan", "taiwan,china", "tw"]):
        return "冼浩华", "台湾/泰国规则"
    if any(token in compact for token in ["泰国", "thailand", "thai"]):
        return "冼浩华", "台湾/泰国规则"
    if any(token in compact for token in ["日本", "japan", "jp"]):
        return "李桐欣", "日本规则"
    return "", ""


def _with_assignment(row: dict, owner: str, reason: str) -> dict:
    out = dict(row)
    out["original_owner"] = row.get("owner") or "未分配"
    out["owner"] = owner
    out["assignment_reason"] = reason
    return out


def _assign_rows(rows: list[dict], *, limit_per_owner: int, owner_filter: str = "") -> tuple[dict[str, list[dict]], list[dict], dict]:
    owners = _dispatch_owners()
    buckets: dict[str, list[dict]] = {owner: [] for owner in owners}
    queued: list[dict] = []
    stats = {"country_rule": 0, "balanced": 0, "queued_capacity": 0}

    forced_rows = []
    balanced_rows = []
    for row in rows:
        forced_owner, reason = _country_rule_owner(row.get("country") or "")
        if forced_owner:
            forced_rows.append((row, forced_owner, reason))
        else:
            balanced_rows.append(row)

    for row, forced_owner, reason in forced_rows:
        if forced_owner not in buckets:
            buckets[forced_owner] = []
            owners.append(forced_owner)
        if len(buckets[forced_owner]) < limit_per_owner:
            buckets[forced_owner].append(_with_assignment(row, forced_owner, reason))
            stats["country_rule"] += 1
        else:
            queued.append(_with_assignment(row, forced_owner, reason + "，今日名额已满"))
            stats["queued_capacity"] += 1

    for row in balanced_rows:
        candidates = [owner for owner in owners if len(buckets.get(owner, [])) < limit_per_owner]
        if not candidates:
            queued.append(_with_assignment(row, row.get("owner") or "未分配", "排队：今日三人名额已满"))
            stats["queued_capacity"] += 1
            continue
        chosen = sorted(candidates, key=lambda name: (len(buckets.get(name, [])), owners.index(name)))[0]
        buckets[chosen].append(_with_assignment(row, chosen, "非指定国家平均派发"))
        stats["balanced"] += 1

    if owner_filter:
        buckets = {name: owner_rows for name, owner_rows in buckets.items() if name == owner_filter}
    buckets = {name: owner_rows for name, owner_rows in buckets.items() if owner_rows}
    return buckets, queued, stats


async def _sync_assignments(grouped: dict[str, list[dict]], *, commit: bool) -> list[dict]:
    if not commit:
        return []
    updates = []
    for owner_rows in grouped.values():
        for row in owner_rows:
            original_owner = row.get("original_owner") or "未分配"
            if original_owner == row["owner"]:
                continue
            await _update_record(row["record_id"], {"跟进人": row["owner"]})
            updates.append(
                {
                    "record_id": row["record_id"],
                    "company": row["company"],
                    "from": original_owner,
                    "to": row["owner"],
                    "reason": row.get("assignment_reason", ""),
                }
            )
    return updates


def _field(label: str, value: str) -> dict:
    return {"is_short": True, "text": {"tag": "lark_md", "content": f"**{label}**: {value or '-'}"}}


def _button(text: str, url: str | None = None, value: dict | None = None, style: str = "default") -> dict:
    btn = {
        "tag": "button",
        "text": {"tag": "plain_text", "content": text},
        "type": "default",
    }
    if style:
        btn["type"] = style
    if url:
        btn["url"] = url
    if value:
        btn["value"] = value
    return btn


def _row_elements(row: dict, index: int, total: int) -> list[dict]:
    record_id = row["record_id"]
    company = row["company"]
    contact_line = row["contact"] or "待补联系人"
    if row["position"]:
        contact_line += f" / {row['position']}"
    facts = [
        _field("任务", f"{index}/{total}"),
        _field("客户", company),
        _field("联系人", contact_line),
        _field("国家/类型", " / ".join(x for x in [row["country"], row["company_type"]] if x)),
        _field("企业页", "已补齐" if row["linkedin_company"] else "待人工确认"),
        _field("AI等级", f"{row['grade'] or '-'} · {row['score']:.0f}分"),
        _field("当前状态", f"{row['dev_status'] or '待开发'} / {row['reach_status'] or '待触达'}"),
        _field("派发规则", row.get("assignment_reason", "") or "-"),
    ]

    link_actions = []
    if row["linkedin_company"]:
        link_actions.append(_button("🏢 打开企业LinkedIn", url=row["linkedin_company"], style="primary"))
    if row["linkedin_profile"]:
        link_actions.append(_button("👤 打开联系人LinkedIn", url=row["linkedin_profile"]))
    if row["website"]:
        link_actions.append(_button("🌐 打开官网", url=row["website"]))
    link_actions.append(_button("📋 打开线索记录", url=row["url"]))

    receipt_1 = [
        _button("✅ 已加人", value=_card_action(record_id, "linkedin_connected", company), style="primary"),
        _button("💬 已发私信", value=_card_action(record_id, "linkedin_message_sent", company)),
        _button("🎉 已回复", value=_card_action(record_id, "linkedin_replied", company)),
    ]
    receipt_2 = [
        _button("📧 转Email", value=_card_action(record_id, "linkedin_to_email", company)),
        _button("⛔ 不合适", value=_card_action(record_id, "linkedin_not_fit", company), style="danger"),
    ]

    return [
        {"tag": "hr"},
        {"tag": "div", "fields": facts},
        {"tag": "action", "actions": link_actions},
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": "💡 **为什么开发**\n" + (_clip(row["reason"], 320) or "暂无开发理由，请先核对官网和 LinkedIn。"),
            },
        },
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": "🤝 **推荐连接语**\n" + (_clip(row["connect_copy"], 280) or "Hi, I noticed your company works in gaming accessories. Open to connect?"),
            },
        },
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": "💬 **接受后的第一条私信**\n" + (_clip(row["message_copy"], 420) or "Thanks for connecting. Quick question: are you the right person for sourcing gaming accessories?"),
            },
        },
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": (
                    "⏱ **执行 / 降配规则**\n"
                    "- 今日动作: 手动查看 profile, 发送连接邀请; 完成后点 **已加人**\n"
                    "- 接受后动作: 发送上面的推荐私信; 完成后点 **已发私信**\n"
                    "- 业务员 48h 内未点任何回执按钮: 系统判为 **未确认执行**, 次日降低该线索派发优先级, 并提醒跟进人补状态\n"
                    "- 客户私信后 D+3 未回复: 转 Email / 官网表单\n"
                    "- 客户 D+8 仍无回应: 低频跟进或暂停"
                ),
            },
        },
        {"tag": "action", "actions": receipt_1},
        {"tag": "action", "actions": receipt_2},
    ]


def build_card(rows: list[dict], *, owner_name: str = "", preview: bool = False) -> dict:
    owner_label = owner_name or "未分配"
    title_prefix = "🧪 预览 · " if preview else "🔗 "
    elements = [
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": (
                    f"**负责人**: {owner_label}    **今日任务**: {len(rows)} 条\n"
                    "系统只给出客户判断和话术; LinkedIn 查看、加人、私信必须由业务员手动执行。"
                ),
            },
        }
    ]
    total = len(rows)
    for idx, row in enumerate(rows, start=1):
        elements.extend(_row_elements(row, idx, total))
    elements.append(
        {
            "tag": "note",
            "elements": [
                {
                    "tag": "plain_text",
                    "content": "按钮会写回 LinkedIn 线索池; 不会自动操作 LinkedIn。若按钮点错，在备注或群里补充说明。",
                }
            ],
        }
    )
    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "template": "blue",
            "title": {"tag": "plain_text", "content": f"{title_prefix}LinkedIn每日开发卡 · {owner_label} · {len(rows)}条"},
        },
        "elements": elements,
    }


def _target_from_value(value: str) -> tuple[str, str]:
    value = (value or "").strip()
    if not value:
        return "", ""
    if ":" in value and value.split(":", 1)[0] in {"email", "open_id", "union_id", "chat_id"}:
        receive_type, receive_id = value.split(":", 1)
        return receive_type.strip(), receive_id.strip()
    if "@" in value:
        return "email", value
    if value.startswith("ou_"):
        return "open_id", value
    if value.startswith("on_"):
        return "union_id", value
    if value.startswith("oc_"):
        return "chat_id", value
    return "chat_id", value


def _notify_target(owner: str, *, frankie_only: bool = False) -> tuple[str, str]:
    if frankie_only:
        return "email", B2B_LINKEDIN_FRANKIE_EMAIL
    raw = os.environ.get("B2B_LINKEDIN_OWNER_NOTIFY_JSON", "").strip()
    if raw:
        try:
            mapping = json.loads(raw)
            value = mapping.get(owner) or mapping.get("*")
            if isinstance(value, dict):
                receive_type = value.get("receive_type") or value.get("type") or ""
                receive_id = value.get("receive_id") or value.get("id") or ""
                if receive_type and receive_id:
                    return receive_type, receive_id
            if isinstance(value, str):
                target = _target_from_value(value)
                if target[0] and target[1]:
                    return target
        except Exception as exc:
            print(f"[b2b_linkedin_daily_card] bad B2B_LINKEDIN_OWNER_NOTIFY_JSON: {exc}")
    return "chat_id", B2B_GROUP_CHAT_ID


def _date_window_ms(day: str = "") -> tuple[int, int, str]:
    if day:
        base = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=BJ)
    else:
        now = datetime.now(BJ)
        base = datetime(now.year, now.month, now.day, tzinfo=BJ)
    end = base + timedelta(days=1)
    return int(base.timestamp() * 1000), int(end.timestamp() * 1000), base.strftime("%Y-%m-%d")


def _company_key(row: dict) -> str:
    return (row.get("company") or row.get("title") or row.get("record_id") or "").strip().lower()


def _top_counter_lines(counter: Counter, *, limit: int = 8) -> str:
    if not counter:
        return "-"
    parts = [f"{name or '未填'} {count}" for name, count in counter.most_common(limit)]
    rest = sum(counter.values()) - sum(count for _, count in counter.most_common(limit))
    if rest > 0:
        parts.append(f"其他 {rest}")
    return " / ".join(parts)


def _upstream_status_lines() -> str:
    last = b2b_linkedin_auto_pool.get_last_run()
    if not last:
        return "最近执行: 当前服务进程内暂无记录\n新增/计划: -\n过滤: -"
    skips = last.get("skip_reasons") or {}
    skip_text = _top_counter_lines(Counter(skips), limit=5) if skips else "-"
    source = "候选池" if last.get("candidate_source") == "candidate_pool" else "种子兜底"
    return (
        f"最近执行: {last.get('started_at_bj') or '-'} / 批次 {last.get('batch') or '-'}\n"
        f"来源: {source} / 待入池候选库存: {last.get('candidate_pending_total', 0)}\n"
        f"候选域名: {last.get('selected_domains', 0)} / 计划入池: {last.get('planned_records', 0)} / 已新增: {last.get('created_records', 0)}\n"
        f"过滤: {skip_text}"
    )


def _today_rows(rows: list[dict], *, start_ms: int, end_ms: int) -> list[dict]:
    return [row for row in rows if start_ms <= int(row.get("created_time") or 0) < end_ms]


def _build_pool_summary_card(
    *,
    day: str,
    today_rows: list[dict],
    all_eligible_rows: list[dict],
    per_owner_limit: int,
) -> dict:
    company_keys = {_company_key(row) for row in today_rows if _company_key(row)}
    new_company_count = len(company_keys)
    new_record_count = len(today_rows)
    new_dispatchable = [
        row for row in today_rows
        if row["dev_status"] in {"", "待开发"}
        and row["reach_status"] in {"", "待触达"}
        and (not row["crm_match"] or row["crm_match"] == "新线索")
        and row["icp"] != "否"
    ]
    country_counts = Counter(row["country"] or "未填" for row in today_rows)
    grade_counts = Counter(row["grade"] or "未评级" for row in today_rows)
    crm_counts = Counter(row["crm_match"] or "未查重" for row in today_rows)
    icp_counts = Counter(row["icp"] or "未判断" for row in today_rows)
    batch_counts = Counter(row["batch"] or "未填批次" for row in today_rows)

    grouped, queued, _stats = _assign_rows(all_eligible_rows, limit_per_owner=per_owner_limit)
    queue_total = len(all_eligible_rows)
    today_capacity = len(_dispatch_owners()) * per_owner_limit
    today_dispatch = sum(len(rows) for rows in grouped.values())

    top_samples = sorted(today_rows, key=lambda r: (-r["score"], r["company"]))[:10]
    sample_lines = []
    for row in top_samples:
        sample_lines.append(
            f"- {row['company']} · {row['country'] or '未填'} · {row['company_type'] or '类型待补'} · "
            f"{row['grade'] or '未评级'} {row['score']:.0f}分"
        )
    samples = "\n".join(sample_lines) if sample_lines else "- 今日暂无新增入池"

    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "template": "green" if new_record_count else "grey",
            "title": {"tag": "plain_text", "content": f"📥 LinkedIn线索入池日报 · {day}"},
        },
        "elements": [
            {
                "tag": "div",
                "fields": [
                    _field("新增公司", str(new_company_count)),
                    _field("新增线索", str(new_record_count)),
                    _field("新增可派发", str(len(new_dispatchable))),
                    _field("当前待派队列", str(queue_total)),
                    _field("今日派发上限", f"{today_capacity} 条（{len(_dispatch_owners())}人×{per_owner_limit}）"),
                    _field("今日预计派发", str(today_dispatch)),
                ],
            },
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md", "content": "🌍 **国家/地区分布**\n" + _top_counter_lines(country_counts)}},
            {"tag": "div", "text": {"tag": "lark_md", "content": "🏷 **AI等级分布**\n" + _top_counter_lines(grade_counts)}},
            {"tag": "div", "text": {"tag": "lark_md", "content": "🔎 **查重 / ICP**\nCRM: " + _top_counter_lines(crm_counts, limit=5) + "\nICP: " + _top_counter_lines(icp_counts, limit=5)}},
            {"tag": "div", "text": {"tag": "lark_md", "content": "🧾 **入池批次**\n" + _top_counter_lines(batch_counts, limit=5)}},
            {"tag": "div", "text": {"tag": "lark_md", "content": "⚙️ **上游自动入池任务**\n" + _upstream_status_lines()}},
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md", "content": "📌 **新增样例（按AI评分取前10）**\n" + samples}},
            {
                "tag": "note",
                "elements": [
                    {
                        "tag": "plain_text",
                        "content": "入池日报只汇报新增线索与队列情况，不代表已在 LinkedIn 加人或私信；业务执行以每日开发卡回执为准。",
                    }
                ],
            },
        ],
    }


async def run_pool_summary(
    *,
    commit: bool = False,
    notify: bool = False,
    day: str = "",
    per_owner_limit: int = 5,
    frankie_only: bool = False,
) -> dict:
    if notify and not commit:
        raise ValueError("notify=true requires commit=true")
    per_owner_limit = max(1, min(int(per_owner_limit or 5), 10))
    start_ms, end_ms, day_label = _date_window_ms(day)
    all_rows = [_row_from_record(rec) for rec in await _list_records(field_names=SUMMARY_FIELD_NAMES, automatic_fields=True)]
    today = _today_rows(all_rows, start_ms=start_ms, end_ms=end_ms)
    all_eligible = [row for row in all_rows if _eligible(row, include_test=False)]
    card = _build_pool_summary_card(
        day=day_label,
        today_rows=today,
        all_eligible_rows=all_eligible,
        per_owner_limit=per_owner_limit,
    )
    message_id = ""
    send_error = ""
    if notify:
        receive_type, receive_id = ("email", B2B_LINKEDIN_FRANKIE_EMAIL) if frankie_only else ("chat_id", B2B_GROUP_CHAT_ID)
        try:
            message_id = await feishu.send_card_via_b2b_assistant(receive_type, receive_id, card)
        except Exception as exc:
            send_error = f"{type(exc).__name__}: {str(exc)[:240]}"
    country_counts = Counter(row["country"] or "未填" for row in today)
    grade_counts = Counter(row["grade"] or "未评级" for row in today)
    return {
        "commit": commit,
        "notify": notify,
        "day": day_label,
        "new_records": len(today),
        "new_companies": len({_company_key(row) for row in today if _company_key(row)}),
        "country_counts": dict(country_counts),
        "grade_counts": dict(grade_counts),
        "current_queue_total": len(all_eligible),
        "message_id": message_id,
        "send_error": send_error,
    }


async def run(
    *,
    commit: bool = False,
    notify: bool = False,
    limit: int = 5,
    owner: str = "",
    include_test: bool = False,
    frankie_only: bool = False,
) -> dict:
    """Build and optionally send LinkedIn daily cards.

    limit is applied per owner after grouping.
    """
    if notify and not commit:
        raise ValueError("notify=true requires commit=true")
    limit = max(1, min(int(limit or 5), 10))
    rows = await _eligible_rows(include_test=include_test)
    grouped, queued, assignment_stats = _assign_rows(rows, limit_per_owner=limit, owner_filter=owner)
    assignment_updates = await _sync_assignments(grouped, commit=commit)

    message_ids = []
    send_errors = []
    if notify:
        for owner_name, owner_rows in grouped.items():
            card = build_card(owner_rows, owner_name=owner_name)
            receive_type, receive_id = _notify_target(owner_name, frankie_only=frankie_only)
            try:
                message_id = await feishu.send_card_via_b2b_assistant(receive_type, receive_id, card)
                message_ids.append({"owner": owner_name, "receive_type": receive_type, "receive_id": receive_id, "message_id": message_id})
            except Exception as exc:
                send_errors.append({"owner": owner_name, "receive_type": receive_type, "receive_id": receive_id, "error": f"{type(exc).__name__}: {str(exc)[:240]}"})

    preview = {
        owner_name: [
            {
                "record_id": row["record_id"],
                "company": row["company"],
                "contact": row["contact"],
                "position": row["position"],
                "score": row["score"],
                "grade": row["grade"],
                "linkedin_company": row["linkedin_company"],
                "linkedin_profile": row["linkedin_profile"],
                "url": row["url"],
            }
            for row in owner_rows
        ]
        for owner_name, owner_rows in grouped.items()
    }
    return {
        "commit": commit,
        "notify": notify,
        "limit_per_owner": limit,
        "owner_filter": owner,
        "include_test": include_test,
        "frankie_only": frankie_only,
        "eligible_total": len(rows),
        "group_count": len(grouped),
        "groups": {name: len(owner_rows) for name, owner_rows in grouped.items()},
        "dispatch_owners": _dispatch_owners(),
        "assignment_stats": assignment_stats,
        "assignment_updates": assignment_updates,
        "queued_total": len(queued),
        "queued_preview": [
            {
                "record_id": row["record_id"],
                "company": row["company"],
                "country": row["country"],
                "assigned_owner": row["owner"],
                "reason": row.get("assignment_reason", ""),
                "url": row["url"],
            }
            for row in queued[:20]
        ],
        "preview": preview,
        "message_ids": message_ids,
        "send_errors": send_errors,
    }

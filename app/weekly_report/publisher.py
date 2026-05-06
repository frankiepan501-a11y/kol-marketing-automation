"""飞书发布层.

4 个动作 (按依赖顺序):
1. 创建 docx 在飞书 wiki 下 (env WEEKLY_REPORT_PARENT_NODE 指定父节点, 默认是「SEO 文章周报」节点)
   + 写 markdown blocks (heading/paragraph/list/code/blockquote)
2. 入「SEO 周报历史数据」Bitable (KiQQbf7HxaT8TKsYToecfo86noc / tblp8TQhBnWI7Ax9)
3. 飞书消息推送 (Frankie + 独立站运营专员, Phase 3.1 用硬编码 open_id 列表, Phase 3.2 改 job_title 查)
4. 数据缺口告警 (有 gaps 时单独推 Frankie)

复用 app.feishu.api / token, 不重复造 OAuth 轮.
"""
import datetime
import json
import logging
import os
import re

log = logging.getLogger("weekly_report.publisher")

# 收件人列表 (Phase 3.1 硬编码, Phase 3.2 改 job_title 动态查 + 兜底)
RECIPIENTS_OPEN_IDS = [
    ("Frankie", "ou_629ce01f4bc31de078e10fcb038dbf78"),
    ("张佳烨", "ou_d850dab47bdbaea6736709d354de4b0f"),
]

# 飞书 wiki 父节点 - 周报 docx 挂这里
# .strip() 防 user 在 Zeabur env paste 时多个前导/尾随空格 (生产踩过坑)
DEFAULT_PARENT_NODE = os.environ.get("WEEKLY_REPORT_PARENT_NODE", "").strip()
DEFAULT_SPACE_ID = os.environ.get("WEEKLY_REPORT_SPACE_ID", "7610698300903214305").strip()

# 历史 Bitable
HISTORY_APP = "KiQQbf7HxaT8TKsYToecfo86noc"
HISTORY_TABLE = "tblp8TQhBnWI7Ax9"


# ============== 1. markdown → docx blocks ==============
def _md_to_blocks(markdown: str) -> list:
    """把 markdown 转成飞书 docx blocks 列表 (顶层 children).

    支持: # h1 / ## h2 / ### h3 / **bold** 段落 / - bullet / 1. ordered / > blockquote / ``` code.
    不支持: 表格 (Phase 3.2 加, 飞书 table block 结构复杂).
    表格 markdown 保留为代码块, 至少能看.
    """
    import uuid
    blocks = []
    lines = markdown.split("\n")
    i = 0
    while i < len(lines):
        line = lines[i]

        # code block ```
        if line.startswith("```"):
            code_lines = []
            i += 1
            while i < len(lines) and not lines[i].startswith("```"):
                code_lines.append(lines[i])
                i += 1
            i += 1  # skip closing ```
            blocks.append(_block_code("\n".join(code_lines)))
            continue

        # blockquote >
        if line.startswith("> "):
            blocks.append(_block_quote_text(line[2:].strip()))
            i += 1
            continue

        # heading
        if line.startswith("# "):
            blocks.append(_block_heading(1, line[2:].strip()))
        elif line.startswith("## "):
            blocks.append(_block_heading(2, line[3:].strip()))
        elif line.startswith("### "):
            blocks.append(_block_heading(3, line[4:].strip()))
        # bullet list
        elif re.match(r"^[\-\*] ", line):
            blocks.append(_block_bullet(line[2:].strip()))
        # ordered list
        elif re.match(r"^\d+\.\s", line):
            blocks.append(_block_ordered(re.sub(r"^\d+\.\s", "", line)))
        # 表格 row (markdown table 转成 code 段落保留可读)
        elif "|" in line and i + 1 < len(lines) and re.match(r"^[\|\s\-:]+$", lines[i + 1] or ""):
            tbl_lines = [line]
            i += 1
            while i < len(lines) and "|" in lines[i]:
                tbl_lines.append(lines[i])
                i += 1
            blocks.append(_block_code("\n".join(tbl_lines)))
            continue
        # 空行
        elif not line.strip():
            pass  # 跳过, 不生成空 block
        # 普通段落 (含 **bold** 用 inline_code 简化)
        else:
            blocks.append(_block_text(line))

        i += 1

    # 给每个 block 配 temp id
    for idx, b in enumerate(blocks):
        b["block_id"] = f"wr_{idx:03d}_{uuid.uuid4().hex[:8]}"
    return blocks


def _t(content: str, bold: bool = False) -> dict:
    style = {}
    if bold:
        style["bold"] = True
    return {"text_run": {"content": content, "text_element_style": style}}


def _parse_inline(text: str) -> list:
    """把 **bold** 分段成 elements 数组."""
    parts = re.split(r"(\*\*[^*]+\*\*)", text)
    elements = []
    for p in parts:
        if not p:
            continue
        if p.startswith("**") and p.endswith("**"):
            elements.append(_t(p[2:-2], bold=True))
        else:
            elements.append(_t(p))
    return elements or [_t("")]


def _block_heading(level: int, text: str) -> dict:
    field = f"heading{level}"
    return {"block_type": 2 + level, field: {"elements": [_t(text)]}}


def _block_text(text: str) -> dict:
    return {"block_type": 2, "text": {"elements": _parse_inline(text)}}


def _block_bullet(text: str) -> dict:
    return {"block_type": 12, "bullet": {"elements": _parse_inline(text)}}


def _block_ordered(text: str) -> dict:
    return {"block_type": 13, "ordered": {"elements": _parse_inline(text)}}


def _block_quote_text(text: str) -> dict:
    """飞书 docx 的 blockquote 是用 quote_container (块组) - 简化为 text 加 [quote] 前缀."""
    return {"block_type": 2, "text": {"elements": [_t("💬 " + text)]}}


def _block_code(content: str) -> dict:
    return {
        "block_type": 14,
        "code": {
            "elements": [{"text_run": {"content": content}}],
            "style": {"language": 49},
        },
    }


# ============== 2. 创建 docx + 写内容 ==============
async def _create_docx_in_wiki(title: str, parent_node: str, space_id: str) -> dict:
    """在 wiki 节点下创建 docx, 返回 {node_token, obj_token}."""
    from app import feishu
    body = {
        "obj_type": "docx",
        "parent_node_token": parent_node,
        "node_type": "origin",
        "title": title,
    }
    r = await feishu.api("POST", f"/wiki/v2/spaces/{space_id}/nodes", body)
    node = r["data"]["node"]
    return {"node_token": node["node_token"], "obj_token": node["obj_token"]}


async def _write_docx_blocks(doc_obj: str, blocks: list) -> int:
    """往 docx 写顶层 blocks. 返回写入数量. 分片 < 500 blocks/批."""
    from app import feishu
    if not blocks:
        return 0
    BATCH = 400
    total = 0
    cursor = 0
    while cursor < len(blocks):
        batch = blocks[cursor:cursor + BATCH]
        body = {
            "index": cursor,
            "children_id": [b["block_id"] for b in batch],
            "descendants": batch,
        }
        await feishu.api("POST", f"/docx/v1/documents/{doc_obj}/blocks/{doc_obj}/descendant", body)
        total += len(batch)
        cursor += BATCH
    return total


# ============== 3. 历史 Bitable 入库 ==============
def _safe_get(d: dict, *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
        if cur is None:
            return default
    return cur


def _build_bitable_fields(collected: dict, start_date, end_date, doc_url: str) -> dict:
    """组装历史表 21 字段 (容错: collector 失败时跳过对应字段, 不写空值)."""
    fields = {
        "周次": f"{start_date}~{end_date}",
        "起始日期": int(datetime.datetime.combine(start_date, datetime.time.min).timestamp() * 1000),
        "文档链接": {"link": doc_url, "text": "查看周报"} if doc_url else None,
    }

    # GA4 双站
    pk = _safe_get(collected, "ga4", "data", "powkong", "core") or {}
    fl = _safe_get(collected, "ga4", "data", "funlab", "core") or {}
    if pk.get("active_users") is not None:
        fields["Powkong浏览量"] = pk.get("sessions")
        fields["Powkong访客"] = pk.get("active_users")
        fields["Powkong跳出率"] = pk.get("bounce_rate")
        fields["Powkong平均停留"] = pk.get("avg_engagement_time")
    if fl.get("active_users") is not None:
        fields["Funlab浏览量"] = fl.get("sessions")
        fields["Funlab访客"] = fl.get("active_users")
        fields["Funlab跳出率"] = fl.get("bounce_rate")
        fields["Funlab平均停留"] = fl.get("avg_engagement_time")

    # GSC
    pk_g = _safe_get(collected, "gsc", "data", "powkong", "summary") or {}
    fl_g = _safe_get(collected, "gsc", "data", "funlab", "summary") or {}
    if pk_g.get("clicks") is not None:
        fields["Powkong GSC点击"] = pk_g.get("clicks")
        fields["Powkong GSC展现"] = pk_g.get("impressions")
    if fl_g.get("clicks") is not None:
        fields["Funlab GSC点击"] = fl_g.get("clicks")
        fields["Funlab GSC展现"] = fl_g.get("impressions")

    # 博客占比 (来自 GA4 utm_kol 或简化)
    # Phase 3.2: 加博客占比字段 (GA4 dimension landingPagePath ~ /blogs/news/)

    # drop None values
    return {k: v for k, v in fields.items() if v is not None}


async def _write_history_bitable(fields: dict) -> dict:
    from app import feishu
    body = {"fields": fields}
    r = await feishu.api(
        "POST", f"/bitable/v1/apps/{HISTORY_APP}/tables/{HISTORY_TABLE}/records", body)
    return r.get("data", {}).get("record") or {}


# ============== 4. 飞书消息推送 ==============
def _build_card(title: str, summary: str, doc_url: str, gaps: list) -> dict:
    """飞书 interactive 卡片 (v1 schema, 默认). 标题 + 关键洞察 + docx 链接按钮 + 缺口标."""
    elements = [
        {"tag": "div", "text": {"tag": "lark_md", "content": summary}},
    ]
    if gaps:
        elements.append({
            "tag": "div", "text": {
                "tag": "lark_md",
                "content": f"⚠️ **数据缺口 {len(gaps)} 项** — 部分 collector 失败, 详见周报「数据缺口」段",
            }
        })
    if doc_url:
        elements.append({
            "tag": "action",
            "actions": [{
                "tag": "button", "text": {"tag": "plain_text", "content": "查看完整周报"},
                "type": "primary", "url": doc_url,
            }],
        })
    # v1 schema: 顶层 elements (无 schema 字段, 不要写 "schema": "2.0")
    return {
        "config": {"wide_screen_mode": True},
        "header": {"title": {"tag": "plain_text", "content": title}, "template": "blue"},
        "elements": elements,
    }


async def _send_card(open_id: str, card: dict):
    """用 notify app 发卡片. 返回 (ok, error_msg)."""
    from app import feishu
    body = {
        "receive_id": open_id,
        "msg_type": "interactive",
        "content": json.dumps(card, ensure_ascii=False),
    }
    try:
        await feishu.api("POST", "/im/v1/messages?receive_id_type=open_id", body, which="notify")
        return True, ""
    except Exception as e:
        msg = f"{type(e).__name__}: {str(e)[:300]}"
        log.warning("send_card to %s failed: %s", open_id, msg)
        return False, msg


def _summary_from_collected(collected: dict, start_date, end_date) -> str:
    """从 collected 抽 5 条关键洞察作为消息开头."""
    pk_ga = _safe_get(collected, "ga4", "data", "powkong", "core") or {}
    fl_ga = _safe_get(collected, "ga4", "data", "funlab", "core") or {}
    pk_meta = _safe_get(collected, "meta_ads", "data", "powkong", "summary") or {}

    insights = []
    if pk_ga.get("sessions"):
        insights.append(f"📊 Powkong 会话 {pk_ga.get('sessions')} / 收入 ${pk_ga.get('total_revenue', 0):.0f}")
    if fl_ga.get("sessions"):
        insights.append(f"📊 FUNLAB 会话 {fl_ga.get('sessions')} / 收入 ${fl_ga.get('total_revenue', 0):.0f}")
    if pk_meta.get("spend"):
        insights.append(f"💰 PK Meta 花费 ${pk_meta.get('spend', 0):.0f} / ROAS {pk_meta.get('roas', 0):.2f}")

    psi = _safe_get(collected, "psi", "data", "summary") or {}
    if psi.get("performance_avg") is not None:
        red = len(psi.get("red_flags") or [])
        insights.append(f"⚡ Lighthouse 平均 Performance {psi.get('performance_avg')} (🔴 {red})")

    cmpl = _safe_get(collected, "complaints", "data", "powkong") or {}
    if cmpl.get("total"):
        insights.append(f"📩 PK 客诉 {cmpl.get('total')} / 升级率 {cmpl.get('escalation_rate', 0)*100:.0f}%")

    if not insights:
        insights = ["⚠️ 本周数据全部缺口, 请查 dtc-weekly 服务日志"]

    return f"**{start_date} ~ {end_date}**\n\n" + "\n".join(insights[:5])


# ============== 主入口 ==============
async def publish(html_str: str, markdown: str, collected: dict, start_date, end_date, gaps: list) -> dict:
    """4 步发布."""
    log.info("publisher.publish %s ~ %s, html=%dB md=%dB gaps=%d",
              start_date, end_date, len(html_str), len(markdown), len(gaps))

    week_label = f"W{start_date.isocalendar()[1]}"
    title = f"双品牌运营周报 {week_label} · {start_date}~{end_date}"

    result = {"week": week_label, "actions": []}
    doc_url = ""

    # 1. 创建 docx + 写 blocks
    if DEFAULT_PARENT_NODE:
        try:
            tokens = await _create_docx_in_wiki(title, DEFAULT_PARENT_NODE, DEFAULT_SPACE_ID)
            blocks = _md_to_blocks(markdown)
            written = await _write_docx_blocks(tokens["obj_token"], blocks)
            doc_url = f"https://u1wpma3xuhr.feishu.cn/wiki/{tokens['node_token']}"
            result["actions"].append({"step": "docx_create", "ok": True,
                                        "node": tokens["node_token"], "blocks": written, "url": doc_url})
        except Exception as e:
            log.exception("docx create failed")
            result["actions"].append({"step": "docx_create", "ok": False, "error": str(e)})
    else:
        result["actions"].append({"step": "docx_create", "ok": False,
                                    "error": "WEEKLY_REPORT_PARENT_NODE env 未设, 跳过 docx 创建"})

    # 2. Bitable 入历史
    try:
        fields = _build_bitable_fields(collected, start_date, end_date, doc_url)
        rec = await _write_history_bitable(fields)
        result["actions"].append({"step": "bitable_write", "ok": True,
                                    "record_id": rec.get("record_id"), "fields_count": len(fields)})
    except Exception as e:
        log.exception("bitable write failed")
        result["actions"].append({"step": "bitable_write", "ok": False, "error": str(e)})

    # 3. 飞书消息推送
    summary = _summary_from_collected(collected, start_date, end_date)
    card = _build_card(title, summary, doc_url, gaps)
    notified = []
    for name, oid in RECIPIENTS_OPEN_IDS:
        ok, err = await _send_card(oid, card)
        notified.append({"name": name, "ok": ok, "error": err})
    result["actions"].append({"step": "notify", "recipients": notified})

    # 4. 数据缺口告警 (单独发 Frankie, 卡片更显眼)
    if gaps:
        gap_card = _build_card(
            f"⚠️ 周报数据缺口 {len(gaps)} 项",
            "**本周以下 collector 失败, 周报对应 section 已标注「数据缺口」**:\n" +
            "\n".join(f"- {g}" for g in gaps[:10]),
            doc_url,
            [],
        )
        ok, err = await _send_card("ou_629ce01f4bc31de078e10fcb038dbf78", gap_card)
        result["actions"].append({"step": "gap_alert", "ok": ok, "error": err, "gap_count": len(gaps)})

    return result

"""回复监听 - 迁移自本地 scripts/send_loop/reply_monitor.py"""
import re, time, html as html_mod
from . import config, feishu, zoho, deepseek, reply_drafter
from .feishu import ext, xrid

POSITIVE = {"感兴趣", "要报价"}
INTENT_TO_STATUS_KOL = {
    "感兴趣": "洽谈中", "要报价": "洽谈中",
    "委婉拒绝": "不合适", "退订": "黑名单",
    "不明意图": None,
}
INTENT_TO_STATUS_EDITOR = {
    "感兴趣": "洽谈中", "要报价": "洽谈中",
    "委婉拒绝": "不合适", "退订": "不合适",
    "不明意图": None,
}
INTENT_EMOJI = {
    "感兴趣": "✅", "要报价": "💰", "委婉拒绝": "⚠️", "退订": "🛑", "不明意图": "❓",
}


def parse_email(addr: str) -> str:
    m = re.search(r'[\w.+-]+@[\w-]+\.[\w.-]+', addr or "")
    return m.group(0).lower() if m else ""


def html_to_text(s: str) -> str:
    s = re.sub(r'<br\s*/?>', '\n', s or '', flags=re.I)
    s = re.sub(r'</(p|div|h[1-6]|li)>', '\n', s, flags=re.I)
    s = re.sub(r'<[^>]+>', '', s)
    s = html_mod.unescape(s)
    return re.sub(r'\n{3,}', '\n\n', s).strip()


async def classify_intent(from_addr: str, subject: str, body: str):
    prompt = f"""你在审核一封 KOL/编辑 回复我们 cold outreach 邮件,判断其意图并给出建议。

【回复】
From: {from_addr}
Subject: {subject}
Body (前 800 字):
{body[:800]}

【意图类型】
- 感兴趣: 想了解/收到产品/看样品/问细节
- 要报价: 询问价格/佣金/合作条款
- 委婉拒绝: "不适合""暂无档期""不感兴趣"
- 退订: unsubscribe/please remove
- 不明意图: out-of-office/自动回复/无法判断

返回 JSON:
{{"type":"感兴趣|要报价|委婉拒绝|退订|不明意图","confidence":0.0-1.0,"summary":"一句总结","key_quote":"原文 1 句","suggested_action":"下一步建议"}}"""
    try:
        return await deepseek.chat_json(prompt, max_tokens=400)
    except Exception as e:
        return {"type": "不明意图", "confidence": 0.0, "summary": f"API错误: {e}",
                "key_quote": "", "suggested_action": "人工查看"}


async def find_kol_by_email(email: str):
    items = await feishu.search_records(config.T_KOL, [
        {"field_name": "邮箱", "operator": "contains", "value": [email.strip().lower()]}
    ])
    return items[0] if items else None


async def find_editor_by_email(email: str):
    items = await feishu.search_records(config.T_EDITOR, [
        {"field_name": "邮箱", "operator": "contains", "value": [email.strip().lower()]}
    ])
    return items[0] if items else None


async def find_contact(email: str):
    rec = await find_kol_by_email(email)
    if rec: return rec, "KOL"
    rec = await find_editor_by_email(email)
    if rec: return rec, "editor"
    return None, None


async def find_draft(contact_rid: str, contact_type: str):
    link_field = "关联编辑" if contact_type == "editor" else "关联KOL"
    items = await feishu.search_records(config.T_DRAFT, [
        {"field_name": "草稿状态", "operator": "is", "value": ["已发送"]}
    ])
    for rec in items:
        if xrid(rec["fields"].get(link_field)) == contact_rid:
            return rec
    return None


def build_card(contact_type: str, contact_info: dict, brand: str, intent: dict, subject: str):
    intent_type = intent.get("type", "?")
    emoji = INTENT_EMOJI.get(intent_type, "📬")
    conf = intent.get("confidence", 0)
    base_url = f"https://u1wpma3xuhr.feishu.cn/base/{config.FEISHU_APP_TOKEN}"
    target_table = config.T_EDITOR if contact_type == "editor" else config.T_KOL
    return {
        "header": {
            "template": "green" if intent_type in ("感兴趣", "要报价") else "orange" if intent_type == "不明意图" else "red",
            "title": {"tag": "plain_text", "content": f"{emoji} {'编辑' if contact_type=='editor' else 'KOL'} 回复 — {intent_type}"}
        },
        "elements": [
            {"tag": "div", "fields": [
                {"is_short": True, "text": {"tag": "lark_md", "content": f"**姓名**: {contact_info['name']}"}},
                {"is_short": True, "text": {"tag": "lark_md", "content": f"**来源**: {contact_info['source']}"}},
                {"is_short": True, "text": {"tag": "lark_md", "content": f"**国家**: {contact_info['country']}"}},
                {"is_short": True, "text": {"tag": "lark_md", "content": f"**品牌**: {brand}"}},
                {"is_short": True, "text": {"tag": "lark_md", "content": f"**置信度**: {conf:.0%}"}},
                {"is_short": True, "text": {"tag": "lark_md", "content": f"**邮箱**: {contact_info['email']}"}},
            ]},
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"**📝 意图总结**\n{intent.get('summary','')}"}},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"**💬 原话**\n> {intent.get('key_quote','')[:200]}"}},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"**➡️ 建议行动**\n{intent.get('suggested_action','')}"}},
            {"tag": "hr"},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"**原主题**: {subject}"}},
            {"tag": "action", "actions": [
                {"tag": "button", "text": {"tag": "plain_text", "content": f"打开{'编辑' if contact_type=='editor' else 'KOL'}主表"},
                 "url": f"{base_url}?table={target_table}", "type": "primary"},
                {"tag": "button", "text": {"tag": "plain_text", "content": "打开外联草稿"},
                 "url": f"{base_url}?table={config.T_DRAFT}", "type": "default"},
            ]},
        ]
    }


async def notify_all(card):
    # 群
    try: await feishu.send_card_message("chat_id", config.NOTIFY_CHAT_ID, card)
    except Exception as e: print(f"notify chat fail: {e}")
    # 个人
    for name, oid in config.NOTIFY_USERS:
        try: await feishu.send_card_message("open_id", oid, card)
        except Exception as e: print(f"notify {name} fail: {e}")


async def run():
    processed = 0
    results = []
    for brand in ("POWKONG", "FUNLAB"):
        alias = config.BRAND_CONFIG[brand]["alias_from"]
        try:
            msgs = await zoho.search_inbox(brand, f"to:{alias}", limit=30)
        except Exception as e:
            results.append({"brand": brand, "error": str(e)[:200]})
            continue

        for msg in msgs:
            from_addr = parse_email(msg.get("fromAddress") or msg.get("sender") or "")
            if not from_addr or alias.lower() in from_addr.lower():
                continue
            subject = msg.get("subject", "")
            msg_id = msg.get("messageId") or msg.get("summary")
            folder_id = msg.get("folderId")

            contact, ctype = await find_contact(from_addr)
            if not contact: continue

            draft = await find_draft(contact["record_id"], ctype)
            if not draft: continue
            if draft["fields"].get("是否回复"): continue

            # 拉正文
            body_html = ""
            if folder_id:
                try: body_html = await zoho.get_message_content(brand, msg_id, folder_id)
                except Exception: pass
            email_body = html_to_text(body_html) or msg.get("summary", "") or subject

            # 分类
            intent = await classify_intent(from_addr, subject, email_body)
            intent_type = intent.get("type", "不明意图")

            # 回写草稿
            await feishu.update_record(config.T_DRAFT, draft["record_id"], {
                "是否回复": True,
                "回复日期": int(time.time() * 1000),
                "回复意图": intent_type,
                "回复原文": email_body[:500],
            })

            # 更新主表状态 + 跟进记录
            cf = contact["fields"]
            if ctype == "editor":
                new_status = INTENT_TO_STATUS_EDITOR.get(intent_type)
                if new_status:
                    await feishu.update_record(config.T_EDITOR, contact["record_id"], {"合作状态": new_status})
                await feishu.create_record(config.T_EDITOR_FU, {
                    "跟进摘要": f"[编辑回复] {intent_type}: {intent.get('summary','')[:80]}",
                    "跟进日期": int(time.time() * 1000),
                    "跟进方式": "邮件",
                    "跟进内容": f"主题: {subject}\n\n意图: {intent_type} (置信度{intent.get('confidence',0):.2f})\n\n原文:\n{email_body[:600]}",
                    "客户反馈": intent.get("key_quote", "")[:200],
                    "下一步行动": intent.get("suggested_action", "")[:200],
                    "关联编辑": [contact["record_id"]],
                })
                source = ext(cf.get("主要媒体")) or ext(cf.get("所属媒体"))
                name = ext(cf.get("编辑姓名"))
            else:
                new_status = INTENT_TO_STATUS_KOL.get(intent_type)
                if new_status:
                    await feishu.update_record(config.T_KOL, contact["record_id"], {"合作状态": new_status})
                await feishu.create_record(config.T_KOL_FU, {
                    "跟进摘要": f"[KOL回复] {intent_type}: {intent.get('summary','')[:80]}",
                    "跟进日期": int(time.time() * 1000),
                    "跟进方式": "邮件",
                    "跟进内容": f"主题: {subject}\n\n意图: {intent_type} (置信度{intent.get('confidence',0):.2f})\n\n原文:\n{email_body[:600]}",
                    "客户反馈": intent.get("key_quote", "")[:200],
                    "下一步行动": intent.get("suggested_action", "")[:200],
                    "关联KOL": [contact["record_id"]],
                })
                source = ext(cf.get("主平台")) + f" {cf.get('粉丝数', 0):,} 粉"
                name = ext(cf.get("账号名"))

            # 通知
            contact_info = {
                "name": name, "source": source,
                "country": ext(cf.get("国家原文")) or ext(cf.get("国家")),
                "email": from_addr,
            }
            card = build_card(ctype, contact_info, brand, intent, subject)
            await notify_all(card)

            # === 自动生成回复草稿 (走 reviewer 自审通道) ===
            try:
                alias_for_brand = config.BRAND_CONFIG[brand]["alias_from"]
                reply_rid = await reply_drafter.draft_reply(
                    contact_record=contact,
                    contact_type=ctype,
                    brand=brand,
                    intent_type=intent_type,
                    intent_summary=intent.get("summary", ""),
                    original_subject=subject,
                    original_body=email_body,
                    sender_alias=alias_for_brand,
                    related_draft_id=draft["record_id"],
                )
                if reply_rid:
                    print(f"[reply_monitor] reply draft generated rid={reply_rid}")
            except Exception as e:
                print(f"[reply_monitor] draft_reply fail: {e}")

            processed += 1

    return {"processed": processed, "results": results}

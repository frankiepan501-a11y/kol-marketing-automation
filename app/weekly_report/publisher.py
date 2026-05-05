"""飞书发布层.

4 个动作:
1. 在「SEO 文章周报」wiki 节点下创建 docx「双品牌运营周报 W{xx} · {start}~{end}」
   - 内容: markdown 转 docx blocks (descendant API)
   - 末尾贴 HTML 渲染图链接
2. 入「SEO 周报历史数据」Bitable (KiQQbf7HxaT8TKsYToecfo86noc / tblp8TQhBnWI7Ax9)
3. 飞书消息推送 (动态查 job_title='独立站运营专员' + Frankie 兜底)
4. 数据缺口告警 (单独消息推 Frankie + 张佳烨)
"""
import logging

log = logging.getLogger("weekly_report.publisher")


async def publish(html: str, markdown: str, collected: dict, start_date, end_date, gaps: list) -> dict:
    """发布周报.

    Phase 3 实现:
    - feishu.create_docx_under_wiki(parent_node='SEO 文章周报 node_token')
    - feishu.descendant_blocks(doc_id, markdown_to_blocks(markdown))
    - feishu.bitable_create_record(KiQQbf7HxaT8TKsYToecfo86noc, 21 字段)
    - feishu.send_message(...) 给独立站运营专员 + Frankie
    - if gaps: feishu.send_alert(gaps) 给 Frankie + 张佳烨

    复用 app.feishu 已有函数, 不重写.
    """
    log.info("[STUB] publisher.publish html_len=%d, gaps=%d", len(html), len(gaps))
    return {
        "stub": True,
        "html_size": len(html),
        "would_create_docx": f"双品牌运营周报 · {start_date}~{end_date}",
        "would_notify": ["frankie", "独立站运营专员"],
        "gaps_alerted": len(gaps) > 0,
    }

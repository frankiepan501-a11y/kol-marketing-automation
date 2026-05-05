"""HTML 渲染层. Jinja2 + W15 模板.

输入: integrator 产出的 markdown + 原始 collected_data (含数值供模板用)
输出: 完整 HTML 字符串 (暗黑设计风, 12 sections, 红黄绿色标自动)
"""
import logging
import datetime

log = logging.getLogger("weekly_report.renderer")


async def render(markdown: str, collected: dict, start_date, end_date) -> str:
    """渲染 HTML.

    Phase 3 实现:
    - 加载 templates/weekly.html.j2
    - context = { week, start, end, brands.pk, brands.fl, ... }
    - jinja2.Template(...).render(**context)
    - 移动端 responsive 已在 W15 模板验证
    """
    log.info("[STUB] renderer.render %s ~ %s, md_len=%d", start_date, end_date, len(markdown))
    return f"""<html><body><pre>{markdown}</pre><p>STUB rendered at {datetime.datetime.now().isoformat()}</p></body></html>"""

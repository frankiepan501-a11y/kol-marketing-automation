r"""HTML 渲染层. 用 jinja2 模板 + data_shaper view model 渲染 W18 风周报.

Phase 3.2 (2026-05-06): 从纯 markdown 渲染升级为结构化 view model 驱动.
- markdown (integrator 出) → 仅作为 AI 文字洞察, 在每段底部 callout 嵌入
- 数据可视化 (指标卡 / 漏斗 / 流量分布 / GSC 关键词) → collected_data + data_shaper

模板: templates/weekly.html.j2 (基于 D:/Desktop/weekly_report_W18_prototype.html v3)
- 6 个核心段数据驱动: 01 总览 / 02 PK GA4 / 03 PK Meta / 04 PK SEO / 06 FL GA4 / 07 FL SEO
- 其他段保持占位 (02.5 国家分布 / 02.6 产品销量 / 03.5 Google Ads / 05 落地页 /
  06.5 FL 国家 / 06.55 FL 产品 / 06.6 FL Meta / 06.7 FL Google Ads / 08 任务 /
  09 KOL/UTM / 10 客诉 / 11 SEO 产能 / 12 Lighthouse). W19 之前 follow-up 补.
"""
import logging
import os

from . import data_shaper

log = logging.getLogger("weekly_report.renderer")

TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")
TEMPLATE_NAME = "weekly.html.j2"


def _build_env():
    """构建 jinja2 环境. lazy import 避免模块 import 时强制依赖."""
    from jinja2 import Environment, FileSystemLoader, select_autoescape
    env = Environment(
        loader=FileSystemLoader(TEMPLATE_DIR),
        autoescape=select_autoescape(default=False),  # 我们手动过滤 (insights_html | safe)
        trim_blocks=True,
        lstrip_blocks=True,
    )
    return env


async def render(markdown: str, collected: dict, start_date, end_date) -> str:
    """渲染 HTML.

    输入:
      markdown - integrator 产出的 12 sections markdown (用于 AI 文字洞察嵌入)
      collected - 各 collector 的原始结果
      start_date / end_date - 周区间

    输出: 完整 self-contained HTML.
    """
    log.info("renderer.render %s ~ %s, md_len=%d", start_date, end_date, len(markdown or ""))

    # 1. 构造 view model
    view_model = data_shaper.shape(
        collected=collected,
        start_date=start_date,
        end_date=end_date,
        gaps=[],  # gaps 由 main.py 传过来时填, 这里 view_model 内部用 dict
        markdown_insights=markdown or "",
    )

    # 2. 渲染 jinja2 模板
    try:
        env = _build_env()
        tpl = env.get_template(TEMPLATE_NAME)
        html = tpl.render(**view_model)
        log.info("renderer.render done. html_size=%d", len(html))
        return html
    except Exception as e:
        log.exception("renderer jinja2 render failed, falling back to markdown")
        return _fallback_markdown_render(markdown, start_date, end_date, e)


def _fallback_markdown_render(markdown: str, start_date, end_date, err) -> str:
    """jinja2 渲染失败时的兜底: 简易 markdown→HTML, 至少让运营拿到内容."""
    import datetime as _dt
    import html as _html
    try:
        import markdown as md_lib
        body = md_lib.markdown(markdown or "", extensions=["tables", "fenced_code", "nl2br"])
    except Exception:
        body = f"<pre>{_html.escape(markdown or '')}</pre>"
    week = f"W{start_date.isocalendar()[1]}"
    return f"""<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">
<title>双品牌运营周报 {week} (fallback)</title>
<style>body{{font-family:system-ui;max-width:900px;margin:24px auto;padding:0 20px;background:#0a0c10;color:#e8eaf0;}}
table{{border-collapse:collapse;width:100%;margin:16px 0;}}
th,td{{border:1px solid #333;padding:6px 10px;}}
.warn{{background:#7f1d1d;color:#fff;padding:12px;border-radius:6px;margin-bottom:16px;}}</style>
</head><body>
<div class="warn">⚠️ jinja2 模板渲染失败, fallback 到 markdown 兜底输出. 错误: {_html.escape(str(err))}</div>
<h1>双品牌运营周报 {week} · {start_date} ~ {end_date}</h1>
{body}
</body></html>"""

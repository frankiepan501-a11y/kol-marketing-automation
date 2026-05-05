"""HTML 渲染层. markdown → HTML body, 套 W15 暗黑风 CSS wrapper.

输入:
- markdown: integrator 产出的 12 sections markdown
- collected: 原始 collected_data (供后续可参数化扩展)
- start_date / end_date: 周区间

输出: 完整 self-contained HTML (含 CSS, 可直接看 / 上传飞书云盘)

Phase 3.1: 用 markdown lib + 内嵌 CSS, 不复杂 jinja2.
Phase 3.2 升级方向: 完整 W15 模板参数化 (12 sections 全独立组件)
"""
import logging
import datetime
import html

log = logging.getLogger("weekly_report.renderer")


# W15 暗黑风核心 CSS (基于 D:\Desktop\weekly_report_0406_0412.html, 精简版)
CSS = """
* { margin: 0; padding: 0; box-sizing: border-box; }
body {
  background: #0a0c10; color: #e8eaf0;
  font-family: 'Noto Sans SC', -apple-system, sans-serif;
  font-size: 14px; line-height: 1.7;
  padding: 40px 24px 80px;
}
.shell { max-width: 1100px; margin: 0 auto; }
.report-header {
  border-bottom: 1px solid rgba(255,255,255,0.07);
  padding-bottom: 24px; margin-bottom: 36px;
}
h1 {
  font-family: 'Space Grotesk', sans-serif;
  font-size: 28px; font-weight: 700; color: #fff;
  letter-spacing: -0.5px; margin-bottom: 10px;
}
h2 {
  font-family: 'Space Grotesk', sans-serif;
  font-size: 18px; font-weight: 600; color: #fff;
  margin: 36px 0 16px; padding-bottom: 8px;
  border-bottom: 1px solid rgba(255,255,255,0.07);
}
h3 { font-size: 15px; font-weight: 600; color: #fff; margin: 20px 0 10px; }
h4 { font-size: 13px; color: #93c5fd; margin: 14px 0 8px;
     font-family: 'DM Mono', monospace; letter-spacing: 0.05em; }
p { margin: 8px 0; }
strong { color: #fff; font-weight: 600; }
em { color: #93c5fd; font-style: normal; }
ul, ol { margin: 10px 0 10px 22px; }
li { margin: 4px 0; }
table {
  width: 100%; border-collapse: collapse;
  background: #111318; border: 1px solid rgba(255,255,255,0.07);
  border-radius: 12px; overflow: hidden; margin: 16px 0;
}
thead { background: rgba(255,255,255,0.03); }
th, td {
  padding: 10px 14px; text-align: left;
  border-bottom: 1px solid rgba(255,255,255,0.04);
  font-size: 13px;
}
th {
  font-family: 'DM Mono', monospace;
  font-size: 10px; letter-spacing: 0.08em;
  text-transform: uppercase; color: #6b7280;
}
td { font-family: 'DM Mono', monospace; }
tr:hover td { background: rgba(255,255,255,0.02); }
tr:last-child td { border-bottom: none; }
blockquote {
  background: rgba(245,158,11,0.06);
  border: 1px solid rgba(245,158,11,0.2);
  border-left: 4px solid #f59e0b;
  border-radius: 8px;
  padding: 14px 18px; margin: 16px 0;
  color: #fde68a;
}
code {
  font-family: 'DM Mono', monospace;
  background: rgba(255,255,255,0.05);
  padding: 2px 6px; border-radius: 3px;
  font-size: 12px; color: #93c5fd;
}
pre {
  background: #171b22; border: 1px solid rgba(255,255,255,0.07);
  border-radius: 8px; padding: 14px 18px; margin: 14px 0;
  overflow-x: auto; font-size: 12px;
}
pre code { background: none; padding: 0; color: #e8eaf0; }
.report-meta {
  display: flex; gap: 12px; margin-top: 8px;
  font-family: 'DM Mono', monospace;
  font-size: 11px; color: #6b7280;
}
.report-meta .pill {
  padding: 4px 10px; border-radius: 20px;
  background: #111318; border: 1px solid rgba(255,255,255,0.12);
}
.report-footer {
  margin-top: 60px; padding-top: 24px;
  border-top: 1px solid rgba(255,255,255,0.07);
  font-size: 11px; color: #6b7280;
  font-family: 'DM Mono', monospace;
}
/* 涨/跌 inline 配色 (LLM 输出含 ↑/↓ emoji 时手动找 + 套 span) */
.up { color: #10b981; }
.down { color: #ef4444; }
.amber { color: #f59e0b; }
"""


HTML_SHELL = """<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title}</title>
<link href="https://fonts.googleapis.com/css2?family=DM+Mono&family=Noto+Sans+SC:wght@400;500;700&family=Space+Grotesk:wght@600;700&display=swap" rel="stylesheet">
<style>{css}</style>
</head>
<body>
<div class="shell">
<div class="report-header">
  <h1>{title}</h1>
  <div class="report-meta">
    <span class="pill">Powkong + FUNLAB</span>
    <span class="pill">{start_date} ~ {end_date}</span>
    <span class="pill">生成 {generated_at}</span>
  </div>
</div>
{body}
<div class="report-footer">
  双品牌运营周报 · {start_date} ~ {end_date} · 数据来源: Shopify / Shopline / GA4 / GSC / Meta Ads / KOL 营销库 / Zoho 客服 / n8n / PSI<br>
  自动生成 by dtc-weekly module @ kol-auto.zeabur.app
</div>
</div>
</body>
</html>"""


def _markdown_to_html(md: str) -> str:
    """markdown → html body. 用 markdown lib (Phase 3.1)."""
    try:
        import markdown as md_lib
        return md_lib.markdown(md, extensions=["tables", "fenced_code", "nl2br", "sane_lists"])
    except ImportError:
        # 降级: 简易处理 (markdown 包未安装时, Zeabur 应该已装)
        log.warning("markdown lib not installed, using crude fallback")
        return f"<pre>{html.escape(md)}</pre>"


def _add_color_spans(html_body: str) -> str:
    """给 ↑/↓ 数字加颜色 span. LLM 输出可能含 ↑X% / ↓X%."""
    import re
    # ↑XX% / ↑XX → green
    html_body = re.sub(r'(↑[\d.]+%?)', r'<span class="up">\1</span>', html_body)
    html_body = re.sub(r'(↓[\d.]+%?)', r'<span class="down">\1</span>', html_body)
    # 🔴/🟡/🟢/✅ 保持 emoji
    return html_body


async def render(markdown: str, collected: dict, start_date, end_date) -> str:
    """渲染 HTML."""
    log.info("renderer.render %s ~ %s, md_len=%d", start_date, end_date, len(markdown))
    week = f"W{start_date.isocalendar()[1]}"
    title = f"双品牌运营周报 {week}"

    body_html = _markdown_to_html(markdown)
    body_html = _add_color_spans(body_html)

    return HTML_SHELL.format(
        title=html.escape(title),
        css=CSS,
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        generated_at=datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
        body=body_html,
    )

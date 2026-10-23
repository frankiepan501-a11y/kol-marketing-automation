"""Formatting-only subset of company feishu_title helper; no credentials or routing."""

LEVEL_EMOJI = {"P0": "🔴", "P1": "🟠", "P2": "🟡", "P3": "🟢"}
BIZ_TO_GROUP = dict.fromkeys(('AMZ', 'SEO', 'KOL', 'FIN', 'HR', 'PAY', 'INV', 'LOG', 'AOS', 'AUDIT', 'CUS', 'TEAM'))

def format_title(biz: str, level: str, title: str, suffix: str = "") -> str:
    """
    生成统一格式标题.

    >>> format_title("AMZ", "P1", "SP搜索词分析", "黄奕纯 · 2026-05-21")
    '🟠 [AMZ·P1] SP搜索词分析 · 黄奕纯 · 2026-05-21'
    """
    if biz not in BIZ_TO_GROUP:
        raise ValueError(f"unknown biz code: {biz!r} (允许: {list(BIZ_TO_GROUP)})")
    if level not in LEVEL_EMOJI:
        raise ValueError(f"unknown level: {level!r} (允许: P0/P1/P2/P3)")
    emoji = LEVEL_EMOJI[level]
    head  = f"{emoji} [{biz}·{level}] {title}"
    return f"{head} · {suffix}" if suffix else head

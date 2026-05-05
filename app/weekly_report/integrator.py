"""AI 整合层. 把 collected_data dict 整合成 12 sections markdown.

- model: Claude Sonnet 4.6 (高质量整合 + 长 context)
- system prompt 来源: 飞书 wiki 「自动化V1 - 独立站运营 - 周报新增模块 prompt 补丁 V1」
  (https://u1wpma3xuhr.feishu.cn/wiki/MnzuwihUBilANikarOBc4SZXnic)
  + Codex bundle 10-weekly-report-template.md (本地缓存或飞书读)
- 数据缺口处理: gaps 列表注入 user prompt, AI 输出「⚠️ 数据缺口」段而非编造
"""
import logging

log = logging.getLogger("weekly_report.integrator")


async def build_markdown(collected: dict, start_date, end_date, gaps: list) -> str:
    """整合 12 sections markdown.

    Phase 3 实现:
    - 读 system prompt (飞书 wiki 或本地缓存)
    - 拼 user prompt = 时间区间 + collected_data JSON + gaps 列表
    - 调 Claude API (复用 app.deepseek 模式) 拿到 markdown
    - 校验 markdown 至少有 12 个 H1 section
    """
    log.info("[STUB] integrator.build_markdown for %s ~ %s", start_date, end_date)
    return f"""# 双品牌运营周报 W?? · {start_date} ~ {end_date}

**[STUB]** 这是 Phase 1.1 骨架的占位 markdown.

**collectors 状态**:
{chr(10).join(f"- {k}: {v.get('status', '?')}" for k, v in collected.items())}

**数据缺口**:
{chr(10).join(f"- {g}" for g in gaps) if gaps else '无'}

Phase 3 实现真整合.
"""

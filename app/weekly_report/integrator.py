"""AI 整合层. 把 collected_data dict 整合成 12 sections markdown.

- model: DeepSeek Chat (env DEEPSEEK_API_KEY, 已有于 kol-auto)
  Phase 3.1 用 DeepSeek 起步, Phase 3.2 可换 Claude (新加 ANTHROPIC_API_KEY)
- system prompt: hardcode 12 sections 结构 + 数据缺口规则 (不去拉飞书 wiki - 减少依赖)
- user prompt: 时间区间 + collected_data JSON (只指标, 无 raw orders) + gaps 列表
- 数据缺口处理: gaps 列表注入 user prompt, AI 输出「⚠️ 数据缺口」段而非编造

完整 12 sections 规范见飞书 wiki:
- prompt 补丁 V1: https://u1wpma3xuhr.feishu.cn/wiki/MnzuwihUBilANikarOBc4SZXnic
- 周报模板: ~/colleague-bundles/role-specific/dtc-ops/10-weekly-report-template.md
"""
import asyncio
import json
import logging
import os

import httpx

log = logging.getLogger("weekly_report.integrator")

DEEPSEEK_ENDPOINT = "https://api.deepseek.com/chat/completions"
DEFAULT_MODEL = "deepseek-chat"
TIMEOUT = 180.0  # 长文本生成留 3 分钟


SYSTEM_PROMPT = """你是双品牌 (Powkong + FUNLAB) 独立站运营的周报生成助手. 输出 markdown.

# 必填 12 个 section (按顺序)

## 01. 双品牌核心指标总览
- Powkong Shopify 后台: 销售额/订单数/已发货/退款数/退款率
- FUNLAB Shopline 后台: 同上
- GA4 双品牌对比表 7 行 (活跃用户/会话/总收入/购买/转化率/跳出率/平均参与时长)
- 跨平台口径差异提醒 (Shopline > GA4 > Meta, 财务以 Shopline 为准)

## 02. Powkong 独立站 GA4 详情
- 6 metric 卡 + 流量来源 6 类 + 电商漏斗 (会话→加购→结账→购买)

## 03. Powkong Meta 广告
- 6 metric 卡 (花费/ROAS/CPA/CTR/购买/加购)
- 7 天 daily ROAS 曲线 + 漏斗

## 04. Powkong SEO & GSC
- 上周 vs 本周对比 (点击/展现/CTR/平均排名)
- 页面收录情况 + 异常分析 (高展现低 CTR 的关键词)

## 05. Powkong 落地页分析
- 主力落地页指标 (来自 Meta 广告) + 优化亮点 + 仍需优化方向

## 06. FUNLAB 独立站 GA4 详情 (同 02)

## 07. FUNLAB SEO & GSC (同 04, 含收录率与 PK 对比)

## 08. 下周任务清单 (4 张 insight 卡, PK 广告/PK SEO/FL 广告/FL SEO)

## 09. KOL/媒体人 ROI + UTM 流量贡献 ⭐ 新增
- KOL 端 + 媒体人端各 7 列指标 (派单/富化/已发/回复/意向率/拒绝率/寄样)
- UTM 流量贡献 (sessions/收入/Top 5 utm_campaign)
- ROI 估算

## 10. 客诉数 / 退款金额 / 异常订单 ⭐ 新增 ⭐ 双框架核心 KPI
- 总览 (客诉数/已解决/平均首响/退款数/金额)
- 类型分布 (物流/产品/退换货/咨询/其他)
- 异常订单 (断货/地址异常/卡 7 天/重复/欺诈)
- ⭐ 双框架 KPI 独立 callout: Frankie 升级率 / 张佳烨自决率 / 首响合规率 / 退款 ≤$100 自批比例
  - 目标: 升级率 < 20%, 自决率 > 80%, 首响合规 > 95%

## 11. SEO 自动化产能 + 选题池消费率 ⭐ 新增
- 6 工作流执行情况 (本周次数/成功/失败/失败原因)
- 内容产出 (SEO 新闻 12 篇/周, 商业意图 2 篇/周)
- 选题池状态 (按状态分布/新增/消费/低库存预警 < 10)

## 12. 页面性能 Lighthouse ⭐ 新增 (模块 L)
- 4 核心页 × 4 维度评分 (🟢≥90 / 🟡 50-89 / 🔴 <50)
- Performance 6 子项 (LCP/TBT/CLS/FCP/SI/INP)
- Top 3 优化建议 (按预估 LCP/TBT 节省时间排序)
- 与上周对比 (Performance 下降 > 5 分告警)

# 数据缺口处理铁律
- 输入会附 `gaps` 列表 (collector 失败的清单)
- 缺数据的 section 必须输出「⚠️ 数据缺口: <原因>」段, 严禁编造数字
- 零客诉时显式输出「✅ 本周零客诉」, 不要省略 section 10
- 跨平台数据冲突时显式标注差异 (如 Shopify $307 vs GA4 $180)

# 输出风格
- 简洁直接, 表格优先 (markdown table)
- 每个 section 末尾 1-2 句关键洞察
- 数字带环比 (↑/↓/→), 涨绿跌红 (用 emoji 或 markdown)
- 双框架 KPI (10.5 节) 用 > callout 块呈现, 不能并入普通段落
"""


async def _call_llm(system_prompt: str, user_prompt: str, max_tokens: int = 8000) -> str:
    """同步调 DeepSeek chat. 返回 raw markdown text."""
    api_key = os.environ.get("DEEPSEEK_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("DEEPSEEK_API_KEY env 未设")

    body = {
        "model": DEFAULT_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.2,
        "max_tokens": max_tokens,
    }
    async with httpx.AsyncClient(timeout=TIMEOUT) as cli:
        r = await cli.post(DEEPSEEK_ENDPOINT, json=body,
                            headers={"Authorization": f"Bearer {api_key}"})
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"]


def _trim_collected(collected: dict, max_chars_per_collector: int = 12000) -> dict:
    """裁掉过长的 raw 数据 (如 Shopify orders 数组). 保留指标摘要.

    避免 LLM context 超载. 每个 collector data 序列化后超 max_chars 时, 删 raw 字段.
    GSC / KOL Bitable 数据本身就大 (top10 queries + pages + blogs), 上限放宽到 12K.
    """
    out = {}
    for k, v in collected.items():
        if not isinstance(v, dict):
            out[k] = v
            continue
        s = json.dumps(v, ensure_ascii=False, default=str)
        if len(s) <= max_chars_per_collector:
            out[k] = v
        else:
            # 截取: 保留 status + data 的顶层指标, 删除嵌套 raw
            # 单 sub-key 上限放到 5000 (足够装 top 10 entries 详情)
            data = v.get("data", {})
            trimmed_data = {}
            for dk, dv in data.items():
                ds = json.dumps(dv, ensure_ascii=False, default=str)
                if len(ds) <= 5000:
                    trimmed_data[dk] = dv
                else:
                    trimmed_data[dk] = {"_truncated": True, "_size": len(ds)}
            out[k] = {"status": v.get("status"), "data": trimmed_data}
    return out


def _build_user_prompt(collected: dict, start_date, end_date, gaps: list) -> str:
    """组装 user prompt."""
    week_label = f"W{start_date.isocalendar()[1]}"
    trimmed = _trim_collected(collected)
    data_json = json.dumps(trimmed, ensure_ascii=False, indent=2, default=str)

    gaps_text = "\n".join(f"- {g}" for g in gaps) if gaps else "(无)"

    return f"""# 时间区间
{week_label}: {start_date} ~ {end_date}

# 数据缺口 (这些 collector 失败, 对应 section 输出「⚠️ 数据缺口」段, 不要编造)
{gaps_text}

# 收集到的数据 (JSON)
```json
{data_json}
```

# 任务
按 system prompt 的 12 sections 顺序生成 markdown 周报. 严守"数据缺口处理铁律".
"""


async def build_markdown(collected: dict, start_date, end_date, gaps: list) -> str:
    """整合 12 sections markdown."""
    log.info("integrator.build_markdown for %s ~ %s, gaps=%d", start_date, end_date, len(gaps))

    user_prompt = _build_user_prompt(collected, start_date, end_date, gaps)
    log.info("integrator user prompt size: %d chars", len(user_prompt))

    try:
        markdown = await _call_llm(SYSTEM_PROMPT, user_prompt)
    except Exception as e:
        log.exception("integrator LLM call failed")
        # 降级: 输出最简版 markdown 含原始数据 + gaps 列表 (运营至少能看)
        return _fallback_markdown(collected, start_date, end_date, gaps,
                                    error=f"{type(e).__name__}: {e}")

    # 校验 markdown 至少有几个 sections
    h1_count = markdown.count("\n## ") + (1 if markdown.startswith("## ") else 0)
    if h1_count < 8:
        log.warning("integrator output H2 count = %d (<8), might be incomplete", h1_count)

    return markdown


def _fallback_markdown(collected: dict, start_date, end_date, gaps: list, error: str) -> str:
    """LLM 失败时的兜底 markdown - 至少保证运营拿到原始数据."""
    week = f"W{start_date.isocalendar()[1]}"
    lines = [
        f"# 双品牌运营周报 {week} · {start_date} ~ {end_date}",
        "",
        f"⚠️ **AI 整合失败 - fallback 输出原始指标**",
        f"错误: `{error}`",
        "",
        "## 数据缺口",
    ]
    if gaps:
        lines.extend(f"- {g}" for g in gaps)
    else:
        lines.append("(无)")
    lines.extend(["", "## 各 collector 原始指标"])
    for k, v in collected.items():
        lines.append(f"\n### {k}")
        lines.append("```json")
        lines.append(json.dumps(v, ensure_ascii=False, indent=2, default=str)[:2000])
        lines.append("```")
    return "\n".join(lines)

# -*- coding: utf-8 -*-
"""自动关键词供给引擎 (2026-06-15; 2026-06-16 加多语言市场) — 让 YouTube daemon 持续有词抓.

背景(审计 project_kol_intake_audit_2026_06_11): 达人发现根因=关键词断供(脉冲式)。YouTube daemon
扫「爬虫任务台」(tblQnLHnBa1RjJUE) 的 爬虫类型=KOL-YouTube + 任务状态=1-待触发 + 触发=true 任务跑抓取。
本引擎定时按市场补词保持队列, 消除"人偶尔补词"L2 重复劳动。

2026-06-16 扩来源(方向1 多语言): 产品卖 DE/FR/ES/IT/BR/MX 等非英语市场, 但 KOL 库这些市场严重不足
(DE179/FR91/ES93 vs US1779)。本引擎除英语外, 用对应语言生成本地化游戏关键词补各市场队列
(德语词→德语创作者, daemon classify 判国家/语言入库)。零新凭据(复用 daemon+DeepSeek)。

边界: 只补 YouTube 爬虫任务台(KOL库同 app 可写; 96%产能)。TK/IG keywords_queue 在专题9app+Apify$5限暂不。
"""
import time
from collections import Counter
from . import config, feishu, deepseek
from .feishu import ext

T_CRAWLER = "tblQnLHnBa1RjJUE"   # 爬虫任务台 (KOL 营销库内)
PER_BATCH_LIMIT = 50            # 每词 daemon 抓取上限

# 市场配置: 英语为主(水位15), 非英语市场(产品在卖+KOL库不足)各保持小水位
MARKETS = [
    {"lang": "en", "countries": ["US", "UK", "CA", "AU"], "target": 15, "name": "English"},
    {"lang": "de", "countries": ["DE"],        "target": 6, "name": "German (Deutsch)"},
    {"lang": "fr", "countries": ["FR"],        "target": 6, "name": "French (Français)"},
    {"lang": "es", "countries": ["ES", "MX"],  "target": 6, "name": "Spanish (Español)"},
]

_AXES = """5 轴交叉生成:
  ① 游戏 IP/系列(IP 名保留通用拼写): super mario / zelda / pokemon / animal crossing / kirby /
     metroid / sonic / stardew valley / hollow knight / elden ring / final fantasy / splatoon 等
  ② 玩家身份/文化: cozy gamer / retro gamer / jrpg fan / speedrunner / indie gamer / handheld gamer 等
  ③ 平台/设备: steam deck / rog ally / switch 2 / gaming handheld 等
  ④ 场景/美学: cozy gaming room / aesthetic gaming setup / battlestation / desk makeover 等
  × 内容形式: themed gaming / setup / room / collection / fan / review / unboxing / haul / setup tour"""


def _build_prompt(market: dict, n: int, existing_sample: str) -> str:
    lang_line = (
        "全小写英文自然短语。"
        if market["lang"] == "en"
        else f"**用 {market['name']} 书写**这些搜索词(游戏 IP 专有名保留通用拼写如 Zelda/Mario/Pokemon, 其余词本地化成 {market['name']}), 抓 {market['name']} 母语游戏创作者。"
    )
    return f"""你是 KOL 达人发现的关键词拓展助手。为游戏配件品牌(Switch/PS/PC 手柄/收纳包/充电底座/RGB灯饰)
抓取 **YouTube 游戏创作者**, 生成 {n} 个长尾搜索词。

铁律(数据验证, 必须遵守):
- 按"受众/IP/主题向"拓词, **绝不用产品词**。产品词(switch dock/controller 等)实测新增=0; 受众/IP/主题词平均新增 78.6/词。
- {_AXES}
- **IP 轴优先**(高产+喂 IP 匹配评分)。
- {lang_line}
- 不带 # 号。**不要和这些已有词重复**: {existing_sample}

只返回 JSON: {{"keywords": ["...", "..."]}} 共 {n} 个。"""


async def _load() -> tuple:
    """返回 (已有关键词小写集合, {lang: YouTube待触发数})"""
    recs = await feishu.fetch_all_records(T_CRAWLER)
    existing = set()
    pending = Counter()
    for r in recs:
        f = r["fields"]
        kw = ext(f.get("关键词列表"))
        if kw:
            existing.add(kw.strip().lstrip("#").lower())
        if ext(f.get("爬虫类型")) == "KOL-YouTube" and ext(f.get("任务状态")) == "1-待触发":
            langs = f.get("筛选-语言") or []
            lang = (langs[0].get("text") or langs[0].get("name")) if (langs and isinstance(langs[0], dict)) else (langs[0] if langs else "en")
            pending[lang] += 1
    return existing, pending


async def run(dry_run: bool = False) -> dict:
    existing, pending = await _load()
    summary = {"markets": {}, "total_added": 0}
    now = int(time.time() * 1000)

    for m in MARKETS:
        pend = pending.get(m["lang"], 0)
        if pend >= m["target"]:
            summary["markets"][m["lang"]] = {"pending": pend, "skip": "队列充足"}
            continue
        need = m["target"] - pend
        prompt = _build_prompt(m, need * 2, ", ".join(list(existing)[:40]))
        try:
            res = await deepseek.chat_json(prompt, max_tokens=900, temperature=0.7)
        except Exception as e:
            summary["markets"][m["lang"]] = {"error": f"deepseek: {str(e)[:80]}"}
            continue
        words = (res or {}).get("keywords") or []
        fresh, seen = [], set()
        for w in words:
            wn = (w or "").strip().lstrip("#").lower()
            if wn and wn not in existing and wn not in seen and 2 <= len(wn) <= 60:
                seen.add(wn)
                fresh.append(wn)
            if len(fresh) >= need:
                break
        existing |= seen   # 跨市场防重复

        if dry_run:
            summary["markets"][m["lang"]] = {"pending": pend, "need": need, "would_add": fresh}
            continue

        created, errors = 0, []
        for w in fresh:
            try:
                await feishu.create_record(T_CRAWLER, {
                    "任务名": f"[自动] YT KOL - {w}",
                    "爬虫类型": "KOL-YouTube",
                    "关键词列表": w,
                    "筛选-国家": m["countries"],
                    "筛选-语言": [m["lang"]],
                    "每批数量上限": PER_BATCH_LIMIT,
                    "任务状态": "1-待触发",
                    "触发": True,
                    "创建日期": now,
                })
                created += 1
            except Exception as e:
                errors.append(f"{w}: {str(e)[:50]}")
        summary["markets"][m["lang"]] = {"pending_before": pend, "added": created, "keywords": fresh, "errors": errors[:3]}
        summary["total_added"] += created

    return {"ok": True, **summary}

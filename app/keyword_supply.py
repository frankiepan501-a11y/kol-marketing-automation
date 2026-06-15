# -*- coding: utf-8 -*-
"""自动关键词供给引擎 (2026-06-15) — 让 YouTube daemon 持续有词抓, 把达人发现从"脉冲"变"稳定日流".

背景(审计 project_kol_intake_audit_2026_06_11): 达人发现根因=关键词断供。YouTube daemon 扫
「爬虫任务台」(tblQnLHnBa1RjJUE) 的 爬虫类型=KOL-YouTube + 任务状态=1-待触发 + 触发=true 任务跑抓取,
平时队列空就不产出(脉冲式)。本引擎定时补词保持队列, 消除"人偶尔补词"这个 L2 重复劳动。

设计:
- 只补 YouTube 爬虫任务台(KOL库同 app 可写; 96% 产能主力)。TK/IG keywords_queue 在专题9 app+受
  Apify FREE $5 限, 暂不自动补(后续可扩)。
- 队列水位控制: 待触发 < TARGET_PENDING 才补, 补到 TARGET_PENDING(不堆积浪费 daemon)。
- DeepSeek 按 5 轴生成受众/IP/主题向长尾词(铁律: 绝不用产品词——审计实证产品词新增=0, IP/主题词 78.6/词)。
- 去重: 查爬虫任务台所有历史关键词(小写归一), 不重复抓。
- 每词配 daemon 粉丝门槛(yt_mvp --min-fans, 已在 daemon 默认 5000)→ 抓回的自动过滤废号。
"""
import time
from . import config, feishu, deepseek
from .feishu import ext

T_CRAWLER = "tblQnLHnBa1RjJUE"   # 爬虫任务台 (KOL 营销库内, config.FEISHU_APP_TOKEN)
TARGET_PENDING = 15              # YouTube 待触发任务保持水位
PER_BATCH_LIMIT = 50            # 每词 daemon 抓取上限 (加门槛后 50 够, 省 daemon 爬 about 时间)

GEN_PROMPT = """你是 KOL 达人发现的关键词拓展助手。为游戏配件品牌(Switch/PS/PC 手柄/收纳包/充电底座/RGB灯饰)
抓取 **YouTube 游戏创作者**, 生成 {n} 个英文长尾搜索词。

铁律(数据验证, 必须遵守):
- 按"受众/IP/主题向"拓词, **绝不用产品词**。产品词(如 switch dock / controller / charging station)搜出来
  全是已抓过的官方号, 实测新增=0; 受众/IP/主题词实测平均新增 78.6/词。
- 5 轴交叉生成:
  ① 游戏 IP/系列: super mario / zelda / pokemon / animal crossing / kirby / metroid / sonic /
     stardew valley / hollow knight / elden ring / final fantasy / splatoon / fire emblem / 等
  ② 玩家身份/文化: cozy gamer / retro gamer / jrpg fan / speedrunner / indie gamer /
     handheld gamer / girl gamer / variety streamer / vtuber / 等
  ③ 平台/设备: steam deck / rog ally / switch 2 / gaming handheld / emulation handheld / 等
  ④ 场景/美学: cozy gaming room / aesthetic gaming setup / battlestation / desk makeover / 等
  × 内容形式: themed gaming / setup / room / collection / fan / review / unboxing / haul / setup tour
- **IP 轴优先**(高产 + 直接喂 IP 匹配评分)。
- 全小写英文自然短语, 不带 # 号。
- **不要和这些已有词重复**: {existing_sample}

只返回 JSON: {{"keywords": ["word one","word two", ...]}} 共 {n} 个。"""


async def _load() -> tuple:
    """返回 (已有关键词小写集合, YouTube待触发数)"""
    recs = await feishu.fetch_all_records(T_CRAWLER)
    existing = set()
    pending = 0
    for r in recs:
        f = r["fields"]
        kw = ext(f.get("关键词列表"))
        if kw:
            existing.add(kw.strip().lstrip("#").lower())
        if ext(f.get("爬虫类型")) == "KOL-YouTube" and ext(f.get("任务状态")) == "1-待触发":
            pending += 1
    return existing, pending


async def run(dry_run: bool = False) -> dict:
    existing, pending = await _load()
    if pending >= TARGET_PENDING:
        return {"ok": True, "skip": f"YouTube 待触发 {pending} ≥ 水位 {TARGET_PENDING}, 无需补词"}
    need = TARGET_PENDING - pending

    # DeepSeek 生 need*2 个(留去重余量)
    sample = ", ".join(list(existing)[:40])
    prompt = GEN_PROMPT.format(n=need * 2, existing_sample=sample)
    try:
        res = await deepseek.chat_json(prompt, max_tokens=900, temperature=0.7)
    except Exception as e:
        return {"ok": False, "error": f"deepseek gen fail: {str(e)[:120]}"}
    words = (res or {}).get("keywords") or []

    # 去重 + 取 need 个
    fresh, seen = [], set()
    for w in words:
        wn = (w or "").strip().lstrip("#").lower()
        if wn and wn not in existing and wn not in seen and 2 <= len(wn) <= 60:
            seen.add(wn)
            fresh.append(wn)
        if len(fresh) >= need:
            break

    if dry_run:
        return {"ok": True, "dry_run": True, "pending": pending, "need": need,
                "gen_count": len(words), "would_add": fresh}

    created, errors = 0, []
    now = int(time.time() * 1000)
    for w in fresh:
        try:
            await feishu.create_record(T_CRAWLER, {
                "任务名": f"[自动] YT KOL - {w}",
                "爬虫类型": "KOL-YouTube",
                "关键词列表": w,
                "筛选-国家": ["US", "UK", "CA", "AU"],
                "筛选-语言": ["en"],
                "每批数量上限": PER_BATCH_LIMIT,
                "任务状态": "1-待触发",
                "触发": True,
                "创建日期": now,
            })
            created += 1
        except Exception as e:
            errors.append(f"{w}: {str(e)[:60]}")
    return {"ok": True, "pending_before": pending, "added": created,
            "keywords": fresh, "errors": errors[:5]}

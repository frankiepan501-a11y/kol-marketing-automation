"""产品英文名命名规范 — KOL/营销版 (单一真相源, 2026-06-02 Frankie 拍板).

格式: [品牌] [系列英文] [型号英文] - [主关键词]
  例: FUNLAB Firefly Zonai - Hall-Effect Switch Controller

真相源 (运营不手打整条英文名, 减少乱发挥):
  - 品牌 / 系列英文 / 型号英文 → SKU 产品库反查 (老库ERP SKU 作 join 钥匙)
  - 主关键词 → KOL 库「主关键词(英文)」字段 (运营填一个品类描述短语)

落地范围:
  - FUNLAB: 全自动 — 老库ERP SKU 反查 SKU 库系列英文+型号英文, 自动拼 + 回填 KOL 库
  - POWKONG: SKU 库系列矩阵未补 → 暂保留运营手填 + 格式校验 (不阻塞派单)

铁律: 拼不出/查不到时绝不阻塞派单 (派单防死链已由 product_url 兜底),
      改为飞书私聊运营补「老库ERP SKU」, 当轮先用现有手填名 (degraded + WARN)。
"""
from . import config, feishu

ext = feishu.ext


async def _lookup_sku_lib(erp_or_model: str):
    """用「老库ERP SKU」反查 SKU 产品库. 该字段实际可能存 ERP SKU 或 品牌型号
    (如戴夫填的是 FF05A-04 = 品牌型号), 故两个字段都试.
    Returns (series_en, model_en, ip_status);
      - 查不到 → (None, None, None)
      - 找到但 IP合规状态 字段空 → ip_status = '' (用于 default-deny 区分 infra-fail)."""
    erp_or_model = (erp_or_model or "").strip()
    if not erp_or_model:
        return None, None, None
    path = (f"/bitable/v1/apps/{config.SKU_LIB_APP_TOKEN}"
            f"/tables/{config.SKU_LIB_TABLE_ID}/records/search")
    body = {
        "filter": {"conjunction": "or", "conditions": [
            {"field_name": "ERP SKU", "operator": "is", "value": [erp_or_model]},
            {"field_name": "品牌型号", "operator": "is", "value": [erp_or_model]},
        ]},
        "field_names": ["系列英文名", "型号英文名", "ERP SKU", "品牌型号", "IP合规状态"],
    }
    try:
        data = await feishu.api("POST", path, body, which="bitable")
    except Exception as e:
        print(f"[product_naming] SKU 库反查失败 ({erp_or_model}): {str(e)[:120]}")
        return None, None, None
    items = (data.get("data") or {}).get("items") or []
    if not items:
        return None, None, None
    f = items[0]["fields"]
    raw = f.get("IP合规状态")           # 单选: search 返回字符串; 兜底兼容 list
    ip_status = raw.strip() if isinstance(raw, str) else (ext(raw).strip() if raw else "")
    return ext(f.get("系列英文名")).strip(), ext(f.get("型号英文名")).strip(), ip_status


def _compose(brand: str, series_en: str, model_en: str, main_kw: str) -> str:
    """[品牌] [系列英文] [型号英文] - [主关键词]. 主关键词缺则省略后缀."""
    core = " ".join(p for p in [brand, series_en, model_en] if p)
    main_kw = (main_kw or "").strip()
    return f"{core} - {main_kw}" if main_kw else core


async def resolve_product_en(pf: dict) -> tuple:
    """解析产品英文名 (营销用). 不写库, 只返回.

    Returns (name, source, warn):
      - name: 解析出的产品英文名 (可能是现有手填名的降级)
      - source: 'sku_auto' / 'manual' / 'manual_fallback'
      - warn: 非空 = 给运营的提醒文案 (缺老库ERP SKU / 查不到 / POWKONG手填等)
    """
    brand = ext(pf.get("品牌")).strip()           # FUNLAB / POWKONG
    main_kw = ext(pf.get("主关键词(英文)")).strip()
    existing = ext(pf.get("产品英文名")).strip()

    if brand == "FUNLAB":
        erp = ext(pf.get("老库ERP SKU")).strip()
        if not erp:
            return existing, "manual_fallback", "缺「老库ERP SKU」→ 无法自动拼英文名, 当轮用现有手填名。请补老库ERP SKU。"
        series_en, model_en, _ = await _lookup_sku_lib(erp)
        if not series_en:
            return existing, "manual_fallback", f"老库ERP SKU「{erp}」在 SKU 库查不到/系列英文为空 → 用现有手填名。请核对 SKU 或补 SKU 库系列英文。"
        name = _compose(brand, series_en, model_en, main_kw)
        warn = "" if main_kw else "已自动拼英文名, 但缺「主关键词(英文)」(如 Hall-Effect Switch Controller), 名字不够完整。"
        return name, "sku_auto", warn

    # 白牌(Linyuvo) → 纯品类名, 运营手填, 无品牌前缀(2026-06-08)
    if brand == "白牌":
        if not existing:
            return "", "manual", "白牌产品缺「产品英文名」(手填纯品类名, 无品牌前缀, 如 Switch 2 Pro Controller with Hall Effect)。"
        return existing, "manual", ""

    # POWKONG (系列矩阵未补) + 其他 → 手填 + 格式校验
    if not existing:
        return "", "manual", "POWKONG 产品缺「产品英文名」(系列矩阵未补, 暂需手填)。格式: POWKONG [系列] [型号] - [主关键词]。"
    if not existing.upper().startswith((brand or "").upper()):
        return existing, "manual", f"「产品英文名」建议以品牌「{brand}」开头 (现: {existing[:40]})。"
    return existing, "manual", ""


async def resolve_and_backfill(product: dict) -> dict:
    """派单时调用: 解析产品英文名, 若与现值不同则回填 KOL 库 + 同步 product 内存值.
    返回 {name, source, warn, backfilled}. 任何失败都不抛 (不阻塞派单)。"""
    pf = product["fields"]
    try:
        name, source, warn = await resolve_product_en(pf)
    except Exception as e:
        print(f"[product_naming] resolve 异常: {str(e)[:120]}")
        return {"name": ext(pf.get("产品英文名")), "source": "manual_fallback",
                "warn": "解析异常, 用现有手填名", "backfilled": False}

    backfilled = False
    cur = ext(pf.get("产品英文名")).strip()
    # 仅 SKU 自动拼且与现值不同时才回填 (不覆盖 POWKONG 手填)
    if source == "sku_auto" and name and name != cur:
        try:
            await feishu.update_record(config.T_PRODUCT, product["record_id"], {"产品英文名": name})
            pf["产品英文名"] = name          # 同步内存, 下游 generate_email 立即用新名
            backfilled = True
        except Exception as e:
            print(f"[product_naming] 回填产品英文名失败: {str(e)[:120]}")
    return {"name": name, "source": source, "warn": warn, "backfilled": backfilled}


# ===== IP 合规闸 (2026-06-03 Frankie) — 防 AI 以 FUNLAB 名义派单侵权品 =====
# 真相源: FUNLAB SKU 产品库「IP合规状态」单选字段。
#   合规-无IP / 合规-已授权 → 放行; 风险-限非Funlab / 禁售-高风险 / 空(待审) → 拦截(default-deny)。
# 原则: 手柄抽象纹路=合规, 直接角色实体造型(摇杆帽精灵球/太空人Among Us/星之卡比)=风险。
# 风险品可走非 Funlab 渠道/其他品牌名在部分地区卖, 但不挂 FUNLAB → 不进 KOL 派单。
_IP_COMPLIANT = {"合规-无IP", "合规-已授权"}


async def ip_compliance_gate(pf: dict) -> tuple:
    """派单前调。返回 (allowed: bool, status: str, reason: str)。
    - 明确合规 → (True, status, "")
    - 风险/禁售/空(待审) → (False, status, reason)  [default-deny]
    - POWKONG(SKU库无此字段) / 缺老库ERP SKU / 查不到(infra/数据缺口)
        → (True, "", reason)  放行+待核, 不因基建/数据缺口 halt 派单(与命名铁律一致)。
    """
    brand = ext(pf.get("品牌")).strip()
    if brand != "FUNLAB":
        return True, "", ""                      # POWKONG 暂无 IP合规字段, 不 gate
    erp = ext(pf.get("老库ERP SKU")).strip()
    if not erp:
        return True, "", "缺「老库ERP SKU」无法核 IP 合规 → 放行+待补"
    try:
        _, _, status = await _lookup_sku_lib(erp)
    except Exception as e:
        return True, "", f"IP 合规查询异常({erp}): {str(e)[:80]} → 放行+待核"
    if status is None:                            # SKU 库查不到该记录
        return True, "", f"SKU库查不到「{erp}」→ 放行+待核 IP"
    if status in _IP_COMPLIANT:
        return True, status, ""
    return False, status, f"IP合规状态={status or '空(待审)'} → 禁止以 FUNLAB 名义派单(风险品可走非 Funlab 渠道)"

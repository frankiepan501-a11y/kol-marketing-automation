"""产品派单模式与活动归并规则。

产品库未填写「派单模式」时继续按旧逻辑进入常规派单，避免上线后影响
其他历史产品；只有明确标记为「活动专用」或「暂停」才会被常规 cold、
媒体人派单和二次维护排除。
"""

REGULAR_MODE = "常规派单"
ACTIVITY_MODE = "活动专用"
PAUSED_MODE = "暂停"
LOCKED_MODES = {ACTIVITY_MODE, PAUSED_MODE}


def _text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        parts = []
        for item in value:
            if isinstance(item, dict):
                parts.append(str(item.get("text") or item.get("name") or ""))
            elif item is not None:
                parts.append(str(item))
        return "".join(parts).strip()
    if isinstance(value, dict):
        return str(value.get("text") or value.get("name") or "").strip()
    return str(value).strip()


def product_dispatch_mode(fields: dict) -> str:
    """空值按常规派单处理，保证旧产品无需一次性回填。"""
    return _text((fields or {}).get("派单模式")) or REGULAR_MODE


def partition_regular_products(products: list) -> tuple[list, list]:
    """拆出常规派单产品与被活动/暂停锁拦住的产品。"""
    regular = []
    locked = []
    for product in products or []:
        fields = product.get("fields") or {}
        mode = product_dispatch_mode(fields)
        if mode in LOCKED_MODES:
            locked.append({
                "record_id": product.get("record_id") or "",
                "mode": mode,
                "product_name": _text(fields.get("产品名")),
                "merge_key": _text(fields.get("活动归并键")),
                "canonical_record_id": _text(fields.get("活动主记录ID")),
            })
        else:
            regular.append(product)
    return regular, locked


def build_activity_group(products: list) -> dict:
    """验证同一活动产品族只有一个主记录，并返回可供活动层使用的归并信息。"""
    if not products:
        raise ValueError("活动归并组不能为空")

    merge_keys = {
        _text((product.get("fields") or {}).get("活动归并键"))
        for product in products
    }
    merge_keys.discard("")
    if len(merge_keys) != 1:
        raise ValueError("活动归并键必须唯一且非空")

    canonical_ids = {
        _text((product.get("fields") or {}).get("活动主记录ID"))
        for product in products
    }
    canonical_ids.discard("")
    if len(canonical_ids) != 1:
        raise ValueError("活动主记录ID不一致")
    canonical_id = next(iter(canonical_ids))

    member_ids = [product.get("record_id") or "" for product in products]
    if canonical_id not in member_ids:
        raise ValueError("活动主记录ID不在归并组内")

    marked_main_ids = [
        product.get("record_id") or ""
        for product in products
        if bool((product.get("fields") or {}).get("活动主记录"))
    ]
    if marked_main_ids != [canonical_id]:
        raise ValueError("活动归并组必须且只能标记一个主记录")

    return {
        "merge_key": next(iter(merge_keys)),
        "canonical_record_id": canonical_id,
        "member_record_ids": member_ids,
    }

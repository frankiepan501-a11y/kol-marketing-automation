"""草稿表归档清理 (2026-05-27) — 删 N 天前的「已否决/发送失败」草稿.

为何安全: dedup (_DRAFT_REUSABLE_STATES = {已否决, 发送失败}) 本来就**跳过**这俩状态,
所以删它们**不影响**"防重复联系 KOL"的记忆. 其他状态(尤其「已发送」)是 dedup 防重 + ROI
+ 在途状态, **硬保护绝不删**.
"""
import time
from . import config, feishu

CLEANABLE = {"已否决", "发送失败"}  # 唯一可删: dedup 跳过这俩, 删了不影响防重/ROI


async def run(days: int = 30) -> dict:
    cutoff = int(time.time() * 1000) - days * 86400 * 1000
    all_drafts = await feishu.fetch_all_records(config.T_DRAFT)
    to_del = []
    protected = 0
    for d in all_drafts:
        f = d["fields"]
        st = feishu.ext(f.get("邮件草稿状态"))
        if st not in CLEANABLE:        # 硬保护: 已发送/在途/寄样等一律不碰
            protected += 1
            continue
        gen = f.get("生成时间") or 0
        if not isinstance(gen, (int, float)) or gen >= cutoff:
            continue                   # 还没到 N 天, 留
        to_del.append(d["record_id"])
    deleted = 0
    for i in range(0, len(to_del), 500):
        batch = to_del[i:i+500]
        await feishu.api("POST",
            f"/bitable/v1/apps/{config.FEISHU_APP_TOKEN}/tables/{config.T_DRAFT}/records/batch_delete",
            {"records": batch})
        deleted += len(batch)
    print(f"[draft_cleanup] scanned={len(all_drafts)} protected={protected} deleted={deleted} (>{days}d 已否决/失败)")
    return {"scanned": len(all_drafts), "protected_nonclean": protected,
            "deleted": deleted, "retention_days": days, "cleanable_states": list(CLEANABLE)}

"""新品集中上稿活动的确定性倒排计划器。

本模块只计算计划，不读取生产表、不发邮件、不发卡、不寄样、不释放预算。
现有日常 KOL 流程仍由 dispatch/enrich/auto_send 等模块负责。
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from typing import Iterable


@dataclass(frozen=True)
class FunnelScenario:
    name: str
    reply_rate: float
    reply_to_commit_rate: float
    valid_contact_rate: float
    evidence: str


@dataclass(frozen=True)
class CampaignSpec:
    campaign_id: str
    brand: str
    product_name: str
    erp_sku: str
    package_version: str
    launch_date: date
    target_posts: int
    paid_warm_commitments: int
    gifting_commitments: int
    on_time_post_rate: float
    budget_cap_cny: float
    contribution_per_order_cny: float
    paid_warm_lock_rate: float = 0.50
    media_report_target: int | None = None
    click_through_rate: float = 0.01
    conversion_rate: float = 0.03
    attribution_coverage: float = 0.70
    window_before_days: int = 7
    window_after_days: int = 7
    standard_prep_days: int = 42


DEFAULT_FUNNEL_SCENARIOS = (
    FunnelScenario(
        name="保守",
        reply_rate=0.10,
        reply_to_commit_rate=0.30,
        valid_contact_rate=0.75,
        evidence="回复率低于历史基线；回复→承诺率和有效邮箱率为规划假设",
    ),
    FunnelScenario(
        name="中性",
        reply_rate=0.123,
        reply_to_commit_rate=0.40,
        valid_contact_rate=0.80,
        evidence="回复率采用历史去重基线 12.3%；其余为待试点校准的规划假设",
    ),
    FunnelScenario(
        name="进取",
        reply_rate=0.15,
        reply_to_commit_rate=0.50,
        valid_contact_rate=0.85,
        evidence="仅用于观察上界，不可据此提前释放预算",
    ),
)


def _require_rate(name: str, value: float) -> None:
    if not 0 < value <= 1:
        raise ValueError(f"{name} must be in (0, 1]")


def _validate_spec(spec: CampaignSpec) -> None:
    if spec.target_posts <= 0:
        raise ValueError("target_posts must be positive")
    if spec.paid_warm_commitments < 0 or spec.gifting_commitments < 0:
        raise ValueError("commitment lanes cannot be negative")
    if spec.budget_cap_cny <= 0 or spec.contribution_per_order_cny <= 0:
        raise ValueError("budget and contribution must be positive")
    if spec.media_report_target is not None and spec.media_report_target < 0:
        raise ValueError("media_report_target cannot be negative")
    _require_rate("on_time_post_rate", spec.on_time_post_rate)
    _require_rate("paid_warm_lock_rate", spec.paid_warm_lock_rate)
    _require_rate("click_through_rate", spec.click_through_rate)
    _require_rate("conversion_rate", spec.conversion_rate)
    _require_rate("attribution_coverage", spec.attribution_coverage)


def _milestones(spec: CampaignSpec, as_of: date) -> list[dict]:
    schedule = (
        (42, "活动简报、英文名、链接、库存和 press kit 就绪", "品牌/独立站运营"),
        (35, "暖关系和付费保底席位开始锁档", "KOL 运营"),
        (28, "冷启动开发信首轮发出", "KOL 运营"),
        (21, "首轮回复转承诺，未达标时启动备选池", "KOL 运营"),
        (18, "跨境寄样最晚放行；晚于此日只用本地库存", "供应链/KOL 运营"),
        (10, "收内容提纲或初稿，确认合规与链接", "内容审核"),
        (7, "最终锁定发布时间和替补名单", "活动负责人"),
        (0, "集中发布窗口中点 T0", "活动负责人"),
    )
    result = []
    for days, task, owner in schedule:
        due_date = spec.launch_date - timedelta(days=days)
        overdue = due_date < as_of
        result.append({
            "code": f"T-{days}" if days else "T0",
            "due_date": due_date.isoformat(),
            "task": task,
            "owner_role": owner,
            "state": "shadow_overdue" if overdue else "shadow_pending",
            "recovery_action": (
                "快速通道补做；完成并经人工核验前，不得开启对应外部动作"
                if overdue
                else "按节点完成后记录证据"
            ),
        })
    return result


def build_campaign_plan(
    spec: CampaignSpec,
    *,
    as_of: date,
    scenarios: Iterable[FunnelScenario] = DEFAULT_FUNNEL_SCENARIOS,
) -> dict:
    """计算单个产品的影子计划；没有任何外部副作用。"""
    _validate_spec(spec)
    required_commitments = math.ceil(spec.target_posts / spec.on_time_post_rate)
    planned_commitments = spec.paid_warm_commitments + spec.gifting_commitments
    if planned_commitments < required_commitments:
        raise ValueError(
            f"planned commitments {planned_commitments} below required {required_commitments}"
        )

    paid_warm_candidates = math.ceil(
        spec.paid_warm_commitments / spec.paid_warm_lock_rate
    )
    funnel = []
    for scenario in scenarios:
        _require_rate("reply_rate", scenario.reply_rate)
        _require_rate("reply_to_commit_rate", scenario.reply_to_commit_rate)
        _require_rate("valid_contact_rate", scenario.valid_contact_rate)
        cold_emails = math.ceil(
            spec.gifting_commitments
            / (scenario.reply_rate * scenario.reply_to_commit_rate)
        )
        candidate_contacts = math.ceil(cold_emails / scenario.valid_contact_rate)
        funnel.append(
            {
                "scenario": scenario.name,
                "cold_emails_required": cold_emails,
                "candidate_contacts_required": candidate_contacts,
                "total_candidate_contacts_required": (
                    candidate_contacts + paid_warm_candidates
                ),
                "assumptions": {
                    "reply_rate": scenario.reply_rate,
                    "reply_to_commit_rate": scenario.reply_to_commit_rate,
                    "valid_contact_rate": scenario.valid_contact_rate,
                },
                "evidence": scenario.evidence,
            }
        )

    break_even_orders = math.ceil(
        spec.budget_cap_cny / spec.contribution_per_order_cny
    )
    attributed_order_rate = (
        spec.click_through_rate
        * spec.conversion_rate
        * spec.attribution_coverage
    )
    break_even_exposure = math.ceil(break_even_orders / attributed_order_rate)
    ideal_start = spec.launch_date - timedelta(days=spec.standard_prep_days)
    days_to_launch = (spec.launch_date - as_of).days
    fast_track = as_of > spec.launch_date - timedelta(days=28)

    return {
        "campaign": {
            **asdict(spec),
            "launch_date": spec.launch_date.isoformat(),
            "window_start": (
                spec.launch_date - timedelta(days=spec.window_before_days)
            ).isoformat(),
            "window_end": (
                spec.launch_date + timedelta(days=spec.window_after_days)
            ).isoformat(),
        },
        "capacity": {
            "target_scope": {
                "kol_posts": spec.target_posts,
                "media_reports": spec.media_report_target,
                "media_reports_count_toward_kol_target": False,
                "note": (
                    "首个试点的20个目标只统计KOL实际上稿；媒体人目标未设，"
                    "不得套用KOL回复率或计入完成数"
                ),
            },
            "required_commitments": required_commitments,
            "planned_commitments": planned_commitments,
            "paid_warm_commitments": spec.paid_warm_commitments,
            "paid_warm_candidates_required": paid_warm_candidates,
            "paid_warm_lock_rate_assumption": spec.paid_warm_lock_rate,
            "gifting_commitments": spec.gifting_commitments,
            "funnel_scenarios": funnel,
        },
        "economics": {
            "budget_cap_cny": spec.budget_cap_cny,
            "contribution_per_order_cny": spec.contribution_per_order_cny,
            "break_even_orders": break_even_orders,
            "break_even_exposure": break_even_exposure,
            "attributed_order_rate": attributed_order_rate,
        },
        "budget_control": {
            "cap_cny": spec.budget_cap_cny,
            "approved_to_release_cny": 0,
            "committed_cny": 0,
            "spent_cny": 0,
            "available_without_approval_cny": 0,
            "release_gates": [
                {
                    "stage": "first_batch",
                    "eligible": False,
                    "amount_cny": "human_defined_within_product_cap",
                    "requires": [
                        "只读候选预览通过",
                        "测试邮箱 raw content 校验通过",
                        "首批名单与报价人工批准",
                    ],
                },
                {
                    "stage": "remaining_product_cap",
                    "eligible": False,
                    "amount_cny": "human_defined_remaining_balance",
                    "requires": [
                        "承诺缺口和准时风险已回读",
                        "保守曝光与订单预测仍覆盖新增投入",
                        "新增金额人工批准",
                    ],
                },
            ],
            "stop_rules": [
                "不得超过产品预算上限",
                "保守回本预测不覆盖新增投入时停止释放",
                "未经人工批准不得产生付费承诺",
            ],
        },
        "timing": {
            "as_of": as_of.isoformat(),
            "ideal_start_date": ideal_start.isoformat(),
            "days_to_launch": days_to_launch,
            "fast_track": fast_track,
            "risk": "P0-时间不足" if fast_track else "P1-按标准倒排",
            "milestones": _milestones(spec, as_of),
        },
        "execution_gates": {
            "mode": "shadow",
            "email_send": False,
            "paid_commit": False,
            "sample_ship": False,
            "operator_bulk_card": False,
            "reserve_release": False,
            "live_release_requires_human_approval": True,
        },
    }

def build_portfolio_plan(
    specs: Iterable[CampaignSpec], *, as_of: date, reserve_budget_cny: float
) -> dict:
    spec_list = list(specs)
    campaign_ids = [spec.campaign_id.strip() for spec in spec_list]
    if any(not campaign_id for campaign_id in campaign_ids):
        raise ValueError("campaign_id must not be empty")
    if len(campaign_ids) != len(set(campaign_ids)):
        raise ValueError("campaign_id must be unique within a portfolio")
    plans = [build_campaign_plan(spec, as_of=as_of) for spec in spec_list]
    committed_caps = sum(plan["economics"]["budget_cap_cny"] for plan in plans)
    return {
        "mode": "shadow",
        "as_of": as_of.isoformat(),
        "products": plans,
        "budget": {
            "product_caps_cny": committed_caps,
            "reserve_budget_cny": reserve_budget_cny,
            "portfolio_ceiling_cny": committed_caps + reserve_budget_cny,
            "reserve_auto_allocated": False,
            "reserve_approved_to_release_cny": 0,
            "reserve_committed_cny": 0,
            "reserve_remaining_cny": reserve_budget_cny,
            "reserve_release_requires": [
                "两款产品已完成首批承诺缺口回读",
                "新增保守曝光与订单预测覆盖新增投入",
                "储备金额人工批准",
            ],
        },
        "integration_contract": {
            "implementation_state": "planner_and_shadow_tables_only",
            "required_before_live": [
                "复用全局联系人触达锁",
                "复用邮件与回复引擎但隔离活动状态",
                "活动参与记录必须带唯一 campaign_id",
                "支持单条候选回放",
            ],
        },
    }

"""输出 2026-09-15 双产品新品集中上稿影子计划（无外部写入）。"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date

from app.launch_campaign import CampaignSpec, build_portfolio_plan


def build_pilot(as_of: date) -> dict:
    launch = date(2026, 9, 15)
    specs = [
        CampaignSpec(
            campaign_id="launch-20260915-powkong-piranha-v2",
            brand="POWKONG",
            product_name="Piranha Plant Switch 2 Dock / 食人花二代",
            erp_sku="PK02-S2",
            package_version="标准包装",
            launch_date=launch,
            target_posts=20,
            paid_warm_commitments=8,
            gifting_commitments=21,
            on_time_post_rate=0.70,
            budget_cap_cny=20_000,
            contribution_per_order_cny=248.21,
        ),
        CampaignSpec(
            campaign_id="launch-20260915-funlab-dave-ys11-5",
            brand="FUNLAB",
            product_name="FUNLAB Luminex Dave THE DIVER Edition / YS11-5 潜水员戴夫",
            erp_sku="FF05A-04-2",
            package_version="磁吸盒彩盒",
            launch_date=launch,
            target_posts=20,
            paid_warm_commitments=8,
            gifting_commitments=21,
            on_time_post_rate=0.70,
            budget_cap_cny=7_000,
            contribution_per_order_cny=82.90,
        ),
    ]
    return build_portfolio_plan(specs, as_of=as_of, reserve_budget_cny=23_000)


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser()
    parser.add_argument("--as-of", default="2026-08-19")
    parser.add_argument("--compact", action="store_true")
    args = parser.parse_args()
    plan = build_pilot(date.fromisoformat(args.as_of))
    print(json.dumps(plan, ensure_ascii=False, indent=None if args.compact else 2))


if __name__ == "__main__":
    main()

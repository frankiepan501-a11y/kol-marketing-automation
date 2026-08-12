from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

from app.clients import FeishuClient, YouTubeClient
from app.collector import IncrementalCollector


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--commit", action="store_true")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    result = IncrementalCollector(FeishuClient(), YouTubeClient()).run(
        now=datetime.now(timezone.utc),
        commit=args.commit,
        force=args.force,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

TIMELINE_PAGE_SIZE = 100
DEFAULT_INPUT_CSV = Path("sources/top_100_usernames.csv")
DEFAULT_OUTPUT_DIR = Path("output")
DEFAULT_STATE_DB = Path(".sqlite/bulk_fetch_state.sqlite3")
DEFAULT_ENV_FILE = Path(".env")
START_TIME_ISO = (datetime.now(timezone.utc) - timedelta(days=365)).strftime(
    "%Y-%m-%dT%H:%M:%SZ"
)


@dataclass(slots=True, frozen=True)
class AppConfig:
    input_csv: Path
    target_posts: int
    max_profiles: int | None
    concurrency: int
    include_replies: bool
    include_retweets: bool


def parse_args() -> AppConfig:
    parser = argparse.ArgumentParser(description="Idempotent bulk X post scraper.")
    parser.add_argument("--input-csv", type=Path, default=DEFAULT_INPUT_CSV)
    parser.add_argument("--target-posts", type=int, default=100)
    parser.add_argument("--max-profiles", type=int, default=None)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--include-replies", action="store_true")
    parser.add_argument("--include-retweets", action="store_true")
    args = parser.parse_args()
    return AppConfig(
        input_csv=args.input_csv,
        target_posts=max(1, args.target_posts),
        max_profiles=args.max_profiles,
        concurrency=max(1, args.concurrency),
        include_replies=args.include_replies,
        include_retweets=args.include_retweets,
    )


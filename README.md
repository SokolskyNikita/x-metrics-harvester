# Twitter stats bulk scraper

An idempotent, async X timeline scraper for engagement analysis at account-list scale.

The project is built to quantify **views**, **likes**, and **reposts** across a defined set of X users.  
It was originally created to study engagement distribution for top accounts. Because X does not expose a direct "top users" API, the workflow starts from a pre-built list (for example, most-followed accounts). The same pipeline works for any username list.

## What it does

- Fetches posts for many users concurrently via the X SDK.
- Keeps local state so you can stop and rerun safely.
- Exports:
  - `output/last100_summary_<timestamp>.csv`
  - `output/all_cached_posts_raw_<timestamp>.csv`

## repeatable runs

You can run this script multiple times without creating duplicate post rows. It resumes from local state in `.sqlite/bulk_fetch_state.sqlite3`.

## quickstart

1. Install dependencies:

```bash
python3 -m pip install -r requirements.txt
```

1. Create `.env`:

```bash
cp .env.example .env
```

1. Set your token:

```env
X_BEARER_TOKEN=YOUR_X_BEARER_TOKEN
```

1. Run:

```bash
python3 fetch_bulk_posts_idempotent.py --help
python3 fetch_bulk_posts_idempotent.py
```

Optional package mode:

```bash
python3 -m pip install -e .
twitter-stats-bulk --help
```

## CSV files

- `sources/top_100_usernames.csv`: public sample input (top 100 accounts), tracked in git for concision.
- `sources/all_usernames_full.csv`: full private input list (used for larger runs), ignored by git.
- `output/last100_summary_*.csv`: result files from running this scraper against the top 500 X accounts.
- `output/all_cached_posts_raw_*.csv`: raw post-level output from that same top-500 run.

## CLI

- `--input-csv` path to CSV containing a `username` column
- `--target-posts` target posts to cache per user (default: `100`)
- `--max-profiles` optional cap for processed usernames
- `--concurrency` async worker limit (default: `8`)
- `--include-replies` include replies (excluded by default)
- `--include-retweets` include retweets (excluded by default)

## project layout

```text
bulk_posts/
  cli.py          # package entrypoint
  config.py       # constants + CLI parsing
  io_helpers.py   # env + input loading
  x_api.py        # X API client + retries
  models.py       # SQLAlchemy models
  state_store.py  # idempotent persistence layer
  runner.py       # orchestration
```

## license

MIT

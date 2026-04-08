from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pandas as pd

from .config import (
    build_start_time_iso,
    DEFAULT_ENV_FILE,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_STATE_DB,
    AppConfig,
    parse_args,
)
from .state_store import StateStore
from .x_api import XApiClient
from .io_helpers import load_credentials, load_usernames


async def process_username(
    username: str,
    config: AppConfig,
    start_time_iso: str,
    store: StateStore,
    api: XApiClient,
    semaphore: asyncio.Semaphore,
) -> tuple[str, str]:
    async with semaphore:
        await store.ensure_user(username)
        try:
            existing_count = await store.posts_count(username)
            if existing_count >= config.target_posts:
                return username, f"ready_{existing_count}"

            user_id, followers_count = await store.get_cached_identity(username)
            if not user_id:
                user_id, followers_count = await api.lookup_user(username)
                await store.set_identity(username, user_id, followers_count)

            exclude = [
                name
                for name, include in (
                    ("replies", config.include_replies),
                    ("retweets", config.include_retweets),
                )
                if not include
            ]
            until_id = await store.oldest_post_id(username)
            pagination_token: str | None = None

            while True:
                current_count = await store.posts_count(username)
                if current_count >= config.target_posts:
                    return username, f"ready_{current_count}"
                remaining = config.target_posts - current_count

                posts, pagination_token = await api.timeline_page(
                    user_id,
                    max_results=remaining,
                    start_time_iso=start_time_iso,
                    exclude=exclude,
                    pagination_token=pagination_token,
                    until_id=until_id,
                )
                if not posts:
                    await store.set_fetch_complete(username, "timeline_exhausted_empty_page")
                    return username, f"complete_{current_count}"
                # For very small targets (e.g. 1-4), API still returns 5+ per page.
                # Keep storage aligned to the configured target_posts.
                posts = posts[:remaining]

                inserted = await store.add_posts(username, posts)
                if inserted == 0:
                    count_now = await store.posts_count(username)
                    await store.set_fetch_complete(username, "timeline_exhausted_no_new_posts")
                    return username, f"complete_{count_now}"

                if not pagination_token:
                    count_now = await store.posts_count(username)
                    await store.set_fetch_complete(username, "timeline_exhausted_no_next_token")
                    return username, f"complete_{count_now}"
        except Exception as exc:  # noqa: BLE001
            error_text = f"{exc.__class__.__name__}: {exc}"
            await store.set_error(username, error_text)
            return username, f"error: {error_text}"


async def run() -> None:
    config = parse_args()
    start_time_iso = build_start_time_iso(config.days_before)
    DEFAULT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    bearer_token = load_credentials(DEFAULT_ENV_FILE)
    usernames = load_usernames(config.input_csv, config.max_profiles)
    if not usernames:
        raise SystemExit("No usernames found")

    store = await StateStore.open(DEFAULT_STATE_DB)
    api = XApiClient(bearer_token=bearer_token)
    semaphore = asyncio.Semaphore(config.concurrency)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    summary_csv = DEFAULT_OUTPUT_DIR / f"last100_summary_{stamp}.csv"
    raw_csv = DEFAULT_OUTPUT_DIR / f"all_cached_posts_raw_{stamp}.csv"

    print(
        f"profiles={len(usernames)} target_posts={config.target_posts} "
        f"concurrency={config.concurrency} start_time={start_time_iso}",
        flush=True,
    )

    completed = 0
    errors = 0
    ready = 0
    complete = 0
    partial = 0

    try:
        async with asyncio.TaskGroup() as tg:
            tasks = [
                tg.create_task(
                    process_username(name, config, start_time_iso, store, api, semaphore)
                )
                for name in usernames
            ]

        for task in tasks:
            username, status = task.result()
            completed += 1
            if status.startswith("error:"):
                errors += 1
            elif status.startswith("ready_"):
                ready += 1
            elif status.startswith("complete_"):
                complete += 1
            else:
                partial += 1
            if completed % 100 == 0 or status.startswith("error:"):
                print(
                    f"progress={completed}/{len(usernames)} username={username} status={status}",
                    flush=True,
                )
    finally:
        summary_rows = await store.summary_rows(usernames, last_n=min(100, config.target_posts))
        raw_rows = await store.raw_rows(usernames)
        await store.close()

    pd.DataFrame(
        summary_rows,
        columns=["username", "followers_count", "likes_sum", "reposts_sum", "views_sum"],
    ).to_csv(summary_csv, index=False)
    pd.DataFrame(
        raw_rows,
        columns=["username", "post_id", "created_at", "likes", "reposts", "views", "raw_post_json"],
    ).to_csv(raw_csv, index=False)

    print(
        f"done completed={completed} ready={ready} complete={complete} "
        f"partial={partial} errors={errors}",
        flush=True,
    )
    print(f"summary_csv={summary_csv}", flush=True)
    print(f"raw_posts_csv={raw_csv}", flush=True)


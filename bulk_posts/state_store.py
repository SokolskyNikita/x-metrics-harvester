from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from sqlalchemy import func, inspect, select, text, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from .models import Base, PostRecord, UserRecord
from .utils import int_or_zero, utc_now_iso


def _migrate_legacy_schema(sync_conn) -> None:
    inspector = inspect(sync_conn)
    table_names = set(inspector.get_table_names())
    if "users" in table_names:
        user_cols = {col["name"] for col in inspector.get_columns("users")}
        if "updated_at" not in user_cols:
            sync_conn.execute(text("ALTER TABLE users ADD COLUMN updated_at TEXT"))
            sync_conn.execute(
                text("UPDATE users SET updated_at = CURRENT_TIMESTAMP WHERE updated_at IS NULL")
            )
        if "fetch_complete" not in user_cols:
            sync_conn.execute(text("ALTER TABLE users ADD COLUMN fetch_complete INTEGER DEFAULT 0"))
            sync_conn.execute(
                text("UPDATE users SET fetch_complete = 0 WHERE fetch_complete IS NULL")
            )
        if "completion_reason" not in user_cols:
            sync_conn.execute(text("ALTER TABLE users ADD COLUMN completion_reason TEXT"))
    if "posts" in table_names:
        post_cols = {col["name"] for col in inspector.get_columns("posts")}
        if "raw_json" not in post_cols:
            sync_conn.execute(text("ALTER TABLE posts ADD COLUMN raw_json TEXT"))


class StateStore:
    def __init__(self, engine: AsyncEngine, session_factory: async_sessionmaker) -> None:
        self._engine = engine
        self._session_factory = session_factory
        self._lock = asyncio.Lock()

    @classmethod
    async def open(cls, db_path: Path) -> "StateStore":
        db_path.parent.mkdir(parents=True, exist_ok=True)
        engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            await conn.run_sync(_migrate_legacy_schema)
        return cls(engine, async_sessionmaker(engine, expire_on_commit=False))

    @staticmethod
    async def _posts_count_in_session(session, username: str) -> int:
        total = await session.scalar(
            select(func.count()).select_from(PostRecord).where(PostRecord.username == username)
        )
        return int(total or 0)

    async def close(self) -> None:
        await self._engine.dispose()

    async def ensure_user(self, username: str) -> None:
        async with self._lock:
            async with self._session_factory() as session:
                stmt = (
                    sqlite_insert(UserRecord)
                    .values(username=username, updated_at=utc_now_iso())
                    .on_conflict_do_nothing(index_elements=[UserRecord.username])
                )
                await session.execute(stmt)
                await session.commit()

    async def get_cached_identity(self, username: str) -> tuple[str | None, int]:
        async with self._lock:
            async with self._session_factory() as session:
                row = (
                    await session.execute(
                        select(UserRecord.user_id, UserRecord.followers_count).where(
                            UserRecord.username == username
                        )
                    )
                ).one_or_none()
        if row is None:
            return None, 0
        return row[0], int(row[1] or 0)

    async def set_identity(self, username: str, user_id: str, followers_count: int) -> None:
        async with self._lock:
            async with self._session_factory() as session:
                await session.execute(
                    update(UserRecord)
                    .where(UserRecord.username == username)
                    .values(
                        user_id=user_id,
                        followers_count=followers_count,
                        last_error=None,
                        fetch_complete=0,
                        completion_reason=None,
                        updated_at=utc_now_iso(),
                    )
                )
                await session.commit()

    async def set_error(self, username: str, error_text: str) -> None:
        async with self._lock:
            async with self._session_factory() as session:
                await session.execute(
                    update(UserRecord)
                    .where(UserRecord.username == username)
                    .values(
                        last_error=error_text[:800],
                        fetch_complete=0,
                        completion_reason=None,
                        updated_at=utc_now_iso(),
                    )
                )
                await session.commit()

    async def set_fetch_complete(self, username: str, reason: str) -> None:
        async with self._lock:
            async with self._session_factory() as session:
                await session.execute(
                    update(UserRecord)
                    .where(UserRecord.username == username)
                    .values(
                        fetch_complete=1,
                        completion_reason=reason[:200],
                        last_error=None,
                        updated_at=utc_now_iso(),
                    )
                )
                await session.commit()

    async def posts_count(self, username: str) -> int:
        async with self._lock:
            async with self._session_factory() as session:
                return await self._posts_count_in_session(session, username)

    async def oldest_post_id(self, username: str) -> str | None:
        async with self._lock:
            async with self._session_factory() as session:
                return await session.scalar(
                    select(PostRecord.post_id)
                    .where(PostRecord.username == username)
                    .order_by(PostRecord.post_id.asc())
                    .limit(1)
                )

    async def add_posts(self, username: str, posts: list[dict[str, Any]]) -> int:
        payload: list[dict[str, Any]] = []
        for post in posts:
            post_id = str(post.get("id") or "")
            if not post_id:
                continue
            metrics = post.get("public_metrics") or {}
            payload.append(
                {
                    "username": username,
                    "post_id": post_id,
                    "created_at": post.get("created_at"),
                    "likes": int_or_zero(metrics.get("like_count")),
                    "reposts": int_or_zero(metrics.get("retweet_count")),
                    "views": int_or_zero(metrics.get("impression_count")),
                    "raw_json": json.dumps(post, ensure_ascii=False, separators=(",", ":")),
                }
            )

        if not payload:
            return 0

        async with self._lock:
            async with self._session_factory() as session:
                before = await self._posts_count_in_session(session, username)
                stmt = (
                    sqlite_insert(PostRecord)
                    .values(payload)
                    .on_conflict_do_nothing(index_elements=[PostRecord.username, PostRecord.post_id])
                )
                await session.execute(stmt)
                after = await self._posts_count_in_session(session, username)
                await session.commit()
        return after - before

    async def summary_rows(self, usernames: list[str], last_n: int) -> list[tuple[Any, ...]]:
        if not usernames:
            return []
        async with self._lock:
            async with self._session_factory() as session:
                users = (
                    await session.execute(
                        select(UserRecord.username, UserRecord.followers_count).where(
                            UserRecord.username.in_(usernames)
                        )
                    )
                ).all()
                posts = (
                    await session.execute(
                        select(PostRecord.username, PostRecord.likes, PostRecord.reposts, PostRecord.views)
                        .where(PostRecord.username.in_(usernames))
                        .order_by(
                            PostRecord.username.asc(),
                            PostRecord.created_at.desc(),
                            PostRecord.post_id.desc(),
                        )
                    )
                ).all()

        followers_by_user = {username: int(count or 0) for username, count in users}
        stats: dict[str, list[int]] = defaultdict(lambda: [0, 0, 0, 0])
        for username, likes, reposts, views in posts:
            bucket = stats[username]
            if bucket[0] >= last_n:
                continue
            bucket[0] += 1
            bucket[1] += int_or_zero(likes)
            bucket[2] += int_or_zero(reposts)
            bucket[3] += int_or_zero(views)

        rows = [
            (
                username,
                followers_by_user.get(username, 0),
                stats[username][1],
                stats[username][2],
                stats[username][3],
            )
            for username in usernames
        ]
        rows.sort(key=lambda row: (-int_or_zero(row[1]), str(row[0])))
        return rows

    async def raw_rows(self, usernames: list[str]) -> list[tuple[Any, ...]]:
        if not usernames:
            return []
        async with self._lock:
            async with self._session_factory() as session:
                return (
                    await session.execute(
                        select(
                            PostRecord.username,
                            PostRecord.post_id,
                            PostRecord.created_at,
                            PostRecord.likes,
                            PostRecord.reposts,
                            PostRecord.views,
                            PostRecord.raw_json,
                        )
                        .where(PostRecord.username.in_(usernames))
                        .order_by(
                            PostRecord.username.asc(),
                            PostRecord.created_at.desc(),
                            PostRecord.post_id.desc(),
                        )
                    )
                ).all()


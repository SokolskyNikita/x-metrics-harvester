from __future__ import annotations

from sqlalchemy import ForeignKey, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class UserRecord(Base):
    __tablename__ = "users"

    username: Mapped[str] = mapped_column(primary_key=True)
    user_id: Mapped[str | None]
    followers_count: Mapped[int] = mapped_column(default=0)
    last_error: Mapped[str | None]
    updated_at: Mapped[str]


class PostRecord(Base):
    __tablename__ = "posts"

    username: Mapped[str] = mapped_column(ForeignKey("users.username"), primary_key=True)
    post_id: Mapped[str] = mapped_column(primary_key=True)
    created_at: Mapped[str | None]
    likes: Mapped[int] = mapped_column(default=0)
    reposts: Mapped[int] = mapped_column(default=0)
    views: Mapped[int] = mapped_column(default=0)
    raw_json: Mapped[str] = mapped_column(Text)


from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def int_or_zero(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def model_to_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json", exclude_none=True)
    raise TypeError(f"Unsupported model type: {type(value)!r}")


def next_token_from_page(page: Any) -> str | None:
    meta = getattr(page, "meta", None)
    if meta is None:
        return None
    if isinstance(meta, dict):
        return meta.get("next_token")
    if hasattr(meta, "next_token"):
        return getattr(meta, "next_token")
    if hasattr(meta, "model_dump"):
        return meta.model_dump(mode="json", exclude_none=True).get("next_token")
    return None

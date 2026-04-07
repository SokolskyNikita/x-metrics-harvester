from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any

import requests
from xdk import Client

from .config import START_TIME_ISO, TIMELINE_PAGE_SIZE
from .errors import XApiError
from .utils import int_or_zero, model_to_dict, next_token_from_page


class XApiClient:
    def __init__(self, bearer_token: str, retries: int = 4) -> None:
        self._client = Client(bearer_token=bearer_token)
        self._retries = retries

    async def _call(self, label: str, func) -> Any:
        for attempt in range(self._retries + 1):
            try:
                return await asyncio.to_thread(func)
            except requests.HTTPError as exc:
                response = exc.response
                if response is None:
                    if attempt >= self._retries:
                        raise XApiError(f"{label}: {exc}") from exc
                    await asyncio.sleep(2 ** (attempt + 1))
                    continue
                if response.status_code == 429:
                    reset_raw = response.headers.get("x-rate-limit-reset", "")
                    wait = (
                        max(1, int(float(reset_raw) - datetime.now().timestamp()))
                        if reset_raw.isdigit()
                        else 2 ** (attempt + 1)
                    )
                    await asyncio.sleep(wait)
                    continue
                if response.status_code in {500, 502, 503, 504} and attempt < self._retries:
                    await asyncio.sleep(2 ** (attempt + 1))
                    continue
                raise XApiError(f"{label}: {response.status_code}") from exc
            except requests.RequestException as exc:
                if attempt >= self._retries:
                    raise XApiError(f"{label}: {exc}") from exc
                await asyncio.sleep(2 ** (attempt + 1))
        raise XApiError(f"{label}: exhausted retries")

    async def lookup_user(self, username: str) -> tuple[str, int]:
        def run() -> tuple[str, int]:
            response = self._client.users.get_by_username(username, user_fields=["public_metrics"])
            data = model_to_dict(getattr(response, "data", None))
            user_id = str(data.get("id") or "")
            if not user_id:
                raise XApiError(f"lookup @{username}: missing user id")
            followers = int_or_zero((data.get("public_metrics") or {}).get("followers_count"))
            return user_id, followers

        return await self._call(f"lookup @{username}", run)

    async def timeline_page(
        self,
        user_id: str,
        *,
        exclude: list[str],
        pagination_token: str | None,
        until_id: str | None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        def run() -> tuple[list[dict[str, Any]], str | None]:
            iterator = self._client.users.get_posts(
                id=user_id,
                max_results=TIMELINE_PAGE_SIZE,
                exclude=exclude or None,
                start_time=START_TIME_ISO,
                pagination_token=pagination_token,
                until_id=until_id,
                tweet_fields=["created_at", "public_metrics"],
            )
            page = next(iter(iterator), None)
            if page is None:
                return [], None
            posts = [model_to_dict(item) for item in (getattr(page, "data", None) or [])]
            return posts, next_token_from_page(page)

        return await self._call(f"timeline {user_id}", run)


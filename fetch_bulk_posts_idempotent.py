#!/usr/bin/env python3
"""Entrypoint for idempotent bulk X post fetching."""

from __future__ import annotations

import asyncio

from bulk_posts.runner import run


if __name__ == "__main__":
    asyncio.run(run())

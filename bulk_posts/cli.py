from __future__ import annotations

import asyncio

from .runner import run


def main() -> None:
    asyncio.run(run())


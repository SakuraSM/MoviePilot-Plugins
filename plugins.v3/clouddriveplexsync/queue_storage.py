"""Finish serialized queue writes before propagating cancellation."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import TypeVar

StoredValue = TypeVar("StoredValue")


async def persist_values(
    callback: Callable[[StoredValue], None] | None,
    values: StoredValue,
) -> None:
    if callback is None:
        return
    saving = asyncio.create_task(asyncio.to_thread(callback, values))
    try:
        await asyncio.shield(saving)
    except asyncio.CancelledError:
        await saving
        raise

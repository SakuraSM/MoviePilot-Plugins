"""Polling retry and authentication backoff."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from .history_models import TTDAuthenticationError, TTDHistoryGapError, TTDProtocolError

if TYPE_CHECKING:
    from .history_poller import TTDHistoryPoller


async def run_polling(poller: TTDHistoryPoller, stop_event: asyncio.Event) -> None:
    transient_steps = (5.0, 15.0, 30.0, 60.0)
    auth_steps = (300.0, 900.0, 1800.0, 3600.0)
    transient_index = 0
    auth_index = 0
    while not stop_event.is_set():
        delay = float(poller.poll_seconds)
        try:
            await poller.poll_once()
            transient_index = 0
            auth_index = 0
        except asyncio.CancelledError:
            raise
        except TTDAuthenticationError as exc:
            should_report = poller.status.last_error != str(exc)
            poller.status.connected = True
            poller.status.authenticated = False
            poller.status.last_error = str(exc)
            delay = auth_steps[auth_index]
            auth_index = min(auth_index + 1, len(auth_steps) - 1)
            if should_report and poller.on_error:
                poller.on_error(str(exc))
        except (TTDHistoryGapError, TTDProtocolError) as exc:
            should_report = poller.status.last_error != str(exc)
            poller.status.connected = True
            poller.status.authenticated = True
            poller.status.last_error = str(exc)
            if poller.stats:
                poller.stats.ttd_errors += 1
            delay = max(float(poller.poll_seconds), 60.0)
            if should_report and poller.on_error:
                poller.on_error(f"TTD 轮询暂停推进：{exc}")
        except Exception as exc:  # noqa: BLE001 - network errors use transient backoff.
            should_report = poller.status.last_error != str(exc)
            poller.status.connected = False
            poller.status.authenticated = False
            poller.status.last_error = str(exc)
            if poller.stats:
                poller.stats.ttd_errors += 1
            delay = transient_steps[transient_index]
            transient_index = min(transient_index + 1, len(transient_steps) - 1)
            if should_report and poller.on_error:
                poller.on_error(f"TTD 轮询失败：{exc}")
        await poller._wait(stop_event, delay)
    poller.status.next_poll_at = 0


async def wait_polling(
    poller: TTDHistoryPoller, stop_event: asyncio.Event, delay: float
) -> None:
    poller.status.next_poll_at = poller.clock() + delay
    stop_task = asyncio.create_task(stop_event.wait())
    wake_task = asyncio.create_task(poller._wake_event.wait())
    try:
        await asyncio.wait_for(
            asyncio.wait(
                {stop_task, wake_task},
                return_when=asyncio.FIRST_COMPLETED,
            ),
            timeout=delay,
        )
    except TimeoutError:
        pass
    finally:
        stop_task.cancel()
        wake_task.cancel()
        await asyncio.gather(stop_task, wake_task, return_exceptions=True)
        poller._wake_event.clear()

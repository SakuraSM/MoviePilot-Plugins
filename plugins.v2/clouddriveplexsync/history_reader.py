"""Read a bounded newest-first history window and detect missing anchors."""

from __future__ import annotations

from typing import TYPE_CHECKING

from .history_models import TTDHistoryGapError, TTDHistoryMode, TTDHistoryRecord

if TYPE_CHECKING:
    from .history_poller import TTDHistoryPoller


async def read_history(poller: TTDHistoryPoller) -> list[TTDHistoryRecord]:
    if not poller.cursor.initialized:
        first_page = await poller.client.fetch_page(1)
        return list(first_page.records)
    known = set(poller.cursor.seen_keys)
    unseen: list[TTDHistoryRecord] = []
    unseen_keys = set()
    found_cursor = False
    cursor_page = 0
    exhausted = False
    for page in range(1, poller.max_pages + 1):
        page_result = await poller.client.fetch_page(page)
        records = page_result.records
        if not records:
            exhausted = True
            break
        for record in records:
            if record.key in known:
                found_cursor = True
                cursor_page = cursor_page or page
                if not poller.store:
                    continue
            if record.key not in unseen_keys:
                unseen.append(record)
                unseen_keys.add(record.key)
        if found_cursor and page >= cursor_page + poller.overlap_pages:
            break
        if (
            page_result.has_next is False
            or page_result.item_count < poller.client.page_size
        ):
            exhausted = True
            break
    is_new_only = poller.policy.mode == TTDHistoryMode.NEW_ONLY
    if (
        poller.cursor.initialized
        and not found_cursor
        and (known or is_new_only)
        and (not exhausted or (is_new_only and bool(known)))
    ):
        if not poller.audit:
            poller.cursor.history_gap = True
            poller._save_cursor()
            raise TTDHistoryGapError(
                f"连续 {poller.max_pages} 页未找到已保存游标；已停止推进，"
                "请增大 TTD 最大补页数或手动确认历史缺口"
            )
        if not poller.cursor.history_gap and poller.on_error:
            poller.on_error("TTD 未找到历史游标，已启动分批历史对账")
        poller.audit.request(restart=not poller.cursor.history_gap)
        poller.cursor.history_gap = True
    if found_cursor and is_new_only and poller.cursor.history_gap:
        poller.cursor.history_gap = False
        poller._save_cursor()
    return unseen

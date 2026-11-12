"""Attach TTD file identities to the directory scan that covers them."""

from __future__ import annotations

import posixpath
from dataclasses import replace
from typing import TYPE_CHECKING

from .history_models import TTDHistoryRecord
from .models import MediaHint, PendingScan, PlexTarget
from .path_mapper import PathMappingError, is_under, normalize_path
from .plex_bridge import normalize_plex_path

if TYPE_CHECKING:
    from .event_worker import EventWorker


def _valid_file_name(file_name: str) -> bool:
    return (
        bool(file_name)
        and "/" not in file_name
        and "\\" not in file_name
        and file_name not in {".", ".."}
    )


def build_media_hint(
    worker: EventWorker,
    cloud_directory: str,
    plex_directory: str,
    record: TTDHistoryRecord,
) -> MediaHint | None:
    if not _valid_file_name(record.file_name):
        worker.record_info(
            f"TTD 文件名缺失或无效，无法核验 Plex 入库：{cloud_directory}"
        )
        return None
    return MediaHint(
        record_key=record.key,
        revision=record.revision,
        cloud_directory=cloud_directory,
        expected_path=normalize_path(posixpath.join(plex_directory, record.file_name)),
        title=record.title,
        media_type=record.media_type,
        season_num=record.season_num,
        episode_num=record.episode_num,
        tmdb_id=record.tmdb_id,
    )


def current_media_hints(
    worker: EventWorker, item: PendingScan, target: PlexTarget
) -> list[MediaHint]:
    platform = str(getattr(worker.plex.plex, "platform", "") or "")
    current = []
    for hint in item.media:
        try:
            directory = normalize_plex_path(
                worker.mapper.cloud_to_plex(hint.cloud_directory), platform
            )
        except PathMappingError:
            worker.record_info(f"媒体目录映射已失效，未能核验：{hint.cloud_directory}")
            continue
        if not is_under(directory, target.path):
            worker.record_info(f"媒体目录映射已变化，未能核验：{hint.cloud_directory}")
            continue
        current.append(
            replace(
                hint,
                expected_path=normalize_path(
                    posixpath.join(directory, posixpath.basename(hint.expected_path))
                ),
            )
        )
    return current

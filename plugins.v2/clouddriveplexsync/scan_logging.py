"""Scan stages and the Plex paths observed at each stage."""

from __future__ import annotations

import json
from collections.abc import Sequence
from enum import Enum

from .models import PlexTarget


class ScanStage(str, Enum):
    QUEUED = "QUEUE"
    STARTED = "SCAN_START"
    SUBMITTED = "SCAN_SUBMITTED"


def format_scan_log(
    *,
    stage: ScanStage,
    cloud_path: str,
    target: PlexTarget,
    sources: Sequence[str],
    queued_path: str | None = None,
) -> str:
    details = {
        "source": ",".join(sorted(set(sources))),
        "ttd_path": cloud_path,
        "plex_path": target.path,
        "section_id": target.section_id,
        "section_title": target.section_title,
        "section_root": target.location,
    }
    if queued_path is not None:
        details["queued_plex_path"] = queued_path
    return f"{stage.value} {json.dumps(details, ensure_ascii=False)}"

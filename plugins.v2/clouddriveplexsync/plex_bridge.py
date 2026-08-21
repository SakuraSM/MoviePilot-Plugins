"""Safe Plex integration that never falls back to a full-library scan."""

from __future__ import annotations

from typing import Any, Iterable, List, Optional, Sequence, Set, Tuple

from .models import PlexTarget, PluginStats
from .path_mapper import PathMappingError, is_under, normalize_path


MINIMUM_PLEX_VERSION = (1, 20, 0, 3125)


def parse_plex_version(value: str) -> Tuple[int, ...]:
    numeric = (value or "").split("-", 1)[0]
    parts: List[int] = []
    for part in numeric.split("."):
        try:
            parts.append(int(part))
        except ValueError:
            break
    return tuple(parts)


class PlexBridge:
    """Wrap a MoviePilot Plex service instance with strict path checks."""

    def __init__(
        self,
        service_instance: Any,
        allowed_sections: Optional[Iterable[str]] = None,
        stats: Optional[PluginStats] = None,
    ) -> None:
        self.service_instance = service_instance
        self.allowed_sections: Set[str] = {str(item) for item in allowed_sections or [] if str(item)}
        self.stats = stats

    @property
    def plex(self) -> Any:
        getter = getattr(self.service_instance, "get_plex", None)
        plex = getter() if callable(getter) else self.service_instance
        if plex is None:
            raise RuntimeError("Plex service is not connected")
        return plex

    def assert_supported(self) -> str:
        version = str(getattr(self.plex, "version", "") or "")
        parsed = parse_plex_version(version)
        if parsed and parsed < MINIMUM_PLEX_VERSION:
            raise RuntimeError(
                f"Plex {version} does not support path-scoped scans; "
                "version 1.20.0.3125 or newer is required"
            )
        return version

    def sections(self) -> Sequence[Any]:
        return self.plex.library.sections()

    def describe(self) -> dict:
        sections = []
        for section in self.sections():
            section_id = str(getattr(section, "key", ""))
            if self.allowed_sections and section_id not in self.allowed_sections:
                continue
            sections.append(
                {
                    "id": section_id,
                    "title": str(getattr(section, "title", section_id)),
                    "locations": [str(item) for item in getattr(section, "locations", None) or []],
                }
            )
        return {"version": str(getattr(self.plex, "version", "") or ""), "sections": sections}

    def find_target(self, path: str) -> PlexTarget:
        path = normalize_path(path)
        candidates: List[Tuple[int, str, str, str, Any]] = []
        for section in self.sections():
            section_id = str(getattr(section, "key", ""))
            if self.allowed_sections and section_id not in self.allowed_sections:
                continue
            for raw_location in getattr(section, "locations", None) or []:
                location = normalize_path(str(raw_location))
                if is_under(path, location):
                    candidates.append(
                        (
                            len(location),
                            section_id,
                            str(getattr(section, "title", section_id)),
                            location,
                            section,
                        )
                    )
        if not candidates:
            raise PathMappingError(f"no selected Plex library contains {path}")
        best_length = max(item[0] for item in candidates)
        best = [item for item in candidates if item[0] == best_length]
        identities = {(item[1], item[3]) for item in best}
        if len(identities) != 1:
            raise PathMappingError(f"multiple Plex libraries match {path}")
        _, section_id, title, location, _ = best[0]
        return PlexTarget(
            section_id=section_id,
            section_title=title,
            location=location,
            path=path,
        )

    def scan(self, target: PlexTarget) -> None:
        section = next(
            (item for item in self.sections() if str(getattr(item, "key", "")) == target.section_id),
            None,
        )
        if section is None:
            raise RuntimeError(f"Plex library section no longer exists: {target.section_id}")
        # LibrarySection.update(path=...) emits /library/sections/{id}/refresh?path=...
        # and cannot silently turn into a root scan.
        section.update(path=target.path)
        if self.stats:
            self.stats.plex_scans += 1

    def is_scanning(self, section_ids: Iterable[str]) -> bool:
        wanted = {str(item) for item in section_ids}
        for section in self.sections():
            if str(getattr(section, "key", "")) in wanted and bool(
                getattr(section, "refreshing", False)
            ):
                return True
        activities = getattr(self.plex, "activities", None)
        if callable(activities):
            activities = activities()
        for activity in activities or []:
            activity_type = str(getattr(activity, "type", "") or "").lower()
            if activity_type.startswith("library"):
                return True
        return False

    @staticmethod
    def _session_paths(session: Any) -> List[str]:
        paths: List[str] = []
        iterator = getattr(session, "iterParts", None)
        if callable(iterator):
            try:
                for part in iterator():
                    value = getattr(part, "file", None)
                    if value:
                        paths.append(normalize_path(str(value)))
            except Exception:
                pass
        for media in getattr(session, "media", None) or []:
            for part in getattr(media, "parts", None) or []:
                value = getattr(part, "file", None)
                if value:
                    paths.append(normalize_path(str(value)))
        return list(dict.fromkeys(paths))

    def is_playing_under(self, prefixes: Iterable[str]) -> bool:
        normalized = [normalize_path(item) for item in prefixes]
        for session in self.plex.sessions():
            for path in self._session_paths(session):
                if any(is_under(path, prefix) for prefix in normalized):
                    return True
        return False

"""Request and response models for the MoviePilot V3 plugin API."""

from __future__ import annotations

from pydantic import BaseModel


class ResyncRequest(BaseModel):
    cloud_path: str


class PlexResyncRequest(BaseModel):
    plex_path: str


class PluginResponse(BaseModel):
    """MoviePilot V3 plugin API response envelope."""

    success: bool
    message: str = ""
    data: dict[str, object] | None = None

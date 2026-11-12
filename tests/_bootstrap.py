"""Load plugin submodules without importing MoviePilot-dependent __init__.py."""

from __future__ import annotations

import os
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PLUGIN_GENERATION = os.environ.get("MOVIEPILOT_PLUGIN_GENERATION", "v2")
if PLUGIN_GENERATION not in {"v2", "v3"}:
    raise ValueError("Unsupported plugin generation")
PLUGIN_DIR = ROOT / f"plugins.{PLUGIN_GENERATION}" / "clouddriveplexsync"

if "clouddriveplexsync" not in sys.modules:
    package = types.ModuleType("clouddriveplexsync")
    package.__path__ = [str(PLUGIN_DIR)]
    package.__package__ = "clouddriveplexsync"
    sys.modules["clouddriveplexsync"] = package

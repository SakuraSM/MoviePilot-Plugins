"""Load plugin submodules without importing MoviePilot-dependent __init__.py."""

from __future__ import annotations

import sys
import types
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PLUGIN_DIR = ROOT / "plugins.v2" / "clouddriveplexsync"

if "clouddriveplexsync" not in sys.modules:
    package = types.ModuleType("clouddriveplexsync")
    package.__path__ = [str(PLUGIN_DIR)]
    package.__package__ = "clouddriveplexsync"
    sys.modules["clouddriveplexsync"] = package

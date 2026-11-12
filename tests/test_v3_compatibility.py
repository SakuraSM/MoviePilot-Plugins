from __future__ import annotations

import json
import shutil
import subprocess
import unittest
from pathlib import Path
from unittest.mock import patch

import _bootstrap
from test_plugin_contract import load_plugin_module

ROOT = Path(__file__).resolve().parents[1]
V3_ONLY_FILES = {
    "__init__.py",
    "configuration.py",
    "control_ui.py",
    "manual_scan_ui.py",
    "README.md",
}


class VersionedRepositoryTests(unittest.TestCase):
    def test_v3_has_an_independent_market_entry_and_version(self) -> None:
        v2 = json.loads((ROOT / "package.v2.json").read_text())["CloudDrivePlexSync"]
        v3 = json.loads((ROOT / "package.v3.json").read_text())["CloudDrivePlexSync"]
        self.assertIs(v2["v3"], False)
        self.assertEqual(v2["system_version"], ">=2.12.0,<3")
        self.assertEqual(v3["version"], "3.0.0")
        self.assertEqual(v3["system_version"], ">=3.0.3,<4")
        self.assertEqual(next(iter(v3["history"])), "v3.0.0")
        self.assertIn(
            'plugin_version = "3.0.0"',
            (ROOT / "plugins.v3/clouddriveplexsync/__init__.py").read_text(),
        )

    def test_shared_code_matches_v2_without_symlinks(self) -> None:
        v2 = ROOT / "plugins.v2/clouddriveplexsync"
        v3 = ROOT / "plugins.v3/clouddriveplexsync"
        v2_files = {
            path.name
            for path in v2.iterdir()
            if path.is_file() and path.suffix in {".py", ".md"}
        }
        v3_files = {
            path.name
            for path in v3.iterdir()
            if path.is_file() and path.suffix in {".py", ".md"}
        }
        self.assertEqual(v3_files - v2_files, {"api_models.py"})
        self.assertEqual(v2_files - v3_files, set())
        for name in v3_files:
            self.assertFalse((v3 / name).is_symlink(), name)
            if name not in V3_ONLY_FILES and name in v2_files:
                self.assertEqual(
                    (v2 / name).read_bytes(), (v3 / name).read_bytes(), name
                )


@unittest.skipUnless(_bootstrap.PLUGIN_GENERATION == "v3", "V3-specific API contract")
class V3ApiContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.module = load_plugin_module()
        self.plugin = self.module.CloudDrivePlexSync()

    def test_endpoints_use_three_field_response_model(self) -> None:
        self.assertTrue(
            all(
                item["response_model"] is self.module.PluginResponse
                for item in self.plugin.get_api()
            )
        )
        self.assertEqual(
            self.plugin.api_status().model_dump().keys(), {"success", "message", "data"}
        )
        with (
            patch.object(self.plugin, "_api", new=lambda *_args: object()),
            patch.object(
                self.plugin, "_submit", return_value={"success": True, "scans": 3}
            ),
        ):
            response = self.plugin.api_flush()
        self.assertEqual(
            response.model_dump(),
            {"success": True, "message": "", "data": {"scans": 3}},
        )

    def test_business_failure_and_invalid_payload_do_not_report_success(self) -> None:
        with (
            patch.object(self.plugin, "_api", new=lambda *_args: object()),
            patch.object(
                self.plugin,
                "_submit",
                return_value={"success": False, "message": "队列已满"},
            ),
        ):
            self.assertEqual(
                self.plugin.api_resync_plex(
                    type("Request", (), {"plex_path": "/plex"})()
                ).message,
                "队列已满",
            )
        with (
            patch.object(self.plugin, "_api", new=lambda *_args: object()),
            patch.object(self.plugin, "_submit", return_value={"data": {}}),
        ):
            self.assertFalse(self.plugin.api_flush().success)

    def test_clone_ui_actions_address_the_clone(self) -> None:
        clone_type = type(
            "CloudDrivePlexSyncClone_42", (self.module.CloudDrivePlexSync,), {}
        )
        clone = clone_type()
        form, _defaults = clone.get_form()
        page = clone.get_page()
        form_text = json.dumps(form, ensure_ascii=False)
        page_text = json.dumps(page, ensure_ascii=False)
        self.assertIn("plugin/CloudDrivePlexSyncClone_42/resync/plex", form_text)
        self.assertIn("/plugin/CloudDrivePlexSyncClone_42/history/start", page_text)
        self.assertIn("/plugin/CloudDrivePlexSyncClone_42/history/stop", page_text)
        self.assertIn("/plugin/CloudDrivePlexSyncClone_42/status", page_text)
        self.assertNotIn("/plugin/CloudDrivePlexSync/history/", page_text)

    @unittest.skipUnless(
        shutil.which("node"), "Node.js is needed for form action tests"
    )
    def test_clone_manual_action_uses_its_own_route_and_state(self) -> None:
        from clouddriveplexsync.manual_scan_ui import manual_scan_action

        action = manual_scan_action("CloudDrivePlexSyncClone_42")
        javascript = r"""
const assert = require('node:assert/strict');
const action = JSON.parse(require('node:fs').readFileSync(0, 'utf8'));
const model = { manual_plex_path: '/plex/show' };
const invoke = new Function('model', `with (model) { return (${action})(); }`);
let actual;
global.window = { MoviePilotAPI: { post: async (...args) => {
  actual = args; return { success: true, message: '', data: null };
} } };
invoke(model).then(() => {
  assert.deepEqual(actual, ['plugin/CloudDrivePlexSyncClone_42/resync/plex', { plex_path: '/plex/show' }]);
  assert.equal(model[Symbol.for('clouddriveplexsync.manual_scan.CloudDrivePlexSyncClone_42')].ok, true);
}).catch(error => { console.error(error); process.exitCode = 1; });
"""
        result = subprocess.run(
            ["node", "-e", javascript],
            input=json.dumps(action),
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()

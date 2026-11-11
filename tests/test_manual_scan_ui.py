from __future__ import annotations

import json
import shutil
import subprocess
import unittest

import _bootstrap  # noqa: F401
from clouddriveplexsync.manual_scan_ui import MANUAL_SCAN_ACTION

NODE_FORM_TEST = r"""
const assert = require('node:assert/strict');
const fs = require('node:fs');
const action = JSON.parse(fs.readFileSync(0, 'utf8'));
const invoke = new Function('model', 'event', `with (model) { return (${action})(event); }`);
const key = Symbol.for('clouddriveplexsync.manual_scan');

(async () => {
  const model = { manual_plex_path: '' };
  const calls = [];
  let resolveRequest;
  global.window = { MoviePilotAPI: { post: (...args) => {
    calls.push(args);
    return new Promise(resolve => { resolveRequest = resolve; });
  } } };

  await invoke(model, {});
  assert.match(model[key].message, /请输入/);
  assert.equal(calls.length, 0);
  model.manual_plex_path = ' /plex/电影/影片 ';
  const pending = invoke(model, {});
  assert.equal(model[key].busy, true);
  await invoke(model, {});
  assert.equal(calls.length, 1);
  assert.deepEqual(calls[0], [
    'plugin/CloudDrivePlexSync/resync/plex', { plex_path: '/plex/电影/影片' },
  ]);
  resolveRequest({ success: true, message: '已加入扫描队列' });
  await pending;
  assert.equal(model[key].busy, false);
  assert.equal(model[key].ok, true);
  assert.equal(model[key].message, '已加入扫描队列');
  assert.deepEqual(JSON.parse(JSON.stringify(model)), { manual_plex_path: ' /plex/电影/影片 ' });

  window.MoviePilotAPI.post = async () => ({ success: false, message: '媒体库不匹配' });
  await invoke(model, {});
  assert.equal(model[key].ok, false);
  assert.equal(model[key].message, '媒体库不匹配');
  window.MoviePilotAPI.post = async () => { throw new Error('连接失败'); };
  await invoke(model, {});
  assert.equal(model[key].busy, false);
  assert.equal(model[key].message, '连接失败');
})().catch(error => { console.error(error); process.exitCode = 1; });
"""


class ManualScanUITests(unittest.TestCase):
    @unittest.skipUnless(
        shutil.which("node"), "Node.js is needed for form action tests"
    )
    def test_form_action_validates_submits_and_shows_errors(self) -> None:
        result = subprocess.run(
            ["node", "-e", NODE_FORM_TEST],
            input=json.dumps(MANUAL_SCAN_ACTION),
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)

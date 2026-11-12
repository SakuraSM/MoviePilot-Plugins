"""MoviePilot configuration controls for submitting a Plex directory once."""

from __future__ import annotations

import json
from urllib.parse import quote

# FormRender evaluates onClick in the scope of its reactive form model.
_MANUAL_SCAN_TEMPLATE = """async () => {
  const stateKey = Symbol.for(__STATE_KEY__);
  if (!model[stateKey]) model[stateKey] = { busy: false, ok: false, message: '' };
  const state = model[stateKey];
  if (state.busy) return;
  const path = typeof model.manual_plex_path === 'string'
    ? model.manual_plex_path.trim() : '';
  if (!path) {
    state.ok = false;
    state.message = '请输入 Plex 容器中的绝对目录路径';
    return;
  }
  state.busy = true;
  state.message = '';
  try {
    if (!window.MoviePilotAPI) throw new Error('MoviePilot API 不可用');
    const result = await window.MoviePilotAPI.post(
      __API_PATH__, { plex_path: path }
    );
    state.ok = result?.success === true;
    state.message = result?.message || (
      state.ok ? '已加入扫描队列' : '提交失败，请查看插件日志'
    );
  } catch (error) {
    state.ok = false;
    state.message = error?.response?.data?.detail
      || error?.response?.data?.message || error?.message || '提交失败，请查看插件日志';
  } finally {
    state.busy = false;
  }
}"""


def _state_key(plugin_id: str) -> str:
    if plugin_id == "CloudDrivePlexSync":
        return "clouddriveplexsync.manual_scan"
    return f"clouddriveplexsync.manual_scan.{plugin_id}"


def manual_scan_action(plugin_id: str) -> str:
    api_path = f"plugin/{quote(plugin_id, safe='')}/resync/plex"
    return _MANUAL_SCAN_TEMPLATE.replace(
        "__STATE_KEY__", json.dumps(_state_key(plugin_id))
    ).replace("__API_PATH__", json.dumps(api_path))


MANUAL_SCAN_ACTION = manual_scan_action("CloudDrivePlexSync")


def manual_scan_card(*, plugin_id: str = "CloudDrivePlexSync") -> dict[str, object]:
    state = f"model[Symbol.for({json.dumps(_state_key(plugin_id))})]"
    return {
        "component": "VCard",
        "props": {"title": "手动刷新媒体库目录", "class": "my-3 pa-3"},
        "content": [
            {
                "component": "VTextField",
                "props": {
                    "model": "manual_plex_path",
                    "label": "Plex 容器中的目录绝对路径",
                    "variant": "outlined",
                    "placeholder": "/data/CloudNas/Guangya/电影/影片名称",
                    "hint": "点击按钮立即提交，无需保存配置。输入媒体库根目录会扫描该库的整个根目录。",
                    "persistent-hint": True,
                    "clearable": True,
                },
            },
            {
                "component": "VBtn",
                "text": "刷新指定目录",
                "props": {
                    "type": "button",
                    "color": "primary",
                    "class": "mt-3",
                    "disabled": "{{ " + state + "?.busy === true }}",
                    "loading": "{{ " + state + "?.busy === true }}",
                    "onClick": manual_scan_action(plugin_id),
                },
            },
            {
                "component": "VAlert",
                "props": {
                    "class": "mt-3",
                    "show": "{{ Boolean(" + state + "?.message) }}",
                    "text": "{{ " + state + "?.message || '' }}",
                    "type": "{{ " + state + "?.ok ? 'success' : 'error' }}",
                    "role": "status",
                    "aria-live": "polite",
                },
            },
        ],
    }

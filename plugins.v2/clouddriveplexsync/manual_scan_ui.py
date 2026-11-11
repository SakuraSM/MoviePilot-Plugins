"""MoviePilot configuration controls for submitting a Plex directory once."""

from __future__ import annotations

# FormRender evaluates onClick in the scope of its reactive form model.
MANUAL_SCAN_ACTION = """async () => {
  const stateKey = Symbol.for('clouddriveplexsync.manual_scan');
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
      'plugin/CloudDrivePlexSync/resync/plex', { plex_path: path }
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


def manual_scan_card() -> dict[str, object]:
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
                    "disabled": "{{ model[Symbol.for('clouddriveplexsync.manual_scan')]?.busy === true }}",
                    "loading": "{{ model[Symbol.for('clouddriveplexsync.manual_scan')]?.busy === true }}",
                    "onClick": MANUAL_SCAN_ACTION,
                },
            },
            {
                "component": "VAlert",
                "props": {
                    "class": "mt-3",
                    "show": "{{ Boolean(model[Symbol.for('clouddriveplexsync.manual_scan')]?.message) }}",
                    "text": "{{ model[Symbol.for('clouddriveplexsync.manual_scan')]?.message || '' }}",
                    "type": "{{ model[Symbol.for('clouddriveplexsync.manual_scan')]?.ok ? 'success' : 'error' }}",
                    "role": "status",
                    "aria-live": "polite",
                },
            },
        ],
    }

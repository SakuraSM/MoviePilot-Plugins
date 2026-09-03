# SakuraSM MoviePilot Plugins

面向 MoviePilot V2 的第三方插件市场仓库。插件索引位于 `package.v2.json`，每个插件的
运行时代码位于 `plugins.v2/<插件 ID 小写>/`。

## 使用方法

在 MoviePilot V2 的插件市场设置中添加仓库地址：

```text
https://github.com/SakuraSM/MoviePilot-Plugins
```

MoviePilot 会读取仓库 `main` 分支的 `package.v2.json`，并将其中的插件作为独立条目
展示和安装。

## 插件列表

| 插件 ID | 名称 | 版本 | 说明 |
|---|---|---:|---|
| `CloudDrivePlexSync` | CloudDrive Plex 增量同步 | 1.3.1 | 订阅 CD2 Pro 文件变化，或独立轮询 TTD 整理历史并触发 Plex 指定目录扫描 |

详细配置见
[`plugins.v2/clouddriveplexsync/README.md`](plugins.v2/clouddriveplexsync/README.md)。

## 仓库结构

```text
package.v2.json
plugins.v2/
  clouddriveplexsync/
    __init__.py
    README.md
tests/
```

插件 ID、目录和主类必须一一对应：

```text
package.v2.json: CloudDrivePlexSync
Python 主类:     CloudDrivePlexSync
插件目录:        plugins.v2/clouddriveplexsync/
```

## 新增插件

1. 在 `plugins.v2/` 下创建以插件类名全小写命名的目录。
2. 在目录的 `__init__.py` 中定义 MoviePilot V2 插件主类。
3. 在 `package.v2.json` 顶层增加以插件类名为键的元数据。
4. 保持清单版本与类中的 `plugin_version` 一致。
5. 为独立逻辑增加测试，并运行：

```bash
python3 -m unittest discover -s tests -v
```

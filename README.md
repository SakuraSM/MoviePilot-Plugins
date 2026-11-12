# SakuraSM MoviePilot Plugins

面向 MoviePilot V2 和 V3 的第三方插件市场仓库。两个版本使用独立的插件索引和源码目录。

## 使用方法

在 MoviePilot 的插件市场设置中添加仓库地址：

```text
https://github.com/SakuraSM/MoviePilot-Plugins
```

MoviePilot V2 读取 `package.v2.json`；V3 读取 `package.v3.json`。两者均使用仓库的
`main` 分支。

## 插件列表

| 插件 ID | 名称 | V2 版本 | V3 版本 | 说明 |
|---|---|---:|---:|---|
| `CloudDrivePlexSync` | TgToDrive Plex 增量同步 | 2.2.1 | 3.0.0 | 支持 TMDB 核验、历史处理开关和 Plex 局部扫描 |

详细配置见
[`V2 使用说明`](plugins.v2/clouddriveplexsync/README.md)和
[`V3 使用说明`](plugins.v3/clouddriveplexsync/README.md)。

## 仓库结构

```text
package.v2.json
package.v3.json
plugins.v2/
  clouddriveplexsync/
    __init__.py
    README.md
plugins.v3/
  clouddriveplexsync/
    __init__.py
    README.md
tests/
```

插件 ID、目录和主类必须一一对应：

```text
package.v2.json: CloudDrivePlexSync
package.v3.json: CloudDrivePlexSync
Python 主类:     CloudDrivePlexSync
插件目录:        plugins.v2/clouddriveplexsync/ 或 plugins.v3/clouddriveplexsync/
```

## 新增插件

1. 在对应主版本的插件目录下，创建以插件类名全小写命名的目录。
2. 在目录的 `__init__.py` 中定义插件主类。
3. 在对应的 `package.v2.json` 或 `package.v3.json` 中增加以插件类名为键的元数据。
4. 保持清单版本与类中的 `plugin_version` 一致。
5. 为独立逻辑增加测试，并运行：

```bash
python3 -m unittest discover -s tests -v
MOVIEPILOT_PLUGIN_GENERATION=v3 python3 -m unittest discover -s tests -v
```

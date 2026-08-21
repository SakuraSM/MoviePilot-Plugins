# MoviePilot V2 插件目录

每个一级子目录对应一个可独立安装的插件，目录名必须是插件主类名的小写形式。

当前插件：

- `clouddriveplexsync` → `CloudDrivePlexSync`

新增插件时还必须在仓库根目录的 `package.v2.json` 中添加同名插件 ID。插件之间不得
相互导入私有模块；需要共享的代码应复制进插件目录或发布为 MoviePilot 已安装的公共依赖，
确保每个插件可以被单独下载和加载。

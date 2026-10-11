# TgToDrive Plex 增量同步

MoviePilot V2 插件。通过 TgToDrive OpenAPI 读取成功的整理记录，将目标目录映射为
Plex 容器路径，再提交该目录的局部扫描。插件只查询整理历史，不读取媒体内容，也不遍历网盘。

## 升级到 2.0.0

插件 ID、目录和配置前缀仍为 `CloudDrivePlexSync`、`clouddriveplexsync` 和
`clouddriveplexsync_`，现有安装可直接升级。

2.0.0 仅保留 TgToDrive OpenAPI → Plex：

- 移除 CloudDrive2 推送、目录刷新、拓扑查询和挂载点推导。
- 移除 Buffer 调整、恢复和播放状态检测。
- 移除 MoviePilot 入库事件、旧 Cookie 认证及相关配置、API。
- 保留直接路径映射、目录去重、跳过规则、扫描限速和手动补扫。
- 新增近期重叠回查、持久化去重账本、分批完整历史对账和执行中任务恢复。

升级后填写 OpenAPI Token。旧 Cookie 不会转换为 Token。已有直接路径映射、Plex 服务、
最近记录游标和待扫描队列会继续使用；依赖 CD2 MountPoint 的旧映射需要改为直接映射。

首次运行会补扫去重账本尚未登记的历史目录，包括升级前未建立详细账本的记录。
旧游标只作为翻页锚点，不会把仅知道 ID 的记录当作已核对的完整版本。
同目录任务会合并，扫描按配置限速。默认不匹配 Plex 媒体库时保留并重试，旧配置显式选择
`skip` 时仍按该策略跳过。

## 前置条件

- MoviePilot V2 2.12.0 或更高版本。
- TgToDrive 已开启 OpenAPI，并创建有效 Token。
- MoviePilot 已配置并连接 Plex，Plex 版本至少为 1.20.0.3125。
- Plex 容器能够访问映射后的媒体目录。

插件不管理文件挂载。Plex 使用的挂载服务需要自行保持可用。

## 配置

在 TTD 的“实用工具 → OpenAPI”创建 Token，再填写插件配置：

```text
启用插件：开启
TgToDrive 地址：https://ttd.nings.top
TgToDrive API Token：你的 OpenAPI Token
TgToDrive 来源：光鸭云盘
Plex 服务：MoviePilot 中的 Plex 服务名
Plex 媒体库 ID：1,2（留空允许该服务的全部媒体库）
TTD 目标根目录：/光鸭云盘/Media/Video/已整理
TTD 逻辑路径 → Plex 容器路径：
/光鸭云盘/Media/Video/已整理 => /data/CloudNas/Guangya
```

TTD 目标根目录填写云端逻辑路径。Plex 容器路径只填写在映射右侧。
多条映射按最长前缀匹配，并检查完整目录边界。目标越出根目录、包含 `..` 或无法匹配
Plex 媒体库时，不会退化为整库扫描。

Token 支持原始值、`Bearer ...` 和 `Authorization: Bearer ...` 三种形式。
插件只把 Token 放在 Authorization 请求头中，不写入 URL、状态页或运行日志。
TTD 的 OpenAPI 凭据拥有后台管理员权限，请使用 HTTPS 并妥善保存，需要时在 TTD 撤销。

地址可以是服务根地址，也可以是完整的 `/api/organize-history?...` 地址。
反向代理使用子路径时需要包含前缀，已有查询参数会被移除。

“跳过目录”支持相对于目标根目录的路径或完整逻辑路径，每行一条。
跳过目录和显式选择跳过的不匹配媒体库记录会记账，不再自动重试。
修改跳过规则不会自动重放已跳过记录，可通过手动补扫处理指定目录。

## 防漏与恢复

插件请求 `GET /api/organize-history`，传入 `source`、`page`、`limit` 和 `status=success`。
默认配置如下：

| 配置 | 默认值 | 行为 |
|---|---:|---|
| 轮询间隔 | 30 秒 | 快速发现新增成功记录 |
| 每页记录数 | 100 | 单页查询量 |
| 快轮询最大页数 | 5 | 限制单轮请求量 |
| 遇到旧记录后继续回查 | 2 页 | 覆盖近期晚完成的旧记录 |
| 完整历史对账间隔 | 6 小时 | 从上一次对账完成时计算 |
| 完整历史对账批量 | 每轮最多 2 页 | 与快轮询共用周期，断点续跑 |
| 同目录合并等待 | 30 秒 | 合并重复扫描请求 |
| 每秒提交扫描数 | 1 | 限制 Plex 请求速率 |
| 扫描队列容量 | 10,000 | 容量不足时保留整理记录重试 |

通常无新记录时，快轮询读取 3 页。历史对账进行中每轮再读取最多 2 页。
增加回查窗口或缩短轮询间隔会增加 TTD 查询量，不会增加网盘目录遍历。

处理记录后，将记录 ID、目标路径、事件时间和状态对应的版本保存到插件数据目录中的
`history.sqlite3`。账本不受最近 1,000 个游标 ID 的限制。同一 ID 的目标或事件时间变化时，
会作为新版本重新处理。

快轮询遇到已处理 ID 后仍检查当前页，并继续回查配置的页数。窗口之外晚成功的记录由
完整历史对账补查。达到快轮询页数上限仍未找到旧游标时，会记录历史缺口并启动分批对账，
不会把当前窗口视为已经补齐全部历史。

对账每处理完一页才保存下一页页码；提交失败时停留在当前页。重启后从已保存的位置继续。
队列接受任务并持久化后，才将整理记录记入账本。账本写入前中断可能导致重复提交，目录
队列会合并这些重复任务。

扫描任务执行前会持久化为 `inflight`。Plex 接受局部扫描请求后，任务才从队列移除。
重启时 `queued` 和 `inflight` 都恢复为待执行任务。扫描期间同目录出现的新变化会保留为
下一次扫描任务。单个目录失败不会阻塞其它目录。
旧队列格式无效或恢复任务超过配置容量时，插件拒绝启动并报告原因，不会静默丢弃任务。

这里保证的是扫描请求可重试，Plex 接受请求不等于媒体索引已经完成。
服务端当前提供页码分页，未提供快照或稳定增量事件游标。对账期间数据可能变化，因此
不能承诺任意并发修改下绝对不漏；记录在服务端保留足够时间、接口可用且任务最终成功时，
周期对账会持续补查。源记录在查询前被删除或清理后，插件无法恢复其内容。

`401/403` 和登录重定向按 5、15、30、60 分钟退避；网络错误按 5、15、30、60 秒退避。
恢复期间保留游标、账本和待扫描队列。

## 插件 API

所有接口需要 MoviePilot Bearer 认证，插件 ID 仍为 `CloudDrivePlexSync`：

- `GET /api/v1/plugin/CloudDrivePlexSync/status`：查询认证、队列、对账进度和最近记录。
- `POST /api/v1/plugin/CloudDrivePlexSync/test`：验证 TTD 历史查询与 Plex 连接。
- `POST /api/v1/plugin/CloudDrivePlexSync/flush`：立即处理当前待扫描目录。
- `POST /api/v1/plugin/CloudDrivePlexSync/resync`：手动提交一个准确目录。
- `POST /api/v1/plugin/CloudDrivePlexSync/reconcile`：从第一页重新启动历史对账，按账本去重。

手动补扫请求示例：

```json
{
  "cloud_path": "/光鸭云盘/Media/Video/已整理/电影/片名"
}
```

状态中的 `audit_next_page=0` 表示本轮对账结束，其它值为下一页页码。
`history_gap` 表示快轮询曾未找到旧游标，完成一轮对账后清除。
`inflight_scans` 表示正在向 Plex 提交的任务数量。

## 开发验证

核心测试使用临时 SQLite、模拟 HTTP 与 Plex，不会修改真实媒体库：

```bash
python3 -m unittest discover -s tests -v
python3 -m compileall -q plugins.v2/clouddriveplexsync
```

仓库中的 `.env` 和 `.env.*` 只用于本地验证，并已加入 Git 忽略规则。
插件运行时使用 MoviePilot 配置页的 Token，不自动读取开发目录中的凭据文件。

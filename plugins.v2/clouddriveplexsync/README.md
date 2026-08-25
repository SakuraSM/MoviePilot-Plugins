# CloudDrive Plex 增量同步

MoviePilot V2 插件。它订阅 CloudDrive2 Pro 的文件变化推送，也可以增量轮询
TgToDrive（TTD）的整理历史，将变化路径转换为 Plex 容器可见路径，并只扫描发生变化
的目录。插件不会读取媒体内容，也不会在空闲时轮询网盘目录。

## 前置条件

- MoviePilot V2 2.12.0 或更高版本。
- CloudDrive2 1.0.14 Pro。
- Plex Media Server 1.20.0.3125 或更高版本。
- MoviePilot 已配置并连接 Plex。
- CloudDrive2 的 FUSE 子挂载已经以 `rslave` 或 `shared` 传播给 Plex。

CloudDrive2 API Token 只需要以下权限：

- Push Messages
- Get Mounts
- List Files
- Get Cloud APIs
- Modify Cloud APIs

插件不需要 Read Files、Write、Rename 或 Delete 权限。

## TgToDrive 整理历史

TTD 主要负责整理资源时，可以启用 Cookie 认证的整理历史轮询。推荐配置：

```text
启用 TgToDrive 整理历史轮询：开启
TgToDrive 地址：https://ttd.nings.top
TgToDrive 登录 Cookie：session=你的会话值
TgToDrive 来源筛选：光鸭云盘
TgToDrive 目标根目录：/光鸭云盘/Media/Video/已整理
TgToDrive 跳过路径：待整理-通用、_整理中（每行一个，可选）
不属于所选 Plex 媒体库时：跳过并推进游标
TgToDrive 轮询间隔：30
TgToDrive 每页记录数：20
TgToDrive 最大补页数：5
TgToDrive 首次运行：仅建立基线，不处理历史
扫描前刷新准确的 CD2 目标目录：开启
```

Cookie 可以粘贴为 `session=...`，也兼容以 `Cookie:` 开头的完整请求头值。插件只把它
放在 HTTPS 请求头中，不会写入运行日志、状态页或 URL。MoviePilot 配置存储是否加密
取决于 MoviePilot 本身，因此建议使用权限尽可能小、可随时注销的 TTD 会话。
“TgToDrive 地址”既可以填写站点根地址，也可以直接粘贴完整的
`/api/organize-history?...` 地址；插件会移除已有查询参数并使用配置页中的筛选条件。

默认首次连接只保存当前第一页作为基线，不会把既有历史重新送入 Plex。选择“处理
当前最新记录”时才会处理第一页已有的成功记录。后续轮询采用以下保护：

- 记录 ID 存在时以 ID 去重，否则根据目标、时间、来源和文件名生成稳定指纹。
- 新记录超过一页时继续翻页，直到找到已保存游标，最多读取配置的补页数。
- 达到最大补页数仍找不到游标时停止推进并报警，不会静默跳过缺口。
- 同一批次多个记录落入同一目标目录时，只刷新和提交该目录一次。
- 配置的跳过路径和不属于所选 Plex 媒体库的目标会记为一次 `SKIP` 并推进游标，
  不发送错误通知，也不会阻塞后续正常记录。
- Plex 暂时不可用、扫描队列已满等可恢复问题会保留游标并重试；相同原因只上报一次，
  恢复或错误内容变化后才会重新上报。
- `401/403` 或登录重定向会标记 Cookie 失效，并按 5、15、30、60 分钟退避。
- 网络及服务端错误按 5、15、30、60 秒退避。
- TTD 地址、来源或目标根目录改变时自动建立新游标基线，避免跨数据源误去重。

TTD 通常返回相对于整理目标根目录的路径，例如 `动漫/片名/Season 1`。插件将它拼接
到“TgToDrive 目标根目录”，再执行 watch root、路径映射和 Plex Section 校验；任何一层
不匹配都不会退化成整库扫描。配置了“CD2 云端路径 → Plex 路径”时会直接转换，不依赖
CD2 挂载点；未命中直接映射时才使用 MountPoint 和旧的挂载路径覆盖作为兼容兜底。

“TgToDrive 跳过路径”支持相对于目标根目录的路径（如 `待整理-通用`），也支持完整的
CD2 云端路径。匹配按完整目录边界执行，`待整理-通用2` 不会误命中
`待整理-通用`。默认的“不属于所选 Plex 媒体库时”策略为跳过；如确实希望等待媒体库
配置修复后再处理，可改成“保留记录并持续重试”。

开启“扫描前刷新准确的 CD2 目标目录”后，每个去重目录会调用一次
`GetSubFiles(forceRefresh=true)`。插件只刷新 TTD 返回并映射后的准确目标目录；目标目录
不存在表示 TTD 地址或路径映射需要修正。刷新失败时仍会提交 Plex 局部扫描并保留错误记录。

配置页按“基础、路径与触发、TgToDrive、Buffer、高级”五个标签页组织。TTD 目标根目录
始终填写 CD2 云端路径；`/data/...` 形式的 Plex 容器路径只填写在 Plex 路径映射中。

## 当前环境配置示例

监听根目录：

```text
/光鸭云盘/Media/Video/已整理
```

推荐直接将 CD2 云端路径转换成 Plex 容器路径：

```text
/光鸭云盘/Media/Video/已整理 => /data/CloudNas/Guangya
```

以下旧式挂载路径映射继续保留为兼容兜底：

```text
/CloudNAS/Guangya => /data/CloudNas/Guangya
```

如果 MoviePilot 也直接写 CloudDrive2 挂载目录，可配置：

```text
/media/CloudNas/Guangya => /CloudNAS/Guangya
```

路径映射使用最长前缀和完整目录边界，不会检查路径在 MoviePilot 容器中是否真实存在，
因此删除事件也可以正常映射。

## Buffer 模式

- `disabled`：不读取、不修改 CD2 Buffer。
- `fixed`：扫描期间统一使用“空闲扫描 Buffer”。
- `adaptive`：没有播放时使用空闲值；正在播放同一网盘内容时使用播放值。

默认空闲值是 2MB，播放值是 8MB。扫描结束并经过静默期后，插件恢复扫描前的值。
恢复前会重新读取配置；如果用户已经手动修改 Buffer，插件不会覆盖用户的新值。
“Buffer 最小值”默认是 1MB，并同时约束全局值和网盘级覆盖；实际最大值会按 CD2
`maxBufferPoolSizeMBLimit` 自动截断。

可以按网盘覆盖全局值：

```text
光鸭云盘|2|8
OneDrive|4|16
```

标识可使用 `网盘类型|账号`、云端路径、昵称或网盘类型。

## Plex 设置建议

对于网盘媒体库建议关闭：

- 自动扫描媒体库
- 周期扫描
- 扫描后自动清空垃圾箱
- 深度媒体分析
- 自动生成视频预览缩略图

插件直接调用 Plex 的 `LibrarySection.update(path=...)`。路径无法匹配已选择的 Plex
媒体库时会拒绝处理，不会回退为整库扫描。

## 插件 API

所有接口都需要 MoviePilot Bearer 认证：

- `GET /api/v1/plugin/CloudDrivePlexSync/status`
- `POST /api/v1/plugin/CloudDrivePlexSync/test`
- `POST /api/v1/plugin/CloudDrivePlexSync/flush`
- `POST /api/v1/plugin/CloudDrivePlexSync/resync`
- `POST /api/v1/plugin/CloudDrivePlexSync/reconnect`
- `POST /api/v1/plugin/CloudDrivePlexSync/restore-buffer`
- `POST /api/v1/plugin/CloudDrivePlexSync/preview-buffer`

手动补扫请求：

```json
{
  "cloud_path": "/光鸭云盘/Media/Video/已整理/电影/片名"
}
```

补扫只强制重新列出指定目标目录一次，然后提交相同目录的 Plex 局部扫描，不会递归遍历。

## 请求量与限制

未启用 TTD 时，空闲只有一条 `PushMessage` 长连接，不调用 `GetSubFiles`。启用 TTD
后，每个轮询周期增加一次 TTD 历史 HTTP 请求；30 秒间隔约为每小时 120 次。正常无
新记录时不访问 CD2 目录，也不触发 Plex。发现新记录后，每个去重目标目录最多增加
一次 CD2 精确刷新和一次 Plex 本地局部扫描。

CD2 推送协议没有事件游标。断线期间可能漏掉外部网盘变化，恢复连接后插件不会自动
遍历整个网盘；状态页会保留断线时间，此时应对已知目录执行一次手动补扫。

如果 `isCloudEventListenerRunning` 为 false，说明对应网盘当前没有云端事件监听，外部
变化无法保证近实时。

## 开发验证

插件核心模块不依赖 MoviePilot 即可运行单元测试：

```bash
python3 -m unittest discover -s tests -v
python3 -m compileall -q plugins.v2/clouddriveplexsync
```

# CloudDrive Plex 增量同步

MoviePilot V2 插件。它订阅 CloudDrive2 Pro 的文件变化推送，将变化路径转换为 Plex
容器可见路径，并只扫描发生变化的目录。插件不会读取媒体内容，也不会在空闲时轮询
网盘目录。

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

## 当前环境配置示例

监听根目录：

```text
/光鸭云盘/Media/Video/已整理
```

CloudDrive2 容器中的挂载路径转换成 Plex 容器路径：

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

补扫只强制重新列出指定目录一次，然后提交相同目录的 Plex 局部扫描，不会递归遍历。

## 请求量与限制

空闲时只有一条 `PushMessage` 长连接，不调用 `GetSubFiles`。每个扫描批次、每个受
影响网盘最多产生两组配置 GET/SET（应用和恢复）以及每个去重目录一次 Plex 本地扫描。

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

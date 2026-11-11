# OSM Builder

下载、编译并向 Cloudflare R2 发布 OpenStreetMap 地区数据，供 [OSM Worker](https://github.com/sirlaurie/osm-worker) 查询。部署时先上线 OSM Worker，再配置本项目。

## 编译与配置

需要 macOS 或 Linux、Rust 1.95+、C/C++ 工具链。编译产物位于 `.build/tools/rust/release/osm`：

```sh
cargo build --release
```

在项目根目录创建 `.env`（R2 API 令牌需具备 Object Read & Write 权限）：

```dotenv
R2_ACCOUNT_ID="<Cloudflare 账户 ID>"
R2_BUCKET="path-osm-data"
AWS_ACCESS_KEY_ID="<R2 Access Key ID>"
AWS_SECRET_ACCESS_KEY="<R2 Secret Access Key>"
OSM_WORKER_URL="https://aura-osm-data.<子域>.workers.dev"
OSM_PUBLISH_TOKEN="<与 OSM Worker 及 Purchase Worker 相同的 PUBLISH_TOKEN>"
```

本地数据默认存放在 `.build/data`，可通过 `OSM_DATA_DIR` 修改，所在磁盘至少保留 10 GiB 可用空间。多台处理设备共用同一个 OSM Worker 和 R2 存储桶，但各自的 `.device-id` 不可复制。

## 常用命令

| 命令 | 说明 |
| --- | --- |
| `osm list` | 列出 `config/regions.json` 中的全部地区 |
| `osm sync-regions` | 拉取线上用户分布国家，将未覆盖国家写入 `config/regions.json` |
| `osm sync-regions --submit` | 先为新增国家提交更新批次，提交成功后写入 `config/regions.json` |
| `osm bootstrap <地区\|all> --submit-only` | 提交首次构建批次 |
| `osm update <地区\|all> --submit-only` | 提交增量更新批次（指定 `all` 时批次包含未覆盖的用户国家，不改写 `config/regions.json`；加 `--no-sync-regions` 跳过） |
| `osm work --cleanup` | 领取并处理队列任务，发布确认后删除本地临时数据 |
| `osm service --cleanup` | 在本机安装或重启后台常驻服务（macOS `launchd` / Linux `systemd`） |
| `osm deploy` | 通过 SSH 让所有工作机拉取代码、编译并平滑重启后台服务 |
| `osm jobs` | 查看当前批次进度与失败原因 |
| `osm published-state` | 查看线上已发布的地区与版本 |
| `osm cancel` | 取消未完成的批次 |
| `osm retire <地区>` | 下线指定地区 |
| `osm gc --apply` | 清理 R2 中不再被引用的数据块 |

## 工作机常驻与远程发布

1. **首次在工作机安装服务**
   - macOS 工作机请将仓库放在 `~/osm-builder` 等目录，避开受 TCC 隐私保护的 `~/Documents`、`~/Desktop`、`~/Downloads`（`launchd` 访问这些目录会触发弹窗并挂起进程）。
   - Linux 先开启用户级后台驻留：`loginctl enable-linger "$USER"`。
   - 在每台工作机的仓库目录执行：
     ```sh
     cargo build --release && .build/tools/rust/release/osm service --cleanup
     ```
     该命令会注册后台服务，并将当前仓库路径记录到 `~/.config/aura-osm/root`。

2. **在开发机配置工作机列表**
   在开发机的 `.env` 中添加 `OSM_WORKER_HOSTS`。若已在工作机执行过 `osm service`，填 SSH 主机名即可；也可通过 `host:path` 显式指定远端目录：
   ```dotenv
   OSM_WORKER_HOSTS="mac-mini,debian"
   ```

3. **日常推送更新**
   开发机提交并推送 Git 后执行：
   ```sh
   osm deploy
   ```
   `osm deploy` 会依次连接各台工作机拉取代码、编译并执行 `osm service --cleanup`。旧进程收到 `SIGTERM` 后不再领取新任务，等待手头正在处理的地区上传并清理完毕后退出（最长等待 3600 秒），随后由新进程接管。排空期间再次收到 `SIGTERM` 或 `SIGINT`，进程立即退出。

## 地区扩容与每月更新

`osm sync-regions` 会携带 `OSM_PUBLISH_TOKEN` 请求 `https://apple.autheai.com/api/user_countries`（可通过 `OSM_USER_COUNTRIES_URL` 覆盖），筛选生产环境用户数大于 1 的国家（排除 `CN`、`RU`），将尚未覆盖的国家补充到 `config/regions.json`。Geofabrik 目录中没有对应切片的国家会被跳过并输出提示。

`config/regions.json` 以 git 为准。`osm update all --submit-only` 只在批次中加入新国家，不写文件。在开发机执行 `osm sync-regions` 并提交 git，新地区才进入配置。

每月全量更新可在 macOS 上通过 `osm schedule` 安装定时任务，或在 Linux 上配置 cron / systemd timer：

```sh
cd /path/to/osm-builder && git pull --ff-only && cargo build --release \
  && { .build/tools/rust/release/osm gc --apply; \
       .build/tools/rust/release/osm update all --submit-only --replace; }
```

## 测试

```sh
cargo test   # 需安装 osmium-tool
```

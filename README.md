# OSM Builder

OSM Builder 下载、处理和上传 OpenStreetMap 地点数据。数据存储：Cloudflare R2。[OSM Worker](https://github.com/sirlaurie/osm-worker) 读取这些数据，提供查询接口。

部署顺序：

1. OSM Worker
2. OSM Builder

## 安装

环境要求：macOS 或 Linux、Rust 1.95+、C/C++ 编译工具。

```sh
cargo build --release
```

配置步骤：

1. 创建 Cloudflare [R2 API 令牌](https://developers.cloudflare.com/r2/api/tokens/)。令牌权限：Object Read & Write。
2. 创建 `osm-builder/.env` 文件。文件内容：

```dotenv
R2_ACCOUNT_ID="<Cloudflare 账户 ID>"
R2_BUCKET="path-osm-data"
AWS_ACCESS_KEY_ID="<R2 Access Key ID>"
AWS_SECRET_ACCESS_KEY="<R2 Secret Access Key>"
OSM_WORKER_URL="https://aura-osm-data.<你的子域>.workers.dev"
OSM_PUBLISH_TOKEN="<OSM Worker 的 PUBLISH_TOKEN>"
```

默认数据目录：`.build/data`。自定义数据目录：设置 `OSM_DATA_DIR`。磁盘可用空间要求：10 GiB 或以上。

## 使用

| 操作 | 命令 |
| --- | --- |
| 查看可用地区 | `osm list` |
| 处理任务（每台处理设备） | `osm work --cleanup` |
| 安装或重启后台驻留服务（macOS / Linux） | `osm service --cleanup` |
| 远程更新全部处理设备 | `osm deploy` |
| 提交首次构建 | `osm bootstrap all --submit-only` |
| 提交更新 | `osm update all --submit-only` |
| 查看任务进度和失败原因 | `osm jobs` |
| 查看已发布地区 | `osm published-state` |
| 取消未完成的批次 | `osm cancel` |
| 下线地区 | `osm retire <地区>` |
| 删除 R2 中不再使用的数据 | `osm gc --apply` |

`osm` 的路径：`.build/tools/rust/release/osm`。

`bootstrap` 和 `update` 接受一个或多个地区名。`all` 表示 `config/regions.json` 中的全部地区。

所有处理设备共用仓库、OSM Worker 和 R2 存储桶。`.device-id` 文件属于单台设备，禁止复制。

## 后台服务与远程发布

1. 首次初始化：
   - Debian 开启用户级后台常驻：`loginctl enable-linger "$USER"`。
   - Mac mini 和 Debian 在各自仓库目录执行：`cargo build --release && .build/tools/rust/release/osm service --cleanup`（该命令同时将本机仓库路径写入 `~/.config/aura-osm/root`）。
2. 开发机 `osm-builder/.env` 配置目标机器（支持 `host:path` 指定不同路径，或填 `host` 读取远端 `~/.config/aura-osm/root`）：

```dotenv
OSM_WORKER_HOSTS="mac-mini:/Users/<user>/Aura/osm-builder,debian:/home/<user>/Aura/osm-builder"
```

3. 日常代码发布（开发机 `git push` 后执行）：

```sh
osm deploy
```

`osm deploy` 登录各台机器拉取代码、编译并执行 `osm service --cleanup`。`osm service --cleanup` 向旧进程发送 `SIGTERM`。空闲线程立即退出；运行中的任务完成当前地区的处理、上传与清理后退出（最长等待 3600 秒），然后启动新进程。

## 每月更新

执行设备：一台 Linux 设备。执行方式：cron 或 systemd timer。命令：

```sh
cd /path/to/osm-builder && git pull --ff-only && cargo build --release \
  && { .build/tools/rust/release/osm gc --apply; \
       .build/tools/rust/release/osm update all --submit-only --replace; }
```

命令失败的处理：人工处理。定时任务需要失败通知。

Mac 的每月更新任务：`osm schedule`。

## 开发

```sh
cargo test   # 依赖：osmium-tool
```

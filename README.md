# OSM Builder

在本地设备下载 OpenStreetMap 地点数据，处理后上传到 Cloudflare R2，供配套的 [OSM Worker](../osm-worker/README.md) 提供附近地点查询。

**部署顺序：OSM Worker → OSM Builder → 应用查询。** Builder 负责准备和更新数据，Worker 负责对外提供接口。

## 部署

需要 macOS 或 Linux、Rust 1.95+ 和 C/C++ 编译工具；macOS 使用 Xcode Command Line Tools。默认要求数据盘和项目所在盘各有 60 GiB 可用空间；处理全部地区需要更多容量。

在 `osm-builder` 目录编译：

```sh
cargo build --release
```

部署 Worker 后，在本目录创建或填写 `.env`。从 Cloudflare 获取该存储桶的 [R2 Object Read & Write 凭据](https://developers.cloudflare.com/r2/api/tokens/)，填写以下配置：

```dotenv
R2_ACCOUNT_ID="<Cloudflare 账户 ID>"
R2_BUCKET="aura-osm-data"
AWS_ACCESS_KEY_ID="<R2 Access Key ID>"
AWS_SECRET_ACCESS_KEY="<R2 Secret Access Key>"
OSM_WORKER_URL="https://aura-osm-data.<你的子域>.workers.dev"
OSM_PUBLISH_TOKEN="<与 Worker 的 PUBLISH_TOKEN 相同的密钥>"
```

`R2_BUCKET` 与 Worker 绑定的存储桶一致，发布密钥至少 32 个字符，Worker 地址不带 API 路径。数据默认保存在 `.build/data`，使用外置盘时在 `.env` 添加 `OSM_DATA_DIR="/Volumes/你的磁盘/osm-data"`。保留本地数据以支持后续更新，密钥不要提交到 Git。

## 使用

在本目录执行，以澳大利亚新南威尔士州 `au-nsw` 为例：

| 操作 | 命令 |
| --- | --- |
| 查看可用地区 | `.build/tools/rust/release/osm list` |
| 首次下载、处理并发布 | `.build/tools/rust/release/osm bootstrap au-nsw` |
| 更新数据并发布 | `.build/tools/rust/release/osm update au-nsw` |
| 领取并处理云端任务 | `.build/tools/rust/release/osm work` |
| 查看批次与设备状态 | `.build/tools/rust/release/osm jobs` |
| 查看云端已发布地区 | `.build/tools/rust/release/osm published-state` |

发布完成后，通过 [Worker 查询接口](../osm-worker/README.md) 使用数据。`bootstrap` 和 `update` 中的地区名可替换为 `all`，表示 `config/regions.json` 配置的全部地区。

如需月更，完成 `bootstrap all` 后执行 `.build/tools/rust/release/osm schedule`，在这台 Mac 上安装每月 1 日 04:00 提交全部地区更新批次的任务，处理设备需运行 `work`。

## 多设备处理

各设备拉取同一份仓库，保留完整的 `config/regions.json`，填写各自的 `.env`，连接同一个 Worker 和 R2。设备身份保存在数据目录的 `.device-id`；不要把此文件复制到另一台设备。Git 更新不改变任务归属。

每台设备运行：

```sh
.build/tools/rust/release/osm work
```

在任意设备另开终端提交首批任务：

```sh
.build/tools/rust/release/osm bootstrap all --submit-only
```

后续用 `update all --submit-only` 提交更新。这两个命令将所选地区目录提交到 Worker，每次只允许一个批次处于运行状态。`bootstrap` 跳过云端已发布地区；更新已发布地区用 `update`。不带 `--submit-only` 时，提交进程会参与处理并等待批次结束；它与同目录的 `work` 不能并行运行。

`work` 等待后续批次；`work --once` 在当前批次没有待处理或运行中任务时退出。有失败任务时，查看 `jobs` 与设备错误日志，再提交失败地区的批次。若提交结果因断网无法确认，先查看 `jobs`，已有批次用 `work` 继续。

每台设备同一时间持有一个地区的租约，每 60 秒续期，300 秒未续期后可由其他设备接管。已有索引的设备优先领取；接管设备没有索引时下载完整源建立索引，已有索引时应用日差量。序列断档仍会报错并保留索引。

Worker 在同一个 Durable Object 事务中校验租约、合并发布清单并完成任务。失效租约不能发布；不同地区不会因共享版本号而互相覆盖。上传的不可变数据块可复用。

保留数据目录以支持后续更新。`--cleanup` 会在发布确认后删除本机地区索引和对应下载，仅用于不需要保留索引的批量处理。`schedule` 只在一台 Mac 安装，用于每月提交更新批次；其他设备运行 `work`。Linux 可用系统服务托管 `work`，由一台设备的定时任务提交 `update all`。

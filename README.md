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

在任意设备提交首批任务；提交和启动处理设备没有先后要求：

```sh
.build/tools/rust/release/osm bootstrap all --submit-only
```

后续用 `update all --submit-only` 提交更新。这两个命令将所选地区目录提交到 Worker，每次只允许一个批次处于运行状态。`bootstrap` 跳过云端已发布地区；更新已发布地区用 `update`。不带 `--submit-only` 时，提交进程会参与处理并等待批次结束；它与同目录的 `work` 不能并行运行。

`work` 等待后续批次；`work --once` 在当前批次没有待处理或运行中任务时退出。有失败任务时，查看 `jobs` 与设备错误日志，再提交失败地区的批次。若提交结果因断网无法确认，先查看 `jobs`，已有批次用 `work` 继续。

每台设备最多持有两个地区的租约，每个租约每 60 秒续期，300 秒未续期后可由其他设备接管。已有索引的设备优先领取；接管设备没有索引时下载完整源建立索引，已有索引时应用日差量。序列断档会报错并保留索引。

计算 A 时预下载 B，上传 A 时计算 B；A 发布确认并完成清理后，空出的任务位置可领取并预下载 C。每台设备同一时间只计算一个地区、上传一个地区；B 计算结束后若 A 仍在上传，B 等待上传位置。磁盘预算不足时暂停下载或等待前一区域清理；没有可释放空间的任务时退出并保留已有数据。任务失败后停止领取，退回未处理任务，保留失败地区的索引和产物。

Worker 在同一个 Durable Object 事务中校验租约、合并发布清单并完成任务。失效租约不能发布；不同地区不会因共享版本号而互相覆盖。上传的不可变数据块可复用。

保留数据目录以支持后续更新。`--cleanup` 会在发布确认后删除本机地区索引和对应下载，仅用于不需要保留索引的批量处理。`schedule` 只在一台 Mac 安装，用于每月提交更新批次；其他设备运行 `work`。Linux 可用系统服务托管 `work`，由一台设备的定时任务提交 `update all`。

从单任务版本升级时，停止各设备的旧 Builder，部署配套 Worker，再更新并启动 Builder。云端已有发布数据、批次和租约保留；旧租约归入任务位置 0。旧版领取和释放请求不符合新协议，不能与新版混跑。

## R2 打包存储

Builder 保留 0.01° 查询网格和每个 POI 的原始数据，将同一 16 × 16 网格组内的小块拼成最大 1 MiB 的不可变包，上传 `packs/<SHA256>.bin` 和地区 manifest。清单记录包目录以及各小块的哈希、包编号、偏移和长度；Worker 按范围读取所需小块。本地 `blocks/` 供增量构建使用，不再逐块上传。

更新只重打包发生变化的网格组；内容未变的包复用原哈希。发布结果的 `uploaded`、`reused` 统计物理对象数，包含 manifest。`--cleanup` 仍在发布确认后删除本地区本地数据，包括小块和包。

构建日志输出逻辑小块数、物理包数和字节数，`release.json` 保存 `blockCount`、`packCount`、`packBytes`。比较账单时使用相同地区和更新频率；初次迁移需要上传新包，后续更新复用未变的包。打包减少对象写入次数，不改变 POI 数据量。

升级时先部署支持 schema 1 和 2 的 Worker，再更新各设备的 Builder。已发布的旧地区继续可查；`bootstrap all` 会跳过它们，用 `update all --submit-only` 逐地区切换到打包格式。有本地索引时，旧产物转换不需要重新解析 PBF；没有本地索引时仍需下载完整源重建。云端旧对象保留，此次升级不执行 R2 列举或删除。

本地验证使用 `cargo test`。跨仓库联调：在 Worker 目录运行 `npm run test:serve`，将输出的两个回环地址设为 `OSM_TEST_WORKER_URL`、`OSM_TEST_R2_ENDPOINT`，再运行 `cargo test --test worker_integration -- --ignored`；完成后按 Enter 停止服务。此联调使用隔离存储，不产生 Cloudflare 用量。

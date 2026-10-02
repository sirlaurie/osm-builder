# OSM Builder

在本地设备下载 OpenStreetMap 地点数据，处理后上传到 Cloudflare R2，供配套的 [OSM Worker](../osm-worker/README.md) 提供附近地点查询。

**部署顺序：OSM Worker → OSM Builder → 应用查询。** Builder 负责准备和更新数据，Worker 负责对外提供接口。

## 部署

需要 macOS 或 Linux、Rust 1.95+ 和 C/C++ 编译工具；macOS 使用 Xcode Command Line Tools。默认启动新地区要求数据盘和项目所在盘各有 10 GiB 可用空间，后续检查保留 5 GiB；下载预算还计入待下载字节。PBF 是压缩数据，SQLite 索引、临时文件和打包产物会占用额外空间，这些门槛不代表每个地区的容量上限。可用 `OSM_START_FREE_GIB`、`OSM_MIN_FREE_GIB` 覆盖默认值；已有环境变量或 `.env` 中的设置优先于仓库默认配置。

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

`work` 遇到连接超时、传输中断、下载重定向次数耗尽或 HTTP 429/5xx 时，在请求重试耗尽后等待 60 秒再尝试，不退出进程。源端复制状态冲突（包括最新状态落后于本地索引、固定路径序号不符、同序号时间变化及复制链时间倒退）和没有新序列时的目录覆盖范围变化也进入此重试流程，不设总次数上限；冲突状态不用于推进索引或发布。此行为覆盖领取、下载、上传与发布确认；活动任务停止后保留索引和构建产物，退回待处理，再领取租约。未确认发布的数据不执行 `--cleanup`。续租的网络失败在原租约期限内重试，失败不延长期限；过期后必须重新领取。认证失败、非 HTTPS 下载重定向、业务数据损坏和磁盘错误仍会终止任务。`jobs`、`published-state` 和批次提交等单次操作保持有限重试。

每台设备最多持有两个地区的租约，每个租约每 60 秒续期，300 秒未续期后可由其他设备接管。已有索引的设备优先领取；接管设备没有索引时下载完整源建立索引，已有索引时应用日差量。序列断档会报错并保留索引。

计算 A 时预下载 B，上传 A 时计算 B；A 发布确认并完成清理后，空出的任务位置可领取并预下载 C。每台设备同一时间只计算一个地区、上传一个地区；B 计算结束后若 A 仍在上传，B 等待上传位置。磁盘预算不足时暂停下载或等待前一区域清理；没有可释放空间的任务时退出并保留已有数据。任务失败后停止领取，退回未处理任务，保留失败地区的索引和产物。

Worker 在同一个 Durable Object 事务中校验租约、合并发布清单并完成任务。失效租约不能发布；不同地区不会因共享版本号而互相覆盖。上传的不可变数据块可复用。

保留数据目录以支持后续更新。`--cleanup` 会在发布确认后删除本机地区索引和对应下载，仅用于不需要保留索引的批量处理。`schedule` 只在一台 Mac 安装，用于每月提交更新批次；其他设备运行 `work`。Linux 可用系统服务托管 `work`，由一台设备的定时任务提交 `update all`。

从单任务版本升级时，停止各设备的旧 Builder，部署配套 Worker，再更新并启动 Builder。云端已有发布数据、批次和租约保留；旧租约归入任务位置 0。旧版领取和释放请求不符合新协议，不能与新版混跑。

## 数据源切换

Geofabrik 为默认来源。`config/fallback-sources.json` 保存通过准入的 OSM France 地区映射；当前 `regions` 为空，真实地区的自动切源尚未启用。已检查的候选未通过覆盖或引用检查，证据见 [准入报告](docs/source-qualification.md)。不能通过填入相似地名启用备用源。

每项映射包含 `region`、`geofabrikExtract`、`osmFranceExtract` 与固定的 `coverage`。启用前须验证备用 PBF 包含已有覆盖范围、way 节点与业务 relation 完整，并完成 POI 分类、分布和边界检查点的影子比较。范围以线上 manifest 为准；通过这些检查后才能保存映射。配置中的 coverage 作为边界版本，索引记录其 SHA-256。

有准入映射时，来源请求重试耗尽会从另一来源建立全量候选。来源身份绑定到本地索引和 manifest，增量只沿原来源的 sequence 前进。快照下载前后检查 MD5，验证完整文件及 PBF 自身的 replication header；快照换代会触发有限次数的整套重取。HTTP 429 使用有效的 `Retry-After`。重定向只能留在同一批准的 HTTPS 主机。

候选保存在数据目录的 `.candidates/<region>`，原索引与线上版本保留。Builder 在上传前读取线上 manifest；跨源要求数据时间更新、coverage 相同、POI 数不减少、缺失业务 relation 数不增加。OSM France 产物还必须非空且没有被排除的业务 relation，手工发布与未发布地区也受此限制。Worker 在发布事务中复核这些跨源门槛、Current 哈希和租约。未通过验收的数据不会替换 Current，也不会清理旧云端对象。

发布确认后，候选通过本地 journal 晋升为活动索引；中断恢复须核对远端 manifest。切源失败的索引各自保留，发布成功后才清理被替换的候选。为保持下载空间预算，有准入映射的地区在计算完成后开放另一任务的预下载。两个来源均传输失败时沿用 60 秒任务重试；派发、上传、磁盘和数据库错误不触发切源。

恢复时若远端已被另一节点推进至不同 manifest，程序保留 journal 和本地目录并停止该地区，需核对版本后处理本地状态；旧 journal 不会覆盖新的线上版本。

升级顺序为：部署配套 Worker、升级所有 Builder、完成地区准入、启用映射。旧 manifest 缺少来源字段时按 Geofabrik 解释；旧节点不能覆盖已切至 OSM France 的地区。查询 API、region ID、POI ID、Current schema 及旧 manifest 的读取契约不变。

## R2 打包存储

Builder 保留 0.01° 查询网格和每个 POI 的原始数据，将同一 16 × 16 网格组内的小块拼成最大 1 MiB 的不可变包，上传 `packs/<SHA256>.bin` 和地区 manifest。清单记录包目录以及各小块的哈希、包编号、偏移和长度；Worker 按范围读取所需小块。本地 `blocks/` 供增量构建使用，不再逐块上传。

更新只重打包发生变化的网格组；内容未变的包复用原哈希。发布结果的 `uploaded`、`reused` 统计物理对象数，包含 manifest。`--cleanup` 仍在发布确认后删除本地区本地数据，包括小块和包。

构建日志输出逻辑小块数、物理包数和字节数，`release.json` 保存 `blockCount`、`packCount`、`packBytes`。比较账单时使用相同地区和更新频率；初次迁移需要上传新包，后续更新复用未变的包。打包减少对象写入次数，不改变 POI 数据量。

升级时先部署支持 schema 1 和 2 的 Worker，再更新各设备的 Builder。已发布的旧地区继续可查；`bootstrap all` 会跳过它们，用 `update all --submit-only` 逐地区切换到打包格式。有本地索引时，旧产物转换不需要重新解析 PBF；没有本地索引时仍需下载完整源重建。云端旧对象保留，此次升级不执行 R2 列举或删除。

本地验证使用 `cargo test`。跨仓库联调：在 Worker 目录运行 `npm run test:serve`，将输出的两个回环地址设为 `OSM_TEST_WORKER_URL`、`OSM_TEST_R2_ENDPOINT`，再运行 `cargo test --test worker_integration -- --ignored`；完成后按 Enter 停止服务。此联调使用隔离存储，不产生 Cloudflare 用量。

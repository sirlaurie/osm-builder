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
| 取消未完成的批次 | `.build/tools/rust/release/osm cancel` |
| 从云端发布中移除地区 | `.build/tools/rust/release/osm retire <地区> [<地区> …]` |
| 统计或删除 R2 中不再引用的对象 | `.build/tools/rust/release/osm gc [--apply]` |

发布完成后，通过 [Worker 查询接口](../osm-worker/README.md) 使用数据。`bootstrap` 和 `update` 可列出多个地区，或用 `all` 表示 `config/regions.json` 配置的全部地区。

如需月更，完成 `bootstrap all` 后执行 `.build/tools/rust/release/osm schedule`，在这台 Mac 上安装每月 1 日 04:00 提交全部地区更新批次的任务，处理设备需运行 `work`。

在 Linux 上用 cron 或 systemd timer 提交月更时，先更新代码和配置，再以 `--replace` 提交：

```sh
cd /path/to/osm-builder && git pull --ff-only && cargo build --release \
  && .build/tools/rust/release/osm update all --submit-only --replace
```

`--replace` 先取消上个月未完成的批次，再提交新批次；被取消的地区包含在新批次里。`update all` 提交后会核对云端发布清单，若有已发布地区不在本机 `config/regions.json` 中，会列出这些地区并以非零状态退出，批次仍已提交。此时更新这份仓库，或用 `osm retire` 移除不再提供的地区。让定时任务在非零退出时通知到人，例如 cron 的 `MAILTO` 或 systemd 的 `OnFailure=`。

从配置删除地区不会下线已发布的数据，需执行 `osm retire <地区>`。之后运行 `osm gc --apply` 删除不再被任何已发布 manifest 引用的 R2 对象；不带 `--apply` 时只统计。`gc` 在有未完成批次时拒绝删除，每删除 1000 个对象前重新确认没有新批次，并保留 24 小时内上传的对象。

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

`work` 等待后续批次；`work --once` 在当前批次没有待处理或运行中任务时退出。有失败任务时，查看 `jobs` 中各任务的 `lastError` 与设备错误日志，再提交失败地区的批次。若提交结果因断网无法确认，先查看 `jobs`，已有批次用 `work` 继续。

网络失败使用指数退避重试，不设总次数上限，每次等待最多 24 小时。此规则覆盖连接超时、传输中断、响应截断、下载重定向次数耗尽，以及派发、R2 和发布接口的 HTTP 408/425/429/5xx。公共源文件的非 2xx 响应全部进入重试，包括 PBF、MD5、目录、复制状态和 OSC 的 404；错误输出保留请求地址与状态码。源端复制状态冲突和没有新序列时的目录覆盖范围变化也会重试，冲突状态不用于推进索引或发布。源站返回的有效 `Retry-After` 与本地退避取较长者，同样封顶 24 小时。

某个地区遇到网络类失败时，`work` 只把该地区以 `retry` 退回云端，附带失败原因和源站的 `Retry-After`，另一个任务位置继续工作。云端按地区退避，期间所有设备都不领取该地区，`osm jobs` 显示 `retries`、`notBefore` 和 `lastError`。同一设备连续出现网络类失败时，空出的任务位置在领取下一个地区前等待 60 秒、120 秒、240 秒递增至 24 小时，任一地区发布成功后重置。领取请求本身失败时整台设备等待，再次联系上协调器后重置。失败地区的索引和构建产物保留。完整下载会复用；远端 MD5 未变且本地校验通过时跳过 PBF 下载，源文件换代在原受管目录更新，避免失败重试积累完整副本。未确认发布的数据不执行 `--cleanup`。有租约的请求保留有限内层尝试，以便返回工作循环取消在途任务和重新领取；续租等待受原租约期限约束，网络失败不延长期限。

`build/init` 的下载请求、批次提交、`jobs`、`published-state` 和独立 `publish` 均保留进程并重试网络失败。下载和批次提交从配置的请求退避时间起步，其余命令从 60 秒起步；全部封顶 24 小时。批次提交复用同一请求 ID，手工发布重领有效租约，已完成任务须核对原租约、manifest、发布 ACK 与 Current。鉴权拒绝、非 HTTPS 或越权下载重定向、业务数据损坏和磁盘错误不属于网络重试。

每台设备最多持有两个地区的租约，每个租约每 60 秒续期，300 秒未续期后可由其他设备接管。已有索引的设备优先领取；接管设备没有索引时下载完整源建立索引，已有索引时应用日差量。所需序列文件不可用时等待重试，保留索引。

计算 A 时预下载 B，上传 A 时计算 B；A 发布确认并完成清理后，空出的任务位置可领取并预下载 C。每台设备同一时间只计算一个地区、上传一个地区；B 计算结束后若 A 仍在上传，B 等待上传位置。磁盘预算不足时暂停下载或等待前一区域清理；没有可释放空间的任务时退出并保留已有数据。后台预下载因磁盘暂停时，若另一个任务位置需要前台下载，预下载让出下载队列，该地区轮到计算时再前台下载。网络类以外的失败（磁盘、数据损坏、鉴权等）以 `failed` 释放该地区，设备停止领取，退回其他未处理任务，保留失败地区的索引和产物。

`work` 启动时删除中断留下的 `init-*`、`replication-*`、`catalog-*` 临时目录，以及 `.downloads` 中未下载完整的目录；完整下载保留以便复用。

Worker 在同一个 Durable Object 事务中校验租约、合并发布清单并完成任务。失效租约不能发布；不同地区不会因共享版本号而互相覆盖。上传的不可变数据块可复用。

保留数据目录以支持后续更新。`--cleanup` 会在发布确认后删除本机地区索引和对应下载，仅用于不需要保留索引的批量处理。`schedule` 只在一台 Mac 安装，用于每月提交更新批次；其他设备运行 `work`。Linux 可用系统服务托管 `work`，由一台设备的定时任务提交 `update all`。

从单任务版本升级时，停止各设备的旧 Builder，部署配套 Worker，再更新并启动 Builder。云端已有发布数据、批次和租约保留；旧租约归入任务位置 0。旧版领取和释放请求不符合新协议，不能与新版混跑。

## 数据源切换

Geofabrik 为默认来源。`config/fallback-sources.json` 保存通过准入的 OSM France 地区映射；当前 `regions` 为空，真实地区的自动切源尚未启用。已检查的候选未通过覆盖或引用检查，证据见 [准入报告](docs/source-qualification.md)。不能通过填入相似地名启用备用源。

每项映射包含 `region`、`geofabrikExtract`、`osmFranceExtract` 与固定的 `coverage`。启用前须验证备用 PBF 包含已有覆盖范围、way 节点与业务 relation 完整，并完成 POI 分类、分布和边界检查点的影子比较。范围以线上 manifest 为准；通过这些检查后才能保存映射。配置中的 coverage 作为边界版本，索引记录其 SHA-256。

有准入映射时，来源请求的内层重试耗尽会从另一来源建立全量候选。来源身份绑定到本地索引和 manifest，增量只沿原来源的 sequence 前进。快照下载前后检查 MD5，验证完整文件及 PBF 自身的 replication header；快照换代会重新获取，持续变化时进入退避。重定向只能留在同一批准的 HTTPS 主机。

候选保存在数据目录的 `.candidates/<region>`，原索引与线上版本保留。Builder 在上传前读取线上 manifest；跨源要求数据时间更新、coverage 相同、POI 数不减少、缺失业务 relation 数不增加。OSM France 产物还必须非空且没有被排除的业务 relation，手工发布与未发布地区也受此限制。Worker 在发布事务中复核这些跨源门槛、Current 哈希和租约。未通过验收的数据不会替换 Current，也不会清理旧云端对象。

发布确认后，候选通过本地 journal 晋升为活动索引；中断恢复须核对远端 manifest。切源失败的索引各自保留，发布成功后才清理被替换的候选。为保持下载空间预算，有准入映射的地区在计算完成后开放另一任务的预下载。两个来源均传输失败时进入工作循环的指数退避；派发、上传、磁盘和数据库错误不触发切源。

恢复时若远端已被另一节点推进至不同 manifest，程序保留 journal 和本地目录并停止该地区，需核对版本后处理本地状态；旧 journal 不会覆盖新的线上版本。

升级顺序为：部署配套 Worker、升级所有 Builder、完成地区准入、启用映射。旧 manifest 缺少来源字段时按 Geofabrik 解释；旧节点不能覆盖已切至 OSM France 的地区。查询 API、region ID、POI ID、Current schema 及旧 manifest 的读取契约不变。

## R2 打包存储

Builder 保留 0.01° 查询网格和每个 POI 的原始数据，将同一 16 × 16 网格组内的小块拼成最大 1 MiB 的不可变包，上传 `packs/<SHA256>.bin`、每个网格组的索引 `indexes/<SHA256>.json` 和地区 manifest（schema 3）。索引记录本组各小块的哈希、包编号、偏移和长度；manifest 只记录网格组到索引的对照和全部包的列表，大小不再随地区面积增长，大地区无需拆分。Worker 只读取查询需要的索引，再按范围读取所需小块。本地 `blocks/` 供增量构建使用，不再逐块上传。

更新只重打包发生变化的网格组；内容未变的包和索引复用原哈希。发布结果的 `uploaded`、`reused` 统计物理对象数，包含索引和 manifest。发布 schema 3 前须先部署支持它的 Worker。`--cleanup` 仍在发布确认后删除本地区本地数据，包括小块和包。

构建日志输出逻辑小块数、物理包数和字节数，`release.json` 保存 `blockCount`、`packCount`、`packBytes`。比较账单时使用相同地区和更新频率；初次迁移需要上传新包，后续更新复用未变的包。打包减少对象写入次数，不改变 POI 数据量。

升级时先部署支持 schema 1 和 2 的 Worker，再更新各设备的 Builder。已发布的旧地区继续可查；`bootstrap all` 会跳过它们，用 `update all --submit-only` 逐地区切换到打包格式。有本地索引时，旧产物转换不需要重新解析 PBF；没有本地索引时仍需下载完整源重建。云端旧对象保留，此次升级不执行 R2 列举或删除。

本地验证使用 `cargo test`；推送后 GitHub Actions 运行格式、Clippy 和测试检查。POI 判定与网格规则由 `tests/fixtures/poi-contract.json` 约定，Worker 仓库保存同一文件；同级目录存在 `osm-worker` 时，测试会核对两份文件一致。跨仓库联调：在 Worker 目录运行 `npm run test:serve`，将输出的两个回环地址设为 `OSM_TEST_WORKER_URL`、`OSM_TEST_R2_ENDPOINT`，再运行 `cargo test --test worker_integration -- --ignored`；完成后按 Enter 停止服务。此联调使用隔离存储，不产生 Cloudflare 用量。

# OSM 数据源切换研究

研究日期：2026-10-01。范围：公开目录、HTTP 元数据、PBF 头部、提供方源码与工具官方文档；未下载全量 PBF，未访问生产存储，未修改生产代码。

## 来源调查结论

推荐保留 Geofabrik 为主源，为通过准入验证的地区接入 OSM France。源请求重试耗尽后，程序尝试备用源的独立全量候选；候选通过验收后才发布，失败则保留线上版本。

OSM France 是区域 PBF 备用源候选，但不是 Geofabrik 的镜像。两者的区域切分、对象完整性、增量序列与发布流程存在差异。当前证据支持“经审核的区域从备用全量快照重建”，不支持“239 个区域替换域名”或“跨源续接旧增量”。准入验证是启用自动切换的前置条件。

| 方案 | 已验证 | 限制与选择 |
| --- | --- | --- |
| OSM France | 区域 PBF、MD5、独立 state、分钟增量、polygon；巴西北部下载与增量端点可用 | 首选候选；须逐区验证覆盖范围与对象完整性 |
| BBBike | 官方首页提供城市、自选范围及国家/区域导出入口；自选范围上限含 512 MB | 未验证与本项目区域匹配、可用增量链及完整对象契约；不纳入首版切换 |
| OSM 官方 Planet | 官方 latest 链接经两次重定向到 AWS，返回 200 | 覆盖全球，但需下载、存储、切分与更新全球数据；不适合当前节点的空间约束 |

依据：[OSM France 目录](https://download.openstreetmap.fr/)、[BBBike 官方入口](https://download.bbbike.org/osm/)、[Planet 官方入口](https://planet.openstreetmap.org/)。这些检查证明访问时的状态，不代表可用性承诺。

## OSM France 的协议与实测

2026-10-01 09:30–09:33 UTC 的只读检查得到：

| 检查 | 结果 |
| --- | --- |
| `extracts/south-america/brazil/north.osm.pbf` HEAD | HTTP 200；173,171,347 bytes；Last-Modified 为 2026-10-01 01:06:35 GMT；支持 Range |
| 同一 PBF 的前 4,096 bytes | `osmium fileinfo` 解析成功；generator 为 `pyosmium-up-to-date/3.6.0` |
| PBF replication header | sequence 为 `7309649`；timestamp 为 `2026-10-01T01:04:09Z`；base URL 为 `http://download.openstreetmap.fr/replication/./south-america/brazil/north/minute` |
| `north.state.txt` | sequence 和 timestamp 与该 PBF 头部相同 |
| `north.osm.pbf.md5` | 返回摘要及文件名；本次没有下载文件，未验证全文件摘要 |
| `replication/south-america/brazil/north/minute/state.txt` | sequence 为 `7310142`；timestamp 为 `2026-10-01T09:28:36Z` |
| 对应后继 `007/309/650.osc.gz` HEAD | HTTP 200，83 bytes |

来源：[巴西快照目录](https://download.openstreetmap.fr/extracts/south-america/brazil/)、[north.state.txt](https://download.openstreetmap.fr/extracts/south-america/brazil/north.state.txt)、[MD5](https://download.openstreetmap.fr/extracts/south-america/brazil/north.osm.pbf.md5)、[分钟增量 state](https://download.openstreetmap.fr/replication/south-america/brazil/north/minute/state.txt)。这些 URL 的内容会变化，表内数值是本次观测。

PBF 快照时间与最新增量时间不同。应以已下载 PBF 自身的 replication timestamp/sequence 建立起点，不能把请求到的最新 state 填进该 PBF 的索引。header 中的 HTTP URL 含 `./`；实现应校验提供方和路径，并按审核的 HTTPS 端点访问，不应接受任意头部 URL。Header 存在不证明所有对象或引用完整。

## 区域映射与覆盖边界

本项目有 239 个区域。本次没有完成 239 项映射和几何验证，不提供“全部可切换”的结论。

| 本项目 extract | OSM France 候选路径 | 本次证据 |
| --- | --- | --- |
| `south-america/brazil/norte` | `south-america/brazil/north` | PBF、polygon、state、增量端点存在；几何等价未验证 |
| `south-america/brazil/centro-oeste` | `south-america/brazil/central-west` | 官方目录存在；未验证完整下载及几何等价 |
| `south-america/brazil/nordeste` | `south-america/brazil/northeast` | 同上 |
| `south-america/brazil/sudeste` | `south-america/brazil/southeast` | 同上 |
| `south-america/brazil/sul` | `south-america/brazil/south` | 同上 |
| `australia-oceania/australia/new-south-wales` | `oceania/australia/new_south_wales` | 目录存在；洲名和分隔符不同 |
| `asia/china/hong-kong` | `asia/china/hong_kong` | 目录存在；同目录包含 jiangsu、macau |
| `europe/latvia`、`europe/liechtenstein` | 未确定 | Europe 目录没有同名项；两个预期 PBF URL 返回 404；上述官方仓库完整树（`truncated=false`）未找到含 Latvia / Liechtenstein 的路径 |

加拿大 13 省区和法国旧大区可在对应目录找到名称候选；这不证明边界相同。海外地区、争议边界、跨境完整对象及重叠区须单独核验。来源：[澳大利亚](https://download.openstreetmap.fr/extracts/oceania/australia/)、[中国](https://download.openstreetmap.fr/extracts/asia/china/)、[加拿大](https://download.openstreetmap.fr/extracts/north-america/canada/)、[法国](https://download.openstreetmap.fr/extracts/europe/france/)、[欧洲](https://download.openstreetmap.fr/extracts/europe/)。

OSM France 发布 [polygon 目录](https://download.openstreetmap.fr/polygons/)；[官方构建仓库](https://github.com/osm-fr/osm-extract-replication/tree/4a56fef35c3bcc724b43c7b9ad8d3145a27002f9/osc_modif/polygons)保存切分文件。本次读取的 [north.poly](https://download.openstreetmap.fr/polygons/south-america/brazil/north.poly) 与 [Geofabrik norte.poly](https://download.geofabrik.de/south-america/brazil/norte.poly) 坐标列表不同，未完成几何包含关系与对象集合比较。不能用文件名或包围盒代替覆盖验证，也不能给父区域 PBF 附上原 coverage 后宣称等价。

准入须保存明确的候选路径和边界版本，并验证备用输入覆盖既有服务范围、跨边界 way 与项目消费的 relation 不缺引用、POI 分类/几何产物符合现有契约。验证不通过或无映射时，该区域等待原源恢复。完整性检查应针对 Builder 消费的数据类型，不能把允许缺失的非业务 relation 一概当作损坏。

## 完整对象与发布一致性

审阅的官方源码版本为 `4a56fef35c3bcc724b43c7b9ad8d3145a27002f9`。该版本不证明服务端正在运行相同版本。

[init_pbf.py](https://github.com/osm-fr/osm-extract-replication/blob/4a56fef35c3bcc724b43c7b9ad8d3145a27002f9/osc_modif/init_pbf.py) 的 Osmosis 路径指定 `completeWays=yes`、`completeRelations=no`；Osmium 路径没有指定 smart 策略。Osmium 默认 `complete_ways` 保留完整 way，但不保证 relation 成员完整；`smart` 策略才补全指定关系类型。[Osmium 官方说明](https://docs.osmcode.org/osmium/latest/osmium-extract.html)。因此，OSM France 的备用输入不能在缺少验证时被认定为与现有输入等价，特别是跨边界 multipolygon。

[update_pbf.sh](https://github.com/osm-fr/osm-extract-replication/blob/4a56fef35c3bcc724b43c7b9ad8d3145a27002f9/osc_modif/update_pbf.sh) 生成临时 PBF 和 MD5，然后移除旧链接、建立新链接，最后写 state。PBF、MD5、state 不是一个事务发布；读者可能遇到短暂 404 或不同代文件。建议下载前后核对版本元数据/摘要，按 PBF 头部记录起点；跨发布窗口则重试该快照，禁止拼装不同版本。

[update.py](https://github.com/osm-fr/osm-extract-replication/blob/4a56fef35c3bcc724b43c7b9ad8d3145a27002f9/osc_modif/update.py) 先写临时 diff、rename 为最终 diff，再更新 state；[osc_modif.py](https://github.com/osm-fr/osm-extract-replication/blob/4a56fef35c3bcc724b43c7b9ad8d3145a27002f9/osc_modif/osc_modif.py) 使用 polygon 及 buffer 过滤。这里存在提供方特定的区域语义。[clean_diffs.sh](https://github.com/osm-fr/osm-extract-replication/blob/4a56fef35c3bcc724b43c7b9ad8d3145a27002f9/osc_modif/clean_diffs.sh) 会清理旧序列；不能假定增量永久保留，实际保留期未验证。缺失所需序列时应全量重建。

## 切换的来源约束

- 常用 `--cleanup` 模式：从经过准入的备用 PBF 建立新的独立快照；完成下载、校验、重建及发布检查后才有资格更新线上版本。
- 保留增量索引模式：已有索引必须绑定 provider、extract、边界版本与 replication 起点。Geofabrik 索引不能接 OSM France diff；切换需要新索引和全量重建，旧版本保留供线上读取。
- 备用源恢复失败、过旧、校验失败、覆盖不足时，保持线上版本。检测源故障与允许发布是两项条件，网络恢复不等于数据准入通过。
- 若要求覆盖全部 239 区且保持切分语义，须评估自行保存的 Geofabrik 同版快照镜像，或从独立的大范围数据按固定边界重建。前者只能保证已有快照可用，不能保证新鲜度；后者增加存储、切分和完整对象验证成本。当前证据不足以把这两项纳入节点的自动切换路径。

## 现有程序的接入点

以下结论来自本次工作区代码审阅，不代表完成部署验证。

| 位置 | 当前行为 | 实施要求 |
| --- | --- | --- |
| `src/source.rs` 的 `Region`、`prepare`、`stamp` | 拼接 Geofabrik URL；从其 index-v1.json 取得 coverage；校验 PBF 和增量来源 | 为两家提供方编写显式解析逻辑与地区映射，保留 URL 白名单、路径、摘要和头部检查 |
| `src/pipeline.rs` 的 `CloudWork::prepare` | 每个地区先下载 Geofabrik catalog | 将经审核的地区边界和备用映射版本化保存；备用流程不能依赖故障站点的目录 |
| `src/pipeline.rs` 的 `status`、`Operations::update` | 校验索引来源，并按该源 sequence 前进 | 同源增量保留；跨源创建独立全量候选，不放宽旧索引的来源校验 |
| `src/build.rs`、Worker `worker/index.ts` | coverage 是发布及查询覆盖元数据；没有按它逐条裁剪 POI | 父区域 PBF 加旧 coverage 不构成替换方案；需要父区域时，另行实现范围选择及引用完整性验证 |
| `src/publish.rs`、Worker `worker/coordinator.ts` | 不可变对象上传、租约校验、时间防回退、事务发布 | 复用发布链路，补充来源身份及跨源发布门槛 |

项目来源：[source.rs](../src/source.rs)、[pipeline.rs](../src/pipeline.rs)、[incremental.rs](../src/incremental.rs)、[build.rs](../src/build.rs)、[publish.rs](../src/publish.rs)、[Worker 查询](../../osm-worker/worker/index.ts)、[Worker 发布协调](../../osm-worker/worker/coordinator.ts)。

首版使用两个明确的提供方和逐地区映射，不引入供应商插件框架、全局健康服务或备用源打分系统。云端任务仍按既有 region ID 分配；提供方是任务的输入选择，不创建另一套地区任务。记录来源、extract、replication URL、sequence、snapshot SHA-256 和边界版本，作为候选和本地索引的来源身份。

## 自动切换流程

1. 领取现有地区租约，读取线上版本的 manifest 标识和源数据时间。若有本地索引，先检查它绑定的提供方。
2. 访问该地区当前来源，复用已有有界请求重试。只有源请求失败触发备用源选择；派发、R2、磁盘与本地数据库错误不触发切源。
3. 主源重试耗尽，检查该地区是否有通过准入的备用映射。存在则尝试备用源；不存在则沿用等待与重试，线上版本不变。
4. 在候选目录下载备用 PBF 与对应校验文件，验证下载前后快照身份及 PBF header；不能把另一源的字节追加到原 `.part`，不能使用另一源的 MD5 或 sequence。
5. 构建候选索引和产物，执行下节验收。一次候选固定一个提供方；源恢复不导致处理中途切回。保留索引的模式在后续任务中继续使用索引所属源；切回也按全量候选处理。
6. 验收通过后上传不可变对象和 manifest，再执行原发布事务。发布成功且核对远端 Current 后，才更换本地活动索引或执行 `--cleanup`。本地候选晋升需要恢复记录，处理“远端已发布、本地尚未晋升”时进程退出的情况。
7. 两个源的传输均失败时，复用现有 work 的 60 秒等待与租约恢复流程；不发布、不清空线上地区、不把失败构建当作空数据版本。

| 观察到的失败 | 处理 |
| --- | --- |
| 源连接失败、超时、响应截断、HTTP 429/5xx、HTTPS 重定向次数耗尽 | 有界重试后尝试已准入备用源；429 尊重有效的 Retry-After |
| 下载窗口内 PBF、MD5 或 state 换代 | 重新取得一致快照，不能把发布竞态当成已验证的数据 |
| 备用映射 URL 的 404 | 排查路径及该源换代窗口；持续 404 的映射不具备准入条件，不能无限轮换域名 |
| 非 HTTPS 重定向、未知主机、认证错误、稳定版本仍校验失败、解析或边界验证失败 | 拒绝该候选；不得降低校验强度以促成切换 |
| R2/Worker 网络错误或磁盘空间不足 | 保留现有恢复或空间检查语义；换数据源不解决这些错误 |

本方案把“不稳定”定义为源请求在既有重试上限内失败，首版不加入尚无测量依据的低速阈值。现有单次下载超时较长；要求在固定时间内完成切源时，需要依据节点带宽和最大地区体积确定超时预算，不能承诺瞬时切换。

## 线上版本保护与候选验收

“不影响已有版本”在此指：旧 pack、manifest 不修改、不删除；新候选构建失败不改变 Current；查询在切换前读取原版本，切换后读取通过验收的新版本。数据更新允许产生 POI 变化，不能承诺不同日期、不同提供方的输出逐字节相同。

现有保障可复用：`publish.rs` 先上传 pack，再上传 manifest，最后调用发布 API；写入使用 `If-None-Match: *`。Coordinator 在同一事务内检查租约、拒绝源时间倒退、更新该地区 Current 并标记任务完成，保留其他地区。查询缓存包含 revision，分页版本冲突返回 409；本方案保持这套行为。见 [对象存储](../src/storage.rs)、[发布](../src/publish.rs)、[Coordinator](../../osm-worker/worker/coordinator.ts)、[查询](../../osm-worker/worker/index.ts)。

需要补齐的条件：

- **地区身份与范围不缩水。** 保留 region ID 和已发布边界的身份。准入同时校验多边形覆盖及实际数据语义，不以名称、包围盒或覆盖哈希相同推断输入完整。只有经核验的对应区域进入自动路径；父区域再切分不作为首版的隐式替代。
- **跨源数据时间向前。** 备用快照必须新于线上源时间。源时间更旧时保留线上版本；同时间但不同来源的候选先不自动发布。同源重新打包等合法操作仍可沿用现有相等时间规则。绝不跨提供方比较 sequence 数值。
- **完整性验收。** 校验 PBF 全文件摘要、对象引用、所消费 multipolygon 的缺失情况，以及 POI 数量、分类、空间分布和边界检查点。空产物或异常减少不能依赖“文件格式有效”放行。统计阈值须来自准入样本，不能凭空指定统一百分比；这些统计检查也不能替代覆盖与引用验证。
- **本地隔离。** 原索引不作为另一提供方的增量目标；候选构建不能修改原索引。候选需要额外空间时计入旧索引、输入、候选和临时文件，不得为腾空间先删掉唯一可恢复状态。
- **服务端终检。** Builder 执行数据验收；Coordinator 核对跨源发布的来源、时间与既有边界身份。若校验读取了旧 manifest，事务提交时必须确认 Current 仍指向该 manifest，否则重新校验，避免检查与提交之间出现版本变化。
- **兼容契约。** 保持 POI ID、查询 API、对象路径、manifest schema 1/2 的读取能力和 Current schema 1。新增来源元数据使用可选字段，历史 manifest 缺失时按现有 Geofabrik 来源解释；不回写历史对象。跨源保护须先部署至 Worker，再升级处理节点，最后启用地区备用映射。旧节点不得绕过跨源验收门槛。

线上保留旧不可变对象不等于具备运维回滚命令。当前时间防回退会拒绝把较旧 manifest 作为普通更新重新发布；本次自动切源依靠“候选通过前不切 Current”保护线上。若需要上线后的人工回滚，须另行设计经过授权的回滚流程，不能复用自动切源绕过时间检查。

## 实施顺序与验收

1. 为候选地区建立来源映射和固定边界证据，完成小范围影子构建：生成产物、比对结果，但不上传生产或修改 Current。巴西北部可用于验证下载和头部适配，不代表它已通过边界准入。
2. 接入 Builder 的来源身份、快照校验、候选隔离及下载失败切换；保留当前源重试作为无合格备用时的行为。
3. 增加 Worker 的兼容来源元数据及跨源终检。使用隔离服务测试历史 manifest、并发发布、租约过期、时间回退、边界差异、上传失败与发布响应丢失。
4. 只对通过验收的地区启用自动切换，扩充准入清单。未验证地区保持已有来源和线上版本；覆盖全部 239 区是独立的映射与验证工作，不能由一次成功下载推导。

必须可复现的验收场景：主源重定向循环而备用成功；两源不可用时 Current 不变；候选损坏或过旧时拒绝发布；混用增量被拒绝；边界缺失及业务 relation 不完整被拦截；跨源同时间候选不自动上线；并发节点中旧租约不能发布；发布确认前不清理；发布确认后本地晋升中断能够恢复；原有查询和分页协议保持兼容。研究阶段未执行这些实现测试。

## 未验证项

尚未验证：全部区域映射；polygon 几何包含关系；完整 PBF 的校验和与引用完整性；跨源同日期产物差异；服务端部署版本；更新时的实际竞态窗口；长期可用性与带宽政策；BBBike 的本项目适配性；备用构建对现有发布协议的兼容。实施前应完成对应准入验证，不以 HTTP 200 代替它们。

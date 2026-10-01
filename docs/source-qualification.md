# 备用来源真实地区准入调查

调查时间：2026-10-01 09:44–09:52 UTC；巴西北部补充检查为同日 10:09–10:11 UTC。结论：本轮没有找到可启用的真实备用映射。Monaco、Andorra、Luxembourg、Guernsey-Jersey、巴西北部的 OSM France polygon 均未包含 Geofabrik canonical polygon；两个小 PBF 样本还存在 way 节点引用缺失。不得把这些候选写入启用清单。

本轮读取公开源与仓库代码，未访问生产存储，未修改生产配置。范围不是 239 个地区的全量准入。Geofabrik canonical 取本次公开 `index-v1.json`，未读取线上 manifest；线上边界如与该目录不同，需要另行比较。

## 几何筛选

坐标顺序为 longitude、latitude。下表反例点位于 Geofabrik 多边形严格内部，位于候选多边形外部；单个反例足以否证包含关系。

| 地区 | 候选 polygon | Geofabrik 外环顶点数 | 位于候选外的顶点数 | 严格内部反例点 | 结论 |
| --- | --- | ---: | ---: | --- | --- |
| `eu-monaco` | `europe/monaco.poly` | 19 | 2 | `(7.595670, 43.520670)` | 不包含 |
| `eu-andorra` | `europe/andorra.poly` | 117 | 1 | `(1.733424, 42.522870)` | 不包含 |
| `eu-luxembourg` | `europe/luxembourg.poly` | 276 | 23 | `(5.961754, 50.176310)` | 不包含 |
| `eu-guernsey-jersey` | `europe/guernesey.poly` 与 `europe/jersey.poly` 的并集 | 10 | 10 | `(-2.003340, 49.766400)` | 两份合并仍不包含 |

以上四份 Geofabrik geometry 在本次目录中均为一个 polygon、一个外环、无洞；对应 OSM France 文件各有一个外环。点分类采用 Python `fractions.Fraction` 解析十进制坐标，以射线交点奇偶规则计算，单独检测点在线段上的情况；表内反例不在线段上。这里没有用顶点全部落入来证明包含，也没有用包围盒代替多边形。未来候选通过筛选需要完整多边形差集检查，包含边交叉、洞与多部件；本轮候选在反例检查阶段已被否决。

Monaco 和 Guernsey-Jersey 的反例含海域。海域差异不能自行忽略，当前 canonical coverage 声明这些区域有覆盖。即使修改准入规则排除海域，也需要业务范围契约和证据，不能由文件名推导。

现有地区清单还包含 `eu-isle-of-man`、`eu-malta`、`eu-cyprus`、`eu-iceland`。本次 OSM France 欧洲 polygon 目录未列出同名文件；`united_kingdom/` 子目录列出 England 和 Northern Ireland，未列 Isle of Man。本轮没有确认这些地区的其他安全映射，目录未列出不代表整个提供方不存在数据。Gibraltar、San Marino、Vatican City 不在本项目独立地区清单中，不新增地区任务以促成准入。

来源：[Geofabrik catalog](https://download.geofabrik.de/index-v1.json)、[OSM France 欧洲 polygons](https://download.openstreetmap.fr/polygons/europe/)、[英国 polygons](https://download.openstreetmap.fr/polygons/europe/united_kingdom/)。

## 完整 PBF 与引用检查

本轮完整下载 Monaco 和 Andorra 两份小样本，检查提供方快照的摘要与引用。本轮没有下载其他失败候选；Luxembourg 的目录体积为 53 MB，未下载。

下载各 PBF 前、后取得同一 URL 的 MD5 文件；两个 MD5 文件的摘要相同，完整下载的 MD5 与其一致。SHA-256 基于完整下载计算。HTTP 成功与摘要通过只证明该次传输，不证明几何或引用完整。

| 指标 | Monaco | Andorra |
| --- | --- | --- |
| 文件 bytes | 1,103,808 | 3,958,247 |
| MD5 | `47448acee3e1cfe0fb71188dd31fb0d0` | `b17bd1909b5c70b79e8d10583f7f9540` |
| SHA-256 | `7919cacab2cd544e4f3784493a9fb587a47966abe5f348c3521b90713897ee93` | `87808bbc05cd2ebe58b2337ffe4b45425fbb20e641a303118a4ac5310303ee9f` |
| PBF replication timestamp | `2026-10-01T01:15:19Z` | `2026-10-01T01:34:41Z` |
| PBF replication sequence | `7309660` | `7309679` |
| nodes / ways / relations | 68,512 / 10,165 / 371 | 511,463 / 27,066 / 857 |
| `osmium check-refs` 缺失的 way 节点引用次数 | 86 | 814 |
| 涉及缺失节点的 way 数 | 25 | 37 |
| 缺少成员或传递依赖不完整的 relation 数 | 117 | 182 |
| 符合当前命名与 POI 分类规则的 relation 数 | 8 | 18 |
| 其中引用不完整的业务 relation 数 | 0 | 1 |

Monaco 的缺失示例为 `node/13316705332` 被 `way/4978262` 引用；Andorra 的缺失示例为 `node/52613726` 被 `way/6269464` 引用。Andorra 不完整业务 relation 为 `relation/2800131`，`type=multipolygon`，名称为 `Parc Natural de l'Alt Pirineu`。该样本仅用于说明完整性缺口，不据此声称线上缺失了此 POI。

工具为本机已有 `osmium 1.19.1 / libosmium 2.23.1`。执行 `osmium check-refs <sample.osm.pbf>` 检查全体 way 的节点引用；通过 `osmium cat <sample.osm.pbf> -f osm` 读取对象 ID、标签和成员，用 Python 标准库解析 XML，计算缺失引用及向父 relation 的传递。业务筛选按 [format.rs](../src/format.rs) 的 `has_name` 和 `is_poi` 规则执行，包括当前支持的 amenity、shop、railway、tourism、leisure、healthcare 与公交 highway 标签。全部 way 引用检查对应 [build.rs](../src/build.rs) 的 `classify_relations` 前置要求，不能因缺失 way 不属于 POI 而豁免。

两份 PBF 的 generator 为 `pyosmium-up-to-date/3.6.0`，header 中 replication base URL 分别为：

```text
http://dev.download.openstreetmap.fr/replication/./europe/monaco/minute
http://dev.download.openstreetmap.fr/replication/./europe/andorra/minute
```

它们与此前巴西样本的 `download.openstreetmap.fr` 主机不同。不能因为 header 写入某 URL 就扩充可信主机，也不能在未验证同一增量链的前提下改写为生产主机。本轮没有访问上述 dev 增量地址，也没有验证它与其他主机的 sequence 是否等价。

样本来源：[Monaco PBF](https://download.openstreetmap.fr/extracts/europe/monaco.osm.pbf)、[Monaco MD5](https://download.openstreetmap.fr/extracts/europe/monaco.osm.pbf.md5)、[Andorra PBF](https://download.openstreetmap.fr/extracts/europe/andorra.osm.pbf)、[Andorra MD5](https://download.openstreetmap.fr/extracts/europe/andorra.osm.pbf.md5)。这些链接内容会更新，复测结果须关联完整文件摘要。

## 边界证据标识

本次取得文件的 SHA-256：

| 文件 | SHA-256 |
| --- | --- |
| Geofabrik `index-v1.json` | `9b1d170b09eee255c0e56925788eb367646397c1ba1882f36b4c5c79133691fc` |
| OSM France `andorra.poly` | `564abbb3cda9c80ccc3d2152caeeed6bfce74484f5732c0329f1c7c244fa3af5` |
| OSM France `monaco.poly` | `536c560d1dac32335d238583886ebecdd8e5e56ac2a750cbc377415c1ecd642d` |
| OSM France `luxembourg.poly` | `d9b914380505012197b7fb427274d3896d6f06b60be8d17d36b1bd74aca62172` |
| OSM France `guernesey.poly` | `085598a1f1bb5168c3376dad7695d0a3e5d56756a846a230a3015a277634bb85` |
| OSM France `jersey.poly` | `3b475751d7683fb4c5dd09844ad08b4b042ef1f5bdb175362e4d2d64bcf75f5c` |

## 准入决定

本轮启用映射数为 **0**。原因不是网络状态，而是覆盖与引用证据不满足当前程序契约。允许接入提供方适配和候选验收机制，不允许把这些失败样本作为生产备用地区。

本轮没有进行跨源 POI 产物差异比较或生产发布：候选在构建前的几何和引用检查中失败。后续需要找到包含 canonical 范围、完整引用的数据快照，或实现经验证的父区域裁剪与依赖补全，再执行影子构建与发布前验收。不得通过缩小已发布 coverage、忽略缺失 way 节点、拼接另一源 diff 等方式把失败样本改为合格。

## 巴西北部故障目标的补充检查

`br-norte` 的主源 extract 为 `south-america/brazil/norte`，候选为 OSM France `south-america/brazil/north`。本次依据 catalog 中 PBF URL 匹配 canonical feature，检查其完整 geometry 结构；Geofabrik `MultiPolygon` 包含一个 polygon、一个外环、254 个顶点、无洞。OSM France `north.poly` 包含一个外环、1,840 个顶点、无洞。

采用前述精确有理数点分类方法，在 Geofabrik 顶点 `(-60.694010, -13.734070)` 附近取得反例 `(-60.694010, -13.734069)`。该点处于 Geofabrik canonical polygon 严格内部、OSM France polygon 外部，且不在任一多边形边界上。因此候选 polygon 不包含当前 canonical 覆盖，**`br-norte` 不准入自动切源**。

本次 catalog SHA-256 与前次相同：`9b1d170b09eee255c0e56925788eb367646397c1ba1882f36b4c5c79133691fc`。OSM France `north.poly` SHA-256 为 `6cda2f1427b5e55a93865ad38f01166c1164cb9a7702e8ce53420edd7af44fc0`。

此结论证明 polygon 覆盖条件失败，不宣称已验证候选 PBF 中具体 POI 的缺失。未下载约 173 MB 的 north PBF，未执行其摘要、引用或产物比较。完整多边形差集仅在没有反例、需要证明包含时继续执行；本次反例足以拒绝候选。其余四个巴西地区本轮未检查，不能从北部结果推断其准入状态。

来源：[Geofabrik catalog](https://download.geofabrik.de/index-v1.json)、[OSM France north.poly](https://download.openstreetmap.fr/polygons/south-america/brazil/north.poly)。

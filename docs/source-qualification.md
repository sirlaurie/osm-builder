# 备用来源真实地区准入调查

初始调查时间：2026-10-01 09:44–10:11 UTC。
复测与 POI 集合覆盖准入时间：2026-11-08 02:28–02:40 UTC。

## 准入标准变更与结论

项目已执行两项准入与构建规则调整：

1. **Way 节点引用检查范围收缩至业务所需对象**：`build::prepare_geometry` 仅对进入 `needed` 集合的 `way`（自身为 POI 的 `way` 或被 POI `relation` 直接/传递引用的 `way`）强制要求全部节点引用完整；非 POI 且未被任何 POI `relation` 引用的无关 `way` 缺失边界外节点时不再中止构建。
2. **备用源准入标准改为业务 POI 集合覆盖（方案 A）**：不再要求 OSM France 切分多边形在几何上严格包含 Geofabrik 多边形（排除公海缓冲差异导致的伪反例），改为要求：
   - PBF 头部具备可信 `https://download.openstreetmap.fr/replication/<extract>/minute` 增量链且 MD5 摘要一致；
   - 经 `build::run` 编译通过；
   - OSM France 产出的 POI ID 集合 100% 覆盖 Geofabrik 产出的 POI ID 集合（`missing_in_fr == 0`）；
   - 满足发布器的完整性门槛：`count > 0`、`excludedIncompleteRelationCount == 0`，且保留 Geofabrik canonical `coverage` 以通过跨源发布校验。

本轮启用真实备用映射 **2** 个：`au-ashmore-cartier` 与 `eu-monaco`，已写入 [config/fallback-sources.json](../config/fallback-sources.json)。

## 2026-11-08 候选地区全量构建与 POI 覆盖实测

Geofabrik 目录 `index-v1.json` SHA-256：`9b1d170b09eee255c0e56925788eb367646397c1ba1882f36b4c5c79133691fc`。各 PBF 下载前后均核对两次 `.md5` 摘要一致。

| 地区 | Geofabrik extract | OSM France extract | Geofabrik POI 数 | OSM France POI 数 | Geofabrik 缺失被 FR 遗漏数 | Geofabrik 不完整业务 relation | OSM France 不完整业务 relation | 准入结论 |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| `au-ashmore-cartier` | `australia-oceania/australia/ashmore-cartier` | `oceania/australia/ashmore_and_cartier_islands` | 4 | 4 | 0 | 0 | 0 | **准入** |
| `eu-monaco` | `europe/monaco` | `europe/monaco` | 847 | 991 | 0 | 1 | 0 | **准入** |
| `eu-andorra` | `europe/andorra` | `europe/andorra` | 1,889 | 1,892 | 0 | 1 | 1 (`osm_relation_2800131`) | 不准入（含不完整业务 relation） |
| `cn-macau` | `asia/china/macau` | `asia/china/macau` | 3,465 | 3,577 | 0 | 0 | 1 (`osm_relation_17688658`) | 不准入（含不完整业务 relation） |
| `au-christmas-island` | `australia-oceania/australia/christmas-island` | `oceania/australia/christmas_island` | 55 | 55 | 0 | 1 | 1 (`osm_relation_16204964`) | 不准入（含不完整业务 relation） |
| `au-cocos-islands` | `australia-oceania/australia/cocos-islands` | `oceania/australia/cocos_islands` | 29 | 29 | 0 | 1 | 1 (`osm_relation_16204837`) | 不准入（含不完整业务 relation） |
| `au-norfolk-island` | `australia-oceania/australia/norfolk-island` | `oceania/australia/norfolk_island` | 217 | 217 | 0 | 1 | 1 (`osm_relation_8425531`) | 不准入（含不完整业务 relation） |
| `au-coral-sea-islands` | `australia-oceania/australia/coral-sea-islands` | `oceania/australia/coral_sea_islands` | 0 | 1 | 0 | 8 | 7 | 不准入（含不完整业务 relation） |
| `au-heard-mcdonald` | `australia-oceania/australia/heard-mcdonald` | `oceania/australia/heard_island_and_mcdonald_slands` | 1 | 1 | 0 | 1 | 1 | 不准入（含不完整业务 relation） |
| `eu-france-corse` | `europe/france/corse` | `europe/france/corse` | 8,910 | 8,910 | 0 | 1 | 1 (`osm_relation_1023772`) | 不准入（含不完整业务 relation） |

### 已准入地区快照证据

1. **`au-ashmore-cartier`**
   - Geofabrik PBF SHA-256：`45c90c5a86f882f7b5fa30792d23c67adf32d4edccc513b46618c7303bfeb379`（sequence `214`，timestamp `2026-11-07T21:21:36Z`）
   - OSM France PBF SHA-256：`34968e915f5533b7d0f57f82a636025d125d36c085e927d1f983b953ff4e2016`（sequence `7361745`，timestamp `2026-11-07T00:30:51Z`，header replication URL `http://download.openstreetmap.fr/replication/./oceania/australia/ashmore_and_cartier_islands/minute`）
   - POI 对比：两边均产出相同 4 个 POI，3 个逻辑 block 打包为 3 个 pack 对象（2,421 bytes），`excludedIncompleteRelationCount = 0`。

2. **`eu-monaco`**
   - Geofabrik PBF SHA-256：`3395eb90bfe8737cc95016d6fa2dd0d795bdb2772cd5c628ec3bdbb0f5ba323e`（sequence `4961`，timestamp `2026-11-07T21:21:36Z`）
   - OSM France PBF SHA-256：`f78637797818bd5852022a9c1315191aa6649eaa7bee4db3acfc6c52042c7078`（sequence `7361792`，timestamp `2026-11-07T01:18:40Z`，header replication URL `http://download.openstreetmap.fr/replication/./europe/monaco/minute`）
   - POI 对比：Geofabrik 产出 847 个 POI（排除 1 个跨边界不完整 relation `osm_relation_10706777` `Port de Cap d'Ail`）；OSM France 产出 991 个 POI，完整包含 Geofabrik 的全部 847 个 POI，且 `Port de Cap d'Ail` 在 OSM France 快照中成员完整（`excludedIncompleteRelationCount = 0`）。

## 历史几何筛选记录（2026-10-01）

以下为 2026-10-01 针对多边形几何包含关系的检查记录（供参考两源切分边界的差异背景）：

| 地区 | 候选 polygon | Geofabrik 外环顶点数 | 位于候选外的顶点数 | 严格内部反例点 | 几何包含结论 |
| --- | --- | ---: | ---: | --- | --- |
| `eu-monaco` | `europe/monaco.poly` | 19 | 2 | `(7.595670, 43.520670)`（海域） | 不包含（但业务 POI 100% 覆盖） |
| `eu-andorra` | `europe/andorra.poly` | 117 | 1 | `(1.733424, 42.522870)` | 不包含 |
| `eu-luxembourg` | `europe/luxembourg.poly` | 276 | 23 | `(5.961754, 50.176310)` | 不包含 |
| `eu-guernsey-jersey` | `europe/guernesey.poly` 与 `europe/jersey.poly` 并集 | 10 | 10 | `(-2.003340, 49.766400)` | 不包含 |
| `br-norte` | `south-america/brazil/north.poly` | 254 | - | `(-60.694010, -13.734069)` | 不包含 |

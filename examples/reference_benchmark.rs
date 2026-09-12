use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    time::Instant,
};

use anyhow::{Context, Result, ensure};
use clap::Parser;
use rusqlite::{Connection, OpenFlags, OptionalExtension, StatementStatus, params};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const SCHEMA: &str = "CREATE TABLE objects (
    kind TEXT NOT NULL, id INTEGER NOT NULL, tags TEXT NOT NULL,
    lat REAL, lon REAL, poi INTEGER NOT NULL, PRIMARY KEY (kind, id)
) WITHOUT ROWID;
CREATE TABLE refs (
    kind TEXT NOT NULL, id INTEGER NOT NULL, position INTEGER NOT NULL,
    target_kind TEXT NOT NULL, target_id INTEGER NOT NULL,
    PRIMARY KEY (kind, id, position)
) WITHOUT ROWID;
CREATE TABLE bounds (
    kind TEXT NOT NULL, id INTEGER NOT NULL,
    west REAL NOT NULL, south REAL NOT NULL, east REAL NOT NULL, north REAL NOT NULL,
    PRIMARY KEY (kind, id)
) WITHOUT ROWID;
CREATE TABLE pois (
    cell TEXT NOT NULL, id TEXT NOT NULL, payload BLOB NOT NULL,
    PRIMARY KEY (cell, id)
) WITHOUT ROWID;";

const WAY_CHECK: &str = "SELECT refs.id, refs.target_id FROM refs
LEFT JOIN objects ON objects.kind='node' AND objects.id=refs.target_id
WHERE refs.kind='way' AND objects.id IS NULL LIMIT 1";

const ORDERED_WAY_CHECK: &str = "SELECT refs.id, refs.target_id
FROM refs INDEXED BY refs_target
LEFT JOIN objects ON objects.kind='node' AND objects.id=refs.target_id
WHERE refs.kind='way' AND objects.id IS NULL
ORDER BY refs.target_kind, refs.target_id LIMIT 1";

const REVERSE_INDEX: &str = "CREATE INDEX refs_target ON refs(target_kind, target_id, kind, id);";

const INCOMPLETE_CTE: &str = "WITH RECURSIVE incomplete(id) AS (
    SELECT refs.id FROM refs
    LEFT JOIN objects ON objects.kind=refs.target_kind AND objects.id=refs.target_id
    WHERE refs.kind='relation' AND objects.id IS NULL
    UNION
    SELECT refs.id FROM incomplete
    JOIN refs ON refs.target_kind='relation' AND refs.target_id=incomplete.id
    WHERE refs.kind='relation'
)";

#[derive(Parser)]
struct Arguments {
    #[arg(long, default_value = ".build/tools/perf/reference-study")]
    output: PathBuf,
    #[arg(long, default_value_t = 3_000_000)]
    nodes: u64,
    #[arg(long, default_value_t = 300_000)]
    ways: u64,
    #[arg(long, default_value_t = 30_000)]
    relations: u64,
    #[arg(long, default_value_t = 3)]
    rounds: usize,
}

#[derive(Clone, Copy)]
struct Scenario {
    cache_mib: i64,
    threads: i64,
    target_order: bool,
}

impl Scenario {
    fn label(self) -> String {
        format!(
            "cache-{}-threads-{}-{}",
            self.cache_mib,
            self.threads,
            if self.target_order {
                "target"
            } else {
                "source"
            }
        )
    }
}

fn configure(connection: &Connection, scenario: Scenario) -> Result<()> {
    connection.pragma_update(None, "cache_size", -scenario.cache_mib * 1024)?;
    connection.pragma_update(None, "temp_store", "FILE")?;
    connection.pragma_update(None, "synchronous", "FULL")?;
    connection.pragma_update(None, "threads", scenario.threads)?;
    connection.busy_timeout(std::time::Duration::from_secs(5))?;
    Ok(())
}

fn open(path: &Path, scenario: Scenario) -> Result<Connection> {
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    configure(&connection, scenario)?;
    Ok(connection)
}

fn mixed(value: u64) -> u64 {
    let mut value = value.wrapping_add(0x9e3779b97f4a7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
    value ^ (value >> 31)
}

fn build_fixture(path: &Path, arguments: &Arguments) -> Result<(BTreeSet<i64>, Value)> {
    let scenario = Scenario {
        cache_mib: 16,
        threads: 0,
        target_order: false,
    };
    let mut connection = open(path, scenario)?;
    connection.execute_batch(SCHEMA)?;
    let transaction = connection.transaction()?;
    let mut excluded = BTreeSet::new();
    let mut relation_refs = 0u64;
    let mut sample_refs = Vec::new();
    {
        let mut objects = transaction.prepare("INSERT INTO objects VALUES (?,?,?,?,?,?)")?;
        let mut refs = transaction.prepare("INSERT INTO refs VALUES (?,?,?,?,?)")?;
        for id in 1..=arguments.nodes {
            let poi = id % 100 == 0;
            let tags = if poi {
                r#"{"amenity":"cafe","name":"Synthetic Cafe"}"#
            } else {
                "{}"
            };
            objects.execute(params![
                "node",
                id as i64,
                tags,
                -34.0 + (id % 1000) as f64 / 100_000.0,
                151.0 + (id / 1000 % 1000) as f64 / 100_000.0,
                poi
            ])?;
        }
        for id in 1..=arguments.ways {
            objects.execute(params![
                "way",
                id as i64,
                "{}",
                None::<f64>,
                None::<f64>,
                false
            ])?;
            for position in 0..10 {
                let target = 1 + mixed((id - 1) * 10 + position) % arguments.nodes;
                refs.execute(params![
                    "way",
                    id as i64,
                    position as i64,
                    "node",
                    target as i64
                ])?;
                if id <= 3 {
                    sample_refs.push(json!([id, position, target]));
                }
            }
        }
        for id in 1..=arguments.relations {
            objects.execute(params![
                "relation",
                id as i64,
                "{}",
                None::<f64>,
                None::<f64>,
                false
            ])?;
            let mut position = 0i64;
            for member in 0..5 {
                let target = 1 + mixed(0x517cc1b727220a95 ^ (id * 5 + member)) % arguments.ways;
                refs.execute(params![
                    "relation",
                    id as i64,
                    position,
                    "way",
                    target as i64
                ])?;
                position += 1;
            }
            if id % 997 == 0 {
                refs.execute(params![
                    "relation",
                    id as i64,
                    position,
                    "way",
                    (arguments.ways + id) as i64
                ])?;
                position += 1;
                excluded.insert(id as i64);
            }
            if id % 10 == 0 {
                refs.execute(params![
                    "relation",
                    id as i64,
                    position,
                    "relation",
                    (id - 1) as i64
                ])?;
                position += 1;
                if excluded.contains(&((id - 1) as i64)) {
                    excluded.insert(id as i64);
                }
            }
            relation_refs += position as u64;
        }
    }
    transaction.commit()?;
    let page_size: i64 = connection.pragma_query_value(None, "page_size", |row| row.get(0))?;
    connection.close().map_err(|(_, error)| error)?;
    let metadata = json!({
        "nodes": arguments.nodes, "ways": arguments.ways, "relations": arguments.relations,
        "objects": arguments.nodes + arguments.ways + arguments.relations,
        "wayReferences": arguments.ways * 10, "relationReferences": relation_refs,
        "references": arguments.ways * 10 + relation_refs,
        "expectedIncompleteRelations": excluded.len(), "pageSize": page_size,
        "baseDatabaseBytes": fs::metadata(path)?.len(), "sampleWayReferences": sample_refs,
        "generator": "splitmix64-derived targets across the complete node ID range; 10 node refs per way; five way refs per relation; every 997th relation has one missing way; every 10th relation refers to its predecessor",
        "validWayInvariant": "Every normal-fixture way reference targets an existing node; ordered validation retains the original predicate and does not filter on target_kind"
    });
    Ok((excluded, metadata))
}

fn incomplete_sql() -> String {
    format!(
        "CREATE TABLE incomplete_relations (id INTEGER PRIMARY KEY);\n{INCOMPLETE_CTE} INSERT INTO incomplete_relations SELECT id FROM incomplete;\nCREATE TABLE validated_geometry (kind TEXT NOT NULL,id INTEGER NOT NULL,PRIMARY KEY(kind,id)) WITHOUT ROWID;"
    )
}

struct WayMeasurement {
    missing: Option<(i64, i64)>,
    seconds: f64,
    vm_steps: i32,
    sorts: i32,
}

fn check_way(connection: &Connection, target_order: bool) -> Result<WayMeasurement> {
    let started = Instant::now();
    let mut statement = connection.prepare(if target_order {
        ORDERED_WAY_CHECK
    } else {
        WAY_CHECK
    })?;
    let missing = statement
        .query_row([], |row| Ok((row.get(0)?, row.get(1)?)))
        .optional()?;
    let seconds = started.elapsed().as_secs_f64();
    Ok(WayMeasurement {
        missing,
        seconds,
        vm_steps: statement.get_status(StatementStatus::VmStep),
        sorts: statement.get_status(StatementStatus::Sort),
    })
}

fn read_incomplete(connection: &Connection) -> Result<BTreeSet<i64>> {
    let mut statement = connection.prepare("SELECT id FROM incomplete_relations ORDER BY id")?;
    Ok(statement
        .query_map([], |row| row.get(0))?
        .collect::<rusqlite::Result<_>>()?)
}

fn query_plan(connection: &Connection, sql: &str) -> Result<Vec<String>> {
    let mut statement = connection.prepare(&format!("EXPLAIN QUERY PLAN {sql}"))?;
    Ok(statement
        .query_map([], |row| row.get(3))?
        .collect::<rusqlite::Result<_>>()?)
}

fn self_check(scratch: &Path) -> Result<()> {
    for target_order in [false, true] {
        let path = scratch.join(format!("self-check-{target_order}.sqlite"));
        let connection = open(
            &path,
            Scenario {
                cache_mib: 16,
                threads: 0,
                target_order,
            },
        )?;
        connection.execute_batch(SCHEMA)?;
        connection.execute_batch("INSERT INTO objects VALUES ('node',1,'{}',0,0,0),('way',1,'{}',NULL,NULL,0),('relation',10,'{}',NULL,NULL,0),('relation',11,'{}',NULL,NULL,0),('relation',12,'{}',NULL,NULL,0);
            INSERT INTO refs VALUES ('way',1,0,'node',42),('relation',10,0,'way',900),('relation',11,0,'relation',10),('relation',12,0,'relation',11);")?;
        if target_order {
            connection.execute_batch(REVERSE_INDEX)?;
        }
        ensure!(
            check_way(&connection, target_order)?.missing == Some((1, 42)),
            "Missing node self-check failed"
        );
        connection.execute("INSERT INTO objects VALUES ('node',42,'{}',0,0,0)", [])?;
        ensure!(
            check_way(&connection, target_order)?.missing.is_none(),
            "Complete way self-check failed"
        );
        if !target_order {
            connection.execute_batch(REVERSE_INDEX)?;
        }
        connection.execute_batch(&incomplete_sql())?;
        ensure!(
            read_incomplete(&connection)? == BTreeSet::from([10, 11, 12]),
            "Nested incomplete relation self-check failed"
        );
        connection.close().map_err(|(_, error)| error)?;
        fs::remove_file(path)?;
    }
    Ok(())
}

fn timed<T>(operation: impl FnOnce() -> Result<T>) -> Result<(T, f64)> {
    let started = Instant::now();
    let result = operation()?;
    Ok((result, started.elapsed().as_secs_f64()))
}

fn run_one(
    base: &Path,
    scratch: &Path,
    scenario: Scenario,
    round: usize,
    expected: &BTreeSet<i64>,
) -> Result<Value> {
    let directory = tempfile::Builder::new()
        .prefix("run-")
        .tempdir_in(scratch)?;
    let path = directory.path().join("state.sqlite");
    fs::copy(base, &path)?;
    let mut connection = open(&path, scenario)?;
    let effective_threads: i64 =
        connection.pragma_query_value(None, "threads", |row| row.get(0))?;
    let effective_cache: i64 =
        connection.pragma_query_value(None, "cache_size", |row| row.get(0))?;
    let temp_store: i64 = connection.pragma_query_value(None, "temp_store", |row| row.get(0))?;
    let synchronous: i64 = connection.pragma_query_value(None, "synchronous", |row| row.get(0))?;
    ensure!(
        effective_threads == scenario.threads
            && effective_cache == -scenario.cache_mib * 1024
            && temp_store == 1
            && synchronous == 2,
        "SQLite did not accept the requested PRAGMAs"
    );
    let transaction = connection.transaction()?;
    let (way_plan, way_measurement, index_seconds) = if scenario.target_order {
        let (_, index_seconds) = timed(|| Ok(transaction.execute_batch(REVERSE_INDEX)?))?;
        let plan = query_plan(&transaction, ORDERED_WAY_CHECK)?;
        ensure!(
            !plan.iter().any(|line| line.contains("TEMP B-TREE")),
            "Ordered validation unexpectedly needs a temporary sort"
        );
        ensure!(
            plan.iter().any(|line| line.contains("refs_target")),
            "Ordered validation did not use refs_target"
        );
        let measured = check_way(&transaction, true)?;
        ensure!(
            measured.sorts == 0,
            "Ordered validation performed an extra sort"
        );
        (plan, measured, index_seconds)
    } else {
        let plan = query_plan(&transaction, WAY_CHECK)?;
        let measured = check_way(&transaction, false)?;
        let (_, index_seconds) = timed(|| Ok(transaction.execute_batch(REVERSE_INDEX)?))?;
        (plan, measured, index_seconds)
    };
    let WayMeasurement {
        missing,
        seconds: way_seconds,
        vm_steps,
        sorts,
    } = way_measurement;
    ensure!(
        missing.is_none(),
        "Normal fixture contains a missing way node"
    );
    let incomplete_plan = query_plan(
        &transaction,
        &format!("{INCOMPLETE_CTE} SELECT id FROM incomplete"),
    )?;
    let incomplete = incomplete_sql();
    let (_, incomplete_seconds) = timed(|| Ok(transaction.execute_batch(&incomplete)?))?;
    let (_, commit_seconds) = timed(|| Ok(transaction.commit()?))?;
    let actual = read_incomplete(&connection)?;
    ensure!(
        &actual == expected,
        "Incomplete relation output differs from the generator's independent expected set"
    );
    let mut digest = Sha256::new();
    for id in &actual {
        digest.update(format!("{id}\n").as_bytes());
    }
    connection.close().map_err(|(_, error)| error)?;
    let result = json!({
        "scenario": scenario.label(), "round": round, "cacheMiB": scenario.cache_mib,
        "requestedThreads": scenario.threads, "effectiveThreads": effective_threads,
        "effectiveCacheKiB": effective_cache, "tempStore": temp_store, "synchronous": synchronous,
        "order": if scenario.target_order { "index, target-ordered way check, incomplete relations" } else { "source-ordered way check, index, incomplete relations" },
        "wayCheckSeconds": way_seconds, "indexSeconds": index_seconds,
        "incompleteSeconds": incomplete_seconds, "sqlSeconds": way_seconds + index_seconds + incomplete_seconds,
        "commitSeconds": commit_seconds, "measuredSeconds": way_seconds + index_seconds + incomplete_seconds + commit_seconds,
        "wayQueryPlan": way_plan, "wayVmSteps": vm_steps, "waySorts": sorts, "incompleteQueryPlan": incomplete_plan,
        "incompleteCount": actual.len(), "incompleteIdsSHA256": format!("{:x}", digest.finalize()),
        "resultDatabaseBytes": fs::metadata(&path)?.len()
    });
    drop(directory);
    Ok(result)
}

fn median(values: &[f64]) -> f64 {
    let mut values = values.to_vec();
    values.sort_by(f64::total_cmp);
    if values.len().is_multiple_of(2) {
        (values[values.len() / 2 - 1] + values[values.len() / 2]) / 2.0
    } else {
        values[values.len() / 2]
    }
}

fn save_report(
    output: &Path,
    fixture: &Value,
    scenarios: &[Scenario],
    runs: &[Value],
) -> Result<()> {
    let mut summaries = Vec::new();
    let mut markdown = format!(
        "# 引用阶段 SQLite 磁盘基准\n\nSQLite {}；{} 个对象，{} 条 way 引用，{} 条 relation 引用；每配置 {} 轮。\n\n配置仅作用于本基准，没有修改生产引擎或默认值。计时排除造数、复制、连接设置、EXPLAIN、StatementStatus 读取和结果验证；提交单列。所有连接使用 temp_store=FILE 和 synchronous=FULL。每次使用新连接，SQLite 页缓存不继承前次连接；没有清空操作系统文件缓存，复制和先前轮次可能影响命中，本结果不代表冷盘。\n\n| Cache MiB | SQLite threads | 检查顺序 | Way 检查秒 | 反向索引秒 | Relation 秒 | SQL 合计秒 | 提交秒 | 合计秒 |\n| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |\n",
        rusqlite::version(),
        fixture["objects"],
        fixture["wayReferences"],
        fixture["relationReferences"],
        runs.len() / scenarios.len()
    );
    for scenario in scenarios {
        let rows: Vec<_> = runs
            .iter()
            .filter(|run| run["scenario"] == scenario.label())
            .collect();
        let mut summary = json!({"scenario": scenario.label(), "cacheMiB": scenario.cache_mib, "threads": scenario.threads, "targetOrder": scenario.target_order});
        for field in [
            "wayCheckSeconds",
            "indexSeconds",
            "incompleteSeconds",
            "sqlSeconds",
            "commitSeconds",
            "measuredSeconds",
        ] {
            let values: Vec<_> = rows
                .iter()
                .map(|row| row[field].as_f64().unwrap())
                .collect();
            summary[field] = json!({"median": median(&values), "min": values.iter().copied().fold(f64::INFINITY, f64::min), "max": values.iter().copied().fold(0.0, f64::max)});
        }
        markdown.push_str(&format!(
            "| {} | {} | {} | {:.3} | {:.3} | {:.3} | {:.3} | {:.3} | {:.3} |\n",
            scenario.cache_mib,
            scenario.threads,
            if scenario.target_order {
                "先索引，target 顺序"
            } else {
                "优化前顺序"
            },
            summary["wayCheckSeconds"]["median"].as_f64().unwrap(),
            summary["indexSeconds"]["median"].as_f64().unwrap(),
            summary["incompleteSeconds"]["median"].as_f64().unwrap(),
            summary["sqlSeconds"]["median"].as_f64().unwrap(),
            summary["commitSeconds"]["median"].as_f64().unwrap(),
            summary["measuredSeconds"]["median"].as_f64().unwrap()
        ));
        summaries.push(summary);
    }
    markdown.push_str("\n表格为各指标中位数，因此分项中位数之和可能不同于合计中位数。正常数据的全部 way 引用都检查节点是否存在；target 方案仅改变遍历顺序，没有增加过滤条件。小错误样本验证缺失 node 与多层不完整 relation；正常样本的全部结果与生成器计算的 ID 集合一致。\n\n每轮 JSON 保存查询计划、实际 PRAGMA、分项耗时、结果计数和排序 ID 摘要。此基准覆盖跨节点 ID 的乱序引用，未模拟所有真实 OSM 引用局部性、标签分布或冷盘 I/O，不能据此承诺其他机器或地区的提速。数据库与 SQLite 临时数据在退出前清理。\n");
    let summary = json!({"sqliteVersion": rusqlite::version(), "architecture": std::env::consts::ARCH, "os": std::env::consts::OS, "availableParallelism": std::thread::available_parallelism()?.get(), "fixture": fixture, "benchmarkSourceSHA256": format!("{:x}", Sha256::digest(include_bytes!("reference_benchmark.rs"))), "schema": SCHEMA, "baselineWayCheck": WAY_CHECK, "orderedWayCheck": ORDERED_WAY_CHECK, "reverseIndex": REVERSE_INDEX, "incompleteSql": incomplete_sql(), "selfChecks": ["missing way node", "repaired way node", "nested incomplete relations"], "summaries": summaries, "runs": runs});
    fs::write(
        output.join("summary.json"),
        serde_json::to_vec_pretty(&summary)?,
    )?;
    fs::write(output.join("report.md"), markdown)?;
    Ok(())
}

fn main() -> Result<()> {
    let arguments = Arguments::parse();
    ensure!(
        arguments.nodes > 0
            && arguments.ways > 0
            && arguments.relations > 0
            && (1..=10).contains(&arguments.rounds),
        "Invalid fixture size or round count"
    );
    ensure!(
        arguments.nodes <= i64::MAX as u64
            && arguments.ways <= i64::MAX as u64 / 10
            && arguments.relations <= i64::MAX as u64 / 10,
        "Fixture identifiers exceed SQLite range"
    );
    ensure!(
        arguments
            .output
            .components()
            .any(|part| part.as_os_str() == ".build"),
        "Research output must be under .build"
    );
    let sqlite_tmp = std::env::var_os("SQLITE_TMPDIR")
        .context("SQLITE_TMPDIR must be set under .build, as in the production CLI")?;
    ensure!(
        Path::new(&sqlite_tmp)
            .components()
            .any(|part| part.as_os_str() == ".build"),
        "SQLite temporary files must be under .build"
    );
    fs::create_dir_all(&sqlite_tmp)?;
    fs::create_dir_all(&arguments.output)?;
    ensure!(
        !arguments.output.join("summary.json").exists(),
        "Use a new research output directory"
    );
    let scratch = tempfile::Builder::new()
        .prefix("data-")
        .tempdir_in(&arguments.output)?;
    self_check(scratch.path())?;
    let base = scratch.path().join("base.sqlite");
    eprintln!("[reference benchmark] Generating disk fixture; excluded from measurements");
    let (expected, fixture) = build_fixture(&base, &arguments)?;
    fs::write(
        arguments.output.join("fixture.json"),
        serde_json::to_vec_pretty(&fixture)?,
    )?;
    eprintln!(
        "[reference benchmark] Base database: {} MiB",
        fs::metadata(&base)?.len() / 1024 / 1024
    );
    let mut scenarios = Vec::new();
    for cache_mib in [16, 256, 512] {
        for threads in [0, 4] {
            for target_order in [false, true] {
                scenarios.push(Scenario {
                    cache_mib,
                    threads,
                    target_order,
                });
            }
        }
    }
    let mut runs = Vec::new();
    for round in 0..arguments.rounds {
        let mut order = scenarios.clone();
        let length = order.len();
        order.rotate_left((round * 5) % length);
        if round % 2 == 1 {
            order.reverse();
        }
        for scenario in order {
            eprintln!(
                "[reference benchmark] Round {}: {}",
                round + 1,
                scenario.label()
            );
            let result = run_one(&base, scratch.path(), scenario, round + 1, &expected)?;
            fs::write(
                arguments
                    .output
                    .join(format!("round-{}-{}.json", round + 1, scenario.label())),
                serde_json::to_vec_pretty(&result)?,
            )?;
            eprintln!(
                "[reference benchmark] SQL {:.3}s, commit {:.3}s",
                result["sqlSeconds"].as_f64().unwrap(),
                result["commitSeconds"].as_f64().unwrap()
            );
            runs.push(result);
        }
    }
    save_report(&arguments.output, &fixture, &scenarios, &runs)?;
    drop(scratch);
    println!("{}", arguments.output.join("report.md").display());
    Ok(())
}

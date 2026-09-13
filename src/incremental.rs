use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail, ensure};
use flate2::read::MultiGzDecoder;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::build::{self, ComputeOptions, Progress};
use crate::compute;
use crate::format::{
    MAX_ID, canonical_json, hash_bytes, normalize_timestamp, read_coverage, source_metadata,
};
use crate::storage::atomic_write;

#[derive(Clone, Debug)]
pub struct InitOptions {
    pub input: PathBuf,
    pub state: PathBuf,
    pub coverage: PathBuf,
    pub region: String,
    pub source_timestamp: String,
    pub source_sequence: u64,
    pub source_sha256: String,
    pub replication_url: String,
}

#[derive(Clone, Debug)]
pub struct ApplyOptions {
    pub input: PathBuf,
    pub state: PathBuf,
    pub sequence: u64,
    pub timestamp: String,
    pub coverage: Option<PathBuf>,
}

fn reject_symlink(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => ensure!(
            !metadata.is_symlink(),
            "State path must not be a symbolic link: {}",
            path.display()
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

fn state_path(path: &Path, create: bool) -> Result<PathBuf> {
    reject_symlink(path)?;
    if create {
        fs::create_dir_all(path)?;
    }
    let path = path
        .canonicalize()
        .context("Region has no initialized state directory")?;
    reject_symlink(&path.join("state.sqlite"))?;
    reject_symlink(&path.join("output"))?;
    Ok(path)
}

fn load_control(connection: &Connection) -> Result<(Value, String, Value)> {
    let version: i64 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    ensure!(
        matches!(version, 1 | 2),
        "State initialization is incomplete or its format is unsupported"
    );
    let record: Option<(String, String, String)> = connection
        .query_row(
            "SELECT metadata,replication_url,receipt FROM control WHERE id=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let (metadata, url, receipt) = record.context("State initialization is incomplete")?;
    Ok((
        serde_json::from_str(&metadata)?,
        url,
        serde_json::from_str(&receipt)?,
    ))
}

fn export_committed(connection: &Connection, state: &Path) -> Result<Value> {
    let (metadata, url, receipt) = load_control(connection)?;
    let output = state.join("output");
    build::write_diagnostics(connection, &output)?;
    atomic_write(&output.join("release.json"), &canonical_json(&receipt)?)?;
    let mut result = receipt
        .as_object()
        .context("Invalid stored release")?
        .clone();
    result.insert("region".to_owned(), metadata["region"].clone());
    result.insert("sequence".to_owned(), metadata["sourceSequence"].clone());
    result.insert("timestamp".to_owned(), metadata["sourceTimestamp"].clone());
    result.insert("replicationUrl".to_owned(), json!(url));
    result.insert(
        "coverageSHA256".to_owned(),
        json!(hash_bytes(&canonical_json(&metadata["coverage"])?)),
    );
    result.insert("output".to_owned(), json!(output));
    Ok(Value::Object(result))
}

fn upgrade_packing(connection: &Connection, state: &Path) -> Result<()> {
    let version: i64 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if version == 2 {
        return Ok(());
    }
    let (metadata, _, previous) = load_control(connection)?;
    let region = metadata["region"]
        .as_str()
        .filter(|region| crate::format::is_region(region))
        .context("Invalid stored region")?;
    let marker = state
        .parent()
        .context("State has no parent directory")?
        .join(format!(".cleanup-{region}.json"));
    match fs::symlink_metadata(&marker) {
        Ok(_) => {
            bail!("Regional cleanup is pending; resume work --cleanup before upgrading the index")
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("Cannot inspect regional cleanup record"),
    }
    build::create_pack_index(connection)?;
    let output = state.join("output");
    build::write_packs(connection, &output, false)?;
    let receipt = build::write_manifest(
        connection,
        &output,
        &metadata,
        previous["count"]
            .as_u64()
            .context("Invalid stored POI count")?,
        previous["excludedIncompleteRelationCount"]
            .as_u64()
            .unwrap_or(0),
    )?;
    connection.execute(
        "UPDATE control SET receipt=? WHERE id=1",
        [String::from_utf8(canonical_json(&receipt)?)?],
    )?;
    connection.pragma_update(None, "user_version", 2)?;
    Ok(())
}

pub fn initialize(options: &InitOptions, compute: &ComputeOptions) -> Result<Value> {
    compute.validate()?;
    let metadata = source_metadata(
        &options.coverage,
        &options.region,
        &options.source_timestamp,
        Some(options.source_sequence),
        &options.source_sha256,
    )?;
    let replication_url = crate::source::replication_url(&options.replication_url)?;
    let state = state_path(&options.state, true)?;
    let database = state.join("state.sqlite");
    let output = state.join("output");
    ensure!(
        !output.exists(),
        "Initialization requires an unused output directory"
    );
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&database)
        .context("State database already exists; initialization cannot overwrite it")?;
    fs::create_dir(&output)?;
    let mut connection = build::open_database(&database, false, compute)?;
    build::ingest(&mut connection, &options.input, compute)?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let receipt = build::compile_database(&transaction, &output, &metadata, compute)?;
    transaction.execute_batch(
        "CREATE TABLE control (
            id INTEGER PRIMARY KEY CHECK(id=1), metadata TEXT NOT NULL,
            replication_url TEXT NOT NULL, receipt TEXT NOT NULL
         );",
    )?;
    transaction.execute(
        "INSERT INTO control VALUES (1,?,?,?)",
        params![
            String::from_utf8(canonical_json(&metadata)?)?,
            replication_url,
            String::from_utf8(canonical_json(&receipt)?)?,
        ],
    )?;
    transaction.pragma_update(None, "user_version", 2)?;
    transaction.commit()?;
    export_committed(&connection, &state)
}

fn stage_changes(
    connection: &mut Connection,
    input: &Path,
    options: &ComputeOptions,
) -> Result<String> {
    let mut checksum = Sha256::new();
    let mut file = BufReader::new(File::open(input)?);
    let mut buffer = [0u8; 65536];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        checksum.update(&buffer[..count]);
    }
    let checksum = format!("{:x}", checksum.finalize());
    let file = File::open(input)?;
    let reader: Box<dyn BufRead> = if input.extension().is_some_and(|extension| extension == "gz") {
        Box::new(BufReader::new(MultiGzDecoder::new(file)))
    } else {
        Box::new(BufReader::new(file))
    };
    let transaction = connection.transaction()?;
    transaction.execute_batch(
        "CREATE TEMP TABLE changes (
            kind TEXT NOT NULL,id INTEGER NOT NULL,version INTEGER NOT NULL,
            deleted INTEGER NOT NULL,tags TEXT NOT NULL,lat REAL,lon REAL,poi INTEGER NOT NULL,
            PRIMARY KEY(kind,id)
         ) WITHOUT ROWID;
         CREATE TEMP TABLE changed_refs (
            kind TEXT NOT NULL,id INTEGER NOT NULL,position INTEGER NOT NULL,
            target_kind TEXT NOT NULL,target_id INTEGER NOT NULL,
            PRIMARY KEY(kind,id,position)
         ) WITHOUT ROWID;
         CREATE TEMP TABLE affected (kind TEXT NOT NULL,id INTEGER NOT NULL,PRIMARY KEY(kind,id)) WITHOUT ROWID;
         CREATE TEMP TABLE dirty_cells (cell TEXT PRIMARY KEY) WITHOUT ROWID;",
    )?;
    let mut progress = Progress::new("OSC parse and change staging");
    {
        let mut previous =
            transaction.prepare("SELECT version FROM changes WHERE kind=? AND id=?")?;
        let mut insert =
            transaction.prepare("INSERT OR REPLACE INTO changes VALUES (?,?,?,?,?,?,?,?)")?;
        let mut remove_refs =
            transaction.prepare("DELETE FROM changed_refs WHERE kind=? AND id=?")?;
        let mut insert_ref = transaction.prepare("INSERT INTO changed_refs VALUES (?,?,?,?,?)")?;
        compute::read_changes(
            reader,
            options.batch_bytes,
            |kind, id, version| {
                let prior: Option<i64> = previous
                    .query_row(params![kind.as_str(), id], |row| row.get(0))
                    .optional()?;
                Ok(prior.is_none_or(|prior| prior <= version))
            },
            |object, version, deleted| {
                insert.execute(params![
                    object.kind.as_str(),
                    object.id,
                    version,
                    deleted,
                    object.tags,
                    object.lat,
                    object.lon,
                    object.poi
                ])?;
                remove_refs.execute(params![object.kind.as_str(), object.id])?;
                for (position, (kind, id)) in object.refs.into_iter().enumerate() {
                    insert_ref.execute(params![
                        object.kind.as_str(),
                        object.id,
                        position as i64,
                        kind.as_str(),
                        id
                    ])?;
                }
                progress.advance(1);
                Ok(())
            },
        )?;
    }
    transaction.commit()?;
    progress.finish();
    Ok(checksum)
}

fn add_affected_parents(connection: &Connection) -> Result<()> {
    connection.execute_batch(
        "WITH RECURSIVE parents(kind,id) AS (
            SELECT kind,id FROM changes
            UNION
            SELECT refs.kind,refs.id FROM parents
            JOIN refs ON refs.target_kind=parents.kind AND refs.target_id=parents.id
         ) INSERT OR IGNORE INTO affected SELECT kind,id FROM parents;",
    )?;
    Ok(())
}

fn apply_objects(connection: &Connection) -> Result<()> {
    let mut progress = Progress::new("Affected reference graph");
    add_affected_parents(connection)?;
    connection.execute_batch(
        "DELETE FROM refs WHERE (kind,id) IN (SELECT kind,id FROM changes);
         DELETE FROM objects WHERE (kind,id) IN (SELECT kind,id FROM changes WHERE deleted=1);
         INSERT OR REPLACE INTO objects(kind,id,tags,lat,lon,poi)
            SELECT kind,id,tags,lat,lon,poi FROM changes WHERE deleted=0;
         INSERT INTO refs SELECT * FROM changed_refs;",
    )?;
    add_affected_parents(connection)?;
    connection.execute_batch(
        "DELETE FROM bounds WHERE (kind,id) IN (SELECT kind,id FROM affected);
         DELETE FROM validated_geometry WHERE (kind,id) IN (SELECT kind,id FROM affected);",
    )?;
    let missing: Option<(i64, i64)> = connection
        .query_row(
            "SELECT refs.id,refs.target_id FROM affected
         CROSS JOIN refs ON refs.kind='way' AND refs.id=affected.id
         LEFT JOIN objects ON objects.kind='node' AND objects.id=refs.target_id
         WHERE affected.kind='way' AND objects.id IS NULL LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if let Some((way, node)) = missing {
        bail!("Missing reference node/{node} in way/{way}");
    }
    connection.execute_batch(
        "DELETE FROM incomplete_relations WHERE id IN (SELECT id FROM affected WHERE kind='relation');
         WITH RECURSIVE incomplete(id) AS (
            SELECT refs.id FROM affected
            CROSS JOIN refs ON refs.kind='relation' AND refs.id=affected.id
            LEFT JOIN objects ON objects.kind=refs.target_kind AND objects.id=refs.target_id
            WHERE affected.kind='relation' AND objects.id IS NULL
            UNION
            SELECT refs.id FROM affected
            CROSS JOIN refs ON refs.kind='relation' AND refs.id=affected.id
            JOIN incomplete_relations ON refs.target_kind='relation' AND refs.target_id=incomplete_relations.id
            WHERE affected.kind='relation'
            UNION
            SELECT refs.id FROM incomplete
            JOIN refs ON refs.target_kind='relation' AND refs.target_id=incomplete.id
            JOIN affected ON affected.kind=refs.kind AND affected.id=refs.id
            WHERE refs.kind='relation'
         ) INSERT OR IGNORE INTO incomplete_relations SELECT id FROM incomplete;",
    )?;
    progress.advance(
        connection.query_row("SELECT COUNT(*) FROM affected", [], |row| {
            row.get::<_, i64>(0)
        })? as u64,
    );
    progress.finish();
    Ok(())
}

fn refresh_pois(
    connection: &Connection,
    options: &ComputeOptions,
    prior: &Value,
) -> Result<(u64, u64)> {
    let removed = connection.query_row(
        "SELECT COUNT(*) FROM affected CROSS JOIN pois ON pois.id=('osm_'||affected.kind||'_'||affected.id)",
        [], |row| row.get::<_, i64>(0),
    )? as u64;
    let excluded_removed = connection.query_row(
        "SELECT COUNT(*) FROM affected CROSS JOIN exclusions ON exclusions.id=affected.id WHERE affected.kind='relation'",
        [], |row| row.get::<_, i64>(0),
    )? as u64;
    connection.execute_batch(
        "INSERT OR IGNORE INTO dirty_cells SELECT pois.cell FROM affected
         CROSS JOIN pois ON pois.id=('osm_'||affected.kind||'_'||affected.id);
         DELETE FROM pois WHERE id IN (SELECT 'osm_'||kind||'_'||id FROM affected);
         DELETE FROM exclusions WHERE id IN (SELECT id FROM affected WHERE kind='relation');
         DELETE FROM missing_members WHERE relation_id IN (SELECT id FROM affected WHERE kind='relation');",
    )?;
    build::select_pois(connection, true)?;
    build::prepare_geometry(connection)?;
    let excluded_added = build::record_exclusions(connection)?;
    let added = build::populate_pois(connection, options, true)?;
    let count = prior["count"]
        .as_u64()
        .context("Invalid prior POI count")?
        .checked_sub(removed)
        .and_then(|count| count.checked_add(added))
        .context("Inconsistent POI count")?;
    let excluded = prior["excludedIncompleteRelationCount"]
        .as_u64()
        .context("Invalid prior exclusion count")?
        .checked_sub(excluded_removed)
        .and_then(|count| count.checked_add(excluded_added))
        .context("Inconsistent exclusion count")?;
    Ok((count, excluded))
}

pub fn apply_diff(options: &ApplyOptions, compute: &ComputeOptions) -> Result<Value> {
    compute.validate()?;
    let timestamp = normalize_timestamp(&options.timestamp)?;
    ensure!(
        options.sequence <= MAX_ID,
        "Diff sequence must be a nonnegative safe integer"
    );
    let state = state_path(&options.state, false)?;
    let mut connection = build::open_database(&state.join("state.sqlite"), false, compute)?;
    load_control(&connection)?;
    let digest = stage_changes(&mut connection, &options.input, compute)?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    upgrade_packing(&transaction, &state)?;
    let (mut metadata, _, prior) = load_control(&transaction)?;
    let expected = metadata["sourceSequence"]
        .as_u64()
        .context("Invalid current replication sequence")?
        .checked_add(1)
        .context("Replication sequence overflow")?;
    ensure!(
        options.sequence == expected,
        "Expected replication sequence {expected}, received {}",
        options.sequence
    );
    ensure!(
        timestamp.as_str()
            >= metadata["sourceTimestamp"]
                .as_str()
                .context("Invalid current replication timestamp")?,
        "Replication timestamp moved backwards"
    );
    apply_objects(&transaction)?;
    let (count, excluded) = refresh_pois(&transaction, compute, &prior)?;
    let output = state.join("output");
    build::write_cells(&transaction, &output, true)?;
    metadata["sourceSequence"] = json!(options.sequence);
    metadata["sourceTimestamp"] = json!(timestamp);
    metadata["replicationDiffSHA256"] = json!(digest);
    if let Some(coverage) = &options.coverage {
        metadata["coverage"] = read_coverage(coverage)?;
    }
    let receipt = build::write_manifest(&transaction, &output, &metadata, count, excluded)?;
    transaction.execute(
        "UPDATE control SET metadata=?,receipt=? WHERE id=1",
        params![
            String::from_utf8(canonical_json(&metadata)?)?,
            String::from_utf8(canonical_json(&receipt)?)?,
        ],
    )?;
    transaction.commit()?;
    let mut result = export_committed(&connection, &state)?;
    for (key, table) in [
        ("changedObjects", "changes"),
        ("affectedObjects", "affected"),
        ("dirtyCells", "dirty_cells"),
    ] {
        let count = connection.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get::<_, i64>(0)
        })? as u64;
        result[key] = json!(count);
    }
    Ok(result)
}

pub fn status(state: &Path, options: &ComputeOptions) -> Result<Value> {
    let state = state_path(state, false)?;
    let mut connection = build::open_database(&state.join("state.sqlite"), false, options)?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    upgrade_packing(&transaction, &state)?;
    transaction.commit()?;
    let snapshot = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let result = export_committed(&snapshot, &state)?;
    snapshot.commit()?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_common;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    #[test]
    fn affected_refresh_stays_within_vm_budget_with_thirty_thousand_unrelated_objects() {
        let work = test_common::work("incremental-budget-");
        let input = test_common::write_pbf(
            work.path(),
            "input",
            r#"<osm version="0.6"><node id="1" lat="-33.9" lon="151.1"/><node id="2" lat="-33.8" lon="151.2"/><way id="10"><nd ref="1"/><nd ref="2"/><tag k="name" v="Cafe"/><tag k="amenity" v="cafe"/></way><relation id="20"><member type="way" ref="10"/><tag k="name" v="Park"/><tag k="leisure" v="park"/></relation></osm>"#,
            &[],
        );
        let coverage = work.path().join("coverage.json");
        fs::write(&coverage, r#"{"type":"Polygon","coordinates":[[[150,-35],[152,-35],[152,-32],[150,-32],[150,-35]]]}"#).unwrap();
        let options = ComputeOptions {
            workers: 2,
            batch_size: 1000,
            pending_batches: 4,
            batch_bytes: 1 << 20,
            sqlite_cache_mib: 16,
        };
        let state = work.path().join("state");
        let prior = initialize(&InitOptions { input, state: state.clone(), coverage, region: "au-nsw".into(), source_timestamp: "2020-01-01T00:00:00Z".into(), source_sequence: 10, source_sha256: "a".repeat(64), replication_url: "https://download.geofabrik.de/australia-oceania/australia/new-south-wales-updates".into() }, &options).unwrap();
        let mut connection =
            build::open_database(&state.join("state.sqlite"), false, &options).unwrap();
        for (pragma, expected) in [("threads", 4), ("cache_size", -16 * 1024)] {
            assert_eq!(
                connection
                    .pragma_query_value(None, pragma, |row| row.get::<_, i64>(0))
                    .unwrap(),
                expected
            );
        }
        let transaction = connection.transaction().unwrap();
        {
            let mut objects = transaction
                .prepare("INSERT INTO objects VALUES (?, ?, '{}', ?, ?, 0)")
                .unwrap();
            let mut refs = transaction
                .prepare("INSERT INTO refs VALUES (?, ?, 0, ?, ?)")
                .unwrap();
            for id in 1000..11000 {
                for kind in ["node", "way", "relation"] {
                    objects
                        .execute(params![
                            kind,
                            id,
                            (kind == "node").then_some(-33.7),
                            (kind == "node").then_some(151.3)
                        ])
                        .unwrap();
                }
                refs.execute(params!["way", id, "node", id]).unwrap();
                refs.execute(params!["relation", id, "way", id]).unwrap();
            }
        }
        transaction.commit().unwrap();
        let change = work.path().join("change.osc");
        fs::write(&change, r#"<osmChange><modify><node id="1" version="2" lat="-33.5" lon="151.5"/></modify></osmChange>"#).unwrap();
        stage_changes(&mut connection, &change, &options).unwrap();
        let ticks = Arc::new(AtomicUsize::new(0));
        let counter = ticks.clone();
        connection
            .progress_handler(
                1000,
                Some(move || counter.fetch_add(1, Ordering::Relaxed) >= 30),
            )
            .unwrap();
        apply_objects(&connection).unwrap();
        assert_eq!(refresh_pois(&connection, &options, &prior).unwrap(), (2, 0));
        connection
            .progress_handler(0, None::<fn() -> bool>)
            .unwrap();
        assert!(ticks.load(Ordering::Relaxed) <= 30);
        assert_eq!(
            connection
                .query_row("SELECT COUNT(*) FROM affected", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            3
        );
    }
}

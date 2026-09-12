use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use rayon::ThreadPoolBuilder;
use rayon::prelude::*;
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use serde_json::{Map, Value, json};

use crate::compute;
use crate::format::{MAX_BLOCK, MAX_MANIFEST, canonical_json, source_metadata};
use crate::progress::ProgressLine;
use crate::storage::{atomic_file, atomic_write, write_object};

#[derive(Clone, Debug)]
pub struct ComputeOptions {
    pub workers: usize,
    pub batch_size: usize,
    pub pending_batches: usize,
    pub batch_bytes: usize,
    pub sqlite_cache_mib: u64,
}

impl ComputeOptions {
    pub fn validate(&self) -> Result<()> {
        ensure!(self.workers > 0, "OSM_COMPUTE_WORKERS must be positive");
        ensure!(
            self.batch_size > 0,
            "OSM_COMPUTE_BATCH_SIZE must be positive"
        );
        ensure!(
            self.pending_batches > 0,
            "OSM_COMPUTE_PENDING_BATCHES must be positive"
        );
        ensure!(
            self.batch_bytes > 0,
            "OSM_COMPUTE_BATCH_BYTES must be positive"
        );
        ensure!(
            self.sqlite_cache_mib > 0 && self.sqlite_cache_mib <= i64::MAX as u64 / 1024,
            "Invalid OSM_SQLITE_CACHE_MIB"
        );
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct BuildOptions {
    pub input: PathBuf,
    pub coverage: PathBuf,
    pub region: String,
    pub source_timestamp: String,
    pub source_sequence: Option<u64>,
    pub source_sha256: String,
    pub output: PathBuf,
    pub scratch: PathBuf,
}

pub(crate) struct Progress {
    label: &'static str,
    started: Instant,
    reported: Instant,
    count: u64,
    output: ProgressLine,
}

impl Progress {
    pub(crate) fn new(label: &'static str) -> Self {
        let mut output = ProgressLine::default();
        output.render(&format!("[osm] {label}: started"));
        Self {
            label,
            started: Instant::now(),
            reported: Instant::now(),
            count: 0,
            output,
        }
    }

    pub(crate) fn advance(&mut self, count: u64) {
        self.count += count;
        if self.reported.elapsed() >= Duration::from_secs(2) {
            self.output.render(&format!(
                "[osm] {}: {} items, {:.1}s",
                self.label,
                self.count,
                self.started.elapsed().as_secs_f64()
            ));
            self.reported = Instant::now();
        }
    }

    pub(crate) fn finish(mut self) {
        self.output.render(&format!(
            "[osm] {}: {} items, {:.2}s, complete",
            self.label,
            self.count,
            self.started.elapsed().as_secs_f64()
        ));
    }

    fn finish_timing(mut self) {
        self.output.render(&format!(
            "[osm] {}: {:.2}s, complete",
            self.label,
            self.started.elapsed().as_secs_f64()
        ));
    }
}

pub(crate) fn open_database(
    path: &Path,
    create: bool,
    options: &ComputeOptions,
) -> Result<Connection> {
    options.validate()?;
    let temporary = std::env::var_os("SQLITE_TMPDIR")
        .context("SQLITE_TMPDIR must be configured by the OSM entry point")?;
    let temporary = Path::new(&temporary);
    ensure!(
        temporary
            .components()
            .any(|part| part.as_os_str() == ".build"),
        "SQLite temporary files must be under .build"
    );
    fs::create_dir_all(temporary)?;
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let flags = if create {
        flags | OpenFlags::SQLITE_OPEN_CREATE
    } else {
        flags
    };
    let connection = Connection::open_with_flags(path, flags)?;
    connection.pragma_update(
        None,
        "cache_size",
        -(options.sqlite_cache_mib as i64 * 1024),
    )?;
    connection.pragma_update(None, "temp_store", "FILE")?;
    connection.pragma_update(None, "threads", 4)?;
    connection.pragma_update(None, "synchronous", "FULL")?;
    connection.busy_timeout(Duration::from_secs(5))?;
    Ok(connection)
}

pub(crate) fn ingest(
    connection: &mut Connection,
    input: &Path,
    options: &ComputeOptions,
) -> Result<()> {
    connection.execute_batch(
        "CREATE TABLE objects (
            kind TEXT NOT NULL, id INTEGER NOT NULL, tags TEXT NOT NULL,
            lat REAL, lon REAL, poi INTEGER NOT NULL,
            PRIMARY KEY (kind, id)
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
         ) WITHOUT ROWID;",
    )?;
    let transaction = connection.transaction()?;
    let mut progress = Progress::new("PBF decode and object index");
    {
        let mut objects =
            transaction.prepare_cached("INSERT INTO objects VALUES (?, ?, ?, ?, ?, ?)")?;
        let mut refs = transaction.prepare_cached("INSERT INTO refs VALUES (?, ?, ?, ?, ?)")?;
        compute::read_pbf(input, options, |batch| {
            let count = batch.len() as u64;
            for object in batch {
                objects.execute(params![
                    object.kind.as_str(),
                    object.id,
                    object.tags,
                    object.lat,
                    object.lon,
                    object.poi
                ])?;
                for (position, (kind, id)) in object.refs.into_iter().enumerate() {
                    refs.execute(params![
                        object.kind.as_str(),
                        object.id,
                        position as i64,
                        kind.as_str(),
                        id
                    ])?;
                }
            }
            progress.advance(count);
            Ok(())
        })?;
    }
    transaction.commit()?;
    progress.finish();
    Ok(())
}

fn classify_relations(connection: &Connection) -> Result<()> {
    let progress = Progress::new("Reverse reference index");
    connection
        .execute_batch("CREATE INDEX refs_target ON refs(target_kind, target_id, kind, id);")?;
    progress.finish_timing();
    let progress = Progress::new("Way node reference check");
    let missing: Option<(i64, i64)> = connection
        .query_row(
            "SELECT refs.id, refs.target_id FROM refs INDEXED BY refs_target
         LEFT JOIN objects ON objects.kind='node' AND objects.id=refs.target_id
         WHERE refs.kind='way' AND objects.id IS NULL
         ORDER BY refs.target_kind, refs.target_id LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if let Some((way, node)) = missing {
        bail!("Missing reference node/{node} in way/{way}");
    }
    progress.finish_timing();
    let mut progress = Progress::new("Incomplete relation propagation");
    connection.execute_batch(
        "CREATE TABLE incomplete_relations (id INTEGER PRIMARY KEY);
         WITH RECURSIVE incomplete(id) AS (
            SELECT refs.id FROM refs
            LEFT JOIN objects ON objects.kind=refs.target_kind AND objects.id=refs.target_id
            WHERE refs.kind='relation' AND objects.id IS NULL
            UNION
            SELECT refs.id FROM incomplete
            JOIN refs ON refs.target_kind='relation' AND refs.target_id=incomplete.id
            WHERE refs.kind='relation'
         ) INSERT INTO incomplete_relations SELECT id FROM incomplete;
         CREATE TABLE validated_geometry (
            kind TEXT NOT NULL, id INTEGER NOT NULL, PRIMARY KEY (kind, id)
         ) WITHOUT ROWID;",
    )?;
    progress.advance(connection.query_row(
        "SELECT COUNT(*) FROM incomplete_relations",
        [],
        |row| row.get::<_, i64>(0),
    )? as u64);
    progress.finish();
    Ok(())
}

pub(crate) fn create_output_index(connection: &Connection) -> Result<()> {
    connection.execute_batch(
        "CREATE UNIQUE INDEX pois_id ON pois(id);
         CREATE TABLE cell_blocks (cell TEXT PRIMARY KEY, hashes TEXT NOT NULL) WITHOUT ROWID;
         CREATE TABLE exclusions (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
         CREATE TABLE missing_members (
            relation_id INTEGER NOT NULL, owner INTEGER NOT NULL,
            kind TEXT NOT NULL, target_id INTEGER NOT NULL,
            PRIMARY KEY (relation_id, owner, kind, target_id)
         ) WITHOUT ROWID;",
    )?;
    Ok(())
}

pub(crate) fn select_pois(connection: &Connection, affected_only: bool) -> Result<()> {
    connection.execute_batch(
        "CREATE TEMP TABLE selected (
            kind TEXT NOT NULL, id INTEGER NOT NULL, PRIMARY KEY(kind, id)
         ) WITHOUT ROWID;",
    )?;
    let sql = if affected_only {
        "INSERT INTO selected SELECT objects.kind, objects.id FROM affected
         CROSS JOIN objects ON objects.kind=affected.kind AND objects.id=affected.id
         WHERE objects.poi=1"
    } else {
        "INSERT INTO selected SELECT kind, id FROM objects WHERE poi=1"
    };
    connection.execute(sql, [])?;
    Ok(())
}

pub(crate) fn prepare_geometry(connection: &Connection) -> Result<()> {
    let mut progress = Progress::new("Selected node, way and relation geometry");
    connection.execute_batch(
        "CREATE TEMP TABLE needed (
            kind TEXT NOT NULL, id INTEGER NOT NULL, PRIMARY KEY(kind,id)
         ) WITHOUT ROWID;
         WITH RECURSIVE descendants(kind,id) AS (
            SELECT kind,id FROM selected
            UNION
            SELECT refs.target_kind,refs.target_id FROM descendants
            JOIN refs ON refs.kind=descendants.kind AND refs.id=descendants.id
            JOIN objects ON objects.kind=refs.target_kind AND objects.id=refs.target_id
            LEFT JOIN bounds ON bounds.kind=descendants.kind AND bounds.id=descendants.id
            LEFT JOIN incomplete_relations AS incomplete
                ON descendants.kind='relation' AND incomplete.id=descendants.id
            LEFT JOIN validated_geometry AS validated
                ON validated.kind=descendants.kind AND validated.id=descendants.id
            WHERE bounds.kind IS NULL AND NOT (incomplete.id IS NOT NULL AND validated.kind IS NOT NULL)
         ) INSERT INTO needed SELECT kind,id FROM descendants;",
    )?;
    let empty: Option<(String, i64)> = connection.query_row(
        "SELECT needed.kind,needed.id FROM needed
         LEFT JOIN bounds ON bounds.kind=needed.kind AND bounds.id=needed.id
         LEFT JOIN incomplete_relations AS incomplete ON needed.kind='relation' AND incomplete.id=needed.id
         LEFT JOIN validated_geometry AS validated ON validated.kind=needed.kind AND validated.id=needed.id
         WHERE needed.kind!='node' AND bounds.kind IS NULL
           AND NOT (incomplete.id IS NOT NULL AND validated.kind IS NOT NULL)
           AND NOT EXISTS (
               SELECT 1 FROM refs WHERE refs.kind=needed.kind AND refs.id=needed.id
                 AND NOT (refs.target_kind=refs.kind AND refs.target_id=refs.id)
           )
         ORDER BY needed.kind,needed.id LIMIT 1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    ).optional()?;
    if let Some((kind, id)) = empty {
        bail!("No geometry for {kind}/{id}");
    }
    connection.execute_batch(
        "INSERT INTO bounds(kind,id,west,south,east,north)
         SELECT 'way',needed.id,MIN(nodes.lon),MIN(nodes.lat),MAX(nodes.lon),MAX(nodes.lat)
         FROM needed
         CROSS JOIN refs ON refs.kind='way' AND refs.id=needed.id
         JOIN objects AS nodes ON nodes.kind='node' AND nodes.id=refs.target_id
         LEFT JOIN bounds ON bounds.kind='way' AND bounds.id=needed.id
         WHERE needed.kind='way' AND bounds.id IS NULL GROUP BY needed.id;
         CREATE TEMP TABLE relation_work (id INTEGER PRIMARY KEY, remaining INTEGER NOT NULL);
         INSERT INTO relation_work
         SELECT needed.id,0 FROM needed
         LEFT JOIN bounds ON bounds.kind='relation' AND bounds.id=needed.id
         LEFT JOIN incomplete_relations AS incomplete ON incomplete.id=needed.id
         LEFT JOIN validated_geometry AS validated ON validated.kind='relation' AND validated.id=needed.id
         WHERE needed.kind='relation' AND bounds.id IS NULL
           AND NOT (incomplete.id IS NOT NULL AND validated.id IS NOT NULL);
         UPDATE relation_work SET remaining=(
            SELECT COUNT(DISTINCT refs.target_id) FROM refs
            JOIN relation_work AS child ON child.id=refs.target_id
            WHERE refs.kind='relation' AND refs.id=relation_work.id AND refs.target_kind='relation'
              AND refs.target_id!=refs.id
         );
         CREATE INDEX relation_work_remaining ON relation_work(remaining);
         CREATE TEMP TABLE relation_ready (id INTEGER PRIMARY KEY);",
    )?;
    loop {
        connection.execute(
            "INSERT INTO relation_ready SELECT id FROM relation_work WHERE remaining=0",
            [],
        )?;
        let ready = connection.changes();
        if ready == 0 {
            let cycle: Option<i64> = connection
                .query_row(
                    "SELECT id FROM relation_work ORDER BY id LIMIT 1",
                    [],
                    |row| row.get(0),
                )
                .optional()?;
            if let Some(id) = cycle {
                bail!("Relation reference cycle at relation/{id}");
            }
            break;
        }
        connection.execute_batch(
            "INSERT INTO bounds(kind,id,west,south,east,north)
             SELECT 'relation',relation_ready.id,
                MIN(CASE WHEN refs.target_kind='node' THEN objects.lon ELSE child.west END),
                MIN(CASE WHEN refs.target_kind='node' THEN objects.lat ELSE child.south END),
                MAX(CASE WHEN refs.target_kind='node' THEN objects.lon ELSE child.east END),
                MAX(CASE WHEN refs.target_kind='node' THEN objects.lat ELSE child.north END)
             FROM relation_ready
             CROSS JOIN refs ON refs.kind='relation' AND refs.id=relation_ready.id
             JOIN objects ON objects.kind=refs.target_kind AND objects.id=refs.target_id
             LEFT JOIN bounds AS child ON child.kind=refs.target_kind AND child.id=refs.target_id
             WHERE (refs.target_kind!='relation' OR refs.target_id!=relation_ready.id)
               AND NOT EXISTS (SELECT 1 FROM incomplete_relations WHERE id=relation_ready.id)
             GROUP BY relation_ready.id;
             INSERT OR IGNORE INTO validated_geometry SELECT 'relation',id FROM relation_ready;
             UPDATE relation_work SET remaining=remaining-(
                SELECT COUNT(DISTINCT refs.target_id) FROM refs
                JOIN relation_ready ON relation_ready.id=refs.target_id
                WHERE refs.kind='relation' AND refs.id=relation_work.id AND refs.target_kind='relation'
                  AND refs.target_id!=refs.id
             ) WHERE id IN (
                SELECT refs.id FROM relation_ready
                CROSS JOIN refs ON refs.target_kind='relation' AND refs.target_id=relation_ready.id
                WHERE refs.kind='relation'
             );
             DELETE FROM relation_work WHERE id IN (SELECT id FROM relation_ready);
             DELETE FROM relation_ready;",
        )?;
        progress.advance(ready);
    }
    progress.finish();
    Ok(())
}

fn relation_name(tags: &str) -> Result<String> {
    let tags: BTreeMap<String, String> = serde_json::from_str(tags)?;
    for key in ["name:zh", "name", "name:en", "int_name"] {
        if let Some(value) = tags.get(key).filter(|value| !value.trim().is_empty()) {
            return Ok(value.trim().to_owned());
        }
    }
    for (key, value) in &tags {
        if crate::format::has_name(&BTreeMap::from([(key.clone(), value.clone())])) {
            return Ok(value.trim().to_owned());
        }
    }
    bail!("POI relation has no name")
}

pub(crate) fn record_exclusions(connection: &Connection) -> Result<u64> {
    let mut progress = Progress::new("Excluded relation diagnostics");
    let mut candidates = connection.prepare(
        "SELECT objects.id,objects.tags FROM selected
         CROSS JOIN objects ON objects.kind='relation' AND objects.id=selected.id
         JOIN incomplete_relations ON incomplete_relations.id=objects.id
         WHERE selected.kind='relation' ORDER BY objects.id",
    )?;
    let mut candidates = candidates.query([])?;
    let mut missing = connection.prepare(
        "WITH RECURSIVE descendants(id) AS (
            VALUES (?)
            UNION
            SELECT refs.target_id FROM descendants
            CROSS JOIN refs ON refs.kind='relation' AND refs.id=descendants.id
            JOIN objects ON objects.kind='relation' AND objects.id=refs.target_id
            WHERE refs.target_kind='relation'
         ) SELECT DISTINCT refs.id,refs.target_kind,refs.target_id
         FROM descendants CROSS JOIN refs ON refs.kind='relation' AND refs.id=descendants.id
         LEFT JOIN objects ON objects.kind=refs.target_kind AND objects.id=refs.target_id
         WHERE objects.id IS NULL ORDER BY refs.id,refs.target_kind,refs.target_id",
    )?;
    let mut insert_exclusion = connection.prepare("INSERT INTO exclusions VALUES (?,?)")?;
    let mut insert_missing = connection.prepare("INSERT INTO missing_members VALUES (?,?,?,?)")?;
    let mut count = 0;
    while let Some(candidate) = candidates.next()? {
        let id: i64 = candidate.get(0)?;
        let tags: String = candidate.get(1)?;
        insert_exclusion.execute(params![id, relation_name(&tags)?])?;
        let mut members = missing.query([id])?;
        while let Some(member) = members.next()? {
            insert_missing.execute(params![
                id,
                member.get::<_, i64>(0)?,
                member.get::<_, String>(1)?,
                member.get::<_, i64>(2)?
            ])?;
        }
        count += 1;
        progress.advance(1);
    }
    progress.finish();
    Ok(count)
}

struct PoiInput {
    kind: String,
    id: i64,
    tags: String,
    west: f64,
    south: f64,
    east: f64,
    north: f64,
}

struct PoiOutput {
    cell: String,
    id: String,
    payload: Vec<u8>,
}

fn encode_poi(input: PoiInput) -> Result<PoiOutput> {
    let latitude = (input.south + input.north) / 2.0;
    let mut longitude = (input.west + input.east) / 2.0;
    if longitude == 180.0 {
        longitude = -180.0;
    }
    let x = 35999i64.min(((longitude + 180.0) * 100.0).floor() as i64);
    let y = 17999i64.min(((latitude + 90.0) * 100.0).floor() as i64);
    let id = format!("osm_{}_{}", input.kind, input.id);
    let payload = canonical_json(
        &json!({"id": id, "lat": latitude, "lon": longitude, "tags": serde_json::from_str::<Value>(&input.tags)?}),
    )?;
    ensure!(
        payload.len() + 2 <= MAX_BLOCK,
        "POI exceeds block limit: {id}"
    );
    Ok(PoiOutput {
        cell: format!("{y}_{x}"),
        id,
        payload,
    })
}

pub(crate) fn populate_pois(
    connection: &Connection,
    options: &ComputeOptions,
    mark_dirty: bool,
) -> Result<u64> {
    let mut progress = Progress::new("POI encoding and cell index");
    let pool = ThreadPoolBuilder::new()
        .num_threads(options.workers)
        .build()?;
    let mut statement = connection.prepare(
        "SELECT objects.kind,objects.id,objects.tags,
            CASE WHEN objects.kind='node' THEN objects.lon ELSE bounds.west END,
            CASE WHEN objects.kind='node' THEN objects.lat ELSE bounds.south END,
            CASE WHEN objects.kind='node' THEN objects.lon ELSE bounds.east END,
            CASE WHEN objects.kind='node' THEN objects.lat ELSE bounds.north END
         FROM selected
         CROSS JOIN objects ON objects.kind=selected.kind AND objects.id=selected.id
         LEFT JOIN bounds ON bounds.kind=objects.kind AND bounds.id=objects.id
         LEFT JOIN incomplete_relations ON objects.kind='relation' AND incomplete_relations.id=objects.id
         WHERE incomplete_relations.id IS NULL ORDER BY selected.kind,selected.id",
    )?;
    let mut rows = statement.query([])?;
    let mut insert = connection.prepare("INSERT INTO pois VALUES (?,?,?)")?;
    let mut batch = Vec::new();
    let mut bytes = 0usize;
    let mut count = 0u64;
    let mut flush = |batch: Vec<PoiInput>| -> Result<()> {
        let encoded: Result<Vec<_>> =
            pool.install(|| batch.into_par_iter().map(encode_poi).collect());
        for output in encoded? {
            insert.execute(params![output.cell, output.id, output.payload])?;
            if mark_dirty {
                connection.execute(
                    "INSERT OR IGNORE INTO dirty_cells VALUES (?)",
                    [&output.cell],
                )?;
            }
            count += 1;
            progress.advance(1);
        }
        Ok(())
    };
    while let Some(row) = rows.next()? {
        let input = PoiInput {
            kind: row.get(0)?,
            id: row.get(1)?,
            tags: row.get(2)?,
            west: row.get(3)?,
            south: row.get(4)?,
            east: row.get(5)?,
            north: row.get(6)?,
        };
        if !batch.is_empty()
            && (batch.len() >= options.batch_size
                || bytes.saturating_add(input.tags.len()) > options.batch_bytes)
        {
            flush(std::mem::take(&mut batch))?;
            bytes = 0;
        }
        bytes = bytes.saturating_add(input.tags.len());
        batch.push(input);
    }
    if !batch.is_empty() {
        flush(batch)?;
    }
    progress.finish();
    Ok(count)
}

pub(crate) fn write_diagnostics(connection: &Connection, output: &Path) -> Result<()> {
    atomic_file(&output.join("excluded-relations.jsonl"), |stream| {
        let mut statement = connection.prepare("SELECT id,name FROM exclusions ORDER BY id")?;
        let mut rows = statement.query([])?;
        let mut missing = connection.prepare("SELECT owner,kind,target_id FROM missing_members WHERE relation_id=? ORDER BY owner,kind,target_id")?;
        while let Some(row) = rows.next()? {
            let id: i64 = row.get(0)?;
            let header = canonical_json(
                &json!({"id": format!("osm_relation_{id}"), "name": row.get::<_, String>(1)?}),
            )?;
            stream.write_all(&header[..header.len() - 1])?;
            stream.write_all(b",\"missingMembers\":[")?;
            let mut members = missing.query([id])?;
            let mut first = true;
            while let Some(member) = members.next()? {
                if !first {
                    stream.write_all(b",")?;
                }
                first = false;
                stream.write_all(&canonical_json(&json!({
                    "relation": format!("osm_relation_{}", member.get::<_, i64>(0)?),
                    "type": member.get::<_, String>(1)?, "id": member.get::<_, i64>(2)?,
                }))?)?;
            }
            stream.write_all(b"]}\n")?;
        }
        Ok(())
    })
}

pub(crate) fn write_cells(connection: &Connection, output: &Path, dirty_only: bool) -> Result<()> {
    let mut progress = Progress::new("Immutable POI blocks");
    if dirty_only {
        connection.execute(
            "DELETE FROM cell_blocks WHERE cell IN (SELECT cell FROM dirty_cells)",
            [],
        )?;
    }
    let sql = if dirty_only {
        "SELECT pois.cell,pois.payload FROM dirty_cells
         CROSS JOIN pois ON pois.cell=dirty_cells.cell ORDER BY dirty_cells.cell,pois.id"
    } else {
        "SELECT cell,payload FROM pois ORDER BY cell,id"
    };
    let mut statement = connection.prepare(sql)?;
    let mut rows = statement.query([])?;
    let mut insert = connection.prepare("INSERT INTO cell_blocks VALUES (?,?)")?;
    let mut cell: Option<String> = None;
    let mut hashes: Vec<String> = Vec::new();
    let mut page = Vec::new();
    page.push(b'[');
    let mut page_count = 0usize;
    let mut flush_page =
        |page: &mut Vec<u8>, page_count: &mut usize, hashes: &mut Vec<String>| -> Result<()> {
            if *page_count != 0 {
                page.push(b']');
                hashes.push(write_object(output, "blocks", page)?);
                ensure!(
                    hashes.len() <= MAX_MANIFEST / 67,
                    "Region manifest exceeds 8 MiB; split the region"
                );
                progress.advance(1);
                page.clear();
                page.push(b'[');
                *page_count = 0;
            }
            Ok(())
        };
    while let Some(row) = rows.next()? {
        let next_cell: String = row.get(0)?;
        let payload: Vec<u8> = row.get(1)?;
        ensure!(payload.len() + 2 <= MAX_BLOCK, "POI exceeds block limit");
        if cell.as_deref() != Some(&next_cell) {
            flush_page(&mut page, &mut page_count, &mut hashes)?;
            if let Some(cell) = &cell {
                insert.execute(params![
                    cell,
                    String::from_utf8(canonical_json(&json!(hashes))?)?
                ])?;
            }
            hashes.clear();
            cell = Some(next_cell);
        }
        if page.len() + payload.len() + usize::from(page_count > 0) + 1 > MAX_BLOCK {
            flush_page(&mut page, &mut page_count, &mut hashes)?;
        }
        if page_count > 0 {
            page.push(b',');
        }
        page.extend_from_slice(&payload);
        page_count += 1;
    }
    flush_page(&mut page, &mut page_count, &mut hashes)?;
    if let Some(cell) = cell {
        insert.execute(params![
            cell,
            String::from_utf8(canonical_json(&json!(hashes))?)?
        ])?;
    }
    progress.finish();
    Ok(())
}

pub(crate) fn write_manifest(
    connection: &Connection,
    output: &Path,
    metadata: &Value,
    count: u64,
    excluded: u64,
) -> Result<Value> {
    let mut manifest = metadata
        .as_object()
        .context("Invalid source metadata")?
        .clone();
    manifest.insert("schema".to_owned(), json!(1));
    manifest.insert("count".to_owned(), json!(count));
    manifest.insert(
        "excludedIncompleteRelationCount".to_owned(),
        json!(excluded),
    );
    manifest.insert("cells".to_owned(), json!({}));
    let mut size = canonical_json(&Value::Object(manifest.clone()))?.len();
    ensure!(
        size <= MAX_MANIFEST,
        "Region manifest exceeds 8 MiB; split the region"
    );
    let mut cells = Map::new();
    let mut statement = connection.prepare("SELECT cell,hashes FROM cell_blocks ORDER BY cell")?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let cell: String = row.get(0)?;
        let hashes: Value = serde_json::from_str(&row.get::<_, String>(1)?)?;
        size += canonical_json(&json!(&cell))?.len()
            + 1
            + canonical_json(&hashes)?.len()
            + usize::from(!cells.is_empty());
        ensure!(
            size <= MAX_MANIFEST,
            "Region manifest exceeds 8 MiB; split the region"
        );
        cells.insert(cell, hashes);
    }
    manifest.insert("cells".to_owned(), Value::Object(cells));
    let payload = canonical_json(&Value::Object(manifest))?;
    ensure!(
        payload.len() == size && payload.len() <= MAX_MANIFEST,
        "Manifest size accounting mismatch"
    );
    let digest = write_object(output, "manifests", &payload)?;
    Ok(json!({"region": metadata["region"], "manifest": digest,
        "sourceTimestamp": metadata["sourceTimestamp"], "count": count,
        "excludedIncompleteRelationCount": excluded}))
}

pub(crate) fn compile_database(
    connection: &Connection,
    output: &Path,
    metadata: &Value,
    options: &ComputeOptions,
) -> Result<Value> {
    classify_relations(connection)?;
    create_output_index(connection)?;
    select_pois(connection, false)?;
    prepare_geometry(connection)?;
    let excluded = record_exclusions(connection)?;
    let count = populate_pois(connection, options, false)?;
    write_cells(connection, output, false)?;
    let receipt = write_manifest(connection, output, metadata, count, excluded)?;
    write_diagnostics(connection, output)?;
    Ok(receipt)
}

pub fn run(options: &BuildOptions, compute: &ComputeOptions) -> Result<Value> {
    compute.validate()?;
    let metadata = source_metadata(
        &options.coverage,
        &options.region,
        &options.source_timestamp,
        options.source_sequence,
        &options.source_sha256,
    )?;
    ensure!(
        options
            .scratch
            .components()
            .any(|part| part.as_os_str() == ".build"),
        "Scratch directory must be under a .build directory"
    );
    ensure!(
        !options.output.join("release.json").exists(),
        "Output already contains a release; use a new output directory"
    );
    fs::create_dir_all(&options.output)?;
    fs::create_dir_all(&options.scratch)?;
    let scratch = tempfile::Builder::new()
        .prefix("compile-")
        .tempdir_in(&options.scratch)?;
    let mut connection = open_database(&scratch.path().join("objects.sqlite"), true, compute)?;
    ingest(&mut connection, &options.input, compute)?;
    let transaction = connection.transaction()?;
    let receipt = compile_database(&transaction, &options.output, &metadata, compute)?;
    transaction.commit()?;
    atomic_write(
        &options.output.join("release.json"),
        &canonical_json(&receipt)?,
    )?;
    connection.close().map_err(|(_, error)| error)?;
    Ok(receipt)
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
    fn exclusion_diagnostics_do_not_scan_unrelated_relation_references() {
        let work = test_common::work("exclusion-budget-");
        let options = ComputeOptions {
            workers: 4,
            batch_size: 4096,
            pending_batches: 4,
            batch_bytes: 4 << 20,
            sqlite_cache_mib: 16,
        };
        let input = test_common::write_pbf(
            work.path(),
            "input",
            r#"<osm version="0.6"><node id="1" lat="-33" lon="151"/><way id="10"><nd ref="1"/></way><relation id="20"><member type="relation" ref="21"/><tag k="name" v="First park"/><tag k="leisure" v="park"/></relation><relation id="21"><member type="relation" ref="22"/></relation><relation id="22"><member type="way" ref="999"/></relation><relation id="30"><member type="relation" ref="21"/><tag k="name" v="Second park"/><tag k="leisure" v="park"/></relation></osm>"#,
            &[],
        );
        for unrelated in [0, 30_000] {
            let mut connection = open_database(
                &work.path().join(format!("state-{unrelated}.sqlite")),
                true,
                &options,
            )
            .unwrap();
            ingest(&mut connection, &input, &options).unwrap();
            let transaction = connection.transaction().unwrap();
            {
                let mut objects = transaction
                    .prepare("INSERT INTO objects VALUES ('relation',?,'{}',NULL,NULL,0)")
                    .unwrap();
                let mut refs = transaction
                    .prepare("INSERT INTO refs VALUES ('relation',?,0,'relation',?)")
                    .unwrap();
                for id in 1000..1000 + unrelated {
                    objects.execute([id]).unwrap();
                    refs.execute(params![id, id]).unwrap();
                }
            }
            classify_relations(&transaction).unwrap();
            create_output_index(&transaction).unwrap();
            select_pois(&transaction, false).unwrap();
            prepare_geometry(&transaction).unwrap();
            let ticks = Arc::new(AtomicUsize::new(0));
            let counter = ticks.clone();
            transaction
                .progress_handler(
                    1000,
                    Some(move || counter.fetch_add(1, Ordering::Relaxed) >= 100),
                )
                .unwrap();
            let started = Instant::now();
            let result = record_exclusions(&transaction);
            let elapsed = started.elapsed();
            transaction
                .progress_handler(0, None::<fn() -> bool>)
                .unwrap();
            eprintln!(
                "SQLite {}: {unrelated} unrelated references, {} VM ticks, {elapsed:?}, {result:?}",
                rusqlite::version(),
                ticks.load(Ordering::Relaxed)
            );
            assert_eq!(
                result.expect("Exclusion diagnostics exceeded the SQLite work budget"),
                2
            );
            let mut members = transaction
                .prepare("SELECT relation_id,owner,kind,target_id FROM missing_members ORDER BY relation_id")
                .unwrap();
            let actual = members
                .query_map([], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap();
            assert_eq!(
                actual,
                [(20, 22, "way".into(), 999), (30, 22, "way".into(), 999)]
            );
        }
    }
}

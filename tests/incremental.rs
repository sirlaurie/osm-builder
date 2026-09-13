mod common;

use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::PathBuf;

use aura_osm::build::{self, BuildOptions, ComputeOptions};
use aura_osm::format::hash_bytes;
use aura_osm::incremental::{self, ApplyOptions, InitOptions};
use flate2::{Compression, write::GzEncoder};
use rusqlite::Connection;
use serde_json::{Value, json};

const SNAPSHOT: &str = r#"<osm version="0.6">
  <node id="1" lat="-33.9" lon="151.1"><tag k="name" v="First Cafe"/><tag k="amenity" v="cafe"/></node>
  <node id="2" lat="-33.8" lon="151.2"/>
  <node id="50" lat="-34.5" lon="150.5"><tag k="name" v="Unchanged Shop"/><tag k="shop" v="books"/></node>
  <node id="100" lat="-33.7" lon="151.3"><tag k="building" v="yes"/><tag k="addr:street" v="Example Street"/></node>
  <node id="101" lat="-33.6" lon="151.4"/>
  <way id="10"><nd ref="1"/><nd ref="2"/><tag k="name" v="Way Cafe"/><tag k="amenity" v="cafe"/></way>
  <relation id="20"><member type="way" ref="10"/><tag k="name" v="Museum"/><tag k="tourism" v="museum"/></relation>
  <relation id="306382"><member type="way" ref="972806795"/><tag k="name" v="Partial Park"/><tag k="leisure" v="nature_reserve"/></relation>
</osm>"#;

struct Fixture {
    work: tempfile::TempDir,
    state: PathBuf,
    init: InitOptions,
    compute: ComputeOptions,
}

impl Fixture {
    fn new() -> Self {
        let work = common::work("incremental-");
        let state = work.path().join("state");
        let input = common::write_pbf(work.path(), "source", SNAPSHOT, &[]);
        let coverage = work.path().join("coverage.json");
        fs::write(&coverage, serde_json::to_vec(&json!({"type":"Polygon","coordinates":[[[150,-35],[152,-35],[152,-32],[150,-32],[150,-35]]]})).unwrap()).unwrap();
        let init = InitOptions {
            source_sha256: hash_bytes(&fs::read(&input).unwrap()),
            input,
            state: state.clone(),
            coverage,
            region: "au-nsw".into(),
            source_timestamp: "2020-01-01T00:00:00Z".into(),
            source_sequence: 10,
            replication_url:
                "https://download.geofabrik.de/australia-oceania/australia/new-south-wales-updates"
                    .into(),
        };
        let compute = ComputeOptions {
            workers: 2,
            batch_size: 1,
            pending_batches: 8,
            batch_bytes: 1 << 20,
            sqlite_cache_mib: 16,
        };
        incremental::initialize(&init, &compute).unwrap();
        Self {
            work,
            state,
            init,
            compute,
        }
    }

    fn options(&self, body: &str, sequence: u64) -> ApplyOptions {
        let input = self.work.path().join(format!("change-{sequence}.osc.gz"));
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        write!(encoder, "<osmChange version=\"0.6\">{body}</osmChange>").unwrap();
        fs::write(&input, encoder.finish().unwrap()).unwrap();
        ApplyOptions {
            input,
            state: self.state.clone(),
            sequence,
            timestamp: format!("2020-01-{:02}T00:00:00Z", sequence - 9),
            coverage: None,
        }
    }

    fn change(&self, body: &str, sequence: u64) -> anyhow::Result<Value> {
        incremental::apply_diff(&self.options(body, sequence), &self.compute)
    }

    fn status(&self) -> Value {
        incremental::status(&self.state, &self.compute).unwrap()
    }

    fn snapshot(&self) -> (Value, BTreeMap<String, Value>) {
        let status = self.status();
        let output = self.state.join("output");
        let digest = status["manifest"].as_str().unwrap();
        let bytes = fs::read(output.join(format!("manifests/{digest}.json"))).unwrap();
        assert_eq!(hash_bytes(&bytes), digest);
        let manifest: Value = serde_json::from_slice(&bytes).unwrap();
        let mut records = BTreeMap::new();
        for hashes in manifest["cells"].as_object().unwrap().values() {
            for page in hashes.as_array().unwrap() {
                let digest = page[0].as_str().unwrap();
                let pack = manifest["packs"][page[1].as_u64().unwrap() as usize]
                    .as_str()
                    .unwrap();
                let packed = fs::read(output.join(format!("packs/{pack}.bin"))).unwrap();
                assert_eq!(hash_bytes(&packed), pack);
                let offset = page[2].as_u64().unwrap() as usize;
                let length = page[3].as_u64().unwrap() as usize;
                let bytes = &packed[offset..offset + length];
                assert_eq!(
                    bytes,
                    fs::read(output.join(format!("blocks/{digest}.json"))).unwrap()
                );
                assert_eq!(hash_bytes(bytes), digest);
                for record in serde_json::from_slice::<Vec<Value>>(bytes).unwrap() {
                    assert!(
                        records
                            .insert(record["id"].as_str().unwrap().into(), record)
                            .is_none()
                    );
                }
            }
        }
        assert_eq!(records.len() as u64, manifest["count"].as_u64().unwrap());
        aura_osm::format::validate_manifest(&manifest).unwrap();
        (manifest, records)
    }

    fn database(&self) -> Connection {
        Connection::open(self.state.join("state.sqlite")).unwrap()
    }

    fn legacy_state(&self) -> (Value, Value) {
        let (packed, _) = self.snapshot();
        let mut legacy = packed.clone();
        legacy["schema"] = json!(1);
        legacy.as_object_mut().unwrap().remove("packs");
        for pages in legacy["cells"].as_object_mut().unwrap().values_mut() {
            *pages = json!(
                pages
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|page| page[0].clone())
                    .collect::<Vec<_>>()
            );
        }
        let digest = aura_osm::storage::write_object(
            &self.state.join("output"),
            "manifests",
            &aura_osm::format::canonical_json(&legacy).unwrap(),
        )
        .unwrap();
        let connection = self.database();
        let stored: String = connection
            .query_row("SELECT receipt FROM control WHERE id=1", [], |row| {
                row.get(0)
            })
            .unwrap();
        let mut receipt: Value = serde_json::from_str(&stored).unwrap();
        receipt["manifest"] = json!(digest);
        for field in ["blockCount", "packCount", "packBytes"] {
            receipt.as_object_mut().unwrap().remove(field);
        }
        connection
            .execute(
                "UPDATE control SET receipt=? WHERE id=1",
                [serde_json::to_string(&receipt).unwrap()],
            )
            .unwrap();
        connection
            .execute_batch(
                "DROP INDEX cell_blocks_group; DROP TABLE packed_groups; PRAGMA user_version=1;",
            )
            .unwrap();
        fs::write(
            self.state.join("output/release.json"),
            aura_osm::format::canonical_json(&receipt).unwrap(),
        )
        .unwrap();
        (packed, receipt)
    }
}

#[test]
fn legacy_status_upgrades_packing_without_a_pbf_and_recovers_the_committed_export() {
    let fixture = Fixture::new();
    let (packed, legacy_receipt) = fixture.legacy_state();
    fs::write(
        &fixture.init.input,
        b"PBF is not available during index migration",
    )
    .unwrap();
    let upgraded = fixture.status();
    let (manifest, records) = fixture.snapshot();
    assert_eq!(manifest, packed);
    assert_eq!(upgraded["sequence"], 10);
    assert_eq!(records.len(), 4);
    assert_eq!(
        fixture
            .database()
            .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
            .unwrap(),
        2
    );
    fs::write(
        fixture.state.join("output/release.json"),
        serde_json::to_vec(&legacy_receipt).unwrap(),
    )
    .unwrap();
    assert_eq!(fixture.status()["manifest"], upgraded["manifest"]);
    let restored: Value =
        serde_json::from_slice(&fs::read(fixture.state.join("output/release.json")).unwrap())
            .unwrap();
    assert_eq!(restored["manifest"], upgraded["manifest"]);
}

#[test]
fn failed_pack_migration_rolls_back_schema_and_preserves_the_published_local_receipt() {
    let fixture = Fixture::new();
    let (packed, legacy_receipt) = fixture.legacy_state();
    let hash = packed["cells"]
        .as_object()
        .unwrap()
        .values()
        .next()
        .unwrap()[0][0]
        .as_str()
        .unwrap();
    let block = fixture.state.join(format!("output/blocks/{hash}.json"));
    let bytes = fs::read(&block).unwrap();
    fs::write(&block, b"corrupt block").unwrap();
    assert!(incremental::status(&fixture.state, &fixture.compute).is_err());
    let connection = fixture.database();
    assert_eq!(
        connection
            .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE name='packed_groups'",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
    let release: Value =
        serde_json::from_slice(&fs::read(fixture.state.join("output/release.json")).unwrap())
            .unwrap();
    assert_eq!(release, legacy_receipt);
    fs::write(block, bytes).unwrap();
    assert_eq!(fixture.snapshot().0, packed);
}

#[test]
fn applying_a_diff_upgrades_legacy_packing_in_the_same_transaction() {
    let fixture = Fixture::new();
    fixture.legacy_state();
    let changed = fixture.change("", 11).unwrap();
    assert_eq!(changed["sequence"], 11);
    assert_eq!(fixture.snapshot().0["schema"], 2);
    assert_eq!(
        fixture
            .database()
            .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
            .unwrap(),
        2
    );
}

#[test]
fn pending_cleanup_preserves_legacy_receipt_and_blocks_status_and_diff_migration() {
    let fixture = Fixture::new();
    let (_, legacy_receipt) = fixture.legacy_state();
    let marker = fixture.work.path().join(".cleanup-au-nsw.json");
    let record = serde_json::to_vec(
        &json!({"region":"au-nsw", "manifest":legacy_receipt["manifest"], "downloads":[]}),
    )
    .unwrap();
    fs::write(&marker, &record).unwrap();
    let release_path = fixture.state.join("output/release.json");
    let release = fs::read(&release_path).unwrap();
    for error in [
        incremental::status(&fixture.state, &fixture.compute).unwrap_err(),
        fixture.change("", 11).unwrap_err(),
    ] {
        assert!(error.to_string().contains("resume work --cleanup"));
    }
    assert_eq!(fs::read(marker).unwrap(), record);
    assert_eq!(fs::read(release_path).unwrap(), release);
    let connection = fixture.database();
    assert_eq!(
        connection
            .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE name='packed_groups'",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
}

#[test]
fn dirty_group_repacking_leaves_other_groups_untouched_and_removes_empty_groups() {
    let fixture = Fixture::new();
    let connection = fixture.database();
    let groups = |connection: &Connection| {
        connection
            .prepare("SELECT group_id,block_references FROM packed_groups ORDER BY group_id")
            .unwrap()
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .unwrap()
            .collect::<rusqlite::Result<BTreeMap<_, _>>>()
            .unwrap()
    };
    let before = groups(&connection);
    let target = "346_2065";
    assert!(before.contains_key(target));
    connection.execute_batch(&format!("CREATE TRIGGER protect_other_groups BEFORE DELETE ON packed_groups WHEN OLD.group_id!='{target}' BEGIN SELECT RAISE(ABORT,'unrelated group was repacked'); END;")).unwrap();
    fixture.change(r#"<modify><node id="50" version="2" lat="-34.5" lon="150.5"><tag k="name" v="Renamed Shop"/><tag k="shop" v="books"/></node></modify>"#, 11).unwrap();
    let after = groups(&connection);
    assert_ne!(before[target], after[target]);
    for (group, references) in &before {
        if group != target {
            assert_eq!(&after[group], references);
        }
    }
    fixture
        .change(r#"<delete><node id="50" version="3"/></delete>"#, 12)
        .unwrap();
    assert!(!groups(&connection).contains_key(target));
    assert!(!fixture.snapshot().1.contains_key("osm_node_50"));
}

#[test]
fn initialization_matches_full_build_and_retains_geometry_without_unselected_tags() {
    let fixture = Fixture::new();
    let status = fixture.status();
    assert_eq!(status["sequence"], 10);
    assert_eq!(status["count"], 4);
    assert_eq!(status["excludedIncompleteRelationCount"], 1);
    let fresh = build::run(
        &BuildOptions {
            input: fixture.init.input.clone(),
            coverage: fixture.init.coverage.clone(),
            region: fixture.init.region.clone(),
            source_timestamp: fixture.init.source_timestamp.clone(),
            source_sequence: Some(10),
            source_sha256: fixture.init.source_sha256.clone(),
            output: fixture.work.path().join("fresh"),
            scratch: fixture.work.path().join("scratch"),
        },
        &fixture.compute,
    )
    .unwrap();
    assert_eq!(status["manifest"], fresh["manifest"]);
    let stored: (String, f64) = fixture
        .database()
        .query_row(
            "SELECT tags,lat FROM objects WHERE kind='node' AND id=100",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(stored, ("{}".into(), -33.7));
}

#[test]
fn node_move_propagates_to_parents_and_preserves_unaffected_block_files() {
    let fixture = Fixture::new();
    let (before, before_pois) = fixture.snapshot();
    let block_dir = fixture.state.join("output/blocks");
    let stable_cell = before["cells"]
        .as_object()
        .unwrap()
        .iter()
        .find(|(_, hashes)| {
            hashes.as_array().unwrap().iter().any(|hash| {
                fs::read_to_string(block_dir.join(format!("{}.json", hash[0].as_str().unwrap())))
                    .unwrap()
                    .contains("osm_node_50")
            })
        })
        .unwrap()
        .0
        .clone();
    let stable_files: Vec<_> = before["cells"][&stable_cell]
        .as_array()
        .unwrap()
        .iter()
        .map(|hash| {
            let path = block_dir.join(format!("{}.json", hash[0].as_str().unwrap()));
            let modified = fs::metadata(&path).unwrap().modified().unwrap();
            (path, modified)
        })
        .collect();
    let result = fixture.change(r#"<modify><node id="1" version="2" lat="-33.5" lon="151.5"><tag k="name" v="First Cafe"/><tag k="amenity" v="cafe"/></node></modify>"#, 11).unwrap();
    let (after, pois) = fixture.snapshot();
    assert_eq!(result["changedObjects"], 1);
    assert_eq!(result["affectedObjects"], 3);
    assert_eq!(pois["osm_way_10"]["lat"], (-33.5 - 33.8) / 2.0);
    assert_eq!(pois["osm_relation_20"]["lon"], (151.5 + 151.2) / 2.0);
    assert_eq!(pois["osm_node_50"], before_pois["osm_node_50"]);
    let references = |manifest: &Value| {
        manifest["cells"][&stable_cell]
            .as_array()
            .unwrap()
            .iter()
            .map(|page| {
                json!([
                    page[0],
                    manifest["packs"][page[1].as_u64().unwrap() as usize],
                    page[2],
                    page[3]
                ])
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(references(&before), references(&after));
    for (path, modified) in stable_files {
        assert_eq!(fs::metadata(path).unwrap().modified().unwrap(), modified);
    }
}

#[test]
fn new_tags_reuse_stored_references_and_partial_relations_enter_and_leave_output() {
    let fixture = Fixture::new();
    let result = fixture.change(r#"<create><way id="972806795" version="1"><nd ref="100"/><nd ref="101"/><tag k="building" v="yes"/></way></create>
      <modify><node id="100" version="2" lat="-33.7" lon="151.3"><tag k="name:ja" v="喫茶店"/><tag k="amenity" v="cafe"/></node></modify>"#, 11).unwrap();
    let (_, pois) = fixture.snapshot();
    assert!(pois.contains_key("osm_relation_306382"));
    assert_eq!(
        pois["osm_node_100"]["tags"],
        json!({"name:ja":"喫茶店","amenity":"cafe"})
    );
    assert_eq!(result["excludedIncompleteRelationCount"], 0);
    assert_eq!(
        fs::read_to_string(fixture.state.join("output/excluded-relations.jsonl")).unwrap(),
        ""
    );
    assert_eq!(
        fixture
            .database()
            .query_row(
                "SELECT tags FROM objects WHERE kind='way' AND id=972806795",
                [],
                |row| row.get::<_, String>(0)
            )
            .unwrap(),
        "{}"
    );
    let result = fixture
        .change(r#"<delete><way id="972806795" version="1"/></delete>"#, 12)
        .unwrap();
    assert!(!fixture.snapshot().1.contains_key("osm_relation_306382"));
    assert_eq!(result["excludedIncompleteRelationCount"], 1);
}

#[test]
fn duplicate_versions_keep_highest_or_last_equal_and_skip_invalid_lower_body() {
    let fixture = Fixture::new();
    fixture.change(r#"<modify>
      <node id="100" version="3" lat="-33.7" lon="151.3"><tag k="name" v="Current Cafe"/><tag k="amenity" v="cafe"/></node>
      <way id="10" version="4"><nd ref="100"/><nd ref="101"/><tag k="name" v="First Way"/><tag k="amenity" v="cafe"/></way>
      <node id="100" version="2" lat="invalid" lon="invalid"><tag k="name" v="Ignored"/><tag k="name" v="Duplicate"/></node>
      <way id="10" version="4"><nd ref="1"/><nd ref="2"/><tag k="name" v="Last Way"/><tag k="amenity" v="cafe"/></way>
      <node id="1" version="2" lat="-33.5" lon="151.5"><tag k="name" v="Moved Cafe"/><tag k="amenity" v="cafe"/></node>
    </modify>"#, 11).unwrap();
    let (_, pois) = fixture.snapshot();
    assert_eq!(pois["osm_node_100"]["tags"]["name"], "Current Cafe");
    assert_eq!(pois["osm_way_10"]["tags"]["name"], "Last Way");
    assert_eq!(pois["osm_way_10"]["lat"], (-33.5 - 33.8) / 2.0);
    assert_eq!(pois["osm_relation_20"]["lon"], (151.5 + 151.2) / 2.0);
}

#[test]
fn deletion_name_loss_and_recreation_remove_and_restore_pois() {
    let fixture = Fixture::new();
    fixture.change(r#"<delete><node id="50" version="2"/></delete><modify><node id="1" version="2" lat="-33.9" lon="151.1"><tag k="amenity" v="cafe"/></node></modify>"#, 11).unwrap();
    let (_, pois) = fixture.snapshot();
    assert!(!pois.contains_key("osm_node_50"));
    assert!(!pois.contains_key("osm_node_1"));
    assert!(pois.contains_key("osm_way_10"));
    fixture.change(r#"<create><node id="50" version="2" lat="-34.5" lon="150.5"><tag k="name" v="Returned Shop"/><tag k="shop" v="books"/></node></create>"#, 12).unwrap();
    assert_eq!(
        fixture.snapshot().1["osm_node_50"]["tags"]["name"],
        "Returned Shop"
    );
}

#[test]
fn replacing_way_references_removes_old_dependency_edges() {
    let fixture = Fixture::new();
    fixture.change(r#"<modify><way id="10" version="2"><nd ref="100"/><nd ref="101"/><tag k="name" v="Way Cafe"/><tag k="amenity" v="cafe"/></way>
      <node id="1" version="2" lat="-33.4" lon="151.6"><tag k="name" v="First Cafe"/><tag k="amenity" v="cafe"/></node></modify>"#, 11).unwrap();
    let (_, pois) = fixture.snapshot();
    assert_eq!(pois["osm_way_10"]["lat"], (-33.7 - 33.6) / 2.0);
    assert_eq!(pois["osm_relation_20"]["lat"], (-33.7 - 33.6) / 2.0);
    let result = fixture.change(r#"<modify><node id="1" version="3" lat="-33.3" lon="151.7"><tag k="name" v="First Cafe"/><tag k="amenity" v="cafe"/></node></modify>"#, 12).unwrap();
    assert_eq!(result["affectedObjects"], 1);
}

#[test]
fn relation_self_reference_preserves_geometry_and_later_member_updates() {
    let fixture = Fixture::new();
    let (before, before_pois) = fixture.snapshot();
    fixture.change(r#"<modify><relation id="20" version="2"><member type="way" ref="10"/><member type="relation" ref="20"/><tag k="name" v="Museum"/><tag k="tourism" v="museum"/></relation></modify>"#, 11).unwrap();
    let (after, after_pois) = fixture.snapshot();
    assert_eq!(after_pois, before_pois);
    assert_eq!(after["cells"], before["cells"]);
    assert_eq!(
        fixture
            .database()
            .query_row(
                "SELECT COUNT(*) FROM refs WHERE kind='relation' AND id=20 AND target_kind='relation' AND target_id=20",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );
    let result = fixture.change(r#"<modify><node id="1" version="2" lat="-33.5" lon="151.5"><tag k="name" v="First Cafe"/><tag k="amenity" v="cafe"/></node></modify>"#, 12).unwrap();
    assert_eq!(result["affectedObjects"], 3);
    let (_, moved) = fixture.snapshot();
    assert_eq!(moved["osm_relation_20"]["lat"], (-33.5 - 33.8) / 2.0);
    assert_eq!(moved["osm_relation_20"]["lon"], (151.5 + 151.2) / 2.0);
    assert_eq!(moved["osm_node_50"], before_pois["osm_node_50"]);
}

#[test]
fn invalid_geometry_and_object_write_failure_roll_back_the_database() {
    let fixture = Fixture::new();
    let before = fixture.snapshot();
    let release = fs::read(fixture.state.join("output/release.json")).unwrap();
    for body in [
        r#"<delete><node id="1" version="2"/></delete>"#,
        r#"<modify><relation id="20" version="2"><member type="relation" ref="20"/><tag k="name" v="Museum"/><tag k="tourism" v="museum"/></relation></modify>"#,
    ] {
        assert!(fixture.change(body, 11).is_err());
        assert_eq!(fixture.status()["sequence"], 10);
        assert_eq!(fixture.snapshot(), before);
        assert_eq!(
            fs::read(fixture.state.join("output/release.json")).unwrap(),
            release
        );
    }
    let manifests = fixture.state.join("output/manifests");
    let moved = fixture.state.join("output/saved-manifests");
    fs::rename(&manifests, &moved).unwrap();
    fs::write(&manifests, b"blocked directory").unwrap();
    assert!(
        fixture
            .change(r#"<delete><node id="50" version="2"/></delete>"#, 11)
            .is_err()
    );
    fs::remove_file(&manifests).unwrap();
    fs::rename(moved, manifests).unwrap();
    assert_eq!(fixture.status()["sequence"], 10);
    assert_eq!(fixture.snapshot(), before);
}

#[test]
fn truncated_gzip_and_malformed_xml_leave_the_previous_generation_intact() {
    let fixture = Fixture::new();
    let before = fixture.snapshot();
    let options = fixture.options(r#"<delete><node id="50" version="2"/></delete>"#, 11);
    let bytes = fs::read(&options.input).unwrap();
    fs::write(&options.input, &bytes[..bytes.len() - 5]).unwrap();
    assert!(incremental::apply_diff(&options, &fixture.compute).is_err());
    for (index, xml) in ["<osmChange><delete><node id=\"50\"/></delete>", "<osmChange/><osmChange/>", "<osmChange><modify><node id=\"1\" lat=\"-33\" lon=\"151\"><tag k=\"name\" v=\"A\"/><tag k=\"name\" v=\"B\"/></node></modify></osmChange>"].iter().enumerate() {
        let mut options = fixture.options("", 11);
        options.input = fixture.work.path().join(format!("invalid-{index}.osc"));
        fs::write(&options.input, xml).unwrap();
        assert!(incremental::apply_diff(&options, &fixture.compute).is_err(), "accepted {xml}");
    }
    assert_eq!(fixture.status()["sequence"], 10);
    assert_eq!(fixture.snapshot(), before);
}

#[test]
fn status_repairs_exports_after_commit_and_directory_relocation() {
    let mut fixture = Fixture::new();
    let release = fixture.state.join("output/release.json");
    fs::remove_file(&release).unwrap();
    fs::create_dir(&release).unwrap();
    assert!(
        fixture
            .change(r#"<delete><node id="50" version="2"/></delete>"#, 11)
            .is_err()
    );
    fs::remove_dir(&release).unwrap();
    let relocated = fixture.work.path().join("relocated");
    fs::rename(&fixture.state, &relocated).unwrap();
    fixture.state = relocated;
    let status = fixture.status();
    assert_eq!(status["sequence"], 11);
    assert_eq!(
        status["output"],
        fixture.state.join("output").to_str().unwrap()
    );
    let restored: Value =
        serde_json::from_slice(&fs::read(fixture.state.join("output/release.json")).unwrap())
            .unwrap();
    assert_eq!(restored["manifest"], status["manifest"]);
    assert!(!fixture.snapshot().1.contains_key("osm_node_50"));
}

#[test]
fn sequence_gaps_backwards_time_and_partial_state_are_rejected() {
    let fixture = Fixture::new();
    assert!(
        fixture
            .change("", 12)
            .unwrap_err()
            .to_string()
            .contains("sequence 11")
    );
    let mut options = fixture.options("", 11);
    options.timestamp = "2019-01-01T00:00:00Z".into();
    assert!(incremental::apply_diff(&options, &fixture.compute).is_err());
    let partial = fixture.work.path().join("partial");
    fs::create_dir(&partial).unwrap();
    Connection::open(partial.join("state.sqlite")).unwrap();
    assert!(incremental::status(&partial, &fixture.compute).is_err());
    assert!(incremental::initialize(&fixture.init, &fixture.compute).is_err());
}

#[test]
fn empty_diff_updates_coverage_and_sequence_without_changing_cells() {
    let fixture = Fixture::new();
    let (before, _) = fixture.snapshot();
    let mut options = fixture.options("", 11);
    let coverage = fixture.work.path().join("new-coverage.json");
    let polygon = json!({"type":"Polygon","coordinates":[[[150,-36],[153,-36],[153,-31],[150,-31],[150,-36]]]});
    fs::write(&coverage, serde_json::to_vec(&polygon).unwrap()).unwrap();
    options.coverage = Some(coverage);
    let result = incremental::apply_diff(&options, &fixture.compute).unwrap();
    assert_eq!(result["changedObjects"], 0);
    assert_eq!(result["dirtyCells"], 0);
    let (after, _) = fixture.snapshot();
    assert_eq!(before["cells"], after["cells"]);
    assert_eq!(after["coverage"], polygon);
    assert_eq!(
        after["replicationDiffSHA256"],
        hash_bytes(&fs::read(options.input).unwrap())
    );
}

#[test]
fn sixty_mixed_diffs_match_osmium_full_snapshot_rebuilds() {
    let fixture = Fixture::new();
    let mut changes = Vec::new();
    for step in 0..60 {
        let version = step + 2;
        let body = match step % 6 {
            0 => format!(
                r#"<modify><node id="1" version="{version}" lat="{:.7}" lon="151.1"><tag k="name:zh" v="咖啡 {step}"/><tag k="amenity" v="cafe"/></node></modify>"#,
                -33.9 + f64::from(step) / 10000.0
            ),
            1 => format!(
                r#"<modify><way id="10" version="{version}"><nd ref="100"/><nd ref="101"/><tag k="name" v="Changed Way {step}"/><tag k="amenity" v="cafe"/></way></modify>"#
            ),
            2 => format!(r#"<delete><node id="50" version="{version}"/></delete>"#),
            3 => format!(
                r#"<create><node id="50" version="{version}" lat="-34.5" lon="150.5"><tag k="name" v="Returned Shop {step}"/><tag k="shop" v="books"/></node></create>"#
            ),
            4 if step / 6 % 2 == 0 => format!(
                r#"<create><way id="972806795" version="{version}"><nd ref="100"/><nd ref="101"/></way></create>"#
            ),
            4 => format!(r#"<delete><way id="972806795" version="{version}"/></delete>"#),
            _ => format!(
                r#"<modify><way id="10" version="{version}"><nd ref="1"/><nd ref="2"/><tag k="name" v="Original Way {step}"/><tag k="amenity" v="cafe"/></way></modify>"#
            ),
        };
        let mut options = fixture.options(&body, 11 + step as u64);
        options.timestamp = "2020-01-02T00:00:00Z".into();
        incremental::apply_diff(&options, &fixture.compute).unwrap();
        changes.push(options.input);
        if step % 10 == 9 {
            let input = fixture
                .work
                .path()
                .join(format!("reference-{step}.osm.pbf"));
            let result = std::process::Command::new("osmium")
                .arg("apply-changes")
                .arg(&fixture.init.input)
                .args(&changes)
                .arg("-o")
                .arg(&input)
                .output()
                .unwrap();
            assert!(
                result.status.success(),
                "{}",
                String::from_utf8_lossy(&result.stderr)
            );
            let output = fixture.work.path().join(format!("reference-{step}"));
            let receipt = build::run(
                &BuildOptions {
                    input,
                    output: output.clone(),
                    coverage: fixture.init.coverage.clone(),
                    region: "au-nsw".into(),
                    source_timestamp: options.timestamp,
                    source_sequence: Some(options.sequence),
                    source_sha256: "a".repeat(64),
                    scratch: fixture.work.path().join("scratch"),
                },
                &fixture.compute,
            )
            .unwrap();
            let reference: Value = serde_json::from_slice(
                &fs::read(output.join(format!(
                    "manifests/{}.json",
                    receipt["manifest"].as_str().unwrap()
                )))
                .unwrap(),
            )
            .unwrap();
            let (actual, _) = fixture.snapshot();
            for key in ["cells", "count", "excludedIncompleteRelationCount"] {
                assert_eq!(
                    actual[key],
                    reference[key],
                    "{key} differs at diff {}",
                    step + 1
                );
            }
            assert_eq!(
                fs::read(fixture.state.join("output/excluded-relations.jsonl")).unwrap(),
                fs::read(output.join("excluded-relations.jsonl")).unwrap()
            );
        }
    }
}

#[test]
fn oversized_xml_attributes_and_text_roll_back() {
    let mut fixture = Fixture::new();
    fixture.compute.batch_bytes = 4096;
    let before = fixture.snapshot();
    for body in [
        format!(
            r#"<modify><node id="50" version="2" lat="-34" lon="151"><tag k="name" v="{}"/></node></modify>"#,
            "a".repeat(100_000)
        ),
        format!("<delete>{}</delete>", "a".repeat(100_000)),
        format!("<![CDATA[{}]]>", "a".repeat(100_000)),
    ] {
        assert!(fixture.change(&body, 11).is_err());
        assert_eq!(fixture.status()["sequence"], 10);
        assert_eq!(fixture.snapshot(), before);
    }
}

#[test]
fn installed_version_one_state_from_previous_engine_can_be_read_and_updated() {
    let work = common::work("installed-state-");
    let state = work.path().join("state");
    fs::create_dir_all(state.join("output")).unwrap();
    let database = Connection::open(state.join("state.sqlite")).unwrap();
    database
        .execute_batch(include_str!("fixtures/installed-state-v1.sql"))
        .unwrap();
    let payload: Vec<u8> = database
        .query_row(
            "SELECT payload FROM pois WHERE id='osm_node_1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let mut block = vec![b'['];
    block.extend_from_slice(&payload);
    block.push(b']');
    let digest = aura_osm::storage::write_object(&state.join("output"), "blocks", &block).unwrap();
    let hashes: String = database
        .query_row("SELECT hashes FROM cell_blocks", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Vec<String>>(&hashes).unwrap(),
        [digest]
    );
    database.close().unwrap();
    let compute = ComputeOptions {
        workers: 2,
        batch_size: 4096,
        pending_batches: 2,
        batch_bytes: 4194304,
        sqlite_cache_mib: 16,
    };
    let before = incremental::status(&state, &compute).unwrap();
    assert_eq!(before["sequence"], 10);
    assert_eq!(before["count"], 1);
    let input = work.path().join("change.osc");
    fs::write(&input, r#"<osmChange><modify><node id="1" version="2" lat="-33.5" lon="151.5"><tag k="name" v="Updated Cafe"/><tag k="amenity" v="cafe"/></node></modify></osmChange>"#).unwrap();
    let after = incremental::apply_diff(
        &ApplyOptions {
            input,
            state: state.clone(),
            sequence: 11,
            timestamp: "2020-01-02T00:00:00Z".into(),
            coverage: None,
        },
        &compute,
    )
    .unwrap();
    assert_eq!(after["sequence"], 11);
    assert_eq!(after["count"], 1);
    let manifest: Value = serde_json::from_slice(
        &fs::read(state.join(format!(
            "output/manifests/{}.json",
            after["manifest"].as_str().unwrap()
        )))
        .unwrap(),
    )
    .unwrap();
    let cells = manifest["cells"].as_object().unwrap();
    assert_eq!(cells.len(), 1);
    let digest = cells.values().next().unwrap()[0][0].as_str().unwrap();
    let records: Value = serde_json::from_slice(
        &fs::read(state.join(format!("output/blocks/{digest}.json"))).unwrap(),
    )
    .unwrap();
    assert_eq!(records[0]["tags"]["name"], "Updated Cafe");
    assert_eq!(records[0]["lat"], -33.5);
    assert_eq!(records[0]["lon"], 151.5);
}

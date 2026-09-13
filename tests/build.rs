mod common;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use aura_osm::build::{self, BuildOptions, ComputeOptions};
use aura_osm::format::hash_bytes;
use serde_json::{Value, json};

fn compute(workers: usize) -> ComputeOptions {
    ComputeOptions {
        workers,
        batch_size: 3,
        pending_batches: 8,
        batch_bytes: 1 << 20,
        sqlite_cache_mib: 16,
    }
}

fn coverage(work: &Path) -> PathBuf {
    let path = work.join("coverage.json");
    fs::write(&path, serde_json::to_vec(&json!({"type":"Polygon","coordinates":[[[150,-35],[152,-35],[152,-32],[150,-32],[150,-35]]]})).unwrap()).unwrap();
    path
}

fn compile(
    work: &Path,
    name: &str,
    xml: &str,
    workers: usize,
) -> anyhow::Result<(Value, Value, Vec<Value>)> {
    let input = common::write_pbf(work, name, xml, &[]);
    let output = work.join(name);
    let receipt = build::run(
        &BuildOptions {
            source_sha256: hash_bytes(&fs::read(&input)?),
            input,
            output: output.clone(),
            coverage: coverage(work),
            region: "au-nsw".into(),
            source_timestamp: "2020-01-01T00:00:00Z".into(),
            source_sequence: Some(5),
            scratch: work.join("scratch"),
        },
        &compute(workers),
    )?;
    let (manifest, records) = read_output(&output, &receipt);
    Ok((receipt, manifest, records))
}

fn read_output(output: &Path, receipt: &Value) -> (Value, Vec<Value>) {
    let digest = receipt["manifest"].as_str().unwrap();
    let bytes = fs::read(output.join(format!("manifests/{digest}.json"))).unwrap();
    assert_eq!(hash_bytes(&bytes), digest);
    let manifest: Value = serde_json::from_slice(&bytes).unwrap();
    let mut records = Vec::new();
    for hashes in manifest["cells"].as_object().unwrap().values() {
        for page in hashes.as_array().unwrap() {
            let digest = page[0].as_str().unwrap();
            let pack = manifest["packs"][page[1].as_u64().unwrap() as usize]
                .as_str()
                .unwrap();
            let packed = fs::read(output.join(format!("packs/{pack}.bin"))).unwrap();
            assert_eq!(hash_bytes(&packed), pack);
            assert!(packed.len() <= aura_osm::format::MAX_PACK);
            let offset = page[2].as_u64().unwrap() as usize;
            let length = page[3].as_u64().unwrap() as usize;
            let bytes = &packed[offset..offset + length];
            assert_eq!(
                bytes,
                fs::read(output.join(format!("blocks/{digest}.json"))).unwrap()
            );
            assert_eq!(hash_bytes(bytes), digest);
            assert!(bytes.len() <= 262144);
            records.extend(serde_json::from_slice::<Vec<Value>>(bytes).unwrap());
        }
    }
    assert_eq!(records.len() as u64, receipt["count"].as_u64().unwrap());
    aura_osm::format::validate_manifest(&manifest).unwrap();
    (manifest, records)
}

#[test]
fn unordered_nested_geometry_and_multilingual_names() {
    let work = common::work("build-nested-");
    let (_, manifest, records) = compile(work.path(), "nested", r#"<osm version="0.6">
      <relation id="20"><member type="way" ref="10"/><member type="relation" ref="21"/><tag k="name:ja" v="公園"/><tag k="leisure" v="park"/></relation>
      <relation id="21"><member type="node" ref="3"/></relation>
      <way id="10"><nd ref="1"/><nd ref="2"/><tag k="name" v="Way Cafe"/><tag k="amenity" v="cafe"/></way>
      <node id="1" lat="-34" lon="150"><tag k="name:zh" v="咖啡"/><tag k="amenity" v="cafe"/></node>
      <node id="2" lat="-33" lon="151"/><node id="3" lat="-32" lon="152"/>
      <node id="4" lat="-33" lon="151"><tag k="amenity" v="parking"/></node>
      <relation id="70"/><relation id="71"><member type="relation" ref="72"/></relation>
      <relation id="72"><member type="relation" ref="71"/></relation></osm>"#, 4).unwrap();
    assert_eq!(manifest["sourceSequence"], 5);
    assert_eq!(records.len(), 3);
    let by_id: BTreeMap<_, _> = records
        .iter()
        .map(|record| (record["id"].as_str().unwrap(), record))
        .collect();
    assert_eq!(by_id["osm_node_1"]["tags"]["name:zh"], "咖啡");
    assert_eq!(by_id["osm_way_10"]["lat"], -33.5);
    assert_eq!(by_id["osm_way_10"]["lon"], 150.5);
    assert_eq!(by_id["osm_relation_20"]["lat"], -33.0);
    assert_eq!(by_id["osm_relation_20"]["lon"], 151.0);
}

#[test]
fn self_referencing_relations_keep_the_same_geometry_as_the_repaired_source() {
    let work = common::work("build-self-reference-");
    let source = r#"<osm version="0.6">
      <node id="1" lat="54.6" lon="11.4"/>
      <node id="2" lat="54.7" lon="11.6"/>
      <way id="19505966"><nd ref="1"/><nd ref="2"/></way>
      <relation id="19505966"><member type="way" ref="19505966" role="outer"/><member type="relation" ref="19505966" role="outer"/><tag k="name" v="Saksfjed Vildmark"/><tag k="leisure" v="nature_reserve"/><tag k="type" v="multipolygon"/></relation>
      <relation id="19505967"><member type="relation" ref="19505966"/><tag k="name" v="Parent Park"/><tag k="leisure" v="park"/></relation>
    </osm>"#;
    let repaired = source.replace(
        r#"<member type="relation" ref="19505966" role="outer"/>"#,
        "",
    );
    let (_, expected_manifest, expected) = compile(work.path(), "repaired", &repaired, 1).unwrap();
    let (receipt, manifest, records) = compile(work.path(), "original", source, 2).unwrap();
    assert_eq!(records, expected);
    assert_eq!(manifest["cells"], expected_manifest["cells"]);
    assert_eq!(receipt["count"], 2);
    assert_eq!(receipt["excludedIncompleteRelationCount"], 0);
    assert!(records.iter().all(|record| record["lon"] == 11.5));
    assert!(
        records
            .iter()
            .any(|record| record["id"] == "osm_relation_19505966")
    );
}

#[test]
fn incomplete_relations_preserve_terminal_diagnostics_and_parent_exclusion() {
    let work = common::work("build-incomplete-");
    for kind in ["node", "way", "relation"] {
        let xml = format!(
            r#"<osm version="0.6"><node id="1" lat="-33.9" lon="151.1"><tag k="name" v="Cafe"/><tag k="amenity" v="cafe"/></node>
          <relation id="30"><member type="{kind}" ref="99"/><tag k="name:ja" v="博物館"/><tag k="tourism" v="museum"/></relation>
          <relation id="40"><member type="relation" ref="30"/></relation>
          <relation id="50"><member type="relation" ref="40"/><tag k="name" v="Parent Park"/><tag k="leisure" v="park"/></relation></osm>"#
        );
        let (receipt, manifest, records) = compile(work.path(), kind, &xml, 2).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(receipt["excludedIncompleteRelationCount"], 2);
        assert_eq!(manifest["excludedIncompleteRelationCount"], 2);
        let diagnostics =
            fs::read_to_string(work.path().join(kind).join("excluded-relations.jsonl")).unwrap();
        let diagnostics: Vec<Value> = diagnostics
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(diagnostics.len(), 2);
        assert_eq!(diagnostics[0]["name"], "博物館");
        for row in diagnostics {
            assert_eq!(
                row["missingMembers"],
                json!([{"relation":"osm_relation_30","type":kind,"id":99}])
            );
        }
    }
}

#[test]
fn broken_way_cycle_or_empty_geometry_cannot_publish_or_hide_in_partial_relation() {
    let work = common::work("build-errors-");
    let inputs = [
        r#"<way id="1"><nd ref="99"/></way>"#,
        r#"<relation id="1"><tag k="name" v="Park"/><tag k="leisure" v="park"/></relation>"#,
        r#"<relation id="1"><member type="relation" ref="1"/><tag k="name" v="Park"/><tag k="leisure" v="park"/></relation>"#,
        r#"<relation id="1"><member type="relation" ref="2"/><tag k="name" v="Park"/><tag k="leisure" v="park"/></relation><relation id="2"><member type="relation" ref="1"/></relation>"#,
        r#"<relation id="10"/><relation id="1"><member type="relation" ref="10"/><member type="way" ref="99"/><tag k="name" v="Park"/><tag k="leisure" v="park"/></relation>"#,
        r#"<relation id="10"><member type="relation" ref="1"/></relation><relation id="1"><member type="relation" ref="10"/><member type="way" ref="99"/><tag k="name" v="Park"/><tag k="leisure" v="park"/></relation>"#,
    ];
    for (index, body) in inputs.iter().enumerate() {
        let name = format!("bad-{index}");
        assert!(
            compile(
                work.path(),
                &name,
                &format!("<osm version=\"0.6\">{body}</osm>"),
                2
            )
            .is_err(),
            "accepted {index}"
        );
        assert!(!work.path().join(name).join("release.json").exists());
    }
}

#[test]
fn page_boundaries_hashes_and_worker_counts_are_deterministic() {
    let work = common::work("build-pages-");
    let mut xml = String::from("<osm version=\"0.6\">");
    for index in 1..1601 {
        xml.push_str(&format!(r#"<node id="{index}" lat="-33" lon="151"><tag k="name" v="{index} {}"/><tag k="shop" v="books"/></node>"#, "x".repeat(900)));
    }
    xml.push_str("</osm>");
    let (_, first, records) = compile(work.path(), "serial", &xml, 1).unwrap();
    let (_, second, second_records) = compile(work.path(), "parallel", &xml, 4).unwrap();
    assert_eq!(first["cells"], second["cells"]);
    assert_eq!(records, second_records);
    assert_eq!(records.len(), 1600);
    assert!(first["packs"].as_array().unwrap().len() > 1);
    assert_eq!(first["packs"], second["packs"]);
    let ids: Vec<_> = records
        .iter()
        .map(|row| row["id"].as_str().unwrap())
        .collect();
    assert!(ids.windows(2).all(|window| window[0] < window[1]));
    assert!(
        first["cells"]
            .as_object()
            .unwrap()
            .values()
            .next()
            .unwrap()
            .as_array()
            .unwrap()
            .len()
            > 1
    );
}

#[test]
fn sparse_cells_pack_into_one_object_without_changing_any_logical_block_bytes() {
    let work = common::work("build-sparse-packs-");
    let mut xml = String::from("<osm version=\"0.6\">");
    for y in 0..16 {
        for x in 0..16 {
            let id = y * 16 + x + 1;
            let lat = -33.995 + f64::from(y) * 0.01;
            let lon = 150.885 + f64::from(x) * 0.01;
            xml.push_str(&format!(r#"<node id="{id}" lat="{lat:.3}" lon="{lon:.3}"><tag k="name" v="Cafe {id}"/><tag k="amenity" v="cafe"/></node>"#));
        }
    }
    xml.push_str("</osm>");
    let (receipt, manifest, records) = compile(work.path(), "sparse", &xml, 2).unwrap();
    assert_eq!(records.len(), 256);
    assert_eq!(manifest["schema"], 2);
    assert_eq!(manifest["cells"].as_object().unwrap().len(), 256);
    assert_eq!(receipt["blockCount"], 256);
    assert_eq!(receipt["packCount"], 1);
    let pack = manifest["packs"][0].as_str().unwrap();
    let bytes = fs::read(work.path().join(format!("sparse/packs/{pack}.bin"))).unwrap();
    assert_eq!(receipt["packBytes"], bytes.len());
    println!(
        "Sparse fixture: 256 logical blocks -> 1 physical pack; {} POIs, {} bytes; 257 -> 2 immutable-object PUTs including manifest",
        records.len(),
        bytes.len()
    );
}

#[test]
fn pole_and_antimeridian_map_to_last_row_and_first_column() {
    let work = common::work("build-pole-");
    let (_, manifest, records) = compile(work.path(), "pole", r#"<osm version="0.6"><node id="1" lat="90" lon="180"><tag k="int_name" v="North"/><tag k="tourism" v="viewpoint"/></node></osm>"#, 1).unwrap();
    assert_eq!(
        manifest["cells"]
            .as_object()
            .unwrap()
            .keys()
            .collect::<Vec<_>>(),
        ["17999_0"]
    );
    assert_eq!(records[0]["lon"], -180.0);
}

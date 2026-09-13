mod common;

use std::{collections::BTreeMap, fs};

use aura_osm::{
    config::{Environment, Runtime},
    pipeline,
};
use md5::{Digest, Md5};
use serde_json::{Value, json};

#[test]
fn rebuild_reads_the_original_pbf_and_writes_a_new_release_without_cloud_settings() {
    let work = common::work("pipeline-rebuild-");
    let root = work.path();
    fs::create_dir(root.join("config")).unwrap();
    fs::write(
        root.join("config/regions.json"),
        r#"{"regions":[{"id":"au-nsw","extract":"australia-oceania/australia/new-south-wales"}]}"#,
    )
    .unwrap();
    fs::write(
        root.join("config/runtime.json"),
        include_str!("../config/runtime.json"),
    )
    .unwrap();
    let source = common::write_pbf(
        work.path(),
        "source",
        "<osm version=\"0.6\"><node id=\"1\" version=\"1\" lat=\"0.5\" lon=\"0.5\"><tag k=\"amenity\" v=\"cafe\"/><tag k=\"name\" v=\"Cafe\"/></node></osm>",
        &[
            ("osmosis_replication_timestamp", "2025-01-02T03:04:05Z"),
            ("osmosis_replication_sequence_number", "42"),
            (
                "osmosis_replication_base_url",
                "https://download.geofabrik.de/australia-oceania/australia/new-south-wales-updates",
            ),
        ],
    );
    let original = fs::read(&source).unwrap();
    fs::write(
        work.path().join("source.md5"),
        format!("{:x}", Md5::digest(&original)),
    )
    .unwrap();
    fs::write(work.path().join("source.url"), "https://download.geofabrik.de/australia-oceania/australia/new-south-wales-latest.osm.pbf\n").unwrap();
    fs::write(
        work.path().join("coverage.json"),
        serde_json::to_vec(
            &json!({"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,1],[0,0]]]}),
        )
        .unwrap(),
    )
    .unwrap();
    let environment = Environment::from_values(BTreeMap::from([
        (
            "OSM_DATA_DIR".into(),
            work.path().join("data").to_string_lossy().into_owned(),
        ),
        ("OSM_START_FREE_GIB".into(), "0".into()),
        ("OSM_MIN_FREE_GIB".into(), "0".into()),
        ("OSM_COMPUTE_WORKERS".into(), "1".into()),
    ]));
    let runtime = Runtime::load(root, &environment).unwrap();
    let output = pipeline::build(root, &runtime, "au-nsw", Some(work.path())).unwrap();
    assert!(output.starts_with(work.path()));
    assert!(
        output
            .parent()
            .unwrap()
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("rebuild-")
    );
    let release: Value =
        serde_json::from_slice(&fs::read(output.join("release.json")).unwrap()).unwrap();
    assert_eq!(release["region"], "au-nsw");
    assert_eq!(release["count"], 1);
    assert_eq!(fs::read(&source).unwrap(), original);
    assert!(!output.join("receipt.json").exists());
}

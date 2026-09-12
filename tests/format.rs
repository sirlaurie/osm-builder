use aura_osm::format::{
    canonical_json, has_name, is_poi, normalize_timestamp, source_metadata, validate_block,
    validate_coverage, validate_current, validate_manifest,
};
use serde_json::json;
use std::{collections::BTreeMap, fs, path::Path};

#[test]
fn canonical_bytes_preserve_existing_hash_contract() {
    let value = json!({ "z": "喫茶店 é /\n", "a": [-0.0, 1.0, 1e-7, 1e-5, 1e-4, 1e15, 1e16, 1e20, 0.30000000000000004] });
    assert_eq!(
        String::from_utf8(canonical_json(&value).unwrap()).unwrap(),
        "{\"a\":[-0.0,1.0,1e-07,1e-05,0.0001,1000000000000000.0,1e+16,1e+20,0.30000000000000004],\"z\":\"喫茶店 é /\\n\"}"
    );
}

#[test]
fn coverage_validation_rejects_shape_and_coordinate_errors() {
    let valid = json!({"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,0]]]});
    validate_coverage(&valid).unwrap();
    for invalid in [
        json!({"type":"Polygon","coordinates":[]}),
        json!({"type":"Polygon","coordinates":[[[0,0],[0,1],[0,2],[0,0]]]}),
        json!({"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[1,2]]]}),
        json!({"type":"Polygon","coordinates":[[[0,0],[181,0],[1,1],[0,0]]]}),
        json!({"type":"Polygon","coordinates":[[[false,0],[1,0],[1,1],[0,0]]]}),
    ] {
        assert!(validate_coverage(&invalid).is_err());
    }
    assert_eq!(
        normalize_timestamp("2026-01-01T12:00:00+10:00").unwrap(),
        "2026-01-01T02:00:00Z"
    );
    assert!(normalize_timestamp("2026-02-30T00:00:00Z").is_err());
    assert!(normalize_timestamp("2026-01-01T12:00:00").is_err());
    assert!(normalize_timestamp("2999-01-01T00:00:00Z").is_err());
    let tests = Path::new(env!("CARGO_MANIFEST_DIR")).join(".build/tools/tests");
    fs::create_dir_all(&tests).unwrap();
    let temporary = tempfile::tempdir_in(tests).unwrap();
    let path = temporary.path().join("coverage.json");
    fs::write(
        &path,
        serde_json::to_vec(&json!({"type":"Feature","geometry":valid,"properties":{}})).unwrap(),
    )
    .unwrap();
    let metadata = source_metadata(
        &path,
        "au-nsw",
        "2026-01-01T00:00:00Z",
        Some(12),
        &"a".repeat(64),
    )
    .unwrap();
    assert_eq!(metadata["coverage"], valid);
}

#[test]
fn format_trust_boundaries_preserve_worker_constraints() {
    let manifest = json!({"schema":1,"region":"test","sourceTimestamp":"2026-01-01T00:00:00Z",
        "sourceSequence":null,"sourceSHA256":"a".repeat(64),"coverage":{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,0]]]},"cells":{},"count":0});
    validate_manifest(&manifest).unwrap();
    let mut missing_sequence = manifest.clone();
    missing_sequence
        .as_object_mut()
        .unwrap()
        .remove("sourceSequence");
    assert!(validate_manifest(&missing_sequence).is_err());
    let mut bad_cell = manifest.clone();
    bad_cell["cells"] = json!({"09000_18000":["b".repeat(64)]});
    bad_cell["count"] = json!(1);
    assert!(validate_manifest(&bad_cell).is_err());
    let current = json!({"schema":1,"revision":"11111111-1111-1111-1111-111111111111","regions":[
        {"region":"test","manifest":"a".repeat(64),"sourceTimestamp":"2026-01-01T00:00:00Z","bbox":[0,0,1,1]}]});
    validate_current(&current).unwrap();
    let mut duplicate = current.clone();
    duplicate["regions"]
        .as_array_mut()
        .unwrap()
        .push(current["regions"][0].clone());
    assert!(validate_current(&duplicate).is_err());
    let mut invalid = current;
    invalid["regions"][0]["bbox"] = json!([1, 0, 0, 1]);
    assert!(validate_current(&invalid).is_err());
    let valid =
        json!([{"id":"osm_node_1","lat":0,"lon":0,"tags":{"name":"Cafe","amenity":"cafe"}}]);
    validate_block(&valid).unwrap();
    let mut invalid = valid;
    invalid[0]["id"] = json!("osm_node_9007199254740992");
    assert!(validate_block(&invalid).is_err());
}

#[test]
fn poi_selection_requires_a_name_and_supported_category() {
    let mut tags: BTreeMap<String, String> = [
        ("name:zh-Hant".into(), "咖啡館".into()),
        ("amenity".into(), "cafe".into()),
    ]
    .into();
    assert!(is_poi(&tags));
    tags.remove("amenity");
    assert!(has_name(&tags));
    assert!(!is_poi(&tags));
    tags.insert("shop".into(), "books".into());
    assert!(is_poi(&tags));
    tags.insert("name:zh-Hant".into(), "  ".into());
    assert!(!is_poi(&tags));
}

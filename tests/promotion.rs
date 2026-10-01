use std::{fs, os::unix::fs::symlink, path::Path};

use aura_osm::promotion::{promote, recover};
use serde_json::json;

fn state(path: &Path, region: &str, manifest: &str) {
    fs::create_dir_all(path.join("output")).unwrap();
    fs::write(
        path.join("output/release.json"),
        serde_json::to_vec(&json!({
            "region": region,
            "manifest": manifest,
            "sourceTimestamp": "2020-01-01T00:00:00Z",
            "count": 1
        }))
        .unwrap(),
    )
    .unwrap();
}

fn work() -> tempfile::TempDir {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join(".build/tools/tests");
    fs::create_dir_all(&root).unwrap();
    tempfile::tempdir_in(root).unwrap()
}

fn journal(data: &Path, manifest: &str) {
    fs::write(
        data.join(".candidates/region/promotion.json"),
        serde_json::to_vec(&json!({"region": "region", "manifest": manifest})).unwrap(),
    )
    .unwrap();
}

#[test]
fn recovery_without_a_publish_journal_keeps_the_active_index() {
    let root = work();
    let data = root.path();
    state(&data.join("region"), "region", &"a".repeat(64));
    state(
        &data.join(".candidates/region/region"),
        "region",
        &"b".repeat(64),
    );
    recover(data, "region", Some(&"b".repeat(64))).unwrap();
    assert!(data.join(".candidates/region/region").is_dir());
    assert!(
        fs::read_to_string(data.join("region/output/release.json"))
            .unwrap()
            .contains(&"a".repeat(64))
    );
}

#[test]
fn interrupted_promotion_recovers_each_rename_boundary() {
    for stage in 0..=2 {
        let root = work();
        let data = root.path();
        let candidate = data.join(".candidates/region/region");
        let previous = data.join(".candidates/region/previous");
        let active = data.join("region");
        state(&active, "region", &"a".repeat(64));
        state(&candidate, "region", &"b".repeat(64));
        journal(data, &"b".repeat(64));
        if stage >= 1 {
            fs::rename(&active, &previous).unwrap();
        }
        if stage == 2 {
            fs::rename(&candidate, &active).unwrap();
        }
        recover(data, "region", Some(&"b".repeat(64))).unwrap();
        assert!(
            fs::read_to_string(active.join("output/release.json"))
                .unwrap()
                .contains(&"b".repeat(64))
        );
        assert!(!previous.exists());
        assert!(!candidate.exists());
        assert!(!data.join(".candidates/region/promotion.json").exists());
        recover(data, "region", Some(&"b".repeat(64))).unwrap();
    }
}

#[test]
fn recovery_refuses_an_unconfirmed_or_different_remote_manifest() {
    let root = work();
    let data = root.path();
    state(&data.join("region"), "region", &"a".repeat(64));
    state(
        &data.join(".candidates/region/region"),
        "region",
        &"b".repeat(64),
    );
    journal(data, &"b".repeat(64));
    assert!(recover(data, "region", None).is_err());
    assert!(recover(data, "region", Some(&"a".repeat(64))).is_err());
    assert!(data.join("region").is_dir());
    assert!(data.join(".candidates/region/region").is_dir());
}

#[test]
fn promotion_rejects_candidate_receipts_with_a_different_region_or_manifest() {
    let root = work();
    let data = root.path();
    let candidate = data.join(".candidates/region/region");
    state(&candidate, "other", &"b".repeat(64));
    assert!(promote(data, "region", &"b".repeat(64)).is_err());
    state(&candidate, "region", &"a".repeat(64));
    assert!(promote(data, "region", &"b".repeat(64)).is_err());
    assert!(!data.join(".candidates/region/promotion.json").exists());
}

#[test]
fn promotion_refuses_symbolic_links_without_touching_their_targets() {
    for component in [
        ".candidates",
        ".candidates/region",
        ".candidates/region/region",
        ".candidates/region/region/output",
        ".candidates/region/region/output/release.json",
        ".candidates/region/promotion.json",
        ".candidates/region/previous",
        "region",
    ] {
        let root = work();
        let data = root.path().join("data");
        fs::create_dir(&data).unwrap();
        let outside = root.path().join("outside");
        state(&outside, "region", &"b".repeat(64));
        let link = data.join(component);
        fs::create_dir_all(link.parent().unwrap()).unwrap();
        symlink(&outside, &link).unwrap();
        assert!(
            promote(&data, "region", &"b".repeat(64)).is_err(),
            "{component}"
        );
        assert!(outside.join("output/release.json").is_file());
    }
}

#[test]
fn confirmed_promotion_replaces_the_index_and_preserves_candidate_downloads() {
    let root = work();
    let data = root.path();
    state(&data.join("region"), "region", &"a".repeat(64));
    state(
        &data.join(".candidates/region/region"),
        "region",
        &"b".repeat(64),
    );
    fs::create_dir(data.join(".candidates/region/.downloads")).unwrap();
    promote(data, "region", &"b".repeat(64)).unwrap();
    assert!(data.join("region/output/release.json").is_file());
    assert!(data.join(".candidates/region/.downloads").is_dir());
    assert!(!data.join(".candidates/region/previous").exists());
}

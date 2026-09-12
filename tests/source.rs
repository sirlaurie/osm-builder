mod common;

use std::{
    fs,
    path::{Path, PathBuf},
};

use aura_osm::source;
use md5::{Digest, Md5};

fn checksum(path: &Path) -> PathBuf {
    let output = path.with_extension("md5");
    fs::write(
        &output,
        format!(
            "{:x}  source.osm.pbf\n",
            Md5::digest(fs::read(path).unwrap())
        ),
    )
    .unwrap();
    output
}

#[test]
fn pbf_header_and_checksum_identify_the_regional_snapshot() {
    let work = common::work("source-header-");
    let pbf = common::write_pbf(
        work.path(),
        "source",
        "<osm version=\"0.6\"><node id=\"1\" version=\"1\" lat=\"0\" lon=\"0\"/></osm>",
        &[
            ("osmosis_replication_timestamp", "2025-01-02T03:04:05Z"),
            ("osmosis_replication_sequence_number", "42"),
            (
                "osmosis_replication_base_url",
                "http://download.geofabrik.de/europe/france-updates",
            ),
        ],
    );
    let checksum = checksum(&pbf);
    let snapshot = source::stamp(
        &pbf,
        &checksum,
        "https://download.geofabrik.de/europe/france-updates",
    )
    .unwrap();
    assert_eq!(snapshot.timestamp, "2025-01-02T03:04:05Z");
    assert_eq!(snapshot.sequence, 42);
    assert_eq!(
        snapshot.replication_url,
        "https://download.geofabrik.de/europe/france-updates"
    );
    assert_eq!(
        snapshot.sha256,
        aura_osm::format::hash_bytes(&fs::read(&pbf).unwrap())
    );
    assert!(
        source::stamp(
            &pbf,
            &checksum,
            "https://download.geofabrik.de/europe/germany-updates"
        )
        .is_err()
    );
    fs::write(&checksum, "00000000000000000000000000000000 source.osm.pbf").unwrap();
    assert!(
        source::stamp(
            &pbf,
            &checksum,
            "https://download.geofabrik.de/europe/france-updates"
        )
        .is_err()
    );
}

#[test]
fn pbf_missing_regional_header_cannot_initialize_replication() {
    let work = common::work("source-missing-");
    let pbf = common::write_pbf(
        work.path(),
        "missing",
        "<osm version=\"0.6\"><node id=\"1\" version=\"1\" lat=\"0\" lon=\"0\"/></osm>",
        &[],
    );
    assert!(
        source::stamp(
            &pbf,
            &checksum(&pbf),
            "https://download.geofabrik.de/europe/france-updates"
        )
        .is_err()
    );
}

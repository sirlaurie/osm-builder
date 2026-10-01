mod common;

use std::{
    fs,
    path::{Path, PathBuf},
};

use aura_osm::source;
use md5::{Digest, Md5};
use serde_json::json;

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

#[test]
fn osm_france_snapshot_uses_its_header_sequence_and_rejects_other_sources() {
    let work = common::work("source-france-header-");
    let pbf = common::write_pbf(
        work.path(),
        "source",
        "<osm version=\"0.6\"><node id=\"1\" version=\"1\" lat=\"0\" lon=\"0\"/></osm>",
        &[
            ("osmosis_replication_timestamp", "2025-01-02T03:04:05Z"),
            ("osmosis_replication_sequence_number", "7309649"),
            (
                "osmosis_replication_base_url",
                "http://download.openstreetmap.fr/replication/./south-america/brazil/north/minute",
            ),
        ],
    );
    let checksum = checksum(&pbf);
    let snapshot = source::stamp(
        &pbf,
        &checksum,
        "https://download.openstreetmap.fr/replication/south-america/brazil/north/minute",
    )
    .unwrap();
    assert_eq!(snapshot.sequence, 7309649);
    assert_eq!(snapshot.timestamp, "2025-01-02T03:04:05Z");
    assert_eq!(
        snapshot.replication_url,
        "https://download.openstreetmap.fr/replication/south-america/brazil/north/minute"
    );
    for other in [
        "https://download.geofabrik.de/south-america/brazil/norte-updates",
        "https://download.openstreetmap.fr/replication/south-america/brazil/south/minute",
    ] {
        assert!(source::stamp(&pbf, &checksum, other).is_err());
    }
}

#[test]
fn osm_france_replication_urls_normalize_only_trusted_header_variants() {
    let expected =
        "https://download.openstreetmap.fr/replication/oceania/australia/new_south_wales/minute";
    for value in [
        expected,
        "http://download.openstreetmap.fr/replication/./oceania/australia/new_south_wales/minute",
        "https://download.openstreetmap.fr/replication/oceania/./australia/new_south_wales/minute/",
    ] {
        assert_eq!(source::replication_url(value).unwrap(), expected);
    }
    for value in [
        "https://download.openstreetmap.fr.evil.test/replication/asia/china/minute",
        "https://user@download.openstreetmap.fr/replication/asia/china/minute",
        "https://download.openstreetmap.fr:443/replication/asia/china/minute",
        "https://download.openstreetmap.fr/replication/asia/china/minute?x=1",
        "https://download.openstreetmap.fr/replication/asia/china/minute#state",
        "https://download.openstreetmap.fr/replication/asia/../china/minute",
        "https://download.openstreetmap.fr/replication/asia/%2e/china/minute",
        "https://download.openstreetmap.fr/replication/asia//china/minute",
        "https://download.openstreetmap.fr/replication/Asia/china/minute",
        "https://download.openstreetmap.fr/replication/asia/china/hour",
        "https://download.openstreetmap.fr/replication/minute",
        "https://download.openstreetmap.fr/extracts/asia/china/minute",
        "https://download.openstreetmap.fr/replication/asia\\china/minute",
        "ftp://download.openstreetmap.fr/replication/asia/china/minute",
        "https://download.geofabrik.de/europe/./france-updates",
    ] {
        assert!(source::replication_url(value).is_err(), "{value}");
    }
}

#[test]
fn source_identity_binds_provider_to_canonical_url_and_matching_downloads() {
    let mut identity = source::SourceIdentity::from_replication_url(
        "http://download.openstreetmap.fr/replication/./asia/china/hong_kong/minute",
    )
    .unwrap();
    assert_eq!(identity.provider, source::SourceProvider::OsmFrance);
    assert_eq!(
        identity.source_url().unwrap(),
        "https://download.openstreetmap.fr/extracts/asia/china/hong_kong.osm.pbf"
    );
    assert_eq!(
        identity.checksum_url().unwrap(),
        "https://download.openstreetmap.fr/extracts/asia/china/hong_kong.osm.pbf.md5"
    );
    assert_eq!(
        serde_json::to_value(&identity).unwrap(),
        json!({"provider":"osm-fr","replicationUrl":"https://download.openstreetmap.fr/replication/asia/china/hong_kong/minute"})
    );
    identity.provider = source::SourceProvider::Geofabrik;
    assert!(identity.validate().is_err());
    assert!(identity.source_url().is_err());
    let identity = source::SourceIdentity::from_replication_url(
        "http://download.geofabrik.de/europe/france-updates/",
    )
    .unwrap();
    assert_eq!(identity.provider, source::SourceProvider::Geofabrik);
    assert_eq!(
        identity.checksum_url().unwrap(),
        "https://download.geofabrik.de/europe/france-latest.osm.pbf.md5"
    );
}

#[test]
fn source_identity_requires_bounded_nonempty_extract_segments() {
    for (provider, prefix, suffix) in [
        (
            source::SourceProvider::Geofabrik,
            "https://download.geofabrik.de/",
            "-updates",
        ),
        (
            source::SourceProvider::OsmFrance,
            "https://download.openstreetmap.fr/replication/",
            "/minute",
        ),
    ] {
        for extract in ["a".repeat(512), "region/extract".into()] {
            source::SourceIdentity {
                provider,
                replication_url: format!("{prefix}{extract}{suffix}"),
            }
            .validate()
            .unwrap();
        }
        for extract in [
            "a".repeat(513),
            "region//extract".into(),
            "region/".into(),
            "/region".into(),
        ] {
            let identity = source::SourceIdentity {
                provider,
                replication_url: format!("{prefix}{extract}{suffix}"),
            };
            assert!(identity.validate().is_err());
            assert!(
                source::SourceIdentity::from_replication_url(&identity.replication_url).is_err()
            );
        }
    }
    assert!(
        serde_json::from_value::<source::SourceIdentity>(json!({
            "provider":"geofabrik",
            "replicationUrl":"https://download.geofabrik.de/europe/france-updates",
            "unexpected":true,
        }))
        .is_err()
    );
}

#[test]
fn admitted_fallback_prepares_fixed_coverage_without_primary_catalog() {
    let work = common::work("source-admission-");
    let path = work.path().join("fallback-sources.json");
    let regions = [source::Region {
        id: "configured-region".into(),
        extract: "region/primary".into(),
    }];
    assert!(
        source::fallback_sources(&path, &regions)
            .unwrap()
            .is_empty()
    );
    let coverage = json!({"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,0]]]});
    fs::write(
        &path,
        serde_json::to_vec(&json!({"regions":[{
            "region":"configured-region",
            "geofabrikExtract":"region/primary",
            "osmFranceExtract":"region/secondary",
            "coverage":coverage,
        }]}))
        .unwrap(),
    )
    .unwrap();
    let sources = source::fallback_sources(&path, &regions).unwrap();
    let fallback = source::fallback_for(&sources, &regions[0]).unwrap();
    let output = work.path().join("candidate");
    source::prepare_fallback(fallback, &output).unwrap();
    assert_eq!(
        fs::read_to_string(output.join("source.url")).unwrap(),
        "https://download.openstreetmap.fr/extracts/region/secondary.osm.pbf\n"
    );
    assert_eq!(
        fs::read_to_string(output.join("updates.url")).unwrap(),
        "https://download.openstreetmap.fr/replication/region/secondary/minute\n"
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &fs::read(output.join("coverage.json")).unwrap()
        )
        .unwrap(),
        coverage
    );
    assert!(
        source::fallback_for(
            &sources,
            &source::Region {
                id: "configured-region".into(),
                extract: "region/other".into(),
            }
        )
        .is_none()
    );
}

#[test]
fn fallback_admission_rejects_unmatched_regions_unsafe_paths_and_invalid_coverage() {
    let work = common::work("source-invalid-admission-");
    let path = work.path().join("fallback-sources.json");
    let regions = [source::Region {
        id: "configured-region".into(),
        extract: "region/primary".into(),
    }];
    let valid = json!({
        "region":"configured-region",
        "geofabrikExtract":"region/primary",
        "osmFranceExtract":"region/secondary",
        "coverage":{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,0]]]},
    });
    for (field, value) in [
        ("region", json!("unconfigured-region")),
        ("geofabrikExtract", json!("region/other")),
        ("osmFranceExtract", json!("https://evil.test/file")),
        ("osmFranceExtract", json!("region/../secondary")),
        ("osmFranceExtract", json!("region/secondary?query=1")),
        ("osmFranceExtract", json!("region/secondary#fragment")),
        ("coverage", json!({"type":"Polygon","coordinates":[]})),
        ("unexpected", json!(true)),
    ] {
        let mut invalid = valid.clone();
        invalid[field] = value;
        fs::write(
            &path,
            serde_json::to_vec(&json!({"regions":[invalid]})).unwrap(),
        )
        .unwrap();
        assert!(
            source::fallback_sources(&path, &regions).is_err(),
            "{field}"
        );
    }
    fs::write(
        &path,
        serde_json::to_vec(&json!({"regions":[valid,valid]})).unwrap(),
    )
    .unwrap();
    assert!(source::fallback_sources(&path, &regions).is_err());
}

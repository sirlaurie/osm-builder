use std::{collections::HashSet, fs, io::Read, path::Path};

use anyhow::{Context, Result, ensure};
use chrono::{DateTime, SecondsFormat};
use md5::Md5;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{
    compute::read_pbf_header,
    format::{is_region, normalize_timestamp},
    storage::atomic_write,
};

const MAX_SEQUENCE: u64 = 9_007_199_254_740_991;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Region {
    pub id: String,
    pub extract: String,
}

impl Region {
    pub fn source_url(&self) -> String {
        format!(
            "https://download.geofabrik.de/{}-latest.osm.pbf",
            self.extract
        )
    }

    pub fn updates_url(&self) -> String {
        format!("https://download.geofabrik.de/{}-updates", self.extract)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicationState {
    pub sequence: u64,
    pub timestamp: String,
}

#[derive(Clone, Debug)]
pub struct Snapshot {
    pub timestamp: String,
    pub sequence: u64,
    pub sha256: String,
    pub replication_url: String,
}

fn lower_digit(byte: u8) -> bool {
    byte.is_ascii_lowercase() || byte.is_ascii_digit()
}

pub(crate) fn valid_extract(extract: &str) -> bool {
    extract.len() <= 512
        && extract.split('/').all(|part| {
            !part.is_empty() && part.bytes().all(|byte| lower_digit(byte) || byte == b'-')
        })
}

pub fn regions(path: &Path) -> Result<Vec<Region>> {
    #[derive(Deserialize)]
    struct Configuration {
        regions: Vec<Region>,
    }
    let config: Configuration = serde_json::from_slice(&fs::read(path)?)?;
    ensure!(
        (1..=256).contains(&config.regions.len()),
        "Configure between 1 and 256 regional jobs"
    );
    let mut ids = HashSet::new();
    let mut extracts = HashSet::new();
    for entry in &config.regions {
        ensure!(
            is_region(&entry.id) && ids.insert(&entry.id),
            "Invalid or duplicate region id"
        );
        ensure!(
            valid_extract(&entry.extract),
            "Invalid extract path: {}",
            entry.extract
        );
        ensure!(
            extracts.insert(&entry.extract),
            "Duplicate extract: {}",
            entry.extract
        );
    }
    Ok(config.regions)
}

pub fn prepare(entry: &Region, index: &Path, output: &Path) -> Result<()> {
    let catalog: Value = serde_json::from_slice(&fs::read(index)?)?;
    let features = catalog["features"]
        .as_array()
        .context("Invalid Geofabrik catalog")?;
    let url = entry.source_url();
    let matches: Vec<_> = features
        .iter()
        .filter(|feature| feature["properties"]["urls"]["pbf"].as_str() == Some(url.as_str()))
        .collect();
    ensure!(
        matches.len() == 1,
        "Source or coverage not present in the Geofabrik catalog"
    );
    let feature = matches[0];
    ensure!(
        matches!(
            feature["geometry"]["type"].as_str(),
            Some("Polygon" | "MultiPolygon")
        ),
        "Source or coverage not present in the Geofabrik catalog"
    );
    let updates = replication_url(
        feature["properties"]["urls"]["updates"]
            .as_str()
            .context("Missing regional replication URL")?,
    )?;
    ensure!(
        updates == entry.updates_url(),
        "Catalog replication URL does not match the configured extract"
    );
    fs::create_dir_all(output)?;
    atomic_write(
        &output.join("coverage.json"),
        &serde_json::to_vec(&feature["geometry"])?,
    )?;
    atomic_write(&output.join("source.url"), format!("{url}\n").as_bytes())?;
    atomic_write(
        &output.join("source-catalog.json"),
        &serde_json::to_vec_pretty(&feature["properties"])?,
    )?;
    atomic_write(
        &output.join("updates.url"),
        format!("{updates}\n").as_bytes(),
    )?;
    Ok(())
}

pub fn replication_url(value: &str) -> Result<String> {
    let raw_path = value
        .strip_prefix("https://download.geofabrik.de")
        .or_else(|| value.strip_prefix("http://download.geofabrik.de"))
        .context("Invalid Geofabrik replication URL")?;
    ensure!(
        raw_path.starts_with('/')
            && raw_path
                .bytes()
                .all(|byte| lower_digit(byte) || matches!(byte, b'/' | b'-')),
        "Invalid Geofabrik replication URL"
    );
    let url = reqwest::Url::parse(value).context("Invalid Geofabrik replication URL")?;
    let path = url.path().trim_end_matches('/');
    ensure!(
        matches!(url.scheme(), "http" | "https")
            && url.host_str() == Some("download.geofabrik.de")
            && url.username().is_empty()
            && url.password().is_none()
            && url.port().is_none()
            && url.query().is_none()
            && url.fragment().is_none()
            && path.starts_with('/')
            && path.len() > "-updates".len() + 1
            && path.ends_with("-updates")
            && path
                .bytes()
                .all(|byte| lower_digit(byte) || matches!(byte, b'/' | b'-')),
        "Invalid Geofabrik replication URL"
    );
    Ok(format!("https://download.geofabrik.de{path}"))
}

pub fn replication_state(value: &str) -> Result<ReplicationState> {
    let mut fields = std::collections::HashMap::new();
    for line in value
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
    {
        let (key, content) = line
            .split_once('=')
            .context("Malformed replication state")?;
        ensure!(
            fields.insert(key, content.replace("\\:", ":")).is_none(),
            "Malformed replication state"
        );
    }
    let sequence = fields
        .get("sequenceNumber")
        .context("Invalid replication state sequence")?;
    ensure!(
        !sequence.is_empty() && sequence.bytes().all(|byte| byte.is_ascii_digit()),
        "Invalid replication state sequence"
    );
    let sequence = sequence.parse::<u64>()?;
    ensure!(
        sequence <= MAX_SEQUENCE,
        "Invalid replication state sequence"
    );
    let timestamp = normalize_timestamp(
        fields
            .get("timestamp")
            .context("Missing replication timestamp")?,
    )?;
    Ok(ReplicationState {
        sequence,
        timestamp,
    })
}

pub fn sequence_path(sequence: u64) -> Result<String> {
    ensure!(
        sequence <= 999_999_999,
        "Replication sequence must fit the nine-digit download path"
    );
    let number = format!("{sequence:09}");
    Ok(format!(
        "{}/{}/{}",
        &number[..3],
        &number[3..6],
        &number[6..]
    ))
}

pub fn stamp(pbf: &Path, checksum: &Path, expected_url: &str) -> Result<Snapshot> {
    let checksum = fs::read_to_string(checksum)?;
    let expected = checksum
        .split_whitespace()
        .next()
        .context("Invalid Geofabrik MD5 file")?
        .to_ascii_lowercase();
    ensure!(
        expected.len() == 32 && expected.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "Invalid Geofabrik MD5 file"
    );
    let mut file = fs::File::open(pbf)?;
    let mut md5 = Md5::new();
    let mut sha256 = Sha256::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let length = file.read(&mut buffer)?;
        if length == 0 {
            break;
        }
        md5.update(&buffer[..length]);
        sha256.update(&buffer[..length]);
    }
    ensure!(
        format!("{:x}", md5.finalize()) == expected,
        "PBF checksum mismatch; download this snapshot again"
    );
    let header = read_pbf_header(pbf)?;
    let timestamp = header
        .osmosis_replication_timestamp
        .context("PBF has no source replication timestamp")?;
    let timestamp = DateTime::from_timestamp(timestamp, 0)
        .context("Invalid PBF source replication timestamp")?
        .to_rfc3339_opts(SecondsFormat::Secs, true);
    let timestamp = normalize_timestamp(&timestamp)?;
    let sequence = u64::try_from(
        header
            .osmosis_replication_sequence_number
            .context("PBF has no valid regional replication sequence")?,
    )?;
    ensure!(
        sequence <= MAX_SEQUENCE,
        "Source replication sequence exceeds JSON safe integer range"
    );
    let url = replication_url(
        header
            .osmosis_replication_base_url
            .as_deref()
            .context("PBF has no regional replication URL")?,
    )?;
    ensure!(
        url == replication_url(expected_url)?,
        "PBF replication URL does not match the configured extract"
    );
    Ok(Snapshot {
        timestamp,
        sequence,
        sha256: format!("{:x}", sha256.finalize()),
        replication_url: url,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn work() -> tempfile::TempDir {
        let scratch = Path::new(env!("CARGO_MANIFEST_DIR")).join(".build/tests");
        fs::create_dir_all(&scratch).unwrap();
        tempfile::Builder::new()
            .prefix("source-")
            .tempdir_in(scratch)
            .unwrap()
    }

    #[test]
    fn configured_catalog_is_valid() {
        regions(&Path::new(env!("CARGO_MANIFEST_DIR")).join("config/regions.json")).unwrap();
    }

    #[test]
    fn region_config_rejects_duplicate_ids_extracts_and_unsafe_paths() {
        let work = work();
        let path = work.path().join("regions.json");
        for entries in [
            serde_json::json!([]),
            serde_json::json!([{"id":"france","extract":"europe/france"},{"id":"france","extract":"europe/germany"}]),
            serde_json::json!([{"id":"france","extract":"europe/france"},{"id":"other","extract":"europe/france"}]),
            serde_json::json!([{"id":"../escape","extract":"europe/france"}]),
            serde_json::json!([{"id":"china","extract":"asia/../china"}]),
        ] {
            fs::write(
                &path,
                serde_json::to_vec(&serde_json::json!({"regions":entries})).unwrap(),
            )
            .unwrap();
            assert!(regions(&path).is_err());
        }
    }

    #[test]
    fn prepare_requires_unique_source_and_matching_replication_url() {
        let work = work();
        let index = work.path().join("index.json");
        let output = work.path().join("job");
        let region = Region {
            id: "france".into(),
            extract: "europe/france".into(),
        };
        let feature = serde_json::json!({"properties":{"urls":{"pbf":region.source_url(),"updates":region.updates_url()}},"geometry":{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,0]]]}});
        fs::write(
            &index,
            serde_json::to_vec(&serde_json::json!({"features":[feature]})).unwrap(),
        )
        .unwrap();
        prepare(&region, &index, &output).unwrap();
        assert_eq!(
            fs::read_to_string(output.join("source.url"))
                .unwrap()
                .trim(),
            region.source_url()
        );
        let mut wrong = feature.clone();
        wrong["properties"]["urls"]["updates"] =
            serde_json::json!("https://download.geofabrik.de/europe/germany-updates");
        for features in [
            serde_json::json!([feature, feature]),
            serde_json::json!([wrong]),
            serde_json::json!([]),
        ] {
            fs::write(
                &index,
                serde_json::to_vec(&serde_json::json!({"features":features})).unwrap(),
            )
            .unwrap();
            assert!(prepare(&region, &index, &output).is_err());
        }
    }

    #[test]
    fn region_config_accepts_safe_extract_paths_without_a_geographic_allowlist() {
        let work = work();
        let path = work.path().join("regions.json");
        for extract in [
            "europe/germany/bayern",
            "russia/central-fed-district",
            "north-america/us/california",
            "north-america/canada/ontario",
            "australia-oceania/australia/new-south-wales",
            "asia/japan/kanto",
            "asia/taiwan",
            "africa/canary-islands",
            "asia/china/jiangsu",
            "asia/china",
            "south-america/brazil",
            "africa/kenya",
            "north-america/us",
            "asia/japan",
            "europe",
        ] {
            fs::write(
                &path,
                serde_json::to_vec(
                    &serde_json::json!({"regions":[{"id":"configured-region","extract":extract}]}),
                )
                .unwrap(),
            )
            .unwrap();
            let entries = regions(&path).unwrap_or_else(|error| panic!("{extract}: {error}"));
            assert_eq!(entries[0].extract, extract);
        }
        for extract in [
            "",
            "/europe/france",
            "europe/france/",
            "europe//france",
            "europe/../asia",
            "europe/France",
            "europe/france?query=1",
            "https://example.com/europe/france",
        ] {
            assert!(!valid_extract(extract), "{extract}");
        }
    }

    #[test]
    fn state_uses_regional_sequence_and_utc_timestamp() {
        let state = replication_state("# planet sequenceNumber=987654321\nsequenceNumber=42\ntimestamp=2025-01-02T03\\:04\\:05+02\\:00\n").unwrap();
        assert_eq!(state.sequence, 42);
        assert_eq!(state.timestamp, "2025-01-02T01:04:05Z");
        for invalid in [
            "sequenceNumber=1\nsequenceNumber=2\ntimestamp=2025-01-01T00:00:00Z",
            "sequenceNumber=-1\ntimestamp=2025-01-01T00:00:00Z",
            "sequenceNumber=9007199254740992\ntimestamp=2025-01-01T00:00:00Z",
            "sequenceNumber=1\ntimestamp=2025-01-01T00:00:00",
            "sequenceNumber=1\ntimestamp=2999-01-01T00:00:00Z",
        ] {
            assert!(replication_state(invalid).is_err());
        }
    }

    #[test]
    fn replication_urls_and_download_paths_are_bounded() {
        assert_eq!(
            replication_url("http://download.geofabrik.de/europe/france-updates/").unwrap(),
            "https://download.geofabrik.de/europe/france-updates"
        );
        for invalid in [
            "https://evil.test/europe/france-updates",
            "https://download.geofabrik.de/europe/france-updates?q=1",
            "https://user@download.geofabrik.de/europe/france-updates",
            "file:///europe/france-updates",
        ] {
            assert!(replication_url(invalid).is_err());
        }
        assert_eq!(sequence_path(42).unwrap(), "000/000/042");
        assert_eq!(sequence_path(999_999_999).unwrap(), "999/999/999");
        assert!(sequence_path(1_000_000_000).is_err());
    }
}

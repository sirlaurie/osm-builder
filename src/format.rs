use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{self, Write},
    path::Path,
    sync::LazyLock,
};

use anyhow::{Context, Result, ensure};
use chrono::{DateTime, SecondsFormat, Utc};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

pub const MAX_ID: u64 = 9_007_199_254_740_991;
pub const MAX_BLOCK: usize = 262_144;
pub const MAX_MANIFEST: usize = 8 * 1024 * 1024;
pub const MAX_CURRENT: usize = 1024 * 1024;
pub const MAX_REGIONS: usize = 256;
pub const MAX_COVERAGE_POINTS: usize = 100_000;

static NAME_KEY: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)^name:[a-z]{2,3}(?:[-_][a-z0-9]+)*$").expect("static name expression")
});
static UUID: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$")
        .expect("static UUID expression")
});
static TIMESTAMP: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{3})?Z$")
        .expect("static timestamp expression")
});

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Poi {
    pub id: String,
    pub lat: f64,
    pub lon: f64,
    pub tags: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Manifest {
    pub schema: u8,
    pub region: String,
    pub source_timestamp: String,
    pub source_sequence: Option<u64>,
    #[serde(rename = "sourceSHA256")]
    pub source_sha256: String,
    pub coverage: Value,
    pub cells: BTreeMap<String, Vec<String>>,
    pub count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub excluded_incomplete_relation_count: Option<u64>,
    #[serde(
        default,
        rename = "replicationDiffSHA256",
        skip_serializing_if = "Option::is_none"
    )]
    pub replication_diff_sha256: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Release {
    pub region: String,
    pub manifest: String,
    pub source_timestamp: String,
    pub count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub excluded_incomplete_relation_count: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RegionRelease {
    pub region: String,
    pub manifest: String,
    pub source_timestamp: String,
    pub bbox: [f64; 4],
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Current {
    pub schema: u8,
    pub revision: String,
    pub regions: Vec<RegionRelease>,
}

pub fn hash_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub fn is_hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub fn is_region(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.bytes().enumerate().all(|(index, byte)| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || (index > 0 && matches!(byte, b'_' | b'-'))
        })
}

pub fn has_name(tags: &BTreeMap<String, String>) -> bool {
    tags.iter().any(|(key, value)| {
        !value.trim().is_empty() && (key == "name" || key == "int_name" || NAME_KEY.is_match(key))
    })
}

pub fn is_poi(tags: &BTreeMap<String, String>) -> bool {
    has_name(tags)
        && (tags.get("amenity").is_some_and(|value| {
            matches!(
                value.as_str(),
                "restaurant"
                    | "fast_food"
                    | "food_court"
                    | "cafe"
                    | "bar"
                    | "pub"
                    | "biergarten"
                    | "bank"
                    | "atm"
                    | "pharmacy"
                    | "clinic"
                    | "hospital"
                    | "doctors"
                    | "dentist"
                    | "school"
                    | "university"
                    | "college"
                    | "kindergarten"
                    | "library"
                    | "parking"
                    | "fuel"
                    | "charging_station"
                    | "bus_station"
                    | "cinema"
                    | "theatre"
            )
        }) || ["shop", "railway", "tourism", "leisure", "healthcare"]
            .iter()
            .any(|key| tags.get(*key).is_some_and(|value| !value.is_empty()))
            || tags
                .get("highway")
                .is_some_and(|value| matches!(value.as_str(), "bus_stop" | "platform")))
}

pub fn normalize_timestamp(value: &str) -> Result<String> {
    let parsed = DateTime::parse_from_rfc3339(value)
        .context("Source timestamp must include a valid timezone")?;
    ensure!(
        parsed.timestamp_subsec_nanos() < 1_000_000_000,
        "Leap-second source timestamps are unsupported"
    );
    ensure!(
        parsed <= Utc::now() + chrono::Duration::minutes(5),
        "Source timestamp is more than five minutes in the future"
    );
    Ok(parsed
        .with_timezone(&Utc)
        .to_rfc3339_opts(SecondsFormat::Secs, true))
}

fn valid_timestamp(value: &str) -> bool {
    TIMESTAMP.is_match(value)
        && DateTime::parse_from_rfc3339(value).is_ok_and(|time| {
            time.timestamp_subsec_nanos() < 1_000_000_000
                && time.to_rfc3339_opts(
                    if value.contains('.') {
                        SecondsFormat::Millis
                    } else {
                        SecondsFormat::Secs
                    },
                    true,
                ) == value
        })
}

pub fn validate_coverage(value: &Value) -> Result<()> {
    coverage_bounds(value).map(|_| ())
}

pub fn coverage_bounds(value: &Value) -> Result<[f64; 4]> {
    let coordinates = value
        .get("coordinates")
        .and_then(Value::as_array)
        .context("Coverage has no coordinates")?;
    let polygons: Vec<&Vec<Value>> = match value.get("type").and_then(Value::as_str) {
        Some("Polygon") => vec![coordinates],
        Some("MultiPolygon") => coordinates
            .iter()
            .map(|value| value.as_array().context("Invalid coverage polygon"))
            .collect::<Result<_>>()?,
        _ => anyhow::bail!("Coverage must be Polygon or MultiPolygon"),
    };
    ensure!(!polygons.is_empty(), "Coverage has no polygons");
    let mut count = 0;
    let mut bounds: [f64; 4] = [180.0, 90.0, -180.0, -90.0];
    for polygon in polygons {
        ensure!(!polygon.is_empty(), "Coverage polygon has no rings");
        for ring in polygon {
            let ring = ring.as_array().context("Invalid coverage ring")?;
            ensure!(
                ring.len() >= 4,
                "Coverage ring needs at least four positions"
            );
            count += ring.len();
            ensure!(
                count <= MAX_COVERAGE_POINTS,
                "Coverage exceeds {MAX_COVERAGE_POINTS} positions"
            );
            let mut first = None;
            let mut previous = None;
            let mut area = 0.0;
            for point in ring {
                let point = point
                    .as_array()
                    .context("Coverage position must be two numbers")?;
                ensure!(point.len() == 2, "Coverage position must be two numbers");
                let x = point[0].as_f64().context("Invalid coverage longitude")?;
                let y = point[1].as_f64().context("Invalid coverage latitude")?;
                ensure!(
                    x.is_finite()
                        && y.is_finite()
                        && (-180.0..=180.0).contains(&x)
                        && (-90.0..=90.0).contains(&y),
                    "Coverage coordinate is outside geographic bounds"
                );
                if let Some((px, py)) = previous {
                    area += px * y - x * py;
                }
                first.get_or_insert((x, y));
                previous = Some((x, y));
                bounds = [
                    bounds[0].min(x),
                    bounds[1].min(y),
                    bounds[2].max(x),
                    bounds[3].max(y),
                ];
            }
            ensure!(
                first == previous && area != 0.0,
                "Coverage ring must be closed with nonzero area"
            );
        }
    }
    Ok(bounds)
}

pub fn read_coverage(path: &Path) -> Result<Value> {
    let value: Value = serde_json::from_slice(&fs::read(path)?).context("Invalid coverage JSON")?;
    let value = if value.get("type").and_then(Value::as_str) == Some("Feature") {
        value
            .get("geometry")
            .context("Coverage feature has no geometry")?
            .clone()
    } else {
        value
    };
    validate_coverage(&value)?;
    Ok(json!({ "type": value["type"], "coordinates": value["coordinates"] }))
}

pub fn source_metadata(
    coverage_path: &Path,
    region: &str,
    timestamp: &str,
    sequence: Option<u64>,
    sha: &str,
) -> Result<Value> {
    ensure!(is_region(region), "Invalid region identifier");
    ensure!(
        sequence.is_none_or(|sequence| sequence <= MAX_ID),
        "Source sequence exceeds JSON safe integer range"
    );
    ensure!(
        is_hash(sha),
        "Source SHA-256 must be 64 lowercase hexadecimal characters"
    );
    Ok(
        json!({"region": region, "sourceTimestamp": normalize_timestamp(timestamp)?,
        "sourceSequence": sequence, "sourceSHA256": sha, "coverage": read_coverage(coverage_path)?}),
    )
}

pub fn validate_release(value: &Value) -> Result<Release> {
    let release: Release = serde_json::from_value(value.clone()).context("Invalid release.json")?;
    ensure!(
        is_region(&release.region)
            && is_hash(&release.manifest)
            && valid_timestamp(&release.source_timestamp)
            && release.count <= MAX_ID
            && release
                .excluded_incomplete_relation_count
                .is_none_or(|count| count <= MAX_ID),
        "Invalid release.json"
    );
    Ok(release)
}

pub fn validate_manifest(value: &Value) -> Result<Manifest> {
    ensure!(
        value.get("sourceSequence").is_some(),
        "Manifest has no sourceSequence"
    );
    let manifest: Manifest = serde_json::from_value(value.clone()).context("Invalid manifest")?;
    ensure!(
        manifest.schema == 1
            && is_region(&manifest.region)
            && valid_timestamp(&manifest.source_timestamp)
            && manifest
                .source_sequence
                .is_none_or(|sequence| sequence <= MAX_ID)
            && is_hash(&manifest.source_sha256)
            && manifest.count <= MAX_ID
            && manifest
                .excluded_incomplete_relation_count
                .is_none_or(|count| count <= MAX_ID)
            && manifest
                .replication_diff_sha256
                .as_deref()
                .is_none_or(is_hash),
        "Invalid manifest"
    );
    validate_coverage(&manifest.coverage)?;
    for (cell, pages) in &manifest.cells {
        let (y, x) = cell.split_once('_').context("Invalid manifest cell")?;
        let valid_index = |value: &str, maximum: u32| -> bool {
            !value.is_empty()
                && value.bytes().all(|byte| byte.is_ascii_digit())
                && (value.len() == 1 || !value.starts_with('0'))
                && value.parse::<u32>().is_ok_and(|n| n <= maximum)
        };
        ensure!(
            valid_index(y, 17_999)
                && valid_index(x, 35_999)
                && !pages.is_empty()
                && pages.iter().all(|hash| is_hash(hash))
                && pages.iter().collect::<BTreeSet<_>>().len() == pages.len(),
            "Invalid manifest cell"
        );
    }
    ensure!(
        manifest.cells.is_empty() == (manifest.count == 0),
        "Manifest cells do not match count"
    );
    Ok(manifest)
}

pub fn validate_current(value: &Value) -> Result<Current> {
    let current: Current =
        serde_json::from_value(value.clone()).context("Invalid current state")?;
    ensure!(
        current.schema == 1
            && UUID.is_match(&current.revision)
            && current.regions.len() <= MAX_REGIONS,
        "Invalid current state"
    );
    let mut ids = BTreeSet::new();
    for region in &current.regions {
        let [west, south, east, north] = region.bbox;
        ensure!(
            is_region(&region.region)
                && ids.insert(&region.region)
                && is_hash(&region.manifest)
                && valid_timestamp(&region.source_timestamp)
                && [-180.0..=180.0, -90.0..=90.0, -180.0..=180.0, -90.0..=90.0]
                    .iter()
                    .zip(region.bbox)
                    .all(|(range, value)| value.is_finite() && range.contains(&value))
                && west <= east
                && south <= north,
            "Invalid current region"
        );
    }
    Ok(current)
}

pub fn validate_block(value: &Value) -> Result<Vec<Poi>> {
    let pois: Vec<Poi> = serde_json::from_value(value.clone()).context("Invalid POI block")?;
    let mut ids = BTreeSet::new();
    for poi in &pois {
        let mut parts = poi.id.split('_');
        ensure!(
            parts.next() == Some("osm")
                && matches!(parts.next(), Some("node" | "way" | "relation")),
            "Invalid POI identifier"
        );
        let id = parts.next().context("Missing OSM identifier")?;
        ensure!(
            !id.starts_with('0')
                && !id.is_empty()
                && id.bytes().all(|byte| byte.is_ascii_digit())
                && id.parse::<u64>().is_ok_and(|number| number <= MAX_ID)
                && parts.next().is_none()
                && ids.insert(&poi.id)
                && poi.lat.is_finite()
                && poi.lon.is_finite()
                && (-90.0..=90.0).contains(&poi.lat)
                && (-180.0..=180.0).contains(&poi.lon),
            "Invalid POI"
        );
    }
    Ok(pois)
}

struct CanonicalFormatter;

impl serde_json::ser::Formatter for CanonicalFormatter {
    fn write_f64<W: ?Sized + Write>(&mut self, writer: &mut W, value: f64) -> io::Result<()> {
        let text = format!("{value:?}");
        if let Some((mantissa, exponent)) = text.split_once('e') {
            let exponent: i32 = exponent.parse().map_err(io::Error::other)?;
            write!(writer, "{mantissa}e{exponent:+03}")
        } else {
            writer.write_all(text.as_bytes())
        }
    }
}

pub fn canonical_json(value: &Value) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut serializer = serde_json::Serializer::with_formatter(&mut bytes, CanonicalFormatter);
    value.serialize(&mut serializer)?;
    Ok(bytes)
}

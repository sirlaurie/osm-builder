use aura_osm::build::{self, BuildOptions, ComputeOptions};
use std::{fs, io::Write, path::Path};

fn varint(mut value: u64) -> Vec<u8> {
    let mut bytes = Vec::new();
    while value >= 128 {
        bytes.push((value as u8 & 127) | 128);
        value >>= 7;
    }
    bytes.push(value as u8);
    bytes
}

fn bytes(field: u64, value: &[u8]) -> Vec<u8> {
    [
        varint(field * 8 + 2),
        varint(value.len() as u64),
        value.to_vec(),
    ]
    .concat()
}

fn number(field: u64, value: u64) -> Vec<u8> {
    [varint(field * 8), varint(value)].concat()
}

fn packed(field: u64, values: &[u64]) -> Vec<u8> {
    bytes(
        field,
        &values
            .iter()
            .flat_map(|value| varint(*value))
            .collect::<Vec<_>>(),
    )
}

fn blob(kind: &str, payload: &[u8]) -> Vec<u8> {
    framed_blob(kind, bytes(1, payload))
}

fn framed_blob(kind: &str, body: Vec<u8>) -> Vec<u8> {
    let header = [bytes(1, kind.as_bytes()), number(3, body.len() as u64)].concat();
    [(header.len() as u32).to_be_bytes().to_vec(), header, body].concat()
}

fn build_group(group: &[u8]) -> anyhow::Result<serde_json::Value> {
    build_file(&blob("OSMData", &primitive_block(group)))
}

fn primitive_block(group: &[u8]) -> Vec<u8> {
    let strings = ["", "amenity", "cafe", "name", "Cafe", "extra"]
        .iter()
        .flat_map(|value| bytes(1, value.as_bytes()))
        .collect::<Vec<_>>();
    [bytes(1, &strings), bytes(2, group)].concat()
}

fn build_file(data_blob: &[u8]) -> anyhow::Result<serde_json::Value> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join(".build/tools/tests");
    fs::create_dir_all(&root)?;
    let work = tempfile::Builder::new()
        .prefix("pbf-integrity-")
        .tempdir_in(root)?;
    let header = bytes(4, b"OsmSchema-V0.6");
    let input = work.path().join("source.osm.pbf");
    fs::write(
        &input,
        [blob("OSMHeader", &header), data_blob.to_vec()].concat(),
    )?;
    let coverage = work.path().join("coverage.json");
    fs::write(
        &coverage,
        r#"{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,1],[0,0]]]}"#,
    )?;
    build::run(
        &BuildOptions {
            input,
            coverage,
            region: "test".into(),
            source_timestamp: "2025-01-01T00:00:00Z".into(),
            source_sequence: Some(1),
            source_sha256: "a".repeat(64),
            output: work.path().join("output"),
            scratch: work.path().join("scratch"),
        },
        &ComputeOptions {
            workers: 1,
            batch_size: 2,
            pending_batches: 1,
            batch_bytes: 4096,
            sqlite_cache_mib: 16,
        },
    )
}

fn valid_node() -> Vec<u8> {
    [
        number(1, 2),
        packed(2, &[1, 3]),
        packed(3, &[2, 4]),
        number(8, 10_000_000),
        number(9, 10_000_000),
    ]
    .concat()
}

#[test]
fn valid_raw_and_zlib_blocks_produce_the_same_manifest() {
    let group = bytes(1, &valid_node());
    let raw = build_group(&group).unwrap();
    let block = primitive_block(&group);
    let mut compressed =
        flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    compressed.write_all(&block).unwrap();
    let compressed = framed_blob(
        "OSMData",
        [
            number(2, block.len() as u64),
            bytes(3, &compressed.finish().unwrap()),
        ]
        .concat(),
    );
    assert_eq!(raw["count"], 1);
    assert_eq!(raw, build_file(&compressed).unwrap());
}

#[test]
fn malformed_tag_values_and_coordinates_cannot_be_hidden_after_shorter_arrays() {
    let node = [
        number(1, 2),
        packed(2, &[1, 3]),
        packed(3, &[2, 4, 5]),
        number(8, 10_000_000),
        number(9, 10_000_000),
    ]
    .concat();
    assert!(build_group(&bytes(1, &node)).is_err());
    let dense = [
        packed(1, &[2]),
        packed(8, &[10_000_000, 0]),
        packed(9, &[10_000_000, 0]),
        packed(10, &[1, 2, 3, 4, 0]),
    ]
    .concat();
    assert!(build_group(&bytes(2, &dense)).is_err());
}

#[test]
fn dense_tags_require_complete_pairs_and_a_terminator_per_node() {
    for tags in [
        &[1, 2, 3, 4][..],
        &[1, 2, 3, 4, 5][..],
        &[1, 2, 3, 4, 0, 0][..],
        &[1, 2, 3, 4, u64::MAX, 4, 0][..],
    ] {
        let dense = [
            packed(1, &[2]),
            packed(8, &[10_000_000]),
            packed(9, &[10_000_000]),
            packed(10, tags),
        ]
        .concat();
        assert!(
            build_group(&bytes(2, &dense)).is_err(),
            "Invalid dense tags accepted: {tags:?}"
        );
    }
}

#[test]
fn relation_member_metadata_is_validated_even_when_roles_do_not_change_geometry() {
    for (roles, members, types) in [
        (&[0, 0][..], &[2][..], &[0][..]),
        (&[99][..], &[2][..], &[0][..]),
        (&[0][..], &[2][..], &[99][..]),
    ] {
        let relation = [
            number(1, 2),
            packed(2, &[1, 3]),
            packed(3, &[2, 4]),
            packed(8, roles),
            packed(9, members),
            packed(10, types),
        ]
        .concat();
        assert!(build_group(&[bytes(1, &valid_node()), bytes(4, &relation)].concat()).is_err());
    }
}

#[test]
fn coordinate_integer_overflow_is_an_error_instead_of_a_wrapped_location() {
    let node = [
        number(1, 2),
        packed(2, &[1, 3]),
        packed(3, &[2, 4]),
        number(8, u64::MAX - 1),
        number(9, 0),
    ]
    .concat();
    assert!(build_group(&bytes(1, &node)).is_err());
}

#[test]
fn compressed_blob_size_and_complete_stream_are_verified() {
    let block = primitive_block(&bytes(1, &valid_node()));
    let mut compressed =
        flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    compressed.write_all(&block).unwrap();
    let compressed = compressed.finish().unwrap();
    for size in [block.len() - 1, block.len() + 1] {
        let body = [number(2, size as u64), bytes(3, &compressed)].concat();
        assert!(build_file(&framed_blob("OSMData", body)).is_err());
    }
    let truncated = [
        number(2, block.len() as u64),
        bytes(3, &compressed[..compressed.len() - 2]),
    ]
    .concat();
    assert!(build_file(&framed_blob("OSMData", truncated)).is_err());
    let trailing = [
        number(2, block.len() as u64),
        bytes(3, &[compressed, b"extra".to_vec()].concat()),
    ]
    .concat();
    assert!(build_file(&framed_blob("OSMData", trailing)).is_err());
}

#[test]
fn a_blob_cannot_carry_conflicting_encodings() {
    let block = primitive_block(&bytes(1, &valid_node()));
    let body = [
        bytes(1, &block),
        number(2, block.len() as u64),
        bytes(3, b"not-zlib"),
    ]
    .concat();
    assert!(build_file(&framed_blob("OSMData", body)).is_err());
}

#[test]
fn malformed_node_tag_arrays_must_not_drop_trailing_key() {
    let node = [
        number(1, 2),
        packed(2, &[1, 3, 5]),
        packed(3, &[2, 4]),
        number(8, 10_000_000),
        number(9, 10_000_000),
    ]
    .concat();
    assert!(
        build_group(&bytes(1, &node)).is_err(),
        "Unpaired tag key was silently discarded"
    );
}

#[test]
fn malformed_relation_arrays_must_not_drop_missing_member() {
    let node = [number(1, 2), number(8, 10_000_000), number(9, 10_000_000)].concat();
    let relation = [
        number(1, 2),
        packed(2, &[1, 3]),
        packed(3, &[2, 4]),
        packed(8, &[0]),
        packed(9, &[2, 196]),
        packed(10, &[0]),
    ]
    .concat();
    assert!(
        build_group(&[bytes(1, &node), bytes(4, &relation)].concat()).is_err(),
        "Unpaired relation member was silently discarded"
    );
}

#[test]
fn malformed_dense_arrays_must_not_drop_trailing_node() {
    let dense = [
        packed(1, &[2, 2]),
        packed(8, &[10_000_000]),
        packed(9, &[10_000_000]),
        packed(10, &[1, 2, 3, 4, 0]),
    ]
    .concat();
    assert!(
        build_group(&bytes(2, &dense)).is_err(),
        "Unpaired dense node was silently discarded"
    );
}

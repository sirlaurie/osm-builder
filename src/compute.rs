use std::collections::{BTreeMap, VecDeque};
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};

use anyhow::{Context, Result, bail, ensure};
use flate2::read::ZlibDecoder;
use osmpbfreader::{
    fileformat::{Blob, BlobHeader},
    osmformat::{HeaderBlock, PrimitiveBlock},
};
use protobuf::Message;
use quick_xml::Reader;
use quick_xml::events::{BytesStart, Event};
use rayon::ThreadPoolBuilder;

use crate::build::ComputeOptions;
use crate::format::{MAX_ID, canonical_json, is_poi};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum Kind {
    Node,
    Way,
    Relation,
}

impl Kind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Node => "node",
            Self::Way => "way",
            Self::Relation => "relation",
        }
    }

    pub(crate) fn parse(value: &str) -> Result<Self> {
        match value {
            "node" => Ok(Self::Node),
            "way" => Ok(Self::Way),
            "relation" => Ok(Self::Relation),
            _ => bail!("Invalid OSM object type: {value}"),
        }
    }
}

#[derive(Debug)]
pub(crate) struct Object {
    pub kind: Kind,
    pub id: i64,
    pub tags: String,
    pub lat: Option<f64>,
    pub lon: Option<f64>,
    pub poi: bool,
    pub refs: Vec<(Kind, i64)>,
}

impl Object {
    fn new(
        kind: Kind,
        id: i64,
        tags: BTreeMap<String, String>,
        location: Option<(f64, f64)>,
        refs: Vec<(Kind, i64)>,
    ) -> Result<Self> {
        object_id(id)?;
        for (_, reference) in &refs {
            object_id(*reference)?;
        }
        if let Some((lat, lon)) = location {
            coordinate(lat, 90.0)?;
            coordinate(lon, 180.0)?;
        }
        let poi = is_poi(&tags);
        let tags = if poi {
            String::from_utf8(canonical_json(&serde_json::to_value(tags)?)?)?
        } else {
            "{}".to_owned()
        };
        Ok(Self {
            kind,
            id,
            tags,
            lat: location.map(|point| point.0),
            lon: location.map(|point| point.1),
            poi,
            refs,
        })
    }

    fn memory_size(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(self.tags.len())
            .saturating_add(
                self.refs
                    .len()
                    .saturating_mul(std::mem::size_of::<(Kind, i64)>()),
            )
    }
}

pub(crate) fn object_id(value: i64) -> Result<i64> {
    ensure!(
        value > 0 && value as u64 <= MAX_ID,
        "OSM identifier outside safe integer range"
    );
    Ok(value)
}

fn text_id(value: &str) -> Result<i64> {
    ensure!(
        !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()),
        "Invalid OSM identifier"
    );
    object_id(value.parse().context("Invalid OSM identifier")?)
}

fn coordinate(value: f64, limit: f64) -> Result<f64> {
    ensure!(
        value.is_finite() && value.abs() <= limit,
        "Coordinate outside geographic bounds"
    );
    Ok(value)
}

fn pbf_tags(
    indices: impl Iterator<Item = (u32, u32)>,
    strings: &[Vec<u8>],
    budget: usize,
) -> Result<BTreeMap<String, String>> {
    let mut result = BTreeMap::new();
    let mut bytes = 0usize;
    for (key, value) in indices {
        let key = std::str::from_utf8(strings.get(key as usize).context("Invalid PBF tag key")?)?;
        let value = std::str::from_utf8(
            strings
                .get(value as usize)
                .context("Invalid PBF tag value")?,
        )?;
        bytes = bytes
            .saturating_add(key.len())
            .saturating_add(value.len())
            .saturating_add(std::mem::size_of::<(String, String)>());
        ensure!(
            bytes <= budget,
            "OSM object exceeds OSM_COMPUTE_BATCH_BYTES"
        );
        ensure!(
            result.insert(key.to_owned(), value.to_owned()).is_none(),
            "Invalid or duplicate OSM tag"
        );
    }
    Ok(result)
}

fn parallel_tags(
    keys: &[u32],
    values: &[u32],
    strings: &[Vec<u8>],
    budget: usize,
) -> Result<BTreeMap<String, String>> {
    ensure!(
        keys.len() == values.len(),
        "PBF tag key/value length mismatch"
    );
    pbf_tags(
        keys.iter().copied().zip(values.iter().copied()),
        strings,
        budget,
    )
}

fn tag_budget(reference_count: usize, object_budget: usize) -> Result<usize> {
    let reference_bytes = reference_count
        .checked_mul(std::mem::size_of::<(Kind, i64)>())
        .context("OSM reference count exceeds address space")?;
    object_budget
        .checked_sub(std::mem::size_of::<Object>())
        .and_then(|budget| budget.checked_sub(reference_bytes))
        .context("OSM object exceeds OSM_COMPUTE_BATCH_BYTES")
}

fn add_delta(value: &mut i64, delta: i64) -> Result<i64> {
    *value = value
        .checked_add(delta)
        .context("PBF delta integer overflow")?;
    Ok(*value)
}

fn pbf_coordinate(value: i64, offset: i64, granularity: i32) -> Result<f64> {
    let nanos = value
        .checked_mul(i64::from(granularity))
        .and_then(|value| value.checked_add(offset))
        .context("PBF coordinate integer overflow")?;
    Ok(nanos as f64 / 1_000_000_000.0)
}

fn decode_objects(
    block: &PrimitiveBlock,
    budget: usize,
    mut consume: impl FnMut(Object) -> Result<()>,
) -> Result<()> {
    let strings = &block
        .stringtable
        .as_ref()
        .context("PBF string table is missing")?
        .s;
    ensure!(
        strings.first().is_some_and(Vec::is_empty),
        "Invalid PBF empty string entry"
    );
    ensure!(
        block.granularity() > 0,
        "Invalid PBF coordinate granularity"
    );
    let location = |lat, lon| -> Result<(f64, f64)> {
        Ok((
            pbf_coordinate(lat, block.lat_offset(), block.granularity())?,
            pbf_coordinate(lon, block.lon_offset(), block.granularity())?,
        ))
    };
    for group in &block.primitivegroup {
        for node in &group.nodes {
            ensure!(
                node.info
                    .as_ref()
                    .and_then(|info| info.visible)
                    .unwrap_or(true),
                "Deleted objects are not a current OSM snapshot"
            );
            consume(Object::new(
                Kind::Node,
                node.id(),
                parallel_tags(&node.keys, &node.vals, strings, tag_budget(0, budget)?)?,
                Some(location(node.lat(), node.lon())?),
                Vec::new(),
            )?)?;
        }
        if let Some(dense) = group.dense.as_ref() {
            let count = dense.id.len();
            ensure!(
                dense.lat.len() == count && dense.lon.len() == count,
                "PBF dense node coordinate length mismatch"
            );
            if let Some(info) = dense.denseinfo.as_ref() {
                for length in [
                    info.version.len(),
                    info.timestamp.len(),
                    info.changeset.len(),
                    info.uid.len(),
                    info.user_sid.len(),
                    info.visible.len(),
                ] {
                    ensure!(
                        length == 0 || length == count,
                        "PBF dense node metadata length mismatch"
                    );
                }
                ensure!(
                    info.visible.iter().all(|visible| *visible),
                    "Deleted objects are not a current OSM snapshot"
                );
            }
            let (mut id, mut lat, mut lon, mut tag_index) = (0i64, 0i64, 0i64, 0usize);
            for index in 0..count {
                let tag_start = tag_index;
                if !dense.keys_vals.is_empty() {
                    loop {
                        let key = *dense
                            .keys_vals
                            .get(tag_index)
                            .context("Missing PBF dense node tag delimiter")?;
                        tag_index += 1;
                        if key == 0 {
                            break;
                        }
                        let value = *dense
                            .keys_vals
                            .get(tag_index)
                            .context("Missing PBF dense tag value")?;
                        tag_index += 1;
                        ensure!(key > 0 && value >= 0, "Invalid PBF dense tag index");
                    }
                }
                let tag_end = if dense.keys_vals.is_empty() {
                    tag_index
                } else {
                    tag_index - 1
                };
                let tags = dense.keys_vals[tag_start..tag_end]
                    .chunks_exact(2)
                    .map(|pair| (pair[0] as u32, pair[1] as u32));
                consume(Object::new(
                    Kind::Node,
                    add_delta(&mut id, dense.id[index])?,
                    pbf_tags(tags, strings, tag_budget(0, budget)?)?,
                    Some(location(
                        add_delta(&mut lat, dense.lat[index])?,
                        add_delta(&mut lon, dense.lon[index])?,
                    )?),
                    Vec::new(),
                )?)?;
            }
            ensure!(
                tag_index == dense.keys_vals.len(),
                "Extra PBF dense node tags"
            );
        }
        for way in &group.ways {
            ensure!(
                way.info
                    .as_ref()
                    .and_then(|info| info.visible)
                    .unwrap_or(true),
                "Deleted objects are not a current OSM snapshot"
            );
            let tag_budget = tag_budget(way.refs.len(), budget)?;
            let mut reference = 0;
            let refs = way
                .refs
                .iter()
                .map(|delta| Ok((Kind::Node, add_delta(&mut reference, *delta)?)))
                .collect::<Result<Vec<_>>>()?;
            consume(Object::new(
                Kind::Way,
                way.id(),
                parallel_tags(&way.keys, &way.vals, strings, tag_budget)?,
                None,
                refs,
            )?)?;
        }
        for relation in &group.relations {
            ensure!(
                relation
                    .info
                    .as_ref()
                    .and_then(|info| info.visible)
                    .unwrap_or(true),
                "Deleted objects are not a current OSM snapshot"
            );
            ensure!(
                relation.memids.len() == relation.roles_sid.len()
                    && relation.memids.len() == relation.types.len(),
                "PBF relation member array length mismatch"
            );
            let tag_budget = tag_budget(relation.memids.len(), budget)?;
            let mut reference = 0;
            let mut refs = Vec::with_capacity(relation.memids.len());
            for index in 0..relation.memids.len() {
                let role = relation.roles_sid[index];
                ensure!(role >= 0, "Invalid PBF relation role index");
                std::str::from_utf8(
                    strings
                        .get(role as usize)
                        .context("Invalid PBF relation role index")?,
                )?;
                let kind = match relation.types[index].value() {
                    0 => Kind::Node,
                    1 => Kind::Way,
                    2 => Kind::Relation,
                    _ => bail!("Invalid PBF relation member type"),
                };
                refs.push((kind, add_delta(&mut reference, relation.memids[index])?));
            }
            consume(Object::new(
                Kind::Relation,
                relation.id(),
                parallel_tags(&relation.keys, &relation.vals, strings, tag_budget)?,
                None,
                refs,
            )?)?;
        }
        ensure!(
            group.changesets.is_empty(),
            "Unsupported PBF changeset group"
        );
    }
    Ok(())
}

const PBF_RAW_LIMIT: usize = 32 * 1024 * 1024;
const PBF_HEADER_LIMIT: usize = 64 * 1024;

fn read_blob(reader: &mut impl BufRead) -> Result<Option<(String, Blob)>> {
    if reader.fill_buf()?.is_empty() {
        return Ok(None);
    }
    let mut length = [0u8; 4];
    reader
        .read_exact(&mut length)
        .context("Truncated PBF block header length")?;
    let length = u32::from_be_bytes(length) as usize;
    ensure!(
        length > 0 && length <= PBF_HEADER_LIMIT,
        "Invalid PBF block header size"
    );
    let mut bytes = vec![0u8; length];
    reader
        .read_exact(&mut bytes)
        .context("Truncated PBF block header")?;
    let header = BlobHeader::parse_from_bytes(&bytes)?;
    let length = header.datasize();
    ensure!(
        length > 0 && length as usize <= PBF_RAW_LIMIT + PBF_HEADER_LIMIT,
        "Invalid PBF blob size"
    );
    bytes.resize(length as usize, 0);
    reader
        .read_exact(&mut bytes)
        .context("Truncated PBF blob content")?;
    Ok(Some((
        header.type_().to_owned(),
        Blob::parse_from_bytes(&bytes)?,
    )))
}

fn blob_payload(blob: &Blob) -> Result<Vec<u8>> {
    ensure!(
        !blob.has_lzma_data() && !blob.has_OBSOLETE_bzip2_data(),
        "Unsupported PBF compression"
    );
    ensure!(
        blob.has_raw() != blob.has_zlib_data(),
        "PBF blob must have exactly one supported payload"
    );
    if blob.has_raw() {
        ensure!(
            blob.raw().len() <= PBF_RAW_LIMIT,
            "PBF raw payload exceeds size limit"
        );
        ensure!(
            !blob.has_raw_size()
                || blob.raw_size() >= 0 && blob.raw_size() as usize == blob.raw().len(),
            "PBF raw size mismatch"
        );
        return Ok(blob.raw().to_vec());
    }
    ensure!(
        blob.has_raw_size() && blob.raw_size() > 0 && blob.raw_size() as usize <= PBF_RAW_LIMIT,
        "Invalid PBF decompressed size"
    );
    let mut decoder = ZlibDecoder::new(blob.zlib_data());
    let mut result = Vec::with_capacity(blob.raw_size() as usize);
    decoder
        .by_ref()
        .take(blob.raw_size() as u64 + 1)
        .read_to_end(&mut result)?;
    ensure!(
        result.len() == blob.raw_size() as usize,
        "PBF decompressed size mismatch"
    );
    ensure!(
        decoder.total_in() as usize == blob.zlib_data().len(),
        "Trailing PBF compressed data"
    );
    Ok(result)
}

fn decode_header(blob: &Blob) -> Result<HeaderBlock> {
    let header = HeaderBlock::parse_from_bytes(&blob_payload(blob)?)?;
    for feature in &header.required_features {
        ensure!(
            matches!(feature.as_str(), "OsmSchema-V0.6" | "DenseNodes"),
            "Unsupported required PBF feature: {feature}"
        );
    }
    Ok(header)
}

pub(crate) fn read_pbf_header(path: &Path) -> Result<HeaderBlock> {
    let mut reader = BufReader::new(File::open(path)?);
    let (kind, blob) = read_blob(&mut reader)?.context("PBF source has no OSM header")?;
    ensure!(
        kind == "OSMHeader",
        "PBF source does not begin with its OSM header"
    );
    decode_header(&blob)
}

fn decode_blob(
    blob: &Blob,
    options: &ComputeOptions,
    sender: &SyncSender<Result<Vec<Object>>>,
) -> Result<()> {
    let block = PrimitiveBlock::parse_from_bytes(&blob_payload(blob)?)?;
    let mut batch = Vec::new();
    let mut bytes = 0usize;
    decode_objects(&block, options.batch_bytes, |object| {
        let object_bytes = object.memory_size();
        ensure!(
            object_bytes <= options.batch_bytes,
            "OSM object exceeds OSM_COMPUTE_BATCH_BYTES"
        );
        if !batch.is_empty()
            && (batch.len() >= options.batch_size
                || bytes.saturating_add(object_bytes) > options.batch_bytes)
        {
            if sender.send(Ok(std::mem::take(&mut batch))).is_err() {
                bail!("PBF batch consumer stopped");
            }
            bytes = 0;
        }
        bytes += object_bytes;
        batch.push(object);
        Ok(())
    })?;
    if !batch.is_empty() {
        let _ = sender.send(Ok(batch));
    }
    Ok(())
}

fn next_data_blob(reader: &mut impl BufRead, header_seen: &mut bool) -> Result<Option<Blob>> {
    loop {
        let Some((kind, blob)) = read_blob(reader)? else {
            ensure!(*header_seen, "PBF source has no OSM header");
            return Ok(None);
        };
        ensure!(
            *header_seen || kind == "OSMHeader",
            "PBF source does not begin with its OSM header"
        );
        match kind.as_str() {
            "OSMHeader" => {
                ensure!(!*header_seen, "Unexpected duplicate OSM header");
                decode_header(&blob)?;
                *header_seen = true;
            }
            "OSMData" => {
                ensure!(*header_seen, "PBF data precedes its OSM header");
                return Ok(Some(blob));
            }
            _ => {}
        }
    }
}

pub(crate) fn read_pbf(
    path: &Path,
    options: &ComputeOptions,
    mut consume: impl FnMut(Vec<Object>) -> Result<()>,
) -> Result<()> {
    options.validate()?;
    let mut reader = BufReader::new(File::open(path)?);
    let mut header_seen = false;
    let pool = ThreadPoolBuilder::new()
        .num_threads(options.workers)
        .build()?;
    pool.in_place_scope(|scope| -> Result<()> {
        let mut pending: VecDeque<Receiver<Result<Vec<Object>>>> = VecDeque::new();
        let mut exhausted = false;
        loop {
            while !exhausted && pending.len() < options.pending_batches.min(options.workers) {
                if let Some(blob) = next_data_blob(&mut reader, &mut header_seen)? {
                    let (sender, receiver) = sync_channel(1);
                    scope.spawn(move |_| {
                        if let Err(error) = decode_blob(&blob, options, &sender) {
                            let _ = sender.send(Err(error));
                        }
                    });
                    pending.push_back(receiver);
                } else {
                    exhausted = true;
                }
            }
            let Some(receiver) = pending.pop_front() else {
                return Ok(());
            };
            for batch in receiver {
                consume(batch?)?;
            }
        }
    })
}

#[derive(Debug)]
struct ChangeObject {
    kind: Kind,
    id: i64,
    version: i64,
    deleted: bool,
    selected: bool,
    lat: Option<String>,
    lon: Option<String>,
    tags: BTreeMap<String, String>,
    refs: Vec<(Kind, i64)>,
    bytes: usize,
}

fn attributes(event: &BytesStart<'_>) -> Result<BTreeMap<String, String>> {
    let mut result = BTreeMap::new();
    for attribute in event.attributes() {
        let attribute = attribute?;
        let key = attribute.key.as_ref().to_owned();
        let value = attribute
            .normalized_value(quick_xml::XmlVersion::Implicit1_0)?
            .into_owned();
        ensure!(
            result.insert(key, value).is_none(),
            "Duplicate XML attribute"
        );
    }
    Ok(result)
}

fn required<'a>(attributes: &'a BTreeMap<String, String>, key: &str) -> Result<&'a str> {
    attributes
        .get(key)
        .map(String::as_str)
        .with_context(|| format!("Missing OSM {key}"))
}

pub(crate) fn read_changes<R: BufRead>(
    input: R,
    batch_bytes: usize,
    mut select: impl FnMut(Kind, i64, i64) -> Result<bool>,
    mut consume: impl FnMut(Object, i64, bool) -> Result<()>,
) -> Result<()> {
    let event_limit = u64::try_from(batch_bytes)?
        .checked_add(1)
        .context("Invalid XML event byte limit")?;
    let mut reader = Reader::from_reader(input.take(event_limit));
    reader.config_mut().check_end_names = true;
    let mut buffer = Vec::new();
    let mut stack: Vec<String> = Vec::new();
    let mut root_seen = false;
    let mut root_closed = false;
    let mut action: Option<bool> = None;
    let mut object: Option<ChangeObject> = None;
    loop {
        reader.get_mut().set_limit(event_limit);
        let event = reader.read_event_into(&mut buffer)?;
        ensure!(
            reader.get_ref().limit() > 0,
            "XML event exceeds OSM_COMPUTE_BATCH_BYTES"
        );
        if let Event::Start(start) | Event::Empty(start) = &event {
            let name = start.name();
            let name = name.as_ref();
            let attrs = attributes(start)?;
            if stack.is_empty() {
                ensure!(
                    !root_seen && !root_closed && name == "osmChange",
                    "Expected an osmChange document"
                );
                root_seen = true;
            } else if stack.len() == 1 && matches!(name, "create" | "modify" | "delete") {
                action = Some(name == "delete");
            } else if matches!(name, "node" | "way" | "relation") {
                ensure!(
                    stack.len() == 2 && action.is_some() && object.is_none(),
                    "OSM change object has no create/modify/delete operation"
                );
                let kind = Kind::parse(name)?;
                let id = text_id(required(&attrs, "id")?)?;
                let version = text_id(required(&attrs, "version")?)?;
                let selected = select(kind, id, version)?;
                object = Some(ChangeObject {
                    kind,
                    id,
                    version,
                    deleted: action == Some(true)
                        || attrs.get("visible").is_some_and(|value| value == "false"),
                    selected,
                    lat: attrs.get("lat").cloned(),
                    lon: attrs.get("lon").cloned(),
                    tags: BTreeMap::new(),
                    refs: Vec::new(),
                    bytes: start.len(),
                });
            } else if stack.len() == 3
                && let Some(object) = object.as_mut()
                && object.selected
                && !object.deleted
            {
                object.bytes = object.bytes.saturating_add(start.len());
                ensure!(
                    object.bytes <= batch_bytes,
                    "OSM object exceeds OSM_COMPUTE_BATCH_BYTES"
                );
                match name {
                    "tag" => {
                        let key = required(&attrs, "k")?;
                        let value = required(&attrs, "v")?;
                        ensure!(
                            object
                                .tags
                                .insert(key.to_owned(), value.to_owned())
                                .is_none(),
                            "Invalid or duplicate tag on {}/{}",
                            object.kind.as_str(),
                            object.id
                        );
                    }
                    "nd" if object.kind == Kind::Way => object
                        .refs
                        .push((Kind::Node, text_id(required(&attrs, "ref")?)?)),
                    "member" if object.kind == Kind::Relation => object.refs.push((
                        Kind::parse(required(&attrs, "type")?)?,
                        text_id(required(&attrs, "ref")?)?,
                    )),
                    _ => {}
                }
            }
            stack.push(name.to_owned());
        }
        if let Event::End(_) | Event::Empty(_) = &event {
            let name = stack.pop().context("Unexpected XML closing element")?;
            if stack.len() == 2 && matches!(name.as_str(), "node" | "way" | "relation") {
                let object = object.take().context("Missing change object")?;
                if object.selected {
                    let location = if object.kind == Kind::Node && !object.deleted {
                        Some((
                            object
                                .lat
                                .context("Invalid coordinate")?
                                .parse::<f64>()
                                .context("Invalid coordinate")?,
                            object
                                .lon
                                .context("Invalid coordinate")?
                                .parse::<f64>()
                                .context("Invalid coordinate")?,
                        ))
                    } else {
                        None
                    };
                    let value =
                        Object::new(object.kind, object.id, object.tags, location, object.refs)?;
                    ensure!(
                        value.memory_size() <= batch_bytes,
                        "OSM object exceeds OSM_COMPUTE_BATCH_BYTES"
                    );
                    consume(value, object.version, object.deleted)?;
                }
            } else if stack.len() == 1 && matches!(name.as_str(), "create" | "modify" | "delete") {
                action = None;
            } else if stack.is_empty() {
                root_closed = true;
            }
        }
        match event {
            Event::Eof => {
                ensure!(
                    root_seen && root_closed && stack.is_empty(),
                    "Truncated osmChange document"
                );
                break;
            }
            Event::Text(text) if stack.is_empty() => {
                ensure!(
                    text.as_ref().bytes().all(|byte| byte.is_ascii_whitespace()),
                    "Text outside osmChange root"
                );
            }
            Event::DocType(_) => {
                bail!("OSM change files must not contain a document type declaration")
            }
            _ => {}
        }
        buffer.clear();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn repeated_pbf_strings_and_reference_vectors_obey_the_object_budget() {
        let strings = vec![
            Vec::new(),
            b"name".to_vec(),
            b"amenity".to_vec(),
            vec![b'x'; 1024],
        ];
        assert!(pbf_tags([(1, 3), (2, 3)].into_iter(), &strings, 1536).is_err());
        assert!(tag_budget(1_000_000, 4096).is_err());
        assert!(tag_budget(usize::MAX, 4096).is_err());
    }

    #[test]
    fn oversized_xml_token_stops_reading_at_the_configured_limit() {
        for body in [
            format!(
                r#"<modify><node id="1" version="2" lat="0" lon="0"><tag k="name" v="{}"/></node></modify>"#,
                "x".repeat(100_000)
            ),
            "x".repeat(100_000),
            format!("<![CDATA[{}]]>", "x".repeat(100_000)),
        ] {
            let mut input = Cursor::new(format!("<osmChange>{body}</osmChange>").into_bytes());
            let mut consumed_objects = 0;
            let result = read_changes(
                &mut input,
                4096,
                |_, _, _| Ok(true),
                |_, _, _| {
                    consumed_objects += 1;
                    Ok(())
                },
            );
            assert!(result.is_err());
            assert!(
                input.position() < 8192,
                "Read beyond the single-token budget"
            );
            assert_eq!(consumed_objects, 0);
        }
    }
}

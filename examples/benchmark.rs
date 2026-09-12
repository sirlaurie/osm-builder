use std::{
    fs::{self, File},
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    process::Command,
    time::Instant,
};

use anyhow::{Context, Result, ensure};
use aura_osm::{
    config::{Environment, Runtime},
    incremental::{self, InitOptions},
};
use serde_json::json;
use sha2::{Digest, Sha256};

fn main() -> Result<()> {
    let mut arguments = std::env::args().skip(1);
    let directory = PathBuf::from(
        arguments
            .next()
            .context("Usage: benchmark OUTPUT_UNDER_BUILD NODES WORKERS")?,
    );
    let nodes: u64 = arguments.next().context("Missing node count")?.parse()?;
    let workers: usize = arguments.next().context("Missing worker count")?.parse()?;
    ensure!(
        (10..=100_000_000).contains(&nodes) && workers > 0,
        "Invalid benchmark size or worker count"
    );
    ensure!(
        directory
            .components()
            .any(|part| part.as_os_str() == ".build"),
        "Benchmark output must be under .build"
    );
    fs::create_dir_all(&directory)?;
    let input = directory.join("fixture.osm.pbf");
    if !input.exists() {
        let xml = directory.join("fixture.osm");
        let mut stream = BufWriter::new(File::create(&xml)?);
        writeln!(stream, "<osm version=\"0.6\">")?;
        for id in 1..=nodes {
            let latitude = -34.0 + (id % 1000) as f64 / 100_000.0;
            let longitude = 151.0 + ((id / 1000) % 1000) as f64 / 100_000.0;
            write!(
                stream,
                "<node id=\"{id}\" lat=\"{latitude:.7}\" lon=\"{longitude:.7}\">"
            )?;
            if id % 20 == 0 {
                write!(
                    stream,
                    "<tag k=\"name\" v=\"Cafe {id}\"/><tag k=\"name:ja\" v=\"喫茶店 {id}\"/><tag k=\"amenity\" v=\"cafe\"/>"
                )?;
            }
            writeln!(stream, "</node>")?;
        }
        for id in 1..=nodes / 5 {
            write!(stream, "<way id=\"{id}\">")?;
            for offset in 0..5 {
                write!(stream, "<nd ref=\"{}\"/>", (id - 1) * 5 + offset + 1)?;
            }
            if id % 10 == 0 {
                write!(
                    stream,
                    "<tag k=\"name\" v=\"Shop {id}\"/><tag k=\"shop\" v=\"books\"/>"
                )?;
            }
            writeln!(stream, "</way>")?;
        }
        for id in 1..=nodes / 100 {
            write!(stream, "<relation id=\"{id}\">")?;
            for offset in 0..5 {
                write!(
                    stream,
                    "<member type=\"way\" ref=\"{}\" role=\"outer\"/>",
                    (id - 1) * 5 + offset + 1
                )?;
            }
            writeln!(
                stream,
                "<tag k=\"name\" v=\"Park {id}\"/><tag k=\"leisure\" v=\"park\"/></relation>"
            )?;
        }
        writeln!(stream, "</osm>")?;
        stream.flush()?;
        ensure!(
            Command::new("osmium")
                .args(["cat", "-f", "pbf", "--no-progress", "-o"])
                .arg(&input)
                .arg(&xml)
                .status()?
                .success(),
            "Cannot generate benchmark PBF"
        );
        fs::write(
            directory.join("coverage.json"),
            r#"{"type":"Polygon","coordinates":[[[150,-35],[152,-35],[152,-32],[150,-32],[150,-35]]]}"#,
        )?;
        fs::write(directory.join("fixture-nodes.txt"), nodes.to_string())?;
    }
    ensure!(
        fs::read_to_string(directory.join("fixture-nodes.txt"))? == nodes.to_string(),
        "Existing fixture has another node count"
    );
    let state = directory.join(format!("native-{workers}"));
    let environment = Environment::from_values(
        [("OSM_COMPUTE_WORKERS".into(), workers.to_string())]
            .into_iter()
            .collect(),
    );
    let options = Runtime::from_json(
        Path::new(env!("CARGO_MANIFEST_DIR")),
        include_str!("../config/runtime.json"),
        &environment,
    )?
    .compute_options();
    let mut source = BufReader::new(File::open(&input)?);
    let mut hash = Sha256::new();
    let mut buffer = [0u8; 65536];
    loop {
        let length = source.read(&mut buffer)?;
        if length == 0 {
            break;
        }
        hash.update(&buffer[..length]);
    }
    let init = InitOptions {
        source_sha256: format!("{:x}", hash.finalize()),
        input,
        state: state.clone(),
        coverage: directory.join("coverage.json"),
        region: "au-nsw".into(),
        source_timestamp: "2020-01-01T00:00:00Z".into(),
        source_sequence: 10,
        replication_url:
            "https://download.geofabrik.de/australia-oceania/australia/new-south-wales-updates"
                .into(),
    };
    let started = Instant::now();
    let receipt = incremental::initialize(&init, &options)?;
    let seconds = started.elapsed().as_secs_f64();
    let result = json!({"workers": workers, "nodes": nodes, "objects": nodes + nodes / 5 + nodes / 100, "seconds": seconds, "stateBytes": fs::metadata(state.join("state.sqlite"))?.len(), "count": receipt["count"], "manifest": receipt["manifest"], "sourceSHA256": init.source_sha256});
    fs::write(
        directory.join(format!("native-{workers}.json")),
        serde_json::to_vec_pretty(&result)?,
    )?;
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}

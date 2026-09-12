use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

pub fn work(prefix: &str) -> tempfile::TempDir {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join(".build/tools/tests");
    fs::create_dir_all(&root).expect("create test root");
    tempfile::Builder::new()
        .prefix(prefix)
        .tempdir_in(root)
        .expect("create test directory")
}

pub fn write_pbf(directory: &Path, name: &str, xml: &str, headers: &[(&str, &str)]) -> PathBuf {
    let input = directory.join(format!("{name}.osm"));
    let output = directory.join(format!("{name}.osm.pbf"));
    fs::write(&input, xml).expect("write OSM fixture");
    let mut command = Command::new("osmium");
    command
        .args(["cat", "-f", "pbf", "--no-progress", "-o"])
        .arg(&output);
    for (key, value) in headers {
        command.arg(format!("--output-header={key}={value}"));
    }
    let result = command
        .arg(&input)
        .output()
        .expect("osmium is required to generate PBF test fixtures");
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    output
}

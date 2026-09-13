use aura_osm::{
    format::hash_bytes,
    storage::{atomic_write, read_bytes, read_local, write_object},
};
use std::{
    fs,
    os::unix::fs::{PermissionsExt, symlink},
    path::Path,
};

#[test]
fn local_io_preserves_immutable_content_and_refuses_symlink_escape() {
    let tests = Path::new(env!("CARGO_MANIFEST_DIR")).join(".build/tools/tests");
    fs::create_dir_all(&tests).unwrap();
    let root = tempfile::tempdir_in(tests).unwrap();
    let output = root.path().join("output");
    fs::create_dir(&output).unwrap();
    let hash = write_object(&output, "blocks", b"[]").unwrap();
    let pack = b"[][]";
    let pack_hash = write_object(&output, "packs", pack).unwrap();
    let pack_key = format!("packs/{pack_hash}.bin");
    assert_eq!(
        read_bytes(&output, &pack_key, pack.len(), Some(&pack_hash))
            .unwrap()
            .0,
        pack
    );
    assert_eq!(write_object(&output, "packs", pack).unwrap(), pack_hash);
    assert!(read_local(&output, &pack_key, pack.len(), Some(&pack_hash)).is_err());
    fs::write(output.join(&pack_key), b"{}{}").unwrap();
    assert!(write_object(&output, "packs", pack).is_err());
    assert_eq!(hash, hash_bytes(b"[]"));
    assert_eq!(write_object(&output, "blocks", b"[]").unwrap(), hash);
    let key = format!("blocks/{hash}.json");
    assert_eq!(
        read_local(&output, &key, 2, Some(&hash)).unwrap().bytes,
        b"[]"
    );
    assert!(read_local(&output, &key, 1, Some(&hash)).is_err());
    assert!(read_local(&output, &key, 2, Some(&"a".repeat(64))).is_err());
    let outside = root.path().join("outside.json");
    fs::write(&outside, b"{}").unwrap();
    symlink(&outside, output.join("release.json")).unwrap();
    assert!(read_local(&output, "release.json", 20, None).is_err());
    assert!(atomic_write(&output.join("release.json"), b"{}").is_err());
    assert_eq!(fs::read(&outside).unwrap(), b"{}");
    let own = output.join("own.json");
    atomic_write(&own, b"{}").unwrap();
    assert_eq!(
        fs::metadata(&own).unwrap().permissions().mode() & 0o777,
        0o600
    );
    fs::write(output.join(key), b"{}").unwrap();
    assert!(write_object(&output, "blocks", b"[]").is_err());
    let fifo = output.join("fifo.json");
    assert!(
        std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap()
            .success()
    );
    assert!(read_local(&output, "fifo.json", 20, None).is_err());
}

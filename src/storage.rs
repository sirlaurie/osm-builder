use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::Path,
};

use anyhow::{Context, Result, ensure};
use serde_json::Value;

use crate::format::hash_bytes;

pub struct LocalObject {
    pub key: String,
    pub hash: String,
    pub size: usize,
    pub bytes: Vec<u8>,
    pub value: Value,
}

pub fn sync_directory(path: &Path) -> Result<()> {
    sync_file(&File::open(path)?).context("Cannot synchronize directory")
}

pub fn sync_file(file: &File) -> Result<()> {
    rustix::io::retry_on_intr(|| rustix::fs::fsync(file)).context("Cannot synchronize file")
}

pub fn atomic_file(path: &Path, writer: impl FnOnce(&mut File) -> Result<()>) -> Result<()> {
    if let Ok(metadata) = fs::symlink_metadata(path) {
        ensure!(
            metadata.is_file() && !metadata.is_symlink(),
            "Output must be a regular file: {}",
            path.display()
        );
    }
    let parent = path.parent().context("Output has no parent directory")?;
    let mut temporary = tempfile::Builder::new()
        .prefix(".write-")
        .tempfile_in(parent)?;
    temporary
        .as_file()
        .set_permissions(fs::Permissions::from_mode(0o600))?;
    writer(temporary.as_file_mut())?;
    temporary.as_file_mut().flush()?;
    sync_file(temporary.as_file())?;
    if let Ok(metadata) = fs::symlink_metadata(path) {
        ensure!(
            metadata.is_file() && !metadata.is_symlink(),
            "Output changed to a non-file: {}",
            path.display()
        );
    }
    temporary
        .persist(path)
        .context("Cannot commit output file")?;
    sync_directory(parent)
}

pub fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    atomic_file(path, |file| {
        file.write_all(bytes)?;
        Ok(())
    })
}

pub fn write_object(output: &Path, directory: &str, payload: &[u8]) -> Result<String> {
    ensure!(
        matches!(directory, "blocks" | "manifests" | "packs"),
        "Invalid immutable object directory"
    );
    let folder = output.join(directory);
    match fs::create_dir(&folder) {
        Ok(()) => sync_directory(output)?,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let metadata = fs::symlink_metadata(&folder)?;
            ensure!(
                metadata.is_dir() && !metadata.is_symlink(),
                "Immutable object directory must not be a symlink"
            );
        }
        Err(error) => return Err(error.into()),
    }
    let hash = hash_bytes(payload);
    let extension = if directory == "packs" { "bin" } else { "json" };
    let key = format!("{directory}/{hash}.{extension}");
    let path = output.join(&key);
    match fs::symlink_metadata(&path) {
        Ok(metadata) => {
            ensure!(
                metadata.is_file() && !metadata.is_symlink(),
                "Immutable object is not a regular file"
            );
            let (bytes, _) = read_bytes(output, &key, payload.len(), Some(&hash))?;
            ensure!(
                bytes == payload,
                "Existing content-addressed object is corrupt"
            );
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => atomic_write(&path, payload)?,
        Err(error) => return Err(error.into()),
    }
    Ok(hash)
}

pub fn read_local(
    root: &Path,
    key: &str,
    limit: usize,
    expected_hash: Option<&str>,
) -> Result<LocalObject> {
    let (bytes, hash) = read_bytes(root, key, limit, expected_hash)?;
    let value =
        serde_json::from_slice(&bytes).with_context(|| format!("Invalid UTF-8 JSON: {key}"))?;
    Ok(LocalObject {
        key: key.to_owned(),
        hash,
        size: bytes.len(),
        bytes,
        value,
    })
}

pub fn read_bytes(
    root: &Path,
    key: &str,
    limit: usize,
    expected_hash: Option<&str>,
) -> Result<(Vec<u8>, String)> {
    let root = root.canonicalize().context("Cannot open build directory")?;
    let path = root.join(key);
    let actual = path
        .canonicalize()
        .with_context(|| format!("Cannot read local object: {key}"))?;
    ensure!(
        actual.starts_with(&root),
        "Build reference escapes directory"
    );
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(&path)
        .with_context(|| format!("Cannot open local object: {key}"))?;
    let metadata = file.metadata()?;
    ensure!(
        metadata.is_file() && metadata.len() > 0 && metadata.len() <= limit as u64,
        "Invalid size: {key}"
    );
    let size = usize::try_from(metadata.len()).context("Local object exceeds address space")?;
    let mut bytes = Vec::with_capacity(size);
    file.take(size as u64 + 1).read_to_end(&mut bytes)?;
    ensure!(bytes.len() == size, "File changed while reading: {key}");
    let hash = hash_bytes(&bytes);
    ensure!(
        expected_hash.is_none_or(|expected| expected == hash),
        "SHA-256 mismatch: {key}"
    );
    Ok((bytes, hash))
}

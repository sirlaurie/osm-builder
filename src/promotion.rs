use std::{fs, path::Path};

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};

use crate::{
    format::{MAX_CURRENT, canonical_json, is_hash, is_region, validate_release},
    storage::{atomic_write, read_local, sync_directory},
};

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Journal {
    region: String,
    manifest: String,
}

fn directory(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            ensure!(
                metadata.is_dir() && !metadata.is_symlink(),
                "Promotion path must be a directory, not a symbolic link: {}",
                path.display()
            );
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn receipt(state: &Path, region: &str, manifest: Option<&str>) -> Result<()> {
    ensure!(directory(state)?, "Promotion index is missing");
    let output = state.join("output");
    ensure!(directory(&output)?, "Promotion output is missing");
    let release = validate_release(&read_local(&output, "release.json", MAX_CURRENT, None)?.value)?;
    ensure!(
        release.region == region && manifest.is_none_or(|hash| hash == release.manifest),
        "Promotion index does not match the confirmed regional release"
    );
    Ok(())
}

fn workspace(data: &Path, region: &str) -> Result<std::path::PathBuf> {
    ensure!(is_region(region), "Invalid promotion region");
    ensure!(directory(data)?, "Promotion data directory is missing");
    let candidates = data.join(".candidates");
    directory(&candidates)?;
    let work = candidates.join(region);
    directory(&work)?;
    Ok(work)
}

pub fn promote(data: &Path, region: &str, manifest: &str) -> Result<()> {
    ensure!(is_hash(manifest), "Invalid promotion manifest");
    let work = workspace(data, region)?;
    let journal = work.join("promotion.json");
    match fs::symlink_metadata(&journal) {
        Ok(_) => return recover(data, region, Some(manifest)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    ensure!(
        !directory(&work.join("previous"))?,
        "Previous promotion index exists without a recovery journal"
    );
    receipt(&work.join(region), region, Some(manifest))?;
    if directory(&data.join(region))? {
        receipt(&data.join(region), region, None)?;
    }
    atomic_write(
        &journal,
        &canonical_json(&serde_json::to_value(Journal {
            region: region.to_owned(),
            manifest: manifest.to_owned(),
        })?)?,
    )?;
    recover(data, region, Some(manifest))
}

pub fn recover(data: &Path, region: &str, remote_manifest: Option<&str>) -> Result<()> {
    let work = workspace(data, region)?;
    let journal = work.join("promotion.json");
    match fs::symlink_metadata(&journal) {
        Ok(metadata) => ensure!(
            metadata.is_file() && !metadata.is_symlink(),
            "Promotion journal must be a regular file"
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    }
    let record: Journal =
        serde_json::from_value(read_local(&work, "promotion.json", MAX_CURRENT, None)?.value)
            .context("Invalid promotion journal")?;
    ensure!(
        record.region == region
            && is_hash(&record.manifest)
            && remote_manifest == Some(record.manifest.as_str()),
        "Promotion requires the exact remotely confirmed regional manifest"
    );
    let candidate = work.join(region);
    let active = data.join(region);
    let previous = work.join("previous");
    let has_candidate = directory(&candidate)?;
    let has_active = directory(&active)?;
    let has_previous = directory(&previous)?;
    if has_candidate {
        receipt(&candidate, region, Some(&record.manifest))?;
        if has_active {
            ensure!(
                !has_previous,
                "Promotion has conflicting active and previous indexes"
            );
            receipt(&active, region, None)?;
            fs::rename(&active, &previous)?;
            sync_directory(data)?;
            sync_directory(&work)?;
        }
        fs::rename(&candidate, &active)?;
        sync_directory(&work)?;
        sync_directory(data)?;
    }
    receipt(&active, region, Some(&record.manifest))?;
    if directory(&previous)? {
        fs::remove_dir_all(&previous)?;
        sync_directory(&work)?;
    }
    fs::remove_file(&journal)?;
    sync_directory(&work)
}

use std::{
    collections::{BTreeMap, HashSet},
    sync::{Mutex, atomic::AtomicBool},
    thread,
    time::Duration,
};

use anyhow::{Context, Result, anyhow, ensure};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};

use crate::{
    dispatch,
    format::{self, CellPage},
    network,
    publish::{Publisher, StoredObject},
};

const PREFIXES: [(&str, &str); 3] = [
    ("manifests/", ".json"),
    ("packs/", ".bin"),
    ("blocks/", ".json"),
];
const MINIMUM_AGE: chrono::TimeDelta = chrono::TimeDelta::hours(24);
const SHARDS: &str = "0123456789abcdef";

#[derive(Default)]
struct Tally {
    objects: u64,
    bytes: u64,
}

impl Tally {
    fn add(&mut self, object: &StoredObject) {
        self.objects += 1;
        self.bytes += object.size;
    }

    fn json(&self) -> Value {
        json!({ "objects": self.objects, "bytes": self.bytes })
    }
}

#[derive(Default)]
struct Report {
    referenced: Tally,
    recent: Tally,
    unrecognized: Tally,
    deletable: Tally,
    deleted: Tally,
}

enum Verdict {
    Referenced,
    Recent,
    Unrecognized,
    Deletable,
}

fn verdict(
    object: &StoredObject,
    prefix: &str,
    extension: &str,
    referenced: &HashSet<String>,
    cutoff: DateTime<Utc>,
) -> Verdict {
    let recognized = object
        .key
        .strip_prefix(prefix)
        .and_then(|name| name.strip_suffix(extension))
        .is_some_and(format::is_hash);
    if !recognized {
        Verdict::Unrecognized
    } else if referenced.contains(&object.key) {
        Verdict::Referenced
    } else if object.modified > cutoff {
        Verdict::Recent
    } else {
        Verdict::Deletable
    }
}

fn active_batch(client: &dispatch::Client) -> Result<Option<String>> {
    let jobs = network::retry(Duration::from_secs(60), || client.jobs())?;
    Ok(jobs["batchId"]
        .as_str()
        .filter(|_| jobs["finishedAt"].is_null())
        .map(str::to_owned))
}

fn referenced(publisher: &Publisher, cancelled: &AtomicBool) -> Result<(usize, HashSet<String>)> {
    let current = network::retry(Duration::from_secs(60), || publisher.read_state())?
        .context("No published state; refusing to collect R2 objects")?;
    ensure!(
        !current.regions.is_empty(),
        "Published state has no regions; refusing to collect R2 objects"
    );
    let keys = Mutex::new(HashSet::new());
    let next = Mutex::new(current.regions.iter());
    thread::scope(|scope| -> Result<()> {
        let workers = (0..8)
            .map(|_| {
                scope.spawn(|| -> Result<()> {
                    loop {
                        let Some(release) = next.lock().expect("manifest queue").next() else {
                            return Ok(());
                        };
                        let manifest = network::retry(Duration::from_secs(60), || {
                            publisher.read_manifest(&release.manifest, cancelled)
                        })?;
                        ensure!(
                            manifest.region == release.region,
                            "Published manifest belongs to another region: {}",
                            release.region
                        );
                        let mut found = vec![format!("manifests/{}.json", release.manifest)];
                        found.extend(
                            manifest
                                .packs
                                .iter()
                                .map(|pack| format!("packs/{pack}.bin")),
                        );
                        found.extend(
                            manifest
                                .cells
                                .values()
                                .flatten()
                                .filter(|page| matches!(page, CellPage::Legacy(_)))
                                .map(|page| format!("blocks/{}.json", page.hash())),
                        );
                        keys.lock().expect("referenced keys").extend(found);
                    }
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            worker
                .join()
                .map_err(|_| anyhow!("Manifest reader panicked"))??;
        }
        Ok(())
    })?;
    Ok((
        current.regions.len(),
        keys.into_inner().expect("referenced keys"),
    ))
}

pub fn run(publisher: &Publisher, apply: bool) -> Result<Value> {
    let cancelled = AtomicBool::new(false);
    let client = publisher.coordinator();
    let active = active_batch(&client)?;
    ensure!(
        !apply || active.is_none(),
        "Batch {} is unfinished; wait for it or run osm cancel before deleting R2 objects",
        active.as_deref().unwrap_or_default()
    );
    let (regions, referenced) = referenced(publisher, &cancelled)?;
    let cutoff = Utc::now() - MINIMUM_AGE;
    let reports = Mutex::new(BTreeMap::<&str, Report>::new());
    for (prefix, extension) in PREFIXES {
        thread::scope(|scope| -> Result<()> {
            let workers = SHARDS
                .chars()
                .map(|shard| {
                    let reports = &reports;
                    let referenced = &referenced;
                    let client = &client;
                    let cancelled = &cancelled;
                    scope.spawn(move || -> Result<()> {
                        let mut report = Report::default();
                        let mut pending: Vec<StoredObject> = Vec::new();
                        let delete = |batch: &mut Vec<StoredObject>, deleted: &mut Tally| -> Result<()> {
                            if batch.is_empty() {
                                return Ok(());
                            }
                            ensure!(
                                active_batch(client)?.is_none(),
                                "A batch started during R2 collection; stopped before deleting more objects"
                            );
                            let keys = batch
                                .iter()
                                .map(|object| object.key.clone())
                                .collect::<Vec<_>>();
                            network::retry(Duration::from_secs(60), || {
                                publisher.delete_objects(&keys, cancelled)
                            })?;
                            batch.iter().for_each(|object| deleted.add(object));
                            batch.clear();
                            Ok(())
                        };
                        publisher.list_objects(&format!("{prefix}{shard}"), cancelled, |page| {
                            for object in page {
                                match verdict(&object, prefix, extension, referenced, cutoff) {
                                    Verdict::Referenced => report.referenced.add(&object),
                                    Verdict::Recent => report.recent.add(&object),
                                    Verdict::Unrecognized => report.unrecognized.add(&object),
                                    Verdict::Deletable => {
                                        report.deletable.add(&object);
                                        if apply {
                                            pending.push(object);
                                        }
                                    }
                                }
                                if pending.len() == 1000 {
                                    delete(&mut pending, &mut report.deleted)?;
                                }
                            }
                            Ok(())
                        })?;
                        delete(&mut pending, &mut report.deleted)?;
                        let mut reports = reports.lock().expect("collection report");
                        let total = reports.entry(prefix).or_default();
                        for (total, part) in [
                            (&mut total.referenced, &report.referenced),
                            (&mut total.recent, &report.recent),
                            (&mut total.unrecognized, &report.unrecognized),
                            (&mut total.deletable, &report.deletable),
                            (&mut total.deleted, &report.deleted),
                        ] {
                            total.objects += part.objects;
                            total.bytes += part.bytes;
                        }
                        Ok(())
                    })
                })
                .collect::<Vec<_>>();
            for worker in workers {
                worker
                    .join()
                    .map_err(|_| anyhow!("R2 collector panicked"))??;
            }
            Ok(())
        })?;
    }
    let prefixes = reports
        .into_inner()
        .expect("collection report")
        .into_iter()
        .map(|(prefix, report)| {
            (
                prefix.to_owned(),
                json!({
                    "referenced": report.referenced.json(),
                    "recent": report.recent.json(),
                    "unrecognized": report.unrecognized.json(),
                    "deletable": report.deletable.json(),
                    "deleted": report.deleted.json(),
                }),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    Ok(json!({
        "applied": apply,
        "activeBatch": active,
        "regions": regions,
        "prefixes": prefixes,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn object(key: &str, hours: i64) -> StoredObject {
        StoredObject {
            key: key.into(),
            size: 1,
            modified: DateTime::parse_from_rfc3339("2026-10-09T12:00:00Z")
                .unwrap()
                .with_timezone(&Utc)
                - chrono::TimeDelta::hours(hours),
        }
    }

    #[test]
    fn only_old_unreferenced_hash_objects_are_deletable() {
        let hash = "a".repeat(64);
        let referenced = HashSet::from([format!("packs/{hash}.bin")]);
        let cutoff = DateTime::parse_from_rfc3339("2026-10-08T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let other = "b".repeat(64);
        for (key, hours, expected) in [
            (format!("packs/{hash}.bin"), 48, "referenced"),
            (format!("packs/{other}.bin"), 1, "recent"),
            (format!("packs/{other}.bin"), 48, "deletable"),
            (format!("packs/{other}.json"), 48, "unrecognized"),
            ("packs/notes.txt".to_owned(), 48, "unrecognized"),
        ] {
            let actual = match verdict(&object(&key, hours), "packs/", ".bin", &referenced, cutoff)
            {
                Verdict::Referenced => "referenced",
                Verdict::Recent => "recent",
                Verdict::Unrecognized => "unrecognized",
                Verdict::Deletable => "deletable",
            };
            assert_eq!(actual, expected, "{key}");
        }
    }
}

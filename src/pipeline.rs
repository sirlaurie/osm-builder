use std::{
    collections::BTreeMap,
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Arc, Condvar, Mutex, Weak,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread::JoinHandle,
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use reqwest::{Client, redirect::Policy};
use serde_json::{Value, json};

use crate::{
    build,
    config::{Environment, Runtime},
    dispatch, format, incremental, progress,
    publish::{ProgressReporter, PublishConfig, Publisher},
    source::{self, Region, ReplicationState},
    storage::{atomic_write, sync_directory},
};

const INDEX_URL: &str = "https://download.geofabrik.de/index-v1.json";

fn disk_check(directory: &Path, minimum_gib: u64) -> Result<()> {
    let available = fs2::available_space(directory)
        .with_context(|| format!("Cannot inspect free space at {}", directory.display()))?;
    ensure!(
        available / (1024 * 1024 * 1024) >= minimum_gib,
        "Need {minimum_gib} GiB free at {}; available {} GiB",
        directory.display(),
        available / (1024 * 1024 * 1024)
    );
    Ok(())
}

struct Downloader {
    client: Client,
    executor: Option<tokio::runtime::Runtime>,
    control: Option<Arc<DownloadControl>>,
}

#[derive(Default)]
struct DownloadControl {
    cancelled: AtomicBool,
    failed: AtomicBool,
    foreground: AtomicBool,
    changed: tokio::sync::Notify,
    pipeline: Option<(Arc<PipelineFlow>, usize)>,
}

impl DownloadControl {
    fn download_turn(&self) -> Result<()> {
        if let Some((flow, slot)) = &self.pipeline {
            while !self.foreground.load(Ordering::Acquire)
                && !flow.sources_ready.lock().expect("source readiness lock")[1 - slot]
            {
                ensure!(
                    !self.cancelled.load(Ordering::Acquire),
                    "Prefetch cancelled"
                );
                flow.wait(Duration::from_millis(20));
            }
        }
        ensure!(
            !self.cancelled.load(Ordering::Acquire),
            "Prefetch cancelled"
        );
        Ok(())
    }

    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        self.changed.notify_one();
    }

    fn consume(&self) {
        self.foreground.store(true, Ordering::Release);
        self.changed.notify_one();
    }

    async fn cancelled(&self) {
        loop {
            let changed = self.changed.notified();
            if self.cancelled.load(Ordering::Acquire) {
                return;
            }
            changed.await;
        }
    }
}

impl Downloader {
    fn new(runtime: &Runtime) -> Result<Self> {
        let client = Client::builder()
            .connect_timeout(runtime.connect_timeout)
            .retry(reqwest::retry::never())
            .redirect(Policy::custom(|attempt| {
                if attempt.url().scheme() != "https" {
                    attempt.error("Downloads require HTTPS redirects")
                } else if attempt.previous().len() >= 10 {
                    attempt.error("Too many download redirects")
                } else {
                    attempt.follow()
                }
            }))
            .build()?;
        Ok(Self {
            client,
            executor: Some(
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?,
            ),
            control: None,
        })
    }

    fn download(
        &self,
        runtime: &Runtime,
        url: &str,
        path: &Path,
        limit: Option<u64>,
    ) -> Result<()> {
        ensure!(
            reqwest::Url::parse(url)?.scheme() == "https",
            "Downloads require HTTPS"
        );
        let timeout = match limit {
            None => runtime.download_timeout,
            Some(limit) if limit <= 16_384 => runtime.metadata_timeout,
            Some(_) => runtime.diff_timeout,
        };
        let partial = path.with_file_name(format!(
            "{}.part",
            path.file_name()
                .context("Invalid download path")?
                .to_string_lossy()
        ));
        self.executor().block_on(async {
            for attempt in 0..=runtime.download_retries {
                let result = self
                    .download_attempt(runtime, url, &partial, timeout, limit)
                    .await;
                match result {
                    Ok(()) => {
                        fs::rename(&partial, path)?;
                        return Ok(());
                    }
                    Err(error) if attempt < runtime.download_retries => {
                        self.check_cancelled()?;
                        progress::message(&format!(
                            "Download attempt {} failed: {error}",
                            attempt + 1
                        ));
                        self.interruptible(tokio::time::sleep(runtime.download_retry_delay))
                            .await?;
                    }
                    Err(error) => return Err(error),
                }
            }
            unreachable!()
        })
    }

    fn check_cancelled(&self) -> Result<()> {
        ensure!(
            self.control
                .as_ref()
                .is_none_or(|control| !control.cancelled.load(Ordering::Acquire)),
            "Prefetch cancelled"
        );
        Ok(())
    }

    fn executor(&self) -> &tokio::runtime::Runtime {
        self.executor
            .as_ref()
            .expect("Download executor is available")
    }

    async fn interruptible<T>(&self, future: impl std::future::Future<Output = T>) -> Result<T> {
        if let Some(control) = &self.control {
            tokio::select! {
                biased;
                _ = control.cancelled() => bail!("Prefetch cancelled"),
                result = future => Ok(result),
            }
        } else {
            Ok(future.await)
        }
    }

    async fn budget(&self, runtime: &Runtime, path: &Path, remaining: u64) -> Result<Duration> {
        let Some(control) = &self.control else {
            return Ok(Duration::ZERO);
        };
        let started = std::time::Instant::now();
        let mut paused = false;
        loop {
            self.check_cancelled()?;
            let foreground = control.foreground.load(Ordering::Acquire);
            let reserve = if foreground {
                runtime.min_free_gib
            } else {
                runtime.start_free_gib.max(runtime.min_free_gib)
            }
            .checked_mul(1024 * 1024 * 1024)
            .context("Disk reserve overflow")?;
            let needed = reserve
                .checked_add(remaining)
                .context("Download disk budget overflow")?;
            let available = fs2::available_space(path)?;
            if available >= needed {
                if paused {
                    progress::message(&format!(
                        "Prefetch resumed at {}: {} GiB free",
                        path.display(),
                        available / (1024 * 1024 * 1024)
                    ));
                }
                return Ok(started.elapsed());
            }
            if foreground {
                ensure!(
                    control
                        .pipeline
                        .as_ref()
                        .is_some_and(|(flow, slot)| flow.can_reclaim(*slot)),
                    "Insufficient space for download and {} GiB reserve at {}",
                    runtime.min_free_gib,
                    path.display()
                );
            }
            if !paused {
                progress::message(&format!(
                    "Prefetch paused at {}: {} GiB free, {} GiB reserved for current work and remaining download",
                    path.display(),
                    available / (1024 * 1024 * 1024),
                    needed.div_ceil(1024 * 1024 * 1024)
                ));
                paused = true;
            }
            self.interruptible(tokio::time::sleep(Duration::from_secs(1)))
                .await?;
        }
    }

    async fn download_attempt(
        &self,
        runtime: &Runtime,
        url: &str,
        path: &Path,
        timeout: Duration,
        limit: Option<u64>,
    ) -> Result<()> {
        let mut deadline = tokio::time::Instant::now() + timeout;
        let mut response = self
            .interruptible(tokio::time::timeout_at(
                deadline,
                self.client.get(url).send(),
            ))
            .await???
            .error_for_status()?;
        if let (Some(length), Some(limit)) = (response.content_length(), limit) {
            ensure!(length <= limit, "Download exceeds limit: {url}");
        }
        let directory = path.parent().context("Invalid download directory")?;
        let mut file = fs::File::create(path)?;
        deadline += self
            .budget(
                runtime,
                directory,
                response.content_length().unwrap_or(16 * 1024 * 1024),
            )
            .await?;
        let mut length = 0_u64;
        let mut checked = 0_u64;
        let expected = response.content_length();
        loop {
            let Some(buffer) = self
                .interruptible(tokio::time::timeout_at(deadline, response.chunk()))
                .await???
            else {
                break;
            };
            let count = buffer.len();
            let written = length;
            length = length
                .checked_add(count as u64)
                .context("Download size overflow")?;
            ensure!(
                limit.is_none_or(|limit| length <= limit),
                "Download exceeds limit: {url}"
            );
            if length.saturating_sub(checked) >= 16 * 1024 * 1024 {
                deadline += self
                    .budget(
                        runtime,
                        directory,
                        expected.map_or(16 * 1024 * 1024, |expected| {
                            expected.saturating_sub(written)
                        }),
                    )
                    .await?;
                checked = length;
            }
            self.check_cancelled()?;
            file.write_all(&buffer)?;
        }
        crate::storage::sync_file(&file)?;
        Ok(())
    }

    fn remote_state(
        &self,
        runtime: &Runtime,
        url: &str,
        work: &Path,
        sequence: Option<u64>,
    ) -> Result<ReplicationState> {
        let suffix = match sequence {
            Some(sequence) => format!("{}.state", source::sequence_path(sequence)?),
            None => "state".into(),
        };
        let path = work.join("state.txt");
        self.download(runtime, &format!("{url}/{suffix}.txt"), &path, Some(16_384))?;
        let state = source::replication_state(&fs::read_to_string(path)?)?;
        ensure!(
            sequence.is_none_or(|sequence| sequence == state.sequence),
            "Replication state sequence does not match its download path"
        );
        Ok(state)
    }

    fn source_snapshot(
        &self,
        runtime: &Runtime,
        entry: &Region,
        catalog: &Path,
        job: &Path,
    ) -> Result<()> {
        source::prepare(entry, catalog, job)?;
        let url = entry.source_url();
        progress::message(&format!("[{}] Downloading initial source: {url}", entry.id));
        self.download(runtime, &url, &job.join("source.osm.pbf"), None)?;
        self.download(
            runtime,
            &format!("{url}.md5"),
            &job.join("source.md5"),
            Some(4096),
        )
    }
}

impl Drop for Downloader {
    fn drop(&mut self) {
        if let Some(executor) = self.executor.take() {
            executor.shutdown_background();
        }
    }
}

fn string<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value[key]
        .as_str()
        .with_context(|| format!("Missing index field: {key}"))
}

fn sequence(value: &Value) -> Result<u64> {
    value["sequence"].as_u64().context("Missing index sequence")
}

fn status(entry: &Region, data: &Path, runtime: &Runtime) -> Result<Value> {
    let state = data.join(&entry.id);
    ensure!(
        state.is_dir(),
        "{} has no incremental index; run osm init {} first",
        entry.id,
        entry.id
    );
    let current = incremental::status(&state, &runtime.compute_options())?;
    ensure!(
        string(&current, "region")? == entry.id
            && string(&current, "replicationUrl")? == entry.updates_url(),
        "Incremental index does not match the configured regional source"
    );
    Ok(current)
}

struct Operations<'a> {
    runtime: &'a Runtime,
    environment: &'a Environment,
    scratch: &'a Path,
    downloader: Downloader,
    catalog: PathBuf,
    publisher: Option<Publisher>,
    lease: Option<&'a dispatch::Guard>,
    prefetch: Option<Prefetch>,
}

struct Prefetch {
    region: String,
    control: Arc<DownloadControl>,
    task: Option<JoinHandle<Result<tempfile::TempDir>>>,
    error: Option<anyhow::Error>,
}

impl Prefetch {
    fn start(runtime: &Runtime, entry: &Region, catalog: &Path, data: &Path) -> Self {
        Self::controlled(
            runtime,
            entry,
            catalog,
            data,
            Arc::new(DownloadControl::default()),
        )
    }

    fn controlled(
        runtime: &Runtime,
        entry: &Region,
        catalog: &Path,
        data: &Path,
        control: Arc<DownloadControl>,
    ) -> Self {
        let thread_control = Arc::clone(&control);
        let runtime = runtime.clone();
        let entry = entry.clone();
        let region = entry.id.clone();
        let catalog = catalog.to_path_buf();
        let downloads = data.join(".downloads");
        let result = std::thread::Builder::new()
            .name(format!("prefetch-{region}"))
            .spawn(move || {
                let _label = progress::region(&entry.id);
                let result = (|| -> Result<tempfile::TempDir> {
                    let pipeline = thread_control.pipeline.clone();
                    thread_control.download_turn()?;
                    let _download = pipeline
                        .as_ref()
                        .map(|(flow, _)| flow.download.lock().expect("download lock"));
                    fs::create_dir_all(&downloads)?;
                    let job = tempfile::Builder::new()
                        .prefix(&format!("{}-", entry.id))
                        .rand_bytes(10)
                        .tempdir_in(downloads)?;
                    let mut downloader = Downloader::new(&runtime)?;
                    downloader.control = Some(Arc::clone(&thread_control));
                    downloader.source_snapshot(&runtime, &entry, &catalog, job.path())?;
                    if let Some((flow, slot)) = pipeline.as_ref() {
                        flow.sources_ready.lock().expect("source readiness lock")[*slot] = true;
                        flow.changed.notify_all();
                    }
                    Ok(job)
                })();
                if let Err(error) = &result
                    && !thread_control.cancelled.load(Ordering::Acquire)
                    && let Some((flow, _)) = &thread_control.pipeline
                {
                    thread_control.failed.store(true, Ordering::Release);
                    *flow.failure.lock().expect("pipeline failure lock") =
                        Some(format!("Region {} prefetch failed: {error:#}", entry.id));
                    flow.stop();
                }
                result
            });
        let (task, error) = match result {
            Ok(task) => (Some(task), None),
            Err(error) => (None, Some(error.into())),
        };
        Self {
            region,
            control,
            task,
            error,
        }
    }

    fn consume(mut self) -> Result<PathBuf> {
        self.control.consume();
        if let Some(error) = self.error.take() {
            return Err(error);
        }
        let job = self
            .task
            .take()
            .context("Missing prefetch task")?
            .join()
            .map_err(|_| anyhow::anyhow!("Prefetch thread panicked"))??;
        Ok(job.keep())
    }
}

impl Drop for Prefetch {
    fn drop(&mut self) {
        self.control.cancel();
        if let Some(task) = self.task.take() {
            let _ = task.join();
        }
    }
}

impl Operations<'_> {
    fn check_disks(&self, data: &Path, minimum_gib: u64) -> Result<()> {
        disk_check(data, minimum_gib)?;
        disk_check(self.scratch, minimum_gib)
    }

    fn publisher(&mut self) -> Result<&Publisher> {
        if self.publisher.is_none() {
            self.publisher = Some(Publisher::new(
                self.runtime,
                &PublishConfig::from_environment(self.environment)?,
            )?);
        }
        Ok(self.publisher.as_ref().expect("Publisher is initialized"))
    }

    fn initialize(
        &mut self,
        entry: &Region,
        data: &Path,
        job: Option<&Path>,
        next: Option<&Region>,
    ) -> Result<Value> {
        let pending = if self
            .prefetch
            .as_ref()
            .is_some_and(|prefetch| prefetch.region == entry.id)
        {
            self.prefetch.take()
        } else {
            None
        };
        let state = data.join(&entry.id);
        ensure!(
            !state.try_exists()?,
            "Incremental index already exists: {}",
            state.display()
        );
        self.check_disks(data, self.runtime.start_free_gib)?;
        let job = match job {
            Some(job) => job.canonicalize()?,
            None if pending.is_some() => pending.expect("Prefetch target matches").consume()?,
            None => {
                let downloads = data.join(".downloads");
                fs::create_dir_all(&downloads)?;
                let job = tempfile::Builder::new()
                    .prefix(&format!("{}-", entry.id))
                    .rand_bytes(10)
                    .tempdir_in(&downloads)?
                    .keep();
                self.downloader
                    .source_snapshot(self.runtime, entry, &self.catalog, &job)?;
                job
            }
        };
        ensure!(
            fs::read_to_string(job.join("source.url"))?.trim() == entry.source_url(),
            "Downloaded job does not match the configured regional source"
        );
        let pbf = job.join("source.osm.pbf");
        progress::message(&format!(
            "[{}] Checking complete source and replication header",
            entry.id
        ));
        let metadata = source::stamp(&pbf, &job.join("source.md5"), &entry.updates_url())?;
        let work = tempfile::Builder::new().prefix("init-").tempdir_in(data)?;
        let anchor = self.downloader.remote_state(
            self.runtime,
            &metadata.replication_url,
            work.path(),
            Some(metadata.sequence),
        )?;
        ensure!(
            anchor.timestamp == metadata.timestamp,
            "PBF timestamp does not match its regional replication sequence"
        );
        let candidate = work.path().join("index");
        self.check_disks(data, self.runtime.min_free_gib)?;
        self.downloader.check_cancelled()?;
        self.start_prefetch(next, data);
        progress::message(&format!(
            "[{}] Building persistent object index from the complete source",
            entry.id
        ));
        incremental::initialize(
            &incremental::InitOptions {
                input: pbf,
                state: candidate.clone(),
                coverage: job.join("coverage.json"),
                region: entry.id.clone(),
                source_timestamp: metadata.timestamp,
                source_sequence: metadata.sequence,
                source_sha256: metadata.sha256,
                replication_url: metadata.replication_url,
            },
            &self.runtime.compute_options(),
        )?;
        self.check_disks(data, self.runtime.min_free_gib)?;
        fs::rename(&candidate, &state)?;
        sync_directory(data)?;
        let current = status(entry, data, self.runtime)?;
        progress::message(&format!(
            "[{}] Index ready at sequence {}: {}",
            entry.id,
            sequence(&current)?,
            string(&current, "output")?
        ));
        Ok(current)
    }

    fn update(&self, entry: &Region, data: &Path) -> Result<Value> {
        self.check_disks(data, self.runtime.min_free_gib)?;
        let mut current = status(entry, data, self.runtime)?;
        let work = tempfile::Builder::new()
            .prefix("replication-")
            .tempdir_in(data)?;
        let url = string(&current, "replicationUrl")?.to_owned();
        let anchor = self.downloader.remote_state(
            self.runtime,
            &url,
            work.path(),
            Some(sequence(&current)?),
        )?;
        ensure!(
            anchor.timestamp == string(&current, "timestamp")?,
            "Regional replication history changed; the local anchor no longer matches"
        );
        let latest = self
            .downloader
            .remote_state(self.runtime, &url, work.path(), None)?;
        ensure!(
            latest.sequence >= anchor.sequence && latest.timestamp >= anchor.timestamp,
            "Regional replication sequence or timestamp regressed"
        );
        let pending = latest.sequence - anchor.sequence;
        source::prepare(entry, &self.catalog, work.path())?;
        let coverage_path = work.path().join("coverage.json");
        let coverage_hash = format::hash_bytes(&format::canonical_json(&format::read_coverage(
            &coverage_path,
        )?)?);
        ensure!(
            pending != 0 || string(&current, "coverageSHA256")? == coverage_hash,
            "Catalog coverage changed without a new regional replication sequence; wait for the next extract"
        );
        progress::message(&format!(
            "[{}] {pending} daily changes: {} → {}",
            entry.id, anchor.sequence, latest.sequence
        ));
        for number in anchor.sequence + 1..=latest.sequence {
            self.downloader.check_cancelled()?;
            if let Some(guard) = self.lease {
                guard.lease()?;
            }
            self.check_disks(data, self.runtime.min_free_gib)?;
            let next =
                self.downloader
                    .remote_state(self.runtime, &url, work.path(), Some(number))?;
            ensure!(
                next.timestamp.as_str() >= string(&current, "timestamp")?
                    && next.timestamp <= latest.timestamp,
                "Replication timestamps are not a continuous forward sequence"
            );
            ensure!(
                number != latest.sequence || next == latest,
                "Latest replication state changed within the selected sequence"
            );
            let change = work.path().join("change.osc.gz");
            self.downloader.download(
                self.runtime,
                &format!("{url}/{}.osc.gz", source::sequence_path(number)?),
                &change,
                Some(self.runtime.diff_max_bytes),
            )?;
            current = incremental::apply_diff(
                &incremental::ApplyOptions {
                    input: change,
                    state: data.join(&entry.id),
                    sequence: number,
                    timestamp: next.timestamp,
                    coverage: (number == latest.sequence).then(|| coverage_path.clone()),
                },
                &self.runtime.compute_options(),
            )?;
            progress::message(&format!(
                "[{}] Applied sequence {number}/{}",
                entry.id, latest.sequence
            ));
        }
        Ok(current)
    }
}

trait RegionalOperations {
    fn initialize(
        &mut self,
        entry: &Region,
        data: &Path,
        job: Option<&Path>,
        next: Option<&Region>,
    ) -> Result<Value>;
    fn update(&mut self, entry: &Region, data: &Path) -> Result<Value>;
    fn status(&mut self, entry: &Region, data: &Path) -> Result<Value>;
    fn publish(&mut self, output: &Path) -> Result<()>;
    fn published_regions(&mut self) -> Result<BTreeMap<String, String>>;
    fn start_prefetch(&mut self, next: Option<&Region>, data: &Path);
    fn cancel_prefetch(&mut self) {}
}

impl RegionalOperations for Operations<'_> {
    fn initialize(
        &mut self,
        entry: &Region,
        data: &Path,
        job: Option<&Path>,
        next: Option<&Region>,
    ) -> Result<Value> {
        Operations::initialize(self, entry, data, job, next)
    }
    fn update(&mut self, entry: &Region, data: &Path) -> Result<Value> {
        Operations::update(self, entry, data)
    }
    fn status(&mut self, entry: &Region, data: &Path) -> Result<Value> {
        status(entry, data, self.runtime)
    }
    fn publish(&mut self, output: &Path) -> Result<()> {
        let lease = self
            .lease
            .context("Publication requires a claimed task")?
            .lease()?;
        let mut reporter = ProgressReporter::default();
        self.publisher()?
            .publish(output, &lease, |event| reporter.update(event))?;
        Ok(())
    }
    fn published_regions(&mut self) -> Result<BTreeMap<String, String>> {
        let Some(state) = self.publisher()?.read_state()? else {
            return Ok(BTreeMap::new());
        };
        Ok(state
            .regions
            .into_iter()
            .map(|region| (region.region, region.manifest))
            .collect())
    }
    fn cancel_prefetch(&mut self) {
        self.prefetch = None;
    }
    fn start_prefetch(&mut self, next: Option<&Region>, data: &Path) {
        if self.prefetch.is_none()
            && let Some(next) = next
        {
            self.prefetch = Some(Prefetch::start(self.runtime, next, &self.catalog, data));
        }
    }
}

fn is_symlink(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => Ok(metadata.file_type().is_symlink()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn reject_symlinks(paths: &[&Path]) -> Result<()> {
    for path in paths {
        ensure!(
            !is_symlink(path)?,
            "Cleanup paths must not be symbolic links: {}",
            path.display()
        );
    }
    Ok(())
}

fn managed_download(name: &str, entry: &Region) -> bool {
    name.strip_prefix(&format!("{}-", entry.id))
        .is_some_and(|suffix| {
            !suffix.is_empty()
                && suffix
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        })
}

fn validate_release(path: &Path, entry: &Region, manifest: &str) -> Result<()> {
    let release: Value = serde_json::from_slice(&fs::read(path)?)?;
    ensure!(
        release["region"].as_str() == Some(&entry.id)
            && release["manifest"].as_str() == Some(manifest),
        "Local release does not match the confirmed published manifest"
    );
    Ok(())
}

pub fn resume_cleanup(entry: &Region, data: &Path, manifest: &str) -> Result<()> {
    let marker = data.join(format!(".cleanup-{}.json", entry.id));
    let downloads = data.join(".downloads");
    reject_symlinks(&[data, &marker, &downloads])?;
    ensure!(
        fs::metadata(&marker)?.len() <= 1024 * 1024,
        "Invalid cleanup record"
    );
    let record: Value = serde_json::from_slice(&fs::read(&marker)?)?;
    ensure!(
        format::is_hash(manifest)
            && record["region"].as_str() == Some(&entry.id)
            && record["manifest"].as_str() == Some(manifest),
        "Cleanup record does not match the published region manifest"
    );
    let names = record["downloads"]
        .as_array()
        .context("Invalid download path in cleanup record")?;
    let mut targets = vec![data.join(&entry.id)];
    for name in names {
        let name = name
            .as_str()
            .context("Invalid download path in cleanup record")?;
        ensure!(
            managed_download(name, entry),
            "Invalid download path in cleanup record"
        );
        targets.push(downloads.join(name));
    }
    for target in &targets {
        reject_symlinks(&[target])?;
        ensure!(
            !target.try_exists()? || target.is_dir(),
            "Cleanup target is not a managed directory: {}",
            target.display()
        );
    }
    let release = targets[0].join("output/release.json");
    reject_symlinks(&[release.parent().context("Invalid release path")?, &release])?;
    if release.try_exists()? {
        validate_release(&release, entry, manifest)?;
    }
    for target in &targets[1..] {
        let url = target.join("source.url");
        reject_symlinks(&[&url])?;
        if url.try_exists()? {
            ensure!(
                fs::read_to_string(url)?.trim() == entry.source_url(),
                "Download source changed after publication"
            );
        }
    }
    for target in targets {
        if target.try_exists()? {
            fs::remove_dir_all(target)?;
        }
    }
    if downloads.try_exists()? {
        sync_directory(&downloads)?;
    }
    fs::remove_file(marker)?;
    sync_directory(data)?;
    progress::message(&format!(
        "[{}] Published data removed from local disk",
        entry.id
    ));
    Ok(())
}

pub fn cleanup_region(entry: &Region, data: &Path, manifest: &str) -> Result<()> {
    ensure!(format::is_hash(manifest), "Invalid published manifest");
    let state = data.join(&entry.id);
    let downloads = data.join(".downloads");
    let release = state.join("output/release.json");
    let marker = data.join(format!(".cleanup-{}.json", entry.id));
    reject_symlinks(&[
        data,
        &state,
        &state.join("output"),
        &release,
        &downloads,
        &marker,
    ])?;
    ensure!(
        !marker.try_exists()?,
        "Cleanup is already pending for this region"
    );
    validate_release(&release, entry, manifest)?;
    let mut names = Vec::new();
    if downloads.try_exists()? {
        for directory in fs::read_dir(&downloads)? {
            let directory = directory?;
            let name = directory.file_name();
            let Some(name) = name.to_str().filter(|name| managed_download(name, entry)) else {
                continue;
            };
            let path = directory.path();
            let url = path.join("source.url");
            reject_symlinks(&[&path, &url])?;
            if path.is_dir()
                && url.is_file()
                && fs::read_to_string(&url)?.trim() == entry.source_url()
            {
                names.push(name.to_owned());
            }
        }
    }
    names.sort();
    atomic_write(
        &marker,
        &serde_json::to_vec(&json!({"region":entry.id,"manifest":manifest,"downloads":names}))?,
    )?;
    resume_cleanup(entry, data, manifest)
}

fn execute_regions(
    operations: &mut impl RegionalOperations,
    entries: &[Region],
    command: &str,
    data: &Path,
    job: Option<&Path>,
    cleanup: bool,
) -> Result<()> {
    let published = if cleanup {
        operations.published_regions()?
    } else {
        BTreeMap::new()
    };
    let mut failures = Vec::new();
    for (number, entry) in entries.iter().enumerate() {
        println!("[{}/{}] {command} {}", number + 1, entries.len(), entry.id);
        let result = (|| -> Result<()> {
            let remote = published.get(&entry.id);
            let marker = data.join(format!(".cleanup-{}.json", entry.id));
            if marker.try_exists()? || is_symlink(&marker)? {
                ensure!(
                    cleanup,
                    "Region cleanup is incomplete; resume bootstrap with --cleanup"
                );
                resume_cleanup(
                    entry,
                    data,
                    remote.context("Cleanup record has no published region")?,
                )?;
            }
            if cleanup && let Some(manifest) = remote {
                if !data.join(&entry.id).try_exists()? {
                    println!("[{}] Already published; skipping build", entry.id);
                    return Ok(());
                }
                let current = operations.status(entry, data)?;
                if string(&current, "manifest")? == manifest {
                    return cleanup_region(entry, data, manifest);
                }
            }
            if command == "init" {
                operations.initialize(entry, data, job, None)?;
                return Ok(());
            }
            if command == "bootstrap" {
                let next = entries[number + 1..].iter().find(|candidate| {
                    [
                        data.join(&candidate.id),
                        data.join(format!(".cleanup-{}.json", candidate.id)),
                    ]
                    .iter()
                    .all(|path| {
                        fs::symlink_metadata(path)
                            .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
                    }) && !(cleanup && published.contains_key(&candidate.id))
                });
                if !data.join(&entry.id).try_exists()? {
                    operations.initialize(entry, data, None, next)?;
                } else {
                    operations.start_prefetch(next, data);
                }
            }
            let current = operations.update(entry, data)?;
            operations.publish(Path::new(string(&current, "output")?))?;
            if cleanup {
                let confirmed = operations.published_regions()?;
                let manifest = string(&current, "manifest")?;
                ensure!(
                    confirmed
                        .get(&entry.id)
                        .is_some_and(|value| value == manifest),
                    "Published region does not match the local release; retaining local data"
                );
                cleanup_region(entry, data, manifest)?;
            }
            Ok(())
        })();
        if let Err(error) = result {
            eprintln!("[{}] Failed: {error:#}", entry.id);
            failures.push(entry.id.as_str());
            if cleanup {
                break;
            }
        }
    }
    operations.cancel_prefetch();
    if !failures.is_empty() {
        bail!("Failed regions: {}", failures.join(", "));
    }
    Ok(())
}

pub fn submit(
    root: &Path,
    runtime: &Runtime,
    environment: &Environment,
    mode: &str,
    selection: &str,
) -> Result<String> {
    let entries = source::regions(&root.join("config/regions.json"))?
        .into_iter()
        .filter(|entry| selection == "all" || entry.id == selection)
        .collect::<Vec<_>>();
    ensure!(!entries.is_empty(), "Region is not configured: {selection}");
    Publisher::new(runtime, &PublishConfig::from_environment(environment)?)?
        .coordinator()
        .start(mode, &entries)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum WorkPhase {
    Idle,
    Preparing,
    Computing,
    Uploading,
}

struct PipelineFlow {
    cancelled: Arc<AtomicBool>,
    phases: Mutex<[WorkPhase; 2]>,
    sources_ready: Mutex<[bool; 2]>,
    changed: Condvar,
    claims: Mutex<()>,
    issued: AtomicUsize,
    compute: Mutex<usize>,
    compute_changed: Condvar,
    upload: Mutex<()>,
    download: Mutex<()>,
    controls: Mutex<Vec<Weak<DownloadControl>>>,
    failure: Mutex<Option<String>>,
    cleanup: bool,
}

impl PipelineFlow {
    fn new(cleanup: bool) -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
            phases: Mutex::new([WorkPhase::Idle; 2]),
            sources_ready: Mutex::new([false; 2]),
            changed: Condvar::new(),
            claims: Mutex::new(()),
            issued: AtomicUsize::new(0),
            compute: Mutex::new(0),
            compute_changed: Condvar::new(),
            upload: Mutex::new(()),
            download: Mutex::new(()),
            controls: Mutex::new(Vec::new()),
            failure: Mutex::new(None),
            cleanup,
        }
    }

    fn check(&self) -> Result<()> {
        ensure!(
            !self.cancelled.load(Ordering::Acquire),
            "Pipeline stopped after a regional failure"
        );
        Ok(())
    }

    fn stop(&self) {
        self.cancelled.store(true, Ordering::Release);
        for control in self
            .controls
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .filter_map(Weak::upgrade)
        {
            control.cancel();
        }
        let _phases = self
            .phases
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        self.changed.notify_all();
        let _compute = self
            .compute
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        self.compute_changed.notify_all();
    }

    fn phase(&self, slot: usize, phase: WorkPhase) {
        if phase == WorkPhase::Preparing {
            self.sources_ready.lock().expect("source readiness lock")[slot] = false;
        }
        self.phases.lock().expect("pipeline phases lock")[slot] = phase;
        self.changed.notify_all();
    }

    fn can_reclaim(&self, slot: usize) -> bool {
        self.cleanup
            && self.phases.lock().expect("pipeline phases lock")[1 - slot] == WorkPhase::Uploading
    }

    fn control(self: &Arc<Self>, slot: usize) -> Arc<DownloadControl> {
        let control = Arc::new(DownloadControl {
            pipeline: Some((Arc::clone(self), slot)),
            ..DownloadControl::default()
        });
        let mut controls = self.controls.lock().expect("download controls lock");
        controls.retain(|control| control.strong_count() != 0);
        controls.push(Arc::downgrade(&control));
        if self.cancelled.load(Ordering::Acquire) {
            control.cancel();
        }
        control
    }

    fn wait(&self, duration: Duration) {
        let phases = self.phases.lock().expect("pipeline phases lock");
        if self.cancelled.load(Ordering::Acquire) {
            return;
        }
        let _ = self
            .changed
            .wait_timeout(phases, duration)
            .expect("pipeline wait");
    }

    fn compute_turn(&self, ticket: usize) -> Result<ComputeTurn<'_>> {
        let mut next = self.compute.lock().expect("compute order lock");
        loop {
            self.check()?;
            if *next == ticket {
                return Ok(ComputeTurn(self));
            }
            next = self.compute_changed.wait(next).expect("compute order wait");
        }
    }
}

struct ComputeTurn<'a>(&'a PipelineFlow);

impl Drop for ComputeTurn<'_> {
    fn drop(&mut self) {
        *self
            .0
            .compute
            .lock()
            .unwrap_or_else(|error| error.into_inner()) += 1;
        self.0.compute_changed.notify_all();
    }
}

enum WorkClaim<T> {
    Task(T),
    Idle {
        done: bool,
        failed: usize,
        retry: Duration,
    },
}

struct StopOnPanic<'a>(&'a PipelineFlow);

impl Drop for StopOnPanic<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.stop();
        }
    }
}

trait WorkOperations: Sync {
    type Task: Send;
    type Output;

    fn claim(&self, slot: u8) -> Result<WorkClaim<Self::Task>>;
    fn region<'a>(&self, task: &'a Self::Task) -> &'a str;
    fn prepare(&self, task: &mut Self::Task, flow: &Arc<PipelineFlow>, slot: usize) -> Result<()>;
    fn ready(&self, task: &Self::Task) -> Result<()>;
    fn compute(
        &self,
        task: &mut Self::Task,
        flow: &Arc<PipelineFlow>,
        slot: usize,
    ) -> Result<Self::Output>;
    fn publish(
        &self,
        task: &Self::Task,
        output: &Self::Output,
        cancelled: &AtomicBool,
    ) -> Result<()>;
    fn cleanup(&self, task: &Self::Task, output: &Self::Output) -> Result<()>;
    fn release(&self, task: &mut Self::Task, outcome: dispatch::ReleaseOutcome) -> Result<()>;
    fn failed(&self, _task: &Self::Task) -> bool {
        false
    }
}

fn execute_work(operations: &impl WorkOperations, once: bool, cleanup: bool) -> Result<()> {
    let flow = Arc::new(PipelineFlow::new(cleanup));
    std::thread::scope(|scope| {
        let mut workers = Vec::new();
        for slot in 0..2 {
            let flow = Arc::clone(&flow);
            workers.push(scope.spawn(move || -> Result<()> {
                let _stop_on_panic = StopOnPanic(&flow);
                loop {
                    let (claim, ticket) = {
                        let _claim = flow.claims.lock().expect("claim lock");
                        if flow.check().is_err() {
                            return Ok(());
                        }
                        match operations.claim(slot as u8) {
                            Ok(claim) => {
                                let ticket = if matches!(&claim, WorkClaim::Task(_)) {
                                    Some(flow.issued.fetch_add(1, Ordering::Relaxed))
                                } else { None };
                                (claim, ticket)
                            },
                            Err(error) => {
                                flow.stop();
                                return Err(error);
                            }
                        }
                    };
                    let mut task = match claim {
                        WorkClaim::Task(task) => task,
                        WorkClaim::Idle { done, failed, retry } => {
                            if once && done {
                                if failed != 0 {
                                    flow.stop();
                                    bail!("Batch has {failed} failed regions; inspect osm jobs and submit a retry batch");
                                }
                                return Ok(());
                            }
                            flow.wait(retry);
                            continue;
                        }
                    };
                    let region = operations.region(&task).to_owned();
                    let _label = progress::region(&region);
                    flow.phase(slot, WorkPhase::Preparing);
                    let result = (|| -> Result<()> {
                        flow.check()?;
                        operations.prepare(&mut task, &flow, slot)?;
                        let output = {
                            let _compute = flow.compute_turn(ticket.expect("Claimed task has a compute ticket"))?;
                            let mut waiting = false;
                            loop {
                                flow.check()?;
                                match operations.ready(&task) {
                                    Ok(()) => break,
                                    Err(error) if flow.can_reclaim(slot) => {
                                        if !waiting {
                                            progress::message(&format!("Waiting for publication cleanup: {error}"));
                                            waiting = true;
                                        }
                                        flow.wait(Duration::from_secs(1));
                                    }
                                    Err(error) => return Err(error),
                                }
                            }
                            flow.phase(slot, WorkPhase::Computing);
                            let output = operations.compute(&mut task, &flow, slot)?;
                            flow.phase(slot, WorkPhase::Uploading);
                            output
                        };
                        let _upload = flow.upload.lock().expect("upload lock");
                        flow.check()?;
                        operations.publish(&task, &output, &flow.cancelled)?;
                        if cleanup {
                            operations.cleanup(&task, &output)?;
                        }
                        progress::message("Published");
                        Ok(())
                    })();
                    if let Err(error) = result {
                        let outcome = if flow.cancelled.load(Ordering::Acquire) && !operations.failed(&task) {
                            dispatch::ReleaseOutcome::Retry
                        } else {
                            dispatch::ReleaseOutcome::Failed
                        };
                        flow.stop();
                        if let Err(release_error) = operations.release(&mut task, outcome) {
                            progress::message(&format!("Could not release task: {release_error:#}"));
                        }
                        return Err(error.context(format!("Region {region} failed")));
                    }
                    flow.phase(slot, WorkPhase::Idle);
                }
            }));
        }
        let mut errors = Vec::new();
        for worker in workers {
            match worker.join() {
                Ok(Ok(())) => {}
                Ok(Err(error)) => errors.push(format!("{error:#}")),
                Err(_) => {
                    flow.stop();
                    errors.push("Regional worker panicked".into());
                }
            }
        }
        if let Some(failure) = flow.failure.lock().expect("pipeline failure lock").take() {
            errors.insert(0, failure);
        }
        ensure!(errors.is_empty(), "{}", errors.join("; "));
        Ok(())
    })
}

struct ClaimedRegion {
    lease: dispatch::Lease,
    prefetch: Option<Prefetch>,
    download_control: Option<Arc<DownloadControl>>,
    catalog: Option<tempfile::TempDir>,
    guard: Option<dispatch::Guard>,
}

struct CloudWork<'a> {
    runtime: &'a Runtime,
    environment: &'a Environment,
    scratch: PathBuf,
    device: String,
    publisher: Publisher,
    client: dispatch::Client,
    cleanup: bool,
}

impl WorkOperations for CloudWork<'_> {
    type Task = ClaimedRegion;
    type Output = Value;

    fn claim(&self, slot: u8) -> Result<WorkClaim<Self::Task>> {
        let local_regions = fs::read_dir(&self.runtime.data_dir)?
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| {
                let name = entry.file_name().into_string().ok()?;
                (format::is_region(&name) && entry.path().join("state.sqlite").is_file())
                    .then_some(name)
            })
            .collect::<Vec<_>>();
        let claim = self.client.claim(&self.device, &local_regions, slot)?;
        Ok(match claim.lease {
            Some(lease) => WorkClaim::Task(ClaimedRegion {
                lease,
                guard: None,
                catalog: None,
                prefetch: None,
                download_control: None,
            }),
            None => WorkClaim::Idle {
                done: claim.pending == 0 && claim.running == 0,
                failed: claim.failed,
                retry: Duration::from_secs(claim.retry_after_seconds.clamp(1, 60)),
            },
        })
    }

    fn region<'a>(&self, task: &'a Self::Task) -> &'a str {
        &task.lease.region
    }

    fn prepare(&self, task: &mut Self::Task, flow: &Arc<PipelineFlow>, slot: usize) -> Result<()> {
        task.guard = Some(dispatch::Guard::start(
            self.client.clone(),
            task.lease.clone(),
        )?);
        let entry = Region {
            id: task.lease.region.clone(),
            extract: task.lease.extract.clone(),
        };
        progress::message(&format!("Claimed generation {}", task.lease.generation));
        let data = &self.runtime.data_dir;
        let marker = data.join(format!(".cleanup-{}.json", entry.id));
        if marker.try_exists()? || is_symlink(&marker)? {
            ensure!(
                self.cleanup,
                "Region cleanup is incomplete; resume work with --cleanup"
            );
            let published = self
                .publisher
                .read_state()?
                .context("Cleanup record has no published state")?;
            let manifest = &published
                .regions
                .iter()
                .find(|region| region.region == entry.id)
                .context("Cleanup record has no published region")?
                .manifest;
            resume_cleanup(&entry, data, manifest)?;
        }
        let catalog = tempfile::Builder::new()
            .prefix("catalog-")
            .tempdir_in(&self.scratch)?;
        let mut downloader = Downloader::new(self.runtime)?;
        let control = flow.control(slot);
        control.consume();
        downloader.control = Some(control);
        downloader.download(
            self.runtime,
            INDEX_URL,
            &catalog.path().join("index.json"),
            Some(64 * 1024 * 1024),
        )?;
        flow.check()?;
        if !data.join(&entry.id).try_exists()? {
            let control = flow.control(slot);
            task.prefetch = Some(Prefetch::controlled(
                self.runtime,
                &entry,
                &catalog.path().join("index.json"),
                data,
                Arc::clone(&control),
            ));
            task.download_control = Some(control);
        } else {
            flow.sources_ready.lock().expect("source readiness lock")[slot] = true;
        }
        task.catalog = Some(catalog);
        Ok(())
    }

    fn ready(&self, task: &Self::Task) -> Result<()> {
        let minimum = if self
            .runtime
            .data_dir
            .join(&task.lease.region)
            .try_exists()?
        {
            self.runtime.min_free_gib
        } else {
            self.runtime.start_free_gib.max(self.runtime.min_free_gib)
        };
        disk_check(&self.runtime.data_dir, minimum)?;
        disk_check(&self.scratch, minimum)
    }

    fn compute(
        &self,
        task: &mut Self::Task,
        flow: &Arc<PipelineFlow>,
        slot: usize,
    ) -> Result<Value> {
        let guard = task
            .guard
            .as_ref()
            .context("Missing regional lease guard")?;
        let entry = Region {
            id: task.lease.region.clone(),
            extract: task.lease.extract.clone(),
        };
        let mut downloader = Downloader::new(self.runtime)?;
        let control = flow.control(slot);
        control.consume();
        downloader.control = Some(control);
        let mut operations = Operations {
            runtime: self.runtime,
            environment: self.environment,
            scratch: &self.scratch,
            downloader,
            catalog: task
                .catalog
                .as_ref()
                .context("Missing regional catalog")?
                .path()
                .join("index.json"),
            publisher: None,
            lease: Some(guard),
            prefetch: task.prefetch.take(),
        };
        guard.lease()?;
        if !self.runtime.data_dir.join(&entry.id).try_exists()? {
            operations.initialize(&entry, &self.runtime.data_dir, None, None)?;
        }
        flow.check()?;
        guard.lease()?;
        operations.update(&entry, &self.runtime.data_dir)
    }

    fn publish(&self, task: &Self::Task, output: &Value, cancelled: &AtomicBool) -> Result<()> {
        let lease = task
            .guard
            .as_ref()
            .context("Missing regional lease guard")?
            .lease()?;
        let mut reporter = ProgressReporter::default();
        self.publisher.publish_with_cancel(
            Path::new(string(output, "output")?),
            &lease,
            cancelled,
            |event| reporter.update(event),
        )?;
        Ok(())
    }

    fn cleanup(&self, task: &Self::Task, output: &Value) -> Result<()> {
        cleanup_region(
            &Region {
                id: task.lease.region.clone(),
                extract: task.lease.extract.clone(),
            },
            &self.runtime.data_dir,
            string(output, "manifest")?,
        )
    }

    fn release(&self, task: &mut Self::Task, outcome: dispatch::ReleaseOutcome) -> Result<()> {
        task.prefetch = None;
        self.client.release(&task.lease, outcome)
    }

    fn failed(&self, task: &Self::Task) -> bool {
        task.download_control
            .as_ref()
            .is_some_and(|control| control.failed.load(Ordering::Acquire))
            || task
                .guard
                .as_ref()
                .is_some_and(|guard| guard.lease().is_err())
    }
}

pub fn work(
    root: &Path,
    runtime: &Runtime,
    environment: &Environment,
    once: bool,
    cleanup: bool,
) -> Result<()> {
    let data = &runtime.data_dir;
    fs::create_dir_all(data)?;
    let device = dispatch::device_id(data)?;
    let publisher = Publisher::new(runtime, &PublishConfig::from_environment(environment)?)?;
    let client = publisher.coordinator();
    let scratch = root.join(".build/tools/tmp");
    fs::create_dir_all(&scratch)?;
    println!("Device {device}: waiting for regional tasks");
    execute_work(
        &CloudWork {
            runtime,
            environment,
            scratch,
            device,
            publisher,
            client,
            cleanup,
        },
        once,
        cleanup,
    )
}

pub fn build(
    root: &Path,
    runtime: &Runtime,
    selection: &str,
    job: Option<&Path>,
) -> Result<PathBuf> {
    let entry = source::regions(&root.join("config/regions.json"))?
        .into_iter()
        .find(|entry| entry.id == selection)
        .with_context(|| format!("Region is not configured: {selection}"))?;
    let scratch = root.join(".build/tools/tmp");
    fs::create_dir_all(&runtime.data_dir)?;
    fs::create_dir_all(&scratch)?;
    let minimum = if job.is_some() {
        runtime.min_free_gib
    } else {
        runtime.start_free_gib
    };
    disk_check(&runtime.data_dir, minimum)?;
    disk_check(&scratch, minimum)?;
    let (input, work) = match job {
        Some(input) => {
            let input = input.canonicalize()?;
            ensure!(
                fs::read_to_string(input.join("source.url"))?.trim() == entry.source_url(),
                "Downloaded job does not match the configured regional source"
            );
            for name in ["source.osm.pbf", "source.md5", "coverage.json"] {
                ensure!(
                    input.join(name).is_file(),
                    "Missing {}",
                    input.join(name).display()
                );
            }
            let work = tempfile::Builder::new()
                .prefix("rebuild-")
                .tempdir_in(&input)?
                .keep();
            (input, work)
        }
        None => {
            let builds = runtime.data_dir.join(".builds").join(&entry.id);
            fs::create_dir_all(&builds)?;
            let work = tempfile::Builder::new()
                .prefix(&chrono::Utc::now().format("%Y%m%dT%H%M%SZ-").to_string())
                .tempdir_in(&builds)?
                .keep();
            let downloader = Downloader::new(runtime)?;
            let catalog = work.join("index.json");
            downloader.download(runtime, INDEX_URL, &catalog, Some(64 * 1024 * 1024))?;
            downloader.source_snapshot(runtime, &entry, &catalog, &work)?;
            (work.clone(), work)
        }
    };
    let metadata = source::stamp(
        &input.join("source.osm.pbf"),
        &input.join("source.md5"),
        &entry.updates_url(),
    )?;
    atomic_write(
        &work.join("source.json"),
        &serde_json::to_vec_pretty(&json!({
            "sourceTimestamp":metadata.timestamp,"sourceSequence":metadata.sequence,"sourceSHA256":metadata.sha256,
        }))?,
    )?;
    disk_check(&runtime.data_dir, runtime.min_free_gib)?;
    disk_check(&scratch, runtime.min_free_gib)?;
    let output = work.join("output");
    build::run(
        &build::BuildOptions {
            input: input.join("source.osm.pbf"),
            coverage: input.join("coverage.json"),
            region: entry.id,
            source_timestamp: metadata.timestamp,
            source_sequence: Some(metadata.sequence),
            source_sha256: metadata.sha256,
            output: output.clone(),
            scratch,
        },
        &runtime.compute_options(),
    )?;
    disk_check(&runtime.data_dir, runtime.min_free_gib)?;
    Ok(output)
}

pub fn run(
    root: &Path,
    runtime: &Runtime,
    environment: &Environment,
    command: &str,
    selection: &str,
    job: Option<&Path>,
    cleanup: bool,
) -> Result<()> {
    ensure!(
        matches!(command, "init" | "bootstrap" | "update"),
        "Unknown pipeline command: {command}"
    );
    ensure!(
        !cleanup || command == "bootstrap",
        "--cleanup is only supported by bootstrap"
    );
    ensure!(
        job.is_none() || command == "init",
        "Only init accepts an existing downloaded job"
    );
    ensure!(
        command != "init" || selection != "all",
        "init requires one region; use bootstrap all for the initial batch"
    );
    let mut entries = source::regions(&root.join("config/regions.json"))?;
    if selection != "all" {
        entries.retain(|entry| entry.id == selection);
    }
    ensure!(!entries.is_empty(), "Region is not configured: {selection}");
    if command != "init" {
        let publisher = Publisher::new(runtime, &PublishConfig::from_environment(environment)?)?;
        if cleanup {
            for entry in &entries {
                let marker = runtime.data_dir.join(format!(".cleanup-{}.json", entry.id));
                if marker.try_exists()? || is_symlink(&marker)? {
                    let current = publisher
                        .read_state()?
                        .context("Cleanup record has no published state")?;
                    let release = current
                        .regions
                        .iter()
                        .find(|region| region.region == entry.id)
                        .context("Cleanup record has no published region")?;
                    resume_cleanup(entry, &runtime.data_dir, &release.manifest)?;
                }
            }
        }
        let batch = publisher.coordinator().start(command, &entries)?;
        println!("Submitted batch: {batch}");
        return work(root, runtime, environment, true, cleanup);
    }
    let data = &runtime.data_dir;
    if cleanup {
        reject_symlinks(&[data])?;
    }
    fs::create_dir_all(data)?;
    let data = data.canonicalize()?;
    let scratch = root.join(".build/tools/tmp");
    fs::create_dir_all(&scratch)?;
    let work = tempfile::Builder::new()
        .prefix("catalog-")
        .tempdir_in(&scratch)?;
    let mut operations = Operations {
        runtime,
        environment,
        scratch: &scratch,
        downloader: Downloader::new(runtime)?,
        catalog: work.path().join("index.json"),
        publisher: None,
        lease: None,
        prefetch: None,
    };
    if cleanup {
        operations.publisher()?;
    }
    if command != "init" || job.is_none() {
        operations.downloader.download(
            runtime,
            INDEX_URL,
            &operations.catalog,
            Some(64 * 1024 * 1024),
        )?;
    }
    println!(
        "Regions run one at a time; compute worker limit: {}",
        runtime.compute_workers
    );
    execute_regions(&mut operations, &entries, command, &data, job, cleanup)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn runtime() -> Runtime {
        Runtime::load(
            Path::new(env!("CARGO_MANIFEST_DIR")),
            &Environment::default(),
        )
        .unwrap()
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum ScheduleCase {
        Overlap,
        LowDisk,
        UploadFailure,
        ComputeOrder,
    }

    #[derive(Default)]
    struct ScheduleState {
        next: usize,
        active: usize,
        maximum: usize,
        computes: usize,
        uploads: usize,
        events: Vec<String>,
    }

    struct ScheduleOperations {
        case: ScheduleCase,
        state: Mutex<ScheduleState>,
        changed: Condvar,
    }

    impl ScheduleOperations {
        fn new(case: ScheduleCase) -> Self {
            Self {
                case,
                state: Mutex::new(ScheduleState::default()),
                changed: Condvar::new(),
            }
        }

        fn event(&self, event: &str) {
            self.state.lock().unwrap().events.push(event.into());
            self.changed.notify_all();
        }

        fn wait_for(&self, event: &str) {
            let state = self.state.lock().unwrap();
            let (state, timed) = self
                .changed
                .wait_timeout_while(state, Duration::from_secs(5), |state| {
                    !state.events.iter().any(|value| value == event)
                })
                .unwrap();
            assert!(
                !timed.timed_out(),
                "Missing {event}; events: {:?}",
                state.events
            );
        }
    }

    impl WorkOperations for ScheduleOperations {
        type Task = &'static str;
        type Output = ();

        fn claim(&self, _slot: u8) -> Result<WorkClaim<Self::Task>> {
            let mut state = self.state.lock().unwrap();
            let total = if self.case == ScheduleCase::LowDisk {
                2
            } else {
                3
            };
            if state.next == total {
                return Ok(WorkClaim::Idle {
                    done: state.active == 0,
                    failed: 0,
                    retry: Duration::from_millis(10),
                });
            }
            let region = ["a", "b", "c"][state.next];
            state.next += 1;
            state.active += 1;
            state.maximum = state.maximum.max(state.active);
            assert!(state.active <= 2);
            state.events.push(format!("claim:{region}"));
            self.changed.notify_all();
            Ok(WorkClaim::Task(region))
        }

        fn region<'a>(&self, task: &'a Self::Task) -> &'a str {
            task
        }

        fn prepare(
            &self,
            task: &mut Self::Task,
            _flow: &Arc<PipelineFlow>,
            _slot: usize,
        ) -> Result<()> {
            if *task == "b" {
                self.wait_for("compute:a");
            }
            self.event(&format!("prepare:{task}"));
            if *task == "b" && self.case == ScheduleCase::ComputeOrder {
                self.wait_for("claim:c");
            }
            Ok(())
        }

        fn ready(&self, task: &Self::Task) -> Result<()> {
            if *task == "b"
                && self.case == ScheduleCase::LowDisk
                && !self
                    .state
                    .lock()
                    .unwrap()
                    .events
                    .iter()
                    .any(|event| event == "cleanup:a")
            {
                self.event("wait:b");
                bail!("Injected insufficient disk space");
            }
            Ok(())
        }

        fn compute(
            &self,
            task: &mut Self::Task,
            flow: &Arc<PipelineFlow>,
            _slot: usize,
        ) -> Result<()> {
            {
                let mut state = self.state.lock().unwrap();
                state.computes += 1;
                assert_eq!(state.computes, 1);
            }
            self.event(&format!("compute:{task}"));
            if *task == "a" {
                self.wait_for("prepare:b");
            } else if *task == "b" {
                match self.case {
                    ScheduleCase::Overlap => {
                        self.wait_for("upload:a");
                        self.wait_for("prepare:c");
                    }
                    ScheduleCase::LowDisk => self.wait_for("cleanup:a"),
                    ScheduleCase::UploadFailure => {
                        let deadline = std::time::Instant::now() + Duration::from_secs(5);
                        while !flow.cancelled.load(Ordering::Acquire) {
                            assert!(
                                std::time::Instant::now() < deadline,
                                "Publication failure did not stop computation scheduling"
                            );
                            flow.wait(Duration::from_millis(10));
                        }
                    }
                    ScheduleCase::ComputeOrder => {}
                }
            } else if *task == "c" && self.case == ScheduleCase::ComputeOrder {
                self.wait_for("computed:b");
            }
            self.event(&format!("computed:{task}"));
            self.state.lock().unwrap().computes -= 1;
            Ok(())
        }

        fn publish(&self, task: &Self::Task, _output: &(), _cancelled: &AtomicBool) -> Result<()> {
            {
                let mut state = self.state.lock().unwrap();
                state.uploads += 1;
                assert_eq!(state.uploads, 1);
            }
            self.event(&format!("upload:{task}"));
            if *task == "a" && self.case != ScheduleCase::ComputeOrder {
                self.wait_for(if self.case == ScheduleCase::LowDisk {
                    "wait:b"
                } else {
                    "compute:b"
                });
            }
            self.state.lock().unwrap().uploads -= 1;
            ensure!(
                *task != "a" || self.case != ScheduleCase::UploadFailure,
                "Injected upload failure"
            );
            self.event(&format!("ack:{task}"));
            Ok(())
        }

        fn cleanup(&self, task: &Self::Task, _output: &()) -> Result<()> {
            let mut state = self.state.lock().unwrap();
            assert!(state.events.contains(&format!("ack:{task}")));
            state.events.push(format!("cleanup:{task}"));
            state.active -= 1;
            self.changed.notify_all();
            Ok(())
        }

        fn release(&self, task: &mut Self::Task, outcome: dispatch::ReleaseOutcome) -> Result<()> {
            let outcome = match outcome {
                dispatch::ReleaseOutcome::Failed => "failed",
                dispatch::ReleaseOutcome::Retry => "retry",
            };
            self.event(&format!("release:{task}:{outcome}"));
            self.state.lock().unwrap().active -= 1;
            Ok(())
        }
    }

    #[test]
    fn distributed_pipeline_overlaps_compute_upload_and_next_prefetch_with_two_regions() {
        let operations = ScheduleOperations::new(ScheduleCase::Overlap);
        execute_work(&operations, true, true).unwrap();
        let state = operations.state.lock().unwrap();
        assert_eq!(state.maximum, 2);
        assert_eq!(state.active, 0);
        let position = |event: &str| {
            state
                .events
                .iter()
                .position(|value| value == event)
                .unwrap()
        };
        assert!(position("compute:a") < position("prepare:b"));
        assert!(position("prepare:b") < position("computed:a"));
        assert!(position("compute:b") < position("ack:a"));
        assert!(position("cleanup:a") < position("prepare:c"));
        assert!(position("prepare:c") < position("computed:b"));
    }

    #[test]
    fn distributed_pipeline_waits_for_confirmed_cleanup_before_computing_on_low_disk() {
        let operations = ScheduleOperations::new(ScheduleCase::LowDisk);
        execute_work(&operations, true, true).unwrap();
        let state = operations.state.lock().unwrap();
        let position = |event: &str| {
            state
                .events
                .iter()
                .position(|value| value == event)
                .unwrap()
        };
        assert!(position("wait:b") < position("cleanup:a"));
        assert!(position("cleanup:a") < position("compute:b"));
        assert_eq!(state.maximum, 2);
    }

    #[test]
    fn next_claim_cannot_overtake_a_waiting_prefetch_for_the_compute_turn() {
        let operations = ScheduleOperations::new(ScheduleCase::ComputeOrder);
        execute_work(&operations, true, true).unwrap();
        let state = operations.state.lock().unwrap();
        let position = |event: &str| {
            state
                .events
                .iter()
                .position(|value| value == event)
                .unwrap()
        };
        assert!(position("claim:c") < position("compute:b"));
        assert!(position("computed:b") < position("compute:c"));
        assert_eq!(state.maximum, 2);
    }

    #[test]
    fn distributed_pipeline_failure_retains_builds_and_stops_claiming_or_publishing_more_regions() {
        let operations = ScheduleOperations::new(ScheduleCase::UploadFailure);
        let error = execute_work(&operations, true, true).unwrap_err();
        assert!(error.to_string().contains("Injected upload failure"));
        let state = operations.state.lock().unwrap();
        assert_eq!(state.next, 2);
        for event in [
            "computed:a",
            "computed:b",
            "release:a:failed",
            "release:b:retry",
        ] {
            assert!(
                state.events.contains(&event.into()),
                "{event}: {:?}",
                state.events
            );
        }
        assert!(
            !state
                .events
                .iter()
                .any(|event| event.starts_with("cleanup:"))
        );
        assert!(!state.events.contains(&"upload:b".into()));
    }

    #[test]
    fn background_download_cannot_hold_the_queue_before_foreground_source_is_ready() {
        let flow = Arc::new(PipelineFlow::new(true));
        flow.phase(0, WorkPhase::Preparing);
        flow.phase(1, WorkPhase::Preparing);
        let foreground = flow.control(0);
        let background = flow.control(1);
        let thread_flow = Arc::clone(&flow);
        let (queued, queued_rx) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            background.download_turn().unwrap();
            let _download = thread_flow.download.lock().unwrap();
            queued.send(()).unwrap();
        });
        assert!(queued_rx.recv_timeout(Duration::from_millis(50)).is_err());
        foreground.consume();
        foreground.download_turn().unwrap();
        let download = flow
            .download
            .try_lock()
            .expect("Background task blocked the foreground download");
        flow.sources_ready.lock().unwrap()[0] = true;
        drop(download);
        queued_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        thread.join().unwrap();
        flow.phase(0, WorkPhase::Preparing);
        assert!(!flow.sources_ready.lock().unwrap()[0]);
    }

    #[test]
    fn foreground_download_keeps_the_disk_floor_and_cancel_wakes_background_waiters() {
        let work = work();
        let mut settings = runtime();
        settings.min_free_gib =
            fs2::available_space(work.path()).unwrap() / (1024 * 1024 * 1024) + 1;
        let flow = Arc::new(PipelineFlow::new(true));
        let control = flow.control(0);
        control.consume();
        let mut downloader = Downloader::new(&settings).unwrap();
        downloader.control = Some(control);
        let result = downloader
            .executor()
            .block_on(downloader.budget(&settings, work.path(), 1));
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Insufficient space")
        );
        let background = flow.control(1);
        let waiter = std::thread::spawn(move || background.download_turn());
        flow.stop();
        assert!(waiter.join().unwrap().is_err());
    }

    #[test]
    fn prefetch_failure_stops_other_slots_before_the_source_is_consumed() {
        let work = work();
        let flow = Arc::new(PipelineFlow::new(true));
        let failed = flow.control(0);
        let other = flow.control(1);
        failed.consume();
        let prefetch = Prefetch::controlled(
            &runtime(),
            &entry("france"),
            &work.path().join("missing-catalog.json"),
            work.path(),
            Arc::clone(&failed),
        );
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        while !flow.cancelled.load(Ordering::Acquire) && std::time::Instant::now() < deadline {
            flow.wait(Duration::from_millis(10));
        }
        assert!(flow.cancelled.load(Ordering::Acquire));
        assert!(failed.failed.load(Ordering::Acquire));
        assert!(other.cancelled.load(Ordering::Acquire));
        assert!(!other.failed.load(Ordering::Acquire));
        assert!(prefetch.consume().is_err());
        assert!(
            flow.failure
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .contains("france prefetch failed")
        );
    }

    #[test]
    fn invalid_commands_have_no_filesystem_or_network_effects() {
        let work = work();
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let mut runtime = runtime();
        runtime.data_dir = work.path().join("must-not-exist");
        for (command, selection, job, cleanup) in [
            ("update", "all", None, true),
            ("init", "all", None, false),
            ("bootstrap", "all", Some(Path::new("unused")), false),
            ("unknown", "all", None, false),
            ("update", "unknown-region", None, false),
        ] {
            assert!(
                run(
                    root,
                    &runtime,
                    &Environment::default(),
                    command,
                    selection,
                    job,
                    cleanup
                )
                .is_err()
            );
            assert!(!runtime.data_dir.exists());
        }
    }

    #[test]
    fn initialization_and_updates_check_the_scratch_volume_before_io() {
        let work = work();
        let scratch = work.path().join("missing-scratch");
        let mut settings = runtime();
        settings.start_free_gib = 0;
        settings.min_free_gib = 0;
        let environment = Environment::default();
        let mut operations = Operations {
            runtime: &settings,
            environment: &environment,
            scratch: &scratch,
            downloader: Downloader::new(&settings).unwrap(),
            catalog: work.path().join("unused-catalog"),
            publisher: None,
            lease: None,
            prefetch: None,
        };
        disk_check(work.path(), 0).unwrap();
        let region = entry("france");
        for error in [
            operations
                .initialize(&region, work.path(), Some(work.path()), None)
                .unwrap_err(),
            operations.update(&region, work.path()).unwrap_err(),
        ] {
            assert!(
                error.to_string().contains(scratch.to_str().unwrap()),
                "{error:#}"
            );
        }
        assert!(!work.path().join(&region.id).exists());
        assert!(!work.path().join(".downloads").exists());
        assert!(operations.prefetch.is_none());
    }

    #[test]
    fn download_stream_rejects_oversized_and_truncated_bodies() {
        use std::net::TcpListener;
        for (response, limit) in [
            (
                "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n10\r\n0123456789abcdef\r\n0\r\n\r\n",
                Some(8),
            ),
            (
                "HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\nshort",
                None,
            ),
        ] {
            let work = work();
            let server = TcpListener::bind("127.0.0.1:0").unwrap();
            let url = format!("http://{}/source", server.local_addr().unwrap());
            let server = std::thread::spawn(move || {
                use std::io::BufRead;
                let (mut stream, _) = server.accept().unwrap();
                let mut request = String::new();
                let mut reader = std::io::BufReader::new(&mut stream);
                while !request.ends_with("\r\n\r\n") {
                    assert!(reader.read_line(&mut request).unwrap() > 0);
                    assert!(request.len() <= 4096);
                }
                stream.write_all(response.as_bytes()).unwrap();
            });
            let downloader = Downloader::new(&runtime()).unwrap();
            let path = work.path().join("source.part");
            assert!(
                downloader
                    .executor()
                    .block_on(downloader.download_attempt(
                        &runtime(),
                        &url,
                        &path,
                        Duration::from_secs(2),
                        limit
                    ))
                    .is_err()
            );
            if let Some(limit) = limit {
                assert!(fs::metadata(path).unwrap().len() <= limit);
            }
            server.join().unwrap();
        }
    }

    fn work() -> tempfile::TempDir {
        let scratch = Path::new(env!("CARGO_MANIFEST_DIR")).join(".build/tests");
        fs::create_dir_all(&scratch).unwrap();
        tempfile::Builder::new()
            .prefix("pipeline-")
            .tempdir_in(scratch)
            .unwrap()
    }

    fn entry(id: &str) -> Region {
        Region {
            id: id.into(),
            extract: format!("europe/{id}"),
        }
    }
    fn manifest() -> String {
        "a".repeat(64)
    }

    fn release(data: &Path, entry: &Region, manifest: &str) -> Value {
        let output = data.join(&entry.id).join("output");
        fs::create_dir_all(&output).unwrap();
        let value =
            json!({"region":entry.id,"manifest":manifest,"output":output.to_str().unwrap()});
        fs::write(
            output.join("release.json"),
            serde_json::to_vec(&value).unwrap(),
        )
        .unwrap();
        value
    }

    fn download_job(data: &Path, entry: &Region, suffix: &str) -> PathBuf {
        let path = data
            .join(".downloads")
            .join(format!("{}-{suffix}", entry.id));
        fs::create_dir_all(&path).unwrap();
        fs::write(path.join("source.url"), entry.source_url()).unwrap();
        fs::write(path.join("source.osm.pbf"), b"fixture").unwrap();
        path
    }

    fn marker(data: &Path, entry: &Region, names: &[String]) -> PathBuf {
        let path = data.join(format!(".cleanup-{}.json", entry.id));
        fs::write(
            &path,
            serde_json::to_vec(&json!({"region":entry.id,"manifest":manifest(),"downloads":names}))
                .unwrap(),
        )
        .unwrap();
        path
    }

    #[test]
    fn cleanup_removes_only_confirmed_region_and_owned_downloads() {
        let work = work();
        let entry = entry("france");
        release(work.path(), &entry, &manifest());
        let own = download_job(work.path(), &entry, "ab12");
        let pending = download_job(work.path(), &super::tests::entry("germany"), "pending");
        let other = download_job(work.path(), &entry, "other");
        fs::write(
            other.join("source.url"),
            "https://download.geofabrik.de/europe/germany-latest.osm.pbf",
        )
        .unwrap();
        let builds = work.path().join(".builds");
        fs::create_dir(&builds).unwrap();
        cleanup_region(&entry, work.path(), &manifest()).unwrap();
        assert!(!work.path().join(&entry.id).exists());
        assert!(!own.exists());
        assert!(pending.exists());
        assert!(other.exists());
        assert!(builds.exists());
        assert!(!work.path().join(".cleanup-france.json").exists());
    }

    #[test]
    fn cleanup_rejects_unconfirmed_manifest_without_deleting_data() {
        let work = work();
        let entry = entry("france");
        release(work.path(), &entry, &manifest());
        let own = download_job(work.path(), &entry, "ab12");
        assert!(cleanup_region(&entry, work.path(), &"b".repeat(64)).is_err());
        assert!(work.path().join(&entry.id).exists());
        assert!(own.exists());
    }

    #[test]
    fn interrupted_cleanup_resumes_after_state_or_source_file_was_deleted() {
        let work = work();
        let entry = entry("france");
        let own = download_job(work.path(), &entry, "ab12");
        fs::remove_file(own.join("source.url")).unwrap();
        marker(
            work.path(),
            &entry,
            &["france-ab12".into(), "france-alreadygone".into()],
        );
        resume_cleanup(&entry, work.path(), &manifest()).unwrap();
        assert!(!own.exists());
    }

    #[test]
    fn resume_validates_every_target_before_deleting_any() {
        let work = work();
        let entry = entry("france");
        release(work.path(), &entry, &manifest());
        let own = download_job(work.path(), &entry, "ab12");
        let changed = download_job(work.path(), &entry, "changed");
        fs::write(changed.join("source.url"), "changed").unwrap();
        marker(
            work.path(),
            &entry,
            &["france-ab12".into(), "france-changed".into()],
        );
        assert!(resume_cleanup(&entry, work.path(), &manifest()).is_err());
        assert!(own.exists());
        assert!(work.path().join(&entry.id).exists());
    }

    #[test]
    fn resume_rejects_escape_remote_change_and_newer_local_release() {
        let work = work();
        let entry = entry("france");
        release(work.path(), &entry, &manifest());
        marker(work.path(), &entry, &["../outside".into()]);
        assert!(resume_cleanup(&entry, work.path(), &manifest()).is_err());
        marker(work.path(), &entry, &[]);
        assert!(resume_cleanup(&entry, work.path(), &"b".repeat(64)).is_err());
        release(work.path(), &entry, &"b".repeat(64));
        assert!(resume_cleanup(&entry, work.path(), &manifest()).is_err());
        assert!(work.path().join(&entry.id).exists());
    }

    #[cfg(unix)]
    #[test]
    fn cleanup_rejects_links_to_state_downloads_and_source_files() {
        use std::os::unix::fs::symlink;
        for linked in [
            "state",
            "downloads",
            "job",
            "source",
            "marker",
            "output",
            "release",
        ] {
            let work = work();
            let entry = entry("france");
            release(work.path(), &entry, &manifest());
            let own = download_job(work.path(), &entry, "ab12");
            let target = match linked {
                "state" => work.path().join("france"),
                "downloads" => work.path().join(".downloads"),
                "job" => own,
                "source" => own.join("source.url"),
                "marker" => work.path().join(".cleanup-france.json"),
                "output" => work.path().join("france/output"),
                _ => work.path().join("france/output/release.json"),
            };
            let external = work.path().join("preserved");
            if target.exists() {
                fs::rename(&target, &external).unwrap();
            } else {
                fs::write(&external, "preserve").unwrap();
            }
            symlink(&external, &target).unwrap();
            assert!(
                cleanup_region(&entry, work.path(), &manifest()).is_err(),
                "{linked}"
            );
            assert!(external.exists(), "{linked}");
        }
    }

    #[derive(Default)]
    struct Fake {
        events: Vec<String>,
        remote: BTreeMap<String, String>,
        fail_update: Option<String>,
        fail_publish: Option<String>,
        confirm: bool,
        cleaned_before: Option<String>,
        prefetched: Option<String>,
        prefetches: Vec<String>,
        fail_prefetch: Option<String>,
        cancelled: bool,
    }

    impl RegionalOperations for Fake {
        fn initialize(
            &mut self,
            entry: &Region,
            data: &Path,
            _job: Option<&Path>,
            next: Option<&Region>,
        ) -> Result<Value> {
            if let Some(previous) = &self.cleaned_before {
                ensure!(
                    !data.join(previous).exists(),
                    "Prior region must be cleaned before next initialization"
                );
            }
            self.events.push(format!("init:{}", entry.id));
            if self.prefetched.as_deref() == Some(&entry.id) {
                self.prefetched = None;
                ensure!(
                    self.fail_prefetch.as_deref() != Some(&entry.id),
                    "Injected prefetch failure"
                );
            }
            if self.prefetched.is_none()
                && let Some(next) = next
            {
                self.prefetched = Some(next.id.clone());
                self.prefetches.push(next.id.clone());
            }
            Ok(release(data, entry, &manifest()))
        }
        fn update(&mut self, entry: &Region, data: &Path) -> Result<Value> {
            self.events.push(format!("update:{}", entry.id));
            ensure!(
                self.fail_update.as_deref() != Some(&entry.id),
                "Injected update failure"
            );
            Ok(release(data, entry, &manifest()))
        }
        fn status(&mut self, entry: &Region, data: &Path) -> Result<Value> {
            self.events.push(format!("status:{}", entry.id));
            Ok(serde_json::from_slice(&fs::read(
                data.join(&entry.id).join("output/release.json"),
            )?)?)
        }
        fn publish(&mut self, output: &Path) -> Result<()> {
            let value: Value = serde_json::from_slice(&fs::read(output.join("release.json"))?)?;
            let id = string(&value, "region")?;
            self.events.push(format!("publish:{id}"));
            ensure!(
                self.fail_publish.as_deref() != Some(id),
                "Injected publication failure"
            );
            if self.confirm {
                self.remote
                    .insert(id.into(), string(&value, "manifest")?.into());
            }
            Ok(())
        }
        fn published_regions(&mut self) -> Result<BTreeMap<String, String>> {
            Ok(self.remote.clone())
        }
        fn cancel_prefetch(&mut self) {
            self.cancelled = true;
            self.prefetched = None;
        }
        fn start_prefetch(&mut self, next: Option<&Region>, _data: &Path) {
            if self.prefetched.is_none()
                && let Some(next) = next
            {
                self.prefetched = Some(next.id.clone());
                self.prefetches.push(next.id.clone());
            }
        }
    }

    #[test]
    fn each_region_updates_and_publishes_before_next_region() {
        let work = work();
        let mut operations = Fake::default();
        execute_regions(
            &mut operations,
            &[entry("france"), entry("germany")],
            "bootstrap",
            work.path(),
            None,
            false,
        )
        .unwrap();
        assert_eq!(
            operations.events,
            [
                "init:france",
                "update:france",
                "publish:france",
                "init:germany",
                "update:germany",
                "publish:germany"
            ]
        );
        assert_eq!(operations.prefetches, ["germany"]);
    }

    #[test]
    fn normal_batch_continues_after_failure_and_never_publishes_partial_update() {
        let work = work();
        let mut operations = Fake {
            fail_update: Some("france".into()),
            ..Fake::default()
        };
        assert!(
            execute_regions(
                &mut operations,
                &[entry("france"), entry("germany")],
                "bootstrap",
                work.path(),
                None,
                false
            )
            .is_err()
        );
        assert_eq!(
            operations.events,
            [
                "init:france",
                "update:france",
                "init:germany",
                "update:germany",
                "publish:germany"
            ]
        );
    }

    #[test]
    fn cleanup_stops_on_publication_failure_and_retains_data() {
        let work = work();
        let mut operations = Fake {
            fail_publish: Some("france".into()),
            ..Fake::default()
        };
        assert!(
            execute_regions(
                &mut operations,
                &[entry("france"), entry("germany")],
                "bootstrap",
                work.path(),
                None,
                true
            )
            .is_err()
        );
        assert_eq!(
            operations.events,
            ["init:france", "update:france", "publish:france"]
        );
        assert!(work.path().join("france").exists());
        assert!(!work.path().join("germany").exists());
    }

    #[test]
    fn cleanup_waits_for_remote_confirmation() {
        let work = work();
        let mut operations = Fake::default();
        assert!(
            execute_regions(
                &mut operations,
                &[entry("france")],
                "bootstrap",
                work.path(),
                None,
                true
            )
            .is_err()
        );
        assert!(work.path().join("france").exists());
        operations.confirm = true;
        execute_regions(
            &mut operations,
            &[entry("france")],
            "bootstrap",
            work.path(),
            None,
            true,
        )
        .unwrap();
        assert!(!work.path().join("france").exists());
    }

    #[test]
    fn cleanup_completes_before_starting_next_region_and_repeat_skips_build() {
        let work = work();
        let mut operations = Fake {
            confirm: true,
            cleaned_before: Some("france".into()),
            ..Fake::default()
        };
        let entries = [entry("france"), entry("germany")];
        execute_regions(
            &mut operations,
            &entries,
            "bootstrap",
            work.path(),
            None,
            true,
        )
        .unwrap();
        assert!(!work.path().join("france").exists());
        assert!(!work.path().join("germany").exists());
        operations.events.clear();
        execute_regions(
            &mut operations,
            &entries,
            "bootstrap",
            work.path(),
            None,
            true,
        )
        .unwrap();
        assert!(operations.events.is_empty());
    }

    #[test]
    fn normal_bootstrap_cannot_bypass_interrupted_cleanup() {
        let work = work();
        let entry = entry("france");
        marker(work.path(), &entry, &[]);
        let mut operations = Fake::default();
        assert!(
            execute_regions(
                &mut operations,
                &[entry],
                "bootstrap",
                work.path(),
                None,
                false
            )
            .is_err()
        );
        assert!(operations.events.is_empty());
    }

    #[test]
    fn initial_command_builds_without_publication() {
        let work = work();
        let mut operations = Fake::default();
        execute_regions(
            &mut operations,
            &[entry("france")],
            "init",
            work.path(),
            None,
            false,
        )
        .unwrap();
        assert_eq!(operations.events, ["init:france"]);
        assert!(operations.prefetches.is_empty());
    }

    #[test]
    fn prefetch_skips_existing_and_published_regions_and_failure_belongs_to_target() {
        let work = work();
        let entries = [
            entry("france"),
            entry("existing"),
            entry("published"),
            entry("germany"),
            entry("italy"),
        ];
        release(work.path(), &entries[1], &manifest());
        let mut operations = Fake {
            confirm: true,
            remote: BTreeMap::from([("published".into(), manifest())]),
            fail_prefetch: Some("germany".into()),
            ..Fake::default()
        };
        let error = execute_regions(
            &mut operations,
            &entries,
            "bootstrap",
            work.path(),
            None,
            true,
        )
        .unwrap_err();
        assert_eq!(error.to_string(), "Failed regions: germany");
        assert_eq!(operations.prefetches, ["germany"]);
        assert!(operations.events.contains(&"publish:france".into()));
        assert!(operations.events.contains(&"publish:existing".into()));
        assert!(!operations.events.contains(&"init:published".into()));
        assert!(!operations.events.contains(&"publish:germany".into()));
        assert!(!operations.events.contains(&"init:italy".into()));
        assert!(operations.cancelled);
        assert!(!work.path().join("france").exists());
    }

    #[test]
    fn normal_batch_keeps_next_download_when_current_region_fails() {
        let work = work();
        let mut operations = Fake {
            fail_update: Some("france".into()),
            ..Fake::default()
        };
        execute_regions(
            &mut operations,
            &[entry("france"), entry("germany"), entry("italy")],
            "bootstrap",
            work.path(),
            None,
            false,
        )
        .unwrap_err();
        assert_eq!(operations.prefetches, ["germany", "italy"]);
        assert!(operations.events.contains(&"publish:germany".into()));
        assert!(operations.events.contains(&"publish:italy".into()));
    }

    #[cfg(unix)]
    #[test]
    fn cleanup_failure_cancels_pending_prefetch() {
        let work = work();
        let france = entry("france");
        let job = download_job(work.path(), &france, "unsafe");
        let retained = job.join("retained.url");
        fs::rename(job.join("source.url"), &retained).unwrap();
        std::os::unix::fs::symlink(&retained, job.join("source.url")).unwrap();
        let mut operations = Fake {
            confirm: true,
            ..Fake::default()
        };
        let error = execute_regions(
            &mut operations,
            &[france, entry("germany")],
            "bootstrap",
            work.path(),
            None,
            true,
        )
        .unwrap_err();
        assert_eq!(error.to_string(), "Failed regions: france");
        assert_eq!(operations.prefetches, ["germany"]);
        assert!(!operations.events.contains(&"init:germany".into()));
        assert!(operations.cancelled);
        assert!(job.exists());
    }

    #[test]
    fn prefetch_drop_cancels_joins_and_removes_only_its_unconsumed_directory() {
        let work = work();
        let owned = tempfile::Builder::new()
            .prefix("pending-")
            .tempdir_in(work.path())
            .unwrap();
        let path = owned.path().to_path_buf();
        fs::write(path.join("source.osm.pbf.part"), b"partial").unwrap();
        let preserved = work.path().join("preserved");
        fs::write(&preserved, "user data").unwrap();
        let control = Arc::new(DownloadControl::default());
        let thread_control = Arc::clone(&control);
        let (finished, receiver) = std::sync::mpsc::channel();
        let task = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(thread_control.cancelled());
            finished.send(()).unwrap();
            Ok(owned)
        });
        drop(Prefetch {
            region: "germany".into(),
            control,
            task: Some(task),
            error: None,
        });
        receiver.try_recv().unwrap();
        assert!(!path.exists());
        assert!(preserved.exists());
    }

    #[test]
    fn completed_prefetch_stays_owned_until_consumption() {
        let work = work();
        let owned = tempfile::Builder::new()
            .prefix("ready-")
            .tempdir_in(work.path())
            .unwrap();
        let path = owned.path().to_path_buf();
        let prefetch = Prefetch {
            region: "germany".into(),
            control: Arc::new(DownloadControl::default()),
            task: Some(std::thread::spawn(move || Ok(owned))),
            error: None,
        };
        let settings = runtime();
        let environment = Environment::default();
        let mut operations = Operations {
            runtime: &settings,
            environment: &environment,
            scratch: work.path(),
            downloader: Downloader::new(&settings).unwrap(),
            catalog: work.path().join("unused-catalog"),
            publisher: None,
            lease: None,
            prefetch: Some(prefetch),
        };
        operations.start_prefetch(Some(&entry("italy")), work.path());
        assert_eq!(operations.prefetch.as_ref().unwrap().region, "germany");
        assert_eq!(operations.prefetch.take().unwrap().consume().unwrap(), path);
        assert!(path.exists());
    }

    #[cfg(unix)]
    #[test]
    fn existing_state_prefetches_past_an_interrupted_region() {
        let work = work();
        let entries = [entry("france"), entry("germany"), entry("italy")];
        release(work.path(), &entries[0], &manifest());
        std::os::unix::fs::symlink(
            work.path().join("absent"),
            work.path().join(".cleanup-germany.json"),
        )
        .unwrap();
        let mut operations = Fake::default();
        let error = execute_regions(
            &mut operations,
            &entries,
            "bootstrap",
            work.path(),
            None,
            false,
        )
        .unwrap_err();
        assert_eq!(error.to_string(), "Failed regions: germany");
        assert_eq!(operations.prefetches, ["italy"]);
        assert!(!operations.events.contains(&"init:france".into()));
        assert!(operations.events.contains(&"publish:france".into()));
        assert!(operations.events.contains(&"publish:italy".into()));
    }

    #[test]
    fn rejected_initialization_releases_its_prefetch_slot_and_owned_files() {
        let work = work();
        let owned = tempfile::Builder::new()
            .prefix("pending-")
            .tempdir_in(work.path())
            .unwrap();
        let path = owned.path().to_path_buf();
        let mut settings = runtime();
        settings.start_free_gib =
            fs2::available_space(work.path()).unwrap() / (1024 * 1024 * 1024) + 1;
        settings.min_free_gib = 0;
        let environment = Environment::default();
        let mut operations = Operations {
            runtime: &settings,
            environment: &environment,
            scratch: work.path(),
            downloader: Downloader::new(&settings).unwrap(),
            catalog: work.path().join("unused-catalog"),
            publisher: None,
            lease: None,
            prefetch: Some(Prefetch {
                region: "germany".into(),
                control: Arc::new(DownloadControl::default()),
                task: Some(std::thread::spawn(move || Ok(owned))),
                error: None,
            }),
        };
        let error = operations
            .initialize(&entry("germany"), work.path(), None, None)
            .unwrap_err();
        assert!(error.to_string().contains("GiB free"));
        assert!(operations.prefetch.is_none());
        assert!(!path.exists());
    }

    #[test]
    fn cancelled_download_interrupts_stalled_headers_and_body() {
        use std::net::TcpListener;
        for headers in [false, true] {
            let work = work();
            let server = TcpListener::bind("127.0.0.1:0").unwrap();
            let url = format!("http://{}/source", server.local_addr().unwrap());
            let (ready, ready_rx) = std::sync::mpsc::channel();
            let (release, release_rx) = std::sync::mpsc::channel();
            let server = std::thread::spawn(move || {
                use std::io::Read;
                let (mut stream, _) = server.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                let mut request = [0_u8; 4096];
                assert!(stream.read(&mut request).unwrap() > 0);
                if headers {
                    stream
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\nx",
                        )
                        .unwrap();
                }
                ready.send(()).unwrap();
                release_rx.recv_timeout(Duration::from_secs(3)).unwrap();
            });
            let control = Arc::new(DownloadControl::default());
            let thread_control = Arc::clone(&control);
            let path = work.path().join("source.part");
            let observed_path = path.clone();
            let (finished, finished_rx) = std::sync::mpsc::channel();
            let task = std::thread::spawn(move || {
                let mut runtime = runtime();
                runtime.start_free_gib = 0;
                runtime.min_free_gib = 0;
                let mut downloader = Downloader::new(&runtime).unwrap();
                downloader.control = Some(thread_control);
                let result = downloader.executor().block_on(downloader.download_attempt(
                    &runtime,
                    &url,
                    &path,
                    Duration::from_secs(60),
                    None,
                ));
                drop(downloader);
                finished.send(result).unwrap();
            });
            ready_rx.recv_timeout(Duration::from_secs(3)).unwrap();
            if headers {
                let until = std::time::Instant::now() + Duration::from_secs(2);
                while fs::metadata(&observed_path).map_or(0, |metadata| metadata.len()) != 1
                    && std::time::Instant::now() < until
                {
                    std::thread::sleep(Duration::from_millis(5));
                }
                assert_eq!(fs::metadata(&observed_path).unwrap().len(), 1);
            }
            control.cancel();
            let result = finished_rx.recv_timeout(Duration::from_secs(1));
            release.send(()).unwrap();
            server.join().unwrap();
            task.join().unwrap();
            assert!(
                result
                    .unwrap()
                    .unwrap_err()
                    .to_string()
                    .contains("cancelled")
            );
        }
    }

    #[test]
    fn disk_pause_resumes_the_same_response_without_consuming_network_timeout() {
        use std::net::TcpListener;
        let work = work();
        let server = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/source", server.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            use std::io::Read;
            let (mut stream, _) = server.accept().unwrap();
            let mut request = [0_u8; 4096];
            assert!(stream.read(&mut request).unwrap() > 0);
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 7\r\nConnection: close\r\n\r\ncontent",
                )
                .unwrap();
        });
        let control = Arc::new(DownloadControl::default());
        let thread_control = Arc::clone(&control);
        let path = work.path().join("source.part");
        let thread_path = path.clone();
        let mut settings = runtime();
        settings.start_free_gib =
            fs2::available_space(work.path()).unwrap() / (1024 * 1024 * 1024) + 1;
        let (finished, finished_rx) = std::sync::mpsc::channel();
        let task = std::thread::spawn(move || {
            let mut downloader = Downloader::new(&settings).unwrap();
            downloader.control = Some(thread_control);
            let result = downloader.executor().block_on(downloader.download_attempt(
                &settings,
                &url,
                &thread_path,
                Duration::from_millis(500),
                None,
            ));
            finished.send(result).unwrap();
        });
        let until = std::time::Instant::now() + Duration::from_secs(3);
        while !path.exists() && std::time::Instant::now() < until {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(path.exists());
        assert!(
            finished_rx
                .recv_timeout(Duration::from_millis(600))
                .is_err()
        );
        assert_eq!(fs::metadata(&path).unwrap().len(), 0);
        control.consume();
        finished_rx
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"content");
        task.join().unwrap();
        server.join().unwrap();
    }

    #[test]
    fn downloader_drop_does_not_wait_for_a_system_resolver_task() {
        let downloader = Downloader::new(&runtime()).unwrap();
        let (started, started_rx) = std::sync::mpsc::channel();
        let (release, release_rx) = std::sync::mpsc::channel();
        let (finished, finished_rx) = std::sync::mpsc::channel();
        downloader.executor().spawn_blocking(move || {
            started.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(3)).unwrap();
            finished.send(()).unwrap();
        });
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let before = std::time::Instant::now();
        drop(downloader);
        let elapsed = before.elapsed();
        release.send(()).unwrap();
        finished_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(elapsed < Duration::from_millis(500));
    }
}

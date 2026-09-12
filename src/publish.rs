use crate::config::{Environment, Runtime};
use crate::format::{self, Current, MAX_BLOCK, MAX_CURRENT, MAX_MANIFEST, Release};
use crate::progress::ProgressLine;
use crate::storage::{atomic_write, read_local};
use anyhow::{Result, anyhow, bail, ensure};
use aws_credential_types::Credentials;
use aws_sigv4::http_request::{
    PayloadChecksumKind, PercentEncodingMode, SignableBody, SignableRequest, SigningSettings,
    UriPathNormalizationMode, sign,
};
use aws_sigv4::sign::v4;
use reqwest::blocking::{Client, Response};
use reqwest::header::{CONTENT_LENGTH, HeaderMap, HeaderValue};
use reqwest::{Method, StatusCode, Url};
use serde_json::{Value, json};
use std::collections::HashSet;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

pub struct PublishConfig {
    account: String,
    bucket: String,
    access_key: String,
    secret_key: String,
    worker: Url,
    token: String,
}

impl PublishConfig {
    pub fn from_environment(environment: &Environment) -> Result<Self> {
        let required = |name: &str| -> Result<String> {
            environment
                .get(name)
                .filter(|value| !value.trim().is_empty())
                .map(str::to_owned)
                .ok_or_else(|| anyhow!("Missing {name}"))
        };
        let account = required("R2_ACCOUNT_ID")?;
        ensure!(
            account.len() == 32 && account.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "Invalid R2_ACCOUNT_ID"
        );
        let bucket = required("R2_BUCKET")?;
        ensure!(
            (3..=63).contains(&bucket.len())
                && bucket.bytes().all(|byte| byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || byte == b'-'
                    || byte == b'.')
                && bucket.as_bytes()[0].is_ascii_alphanumeric()
                && bucket.as_bytes()[bucket.len() - 1].is_ascii_alphanumeric(),
            "Invalid R2_BUCKET"
        );
        let access_key = required("AWS_ACCESS_KEY_ID")?;
        let secret_key = required("AWS_SECRET_ACCESS_KEY")?;
        let worker = origin(&required("OSM_WORKER_URL")?, "OSM_WORKER_URL", false)?;
        let token = required("OSM_PUBLISH_TOKEN")?;
        ensure!(
            token.chars().count() >= 32
                && !token.contains(['\r', '\n'])
                && HeaderValue::from_str(&format!("Bearer {token}")).is_ok(),
            "Invalid OSM_PUBLISH_TOKEN"
        );
        Ok(Self {
            account,
            bucket,
            access_key,
            secret_key,
            worker,
            token,
        })
    }
}

fn origin(value: &str, name: &str, local_only: bool) -> Result<Url> {
    let url = Url::parse(value).map_err(|_| anyhow!("Invalid {name}"))?;
    let local = matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"));
    ensure!(
        (url.scheme() == "https" || (url.scheme() == "http" && local)) && (!local_only || local),
        "{name} requires {}",
        if local_only {
            "a loopback origin"
        } else {
            "HTTPS"
        }
    );
    ensure!(
        url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none()
            && url.path() == "/",
        "{name} must be an origin without credentials"
    );
    Ok(url)
}

#[derive(Clone, Debug, Default)]
pub struct Progress {
    pub stage: &'static str,
    pub completed: Option<usize>,
    pub total: Option<usize>,
    pub uploaded: usize,
    pub reused: usize,
    pub bytes: usize,
    pub reused_bytes: usize,
    pub total_bytes: usize,
    pub attempt: Option<u32>,
    pub max_attempts: Option<u32>,
}

impl Progress {
    fn stage(stage: &'static str) -> Self {
        Self {
            stage,
            ..Self::default()
        }
    }
}

struct Object {
    key: String,
    hash: String,
    size: usize,
}

struct Build {
    root: PathBuf,
    release: Release,
    objects: Vec<Object>,
}

fn validate_build(directory: &Path, progress: &mut impl FnMut(Progress)) -> Result<Build> {
    progress(Progress::stage("validate"));
    let root = directory.canonicalize()?;
    let release =
        format::validate_release(&read_local(&root, "release.json", MAX_CURRENT, None)?.value)?;
    let manifest_key = format!("manifests/{}.json", release.manifest);
    let local = read_local(&root, &manifest_key, MAX_MANIFEST, Some(&release.manifest))?;
    let manifest = format::validate_manifest(&local.value)?;
    ensure!(
        manifest.region == release.region
            && manifest.source_timestamp == release.source_timestamp
            && manifest.count == release.count,
        "Release does not match manifest"
    );
    let total = manifest.cells.values().map(Vec::len).sum::<usize>() + 1;
    progress(Progress {
        completed: Some(0),
        total: Some(total),
        ..Progress::stage("validate")
    });
    let mut objects = Vec::with_capacity(total);
    let mut hashes = HashSet::new();
    let mut ids = HashSet::new();
    for (cell, pages) in &manifest.cells {
        let mut previous_id = String::new();
        for hash in pages {
            ensure!(hashes.insert(hash), "Repeated block reference: {cell}");
            let key = format!("blocks/{hash}.json");
            let block = read_local(&root, &key, MAX_BLOCK, Some(hash))?;
            let pois = format::validate_block(&block.value)?;
            ensure!(!pois.is_empty(), "Empty block: {hash}");
            for poi in pois {
                let id = poi
                    .id
                    .rsplit('_')
                    .next()
                    .and_then(|id| id.parse::<u64>().ok());
                ensure!(
                    id.is_some_and(|id| id <= format::MAX_ID) && poi.lon < 180.0,
                    "Invalid POI: {hash}"
                );
                ensure!(format::has_name(&poi.tags), "Unnamed POI: {}", poi.id);
                let expected = format!(
                    "{}_{}",
                    ((poi.lat + 90.0) * 100.0).floor().min(17999.0) as u32,
                    ((poi.lon + 180.0) * 100.0).floor() as u32
                );
                ensure!(&expected == cell, "POI in wrong cell: {}", poi.id);
                ensure!(
                    poi.id > previous_id && ids.insert(poi.id.clone()),
                    "Duplicate or unsorted POI: {}",
                    poi.id
                );
                previous_id = poi.id;
            }
            objects.push(Object {
                key,
                hash: hash.clone(),
                size: block.size,
            });
            progress(Progress {
                completed: Some(objects.len()),
                total: Some(total),
                ..Progress::stage("validate")
            });
        }
    }
    ensure!(
        ids.len() as u64 == manifest.count,
        "Manifest count does not match blocks"
    );
    objects.push(Object {
        key: manifest_key,
        hash: local.hash,
        size: local.size,
    });
    progress(Progress {
        completed: Some(total),
        total: Some(total),
        ..Progress::stage("validate")
    });
    Ok(Build {
        root,
        release,
        objects,
    })
}

pub struct Publisher {
    client: Client,
    credentials: Credentials,
    worker: Url,
    endpoint: Url,
    bucket: String,
    authorization: HeaderValue,
    concurrency: usize,
    attempts: u32,
    retry_delay: Duration,
}

impl Publisher {
    pub fn new(runtime: &Runtime, config: &PublishConfig) -> Result<Self> {
        ensure!(
            (1..=64).contains(&runtime.upload_concurrency),
            "Invalid OSM_UPLOAD_CONCURRENCY"
        );
        ensure!(
            (1..=10).contains(&runtime.http_attempts),
            "Invalid OSM_HTTP_ATTEMPTS"
        );
        ensure!(
            !runtime.http_timeout.is_zero() && !runtime.http_connect_timeout.is_zero(),
            "Invalid HTTP timeout"
        );
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .timeout(runtime.http_timeout)
            .connect_timeout(runtime.http_connect_timeout)
            .pool_max_idle_per_host(runtime.upload_concurrency)
            .build()
            .map_err(|_| anyhow!("HTTP client initialization failed"))?;
        let mut authorization = HeaderValue::from_str(&format!("Bearer {}", config.token))
            .map_err(|_| anyhow!("Invalid OSM_PUBLISH_TOKEN"))?;
        authorization.set_sensitive(true);
        Ok(Self {
            client,
            credentials: Credentials::new(
                config.access_key.clone(),
                config.secret_key.clone(),
                None,
                None,
                "OSM",
            ),
            worker: config.worker.clone(),
            endpoint: Url::parse(&format!(
                "https://{}.r2.cloudflarestorage.com",
                config.account
            ))
            .map_err(|_| anyhow!("Invalid R2_ACCOUNT_ID"))?,
            bucket: config.bucket.clone(),
            authorization,
            concurrency: runtime.upload_concurrency,
            attempts: runtime.http_attempts,
            retry_delay: runtime.http_retry_delay,
        })
    }

    pub fn with_local_r2_endpoint(mut self, endpoint: &str) -> Result<Self> {
        self.endpoint = origin(endpoint, "R2 test endpoint", true)?;
        Ok(self)
    }

    fn backoff(&self, attempt: u32) {
        thread::sleep(self.retry_delay.saturating_mul(1u32 << attempt));
    }

    pub fn read_state(&self) -> Result<Option<Current>> {
        for attempt in 0..self.attempts {
            let response = self
                .client
                .get(self.worker.join("admin/state").expect("validated origin"))
                .header("Authorization", self.authorization.clone())
                .send();
            match response {
                Ok(response) if response.status().is_success() => {
                    ensure!(
                        response
                            .content_length()
                            .is_none_or(|size| size <= MAX_CURRENT as u64),
                        "State response is too large"
                    );
                    let mut bytes = Vec::new();
                    match response
                        .take(MAX_CURRENT as u64 + 1)
                        .read_to_end(&mut bytes)
                    {
                        Ok(_) => {
                            ensure!(bytes.len() <= MAX_CURRENT, "State response is too large");
                            let value: Value = serde_json::from_slice(&bytes)
                                .map_err(|_| anyhow!("Invalid state response JSON"))?;
                            return if value.is_null() {
                                Ok(None)
                            } else {
                                format::validate_current(&value)
                                    .map(Some)
                                    .map_err(|_| anyhow!("Invalid state response schema"))
                            };
                        }
                        Err(_) if attempt + 1 == self.attempts => {
                            bail!("State response read failed")
                        }
                        Err(_) => {}
                    }
                }
                Ok(response) => {
                    let status = response.status().as_u16();
                    if status < 500 && status != 429 {
                        bail!("State request rejected: HTTP {status}");
                    }
                    if attempt + 1 == self.attempts {
                        bail!("State request failed: HTTP {status}");
                    }
                }
                Err(_) if attempt + 1 == self.attempts => {
                    bail!("State request failed after {} attempts", self.attempts)
                }
                Err(_) => {}
            }
            self.backoff(attempt);
        }
        bail!("State request has no attempts")
    }

    fn s3_request(&self, method: Method, object: &Object, body: Option<&[u8]>) -> Result<Response> {
        let url = self
            .endpoint
            .join(&format!("{}/{}", self.bucket, object.key))
            .expect("validated object key");
        for attempt in 0..self.attempts {
            let mut headers = HeaderMap::new();
            if let Some(bytes) = body {
                headers.insert(CONTENT_LENGTH, HeaderValue::from(bytes.len()));
                headers.insert("content-type", HeaderValue::from_static("application/json"));
                headers.insert(
                    "cache-control",
                    HeaderValue::from_static("public, max-age=31536000, immutable"),
                );
                headers.insert("if-none-match", HeaderValue::from_static("*"));
                headers.insert(
                    "x-amz-meta-sha256",
                    HeaderValue::from_str(&object.hash).expect("validated hash"),
                );
            }
            let identity = self.credentials.clone().into();
            let mut settings = SigningSettings::default();
            settings.percent_encoding_mode = PercentEncodingMode::Single;
            settings.uri_path_normalization_mode = UriPathNormalizationMode::Disabled;
            settings.payload_checksum_kind = PayloadChecksumKind::XAmzSha256;
            let params = v4::SigningParams::builder()
                .identity(&identity)
                .region("auto")
                .name("s3")
                .time(SystemTime::now())
                .settings(settings)
                .build()
                .map_err(|_| anyhow!("R2 signing failed"))?
                .into();
            let request = SignableRequest::new(
                method.as_str(),
                url.as_str(),
                headers.iter().map(|(name, value)| {
                    (name.as_str(), value.to_str().expect("validated header"))
                }),
                SignableBody::Bytes(body.unwrap_or_default()),
            )
            .map_err(|_| anyhow!("R2 signing failed"))?;
            let (instructions, _) = sign(request, &params)
                .map_err(|_| anyhow!("R2 signing failed"))?
                .into_parts();
            for (name, value) in instructions.headers() {
                let mut value =
                    HeaderValue::from_str(value).map_err(|_| anyhow!("R2 signing failed"))?;
                if name.eq_ignore_ascii_case("authorization") {
                    value.set_sensitive(true);
                }
                headers.insert(
                    reqwest::header::HeaderName::from_bytes(name.as_bytes())
                        .map_err(|_| anyhow!("R2 signing failed"))?,
                    value,
                );
            }
            let mut request = self
                .client
                .request(method.clone(), url.clone())
                .headers(headers);
            if let Some(bytes) = body {
                request = request.body(bytes.to_vec());
            }
            match request.send() {
                Ok(response) => {
                    let status = response.status();
                    if !(status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS)
                        || attempt + 1 == self.attempts
                    {
                        return Ok(response);
                    }
                }
                Err(_) if attempt + 1 == self.attempts => bail!("R2 request failed"),
                Err(_) => {}
            }
            self.backoff(attempt);
        }
        bail!("R2 request has no attempts")
    }

    fn head(&self, object: &Object) -> Result<bool> {
        let response = self
            .s3_request(Method::HEAD, object, None)
            .map_err(|_| anyhow!("R2 HEAD failed: {}", object.key))?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(false);
        }
        ensure!(
            response.status().is_success(),
            "R2 HEAD failed: {}",
            object.key
        );
        let size = response
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<usize>().ok());
        let hash = response
            .headers()
            .get("x-amz-meta-sha256")
            .and_then(|value| value.to_str().ok());
        ensure!(
            size == Some(object.size) && hash == Some(object.hash.as_str()),
            "Immutable object mismatch: {}",
            object.key
        );
        Ok(true)
    }

    fn upload(&self, root: &Path, object: &Object) -> Result<bool> {
        if self.head(object)? {
            return Ok(false);
        }
        let local = read_local(root, &object.key, object.size, Some(&object.hash))?;
        let response = self
            .s3_request(Method::PUT, object, Some(&local.bytes))
            .map_err(|_| anyhow!("R2 upload failed: {}", object.key))?;
        if matches!(
            response.status(),
            StatusCode::CONFLICT | StatusCode::PRECONDITION_FAILED
        ) {
            ensure!(
                self.head(object)?,
                "Concurrent upload did not create object: {}",
                object.key
            );
            return Ok(false);
        }
        ensure!(
            response.status().is_success(),
            "R2 upload failed: {}",
            object.key
        );
        ensure!(
            self.head(object)?,
            "Uploaded object is missing: {}",
            object.key
        );
        Ok(true)
    }

    pub fn publish(&self, directory: &Path, mut progress: impl FnMut(Progress)) -> Result<Value> {
        let build = validate_build(directory, &mut progress)?;
        progress(Progress::stage("state"));
        let initial = self.read_state()?;
        let base_revision = initial.as_ref().map(|state| state.revision.clone());
        let mut stats = Progress {
            completed: Some(0),
            total: Some(build.objects.len()),
            total_bytes: build.objects.iter().map(|object| object.size).sum(),
            ..Progress::stage("upload")
        };
        progress(stats.clone());
        let blocks = &build.objects[..build.objects.len() - 1];
        let queue = Mutex::new((0usize, false));
        thread::scope(|scope| -> Result<()> {
            let (sender, receiver) = mpsc::sync_channel(self.concurrency);
            let mut handles = Vec::new();
            for _ in 0..self.concurrency.min(blocks.len()) {
                let sender = sender.clone();
                let queue = &queue;
                let root = &build.root;
                handles.push(scope.spawn(move || {
                    loop {
                        let object = {
                            let mut queue = queue.lock().expect("upload queue lock");
                            if queue.1 || queue.0 == blocks.len() {
                                break;
                            }
                            let object = &blocks[queue.0];
                            queue.0 += 1;
                            object
                        };
                        let result = self.upload(root, object);
                        if result.is_err() {
                            queue.lock().expect("upload queue lock").1 = true;
                        }
                        if sender.send((object.size, result)).is_err() {
                            break;
                        }
                    }
                }));
            }
            drop(sender);
            let mut failure = None;
            for (size, result) in receiver {
                match result {
                    Ok(uploaded) => {
                        record_upload(&mut stats, size, uploaded);
                        progress(stats.clone());
                    }
                    Err(error) if failure.is_none() => failure = Some(error),
                    Err(_) => {}
                }
            }
            for handle in handles {
                if handle.join().is_err() && failure.is_none() {
                    failure = Some(anyhow!("Upload worker failed"));
                }
            }
            if let Some(error) = failure {
                return Err(error);
            }
            Ok(())
        })?;
        let manifest = build.objects.last().expect("validated manifest");
        let uploaded = self.upload(&build.root, manifest)?;
        record_upload(&mut stats, manifest.size, uploaded);
        progress(stats.clone());
        let mut published = None;
        for attempt in 0..self.attempts {
            let event = Progress {
                attempt: Some(attempt + 1),
                max_attempts: Some(self.attempts),
                ..Progress::stage("publish")
            };
            progress(event.clone());
            let response = self.client.post(self.worker.join("admin/publish").expect("validated origin"))
                .header("Authorization", self.authorization.clone())
                .json(&json!({ "region": build.release.region, "manifest": build.release.manifest, "baseRevision": base_revision })).send();
            let status = response.as_ref().ok().map(Response::status);
            drop(response);
            if status == Some(StatusCode::CONFLICT) {
                bail!("Publish conflict: remote revision changed; no rebase was attempted");
            }
            if let Some(status) = status.filter(|status| {
                !status.is_success()
                    && status.as_u16() < 500
                    && *status != StatusCode::TOO_MANY_REQUESTS
            }) {
                bail!("Publish rejected: HTTP {}", status.as_u16());
            }
            progress(Progress {
                stage: "confirm",
                ..event
            });
            let state = self.read_state()?;
            if state.as_ref().is_some_and(|state| {
                state.regions.iter().any(|region| {
                    region.region == build.release.region
                        && region.manifest == build.release.manifest
                })
            }) {
                published = state;
                break;
            }
            ensure!(
                !status.is_some_and(|status| status.is_success()),
                "Publish acknowledged but target manifest is not current"
            );
            ensure!(
                state.as_ref().map(|state| &state.revision) == base_revision.as_ref(),
                "Publish outcome is unconfirmed and remote revision changed; no rebase was attempted"
            );
            ensure!(
                attempt + 1 < self.attempts,
                "Publish outcome is unconfirmed after {} attempts; inspect /admin/state before retrying",
                self.attempts
            );
            self.backoff(attempt);
        }
        let state = published.ok_or_else(|| anyhow!("Publish outcome is unconfirmed"))?;
        let mut receipt = serde_json::to_value(&build.release)?;
        receipt["revision"] = json!(state.revision);
        receipt["publishedAt"] =
            json!(chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true));
        receipt["uploaded"] = json!(stats.uploaded);
        receipt["reused"] = json!(stats.reused);
        receipt["bytes"] = json!(stats.bytes);
        progress(Progress::stage("receipt"));
        let mut bytes = serde_json::to_vec_pretty(&receipt)?;
        bytes.push(b'\n');
        atomic_write(&build.root.join("publish-receipt.json"), &bytes)?;
        progress(Progress::stage("done"));
        Ok(receipt)
    }
}

fn record_upload(stats: &mut Progress, size: usize, uploaded: bool) {
    if uploaded {
        stats.uploaded += 1;
        stats.bytes += size;
    } else {
        stats.reused += 1;
        stats.reused_bytes += size;
    }
    stats.completed = Some(stats.uploaded + stats.reused);
}

pub struct ProgressReporter {
    shared: Arc<(Mutex<ReporterState>, Condvar)>,
    worker: Option<thread::JoinHandle<()>>,
}

struct ReporterState {
    started: Instant,
    last_write: Option<Instant>,
    latest: Option<Progress>,
    closed: bool,
    output: ProgressLine,
}

impl Default for ProgressReporter {
    fn default() -> Self {
        Self::with_line(ProgressLine::default())
    }
}

impl ProgressReporter {
    #[cfg(test)]
    fn with_output(
        output: Box<dyn std::io::Write + Send>,
        terminal: bool,
        columns: Option<usize>,
    ) -> Self {
        Self::with_line(ProgressLine::with_output(output, terminal, columns))
    }

    fn with_line(output: ProgressLine) -> Self {
        let shared = Arc::new((
            Mutex::new(ReporterState {
                started: Instant::now(),
                last_write: None,
                latest: None,
                closed: false,
                output,
            }),
            Condvar::new(),
        ));
        let state = shared.clone();
        let worker = thread::spawn(move || {
            let (lock, condition) = &*state;
            let mut state = lock.lock().expect("progress lock");
            while !state.closed {
                state = condition
                    .wait_timeout(state, Duration::from_secs(1))
                    .expect("progress wait")
                    .0;
                if !state.closed {
                    state.render(false);
                }
            }
        });
        Self {
            shared,
            worker: Some(worker),
        }
    }

    pub fn update(&mut self, progress: Progress) {
        let mut state = self.shared.0.lock().expect("progress lock");
        let complete = progress.total.is_some() && progress.total == progress.completed;
        let force = state
            .latest
            .as_ref()
            .is_none_or(|latest| latest.stage != progress.stage)
            || complete;
        state.latest = Some(progress);
        state.render(force);
    }
}

impl Drop for ProgressReporter {
    fn drop(&mut self) {
        {
            let mut state = self.shared.0.lock().expect("progress lock");
            state.closed = true;
        }
        self.shared.1.notify_one();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl ReporterState {
    fn render(&mut self, force: bool) {
        let Some(progress) = &self.latest else {
            return;
        };
        let interval = Duration::from_secs(if self.output.is_terminal() { 1 } else { 5 });
        if !force
            && self
                .last_write
                .is_some_and(|last| last.elapsed() < interval)
        {
            return;
        }
        self.last_write = Some(Instant::now());
        let label = match progress.stage {
            "validate" => "Validating local files",
            "state" => "Reading remote revision",
            "upload" => "Uploading objects",
            "publish" => "Publishing revision",
            "confirm" => "Confirming remote revision",
            "receipt" => "Saving receipt",
            "done" => "Publish complete",
            _ => progress.stage,
        };
        let mut line = label.to_owned();
        if let (Some(done), Some(total)) = (progress.completed, progress.total) {
            line.push_str(&format!(
                " | {done}/{total} ({}%)",
                (done * 100).checked_div(total).unwrap_or(100)
            ));
        }
        if progress.stage == "upload" {
            line.push_str(&format!(
                " | uploaded {} / {:.2} MiB | reused {} / {:.2} MiB | total {:.2} MiB",
                progress.uploaded,
                progress.bytes as f64 / 1048576.0,
                progress.reused,
                progress.reused_bytes as f64 / 1048576.0,
                progress.total_bytes as f64 / 1048576.0
            ));
        }
        if let (Some(attempt), Some(maximum)) = (progress.attempt, progress.max_attempts) {
            line.push_str(&format!(" | attempt {attempt}/{maximum}"));
        }
        line.push_str(&format!(" | {}s elapsed", self.started.elapsed().as_secs()));
        self.output.render(&line);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[derive(Clone)]
    struct Output(Arc<Mutex<Vec<u8>>>);

    impl Write for Output {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn progress_wraps_terminal_rows_and_throttles_log_updates() {
        for terminal in [false, true] {
            let output = Output(Arc::default());
            let mut reporter =
                ProgressReporter::with_output(Box::new(output.clone()), terminal, Some(40));
            let event = Progress {
                completed: Some(0),
                total: Some(2),
                total_bytes: 1_048_576,
                ..Progress::stage("upload")
            };
            reporter.update(event.clone());
            let first = output.0.lock().unwrap().clone();
            reporter.update(Progress {
                completed: Some(1),
                uploaded: 1,
                bytes: 1_048_576,
                ..event
            });
            assert_eq!(*output.0.lock().unwrap(), first);
            {
                let mut state = reporter.shared.0.lock().unwrap();
                state.last_write = Some(Instant::now() - Duration::from_secs(10));
                state.render(false);
            }
            let bytes = output.0.lock().unwrap().clone();
            let text = String::from_utf8(bytes).unwrap();
            assert!(text.contains("1/2 (50%)"));
            assert!(text.contains("uploaded 1 / 1.00 MiB"));
            assert_eq!(text.contains("\x1b["), terminal);
            if terminal {
                let first_line = String::from_utf8(first)
                    .unwrap()
                    .trim_start_matches("\r\x1b[0J")
                    .to_owned();
                assert!(text.contains(&format!(
                    "\x1b[{}A\r\x1b[0J",
                    first_line.len().div_ceil(40) - 1
                )));
            }
            reporter.update(Progress::stage("done"));
            drop(reporter);
            assert!(
                String::from_utf8(output.0.lock().unwrap().clone())
                    .unwrap()
                    .contains("Publish complete")
            );
        }
    }

    #[test]
    fn terminal_progress_refreshes_while_waiting_and_drop_joins_timer() {
        let output = Output(Arc::default());
        let mut reporter = ProgressReporter::with_output(Box::new(output.clone()), true, Some(80));
        reporter.update(Progress::stage("state"));
        let first_length = output.0.lock().unwrap().len();
        thread::sleep(Duration::from_millis(1150));
        assert!(output.0.lock().unwrap().len() > first_length);
        drop(reporter);
        let finished_length = output.0.lock().unwrap().len();
        thread::sleep(Duration::from_millis(50));
        assert_eq!(output.0.lock().unwrap().len(), finished_length);
    }
}

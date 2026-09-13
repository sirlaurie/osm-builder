use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow, bail, ensure};
use chrono::DateTime;
use reqwest::{Method, Url, header::HeaderValue};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    format,
    source::{Region, valid_extract},
    storage,
};

const MAX_RESPONSE: usize = 1024 * 1024;
const LEASE_TTL: Duration = Duration::from_secs(300);

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Lease {
    pub batch_id: String,
    pub region: String,
    pub extract: String,
    pub device_id: String,
    pub token: String,
    pub generation: u64,
    pub expires_at: String,
    pub renew_after_seconds: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Claim {
    pub batch_id: Option<String>,
    pub lease: Option<Lease>,
    pub pending: usize,
    pub running: usize,
    pub failed: usize,
    pub retry_after_seconds: u64,
}

fn valid_uuid(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                byte == b'-'
            } else {
                byte.is_ascii_hexdigit()
            }
        })
}

fn uuid() -> Result<String> {
    let mut bytes = [0u8; 16];
    File::open("/dev/urandom")
        .and_then(|mut file| file.read_exact(&mut bytes))
        .map_err(|_| anyhow!("Cannot generate dispatch identity"))?;
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
    Ok(format!(
        "{}-{}-{}-{}-{}",
        &hex[..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..]
    ))
}

impl Lease {
    fn validate(&self) -> Result<()> {
        let expires = DateTime::parse_from_rfc3339(&self.expires_at)
            .map_err(|_| anyhow!("Invalid dispatch lease"))?;
        ensure!(
            self.expires_at.ends_with('Z')
                && expires.offset().local_minus_utc() == 0
                && expires.timestamp_subsec_nanos() < 1_000_000_000,
            "Invalid dispatch lease"
        );
        ensure!(
            valid_uuid(&self.batch_id)
                && format::is_region(&self.region)
                && valid_extract(&self.extract)
                && valid_uuid(&self.device_id)
                && valid_uuid(&self.token)
                && (1..=format::MAX_ID).contains(&self.generation)
                && (1..=60).contains(&self.renew_after_seconds),
            "Invalid dispatch lease"
        );
        Ok(())
    }

    fn same_owner(&self, other: &Self) -> bool {
        self.batch_id == other.batch_id
            && self.region == other.region
            && self.extract == other.extract
            && self.device_id == other.device_id
            && self.token == other.token
            && self.generation == other.generation
    }
}

#[derive(Clone)]
pub struct Client {
    http: reqwest::blocking::Client,
    origin: Url,
    authorization: HeaderValue,
    attempts: u32,
    retry_delay: Duration,
}

impl Client {
    pub fn new(
        http: reqwest::blocking::Client,
        origin: Url,
        mut authorization: HeaderValue,
        attempts: u32,
        retry_delay: Duration,
    ) -> Self {
        authorization.set_sensitive(true);
        Self {
            http,
            origin,
            authorization,
            attempts,
            retry_delay,
        }
    }

    fn request(&self, method: Method, path: &str, body: Option<&Value>) -> Result<Value> {
        ensure!(
            (1..=10).contains(&self.attempts),
            "Invalid dispatch retry limit"
        );
        let url = self
            .origin
            .join(path)
            .map_err(|_| anyhow!("Invalid dispatch URL"))?;
        for attempt in 0..self.attempts {
            let mut request = self
                .http
                .request(method.clone(), url.clone())
                .timeout(Duration::from_secs(30))
                .header("Authorization", self.authorization.clone());
            if let Some(body) = body {
                request = request.json(body);
            }
            match request.send() {
                Ok(response) => {
                    let status = response.status();
                    if status.as_u16() >= 500 || status.as_u16() == 429 {
                        if attempt + 1 == self.attempts {
                            bail!("Dispatch request failed: HTTP {}", status.as_u16());
                        }
                    } else {
                        ensure!(
                            response
                                .content_length()
                                .is_none_or(|size| size <= MAX_RESPONSE as u64),
                            "Dispatch response is too large"
                        );
                        let mut bytes = Vec::new();
                        match response
                            .take(MAX_RESPONSE as u64 + 1)
                            .read_to_end(&mut bytes)
                        {
                            Ok(_) => {
                                ensure!(
                                    bytes.len() <= MAX_RESPONSE,
                                    "Dispatch response is too large"
                                );
                                let value: Value = serde_json::from_slice(&bytes)
                                    .map_err(|_| anyhow!("Invalid dispatch response JSON"))?;
                                if status.is_success() {
                                    return Ok(value);
                                }
                                let code = value.get("error").and_then(Value::as_str);
                                if let Some(
                                    code @ ("batch_active" | "lease_lost" | "invalid_start"
                                    | "invalid_claim" | "invalid_lease"),
                                ) = code
                                {
                                    bail!("Dispatch request rejected: {code}");
                                }
                                bail!("Dispatch request rejected: HTTP {}", status.as_u16());
                            }
                            Err(_) if attempt + 1 == self.attempts => {
                                bail!("Dispatch response read failed");
                            }
                            Err(_) => {}
                        }
                    }
                }
                Err(_) if attempt + 1 == self.attempts => {
                    bail!("Dispatch request failed after {} attempts", self.attempts);
                }
                Err(_) => {}
            }
            thread::sleep(self.retry_delay.saturating_mul(1 << attempt));
        }
        bail!("Dispatch request has no attempts")
    }

    pub fn start(&self, mode: &str, regions: &[Region]) -> Result<String> {
        let mut ids = HashSet::new();
        let mut extracts = HashSet::new();
        ensure!(
            matches!(mode, "bootstrap" | "update")
                && (1..=256).contains(&regions.len())
                && regions.iter().all(|region| {
                    format::is_region(&region.id)
                        && valid_extract(&region.extract)
                        && ids.insert(&region.id)
                        && extracts.insert(&region.extract)
                }),
            "Invalid dispatch batch"
        );
        let value = self.request(
            Method::POST,
            "/admin/jobs/start",
            Some(&json!({
                "requestId": uuid()?, "mode": mode, "regions": regions,
            })),
        )?;
        value["batchId"]
            .as_str()
            .filter(|value| valid_uuid(value))
            .map(str::to_owned)
            .ok_or_else(|| anyhow!("Invalid dispatch batch response"))
    }

    pub fn claim(&self, device_id: &str, local_regions: &[String]) -> Result<Claim> {
        ensure!(
            valid_uuid(device_id)
                && local_regions.len() <= 256
                && local_regions.iter().all(|region| format::is_region(region)),
            "Invalid dispatch claim"
        );
        let value = self.request(
            Method::POST,
            "/admin/jobs/claim",
            Some(&json!({
                "requestId": uuid()?, "deviceId": device_id, "localRegions": local_regions,
            })),
        )?;
        let claim: Claim = serde_json::from_value(value)
            .map_err(|_| anyhow!("Invalid dispatch claim response"))?;
        ensure!(
            claim
                .batch_id
                .as_ref()
                .is_none_or(|value| valid_uuid(value))
                && claim.pending <= 256
                && claim.running <= 256
                && claim.failed <= 256
                && claim.pending + claim.running + claim.failed <= 256
                && claim.retry_after_seconds <= 60,
            "Invalid dispatch claim response"
        );
        if let Some(lease) = &claim.lease {
            lease.validate()?;
            ensure!(
                claim.batch_id.as_ref() == Some(&lease.batch_id) && lease.device_id == device_id,
                "Dispatch claim returned a different owner"
            );
        }
        ensure!(
            claim.batch_id.is_some()
                || (claim.lease.is_none() && claim.pending + claim.running + claim.failed == 0),
            "Invalid dispatch claim response"
        );
        Ok(claim)
    }

    pub fn renew(&self, lease: &Lease) -> Result<Lease> {
        lease.validate()?;
        let value = self.request(
            Method::POST,
            "/admin/jobs/renew",
            Some(&json!({ "lease": lease })),
        )?;
        let renewed: Lease = serde_json::from_value(value)
            .map_err(|_| anyhow!("Invalid dispatch renewal response"))?;
        renewed.validate()?;
        ensure!(
            lease.same_owner(&renewed),
            "Dispatch renewal changed lease owner"
        );
        Ok(renewed)
    }

    pub fn release(&self, lease: &Lease) -> Result<()> {
        let value = self.request(
            Method::POST,
            "/admin/jobs/release",
            Some(&json!({ "lease": lease })),
        )?;
        ensure!(
            value["success"] == true,
            "Invalid dispatch release response"
        );
        Ok(())
    }

    pub fn jobs(&self) -> Result<Value> {
        self.request(Method::GET, "/admin/jobs", None)
    }
}

pub fn device_id(data: &Path) -> Result<String> {
    fs::create_dir_all(data)?;
    let path = data.join(".device-id");
    if !path.try_exists()? {
        let id = uuid()?;
        let mut temporary = tempfile::Builder::new()
            .prefix(".device-")
            .tempfile_in(data)?;
        writeln!(temporary, "{id}")?;
        storage::sync_file(temporary.as_file())?;
        match temporary.persist_noclobber(&path) {
            Ok(_) => storage::sync_directory(data)?,
            Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(_) => bail!("Cannot save dispatch device identity"),
        }
    }
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(&path)
        .map_err(|_| anyhow!("Cannot read dispatch device identity"))?;
    let metadata = file.metadata()?;
    ensure!(
        metadata.is_file() && (36..=37).contains(&metadata.len()),
        "Invalid dispatch device identity"
    );
    let mut bytes = Vec::new();
    file.take(38).read_to_end(&mut bytes)?;
    let id = std::str::from_utf8(&bytes)
        .ok()
        .map(str::trim)
        .filter(|id| valid_uuid(id))
        .ok_or_else(|| anyhow!("Invalid dispatch device identity"))?;
    Ok(id.to_owned())
}

struct State {
    lease: Lease,
    deadline: Instant,
    stopped: bool,
    failed: bool,
}

pub struct Guard {
    state: Arc<(Mutex<State>, Condvar)>,
    worker: Option<JoinHandle<()>>,
}

impl Guard {
    pub fn start(client: Client, lease: Lease) -> Result<Self> {
        lease.validate()?;
        let state = Arc::new((
            Mutex::new(State {
                lease,
                deadline: Instant::now() + LEASE_TTL,
                stopped: false,
                failed: false,
            }),
            Condvar::new(),
        ));
        let shared = state.clone();
        let worker = thread::Builder::new()
            .name("osm-lease".into())
            .spawn(move || {
                let (lock, wake) = &*shared;
                loop {
                    let mut state = lock.lock().unwrap_or_else(|error| error.into_inner());
                    if state.stopped {
                        break;
                    }
                    let delay = match state.deadline.checked_duration_since(Instant::now()) {
                        Some(remaining) => {
                            remaining.min(Duration::from_secs(state.lease.renew_after_seconds))
                        }
                        None => {
                            state.failed = true;
                            break;
                        }
                    };
                    let (mut state, _) = wake
                        .wait_timeout_while(state, delay, |state| !state.stopped)
                        .unwrap_or_else(|error| error.into_inner());
                    if state.stopped {
                        break;
                    }
                    if Instant::now() >= state.deadline {
                        state.failed = true;
                        break;
                    }
                    let lease = state.lease.clone();
                    drop(state);
                    let renewed = client.renew(&lease);
                    let mut state = lock.lock().unwrap_or_else(|error| error.into_inner());
                    if state.stopped {
                        break;
                    }
                    match renewed {
                        Ok(lease) => {
                            state.lease = lease;
                            state.deadline = Instant::now() + LEASE_TTL;
                        }
                        Err(_) => {
                            state.failed = true;
                            break;
                        }
                    }
                }
            })
            .map_err(|_| anyhow!("Cannot start dispatch lease renewal"))?;
        Ok(Self {
            state,
            worker: Some(worker),
        })
    }

    pub fn lease(&self) -> Result<Lease> {
        ensure!(
            self.worker
                .as_ref()
                .is_some_and(|worker| !worker.is_finished()),
            "Dispatch lease renewal stopped"
        );
        let state = self
            .state
            .0
            .lock()
            .map_err(|_| anyhow!("Dispatch lease renewal failed"))?;
        ensure!(
            !state.failed && !state.stopped,
            "Dispatch lease renewal failed"
        );
        ensure!(Instant::now() < state.deadline, "Dispatch lease expired");
        Ok(state.lease.clone())
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        let mut state = self
            .state
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.stopped = true;
        self.state.1.notify_all();
        drop(state);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

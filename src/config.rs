use std::{
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use serde_json::Value;

#[derive(Clone, Default)]
pub struct Environment {
    values: BTreeMap<String, String>,
}

impl Environment {
    pub fn load(root: &Path) -> Result<Self> {
        let mut values = match fs::read_to_string(root.join(".env")) {
            Ok(content) => parse_dotenv(&content)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => BTreeMap::new(),
            Err(_) => bail!("Cannot read .env"),
        };
        for (key, value) in env::vars_os() {
            if let (Ok(key), Ok(value)) = (key.into_string(), value.into_string()) {
                values.insert(key, value);
            }
        }
        Ok(Self { values })
    }

    pub fn from_values(values: BTreeMap<String, String>) -> Self {
        Self { values }
    }

    pub fn get(&self, name: &str) -> Option<&str> {
        self.values.get(name).map(String::as_str)
    }
}

#[derive(Clone, Debug)]
pub struct Runtime {
    pub data_dir: PathBuf,
    pub compute_workers: usize,
    pub compute_batch_size: usize,
    pub compute_pending_batches: usize,
    pub compute_batch_bytes: usize,
    pub sqlite_cache_mib: u64,
    pub start_free_gib: u64,
    pub min_free_gib: u64,
    pub download_timeout: Duration,
    pub metadata_timeout: Duration,
    pub diff_timeout: Duration,
    pub connect_timeout: Duration,
    pub download_retries: u32,
    pub download_retry_delay: Duration,
    pub diff_max_bytes: u64,
    pub upload_concurrency: usize,
    pub http_attempts: u32,
    pub http_timeout: Duration,
    pub http_connect_timeout: Duration,
    pub http_retry_delay: Duration,
    pub schedule_day: u8,
    pub schedule_hour: u8,
    pub schedule_minute: u8,
}

impl Runtime {
    pub fn compute_options(&self) -> crate::build::ComputeOptions {
        crate::build::ComputeOptions {
            workers: self.compute_workers,
            batch_size: self.compute_batch_size,
            pending_batches: self.compute_pending_batches,
            batch_bytes: self.compute_batch_bytes,
            sqlite_cache_mib: self.sqlite_cache_mib,
        }
    }

    pub fn load(root: &Path, environment: &Environment) -> Result<Self> {
        let content = fs::read_to_string(root.join("config/runtime.json"))
            .context("Cannot read config/runtime.json")?;
        Self::from_json(root, &content, environment)
    }

    pub fn from_json(root: &Path, defaults: &str, environment: &Environment) -> Result<Self> {
        if environment
            .get("OSM_BUILD_CONCURRENCY")
            .is_some_and(|value| !value.is_empty())
        {
            bail!(
                "OSM_BUILD_CONCURRENCY was removed; configure OSM_COMPUTE_WORKERS for single-region processing"
            );
        }
        let defaults: BTreeMap<String, Value> =
            serde_json::from_str(defaults).context("Invalid config/runtime.json")?;
        let keys = [
            "OSM_DATA_DIR",
            "OSM_COMPUTE_WORKERS",
            "OSM_COMPUTE_BATCH_SIZE",
            "OSM_COMPUTE_PENDING_BATCHES",
            "OSM_COMPUTE_BATCH_BYTES",
            "OSM_SQLITE_CACHE_MIB",
            "OSM_START_FREE_GIB",
            "OSM_MIN_FREE_GIB",
            "OSM_DOWNLOAD_TIMEOUT_SECONDS",
            "OSM_METADATA_TIMEOUT_SECONDS",
            "OSM_DIFF_TIMEOUT_SECONDS",
            "OSM_CONNECT_TIMEOUT_SECONDS",
            "OSM_DOWNLOAD_RETRIES",
            "OSM_DOWNLOAD_RETRY_DELAY_SECONDS",
            "OSM_DIFF_MAX_MIB",
            "OSM_UPLOAD_CONCURRENCY",
            "OSM_HTTP_ATTEMPTS",
            "OSM_HTTP_TIMEOUT_MS",
            "OSM_HTTP_CONNECT_TIMEOUT_MS",
            "OSM_HTTP_RETRY_DELAY_MS",
            "OSM_SCHEDULE_DAY",
            "OSM_SCHEDULE_HOUR",
            "OSM_SCHEDULE_MINUTE",
        ];
        ensure!(
            defaults.len() == keys.len() && keys.iter().all(|key| defaults.contains_key(*key)),
            "config/runtime.json must contain the supported runtime settings"
        );
        let value = |key: &str| -> Result<String> {
            if let Some(value) = environment.get(key) {
                return Ok(value.to_owned());
            }
            match defaults.get(key) {
                Some(Value::String(value)) => Ok(value.clone()),
                Some(Value::Number(value)) if value.as_u64().is_some() => Ok(value.to_string()),
                _ => bail!("Invalid runtime setting: {key}"),
            }
        };
        let number = |key: &str, minimum: u64, maximum: u64| -> Result<u64> {
            let raw = value(key)?;
            ensure!(
                !raw.is_empty()
                    && raw.bytes().all(|byte| byte.is_ascii_digit())
                    && (raw.len() == 1 || !raw.starts_with('0')),
                "{key} must be an integer"
            );
            let number: u64 = raw
                .parse()
                .with_context(|| format!("{key} exceeds its supported range"))?;
            ensure!(
                (minimum..=maximum).contains(&number),
                "{key} must be from {minimum} to {maximum}"
            );
            Ok(number)
        };
        let maximum = 9_007_199_254_740_991_u64;
        let size = |key: &str| -> Result<usize> {
            usize::try_from(number(key, 1, maximum)?)
                .with_context(|| format!("{key} exceeds this platform's range"))
        };
        let seconds =
            |key: &str| -> Result<Duration> { Ok(Duration::from_secs(number(key, 1, maximum)?)) };
        let compute_workers = if value("OSM_COMPUTE_WORKERS")? == "auto" {
            std::thread::available_parallelism()
                .context("Cannot determine available CPU cores")?
                .get()
        } else {
            size("OSM_COMPUTE_WORKERS")?
        };
        let compute_pending_batches = if value("OSM_COMPUTE_PENDING_BATCHES")? == "auto" {
            compute_workers
        } else {
            size("OSM_COMPUTE_PENDING_BATCHES")?
        };
        let directory = value("OSM_DATA_DIR")?;
        ensure!(
            !directory.trim().is_empty() && !directory.contains('\0'),
            "Invalid OSM_DATA_DIR"
        );
        let data_dir = if directory == "~" || directory.starts_with("~/") {
            let home = environment
                .get("HOME")
                .context("HOME is required for OSM_DATA_DIR starting with ~")?;
            ensure!(Path::new(home).is_absolute(), "HOME must be absolute");
            Path::new(home).join(directory.trim_start_matches('~').trim_start_matches('/'))
        } else {
            ensure!(
                !directory.starts_with('~'),
                "OSM_DATA_DIR supports only ~ or ~/ for home paths"
            );
            root.join(directory)
        };
        let http_attempts = number("OSM_HTTP_ATTEMPTS", 1, 10)? as u32;
        let retry_ms = number("OSM_HTTP_RETRY_DELAY_MS", 0, 2_147_483_647)?;
        ensure!(
            retry_ms * 2_u64.pow(http_attempts.saturating_sub(2)) <= 2_147_483_647,
            "OSM_HTTP_RETRY_DELAY_MS exceeds the timer limit for OSM_HTTP_ATTEMPTS"
        );
        Ok(Self {
            data_dir,
            compute_workers,
            compute_batch_size: size("OSM_COMPUTE_BATCH_SIZE")?,
            compute_pending_batches,
            compute_batch_bytes: size("OSM_COMPUTE_BATCH_BYTES")?,
            sqlite_cache_mib: number("OSM_SQLITE_CACHE_MIB", 1, i32::MAX as u64 / 1024)?,
            start_free_gib: number("OSM_START_FREE_GIB", 0, u64::MAX / 1024_u64.pow(3))?,
            min_free_gib: number("OSM_MIN_FREE_GIB", 0, u64::MAX / 1024_u64.pow(3))?,
            download_timeout: seconds("OSM_DOWNLOAD_TIMEOUT_SECONDS")?,
            metadata_timeout: seconds("OSM_METADATA_TIMEOUT_SECONDS")?,
            diff_timeout: seconds("OSM_DIFF_TIMEOUT_SECONDS")?,
            connect_timeout: seconds("OSM_CONNECT_TIMEOUT_SECONDS")?,
            download_retries: number("OSM_DOWNLOAD_RETRIES", 0, u32::MAX as u64)? as u32,
            download_retry_delay: Duration::from_secs(number(
                "OSM_DOWNLOAD_RETRY_DELAY_SECONDS",
                0,
                maximum,
            )?),
            diff_max_bytes: number("OSM_DIFF_MAX_MIB", 1, u64::MAX / 1024_u64.pow(2))?
                * 1024_u64.pow(2),
            upload_concurrency: number("OSM_UPLOAD_CONCURRENCY", 1, 64)? as usize,
            http_attempts,
            http_timeout: Duration::from_millis(number("OSM_HTTP_TIMEOUT_MS", 1, 2_147_483_647)?),
            http_connect_timeout: Duration::from_millis(number(
                "OSM_HTTP_CONNECT_TIMEOUT_MS",
                1,
                2_147_483_647,
            )?),
            http_retry_delay: Duration::from_millis(retry_ms),
            schedule_day: number("OSM_SCHEDULE_DAY", 1, 31)? as u8,
            schedule_hour: number("OSM_SCHEDULE_HOUR", 0, 23)? as u8,
            schedule_minute: number("OSM_SCHEDULE_MINUTE", 0, 59)? as u8,
        })
    }
}

fn parse_dotenv(content: &str) -> Result<BTreeMap<String, String>> {
    let mut values = BTreeMap::new();
    let mut remaining = content.trim_start_matches('\u{feff}');
    while !remaining.trim_start().is_empty() {
        remaining = remaining.trim_start();
        if remaining.starts_with('#') {
            remaining = remaining.split_once('\n').map_or("", |(_, tail)| tail);
            continue;
        }
        if let Some(tail) = remaining.strip_prefix("export")
            && tail.starts_with([' ', '\t'])
        {
            remaining = tail.trim_start_matches([' ', '\t']);
        }
        let (key, tail) = remaining
            .split_once('=')
            .context("Invalid .env assignment")?;
        let key = key.trim();
        ensure!(
            !key.is_empty()
                && key.bytes().enumerate().all(|(index, byte)| {
                    byte == b'_'
                        || byte.is_ascii_alphabetic()
                        || (index > 0 && byte.is_ascii_digit())
                }),
            "Invalid .env variable name"
        );
        remaining = tail.trim_start_matches([' ', '\t']);
        let value = if let Some(quote @ ('\'' | '"' | '`')) = remaining.chars().next() {
            let (text, tail) = remaining[1..]
                .split_once(quote)
                .context("Unclosed .env quoted value")?;
            remaining = tail.trim_start_matches([' ', '\t', '\r']);
            ensure!(
                remaining.is_empty() || remaining.starts_with(['\n', '#']),
                "Unexpected text after .env quoted value"
            );
            if quote == '"' {
                text.replace("\\n", "\n").replace("\\r", "\r")
            } else {
                text.to_owned()
            }
        } else {
            let (text, tail) = remaining.split_once('\n').unwrap_or((remaining, ""));
            remaining = tail;
            text.split('#').next().unwrap_or_default().trim().to_owned()
        };
        values.insert(key.to_owned(), value);
    }
    Ok(values)
}

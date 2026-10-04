use std::{error::Error, fmt, thread, time::Duration};

pub const MAX_RETRY_DELAY: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Clone, Copy, Debug)]
pub struct RetryAfter(pub Duration);

impl fmt::Display for RetryAfter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Source requested a retry after {}s",
            self.0.as_secs()
        )
    }
}

pub fn backoff(initial: Duration, attempt: u32) -> Duration {
    let mut delay = initial.min(MAX_RETRY_DELAY);
    for _ in 0..attempt {
        if delay.is_zero() || delay == MAX_RETRY_DELAY {
            break;
        }
        delay = delay.saturating_mul(2).min(MAX_RETRY_DELAY);
    }
    delay
}

pub fn retry_delay(error: &anyhow::Error, initial: Duration, attempt: u32) -> Duration {
    backoff(initial, attempt).max(
        error
            .downcast_ref::<RetryAfter>()
            .map_or(Duration::ZERO, |hint| hint.0.min(MAX_RETRY_DELAY)),
    )
}

pub fn retry<T>(
    initial: Duration,
    mut operation: impl FnMut() -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    let mut attempt = 0_u32;
    loop {
        match operation() {
            Err(error) if retryable(&error) => {
                let delay = retry_delay(&error, initial, attempt);
                crate::progress::message(&format!(
                    "Network unavailable: {error:#}; retrying in {}s",
                    delay.as_secs()
                ));
                thread::sleep(delay);
                attempt = attempt.saturating_add(1);
            }
            result => return result,
        }
    }
}

pub fn retryable_status(status: reqwest::StatusCode) -> bool {
    status.is_server_error() || matches!(status.as_u16(), 408 | 425 | 429)
}

#[derive(Clone, Debug)]
pub struct Transient(pub String);

impl fmt::Display for Transient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for Transient {}

pub fn request(error: reqwest::Error) -> anyhow::Error {
    let mut source = error.source();
    while let Some(cause) = source {
        if let Some(transient) = cause.downcast_ref::<Transient>() {
            return transient.clone().into();
        }
        source = cause.source();
    }
    let reason = if error.is_timeout() {
        "Network timeout (DNS, TCP, TLS or response)"
    } else if error.is_connect() {
        "Network connection failed (DNS, TCP or TLS)"
    } else if error.is_builder() || error.is_redirect() {
        return anyhow::anyhow!("Invalid HTTP request or response");
    } else if error.is_decode() {
        "Network response body decoding failed"
    } else if let Some(status) = error.status() {
        let message = format!("HTTP request failed: {status}");
        return if retryable_status(status) {
            Transient(message).into()
        } else {
            anyhow::anyhow!(message)
        };
    } else {
        "Network transport failed"
    };
    Transient(reason.into()).into()
}

pub fn retryable(error: &anyhow::Error) -> bool {
    error.downcast_ref::<Transient>().is_some()
}

pub fn summary(error: &anyhow::Error, message: String) -> anyhow::Error {
    let summary = if retryable(error) {
        Transient(message).into()
    } else {
        anyhow::anyhow!(message)
    };
    match error.downcast_ref::<RetryAfter>() {
        Some(hint) => summary.context(*hint),
        None => summary,
    }
}

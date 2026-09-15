use std::fmt;

#[derive(Clone, Debug)]
pub struct Transient(pub String);

impl fmt::Display for Transient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for Transient {}

pub fn request(error: reqwest::Error) -> anyhow::Error {
    let reason = if error.is_timeout() {
        "Network timeout (DNS, TCP, TLS or response)"
    } else if error.is_connect() {
        "Network connection failed (DNS, TCP or TLS)"
    } else if error.is_builder() || error.is_redirect() || error.is_decode() {
        return anyhow::anyhow!("Invalid HTTP request or response");
    } else if let Some(status) = error.status() {
        let message = format!("HTTP request failed: {status}");
        return if status.is_server_error() || status.as_u16() == 429 {
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
    if retryable(error) {
        Transient(message).into()
    } else {
        anyhow::anyhow!(message)
    }
}

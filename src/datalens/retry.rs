use std::time::Duration;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DatalensFailureKind {
    ProviderLimit,
    Transient,
    Other,
}

pub fn classify_datalens_failure_message(message: &str) -> DatalensFailureKind {
    let message = message.to_ascii_lowercase();
    if message.contains("query returns too many logs")
        || message.contains("too many logs")
        || message.contains("narrow your filter")
        || message.contains("provider limit")
        || message.contains("range limit")
        || message.contains("block range too large")
        || message.contains("range too large")
    {
        return DatalensFailureKind::ProviderLimit;
    }

    if message.contains("timeout")
        || message.contains("timed out")
        || message.contains("provider_failure")
        || message.contains("providerfailure")
        || message.contains("provider_error")
        || message.contains("rate-limit")
        || message.contains("rate limit")
        || message.contains("rate_limited")
        || message.contains("quota_exceeded")
        || message.contains("bad gateway")
        || message.contains("service unavailable")
        || message.contains("gateway timeout")
    {
        return DatalensFailureKind::Transient;
    }

    DatalensFailureKind::Other
}

pub(super) fn http_retry_after(body: &str) -> Option<Duration> {
    let payload = serde_json::from_str::<serde_json::Value>(body).ok()?;
    retry_after_from_value(&payload)
}

fn retry_after_from_value(value: &serde_json::Value) -> Option<Duration> {
    match value {
        serde_json::Value::Object(object) => {
            if let Some(seconds) = object
                .get("retry_after_seconds")
                .and_then(retry_after_seconds)
            {
                return Some(Duration::from_secs(seconds));
            }
            object.values().find_map(retry_after_from_value)
        }
        serde_json::Value::Array(values) => values.iter().find_map(retry_after_from_value),
        _ => None,
    }
}

fn retry_after_seconds(value: &serde_json::Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|value| value.parse::<u64>().ok()))
}

pub(super) fn datalens_retry_delay(attempt: u64) -> Duration {
    let millis = 250_u64.saturating_mul(1_u64 << attempt.saturating_sub(1).min(2));
    Duration::from_millis(millis.min(1_000))
}

pub(super) fn is_retryable_http_status(status: reqwest::StatusCode) -> bool {
    status == reqwest::StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
        || status.as_u16() == 524
}

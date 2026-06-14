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
        || message.contains("rate-limit")
        || message.contains("rate limit")
        || message.contains("rate_limited")
        || message.contains("bad gateway")
        || message.contains("service unavailable")
        || message.contains("gateway timeout")
    {
        return DatalensFailureKind::Transient;
    }

    DatalensFailureKind::Other
}

pub(super) fn body_has_retryable_graphql_errors(body: &str) -> bool {
    let Ok(payload) = serde_json::from_str::<serde_json::Value>(body) else {
        return false;
    };
    let Some(errors) = payload.get("errors") else {
        return false;
    };
    errors
        .as_array()
        .map(|errors| errors.iter().any(graphql_error_is_retryable))
        .unwrap_or_else(|| graphql_error_is_retryable(errors))
}

fn graphql_error_is_retryable(error: &serde_json::Value) -> bool {
    if let Some(message) = error.get("message").and_then(serde_json::Value::as_str)
        && matches!(
            classify_datalens_failure_message(message),
            DatalensFailureKind::Transient
        )
    {
        return true;
    }

    let Some(extensions) = error.get("extensions") else {
        return false;
    };

    if let Some(code) = extensions.get("code").and_then(serde_json::Value::as_str)
        && matches!(
            classify_datalens_failure_message(code),
            DatalensFailureKind::Transient
        )
    {
        return true;
    }

    if let Some(kind) = extensions.get("kind").and_then(serde_json::Value::as_str)
        && matches!(
            classify_datalens_failure_message(kind),
            DatalensFailureKind::Transient
        )
    {
        return true;
    }

    ["status", "statusCode", "httpStatus"]
        .iter()
        .filter_map(|key| extensions.get(*key))
        .any(retryable_status_value)
}

pub(super) fn graphql_retry_after(body: &str) -> Option<Duration> {
    let payload = serde_json::from_str::<serde_json::Value>(body).ok()?;
    let errors = payload.get("errors")?;
    if let Some(errors) = errors.as_array() {
        errors
            .iter()
            .filter(|error| graphql_error_is_retryable(error))
            .find_map(retry_after_from_value)
    } else if graphql_error_is_retryable(errors) {
        retry_after_from_value(errors)
    } else {
        None
    }
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

fn retryable_status_value(value: &serde_json::Value) -> bool {
    let status = value
        .as_u64()
        .or_else(|| value.as_str().and_then(|value| value.parse::<u64>().ok()));
    matches!(status, Some(429 | 500..=599))
}

pub(super) fn datalens_retry_delay(attempt: u64) -> std::time::Duration {
    let millis = 250_u64.saturating_mul(1_u64 << attempt.saturating_sub(1).min(2));
    std::time::Duration::from_millis(millis.min(1_000))
}

pub(super) fn is_retryable_http_status(status: reqwest::StatusCode) -> bool {
    status == reqwest::StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
        || status.as_u16() == 524
}

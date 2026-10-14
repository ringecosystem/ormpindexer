use std::time::Duration;

use reqwest::header::HeaderMap;

pub(super) fn retry_after_header(headers: &HeaderMap) -> Option<Duration> {
    let value = headers.get(reqwest::header::RETRY_AFTER)?.to_str().ok()?;
    value.parse::<u64>().ok().map(Duration::from_secs)
}

pub(super) fn datalens_retry_delay(attempt: u64) -> Duration {
    let millis = 250_u64.saturating_mul(1_u64 << attempt.saturating_sub(1).min(2));
    Duration::from_millis(millis.min(1_000))
}

pub(super) fn is_retryable_http_status(status: reqwest::StatusCode) -> bool {
    status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_after_header_reads_delta_seconds() {
        let mut headers = HeaderMap::new();
        headers.insert(reqwest::header::RETRY_AFTER, "0".parse().expect("header"));
        assert_eq!(retry_after_header(&headers), Some(Duration::from_secs(0)));
        assert_eq!(retry_after_header(&HeaderMap::new()), None);
    }
}

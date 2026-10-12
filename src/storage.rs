use opendal::layers::{RetryLayer, TimeoutLayer};
use opendal::{FuturesAsyncReader, FuturesAsyncWriter, Operator};

use std::io;
use std::time::Duration;

/// Retries per request before an error is returned.
///
/// Common Crawl answers `503 SlowDown` to everyone while its bucket is overloaded, often for tens
/// of seconds, so a few short retries are not enough.
const RETRY_MAX_TIMES: usize = 10;
/// Longest delay between retries; delays start at 1s and double, with jitter.
const RETRY_MAX_DELAY: Duration = Duration::from_secs(30);

/// Opens `path` for reading as a single streaming request.
///
/// Every request may be throttled, so one request per file survives rate limiting far better than
/// many ranged chunk requests.
pub(crate) async fn reader(operator: &Operator, path: &str) -> io::Result<FuturesAsyncReader> {
    Ok(with_default_layers(operator)
        .reader(path)
        .await?
        .into_futures_async_read(..)
        .await?)
}

/// Opens `path` for writing.
pub(crate) async fn writer(operator: &Operator, path: &str) -> io::Result<FuturesAsyncWriter> {
    Ok(with_default_layers(operator)
        .writer(path)
        .await?
        .into_futures_async_write())
}

/// Adds timeout and retry layers, and installs OpenDAL's default HTTP transport, which remote
/// services such as S3 need and which is not installed with default features disabled.
fn with_default_layers(operator: &Operator) -> Operator {
    opendal::install_default();
    operator.clone().layer(TimeoutLayer::default()).layer(
        RetryLayer::new()
            .with_jitter()
            .with_max_times(RETRY_MAX_TIMES)
            .with_max_delay(RETRY_MAX_DELAY),
    )
}

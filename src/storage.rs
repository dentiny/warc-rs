use opendal::layers::{RetryLayer, TimeoutLayer};
use opendal::{FuturesAsyncReader, FuturesAsyncWriter, Operator};

use std::io;

/// Size of each ranged request issued when reading.
const READ_CHUNK_BYTES: usize = 8 * 1_048_576;
/// Number of ranged requests kept in flight.
const READ_CONCURRENCY: usize = 4;
/// Bytes a reader may have requested ahead of its position.
pub(crate) const READ_AHEAD_BYTES: u64 = (READ_CHUNK_BYTES * READ_CONCURRENCY) as u64;

/// Opens `path` for reading through OpenDAL's default timeout and retry layers.
pub(crate) async fn reader(operator: &Operator, path: &str) -> io::Result<FuturesAsyncReader> {
    Ok(with_default_layers(operator)
        .reader_with(path)
        .chunk(READ_CHUNK_BYTES)
        .concurrent(READ_CONCURRENCY)
        .await?
        .into_futures_async_read(..)
        .await?)
}

/// Opens `path` for writing through OpenDAL's default timeout and retry layers.
pub(crate) async fn writer(operator: &Operator, path: &str) -> io::Result<FuturesAsyncWriter> {
    Ok(with_default_layers(operator)
        .writer(path)
        .await?
        .into_futures_async_write())
}

fn with_default_layers(operator: &Operator) -> Operator {
    operator
        .clone()
        .layer(TimeoutLayer::default())
        .layer(RetryLayer::default())
}

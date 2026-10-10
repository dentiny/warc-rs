use opendal::layers::{RetryLayer, TimeoutLayer};
use opendal::{FuturesAsyncReader, FuturesAsyncWriter, Operator};

use std::io;

/// Opens `path` for reading through OpenDAL's default timeout and retry layers.
pub(crate) async fn reader(operator: &Operator, path: &str) -> io::Result<FuturesAsyncReader> {
    Ok(with_default_layers(operator)
        .reader(path)
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

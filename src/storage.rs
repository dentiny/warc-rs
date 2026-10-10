use opendal::blocking::{self, StdReader, StdWriter};
use opendal::layers::{RetryLayer, TimeoutLayer};
use opendal::Operator;

use std::io;
use std::io::BufReader;

const MB: usize = 1_048_576;

/// Opens `path` for reading through OpenDAL's default timeout and retry layers.
///
/// The calling thread must have entered a Tokio runtime without running async code on it, such
/// as inside `tokio::task::spawn_blocking` or after `Runtime::enter`.
pub(crate) fn reader(operator: &Operator, path: &str) -> io::Result<BufReader<StdReader>> {
    let reader = blocking_operator(operator)?
        .reader(path)?
        .into_std_read(..)?;
    Ok(BufReader::with_capacity(MB, reader))
}

/// Opens `path` for writing, with the same layers and runtime requirements as [`reader`].
pub(crate) fn writer(operator: &Operator, path: &str) -> io::Result<StdWriter> {
    Ok(blocking_operator(operator)?.writer(path)?.into_std_write())
}

fn blocking_operator(operator: &Operator) -> io::Result<blocking::Operator> {
    let operator = operator
        .clone()
        .layer(TimeoutLayer::default())
        .layer(RetryLayer::default());
    Ok(blocking::Operator::new(operator)?)
}

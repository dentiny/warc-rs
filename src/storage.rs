use opendal::blocking;
use opendal::layers::{RetryLayer, TimeoutLayer};
use opendal::Operator;

use std::io;

/// Wraps `operator` with OpenDAL's default timeout and retry layers for blocking IO.
///
/// The calling thread must have entered a Tokio runtime without running async code on it, such
/// as inside `tokio::task::spawn_blocking` or after `Runtime::enter`.
pub(crate) fn blocking_operator(operator: &Operator) -> io::Result<blocking::Operator> {
    let operator = operator
        .clone()
        .layer(TimeoutLayer::default())
        .layer(RetryLayer::default());
    Ok(blocking::Operator::new(operator)?)
}

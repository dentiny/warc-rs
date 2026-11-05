#![deny(missing_docs)]
//! A WARC (Web ARChive) library

mod error;
pub use error::Error;

#[cfg(feature = "storage")]
pub use opendal;

#[cfg(feature = "gzip")]
mod gzip;
#[cfg(feature = "gzip")]
pub use gzip::GzipReader;

#[cfg(feature = "storage")]
mod storage;

mod warc_reader;
pub use warc_reader::*;
mod warc_writer;
pub use warc_writer::*;

mod header;
pub use header::WarcHeader;

/// Core functions for parsing. Not recommended for direct use.
pub mod parser;

mod record;
pub use record::{BufferedBody, EmptyBody, RawRecordHeader, Record, RecordBuilder, StreamingBody};

mod record_type;
pub use record_type::RecordType;

mod truncated_type;
pub use truncated_type::TruncatedType;

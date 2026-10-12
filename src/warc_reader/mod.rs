mod framing;
mod record_iter;
mod streaming_iter;

pub use record_iter::{RawRecordIter, RecordIter};
pub use streaming_iter::StreamingIter;

use crate::storage;

use futures_util::io::{AsyncBufRead, AsyncSeek};
use opendal::{FuturesAsyncReader, Operator};

use std::io;
use std::io::SeekFrom;
use std::pin::Pin;
use std::task::{Context, Poll};

#[cfg(feature = "gzip")]
use crate::GzipReader;

/// The `AsyncSeek::poll_seek` of a seekable reader.
type PollSeek<R> = fn(Pin<&mut R>, &mut Context<'_>, SeekFrom) -> Poll<io::Result<u64>>;

/// A reader which iteratively parses WARC records from a stream.
pub struct WarcReader<R> {
    reader: R,
    seek: Option<PollSeek<R>>,
}

impl<R: AsyncBufRead + Unpin> WarcReader<R> {
    /// Create a new reader.
    pub fn new(r: R) -> Self {
        WarcReader {
            reader: r,
            seek: None,
        }
    }

    /// Create a stream over all of the raw records read.
    ///
    /// This only does well-formedness checks on the headers. See `RawRecordHeader` for more
    /// information.
    pub fn iter_raw_records(self) -> RawRecordIter<R> {
        RawRecordIter::new(self.reader)
    }

    /// Create a stream over all of the records read.
    ///
    /// This will fully build each record and check it for semantic correctness. See the `Record`
    /// type for more information.
    pub fn iter_records(self) -> RecordIter<R> {
        RecordIter::new(self.reader)
    }

    /// Create a streaming iterator over all of the records read.
    ///
    /// This will build each record header, and allow the caller to decide whether to read
    /// the body or not. For readers created with `new_seekable`, large bodies the caller does
    /// not read are skipped by seeking.
    pub fn stream_records(&mut self) -> StreamingIter<'_, R> {
        StreamingIter::new(&mut self.reader, self.seek)
    }
}

impl<R: AsyncBufRead + AsyncSeek + Unpin> WarcReader<R> {
    /// Create a new reader over a seekable stream.
    ///
    /// When `stream_records` moves to the next record, it seeks past the unread part of the
    /// current body if at least 32 MiB remain, instead of reading and discarding it.
    pub fn new_seekable(r: R) -> Self {
        WarcReader {
            reader: r,
            seek: Some(R::poll_seek),
        }
    }
}

impl WarcReader<FuturesAsyncReader> {
    /// Create a new reader which reads `path` from `operator`.
    ///
    /// Reads go through OpenDAL's default timeout and retry layers, and fetch 2 MiB ranges with up
    /// to 16 in flight, so each reader may buffer about 32 MiB ahead. Large bodies skipped by
    /// `stream_records` are seeked past rather than downloaded; see `new_seekable`.
    pub async fn from_path(operator: &Operator, path: &str) -> io::Result<Self> {
        Ok(WarcReader::new_seekable(
            storage::reader(operator, path).await?,
        ))
    }
}

#[cfg(feature = "gzip")]
impl WarcReader<GzipReader<FuturesAsyncReader>> {
    /// Create a new reader which reads a GZIP-compressed `path` from `operator`.
    ///
    /// See [`GzipReader`] for how members are decompressed.
    pub async fn from_path_gzip(operator: &Operator, path: &str) -> io::Result<Self> {
        let reader = storage::reader(operator, path).await?;
        Ok(WarcReader::new(GzipReader::new(reader)))
    }
}

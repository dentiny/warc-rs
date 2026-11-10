mod framing;
mod record_iter;
mod streaming_iter;

pub use record_iter::{RawRecordIter, RecordIter};
pub use streaming_iter::StreamingIter;

#[cfg(feature = "storage")]
use crate::storage;

use futures_util::io::{AsyncBufRead, AsyncSeek};
#[cfg(feature = "storage")]
use opendal::{FuturesAsyncReader, Operator};

use std::io;
use std::io::SeekFrom;
use std::pin::Pin;
use std::task::{Context, Poll};

#[cfg(all(feature = "gzip", feature = "storage"))]
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

    /// Read raw records, checking header syntax without validating required fields.
    pub fn iter_raw_records(self) -> RawRecordIter<R> {
        RawRecordIter::new(self.reader)
    }

    /// Read buffered records, validating headers through [`crate::Record`].
    pub fn iter_records(self) -> RecordIter<R> {
        RecordIter::new(self.reader)
    }

    /// Read headers and choose whether to consume each body.
    /// Readers created with [`Self::new_seekable`] seek past large unread bodies.
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

#[cfg(feature = "storage")]
impl WarcReader<FuturesAsyncReader> {
    /// Create a new reader which reads `path` from `operator`.
    ///
    /// The file is read as a single streaming request, with timeouts and up to 10 jittered retries
    /// per request. Large bodies skipped by `stream_records` are seeked past rather than
    /// downloaded; see `new_seekable`.
    ///
    /// To read in concurrent ranged chunks instead, build the reader with OpenDAL's
    /// `reader_with(path).chunk(..).concurrent(..)` and pass it to `new_seekable`.
    pub async fn from_path(operator: &Operator, path: &str) -> io::Result<Self> {
        Ok(WarcReader::new_seekable(
            storage::reader(operator, path).await?,
        ))
    }
}

#[cfg(all(feature = "gzip", feature = "storage"))]
impl WarcReader<GzipReader<FuturesAsyncReader>> {
    /// Create a new reader which reads a GZIP-compressed `path` from `operator`.
    ///
    /// See [`GzipReader`] for how members are decompressed.
    pub async fn from_path_gzip(operator: &Operator, path: &str) -> io::Result<Self> {
        let reader = storage::reader(operator, path).await?;
        Ok(WarcReader::new(GzipReader::new(reader)))
    }
}

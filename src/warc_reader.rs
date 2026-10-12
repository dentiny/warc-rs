use crate::{parser, storage};
use crate::{BufferedBody, EmptyBody, Error, RawRecordHeader, Record, StreamingBody, WarcHeader};

use futures_util::io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncReadExt};
use opendal::{FuturesAsyncReader, Operator};

use std::collections::HashMap;
use std::convert::TryFrom;
use std::io;

#[cfg(feature = "gzip")]
use crate::GzipReader;

const MB: usize = 1_048_576;
/// Upper bound for one record's header block, so corrupt input cannot grow it without limit.
const MAX_HEADER_BLOCK_BYTES: u64 = 4 * MB as u64;

/// Reads one header block, up to and including the empty line that ends it, into `buffer`.
///
/// Returns `Ok(false)` when the input ends cleanly before another record. Input that ends after
/// non-whitespace header bytes is reported as an error rather than silently dropped.
async fn read_header_block<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    buffer: &mut Vec<u8>,
) -> Result<bool, Error> {
    loop {
        let remaining = MAX_HEADER_BLOCK_BYTES - buffer.len() as u64;
        if remaining == 0 {
            return Err(Error::ReadData(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("WARC header block exceeds {} bytes", MAX_HEADER_BLOCK_BYTES),
            )));
        }
        let bytes_read = (&mut *reader)
            .take(remaining)
            .read_until(b'\n', buffer)
            .await
            .map_err(Error::ReadData)?;
        if bytes_read == 0 {
            if buffer.iter().all(u8::is_ascii_whitespace) {
                return Ok(false);
            }
            return Err(Error::ReadData(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "input ended inside a WARC header block",
            )));
        }
        if bytes_read == 2 && buffer.ends_with(b"\r\n") {
            return Ok(true);
        }
    }
}

/// Reads and parses the next header block, returning it with the declared body length.
///
/// `header_buffer` is scratch space reused across records. Returns `None` when the input ends
/// cleanly before another record.
async fn read_record_header<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    header_buffer: &mut Vec<u8>,
) -> Option<Result<(RawRecordHeader, u64), Error>> {
    header_buffer.clear();
    match read_header_block(reader, header_buffer).await {
        Ok(true) => {}
        Ok(false) => return None,
        Err(e) => return Some(Err(e)),
    }

    let line_count = header_buffer.iter().filter(|&&b| b == b'\n').count();
    let mut headers = HashMap::with_capacity(line_count);
    let parsed = parser::headers_with(header_buffer, |token, value| {
        headers.insert(WarcHeader::from(token), value.to_owned());
    });
    let (version, body_len) = match parsed {
        Ok((_, parsed)) => parsed,
        Err(e) => {
            return Some(Err(Error::ParseHeaders(
                e.map(|inner| (inner.input.to_owned(), inner.code)),
            )))
        }
    };
    let headers = RawRecordHeader {
        version: version.to_owned(),
        headers,
    };
    Some(Ok((headers, body_len as u64)))
}

/// Reads a body of `len` bytes followed by the record terminator.
async fn read_body<R: AsyncBufRead + Unpin>(reader: &mut R, len: u64) -> Result<Vec<u8>, Error> {
    let mut body = Vec::with_capacity(std::cmp::min(len, MB as u64) as usize);
    (&mut *reader)
        .take(len)
        .read_to_end(&mut body)
        .await
        .map_err(Error::ReadData)?;
    if (body.len() as u64) < len {
        return Err(Error::UnexpectedEOB);
    }
    read_terminator(reader).await?;
    Ok(body)
}

/// Reads the `\r\n\r\n` that ends every record.
async fn read_terminator<R: AsyncRead + Unpin>(reader: &mut R) -> Result<(), Error> {
    let mut crlfs = [0; 4];
    match reader.read_exact(&mut crlfs).await {
        Ok(()) => (),
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Err(Error::UnexpectedEOB),
        Err(io) => return Err(Error::ReadData(io)),
    }

    if &crlfs == b"\r\n\r\n" {
        Ok(())
    } else {
        Err(Error::ParseHeaders(nom::Err::Failure((
            b"\r\n\r\n".to_vec(),
            nom::error::ErrorKind::Tag,
        ))))
    }
}

/// A reader which iteratively parses WARC records from a stream.
pub struct WarcReader<R> {
    reader: R,
}

impl<R: AsyncBufRead + Unpin> WarcReader<R> {
    /// Create a new reader.
    pub fn new(r: R) -> Self {
        WarcReader { reader: r }
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
    /// the body or not.
    pub fn stream_records(&mut self) -> StreamingIter<'_, R> {
        StreamingIter::new(&mut self.reader)
    }
}

impl WarcReader<FuturesAsyncReader> {
    /// Create a new reader which reads `path` from `operator`.
    ///
    /// Reads go through OpenDAL's default timeout and retry layers.
    pub async fn from_path(operator: &Operator, path: &str) -> io::Result<Self> {
        Ok(WarcReader::new(storage::reader(operator, path).await?))
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

/// A stream of raw records read from a reader. See `RawRecord` for more information.
pub struct RawRecordIter<R> {
    reader: R,
    header_buffer: Vec<u8>,
}

impl<R: AsyncBufRead + Unpin> RawRecordIter<R> {
    pub(crate) fn new(reader: R) -> RawRecordIter<R> {
        RawRecordIter {
            reader,
            header_buffer: Vec::new(),
        }
    }

    /// Read the next raw record, or `None` once the input ends.
    pub async fn next(&mut self) -> Option<Result<(RawRecordHeader, Vec<u8>), Error>> {
        let (headers, body_len) =
            match read_record_header(&mut self.reader, &mut self.header_buffer).await? {
                Ok(header) => header,
                Err(e) => return Some(Err(e)),
            };
        Some(
            read_body(&mut self.reader, body_len)
                .await
                .map(|body| (headers, body)),
        )
    }
}

/// A stream of records read from a reader.
pub struct RecordIter<R> {
    raw: RawRecordIter<R>,
}

impl<R: AsyncBufRead + Unpin> RecordIter<R> {
    pub(crate) fn new(reader: R) -> RecordIter<R> {
        RecordIter {
            raw: RawRecordIter::new(reader),
        }
    }

    /// Read the next record, or `None` once the input ends.
    pub async fn next(&mut self) -> Option<Result<Record<BufferedBody>, Error>> {
        let record = self.raw.next().await?.and_then(|(headers, body)| {
            Record::<EmptyBody>::try_from(headers).map(|record| record.add_body(body))
        });
        Some(record)
    }
}

/// An iterator-like type to "stream" records from a reader.
///
/// This API returns records which use the `StreamingBody` type. This allows reading record headers
/// and metadata without reading the bodies. Bodies can be read or skipped as desired.
///
/// This is streaming iterator is particularly useful for streams of records which are indefinite
/// or contain and records of unknown size.
pub struct StreamingIter<'r, R> {
    reader: &'r mut R,
    header_buffer: Vec<u8>,
    current_item_size: u64,
    first_record: bool,
}

impl<R: AsyncBufRead + Unpin> StreamingIter<'_, R> {
    pub(crate) fn new(reader: &mut R) -> StreamingIter<'_, R> {
        StreamingIter {
            reader,
            header_buffer: Vec::new(),
            current_item_size: 0,
            first_record: true,
        }
    }

    async fn skip_body(&mut self) -> Result<(), Error> {
        let body_bytes_left = self.current_item_size;
        if body_bytes_left > 0 {
            let skipped = futures_util::io::copy_buf(
                (&mut *self.reader).take(body_bytes_left),
                &mut futures_util::io::sink(),
            )
            .await
            .map_err(Error::ReadData)?;
            if skipped < body_bytes_left {
                return Err(Error::UnexpectedEOB);
            }
        }
        read_terminator(self.reader).await
    }

    /// Advance the stream to the next item.
    ///
    /// Returns one of the following:
    /// * Some(Ok(r))` is the next record read from the stream.
    /// * `Some(Err)` indicates there was a read error.
    /// * `None` indicates no more records are returned.
    pub async fn next_item(&mut self) -> Option<Result<Record<StreamingBody<'_, R>>, Error>> {
        if self.first_record {
            self.first_record = false;
        } else if let Err(e) = self.skip_body().await {
            return Some(Err(e));
        }

        let (headers, body_len) =
            match read_record_header(self.reader, &mut self.header_buffer).await? {
                Ok(header) => header,
                Err(e) => return Some(Err(e)),
            };
        self.current_item_size = body_len;

        let record = match Record::<EmptyBody>::try_from(headers) {
            Ok(record) => record,
            Err(e) => return Some(Err(e)),
        };
        Some(
            record
                .add_fixed_stream(self.reader, &mut self.current_item_size)
                .map_err(Error::ReadData),
        )
    }
}

#[cfg(test)]
mod iter_raw_tests {
    use futures_executor::block_on;
    use futures_util::io::Cursor;
    use std::collections::HashMap;
    use std::iter::FromIterator;

    use crate::{WarcHeader, WarcReader};

    #[test]
    fn basic_record() {
        let raw = b"\
            WARC/1.0\r\n\
            Warc-Type: dunno\r\n\
            Content-Length: 5\r\n\
            WARC-Record-Id: <urn:test:basic-record:record-0>\r\n\
            WARC-Date: 2020-07-08T02:52:55Z\r\n\
            \r\n\
            12345\r\n\
            \r\n\
        ";

        let expected_version = "1.0";
        let expected_headers: HashMap<WarcHeader, Vec<u8>> = HashMap::from_iter(vec![
            (WarcHeader::WarcType, b"dunno".to_vec()),
            (WarcHeader::ContentLength, b"5".to_vec()),
            (
                WarcHeader::RecordID,
                b"<urn:test:basic-record:record-0>".to_vec(),
            ),
            (WarcHeader::Date, b"2020-07-08T02:52:55Z".to_vec()),
        ]);
        let expected_body: &[u8] = b"12345";

        let mut reader = WarcReader::new(Cursor::new(&raw[..])).iter_raw_records();
        let (headers, body) = block_on(reader.next()).unwrap().unwrap();
        assert_eq!(headers.version, expected_version);
        assert_eq!(headers.as_ref(), &expected_headers);
        assert_eq!(body, expected_body);
    }

    #[test]
    fn two_records() {
        let raw = b"\
            WARC/1.0\r\n\
            Warc-Type: dunno\r\n\
            Content-Length: 5\r\n\
            WARC-Record-Id: <urn:test:two-records:record-0>\r\n\
            WARC-Date: 2020-07-08T02:52:55Z\r\n\
            \r\n\
            12345\r\n\
            \r\n\
            WARC/1.0\r\n\
            Warc-Type: another\r\n\
            WARC-Record-Id: <urn:test:two-records:record-1>\r\n\
            WARC-Date: 2020-07-08T02:52:56Z\r\n\
            Content-Length: 6\r\n\
            \r\n\
            123456\r\n\
            \r\n\
        ";

        let mut reader = WarcReader::new(Cursor::new(&raw[..])).iter_raw_records();
        {
            let expected_version = "1.0";
            let expected_headers: HashMap<WarcHeader, Vec<u8>> = HashMap::from_iter(vec![
                (WarcHeader::WarcType, b"dunno".to_vec()),
                (WarcHeader::ContentLength, b"5".to_vec()),
                (
                    WarcHeader::RecordID,
                    b"<urn:test:two-records:record-0>".to_vec(),
                ),
                (WarcHeader::Date, b"2020-07-08T02:52:55Z".to_vec()),
            ]);
            let expected_body: &[u8] = b"12345";

            let (headers, body) = block_on(reader.next()).unwrap().unwrap();
            assert_eq!(headers.version, expected_version);
            assert_eq!(headers.as_ref(), &expected_headers);
            assert_eq!(body, expected_body);
        }

        {
            let expected_version = "1.0";
            let expected_headers: HashMap<WarcHeader, Vec<u8>> = HashMap::from_iter(vec![
                (WarcHeader::WarcType, b"another".to_vec()),
                (WarcHeader::ContentLength, b"6".to_vec()),
                (
                    WarcHeader::RecordID,
                    b"<urn:test:two-records:record-1>".to_vec(),
                ),
                (WarcHeader::Date, b"2020-07-08T02:52:56Z".to_vec()),
            ]);
            let expected_body: &[u8] = b"123456";

            let (headers, body) = block_on(reader.next()).unwrap().unwrap();
            assert_eq!(headers.version, expected_version);
            assert_eq!(headers.as_ref(), &expected_headers);
            assert_eq!(body, expected_body);
        }
    }
}

#[cfg(test)]
mod next_item_tests {
    use futures_executor::block_on;
    use futures_util::io::Cursor;

    use crate::WarcReader;

    #[test]
    fn first_item() {
        let raw = b"\
            WARC/1.0\r\n\
            Warc-Type: dunno\r\n\
            Content-Length: 5\r\n\
            WARC-Record-Id: <urn:test:basic-record:record-0>\r\n\
            WARC-Date: 2020-07-08T02:52:55Z\r\n\
            \r\n\
            12345\r\n\
            \r\n\
        ";

        block_on(async {
            let mut reader = WarcReader::new(Cursor::new(&raw[..]));
            let mut stream_iter = reader.stream_records();
            let record = stream_iter
                .next_item()
                .await
                .unwrap()
                .unwrap()
                .into_buffered()
                .await
                .unwrap();
            assert_eq!(record.warc_version(), "1.0");
            assert_eq!(record.content_length(), 5);
            assert_eq!(record.warc_id(), "<urn:test:basic-record:record-0>");
            assert_eq!(record.body(), b"12345");
        });
    }

    #[test]
    fn both_items() {
        let raw = b"\
            WARC/1.0\r\n\
            Warc-Type: dunno\r\n\
            Content-Length: 5\r\n\
            WARC-Record-Id: <urn:test:two-records:record-0>\r\n\
            WARC-Date: 2020-07-08T02:52:55Z\r\n\
            \r\n\
            12345\r\n\
            \r\n\
            WARC/1.0\r\n\
            Warc-Type: another\r\n\
            WARC-Record-Id: <urn:test:two-records:record-1>\r\n\
            WARC-Date: 2020-07-08T02:52:56Z\r\n\
            Content-Length: 6\r\n\
            \r\n\
            123456\r\n\
            \r\n\
        ";

        block_on(async {
            let mut reader = WarcReader::new(Cursor::new(&raw[..]));
            let mut stream_iter = reader.stream_records();

            {
                let record = stream_iter
                    .next_item()
                    .await
                    .unwrap()
                    .unwrap()
                    .into_buffered()
                    .await
                    .unwrap();
                assert_eq!(record.warc_version(), "1.0");
                assert_eq!(record.content_length(), 5);
                assert_eq!(record.warc_id(), "<urn:test:two-records:record-0>");
                assert_eq!(record.body(), b"12345");
            }

            {
                let record = stream_iter
                    .next_item()
                    .await
                    .unwrap()
                    .unwrap()
                    .into_buffered()
                    .await
                    .unwrap();
                assert_eq!(record.warc_version(), "1.0");
                assert_eq!(record.content_length(), 6);
                assert_eq!(record.warc_id(), "<urn:test:two-records:record-1>");
                assert_eq!(record.body(), b"123456");
            }
        });
    }

    #[test]
    fn only_second_item() {
        let raw = b"\
            WARC/1.0\r\n\
            Warc-Type: dunno\r\n\
            Content-Length: 5\r\n\
            WARC-Record-Id: <urn:test:two-records:record-0>\r\n\
            WARC-Date: 2020-07-08T02:52:55Z\r\n\
            \r\n\
            12345\r\n\
            \r\n\
            WARC/1.0\r\n\
            Warc-Type: another\r\n\
            WARC-Record-Id: <urn:test:two-records:record-1>\r\n\
            WARC-Date: 2020-07-08T02:52:56Z\r\n\
            Content-Length: 6\r\n\
            \r\n\
            123456\r\n\
            \r\n\
        ";

        block_on(async {
            let mut reader = WarcReader::new(Cursor::new(&raw[..]));
            let mut stream_iter = reader.stream_records();

            let _skipped = stream_iter.next_item().await.unwrap().unwrap();

            let record = stream_iter
                .next_item()
                .await
                .unwrap()
                .unwrap()
                .into_buffered()
                .await
                .unwrap();
            assert_eq!(record.warc_version(), "1.0");
            assert_eq!(record.content_length(), 6);
            assert_eq!(record.warc_id(), "<urn:test:two-records:record-1>");
            assert_eq!(record.body(), b"123456");
        });
    }

    #[test]
    fn triple_items() {
        let raw = b"\
            WARC/1.0\r\n\
            Warc-Type: dunno\r\n\
            Content-Length: 5\r\n\
            WARC-Record-Id: <urn:test:three-records:record-0>\r\n\
            WARC-Date: 2020-07-08T02:52:55Z\r\n\
            \r\n\
            12345\r\n\
            \r\n\
            WARC/1.0\r\n\
            Warc-Type: another\r\n\
            WARC-Record-Id: <urn:test:three-records:record-1>\r\n\
            WARC-Date: 2020-07-08T02:52:56Z\r\n\
            Content-Length: 6\r\n\
            \r\n\
            123456\r\n\
            \r\n\
            WARC/1.0\r\n\
            Warc-Type: yet another\r\n\
            WARC-Record-Id: <urn:test:three-records:record-2>\r\n\
            WARC-Date: 2020-07-08T02:52:56Z\r\n\
            Content-Length: 8\r\n\
            \r\n\
            12345678\r\n\
            \r\n\
        ";

        block_on(async {
            let mut reader = WarcReader::new(Cursor::new(&raw[..]));
            let mut stream_iter = reader.stream_records();

            for (len, id, body) in [
                (5, "<urn:test:three-records:record-0>", &b"12345"[..]),
                (6, "<urn:test:three-records:record-1>", &b"123456"[..]),
                (8, "<urn:test:three-records:record-2>", &b"12345678"[..]),
            ]
            .iter()
            {
                let record = stream_iter
                    .next_item()
                    .await
                    .unwrap()
                    .unwrap()
                    .into_buffered()
                    .await
                    .unwrap();
                assert_eq!(record.warc_version(), "1.0");
                assert_eq!(record.content_length(), *len);
                assert_eq!(record.warc_id(), *id);
                assert_eq!(record.body(), *body);
            }
        });
    }

    #[test]
    fn empty_content_length() {
        let raw = b"\
        WARC/1.0\r\n\
        Warc-Type: empty-record\r\n\
        Content-Length: 0\r\n\
        WARC-Record-Id: <urn:test:empty-content-length>\r\n\
        WARC-Date: 2020-07-08T02:52:57Z\r\n\
        \r\n\
        \r\n\
    ";

        block_on(async {
            let mut reader = WarcReader::new(Cursor::new(&raw[..]));
            let mut stream_iter = reader.stream_records();

            let record = stream_iter
                .next_item()
                .await
                .unwrap()
                .unwrap()
                .into_buffered()
                .await
                .unwrap();
            assert_eq!(record.warc_version(), "1.0");
            assert_eq!(record.content_length(), 0);
            assert_eq!(record.warc_id(), "<urn:test:empty-content-length>");
            assert!(record.body().is_empty());
        });
    }

    #[test]
    fn zero_and_nonzero_content_length() {
        let raw = b"\
        WARC/1.0\r\n\
        Warc-Type: empty-record\r\n\
        Content-Length: 0\r\n\
        WARC-Record-Id: <urn:test:zero-content-length>\r\n\
        WARC-Date: 2020-07-08T02:52:57Z\r\n\
        \r\n\
        \r\n\
        \r\n\
        WARC/1.0\r\n\
        Warc-Type: non-empty-record\r\n\
        Content-Length: 7\r\n\
        WARC-Record-Id: <urn:test:nonzero-content-length>\r\n\
        WARC-Date: 2020-07-08T02:52:58Z\r\n\
        \r\n\
        1234567\r\n\
        \r\n\
    ";

        let mut iter = WarcReader::new(Cursor::new(&raw[..])).iter_records();

        let record = block_on(iter.next()).unwrap().unwrap();
        assert_eq!(record.warc_version(), "1.0");
        assert_eq!(record.content_length(), 0);
        assert_eq!(record.warc_id(), "<urn:test:zero-content-length>");
        assert!(record.body().is_empty());

        let record = block_on(iter.next()).unwrap().unwrap();
        assert_eq!(record.warc_version(), "1.0");
        assert_eq!(record.content_length(), 7);
        assert_eq!(record.warc_id(), "<urn:test:nonzero-content-length>");
        assert_eq!(record.body(), b"1234567");
    }
}

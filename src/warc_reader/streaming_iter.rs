use super::framing::{read_record_header, read_terminator};
use super::PollSeek;
use crate::{storage, EmptyBody, Error, Record, StreamingBody};

use futures_util::future::poll_fn;
use futures_util::io::{AsyncBufRead, AsyncReadExt};

use std::convert::TryFrom;
use std::io;
use std::io::SeekFrom;
use std::pin::Pin;

/// Smallest remaining body that `StreamingIter` skips by seeking rather than reading.
///
/// Seeking a remote reader outside its buffer discards the ranges already in flight and starts new
/// requests, so skipping less than the read-ahead window costs more than reading through it.
const SEEK_THRESHOLD: u64 = storage::READ_AHEAD_BYTES;

/// An iterator-like type to "stream" records from a reader.
///
/// This API returns records which use the `StreamingBody` type. This allows reading record headers
/// and metadata without reading the bodies. Bodies can be read or skipped as desired.
///
/// This is streaming iterator is particularly useful for streams of records which are indefinite
/// or contain and records of unknown size.
pub struct StreamingIter<'r, R> {
    reader: &'r mut R,
    seek: Option<PollSeek<R>>,
    header_buffer: Vec<u8>,
    current_item_size: u64,
    /// Whether the previous record's body and terminator have yet to be consumed.
    in_record: bool,
}

impl<R: AsyncBufRead + Unpin> StreamingIter<'_, R> {
    pub(crate) fn new(reader: &mut R, seek: Option<PollSeek<R>>) -> StreamingIter<'_, R> {
        StreamingIter {
            reader,
            seek,
            header_buffer: Vec::new(),
            current_item_size: 0,
            in_record: false,
        }
    }

    async fn skip_body(&mut self) -> Result<(), Error> {
        let body_bytes_left = std::mem::take(&mut self.current_item_size);
        let seek_offset = self
            .seek
            .filter(|_| body_bytes_left >= SEEK_THRESHOLD)
            .and_then(|poll_seek| Some((poll_seek, i64::try_from(body_bytes_left).ok()?)));

        if let Some((poll_seek, offset)) = seek_offset {
            let reader = &mut *self.reader;
            poll_fn(|cx| poll_seek(Pin::new(&mut *reader), cx, SeekFrom::Current(offset)))
                .await
                .map_err(|e| match e.kind() {
                    // OpenDAL rejects seeking past the end of the file with `InvalidInput`.
                    io::ErrorKind::InvalidInput => Error::UnexpectedEOB,
                    _ => Error::ReadData(e),
                })?;
        } else if body_bytes_left > 0 {
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
        if std::mem::take(&mut self.in_record) {
            if let Err(e) = self.skip_body().await {
                return Some(Err(e));
            }
        }

        let (headers, body_len) =
            match read_record_header(self.reader, &mut self.header_buffer).await? {
                Ok(header) => header,
                Err(e) => return Some(Err(e)),
            };
        self.current_item_size = body_len;
        self.in_record = true;

        let record = match Record::<EmptyBody>::try_from(headers) {
            Ok(record) => record,
            Err(e) => return Some(Err(e)),
        };
        Some(Ok(
            record.add_fixed_stream(self.reader, &mut self.current_item_size)
        ))
    }
}

#[cfg(test)]
mod tests {
    use futures_executor::block_on;
    use futures_util::io::{AsyncBufRead, AsyncRead, AsyncReadExt, AsyncSeek, Cursor};
    use opendal::services::Memory;
    use opendal::Operator;

    use std::io;
    use std::io::SeekFrom;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use super::SEEK_THRESHOLD;
    use crate::{EmptyBody, Error, Record, WarcReader, WarcWriter};

    const MB: usize = 1_048_576;

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
    fn bad_terminator_does_not_skip_body_twice() {
        let raw = b"\
            WARC/1.0\r\n\
            Warc-Type: dunno\r\n\
            Content-Length: 5\r\n\
            WARC-Record-Id: <urn:test:bad-terminator:record-0>\r\n\
            WARC-Date: 2020-07-08T02:52:55Z\r\n\
            \r\n\
            12345XXXX\
            WARC/1.0\r\n\
            Warc-Type: another\r\n\
            WARC-Record-Id: <urn:test:bad-terminator:record-1>\r\n\
            WARC-Date: 2020-07-08T02:52:56Z\r\n\
            Content-Length: 6\r\n\
            \r\n\
            123456\r\n\
            \r\n\
        ";

        block_on(async {
            let mut reader = WarcReader::new(Cursor::new(&raw[..]));
            let mut stream_iter = reader.stream_records();

            stream_iter.next_item().await.unwrap().unwrap();
            assert!(stream_iter.next_item().await.unwrap().is_err());
            let record = stream_iter
                .next_item()
                .await
                .unwrap()
                .unwrap()
                .into_buffered()
                .await
                .unwrap();
            assert_eq!(record.warc_id(), "<urn:test:bad-terminator:record-1>");
            assert_eq!(record.body(), b"123456");
        });
    }

    /// Counts the bytes consumed from, and the seeks issued to, an in-memory reader.
    struct CountingReader {
        inner: Cursor<Vec<u8>>,
        consumed: u64,
        seeks: usize,
    }

    impl CountingReader {
        fn new(data: Vec<u8>) -> Self {
            CountingReader {
                inner: Cursor::new(data),
                consumed: 0,
                seeks: 0,
            }
        }
    }

    impl AsyncRead for CountingReader {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut [u8],
        ) -> Poll<io::Result<usize>> {
            let this = self.get_mut();
            let n = futures_util::ready!(Pin::new(&mut this.inner).poll_read(cx, buf))?;
            this.consumed += n as u64;
            Poll::Ready(Ok(n))
        }
    }

    impl AsyncBufRead for CountingReader {
        fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
            Pin::new(&mut self.get_mut().inner).poll_fill_buf(cx)
        }

        fn consume(self: Pin<&mut Self>, amt: usize) {
            let this = self.get_mut();
            this.consumed += amt as u64;
            Pin::new(&mut this.inner).consume(amt);
        }
    }

    impl AsyncSeek for CountingReader {
        fn poll_seek(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            pos: SeekFrom,
        ) -> Poll<io::Result<u64>> {
            let this = self.get_mut();
            this.seeks += 1;
            Pin::new(&mut this.inner).poll_seek(cx, pos)
        }
    }

    fn large_body() -> Vec<u8> {
        vec![b'x'; 2 * SEEK_THRESHOLD as usize]
    }

    fn warc(bodies: &[&[u8]]) -> Vec<u8> {
        let mut writer = WarcWriter::new(Vec::new());
        for body in bodies {
            let record = Record::<EmptyBody>::default().add_body(body.to_vec());
            block_on(writer.write(&record)).unwrap();
        }
        writer.into_inner()
    }

    fn skip_first_read_second<R: AsyncBufRead + Unpin>(reader: &mut WarcReader<R>) -> Vec<u8> {
        block_on(async {
            let mut iter = reader.stream_records();
            iter.next_item().await.unwrap().unwrap();
            let second = iter.next_item().await.unwrap().unwrap();
            second.into_buffered().await.unwrap().body().to_vec()
        })
    }

    #[test]
    fn seeks_past_large_unread_body() {
        let data = warc(&[&large_body(), b"hello"]);
        let mut reader = WarcReader::new_seekable(CountingReader::new(data));
        assert_eq!(skip_first_read_second(&mut reader), b"hello");
        assert_eq!(reader.reader.seeks, 1);
        assert!(reader.reader.consumed < SEEK_THRESHOLD);
    }

    #[test]
    fn reads_through_small_body() {
        let data = warc(&[&[b'x'; 1_024], b"hello"]);
        let mut reader = WarcReader::new_seekable(CountingReader::new(data));
        assert_eq!(skip_first_read_second(&mut reader), b"hello");
        assert_eq!(reader.reader.seeks, 0);
    }

    #[test]
    fn new_never_seeks() {
        let data = warc(&[&large_body(), b"hello"]);
        let mut reader = WarcReader::new(CountingReader::new(data));
        assert_eq!(skip_first_read_second(&mut reader), b"hello");
        assert_eq!(reader.reader.seeks, 0);
        assert!(reader.reader.consumed > 2 * SEEK_THRESHOLD);
    }

    #[test]
    fn seeks_past_rest_of_partially_read_body() {
        let data = warc(&[&large_body(), b"hello"]);
        let mut reader = WarcReader::new_seekable(CountingReader::new(data));
        block_on(async {
            let mut iter = reader.stream_records();
            let mut first = iter.next_item().await.unwrap().unwrap();
            let mut prefix = [0; 100];
            first.read_exact(&mut prefix).await.unwrap();
            assert_eq!(prefix, [b'x'; 100]);
            let second = iter.next_item().await.unwrap().unwrap();
            assert_eq!(second.into_buffered().await.unwrap().body(), b"hello");
        });
        assert_eq!(reader.reader.seeks, 1);
    }

    #[test]
    fn seek_past_truncated_body_is_unexpected_eob() {
        let mut data = warc(&[&large_body()]);
        data.truncate(data.len() - SEEK_THRESHOLD as usize);
        let mut reader = WarcReader::new_seekable(CountingReader::new(data));
        block_on(async {
            let mut iter = reader.stream_records();
            iter.next_item().await.unwrap().unwrap();
            assert!(matches!(
                iter.next_item().await,
                Some(Err(Error::UnexpectedEOB))
            ));
        });
    }

    #[tokio::test]
    async fn from_path_seeks_past_large_bodies() {
        let operator = Operator::new(Memory::default()).unwrap();
        operator
            .write("a.warc", warc(&[&large_body(), b"hello"]))
            .await
            .unwrap();
        let mut reader = WarcReader::from_path(&operator, "a.warc").await.unwrap();
        let mut iter = reader.stream_records();
        iter.next_item().await.unwrap().unwrap();
        let second = iter.next_item().await.unwrap().unwrap();
        assert_eq!(second.into_buffered().await.unwrap().body(), b"hello");
        assert!(iter.next_item().await.is_none());
    }

    #[tokio::test]
    async fn from_path_reads_records_across_chunks() {
        let operator = Operator::new(Memory::default()).unwrap();
        let first: Vec<u8> = (0..3 * MB + 7).map(|i| i as u8).collect();
        let last: Vec<u8> = (0..5 * MB + 11).map(|i| (i * 7) as u8).collect();
        operator
            .write("a.warc", warc(&[&first, b"hello", &last]))
            .await
            .unwrap();
        let mut iter = WarcReader::from_path(&operator, "a.warc")
            .await
            .unwrap()
            .iter_records();
        for expected in [&first[..], b"hello", &last[..]].iter() {
            let record = iter.next().await.unwrap().unwrap();
            assert!(record.body() == *expected);
        }
        assert!(iter.next().await.is_none());
    }

    #[tokio::test]
    async fn from_path_truncated_body_is_unexpected_eob() {
        let operator = Operator::new(Memory::default()).unwrap();
        let mut data = warc(&[&large_body()]);
        data.truncate(data.len() - SEEK_THRESHOLD as usize);
        operator.write("a.warc", data).await.unwrap();
        let mut reader = WarcReader::from_path(&operator, "a.warc").await.unwrap();
        let mut iter = reader.stream_records();
        iter.next_item().await.unwrap().unwrap();
        assert!(matches!(
            iter.next_item().await,
            Some(Err(Error::UnexpectedEOB))
        ));
    }
}

use super::framing::{read_body, read_record_header};
use crate::{BufferedBody, EmptyBody, Error, RawRecordHeader, Record};

use futures_util::io::AsyncBufRead;

use std::convert::TryFrom;

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

#[cfg(test)]
mod tests {
    use futures_executor::block_on;
    use futures_util::io::Cursor;
    use std::collections::HashMap;
    use std::iter::FromIterator;

    use crate::{Error, WarcHeader, WarcReader};

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

    #[test]
    fn truncated_body_is_unexpected_eob() {
        let raw = b"\
            WARC/1.0\r\n\
            Content-Length: 10\r\n\
            \r\n\
            12345\
        ";

        let mut reader = WarcReader::new(Cursor::new(&raw[..])).iter_raw_records();
        assert!(matches!(
            block_on(reader.next()),
            Some(Err(Error::UnexpectedEOB))
        ));
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

use crate::parser;
use crate::record::MAX_BODY_PREALLOCATION;
use crate::{Error, RawRecordHeader, WarcHeader};

use futures_util::io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncReadExt};

use std::collections::HashMap;
use std::io;

/// Upper bound for one record's header block, so corrupt input cannot grow it without limit.
const MAX_HEADER_BLOCK_BYTES: u64 = 4 * 1_048_576;

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
pub(super) async fn read_record_header<R: AsyncBufRead + Unpin>(
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
pub(super) async fn read_body<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    len: u64,
) -> Result<Vec<u8>, Error> {
    let mut body = Vec::with_capacity(std::cmp::min(len, MAX_BODY_PREALLOCATION) as usize);
    let mut remaining = len;
    while remaining > 0 {
        let chunk = reader.fill_buf().await.map_err(Error::ReadData)?;
        if chunk.is_empty() {
            return Err(Error::UnexpectedEOB);
        }
        let n = std::cmp::min(chunk.len() as u64, remaining) as usize;
        body.extend_from_slice(&chunk[..n]);
        reader.consume_unpin(n);
        remaining -= n as u64;
    }
    read_terminator(reader).await?;
    Ok(body)
}

/// Reads the `\r\n\r\n` that ends every record.
pub(super) async fn read_terminator<R: AsyncRead + Unpin>(reader: &mut R) -> Result<(), Error> {
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

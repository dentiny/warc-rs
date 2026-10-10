use futures_util::io::{AsyncBufRead, AsyncRead};
use libflate::gzip::Decoder;

use std::cmp;
use std::io;
use std::io::{Cursor, Read};
use std::pin::Pin;
use std::task::{ready, Context, Poll};

const MIN_READ_AHEAD: usize = 64 * 1_024;

/// An async reader which decompresses a multi-member GZIP stream.
///
/// Decompression is synchronous: each member is buffered until it is complete, then decoded in
/// memory. Memory use therefore grows with the largest member, so this suits WARC files that
/// compress each record as its own member, as the WARC specification recommends.
pub struct GzipReader<R> {
    inner: R,
    input: Vec<u8>,
    /// Bytes `input` must hold before decoding is retried after an incomplete member.
    wanted: usize,
    output: Vec<u8>,
    output_pos: usize,
    eof: bool,
}

impl<R> GzipReader<R> {
    /// Create a reader which decompresses `inner`.
    pub fn new(inner: R) -> Self {
        GzipReader {
            inner,
            input: Vec::new(),
            wanted: 0,
            output: Vec::new(),
            output_pos: 0,
            eof: false,
        }
    }
}

/// Decodes the first member of `input`, returning its data and the compressed length.
fn decode_member(input: &[u8]) -> io::Result<(Vec<u8>, usize)> {
    let mut cursor = Cursor::new(input);
    let mut output = Vec::new();
    Decoder::new(&mut cursor)?.read_to_end(&mut output)?;
    Ok((output, cursor.position() as usize))
}

impl<R: AsyncBufRead + Unpin> AsyncBufRead for GzipReader<R> {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        let this = self.get_mut();
        loop {
            if this.output_pos < this.output.len() {
                return Poll::Ready(Ok(&this.output[this.output_pos..]));
            }
            let ready_to_decode = this.input.len() >= cmp::max(this.wanted, 1);
            if ready_to_decode || (this.eof && !this.input.is_empty()) {
                match decode_member(&this.input) {
                    Ok((output, used)) => {
                        this.input.drain(..used);
                        this.output = output;
                        this.output_pos = 0;
                        this.wanted = 0;
                        continue;
                    }
                    Err(e) if e.kind() == io::ErrorKind::UnexpectedEof && !this.eof => {
                        this.wanted = cmp::max(this.input.len() * 2, MIN_READ_AHEAD);
                    }
                    Err(e) => return Poll::Ready(Err(e)),
                }
            }
            if this.eof {
                return Poll::Ready(Ok(&[]));
            }
            let chunk = ready!(Pin::new(&mut this.inner).poll_fill_buf(cx))?;
            if chunk.is_empty() {
                this.eof = true;
                continue;
            }
            let len = chunk.len();
            this.input.extend_from_slice(chunk);
            Pin::new(&mut this.inner).consume(len);
        }
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        self.get_mut().output_pos += amt;
    }
}

impl<R: AsyncBufRead + Unpin> AsyncRead for GzipReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let available = ready!(self.as_mut().poll_fill_buf(cx))?;
        let len = cmp::min(available.len(), buf.len());
        buf[..len].copy_from_slice(&available[..len]);
        self.consume(len);
        Poll::Ready(Ok(len))
    }
}

#[cfg(test)]
mod tests {
    use super::GzipReader;

    use futures_executor::block_on;
    use futures_util::io::{AsyncReadExt, BufReader, Cursor};
    use libflate::gzip::Encoder;
    use std::io::{ErrorKind, Write};

    fn member(data: &[u8]) -> Vec<u8> {
        let mut encoder = Encoder::new(Vec::new()).unwrap();
        encoder.write_all(data).unwrap();
        encoder.finish().into_result().unwrap()
    }

    fn read_in_small_chunks(compressed: &[u8]) -> std::io::Result<Vec<u8>> {
        let inner = BufReader::with_capacity(7, Cursor::new(compressed));
        let mut output = Vec::new();
        block_on(GzipReader::new(inner).read_to_end(&mut output))?;
        Ok(output)
    }

    #[test]
    fn decodes_members_split_across_reads() {
        let large: Vec<u8> = (0..300_000u32).map(|i| (i * 31 % 251) as u8).collect();
        let mut compressed = member(b"first");
        compressed.extend(member(&large));
        compressed.extend(member(b""));
        compressed.extend(member(b"last"));

        let mut expected = b"first".to_vec();
        expected.extend(&large);
        expected.extend(b"last");
        assert_eq!(read_in_small_chunks(&compressed).unwrap(), expected);
    }

    #[test]
    fn truncated_member_is_an_error() {
        let compressed = member(b"hello");
        let error = read_in_small_chunks(&compressed[..compressed.len() - 1]).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::UnexpectedEof);
    }
}

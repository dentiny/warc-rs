use crate::storage;
use crate::{BufferedBody, RawRecordHeader, Record};

use futures_util::io::{AsyncWrite, AsyncWriteExt};
use opendal::{FuturesAsyncWriter, Operator};

use std::io;

#[cfg(feature = "gzip")]
use libflate::gzip::Encoder as GzipWriter;
#[cfg(feature = "gzip")]
use std::io::Write;

/// A writer which writes records to an output stream.
pub struct WarcWriter<W> {
    writer: W,
    #[cfg(feature = "gzip")]
    gzip: bool,
}

impl<W: AsyncWrite + Unpin> WarcWriter<W> {
    /// Create a new writer.
    pub fn new(w: W) -> Self {
        WarcWriter {
            writer: w,
            #[cfg(feature = "gzip")]
            gzip: false,
        }
    }

    /// Create a new writer which compresses each record as its own GZIP member.
    #[cfg(feature = "gzip")]
    pub fn new_gzip(w: W) -> Self {
        WarcWriter {
            writer: w,
            gzip: true,
        }
    }

    /// Write a single record.
    ///
    /// The number of uncompressed bytes written is returned upon success.
    pub async fn write(&mut self, record: &Record<BufferedBody>) -> io::Result<usize> {
        self.write_raw(record.to_raw_header(), &record.body()).await
    }

    /// Write a single raw record.
    ///
    /// The number of uncompressed bytes written is returned upon success.
    pub async fn write_raw<B>(&mut self, headers: RawRecordHeader, body: &B) -> io::Result<usize>
    where
        B: AsRef<[u8]>,
    {
        let mut header = b"WARC/".to_vec();
        header.extend_from_slice(headers.version.as_bytes());
        header.extend_from_slice(b"\r\n");
        for (token, value) in headers.as_ref().iter() {
            header.extend_from_slice(token.to_string().as_bytes());
            header.extend_from_slice(b": ");
            header.extend_from_slice(value);
            header.extend_from_slice(b"\r\n");
        }
        header.extend_from_slice(b"\r\n");

        let parts: [&[u8]; 3] = [&header, body.as_ref(), b"\r\n\r\n"];
        let len = parts.iter().map(|part| part.len()).sum();

        #[cfg(feature = "gzip")]
        {
            if self.gzip {
                let mut encoder = GzipWriter::new(Vec::new())?;
                for part in parts.iter() {
                    encoder.write_all(part)?;
                }
                let member = encoder.finish().into_result()?;
                self.writer.write_all(&member).await?;
                return Ok(len);
            }
        }

        for part in parts.iter() {
            self.writer.write_all(part).await?;
        }
        Ok(len)
    }

    /// Flush buffered records and close the output stream.
    ///
    /// For writers created by `from_path`, records are only guaranteed to be stored once this
    /// returns `Ok`.
    pub async fn close(mut self) -> io::Result<()> {
        self.writer.close().await
    }

    /// Consume this writer and return the inner writer.
    pub fn into_inner(self) -> W {
        self.writer
    }
}

impl WarcWriter<FuturesAsyncWriter> {
    /// Create a new writer which writes `path` through `operator`, replacing any existing file.
    ///
    /// Writes go through OpenDAL's default timeout and retry layers. Records are only guaranteed
    /// to be stored once `close` returns `Ok`.
    pub async fn from_path(operator: &Operator, path: &str) -> io::Result<Self> {
        Ok(WarcWriter::new(storage::writer(operator, path).await?))
    }

    /// Create a new writer which writes a GZIP-compressed `path` through `operator`, replacing
    /// any existing file. Each record is compressed as its own GZIP member.
    ///
    /// See [`WarcWriter::from_path`] for when records are stored.
    #[cfg(feature = "gzip")]
    pub async fn from_path_gzip(operator: &Operator, path: &str) -> io::Result<Self> {
        Ok(WarcWriter::new_gzip(storage::writer(operator, path).await?))
    }
}

#[cfg(test)]
mod tests {
    use crate::{BufferedBody, EmptyBody, Record, WarcReader, WarcWriter};

    use futures_util::io::AsyncBufRead;
    use opendal::services::Memory;
    use opendal::Operator;

    async fn assert_reads_back<R: AsyncBufRead + Unpin>(
        reader: WarcReader<R>,
        expected: &[Record<BufferedBody>],
    ) {
        let mut records = reader.iter_records();
        for expected in expected {
            let record = records.next().await.unwrap().unwrap();
            assert_eq!(record.warc_id(), expected.warc_id());
            assert_eq!(record.body(), expected.body());
        }
        assert!(records.next().await.is_none());
    }

    #[tokio::test]
    async fn round_trips_through_operator() {
        let operator = Operator::new(Memory::default()).unwrap();
        let records = vec![
            Record::<EmptyBody>::default().add_body(b"hello".to_vec()),
            Record::<EmptyBody>::default().add_body(vec![b'x'; 200_000]),
        ];

        let mut writer = WarcWriter::from_path(&operator, "a.warc").await.unwrap();
        for record in &records {
            writer.write(record).await.unwrap();
        }
        writer.close().await.unwrap();
        let reader = WarcReader::from_path(&operator, "a.warc").await.unwrap();
        assert_reads_back(reader, &records).await;

        #[cfg(feature = "gzip")]
        {
            let mut writer = WarcWriter::from_path_gzip(&operator, "a.warc.gz")
                .await
                .unwrap();
            for record in &records {
                writer.write(record).await.unwrap();
            }
            writer.close().await.unwrap();
            let reader = WarcReader::from_path_gzip(&operator, "a.warc.gz")
                .await
                .unwrap();
            assert_reads_back(reader, &records).await;
        }
    }
}

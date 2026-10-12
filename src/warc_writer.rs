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
    /// Scratch space for the serialized header block, reused across records.
    header: Vec<u8>,
    /// Scratch space for each compressed GZIP member, reused across records.
    #[cfg(feature = "gzip")]
    member: Vec<u8>,
    #[cfg(feature = "gzip")]
    gzip: bool,
}

impl<W: AsyncWrite + Unpin> WarcWriter<W> {
    /// Create a new writer.
    pub fn new(w: W) -> Self {
        WarcWriter {
            writer: w,
            header: Vec::new(),
            #[cfg(feature = "gzip")]
            member: Vec::new(),
            #[cfg(feature = "gzip")]
            gzip: false,
        }
    }

    /// Create a new writer which compresses each record as its own GZIP member.
    #[cfg(feature = "gzip")]
    pub fn new_gzip(w: W) -> Self {
        WarcWriter {
            gzip: true,
            ..WarcWriter::new(w)
        }
    }

    /// Write a single record.
    ///
    /// The number of uncompressed bytes written is returned upon success.
    pub async fn write(&mut self, record: &Record<BufferedBody>) -> io::Result<usize> {
        self.header.clear();
        record.write_header_block(&mut self.header);
        self.write_header_and_body(record.body()).await
    }

    /// Write a single raw record.
    ///
    /// The number of uncompressed bytes written is returned upon success.
    pub async fn write_raw<B>(&mut self, headers: RawRecordHeader, body: &B) -> io::Result<usize>
    where
        B: AsRef<[u8]>,
    {
        self.header.clear();
        headers.write_header_block(&mut self.header);
        self.write_header_and_body(body.as_ref()).await
    }

    /// Writes the header block in `self.header`, followed by `body` and the record terminator.
    async fn write_header_and_body(&mut self, body: &[u8]) -> io::Result<usize> {
        let parts: [&[u8]; 3] = [&self.header, body, b"\r\n\r\n"];
        let len = parts.iter().map(|part| part.len()).sum();

        #[cfg(feature = "gzip")]
        {
            if self.gzip {
                let mut member = std::mem::take(&mut self.member);
                member.clear();
                let mut encoder = GzipWriter::new(member)?;
                for part in parts.iter() {
                    encoder.write_all(part)?;
                }
                self.member = encoder.finish().into_result()?;
                self.writer.write_all(&self.member).await?;
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
    use crate::{
        BufferedBody, EmptyBody, Record, RecordBuilder, RecordType, TruncatedType, WarcHeader,
        WarcReader, WarcWriter,
    };

    use futures_executor::block_on;
    use futures_util::io::{AsyncBufRead, Cursor};
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

    #[test]
    fn write_emits_every_header() {
        let record = RecordBuilder::default()
            .warc_type(RecordType::Response)
            .truncated_type(TruncatedType::Length)
            .header(WarcHeader::TargetURI, "https://example.com/")
            .header(WarcHeader::Unknown("x-custom".to_string()), "value")
            .body(b"hello".to_vec())
            .build()
            .unwrap();

        let mut writer = WarcWriter::new(Vec::new());
        let written = block_on(writer.write(&record)).unwrap();
        let output = writer.into_inner();
        assert_eq!(written, output.len());

        let mut raw = WarcReader::new(Cursor::new(output)).iter_raw_records();
        let (headers, body) = block_on(raw.next()).unwrap().unwrap();
        assert_eq!(headers, record.to_raw_header());
        assert_eq!(body, b"hello");
        assert!(block_on(raw.next()).is_none());
    }
}

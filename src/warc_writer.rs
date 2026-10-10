use crate::storage::blocking_operator;
use crate::{BufferedBody, RawRecordHeader, Record};

use opendal::blocking::StdWriter;
use opendal::Operator;

use std::io;
use std::io::{BufWriter, Write};

#[cfg(feature = "gzip")]
use libflate::gzip::Encoder as GzipWriter;

const MB: usize = 1_048_576;

/// A writer which writes records to an output stream.
pub struct WarcWriter<W> {
    writer: W,
}

impl<W: Write> WarcWriter<W> {
    /// Create a new writer.
    pub fn new(w: W) -> Self {
        WarcWriter { writer: w }
    }

    /// Write a single record.
    ///
    /// The number of bytes written is returned upon success.
    pub fn write(&mut self, record: &Record<BufferedBody>) -> io::Result<usize> {
        self.write_raw(record.to_raw_header(), &record.body())
    }

    /// Write a single raw record.
    ///
    /// The number of bytes written is returned upon success.
    pub fn write_raw<B>(&mut self, headers: RawRecordHeader, body: &B) -> io::Result<usize>
    where
        B: AsRef<[u8]>,
    {
        let mut bytes_written = 0;
        let mut write = |bytes: &[u8]| -> io::Result<()> {
            self.writer.write_all(bytes)?;
            bytes_written += bytes.len();
            Ok(())
        };

        write(b"WARC/")?;
        write(headers.version.as_bytes())?;
        write(b"\r\n")?;

        for (token, value) in headers.as_ref().iter() {
            write(token.to_string().as_bytes())?;
            write(b": ")?;
            write(value)?;
            write(b"\r\n")?;
        }
        write(b"\r\n")?;

        write(body.as_ref())?;
        write(b"\r\n\r\n")?;

        Ok(bytes_written)
    }
}

impl<W: Write> WarcWriter<BufWriter<W>> {
    /// Consume this writer and return the inner writer.
    ///
    /// # Flushing Compressed Data Streams
    ///
    /// This method is necessary to be called at the end of a GZIP-compressed stream. An extra call
    /// is needed to flush the buffer of data, and write a trailer to the output stream.
    ///
    /// ```ignore
    /// let gzip_stream = writer.into_inner()?;
    /// gzip_writer.finish().into_result()?;
    /// ```
    ///
    pub fn into_inner(self) -> Result<W, std::io::IntoInnerError<BufWriter<W>>> {
        self.writer.into_inner()
    }
}

fn open_std_writer(operator: &Operator, path: &str) -> io::Result<StdWriter> {
    Ok(blocking_operator(operator)?.writer(path)?.into_std_write())
}

impl WarcWriter<BufWriter<StdWriter>> {
    /// Create a new writer which writes `path` through `operator`, replacing any existing file.
    ///
    /// Writes go through OpenDAL's default timeout and retry layers. The calling thread must have
    /// entered a Tokio runtime without running async code on it, such as inside
    /// `tokio::task::spawn_blocking` or after `Runtime::enter`.
    ///
    /// Records are only guaranteed to be stored once `close` returns `Ok`.
    pub fn from_path(operator: &Operator, path: &str) -> io::Result<Self> {
        let writer = open_std_writer(operator, path)?;
        Ok(WarcWriter::new(BufWriter::with_capacity(MB, writer)))
    }

    /// Flush buffered records and commit the file.
    pub fn close(self) -> io::Result<()> {
        let mut writer = self.writer.into_inner().map_err(|e| e.into_error())?;
        writer.close()
    }
}

#[cfg(feature = "gzip")]
impl WarcWriter<BufWriter<GzipWriter<StdWriter>>> {
    /// Create a new writer which writes a GZIP-compressed `path` through `operator`, replacing
    /// any existing file.
    ///
    /// See [`WarcWriter::from_path`] for the runtime requirements. Records are only guaranteed to
    /// be stored once `close` returns `Ok`.
    pub fn from_path_gzip(operator: &Operator, path: &str) -> io::Result<Self> {
        let gzip_stream = GzipWriter::new(open_std_writer(operator, path)?)?;
        Ok(WarcWriter::new(BufWriter::with_capacity(MB, gzip_stream)))
    }

    /// Flush buffered records, write the GZIP trailer, and commit the file.
    pub fn close(self) -> io::Result<()> {
        let gzip_stream = self.writer.into_inner().map_err(|e| e.into_error())?;
        let mut writer = gzip_stream.finish().into_result()?;
        writer.close()
    }
}

#[cfg(test)]
mod tests {
    use crate::{BufferedBody, EmptyBody, Record, WarcReader, WarcWriter};

    use opendal::services::Memory;
    use opendal::Operator;

    use std::io::BufRead;

    fn assert_reads_back<R: BufRead>(reader: WarcReader<R>, expected: &Record<BufferedBody>) {
        let records: Vec<_> = reader.iter_records().collect::<Result<_, _>>().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].warc_id(), expected.warc_id());
        assert_eq!(records[0].body(), expected.body());
    }

    #[test]
    fn round_trips_through_operator() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let _guard = runtime.enter();
        let operator = Operator::new(Memory::default()).unwrap();
        let record = Record::<EmptyBody>::default().add_body(b"hello".to_vec());

        let mut writer = WarcWriter::from_path(&operator, "a.warc").unwrap();
        writer.write(&record).unwrap();
        writer.close().unwrap();
        assert_reads_back(WarcReader::from_path(&operator, "a.warc").unwrap(), &record);

        #[cfg(feature = "gzip")]
        {
            let mut writer = WarcWriter::from_path_gzip(&operator, "a.warc.gz").unwrap();
            writer.write(&record).unwrap();
            writer.close().unwrap();
            let reader = WarcReader::from_path_gzip(&operator, "a.warc.gz").unwrap();
            assert_reads_back(reader, &record);
        }
    }
}

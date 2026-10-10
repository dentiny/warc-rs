use warc::opendal::services::Fs;
use warc::opendal::Operator;
use chrono::prelude::*;

use warc::WarcHeader;
use warc::{BufferedBody, RawRecordHeader, Record, RecordType, WarcWriter};

fn main() -> Result<(), std::io::Error> {
    let runtime = tokio::runtime::Runtime::new()?;
    let _guard = runtime.enter();
    let operator = Operator::new(Fs::default().root("."))?;
    let date = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);
    let body = format!("wrote to the file on {}", date);
    let body = body.into_bytes();

    let headers = RawRecordHeader {
        version: "1.0".to_owned(),
        headers: vec![
            (
                WarcHeader::RecordID,
                Record::<BufferedBody>::generate_record_id().into_bytes(),
            ),
            (
                WarcHeader::WarcType,
                RecordType::WarcInfo.to_string().into_bytes(),
            ),
            (WarcHeader::Date, date.into_bytes()),
            (WarcHeader::IPAddress, "127.0.0.1".to_owned().into_bytes()),
            (
                WarcHeader::ContentLength,
                body.len().to_string().into_bytes(),
            ),
        ]
        .into_iter()
        .collect(),
    };

    let mut file = WarcWriter::from_path_gzip(&operator, "warc_example.warc.gz")?;

    let bytes_written = file.write_raw(headers, &body)?;
    file.close()?;

    println!("{} bytes written.", bytes_written);

    Ok(())
}

use warc_rs::opendal::services::Fs;
use warc_rs::opendal::Operator;
use chrono::prelude::*;

use warc_rs::WarcHeader;
use warc_rs::{BufferedBody, RawRecordHeader, Record, RecordType, WarcWriter};

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
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

    let mut file = WarcWriter::from_path(&operator, "warc_example.warc").await?;

    let bytes_written = file.write_raw(headers, &body).await?;
    file.close().await?;

    println!("{} bytes written.", bytes_written);

    Ok(())
}

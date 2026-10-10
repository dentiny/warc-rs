use warc::opendal::services::Fs;
use warc::opendal::Operator;
use chrono::prelude::*;

use warc::{BufferedBody, Record, RecordType, WarcHeader, WarcWriter};

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    let operator = Operator::new(Fs::default().root("."))?;
    let date = Utc::now();
    let body = format!("wrote to the file on {}", date);
    let body = body.into_bytes();

    let mut headers = Record::<BufferedBody>::new();
    headers.set_warc_type(RecordType::WarcInfo);
    headers.set_date(date);
    headers
        .set_header(WarcHeader::IPAddress, "127.0.0.1")
        .expect("BUG: should be a valid IP address");
    let record = headers.add_body(body);

    let mut file = WarcWriter::from_path(&operator, "warc_example.warc").await?;

    let bytes_written = file.write(&record).await?;
    file.close().await?;

    println!("{} bytes written.", bytes_written);

    Ok(())
}

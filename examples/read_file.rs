use warc_rs::opendal::services::Fs;
use warc_rs::opendal::Operator;
use warc_rs::WarcHeader;
use warc_rs::WarcReader;

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    let operator = Operator::new(Fs::default().root("."))?;
    let file = WarcReader::from_path(&operator, "warc_example.warc").await?;

    let mut count = 0;
    let mut records = file.iter_records();
    while let Some(record) = records.next().await {
        count += 1;
        match record {
            Err(err) => println!("ERROR: {}\r\n", err),
            Ok(record) => {
                println!("{}: {}", WarcHeader::RecordID, record.warc_id(),);
                println!("{}: {}", WarcHeader::Date, record.date(),);
                println!();
            }
        }
    }

    println!("Total records: {}", count);

    Ok(())
}

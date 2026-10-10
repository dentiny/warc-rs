use warc::opendal::services::Fs;
use warc::opendal::Operator;
use warc::WarcHeader;
use warc::WarcReader;

fn main() -> Result<(), std::io::Error> {
    let runtime = tokio::runtime::Runtime::new()?;
    let _guard = runtime.enter();
    let operator = Operator::new(Fs::default().root("."))?;
    let file = WarcReader::from_path(&operator, "warc_example.warc")?;

    let mut count = 0;
    for record in file.iter_records() {
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

use std::{hint::black_box, time::Instant};
fn main() {
    let args: Vec<_> = std::env::args().collect();
    let iterations: usize = args.get(2).map(|s| s.parse().unwrap()).unwrap_or(10);
    let n = 32768;
    let input="WARC/1.0\r\nWARC-Type: resource\r\nWARC-Record-ID: <urn:uuid:00000000-0000-0000-0000-000000000000>\r\nWARC-Date: 2026-01-01T00:00:00Z\r\nContent-Length: 16\r\n\r\nbenchmark body!!\r\n\r\n".repeat(n);
    let start = Instant::now();
    for _ in 0..iterations {
        futures_executor::block_on(async {
            let mut reader =
                warc_rs::WarcReader::new(futures_util::io::Cursor::new(input.as_bytes()))
                    .iter_records();
            let mut rows = 0;
            while let Some(record) = reader.next().await {
                black_box(record.unwrap());
                rows += 1;
            }
            assert_eq!(rows, n);
        });
    }
    println!("{{\"case\":\"warc-native\",\"iterations\":{},\"input_bytes\":{},\"units\":{},\"seconds\":{}}}",iterations,input.len(),n*iterations,start.elapsed().as_secs_f64());
}

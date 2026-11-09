//! Run with cargo bench --no-default-features --bench headers.
use futures_util::io::{BufReader, Cursor};
use std::{hint::black_box, time::Instant};
use warc_rs::WarcReader;
fn main() {
    let input = "WARC/1.1\r\nWARC-Type: resource\r\nWARC-Record-ID: <urn:test:bench>\r\nWARC-Date: 2026-01-01T00:00:00Z\r\nContent-Length: 4\r\n\r\nbody\r\n\r\n".repeat(20000);
    for chunk in [1, 257, 65536] {
        for iteration in 0..8 {
            let start = Instant::now();
            futures_executor::block_on(async {
                let mut records = WarcReader::new(BufReader::with_capacity(
                    chunk,
                    Cursor::new(input.as_bytes()),
                ))
                .iter_records();
                let mut rows = 0;
                while let Some(record) = records.next().await {
                    black_box(record.unwrap());
                    rows += 1;
                }
                assert_eq!(rows, 20000);
            });
            if iteration > 0 {
                println!("chunk={chunk} seconds={:.6}", start.elapsed().as_secs_f64());
            }
        }
    }
}

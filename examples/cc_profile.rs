//! Temporary profiling harness for Common Crawl WARC reads.
//!
//! cc_profile <net|local> <mode> <duration_secs> <path>...
//! modes: download | gunzip | headers | records | records-default
use futures_util::io::{AsyncBufRead, AsyncRead, AsyncReadExt};
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use warc_rs::opendal::layers::{RetryLayer, TimeoutLayer};
use warc_rs::opendal::services::{Fs, S3};
use warc_rs::opendal::{FuturesAsyncReader, Operator};
use warc_rs::{GzipReader, WarcReader};

struct Counting<R> {
    inner: R,
    count: Arc<AtomicU64>,
}

impl<R: AsyncRead + Unpin> AsyncRead for Counting<R> {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let r = Pin::new(&mut this.inner).poll_read(cx, buf);
        if let Poll::Ready(Ok(n)) = &r {
            this.count.fetch_add(*n as u64, Ordering::Relaxed);
        }
        r
    }
}

impl<R: AsyncBufRead + Unpin> AsyncBufRead for Counting<R> {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        Pin::new(&mut self.get_mut().inner).poll_fill_buf(cx)
    }
    fn consume(self: Pin<&mut Self>, amt: usize) {
        let this = self.get_mut();
        this.count.fetch_add(amt as u64, Ordering::Relaxed);
        Pin::new(&mut this.inner).consume(amt)
    }
}

#[derive(Default)]
struct Stats {
    compressed: Arc<AtomicU64>,
    decompressed: AtomicU64,
    records: AtomicU64,
}

async fn open(op: &Operator, path: &str, chunked: bool) -> io::Result<FuturesAsyncReader> {
    let op = op
        .clone()
        .layer(TimeoutLayer::default())
        .layer(RetryLayer::default());
    let chunk_mb: usize = std::env::var("CHUNK_MB").ok().and_then(|v| v.parse().ok()).unwrap_or(2);
    let conc: usize = std::env::var("CONC").ok().and_then(|v| v.parse().ok()).unwrap_or(16);
    let reader = if chunked && chunk_mb > 0 {
        op.reader_with(path)
            .chunk(chunk_mb * 1_048_576)
            .concurrent(conc)
            .await?
    } else {
        op.reader(path).await?
    };
    Ok(reader.into_futures_async_read(..).await?)
}

async fn run_file(op: &Operator, path: &str, mode: &str, stats: &Stats, deadline: Instant) -> io::Result<bool> {
    let raw = open(op, path, mode != "records-default").await?;
    let mut counted = Counting { inner: raw, count: stats.compressed.clone() };
    let mut buf = vec![0u8; 1 << 16];
    match mode {
        "download" => loop {
            if Instant::now() > deadline {
                return Ok(false);
            }
            if counted.read(&mut buf).await? == 0 {
                return Ok(true);
            }
        },
        "gunzip" => {
            let mut gz = GzipReader::new(counted);
            loop {
                if Instant::now() > deadline {
                    return Ok(false);
                }
                let n = gz.read(&mut buf).await?;
                if n == 0 {
                    return Ok(true);
                }
                stats.decompressed.fetch_add(n as u64, Ordering::Relaxed);
            }
        }
        "headers" => {
            let mut reader = WarcReader::new(GzipReader::new(counted));
            let mut iter = reader.stream_records();
            while let Some(record) = iter.next_item().await {
                let record = record.map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{:?}", e)))?;
                stats.decompressed.fetch_add(record.content_length(), Ordering::Relaxed);
                stats.records.fetch_add(1, Ordering::Relaxed);
                if Instant::now() > deadline {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        _ => {
            let mut iter = WarcReader::new(GzipReader::new(counted)).iter_records();
            while let Some(record) = iter.next().await {
                let record = record.map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{:?}", e)))?;
                stats.decompressed.fetch_add(record.content_length(), Ordering::Relaxed);
                stats.records.fetch_add(1, Ordering::Relaxed);
                if Instant::now() > deadline {
                    return Ok(false);
                }
            }
            Ok(true)
        }
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    warc_rs::opendal::install_default();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (source, mode, secs, paths) = (&args[0], args[1].as_str(), args[2].parse::<u64>().unwrap(), &args[3..]);
    let op = if source == "net" {
        Operator::new(
            S3::default()
                .endpoint("https://data.commoncrawl.org")
                .bucket("crawl-data")
                .region("us-east-1")
                .skip_signature()
                .disable_config_load()
                .disable_ec2_metadata(),
        )?
    } else {
        Operator::new(Fs::default().root("/"))?
    };

    let stats = Arc::new(Stats::default());
    let start = Instant::now();
    let deadline = start + Duration::from_secs(secs);

    let reporter = {
        let stats = stats.clone();
        tokio::spawn(async move {
            let (mut last_c, mut last_d, mut last_r) = (0, 0, 0);
            loop {
                tokio::time::sleep(Duration::from_secs(10)).await;
                let c = stats.compressed.load(Ordering::Relaxed);
                let d = stats.decompressed.load(Ordering::Relaxed);
                let r = stats.records.load(Ordering::Relaxed);
                eprintln!(
                    "[{:>4}s] compressed {:>6.1} MB/s  decompressed {:>6.1} MB/s  records {:>6}/s",
                    start.elapsed().as_secs(),
                    (c - last_c) as f64 / 10e6,
                    (d - last_d) as f64 / 10e6,
                    (r - last_r) / 10
                );
                last_c = c;
                last_d = d;
                last_r = r;
            }
        })
    };

    let mut files = 0;
    let mut errors = 0;
    for path in paths {
        let path = path.strip_prefix("crawl-data/").unwrap_or(path);
        match run_file(&op, path, mode, &stats, deadline).await {
            Ok(true) => files += 1,
            Ok(false) => {}
            Err(e) => {
                let msg = format!("{:?}", e);
                let kind = if msg.contains("SlowDown") { "SlowDown(503)" } else { "other" };
                eprintln!("[{:>4}s] ERROR {} on {}; backing off 30s", start.elapsed().as_secs(), kind, path);
                errors += 1;
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
        }
        if Instant::now() > deadline {
            break;
        }
    }
    reporter.abort();

    let secs = start.elapsed().as_secs_f64();
    let c = stats.compressed.load(Ordering::Relaxed) as f64;
    let d = stats.decompressed.load(Ordering::Relaxed) as f64;
    let r = stats.records.load(Ordering::Relaxed);
    println!(
        "SUMMARY mode={} source={} secs={:.1} files_completed={} compressed={:.0}MB ({:.1} MB/s) body={:.0}MB ({:.1} MB/s) records={} ({:.0}/s) errors={}",
        mode, source, secs, files, c / 1e6, c / 1e6 / secs, d / 1e6, d / 1e6 / secs, r, r as f64 / secs, errors
    );
    Ok(())
}

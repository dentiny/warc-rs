# warc-rs

A Rust library for reading and writing WARC (Web ARChive) files.

## Hard fork

This repository is a hard fork of
[jedireza/warc](https://github.com/jedireza/warc), which has not been
maintained since November 2024. It is developed independently and does not
track or sync with upstream.

The fork starts from upstream commit
[`31235a2`](https://github.com/jedireza/warc/commit/31235a29c7e277577b23648428232326144ecf48)
("Upgrade nom dependency to 7.1.3"), the same source published to crates.io
as `warc` 0.4.0. The full upstream history is preserved. Changes after that
commit belong only to this fork.

Changes made here are not sent upstream, and fixes from upstream or its other
forks are not picked up automatically.

## Usage

```toml
[dependencies]
warc = { git = "https://github.com/dentiny/warc-rs" }
```

`WarcReader::new` and `WarcWriter::new` accept any `BufRead` or `Write`.
`from_path` and `from_path_gzip` open files through an
[OpenDAL](https://opendal.apache.org) `Operator` (re-exported as
`warc::opendal`) with its default timeout and retry layers. The memory, local
filesystem, and S3 services are enabled.

```rust
use warc::opendal::{services::Fs, Operator};
use warc::WarcReader;

let runtime = tokio::runtime::Runtime::new()?;
let _guard = runtime.enter();
let operator = Operator::new(Fs::default().root("/data"))?;
let reader = WarcReader::from_path_gzip(&operator, "crawl.warc.gz")?;
```

These constructors use OpenDAL's blocking API. Call them on a thread that has
entered a Tokio runtime but is not running async code, such as inside
`tokio::task::spawn_blocking` or after `Runtime::enter`. Writers must be
finished with `close()`; dropping one without closing may lose the file.

## License

MIT. See [LICENSE](LICENSE). The original copyright of the upstream project is
retained.

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
warc-rs = "0.3"
```

IO is async. `WarcReader::new` and `WarcWriter::new` accept any
`futures::io::AsyncBufRead` or `AsyncWrite`. `from_path` and `from_path_gzip`
open files through an [OpenDAL](https://opendal.apache.org) `Operator`
(re-exported as `warc_rs::opendal`) with its default timeout and retry layers. The
memory, local filesystem, and S3 services are enabled.

```rust
use warc_rs::opendal::{services::Fs, Operator};
use warc_rs::WarcReader;

let operator = Operator::new(Fs::default().root("/data"))?;
let reader = WarcReader::from_path_gzip(&operator, "crawl.warc.gz").await?;
let mut records = reader.iter_records();
while let Some(record) = records.next().await {
    let record = record?;
}
```

Writers must be finished with `close().await`; dropping one without closing may
lose the file.

`stream_records` lets callers skip record bodies. For uncompressed files opened
with `from_path`, or readers created with `WarcReader::new_seekable`, skipped
bodies of 32 MiB or more are seeked past instead of read, so they are not
downloaded. GZIP streams cannot seek and always read through.

`from_path` and `from_path_gzip` read each file as a single streaming request
and retry throttled requests with jittered backoff. Rate-limited sources such as
Common Crawl check every request, so one request per file is far more reliable
than many ranged chunks. For more throughput, read several files concurrently.

GZIP compression stays synchronous and in memory. The writer compresses each
record as its own GZIP member, as the WARC specification recommends and Common
Crawl does. `GzipReader` decompresses incrementally, so memory use stays bounded
whether a file uses one member per record or is compressed as a single member.

## License

MIT. See [LICENSE](LICENSE). The original copyright of the upstream project is
retained.

## Cargo features

| Feature | Enabled by default | API |
| --- | --- | --- |
| `default` | Yes | Enables `gzip` and `storage`. |
| `storage` | Via `default` | OpenDAL dependency/re-export and reader/writer `from_path` constructors. |
| `gzip` | Via `default` | Streaming GZIP decoding and GZIP writers; path constructors additionally require `storage`. |
| `with_serde` | No | Serde serialization/deserialization for `WarcHeader`. |

Disable default features to use `WarcReader::new` and `WarcWriter::new` with
caller-owned asynchronous streams, without OpenDAL or GZIP dependencies:

```toml
warc-rs = { version = "0.3", default-features = false }
```

Default builds keep their existing behavior. Previously, even
`default-features = false` still enabled OpenDAL. Callers using path constructors
with defaults disabled must now explicitly enable `storage`:

```toml
warc-rs = { version = "0.3", default-features = false, features = ["storage"] }
```

This changes the API/dependency set for existing no-default storage callers,
which is why the feature split is released as 0.3.0 rather than a 0.2.x patch.

GZIP without OpenDAL:

```toml
warc-rs = { version = "0.3", default-features = false, features = ["gzip"] }
```

Parser and Serde support without storage or GZIP:

```toml
warc-rs = { version = "0.3", default-features = false, features = ["with_serde"] }
```

Omitting `default-features = false` retains both default features; additional
features are additive. The optional-storage feature split is available starting
in version 0.3.0.

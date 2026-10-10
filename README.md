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

## License

MIT. See [LICENSE](LICENSE). The original copyright of the upstream project is
retained.

# Conversion performance

The `conversion` benchmark performs 32,768 records per scan using buffered WARC resource records with four required headers and a 16-byte body. Fixture creation is outside the timed region. Arrow batches are flushed every 1,024 rows where applicable; output is consumed through `black_box`.

```sh
cargo bench --no-default-features --bench conversion -- ignored 3
```

The positional arguments are an unused case label and the scan iteration count. Build once with `--no-run` and run the printed executable directly to exclude Cargo startup. The executable prints JSON with elapsed time, iterations, input size and record count. This is a deterministic allocation-sensitive workload, not a representative corpus or a CI performance threshold.

For a controlled comparison against `00af372`, copy only the benchmark file and its `[[bench]]` manifest entry onto that base; leave production sources unchanged. Build release executables for both trees with the same toolchain and lockfile. Warm both executables, alternate baseline/candidate processes, and compare elapsed seconds per scan.

Measured evidence and independent optimization comparisons are linked from the open-arrow-access benchmark report.

On the local Apple Silicon release build (Rust 1.97.0), ten alternating measured processes per variant gave median scan times **16.205 → 14.851 ms**, or **1.09× throughput** (bootstrap 95% interval 1.079–1.107×). Warm-up processes were excluded.

Raw samples and the comparison script are included in [open-arrow-access PR #168](https://github.com/dentiny/open-arrow-access/pull/168), under `benchmarks/results/2026-11-09-main32e6805/`.

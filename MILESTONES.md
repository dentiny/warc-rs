# WARC DataFusion milestones

This checkout contains milestones M1 through M1.

Each milestone is one reviewable PR, based on the previous milestone. Submit
the next PR after its dependency is merged, or use explicit stacked PR bases.
Do not submit the complete implementation as one PR. Existing benchmark work
is independent and must not be included in this series.

| PR | Deliverable | Depends on | Acceptance |
| --- | --- | --- | --- |
| M1 | Single-file streaming IO | baseline | Local and memory inputs reopen correctly; describing a missing file creates no directories. |
| M2 | Projection-aware Arrow batches | M1 | Zero-column row counts, reordered projections, truncation, byte bounds and stream drop pass. |
| M3 | DataFusion table provider | M2 | COUNT, filters, aggregation, repeat scans, gzip, empty inputs and lazy EXPLAIN pass. |
| M4 | SQL function and bounded partitions | M3 | SQL argument validation passes; two files expose two partitions; LIMIT avoids later inputs. |
| M5 | HTTP and configured S3 | M4 | Encoded paths, a temporary 503 retry, configured S3 GET and incremental gzip pass. |
| M6 | Common Crawl discovery | M5 | Segment prefixes, deduplication, malformed manifests and traversal rejection pass. |
| M7 | Indexed range scans | M6 | Sorted ranges, mirrored filenames, overlaps, overflow, gzip member boundaries and S3 ranges pass. |

## Scope boundaries

### M1: Single-file streaming IO

Add a workspace and warc-io for reopenable local/file-URL and configured-operator inputs.

Acceptance: Local and memory inputs reopen correctly; describing a missing file creates no directories.

### M2: Projection-aware Arrow batches

Add warc-arrow with a fixed schema, projected builders and per-record/batch bounds.

Acceptance: Zero-column row counts, reordered projections, truncation, byte bounds and stream drop pass.

### M3: DataFusion table provider

Add Rust table registration and a single scan partition; keep filtering in DataFusion.

Acceptance: COUNT, filters, aggregation, repeat scans, gzip, empty inputs and lazy EXPLAIN pass.

### M4: SQL function and bounded partitions

Add read_warc(...), a CLI example and file grouping capped by session/scan settings.

Acceptance: SQL argument validation passes; two files expose two partitions; LIMIT avoids later inputs.

### M5: HTTP and configured S3

Add public HTTP URI resolution and verify remote storage using deterministic local fixtures.

Acceptance: Encoded paths, a temporary 503 retry, configured S3 GET and incremental gzip pass.

### M6: Common Crawl discovery

Add explicit segment/crawl discovery from bounded gzip manifests before table registration.

Acceptance: Segment prefixes, deduplication, malformed manifests and traversal rejection pass.

### M7: Indexed range scans

Add streaming byte ranges and JSONL/CDXJ index parsing with validation.

Acceptance: Sorted ranges, mirrored filenames, overlaps, overflow, gzip member boundaries and S3 ranges pass.

## Validation

At every milestone, test the parser and every workspace member available at that
stage. Format and lint only the new member crates, so existing unrelated
benchmark edits remain independent. Add CI entries alongside each new crate.

M1-M2 introduce no DataFusion dependency. M3 introduces the Rust provider
without SQL function registration or parallel scanning. M4 adds those query
entry points. Remote backends, discovery and indexes each have their own PR.

The previous complete implementation is backed up outside this checkout.
Numbered patches represent this sequence; each patch is applied once, in order,
starting at the recorded baseline commit. Cargo.lock is ignored by this library
repository and is resolved locally rather than included in the patches.

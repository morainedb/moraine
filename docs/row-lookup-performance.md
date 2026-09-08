# Selective row lookup

`locate_row_ids` now queries verified dense file intervals and checks only the
requested inline IDs. `recent_row` selects the requested offsets before loading
bodies or constructing row objects. Position validation uses the same inline
selection, and resolving explicitly named files no longer copies all file
metadata.

## What is cached

A file directory indexes dense ranges established by actual file summaries.
Recorded `row_id_start` alone is insufficient: files with an embedded row-ID
column can contain IDs outside that range. Arbitrary-ID and unreadable files
remain on the conservative probe path; failures continue to produce every
requested ID as a candidate for that file and are retried on later calls.

A warm dense-file lookup costs an interval query plus the matching outputs,
without visiting unrelated file summaries. Arbitrary-file work still scales
with the number of arbitrary files. File directories require the same shared
file map, data-store identity, and resolved path prefixes. Changed file metadata
rebuilds the directory; unrelated catalog commits can reuse it.

An inline directory indexes chunk ranges, including overlapping versions.
Only requested offsets are materialized, only those IDs' tombstones are scanned,
and only selected live chunks and their schemas are fetched. The directory
requires the read session's full head stamp, including maintenance batch sequence.
An UPDATE's tombstone ends earlier versions without killing its same-snapshot
replacement. Full scans and time-travel scans retain their existing behavior.

Manifest-following read-only handles validate the full head stamp before and
after a lookup, including body/schema reads and failures. A moving manifest
retries from its new head. The consistency hardening following these benchmark
runs uses the common eight-attempt budget and returns `RetryBudgetExhausted`
under sustained changes, with no unguarded fallback. A stable reader can cache
its directory just like a writer. The measurements below predate that hardening.

Each handle caches at most 64 table directories per source kind, with arbitrary
entry eviction on admission at capacity. Directory memory follows file/chunk
counts and is included in `projection_bytes`; shared file metadata can be counted
there again beside the materialized view. The directory does not pin arbitrary
row-ID summaries outside their existing bounded auxiliary cache.

## Cold-read limits

Cold lookup is not independent of source count. The first file lookup verifies
all current files and builds the interval directory. The first inline lookup
checks directory completeness against a chunk scan, including bodies; later
rebuilds use body-free locators once completeness is established. An incomplete
persistent directory remains correct through the chunk-scan fallback. These
passes retain source metadata rather than expanding every inline row.

After eviction, file changes, or a new inline head, directory construction is
paid again. This change introduces no persistent index or store-format change.

## Reproduce

```bash
CARGO_INCREMENTAL=0 cargo build -p moraine --example row_lookup_bench --release --locked
# mode: files, inline membership, or recent_row; then source count
target/release/examples/row_lookup_bench files 1024
target/release/examples/row_lookup_bench inline 1024
target/release/examples/row_lookup_bench recent 1024
```

Run each mode at 16, 128, and 1,024 sources in a separate process, with no other
benchmarks or builds running. Each source contains 128 rows. Every lookup requests
one ID in the middle source. The fixture uses real SlateDB and in-memory object
stores; files are valid dense Parquet, while inline chunks carry 1,024 opaque
body bytes (these APIs return bytes without decoding Arrow).

Fixture preparation is excluded. Closing and reopening the catalog before the
first lookup makes the catalog/directory caches cold; a fresh `DataStore` identity
also isolates file caches. The cold sample includes catalog materialization.
The warm result averages 32 subsequent calls on the same handles. Process CPU,
wall-clock latency, allocation count/bytes (`stats_alloc`), and physical data-store
GETs are measured separately. Allocations include background store activity.
There are no measured commits or WAL waits. This is an in-memory workload, so
cold remote-store latency will depend on network service times.

Collected September 8, 2026 on the Linux x86-64 workspace, Rust 1.96.1 and
SlateDB 0.15.0. The baseline is commit `f49a4d0` with the same harness. These are
single cold samples and warm sample means, not statistical latency guarantees.
Raw data: [before](row-lookup-before.csv), [after](row-lookup-after.csv).

## Warm results

All times below are microseconds per one-hit lookup.

| Lookup | Sources | CPU before → after | Latency before → after | Allocations before → after |
| --- | ---: | ---: | ---: | ---: |
| files | 16 | 136.6 → 33.6 | 136.6 → 33.6 | 825.8 → 211.2 |
| files | 128 | 652.1 → 35.4 | 699.1 → 35.3 | 4,077.1 → 211.2 |
| files | 1024 | 4,885.6 → 37.7 | 4,885.8 → 37.6 | 30,063.0 → 211.2 |
| inline | 16 | 207.1 → 47.1 | 207.0 → 47.1 | 764.3 → 299.4 |
| inline | 128 | 1,410.4 → 48.3 | 1,410.3 → 48.2 | 3,176.6 → 299.4 |
| inline | 1024 | 12,950.1 → 65.2 | 12,950.0 → 65.1 | 22,004.0 → 299.6 |
| recent | 16 | 790.1 → 103.8 | 791.1 → 103.8 | 2,675.5 → 561.3 |
| recent | 128 | 5,612.8 → 91.3 | 5,612.7 → 91.2 | 17,215.6 → 511.6 |
| recent | 1024 | 47,661.3 → 98.4 | 47,668.7 → 98.3 | 144,107.7 → 533.6 |

At 1,024 sources, warm allocation traffic falls from 5.0 MB to 24 KB for
file location, 24.1 MB to 38 KB for inline membership, and 48.6 MB to 71 KB for
`recent_row`. All warm file cases issue zero physical data-store GETs both
before and after; the improvement removes CPU and allocation work on cache hits.

## Cold results

| Lookup | Sources | CPU before → after, µs | Latency before → after, µs | Data GETs before → after |
| --- | ---: | ---: | ---: | ---: |
| files | 16 | 907.2 → 1,830.3 | 882.2 → 1,804.3 | 16.0 → 16.0 |
| files | 128 | 3,186.4 → 4,045.0 | 3,164.2 → 4,011.9 | 128.0 → 128.0 |
| files | 1024 | 18,445.3 → 20,603.2 | 18,434.2 → 20,603.2 | 1,024.0 → 1,024.0 |
| inline | 16 | 902.3 → 1,346.4 | 855.8 → 1,311.0 | 0.0 → 0.0 |
| inline | 128 | 2,786.8 → 1,536.8 | 2,744.1 → 1,507.6 | 0.0 → 0.0 |
| inline | 1024 | 18,447.9 → 8,603.8 | 18,383.3 → 8,558.4 | 0.0 → 0.0 |
| recent | 16 | 836.1 → 542.9 | 799.1 → 522.0 | 0.0 → 0.0 |
| recent | 128 | 4,101.3 → 1,173.4 | 4,068.9 → 1,156.0 | 0.0 → 0.0 |
| recent | 1024 | 30,807.7 → 6,589.7 | 30,748.4 → 6,570.1 | 0.0 → 0.0 |

Cold file lookups still issue one footer read per dense file. Building a directory
adds overhead, visible especially in the smallest file case; at 1,024 files,
measured cold CPU and latency increased by about 12%. Larger cold
inline cases improve by avoiding construction of every row, but still scale with
chunk count. Warm `recent_row` allocations vary somewhat with store/cache activity;
the source-count growth from the baseline is removed.

## Read-only handles

The same harness accepts `reader` as a third argument:

```bash
target/release/examples/row_lookup_bench recent 1024 reader
```

These additional post-change runs use manifest-following read-only handles,
with the full head-stamp guard enabled. All cold and warm results are retained
in [the read-only CSV](row-lookup-read-only.csv). Warm one-hit means:

| Lookup | Sources | CPU, µs | Latency, µs | Allocations |
| --- | ---: | ---: | ---: | ---: |
| files | 16 | 74.2 | 74.2 | 491.0 |
| files | 128 | 83.6 | 84.9 | 491.0 |
| files | 1024 | 100.3 | 100.3 | 491.0 |
| inline | 16 | 92.3 | 92.3 | 583.0 |
| inline | 128 | 96.5 | 96.5 | 577.0 |
| inline | 1024 | 89.3 | 89.3 | 577.0 |
| recent | 16 | 109.2 | 109.1 | 597.0 |
| recent | 128 | 97.5 | 97.5 | 589.0 |
| recent | 1024 | 109.5 | 109.4 | 611.0 |

## Validation

The selective-body regression failed first: the old `recent_row` fetched an
unrequested live chunk. Real-store tests also cover overlapping inline updates,
wider chunks after warming, tombstones, file replacement, request order and
deduplication, switching data-store identities, and a manifest reader retrying a chunk
removed by a maintenance batch without changing the snapshot ID. Interval lookup has exhaustive
membership property tests and explicit nested-range/domain-boundary coverage.

The final local gate passed: pinned-nightly formatting, workspace Clippy,
workspace tests (including 700 core and 208 integration tests), warning-free
rustdoc, dependency/advisory and pin checks, and end-to-end validation with
154 DuckLake tests and 173 SQL assertions. All 18 isolated post-change benchmark
scenarios (nine writer and nine reader) verified their lookup results.

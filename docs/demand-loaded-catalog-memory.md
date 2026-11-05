# Demand-loaded catalog memory measurement

A fresh DuckDB process scanned two million file-column statistics records with
80.9 MiB peak RSS, a 16 MiB Moraine working-memory allowance, and a 16 MiB Foyer
budget. Increasing the catalog from 20,000 to two million statistics records
increased the observed scan peak by 6.4 MiB. These are whole-process measurements,
including DuckDB, Rust runtime, SlateDB, preloading, and the query.

Measured on 2026-11-04 in the Linux x86_64 Conductor workspace, using this change's
release extension and DuckDB CLI. One local-filesystem catalog contains one table,
20 VARCHAR columns, and the stated number of registered files. Each file has 20
statistics records with 96-character minima and 97-character maxima. The fixture
contains metadata only; this query reads no Parquet files.

| Registered files | Statistics records | Attach peak RSS (MiB) | Scan peak RSS (MiB) | Attach seconds | Scan seconds |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000 | 20,000 | 72.0 | 74.5 | 0.121 | 0.167 |
| 10,000 | 200,000 | 67.2 | 73.9 | 0.079 | 0.798 |
| 100,000 | 2,000,000 | 71.0 | 80.9 | 0.080 | 7.759 |

Each cell is one fresh process. Times include process startup, extension loading,
attach, query, and shutdown. The query was `SELECT count(*) FROM
m.main.ducklake_file_column_stats`; the attach case substituted `SELECT 1`.
DuckDB used two threads. Default L0 preloading remained enabled. All six processes
exited successfully. OS filesystem caches were uncontrolled and had been warmed
by fixture creation and previous experiments; these are not cold-storage or S3
latency measurements. The small RSS differences between adjacent runs include
allocator and scheduling variation.

## What the experiment caught

Two surrounding operations defeated an otherwise bounded metadata cursor:

- The published checkpoint could still refer to a large WAL after the writer
  flushed those rows to SSTs during close. A new fixed reader then replayed that
  old WAL. Clean writer close now publishes the flushed manifest again.
- Default background preloading materialized the entire catalog to discover
  tables, then materialized it again to warm each table. Discovery now retains
  at most one page of table IDs; warming streams only that table's index
  definitions. Explicit `CACHE_PRELOAD 'all'` also streams and discards raw rows.

The fixtures used here had cleanly closed writers and flushed publications.
Active writers and overlapping reader generations can retain additional WAL
memtables; their worst-case memory and remote publication costs still need
measurement. This result does not establish bounds for staged metadata overlays,
index backfills, schema/name lookup, arbitrary SQL result sets, or explicit
`Catalog::snapshot()` materialization. It is not a before/after comparison with
`origin/main`.

## Reproduction

Build the CLI and extension with `cargo xtask build-extension`. Create each fixture
in a fresh directory, changing the file count to 1,000, 10,000, or 100,000:

```sh
MORAINE_MEMORY_FIXTURE="$PWD/.context/catalog-memory-100000" \
MORAINE_MEMORY_FILES=100000 \
cargo test -p moraine --lib write_memory_measurement_fixture -- --ignored --nocapture
```

Run each query in a fresh child process. Set `MORAINE_WORKING_MEMORY_BYTES=16777216`
in its environment and execute:

```sql
SET threads=2;
LOAD 'build/release/extension/moraine/moraine.duckdb_extension';
ATTACH 'moraine:.context/catalog-memory-100000' AS m
  (READ_ONLY, CACHE_MEMORY 16777216);
SELECT count(*) FROM m.main.ducklake_file_column_stats;
```

The measurements above used Python's `os.fork`, `os.execv`, and `os.wait4` to run
`build/release/duckdb -unsigned -csv -c <SQL>`. Record `ru_maxrss` from the child's
`wait4` result (KiB on Linux), elapsed `time.monotonic()`, exit status, and output.
Measure fixture creation separately. For attach-only runs, replace the last SQL
statement with `SELECT 1`.

Two ignored regression tests exercise the preloading paths against the fixture
under the same small working-memory allowance:

```sh
MORAINE_WORKING_MEMORY_BYTES=16777216 \
MORAINE_MEMORY_FIXTURE="$PWD/.context/catalog-memory-100000" \
cargo test -p moraine-duckdb --lib preload_streams_a_persisted_catalog -- --ignored

MORAINE_WORKING_MEMORY_BYTES=16777216 \
MORAINE_MEMORY_FIXTURE="$PWD/.context/catalog-memory-100000" \
cargo test -p moraine --lib deep_preload_uses_bounded_working_memory -- --ignored
```

## Active-writer reader generations

A core-process measurement on 2026-11-05 used local object storage, a 16 MiB
Foyer memory cache, and a 32 MiB shared working allowance. Each writer commit
added 384 statistics records with 2 KiB values (768 KiB of value text). The writer
remained open; its published checkpoints included WAL-durable metadata. These
are debug-build core measurements, not DuckDB RSS or remote-storage latency.

Holding five generations simultaneously reserved 24.7 MiB for replay. The sixth
reader was refused without increasing retained reservations or invalidating the
older readers. RSS grew from 38.6 MiB at baseline to 61.5 MiB with five readers;
the failed open peaked at 66.1 MiB. After releasing the readers and closing the
writer, which flushes SSTs, a fresh reader reserved about 2 KiB. RSS remained
67.0 MiB in that same process: allocator retention, writer buffers, and cache
memory do not disappear when replay reservations are released. This is an
admission bound, not a hard RSS cap.

A second measurement opened and released one generation at a time. It exposed
an unnecessary overlap: the cached previous reader and its cleanup task could
retain replay bytes while the replacement opened. At generation nine, retained
working memory was 29.7 MiB and generation ten failed. The reader lifecycle now
retires an unused cached generation and waits for its replay memory to release
before opening its replacement. With that fix, generation nine reserved
14.8 MiB and all twelve generations succeeded. Generation twelve reserved
19.8 MiB, used 65.3 MiB RSS, and opened in 18 ms. Active older scopes still keep
their reservations and remain subject to the shared admission limit.

Reproduce in a fresh directory for each run:

```sh
MORAINE_WORKING_MEMORY_BYTES=33554432 \
MORAINE_MEMORY_FIXTURE="$PWD/.context/active-writer-new" \
cargo test -p moraine --lib measure_active_writer_reader_generations \
  --locked -- --ignored --nocapture

MORAINE_WORKING_MEMORY_BYTES=33554432 \
MORAINE_MEMORY_FIXTURE="$PWD/.context/sequential-reader-new" \
cargo test -p moraine --lib unused_reader_generation_releases_replay_before_rotating \
  --locked -- --ignored --nocapture
```

These measurements preserve WAL-fresh publication. There is no forced SST flush
per commit. WAL replay remains non-evictable inside SlateDB; SST-backed reads use
Foyer. The adapter admits retained replay after one upstream batch is decoded,
so that transient batch and native allocator overhead remain outside the bound.

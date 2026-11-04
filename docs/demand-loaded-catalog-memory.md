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

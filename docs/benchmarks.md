# Benchmarks

Moraine replaces DuckLake's metadata catalog. These harnesses measure
key catalog speeds: commit, catalog open, and what a statement reads
(outside of Parquet).

Unless stated otherwise, measurements are from a release build on an 8-vCPU
x86-64 Linux VM against the in-memory object store.

- A commit is durable once its write-ahead-log object lands. The writer paces
  flushes at `META_FLUSH_INTERVAL_MS` (default 100 ms). A writer with no
  concurrent commits flushes after a tenth of that, so a lone commit costs about
  11 ms. Concurrent commits share one flush.
- Against a real endpoint the PUT round trip is the floor.

## Running the harnesses

| question | command | needs |
|---|---|---|
| moraine against other DuckLake catalogs | `cargo xtask bench` | Postgres on `PATH` or `MORAINE_BENCH_POSTGRES`, else that backend is skipped |
| read path on a published scale | `cargo xtask clickbench run --clickbench <checkout> --machine <type>` | a ClickBench checkout, 40 GB of disk, passwordless `sudo` |
| where a commit's time goes | `cargo xtask commit-bench` | an S3 endpoint in `MORAINE_S3_BUCKET` |
| what a cold reader pays | `cargo xtask reader-bench` | an S3 endpoint for round-trip counts |
| located lookups | `cargo xtask locate-bench` | nothing |
| indexed scan strategies | `cargo xtask filter-bench <extension> <results-directory>` | a built extension |
| the core against a real endpoint | `cargo xtask s3` | nothing; it starts a pinned RustFS |
| the core in isolation | `cargo test -p moraine --test it --release -- --ignored --nocapture measure_` | nothing |
| the two needing crate-internal seams | `cargo test -p moraine --lib --release -- --ignored --nocapture measure_` | nothing |
| <!-- site:omit -->allocations on the commit path | `cargo test -p moraine --lib --release -- --exact --ignored --test-threads=1 --nocapture transaction::commit::tests::allocation::<name>` | nothing<!-- /site:omit --> |
| peak heap of a staged index build | `cargo test -p moraine --test staged_build_memory --release -- --ignored --nocapture` | nothing |
| a commit's conflict check against retained snapshots | `cargo test -p moraine-duckdb --test ducklake_load -- --include-ignored --nocapture measure_staged_conflict_check_by_retained_snapshots` | a built extension, as `cargo xtask e2e` sets up |
| the core against AWS S3 | the `Real S3 benchmark` workflow | the AWS stack in [CONTRIBUTING.md](../CONTRIBUTING.md#the-real-s3-benchmark) |

The core harnesses use the in-memory object store, so their durable cost is
flush wait plus compute; a real store adds its PUT round trip.

## Commits

### Where the time goes

`cargo xtask commit-bench` splits an S3-backed one-row `UPDATE` into
DuckLake's metadata SQL, moraine's commit work, the durable wait, and the
object-store requests. Parquet stays local, so every request counted is
metadata.

```text
MORAINE_S3_BUCKET=... MORAINE_S3_PREFIX=... AWS_REGION=... \
  cargo xtask commit-bench [--files 16,128] [--commits 7] [--flush-ms 25]
```

Loopback RustFS, same VM<!-- site:omit -->, 2026-10-20<!-- /site:omit -->,
medians of seven commits:

| files | commit | DuckLake metadata | moraine core | durable wait | requests |
|---:|---:|---:|---:|---:|---|
| 16 | 23.0 ms | 13.4 ms | 3.3 ms | 3.0 ms | 1 PUT, 2.7 ms |
| 128 | 25.0 ms | 14.1 ms | 3.4 ms | 3.0 ms | 1 PUT, 2.9 ms |

- DuckLake's eight metadata statements are about 60% of the commit. The cost
  does not grow with the file count.
- Two statements are 69% of that: the per-column file-list query and the
  snapshot insert.
- A local-filesystem store is within 0.2 ms of RustFS, and the task records zero
  GETs per commit. DuckDB spends the time parsing, binding, and optimising about
  a kilobyte of generated SQL. Preparing the statement saves the parse, because
  DuckDB re-binds prepared statements that read a catalog.
- A CPU profile puts Moraine's own code and its SlateDB reads under 4% of the
  commit.

### Flush interval

`measure_commit_latency_by_flush_interval`, 60 sequential commits:

| flush interval | median commit |
|---|---|
| 1 ms | 0.6 ms |
| 10 ms | 1.7 ms |
| 100 ms (default) | 11.4 ms |
| 250 ms | 26.2 ms |

A lone writer waits a tenth of the interval plus about 0.5 ms of compute.
Before the pacing floor existed it waited the whole interval (103 ms at
the default on an Apple M2).

### Write round trip

`measure_commit_latency_by_write_rtt` delays every PUT and sweeps that
delay against the flush interval. 30 commits per cell, median:

| PUT round trip | flush 1 ms | flush 25 ms | flush 100 ms |
|---|---|---|---|
| 0 ms | 0.5 ms | 3.8 ms | 11.3 ms |
| 5 ms | 19.6 ms | 19.5 ms | 19.5 ms |
| 25 ms | 80.0 ms | 79.9 ms | 80.0 ms |
| 100 ms | 305 ms | 305 ms | 305 ms |

<!-- site:omit -->
Once the round trip exceeds the pacing floor, the interval no longer
matters and a commit costs about three round trips. Before the SlateDB 0.18
vendoring it cost one.

### Real endpoints

`measure_commit_latency_against_the_endpoint` (`tests/object_storage.rs`)
runs the same sweep against a real endpoint. `cargo xtask s3` runs it
against loopback RustFS, where a PUT costs about 1 ms. The `Real S3
benchmark` workflow runs it from an ARM CodeBuild worker against
same-region AWS S3. Twenty commits per interval, 2026-08-09, before the
pacing floor:

| flush interval | median | min | max |
|---|---|---|---|
| 1 ms | 29.6 ms | 24.0 ms | 40.6 ms |
| 25 ms | 28.1 ms | 22.2 ms | 35.7 ms |
| 100 ms | 101.9 ms | 73.9 ms | 143.4 ms |

The S3 floor was one round trip, about 30 ms. Going below a 25 ms
interval gained nothing.
<!-- /site:omit -->

### Concurrency

`measure_commit_throughput_by_concurrency`: K callers share 128 commits,
each appending to its own table, at the default interval. Batch size is
the WAL objects written.

| concurrency | wall | commits/s | WAL writes | mean batch |
|---|---|---|---|---|
| 1 | 1.42 s | 90 | 128 | 1.0 |
| 4 | 354 ms | 361 | 32 | 4.0 |
| 16 | 87 ms | 1,465 | 8 | 16.0 |
| 64 | 25 ms | 5,212 | 2 | 64.0 |
| 128 | 23 ms | 5,575 | 2 | 64.0 |

Batch size equals concurrency up to the 64-member ceiling, and throughput
scales linearly with K in this run.

<!-- site:omit -->
### Allocations

The tests in `transaction::commit::tests::allocation` count allocator
calls. Run each alone with `--test-threads=1`; compare scaling, not
milliseconds.

| test | measures |
|---|---|
| `snapshot_refresh` | one-change refresh against full rematerialization, 16 to 1,024 tables |
| `warm_reads` | a retained snapshot read 2,000 times; currently fails |
| `index_lookups` | point hits, misses, and ranges over a reopened unique index |
| `index_maintenance` | appends of 1, 64, and 1,024 entries into indexes of 1,024 to 65,536 |
| `index_maintenance_input_order` | 4,096-entry appends, ascending, descending, scattered |
| `refresh_count_does_not_allocate` | the refresh heuristic's entity count allocates nothing |
| `index_maintenance_profile` | twenty 1,024-entry appends, shaped for Valgrind DHAT |
| `verb_commit_scaling` | statistics updates while unrelated tables grow, alone and in groups of four |
| `row_lookups` | one-hit file, inline, and `recent_row` lookups by source count |

- A statistics update costs 1,764 allocations and 0.55 to 0.72 ms with 0
  or 4,096 unrelated tables. In a group of four: 981 allocations and
  0.25 to 0.31 ms per update.
- A 1,024-entry index append costs 70,146 allocations (67 per entry),
  10.8 MB, and 11.5 ms, the same into an index of 1,024 or 65,536
  entries. Input order makes no difference. A dense append previously
  cost 13 per entry.
- A warm point lookup costs 340 allocations and 47 µs; a miss 277 and
  39 µs; a 32-row range 1,054 and 161 µs. All issue zero GETs at any
  index size.
- A one-change refresh allocates 192 to 198 times at any catalog size
  (18 KB at 16 tables, 43 KB at 1,024). A rematerialization at 1,024
  tables allocates 340,133 times and 38 MB. Refresh time still grows
  with the catalog (51 µs to 5.0 ms) because advancing a shared view
  copies it.

For a heap profile, build the test executable with `--no-run`, run
`index_maintenance_profile` under `valgrind --tool=dhat`, and filter
stacks by `profile_index_append`.
<!-- /site:omit -->

### Reclaim

`measure_reclaim_by_batch_size`, 50,000 dead index entries:

| batch | commits | median sweep | entries/s |
|---|---|---|---|
| 256 | 196 | 218 ms | 229,744 |
| 4,096 | 13 | 260 ms | 192,238 |
| 65,536 | 1 | 265 ms | 188,923 |

The sweep is compute-bound; batch size does not matter. Before the pacing
floor each batch waited a full flush and the 256 case took 20 s.

## Against other DuckLake catalogs

`cargo xtask bench` runs identical workloads against moraine, a DuckDB
file catalog, and Postgres through the same pinned DuckDB CLI. Data is
local Parquet everywhere; only the catalog differs.

```text
cargo xtask bench [--backends moraine,duckdb,postgres] [--workloads <name>,...]
                  [--scale small|medium|large] [--repeat N]
                  [--concurrency 1,2,4,8] [--commits N] [--indexed]
                  [--flush-interval-ms N]
```

Defaults: all backends, all workloads, `small`, three repeats. Postgres
is provisioned from binaries on `PATH` or under
`/opt/homebrew/opt/postgresql@*`; `MORAINE_BENCH_POSTGRES` names an
existing server. Output is a table of `median (min…max)` per phase with a
ratio against moraine, also written to `target/bench/report.json`.

| workload | measures |
|---|---|
| `bulk_load` | `CREATE TABLE` plus one large `INSERT` |
| `small_commits` | K single-row inserts; the headline latency number |
| `many_tables` | T × `CREATE TABLE` |
| `scan` | full and filtered scans, time travel, snapshot listing |
| `maintenance` | `merge_adjacent_files`, `expire_snapshots`, `cleanup_old_files` |
| `deletes_repeated` | D single-key deletes, one commit each |
| `deletes_bulk` | the same rows in one statement |
| `deletes_covering` | a partition-aligned delete that drops the file |
| `deletes_partial` | the same, one row short of covering |
| `deletes_indexed_*` | `repeated` and `covering` with an equality index |
| `concurrent_processes` | N writer processes |
| `concurrent_connections` | N connections over one instance |

Each delete workload gets a lake with one table, because delete cost
rises with the table count. Compare `repeated` with `bulk` (what N
commits cost over one) and `covering` with `partial` (what dropping the
file saves); do not compare across pairs. The `indexed` twins add one
`CREATE INDEX`; the other backends have no equality index and skip them.
`ATTACH` is a timed phase.

### Single writer

`bulk_load` and `small_commits` at the `small` scale, loopback Postgres 16,
three repeats<!-- site:omit -->, 2026-11-11 at commit
`dd35dce`<!-- /site:omit -->:

| phase | moraine | duckdb | postgres |
|---|---:|---:|---:|
| attach | 8 ms | 19 ms | 113 ms |
| `CREATE TABLE` | 53 ms | 57 ms | 75 ms |
| 100,000-row `INSERT` | 41 ms | 45 ms | 49 ms |
| 20 single-row inserts | 254 ms | 218 ms | 135 ms |

Serial commits pay moraine's flush each, at the 1 ms interval the latency
workloads pin; a loopback Postgres commits in about 7 ms and a DuckDB file
in about 11 ms.

### Concurrent writers

`--commits` single-row inserts are spread over N writers, released
together on a barrier. `concurrent_connections` uses
`xtask/cpp/concurrent_writers.c`, because the CLI holds one connection.
128 commits, three repeats.

Connections over one instance<!-- site:omit -->, 2026-11-11 at commit
`dd35dce`<!-- /site:omit -->:

| writers | moraine | duckdb | postgres |
|---|---|---|---|
| 1 | 1.47 s | 1.08 s | 665 ms |
| 4 | 597 ms | 1.78 s | 953 ms |
| 8 | 417 ms | 1.76 s | 1.77 s |
| 16 | 461 ms | 2.85 s | 2.94 s |

Within one process, moraine is the only catalog that gets faster with
writers, and the fastest from four up. Across processes, both embedded
catalogs are single-writer: moraine fences the previous writer with a typed
error, and the DuckDB file catalog fails on the file lock. Postgres accepts
more writers and gets slower.

<!-- site:omit -->
Writer processes, 2026-08, before the pacing floor, not re-run:

| writers | moraine | duckdb | postgres |
|---|---|---|---|
| 1 | 12.90 s | 1.30 s | 819 ms |
| 2 | refused | refused | 853 ms |
| 4 | refused | refused | 996 ms |
| 8 | refused | refused | 1.29 s |

At 512 commits (2026-08), DuckDB-file exhausts DuckLake's retry budget at
16 writers. Postgres slows with every doubling. Moraine: 3.34 s at 16,
1.92 s at 32, 2.10 s at 64.
<!-- /site:omit -->

These runs use local storage and report median, minimum, and maximum
latency. They do not run in CI.

## Reads

<!-- site:omit -->
### ClickBench

`benchmark/clickbench` is a [ClickBench](https://benchmark.clickhouse.com)
system entry for a moraine-backed DuckLake.

```sh
git clone https://github.com/ClickHouse/ClickBench.git
cargo xtask clickbench run --clickbench ./ClickBench --machine c6a.4xlarge
cargo xtask clickbench report --clickbench ./ClickBench
```

`run` drives upstream's own `lib/benchmark-common.sh` and writes
`benchmark/clickbench/results/<date>/<machine>.json`. `report` scores it
against the newest published run of every system on the same machine.
A scan touches the catalog once, so ClickBench measures the read path,
not commits. The entry's [README](../benchmark/clickbench/README.md) has
the methodology. No result is recorded yet.
<!-- /site:omit -->

### Cold reader

`cargo xtask reader-bench` measures what a fresh read-only process pays
over thousands of files: attach (split into store open and first
materialization), file listing, a pruned query plan, and a warm repeat.
Each repeat is a new process; only `--cache-dir` survives.

```text
MORAINE_S3_BUCKET=... [MORAINE_S3_ENDPOINT=...] \
  cargo xtask reader-bench [--files 2000,20000] [--tables 1] [--deletes 0]
                           [--repeat 5] [--cache-dir <dir>]
```

`--tables` spreads the files over that many tables and queries one.
`--deletes` gives that share of files a delete file.
<!-- site:omit -->
Findings, 2026-08, before the demand-loaded catalog:

- With 20,000 files behind a 20 ms endpoint, a restarted reader opens in
  451 ms with a populated cache directory, 1,378 ms without.
- 20,000 delete files move a first materialization from 284 ms to
  363 ms. Routine maintenance does not retire them; `CALL
  ducklake_rewrite_data_files('lake', delete_threshold => 0.01)` does.

### Cold attach on AWS S3

`measure_attach_latency_against_the_endpoint`: 40 tables × 8 columns,
160 statistics rewrites, then seven fresh read-only handles before and
after `compact_store`. ARM CodeBuild worker, same-region S3, 2026-08:

| state | subspace bytes / SSTs | open | first view | total |
|---|---:|---:|---:|---:|
| churned | 135,443 / 24 | 248 ms | 151 ms | 401 ms |
| merged | 75,379 / 11 | 263 ms | 149 ms | 412 ms |

This catalog attached in about 0.4 s. Compaction halved the SST count
without reducing attach latency.
<!-- /site:omit -->

### What an attach reads

Three in-memory sweeps, median read-only attaches.

`measure_attach_cost_by_dead_fraction` keeps the live catalog fixed and
rewrites statistics so only dead versions grow:

| churn rounds | `current` bytes | L0 SSTs | `snapshot` bytes | attach |
|---|---|---|---|---|
| 0 | 21,657 | 1 | 3,139 | 1.93 ms |
| 40 | 32,868 | 5 | 5,801 | 3.55 ms |
| 160 | 32,868 | 5 | 11,479 | 6.03 ms |

Attach cost triples while live entities stay constant. The last two rows
differ only in `snapshot`, one record per commit. `compact_store` removes
the dead versions.

`measure_attach_cost_under_get_latency` adds latency to every GET, so
the GET count is the slope:

| per-GET latency | churned (38 SSTs) | merged (14 SSTs) |
|---|---|---|
| 0 ms | 6.5 ms | 8.0 ms |
| 10 ms | 202.2 ms | 212.2 ms |
| slope (GETs) | 19.6 | 20.4 |

At 10 ms per GET the attach costs 31× its in-memory time. The merge cut
SSTs by 63% and did not reduce GETs, matching the S3 run above. Measured
<!-- site:omit -->at `c224745`, <!-- /site:omit -->after a reader stopped
writing a checkpoint of its own on open; before that<!-- site:omit --> (at
`8146cbf`)<!-- /site:omit --> the slopes were 22.6 and 23.7, and before the
demand-loaded catalog the merge cut GETs from 14.5 to 12.3.

`measure_attach_cost_by_index_size` grows only the `index` subspace:

| entries | `index` bytes | all SSTs | manifest | open | first view |
|---|---|---|---|---|---|
| 0 | 0 | 6 | 11 KB | 1.90 ms | 1.85 ms |
| 262,144 | 9.5 MB | 57 | 3.5 MB | 4.66 ms | 3.60 ms |
| 1,048,576 | 37.8 MB | 76 | 22.9 MB | 7.84 ms | 5.82 ms |

The manifest lists every SST and is read whole, so attach grows with SST
count (about 85 µs each), never with bytes. A production attach took 833 s
to read 12.8 MB because SlateDB's default scan fetched one 4 KB block per
round trip. Reading 4 MiB ahead with eight fetches in
flight fixed it.

### Materialization and refresh

<!-- site:omit -->
`measure_materialization_by_catalog_size` opens a read scope at 250 to 20,000
entities: 0.24 to 0.30 ms at every size. Since the demand-loaded catalog
(2026-11) a scope decodes nothing until a lookup, so scope creation no longer
measures the cost of reading those entities. In 2026-08 it measured 5 to 7 µs
per entity.
<!-- /site:omit -->

`measure_refresh_versus_rematerialization` (library tests): a cache
behind head replays the changelog or rescans. Median of 9:

| entities | gap commits | churn / entities | rescan | replay |
|---|---|---|---|---|
| 503 | 1 | 0.01 | 1.32 ms | 0.10 ms |
| 503 | 32 | 0.32 | 1.75 ms | 1.28 ms |
| 503 | 64 | 0.61 | 2.08 ms | 2.25 ms |
| 8,003 | 1 | 0.001 | 25.4 ms | 1.54 ms |
| 8,003 | 64 | 0.04 | 31.3 ms | 6.15 ms |

Replaying changes becomes slower than rescanning when about 60% of the catalog
has changed; Moraine switches to a rescan at 50%. The changelog cap is 4,096
keys so ordinary bulk flushes stay replayable.

`measure_changelog_read_cost`: in its own subspace the changelog holds a
flat 17,344 bytes at any commit count, and snapshot records are back to
2,985 bytes at 64 commits. Inside the snapshot record it had grown them
6.8×.

### Warm reads

`measure_warm_read_attribution`, 2,000 reads per thread over 50
tables<!-- site:omit -->, 2026-11-10 at `c224745`<!-- /site:omit -->, where a
read-only transaction pins the writer's publication instead of listing the
manifest:

| threads | read-write | read-only | held view, no check |
|---|---|---|---|
| 1 | 12.2 µs | 35 µs | 0.41 µs |
| 8 | 29.6 µs | 229 µs | 4.1 µs |
| 24 | 87.0 µs | 636 µs | 11.1 µs |

Reads through the writer plateau near 275,000 reads a second. Reads through a
read-only handle plateau near 37,000 a second from two threads up. Before
that<!-- site:omit --> (at `8146cbf`)<!-- /site:omit -->, when every read-only
transaction listed the manifest under one lock, it serialized near 8,000 a
second at any thread count: 90 µs, 1,056 µs, and 2,926 µs down the same
column. Before the demand-loaded catalog the figures were 0.12 µs and 6.4 µs.

`measure_reader_round_trips_under_get_latency`, 20 tables<!-- site:omit -->,
2026-11-10 at `c224745`<!-- /site:omit -->:

| GET latency | warm read-only read | round trips | cold handle | round trips |
|---|---|---|---|---|
| 2 ms | 0.04 ms | 0.0 | 47.0 ms | 23.5 |
| 20 ms | 0.05 ms | 0.0 | 300.4 ms | 15.0 |

A warm read through a read-only handle issues no GET: a transaction reuses its
handle's pinned publication for `READER_POLL_INTERVAL_MS` before listing for a
newer one. Before that<!-- site:omit --> (at `8146cbf`)<!-- /site:omit --> it
issued about two, the manifest listing every
transaction paid, and a cold handle 28.3 and 18.3. Metadata reads within the
interval require no endpoint round trip; checking for a new publication requires
a manifest read.

### Decoded value ownership

Question: how much synchronous CPU and allocation does decoding one file
record cost compared with deep cloning or sharing an existing value?

```text
cargo test -p moraine --lib --release --locked measure_decoding_and_shared_reuse -- --ignored --nocapture --test-threads=1
```

2026-11-13, commit `30c961a`, 8-vCPU x86-64 Linux sandbox, Intel Xeon
2.90 GHz, Rust 1.96.1 release build with the test allocation counter. Each
mode runs 100,000 iterations over a 69-byte framed `DataFileValue` containing
one path string. CPU is measured on the calling thread; no store or I/O is
involved.

| operation | CPU per record | allocations per record | allocated bytes per record |
|---|---:|---:|---:|
| protobuf decode | 99 ns | 1 | 50 |
| deep clone | 45 ns | 1 | 50 |
| shared ownership clone | 11 ns | 0 | 0 |

This small-record fixture isolates ownership costs. It does not measure a
catalog lookup, larger nested records, or the benefit of a decoded-value cache.

### Staged conflict check

`measure_staged_conflict_check_by_retained_snapshots` seeds 128, 512, and 2,048
DuckLake commits carrying 4 KiB of commit info each, stages a write, and runs
the changes arm of DuckLake's commit-time conflict check twenty times. GETs are
what the twenty runs issued; the per-check time is amortized against a session
that runs none, through the DuckDB CLI over a local store:

| retained | GETs, every snapshot read | GETs, from the floor | ms per check, before | after |
|---:|---:|---:|---:|---:|
| 128 | 302 | 0 | 12.8 | 1.6 |
| 512 | 607 | 0 | 7.5 | 1.1 |
| 2,048 | 1,553 | 0 | noise | 0.5 |

The paged scan read and decoded every retained snapshot and DuckDB filtered
afterwards. A cursor that opens at the pushed `snapshot_id` floor reads the
snapshots since the transaction began, which the block cache already holds,
so the check issues no object-store read at any retained count.

<!-- site:omit -->
### Worker pool

`measure_read_concurrency_under_io_latency` and
`measure_cpu_bound_decode_under_worker_pressure` showed in 2026-08 that
materialization neither starves the worker pool on IO nor monopolizes a worker
on decode. Both now call the lazy `read_scope` constructor without reading
records. They need to run lookups to measure I/O and decoding again.
<!-- /site:omit -->

## Index lookups

### Probe fetch

`measure_probe_cost_by_index_size`, 32 probes across the whole `index`
run:

| entries | index bytes | cold GETs | cold bytes / probe | warm GETs | warm ms / probe |
|---|---|---|---|---|---|
| 8,192 | 297 KB | 34 | 4,530 | 0 | 0.071 |
| 1,048,576 | 37.8 MB | 39 | 61,595 | 0 | 0.072 |

A cold probe into 37.8 MB fetches 62 KB (a part-grained cache would fetch
4 MiB). A warm probe fetches nothing.

`measure_index_maintenance_by_store_size`: 32 commits of 8,192 rows into
one unique index. Clustered commits cost 75 to 85 ms throughout;
scattered ones 1.1 to 1.8× that.

<!-- site:omit -->
### Lookup latency on AWS S3

`measure_index_lookup_latency_against_the_endpoint`: unique and
non-unique indexes over 64 files × 2,000 rows, fresh read-only attaches,
five repetitions. 2026-08-16, before the demand-loaded catalog:

| phase | median | GETs |
|---|---:|---:|
| `attach_read_only` | 345 ms | 27 |
| `cold_first_lookup` | 397 ms | 56 |
| `warm_second_lookup` | 0.28 ms | 0 |
| `in_list_lookup_many` (50 keys) | 1.36 ms | 0 |
| `attach_read_only_l0` | 465 ms | 97 |
| `cold_first_lookup_l0` | 2.78 ms | 0 |

The first lookup is a chain of about ten dependent round trips. With
`cache_preload = L0` the attach absorbs it (+120 ms, all concurrent) and
the first lookup costs 3 ms. The extension preloads `L0` by default.
<!-- /site:omit -->

### Located lookups

Row summaries (a range, Roaring bitmap, or sorted vector per data file)
map a row id to the files that hold it.

`cargo xtask locate-bench [--files N] [--rows-per-file N] [--update-batches N]`
reports summary bytes, cold and warm lookup time, and the files a scan
reads with and without the located join.
<!-- site:omit -->
Apple M2,64 files × 8,000 rows: 121,664 summary bytes (a quarter
byte per row), 0.2 ms warm lookup, and after six update batches the row-id
path reads 7 files where the located join reads 1.

`measure_located_lookup_latency_against_the_endpoint` builds summaries
from 64 real Parquet objects on AWS S3 (2026-08-16): 463 ms cold with 96
data-store GETs in waves of eight files; 0.45 ms warm with none. Fan-out
is now 64.
<!-- /site:omit -->

`row_lookups`<!-- site:omit --> (allocation tests)<!-- /site:omit -->, warm,
one hit:

| lookup | 16 sources | 128 | 1,024 |
|---|---|---|---|
| file-located | 442 µs | 2.45 ms | 20.2 ms |
| inline membership | 188 µs | 194 µs | 288 µs |
| `recent_row` | 91 µs | 84 µs | 83 µs |

<!-- site:omit -->
At 1,024 sources those are 118,871, 1,113, and 512 allocations.
<!-- /site:omit -->

Inline and recent lookups are flat. A file-located lookup costs about 20
µs<!-- site:omit --> and 116 allocations<!-- /site:omit --> per registered file,
because the demand-loaded catalog streams the file inventory a page at a time. A
retained directory previously answered in 35 µs at any size. Cold, it also reads
one footer per dense file.

### Scan strategies

`cargo xtask filter-bench <extension> <results-directory>` times wide
located filters against clustered, scattered, and broad matches.
`--baseline <extension>` compares a second build; `--fixture <directory>`
reuses an earlier run's data. Output is CSV.

## Memory

### Staged index build

`measure_staged_build_peak_heap` profiles one `create_index_staged` call
with dhat's allocator, 1,024-byte string values, 8 MiB cache:

| chunks × rows | step entries | peak heap | retained at return | time |
|---|---:|---:|---:|---:|
| 16 × 128 | 128 | 3.40 MB | 2.65 MB | 0.36 s |
| 128 × 128 | 128 | 22.80 MB | 21.08 MB | 4.9 s |
| 128 × 128 | 16 | 25.16 MB | 22.63 MB | 74 s |
| 16 × 1,024 | 128 | 23.56 MB | 21.06 MB | 3.5 s |

`BuildStep.entries` bounds the commit buffer, not the heap; most of the
peak is the resulting index and store buffers. A smaller step costs 15×
the time for no memory gain. Before streaming, the 128 × 128 case peaked
at 40 MB.

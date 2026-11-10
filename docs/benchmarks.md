# Benchmarks

Moraine replaces DuckLake's metadata catalog, so what it changes is the
metadata path: the commit, the catalog open, and what a statement reads
before it touches Parquet. Every harness here isolates that path, and
every number carries the date, machine, and store it was measured on.
The numbers are machine-specific; the shapes and ratios are the findings.

Unless a line says otherwise, a number below is from one release run on
an 8-vCPU x86-64 Linux VM with six schedulable CPUs, on 2026-11-10 at
commit `8146cbf`, against the in-memory object store. Numbers from other
machines or dates are stamped inline. Where a harness no longer measures
what it was written to measure, the entry says so rather than carrying
the old result forward.

Two terms shape the write path. A commit is durable once its
write-ahead-log object lands, and the writer paces those flushes at
`META_FLUSH_INTERVAL_MS`, 100 ms by default; a writer without siblings
closes its flush after a tenth of that, so a lone commit costs about
11 ms at the default and concurrent commits share one flush. Against a
real endpoint the PUT round trip binds instead: about 30 ms on
same-region S3.

## Running the harnesses

| question | command | needs |
|---|---|---|
| moraine against the other DuckLake catalogs | `cargo xtask bench` | the pinned DuckDB CLI, built by the task; Postgres on `PATH` or `MORAINE_BENCH_POSTGRES`, else skipped |
| the read path on a published scale | `cargo xtask clickbench run --clickbench <checkout> --machine <type>` | a ClickBench checkout, 40 GB of disk, passwordless `sudo` |
| where a commit's time goes | `cargo xtask commit-bench` | an S3 endpoint in `MORAINE_S3_BUCKET` |
| what a cold reader pays | `cargo xtask reader-bench` | an S3 endpoint for round-trip counts; local otherwise |
| what located lookups cost and save | `cargo xtask locate-bench` | nothing beyond the build |
| indexed scan strategies | `cargo xtask filter-bench <extension> <results-directory>` | a built extension |
| the core against a real endpoint | `cargo xtask s3` | nothing; it starts a pinned RustFS |
| the core in isolation | `cargo test -p moraine --test it --release -- --ignored --nocapture measure_` | nothing |
| the two that need crate-internal seams | `cargo test -p moraine --lib --release -- --ignored --nocapture measure_` | nothing |
| allocation counts on the commit path | `cargo test -p moraine --lib --release -- --exact --ignored --test-threads=1 --nocapture transaction::commit::tests::allocation::<name>` | nothing |
| peak heap of a staged index build | `cargo test -p moraine --test staged_build_memory --release -- --ignored --nocapture` | nothing |
| the core against AWS S3 | the `Real S3 benchmark` workflow | the AWS stack in [CONTRIBUTING.md](../CONTRIBUTING.md#the-real-s3-benchmark) |

Run measurements in `--release`; a debug build's numbers are meaningless.
The core harnesses run on the in-memory `object_store`, where a durable
commit performs no network IO, so the durable cost they report is the
flush wait plus compute, and a real store adds its PUT round trip on top.
Each harness stamps its output with the commit it measured, the host, and
the build profile.

## Where a commit's time goes

### The split across the stack

`cargo xtask commit-bench` splits an S3-backed one-row `UPDATE` into
DuckLake's metadata statements, moraine's own commit work, the durable
write wait, and the physical object-store requests behind them. Parquet
stays local, so every request it counts belongs to metadata. It builds
and loads the patched DuckLake, seeds one table with 1,600 rows over
`--files` files, and times `--commits` updates at each file count.

```text
MORAINE_S3_BUCKET=... MORAINE_S3_PREFIX=... AWS_REGION=... \
  cargo xtask commit-bench [--files 16,128] [--commits 7] [--flush-ms 25]
```

Credentials resolve through the normal AWS chain; a local S3 server also
takes `MORAINE_S3_ENDPOINT`, `AWS_ACCESS_KEY_ID`, and
`AWS_SECRET_ACCESS_KEY`. Against a loopback RustFS on the same VM,
2026-10-20, medians over seven commits:

| files | commit | DuckLake metadata | moraine core | durable wait | requests |
|---:|---:|---:|---:|---:|---|
| 16 | 23.0 ms | 13.4 ms | 3.3 ms | 3.0 ms | 1 PUT, 2.7 ms |
| 128 | 25.0 ms | 14.1 ms | 3.4 ms | 3.0 ms | 1 PUT, 2.9 ms |

DuckLake's eight metadata statements are about 60% of the commit and a
fixed per-commit cost: the total barely moves from 16 to 128 files. Two
statements carry 69% of it, the per-column file-list query DuckLake plans
every scan from and the snapshot insert, and neither is data access: the
same workload against a local-filesystem store costs within 0.2 ms of the
S3 one, and the task records zero GETs per commit. The time is DuckDB's
SQL front end, parsing, binding, and optimising about a kilobyte of
generated SQL to run a query that returns one row. Preparing the
statement buys back only the parse, because DuckDB re-binds and
re-optimises any prepared statement that reads a catalog. Under `perf`,
moraine's own code and its SlateDB reads together are under 4% of the
commit, so the lever is upstream: a bind-stable plan, or a catalog call
that never becomes SQL.

### Durable-commit latency against the flush interval

`measure_commit_latency_by_flush_interval`: 60 sequential durable
commits per interval.

| flush interval | median commit |
|---|---|
| 1 ms | 0.6 ms |
| 10 ms | 1.7 ms |
| 50 ms | 6.0 ms |
| 100 ms (default) | 11.4 ms |
| 250 ms | 26.2 ms |

A lone writer waits the pacing floor, a tenth of the interval, plus about
half a millisecond of compute. Before the floor existed a back-to-back
commit waited the whole interval, 103 ms at the default on an Apple M2 in
2026-08.

### Flush cadence against the write round trip

`measure_commit_latency_by_write_rtt` wraps the store in a
`ThrottledStore` that sleeps before every PUT and sweeps that delay
against the interval. 30 sequential commits per cell, median:

| PUT round trip | flush 1 ms | flush 25 ms | flush 100 ms |
|---|---|---|---|
| 0 ms | 0.5 ms | 3.8 ms | 11.3 ms |
| 5 ms | 19.6 ms | 19.5 ms | 19.5 ms |
| 25 ms | 80.0 ms | 79.9 ms | 80.0 ms |
| 100 ms | 305 ms | 305 ms | 305 ms |

Once the PUT round trip exceeds the pacing floor the interval drops out
entirely, and a commit then costs about three round trips: three
dependent PUTs stand between a commit and durability in this harness. In
2026-08 the same sweep measured `max(flush interval, round trip) + 2 ms`,
one PUT per commit; the SlateDB 0.18 vendoring sits between the two runs
and is the first place to look. The AWS numbers below predate it.

### Against real endpoints

`measure_commit_latency_against_the_endpoint` in `tests/object_storage.rs`
runs the same sweep where the PUT is an endpoint's own round trip.
`cargo xtask s3` runs it against a pinned RustFS on loopback, whose PUT
costs about a millisecond, so the run validates the composition rather
than pricing S3. The `Real S3 benchmark` workflow runs it from an ARM
CodeBuild worker against a same-region AWS S3 bucket in `us-west-2`.
Twenty sequential commits per interval, 2026-08-09, before the pacing
floor:

| flush interval | median commit | min | max |
|---|---|---|---|
| 1 ms | 29.6 ms | 24.0 ms | 40.6 ms |
| 25 ms | 28.1 ms | 22.2 ms | 35.7 ms |
| 100 ms | 101.9 ms | 73.9 ms | 143.4 ms |

The production endpoint put the durable-write floor near 30 ms, one PUT
round trip: lowering the cadence from 25 ms to 1 ms bought nothing, while
the 100 ms cadence still bound. With the pacing floor the default cadence
no longer binds a lone writer, so a fresh run should sit near the round
trip at every interval, once the three-PUT shape above is understood.

### Commit throughput against concurrency

`measure_commit_throughput_by_concurrency`: K callers commit 128/K times
each, every caller appending to a table of its own, at the 100 ms
default. Batch size is measured by counting the WAL objects the burst
wrote.

| concurrency | wall | commits/s | WAL writes | mean batch |
|---|---|---|---|---|
| 1 | 1.42 s | 90 | 128 | 1.0 |
| 4 | 354 ms | 361 | 32 | 4.0 |
| 16 | 87 ms | 1,465 | 8 | 16.0 |
| 64 | 25 ms | 5,212 | 2 | 64.0 |
| 128 | 23 ms | 5,575 | 2 | 64.0 |

Batch size equals concurrency until the 64-member batch ceiling binds, so
K commits cost the flushes of one and throughput is linear in K. A lone
writer lands one commit per pacing floor, 90 a second; at 128 the burst
arrives as two full batches and lands at the same rate as 64. Raising the
ceiling is one lever past that; lowering the interval is the other, and
they multiply. This is what makes routing a fleet's commits through one
writer affordable: they share a flush rather than a queue.

### What preparing a commit allocates

The allocation tests in `transaction::commit::tests::allocation` install
a counting allocator in the library's unit-test executable and reach the
private refresh path. Each runs alone with `--test-threads=1`;
instrumentation changes absolute timing, so compare scaling, not
milliseconds.

| test | measures |
|---|---|
| `snapshot_refresh` | a one-change incremental refresh against a full rematerialization, at 16 to 1,024 tables |
| `warm_reads` | a retained snapshot read 2,000 times; asserts one shared view, which the demand-loaded catalog no longer guarantees, so it fails today |
| `index_lookups` | point hits, misses, and ranges over a reopened unique index, first and warm pass |
| `index_maintenance` | appends of 1, 64, and 1,024 entries into indexes of 1,024 to 65,536 |
| `index_maintenance_input_order` | 4,096-entry appends in ascending, descending, and scattered order |
| `refresh_count_does_not_allocate` | asserts the refresh heuristic's entity count allocates nothing |
| `index_maintenance_profile` | twenty 1,024-entry appends, shaped for a Valgrind DHAT run |
| `verb_commit_scaling` | statistics updates on one table as unrelated tables and files grow, alone and in groups of four |
| `row_lookups` | one-hit file, inline, and `recent_row` lookups by source count, cold and warm, on both handle kinds |

- `verb_commit_scaling`: a mutation costs 1,764 allocations and about
  0.55 to 0.72 ms whether the lake holds no unrelated tables or 4,096 of
  them with eight files each; in a group of four it costs 981 allocations
  and 0.25 to 0.31 ms per mutation, since four share one durable batch.
  The retained pre-mutation snapshot is held throughout. Before
  structural sharing, 4,096 unrelated tables cost 66 ms of CPU per
  mutation (2026-09).
- `index_maintenance`: a 1,024-entry append costs 70,146 allocations,
  10.8 MB, and 11.5 ms into an index of 1,024 entries and the same into
  one of 65,536, so the cost follows the batch and not the index.
  Ascending, descending, and scattered 4,096-entry appends cost the same
  67 allocations per entry. In 2026-09 a dense append cost 13 per entry,
  with up to 128 ready uniqueness probes sharing one transactional range
  read and scattered input falling back to 61; the shared read no longer
  shows in this harness, and that regression is open.
- `index_lookups`: a warm point hit costs 340 allocations, 49 KB, and
  47 µs; a miss 277 and 39 µs; a 32-row range 1,054 and 161 µs, all at
  zero GETs and independent of index size from 1,024 to 65,536 entries.
  The first pass over a reopened index issues 25 to 56 GETs for the hits.
- `snapshot_refresh`: a one-change refresh allocates 192 to 198 times at
  every catalog size, 18 KB at 16 tables and 43 KB at 1,024, against
  5,644 allocations and 629 KB, then 340,133 and 38 MB, for a
  rematerialization. Its time still grows with the catalog, 51 µs to
  5.0 ms, because a view is immutable and shared, so advancing one copies
  it; the rematerialization grows 626 µs to 57.7 ms.

For a heap profile, build the unit-test executable with `--no-run`, run
`index_maintenance_profile` under `valgrind --tool=dhat`, and filter the
stacks by `profile_index_append`, which encloses commit preparation only.

### Reclaim throughput against batch size

`measure_reclaim_by_batch_size`: 50,000 dead index entries swept, each
batch one commit.

| batch | commits | median sweep | entries/s |
|---|---|---|---|
| 256 | 196 | 218 ms | 229,744 |
| 1,024 | 49 | 210 ms | 238,158 |
| 4,096 | 13 | 260 ms | 192,238 |
| 16,384 | 4 | 299 ms | 167,137 |
| 65,536 | 1 | 265 ms | 188,923 |

The sweep is compute-bound at every batch size: 196 commits cost about
1 ms each. In 2026-08 the same sweep awaited a full 100 ms flush per
batch and took 20 s at 256; batch size no longer matters to it.

## How moraine compares with the other DuckLake catalogs

`cargo xtask bench` runs identical DuckLake workloads against three
metadata catalogs, moraine's SlateDB store, a stock DuckDB-file catalog,
and a stock Postgres catalog, through the same pinned DuckDB CLI, and
reports per-phase wall-clock timings side by side. The data layer is
Parquet under a local `DATA_PATH` in every configuration; only the catalog
varies, so the numbers isolate metadata-path cost.

```text
cargo xtask bench [--backends moraine,duckdb,postgres] [--workloads <name>,...]
                  [--scale small|medium|large] [--repeat N]
                  [--concurrency 1,2,4,8] [--commits N] [--indexed]
                  [--flush-interval-ms N]
```

Defaults: all backends, all workloads, `--scale small`, `--repeat 3`. It
reuses the `e2e` plumbing to fetch the CLI and build the extension. The
Postgres backend self-provisions an ephemeral cluster from binaries on
`PATH` or under `/opt/homebrew/opt/postgresql@*`; `MORAINE_BENCH_POSTGRES`
names an existing server instead, and with neither the backend is
skipped with a notice. The bench pins moraine's flush interval low so
`small_commits` measures catalog work rather than the wait; the
concurrency workloads keep the 100 ms default, since the pacing is what
they test.

### Workloads

`small`, `medium`, and `large` scale bulk rows, small commits, tables,
and deletes as (100k, 20, 10, 20), (1M, 50, 25, 50), and (10M, 200, 100,
200).

| workload | measures |
|---|---|
| `bulk_load` | `CREATE TABLE` plus one large `INSERT`, data-plane dominated |
| `small_commits` | K single-row `INSERT`s, the headline catalog-latency number |
| `many_tables` | T × `CREATE TABLE`, the DDL commit path |
| `scan` | full and filtered scans, time travel, snapshot listing over a seeded table |
| `maintenance` | `merge_adjacent_files`, `expire_snapshots`, `cleanup_old_files` |
| `deletes_repeated` | D × single-key `DELETE`, one commit each |
| `deletes_bulk` | the same rows as `deletes_repeated`, in one statement |
| `deletes_covering` | a partition-aligned `DELETE`, which drops the file |
| `deletes_partial` | `deletes_covering`'s control: same file, one row short of covered |
| `deletes_indexed_repeated` | `deletes_repeated` against an equality index |
| `deletes_indexed_covering` | `deletes_covering` against an equality index |
| `concurrent_processes` | N writer processes committing at once |
| `concurrent_connections` | N connections over one instance, parallel DuckLake transactions |

Delete cost rises with how many tables the lake holds, whichever is the
target, so each delete workload gets a lake holding one table and nothing
else. That buys two comparisons between identically seeded workloads:
`deletes_repeated` against `deletes_bulk` bounds what N commits cost over
one, and `deletes_covering` against `deletes_partial` isolates the file
drop. The pairs are not comparable with each other, since a covering
delete needs a partitioned table. The `indexed` twins add one
`CREATE INDEX`, so subtracting the twin gives what maintaining index
entries costs a delete; the other backends have no equality index and sit
those out.

Every backend runs byte-identical SQL through the same binary, one
statement timed at a time via `.timer on`, with `ATTACH` a timed phase.
Each (workload, repeat) runs in fresh directories. Stdout prints a table
of `median (min…max)` per phase with a ratio column against moraine, and
the same data lands in `target/bench/report.json`.

### Concurrent writers

The two concurrency workloads spread `--commits` single-row inserts over
N writers, each appending to its own table, released together on a
barrier after attach and table creation. `concurrent_connections` runs
through `xtask/cpp/concurrent_writers.c`, linked against the DuckDB the
extension was built for, because the CLI holds one connection and cannot
overlap two DuckLake transactions. 128 commits, three repeats, 8-vCPU
x86-64 Linux, local `DATA_PATH`, 2026-08:

| writers | processes: moraine | duckdb | postgres | connections: moraine | duckdb | postgres | moraine WAL objects |
|---|---|---|---|---|---|---|---|
| 1 | 12.90 s | 1.30 s | 819 ms | 1.44 s | 1.25 s | 889 ms | 131 |
| 2 | refused | refused | 853 ms | 832 ms | 1.40 s | 801 ms | 68 |
| 4 | refused | refused | 996 ms | 489 ms | 1.74 s | 1.16 s | 39 |
| 8 | refused | refused | 1.29 s | 413 ms | 2.19 s | 1.28 s | 28 |
| 16 | | | | 437 ms | 2.44 s | 1.39 s | 27–35 |

Across processes both embedded catalogs are single-writer: moraine
fences, so the previous writer's next commit fails with a diagnosable
error, and the DuckDB file catalog's second process cannot take the file
lock. Postgres accepts the writers and throughput falls as they are
added. The one-process moraine column was measured before the pacing
floor, which is why a lone writer there waits the whole interval.

Within one process, moraine is the only catalog that gets faster with
writers, and from four up it is the fastest here. The last column is the
burst's cost in WAL PUTs: one per commit for a lone writer, one per round
under concurrency. Three rules close a flush before the spacing elapses:
a writer without siblings waits only a tenth of it; under concurrency the
flush closes once as many commits have joined as a recent flush carried;
and one flush in ten waits the spacing out to re-measure the population.
Against the spacing alone, the objects are the same within noise and the
latency is nine times better at one writer and twice at sixteen. Staged
DuckLake DML joins the same batch as verb commits and is rebased onto
what landed ahead, so a lost race costs nothing; before that, eight
writers took 4.15 s and sixteen exhausted DuckLake's retry budget. An
equality index on every table changes the curve by nothing (`--indexed`).

Past sixteen writers the burst is 512 commits. DuckDB-file exhausts
DuckLake's retry budget at sixteen, since the loser of every round
re-stamps the same snapshot id until it has lost ten times; Postgres gets
slower with every doubling; moraine runs 3.34 s at 16, 1.92 s at 32, and
2.10 s at 64, where CPU contention in the DuckLake path on eight vCPUs is
the likelier bound. Correctness under these races is
`test/sql/commit_concurrency.test` and the core's
`staged::tests::batching`, not this.

Not covered: remote object stores, cross-machine reproducibility, or
statistical rigour beyond median, min, and max. It is a local tool; CI
runs `e2e`, not this.

## What the catalog costs a read

### ClickBench

[ClickBench](https://benchmark.clickhouse.com) ranks analytical databases
on 43 queries over one 100-million-row table. `benchmark/clickbench` is a
ClickBench system entry for a moraine-backed DuckLake, and
`cargo xtask clickbench` runs it through upstream's own driver and ranks
the result against the published numbers:

```sh
git clone https://github.com/ClickHouse/ClickBench.git
cargo xtask clickbench run --clickbench ./ClickBench --machine c6a.4xlarge
cargo xtask clickbench report --clickbench ./ClickBench
```

`run` stages the entry into the checkout, executes
`lib/benchmark-common.sh`, and writes
`benchmark/clickbench/results/<date>/<machine>.json` in the published
format; `report` scores it against the newest published run of every
system on the same machine with ClickBench's own arithmetic. Comparisons
hold only within one machine, which is why `--machine` has no default.
ClickBench is a read benchmark: a scan touches the catalog once, so it
says what the metadata layer costs a read, which is little, and nothing
about the commit path. The entry's
[README](../benchmark/clickbench/README.md) has the methodology and the
two timing choices it inherits from the other embedded entries.

### A cold reader over a large catalog

`cargo xtask reader-bench` measures what a fresh read-only process pays
over a catalog with thousands of data files: the attach, the first
listing of every data file, one query planned against the file list and
pruned to one file, and a warm repeat. Every repeat is a new DuckDB
process, so only what `--cache-dir` keeps on disk survives between them.
Parquet stays local, so the object-store counters describe only the
metadata path.

```text
MORAINE_S3_BUCKET=... [MORAINE_S3_ENDPOINT=...] \
  cargo xtask reader-bench [--files 2000,20000] [--tables 1] [--deletes 0]
                           [--repeat 5] [--cache-dir <dir>]
```

Without a bucket the catalog is a local directory, which checks the task
but shows no round trips. `--tables` spreads the files over that many
tables and measures a query addressing one, which is what a many-table
lake pays per statement. `--deletes` gives that share of the files a
delete file, the kind DuckLake joins into every file list. The report is
medians of attach, its two halves read from moraine's own log events (the
store open and the first materialization), file listing, cold plan, and
warm plan, then GET counts and summed GET time; what the attach costs
beyond the two halves is DuckLake's own work over the rows.

Two findings from it, 2026-08. Over 20,000 files behind an endpoint
costing about 20 ms a request, a restarted reader opened in 451 ms with a
populated cache directory against 1,378 ms without one. And delete files
are not free: 20,000 of them moved a first materialization from 284 ms to
363 ms, since they are 20,000 more records in the shared set. Routine
maintenance does not retire them, since `ducklake_merge_adjacent_files`
skips files carrying deletions and `ducklake_rewrite_data_files` rewrites
only files 95% deleted; `CALL ducklake_rewrite_data_files('lake',
delete_threshold => 0.01)` took a fixture from 400 data and 400 delete
files to 200 and none.

### A cold attach against AWS S3

`measure_attach_latency_against_the_endpoint` in `tests/object_storage.rs`
runs in `cargo xtask s3` and in the `Real S3 benchmark` workflow: 40
tables × 8 columns, 160 statistics-rewrite rounds, then seven fresh
read-only handles before and after `compact_store`, with the store open
and the first materialized view timed separately. From an ARM CodeBuild
worker against same-region AWS S3, 2026-08:

| state | subspace bytes / SSTs | open | first view | total (median) |
|---|---:|---:|---:|---:|
| churned | 135,443 / 24 | 248.4 ms | 150.6 ms | 401.1 ms |
| merged | 75,379 / 11 | 262.8 ms | 148.8 ms | 411.6 ms |

The same-region number for a small catalog is about 0.4 s, split roughly
0.25 s opening the reader and 0.15 s materializing its first view. The
merge removed 44% of subspace bytes and 54% of SSTs and bought no
measurable attach improvement, which the in-memory slope below now
reproduces. Use the census before merging, and do not read a fall in SST
count as a promise of lower attach latency.

### What a cold attach reads

Three in-memory sweeps say which physical property of the store an
attach pays for. Median read-only attaches:

`measure_attach_cost_by_dead_fraction` holds 40 tables × 8 columns live
and rewrites every table's statistics per round, so only dead versions
grow:

| churn rounds | `current` bytes | L0 SSTs | `snapshot` bytes | median attach |
|---|---|---|---|---|
| 0 | 21,657 | 1 | 3,139 | 1.93 ms |
| 10 | 27,989 | 3 | 4,111 | 2.86 ms |
| 40 | 32,868 | 5 | 5,801 | 3.55 ms |
| 160 | 32,868 | 5 | 11,479 | 6.03 ms |

Live entities never move; attach cost triples. The last two rows have a
byte-identical `current` and still differ by 70%, because `snapshot`
doubled: one record per commit, read at every attach. An attach tracks
the physical size of every subspace it touches, and
`Catalog::compact_store` is what removes the dead versions.

`measure_attach_cost_under_get_latency` injects per-GET latency so the
GET count can be read off the slope, since attach time is roughly
`GETs × latency + decode`. Same shape, 160 rounds, before and after a
merge of four subspaces:

| per-GET latency | churned (38 SSTs, 93 KB) | merged (14 SSTs, 74 KB) |
|---|---|---|
| 0 ms | 8.5 ms | 7.0 ms |
| 2 ms | 73.1 ms | 76.9 ms |
| 10 ms | 234.8 ms | 244.5 ms |
| slope (GETs) | 22.6 | 23.7 |

IO dominates once latency is realistic, 28× at 10 ms a GET, so a
measurement that leaves latency out answers a different question. The
merge cut SSTs by 63% and bytes by 20% and left the GET count where it
was: a cold attach's round trips no longer follow the SST count, and a
merge buys it nothing, which is what the AWS run above found. In 2026-08
the same merge cut GETs from 14.5 to 12.3, when attach cost did track
SSTs in the read path.

`measure_attach_cost_by_index_size` holds the reader-visible subspaces
fixed and grows `index` alone:

| entries | `index` bytes | index SSTs | all SSTs | manifest bytes | open | first view |
|---|---|---|---|---|---|---|
| 0 | 0 | 0 | 6 | 11 KB | 1.90 ms | 1.85 ms |
| 65,536 | 2.4 MB | 8 | 34 | 445 KB | 2.62 ms | 2.71 ms |
| 262,144 | 9.5 MB | 32 | 57 | 3.5 MB | 4.66 ms | 3.60 ms |
| 1,048,576 | 37.8 MB | 44 | 76 | 22.9 MB | 7.84 ms | 5.82 ms |

The manifest lists every SST in every segment and is read whole before
any segment routing applies, so both halves grow with the SST count, by
roughly 85 µs per SST for the open here. It is the count that reaches the
attach, never the bytes: a store with ten SSTs and 3.3 GB of `index` pays
well under a millisecond for it. What made a production attach take
833 s against 12.8 MB of live `current` was none of these: SlateDB's
default scan read one block ahead with one fetch in flight, so a
materialization paid one round trip per 4 KB block, about 3,200
sequential fetches. Reading 4 MiB ahead with eight fetches in flight is
the fix.

### Materialization, refresh, and the changelog

`measure_materialization_by_catalog_size` opens a read scope on a fresh
handle at 250 to 20,000 live entities and times it: 0.24 to 0.30 ms at
every size. Since the demand-loaded catalog landed in 2026-11 a scope
decodes nothing until a lookup asks for a record, so the harness now
prices opening the scope, which is constant, where in 2026-08 it priced
a full materialization that grew at 5 to 7 µs per entity to 146 ms at
20,000. What a first lookup costs by catalog size is the question it
should be re-aimed at.

`measure_refresh_versus_rematerialization`, in the library tests: a
cache behind head can replay the `current` keys each commit in the gap
recorded or rescan outright, and moraine declines a replay whose churn
passes a share of the live catalog. 8 columns per table, 4 files per gap
commit, median of 9:

| live entities | gap commits | churn / entities | rescan | replay | speedup |
|---|---|---|---|---|---|
| 503 | 1 | 0.01 | 1.32 ms | 0.10 ms | 13.0× |
| 503 | 32 | 0.32 | 1.75 ms | 1.28 ms | 1.4× |
| 503 | 64 | 0.61 | 2.08 ms | 2.25 ms | 0.92× |
| 2,003 | 64 | 0.16 | 6.31 ms | 2.54 ms | 2.5× |
| 8,003 | 1 | 0.001 | 25.4 ms | 1.54 ms | 16.5× |
| 8,003 | 64 | 0.04 | 31.3 ms | 6.15 ms | 5.1× |

The crossover is near a churn share of 0.6, so the shipped backstop of
half the live entity count declines just before replay stops paying. A
replay is clone plus churn, since a view is immutable and shared, which
is why the speedup stops growing with catalog size at fixed churn. The
changelog cap is 4,096 keys so that an ordinary bulk flush stays on the
replay side; production showed one at 259 keys, which the previous cap of
256 declined.

`measure_changelog_read_cost`, in the library tests: the changelog once
rode in the snapshot record, where over 64 commits it grew the record
6.8× and slowed its scan 1.45×. In its own subspace the snapshot record
is back to 2,985 bytes at 64 commits and the changelog holds a flat
17,344 bytes at 64, 256, and 1,024 commits, since each commit deletes the
record 64 snapshots back.

### Warm reads

`measure_warm_read_attribution`: 2,000 reads per thread over a 50-table
catalog, on a read-write handle, a read-only handle, and the held view
with no check at all.

| threads | read-write | read-only | floor (no check) |
|---|---|---|---|
| 1 | 11.2 µs | 90 µs | 0.36 µs |
| 8 | 26.1 µs | 1,056 µs | 4.2 µs |
| 24 | 73.0 µs | 2,926 µs | 9.7 µs |

A read-write read costs about 11 µs alone and plateaus near 330,000 reads
a second from eight threads on. A read-only read costs 90 µs alone and
serializes: total throughput stays near 8,000 reads a second from one
thread to 24, so at 24 each read waits 2.9 ms. In 2026-08, on the
retained-view design, the same reads cost 0.12 µs and 6.4 µs, and the
read-only path held 300,000 a second; the serialization point in the
demand-loaded read-only path is open.

`measure_reader_round_trips_under_get_latency` injects per-GET latency
on 20 tables:

| GET latency | warm read-only read | warm round trips | cold handle | cold round trips |
|---|---|---|---|---|
| 2 ms | 6.5 ms | 3.3 | 56.7 ms | 28.3 |
| 5 ms | 12.8 ms | 2.6 | 109.7 ms | 22.0 |
| 20 ms | 43.3 ms | 2.2 | 366.4 ms | 18.3 |

A warm demand-loaded read-only read issues about two GETs: it retains no
decoded view, so every read goes to the store for the head and the
records it asks for. That is the trade the design makes for bounded
memory, and the number an operator should carry: a read-only statement's
metadata costs about two endpoint round trips. In 2026-08 the same read
issued none and a cold one four; a cold handle now issues 18 to 28.

### The worker pool

`measure_read_concurrency_under_io_latency` and
`measure_cpu_bound_decode_under_worker_pressure` were written to show
that a cold materialization neither starves the shared worker pool on IO
nor monopolizes a worker on decode: in 2026-08, 32 concurrent
materializations under 10 ms GET latency completed in the time of one,
and a small read beside eight concurrent 25 ms decodes cost 0.43 ms. Both
drive `read_scope`, which the demand-loaded catalog made lazy, so today
they measure an operation that does no IO and no decode: 0.08 to 0.42 ms
batches, 0.66 ms per "decode". They need re-aiming at a lookup that
actually reads before their result means anything again.

## What an index lookup costs

### What a probe fetches

`measure_probe_cost_by_index_size`: 32 probes spread across the whole
`index` run against a counting store, so the bytes are the transferable
number.

| entries | index bytes | cold GETs | cold bytes / probe | warm GETs | warm ms / probe |
|---|---|---|---|---|---|
| 8,192 | 297 KB | 34 | 4,530 | 0 | 0.071 |
| 262,144 | 9.5 MB | 34 | 18,473 | 0 | 0.072 |
| 1,048,576 | 37.8 MB | 39 | 61,595 | 0 | 0.072 |

A cold probe into a 37.8 MB run fetches 62 KB, against the 4 MiB a
part-grained cache would have faulted; a warm probe fetches nothing and
costs 70 µs at every size.

`measure_index_maintenance_by_store_size` is the write side: 32 commits
of 8,192 rows each into one unique index, to 262,144 rows. A clustered
commit costs 75 to 85 ms throughout; a scattered one 90 to 160 ms, 1.1
to 1.8× the clustered, with no trend as the store grows.

### Lookup latency against AWS S3

`measure_index_lookup_latency_against_the_endpoint` seeds one table with
a unique and a non-unique index over 64 files × 2,000 rows, spread over
several SSTs, and times the read path from fresh read-only attaches, five
repetitions each. ARM CodeBuild worker, same-region AWS S3, 2026-08-16,
before the demand-loaded catalog:

| phase | median | GETs |
|---|---:|---:|
| `attach_read_only` | 345.3 ms | 27 |
| `cold_first_lookup` | 396.9 ms | 56 |
| `warm_second_lookup` | 0.28 ms | 0 |
| `in_list_lookup_many` (50 keys) | 1.36 ms | 0 |
| `range` (100 entries) | 0.34 ms | 0 |
| `attach_read_only_l0` | 464.6 ms | 97 |
| `cold_first_lookup_l0` | 2.78 ms | 0 |

Once a table's SST metadata and blocks are resident every probe is
memory-speed and issues no GET. Getting there is a chain of dependent
round trips about ten hops deep, which wider fan-out cannot shorten, so
the lever is when it is paid: with `cache_preload = L0` the open absorbs
it, 120 ms more and all concurrent, and the first lookup on every table
costs 3 ms instead of 400. That is why the extension preloads `L0` by
default. The warm rows should be re-measured against the two round trips
a demand-loaded read-only read now pays.

### Located lookups

Row summaries, a `Range`, Roaring bitmap, or sorted vector per data
file, turn a row id into the files that physically hold it, so a lookup
scans those files instead of every current one.

`cargo xtask locate-bench [--files N] [--rows-per-file N] [--update-batches N]`
builds a lake through the pinned CLI and reports the summary cache's
resident bytes, a cold and a warm lookup, and the files a DuckLake scan
reads with the located join against the row-id path, before and after
update batches whose files span the whole row-id range. Apple M2,
2026-08, 64 files × 8,000 rows: 121,664 summary bytes, about a quarter of
a byte per row; a warm located lookup at 0.2 ms; and after six update
batches the row-id path reads 7 files where the located join reads 1, the
same answer a constant-id predicate gets without knowing the file.

`measure_located_lookup_latency_against_the_endpoint` writes 64 real
Parquet objects under the run prefix, half dense and half with gapped
embedded row ids, and prices the summary build against AWS S3
(2026-08-16): `locate_cold` at 462.7 ms with 96 data-store GETs, 64
footers plus 32 row-id columns, in eight waves of eight files;
`locate_warm` at 0.45 ms with no GET against either store. The fan-out
has since widened to 64, so one wave is the target.

`row_lookups` in the allocation tests times one-hit lookups by source
count, cold and warm, on both handle kinds. Warm:

| lookup | 16 sources | 128 | 1,024 | allocations at 1,024 |
|---|---|---|---|---|
| file-located | 442 µs | 2.45 ms | 20.2 ms | 118,871 |
| inline membership | 188 µs | 194 µs | 288 µs | 1,113 |
| `recent_row` | 91 µs | 84 µs | 83 µs | 512 |

Inline and recent lookups are constant in source count. A file-located
lookup now costs about 116 allocations and 20 µs per registered file,
warm or cold, because the demand-loaded catalog streams the file
inventory a page at a time and retains none of it; in 2026-09 a retained
file directory answered the same lookup in 35 µs at any size. A cold
file-located lookup also reads one footer per dense file: 1,024 data GETs
at 1,024 files. The read-only handle costs the same as the read-write
one here.

### Indexed scan strategies

`cargo xtask filter-bench <extension> <results-directory>` times wide
located filters against clustered, scattered, and broad matches on local
Parquet through the CLI, with `--baseline <extension>` for a second build
to compare and `--fixture <directory>` to reuse an earlier run's data.
Its output is a CSV of first-profile and warm-median seconds per case,
query, build, thread count, and scan path.

## Memory

### Peak heap of a staged index build

`measure_staged_build_peak_heap` has a test binary of its own, since
dhat's allocator cannot sit beside the counting one. It profiles a whole
`create_index_staged` call over a persisted local store, 1,024-byte
string values, an 8 MiB cache budget:

| chunks × rows | step entries | inline bodies | peak heap | retained at return | build time |
|---|---:|---:|---:|---:|---:|
| 16 × 128 | 128 | 2.1 MB | 3.40 MB | 2.65 MB | 0.36 s |
| 128 × 128 | 128 | 16.9 MB | 22.80 MB | 21.08 MB | 4.9 s |
| 128 × 128 | 16 | 16.9 MB | 25.16 MB | 22.63 MB | 74 s |
| 16 × 1,024 | 128 | 16.8 MB | 23.56 MB | 21.06 MB | 3.5 s |

`BuildStep.entries` bounds the commit buffer, not the heap: most of the
peak lives past the build's return, in the resulting index and store
buffers. A smaller step raises the peak slightly and costs fifteen times
the wall clock, because more commits retain more store state. Before the
driver streamed one chunk at a time the 128 × 128 case peaked at 40 MB
(2026-09).

### A demand-loaded catalog in a fresh process

Whole-process memory is measured from a fresh DuckDB process over a
persisted fixture: `write_memory_measurement_fixture` in the library
tests writes it to `MORAINE_MEMORY_FIXTURE` with `MORAINE_MEMORY_FILES`
registered files, and the process under measurement runs with
`MORAINE_WORKING_MEMORY_BYTES` set. With a 16 MiB working-memory
allowance and a 16 MiB cache budget, a count over two million
file-column statistics records peaked at 80.9 MiB RSS, 6.4 MiB above the
same scan over 20,000 records (2026-11-04). The preloading tests beside
it, `deep_preload_uses_bounded_working_memory` and the extension's
`preload_streams_a_persisted_catalog`, hold the same allowance.
`measure_active_writer_reader_generations` prices readers overlapping an
open writer: five generations reserved 24.7 MiB of WAL replay and the
sixth was refused, an admission bound rather than an RSS cap
(2026-11-05).

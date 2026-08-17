# Benchmarking

`cargo xtask bench` runs identical DuckLake workloads against three metadata
catalogs — moraine's SlateDB store, a stock DuckDB-file catalog, and a stock
Postgres catalog — through the same pinned DuckDB CLI, and reports per-phase
wall-clock timings side by side. The data layer is Parquet under a local
`DATA_PATH` in every configuration; only the catalog backend varies, so the
numbers isolate metadata-path cost — the thing moraine replaces.

## Running it

```text
cargo xtask bench [--backends moraine,duckdb,postgres]
                  [--workloads <name>,...]
                  [--scale small|medium|large]
                  [--repeat N]
```

Defaults: all backends, all workloads, `--scale small`, `--repeat 3`. It
reuses the `e2e` plumbing — downloads/caches the pinned DuckDB CLI, builds and
packages the extension, caches the `ducklake`/`postgres` extensions.

The Postgres backend self-provisions an ephemeral cluster (`initdb` +
`pg_ctl` on a Unix socket, torn down on exit) from Postgres binaries on
`$PATH` or under `/opt/homebrew/opt/postgresql@*`. Set
`MORAINE_BENCH_POSTGRES=<libpq DSN>` to use an existing server instead. If no
Postgres is found, that backend is skipped with a notice — the suite never
fails because a backend is unavailable.

moraine's per-commit latency is bounded by its WAL flush cadence (100ms by
default); the bench pins it low so `small_commits` measures catalog work
rather than the flush wait. Tune it on any attach with
`META_FLUSH_INTERVAL_MS <n>`, at the cost of more frequent object-store PUTs.

## Workloads

`small` / `medium` / `large` scale (bulk rows, small commits, tables) as
(100k, 20, 10) / (1M, 50, 25) / (10M, 200, 100).

| workload | measures |
|---|---|
| `bulk_load` | `CREATE TABLE` + one large `INSERT` — data-plane dominated |
| `small_commits` | K single-row `INSERT`s — the headline catalog-latency number |
| `many_tables` | T × `CREATE TABLE` — the DDL commit path |
| `scan` | full/filtered scans, time travel, snapshot listing over a seeded table |
| `maintenance` | `merge_adjacent_files`, `expire_snapshots`, `cleanup_old_files` |

Every backend runs the byte-identical SQL through the same DuckDB binary, one
statement timed at a time via `.timer on`, so differences come only from the
catalog backend. `ATTACH` is a timed phase, so catalog-open cost is measured
too. Each (workload, repeat) runs in fresh directories, so repeats are
independent.

## Results

Stdout prints an aligned table — rows are `workload/phase`, columns are
backends, cells are `median (min…max)`, plus a ratio column against moraine.
The same data lands in `target/bench/report.json` for diffing across
checkouts.

Not covered: concurrency/contention, remote object stores, cross-machine
reproducibility, or statistical rigor beyond median/min/max. It's a local
tool; CI runs `e2e`, not this.

## Located lookups

`cargo xtask locate-bench` measures what file-located index lookups cost and
save: the row summaries (a `Range`, Roaring bitmap, or sorted vector per data
file) that turn a row id into the files that physically hold it, so a lookup
scans those files instead of every current one.

```text
cargo xtask locate-bench [--files N] [--rows-per-file N] [--update-batches N]
```

It builds a lake of `--files` Parquet files through the pinned DuckDB CLI,
then reports the summary cache's resident bytes, one cold lookup (a fresh
attach that builds every summary), the first and a warm lookup on one
attach, and the files a DuckLake scan reads with the located join against
the row-id path — once for disjoint files, then after each of
`--update-batches` update batches, each of which writes one file whose row-id
range spans the whole table so statistics can no longer exclude it.

Two runs on an **Apple M2 (Mac14,2), arm64**, local `DATA_PATH`:

| | 16 files × 2,000 rows | 64 files × 8,000 rows |
|---|---|---|
| summary cache bytes | 30,416 | 121,664 |
| cold lookup (fresh attach, builds) | 99.7 ms | 43.8 ms |
| first lookup on an attach (builds) | 79.2 ms | 43.6 ms |
| warm lookup | 16.0 ms | 0.2 ms |
| files read, row id only / located | 1 / 1 | 1 / 1 |

Files read after each update batch (both scales identical):

| batch | row id only | located | id as a constant |
|---|---|---|---|
| 1 | 2 | 1 | 1 |
| 2 | 3 | 1 | 1 |
| 3 | 4 | 1 | 1 |
| 6 | 7 | 1 | 1 |

The summaries cost about a quarter of a byte per row (512k rows in 119 KB),
and a warm located lookup is sub-millisecond at the larger scale. The
files-read table is the point: without location, every update batch adds one
file the row-id path must read, because the batch's file spans the whole
row-id range; with location it stays at one file regardless of history —
the same answer a constant-id predicate gets, without knowing the file. Real
endpoint numbers for the summary build live in the located-lookup measurement
below.

# Core measurements

`cargo xtask bench` times the whole stack through the DuckDB CLI. A second,
finer harness times moraine's core directly, for questions the end-to-end
tool cannot isolate — materialization cost, commit latency, reclaim
throughput. These live as `#[ignore]`d tests, one per question, in
`crates/moraine/tests/it/measure.rs`:

```text
cargo test -p moraine --test it --release -- --ignored --nocapture measure_
```

Two more need crate-internal seams (the refresh path is not public), and
live in the library's own tests:

```text
cargo test -p moraine --lib --release -- --ignored --nocapture measure_
```

Run in `--release`; a debug build's numbers are meaningless. They use the
in-memory `object_store`, so a durable commit performs no network IO — the
durable cost measured is moraine's flush poll plus compute, and a real object
store adds its WAL-object PUT round-trip on top. Each harness prints its
conditions.

The results below are one representative run on an **Apple M2 (Mac14,2),
arm64**. Absolute numbers are machine-specific; the shapes and ratios are the
findings.

**One thing dominates: the WAL flush interval.** A durable commit waits for
the next flush tick, so at the 100 ms default a single commit costs ~100 ms —
regardless of backend, and swamping the ~2 ms of compute underneath. Every
durable-commit-bound cost inherits this. Concurrency is the way out, not a
faster commit: K concurrent commits share one flush.

### Durable-commit latency vs. flush interval

60 sequential `await_durable` commits, per interval:

| flush interval | median commit |
|---|---|
| 1 ms | 2.8 ms |
| 10 ms | 12.7 ms |
| 50 ms | 52.5 ms |
| 100 ms (default) | 102.7 ms |
| 250 ms | 252.7 ms |

Commit latency is `flush_interval + ~2 ms`. The ~2 ms is the real compute
floor; everything above it is the wait for the flush tick. On a real object
store the tick wait is replaced (or joined) by the WAL PUT round-trip — the
next two sections measure that term rather than assume it.

### Durable-commit latency vs. write round-trip

The composition of the two terms, measured by wrapping the in-memory store in
a `ThrottledStore` that sleeps before every PUT and sweeping that delay
against the flush interval. 30 sequential commits per cell, median:

| PUT round-trip | flush 1 ms | flush 25 ms | flush 100 ms |
|---|---|---|---|
| 0 ms | 2.2 ms | 26.4 ms | 101.4 ms |
| 5 ms | 7.5 ms | 26.3 ms | 100.9 ms |
| 25 ms | 27.5 ms | 27.6 ms | 101.0 ms |
| 100 ms | 102.7 ms | 102.7 ms | 102.6 ms |

Every cell is the **larger** of the two terms plus the ~2 ms compute floor,
never their sum: a commit waits for one flush, and that flush waits for
whichever of the tick and the PUT is slower. So the model is
`max(flush cadence, write RTT) + ~2 ms`, confirmed rather than posited. The
practical reading: lowering the flush interval below the store's write RTT
buys nothing, and a fast store cannot make up for a slow flush cadence.

### Durable-commit latency against a real endpoint

The same sweep against a live S3-compatible endpoint, where the PUT is the
endpoint's own round trip (`cargo xtask s3`, which runs
`object_storage.rs`'s `measure_commit_latency_against_the_endpoint` in
release against a pinned MinIO):

| flush interval | median commit | min | max |
|---|---|---|---|
| 1 ms | 2.5 ms | 1.5 ms | 3.7 ms |
| 25 ms | 25.9 ms | 22.5 ms | 29.9 ms |
| 100 ms | 101.4 ms | 83.0 ms | 116.7 ms |

A loopback MinIO's PUT costs about a millisecond, so it lands in the
`RTT ≈ 0` row of the table above and the flush cadence dominates everywhere:
this validates the composition against a real object-storage protocol, but
it understates S3.

The main-only [`Real S3 benchmark`](docs/real-s3-benchmark.md) ran the same
sweep from an ARM CodeBuild worker against AWS S3 in `us-west-2`, with the
worker and bucket in the same region. Twenty sequential commits per interval
on 2026-08-09 produced:

| flush interval | median commit | min | max |
|---|---|---|---|
| 1 ms | 29.6 ms | 24.0 ms | 40.6 ms |
| 25 ms | 28.1 ms | 22.2 ms | 35.7 ms |
| 100 ms | 101.9 ms | 73.9 ms | 143.4 ms |

The production endpoint puts the durable-write floor at roughly 28–30 ms for
this deployment: lowering the cadence from 25 ms to 1 ms buys nothing, while
the 100 ms cadence still binds. This is the real-endpoint row the injected-RTT
model predicted, not a new term in the composition.

### Index lookup latency against a real endpoint

The in-memory probe measurement further down prices what an index probe
*fetches*; this prices what it *waits*. `object_storage.rs`'s
`measure_index_lookup_latency_against_the_endpoint` seeds one table with a
unique and a non-unique index and 64 files x 2 000 rows (128 000 entries per
index, spread over several SSTs by closing the seed writer every 8 files;
no Parquet is written, since a lookup reads only the store), then times the
current read path — per-table warm on first touch, `warm_tables`, batched
probes, remote-sized read-ahead — from fresh read-only attaches, five
repetitions each, reporting median/min/max and the median main-store GET
count per phase:

| phase | what it times |
|---|---|
| `attach_read_only` | `Catalog::open_read_only` |
| `cold_first_lookup` | first `index_lookup` on a fresh attach (the first-touch warm starts here) |
| `warm_second_lookup` | a second, different key on the same attach |
| `steady_lookup` | 20 distinct keys on the warm attach, pooled per lookup |
| `non_unique_lookup` | one key of the non-unique index (8 rows) |
| `in_list_lookup_many` | one `index_lookup_many` of 50 keys |
| `range` | one `index_range` over 100 entries |
| `locate_row_ids` | 50 row ids with no data store (catalog side only) |
| `warm_tables` | explicit `warm_tables` on a fresh attach |
| `lookup_after_warm` | the first `index_lookup` behind that warm |
| `attach_read_only_l0` | `Catalog::open_read_only` with `cache_preload = L0` (SST metadata preloaded at open) |
| `cold_first_lookup_l0` | first `index_lookup` on that L0-preloaded attach |
| `warm_tables_l0` | explicit `warm_tables` on a fresh L0-preloaded attach |

The `_l0` rows show what preloading the newest SSTs' metadata at attach
buys the cold row: what moves out of `cold_first_lookup` and into the attach.

Locally, `cargo xtask s3` runs it in release against the pinned MinIO; the
main-only [`Real S3 benchmark`](docs/real-s3-benchmark.md) workflow runs it
against AWS S3 and keeps the printed table in the run artifact. Results are
recorded here after a run:

| phase | median | min | max | gets |
|---|---:|---:|---:|---:|
| _(after a run)_ | | | | |

### Located lookup latency against a real endpoint

The index measurement above prices `locate_row_ids` with no data store, so
it sees the catalog side only. `measure_located_lookup_latency_against_the_endpoint`
in the same file prices the other half: the row-summary path that reads a
Parquet file's row-id column once and answers from the resident summary
afterwards. It writes 64 real Parquet objects x 2 000 rows under the run
prefix's data path and registers them with their true `file_size_bytes` and
`footer_size`: 32 dense files carry no row-id column and answer from the
range the commit allocated, 32 embed `_ducklake_internal_row_id` with gaps
(24 stepping by 2, 8 striding past a Roaring container per id), so their
summaries must be built from the object. Each of five repetitions attaches
fresh and hands the lookup a fresh data-store handle, since the summary
cache is keyed by the handle; every `locate_row_ids` asks for 50 present
ids, half dense and half sparse. Rows report median/min/max and the median
GET count against the catalog store (`main_gets`) and the data store
(`data_gets`):

| phase | what it times |
|---|---|
| `attach_read_only` | `Catalog::open_read_only` |
| `locate_cold` | first `locate_row_ids` on the fresh attach and handle: every sparse file's row-id column read and summarised, every dense file's footer checked |
| `locate_warm` | the same ids again on the same handle: summaries resident, so no data-store GET is expected |
| `warm_row_summaries` | explicit `warm_row_summaries` on another fresh attach and handle |
| `locate_after_warm_row_summaries` | the first `locate_row_ids` behind that warm |

The conditions lines also print how many summaries the warm built and how
many bytes the cold locate added to the auxiliary cache (footers and
summaries together; the public status does not split them or name the
summary shapes). It runs with the rest of the suite under `cargo xtask s3`
and in the `Real S3 benchmark` workflow. Results are recorded here after a
run:

| phase | median | min | max | main_gets | data_gets |
|---|---:|---:|---:|---:|---:|
| _(after a run)_ | | | | | |

### Commit throughput vs. concurrency

The sequential number above is one commit per flush by construction. This is
the concurrent one, now that concurrent commits coalesce into a shared batch:
K callers commit 128/K times each, every caller appending to a table of its
own, at the 100 ms default interval. Batch size is measured, not inferred —
the harness counts the WAL objects the burst wrote:

| concurrency | wall | commits/s | WAL writes | mean batch |
|---|---|---|---|---|
| 1 | 12.94 s | 9.9 | 128 | 1.0 |
| 2 | 6.47 s | 19.8 | 64 | 2.0 |
| 4 | 3.24 s | 39.6 | 32 | 4.0 |
| 8 | 1.62 s | 79.1 | 16 | 8.0 |
| 16 | 0.81 s | 158.3 | 8 | 16.0 |
| 32 | 0.40 s | 316.3 | 4 | 32.0 |
| 64 | 0.20 s | 632.1 | 2 | 64.0 |
| 128 | 0.20 s | 632.5 | 2 | 64.0 |

**Batch size equals concurrency, exactly, until the member ceiling binds.**
Throughput is therefore `concurrency / flush_interval` — linear, with no
coalescing overhead visible at any level: K commits cost the flushes of one.
The ceiling is `MAX_BATCH_MEMBERS` (64), and it binds precisely where it
says: 128 concurrent commits take the same wall-clock as 64 and land at the
same rate, because they arrive as two full batches instead of one. Raising
the ceiling is the lever if a workload ever needs past ~630 commits/s at the
default cadence; lowering the flush interval is the other, and the two
multiply.

This is what makes the single-writer topology's funnel affordable: routing a
fleet's commits through one process costs them a shared flush, not a queue.

### Reclaim throughput vs. maintenance batch size

50 000 dead index entries swept at the 100 ms default; each batch is one
durable commit, so `commits = ceil(entries / batch)`:

| batch | commits | median sweep | entries/s |
|---|---|---|---|
| 256 | 196 | 20.3 s | 2 467 |
| 1024 (strawman) | 49 | 5.08 s | 9 849 |
| 4096 | 13 | 1.34 s | 37 378 |
| 16384 | 4 | 0.41 s | 120 948 |
| 65536 | 1 | 0.14 s | 355 768 |

Sweep time is `commits × flush_interval` to within a few percent — the
per-entry compute (~1 µs) is negligible. The reclaim loop awaits durability
*per batch*, so the whole sweep is flush-bound. The strawman 1024 spends ~4.9 s
of the 5.08 s waiting on flush ticks. Two independent levers cut that: a much
larger batch (16k–64k here), or not awaiting durability on every intermediate
batch — the entries are tombstoned idempotently, so only the last batch needs
to be durable. The second lever makes batch size almost irrelevant and is the
better fix; recorded as a follow-up in `docs/rfcs/tasks.md` under 0021.

### Cold attach cost vs. the physical bytes a read touches

The claim the store census and the store merge rest on: a cold attach reads
through every superseded version the store still holds, so it costs what the
store *physically* weighs rather than what it lives. 40 tables x 8 columns,
the live set held identical across every row — each round rewrites every
table's statistics and closes, superseding one record per table without
changing what a reader sees. Median of 7 **read-only** attaches, a fresh
handle each:

| churn rounds | live keys | `current` bytes | L0 | runs | `snapshot` bytes | median attach |
|---|---|---|---|---|---|---|
| 0 | 402 | 21 069 | 1 | 0 | 3 018 | 0.96 ms |
| 10 | 402 | 26 925 | 3 | 2 | 3 700 | 1.37 ms |
| 40 | 402 | 31 447 | 5 | 3 | 5 144 | 1.48 ms |
| 160 | 402 | 31 447 | 5 | 3 | 10 465 | 1.82 ms |

Live entities never move; attach cost nearly doubles. That is the shape
behind a 3.4 GB store serving one snapshot at a 642 s attach — the cost is in
the dead versions, and `Catalog::compact_store` is what removes them.

**Two subspaces drive it, not one.** A materialization scans `current` and
point-reads `sys/head` and the head `snapshot` record, so its cost tracks the
physical size of all three. Rows 1–3 grow `current`; the last two rows have a
byte-identical `current` (same 5 L0 SSTs, same 3 sorted runs) and still
differ by ~17%, because `snapshot` doubled — one record per commit, and 160
rounds is 160 more commits. Point reads are not free when the segment holding
them has more SSTs to probe. So "attach cost tracks physical bytes" is right,
provided *bytes* means every subspace a read touches rather than `current`
alone. A merge targets each subspace independently, so both are reclaimable.

**What this does not reach.** The production store was 3.4 GB and this tops
out at ~43 KB. Building a genuinely large one means many SSTs, and SlateDB
writes one only when its in-memory buffer fills, at a 64 MB default — so
~50 SSTs the natural way is ~3 GB pushed through the catalog, far too slow
for a test. Closing the handle forces an early flush, which is how the rows
above were produced, but those SSTs are all tiny: the harness can vary SST
*count* or *size*, not both. Reaching the production regime needs either a
real bloated store or an `l0_sst_size` knob on `CatalogOptions`, which is
not exposed today.

### What a store merge is worth once a GET costs what S3 charges

The table above runs in memory, so it prices decode. The production
incident did not: ~5.3 MB/s effective, a read pulling the store across the
network. There the term that matters is **object-store GETs issued**, and
GETs scale with how many SSTs a scan opens rather than how much they hold —
so an in-memory store measures the wrong term however large it grows, while
injected per-GET latency measures the right one at any size.

Sweeping latency for a fixed store makes the GET count readable off the
slope, since attach time is roughly `GETs x latency + decode`. Same shape as
above (40 tables, 160 churn rounds, live set constant), read-only probes,
median of 5:

| per-GET latency | churned (37 SSTs) | after merge (25 SSTs) |
|---|---|---|
| 0 ms | 1.93 ms | 1.80 ms |
| 2 ms | 42.0 ms | 35.4 ms |
| 5 ms | 81.5 ms | 69.4 ms |
| 10 ms | 146.9 ms | 124.6 ms |
| **slope (GETs)** | **14.5** | **12.3** |

Two things to read off it. **IO dominates completely** once latency is
realistic: at 10 ms/GET the same attach costs 147 ms against 1.9 ms
unthrottled, ~75x, so any measurement that leaves latency out is answering a
different question. And **the merge pays**, cutting the GET count 14.5 to
12.3 and attach time ~15%.

Note the merge cut SSTs by 32% but GETs by only 15%: a materialization
opens SSTs in the subspaces it actually reads, so merging `index` — which no
scan touches — costs GETs nothing. That is the same asymmetry the census
exists to expose before an operator spends a merge on the wrong subspace.

The relationship is linear in both terms, so the production regime
extrapolates: a store with N times the SSTs in the read path costs N times
the GETs, at whatever the endpoint's latency is. That is what turns 1.9 ms
into 642 s without needing 3.4 GB on hand to see it.

**How this was arrived at matters.** The first revision of this harness
reported a flat line — churn that never leaves the memtable never reaches
the manifest, so closing per round is what writes it out. The second opened
a writer per repeat, whose compactor moved the state between repeats. And
the merged column read `0 subspaces merged` until the run exposed that a
merge asked for straight after an attach found every tree already being
merged by the writer's own compactor and skipped them all; adopting the
in-flight merge instead is what makes this column mean anything.

### Cold attach against AWS S3

The main-only real-S3 workflow ran that same catalog shape from an ARM
CodeBuild worker against a bucket in the worker's `us-west-2` region: 40
tables x 8 columns, 160 stats-rewrite rounds, then seven fresh read-only
handles before and after `compact_store`. Open and first materialized view
were timed separately; total excludes close.

The census immediately before each timing row was:

| state | subspace bytes / SSTs | `current` bytes / SSTs | WAL objects / bytes | manifest bytes |
|---|---:|---:|---:|---:|
| churned | 135 443 / 24 | 25 631 / 4 | 265 / 231 099 | 5 209 474 |
| merged | 75 379 / 11 | 22 763 / 2 | 257 / 211 119 | 5 011 208 |

And the cold timings, median of seven:

| state | open | first view | total | total min | total max |
|---|---:|---:|---:|---:|---:|
| churned | 248.4 ms | 150.6 ms | 401.1 ms | 391.6 ms | 436.2 ms |
| merged | 262.8 ms | 148.8 ms | 411.6 ms | 383.9 ms | 446.5 ms |

**The absolute same-region endpoint number for this small catalog is about
0.4 s, split roughly 0.25 s opening the reader and 0.15 s materializing its
first view.** The merge completed four subspaces and removed 44% of subspace
bytes and 54% of SSTs, but the timing distributions overlap completely; it
bought no measurable attach improvement here. That does not contradict the
injected-GET slope: this store is dominated in the census by object classes a
subspace merge does not reclaim, and the fixed reader-open work is already
larger than the materialization. It does make the operational rule concrete:
use the census before merging, and do not treat a fall in total SST count as a
promise of lower attach latency.

### Does a cold attach pay for the `index` subspace?

A production store measured 3.364 GB in `index` — 75.6M live entries, 99.6%
of the store — against ~13 MB across every subspace a reader touches, and
still attached slowly. Materialization scans `current` and point-reads
`sys`/`snapshot`, so by the segment routing nothing should read `index` at
all. This holds the reader-visible subspaces fixed and grows `index` alone,
timing the reader open and the first view apart so a cost names its phase.

| entries | `index` bytes | index SSTs | all SSTs | manifest bytes | open | first view |
|---|---|---|---|---|---|---|
| 0 | 0 | 0 | 5 | 5 KB | 0.51 ms | 0.19 ms |
| 65 536 | 2.4 MB | 8 | 33 | 183 KB | 0.54 ms | 0.25 ms |
| 262 144 | 9.5 MB | 20 | 45 | 1.3 MB | 0.68 ms | 0.31 ms |
| 1 048 576 | 37.8 MB | 20 | 54 | 8.0 MB | 1.24 ms | 1.00 ms |

**One channel exists, and it is small.** The manifest lists every SST in
every segment and is read whole before any segment routing applies, so an
open costs roughly **15 µs per SST across the whole store** — the index
segment included. An earlier revision of this sweep grew `index` to 37.8 MB
while holding SST count at one or two and measured a perfectly flat open,
which is the other half of the result: it is the SST *count* that reaches
the attach, never the bytes.

The constant is what matters for sizing. Measured on the store that
prompted this: every segment carries a single L0 SST and at most one sorted
run, ~10 SSTs across the whole store, 3.34 GB of which sits in one `index`
run. This term is ~0.2 ms there; it would take on the order of a million
SSTs to cost minutes. An attach that is slow against a large `index` is
therefore not slow *because of* it.

What remained was `current`'s **live** bytes — 12.8 MB on that store — and
what a read did with them, which turned out to be the whole answer.

SlateDB's `ScanOptions::default()` is `read_ahead_bytes: 1`, rounding up to
one block, with `max_fetch_tasks: 1`. Every whole-subspace scan took it, so
a materialization paid one object-store round trip per block. The
instrumented attach measured a single materialization — the view cache was
working, `materializations=1` — at **276.7 s** for 12.8 MB. That is
~46 KB/s, which is not a throughput figure at all: it is the latency of
~3 200 sequential 4 KB fetches, against 6.9 s of user CPU across the whole
833 s attach. Reading 4 MiB ahead with 8 fetches in flight is the fix; the
same scan costs 5 object-store reads instead of 89 on an in-memory store,
where the latency this hid behind is nil.

The order of the refutations is worth keeping. Dead versions were refuted by
a full-store merge reclaiming 0.08%; index bulk in the scan path by the
segment routing; index bulk via the manifest by ~15 µs per SST against a
store carrying ~10 of them; and repeated uncached materialization by an
attach that was still slow on a build carrying the cache. Every one of those
was a property of the *store*, and the cause was a property of how a scan
was configured to read it — which no measurement of the store could have
found, and which the attach named in one run once it was asked to report on
itself.

### Materialization cost vs. catalog size

This is the cost of a *cold* read — one whose handle holds no cached view.
A warm read serves from the projection cache and pays none of it. Median of
9 materializations, 8 columns and 16 files per table, a fresh handle per
repeat so no cache hit is timed:

| tables | live entities | median materialization | µs / 1 000 entities |
|---|---|---|---|
| 10 | 250 | 1.1 ms | 4 300 |
| 50 | 1 250 | 6.8 ms | 5 400 |
| 200 | 5 000 | 17.3 ms | 3 500 |
| 800 | 20 000 | 146 ms | 7 300 |

Materialization is roughly linear at ~5–7 µs per live entity — flush-interval
independent, since it is a read. A 20 000-entity catalog costs ~150 ms; a
100 000-entity catalog would cost most of a second. Readers now serve from
the cache rather than paying this per read; lazy materialization to bound
the cold cost on a large catalog stays open.

### Changelog replay vs. rematerialization

A cache that has fallen behind head can replay the `current` keys each
commit in the gap recorded, or rescan `current` outright. Both build the
same view, so the choice is pure cost, and moraine declines a replay whose
churn passes a share of the live catalog. This is where that share comes
from — the size backstop is lifted so the expensive side stays visible.
8 columns per table, 4 files per gap commit, median of 9:

| live entities | gap commits | churn | churn / entities | rescan | replay | speedup |
|---|---|---|---|---|---|---|
| 503 | 1 | 5 | 0.01 | 0.77 ms | 0.11 ms | 7.2× |
| 503 | 16 | 80 | 0.16 | 0.86 ms | 0.37 ms | 2.3× |
| 503 | 32 | 160 | 0.32 | 0.97 ms | 0.70 ms | 1.4× |
| 503 | 64 | 306 | 0.61 | 1.19 ms | 1.27 ms | 0.94× |
| 2 003 | 16 | 80 | 0.04 | 3.07 ms | 0.63 ms | 4.9× |
| 2 003 | 64 | 320 | 0.16 | 3.41 ms | 1.56 ms | 2.2× |
| 8 003 | 16 | 80 | 0.01 | 12.3 ms | 1.99 ms | 6.2× |
| 8 003 | 64 | 320 | 0.04 | 12.5 ms | 2.89 ms | 4.3× |

**The crossover is at a churn share of ~0.57**, interpolating the 503-entity
rows either side of parity — so the shipped backstop of half the live entity
count declines just before replay stops paying, which is the right side to
err on. Note the replay is *clone plus churn*, not churn alone: a view is
immutable and shared, so advancing one copies it first, and that copy is
linear in catalog size. That floor is what the 8 003-entity rows show at
~1.6 ms with a churn of five keys — still 6× cheaper than the rescan, but it
is why the speedup does not keep growing with catalog size at fixed churn.

### What the changelog costs the read path

The changelog first rode in the snapshot record. Measured there — one data
file per commit over an 8-column table, stats for every column — it grew
snapshot records **6.8×** (2 985 → 20 346 bytes over 64 commits) and slowed
their scan **~1.45×**. Snapshot records are what DuckLake re-reads at every
transaction and what moraine keeps a decoded projection of, so that is a
cost on the hot path for a benefit only a refresh takes. It lives in its own
`changelog` subspace now, and the same workload measures:

| commits | snapshot bytes | changelog records | changelog bytes | snapshot scan |
|---|---|---|---|---|
| 64 | 2 985 | 64 | 17 344 | 0.09 ms |
| 256 | 11 884 | 64 | 17 344 | 0.37 ms |
| 1 024 | 47 980 | 64 | 17 344 | 1.37 ms |

Snapshot records and their scan are back to exactly what they were without
the changelog, and the changelog subspace is flat: each commit deletes the
record 64 snapshots back, so a sliding window bounds it whatever the commit
count and nothing else has to reclaim it.

### What a warm read on a read-write handle has left

A read-write handle holds the writer epoch, so it resolves the head from
the view it already holds rather than reading `sys/head`, and skips the
`sys/migration` probe a scan it will not perform would need. What a warm
read does now is open a read session, take the held view, and release. The
session is the fence check, and it issues no store IO — `Db::begin` is a
closed-check plus a registration in SlateDB's transaction manager, under
that manager's global write lock.

A session was measured first and then removed, because it did not scale:
serving the view *through* one ran at 2.7M reads/s with a single reader and
fell to **519k** at 24, where the same reads without it held flat at ~27M/s.
That shape is the global write lock, not any work being done. A warm read
now checks the fence on the writer's status channel instead — a watch
borrow, which readers share.

Three series over one 50-table catalog: a warm read on the read-write
handle, the same on a read-only handle (which has no writer-local premise,
so it opens a session and issues both point reads), and the floor of
handing back the view with no check at all.

| threads | read-write µs | read-write /s | read-only µs | read-only /s | floor µs | floor /s |
|---|---|---|---|---|---|---|
| 1 | 0.12 | 8 118 267 | 6.36 | 157 263 | 0.05 | 22 178 604 |
| 2 | 0.70 | 2 844 847 | 8.70 | 229 925 | 0.09 | 22 244 096 |
| 4 | 0.85 | 4 718 678 | 13.86 | 288 599 | 0.18 | 22 494 405 |
| 8 | 1.80 | 4 434 621 | 25.81 | 309 965 | 0.33 | 24 125 416 |
| 16 | 3.52 | 4 544 039 | 51.96 | 307 900 | 0.68 | 23 627 536 |
| 24 | 5.07 | 4 736 871 | 45.95 | 522 321 | 0.98 | 24 421 714 |

One run per rung, so aggregate rates move a few tens of percent between
runs; the single-reader costs and the *shapes* are what reproduce.

**A warm read is ~0.1 µs and plateaus rather than degrades.** Against the
session it is 3× faster with one reader and **~9× at 24** (4.7M/s against
519k), and the curve flattens from four readers on instead of falling away.
The residue against the floor is the watch borrow's read lock, which shares.

**A read-only warm read costs ~70× a read-write one** — 6.36 µs against
0.12 µs, the most stable figure in the table. It cannot hold a writer-local
premise, so it opens a session and issues two point reads before it can
serve a cache hit. Folding the migration state onto `sys/head` would halve
that and is rejected on cost — it would put the store behind a format stamp
older binaries cannot open, by default, on the first commit after an upgrade
(RFC 0009). The next section measures what those two reads are worth in
round trips, which is the other half of why.

The absolute figures matter for reading a production trace. Even a
read-only read fully contended at 24 threads costs tens of microseconds. A
warm read measured in the hundreds of milliseconds is therefore neither of
these — it is IO the warm path no longer issues, or serialization above
moraine (DuckLake's own metadata connection is serialized; see RFC 0006).

### How many round trips a read-only read costs

The section above measures a read-only read at ~6 µs and attributes it to a
session and two point reads. That says nothing about *round trips*, and a
get has to cost something before round trips are visible — so this injects
per-GET latency and reads the answer off the ratio. 20 tables, median of 9:

| get latency | warm median | warm round trips | cold median | cold round trips |
|---|---|---|---|---|
| 2 ms | 0.02 ms | 0.01 | 13.26 ms | 6.63 |
| 5 ms | 0.02 ms | 0.00 | 25.44 ms | 5.09 |
| 10 ms | 0.02 ms | 0.00 | 45.45 ms | 4.54 |
| 20 ms | 0.02 ms | 0.00 | 85.49 ms | 4.27 |

**A warm read-only read issues no object-store GET at all.** 0.02 ms at
every injected latency, including 20 ms — so both point reads are served
from SlateDB's in-memory state, and the ~6 µs the section above measures is
CPU and lock, not IO. The `sys/migration` probe is a guaranteed *key* miss,
but a miss the filters answer in memory once they are resident; it is not a
round trip.

The cold column fits `4 × latency + 5.4 ms` at every rung — a constant four
GETs, which is the manifest and the SSTs a first materialization touches,
not the point reads. So a reader's two point reads are worth no round trip
warm and at most one of four cold. Overlapping them was implemented against
this measurement and reverted by it: there was nothing there to save.

### Read concurrency under IO latency

Does slow object-store IO starve the worker pool, so concurrent scans
serialize instead of overlapping? The in-memory store is wrapped in a
`ThrottledStore` adding 10 ms to every GET/LIST, seeded unthrottled, then K
concurrent `snapshot()` materializations run on a fixed 4-worker runtime:

| concurrency | batch | per op |
|---|---|---|
| 1 | 50.5 ms | 50.5 ms |
| 2 | 51.9 ms | 26.0 ms |
| 4 | 53.4 ms | 13.3 ms |
| 8 | 54.9 ms | 6.9 ms |
| 16 | 55.0 ms | 3.4 ms |
| 32 | 63.8 ms | 2.0 ms |

The batch stays flat as concurrency grows to 32 — ~8× the worker count — while
the per-op cost falls almost exactly 1/K. The IO awaits yield the worker back
to the pool, so the latency of 32 reads overlaps into the time of roughly one;
serialization would have made the batch grow to ~1.6 s. So object-store read
latency does not starve the pool, and a separate IO-dedicated runtime is
unnecessary on that axis. This isolates IO latency only; the next section
takes the compute axis.

### CPU-bound decode under worker pressure

The other half of the same question: a cold materialization decodes every
live record, and decode does not wait on anything — so does it hold its
worker long enough to crowd out a latency-sensitive call? This is the one
place a `spawn_blocking` discipline would earn its complexity.

No throttle here, so every millisecond is compute. K cold materializations
of a 400-table catalog (10 000 live entities) run concurrently on a fixed
4-worker runtime, with a small read on a one-table catalog queued alongside
them. Both catalogs are read through read-only handles, which maintain no
projection cache, so every read is a full materialization. Measured on an
**8-core Intel Xeon @ 2.90 GHz, x86-64** — a different machine from the rows
above, so compare the shape here, not the absolute numbers:

| concurrency | batch | per decode | small read alongside |
|---|---|---|---|
| 1 | 24.8 ms | 24.8 ms | 0.19 ms |
| 2 | 22.9 ms | 11.4 ms | 0.17 ms |
| 4 | 24.4 ms | 6.1 ms | 0.15 ms |
| 8 | 58.4 ms | 7.3 ms | 0.43 ms |

The same small read with an idle pool costs 0.11 ms.

Decode does not monopolize a worker. Four concurrent decodes cost what one
does (24.4 ms against 24.8 ms), so they genuinely overlap across the four
workers; the batch doubles only past the worker count, which is what CPU
work is supposed to do. The finding is the last column: the small read never
approaches a decode's duration. At 8 concurrent decodes — twice the worker
count — it costs 0.43 ms, ~4× its idle cost and ~1/50th of one decode. A
monopolized worker would have made it wait a whole decode.

So the decode's awaits yield often enough that a short call still gets
scheduled promptly, and a `spawn_blocking` discipline buys nothing on this
axis. Both halves of the worker-pool question are now answered the same
way: the pool does not need protecting from either IO latency or decode
compute.

### What an index probe fetches, cold and warm

The design replaced a part-grained object cache (4 MiB parts) with a
block-grained one, and argued the swap from grain arithmetic alone. This
puts bytes under it: 32 probes spread across the whole `index` run, so no
probe rides another's block, against a counting object store. On an
in-memory store a fetch costs no round trip, so the milliseconds are
decode and the **bytes** are the transferable number.

| entries | index bytes | cold gets | cold bytes/probe | warm gets | warm bytes/probe | warm ms/probe |
|---|---|---|---|---|---|---|
| 8 192 | 297 KB | 48 | 4 554 | 0 | 0 | 0.086 |
| 65 536 | 2.38 MB | 48 | 7 733 | 0 | 0 | 0.097 |
| 262 144 | 9.48 MB | 48 | 18 630 | 0 | 0 | 0.107 |
| 1 048 576 | 37.8 MB | 54 | 61 926 | 0 | 0 | 0.154 |

**The grain claim holds with room to spare.** A cold probe into a
37.8 MB run fetches 62 KB — against the 4 MiB a part-grained cache would
have faulted to answer the same lookup, a 68× difference, and the gap
widens as the run grows because a part is a fixed size while a probe's
block working set is not.

**A warm probe fetches nothing at all.** Zero bytes and zero GETs at
every size: the blocks are resident and the probe's own scan admits
them, which is what the block slot and the probe shape are for.

That row is the *second* reading. The first found a warm probe still
costing one to two GETs and 0.5–9.6 KB, which was not the cache failing
but `index_lookup` calling `materialize` instead of serving the handle's
held view — so every probe re-scanned `current` under a bulk shape that
admits nothing. Serving the held view took warm probes to zero and cut
the cold path too (79 GETs to 48, 71 KB to 62 KB per probe), because the
rescan was in both. The measurement found the defect; the numbers above
are after the fix.

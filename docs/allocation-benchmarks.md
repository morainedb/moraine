# Allocation benchmarks

These ignored unit tests measure snapshot construction, cached reads, index
queries, and index maintenance over real SlateDB with an in-memory object store.
They complement the [verb-commit benchmark](verb-commit-benchmark.md).

## Running

Run each test in a separate process with no other benchmarks, builds, or tests
running. The exact filter and single test thread isolate the measured workload:

```bash
CARGO_INCREMENTAL=0 cargo test -p moraine --lib --release --locked transaction::commit::tests::allocation::snapshot_refresh -- --exact --ignored --test-threads=1 --nocapture
CARGO_INCREMENTAL=0 cargo test -p moraine --lib --release --locked transaction::commit::tests::allocation::warm_reads -- --exact --ignored --test-threads=1 --nocapture
CARGO_INCREMENTAL=0 cargo test -p moraine --lib --release --locked transaction::commit::tests::allocation::index_lookups -- --exact --ignored --test-threads=1 --nocapture
CARGO_INCREMENTAL=0 cargo test -p moraine --lib --release --locked transaction::commit::tests::allocation::index_maintenance -- --exact --ignored --test-threads=1 --nocapture
```

The tests emit CSV rows prefixed with `ALLOC,`. Compilation and fixture setup
precede measurement. Each test checks the resulting snapshot, row IDs, or inserted
batch. Normal test runs skip these measurements.

The `stats_alloc` global allocator is confined to the Rust library's unit-test
executable; it also instruments ordinary tests in that executable. Production
libraries and the DuckDB extension do not install it. The tests live under
`transaction::commit::tests` to reach private refresh and materialization APIs
without adding a production API.

## Measurements

Times and allocations are arithmetic means per operation, except
`total_gets` and `total_puts`, which are totals over all measured samples.
`allocations` counts allocation and reallocation calls.
`allocated_bytes` is cumulative allocated bytes including reallocation growth,
not retained memory or peak RSS. CPU time covers the process; latency is elapsed
wall time. Both allocation and CPU counters include SlateDB background threads
during each interval, as well as measurement overhead.

For maintenance commits, `durable_us` comes from the commit durability event:
validation and the durable write, including flush scheduling. It is not just WAL
object PUT time. `non_durable_us` subtracts this interval from total latency;
`put_us` separately sums observed main-store and WAL-store PUT durations.
Read-only measurements have zero durability time. These in-memory-store results
do not predict remote object-store latency. Allocator instrumentation changes
absolute timing; use these numbers to compare scaling.

- **Snapshot refresh:** 16, 128, or 1,024 tables, each with four columns and eight
  registered files. Hold the base snapshot, update one table's statistics, and
  compare a full materialization with incremental refresh from that same base at
  the same pinned store head. Warm both paths, then measure nine constructions
  each. Result checks and destruction occur outside the measured intervals. Both
  results coexist until checked. The comparison covers actual refresh selection,
  including its catalog-size heuristic.
- **Warm snapshots:** after seeding 16, 128, or 1,024 four-column tables, retain a
  snapshot and read it 2,000 times. Every read must return that same shared view.
- **Index lookups:** seed a unique integer index with 1,024, 16,384, or 65,536
  entries, close durably, and reopen before querying. Run 32 spread-out point
  hits, 32 point misses, and 32 ranges returning 32 rows each. For each operation,
  repeat the same queries once to measure the warm pass. Operations share the
  reopened catalog, so “first” means that operation's first pass, not an empty
  cache. Query construction, result checks, and result destruction are included;
  initial key preparation is excluded. Object GET counts help distinguish
  cache activity from allocation work.
- **Index maintenance:** seed the same index sizes and append batches of 1, 64,
  or 1,024 entries by registering one data file per commit. Input entries are
  prepared outside measurement. Run three warmup commits and five measured
  commits, checking the complete inserted batch after each interval. `size` is
  the initial index size; it grows by eight batches per case. Measurements are
  per commit, not per inserted entry, and include catalog metadata and durability.
  These are ascending inserts; deletes, random insert positions, staged builds,
  and C++ allocations are outside this benchmark.

## Baseline before read optimizations

Results below were collected on September 8, 2026, on the workspace's Linux
x86-64 VM with Rust 1.96.1 release builds and imbl 5.0.0. Baseline raw output is in
[allocation-benchmarks-before-read-optimizations.csv](allocation-benchmarks-before-read-optimizations.csv).
The [latest output](allocation-benchmarks.csv) and the comparison below include
catalog-view reuse and the maintained entity count.

All KB and MB below are decimal. These are single-run scaling observations, not
statistical latency guarantees.

| Snapshot path | Tables | CPU | Latency | Allocation calls | Allocated |
| --- | ---: | ---: | ---: | ---: | ---: |
| Full materialization | 16 | 0.687 ms | 0.678 ms | 4,708 | 1.11 MB |
| Incremental refresh | 16 | 0.042 ms | 0.038 ms | 187 | 16.6 KB |
| Full materialization | 1,024 | 50.892 ms | 49.882 ms | 282,476 | 65.51 MB |
| Incremental refresh | 1,024 | 0.201 ms | 0.198 ms | 187 | 44.0 KB |

At 1,024 tables, replay allocated about 1,488 times fewer bytes and took about
252 times less elapsed time than rebuilding. At this point refresh was not constant in
catalog size: its selection heuristic called `live_entity_count`, which walked
the outer maps of nested entities. Copying shared tree paths also had
size-dependent costs. The baseline did not attribute all growth to either cause.

Warm snapshot reads returned the same shared view, with zero observed allocations
and zero GETs at every size. Their measured latency was approximately 0.06 µs
per call, including the pointer check and result drop.

| Warm query, 65,536 index entries | CPU | Latency | Allocation calls | Allocated | GETs |
| --- | ---: | ---: | ---: | ---: | ---: |
| Point hit | 305 µs | 284 µs | 709 | 202.5 KB | 0 |
| Point miss | 245 µs | 229 µs | 617 | 191.9 KB | 0 |
| Range returning 32 rows | 419 µs | 396 µs | 1,425 | 374.4 KB | 0 |

Warm query allocation totals were nearly identical at all three index sizes.
The first point-hit passes made 53, 74, and 76 GETs respectively across their
32 queries; every warm pass made zero GETs. This identified repeated cached index queries as an optimization
candidate. Code inspection subsequently found that query-only workloads did not
retain their materialized catalog view; the next section measures that fix.

| Initial index entries | Inserted per commit | CPU | Total latency | Durability interval | Allocated |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1,024 | 1 | 0.255 ms | 1.891 ms | 1.740 ms | 167.3 KB |
| 65,536 | 1 | 0.298 ms | 2.320 ms | 2.141 ms | 167.4 KB |
| 1,024 | 64 | 0.715 ms | 1.868 ms | 1.360 ms | 511.4 KB |
| 65,536 | 64 | 1.067 ms | 2.221 ms | 1.396 ms | 513.2 KB |
| 1,024 | 1,024 | 8.786 ms | 8.606 ms | 2.120 ms | 5.907 MB |
| 65,536 | 1,024 | 11.427 ms | 11.515 ms | 2.662 ms | 5.912 MB |

Maintenance allocations scale with the inserted batch and remain nearly flat as
the existing index grows 64-fold. CPU still rises with existing index size, so
this does not establish constant CPU cost. At 65,536 existing entries, the
1,024-entry commit uses about 67,367 allocation calls (66 per inserted entry).
Measured commits made no GETs and one PUT each; observed PUT service time averaged
only 3–6 µs, much smaller than the complete durability interval. This fixture
keeps the seeded index in the writer's memory tables; it measures warm append
maintenance, not probes against an uncached remote index.

All four isolated measurement tests and the complete local gate passed,
including 154 DuckLake integration tests and 173 SQL assertions.

## After catalog-view reuse and incremental entity counting

Index queries now install their materialized catalog view using the same
head stamp and invalidation epoch as snapshot reads. The epoch is captured
before opening the read session, so a late reader cannot replace a view installed
by an intervening commit. Equality, batched equality, range, and NULL lookups
reuse metadata while resolving index entries afresh in their read session.

The refresh heuristic now reads a maintained count for nested records plus
the flat maps' constant-time lengths. Snapshot mutators maintain the count
through insertions, replacements, removals, and cascades; staged statistics
deletions use those same mutators. A separate isolated assertion verifies that
counting 1,024 tables performs zero allocations.

The same release workloads were rerun after the complete gate, with no competing
builds or tests. Times remain single-run observations; allocation savings are
more stable than VM timing.

| Operation | Size | Allocated before → after | Calls before → after | CPU before → after | Latency before → after |
| --- | ---: | ---: | ---: | ---: | ---: |
| Warm point hit | 65,536 entries | 202.5 → 37.9 KB | 709 → 377 | 305 → 109 µs | 284 → 100 µs |
| Warm point miss | 65,536 entries | 191.9 → 27.2 KB | 617 → 285 | 245 → 74 µs | 229 → 68 µs |
| Warm 32-row range | 65,536 entries | 374.4 → 209.7 KB | 1,425 → 1,093 | 419 → 191 µs | 396 → 180 µs |
| One-change refresh | 16 tables | 16.6 → 16.0 KB | 187 → 182 | 42 → 34 µs | 38 → 33 µs |
| One-change refresh | 1,024 tables | 44.0 → 43.8 KB | 187 → 183 | 201 → 139 µs | 198 → 138 µs |

View reuse removes roughly 165 KB and 332 allocations per warm query in this
fixture. All warm query passes still made zero GETs. Refresh's entity-count scan
is gone, but overall refresh remains sensitive to catalog size through its other
work, including shared tree-path copying; it is not a constant-time operation.
Warm snapshots still showed zero allocations.

Index-maintenance allocation scaling remains effectively unchanged: the
65,536-entry index used about 167 KB for a one-entry append, 513 KB for 64, and
5.91 MB for 1,024. The [heap profile](index-maintenance-profile.md) explains the
roughly 66 allocation calls per inserted entry.

Run the count assertion separately:

```bash
CARGO_INCREMENTAL=0 cargo test -p moraine --lib --release --locked transaction::commit::tests::allocation::refresh_count_does_not_allocate -- --exact --ignored --test-threads=1
```

Validation passed: cache reuse for all four lookup forms, later index removal,
late-read installation after a commit, randomized counts across snapshot clones
and cascades, staged statistics deletion, all six isolated measurement/profile
tests, and the full local gate (including 154 DuckLake tests and 173 SQL
assertions).

## Probe optimization measurements

The next comparison starts from the read-optimized baseline, retained in
[allocation-benchmarks-before-probe-optimizations.csv](allocation-benchmarks-before-probe-optimizations.csv).
Physical key encoding now reserves its framed capacity, probe futures are
concrete, and eligible batches share transactional read setup. The
[allocation profile](index-maintenance-profile.md) describes the implementation
and its conservative fallbacks.

Two additional isolated release benchmarks cover those fallbacks:

```bash
CARGO_INCREMENTAL=0 cargo test -p moraine --lib --release --locked transaction::commit::tests::allocation::probe_read_batches -- --exact --ignored --test-threads=1 --nocapture
CARGO_INCREMENTAL=0 cargo test -p moraine --lib --release --locked transaction::commit::tests::allocation::index_maintenance_input_order -- --exact --ignored --test-threads=1 --nocapture
```

`probe_read_batches` compares 128 concurrent point reads with the shared reader
at one transaction snapshot over 32,768 persisted keys, after closing and
reopening the store. Dense and sparse hit/miss patterns each run one warmup and
five measured batches, with identical results checked. CSV costs are **per key**
(`samples = 5 × 128`); GET/PUT counts still cover all measured batches. The point
path runs first, and both paths are warm after the discarded pass.

`index_maintenance_input_order` commits 4,096 entries in ascending, descending,
and scattered order, starting with 1,024 existing entries. It discards one
warmup commit and measures five more, checking all inserted rows. CSV costs are
**per commit**. Scattered input permutes the same contiguous range of values;
it tests overlapping local-write spans, not remote sparse-key performance.
Neither benchmark measures cold remote-store latency.

The latest [main results](allocation-benchmarks.csv) and
[probe/order results](index-probe-benchmarks.csv) were collected without competing
builds or tests on the same VM. The comparison is against the immediately
preceding read-optimized run; timings remain single-run observations.

| Append into 65,536-entry index | Before | After |
| --- | ---: | ---: |
| 1,024 entries: allocation calls | 67,366 | 13,767 |
| Allocated bytes | 5.911 MB | 2.903 MB |
| CPU | 11.561 ms | 3.754 ms |
| Total latency | 12.034 ms | 3.759 ms |
| Durability interval | 3.060 ms | 2.373 ms |
| Non-durable latency | 8.974 ms | 1.386 ms |
| 64 entries: allocation calls | 4,525 | 1,220 |
| Allocated bytes | 512.6 KB | 337.2 KB |
| One entry: allocation calls | 382 | 382 |
| Allocated bytes | 167.4 KB | 177.6 KB |

For the 1,024-entry append, calls fell about 80%, bytes 51%, CPU 68%, and
elapsed time 69%. The measured commits still made no GETs and one PUT each;
PUT service remained only a few microseconds. Most of the elapsed reduction
is outside the durability interval. Calls and bytes remain nearly unchanged
across 1,024, 16,384, and 65,536 existing entries.

The concrete batched future carries more fixed state: one-entry allocation
bytes increased **10,256 bytes (about 6%)**, although the call count stayed at
382. This is a measured tradeoff of the batch implementation, not a claim that
every commit improved. Cached snapshots continue to allocate nothing.

| Warm persisted reads, per key | Point calls → batch calls | Point CPU → batch CPU | Point bytes → batch bytes |
| --- | ---: | ---: | ---: |
| Dense hits | 139.8 → 21.0 | 34.65 → 2.58 µs | 13.20 → 5.18 KB |
| Dense misses | 50.1 → 0.7 | 4.35 → 0.10 µs | 2.90 → 0.16 KB |
| Sparse hits | 140.2 → 140.7 | 33.78 → 34.92 µs | 13.20 → 14.55 KB |
| Sparse misses | 83.1 → 83.8 | 8.01 → 9.37 µs | 6.63 → 8.03 KB |

Sparse reads pay a bounded initial scan plus the concurrent point fallback;
they show modest additional calls and **10–21% more bytes** here. Avoiding that
initial scan when sparsity is known is a remaining optimization opportunity.

For 4,096-entry commits, ascending and descending order each used about
**13.2 calls and 2.72 KB per entry**, with CPU of 13.76 and 13.36 ms per commit.
Scattered order used **61.1 calls and 6.75 KB per entry**, with CPU of 32.69 ms.
The span guard disables sharing once ranges overlap earlier planned writes;
this preserves bounded scan-copy work but retains most point-read cost and
adds batching overhead. No before-change scattered-order measurement was taken,
so these order results are not a before/after performance claim.

Validation passed after these changes: all ten isolated release allocation,
benchmark, and profile tests; the DHAT workload's result checks; nightly format,
workspace Clippy, workspace tests, warning-free rustdoc, dependency/advisory and
pin checks; and the full end-to-end gate, including 154 DuckLake integration
tests and 173 SQL assertions. The workspace's core unit tests passed 688 tests
with 14 intentionally ignored tests, and its main integration suite passed 206.

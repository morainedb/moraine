# Verb commit scaling

This benchmark updates one table's statistics while increasing unrelated catalog
state. It measures the cost of preparing, grouping, persisting, and folding verb
commits with a warm catalog view. The retained pre-mutation snapshot forces the
implementation to preserve shared readers.

See [allocation benchmarks](allocation-benchmarks.md) for snapshot refresh,
warm reads, index queries, and index maintenance.

## Reproduce

```sh
cargo build --release --locked -p moraine --example verb_commit_bench
# unrelated tables, files per table, measured groups, members per group, flush ms
target/release/examples/verb_commit_bench 4096 8 60 1 0
```

The output is one CSV row with this header:

```text
tables,files_per_table,samples,group_size,flush_ms,total_us,cpu_us,durable_us,non_durable_us,object_put_us,allocations,allocated_bytes
```

For table scaling, use 0, 128, 1024, and 4096 unrelated tables, eight files per
table, and group sizes one and four. For file scaling, use one unrelated table
with 128, 1024, and 32768 files. Repeat the zero- and 4096-table single-member
cases with a 10 ms flush interval to expose the durability wait separately.
Run cases sequentially without competing builds or tests.

Each process seeds a fresh real SlateDB over an in-memory object store, warms up
five groups, then measures 60 groups. Every member updates the same target table
with a distinct record count. Setup and shutdown are excluded. No Parquet data
is read: these files are registered metadata. All numbers are means **per logical
mutation**, including in four-member groups, where one durable batch serves four
mutations.

- `total_us`: elapsed wall time over the measured loop.
- `cpu_us`: process CPU time, including SlateDB worker threads.
- `durable_us`: time inside the durable transaction commit, observed through the
  commit event's nanosecond field. This includes validation, WAL flush scheduling,
  and the object PUT; it is not pure storage-service latency.
- `non_durable_us`: total minus the durability phase, including preparation,
  folding, projection updates, and caller overhead.
- `object_put_us`: physical main-store PUT duration from the object-store tally.
  The default layout places the WAL there. Background PUTs, if any, also count.
- `allocations`: allocator calls plus reallocations across all process threads.
- `allocated_bytes`: newly allocated bytes plus reallocation growth, not peak
  memory or retained memory.

Global allocator instrumentation adds overhead to both versions. Process CPU and
wall time overlap and must not be added together. Zero configured flush interval
still incurs scheduler/timer delay; the 10 ms interval is not a fixed per-commit
sleep. These measurements isolate catalog scaling, not S3 or DuckDB performance.

## Measured comparison

All after measurements use **imbl 5.0.0**. Our regression tests reproduced a
correctness bug in 7.0.1: its diff can omit an updated record, which would omit
that record's write from a successful commit. This matches upstream
[issue #161](https://github.com/jneem/imbl/issues/161); proposed
[fix #166](https://github.com/jneem/imbl/pull/166) remains unmerged as of
2026-09-07. The [commit protocol notes](rfcs/0004-commit-protocol.md#mutation-sized-verb-preparation)
record the reproduction and upgrade constraint.

Environment: Amazon Linux 2023 x86_64 cloud VM, eight logical CPUs (Intel Xeon
2.50 GHz), Rust 1.96.1, release optimization, two Tokio worker threads. Baseline:
`1e38abf` with this benchmark harness and nanosecond commit telemetry added, before
persistent maps or changed-branch diffing. Both versions use identical workloads
and instrumentation. Raw measurements are alongside this report.

Raw data: [before](verb-commit-before.csv), [after](verb-commit-after.csv).

### More unrelated tables (eight files each, zero configured flush interval)

| Tables | Group | CPU ms before → after | Allocation calls before → after | Allocated MB before → after | Total ms before → after | Durability ms before → after |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 1 | 0.224 → 0.203 | 288 → 243 | 0.129 → 0.119 | 1.242 → 1.219 | 1.113 → 1.132 |
| 128 | 1 | 1.559 → 0.334 | 6815 → 257 | 1.819 → 0.129 | 2.512 → 1.346 | 1.131 → 1.219 |
| 1024 | 1 | 14.639 → 0.176 | 52606 → 258 | 13.752 → 0.130 | 15.176 → 1.209 | 1.161 → 1.110 |
| 4096 | 1 | 66.159 → 0.216 | 209590 → 261 | 54.655 → 0.138 | 65.349 → 1.219 | 1.172 → 1.091 |
| 0 | 4 | 0.069 → 0.064 | 157 → 122 | 0.100 → 0.093 | 0.325 → 0.319 | 0.283 → 0.282 |
| 128 | 4 | 1.353 → 0.067 | 5107 → 136 | 1.385 → 0.101 | 1.447 → 0.322 | 0.293 → 0.283 |
| 1024 | 4 | 11.969 → 0.078 | 39829 → 138 | 10.459 → 0.103 | 11.035 → 0.313 | 0.292 → 0.274 |
| 4096 | 4 | 59.370 → 0.072 | 158894 → 140 | 41.580 → 0.108 | 54.310 → 0.326 | 0.300 → 0.282 |

### More files in one unrelated table (single-member commits)

| Files | CPU ms before → after | Allocated MB before → after | Total ms before → after | Durability ms before → after |
| ---: | ---: | ---: | ---: | ---: |
| 128 | 0.447 → 0.208 | 0.268 → 0.119 | 1.306 → 1.222 | 1.006 → 1.117 |
| 1024 | 1.240 → 0.249 | 1.210 → 0.119 | 1.940 → 1.233 | 0.869 → 1.094 |
| 32768 | 46.581 → 0.199 | 34.603 → 0.119 | 46.361 → 1.249 | 1.530 → 1.123 |

### Durability wait control (10 ms flush interval, single-member commits)

| Tables | CPU ms before → after | Total ms before → after | Durability ms before → after | Outside durability ms before → after |
| ---: | ---: | ---: | ---: | ---: |
| 0 | 0.396 → 0.366 | 11.152 → 11.146 | 10.952 → 10.965 | 0.200 → 0.181 |
| 4096 | 63.451 → 0.416 | 68.276 → 11.071 | 5.618 → 10.864 | 62.658 → 0.207 |

The 4096-table single-member case reduces process CPU about 306×, allocated
bytes about 396×, and total latency about 54×. Grouping four mutations also
stays approximately flat instead of copying the catalog for each member.
With 32768 files in a single unrelated table, CPU falls from 46.6 ms to 0.20 ms.

This removes the catalog-size slope, not the durability floor. At a 10 ms flush
interval, nearly all final latency is the durability phase. The baseline's
variable preparation time intersects that timer at different phases, so its
measured durability wait is not constant across catalog sizes. These short
instrumented runs establish the scaling change; they are not tail-latency or
production throughput claims.

### Removing empty fallback maps

The initial structural-sharing implementation eagerly allocated empty maps
when diffing missing table-scoped records. Handling missing maps directly removes
about 155 KB allocated per mutation across the measured cases. No collection
version changed: both implementations use imbl 5.0.0.

The final measurements above include this cleanup, measured on 2026-09-08.
[Intermediate raw measurements](verb-commit-before-empty-map-cleanup.csv) preserve
the earlier implementation for comparison. Values below are per logical mutation,
with zero configured flush interval; KB means 1000 bytes.

| Unrelated tables | Group | Allocated KB before cleanup → after | Allocation calls before cleanup → after | Total ms before cleanup → after |
| ---: | ---: | ---: | ---: | ---: |
| 0 | 1 | 274.1 → 119.1 | 255.8 → 242.6 | 1.213 → 1.219 |
| 4096 | 1 | 292.7 → 137.9 | 273.4 → 260.8 | 1.241 → 1.219 |
| 0 | 4 | 247.9 → 92.9 | 135.5 → 122.4 | 0.332 → 0.319 |
| 4096 | 4 | 263.1 → 108.1 | 152.6 → 139.5 | 0.338 → 0.326 |

For zero unrelated tables, allocated bytes now fall below the original
non-sharing baseline: 119 KB versus 129 KB for a single-member commit, and
93 KB versus 100 KB per mutation in a four-member group. The previously reported
fixed allocation increase is eliminated in these cases. Single-member latency
remains approximately 1.2 ms and is dominated by the durability phase.

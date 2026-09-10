# Staged-build memory

The staged driver streams inline chunks in durable source order, decodes one
chunk at a time, and derives entries individually into its commit buffer. It
streams tombstones for the current row range and caches only the current inline
schema. Resume uses chunk identity and row offset, so row IDs need not be sorted
or monotonic between chunks. A checkpoint can skip a completed chunk without
reading its body.

The file leg walks the snapshot's shared file map without cloning all file
metadata, loads deletion state only for the current data file, and caps its
projected entry batches at the smaller of `BuildStep.entries` and 8,192 rows.

`BuildStep` bounds the commit buffer, not the entire heap. Derivation also holds
one Arrow chunk/batch, a pending entry or batch, schema projection, source-local
deletion state, and bounded store read-ahead: each inline source iterator
(chunks, tombstones, file deletes) reads ahead 256 KiB with two fetches in
flight, so an iterator holds at most about 512 KiB of undelivered blocks per
SST it is positioned in — a few MiB across the sorted runs of one table's
inline range, rather than the one 4 KiB block per round trip it fetched before.
An individual chunk, Parquet page, or file's deletion set can exceed a step.
Store caches, catalog metadata, WAL buffers, and the growing index also
contribute to whole-operation memory.

## Checkpoints and compatibility

The optional `IndexValue.build_inline_cursor` protobuf field records schema
version, insertion snapshot, chunk sequence, next row position, chunk/leg
completion, and the snapshot of the last committed step. Each update lands
atomically with its entries and checks the derivation snapshot inside the
commit closure. An interrupted build therefore resumes after committed work.

Inline steps do not advance the older row-ID watermark until the inline leg is
complete. An older binary ignores the new field and safely replays inline data
in its original row order; older definitions still resume by their row watermark.
No store-format bump or data rewrite is needed. Deferred maintenance reuses an
inline checkpoint only at its recorded head; after another writer advances the
head, it streams the live inline sources again to include preserved-ID updates.

## Telemetry

The `staged index backfill derived` event reports:

- `peak_buffered_entries`: entries in the commit buffer only.
- `peak_derived_entries`: source entries plus the commit buffer, including the
  pending inline entry or remaining projected file batch.
- `peak_inline_body_bytes`: largest retained inline body.
- `peak_inline_decoded_bytes`: largest decoded/projected inline Arrow batch.

Body and array storage can overlap; the byte fields must not be added together
as a heap estimate. The whole-operation benchmark below observes allocations
through the allocator, independently of these counters.

## Reproduce the whole-operation measurement

```bash
CARGO_INCREMENTAL=0 cargo build -p moraine --example staged_build_memory --release --locked
# chunks, rows per chunk, step entries, bytes per indexed string
target/release/examples/staged_build_memory 16 128 128 1024
target/release/examples/staged_build_memory 128 128 128 1024
target/release/examples/staged_build_memory 128 128 16 1024
target/release/examples/staged_build_memory 16 1024 128 1024
```

Run each case alone with no competing builds or tests. The example seeds a
real SlateDB store on the local filesystem, closes and reopens it, warms the
catalog metadata, and profiles the complete `create_index_staged` call: initial
definition, derivation, every commit, and publication. Result verification and
closing happen after the measured interval. The cache budget is 8 MiB and WAL
flushes run without a timer.

The example alone installs [dhat's allocator](https://docs.rs/dhat/0.3.3/dhat/),
which records allocations and frees during the profiler's lifetime. Production
and ordinary unit tests retain their existing allocators. `peak_heap_bytes` is
the maximum live size of allocations made during the measured operation,
including background store/commit work; `retained_heap_bytes` is the live size
at return, and `total_allocated_bytes` counts cumulative allocation traffic.
Pre-existing runtime/catalog allocations, native allocator overhead, and RSS
are outside these counters. Backtrace collection is disabled; allocator
instrumentation still affects timing, so elapsed times are diagnostic.

## Results

Collected September 8, 2026, on the Linux x86-64 workspace with Rust 1.96.1,
SlateDB 0.15.0, and dhat 0.3.3, using isolated release runs and 1,024-byte unique
string values. The baseline is the implementation in PR #196 with this same
measurement harness; fixture preparation and result verification are excluded
in both runs. Raw results: [before](staged-build-memory-before.csv) and
[after](staged-build-memory-after.csv). MB are decimal.

| Chunks × rows | Step entries | Inline bodies | Peak heap before → after | Heap retained at return, after |
| --- | ---: | ---: | ---: | ---: |
| 16 × 128 | 128 | 2.11 MB | 5.37 → 3.22 MB | 2.67 MB |
| 128 × 128 | 128 | 16.88 MB | 39.99 → 22.60 MB | 21.11 MB |
| 128 × 128 | 16 | 16.88 MB | 41.29 → 24.00 MB | 22.64 MB |
| 16 × 1,024 | 128 | 16.85 MB | 39.91 → 23.47 MB | 21.09 MB |

At 16,384 rows and a 128-entry step, whole-operation peak falls **43.5%**.
Keeping the same rows in larger chunks raises the new peak by about 0.87 MB,
consistent with retaining one larger source chunk. A 16-entry step still has a
higher whole-operation peak: more commits retain more store/catalog state, so
step size alone is not a total-heap ceiling. Most measured allocations live
past the build's return, including the resulting index and store buffers;
removing a full-table derivation vector does not remove that cost.

Cumulative allocations for the 128-chunk/128-entry-step case changed from
163.3 MB to 164.8 MB, while measured elapsed time changed from 1,069 to 1,091 ms.
The change shortens allocation lifetimes rather than eliminating all allocation
traffic; additional source/checkpoint work has a cost. These are single-run
observations with allocator instrumentation, not latency guarantees.

## Validation

The regression first failed because no step committed before a later malformed
chunk was decoded; it passes with streaming. Additional real-store tests cover
resuming inside a chunk with tombstones, skipping a completed chunk even when
its body cannot be decoded, out-of-order row IDs across schema versions, and
cancelling a running build after a committed checkpoint before resuming it.
The new cursor and enclosing index record have protobuf roundtrip/garbage
property tests.

The full local gate passed: pinned-nightly formatting, workspace Clippy,
workspace tests, warning-free rustdoc, dependency/advisory and pin checks, and
end-to-end validation including 154 DuckLake tests and 173 SQL assertions.
All four isolated before/after memory cases verified the completed index.

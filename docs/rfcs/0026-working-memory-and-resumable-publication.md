# RFC 0026: Working memory and resumable publication

- **Date:** 2026-10-22

## Resource boundaries

`CACHE_MEMORY` bounds estimated cache retention. It cannot bound all memory in a
DuckDB process. Catalog snapshots and caller-owned query results must remain
valid while readers hold them; staged transactions retain authored rows until
commit or rollback. DuckDB operators, Parquet decoding, allocator overhead, and
transport buffers have separate lifetimes.

Preserve indexes, Enterprise's commit-log integration, migrations, WAL relocation,
and checkpoints. Memory admission may refuse oversized work with an actionable
error; it must never truncate results or commit a partially accepted transaction.

## Snapshots and query copies

One catalog handle serializes cold head-view materialization. Waiters recheck the
held projection after admission and share the resulting immutable snapshot. Hot
reads keep their existing path. Time-travel materialization streams current and
history records into the requested view, retaining only visible records instead
of complete intermediate history vectors.

Revision memos reserve result storage before making a cache-only copy. A denied
reservation leaves the caller's result usable and skips the copy. Active reader
ownership and allocated cache containers remain charged as described in
[RFC 0009](0009-reader-consistency-and-caching.md).

These changes reduce duplicate allocations. They do not cap the final catalog
view or memory returned through existing owned `Vec` APIs. Bounding those
lifetimes requires a shared query allocator or tracked result handles; introducing
those cannot silently invalidate existing callers' data.

## Staged rows

`MORAINE_STAGING_MEMORY_BYTES` is a positive integer byte count, defaulting to
268,435,456 (256 MiB) per staged transaction. It accounts for the operation vector's
allocated capacity, cell vectors and strings, Arrow schema bytes, and retained
Arrow body lengths. Shared bodies are conservatively charged per operation;
slices may retain a larger allocation owned by their caller.

`StagedTransaction::try_stage` checks admission before retaining a row. A refusal
releases all authored rows and poisons the transaction: later stages and commit
fail. The existing infallible `stage` entry point retains that failure for commit;
the DuckDB ABI uses `try_stage` to report it immediately. Configuration and memory
errors avoid DuckLake's conflict-retry vocabulary. Rollback remains valid.

This is an authored-row allowance, not a bound on commit translation, index
maintenance, caller-owned input buffers, or SlateDB's write buffers. Large work
can raise the explicit allowance or use smaller transactions.

## Summary builds

`MORAINE_SUMMARY_MEMORY_BYTES` is an integer byte count of at least 64, defaulting
to 268,435,456 (256 MiB) per file. Builders admit at most `bytes / 64` physical rows,
using a conservative allowance for row IDs, sort buffers, membership, positions,
and encoding. Footer row counts are checked before reading the row-ID column;
actual decoded batches cannot exceed the admitted count. Oversized files are left
unpublished rather than retried as transient failures. Foreground position
resolution falls back to a batched row-ID scan, retaining only requested
positions and checking missing rows exactly; indexed SELECT/UPDATE/DELETE remain
available above the summary-build limit. This is an admission estimate, not allocator-level accounting. Parquet footers, compressed pages,
transport buffers, and allocator overhead still require headroom.

At most four construction or encoding tasks run process-wide; background row-ID
construction can hold at most two of those slots. Completed summaries waiting for
encoding or upload have separate lifetimes. CPU-heavy sorting and encoding run
on blocking workers.
Build permits follow the CPU task even when its awaiting future is cancelled, so
a timeout cannot admit replacement buffers while abandoned CPU work still runs.
Construction checks cancellation between phases and periodically while building
positions. A synchronous sort or library codec call cannot be forcibly preempted;
its work is bounded by the admitted file size.

Sparse high-word row IDs can choose sorted storage before constructing a Roaring
representation whose headers already outweigh the raw IDs. Repeated-row builds
release the first sorted copy before building their positional representation.
Sidecar continuation reads refuse lengths exceeding half the configured allowance;
a prefix that already covers the summary is decoded without copying it.

## Publication and warming

`RowSummaryProgress::new(tables, since)` records a selected table set and exclusive
snapshot cutoff. `advance(catalog, data_store, data_prefix)` processes at most four
files and returns whether the pass is complete. Callers must retain the same
catalog and data location for the progress object's lifetime. A caller can drop it
at any time; already published sidecars remain durable progress.

Files run concurrently but progress advances in key order. A cancelled step can
repeat at most its in-flight window. Failed file IDs are remembered separately,
up to 1,024 IDs, and retried without walking successful files again. If that set
fills, the earliest omitted failure preserves a scan frontier. This bounds retry
bookkeeping without losing failed work; overflow can repeat a suffix traversal.
A retry round refreshes the snapshot, allowing dropped files to disappear. Normal
steps hold one shared snapshot until their round finishes or progress is dropped.

The attach worker retains publication and per-table warming progress across
attempt timeouts and retry backoff. Successful table warms are removed from the
pending deque. New commit requests widen the table/cutoff selection and reset
progress, because that work belongs to a newer catalog state. Warming has eight
process-wide range slots, limiting its SlateDB pressure across attachments; this
is not priority scheduling inside SlateDB.

## Host memory

Use DuckDB's `memory_limit` and temporary-storage settings for its managed operator
and external-file buffers. Size that allowance together with Moraine's caches,
active staging, summary builders, snapshots, query results, and storage buffers.
Neither DuckDB's setting nor these Moraine allowances is a whole-process RSS cap.
A container or service memory limit supplies the final ceiling and can terminate
the process when reached. Staged rollbacks flush DuckDB's allocator, matching the existing large/failed
commit path; on glibc the host also trims the system heap. Allocator trimming can
return unused pages; it cannot reclaim live allocations and is not an admission
mechanism.

Validation must cover cancellation, failed-file recovery, early staging refusal
and rollback, concurrent snapshot loads, time travel, row-position invariants,
Enterprise compatibility, and ordinary/indexed SQL comparisons.

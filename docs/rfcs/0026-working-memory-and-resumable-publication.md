# RFC 0026: Working memory and resumable publication

- **Date:** 2026-10-22

## Contract

The extension remains in-process. Admission rejects oversized work with an
actionable error, and cancellation is cooperative. Results are never truncated;
refused staging or translation poisons the transaction so an accepted prefix
cannot commit. Indexes, Enterprise integration, migrations, WAL relocation, and
checkpoints remain available.

`CACHE_MEMORY` limits estimated cache retention. `MORAINE_WORKING_MEMORY_BYTES`
adds a separate process-wide allowance for admitted live working allocations,
defaulting to 1 GiB. Positive byte counts are required. Owners reserve before
known allocations and retain their reservation through use. Estimates include
conservative space for copies and container overhead; they are not allocator or
RSS measurements. A refusal releases temporary owners normally.

`MemoryReservation` supports reservation, resizing, and ownership transfer to
`Bytes` and Arrow batches. Clones, slices, nested Arrow buffers, null masks, and
Arrow C exports retain the allowance until the final buffer owner releases it.
`working_memory_usage()` returns reserved bytes and capacity. These APIs are
additive and leave existing owned-result APIs intact.

`set_memory_release(hook)` installs what the process does to return freed
pages to the kernel. The core calls it after a cold catalog view, a durable
commit, and a publication window — the phases that free in bursts — at most
once a second, on the thread that finished the phase. The host supplies the
call because it owns the allocator: the DuckDB extension installs glibc's
`malloc_trim`, and a platform with no equivalent leaves the hook
uninstalled, which skips the release. Admission accounting is unchanged by
it; a release returns pages the allocator was holding, not reservations.

## Snapshots and results

Cold head-view materialization is serialized per catalog. Snapshot generations
reserve their estimated metadata storage; held readers retain their generation's
reservation while a new generation is admitted independently. Time travel streams
visible records into its view. Store scan buffers and retained record-set keys
hold reservations through decoding and projection ownership.

`CatalogSnapshot::reserve_copy()` lets a caller admit metadata copies before
materializing them. Snapshot C exports use it. Catalog dump conversion reserves
row headers and string bytes. Index, inline, checkpoint, and maintenance arrays
also retain their allowances, and the paired C free function releases that
reservation. Exported IPC bytes retain admission through their paired free or
transfer into staging. Inline cursors admit their materialized row selection before constructing it.
Parquet row batches retain reservations through Arrow
buffer ownership, including slices held after the scan ends.

Legacy Rust methods returning ordinary `Vec` values preserve their signatures.
External callers own allocations made from those results and must retain a
reservation themselves when they need that storage included in admission. An
in-process library cannot account for arbitrary caller closures or copies.

## Staging, translation, and storage

`MORAINE_STAGING_MEMORY_BYTES` defaults to 256 MiB per staged transaction. It
accounts for operation-vector capacity, cells, strings, schemas, and retained
Arrow bodies. Staging also consumes the shared working allowance. Refusal drops
authored rows and poisons the transaction; rollback remains valid.

`MORAINE_COMMIT_MEMORY_BYTES` defaults to 256 MiB per commit session. Translation
scratch and the recorded write ledger consume that allowance and the shared
working allowance. Admission precedes each store write and ledger copy. A refused
write poisons commit even if a caller ignores the immediate error. Submission
transfers the recorded ledger rather than cloning it. Reservations follow
coalesced members and commit submission through acceptance.

`MORAINE_STORE_MEMORY_BYTES` configures SlateDB's per-writer logical unflushed
threshold, defaulting to its existing 1 GiB. The SST freeze threshold is at most
half this setting. Starting a transaction at an already full recorded threshold
is refused; SlateDB also applies its own write backpressure. The gauge is
asynchronous and concurrent admitted batches can cross the threshold. It is not
an exact allocator cap; host headroom must include those batches, compaction,
transport, and allocator overhead.

## Parquet and summary work

Object-store range and prefix requests reserve their requested byte counts before
fetching. Returned bytes retain the reservation. Disk-cache byte records admit
incremental vector growth. Parquet readers retain footer/metadata allowances and
reserve four times the largest selected row group's projected uncompressed size
before page decoding. Input ranges and retained output batches are charged
separately. These estimates can refuse a very large row group even for a narrow
selection; rewrite with smaller row groups or raise the working allowance.

`MORAINE_SUMMARY_MEMORY_BYTES` defaults to 256 MiB per file and must be at least
64. Builders admit at most `bytes / 64` physical rows and reserve that estimate
against the shared working allowance before collecting row IDs. Oversized summary
builds leave the file unpublished. Foreground indexed reads and mutations can
still resolve requested positions by streaming row IDs.

At most four summary construction/encoding tasks run process-wide; background
construction occupies at most two. Blocking tasks retain their permits and
reservations after cancellation of their waiter. Sorts process runs and merges
with checkpoints every 4,096 entries. Encoding checks cancellation between small
writes, and packed positions use an 8 KiB scratch buffer. Library codec calls and
individual native operations complete before cancellation can be observed; there
is no forced interruption or hard cancellation deadline.

Index encoding cancels queued workers when their caller is dropped. Running
workers retain their input-memory allowance and CPU permit until the synchronous
call returns, even if the scan or transaction has already been cancelled.

## Publication and warming

`RowSummaryProgress::new(tables, since)` traverses an attach/backfill selection.
`RowSummaryProgress::for_files(files)` publishes exact registered file IDs without
catalog traversal. Each `advance` processes at most four files. Failed IDs retain
1,024 resident entries and spill the rest to an anonymous temporary file. Retry
rounds consume those IDs, never a suffix of successful files. Temporary-file
errors are reported without advancing the affected frontier. A cancelled step
can repeat its in-flight window. Spools disappear when progress is dropped.

`CommitReport::registered_files` contains final rebased table/file IDs from the
committed writes. The extension queues independent commit passes using those
IDs. New commits never reset a running pass's cursor or completed warms. Failed
passes rotate behind queued work; attach backfill has separate worker slots.
Each pending lane admits at most 128 passes. Queue or memory refusal logs a
warning and skips optional background work; it does not turn an already durable
commit into a failure. Files remain readable and explicit publication/backfill
can publish skipped summaries. Successful table warms are removed from each
pass's deque. New commits can warm that table again for their newer data.

## Host configuration

DuckDB retains its configured `memory_limit` and temporary-storage settings.
Setting `MORAINE_HOST_MEMORY_BYTES` enables a pre-attach check of DuckDB's current
managed limit, Moraine's working allowance, the requested cache allowance, one
writer's storage allowance, and native headroom. Headroom defaults to one quarter
of the host envelope; `MORAINE_HOST_HEADROOM_BYTES` overrides it. Attach fails
before opening the store if these allowances overlap. The core
`validate_host_memory(duckdb_bytes, other_bytes)` also supports embedders supplying
their aggregate cache/store allowance.

The embedder must include other database instances and stores in its aggregate
allowance or headroom and revalidate after raising DuckDB's limit. Set these
environment variables before starting the process. This check does not sample
RSS, intercept every native allocation, or protect against understated Parquet
metadata. A process/container ceiling remains the final failure boundary; leave
headroom rather than sizing the engine and Moraine independently to that ceiling.

Validation covers ownership after eviction/drop, Arrow slices, atomic refusal,
cooperative cancellation, retry spill/order, preserved publication progress,
native SQL admission failures, and Enterprise compatibility.

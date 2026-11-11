# RFC 0012: Testing

## Summary

Tests cover commit atomicity, crash recovery, writer fencing, and DuckLake
SQL compatibility. Core tests use real SlateDB on an in-memory object store.
Integration tests also run against DuckDB, RustFS, and AWS S3.

## Guarantees

- A DuckLake transaction is one SlateDB `WriteBatch`, durable when its
  write-ahead-log object lands. A crash leaves the catalog before the commit
  or after it.
- Operations too large for one batch (a staged index build, reclaiming a
  dropped index's entries, a format migration) commit their progress with
  each batch and resume from it.
- One writer per store. SlateDB gives the writer role to the newest
  read-write open; the previous writer's next write fails with `Fenced` and
  lands nothing. Read-only attaches never take the role.
- A commit whose outcome could not be observed is reported as
  `CommitOutcomeUnknown` and never retried automatically, because a retry
  could apply it twice.

## Crash recovery

`CrashCase` in `crates/moraine/src/fault.rs` names every staged crash, and
the suite matches on it exhaustively, so a new variant fails to compile
until a test drives it. Each test stops the process without a close,
reopens the store on a fresh handle, and asserts:

| Case | After reopen |
|---|---|
| flush never landed | head unchanged, no trace of the commit |
| flush landed, caller never told | head advanced; a re-drive hits `AlreadyExists` or `CommitConflict` rather than landing twice |
| drop of a table, its columns, and its files | wholly present or wholly gone |
| group commit of several transactions | all members landed or none |
| writer A dies mid-commit, writer B opens | B reads a consistent head and continues |
| a fenced writer wakes and commits | `Fenced`; the store is byte-identical to the timeline in which it never woke |
| genesis interrupted, or two processes creating the same catalog | one catalog, created whole, or an untouched store |
| staged index build interrupted | the index serves no reads; a re-run resumes from its cursor |
| entry reclamation interrupted | a re-run finishes the rest; a further pass finds nothing |
| migration interrupted at any of its four seams | `open` refused until `migrate` resumes; every record moves once |

The harness uses four methods:

- Drop a writer handle without `close`. SlateDB does not flush on drop, so
  reopening tests recovery from the last durable write.
- Wrap the object store in `FreezingStore`, which allows a counted number of
  writes and rejects the rest. Tests vary the allowance from zero to the
  operation's total write count to interrupt multi-table drops, group commits,
  and catalog creation at every write boundary.
- Open competing writers through SlateDB to test fencing.
- Inject an error at a migration batch boundary. These hooks are enabled only
  in tests and builds with the `fault-injection` feature.

## Writer fencing

`crates/moraine/tests/it/catalog.rs` checks that a second read-write open
fences the first with a typed error and that a read-only attach does not
fence a live writer. `test/sql/commit_concurrency.test` runs two DuckLake
transactions over one lake from two connections, so DuckLake's own conflict
loop runs and the loser gets an error DuckLake treats as retryable. The
[benchmark harness](../benchmarks.md#concurrent-writers) launches several
writer processes against one lake and records writer-fencing errors.

## Fuzzing

Three `cargo-fuzz` targets run nightly with a corpus kept across runs.
`key_decode` and `record_decode` check that arbitrary bytes decode to a key or
record or return an error without panicking, including a key from one subspace
paired with another subspace's value. `crash_recovery` builds a real store,
freezes it at an offset the fuzzer picks, reopens, and asserts the commit landed
whole or not at all. Every encoder in `store` also has a proptest round trip.

## Against real DuckDB

`cargo xtask e2e` compiles DuckDB at the pinned release, builds the
extension with its bundled DuckLake, loads it into the resulting CLI, and
runs about two hundred tests in `crates/moraine-duckdb/tests/ducklake_load/`:
DDL and schema evolution, deletes, time travel, the change feed,
partitioning, every DuckLake type, inlining, indexes, maintenance,
migration, cancellation, and a kill between a Parquet write and its
commit. Many are differential: the same statements run against a stock
DuckDB-file DuckLake catalog and the results must match row for row. Seven
sqllogictest files run under DuckDB's own runner for cases that need two
connections on one instance. `cargo xtask s3` runs the core against
RustFS, and a scheduled workflow measures commits and attaches against
same-region AWS S3. Each published build is loaded into the stock DuckDB
CLI it names, on every platform it ships for.

## Not covered

No test kills a process mid-PUT against a real endpoint; the crash cases
use the in-memory store. There is no formal model of the commit protocol.
[Crash recovery](0008-crash-recovery.md) lists the cases and expected results.

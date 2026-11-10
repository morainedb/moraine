# RFC 0008: Crash recovery

## Summary

A crash is a process stop without a close, followed by a reopen on a fresh
handle. Every state-changing path in moraine is either one SlateDB write
batch or a sequence of batches with durable progress, so every crash falls
under one of two guarantees: atomicity or resumability. This document
enumerates the `CrashCase` variants, states what must hold after reopen for
each, and describes how the integration suite stages each crash against
real SlateDB on in-memory `object_store`.

## Definition of a crash

The durability boundary is the write-ahead log flush to object storage. A
crash stops the process at a chosen point with no graceful close. A reopen
constructs a fresh moraine handle over the same object store, with no
in-memory carryover from the process that died. Every case below is stated
as a pre-crash state, a crash point, and an assertion that runs against the
reopened handle.

## Two guarantees

Which guarantee covers a case follows from the shape of the path. A path is
either one batch or several.

**Atomicity.** One commit is one SlateDB `WriteBatch`, and one batch is one
durable flush. A crash leaves the whole batch or none of it, so there is no
torn state inside a commit. The only two observable outcomes are that the
flush landed or that it did not. This covers the ordinary commit, a drop
that ends many records, the group commit, genesis, and writer takeover.
Takeover adds no torn state of its own. It changes which process observes
the all-or-none result.

**Resumability.** Some operations are too large for one batch: a staged
index backfill, the reclamation of a dead index's entries, and a structural
format migration. Each runs one batch per commit so a large operation never
holds the writer, and so the operation as a whole is not atomic. Each batch
is atomic, and the operation persists its progress in the same batch as the
work, so a re-run continues from the durable cursor. It does not restart, it
does not double-count, and it does not serve reads from a half-built state.

## Cases

The `CrashCase` enum in `crates/moraine/src/fault.rs` names every case. It
compiles under `test` and under the `fault-injection` feature. The
integration suite matches on it exhaustively to assign a guarantee and to
confirm coverage, so a new variant fails to compile until both are decided.
`CrashCase::ALL` lists the cases in the order the suite drives them.

### Atomicity cases

| Case | Operation interrupted | After reopen |
|---|---|---|
| `CommitNotDurable` | A commit's batch is staged and its WAL flush never reaches object storage. | `sys/head` is unchanged. No record of the commit is visible. The commit that landed before it is untouched. |
| `CommitDurableNotAcknowledged` | The flush landed and the process died before the caller was told. | `sys/head` has advanced and the new snapshot resolves fully. A re-drive of the same logical operation runs against the advanced head. See the re-drive scope below. |
| `MultiTombstoneDrop` | A drop that ends the table row, every column, and every data file record at once, stopped at every write the drop issues. | The table is wholly present or wholly gone. No reachable point exists after the first tombstone and before the last, because the whole drop is one batch. The case fails if any code path splits a drop across batches. |
| `GroupCommit` | Several catalog commits staged into one batch, stopped at every write the group issues. | Every member is committed or none is. The head never stands at a member other than the last, and a group is never missing its tail. |
| `TakeoverMidCommit` | Writer A dies mid-commit and writer B opens the store, bumping the SlateDB writer epoch. | B reads a consistent head. If A's flush landed, B sees the advanced head and continues from it. If not, B sees the prior head and A's batch is invisible. |
| `FencedWriterResumes` | Writer A wakes after B has taken over and attempts its commit. | A returns `Error::Fenced` and writes nothing. The object store is byte-identical to the timeline in which A never woke. |
| `GenesisInterrupted` | An initializer dies partway through creating the catalog, stopped at every write genesis issues. | The store is untouched, so the next open creates the catalog from scratch, or the catalog is wholly created. A `sys/format` with no head is never observed. Genesis is one batch like any commit. |
| `ConcurrentGenesis` | Two processes create the same empty catalog at once. | At most one genesis lands and reopen shows it. There is never a second `sys/format`, a divergent genesis snapshot, or a conflicting head. A loser adopts the winner's catalog, re-attempts, or fails typed. See the genesis section below. |

### Re-drive scope after an unacknowledged commit

`CommitDurableNotAcknowledged` asserts consistency of the landed commit and
guard-surfacing on re-drive. Identifiers never collide, because counters
advanced with the landed commit. A guarded operation surfaces its guard: a
re-driven create returns `Error::AlreadyExists`, and an operation whose
premise moved returns `Error::CommitConflict` or `Error::NotFound`. A
data-only re-drive, such as `register_data_file` of the same file or an
identical inline insert, is a fresh commit and lands a second time with
dense, disjoint row id ranges. Nothing in the protocol deduplicates it. The
caller-facing form of this outcome is `Error::CommitOutcomeUnknown`, whose
handling is specified in [the commit protocol](0003-commit-protocol.md).

### Resumability cases

| Case | Operation interrupted | After reopen |
|---|---|---|
| `StagedBuildInterrupted` | A staged index backfill, some steps committed and the build unfinished. | The index is still `IndexState::Building` and serves no reads: a lookup returns `Error::IndexBuilding`. The rows already backfilled are not served. Its `build_cursor` is durable, so re-issuing the build resumes from the watermark. Entries re-derived below it land as idempotent puts, and the finished index covers every row once. |
| `ReclamationInterrupted` | Reclamation of a dead index's entries, some batches committed and others not. | The committed batches stay reclaimed and the remaining entries survive to be found again. A re-run finishes them without re-reclaiming what is gone, and a further pass reclaims nothing. Live indexes and data files are untouched. |
| `MigrationInterrupted` | A structural format migration, crashed at `CrashPoint::AfterStart`, `AfterStep`, `BeforeFinish`, or `AfterFinish`. | While the migration marker is present, `Catalog::open` is refused with `Error::Migration`. `Catalog::migrate` reads the marker, resumes the named unit from its durable cursor, and finishes. Every record is moved once. After the finish batch the store is stamped the target format and the marker is gone. The flip and the clear land in one batch, so a new-format store with a marker is never observed. |

Those four points are the only seams a migration has. A step's new-key
write, its old-key delete, and its cursor advance share one batch, so there
is no durable intermediate between them. The format record and the
migration marker are specified in [the storage format](0001-storage-format.md).

## Genesis

An empty store has no `sys/head` to conflict on, so concurrent genesis is
guarded by SlateDB's writer epoch across processes and by write-write conflict
detection on `sys/format` within one writer. An open that is fenced while
creating the catalog re-attempts, up to `GENESIS_ATTEMPTS`, which is 3. Nothing
of the fenced attempt reached the store, so the re-attempt adopts the catalog
the winner created or creates one itself. The bound gives termination: every
open takes the writer epoch by a manifest compare-and-swap, so two initializers
could displace each other indefinitely. Only genesis re-attempts. An open that
finds a catalog stages nothing, so a fence anywhere else reaches the caller. A
loser fails typed by one of two guards. If it lost the race to create the
store's first manifest, it never attached, and the error is `Error::OpenRaced`:
nothing was written, and opening again adopts the winner's store. If it created
the store and was displaced afterwards, the error is `Error::Fenced`.
`OpenRaced` is classified by matching SlateDB's message text, because a lost
compare-and-swap and a damaged manifest arrive as the same error kind. The suite
stages a real lost compare-and-swap and pins the wording, so a SlateDB upgrade
that rewords the message fails there.

## Data-file bytes

moraine never writes or deletes a data file. DuckLake's writer encodes the
Parquet and registers it through an ordinary commit, and physical deletion
goes through DuckDB's filesystem, so the ordering of bytes around a commit
is the engine's contract. Bytes are written before the record that
references them and deleted after the record that referenced them is gone.
moraine's half is that the registering commit is one batch and that a
`GcFile` schedule record stays durable until the commit that forgets it
lands. The ordering itself is tested end to end in
`crates/moraine-duckdb/tests/ducklake_load/crash.rs`, which kills a DuckDB
process between the Parquet write and the registering commit and asserts
that the bytes are left orphaned on disk and that no catalog row references
a file that was never written.

## Crash harness

The suite produces a crash three ways. None needs `unsafe`, a fork, or
corruption of SlateDB internals, and no case needs a production knob.

**Dropping the handle.** Where the instant of interest is after a durable
flush, as in `CommitDurableNotAcknowledged`, the harness drops the writer
handle without calling `close` and opens a fresh one. SlateDB's `Db` has no
`Drop` implementation, so releasing the handle flushes nothing.

**Freezing the store.** To observe the flush that did not land, the harness
wraps the in-memory store in `FreezingStore`, a decorator that permits a
bounded number of writes and then refuses every write. Everything written
before the freeze stays durable, nothing after it lands, and reads keep
working so the operation fails on the write it could not persist. The
allowance is a write count. A test measures the writes an operation issues
on an unfrozen run, then sweeps the allowance from zero to that number and
asserts the invariant after each. This is how `MultiTombstoneDrop`,
`GroupCommit`, and `GenesisInterrupted` cover every durability boundary of
the operation. A sweep also asserts that at least one round was
interrupted. A frozen operation never returns, because SlateDB retries a
failed object-store write indefinitely. The harness bounds its own wait and
treats the absence of a success report as the crash.

**Fencing.** `TakeoverMidCommit`, `FencedWriterResumes`, and
`ConcurrentGenesis` need no simulation. SlateDB's writer fencing is the
behavior under test: every `Db::open` increments the writer epoch by a
manifest compare-and-swap, and a writer below the current epoch is fenced
on its next SST or WAL write. The compare-and-swap goes through the same
object-store interface as every other write, so the fence holds on
in-memory `object_store` as on real storage. The harness opens writer A,
opens writer B over the same store, asserts B's view, then has A attempt
its commit and asserts `Error::Fenced` with no bytes written. The genesis
re-attempt and the lost-manifest race are staged from outside with
`RacingStore`, which refuses one contested write: the store's first
manifest write for `OpenRaced`, or the first WAL object carrying a batch
for the mid-bootstrap fence. The fence is SlateDB's own in both.

**Migration seams.** `MigrationInterrupted` is the one case crashed from
inside a library call, because its batch boundaries are internal to
`Catalog::migrate`. The driver calls `crash_seam` at each `CrashPoint`.
Under `test` or the `fault-injection` feature, `inject_crash` arms one
point, which errors once and disarms. A production build carries an empty
function. Every shipped format is additive, so the driver's registry is
empty. `install_migration` installs a `SyntheticMigration` unit into the
registry the shipped planner reads, so the case runs through
`Catalog::migrate` and the planner that ships.

## Durability wait

A commit's durability wait is unbounded in production. A store that refuses
writes permanently, through expired credentials or a revoked bucket policy,
stalls the writer. A deadline would be worse: abandoning the wait does not
undo the staged batch, because the flush continues beneath the caller. The
deadline would report failure for a commit that still lands, and a caller
re-driving it would apply it twice. `CommitDurableNotAcknowledged` is safe
only because nothing reports the commit as failed. moraine instead makes
the stall visible.
`reporting_stalls` in `crates/moraine/src/transaction/commit.rs` logs a
warning every `STALL_INTERVAL`, 10 seconds, naming the operation, the
seconds waited, and the staged bytes. It never gives up on the write.

## Tests

- `every_case_declares_its_guarantee_and_coverage` iterates `CrashCase::ALL` and
  requires each case to name its guarantee and be driven.
- One test per case in `crates/moraine/tests/it/crash_recovery.rs` builds the
  pre-crash state, crashes, reopens, and asserts the case's invariant.
- Idempotence cases re-drive after reopen and assert convergence with no id
  collision and no double-counting.
- Absence cases sweep the write allowance across every durability boundary of
  the operation.
- The `crash_recovery` fuzz target in `fuzz/fuzz_targets/` freezes the store at
  an arbitrary write offset and asserts a commit landed whole or not at all.
- `a_kill_before_the_commit_orphans_the_parquet_it_had_already_written` in
  `crates/moraine-duckdb/tests/ducklake_load/crash.rs` kills a DuckDB process
  between the Parquet write and its commit.

## Alternatives

- **A bounded durability timeout.** Rejected. Abandoning the wait does not stop
  the flush, so a timeout reports failure for a commit that may still land, and
  a re-drive applies it twice.

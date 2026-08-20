# RFC 0011: Crash recovery

- **Date:** 2026-07-09

## Summary

RFC 0001 mandates that integration tests cover "crash-shaped sequences
(commit, reopen, verify)," and RFCs 0004 and 0007 each restate a slice of
that obligation in their own words. "Crash-shaped" is the right instinct but
says neither *where* a process may die nor what must be true once it
restarts. This RFC replaces the prose with a named list of the places a
process can die during a state-changing operation, each paired with the one
thing that must hold after it reopens. Every protocol RFC then cites a case
here instead of carrying its own crash story, and the integration suite
drives the list as data rather than relying on whichever scenarios an author
happened to imagine.

## Goals

- Name every point at which a process can die during a state-changing
  moraine operation — exhaustive over the paths the protocols expose, not a
  representative sample.
- For each, state the single invariant that must hold after reopen, in terms
  an assertion can check against real SlateDB on in-memory `object_store`
  (RFC 0001's integration tests, no store mocks).
- Give each case a stable identifier so tests drive them as data and a new
  interruptible path cannot be added to the code without a corresponding
  case failing to exist.
- Consolidate: RFC 0004's "partial batch never observable" becomes cases
  here, cross-referenced rather than duplicated, as does the catalog half of
  RFC 0007's "cleanup idempotence".
- State the boundary: which crash-recovery obligations are the embedding
  engine's rather than moraine's, so their absence here is legible.

Non-goals:

- **New protocol.** This RFC invents no mechanism. It tests the atomicity,
  fencing, and resumability invariants already specified in RFCs 0002–0008.
  If a
  case here cannot be made to pass, the bug is in the referenced RFC's
  implementation, not in this one.
- **Fuzzing / randomized fault injection.** The list below is enumerated and
  deterministic, and stays the floor. The complementary layer is built and
  lives in the fuzzing tier (RFC 0001) as the `crash_recovery` target: it
  picks the write offset to freeze the store at, so a commit dies at
  boundaries no named case enumerates, and asserts the atomicity guarantee
  on reopen. It runs at single-digit executions per second — each one
  builds and reopens a real store — so it is breadth over a long horizon,
  not a gate.
- **Real-object-storage crash behavior** (MinIO/localstack, torn multipart
  uploads). RFC 0001 lists real-object-storage tests as a later tier; these
  cases run on in-memory `object_store`, where the crash seam is the store
  handle, not the network.

## What a crash is

The durability boundary is the WAL flush. "Crash" means: stop the process at
a chosen point without a graceful close, then reopen the store from object
storage and assert. "Reopen" always means constructing a fresh moraine
handle over the same object store, with no in-memory carryover from the
process that died.

## Two guarantees

Every case below is an instance of one of two guarantees, and which one
applies is decided entirely by whether the path is a single batch or several
steps. That is the whole structure: the guarantee follows from the path, so
there is nothing to cross-tabulate.

**Atomicity — one commit is one `WriteBatch` is one durable WAL flush**
(RFC 0002, "Atomicity invariant"; RFC 0004, step 4). SlateDB batches are
atomic: a crash leaves either the whole batch or none of it. So *within* a
commit there is no torn state to find, and the only two observable outcomes
are "flush landed" and "flush did not." This governs the ordinary commit,
the multi-tombstone `DROP`, the group commit, genesis, and writer takeover —
takeover adds no torn state of its own, it only changes *who* observes the
all-or-none result.

**Resumability — a long operation is several batches, and remembers how far
it got.** Some work is too large for one batch: a staged index backfill, the
reclamation of a dead index's entries, and a structural format migration.
These run one batch per commit so a large operation never holds the writer,
which means they are genuinely *not* atomic as a whole and a crash can land
between batches. What carries
them is that each batch is atomic and the operation persists its own
progress, so a re-run continues from there — never restarting, never
double-counting, and never serving reads from a half-built state.

The asymmetry matters and is easy to miss. A multi-tombstone `DROP` is safe
by atomicity: there is no point to crash at, so the test's job is to prove
that absence. A staged backfill is safe by resumability: it has many such
points by design, and every one of them must be recoverable. Testing them
the same way would miss that one of them has no crash point at all.

### What moraine does not own

The obvious third guarantee — bytes written before the record that
references them, deleted after that record is gone, so a crash yields an
orphan rather than a live dangling reference — is real, but it is not
moraine's. moraine never writes or deletes a data file. DuckLake's writer
encodes the Parquet and registers it through the ordinary file path (RFC
0005), and physical deletion goes through DuckDB's filesystem; RFC 0007
considered and **rejected** a moraine-side expiry engine issuing
object-store DELETEs, precisely to avoid a second policy implementation.

So moraine's half of that contract is exactly the two guarantees above: the
commit that creates a reference is one batch, and a `gcfile` schedule row
stays durable until the commit that forgets it lands. The ordering of the
bytes around those commits belongs to the engine, and a crash between an
engine PUT and its commit can only be staged where both run — the e2e tier,
not here. This RFC does not enumerate those cases; it states the boundary so
their absence is a decision rather than an oversight.

That boundary is covered where it lives. The e2e suite kills a DuckDB
process between DuckLake's Parquet write and the commit that would register
it, then asserts the direction the contract runs in: the bytes are left
**orphaned** — on disk, referenced by nothing — and never the reverse, a
live catalog row pointing at a file that was never written. An orphan
wastes space until cleanup reclaims it; a dangling reference is unreadable
data. moraine's half of that test is the atomicity above, which is why the
uncommitted insert leaves no trace in the catalog at all.

Compaction sits entirely on the engine's side of that line, and introduces
no seam of its own. RFC 0008 makes it an ordinary snapshot-minting commit:
DuckLake writes the new Parquet, then one metadata transaction that lands as
one `WriteBatch` — PUT-then-batch, the shape already above. It allocates no
row ids, carries no cursor, and spans no second batch, so it adds no
resumability case; and RFC 0008 explicitly **rejected** deleting superseded
bytes inside the commit, so it adds no delete ordering either — it schedules
rows and RFC 0007's cleanup deletes later. Compaction is therefore covered
by `CommitNotDurable` and `CommitDurableNotAcknowledged` like any other
commit, with its physical half belonging to the e2e boundary above.

## The cases

### Governed by atomicity

| Case | What the crash interrupts | What must hold after reopen |
|---|---|---|
| `CommitNotDurable` | Batch staged (step 3), WAL flush withheld (step 4 not durable) | `sys/head` still `N`; snapshot `N+1` absent; no `current`/`history`/`inline`/`tstat` record from the commit is visible. All-or-none: none. |
| `CommitDurableNotAcknowledged` | Flush landed; the process dies before returning to the caller | The commit is durable: `sys/head` = `N+1`, snapshot `N+1` resolves fully. A caller re-drive of the same logical operation runs against the advanced head and never corrupts: ids never collide (counters advanced with the landed commit) and logically-guarded operations surface their guard (`AlreadyExists` for a re-driven create, `CommitConflict`/`NotFound` where the premise moved). The scope is deliberate: a re-driven *data-only* operation — `register_data_file` of the same file, an identical inline insert — is a fresh commit and lands a second time; nothing in the protocol dedups it, and nothing will short of a caller-supplied idempotency token (RFC 0004, "Sequence numbers are the store's, not moraine's" — pinning a `seqnum` was considered there and does not substitute). This case asserts consistency of the landed commit and guard-surfacing on re-drive, not universal exactly-once, and that scope is now settled rather than provisional. |
| `MultiTombstoneDrop` | A `DROP TABLE`/`DROP SCHEMA` ending *many* records (the table row, every column, every `file`/`delfile` → `history`; every live `inline` chunk deleted), crashed at every reachable WAL boundary | Reopen shows the table **fully present** or **fully dropped** — never a torn table missing some columns or files. There is **no** reachable point "after the first tombstone, before the last," because the entire multi-tombstone `DROP` is one `WriteBatch` (RFC 0002). This case's job is to *prove that absence*: it fails if any code path splits a `DROP` across batches. |
| `GroupCommit` | Several catalog commits staged into one batch (RFC 0004, "Group commit") | All-or-none over the *whole group*: reopen shows every member committed or none. No partial group — a batch is a batch regardless of how many logical commits it carries. |
| `TakeoverMidCommit` | Writer A dies mid-commit; writer B opens the store and takes over (SlateDB epoch fence) | B reads a consistent head. If A's flush had not landed, B sees `sys/head` = `N` and A's batch is invisible. If A's flush *had* landed, B sees `N+1` and continues from there. No split-brain, no partial-A visibility. |
| `FencedWriterResumes` | A wakes and attempts its CAS/flush *after* B has taken over and bumped the writer epoch | A is fenced: it loses the `sys/head` CAS or is epoch-fenced by SlateDB, returns a typed error, and writes nothing. The store is byte-identical to the timeline in which A never woke. Confirms RFC 0004's "an accidental second writer never corrupts." |
| `GenesisInterrupted` | A single initializer dies after writing `sys/format` but before `sys/head`/snapshot `0` | Reopen shows the store **empty** (genesis re-attempted from scratch) or **fully initialized** — never half-initialized (a `sys/format` with no head). This forces initialization to be **one `WriteBatch`** like any commit, i.e. the atomicity guarantee extends to genesis. |
| `ConcurrentGenesis` | Two processes initialize the *same empty* store concurrently, each trying to write `sys/format` + genesis `sys/head`/snapshot `0` | At most one genesis lands. There is no create-if-absent primitive to lean on; the guards are the real ones: SlateDB's `writer_epoch` (the second `Db::open` fences the first, so the fenced initializer's genesis batch writes nothing) and, within one writer, transactional write-write conflict detection on `sys/format`. An initializer observes the store already initialized and adopts it, re-attempts a genesis it was displaced from, or returns a typed error — `OpenRaced` if it lost the race to create the first manifest, `Fenced` if it was displaced after creating one and its re-attempts are spent, never an untyped store error — and never writes a second `sys/format`, a divergent genesis snapshot, or a conflicting head. Reopen shows a single, coherent genesis. |

`TakeoverMidCommit` decomposes into the two atomicity outcomes under a *new*
process rather than being a third outcome. `FencedWriterResumes` is its
necessary twin: takeover is only safe if the displaced writer, should it
wake, cannot still land its stale batch.

`ConcurrentGenesis` is subtly different from every other case: there is no
`sys/head` to conflict on yet, so the guard is writer fencing across
processes plus conflict detection on `sys/format` within one, and the
loser's correct behavior is to **adopt**, not to conflict — a fresh store is
not a true conflict but a benign "someone beat me to genesis" race.

A genesis displaced mid-bootstrap **is** re-attempted, up to a small
bound. The displaced attempt staged its genesis and never landed it, so
nothing of it reached the store, and the re-attempt either adopts the
catalog the winner created or creates one itself. What the bound buys is
termination: every open takes the writer epoch by a manifest
compare-and-swap, so a re-attempt takes the epoch in turn and may displace
whichever initializer had just won — which is what opening read-write does
anyway, and what the displaced attempt would have done had it been a
moment slower, but two initializers that keep displacing each other would
loop rather than finish. Once the bound is spent, the fence reaches the
caller.

Only *genesis* re-attempts. An open that finds a catalog returns before it
stages anything, so a fence from anywhere else means a live writer took the
store over, and re-taking it is the caller's decision rather than the
open's.

"Exactly one wins" is not what this race guarantees, and never was. A
writer claims the writer epoch and its compactor claims the compactor
epoch in two races that are ordered independently, so the handle that wins
the writer epoch can lose the compactor epoch — and a fenced compactor
closes the handle it belongs to, leaving both initializers fenced. The
re-attempt makes that rare rather than routine. What holds regardless is
the guarantee worth having: genesis is whole or absent, every failure is
typed, and opening again adopts whatever is there.

A loser still fails **typed**, by one of two guards depending on how far
it got. If it lost the race to create the store's first manifest it never
attached at all, and that is `OpenRaced`: benign, nothing written, and
opening again adopts the store the winner created. If it created the store
and was displaced afterwards, that is the ordinary `Fenced`. The two are
worth keeping apart — one says re-attach and you will find a catalog, the
other says you already had one and lost it.

Only the first is peculiar to genesis, and only because there is nothing to
fence yet: once a store exists, the epoch claim retries internally and
absorbs the same race, so a second open takes over instead of failing.
Typing it costs a match on SlateDB's message text, because the condition
that separates a lost CAS from a damaged manifest is private upstream and
both arrive as the same error kind. That is a fragile contract held
deliberately: the suite stages a real lost CAS from outside and asserts the
typed result, so a SlateDB bump that rewords the message fails there rather
than quietly returning every genesis race to an untyped store error.
`GenesisInterrupted` forces genesis itself under the one-batch rule so that
the winner never leaves a torn genesis for the loser (or a reader) to
observe.

### Governed by resumability

| Case | What the crash interrupts | What must hold after reopen |
|---|---|---|
| `StagedBuildInterrupted` | A staged index backfill (RFC 0016) partway through: some steps committed, the build not finished | The index is still `Building`, and **serves no reads** — a lookup returns `IndexBuilding`, never the subset of rows already backfilled, which would be a silently partial answer. Its `build_cursor` is durable, so re-issuing the build resumes from the watermark rather than restarting; entries re-derived below it land as idempotent puts. The finished index covers every row exactly once. |
| `ReclamationInterrupted` | A dead index's entry reclamation (RFC 0007) partway through: some batches committed, others not | The committed batches stay reclaimed and the remaining entries survive to be found again. A re-run finishes them without re-reclaiming what is already gone, and converges: a further pass reclaims nothing. Live indexes and data files are untouched — the sweep only ever removes entries of an index the catalog no longer lists, and an index id is never reused. |
| `MigrationInterrupted` | A structural format migration (RFC 0015) partway through: the driver dies after its start batch, after a step batch, before the finish flip, or after it | Reopen finds the store coherently **old** — stamped the source format, marker present, cursor durable — or coherently **new**: stamped the target format, marker gone. Never new-format-with-marker, because the flip and the clear land in one batch. A re-run reads the marker, resumes the named unit from its durable cursor, and finishes; it never restarts the rewrite from zero. Those four are the whole set of seams: a step's new-key write, its old-key delete, and its cursor advance share one batch, so there is no durable intermediate between them to crash into. |

## Why these are all of them

Every state-changing path in moraine is either one batch or a sequence of
object-store steps around one batch, so it falls under exactly one
guarantee:

- **Single-batch paths** — ordinary commit, multi-tombstone `DROP`, group
  commit, genesis, takeover — reduce to the two atomicity outcomes. The
  cases above instantiate that across every operation that could plausibly
  be tempted to span batches.
- **Multi-batch paths** — the staged index backfill, entry reclamation, and
  the format-migration driver — are the only operations moraine deliberately
  spreads across commits, and all three carry durable progress. The
  migration driver (RFC 0015) is start/step/finish over a cursor, the same
  shape as the other two, and now that it is built it carries
  `MigrationInterrupted`.

Anything else moraine does is one commit, and a crash point inside one
commit is, by RFC 0002's atomicity invariant, not reachable. The `CrashCase`
enum is the executable form of that claim: the test that iterates it matches
exhaustively, so a new case fails to compile until its guarantee and its
coverage are both decided.

The enum lives in the library, beside the crash seams, gated on the same
fault-injection feature. It moved there once `MigrationInterrupted` became
driven: that case is crashed from *inside* a library call, so the case and
the seams it stops at have to be able to name each other. Which guarantee
covers a case, and whether it is driven, stays with the suite — the
enumeration is the vocabulary both halves index by, not the table itself.

## Realizing a crash

The harness produces a crash three ways, none of which needs `unsafe`, a
fork, or corruption of SlateDB internals.

**Dropping the handle.** Where the interesting instant is *after* a durable
flush (`CommitDurableNotAcknowledged`), the harness drops the writer handle
without calling `close()` and opens a fresh one. `Db` has no `Drop` impl at
all — only `DbTransaction` and `DbSnapshot` implement it, for rollback and
deregistration bookkeeping — so releasing the handle flushes nothing.

**Freezing the store.** To observe the "flush did not land" outcome
(`CommitNotDurable`, `MultiTombstoneDrop`, `GroupCommit`,
`GenesisInterrupted`), the harness wraps the object store in a decorator
that stops accepting writes: everything written before the freeze stays
durable, nothing after it ever lands, and reads keep working so the
operation fails on the write it could not persist rather than on garbage it
read back. Reopening constructs a fresh handle over the same bytes with no
decorator, which is exactly "a new process starts against what survived."

The allowance is a **write count**, not a flag. That is what lets a single
test stop the same operation at *every* durability boundary it has: measure
the writes an operation issues on an unfrozen run, then sweep the allowance
from zero to that number, asserting the invariant after each. This is how
the absence cases discharge their "across all WAL boundaries, not just one"
obligation. Once the allowance covers every write, the operation simply
completes — the end of the sweep, not a failure — so a sweep also asserts
that at least one round really was interrupted, or it would prove nothing.

A crashed operation is not required to *return* anything. SlateDB treats a
failed object-store write as retryable and keeps retrying, so a commit whose
writes are frozen does not error — it never returns at all. That is
faithful: a process that dies mid-commit never learns the outcome either.
The harness therefore bounds the wait and treats "did not succeed" as the
crash, asserting only that the operation never reported *success*, since
success would mean the batch became durable after all.

**moraine does not bound that wait in production**, and the reason is not
convenience. A store that refuses writes permanently — expired credentials,
a revoked bucket policy — stalls the writer instead of failing it, which is
a poor thing to do to an operator. But a deadline would be worse than the
stall: abandoning the wait does not undo the staged batch, because the
flush continues beneath us. The deadline would therefore report failure for
a commit that still lands, and a caller re-driving it would apply it twice —
manufacturing the one outcome `CommitDurableNotAcknowledged` exists to say
is safe only because nothing claims it failed. A timeout can honestly say
"I do not know yet," and moraine's error taxonomy has no such answer. What
moraine does instead is make the stall *visible*: a durable commit that has
waited long enough logs what it is waiting for and what to check. That
cannot produce a false negative, and it turns an invisible hang into a
diagnosable one.

This replaces an earlier design that reached for SlateDB's
`Settings { flush_interval: None }` and `WriteOptions { await_durable: false }`
to freeze the pre-flush instant from inside. That would have needed two
knobs plumbed through `CatalogOptions` and the commit path purely for tests
— and `await_durable: true` against a store that cannot flush is precisely
the hang described above. Freezing the store needs no production surface at
all: `Catalog::open` already takes any `ObjectStore`.

**Fencing.** `TakeoverMidCommit`, `FencedWriterResumes`, and
`ConcurrentGenesis` need no simulation — SlateDB's writer fencing *is* the
behavior under test. `writer_epoch` is a monotonic `u64` a writer increments
transactionally (manifest CAS) on `Db::open`, via
`FenceableManifest::init_writer`, which every open runs unconditionally; a
writer below the manifest's current epoch is fenced on its next SST/WAL
write. Because the CAS goes through the same object-store interface as any
other write, the fence is store-agnostic by construction and holds on
in-memory `object_store` exactly as on real storage. The harness drives the
real protocol: open writer A, open writer B over the same `object_store` (B
bumps the epoch — the takeover), assert B's view, then have the still-live A
attempt its commit and assert it fails fenced, writing nothing.

The genesis *re-attempt* is the one fencing behavior a real race samples
too rarely to pin, so it is staged from outside with the same decorator the
lost-manifest case uses: refusing the first WAL object that carries a batch
is what a writer displaced between taking the epoch and flushing its first
commit finds, so the fence is SlateDB's own and the re-attempt runs the
production path.

Nothing here needs an in-code fault-injection hook, and no case needs a
production knob added for it. The resumability cases are interruptible by
construction — their batches are separate commits, so the harness stops
driving and reopens — and every other case is produced from outside the
operation, by dropping a handle, freezing the store, or opening a second
writer. The one path that needs its own seam hook is RFC 0015's migration
driver, which carries one: its batch boundaries are internal to a single
call, so there is no handle to drop between them.

That path needs a second thing the others do not: something to migrate.
Every format shipped so far is additive, so the driver's registry is empty
and the public verb is a no-op against every store in the world. The same
feature that compiles the seams therefore also installs a synthetic unit
into that registry — the registry the shipped planner reads, not a parallel
one — so the case is driven through `Catalog::migrate` and covers the
planner that ships.

## Test obligations

Per RFC 0001, these run in `crates/moraine/tests/` against real SlateDB on
in-memory `object_store`, no store mocks. The obligation is a single
data-driven test that iterates `CrashCase`:

- For each variant, build the pre-crash state, crash at that point, reopen,
  and assert the case's invariant. A variant with no assertion is a test
  failure — this prevents a case from being silently stubbed out.
- The idempotence cases (`CommitDurableNotAcknowledged`,
  `StagedBuildInterrupted`, `ReclamationInterrupted`,
  `MigrationInterrupted`) additionally re-drive the operation after reopen
  and assert convergence — no id collision, no torn state, no
  double-counting, and no error other than the typed ones the protocol
  permits. For `CommitDurableNotAcknowledged` the assertion matches
  that case's stated scope: guarded operations surface their guard, and a
  data-only re-drive lands as a *clean second commit* (the duplicate is the
  caller's to prevent until the `seqnum` question is settled), never a
  corrupted one.
- The absence cases (`MultiTombstoneDrop`, `GroupCommit`,
  `GenesisInterrupted`) assert torn intermediate states are *unobservable*
  across **all** WAL boundaries of the operation, not just one — the test
  enumerates the boundaries and checks each. For `GroupCommit` the torn
  state to rule out is a group missing its tail, or a head standing at a
  member other than the last.

This test **replaces** the generic "crash-shaped sequences (commit, reopen,
verify)" line in RFC 0001's testing table and **subsumes** RFC 0004's
"partial batch never observable" bullet, which cites the commit cases rather
than carrying its own crash prose. RFC 0007's "cleanup idempotence"
obligation splits: the object-store half is the engine's, and the catalog
half — reclaiming entries in resumable batches — is
`ReclamationInterrupted`.

## Alternatives considered

- **Keep the generic "crash-shaped sequences" prose (RFC 0001 status quo).**
  Rejected: it under-specifies where a process may die, so coverage depends
  on author imagination, and it duplicates partial crash stories across RFCs
  0004 and 0007 that can drift out of sync.
- **Group the cases into a grid.** Rejected, and this RFC previously did it:
  cases were lettered `A1`–`D2` by path and presented as a matrix. But the
  guarantee follows from the path — single-batch paths are governed by
  atomicity, multi-batch paths by resumability, and no path is governed by
  both —
  so the grid was a partition wearing a cross-product's clothes, and most of
  its cells were necessarily empty. Grouping by the two guarantees says the
  same thing along one axis, and a coordinate like `A1` named a position
  rather than a scenario: `CommitNotDurable` needs no lookup table.
- **Keep flush and cleanup as crash cases here.** Rejected, and this RFC
  previously did it: it enumerated "the object-store PUTs that precede a
  flush" and "the DELETEs that follow an expiry" as moraine's multi-step
  paths. They are not. RFC 0005 assigns the Parquet write to DuckLake's
  writer, and RFC 0007 rejected a moraine-side delete engine after this RFC
  was written, leaving four cases describing a component moraine does not
  have. An integration test cannot stop a process between an engine PUT and
  its commit, so keeping them here would have meant four permanently
  unreachable cases standing in for coverage that belongs to the e2e tier.
  The boundary is now stated instead, under "What moraine does not own."
- **Randomized/fuzz crash injection only.** Rejected as the *primary*
  mechanism: a fuzzer that crashes at random offsets gives breadth but no
  stable per-scenario identity, so a regression cannot be named, pinned, or
  guaranteed to re-run. The named list is the floor and the fuzz target the
  ceiling; both exist, and the ordering between them is the point.
- **One crash test per RFC, owned by that RFC.** Rejected: the invariants
  are cross-cutting (atomicity recurs in commit, drop, takeover, and
  genesis; resumability in backfill, reclamation, and the migration
  driver), so per-RFC ownership re-derives the same two guarantees several
  times over and makes the coverage argument impossible to state in one
  place.
- **Assert torn `DROP` states are handled rather than unreachable.**
  Rejected: it would encode the *wrong* guarantee. moraine's whole atomicity
  claim is that a multi-tombstone `DROP` cannot tear; a test that tolerates
  a torn intermediate would silently permit chunked-`DROP` implementations
  that violate RFC 0002.

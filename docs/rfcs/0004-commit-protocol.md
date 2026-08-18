# RFC 0004: Commit and transaction protocol

- **Date:** 2026-07-09

## Summary

Defines how a DuckLake catalog mutation becomes a durable, atomic commit in
moraine: how a committer reads the head snapshot, allocates ids, detects
conflicts against concurrent commits, and lands everything in a single
atomic SlateDB write. Builds directly on RFC 0002's atomicity invariant
(one commit = one batch) and treats RFC 0005 inlined writes as first-class
commit participants. The design is optimistic and single-writer-oriented:
commits race on the head pointer under SlateDB's transactional write-write
conflict detection, disjoint concurrent commits retry internally, and
genuine conflicts surface as a typed error for the caller — DuckLake or the
application — to re-drive. The protocol has **two front doors** over one
commit core: the verb/closure API of RFC 0003 (retryable internally,
because moraine authors the mutations) and the staged-row path of RFC 0006
(never retried internally, because DuckLake authors the rows — a lost race
surfaces to DuckLake). The two paths have different retry rights, and
keeping them distinct is load-bearing; this RFC specifies both.

**Implemented.** Both front doors land commits as one atomic SlateDB batch
over the shared core. Multi-statement, cross-table ACID transactions work
end to end through DuckLake: a `BEGIN … COMMIT` spanning tables stages every
statement's writes into one moraine staged tx (opened lazily on the first
write, reused across the transaction) and commits them as one snapshot;
`ROLLBACK` discards them and mints none. A lost write-write race aborts
without internal retry on the staged-row path, the loser's error carrying
the `conflict` substring DuckLake's retry loop scans for. Verified live
(`ducklake_load.rs`'s `ducklake_multi_statement_transaction_commits_atomically`,
which pins that two writes across two tables advance the head by exactly one
snapshot) and at the core (`staged.rs`'s
`lost_race_is_not_retried_and_carries_conflict_text`). A genuine concurrent
race is not reachable through DuckLake's single serialized metadata
connection — a second read-write attach fences rather than races (the
single-writer topology below) — so that path is pinned at the core, not live.

## Goals

- Every commit is exactly one SlateDB `WriteBatch` (RFC 0002). Snapshot
  allocation, id allocation, conflict detection, and the write itself all
  fit inside that envelope — no read-modify-write spanning batches.
- Inlined inserts/deletes (RFC 0005) ride the same batch as their catalog
  metadata. Inlining adds no round trips to the commit path.
- On the verb path (RFC 0003), concurrent commits touching disjoint tables
  — and concurrent pure appends to the *same* table — both succeed without
  caller involvement; incompatible commits resolve to a typed conflict. On
  the staged-row path (RFC 0006), every lost race surfaces to DuckLake.
- Commit latency floor is one durable WAL flush; the protocol enables group
  commit so throughput under a funnel of concurrent transactions is not
  one-flush-per-commit.
- Faithful to DuckLake's own concurrency semantics — moraine implements
  DuckLake's conflict model, it does not invent a stricter or looser one.

Non-goals:

- **Multi-writer coordination.** The supported topology is single-writer,
  many-readers (see Topology). Concurrent *commit coordination* across
  processes is out of scope; the escape hatch is funneling commits through
  one process.
- **Key layout and value codec** — RFC 0002.
- **Inlined data record formats** — RFC 0005. This RFC covers only how
  inlined writes participate in a commit.
- **Snapshot expiry / GC** of `history` and flushed inline data — RFC 0007.

## Background

RFC 0002 established the keyspace: `sys/head` holds the latest committed
`snapshot_id`; the `snapshot` subspace is append-only snapshot records; `current`
and `history` split live from ended entity versions; commits are atomic
`WriteBatch` writes under a single fenced SlateDB writer.

SlateDB (pinned at 0.14.x) provides the concrete primitives this protocol
uses, verified against its source rather than assumed from prose:
`Db::begin(IsolationLevel::Snapshot)` opens a transaction whose commit
performs **write-write conflict detection** on the keys it writes and fails
with a typed `TransactionConflict`; `Db::snapshot()` gives a pinned
consistent read handle (RFC 0009); `DbReader` opens a store **read-only**
without becoming a writer; and writer fencing is a monotonic
`writer_epoch` bumped by manifest CAS on `Db::open`. There is no bespoke
key-level compare-and-swap API, and none is needed — the "head CAS" in
this RFC is realized as conflict detection on `sys/head` (see the commit
sequence).

DuckLake allocates
ids from counters: `next_catalog_id` and `next_file_id` live in the
`ducklake_snapshot` record; `next_row_id` is **per-table** in
`ducklake_table_stats` (`next_row_id += record_count` on insert), preserved
across UPDATE and compaction for row lineage. The `ducklake_snapshot` record
also carries `schema_version` — a counter DuckLake advances only when a
commit changes the catalog *schema*, and that DuckDB uses as its
schema-cache key. This RFC defines the protocol that maintains all of these
under optimistic concurrency.

## Design

### Topology

**Single writer, many readers.** Readers open the store through SlateDB's
`DbReader` — a handle that follows the manifest, never becomes a writer,
and never participates in fencing (only `Db::open` and the compactor bump
manifest epochs); there is no bound on their number. Writers are expected
to be a single logical committer holding the one read-write `Db`.

One source-verified caveat keeps "read-only" from being oversimplified:
`DbReader::open` in latest mode **writes a checkpoint into the manifest**
(a manifest CAS update) and maintains it from a background poller for the
reader's lifetime. So a reader does not fence and does not write data,
but it *does* need write access to the manifest object and generates
manifest traffic, and its checkpoint pins SSTs against SlateDB's own GC
for the checkpoint lifetime. The default "follow latest" mode trades that
small manifest write for consistent WAL-inclusive reads.

Deployments whose reader credentials cannot write at all open against an
existing checkpoint instead. A checkpoint id given at open selects a
`DbReader` that resolves a fixed cut: it neither creates a checkpoint, nor
replays WALs, nor polls the manifest, so the open and everything after it
write nothing whatsoever — verified by opening one against a store that
refuses every write and reading through it. The cost is the other half of
the trade: a pinned reader never sees a commit made after its checkpoint,
however long it stays open. Checkpoints are minted by the writer (which
alone can write the manifest) and released explicitly or by their
lifetime, since one that is neither pins the objects it references against
SlateDB's garbage collection for as long as it exists.

Be precise about what fencing does, because its direction is the opposite
of a lock: SlateDB's `writer_epoch` means **the newest writer wins**. A
second process opening the store read-write bumps the epoch and **fences
the incumbent** — the first writer's next durable write fails, not the
second's. Safety is absolute (a fenced writer writes nothing, never
corrupts — RFC 0011's `FencedWriterResumes`), but availability is not: two processes attaching
read-write do not degrade to "second one fails," they take turns killing
each other's committer. The operational rule follows: **exactly one
process attaches read-write; every other process attaches read-only**
(`DbReader`), and RFC 0006 exposes a read-only attach option so a DuckDB
process can opt into the reader role explicitly.

This is a real product limitation relative to the multi-client SQL
catalogs DuckLake otherwise uses — many DuckDB processes writing through
one PostgreSQL catalog is the common lakehouse shape, and moraine does not
support it: there is no funnel component in the DuckLake deployment model,
so "run one long-lived committer process and route commits through it" is
an escape hatch for embedding hosts, not something a fleet of independent
DuckDB clients gets for free. This constraint is stated here once,
surfaced in README-level documentation, and holds for the default
topology. RFC 0022 takes up the multi-writer coordination design as an
opt-in topology layered above this protocol; the semantics specified
here are unchanged by it.

### Bootstrap — the commit that creates the store

Opening a store that has never been written is itself a commit-shaped
operation and rides the same machinery. On open, the catalog reads
`sys/format` through a transaction:

- **Present** → validate: an unknown format version is refused
  (`Corruption` today; `Migration` once RFC 0015's variant exists), and a
  present `sys/migration` marker is refused outright (RFC 0002 reserves
  that check from format v1).
- **Absent (empty store)** → bootstrap: stage one atomic batch writing
  `sys/format` (format version 1 plus the writing moraine version),
  snapshot `0` (empty catalog: no entities, counters at their initial
  values so the first allocation yields id 0, `schema_version` 0, empty
  `changes_made`), and `sys/head` → `0` at batch count `0`, then commit it under the same
  head conflict detection as any commit. A same-process race between two
  bootstrapping opens arbitrates on the head write like any other commit
  — the loser re-reads and finds the initialized store; across processes
  the single-writer topology and fencing already exclude a second writer.

Bootstrap mints the default `main` schema in the same batch, matching
the initial catalog state a fresh DuckLake metadata store carries: a
moraine store is DuckLake-attachable from birth, without a DuckLake-side
bootstrap pass (whose exists-probe always succeeds against moraine — see
the extension-surface RFC). The `main` row's id comes from the ordinary
counter, so the first user allocation follows it.

### The commit sequence

A commit takes a set of staged catalog mutations (entity inserts, entity
ends, inlined writes) and lands them atomically:

1. **Load head.** Open the SlateDB transaction, then read `sys/head` →
   snapshot id `N`, the snapshot `N` record for the global counters, and
   the `tstat` records for any tables this commit inserts rows into — all
   **through the transaction handle**, so the premise view and the
   conflict-detection window share one start sequence and no commit can
   land unseen between them (RFC 0009, "Commit attempts materialize
   through the transaction").
2. **Allocate ids** locally by bumping in-memory copies of the counters:
   `next_catalog_id`/`next_file_id` from the snapshot record, `next_row_id`
   from each touched table's `tstat`. No store writes yet.
3. **Stage the batch.** Assemble one atomic write set:
   - new entity records into `current` (and, for ended versions, delete the
     `current` key + write the `history` key — RFC 0002);
   - inlined `inline/insert` chunks and `inline/inline_delete`/`inline/file_delete` records
     (RFC 0005);
   - updated `tstat` records carrying advanced `next_row_id`;
   - the new `snapshot` record `N+1` with advanced global counters,
     `schema_version` (bumped or carried forward per the rule below), and
     merged `snapshot_changes`;
   - the `sys/head` update `N → N+1`, with the batch count incremented.
4. **Commit under head conflict detection.** The staged writes commit as
   one SlateDB transaction (`begin(IsolationLevel::Snapshot)`) opened at
   step 1. **Every batch writes `sys/head`** — including a maintenance
   batch, which mints no snapshot and so reuses the standing id while still
   moving the batch count (RFC 0002) — so SlateDB's write-write
   conflict detection on that one key *is* the head CAS: if a concurrent
   commit advanced the head since this transaction began, `commit()`
   returns `TransactionConflict` and nothing was written — go to conflict
   handling. If it lands, the commit is durable once the WAL flush lands —
   done. (This is deliberately not a bespoke compare-and-swap: SlateDB
   exposes no key-level CAS API, and the transaction primitive it does
   expose is exactly the required shape. A transaction's write set is
   applied as one atomic batch, so RFC 0002's one-commit-one-batch
   invariant is preserved verbatim. Two source-verified mechanics worth
   knowing: the conflict check runs inside SlateDB's single batch-writer
   event loop immediately before the write is sequenced, so check and
   apply cannot interleave; and the detection state is **in-process** —
   which is sufficient precisely because the topology admits one writer
   process, with cross-process safety carried by fencing, not by conflict
   detection. Plain non-transactional writes are tracked too, modeled as
   single-op transactions, so a stray direct write cannot slip past a
   concurrent commit's conflict check.)

The read in step 1 is "load head," not a lock; the write in step 4 is the
whole commit. Nothing spans two batches. Within one process, concurrent
commit attempts (multiple connections) may run steps 1–3 optimistically in
parallel — the transaction conflict at step 4 arbitrates — or the
committer may serialize them (see Group commit); across processes, the
topology above guarantees there is no second writer to race.

### Conflict detection — table-level

The `changes_made` field of the snaphot record is written in **DuckLake's
own grammar**, not a moraine dialect — the field is DuckLake-visible on
the staged-row path (DuckLake re-parses it mid-retry, and its parser
*throws* on unknown entry kinds), so the wire format is DuckLake's to
define. Source-verified (writer `WriteSnapshotChanges` in
`ducklake_transaction_state.cpp`, parser `ParseChangesMade` in
`ducklake_transaction_changes.cpp`): comma-separated `kind:payload`
entries; created entries carry SQL-quoted names (`created_schema:"s"`,
`created_table:"s"."t"`, quotes doubled for embedded quotes), all other
entries carry numeric ids (`dropped_schema:1`, `dropped_table:2`,
`altered_table:3`, `inserted_into_table:4`, …). moraine emits entries in
DuckLake's writer order and, when *parsing*, treats any entry kind it
does not model as an unclassifiable change — conservatively a true
conflict — rather than erroring.

The verb-path mapping for data-plane operations, chosen so every emitted
kind is one DuckLake's conflict matrix actually checks (the legacy
`compacted_table` kind is parse-only in DuckLake's source and unchecked
by its matrix — emitting it would make these commits invisible to a
DuckLake retry): `register_data_file` → `inserted_into_table:<id>`;
`register_delete_file` → `deleted_from_table:<id>`; `expire_data_file` →
`merge_adjacent:<id>`; `expire_delete_file` → `rewrite_delete:<id>` (the
two compaction kinds — an expiry's conflict profile is a compaction's:
true against concurrent deletes, drops, and compaction of the table,
benign against appends and alters). Statistics-only mutations emit no
entry: stats are re-derived on retry and conflict with nothing.

When the head conflict fires, the committer compares the set of
`table_id`s it touched against those touched by the intervening commits
`N+1 … N+k` (recoverable from their `snapshot_changes`):

- **Disjoint table sets → benign race.** The concurrent commits do not
  invalidate this commit's premise. Retry from step 1 against the new head
  (what a retry re-runs is defined in Conflict resolution below — on the
  verb path it includes re-running the caller's closure, not just
  re-stamping ids). This is bounded internal retry inside `transaction`; no caller
  involvement.

  Retries wait before re-running: the delay starts at 2ms, doubles, and
  caps at 50ms, with jitter of up to the base delay so two writers that
  just collided do not back off in lockstep and collide again. The whole
  budget spans a few hundred milliseconds. A commit that lands on its
  first attempt never waits, so the backoff costs the uncontended path
  nothing.

  **The budget is deliberately not DuckLake's**, whose own loop waits
  100 ms × 1.5ᵏ over 10 attempts. Two reasons, both consequences of what
  moraine's races actually are. First, a benign race here clears in one
  WAL flush — the shape group commit made visible, where concurrent
  commits inside the process meet in a batch rather than racing at all,
  leaving only a staged-path commit colliding with a verb-path batch. A
  budget of ten attempts capped at 50 ms spans several flush intervals at
  the 100 ms default, which is several times more than the collision it
  waits out. Second, the two budgets **compose**: DuckLake re-drives a
  retryable moraine error up to ten times, so moraine's own waits are
  multiplied by ten before an application sees anything. At DuckLake's
  cadence that product is tens of seconds of invisible waiting; at
  moraine's it is a few seconds worst case, and typically one retry that
  waits 2 ms. Ten attempts is kept: losing ten consecutive races requires
  a pathology the topology does not otherwise admit, and past that
  `RetryBudgetExhausted` is the honest answer.
- **Overlapping table set → true conflict** (subject to the append-append
  refinement below). A concurrent commit mutated a table this commit also
  mutated. The premise may be invalid (the table was dropped, a column
  altered, files this commit references were compacted away). Abort with a
  typed `CommitConflict`.
- **Budget spent → terminal.** A commit that loses `MAX_COMMIT_ATTEMPTS`
  benign races in a row aborts with `RetryBudgetExhausted`, not
  `CommitConflict`. The distinction is a wire contract, not cosmetics:
  DuckLake decides retryability in `DuckLakeTransaction::RetryOnError`, a
  **substring match on the error message** — it re-runs a failed commit
  iff the lowercased text contains `primary key`, `unique`, `conflict`, or
  `concurrent` — and `CommitConflict`'s `Display` carries `conflict` in
  its prefix. The text moraine surfaces for a lost commit is part of the
  interface, not wording. A transient conflict *should* stay retryable and
  keeps that wording; an exhausted budget must not, or the two bounded
  loops compose into `ducklake_max_retry_count` × `MAX_COMMIT_ATTEMPTS`
  re-runs of a commit that already failed to settle ten times.
  `RetryBudgetExhausted`'s text is therefore free of all four substrings,
  exactly as the equality-index uniqueness rejection already is. Every
  exhaustion is logged at `warn` with the head it started from and the
  commits it lost to — without it the failure is a slow commit with no
  stated reason.

**Counter advancement is never, by itself, a conflict.** Every commit reads
and bumps `next_catalog_id`/`next_file_id`; that shared read is precisely
what benign-race retry re-derives. Only overlapping `table_id`s are a
conflict. This is the rule that keeps table-level detection from collapsing
into serialize-everything.

**Append-append refinement.** A same-table overlap is *not* automatically a
true conflict when both sides touched the shared table only as **pure
appends**: `register_data_file` / `inline_insert` records plus the `tstat`
counter/stat advance they imply. Data-file registrations are commutative —
neither invalidates the other's premise — and the `next_row_id`/stats
updates are mechanically re-derivable on retry exactly like the global
counters. So append-append overlap classifies as **benign** on the verb
path: retry re-reads the advanced `next_row_id`, re-allocates row ids,
re-merges stats, and re-commits.

This mirrors DuckLake's own conflict matrix, verified in its source
(`DuckLakeTransactionState::CheckForConflicts`,
`src/storage/ducklake_transaction_state.cpp`): an insert conflicts with a
concurrent **drop**, **alter**, or **delete** on the same table — and with
nothing else. Its `tables_inserted_into` and `tables_inserted_inlined` sets
are checked against the dropped, altered, and deleted-from tables only,
never against another transaction's inserts, and an insert is likewise
compatible with a concurrent inline **flush** or **compaction** of the same
table (their file sets are disjoint: the insert's new files are not the
maintenance operation's inputs). moraine's verb path adopts the same
classification: an append is benign against other appends, flushes, and
compactions of its table; drop/alter/delete on that table are true
conflicts, in both directions.

The refinement is required for verb-path fidelity, not an optimization:
plain table-grain overlap would call a same-table append pair a true
conflict and make moraine strictly stricter than the catalog it serves.

DuckLake is finer-grained in exactly one place — two transactions
*deleting* from the same table conflict only if they touched the same
**data files** (its `CheckForConflicts` fetches the files deleted after
the transaction's snapshot and conflict-checks at `data_file_id` grain) —
and moraine matches it. A `register_delete_file` records the data file it
targets, and two commits deleting from one table classify benign when
those targets are disjoint. The premise holds because moraine allows one
live delete file per data file: disjoint targets write disjoint records,
so neither commit invalidates the other's.

Two cases fall back to table grain, both because the file set is not
knowable rather than as a policy choice. An **inlined** delete names a
row, not a file. And a **DuckLake-authored** change set arrives through
the `changes_made` grammar, which carries `deleted_from_table:<id>` and
no file — so a commit whose file ids moraine did not author is treated as
deleting from all of them. Since data-file ids are global, a shared id is
a shared file and the comparison needs no table key; the file ids ride the
snapshot record as moraine-internal state, exactly as
`schema_changed_table_ids` does, and are invisible to DuckLake.

**Name collisions are invisible to id-set comparison.** Two concurrent
commits each creating a table named `orders` in the same schema allocate
fresh, *disjoint* table ids — the set comparison classifies the race
benign. That is correct as classification (neither commit's staged state is
invalidated), but uniqueness must still be enforced: on the verb path it is
the **closure re-run** on retry that catches it — the re-run resolves names
against the new head, sees the winner's table, and returns `AlreadyExists`;
on the staged-row path every lost head surfaces to DuckLake (below), which
re-drives with full knowledge of the new catalog state. Set comparison
alone never establishes name uniqueness; re-validation against the new head
does. This is stated explicitly because it is the tempting shortcut: a
purely mechanical re-stage-and-retry, with no re-validation, would commit
duplicate names.

One operation does not fit table grain and is handled at **schema-list
granularity**: `CREATE`/`DROP SCHEMA` (mutates the set of schemas).
Concurrent commits that both touch the schema list conflict with each
other — matching DuckLake's matrix, where created/dropped schemas
conflict-check against each other and against creations inside a dropped
schema. Rare enough that coarse handling costs nothing. Option changes
need no grain at all: DuckLake neither versions nor conflict-detects them
(`set_option` writes `ducklake_metadata` outside the snapshot protocol,
last-write-wins — see Schema-version tracking); the verb path mirrors
that, treating `set_option`/`unset_option` as non-conflicting overwrites.

The benign/true classification is mechanical — set comparison over
`table_id`s and mutation kinds — but classification only decides *whether*
to retry; the retry itself re-validates (next section). `tx` still never
needs to understand what SQL produced the mutations.

### Conflict resolution — abort, with internal benign-race retry

**What a benign-race retry re-runs (verb path).** A retry is a full re-run
of the commit cycle, exactly as RFC 0003 specifies for `commit`: re-read a
fresh `CatalogSnapshot` at the new head, **re-run the caller's closure**
against it, re-allocate ids from the advanced counters, re-stage, re-commit.
The closure re-run is load-bearing, not an implementation nicety: it is
what re-resolves names and re-validates logical premises against the state
that actually won (the name-collision case above), and it is why RFC 0003
requires the closure to be pure and re-runnable. A retry that mechanically
re-stamped ids onto the previously staged records — without re-running the
closure — would be cheaper and wrong (see Alternatives).

The core resolves benign races itself (bounded retry, above) and **aborts
true conflicts with a typed `CommitConflict`**. It does not attempt to
rebase a true conflict. Rebasing a true conflict means re-running the
*DuckLake operation* — which `moraine` cannot do, because by RFC 0001 the
core is DuckLake-agnostic and never held the originating SQL. Re-driving a
true conflict belongs to whoever authored the operation:

- **DuckLake**, whose commit loop retries retryable conflict signals
  internally and re-checks its own conflict matrix (source-verified; see
  Staged-row commits), or
- **the application**, re-running its statement when DuckLake classifies
  the conflict as true and throws — as a PostgreSQL `SERIALIZABLE`
  transaction retries on a `40001` serialization failure.

The core's responsibility ends at a truthful typed error.

The internal benign-race retry is bounded (a small fixed attempt count);
exceeding it returns `CommitConflict` rather than looping unbounded, so a
pathological write storm degrades to caller-visible conflicts instead of a
hang.

### Id allocation

- **`next_catalog_id`, `next_file_id`** — global, in the `snapshot` record,
  matching DuckLake. Allocated by bumping the in-memory copy from head and
  writing the advanced value in the new snapshot record. Shared reads of
  these counters are benign (re-derived on retry), never conflicts.
- **`next_row_id`** — **per-table, in the `tstat` record**, matching
  DuckLake's `ducklake_table_stats.next_row_id`. Because the counter lives
  in table-scoped state, bumping it *is* a mutation to that table and is
  inside the table's conflict set: two commits inserting into different
  tables share no row-id counter, so they never contend; two inserting into
  the same table overlap on that `table_id` — and resolve under the
  **append-append refinement** above: the shared counter read is
  re-derivable, so on the verb path both commits land via benign-race retry
  (the loser re-allocates its row ids from the winner's advanced
  `next_row_id`). Row ids stay dense per table for RFC 0005's inlined
  chunks. Verified against the DuckLake spec; the e2e suite validates the
  increment mechanics (`next_row_id += record_count`) and DuckLake's own
  behavior under concurrent same-table inserts.

### Schema-version tracking

`schema_version` is a counter separate from the id counters: DuckLake
advances it **only when a commit changes the catalog schema**, and carries
it forward unchanged for data-only commits. DuckDB keys its schema-metadata
cache on it, so getting this wrong is not a storage bug but a
client-visible one — in both directions. *Always* incrementing defeats the
cache: every `INSERT` looks like a schema change and forces DuckDB to
re-fetch every table's columns, multiplying catalog read traffic on exactly
the small-append workload moraine targets. *Never* incrementing serves a
stale cache: after `add_column`, DuckDB keeps using its cached column list
and returns wrong results or errors on the schema mismatch.

The committer tracks a `schema_changed` flag over the staged mutations. On
commit, the new snapshot record's `schema_version` is `prev + 1` if the
flag is set, else copied from snapshot `N`. The flag is set by the
structural mutators — those that alter the catalog's shape rather than its
data (RFC 0003 verb names):

- **Schema-changing** (`schema_version` bumps): create/drop schema;
  create/rename/drop table and move-to-schema; create/alter/rename/drop
  view; add/remove/rename column, column type change, default and
  null-constraint changes; set partition key; create/drop macros;
  comments and tags on tables and columns. The last of these bump despite
  changing no column: DuckLake models them as `AlterEntry` variants, and
  the rewritten entry enters `new_tables`.
- **Altered but not schema-versioned**: set sort key. DuckLake marks the
  table altered — conflict detection sees the alter — but does not bump
  `schema_version` (a sort change never invalidates cross-file
  compaction, so the `ducklake_schema_versions` grouping is unaffected).
- **Data-only** (`schema_version` carried forward): `register_data_file` /
  `expire_data_file`, `register_delete_file` / `expire_delete_file`,
  statistics updates, column/name-mapping registration, and the RFC 0005
  inlined `inline_insert` / `inline_delete` / `flush_inlined_data`.
- **Outside the snapshot protocol entirely**: option changes.
  `set_option` in DuckLake is an immediate check-then-write on
  `ducklake_metadata` within the transaction's metadata connection — it
  mints no snapshot, bumps no `schema_version`, and is not recorded in
  `snapshot_changes` (so it is invisible to conflict detection; options
  are last-write-wins). Both paths implement exactly that. On the
  staged-row path a `ducklake_metadata` row translates to a write of the
  scope's option record, and because such a commit carries no
  `ducklake_snapshot` insert it takes the head-preserving path — the same
  one snapshot expiry and file cleanup take. So `CALL
  <lake>.set_option(…)` lands the option and moves nothing else: no
  snapshot, no `schema_version`, no `snapshot_changes` entry, and so
  nothing for conflict detection to *classify*. It does still write
  `sys/head` at the standing snapshot id with the batch count advanced, as
  every head-preserving batch does — that is the anchor a concurrent
  commit conflicts on, and the stamp that tells a reader the state moved
  under an unchanged snapshot id.

This classification is source-verified against DuckLake
(`DuckLakeTransactionState::SchemaChangesMade`, which tests exactly the
new/dropped table, schema, view, and macro sets; per-table alters route
through `GetTransactionTableChanges`, which decides per change type
whether the alter also bumps `schema_version` — `SET_SORT_KEY` lands in
`altered_tables` only).

The flag is an explicit property of the mutation set, not something
inferred from the batch's key set after the fact — an inference would
misclassify (e.g. a stats-only touch of a table's state) and silently
re-introduce one of the two failure modes. The exact schema-mutating set is
pinned against DuckLake's own behaviour in the e2e suite.

The flag machinery above belongs to the **verb path only**. On the
staged-row path (next section) DuckLake computes `schema_version` itself
and writes it in the `ducklake_snapshot` row it authors; moraine stores
that value row-faithfully and neither tracks nor second-guesses a flag.
The two paths must agree — the e2e schema-version matrix pins both.

### Staged-row commits — the extension path

RFC 0006's extension surface does not call RFC 0003's verbs. DuckLake
drives moraine with row-level DML against the `ducklake_*` tables: a
DuckLake transaction stages row inserts/updates/deletes, and — critically —
DuckLake itself authors the catalog mechanics. It reads `next_catalog_id` /
`next_file_id` from the snapshot row, computes the new ids, computes
`schema_version`, and writes the new `ducklake_snapshot` row, with the new
snapshot id and `begin_snapshot` values **embedded in every row it
stages**. moraine's job on this path is narrower and different from the
verb path:

- **Translate, don't author.** moraine maps each staged row onto the
  RFC 0002 keyed layout — an INSERT becomes a `current` record; an UPDATE that
  sets `end_snapshot` becomes the end-version bookkeeping (delete `current`
  key, write `history` key); the snapshot row becomes the `snapshot` record and
  the `sys/head` advance. This begin/end-lifecycle translation is the one
  semantic convention moraine interprets (RFC 0006 states and bounds it);
  ids, counters, and `schema_version` are DuckLake's values, stored
  verbatim.
- **Same commit core.** The translated records land through steps 3–4
  above: one transaction, one atomic batch, head conflict detection.
  Nothing about atomicity or durability differs between the paths.
- **No internal benign-race retry.** A retry on the verb path re-runs the
  closure and re-stamps ids moraine allocated. On this path there is
  nothing moraine can safely re-run: the snapshot id, `begin_snapshot`,
  counter values, and `schema_version` are DuckLake-authored and embedded
  in row values moraine must carry faithfully — re-stamping them would be
  semantic surgery on another system's data, the exact re-modeling RFC
  0006 forbids. Therefore **every lost head on the staged-row path
  surfaces as a typed `CommitConflict`** to DuckLake — benign-shaped or
  not — and this is exactly the composition DuckLake expects of its
  catalog, verified in its source: DuckLake's `RunCommitLoop` wraps every
  metadata-catalog commit in its own bounded retry (default
  `ducklake_max_retry_count = 10`, jittered exponential backoff from
  `ducklake_retry_wait_ms = 100` × `ducklake_retry_backoff = 1.5`),
  re-checking its own conflict matrix against `ducklake_snapshot_changes`
  before each re-attempt and re-staging with freshly derived ids. Those
  re-checks are catalog reads moraine must serve *between* attempts —
  DuckLake queries the snapshot list and `ducklake_snapshot_changes` for
  everything committed after its transaction snapshot, so the read surface
  stays live while a commit of its own is in flight. Benign races are
  absorbed by *DuckLake's* loop; true conflicts throw its
  `TransactionException` to the application. The "disjoint concurrent
  commits both succeed without caller involvement" goal is thus a
  **verb-path** property; on the extension path the equivalent behavior
  is provided by DuckLake, one layer up, where it belongs.

Two of the staged path's read phases dominate its latency — the inline
translation (chunk walks for flush-deletes and drops) and equality-index
maintenance (scoped Parquet reads and entry staging) — and they run
**beside** each other rather than in sequence. Both read the transaction's
pre-commit cut, so ordering could only matter through writes: index
maintenance stages only `index/*` keys as it streams — a subspace the
inline translation never reads — and hands its chunk-directory repairs
back unstaged, while the translation stages nothing at all. Neither can
observe the other, so running them together is indistinguishable from
running them in order. The head view resolves first (it is maintenance's
base input, and a warm handle answers it from memory); maintenance still
completes before the batch is assembled, so a poisoned definition rides
the writes it produces; and its directory repairs are ordered ahead of the
batch's own inline writes, so a flush draining a repaired chunk still
deletes the locator the repair added.

The two front doors share the commit core (staging, one-batch atomicity,
head conflict detection, durability) and differ only in who authors the
mutations and therefore who may retry. Keeping that split explicit is what
lets the verb path be aggressive (closure re-run, append-append benign)
without ever risking a silent rewrite of DuckLake-authored state.

### Inlined writes as commit participants

Per RFC 0005, an inlined insert/delete is not a separate path: its
`inline/*` records are added to the same `WriteBatch` in step 3, allocating
row ids from the same per-table `next_row_id` bump as a Parquet write
would. An inline flush is itself a commit — Parquet PUTs first, then one
batch that creates the `file`/`delfile` records and deletes the consumed
`inline/*` records outright (no `history`; `inline/*` is append-then-delete,
not begin/end-versioned). Nothing about inlining changes the commit
sequence or the conflict rules; it only adds record kinds to the batch.

### Group commit

Because the topology funnels concurrent transactions through one committer,
the committer may batch several pending catalog commits into one WAL flush:
allocate ids for each, stage their records into one `WriteBatch`, one CAS,
one durable flush. Per-commit latency stays one-PUT-bound (RFC 0002 /
README); throughput under load stops being one-flush-per-commit and PUT
costs amortize. Group commit is an optimization the protocol permits, not a
correctness requirement — a batch of one is the normal path. Ordering
within a group is the committer's choice; conflicts *within* a group are
impossible because a single committer serializes them before staging.

**Implemented** two ways over one core — a lone commit is a group of one,
so there is one commit path, not two. `Catalog::commit_group` batches what
one caller hands it; concurrent callers of the ordinary `Catalog::commit`
are batched without asking. A commit that finds the store free opens a
batch, one that finds a batch forming joins it, one that finds a batch in
flight waits for the next, and a batch seals as soon as no caller is on its
way into it — so the flush already in the air is the batching window and an
uncontended commit waits for nobody. A batch also seals at a bounded member
count, since under saturation the arrival count never falls to zero.

Steps 1–3 run once per member, against a premise folded forward from the
member before it (the same fold that refreshes the cached head view), so
member *i* allocates its ids and detects its name collisions against
everything members *1..i* did. That is what makes intra-batch conflicts
unreachable rather than unlikely. Step 4 then runs once.

A batch batches commits; it does not merge them. Each member mints its own
snapshot, which time travel resolves separately. What batching changes is
durability granularity, not catalog shape.

Measured, the coalescing is exact: a batch carries as many commits as there
are concurrent committers, so throughput is `concurrency / flush_interval`
with no overhead visible at any level, up to the member ceiling — where 128
concurrent commits land as two full batches at the same rate 64 do
(`BENCHMARK.md`, "Commit throughput vs. concurrency"). The ceiling, not the
flush cadence, is what binds first under saturation, and it is the lever if
a workload ever needs past it.

The single-commit floor is measured against AWS S3 as well as the local and
injected-latency stores. From a worker in the bucket's `us-west-2` region,
1 ms and 25 ms flush cadences both land at roughly 28–30 ms median, while a
100 ms cadence lands at 101.9 ms (`BENCHMARK.md`, "Durable-commit latency
against a real endpoint"). The endpoint round trip therefore replaces the
short cadence as the binding term; it does not add a second serial wait.

The batch is the unit of failure. A member that fails discards the batch,
and its other members re-run; a lost race re-runs every member, conflicting
if any member's change set does; a crash leaves every member committed or
none (RFC 0011, `GroupCommit`). A sealed batch commits on a task of its own
rather than on whichever caller sealed it, because a host interrupt drops
that caller's future (RFC 0010) and the other members are owed an answer.

Both front doors land through one function, so the durable write, the
head-race classification, and the projection bookkeeping exist once. What
the staged-row path does not do is *join* a batch: it commits its own
transaction, because DuckLake authors its snapshot id against the head it
read, and no fold onto a preceding member can re-derive that id. The id is
therefore checked rather than trusted — a staged commit whose id does not
advance the head it lands on is refused, where before it relied on the
head-pointer race to catch the same disagreement. That refusal is a lost
race and must be reported as one: it carries `CommitConflict`'s text, the
substring DuckLake re-drives on. Reporting it as corruption instead would
be a wire-contract bug, not a wording one — DuckLake would abandon a
transaction it could have re-run against the head that won.

So batching is a verb-path property, and SQL through DuckLake reaches it
only where a statement runs a verb (index DDL, maintenance). DuckLake's own
DML and DDL do not batch, and would gain nothing if they did: its metadata
connection is serialized, so successive SQL transactions never overlap and
there is nothing to batch them with. A `BEGIN … COMMIT` still lands as one
snapshot, but that is statements merged, not commits batched.

### Sequence numbers are the store's, not moraine's

A commit writes with SlateDB's generated sequence number; moraine never pins
one. The knob exists — a non-zero `WriteOptions::seqnum` overrides the
generated value, and a write below the store's current maximum is refused —
so it reads at first like an exactly-once primitive: pin the same number on
a re-drive and the duplicate is rejected.

It is not one here, for two reasons. The sequence space is **global to the
store**, not per catalog commit: index build steps, entry reclamation,
inlined writes, and migration batches all advance it while minting no
snapshot, so no function of a snapshot id stays in step with it, and any
mapping moraine chose would be stale the moment a non-committing write
landed. And the number moraine would need to pin does not exist at re-drive
time anyway. A re-drive re-runs the caller's closure against the *advanced*
head, so it is a different logical commit computed from a different premise
— there is nothing stable across the crash to pin *to*.

Exactly-once across a process death needs a **caller-supplied idempotency
token**, recorded in the commit and checked before staging. That is an API
change this RFC does not make, and pinning `seqnum` would not substitute for
it. So a re-driven commit is an ordinary commit: guarded operations surface
their guard, and a data-only re-drive lands a second time (RFC 0011,
`CommitDurableNotAcknowledged`).

### Reader visibility after commit

Step 4 makes a commit **durable**; a *separate* reader observing it is a
second, distinct property. Readers are uncoordinated (Topology) and resolve
snapshots by following the manifest from object storage. A reader that
opened before the commit — or a fresh reader whose manifest view is a poll
interval stale — therefore does not see snapshot `N+1` the instant `commit`
returns.
For moraine's own committer this is a non-issue: it holds the advanced
`sys/head` in memory and reads its own writes. It matters at the boundary
where another process must observe a just-returned commit —
read-your-writes across processes, and writer takeover, where a new writer
must publish the last committed state before it serves.

The contract: `commit` does not return success until the new snapshot is
both durable and reflected in the manifest that readers follow, so a reader
opened *after* `commit` returns resolves `N+1`. (A reader opened *before*
may still lag by its manifest poll interval — that is ordinary snapshot
isolation, not a violation.)

No extra flush call is needed to hold that contract; step 4's own
durability does it. A commit with `await_durable: true` blocks on a
durability watcher that fires only once the WAL object has been PUT to
object storage (`WalBufferManager::do_flush_one_wal` → `notify_durable`),
and both fresh-open paths — `Db::open` and `DbReader::open` in latest
mode — replay WALs from object storage past the manifest state. So a
handle opened after `commit` returns resolves `N+1` by construction.

A validation test in the store harness (open `object_store`, commit, open a
*fresh* reader, assert `N+1` is visible) pins that behavior against real
SlateDB and measures its per-backend latency.

### Test obligations

Per RFC 0001, integration tests exercise the protocol against real
SlateDB on in-memory `object_store` — no mocks of the store:

- Disjoint concurrent commits both succeed (benign-race retry).
- Concurrent pure appends to the same table both succeed (append-append
  refinement); the loser's rows carry row ids re-allocated above the
  winner's advanced `next_row_id`, with no collision or gap.
- Concurrent deletes from the same table are benign when their target data
  files are disjoint and a conflict when they meet on one; an inlined
  delete, and a delete parsed from DuckLake's grammar, fall back to table
  grain.
- Concurrent same-name creates (two commits each creating table `orders`
  in one schema): exactly one succeeds; the other's retry re-runs the
  closure and returns `AlreadyExists` — never two live tables with one
  name.
- Incompatible overlapping concurrent commits: one wins, the other returns
  `CommitConflict`.
- Staged-row commits never retry internally: a lost head on the extension
  path returns `CommitConflict` with the store unchanged by the loser.
- Crash-shaped sequences: owned by RFC 0011 rather than restated here.
  `CommitNotDurable` covers the batch whose flush never landed — the head
  does not move and no record of it is visible — and
  `CommitDurableNotAcknowledged` the flush that landed before the process
  died, where the head and snapshot resolve consistently on reopen and a
  re-drive surfaces its guard.
- Counter monotonicity: `next_catalog_id`/`next_file_id`/per-table
  `next_row_id` never regress or collide across concurrent commits.
- Bounded retry terminates: a forced write storm returns `CommitConflict`
  rather than looping.
- Schema-version matrix: every schema-changing operation increments
  `schema_version`; every data-only operation — including inline
  insert/delete, flush, and mapping registration — carries it forward;
  comment/tag changes increment; `set_option` mints no snapshot and
  changes no version.
- Bootstrap: the first open of an empty store lands format stamp,
  snapshot 0, and head in one batch; a second open (concurrent or later)
  finds the initialized store rather than re-initializing; an
  unknown-format or mid-migration store is refused.
- Fresh-reader visibility: after `commit` returns, a newly opened reader
  resolves the new snapshot (the store-harness validation above, run
  against real SlateDB on in-memory `object_store`).
- Checkpoint readers: a reader pinned to a checkpoint serves the state at
  it and never a later commit; it opens, reads, and closes against a store
  that refuses every write, attempting none, where a latest-following
  reader on the same store writes; a writer refuses a checkpoint, a
  read-only catalog refuses to mint one, and a malformed, unknown, or
  deleted id is refused rather than silently followed.
- DuckLake-facing pins, differential against a stock DuckLake catalog fed
  the identical statements: row ids allocate as `next_row_id +=
  record_count` per table, stay dense, survive an UPDATE of other rows,
  and never contend across tables; `schema_version` bumps for column DDL
  and for comments and tags, and is carried forward by inserts (file and
  inlined) and by name-mapping registration; and DuckLake's retry budget
  (`ducklake_max_retry_count` 10, `ducklake_retry_wait_ms` 100,
  `ducklake_retry_backoff` 1.5) is what the composed budget above assumes.
  A genuine concurrent DuckLake race needs more than one connection on one
  DuckDB instance, which the CLI cannot give: `test/sql` drives DuckDB's
  own sqllogictest runner for that, and pins the loop end to end — two
  overlapping DuckLake transactions where a compatible loser re-drives and
  lands (with row ids dense across the race), and an incompatible one
  throws. What a retry reads mid-flight is pinned alongside it: the
  `ducklake_snapshot_changes` rows above a snapshot id, and the exact
  `changes_made` entries moraine authors on the verb path.
- Options through the staged-row path: `set_option` records the option,
  global or table-scoped, and mints no snapshot; a second write of one key
  wins over the first; a delete removes it and the scope resolves to its
  parent's value again.
- Group commit: a group costs fewer object-store writes than the same
  members committed separately; every member mints its own consecutive
  snapshot and time travel resolves each one to that member's state; a
  member stages against what the members before it left; a failing member
  aborts the group; a group that loses a disjoint race re-runs and lands
  every member.
- Coalescing: a burst of concurrent `commit` calls costs fewer
  object-store writes than the same calls made one at a time, while each
  still mints its own snapshot; and a commit whose future is dropped part
  way — the shape a host interrupt takes, swept across the commit's life —
  never strands the commits sharing its batch.

E2E testing validates the protocol against real DuckLake SQL.

## Alternatives considered

- **Mechanical re-stage on benign-race retry** (re-derive ids, re-stamp the
  previously staged records, re-commit — without re-running the closure).
  Rejected: cheaper per retry, but it re-validates nothing. Logical premises
  the closure established against the old head — name uniqueness above all —
  are never re-checked against the commits that won the race, so two
  concurrent same-name creates would both land. The closure re-run costs one
  in-memory pass and is the only retry form that is correct by construction.
- **Internal rebase of true conflicts (replay staged mutations against the
  new head).** Rejected: replaying a true conflict requires re-running the
  DuckLake operation, which lives above the DuckLake-agnostic core. It
  would put DuckLake transaction semantics in the wrong layer and can
  silently produce wrong results if the replay logic is incomplete. Abort
  + typed error is honest; benign races — safely re-drivable because the
  verb path re-runs the caller's closure against the new head — are the
  only thing retried internally.
- **Snapshot-level conflict detection (any concurrent commit conflicts).**
  Rejected: serializes all commits and defeats the benign-race retry —
  disjoint-table writers would falsely conflict. The single-`WriteBatch`
  CAS already serializes the physical write; there is no reason to also
  serialize logically-disjoint commits.
- **Entity-level conflict detection (per-file/column) everywhere.**
  Rejected, but with one exception now built. DuckLake's matrix is
  operation-grained over tables everywhere except delete-vs-delete on one
  table, which it checks at `data_file_id` grain — and moraine reproduces
  exactly that one check (above), because a delete already names its
  target file and carrying the id costs a set comparison. Generalizing the
  idea to columns or to per-file inserts is what stays rejected: it buys
  no concurrency the append-append refinement does not already give, and
  it would put entity-shaped knowledge in a classifier whose whole virtue
  is not needing any.
- **Global `next_row_id` in the snapshot record.** Rejected: contradicts
  DuckLake (`next_row_id` is per-table in `ducklake_table_stats`) and would
  make every insert into any table contend on one counter, turning benign
  cross-table concurrency into counter churn. Per-table placement aligns
  allocation with conflict granularity.
- **Pessimistic locking (a lease/lock record in `system` a writer must hold).**
  Rejected: reintroduces coordination state and a liveness problem (crashed
  lock-holder) that the optimistic head-CAS avoids. SlateDB fencing already
  provides the safety a lock would; optimism provides the concurrency.

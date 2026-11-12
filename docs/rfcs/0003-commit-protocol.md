# RFC 0003: Commit protocol

## Summary

A DuckLake mutation becomes one SlateDB transaction whose write set is applied
as one atomic batch. Every batch writes the head pointer, so SlateDB's
write-write conflict detection on that key is the head compare-and-set. Lost
races are classified against DuckLake's own conflict matrix: benign races
retry inside the catalog, true conflicts return a typed error. Two entry paths
feed one commit core, concurrent commits coalesce into one batch, and a commit
is acknowledged only after WAL durability and checkpoint publication.

## Topology

One process holds the read-write `Db`. Every other process opens a `DbReader`
through `Catalog::open_read_only`, which returns a `ReadOnlyCatalog`, never
opens a `Db`, and never bootstraps. There is no bound on the number of
readers.

A reader following the writer pins the checkpoint the writer publishes and
writes nothing, so read-only credentials suffice. `CatalogOptions::checkpoint`
pins a reader to an existing checkpoint id instead, for a fixed cut that never
sees a commit made after it. A writer refuses the option.

SlateDB fences by writer epoch, and the newest writer wins. A second process
opening the store read-write fences the incumbent, whose next durable write
fails with `Error::Fenced`. A fenced catalog writes nothing further and must
be reattached. Two processes attaching read-write take turns fencing each
other, so the operational rule is one read-write process per store.

<!-- site:omit -->
The single-writer path is complete on its own. A store may additionally keep
its commits in a bucket-resident commit log, which admits several committers.
That topology is specified in [the commit-log seam](0011-commit-log-seam.md)
and is optional.
<!-- /site:omit -->

## Commit sequence

A commit lands a set of staged catalog mutations in one batch:

1. Begin a SlateDB transaction at snapshot isolation. Read the head pointer,
   the head snapshot record, and the table statistics of every table the
   commit inserts into, all through the transaction handle, so the premise and
   the conflict window share one start sequence.
2. Allocate ids by advancing in-memory copies of the counters. Nothing is
   written yet.
3. Stage the write set. The record kinds are listed below.
4. Commit the transaction. The write set applies as one atomic batch, or the
   commit fails with a transaction conflict and nothing is written.

| Staged record | Content |
|---|---|
| Entity records | New versions into `current`. An ended version deletes its `current` key and writes its `history` key. |
| Inline records | Inlined inserts, inlined deletes, and file deletes, in the same batch as their metadata. |
| Table statistics | The advanced `next_row_id` and merged column statistics per touched table. |
| Snapshot record | Snapshot `N+1`: advanced global counters, `schema_version`, `changes_made`, and the moraine-internal conflict fields. |
| Head pointer | `snapshot_id` and `batch_seq`. |

Every batch writes the head pointer. A batch that mints no snapshot, such as
snapshot expiry, file cleanup, or an option write, reuses the standing
`snapshot_id` and still advances `batch_seq`. That write is what a concurrent
commit conflicts on, and what tells a reader the state moved under an
unchanged snapshot id. A coalesced batch advances `batch_seq` once, however
many snapshots its members mint.

SlateDB's conflict check runs inside its single batch-writer loop immediately
before the write is sequenced, and its detection state is in-process. That is
sufficient because the topology admits one writer process. Cross-process
safety is carried by fencing. The key layout is specified in [the storage
format](0001-storage-format.md).

## Conflict detection

The snapshot record's `changes_made` is written in DuckLake's grammar:
comma-separated `kind:payload` entries, created entries carrying quoted names,
every other entry carrying a numeric id. DuckLake re-parses the field during
its own retry and throws on unknown kinds, so the wire format is DuckLake's to
define. When moraine parses a kind it does not model, the change set is marked
unknown and classifies as a true conflict.

| Verb | `changes_made` entry |
|---|---|
| `create_schema` / `drop_schema` | `created_schema:"s"` / `dropped_schema:<id>` |
| `create_table` / `drop_table` | `created_table:"s"."t"` / `dropped_table:<id>` |
| Table alterations, index creation and drop | `altered_table:<id>` |
| `register_data_file`, `inline_insert`, `build_index_step` | `inserted_into_table:<id>` |
| `register_delete_file`, `inline_delete` | `deleted_from_table:<id>` |
| `expire_data_file`, `flush_inlined_data` | `merge_adjacent:<id>` |
| `expire_delete_file` | `rewrite_delete:<id>` |
| `update_table_stats`, `update_column_stats`, schema tags | no entry |

The legacy `compacted_table` kind is parsed and classified as compaction. It
is never emitted, because DuckLake's matrix does not check it.

When the head race is lost, the committer reads the snapshot records between
its premise and the new head and compares its change set against each. The
classifier in `crates/moraine/src/transaction/operations.rs` is symmetric. Two
commits conflict when any of the following holds:

- Either side carries an unknown kind.
- Both touch the schema list, by creating or dropping a schema. This is the
  one operation handled at schema-list grain.
- A table appears on both sides with incompatible kinds under the matrix
  below.
- Both delete from one table and the deletes meet at data-file grain.
- One created a table or view inside a schema the other dropped.

| | inserted | deleted | altered | dropped | compacted |
|---|---|---|---|---|---|
| inserted | benign | conflict | conflict | conflict | benign |
| deleted | conflict | file grain | conflict | conflict | conflict |
| altered | conflict | conflict | conflict | conflict | benign |
| dropped | conflict | conflict | conflict | benign | conflict |
| compacted | benign | conflict | benign | conflict | conflict |

Append against append is benign. Data-file registrations commute, and the
`next_row_id` and statistics they advance are re-derived on retry like the
global counters. Counter advancement is never a conflict by itself.

Delete against delete is decided at data-file grain. A `register_delete_file`
records the data file it targets in the snapshot record's
`deleted_data_file_ids`, and two commits deleting from one table are benign
when those sets are disjoint. The record also carries
`deletes_untargeted_files`. An inlined delete names a row, and a change set
parsed from DuckLake's grammar names no file, so either sets the flag and the
comparison falls back to table grain.

Option changes are outside the protocol. `set_option` and `unset_option` mint
no snapshot, bump no `schema_version`, and write no `changes_made` entry.
Options are last-write-wins, matching DuckLake.

Name collisions are invisible to the set comparison. Two commits creating
`orders` in one schema allocate disjoint table ids and classify benign.
Uniqueness is enforced by re-validation on retry: the verb path re-runs the
closure against the new head and returns `Error::AlreadyExists`. Staged records
must be re-validated before their snapshot ids are updated.

## Retry and the error contract

A benign race retries inside the catalog, up to `MAX_COMMIT_ATTEMPTS` of 10
attempts. The first attempt never waits. Each retry waits an exponential delay
starting at 2 ms and doubling to a 50 ms cap, plus jitter of up to 2 ms, so
two writers that collided do not collide again in lockstep.

| Outcome | Error |
|---|---|
| True conflict | `Error::CommitConflict` |
| Budget spent on benign races | `Error::RetryBudgetExhausted` |
| Submitted write not acknowledged | `Error::CommitOutcomeUnknown` |
| Writer fenced | `Error::Fenced` |

DuckLake decides retryability by a substring match on the lowercased error
text: it re-drives a commit whose message contains `primary key`, `unique`,
`conflict`, or `concurrent`. `CommitConflict` displays with the prefix `commit
conflict:` and is retried. `RetryBudgetExhausted` is terminal, and its text
contains none of the four substrings, so DuckLake's loop of ten attempts does
not multiply moraine's ten. Every other variant's text is held to the same
rule. An exhausted budget is logged at `warn` with the head it started from
and the snapshots it lost to.

A true conflict is never rebased internally. Re-driving it means re-running
the originating operation, which the core never held. DuckLake re-drives a
retryable error, and the application re-drives what DuckLake throws.

## Id allocation

| Counter | Location | Scope |
|---|---|---|
| `next_catalog_id` | snapshot record | global, every schema, table, view, column, and macro |
| `next_file_id` | snapshot record | global, data and delete files |
| `next_row_id` | table statistics record | per table, advanced by `record_count` on insert |

Global counters are read from the head snapshot and written advanced in the
new one. `next_row_id` lives in table-scoped state, so advancing it is a
mutation of that table, and two commits inserting into different tables never
contend. Two inserting into the same table resolve under the append rule: the
loser re-allocates above the winner's `next_row_id`. Row ids stay dense per
table, which [data inlining](0004-data-inlining.md) relies on.

## Schema version

`schema_version` advances only when a commit changes the catalog's shape.
DuckDB keys its schema cache on it. Advancing it on every insert defeats the
cache, and failing to advance it on `add_column` serves stale columns. The
verb path sets `schema_version` to the previous value plus one when any staged
operation is schema-changing, and carries it forward otherwise.

| Class | Verbs |
|---|---|
| Schema-changing | `create_schema`, `drop_schema`, `create_table`, `rename_table`, `set_table_schema`, `drop_table`, `add_column`, `rename_column`, `alter_column`, `drop_column`, `set_partitioning`, `clear_partitioning`, `create_view`, `alter_view`, `drop_view`, `create_macro`, `drop_macro`, `create_index`, `drop_index`, `set_tag`, `remove_tag` |
| Altered, version carried forward | `set_sorting`, `clear_sorting` |
| Data-only | `register_data_file`, `expire_data_file`, `register_delete_file`, `expire_delete_file`, `update_table_stats`, `update_column_stats`, `inline_insert`, `inline_delete`, `flush_inlined_data` |
| Outside the protocol | `set_option`, `unset_option` |

Tags on tables and views bump because DuckLake models them as alterations. A
sort-key change marks the table altered for conflict detection and does not
bump, matching DuckLake. On the staged-row path DuckLake computes
`schema_version` itself and moraine stores the value it authored.

## Entry paths

Both entry paths use the same commit function. They differ in what
a retry re-runs.

### Verb path

`Catalog::commit` takes an async closure over a `Transaction`. The closure may
run again after a concurrent commit and must not perform external side
effects. A retry re-reads a fresh premise at the new head, re-runs the
closure, re-allocates ids from the advanced counters, and re-stages.
`Catalog::commit_group` commits a slice of `CommitMember` closures as one
batch with one snapshot per member. The verb surface is specified in [the
public API](0002-public-api.md).

### Staged-row path

The extension drives moraine with row-level operations against the
`ducklake_*` tables. DuckLake authors the catalog mechanics itself: it reads
the counters from the snapshot row, computes the new ids and `schema_version`,
and stamps the new snapshot id into every row it stages. Moraine translates
each row onto the keyed layout. Ids, counters, and `schema_version` are stored
as DuckLake wrote them.

A staged commit is checked before translation: a stamped snapshot id at or
below the head it is measured against is a lost race and returns
`CommitConflict`.

A data-only staged commit joins the forming batch and is rebased onto the head
it lands on. Data-only means the parsed change set creates, alters, or drops
no schema, table, view, or macro and carries no unknown kind. The rebase moves
every head-derived value by the distance the head moved, which is the
arithmetic DuckLake's own retry performs:

- The snapshot id, and every `begin_snapshot` stamped equal to it, moves to
  the premise head plus one.
- File ids allocated from `next_file_id` move by the premise's advance of that
  counter.
- For a table an earlier commit also appended to, `row_id_start` on data
  files, `row_id` on inlined rows, and the written `next_row_id` move by that
  table's advance. The table statistics row is re-derived as the member's
  delta over the premise, with column statistics merged by minimum and
  maximum.

The base for the rebase is the view the rows were authored against, handed in
when the transaction opens. The rebase is refused for every other shape, and
for a member that appends rows without carrying its table's statistics row. A
refused member commits as a batch of one, and a lost race there surfaces as
`CommitConflict` for DuckLake to re-drive.

A head-preserving staged commit, such as expiry or cleanup, derives nothing
from the head. Losing the head race is moraine's to re-drive, up to
`MAX_HEAD_PRESERVING_ATTEMPTS` of 10 with the commit backoff, re-staged
against the head it finds.

A member whose own race is lost while staging declines: a `CommitConflict`
raised during staging leaves the forming batch as it found it, and only that
caller gets the error. Any other error from a member poisons the batch,
because a half-staged member cannot be unstaged.

A lone staged commit that loses its race tells the coalescer, and arrivals
hold for `RETRY_YIELD` of 250 ms before staging. Without the yield a session
committing without pause would beat every DuckLake retry of a schema change.

## Group commit

The `Coalescer` in `crates/moraine/src/transaction/commit/group.rs` is one per
store handle. A commit that finds no batch forming opens one. One that finds a
batch forming joins it. One that finds a batch in flight waits for the next. A
batch seals when the count of arriving callers reaches zero, or at
`MAX_BATCH_MEMBERS` of 64, since under saturation the arrival count never
falls to zero.

Members stage in order against a premise folded forward from the member before
them, so member `i` allocates ids and detects name collisions against
everything members `1..i` did. Intra-batch conflicts are unreachable. Each
member mints its own snapshot, and time travel resolves each separately.
Batching changes durability granularity only.

A sealed batch lands on a task of its own, so the caller that sealed it cannot
take the other members down when its own future is dropped. Members wait on a
watch channel for the batch's outcome.

The store has one flight slot. It is claimed under the lock that moves a batch
out of forming and released when the `Flight` guard that claimed it is
dropped, so a submission that panics or is dropped still admits the next
batch. Submission ends when the batch's write is visible and folded into the
projections, before it is durable. The next batch forms and submits while the
first awaits its flush, and the two share that flush.

The write that holds the slot is bounded by `FLIGHT_WRITE_DEADLINE` of five
minutes. A write still pending at the deadline is reported as
`CommitOutcomeUnknown` to every member, since it may still land. The
durability wait after the slot is released is unbounded. Both report a stall
every `STALL_INTERVAL` of 10 seconds in the log.

The batch is the unit of failure. A member that fails discards the batch and
its other members re-run. A lost race re-runs every member, each classifying
its own change set against the winners. A crash leaves every member committed
or none, as [crash recovery](0008-crash-recovery.md) specifies.

A member that stages no writes still shares the batch's outcome when its
premise includes uncommitted writes. A no-op against committed state alone
returns at once.

## Durability and acknowledgement

A commit returns success only after two waits:

1. WAL durability. The transaction's `WriteHandle` is awaited through the
   flush pacer below. An empty transaction returns no handle and waits for
   nothing.
2. Checkpoint publication. The writer's `Publisher` creates a
   `CheckpointScope::Durable` checkpoint named `moraine-read-head-v1` covering
   the handle's sequence. Publication must finish before success is reported.

Publication is coalesced: one checkpoint acknowledges every commit whose
sequence it covers. If publication fails after the write became durable, the
commit is reported as `CommitOutcomeUnknown` and the mutation is never
resubmitted by moraine. A publication-only failure never re-runs a catalog
mutation. On open the writer publishes a checkpoint covering its durable
progress before serving commits.

Readers pin the published checkpoint directly, as specified in [the read
path](0007-read-path-and-caching.md); a superseded publication is kept for a
grace rather than deleted. A reader opened after `commit` returns resolves
`N+1`. A reader opened before may lag by its poll interval, which is ordinary
snapshot isolation.

## Flush pacing

Every WAL flush is one object-store PUT. The writer opens its store with
SlateDB's flush timer off and paces flushes itself under
`CatalogOptions::flush_interval`, read as the minimum spacing between two
flushes, defaulting to 100 ms. `CatalogOptions::flush_on_commit` is the same
setting at zero. The attach options that set them are documented in the
extension README.

1. The floor is the spacing divided by `FLOOR_FRACTION` of 10. No two flushes
   start closer than the floor.
2. A commit that finds the spacing elapsed since the last flush flushes at
   once and waits only on its own PUT.
3. A commit that finds recent flushes carried no other commit waits only the
   floor since the last flush started, then flushes. A writer without siblings
   pays no spacing.
4. A commit that finds the spacing running behind a flush that carried riders
   joins one deferred flush. The first such commit performs it. The rest await
   their handles on it.
5. The deferred flush runs when the spacing elapses, or earlier when as many
   commits have joined it as the population mark, whichever comes first, and
   never before the floor.
6. The population mark is the most commits any of the last `RECENT_FLUSHES` of
   3 flushes carried. A coalescer batch counts its members.
7. An early close carries only the mark and measures nothing. A flush that
   waited the whole spacing out measures the population.
8. A measurement is trusted for `MEASUREMENT_SPACINGS` of 10 spacings. After
   that the next flush waits the spacing out again, which is how the mark
   grows.
9. A commit that finds a flush in the air schedules the next flush for when
   the spacing elapses from that flush's start.
10. A scheduled flush that ends without carrying its riders wakes them, since
    no timer would.

A lone commit costs one PUT of latency. A burst costs at most the spacing plus
one PUT. Under a burst every commit inside one spacing is durable together in
one PUT. The timer stays off because every write reaches the WAL through a
commit that waits for durability, so nothing is left for a timer to sweep up.
A store opened for a migration keeps the timer, under
`CommitDurability::OnFlushInterval`.

## The log's object store

`CatalogOptions::wal_store` takes a `WalStore`, an object store plus the name
the catalog records for it. The WAL is written there, and every other object
stays in the catalog store. The keys do not move: the log keeps the catalog's
own path within the other store. The log store must be as durable as the
catalog store, since a commit no sorted-string table holds yet lives only
there.

The bootstrap records the name as the global `wal_path` option beside
`encrypted` and `data_path`, and every open reads it back in the same point
read that validates the format stamp. An open whose log store disagrees with
the record is refused with `Error::Configuration`:

| Recorded | Configured | Result |
|---|---|---|
| a store | none | refused |
| none | a store | refused |
| a store | a different store | refused |

Replaying an empty log over live state would lose every commit no
sorted-string table carries yet. A migration opens the writer itself and is
held to the same record.

A bootstrap that configures an external log writes its memtable out before the
open returns, so the catalog store carries the stamp and the name from the
moment the catalog exists. Otherwise an open handed the wrong store would find
no catalog and bootstrap a second one over the first.

`Catalog::move_wal_store` requires the catalog to be detached from every
process. It moves the log in two opens and returns a `WalStoreMove`:

1. Open the writer against the recorded log store, refusing any other, and
   write the memtable out as a sorted-string table. The old log then holds
   nothing a replay needs.
2. Open the writer against the new log store, record its name there, and write
   that out too. The recording commit is the only write that reaches the new
   log.

A destination nested with the lake's recorded `data_path` on one store is
refused before the first open drains anything. DuckLake's orphaned-file
cleanup lists that root and would delete log objects. Every crash point leaves
the catalog openable with nothing lost: before the second commit is durable
the record names the drained old store, and after it the record names the new
store, whose log holds the commit saying so. No window refuses both stores.

## Bootstrap

Opening an empty store is itself a commit. The open begins a transaction and
reads the migration marker and the format stamp.

| State | Result |
|---|---|
| Migration marker present | `Error::Migration` |
| Format newer than `MAX_FORMAT_VERSION` | `Error::Migration` |
| Format below `MIN_FORMAT_VERSION` | `Error::Migration`, migrate with `Catalog::migrate` |
| <!-- site:omit -->Format keeps commits in a commit log, no provider registered | `Error::Migration`  <!-- /site:omit -->|
| Format absent | bootstrap |

Bootstrap stages one batch: the format stamp at `MAX_FORMAT_VERSION` with the
writing moraine version, snapshot `0` with `schema_version` 0,
`next_catalog_id` 1, `next_file_id` 0, and `changes_made` of
`created_schema:"main"`, the `main` schema as entity 0 with its name entry,
the global option record, and the head pointer at snapshot 0 and `batch_seq`
0. The batch commits under the same head conflict detection as any commit. The
`main` schema takes id 0 from the ordinary counter, so a fresh store is
DuckLake-attachable without a DuckLake-side bootstrap pass.

A lost bootstrap race re-validates and finds the initialized store. An open
fenced while creating the catalog has written nothing and re-attempts, up to
`GENESIS_ATTEMPTS` of 3.

A commit writes with SlateDB's generated sequence number. `seqnum` is never
pinned. The sequence space is global to the store and advanced by index
builds, reclamation, and migration batches that mint no snapshot, so no
function of a snapshot id stays in step with it. Deduplicating a re-drive
across a process death needs a caller-supplied idempotency token, which the
protocol does not carry. A re-driven commit is an ordinary commit.

<!-- site:omit -->
## Commit log offer

When `CatalogOptions::commit_log` names a provider, or one is registered for
the process, `submit_batch` offers the sealed batch through
`CommitStore::write_at` at the sequence derived from its premise before any
SlateDB commit. A won slot is the commit point, and the session's own
transaction is rolled back. A lost offer is a lost race. Acknowledgement still
waits for a published checkpoint covering the slot. Everything else about the
log is in [the commit-log seam](0011-commit-log-seam.md).
<!-- /site:omit -->

## Submission outcomes and cancel safety

The coalescer distinguishes a forming batch from a submitted one.

| Outcome | Meaning | Members |
|---|---|---|
| `Committed` | durable and acknowledged | return their snapshot ids |
| `LostRace` | rejected at the head | re-run and classify |
| `Abandoned` | dropped before submission, nothing written | may re-attempt |
| `Unknown` | submitted, acknowledgement lost | `CommitOutcomeUnknown`, never re-attempt |
| `Fenced` | writer lost its epoch | `Fenced`, terminal |

The commit write is spawned onto the runtime. An interrupt drops the caller's
future and cannot tear the write. The bridge polls the host's interrupt probe
once before the commit future is first polled, then about every 100 ms while
it is pending. Cancellation has three cases:

- **Interrupted before the first poll.** Preparation is dropped and the
  catalog is unchanged. The ABI reports `INTERRUPTED`.
- **The core future reports its result.** The committed snapshot id is
  returned.
- **Cancelled after polling started.** The ABI reports
  `COMMIT_OUTCOME_UNKNOWN`. A spawned write keeps running and may land durably
  or fail. It is never torn. Cancellation during preparation is reported the
  same way, because the bridge does not observe the core's submission
  boundary.

Moraine never replays an unknown outcome. The caller must retain the
operation's external files and reconcile against catalog state before
resubmitting. A changed head alone does not resolve an operation's fate, and
there is no durable client operation id. Writer fencing after submission
preserves the potentially committed outcome in the same way. How the outcome
crosses the C ABI is specified in [the extension
surface](0006-extension-surface.md).

## Tests

- Disjoint concurrent commits and same-table concurrent appends both land,
  with the loser's row ids dense above the winner's `next_row_id`.
- Concurrent deletes from one table are benign on disjoint target files,
  conflict on a shared file, and fall back to table grain for inlined or
  wire-parsed deletes.
- Concurrent same-name creates: one lands, the other's closure re-run returns
  `AlreadyExists`.
- A data-only staged commit that loses a benign race lands rebased, equal to a
  sequential commit of the same rows after the winner, and a refused shape
  returns `CommitConflict` with the store unchanged.
- Every schema-changing verb increments `schema_version` and every data-only
  verb carries it forward, differential against a stock DuckLake catalog.
- A commit whose future is dropped at any await never strands the members
  sharing its batch, and a stalled write reports `CommitOutcomeUnknown`.

## Alternatives

- **Pessimistic locking with a lease record a writer must hold.** Rejected: it
  adds coordination state and a crashed-holder liveness problem that the
  optimistic head write avoids, while fencing already provides the safety a
  lock would.
- **Per-row or per-column conflict detection.** Rejected: DuckLake's matrix is
  table-grained everywhere except delete-versus-delete, which moraine
  reproduces at data-file grain. Finer grain adds no concurrency beyond what
  the append rule gives.

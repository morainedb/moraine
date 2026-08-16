# RFC 0021: Maintenance orchestration

- **Date:** 2026-07-24 (revised 2026-08-03: substrate merge and store census)

## Summary

Three systems reclaim space beneath a moraine lake: DuckLake (expiry,
cleanup, compaction), SlateDB (LSM compaction and its own file collection),
and moraine itself (orphaned index entries, RFC 0016). An operator drives
the first two by hand in an order nothing documents, and the third does not
exist — RFC 0016 defers its sweep to "RFC 0007's maintenance posture" and
RFC 0007 owns no verb to hang it on. This RFC makes maintenance **a
scheduled pass inside the writer**: a thread the shim starts at `ATTACH`
issues DuckLake's own maintenance SQL through its own connection, reclaims
moraine's orphaned index ranges, and merges the substrate's accumulated
dead versions, in a fixed order, retaining one report per pass. It lives in
the shim because that is the only layer reaching both DuckLake's SQL and
moraine's core, a deliberate exception to the thin-shim rule. moraine
computes no retention policy: **every interval and every step is configured
at attach**, and an attach that configures nothing schedules nothing.

The substrate step and its companion measurement — `moraine_store_census`,
which reports what a store weighs subspace by subspace — were added after a
production store reached 3.4 GB of objects while serving a single live
snapshot, with a read-only attach against it taking 642 s. SlateDB's own
collector does not close the reclamation gap: it deletes objects nothing
references, and superseded versions inside a referenced SST are not that.

**The census adjudicated that incident against the step it arrived with**,
which is what a measurement is for. It found the store ~100% live — 3.36 GB
of equality-index entries, 99.6% of the whole — and a full-store merge
reclaimed 0.08%. So the merge is a standing capability for the store that
*has* dead weight, not that store's remedy, and the census earned its keep
by saying so before an operator spent a rewrite of the store finding out.
The attach cost proved to be live cost, addressed by RFC 0009's caching.

## Goals

- **Maintenance runs without an operator driving it**, in an order that is
  enforced rather than documented.
- **moraine chooses no policy.** No default interval, no default steps.
  RFC 0007's and RFC 0008's non-goals survive.
- **The orphaned-index-entry sweep gets a home and a design.** RFC 0016
  designed it and left it to "a bounded background sweep". Per-index
  reclamation exists (`Catalog::reclaim_index_entries`), but nothing
  discovers *which* indexes are dead or drives the reclamation, so the
  ranges leak in practice.
- **Substrate bytes are measurable and reclaimable.** An operator can learn
  which subspace holds a store's bulk without reading the store, and can
  make a quiet store's dead versions actually go away. Both without taking
  the writer down.
- **Safe by construction.** The sweep rests on an invariant (catalog ids
  are never reused), not on running at a quiet moment. Every step is
  idempotent, so an interrupted pass is safe to re-run.
- **One mechanism.** The scheduled pass and the on-demand trigger run the
  same code, in the same order, producing the same report.

Non-goals:

- **Reimplementing any DuckLake maintenance function.** They are called,
  not wrapped. RFC 0007 and RFC 0008 remain the specifications of what they
  do to moraine's keyspace.
- **A retention policy**, compaction threshold, or grace period. The
  substrate step names a tree and asks for all of it; which SSTs merge into
  which sorted run stays SlateDB's decision.
- **A moraine-side merge engine.** SlateDB executes; moraine submits.
- **Repairing the read-only read path.** RFC 0009's caching and incremental
  refresh remove the *repeat* cost of a read-only read; they do not remove
  the one full `current` materialization a cold attach pays. That
  materialization is proportional to `current`'s **live** entities, which no
  merge reduces — a store measured ~100% live reclaimed 0.08% — so the two
  are complements only in the sense that they address different costs, and
  for a slow attach RFC 0009's is usually the one that matters. Step 8
  reduces dead bytes, and a store may have none.
- **Checkpoint lifecycle.** SlateDB's built-in collector owns it; adding the
  same read-modify-write operation to this pass would create a second owner.

## Background

**DuckLake** drives the data-and-history story: expiry issues the row
cascade moraine translates, cleanup deletes Parquet and drains the
schedule, compaction rewrites live files. RFC 0007 and RFC 0008 establish
that moraine's job is translation and projection, and that a moraine-side
engine was rejected. This RFC sequences DuckLake's functions; it does not
replace them.

**SlateDB** already collects its own superseded objects, in every moraine
writer, with no help from this design — and step 8 is not that, since
collection deletes whole objects nothing references while step 8 rewrites
objects the manifest still does. `StoreBuilder::settings`
(`open.rs:104`) overrides only the flush cadence and cache options, so
`garbage_collector_options` stays at `Settings::default()`'s
`Some(GarbageCollectorOptions::default())` (`config.rs:993`), and
`Db::builder(…).build()` constructs and starts a collector from it
(`db/builder.rs:699-725`): all six tasks — manifest, WAL, WAL fence,
compacted, compactions, detach — every 60s, deleting only what is
superseded, unreferenced by any active checkpoint, and older than a
5-minute `min_age`. A size-tiered compactor polls alongside it every 5s.

The steady state needs nothing: expiry's deletes become tombstones ordinary
compaction removes, the collector reclaims the SSTs that compaction
supersedes, and `TagSegmentExtractor` (`store/segment.rs`) gives each
subspace its own segment so `history` churn compacts without disturbing
`current`.

**The gap is the store that leaves the steady state.** Both mechanisms are
conditioned on write pressure that a quiet store does not supply. The
scheduler is size-tiered and proposes per tree as tiers fill
(`slatedb-0.14.1/src/size_tiered_compaction.rs:179-220`); a tree that stops
receiving flushes proposes nothing, indefinitely. The collector deletes only
what nothing references, and an SST full of superseded versions is
referenced by the manifest. So a store that churned for months and then went
quiet holds its dead weight permanently, and resuming traffic reclaims the
top tier long before the bottom. Measured on the store that motivated this:
71 objects / 3.4 GB, unchanged over fifteen minutes of a fully quiet writer.

**Segmentation is applied at flush, not at merge**, which is what makes a
per-subspace treatment both possible and precise. SlateDB treats an
extractor-configured database as mandatorily fully segmented — the root tree
is empty by construction (`manifest/mod.rs:945`) — and each memtable is split
into one SST per touched prefix *before upload* (`flush.rs:66-140`,
`memtable_flusher/uploader.rs:195-206`). No SST in a moraine store mixes two
subspaces, at any level, under any compaction backlog. Reads are routed to
the overlapping segments only (`reader.rs:156-175`, `manifest/mod.rs:756`),
so a scan of `current` — which is all a materialization does
(`transaction/commit.rs:396`) — opens `current`-segment SSTs and nothing
else. Two consequences follow. A subspace no materialization reads — `index`
above all — costs a catalog **scan** nothing, whatever it weighs. And the
manifest's per-segment sizes *are* a per-subspace census, available without
reading the data.

**Routing isolates scans; it does not isolate an attach.** The claim above is
about the scan, and generalizing it to the whole attach would be wrong: the
manifest lists every SST in every segment and is read whole before any
routing applies, so an open does pay for a large subspace — through the SST
*count* it contributes, never its bytes. That term is measured
(`BENCHMARK.md`) at ~15 µs per SST across the store, with a sweep that grew
`index` to 37.8 MB at a fixed SST count and saw a perfectly flat open. On a
store carrying one L0 SST and at most one run per segment — 3.36 GB of
`index` in a single run — it is ~0.2 ms. It would take on the order of a
million SSTs to matter, so the isolation holds in practice while resting on
a narrower claim than "cannot cost anything".

**moraine** owns one reclamation duty and does not discharge it. RFC 0016's
`index` subspace holds one entry per indexed row. `drop_index` ends the
definition into `history` (and `drop_table` ends its indexes with it), so
entries become invisible immediately — lookups resolve only against live
definitions — but are never deleted. The range leaks permanently, on every
dropped index and every dropped table that had one.

## Design

### Why the writer schedules itself

Maintenance cannot be cronned against a live lake. Opening a SlateDB `Db`
fences — `WriterFencer::fence` bumps the writer epoch and any parallel
older writer fails with `SlateDBError::Fenced` — so a second process
invoking the CLI would kill the application's writer. Since every step but
orphan detection mutates the catalog through the single writer handle, both
the work *and* its schedule must live in the process holding it. That is
RFC 0004's topology, not a preference.

Host-driven scheduling was the obvious alternative and does not survive the
same constraint: the writer is often a DuckDB CLI or an application with
nowhere to put a timer, and an external timer cannot help.

**The core cannot orchestrate.** moraine's core is a Rust library *beneath*
DuckDB, holding a SlateDB handle and knowing nothing of SQL, so
`Catalog::maintain` can never call `ducklake_expire_snapshots`. Nor can the
per-attach tokio runtime (`runtime.rs:32`), which is Rust-side and cannot
reach SQL. The scheduler is therefore a C++ thread holding the
`DatabaseInstance`, opening a fresh `Connection` per pass — the pattern
`wal_replay.cpp:399` already uses.

**The thin-shim exception, stated.** The repository rule is that logic
accumulating in `moraine-duckdb` belongs in the core. This RFC carves out
one exception: *sequencing DuckDB SQL, and scheduling that sequence, is
shim work, because the core can do neither.* It is bounded to sequencing —
the shim composes calls and collects outcomes, parses no results into
catalog state, and holds no maintenance logic of its own. The one
substantive mechanism, the sweep, lives in the core behind
`Catalog::maintain`. The core keeps its no-threads charter (RFC 0003); the
thread is shim-side.

### The sequence

Each pass runs these steps in order, skipping any the attach did not
configure:

| # | Step | Configured by | Issues |
|---|---|---|---|
| 1 | Expire | `EXPIRE_SNAPSHOTS[_OLDER_THAN\|_VERSIONS]` | `CALL ducklake_expire_snapshots('lake', …)` |
| 2 | Flush | `FLUSH_INLINED_DATA` | `CALL ducklake_flush_inlined_data('lake')` |
| 3 | Merge | `MERGE_ADJACENT_FILES` | `CALL ducklake_merge_adjacent_files('lake')` |
| 4 | Rewrite | `REWRITE_DATA_FILES[_DELETE_THRESHOLD]` | `CALL ducklake_rewrite_data_files('lake', …)` |
| 5 | Cleanup | `CLEANUP_OLD_FILES[_OLDER_THAN\|_CLEANUP_ALL]` | `CALL ducklake_cleanup_old_files('lake', …)` |
| 6 | Orphans | `DELETE_ORPHANED_FILES[_OLDER_THAN\|_CLEANUP_ALL]` | `CALL ducklake_delete_orphaned_files('lake', …)` |
| 7 | Sweep | `SWEEP_INDEXES` (default **true**) | `Catalog::maintain` — core |
| 8 | Merge store | `COMPACT_STORE[_SUBSPACE\|_TIMEOUT]` | `Catalog::compact_store` — core |

The call syntax is what the e2e suite already exercises against real
DuckLake (`tests/ducklake_load/maintenance.rs:47,91,150,230,333`).

**Why this order.** Expiry first, because it is the only step that
*shrinks* the catalog rather than adding to it, and DuckLake re-reads the
snapshot projection at the start of every transaction (RFC 0009) — every
later step is served a smaller one. Flush before merge, so its small
Parquet files are merge input. Merge and rewrite before cleanup, because
merge schedules its superseded bytes directly (RFC 0008) and cleanup drains
that schedule in the same pass. Cleanup before orphan detection, so the
schedule is drained first. The sweep before the store merge, because its
input is everything the earlier steps left behind. The store merge last,
because every step above it writes: expiry tombstones rows and the sweep
deletes index ranges, and merging first would leave exactly the tombstones
the pass just created for the next pass to find. The order is a cost
preference — every step is independently safe and idempotent in any order.

**DuckLake compaction — steps 3 and 4, not the store merge — is
deliberately not placed before expiry.** The tempting
rationale — that expiry would then reclaim what compaction superseded —
fails for both verbs, for opposite reasons (RFC 0008). *Merge* leaves
nothing to reclaim: its output backdates `begin_snapshot` to cover every
snapshot the sources covered and carries a per-row `snapshot_id`, so time
travel into it filters rows rather than selecting a different file. Having
subsumed the sources' whole visibility history, it hard-deletes their
catalog rows — current and history alike — and schedules their bytes at
once. *Rewrite* leaves rows ended but not dead: it materializes deletes, so
its output holds fewer rows than the source and a reader below it must
still see the deleted ones. The source is ended into history with nothing
scheduled — but at a snapshot minted moments earlier, so every retained
snapshot sits below it and RFC 0007's dead-row rule (no surviving snapshot
in `[begin_snapshot, end_snapshot)`) cannot fire. Those rows wait under any
ordering, reclaimable only once the rewrite's own snapshot ages out.

### Configuration

Everything is an attach option, alongside the existing family
(`catalog.cpp:553`). `META_MAINTENANCE_INTERVAL` sets the cadence; without
it no thread starts. Steps are named `META_MAINTENANCE_<step>[_<param>]`,
where `<step>` and `<param>` derive from DuckLake's own names by one rule:

> **`<function name minus its `ducklake_` prefix>` enables the step, and
> `<that>_<DuckLake's own parameter name>` supplies a parameter.**

So `META_MAINTENANCE_EXPIRE_SNAPSHOTS_OLDER_THAN` and
`META_MAINTENANCE_CLEANUP_OLD_FILES_CLEANUP_ALL`. The stutter in the second
is deliberate — a derivable rule that occasionally reads awkwardly beats a
vocabulary an operator learns twice. The step prefix is required because
three steps take a parameter named `older_than` and two take `cleanup_all`.

A bare `META_MAINTENANCE_<step> true` runs it with DuckLake's own defaults;
supplying a parameter implies enabling the step. Values pass through
unvalidated — a nonsensical `delete_threshold` is DuckLake's to reject.

One value shape does not survive the passthrough: DuckLake fails an attach
carrying a **list-valued `META_` option**, and not only for moraine's
options — any `META_<name> [1,2]` fails the same way. So
`expire_snapshots`' `versions` is spelled as a string,
`META_MAINTENANCE_EXPIRE_SNAPSHOTS_VERSIONS '[1,2]'`, which DuckLake
accepts for that parameter. This is a property of the passthrough, not of
the derivation rule, and needs no exception in it.

**`older_than` given an interval becomes a rolling window.** This is the
one place a value does not pass through verbatim, and the alternative is
broken rather than merely awkward: attach options are evaluated once, so a
timestamp written as `now()` freezes into a literal and a schedule would
expire against its attach-time instant for as long as it ran — retention
quietly ceasing while the lake moved on. An interval is instead rendered
as `now()::TIMESTAMP - <interval>`, which DuckLake evaluates per pass.
(The cast is load-bearing: `now()` is `TIMESTAMPTZ`, and subtracting an
interval from one needs the `icu` extension, while the `TIMESTAMP`
operator is always available.) A timestamp is still accepted verbatim, and
is the right thing for a one-off trigger call.

The lake name the steps are called with is **not** derived from this
catalog's own name. A lake attached with `METADATA_CATALOG` names its
metadata catalog itself, so the default `__ducklake_metadata_<lake>`
naming neither identifies it nor recovers the lake name by stripping. The
pass instead matches attached databases on path — the lake attaches
`moraine:<store path>` and this catalog reports `<store path>` — and falls
back to the prefix rule only when no such lake is found (a standalone
`moraine:` attach, which has no DuckLake above it).

Resolving a *name* to a moraine catalog has the same problem and one
answer: `ResolveMoraineCatalog` (`cpp/catalog.cpp`) accepts either name,
matching on path when neither derivation finds one. The maintenance
functions and the `moraine_index_*` family both go through it, so the
whole extension resolves lakes identically — the index family previously
carried its own copy of the prefix assumption and inherited the same bug.

Four options are moraine's own rather than derived, because steps 7 and 8
are: `META_MAINTENANCE_SWEEP_INDEXES` (default true) turns the sweep off and
`META_MAINTENANCE_BATCH_SIZE` bounds its deletes per commit;
`META_MAINTENANCE_COMPACT_STORE` (default **false**) enables the store merge,
`META_MAINTENANCE_COMPACT_STORE_SUBSPACE` narrows it to one named subspace
rather than every one, and `META_MAINTENANCE_COMPACT_STORE_TIMEOUT` bounds
how long the pass waits for the merge to commit. All are validated at bind —
an unknown option, an unknown parameter for a known step, an unknown subspace
name, a non-positive interval, batch size, or timeout, and a step disabled
while one of its own parameters is supplied are all `BinderException`s, so a
misconfigured attach fails rather than starting a scheduler that quietly
does the wrong thing. The subspace name is the one option whose vocabulary
the shim does not own, so it asks the core rather than keeping a second
copy — and it asks at bind, because a name checked only when a pass runs
would let a typo attach cleanly and then fail every scheduled pass,
unattended, for as long as it stood.

**DuckLake compatibility is strict.** Each moraine build targets the DuckDB
and DuckLake versions pinned by the repository, so the function names and
parameter signatures above are part of that supported build combination. A
missing function or changed signature is an incompatibility, not a capability
to detect and silently degrade: the configured call fails clearly. New
DuckLake parameters become available only when moraine exposes them and the
pinned-version e2e suite exercises them. The pin checker prevents an
unreviewed version drift, and the maintenance e2e cases make a signature
change fail while updating a pin rather than in a supported release.

**Defaults are the safe floor.** Steps 1–6 mutate the lake — writing
Parquet, minting snapshots, or deleting bytes — so none has a default. Step 8
destroys nothing a query can observe, but rewrites gigabytes and pays for
every byte in object-store traffic, so it defaults off for cost rather than
for safety. An interval alone schedules only the sweep, which touches
nothing a query can observe and costs two seeks when nothing was dropped.
Steps that move bytes run unattended only because an operator wrote down
that they should.

### The scheduler

One thread per read-write attach, started at `ATTACH` when an interval is
configured. Explicit `DETACH` stops and joins it before `moraine_detach`
(`abi.rs:775`) releases the handle. Without `DETACH`, destruction of the last
host `ClientContext` stops and joins it before that context releases its
`DatabaseInstance` reference. Moraine tags the connection a pass creates and
excludes it from the host count. DuckLake's temporary connection for the nested
metadata attach remains a host, but closing it does not stop the scheduler while
the caller's connection remains. The catalog destructor repeats the operation
as an idempotent fallback.

The connection-lifecycle hook is load-bearing. A running pass owns a DuckDB
`Connection`, and that connection retains the `DatabaseInstance`; waiting for
the database or catalog destructor to initiate the join creates an ownership
cycle. The last-host hook breaks the cycle while both the database and catalog
handle are still live. DuckDB calls connection-close callbacks under its
connection-manager lock, so the callback records a context state and that
state's destructor performs the join after the lock is released. Otherwise the
worker connection's own close would deadlock on the same lock.

Once every host context has closed, scheduled passes for that attachment stay
stopped even if an embedder retains the `DuckDB` object and later opens another
connection. Queries and manually triggered maintenance remain available. A new
schedule requires a new attach; this keeps one unambiguous ownership boundary
rather than reviving a thread whose original database-close sequence already
began.

Three properties the scheduler must hold:

- **Single-flight.** A pass still running when the next tick fires skips
  that tick rather than overlapping. Concurrent passes are safe — the sweep
  is idempotent — but their DuckLake steps collide under RFC 0008's
  conflict matrix, and a scheduler that manufactures its own conflicts is
  indefensible.
- **Stops before the database does.** Detach or destruction of the last host
  context sets the stop flag and joins. A pass already in flight completes
  before the join returns, so its connection and handle cannot outlive DuckDB
  state.
- **Failures are visible.** An unattended pass has no one to return an
  error to, so `moraine_maintenance_status` serves the **last 16 passes**,
  newest first, each carrying `started_at` and whether it was `scheduled`
  or `manual`, then one row per step: `step`, `status` (`ran` / `skipped`
  / `failed`), and `detail`. Retaining a window rather than only the
  newest pass is load-bearing — with one slot a failure is erased by the
  next success, and a short interval would hide strictly more than a long
  one. Sixteen is the binding cap: at eight fixed steps it is at most 128
  small diagnostic rows, enough to preserve fifteen later successes after
  one failure without turning status into an event log.

  The window is durable **inside the catalog**, as one unversioned
  `sys/maintenance_status` value holding the passes oldest-to-newest. A
  completed pass atomically reads the value, appends itself, drops the oldest
  overflow, and overwrites it. The status write is a separate durable SlateDB
  transaction after the pass: it mints no DuckLake snapshot, does not move
  `sys/head`, and therefore neither invalidates a catalog view nor appears in
  time travel. The first write lazily stamps additive format 5 so older
  binaries, which would otherwise silently omit the record, refuse the store.
  A status-write failure is logged and does not rewrite the outcome of the
  maintenance work that already completed.

  `started_at` is a `Timestamp`: microseconds from the Unix epoch, the width
  the record stores and the ABI passes. Carrying that width end to end keeps
  the conversion total in both directions, so neither the store boundary nor
  the ABI needs a range check on a value DuckDB already hands over as a
  microsecond count.

  Catalog storage wins over an external sidecar: it follows the lake through
  moves and credentials, uses the existing writer fence and durability path,
  and lets a read-only attach inspect the prior writer's failures. The value
  is bounded and overwritten, so it adds neither an unbounded subspace nor a
  cleanup policy.

**Read-only attaches never schedule.** A `DbReader` never opens a writer, so
no step runs — including step 8, whose merge only a writer's compactor could
execute. The census is the one part of this surface a read-only attach
reaches, and it reaches it through its own table function rather than
through a pass.

**A failed step abandons the rest of the DuckLake sequence, but never the
sweep.** The DuckLake steps depend on each other — cleanup drains what
merge scheduled — so continuing past a failure would do partial work on a
premise that did not hold. The sweep depends on none of them, and
suppressing it would be worse than useless: a misconfigured `older_than`
that DuckLake rejects would abort every future pass at step 1, so
moraine's own reclamation would stop for as long as the misconfiguration
stood while the leak it exists to fix kept growing. Abandoned steps are
still reported (`skipped`, naming what aborted them) rather than dropped,
so every pass emits the same rows and two passes stay comparable. Earlier
effects stand, and the next tick re-runs from the top.

Single-tier by design: one interval, one step set. Steps have genuinely
different natural cadences — the sweep is two seeks when nothing was
dropped, while `delete_orphaned_files` LISTs the entire data prefix — but no
measured deployment needs multiple in-process cadences. Encoding tiers into
flat attach options would multiply scheduler state and lifecycle ownership,
so this surface deliberately keeps one cadence. Evidence of missed work or
material excess cost would motivate a replacement design.

### The on-demand trigger

`CALL moraine_maintenance('lake')` runs one pass immediately and returns
its report. It takes **no parameters**: configuration lives at attach, and
a second configuration surface would be a second thing to keep faithful.

It issues no SQL on the caller's context. It runs the pass inline on the
calling thread but through a **connection of its own**, under the same
single-flight lock the timer takes — so a trigger and a tick can never
overlap, and the trigger waits out a pass already in flight. The separate
connection is what avoids the re-entrancy a SQL-issuing table function
would hit: `ClientContext::Query` takes `context_lock`, a plain
non-recursive `mutex` (`client_context.hpp:318`), so a query issued from
inside a running operator on the *same* context deadlocks. A fresh
connection has its own context, so the problem does not arise.

Running inline rather than handing work to the thread also means the
trigger needs no thread at all: an attach that configured no interval
still answers it.

The trigger exists for two reasons, not for ergonomics: `cargo xtask e2e`
needs a deterministic way to run a pass without waiting on wall-clock, and
an operator needs a way to run one before a backup or after a bulk load
without re-attaching.

**The store merge gets a trigger of its own**, `CALL
moraine_compact_store('lake')`, taking an optional `subspace`, `timeout`, and
`require_completed`. The last requires a timeout and turns any pending or
failed target into a query error; a skipped tree with no sorted runs is a
verified no-op. This is the operator-safe form for an index-only merge:
`moraine_compact_store('lake', subspace := 'index', timeout := 600,
require_completed := true)` cannot mistake successful submission for a
completed merge.

This is the one place a second configuration surface is worth its cost: the
case that motivated the merge is a store that bloated once and needs merging
once, and reaching it only through the pass would mean re-attaching a live
application's catalog to change a maintenance option. That is not a
configuration an operator wants to keep — it is a single action. The pass
remains the scheduled form; both call the same core verb, so neither can
drift from the other. Its two parameters are its own rather than a copy of
the pass's, because the pass configures eight steps and this configures one
merge.

`CALL moraine_store_census('lake')` is a separate table function, not a
trigger and not a step. It takes one named parameter, `live`, for the
scanning leg, issues no SQL, mutates nothing, and is available on a
read-only attach — which is the attach shape an operator investigating a
production store actually has. It emits one row per subspace carrying the
manifest version measured, so two censuses can be told apart, and the live
columns are **NULL rather than zero** when the scan was not asked for: a
subspace with no live keys and a subspace nobody counted are different
answers. It is the intended first move against a store whose size is
unexplained: run the census, read which subspace holds the bulk, and only
then decide whether the answer is step 8, a `cleanup_old_files` the lake
never ran, or neither.

**Explicit transactions are refused.** The caller blocks while its own
second connection writes the catalog, so running inside a user's `BEGIN`
invites a self-deadlock. Refused unless
`context.transaction.IsAutoCommit()` (`transaction_context.hpp:49`).

That refusal is a guard on the caller's own transaction, and it says nothing
about what else may be open elsewhere. The autocommit case is now driven
rather than assumed: a trigger runs to completion while another connection
holds an uncommitted write transaction against the same lake, and while the
calling connection's own implicit transaction is the most recent writer,
with the pass configured to do real work — DuckLake steps issuing SQL on its
connection, and a per-entry sweep committing throughout. What the concurrent
writer meets is a **conflict, not a wait**: the pass expires snapshots and so
alters the table under it, and DuckLake reports that in the retryable
language its own writers already handle. Contending for catalog state is
expected; deadlocking over it is what the refusal exists to prevent, and one
connection cannot express the question — hence a sqllogictest rather than a
CLI case.

### Orphaned index-entry reclamation

**The invariant that makes this safe.** `index_id` is allocated from the
global `next_catalog_id` counter (`verbs.rs:683`), so ids are monotonic and
never reused. Entries under an `index_id` not live at snapshot `S` can
never become live again — a dead range is dead forever. The sweep needs no
lock, no quiet period, and no coordination with the writer: it cannot
delete an entry a live index will want, because a live index's id was live
at `S` and is skipped.

**Discovery is a skip-scan.** Entry keys place `index_id` immediately after
the kind discriminant — `idx_index_prefix(kind, index_id)` covers exactly
`INDEX_KIND_PREFIX_LEN + size_of::<u64>()` bytes (`store/key.rs:551`) — so
the subspace is ordered by index id within each kind. For each kind: seek
to the start, read one key, decode its `index_id`; if live in the
`CatalogSnapshot` at `S`, seek past the whole index at
`idx_index_prefix(kind, index_id + 1)`; otherwise delete the range in
batches and continue. Cost is one seek per *distinct index id present*, not
one read per entry. A store whose indexes are all live pays two seeks per
index and deletes nothing.

**History is not consulted.** The dead set derives from what the `index`
subspace contains, checked against the live catalog — never from ended
definitions in `history`. Expiry may prune an index definition's history
record long before anyone sweeps, after which a history-derived dead set
loses the id and leaks the range forever. Deriving from the data being
reclaimed has no such failure mode, mirroring RFC 0007's preference for a
scan-based dead-row rule over maintained reference counts.

**Each batch is a head-preserving maintenance commit** — RFC 0007's shape:
one `WriteBatch`, no `ducklake_snapshot` insert, and `sys/head` written at
the standing snapshot id with its batch count advanced, which is what every
head-preserving batch does (RFC 0004). The advance is not bookkeeping: it is
the stamp that tells a reader state moved under an unchanged snapshot id
(RFC 0009), and it is the single write-write anchor, so a sweep batch racing
a commit loses or wins that race cleanly instead of interleaving with it.
Between batches the sweep yields, so a large reclamation never holds the
writer. Beyond the head record the two write disjoint keys — the sweep
touches only dead index ids — so a lost race costs a retry of one batch and
never a redo of the scan.

### The store merge

The step names a tree and asks SlateDB to merge all of it. It builds
SlateDB's own scheduler
(`SizeTieredCompactionSchedulerSupplier::compaction_scheduler`), calls
`generate` (`compactor.rs:177`, a provided trait method) with
`CompactionRequest::Full` or `FullSegment { segment }` (`compactor.rs:245`),
and submits each returned spec through `Admin::submit_compaction`
(`admin.rs:200`). It constructs no `CompactionSpec` itself: `plan_full_tree`
(`compactor.rs:222`) makes every sorted run in the tree a source and the
tree's lowest sorted-run id the destination, and a later upstream change to
what "merge this tree fully" means is inherited rather than re-derived.

**The merge is bottom-inclusive by construction**, which is what makes it
reclaim rather than relocate. Sorted-run ids are globally monotonic
(`size_tiered_compaction.rs:456`), so a tree's lowest id is its bottom run;
the worker sets `is_dest_last_run` by comparing the destination against
`compacted.last()` (`compaction_worker.rs:637`) and hands it to the retention
iterator (`compactor_executor.rs:409`), which is the flag permitting
superseded versions and tombstones to be dropped instead of carried forward.
A full-tree plan satisfies it every time. L0 is deliberately excluded
upstream so flushes continue during the merge (`compactor.rs:250-256`); the
residual is bounded by `l0_max_ssts` and is ordinary size-tiered work.

**Submitting is not running, so the step waits.** `submit_compaction`
persists a `Submitted` entry; the compactor already running inside the writer
promotes and executes it on its poll tick
(`compactor.rs:715-721`). The step then polls `Admin::read_compaction`
(`admin.rs:156`) until each id reaches a terminal `CompactionStatus`
(`compactor_state.rs:246`) — `Completed` or `Failed` — and re-reads the
manifest to report the byte delta. On timeout the merge is reported pending
and **is not cancelled**: it keeps running, and a later census shows the
result. The poll cadence derives from the compactor's own poll interval
rather than being configured; a knob there would be a knob about SlateDB's
scheduler, which RFC 0003 keeps out.

**Read-write only.** Submission alone needs no writer — `Admin` opens no `Db`
and fences nothing — but execution does, since the compactor that promotes a
`Submitted` entry lives in the writer process. A read-only catalog would
queue work nothing would run and then wait out its timeout, so it refuses
with `Constraint`, like every other write-path verb.

A tree with no compacted sorted runs is reported skipped rather than failed —
what `Full` does silently. Naming such a subspace explicitly is an upstream
error; the verb reports it the same way, so the whole-store and
single-subspace forms read alike.

**A tree already being merged is adopted, not skipped.** Submitting a second
plan for it would claim a destination the executor refuses, so the step waits
on the merge already in flight and reports its outcome as the tree's. The
alternative — reporting it skipped — was measured and is worse than it
sounds: opening a writer starts SlateDB's compactor, which proposes work
immediately, so an on-demand merge asked for straight after an attach found
every eligible tree busy and reclaimed nothing. Adoption satisfies what the
caller asked for, since the tree ends up merged either way.

**A store whose bulk sits in L0 is skipped, and that is the answer**, not a
gap. L0 holds what recent flushes wrote, so a tree with bytes there and no
sorted runs has never been compacted at all — the size-tiered scheduler has
not yet fired, and it will as soon as the tier fills. Merging L0 here would
mean moraine choosing sources, which is the policy line this design draws,
and it would claim those SSTs for the duration of the merge, stalling the
writer once L0 reached `l0_max_ssts`. The census already tells the operator
which case they are in — `l0_ssts` against `sorted_runs`, per subspace — so
a skip is legible rather than mysterious. The store this design exists for
is the opposite shape: bytes long since merged into runs the scheduler will
never revisit because the writes stopped.

### The store census

Measurement is not a step — it belongs to the on-demand surface, because its
whole purpose is to be run before deciding whether a merge is worth it.
`moraine_store_census('lake')` returns one row per subspace: physical bytes,
L0 SST count, sorted-run count, and SST count across those runs, read from
the latest manifest through `Admin::read_compactor_state_view`
(`admin.rs:185`) → `VersionedManifest::segments()` (`manifest/mod.rs:945`) →
per-segment `l0()` / `compacted()` (`manifest/mod.rs:191,196`) →
`estimate_size()` (`db_state.rs:238,472`).

Every subspace is reported, whether or not the manifest carries a segment
for it: a subspace absent from the manifest is one whose writes have not
been written out, which is a measurement rather than a reason to omit the
row, and two censuses of one store are then comparable row by row. The
physical figures likewise count only what has been written out — a commit
still in the write-ahead log is in none of them.

**The manifest is not the whole store, so the census also lists it.** Those
per-segment figures cover SSTs and nothing else, which would make a store
whose weight is its write-ahead log read as nearly empty while costing every
reader dearly — an unpinned read attach replays that log before it
materializes anything, and no merge touches a byte of it. So a census also
totals the object store by kind (log, manifest, SST, everything else),
summing to the whole, and the gap between `sst_bytes` and `total_bytes` is
what says whether a merge is even the right lever. This is the one part
whose cost grows with the store — one paginated listing — and the one part
that can fail on its own: read-only credentials frequently grant
`GetObject` without `ListBucket`, so a listing that is refused leaves the
totals absent rather than failing the census, since the operator holding
those credentials is exactly the one who needs the rest of it.

That default costs two object reads and is independent of store size. It is
enough to answer the question a bloated store poses — *is the bulk `index`,
which no scan touches, or `current`, which every attach reads?* — because
segmentation is by subspace (Background). Subspace names are moraine's, mapped
from the segment prefix byte by `store`, the only layer that knows the
discriminant assignment; a segment whose prefix is not a known discriminant is
reported as unknown rather than dropped or refused, since a census exists to
describe a store that has gone wrong.

A boolean argument adds a scanning leg: a read-only pass over every subspace
counting live keys and their encoded bytes, breaking `current` down by
`CurrentKey` variant so the deletion schedule (`current/gcfile`, RFC 0007) is
separated from entity records. It decodes keys, never values. It costs a full
read of the store, which is why it is opt-in, and it is the only way to say
what fraction of a subspace is live — that is, how much a merge would
actually reclaim, and whether the answer is instead a `cleanup_old_files` the
lake never ran.

The census serves read-only attaches. Both legs read; neither writes.

Its scans use the core read profile: 8 MiB of read-ahead and 32 fetches in
flight, sized for a remote object store. Those figures are fixed implementation choices, not maintenance
attach options; RFC 0009 records the measurement required to replace them.

The real-endpoint measurement pins the scale and the limit of this lever. An
ARM worker and bucket in the same `us-west-2` region opened a deliberately
churned 40-table catalog in 401.1 ms median over seven fresh read-only handles:
248.4 ms to open the reader and 150.6 ms to materialize the first view. A full
merge completed four subspaces, reducing their physical bytes from 135,443 to
75,379 and their SST count from 24 to 11, yet the next seven handles took
411.6 ms median, with wholly overlapping ranges. The census also showed 257
WAL objects and 5,011,208 manifest bytes after the merge — object classes the
subspace merge does not reclaim. So a merge's measured GET reduction remains
real, but only matters when those GETs carry material weight in the attach;
the census is a prerequisite, not merely post-hoc reporting (`BENCHMARK.md`,
"Cold attach against AWS S3").

### The core verb

```rust
pub struct MaintenanceRequest {
    pub sweep_orphaned_index_entries: bool,  // default true
    pub batch_size: usize,                   // default 1024
}

pub struct MaintenanceReport {
    pub indexes_swept: u64,
    pub index_entries_reclaimed: u64,
}

impl Catalog {
    pub async fn maintain(&self, request: MaintenanceRequest)
        -> Result<MaintenanceReport>;

    pub async fn record_maintenance_pass(&self, pass: MaintenanceStatusPass)
        -> Result<()>;
}

impl ReadOnlyCatalog {
    pub async fn maintenance_status(&self)
        -> Result<Vec<MaintenanceStatusPass>>;
}

pub struct MaintenanceStatusPass {
    pub started_at: Timestamp,
    pub trigger: String,
    pub steps: Vec<MaintenanceStatusStep>,
}

pub struct MaintenanceStatusStep {
    pub step: String,
    pub status: String,
    pub detail: String,
}
```

Steps 8 and the census are two more `Catalog` verbs, neither a `Transaction`
mutator and neither minting a snapshot:

```rust
/// Which trees to merge.
pub enum CompactionTarget {
    /// Every subspace holding compacted sorted runs; the rest are skipped.
    WholeStore,
    /// One subspace.
    Subspace(SubspaceName),
}

pub struct CompactStoreRequest {
    pub target: CompactionTarget,
    /// How long to wait for every submitted merge to commit. `None`
    /// returns as soon as they are submitted.
    pub wait: Option<Duration>,
}

pub struct CompactStoreReport {
    pub merges: Vec<SubspaceMerge>,
}

pub struct SubspaceMerge {
    pub subspace: SubspaceName,
    /// Completed | Failed(String) | Pending | Skipped(&'static str)
    pub outcome: MergeOutcome,
    pub bytes_before: u64,
    pub bytes_after: Option<u64>,
}

pub struct CensusRequest {
    /// Add the scanning leg: live keys and bytes per subspace. Costs a
    /// full read of the store.
    pub count_live_entries: bool,
}

pub struct StoreCensus {
    pub manifest_id: u64,
    pub subspaces: Vec<SubspaceCensus>,
}

pub struct SubspaceCensus {
    pub subspace: SubspaceName,
    pub bytes: u64,
    pub l0_ssts: u32,
    pub sorted_runs: u32,
    pub sorted_run_ssts: u32,
    /// `None` unless the request asked for the scanning leg.
    pub live: Option<LiveCount>,
}

impl Catalog {
    pub async fn compact_store(&self, request: CompactStoreRequest)
        -> Result<CompactStoreReport>;
    pub async fn store_census(&self, request: CensusRequest)
        -> Result<StoreCensus>;
}
```

`SubspaceName` names each subspace RFC 0002 defines — `changelog`
(RFC 0009) included, whose window bounds it but whose physical bytes still
accumulate between merges — plus an unknown discriminant. Every struct is
`#[non_exhaustive]`, so a future leg is additive.

No `slatedb::` type appears in any signature — RFC 0003's substrate
rule holds, and it is why the census reports bytes and counts per subspace
rather than SST ids, key ranges, and run sizes: the latter is the substrate's
vocabulary, and nothing an operator can act on. RFC 0003's operation table
gains all three under a **Maintenance** group.

`Catalog` today holds only the store handle; `Admin` needs the path and the
object store. Both are retained at open alongside the handle, and are not
otherwise observable.

### The `DATA_PATH` overlap guard

`ducklake_delete_orphaned_files` LISTs the data prefix and deletes
everything the catalog does not reference (RFC 0007), and cannot know some
of those objects *are* the catalog. Nothing today prevents attaching
`'ducklake:moraine:s3://bucket/lake/catalog'` with `DATA_PATH
's3://bucket/lake/'`, which places every SlateDB SST, manifest, and WAL
object under the swept prefix. Step 6 running unattended on a timer makes
that a standing hazard rather than a one-off mistake, so the guard ships
with it: attach refuses, with `Constraint`, when the store path and
`DATA_PATH` are on the same object store and either is a prefix of the
other. Containment is compared by path component, so sibling prefixes that
merely share leading text (`…/lake` and `…/lakehouse`) attach normally, as
do different buckets and different store kinds.

Two limits are load-bearing. The check runs **before the catalog is
opened**, because bootstrapping a fresh store records `data_path` — a
check that waited until after the open would persist the dangerous value
and *then* refuse, leaving it for the next attach to inherit. And the
guard sees only the path moraine is told about: `META_DATA_PATH`, or a
value already recorded for the lake. DuckLake keeps its own unprefixed
`DATA_PATH` for the data layer and does not forward it to this metadata
attach, so an attach naming only that leaves nothing to compare.

### The close hazard on a multi-threaded runtime

A store handle that is written to and then closed can wedge on close, and
repeating that cycle in one process eventually hits it. The subject is
SlateDB's `Db::close`, not anything moraine owns: the same loop against
SlateDB alone — build, one `put`, close, no moraine in the chain —
reproduces it, and `Catalog::close` is a one-line delegation to that call.

Three conditions have to hold together, and dropping any one of them
clears 300 cycles: a write between the open and the close, repeated
cycles, and a multi-threaded runtime. The cycle at which it wedges varies
run to run, so it is a race rather than a threshold. At the wedge every
worker is parked at zero CPU with the close future suspended and nothing
runnable anywhere — a lost wakeup in the shutdown path, not a lock cycle
or a spin — and the stall sometimes swallows the runtime's timer with it,
so a `timeout` around the close is not a reliable escape.

This is not a hazard only an embedder can meet. The attach path builds a
multi-threaded runtime deliberately (a one-worker pool would let a
CPU-bound poll stall SlateDB's flush), so a session that repeatedly
attaches, writes, and detaches is running the same cycle. The exposure is
bounded by how the shipped paths use it: one attach opens once and closes
once, and neither the scheduler nor the on-demand trigger opens or closes
a handle — they run passes against the handle the attach already holds.

Nothing here is worked around in moraine, because a wedge inside the
store's shutdown has no seam above it to unwedge: a caller that gives up
waiting still leaves the handle live. It is pinned by a reproducer that
asserts the hang's *presence* against SlateDB alone, so that test failing
is the signal the pin can be removed.

### Test obligations

Per RFC 0001, core tests run against real SlateDB on in-memory
`object_store`; the live path is pinned by `cargo xtask e2e`.

Core:

- **Sweep reclaims a dropped index**, and the drop-table cascade reclaims
  both indexes of a two-index table; a later scan of `index` finds nothing.
- **Sweep spares live indexes** interleaved by id; lookups unchanged. A
  second `maintain` then reports zero and writes nothing.
- **Batching is bounded**, each batch head-preserving — the head snapshot id
  unchanged across the whole sweep while its batch count advances once per
  batch, and no `ducklake_snapshot` record written.
- **No writer conflict.** A sweep interleaved with commits landing entries
  for a *live* index completes, and both ranges end up correct.
- **Discovery seeks by id.** From any starting id, the scan returns the
  lowest index at or after it and `None` past the last — the property that
  makes skipping a live index one seek rather than a walk of its entries.
- **A zero batch size is refused**, and a read-only catalog refuses the
  whole pass.
- **Key ordering.** Index prefixes sort ascending within a kind and the two
  kinds occupy disjoint ranges, so the skip-scan's seek target is sound.
- **The census names the subspaces the store wrote.** A catalog with
  committed history, inlined chunks, and index entries reports non-zero
  bytes for exactly those subspaces and zero for the untouched ones.
- **The census is manifest-only by default**, pinned by an instrumented
  object store: its read count must not scale with the number of committed
  rows. With the scanning leg it does.
- **The live leg counts what is live.** After *n* commits and *k* deletes
  the live count matches an independent scan, and `current` separates
  entity records from `current/gcfile`.
- **An unrecognized segment is reported, not refused.**
- **A merge reclaims.** A subspace churned into several sorted runs reports
  fewer runs and fewer bytes after a merge, and every key reads back
  identically.
- **Superseded versions and tombstones drop.** A key written *n* times plus
  one deleted key leave one live version and no tombstone after a full merge
  of that subspace — asserted through the census's byte and count deltas,
  not through SlateDB internals.
- **Only the named subspace merges.** Merging `current` leaves `index`'s run
  and SST counts untouched.
- **A tree with nothing to merge is skipped**, for both targets, with the
  reason reported.
- **`wait: None` returns without blocking** and the merge still commits; a
  later census shows the reduction.
- **A read-only catalog refuses `compact_store`** and serves `store_census`.
- **The merge does not disturb the catalog.** The head stamp is unchanged
  across it, no snapshot is minted, and a materialization before and after
  is identical.
- **A commit landing during a merge succeeds**, and both the committed rows
  and the merged ones read back correctly.

Live (e2e):

- **Unconfigured attach schedules nothing** — every DuckLake step reports
  `skipped`, the sweep runs, and no data moves.
- **The sweep reclaims a dropped index** and only that: a live index is
  spared, the drop orphans its range, the next pass reports exactly that
  range, and a third finds nothing.
- **Configured pass.** Steps report in sequence order with the configured
  ones `ran` and the rest `skipped`, and the lake's contents are unchanged.
- **The trigger refuses inside an explicit transaction** rather than
  hanging.
- **Misconfiguration fails at bind**, naming the unknown option or the
  step disabled alongside its own parameter.
- **Status retains earlier passes.** A pass that reclaimed stays visible
  after a later pass that did not, each carrying its trigger.
- **Read-only never schedules but does report.** A read-only attach starts no
  thread and refuses the trigger, but `moraine_maintenance_status` reads the
  durable window left by the writer.
- **A failed step does not suppress the sweep.** With a step DuckLake
  rejects, the later DuckLake steps report `skipped` naming what aborted
  them, and the sweep still reclaims its range.
- **The scheduler runs a pass unattended.** An attach configuring only an
  interval reclaims a range orphaned by an *earlier* session, with no
  trigger call, and the window records the pass as `scheduled`.
- **Ticks skip a pass already running.** Against a pass far longer than
  the interval, exactly one pass takes the whole range and every other
  takes nothing — a split would mean two passes ran over it at once.
- **A custom `METADATA_CATALOG` resolves.** The trigger accepts either the
  lake name or the metadata name, and the pass still calls DuckLake with
  the lake name rather than the metadata one.
- **A list parameter spelled as a string** reaches DuckLake unaltered and
  expires exactly the named versions.
- **An interval `older_than` renders as a rolling window**, re-evaluated
  per pass rather than frozen at attach.
- **Detach during a running pass completes**, rather than hanging on the
  join or failing.
- **Process teardown without detach completes a running pass** before the
  last host context releases the database, rather than letting the pass's own
  connection keep the database alive until process exit.
- **Path overlap refused**; sibling locations attach normally.
- **The census table function** reports one row per subspace on a lake with
  data, from a read-write and a read-only attach alike.
- **A configured pass merges** and reports step 8 `ran`; an unconfigured one
  reports it `skipped`; a pass whose merge fails reports `failed` and leaves
  the lake queryable.

The timer tests hold one session open across a real pause so the scheduler
remains alive long enough to run; a later session can read the durable passes
but cannot reproduce live timer contention. Contention is provoked with
shipped knobs rather than test-only scaffolding — `MAINTENANCE_BATCH_SIZE 1`
makes the sweep take one durable commit per entry, each waiting out the WAL
flush cadence, so a twenty-entry range reliably outruns a 100ms interval.
Together they cost a few seconds of the suite, and the paused-session helper
bounds its own wait so a deadlock fails loudly instead of wedging the gate.

Counting passes would *not* pin single-flight: once the slow pass ends,
later ticks correctly run fast empty passes for the rest of the window.
The invariant that distinguishes them is that no pass ever observes a
partially reclaimed range.

Shim unit tests cover the ABI edge the pass rides: `moraine_maintain`
writes its counts through the out-parameters and accepts null slots for
either, and the overlap guard is exercised across store kinds, buckets,
and sibling-versus-nested prefixes without needing a lake.

## Alternatives considered

- **A documented ordering, with no orchestration and no schedule** (this
  RFC's first draft). Rejected: it left the operator running six statements
  in an order nothing enforced, and called that unification.

- **A fully parameterized table function** as the primary surface, with
  scheduling layered on top. Rejected once the writer had to schedule
  itself: it would be a second configuration surface saying the same things
  as the attach options, and a table function that issues SQL on the
  caller's context runs into `context_lock` re-entrancy. Collapsing
  configuration into attach options and reducing the verb to a
  parameterless trigger removes both problems.

- **No trigger at all** — timer only. Tempting, and rejected for the test
  gate: every maintenance e2e would have to configure a tiny interval and
  wait on wall-clock, which is precisely the flaky slow test `cargo xtask
  e2e` should not contain. The trigger also serves the run-it-now case
  without a re-attach.

- **Host-driven scheduling only** (no thread; the verb composes into
  whatever scheduler the host already has). Rejected once the single-writer
  constraint was followed through — see "Why the writer schedules itself".
  The residual cost of the thread is real and accepted: the shim now owns a
  lifecycle it did not have.

- **A substrate-collection step** running `Admin::run_gc_once` in the pass,
  to reclaim without waiting for the background cadence. This is *not* step
  8 under another name: collection deletes objects nothing references, and
  the bytes step 8 exists for are inside objects the manifest still
  references. Carried through several drafts and cut on checking the
  defaults: moraine already runs that exact collector, all six tasks, every
  60 seconds
  (Background). A pass scheduled at minutes-to-hours cadence gains nothing
  by saving up to a minute, and `Admin::run_gc_once` would build a *second*
  collector racing the built-in one — including a second
  `remove_expired_checkpoints`, which is a read-modify-write on the
  manifest. It also could not report anything: `run_gc_task` logs each
  error and continues (`garbage_collector.rs:384-398`) and `run_gc_once`
  returns `()`, so a wholly failed pass is indistinguishable from a clean
  one. Redundant, mildly racy, and unobservable.

- **Checkpoint lifecycle as a maintenance-pass step.** Rejected for the same
  ownership reason. SlateDB's built-in collector already expires checkpoints
  on its own cadence; a pass step would create a second lifecycle owner and
  race the read-modify-write operation instead of adding capability.

- **Declining a forced-compaction lever**, as an earlier revision of this
  RFC did. It gave three reasons and each has since proved false. *Choosing
  which sources merge into which destination is SlateDB's policy to make* —
  true, and `CompactionRequest::Full` / `FullSegment` make it upstream, so
  the caller names a tree and nothing else. *A per-segment spec is a
  substrate detail RFC 0003 keeps out* — the target is a **subspace**, which
  is moraine's own vocabulary; the segment is how it is implemented, not
  what is exposed. *`submit_compaction` only queues, so it cannot deliver a
  synchronous reclaim* — `read_compaction` plus `CompactionStatus`'s
  terminal states make waiting a poll loop, which is what step 8 does. The
  residual upstream want is narrower than that revision recorded: a
  completion signal instead of a poll. The measurement that forced the
  reversal is in Background — a quiet store holding 3.4 GB indefinitely,
  which no shipped mechanism touches.

- **A census by prefix scan only.** Rejected as the default: it costs a full
  read of the store, which is the very cost being investigated, where the
  manifest answers the composition question in two object reads. Kept as an
  opt-in leg, since it is the only source for live-versus-dead *within* a
  subspace — and therefore the only way to distinguish dead weight a merge
  reclaims from a `current/gcfile` schedule only `cleanup_old_files` drains.

- **A standalone census/compaction binary** instead of catalog verbs.
  Tempting, since `Admin` fences nothing and an operator would rather not
  attach at all. Rejected for the merge: nothing executes a submitted
  compaction unless a writer happens to be attached, so the binary's
  behaviour would depend on state it cannot observe. The census has no such
  problem and is reachable from a read-only attach, which covers the
  investigating-operator case without a second artifact to ship and version.

- **A fresh-store copy and repoint** — open the old store read-only, scan the
  live keys, batch them into a new store at a fresh prefix, repoint. Provably
  clean, and the genuine fallback if a merge ever cannot drop everything.
  Rejected as the primary answer: it needs cutover choreography under the
  single-writer rules (RFC 0004), it is a one-shot operation rather than a
  standing capability, and the merge it would replace is bottom-inclusive by
  construction and so leaves nothing over.

- **Merging before the sweep**, or interleaving the two. Rejected: the
  sweep's deletes are exactly the tombstones a merge should drop, so a merge
  that runs first guarantees a second pass is needed to reclaim what the
  first pass created.

- **A compaction filter that prunes dead history.** SlateDB accepts a
  `CompactionFilterSupplier`, so moraine could drop `history` entries below
  the oldest live snapshot during LSM compaction — history pruning for
  free. Rejected: it is exactly the moraine-side reclamation policy
  RFC 0007 and RFC 0008 each rejected, it would diverge moraine's state
  from the catalog DuckLake believes it wrote, and SlateDB documents that
  filters break snapshot consistency (`compaction_filter.rs`).

- **Unifying the retention windows.** DuckLake's `older_than`, cleanup's
  grace, SlateDB's GC `min_age`, and `checkpoint_lifetime` look like one
  knob stated four times. They are not: RFC 0009 releases the SlateDB read
  handle as soon as a `CatalogSnapshot` is materialized, so no moraine
  reader outlives a 5-minute `min_age`, and the DuckLake windows govern
  Parquet and catalog history, which SlateDB's collector never touches.
  Deriving one from another would invent a coupling that does not exist.

- **Two rejected parameter alignments.** *One shared `older_than`* across
  expiry, cleanup, and orphan detection is the most tempting, and actively
  harmful: they are three different windows (history to retain, how long
  scheduled bytes survive, how old an unreferenced file must be),
  independent in RFC 0007 for good reason. *Short moraine-chosen names*
  would be a second vocabulary to learn, drifting from DuckLake's as they
  evolve; the occasional stutter is the price of never looking anything up.

- **Defaulting the DuckLake steps** so a bare interval does everything.
  Rejected: it would make moraine the author of a retention policy against
  RFC 0007's non-goal, and turn configuring a cadence into silent data
  deletion.

- **A durable pending-sweep list** written on drop and drained by the
  sweep, the shape `ducklake_files_scheduled_for_deletion` uses. Rejected:
  it adds a mutable bookkeeping record to every drop commit, and the
  skip-scan derives the same set from the data at negligible cost.
  RFC 0007 rejected reference counts for the same reason.

- **Deriving the dead set from ended definitions in `history`.** The
  obvious source, and wrong: expiry may prune a definition's history record
  while its entries remain, leaving the id unrecoverable and the range
  leaked forever.

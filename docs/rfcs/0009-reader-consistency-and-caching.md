# RFC 0009: Reader consistency and snapshot caching

- **Date:** 2026-07-09

## Summary

RFC 0003 defines `CatalogSnapshot` as "an immutable, materialized read view
built by scanning `current` (or `current` + `history`)" that "touches the store never"
after construction. It does not say how that view is built *consistently*
while a committer writes concurrently, how a long-lived reader learns of new
commits, or what happens to a held view when RFC 0007 reclaims the files it
references. This RFC fills those gaps: a `CatalogSnapshot` is built at a
**single read point** so its scans are mutually consistent — pinned on a
read-write handle, validated against a per-batch head stamp on a read-only
one; it is **refreshed incrementally** from a changelog of the keys each
commit wrote rather than rematerialized; it observes **snapshot isolation**
(a fixed catalog snapshot `S`, never a torn mix); and it carries a
**validity window** tied to RFC 0007's retention horizon, past which a
reader must re-resolve from head. This is the read-side companion to RFC
0004's write-side commit protocol.

It also names the cache stack end to end, assigns every byte to one tier,
and collapses the catalog's physical tier to one process-shared block
cache — SST metadata pinned in memory, data blocks tiered to disk — under
one budget. Data bytes get no moraine cache: they are the engine's to
hold, and the fact that DuckLake does not yet hold them is upstream's to
close rather than moraine's to work around.

## Goals

- **Consistent materialization.** The `current` / `history` / `snapshot` scans that build
  one `CatalogSnapshot` observe a single, consistent store state — never a mix
  of pre- and post-commit records torn by a concurrent commit.
- **Snapshot isolation for reads.** Every accessor on a `CatalogSnapshot`
  reflects exactly the catalog at one snapshot id `S`. Reads never block
  writes and a concurrent commit never mutates a held view (RFC 0004
  append-only + immutable `snapshot`).
- **Cheap refresh.** Advancing a long-lived reader from `S` to head costs work
  proportional to *churn* (the entities changed between `S` and head), not to
  live catalog size — replaying the `current` keys each commit in the gap
  recorded in its `snapshot` record (RFC 0002).
- **Committer read-your-writes.** The single committer (RFC 0004) advances its
  own view by folding in the batch it just committed, without a re-read.
- **A defined validity window.** A held `CatalogSnapshot` is valid only within
  RFC 0007's retention window; a reader that outlives it gets a typed error and
  re-resolves, never a silent dereference of reclaimed files.
- **No new coordination.** All of this holds under RFC 0004's single-writer /
  many-uncoordinated-readers topology. Readers learn of commits by reading
  `sys/head`, never by being notified. Nothing pushes a commit to a reader,
  so a read-only catalog's freshness is bounded by how often it polls —
  an explicit, configurable property of the handle rather than an accident
  of the store layer's default.
- **One cache per byte, one budget per host.** Catalog bytes live in one
  shared block cache with explicit memory and disk budgets; data bytes in
  DuckDB's external-file cache under `memory_limit`. No tier duplicates
  another, and no cache is invisible to sizing.
- **One store scan per head.** At a given head stamp, `current` (and
  `history` when needed) is scanned and decoded at most once; every
  logical cache derives from the shared record set.

Non-goals:

- **The read *API*** — `CatalogSnapshot`, its accessors, `snapshot()` /
  `snapshot_at()` — RFC 0003. This RFC is the machinery behind them.
- **The physical `current` / `history` layout and the current-vs-time-travel split**
  — RFC 0002. This RFC consumes that layout; it does not change it.
- **Reclamation policy** — RFC 0007. This RFC defines only how a reader
  *reacts* to a view falling outside the retention window.
- **The sync↔async / runtime model** under the DuckDB extension — RFC 0010.
- **The attach-option surface** that configures the cache tiers — RFC
  0006. This RFC defines the machinery those options drive.

## Background

RFC 0002 makes the current-catalog load a scan of `current` and time travel a scan
of `current` + the relevant `history` ranges; it merges `ducklake_snapshot` and
`ducklake_snapshot_changes` into one immutable `snapshot` record per snapshot, and
notes that `snapshot` is append-only and `sys/head` holds the latest committed
`snapshot_id`. It also chose *not* to maintain a persistent name index —
name→id resolution runs "against the in-memory catalog snapshot that a client
builds by scanning `current` at attach."

RFC 0004 fixes the topology: readers attach through read-only handles
(SlateDB `DbReader`) that follow the manifest with no coordination and no
bound on their number, and a commit is one atomic batch that advances
`sys/head` under transactional write-write conflict detection.

RFC 0003 makes `CatalogSnapshot` an immutable value — "Reads issue no store
I/O after the snapshot is built; a `CatalogSnapshot` is a value, not a cursor"
— and has `commit` read a *fresh* `CatalogSnapshot` on every attempt.

RFC 0007 introduces the retention horizon `H` (snapshots `< H` are reclaimed)
and the grace window `G` (bytes survive `G` past scheduling), and states the
operator contract that the retention window must exceed the maximum reader
duration. This RFC honors that contract on the reader side.

## Design

### Materialization rests on one read point

The correctness foundation: building a `CatalogSnapshot` issues *many* store
reads (a `current` range scan, `history` ranges for time travel, the `snapshot` record,
`sys/head`). These must observe one consistent store state, or a commit
landing mid-build could yield a view that has a table's new `column` record
but not its new `file` record — a torn read.

So materialization **issues every get and scan against one read point**.
On a read-write handle that read point is a SlateDB primitive, not a
hoped-for one: the pinned SlateDB version (0.14.x) exposes both
`Db::snapshot()` — a `DbSnapshot` fixed at a sequence number, with the same
`get`/`scan`/`scan_prefix` surface as the live handle — and a
`DbTransaction`, which reads the MVCC view at its own start sequence and is
what a commit attempt uses (see below). The moraine snapshot id `S` is the
`sys/head` value read *under the same handle*. The result: the entire
`CatalogSnapshot` is a consistent cut at `S`, immune to concurrent commits,
with no lock or reader/writer coordination — the consistency is inherited
from SlateDB's read isolation, not re-implemented. The handle is released
once the in-memory view is built; per RFC 0003 the finished
`CatalogSnapshot` touches the store never again.

A read-only handle has no such primitive; the next section is what it does
instead.

Holding a read-snapshot is cheap: a `DbSnapshot` is a `(uuid, started_seq)`
pair registered with an in-process snapshot manager, so open and drop are
O(1) with no store I/O and no manifest write. The one holding cost is
version retention — the manager's `min_active_seq` feeds SlateDB's flush
and compaction paths, so a live snapshot keeps pre-snapshot versions from
being reclaimed in-process for as long as it is held. moraine holds one
only for the duration of a single materialization and releases it before
the `CatalogSnapshot` is returned, so the retention window is one
materialization long.

Materialization also reads the **`sys/migration` marker** (RFC 0002 /
RFC 0015) under the same handle. If the marker is present, a structural
migration is rewriting the keyspace and any scan of it may be missing
records mid-move; materialization fails loudly with the typed `Migration`
error (RFC 0003) rather than returning a silently partial catalog. This
check is part of every materialization and refresh from format version 1
onward — it must predate the first migration ever run, or old readers
would have no way to know to refuse (RFC 0015).

`snapshot_at(S')` (time travel) pins the same way, reads `sys/head` only to
validate `S'` is resolvable (≥ RFC 0007's horizon `H`), and materializes from
`current` + `history` filtered by begin/end per RFC 0002.

### A read-only handle validates its cut rather than pinning one

The paragraph above holds for a read-write handle, which materializes
through a `DbTransaction` (or could open a `DbSnapshot`) and so reads at one
sequence by construction. A **read-only** handle has neither. SlateDB
0.14's `DbReader` offers exactly two modes and neither is a live consistent
cut: pinned to an explicit `checkpoint_id` it never spawns a manifest
poller, so its state is fixed and no later commit is ever visible; with no
checkpoint it polls, and every `get`/`scan` takes a fresh clone of whatever
state the poller last installed. Two reads of one materialization can
therefore straddle a refresh.

Validating optimistically on `DbReader::manifest()` does not work, and this
is worth stating because it is the obvious thing to try: the reader's
WAL-replay path rebuilds its state carrying `manifest: current.manifest
.clone()`, so the manifest id is *unchanged* by exactly the refresh that
makes a new commit visible. A manifest-id check would validate every read
it needed to reject. `DbStatus::durable_seq` does move on both refresh
paths, but it is advanced *before* the state is installed, so a reader can
observe the new sequence and then read the old state.

So moraine does not look for the answer in SlateDB's metadata; it puts a
stamp in its own keyspace. **`sys/head` carries a batch count alongside the
snapshot id, and every batch writes the record and increments the count** —
a maintenance batch that reuses the snapshot id included (RFC 0004). The
pair therefore changes whenever any committed state does. A read-only
materialization reads the head record, runs its scans, reads it again, and
accepts the result only if the stamp did not move; a pass that straddled a
refresh is discarded and re-run. The reader's state advances only when its
manifest poller runs, not on every commit, so a pass that loses one round
very rarely loses the next, and a bounded budget of them ends in a typed
error rather than a torn view.

### A read-write handle resolves the head without reading it

The stamp above is what a *read-only* handle needs, because it follows
another process's commits and cannot know when one landed. A **read-write**
handle is in the opposite position: it holds the writer epoch, so it is the
store's only writer and nothing can move `sys/head` under it. Reading the
record back to learn where head is asks the store a question the handle
already knows the answer to — and that point read is on every catalog
read, every metadata dump, and so under every DuckLake statement.

So a read-write handle resolves the head from **the view it already
holds**: a `CatalogSnapshot` carries the snapshot id and batch count it was
built at, which on a sole writer *is* the head. The premise is that a held
view is never behind the store, and that rests on four rules, all in the
commit path:

- A head-advancing batch folds the view forward as it lands, or drops it if
  the fold cannot be applied faithfully.
- A head-preserving batch drops the view *before* its write becomes
  visible, since it reuses the snapshot id and only the batch count would
  give it away.
- A durable write that fails for any reason other than a lost race drops
  the view: a lost race provably did not land, and everything else leaves
  that open.
- A durable write whose task never reports back drops it too, for the same
  reason.

The held view is only ever a **lookup key**, never an answer inferred in
place of one. A dump whose projection does not stand at that stamp falls
through to the ordinary session path and reads the head there, so rows are
never installed under a stamp the store did not supply.

What a warm read does not skip is the fence check, because a handle that
served its cache past its own displacement would answer from a catalog the
store has moved on from, and quietly. It does not open a session to perform
it: SlateDB reports a close by setting it on a status channel the handle
subscribes to at open, so the check is a watch borrow — no store read, no
manifest copy, and no entry in the transaction manager, which is what
opening a session takes a global write lock to make. The reach is identical,
since closing the `Db` is what the fence does. The difference is that a
watch borrow shares between readers and the lock does not: measured, warm
throughput plateaus where it used to fall away (`BENCHMARK.md`).

What a warm read *does* skip, on a writer only, is the `sys/migration`
refusal a session otherwise carries. The marker exists to stop a scan of a keyspace
being rewritten, and a read served from the held view performs no scan —
but the deeper reason is that on a read-write handle the probe was never
the guard. A migrator takes the writer epoch before it writes anything, so
a handle it displaces is fenced before a marker exists, and a fenced handle
reads its own state, which that marker never reached: it reports `Fenced`,
never `Migration`. The probe fires on a writer only for a marker written
through that handle's *own* transaction, which no migrator does. The fence
check is the guard, and that is the one the session still performs.

A **read-only** handle is the one a migration can start under — a migrator
fences a writer but leaves a reader following the store — so every session
it opens re-establishes the marker's absence, and its cold-open refuses a
store already carrying a marker. A writer establishes it while cold too,
which is what catches a migration a crashed predecessor left behind.

What re-establishing costs, though, is no longer a marker read per
session. The marker is an absent key on every store that is not
mid-migration, and an absent key is the store's most expensive read — a
bloom filter answers "might be present", never "absent", so the read walks
every level before it can say no. Warm-cache microbenchmarks made that
walk look free (6.4 µs, no object-store GET — `BENCHMARK.md`); production
flush telemetry showed it as a fixed cache-missing term under every commit
and every read session. So a handle keys the marker's absence on the
**head it observed it at**: a session whose head matches the recorded
stamp opens without re-reading the marker, and one whose head moved
re-reads and re-stamps. Soundness comes from the start batch, not the
cache — a migration start moves the head in the same batch as the marker,
and `migration::start` is the marker's only writer (RFC 0015), so a marker
cannot appear under an unmoved head. A committer maintains the stamp from
the other side without any read at all: winning the head CAS proves no
batch — a migration start included — landed since its base, so a landed
commit records its own resulting head as marker-free.

Carrying the migration state as a field on the head record would buy the
same skip, and that design stays **rejected**, because the encoding is the
easy half and the version gate is not. A reader may trust such a field
only if every migration maintains it, so a durable field means nothing
until a format bump forces every writer onto it — a one-way door, taken by
default, closing the store to yesterday's binary. The head-keyed stamp
needs no gate: it is an in-memory judgment each handle re-derives from
reads it was making anyway, and the start-batch invariant it rests on has
no legacy violations to grandfather (no released binary has ever written
the marker — RFC 0015).

Making the head write unconditional has a second effect the design wants:
`sys/head` becomes the one key every batch touches, so SlateDB's
write-write detection makes it the single conflict anchor. A maintenance
batch staged against a base another commit has since moved now loses its
race and re-drives, where before it could land on stale premises.

**Commit attempts materialize through the transaction, not a
read-snapshot.** SlateDB's write-write conflict detection sees only commits
that land after the transaction's start sequence. If a commit attempt
materialized its planning `CatalogSnapshot` from a `DbSnapshot` pinned at
sequence `X` and then opened its `DbTransaction` at sequence `Y > X`, a
concurrent commit landing in the `X → Y` gap would invalidate the attempt's
premises *without* tripping conflict detection — the attempt's `sys/head`
write only conflicts with writes after `Y`. The commit would land on stale
premises: a silent lost update. So the rule is: a `CatalogSnapshot` built
for a commit attempt (RFC 0003 `commit`, each retry included) is
materialized **through the `DbTransaction` itself** — SlateDB transactions
expose the same `get`/`scan`/`scan_prefix` surface, reading the MVCC view
at the transaction's own start sequence — so the premise view and the
conflict-detection window are anchored to the same sequence number by
construction, and no gap exists. Concretely, materialization is generic
over its read source: `DbSnapshot` (or `DbReader`) for plain readers,
`DbTransaction` for commit attempts. The finished `CatalogSnapshot` is
identical in both cases; only the handle it was built through differs.

### Snapshot isolation is free

Because `snapshot` is immutable and every entity version is append-only with an
`end_snapshot` that is only ever *set once, at commit* (RFC 0002/0004), the set
of records visible at `S` never changes after `S` exists. A concurrent commit
`S+1` adds records and ends others *for `S+1`*, but nothing it does alters what
`begin ≤ S < end` selects. A held `CatalogSnapshot` is therefore a stable
snapshot-isolated view for its whole lifetime with no defensive copying beyond
the initial materialization — this is why RFC 0003 can promise "no store I/O
after build."

### Incremental refresh from the changelog

A long-lived reader advances from `S` to a newer head `S+k` without
rescanning `current`:

1. Read the head stamp and the `sys/migration` marker (refusing with
   `Migration` if the marker is present, as in materialization). If the
   stamp is unchanged, done — the cached view is current.
2. Otherwise read `changelog/{S+1 .. head}`. Each record holds the
   **`current` keys its batch wrote** — moraine's own entity-grained
   changelog, recorded from the write set the commit already assembled.
3. Point-read the union of those keys, still under the same handle — a
   refresh, like a materialization, is one consistent cut. Each key's value
   at head is that entity's new state and an absent key is one that ended,
   which is precisely the shape of a staged write set, so the replay is
   applied by the same fold the committer uses on its own batch. Stamp the
   view with head's snapshot record and batch count.

Cost is churn across the gap plus one copy of the base view, not a function
of catalog size beyond that copy — a view is immutable and shared, so
advancing one copies it first. No step restates any entity kind's rules:
the changelog is encoded keys and the application is the existing fold.

The changelog lives in a subspace of its own rather than in the snapshot
record. Measured in the record it grew snapshot rows 6.8× and slowed their
scan 1.45× (`BENCHMARK.md`), and snapshot rows are read by DuckLake at every
transaction — a cost on the hot path for a benefit only a refresh takes.
Moved out, the serve path pays nothing. Storage is bounded without help from
expiry or a sweep: each commit writes its own record and deletes the one a
fixed window back, so the subspace is flat at the window size whatever the
commit count.

DuckLake's `snapshot_changes` grammar is deliberately *not* the changelog
here. It names tables, not entities; it carries created relations by name
rather than id; and moraine's own stats-only and schema-tag commits produce
no entry in it at all, so replaying it would silently miss writes. The
keys the batch wrote miss nothing by construction.

**Fallback to full rematerialization** when incremental is impossible or not
worth it:

- **A batch in the gap minted no snapshot.** Maintenance reclaims state
  under a reused snapshot id (RFC 0007, RFC 0008) and so writes no
  changelog. The head stamp's batch count still records that it landed:
  when the count moves further than the gap has snapshots, part of the gap
  is unrecorded and the reader rematerializes.
- **`S` fell below the horizon** (`S < H`, RFC 0007). This needs no rule of
  its own: the retained changelog window is far shorter than any retention
  horizon, so a base that old lost its changelogs long before it lost its
  snapshots and reaches the fallback above. (If the reader specifically
  wanted the *old* `S`, that snapshot is gone — see validity window.)
- **The changelog is not there.** Either the commit declined to record one
  — it wrote more keys than the cap — or later commits swept it out of the
  retained window. Both read as an absent record and both mean rescan. The
  cap (4,096 keys) bounds the record, not the replay decision, and it is
  sized generously on purpose: a commit that records nothing forces every
  reader behind it to rematerialize, which costs far more than the
  changelog it declined to write, so an ordinary bulk flush — production
  showed one at 259 keys — must land inside it. The churn threshold below
  still decides whether a replay pays; the cap only decides whether the
  choice exists.
- **The gap is large** relative to catalog size: a full `current` rescan is
  cheaper than replaying a huge changelog. The crossover is measured, not
  reasoned — it sits at a churn share of **~0.57** of the live entity count
  (`BENCHMARK.md` → Changelog replay vs. rematerialization) — and the
  threshold is set at **half**, declining just before replay stops paying.
  The two paths produce identical views, so the choice is purely a cost
  optimization.

### Committer read-your-writes

The single committer (RFC 0004) holds a planning `CatalogSnapshot`. After it
commits `N → N+1`, it must see its own write to plan the next commit. It does
**not** re-read: it already assembled the `WriteBatch`, so it folds the exact
staged mutations into its in-memory view and stamps it `N+1`. This is strictly
cheaper than incremental refresh (no `snapshot` scan, no re-read) and is always
correct because the committer *is* the source of the delta. Under RFC 0004
group commit, the committer folds in the whole committed group at once. A
committer that loses the CAS and retries simply rebuilds from the new head like
any reader.

### Validity window and expiry reaction

A held `CatalogSnapshot` at `S` names object-store files (`data_files_of`,
`delete_files_of`). RFC 0007 may schedule those files for deletion once `S`
passes below the horizon and delete their bytes after the grace window `G`. So
a view is safe to *use for data access* only while `S` remains within the
retention window.

This RFC states the reader-side contract that RFC 0007's operator obligation
implies:

- A `CatalogSnapshot` whose `S` is still `≥ H` is fully valid.
- A reader that tries to materialize or refresh at an `S` that has fallen below
  `H` — detected because its `snapshot`/`history` range is no longer resolvable —
  receives a typed **`SnapshotExpired`** error (RFC 0003 error taxonomy, one
  variant per failure domain) and must re-resolve from head.
- The safety margin is the retention window minus the reader's lifetime; RFC
  0007 sizes the window to exceed the maximum reader duration, so a
  well-behaved reader that refreshes faster than `G` never observes expiry.

**What "maximum reader duration" means under DuckLake** is one explicit
transaction, not one statement. DuckLake holds a single catalog snapshot
per `DuckLakeTransaction`: it is resolved lazily on the first catalog
access (`DuckLakeTransaction::GetSnapshot`, one `SELECT ... WHERE
snapshot_id = (SELECT MAX(snapshot_id) ...)`), cached under a lock for the
transaction's whole life, and never re-read per statement — the only reset
is the commit-retry path. In autocommit mode DuckDB builds a fresh
transaction per statement, so there the snapshot is per statement. So the
retention window must exceed the longest open `BEGIN … COMMIT`, which a
user controls and moraine cannot bound; the statement-length reading would
size it far too tightly.

The reader never silently dereferences a reclaimed file: either its `S` is
still retained (files present) or materialization/refresh fails loudly with
`SnapshotExpired`.

### A DuckLake transaction reads one materialization per metadata table

Everything above concerns a reader holding a `CatalogSnapshot`. DuckLake
holds none: it reads the synthesized `ducklake_*` tables (RFC 0006) as
SQL, and each scan of one is a separate dump served at whatever `sys/head`
is when it runs. The `SnapshotExpired` contract is therefore unreachable
from that path — a table hands back a row set, not a typed error, so a
snapshot that vanished mid-statement reaches DuckLake as a missing *row*
and it has nothing to re-resolve from.

That matters because DuckLake reads one table twice in one statement.
`GetSnapshot` is `SELECT ... WHERE snapshot_id = (SELECT MAX(snapshot_id)
...)` over `ducklake_snapshot` — two table references, two binds, two
dumps. Served at two heads, an expiry (or an ordinary commit) landing
between them leaves the maximum naming a row the other scan never saw, and
DuckLake reports the tear as "No snapshot found".

So the shim pins the read the same way the core pins a materialization,
one level up: **a synthesized table is dumped once per DuckDB transaction
and every reader of it in that transaction is served the same rows.** The
`MoraineTransaction` that already owns one `moraine_snapshot` for its
catalog entries owns these too, and releases them with itself.

Two boundaries are deliberate:

- **The pin is per table, not per transaction.** Each table is materialized
  at the head its first scan observed, so a statement joining two of them
  can still straddle a commit. Closing that needs one read point across
  dumps — an ABI-level read session — which would hold a SlateDB
  transaction open for a whole user transaction and contradicts this RFC's
  one-materialization-long retention. Left open deliberately, not by
  oversight.
- **A writing transaction is not pinned this way.** Once it opens a staged
  transaction, every read of a writable kind goes through that
  transaction's dumps: those already read at one fixed point *and* overlay
  the rows staged so far, which is both the pinning and the read-your-
  writes a writer needs. Nothing caches over them, because that is the
  surface DuckLake's commit loop re-reads between attempts (RFC 0007) and
  a retry served its first attempt's state would re-check its conflict
  matrix against a premise that already lost.

### Caching is per-handle, in-memory, logical

The materialized in-memory catalog *is* the cache. A `Catalog` handle (RFC
0003, an `Arc`-backed clone-cheap handle over `slatedb::Db`) holds the
latest `CatalogSnapshot` and shares it; a refresh replaces it. Every head
read — the public `snapshot()` and a commit attempt's planning view alike —
serves from it when its stamp matches, advances it across the gap when it
does not and the gap replays, and rematerializes otherwise. The view is
shared, never copied out: a warm read costs one point read on `sys/head`
and nothing else.

The cached view is keyed by the **whole head stamp**, snapshot id and batch
count together, so a maintenance commit that reuses the id cannot leave a
view of the state it reclaimed serving. A read-write handle additionally
invalidates the cache before such a commit's write becomes visible, since
it is the writer and knows. That alone would not make installing safe from
a read path: a reader that read head and then installed could land its
now-stale view *after* an invalidation, resurrecting content the
invalidation existed to discard. So the cache carries an **install epoch**,
bumped on every invalidation. A reader captures the epoch before its first
store read and installs only if the epoch still matches; any interleaved
invalidation makes the install a no-op and the reader simply keeps the view
it computed. Installation is thus compare-and-set, and no path needs to
discard a view it has already paid for.

**Read-only handles cache too.** A reader folds no batch of its own — it
has none — so the changelog replay above is the only way it advances, and
the head stamp tells it exactly which store state its held view stands at.
That is what the validated cut buys: without it a torn view would persist
and compound rather than being discarded with the read that built it.

The migration marker's absence is established before the cache is
consulted — from the head-keyed stamp when the head has not moved, from
the store when it has — so a warm handle refuses a mid-migration store
exactly as a cold one does: a cached view must never be the reason a
reader sails through a keyspace move.

There is:

- **No cross-process cache.** RFC 0004's "many readers" are independent
  processes/handles; each materializes its own view. Coordinating a shared
  logical cache would reintroduce exactly the coordination the topology avoids.
- **One physical cache, specified below.** Beneath the logical caches sits
  exactly one byte-tier cache for the catalog ("One block cache: two
  slots, one budget") and none beside it. This RFC's cache is the
  *logical* materialized catalog; the block cache keeps its rebuild cheap
  when the store is remote.

The view is whole-catalog, and that fixes where filtering can usefully
happen. DuckDB pushes projection into the `ducklake_*` scans but never a row
filter, so no predicate reaches the store on the serve path; every metadata
read is a full range scan of its entity kind, and DuckDB filters over the
returned rows. Server-side filter pushdown therefore cannot reduce work while
the whole view is resident — there is nothing left to avoid fetching. Pushdown
becomes worth building only if moraine stops materializing the whole catalog,
because lazy materialization needs predicates to know what to fetch. The two
are one decision, and this RFC rejects both: the full view keeps every accessor
store-free, and no profile has shown its memory or the replay base-view copy to
be a problem. A measured live-catalog memory problem would replace this
decision and take predicate pushdown with it; it does not leave an unbounded
implementation task in the meantime.

### Maintained served projections

The DuckDB shim serves row-faithful `ducklake_*` projections through the
`dump_*` functions, and DuckLake re-reads three of them — snapshots, table
stats, table column stats — at the start of **every** transaction. Rebuilding
each from a subspace scan per statement makes small-commit latency scale with
history. Those three projections are therefore *maintained*, by the same
principle as committer read-your-writes:

- A read-write `Catalog` holds a decoded projection state keyed by the head
  snapshot id it was built at. The first serve pays the full scan and installs
  the state; every commit **folds its own committed batch** into it (the
  committer is the source of the delta), stamping the new head.
- Every serve re-reads `sys/head` (one point read). If the state's head does
  not match, the serve rebuilds from a full scan and reinstalls — it never
  serves a mismatched state. Correctness rests on this head check, not on
  fold-in completeness: a missed or undecodable fold clears the state and
  degrades to a rescan, never to wrong rows.
- The serving contract is row-faithfulness: a maintained projection is equal
  to a fresh scan at the same head, always.
- Scope: **both**, on one key. A read-write handle folds each batch forward
  and a read-only one folds nothing, but folding is not what decides whether
  a projection may be served — the head stamp is, and a reader reads the
  same stamp the writer wrote. So every projection here is keyed on the
  whole stamp rather than the snapshot id, and a reader serves from it on a
  match and rescans on a miss.
  The distinction is load-bearing rather than pedantic: a maintenance batch
  reuses the snapshot id while changing what a scan finds, so an id-keyed
  projection would serve a reader the state that batch reclaimed. A writer
  never noticed, because it folds every batch and is therefore never stale.
  Keying on the stamp is what makes the reader's case safe, and once it is
  safe there is no reason left to withhold it — each of these dumps scans
  the whole of `current`, so a reader rescanning per call pays for it on
  every query, not once at attach. A reader's scan runs under the same
  validated cut every read-only pass does, so the two scans of the entity
  dump observe one store state.
- The fold reads its stamp out of the batch it is folding. Every batch
  writes `sys/head` (RFC 0004), so the head record is among the staged
  writes, and taking it from there rather than being told alongside them
  makes it impossible for the two to disagree. A batch arriving without one
  cannot be attributed to a state, so it clears the projections rather than
  advancing them — unreachable by construction, and a guard rather than a
  path.

- The dumps clone only what they return. The record set behind them is
  shared, and a per-kind dump filters it by reference rather than copying
  it whole — populating DuckLake's metadata tables issues two dozen of
  them, and copying the catalog per call would make the cache the expensive
  path.

### Store facts outlive view invalidation

Everything above is keyed by the head stamp and dropped or replaced when
the state it describes stops standing. Three cached facts follow a
different rule, because each describes the store rather than a view of it,
and each rests on a monotonicity the store enforces rather than on
invalidation discipline. Invalidation leaves all three standing.

- **The format floor.** `sys/format` is forward-only: no writer lowers it.
  A version once observed is therefore a permanent lower bound — seeded at
  open from the validated format, raised when a landed batch carries a
  stamp (true once the batch lands, whatever a read said before). A commit
  whose target format sits at or below the floor owes no stamp and reads
  nothing; only a commit reaching *above* the floor reads the stamp before
  writing it (RFC 0015 owns what the stamp means).
- **The migration-clear head.** The head at which `sys/migration` was
  observed absent ("A read-write handle resolves the head without reading
  it", above). Keyed on the whole head stamp, so it is exact rather than
  monotone; its soundness is RFC 0015's start-batch invariant, and every
  landed commit restamps it for free.
- **Inline chunk-directory completeness, per table.** The chunk-range
  directory (RFC 0005) names every live chunk once verified against a full
  chunk walk — and stays that way only when the store format shuts out
  writers that predate the directory, since from then on every chunk
  arrives with its locator in the same batch. Verified once, remembered,
  and monotone from there; RFC 0005 owns who verifies, who heals, and
  which sessions may judge.

### The stack, named end to end

The caches a moraine-backed query crosses, top to bottom:

| tier | holds | keyed by | budget | who evicts |
|---|---|---|---|---|
| DuckDB external-file cache | Parquet byte ranges of data files | path + version tag | `memory_limit` (buffer-manager blocks) | DuckDB buffer manager |
| DuckDB metadata caches | Parquet footers, HTTP metadata | path | unbounded, opt-in | never / on error |
| DuckLake catalog cache | schema/catalog entries; the per-transaction snapshot | snapshot id + `schema_version` | live-catalog-sized | `schema_version` move; transaction end |
| shim `MetadataRows` | decoded rows per synthesized table | head stamp at first scan | per transaction | transaction end |
| core logical caches | `CatalogSnapshot`, entity record set, maintained projections | head stamp + install epoch | one catalog's decoded size, reported by `projection_bytes` | replaced on stamp move |
| core scoped-read metadata | parsed Parquet footer and page indexes used by equality-index upkeep, and file row-id summaries | object-store location + path + file size + page-index policy | process-wide share of `CACHE_MEMORY`, to a ceiling, over `CACHE_DIR/auxiliary` | byte-bounded LRU (foyer), recovered on restart |
| SlateDB block + meta cache | decoded SST blocks, indexes, filters | scoped SST id + offset | one cache per attached store: an even share of the process budget in memory over `CACHE_DIR/<store>/blocks` | LRU-ish (foyer), recovered on restart |

Two findings drove this section. Before consolidation, one catalog byte could
be resident in five tiers at once, four of them refilled from the tier below on
every miss — duplication that costs copies as well as memory. And SlateDB's
per-store cache defaults (hundreds of MiB in memory, 16 GiB on disk,
*multiplied by attached stores*) were bounded by nothing DuckDB could see, so a
host attaching several catalogs was over-committed by construction. The
subsections below take the row tiers (duplication), DuckLake's tier (composed
with, not consolidated), the byte tier (consolidated), and the data tier
(DuckDB's, untouched).

### Shared budget, separate cache engines

The SlateDB caches and the scoped reader's parsed-Parquet cache share a
**budget and process lifetime, not an entry store**. `CACHE_MEMORY` is split
once, at the first attach: the auxiliary metadata allowance comes off the
top, and the remainder is the block budget, re-split evenly across the
stores currently attached on every attach and detach (a store with nothing
attached keeps 1 MiB, its blocks staying on disk if it has any). No attach
may construct another allowance or grow any cache beyond that split.

The allowance is a **share of the budget to a ceiling**, not a fixed figure.
It cannot share one eviction store with SlateDB's — foyer's cache is
monomorphic and both the key and the value type are SlateDB's own, neither
constructible here — so the bytes are deducted whether the parsed-metadata
cache fills them or not. That reservation is why it scales with the budget
rather than standing at a small host's number, and why it stops scaling
rather than reserving unboundedly on a large one.

The logical caches take no share of the budget at all. They are per handle,
hold one head's decoded state, and are replaced when the stamp moves rather
than evicted under pressure, so they are bounded by the catalog's own size —
which is a bound, but not one an operator can set.
`ReadOnlyCatalog::projection_bytes` reports it, because a cache invisible to
sizing is what this RFC's goals rule out.

Their storage engines remain separate because their entries have different
identity, value, and tiering contracts:

- SlateDB owns keys scoped to an opened database and values such as SST
  filters, indexes, and decoded blocks. Its `DbCache` controls concurrent-fill
  suppression, admission, and the optional Foyer disk tier.
- The scoped reader owns immutable `Arc<ParquetMetaData>` values keyed by
  object-store identity, path, recorded file size, and page-index policy. They
  are useful only inside this process, stay memory-only, and are evicted by
  parsed byte weight. Concurrent misses for one key share one in-flight fetch
  and parse; the result enters the cache once, while a failed fill is shared
  only with its current waiters and remains retryable.

Putting either value into the other engine would erase one of those
contracts: teaching SlateDB's closed entry type about Parquet couples the
catalog store to data-file decoding, while type-erasing both behind a new
hybrid cache loses SlateDB's admission and disk behaviour. A common cache
interface is therefore not a consolidation; it is a second implementation
layer over two caches that still need their own keys and policies.

The shared boundary is configuration and accounting. The slot calculation is
the only source of capacity for both engines, and diagnostics must report the
auxiliary allowance beside the SlateDB slots before it gains an independently
tunable option. Parsed metadata never enters the block slot or its disk tier,
and projected Parquet data never enters either Moraine cache.

### One owned store scan loop

SlateDB's scan iterator yields an owned, reference-counted `Bytes` value.
Moraine therefore has one canonical scan/decode primitive that moves that
value from the iterator into the extractor. An extractor whose protobuf has
shared byte fields consumes it with the owned decoder, allowing those fields
to remain slices of the store allocation. Every other extractor borrows
`&bytes` for the duration of its ordinary decoder call. The scan loop neither
clones nor copies the value in either case.

This is an ownership seam, not another scan abstraction. It continues to:

- decode and validate the key before invoking the extractor;
- preserve SlateDB's key order and the caller's declared `ScanShape`;
- stop at the first key, framing, protobuf, or extractor error; and
- collect only the caller's decoded result, never a second byte buffer.

There is no borrowed and owned pair of scan loops. A second loop would make
ordering, admission, and error semantics changes that have to be maintained
twice for no saving: moving `Bytes` is constant-time, and a decoder that does
not retain it can borrow it locally.

### One scan pair per head

View materialization and the entity dumps each scan the store and each
hold their own decoded copy of the same records at the same head. The rule
replacing that: **at one head stamp, `current` and `history` are each
scanned and decoded at most once, into a shared record set every logical
cache derives from by reference.** Whichever read misses first runs the
scan under its consistent cut and installs; the halves install
independently (a head view needs `current` only and must not grow a
`history` scan; the dumps add `history` when first to want it). The commit
fold and the changelog replay advance view and record set together — one
cache entry with two faces, installed and invalidated under the existing
install-epoch rule.

### A reader that cannot match an ended version does not read them

`history` grows with every ended version and is pruned only by snapshot
expiry, so a scan pair that always reads both halves sawtooths against the
expiry cycle rather than against the live catalog. Most of what it reads is
discarded: every DuckLake read of a versioned table carries
`{SNAPSHOT_ID} >= begin_snapshot AND ({SNAPSHOT_ID} < end_snapshot OR
end_snapshot IS NULL)`, and at the head the second half of that keeps
nothing from `history`.

The rule: **a scan may read `current` alone when its caller has been shown
that no version in `history` can satisfy it.** One filter shape proves
that, and the core — not the shim — checks it:

- A record reaches `history` by being ended, stamped with the snapshot that
  ended it, which is never past the head. A record there always carries an
  `end_snapshot`; the `IS NULL` arm never keeps one.
- So a reader keeping rows only while `filter_snapshot < end_snapshot`
  matches nothing in `history` once `filter_snapshot` has reached the head.
- Time travel reads behind the head and keeps the full pair, which is what
  makes the narrowing safe by construction rather than by care.

The bound crosses the ABI as a snapshot id, not as a boolean, and the
comparison against the head happens inside the same consistent read that
serves the dump. A shim that compared them itself would be racing: a commit
landing between its check and the dump moves the head past a bound that was
at it, and the rows that commit ended would be dropped from a reader that
still matches them.

The shim's side is recognizing the shape. It is a `pushdown_complex_filter`
that consumes nothing — the filter stays, DuckLake keeps applying it — so a
bound recognized too generously costs a narrower materialization, never a
wrong row, and the core still refuses a bound behind its read point.

Two scans are left out deliberately. A scan emitting row ids feeds an
UPDATE or DELETE whose Sink resolves those ids back into the very rows the
scan handed out, so it reads what every other writer of that table reads,
never a narrowed set. And outside a staged transaction the narrowing is not
applied at all: a plain reader resolves its head per scan, so a narrowed and
an unnarrowed read of one table could stand at two heads — the tear
"one materialization per metadata table" exists to prevent. Inside a staged
transaction the read point is pinned for the transaction's life, so no such
tear exists, and that is where the flush path lives.

The narrowed materialization is cached beside the full one, never in place
of it, and staging into the table drops both.

### The stamp crosses the ABI

The shim's per-transaction pin is correct but wasteful as a lifetime:
DuckLake re-reads metadata at every transaction start, autocommit makes
every statement a transaction, and each rebuild pays the full ABI
crossing for rows that are byte-identical whenever no commit landed in
between. So the head stamp crosses the ABI: the attach holds one dumped
row set per synthesized table under the stamp it was dumped at, and a
transaction's first scan asks the store where it stands before paying to
re-dump. The pin becomes what it logically was — capture the stamp at
first scan, serve the transaction at it — and steady-state reads cross
the ABI once per table per *commit*, not per transaction.

Asking costs a read-write handle nothing: its held view is at head by
construction, so the stamp comes from the view rather than the store, and
the write path's saving is the whole ABI crossing with no read added to
buy it. A read-only handle pays the one point read it would have paid
anyway.

Rows that straddled a commit are not held. The stamp is read before and
after the dump and they must agree, because rows spanning two states
stand at neither, and holding them under the earlier one would serve a
concurrent reader at that stamp a row set from beyond it. The writer rule
outranks all of this: staged dumps are never cached over. And the stamp
is the whole stamp, id and batch count, because a maintenance batch
reuses the id.

### DuckLake's caches sit on top, and the stamps compose

DuckLake keeps two caches above all this: the per-transaction snapshot
(which the validity-window section already leans on), and a
schema-metadata cache keyed on the snapshot record's `schema_version`,
which RFC 0004 advances only on shape-changing commits. Its freshness
protocol is a read of `ducklake_snapshot` at every transaction start;
`schema_version` then decides whether any *other* metadata table is read
at all. So the tables DuckLake touches unconditionally — snapshots and
the stats kinds — are exactly the maintained projections, and the read
that validates DuckLake's cache is the read the stamp work above makes
one point read. The same validate-by-comparison runs at three altitudes
on one signal, which makes `schema_version` honesty a cache-stack
obligation, not only a protocol one: always-increment defeats DuckLake's
tier and re-fetches every table's columns through every tier below;
never-increment serves stale schema from a perfectly coherent moraine
stack.

The granularity seam is safe by construction: a maintenance batch moves
neither snapshot id nor `schema_version` — invisible to DuckLake's
caches, correctly, since it changes files and history, never schema — and
what DuckLake cannot see is what the batch count exists for, one tier
down. DuckLake's own catalog-cache race (`SET threads=1`) is upstream's
bug in upstream's tier, tracked by the presence test and nothing more.

### One block cache: two priorities, one budget

The physical tier is one shared cache instance per process, passed to
every `Db` and `DbReader` the extension opens (SlateDB supports sharing;
the sharer owns shutdown). SlateDB's split structure is kept, because the
split is the tuning:

- **SST indexes, filters, and stats are protected by eviction priority,
  not by a slot.** Every probe walks a filter and an index before it can
  touch a data block. Both kinds live in one cache: metadata is admitted at
  normal priority, data blocks at low, and foyer's LRU drains the
  low-priority list entirely before it touches the high-priority one. So a
  scan cannot push filters out and leave every later probe fetching to learn
  "not here" — metadata is evicted only once no data block is left to evict
  — while metadata a store does not need is space blocks may use.

  A fixed fifth of the budget is what this replaces. That slot was a ceiling
  as much as a floor: metadata could not exceed 20% however idle the block
  cache was, so the only way to hold a large store's filters was to inflate
  the whole budget fivefold and take the block cache that came with it.

  The share that replaces it is `METADATA_PRIORITY_POOL_RATIO`, and it is
  **not a reservation**. A data block is admitted whatever the share says,
  so metadata a store does not need is space blocks simply take, and an
  unused protected share costs nothing. It binds in one case only: metadata
  larger than the share, where the surplus is demoted to compete with blocks
  on recency rather than being protected.

  That is also why the share is not measured from the store. A census-seeded
  share was tried and removed: for any store whose metadata fits under the
  share it changes nothing, because nothing was being held back; for a store
  that grew past its census it demotes metadata that would have fit; and for
  a store larger than the cache it lands on the same clamp. The census is
  what the sizing check below is for, not what the share is for.

  What the cache is sized against is **residency, not a hit rate**. A probe
  for an absent key clears a filter for every SST on its path — all of L0,
  which is unpartitioned, plus one per sorted run — so holding all but a
  little of the metadata still leaves most such probes fetching: at
  per-entry residency `h` over `N` SSTs the probe is served whole with
  probability about `h^N`. 95% resident is not 95% as good as fitting.
  Absent-key probes are also the read no data-block cache can help with —
  they reach no block at all — so they are what this protection exists for.

  A store can still outgrow the protected share, and does so silently. Every
  attach reads the manifest once and compares the store's filter, index, and
  statistics bytes against the share actually in force, warning when it
  cannot hold them; `moraine_store_census` reports the same figures per
  subspace. The capacity is read after the open, so it is the one the
  process built rather than the one this attach asked for. The census
  figures are encoded lengths and the cache holds decoded entries, so the
  check understates the requirement and never overstates it.

  Three properties follow from the mechanism rather than from choice.
  Metadata past the protected share is **demoted, not refused** — it
  competes with blocks instead of being rejected — so occupancy may exceed
  the reported capacity, which bounds protection rather than residency. The
  pool is a share of a **shard**, and foyer divides the cache across eight
  by default, so the protection holds for metadata spread across shards as
  real SST keys are and degenerates only for a cache too small to shard.
  And **LRU is required, not preferred**: the priority hint is the only
  thing separating the two kinds, and foyer's LFU, S3-FIFO, and Sieve
  ignore it.

  Neither `CachedKey` nor `CachedEntry` exposes an entry's kind outside
  SlateDB, and `DbCache::insert` carries both — data blocks as a scan reads
  them, metadata under write-through admission. The kind is therefore known
  only on the typed `fetch_index` / `fetch_filter` / `fetch_stats` calls,
  which is where it is recorded so the two can be reported apart, and an
  untyped insert is treated as a block: admitting scan traffic into the
  protected share is the failure this design exists to prevent, and metadata
  that arrives untyped is protected again from its next typed fetch.
- **The cache is the foyer hybrid** when `CACHE_DIR` is set: a memory tier
  spilling at block grain to that device. Metadata shares it, so a
  metadata miss can be served from local disk rather than object storage.

`CachedObjectStore` is no longer configured — coexisting it with a hybrid
cache double-writes disk (SlateDB's own warning). Against it, this buys
one budget however many stores attach (the multiply-by-stores sizing rule
is deleted), block-grain fetches, admission control, and no double
residency. Measured: a cold probe into a 37.8 MB `index` run fetches
62 KB where a part cache would have faulted 4 MiB, and a warm one
fetches nothing (`BENCHMARK.md`).

The replacement covers the same *reusable* reads despite sitting a layer
up. The object cache caught every GET, but manifest versions and WAL
objects are read once per version and held in memory after, so the only
traffic worth caching is SST blocks, indexes, and filters — exactly what
the table store routes through the `DbCache` interface, deduplicating
concurrent misses on one key into a single fetch.

A process-wide cache needs a process-wide executor, and this is the one
place that is not automatic. foyer fixes the hybrid's task spawner when
the cache is *built*, so left to default the cache borrows the runtime of
whichever attach happened to build it — a runtime a detach then drops,
after which tokio cancels the tasks it owned and dropping an in-flight
fetch takes foyer's inflight lock with nothing left running to release
it. The next attach to touch the cache blocks forever. So the cache
builds its own runtime and hands foyer that; nothing the cache does runs
on an attach's runtime.

SlateDB scopes a cache's keys per opened handle from a process-local counter,
so two stores' same-numbered WAL SSTs stay apart within one process, but the
counter resets with the process and follows attach order. One disk device
shared by every store would therefore, after a restart in a different attach
order, serve one store's recovered WAL block for another. So the block cache
is **one foyer instance per store**, its device at `CACHE_DIR/<store>/blocks`
where `<store>` hashes the object store's name and the store's path. Inside
one store's directory the keys can only be that store's, so recovery is
enabled: entries are written to disk on insertion (a shutdown is not needed
to persist them) and read back on the next open. A store re-attached in the
same order gets the same scopes and is served from disk; a different order
misses until re-read, and can never alias. The one thing the directory
cannot tell apart is a store deleted and recreated at the same location,
whose WAL ids restart: its cache directory must be removed with it. Once
SlateDB accepts a caller-supplied stable scope
([`../slatedb.md`](../slatedb.md)) the order condition goes away.

The auxiliary cache is one foyer hybrid at `CACHE_DIR/auxiliary`, its keys
moraine's own, carrying the identity the caller's `DataStore` was built
with: a durable object store is named by its location, so a footer or
summary is served across restarts; an in-memory store is named at random,
so nothing recovered can match it. Footers go to disk in the
Parquet metadata writer's form, page index included; summaries in their own
shape.

**Admission follows read shape.** SlateDB's defaults already split it —
point reads cache their blocks, scans do not — and moraine makes the
split deliberate: two scan-option constructors, bulk (admits nothing; the
row caches absorb scan reuse) and probe (admits; the reuse is real and
block-grained), with every read path naming one. Foyer's admission picker
repeats the rule at the disk device, so scan and compaction churn cannot
wear it or evict the probe set.

Bulk and probe scans use fixed 8 MiB read-ahead with 32 fetches in flight,
sized for a remote object store.
These are implementation constants, not attach policy: they remove the
measured sequential-round-trip failure, while no local/S3 ladder demonstrates
that per-attach tuning improves a supported workload. A different value needs
that measurement and changes the implementation choice directly rather than
adding speculative options.

A SlateDB iterator is one sequential cursor however much it prefetches, so
the whole-subspace scans that materialize a view (`current`, and `history`
for time travel), the per-kind scans a staged transaction's metadata reads
issue, and a preload's deep walks split a large kind into concurrent
sub-ranges, eight in flight (`SCAN_SPLIT`, sized like the read-ahead for a
remote store). Only the kinds whose record counts scale with the data can
split — columns, data files, delete files, file column stats, table column
stats; schema-scaled kinds and the `sys`/`snapshot`/changelog subspaces
stay one iterator. The split is adaptive: the scan walks as one iterator
until a split kind has streamed one read-ahead's worth of bytes (the span
one round trip covers), then seeks that kind's highest key, divides the id
bytes between the last key seen and it evenly — so ids clustered near zero
still split where the records are — reads the remainder as concurrent
sub-ranges, and resumes the walk past the kind. A catalog that fits one
read-ahead therefore costs exactly one iterator, as before. A split scan
yields the same records in the same key order as one iterator would
(sub-ranges are concatenated in range order, never in completion order)
and counts as one scan in the handle's tallies.

The attach options keep their surface (RFC 0006) and change machinery:

- `CACHE_DIR` / `CACHE_SIZE` — the block slot's disk device and cap.
- `CACHE_MEMORY` (new) — one memory budget across both kinds, which share
  one cache and are separated by eviction priority; an attach whose store
  outgrows the protected share says so. Unset:
  what SlateDB gave a single store, now for the whole process. Never
  inert — the memory tiers exist without a `CACHE_DIR`.
- `CACHE_PUTS` — the flush/compaction insertion policy: written SSTs'
  blocks enter decoded, on write. Opt-in as before: compaction output
  evicts what reads warmed.
- `CACHE_PRELOAD` — a segment-aware warm, run as reads rather than as a
  manifest walk. SlateDB's per-SST warm call takes an id type its crate
  does not export, so no caller outside it can name one (the export request is
  tracked in [`../slatedb.md`](../slatedb.md)); reading is in
  any case the cheaper instrument, because a scan admits the blocks it
  touches and SlateDB caches every SST index and filter it walks whatever
  the scan's own admission says. So the levels differ by *subspace*, not
  by SST level: `'l0'` touches every subspace just far enough to pull its
  SST metadata — the bytes that make a cold probe tolerable — and `'all'`
  additionally walks the scan-shaped subspaces (`current`, `sys`,
  `history`, `snapshot`, changelog) whole, so an attach's first
  materialization reads no object storage at all. Neither walks the
  `index` subspace's data bulk, the tail that made `'all'` unaffordable;
  it stays one fetch per probed block behind the filters just warmed. The
  attach contract holds: warm inside the open, caps govern, a shortfall
  is warned with both numbers, a failure is skipped rather than fatal.
- **Per-table warm, explicit.** The `index` and `inline` subspaces
  scale with the data, so their warm is per table and never implicit: no
  read triggers it. `ReadOnlyCatalog::warm_tables` reads, in probe shape,
  the first entry of each named table's index ranges
  (`index/<kind>/<index id>`) and inline ranges (`inline/<table id>` per
  operation kind, its schemas and chunk-range locators), admitting the SST
  metadata and first block a probe there would fetch — one burst rather
  than a round trip per probe. Ranges are read many at a time, sized for a
  remote object store; a range that fails to read is skipped and counted
  in the log, never surfaced. The extension runs it after a commit for the
  tables the commit wrote data files against, alongside the row-summary
  warm; with `CACHE_PRELOAD` set, the extension also warms every table's
  probe ranges in the background after the open, spawned rather than
  awaited so the attach does not wait on it, and ended by detach.

**Hit rates before tuning.** The tiers report: hits and misses per slot,
process-wide through `moraine_cache_tally()` and per attach through
`moraine_cache_tally('lake')` (RFC 0006) — the budget is set at the first
scope and attributed at the second. Metadata and
blocks are counted apart because they are budgeted apart — a metadata
rate short of ~1 says the cache cannot hold the store's filters and
indexes, which the census measures directly, while a low block rate
beside a healthy metadata one is a working set larger than the block
slot. The same tally attributes the subset of hits and misses caused by
attach-time preload and counts subspaces preload failed to warm.
`moraine_cache_status()` reports actual capacity, occupancy, and eviction
counts for each memory cache plus the configured block disk capacity. It
does not report disk occupancy: Foyer's typed cache API does not expose a
reliable live figure. `moraine_store_census` reports each subspace's recorded
SST filter, index, and stats bytes, so metadata capacity can be compared with
the store it is expected to hold. A rate is absent rather than zero before
any lookup. Budgets are sized from these curves, not from the defaults.

Object-store traffic is measured separately per attach through
`moraine_object_store_tally('lake')` (RFC 0006). This is the physical
counterpart to cache hit rates: it distinguishes time in store requests from
time scanning and decoding the catalog, and it counts retries rather than
inferring round trips from DuckLake's metadata-statement count. The commit
breakdown benchmark keeps Parquet local, then reports DuckLake metadata SQL,
the staged transaction's one committed-record scan, Moraine commit time, and
staged bytes, its durable-write wait, and the exact main/WAL request counts and
summed latency over the same commit window. These phases are nested
measurements, not values to add together.

**One budget means one sizing per process.** The first store to open
settles it and its numbers stand; a later attach asking for different ones
is sized by what is settled. That is the budget being the process's rather
than the store's, which is the whole point, but it makes the *first*
attach's options the ones that decide — including whether there is a disk
directory at all.

A cache per attach with its own budget is rejected: it would multiply
`CACHE_MEMORY` by an unnamed attach count. A cache per *store* under one
budget is what is built: every attach of a store shares its instance, the
memory split follows the attached count, and the executor stays process-wide.
Disk is a ceiling per store, not a reservation — a store's device takes
`CACHE_SIZE` as its cap and fills only with what that store reads.

The SST block size is fixed at 4 KiB. Read-ahead removed the per-block network
round trip from scans, while the measured probe shape is zero bytes warm and
62 KB cold, so there is no remaining evidence for a 4/16/64 KiB sweep or an
attach option. A future profile that puts material weight on block grain must
change this binding choice and add the option and the scan/probe benchmark
together.

Two losses, taken knowingly. Part-grain prefetch: replaced by the scan
path's own read-ahead (the measured fix for the 277 s materialization in
`BENCHMARK.md`), with admission per the shape rule. And a `CACHE_DIR`
shared between concurrent processes: a foyer device has one owner at a
time — the deployed topology never shared a directory across hosts.

### The query data path is DuckDB's; index upkeep reads scoped metadata

Ordinary lake scans never send data-file bytes through moraine and add no
data-page cache or read-through layer: a lakehouse's query data path belongs
to the engine reading it, and duplicating DuckDB's would be the mistake this
RFC removes from the catalog tier.

It does not have to. **A lake read goes through DuckDB's caching file
system and populates the external-file cache**, so data bytes are held
under `memory_limit` and evicted by the buffer manager alongside every
other allocation. Measured over a remote `DATA_PATH`: one query fetches
the file's footer and its data ranges, a second differently shaped query
over the same data fetches nothing, and the same pair under
`enable_external_file_cache = false` fetches everything twice.

Equality-index upkeep is the narrow exception. A commit deleting indexed
rows must recover their indexed values, and its Rust scoped reader talks to
`DATA_PATH` directly rather than entering DuckDB's Parquet reader. DuckLake's
recorded file and footer sizes are carried into every registered data-file and
delete-file read. The production scoped-reader API requires both values in one
file descriptor; metadata discovery is not a production state. For a large
object, the footer size lets the first read
prefetch the serialized footer and its trailing length in one request; data
files then fetch only indexed values and row ids, while delete files fetch only
their position column. Objects below the measured crossover remain one
whole-object request. A process-wide byte-bounded cache retains the parsed
footer and page indexes for repeated touches of the same immutable file; it is
keyed by object-store identity, path, file size, and page-index policy, and
holds only metadata. Its allowance is one sixteenth of
`CACHE_MEMORY`'s metadata share, capped at 8 MiB, with the remainder of that
share continuing to hold SlateDB metadata — one Moraine memory budget, not a
third invisible one. Projected data-column ranges are never retained. This
cache exists because DuckDB's metadata cache cannot be reached from the direct
Rust reader, not as a second copy of metadata DuckDB served to it.

**Data-block caching rides on the Parquet reader's prefetch, taken only
for files that are not on local disk** — without it the reader issues a
deliberately non-caching read. So a local `DATA_PATH` caches the footer
and stops there, and `prefetch_all_parquet_files` forces the remote
behaviour, which is how the property stays observable in a test with no
object store. The gate is locality rather than scheme, so a deployment
on S3 takes the prefetching path.

Two shapes therefore report a working cache as an absent one, and a
measurement must avoid both: a lake `count(*)`, answered from catalog
statistics without opening a file at all, and a local path's
footer-only caching.

So the data-page tier is *unified, not un-budgeted*: it is DuckDB's cache,
inside DuckDB's budget. A host sizes DuckDB's `memory_limit` and moraine's
`CACHE_MEMORY`; the latter includes the scoped reader's parsed metadata. A
further tier exists only if an operator adds a filesystem cache.

That remains a real option, and its value is what the external-file
cache does not do: survive the process. The external-file cache is
buffer-manager memory and dies with the instance, so a redeploy or a new
process reads cold. A *filesystem*-level cache intercepts below the
reader and keeps bytes on disk across processes — on S3 the
`cache_httpfs` community extension. It buys that at the cost this RFC
spent the catalog tier removing: its cache is sized by its own knobs,
outside `memory_limit`, and keeps a read-through memory cache even in
its on-disk mode. So it is an operator's trade, made knowingly, and not
something the attach should arrange — moraine arranging it would
re-create an un-budgeted tier one layer down.

The embedder configuration worth setting beside an attach:
`validate_external_file_cache = 'NO_VALIDATION'`, which is live for lake
data and safe because DuckLake never rewrites a data file, so a cached
range cannot go stale under it; `parquet_metadata_cache` (DuckDB's
`ObjectCache`, keyed on file path); and the httpfs metadata and
connection caches at the filesystem layer. All are global, so the shim
sets none of them — an `ATTACH` that mutates global state reaches every
other database in the process — and they are documented as embedding
guidance beside the attach options (RFC 0006).

### Test obligations

Per RFC 0001, integration tests run against real SlateDB on in-memory
`object_store` — no mocks of the store:

- **Consistent cut.** A commit forced to land mid-materialization yields a view
  that is entirely pre- or entirely post-commit, never torn. Both halves are
  staged rather than raced: on a read-write handle the commit lands between
  the pass's head read and its `current` scan and is invisible to both; on a
  read-only one the pass commits from inside itself and waits for the reader
  to observe it, and the stamp check discards and re-runs it.
- **Isolation.** A `CatalogSnapshot` built at `S`, then `k` commits applied,
  still returns exactly the `S` view from every accessor.
- **Incremental == full.** Refreshing `S → head` incrementally yields a view
  identical to a full rematerialization at head — entity maps *and* name
  indexes, so a rename that left a stale name entry behind is caught.
- **Every batch moves the stamp.** A snapshot-minting batch moves both
  halves of the head record; a maintenance batch that reuses the snapshot id
  still moves the batch count.
- **The refresh declines what it cannot replay.** A gap holding a batch
  that minted no snapshot, a gap whose changelogs later commits swept out of
  the retained window, and a gap whose churn passes the size backstop each
  fall back to a full rematerialization rather than replaying part of it.
- **The window bounds the subspace.** However many commits land, the
  changelog subspace holds no more records than the window.
- **Committer read-your-writes.** After `commit`, the committer's folded view
  reflects the just-committed entities without a store re-read.
- **A warm read equals a cold one.** A handle that has served reads across a
  sequence of commits answers exactly as a freshly opened handle does.
- **Install is compare-and-set.** An invalidation interleaved between a
  reader's first store read and its install voids that install; the cache
  never serves the resurrected view.
- **The marker outranks the cache.** A read against a mid-migration store
  returns the typed `Migration` error even when the handle holds a valid
  cached view.
- **Fallback.** A reader whose `S` fell below the horizon rematerializes at
  head; a reader asking for an expired `S` gets `SnapshotExpired`.
- **No dangling file.** A view within the retention window resolves every file
  it names; a view driven past the window fails loudly rather than naming a
  reclaimed file.
- **Migration marker refusal.** A materialization or refresh attempted while
  `sys/migration` is present returns the typed `Migration` error and never
  yields a partial view (RFC 0015's mid-migration reader contract).
- **No conflict-window gap.** A commit forced to land between a commit
  attempt's materialization and its batch write is always detected: the
  attempt either observes it (materialized through the transaction, so the
  premise view includes it) or conflicts on `sys/head`. There is no
  interleaving in which the attempt commits against premises that omit a
  landed commit. Staged, not raced: a transaction opens and materializes,
  a commit lands through the catalog, and only then does the first write.
- **Maintained == scanned.** After every commit in a randomized operation
  sequence (creates, file registrations, stats updates, renames, expiry), each
  maintained projection equals a fresh scan of the same subspaces at the same
  head.
- **A metadata table is one materialization per DuckDB transaction.** Over
  two connections on one instance: a commit landing under an open reader does
  not change what that reader's transaction sees of `ducklake_snapshot`, the
  two-scan resolution shape still resolves to a row inside it, and the
  reader's next transaction sees the commit. A *writing* transaction is
  exempt — it reads its own uncommitted rows across statements.
- **Reads survive an expiry pass under them.** With the scheduler expiring
  everything below head on a sub-second tick, a session that keeps reading
  and committing never sees "No snapshot found".
- **One scan pair per head.** Populating every metadata table and
  materializing the head view at one head costs one `current` scan and at
  most one `history` scan; a head view alone scans no `history`.
- **The stamp crosses the ABI.** A read transaction following another with
  no commit between serves the same metadata rows without a dump rebuild;
  with a commit it re-dumps; a staged-write transaction is served staged
  dumps regardless.
- **A warm probe costs no store read.** An index lookup repeated against
  a resident working set issues no GET and fetches no bytes.
- **One budget across attaches.** Several attached stores share one cache
  and one tally; a later attach's differing options are reported as
  ignored rather than silently applied.
- **Restart persistence and isolation.** Two stores with colliding WAL and
  L0 ids are warmed by one process; a fresh process attaching them in the
  same order reads both without a cache miss, and one attaching them in
  reverse reads the correct store from both. The test is cross-process so
  the scope counter actually resets.
- **Scans cannot evict the probe path.** After a whole-subspace scan and a
  compaction through a warm cache, a probe that was GET-free stays
  GET-free.
- **Admission follows the declared shape.** A bulk scan admits no data
  blocks; a probe admits its own.
- **Preload warms before the first read.** An attach that preloads has
  consulted the cache by the time it returns, and warms no `index` data
  blocks at either level.
- **The cache reports what it served.** A cold read moves the tally, the
  slots are counted apart, and a rate is absent rather than zero until
  something has been looked up.
- **The data path is cached by DuckDB.** A lake read that touches data
  populates `duckdb_external_file_cache()` for its file, and forcing the
  prefetch a remote path takes anyway extends that from the footer to the
  data ranges — so the tier stays inside `memory_limit`, and a regression
  that routes lake reads around the caching file system fails the test. A
  lake `count(*)`, answered from catalog statistics, is pinned as reading
  no file at all: it is the shape that makes a working cache look absent.
- **Owned and borrowed value decoding share one scan.** An inline protobuf's
  shared body remains inside the allocation yielded by SlateDB, while an
  ordinary protobuf decoded by borrowing observes the same key order and
  errors at the same entry. Neither path copies the framed value in the scan
  loop.
- **Concurrent parsed-metadata misses coalesce.** Two scoped readers starting
  cold against the same file issue one footer/page-index fill and retain one
  parsed value, while each continues to read its own projected data ranges.
- **Delete-file reads preserve the crossover.** A small delete file is fetched
  once in full. A large one uses its recorded file and footer sizes, fetches
  only `pos`, and reuses parsed metadata on a later read.

## Alternatives considered

- **Per-get consistency (no single read point).** Rejected: independent gets
  across a concurrent commit can observe a torn view (new `column`, missing
  `file`). One read point — pinned where SlateDB offers one, validated where
  it does not — is the only cheap way to get a consistent cut without
  locking writers.
- **Re-scanning `current` on every read (no cache / no incremental refresh).**
  Rejected: defeats RFC 0003's "no store I/O after build" and pays the full
  scan per refresh. The write set a commit already assembles is a precise,
  cheap changelog; record it and use it.
- **Driving the refresh off DuckLake's `snapshot_changes` instead of a
  moraine-native key list.** Rejected: the grammar is table-grained, names
  created relations by name rather than id, and has no entry at all for a
  stats-only or schema-tag commit — a replay of it would silently miss
  writes. Keeping the two side by side costs a bounded per-commit list and
  buys a changelog that is exact by construction.
- **Validating a read-only cut on `DbReader::manifest()`'s id.** Rejected
  on inspection: the reader's WAL-replay refresh carries the manifest
  forward unchanged, so the id does not move for exactly the commits a live
  reader is trying to see. `DbStatus::durable_seq` does move, but leads the
  state install rather than following it. A stamp in moraine's own keyspace
  is under moraine's control and needs no assumption about SlateDB
  internals.
- **Pinning a read-only handle to a checkpoint.** Rejected as the general
  answer: it is genuinely consistent, and moraine keeps it as an explicit
  option for credentials that cannot write the manifest at all, but it
  spawns no poller and so never sees another commit. Liveness is not
  optional for a reader that is meant to follow a lake.
- **Push-based invalidation (committer notifies readers of new commits).**
  Rejected: contradicts RFC 0004's no-coordination, cross-process topology and
  reintroduces a notification channel the design deliberately omits. Readers
  poll `sys/head`; that is the whole protocol.
- **A persistent name→id index to skip materialization.** Already rejected by
  RFC 0002 (complexity without payoff at live-catalog scale); nothing here
  changes that calculus. Name resolution stays against the in-memory view.
- **Partial or lazy materialization.** Rejected with server-side filter
  pushdown while the live catalog fits comfortably: lazy reads need predicates
  to know what to fetch, and predicates save nothing while the whole view is
  already resident. This also leaves the replay base-view copy proportional to
  catalog size. A profile showing either memory term to be material would be
  evidence for replacing both choices together.
- **Rematerializing fully on every refresh (no incremental path).** Rejected as
  the default: correct but pays catalog-size cost per refresh for a
  churn-sized delta. Kept only as the fallback when the gap is large or the base
  snapshot was reclaimed.
- **Holding files alive for any live reader (reference-tracking readers against
  expiry).** Rejected: it is exactly the cross-process reader coordination RFC
  0004 forbids and RFC 0007's reference-counting alternative already rejected.
  The retention *window* plus a typed `SnapshotExpired` is the coordination-free
  contract.
- **Keeping SlateDB's object cache as the disk tier.** Rejected: per-store
  caps, 4 MiB parts against a point-probe workload, and it cannot coexist
  with a hybrid cache without double-writing disk. Prefetch is covered by
  the scan path's read-ahead, write-side warming by the insertion policy;
  the loss is a cross-process cache directory, weighed above.
- **One pooled hybrid cache, no slots.** Rejected: a pool lets data blocks
  evict filters, after which every probe pays a fetch to learn "not
  here". The metadata is small enough to pin; only data blocks need
  tiering.
- **One entry store for SlateDB and parsed Parquet metadata.** Rejected: the
  former is a database-scoped encoded-block cache with admission and an
  optional disk tier; the latter is an object-store-scoped typed-object cache
  whose values are process-only. They share the one capacity calculation,
  but merging their entries would require either changing SlateDB's closed
  cache types or replacing its cache policy with a weaker type-erased one.
- **Separate borrowed and owned store scan loops.** Rejected: SlateDB already
  yields an owned `Bytes`, whose move is constant-time. One extractor may
  consume it and another may borrow it locally without duplicating ordering,
  admission, collection, and error handling.
- **A moraine-level data-file byte cache.** Rejected: DuckDB's external-file
  cache already holds data bytes inside `memory_limit`; adding one re-creates
  the double-caching this RFC removes. The scoped equality-index reader's
  bounded parsed-metadata cache is the exception above: that reader bypasses
  DuckDB, and it never retains data pages.
- **Setting DuckDB's cache settings from the attach.** Rejected: they are
  global, and an `ATTACH` that mutates global state reaches every other
  database in the process. Guidance over mutation.
- **A cross-process row cache (serialized projections on disk).** Rejected:
  preload bounds process-cold cost at re-fetch cost, while a persisted row
  snapshot is a second durable encoding to version and migrate (RFC 0015).
  Deploy-cold attach must first be measured as a material problem; that result
  would justify a new format design rather than an open-ended cache task.

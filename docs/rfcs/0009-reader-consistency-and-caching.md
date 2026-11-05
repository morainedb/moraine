# RFC 0009: Reader consistency and snapshot caching

- **Date:** 2026-07-09

## Summary

Ordinary metadata reads are demand-loaded at a stable read point. SlateDB's
process-shared Foyer block cache, including its optional disk tier, provides
reuse. Moraine does not retain decoded catalog snapshots, record sets, or
metadata result arrays between operations. Native DuckDB catalog entries and
bounded row-lookup results have their own ownership and limits.

Writers use SlateDB snapshots; independent reader transactions share fixed
checkpoint readers at writer-published durable boundaries. Explicit
`CatalogSnapshot` calls remain a materializing compatibility API, with all
returned memory owned by the caller.

## Goals

- Every transaction reads one consistent store revision across calls.
- Ordinary reads and staged writes decode only their required records.
- Enumeration and maintenance keep bounded working batches, including for one
  table larger than memory.
- A successful commit is covered by a published durable checkpoint.
- Catalog cache reuse stays in Foyer; decoding is operation work.
- Store checkpoint protection and external data-file retention remain separate.

The physical key layout is in RFC 0002, the API in RFC 0003, the commit
protocol in RFC 0004, and data-file reclamation in RFC 0007.

## Design

### Demand-loaded scopes and admission

Writer read scopes retain `DbSnapshot` without loading catalog entities.
Independent readers retain fixed checkpoint readers across calls, using
SlateDB's existing APIs without a dependency patch. Legacy unscoped
materializations still verify their read boundary; scopes never silently
downgrade isolation.

#### Reader scopes using existing checkpoint APIs

The patch-free approach is to rotate checkpoint readers. The writer publishes
a checkpoint with `Db::create_checkpoint(CheckpointScope::Durable, ...)`.
A reader process opens `DbReaderMode::Checkpoint(id)` and shares that reader
among transactions starting at that revision. Each transaction retains its
reader until completion. Publishing a newer reader changes what new
transactions see without changing any existing transaction's view. Reuse the
same process-wide Foyer cache and store identity; retain no decoded catalog
object cache. `store/checkpoint.rs` publishes and leases these boundaries;
`catalog/handle/reader_scopes.rs` shares the fixed readers. At most eight
local generations may remain live; acquisition refuses another generation
until an older transaction or cursor finishes. Reader leases last five
minutes and renew every minute, with a thirty-second renewal timeout.
Every point read and scan yield checks expiry.

Checkpoint publication and ownership have separate lifetimes. A reader can
create its own checkpoint with `CheckpointOptions::source` set to the
published checkpoint, then renew that checkpoint while local scopes use it.
The source must still exist when acquired; retry discovery if acquisition
loses a race with retirement. Close the reader and release its checkpoint
only after its last scope and cursor finish. Finite leases handle crashes;
renewal failures must invalidate scopes before their protection expires.
SlateDB checkpoints protect store files, not DuckLake's external Parquet
files; the existing catalog retention contract still applies to those.

Do not substitute `Admin::create_detached_checkpoint` against the latest
manifest for writer publication. It does not advance the manifest's WAL
boundary. A following reader can already see durable commits that such a
checkpoint omits. On the pinned SlateDB revision, an in-memory object-store
experiment with automatic writer manifest polling disabled confirmed:

- A following reader saw a WAL-durable overwrite while a newly created
  detached checkpoint still returned the previous value.
- A writer-created durable checkpoint included the overwrite.
- Opening the next checkpoint reader advanced new reads while the old reader
  continued returning its original value, with the writer still operational.
- A checkpoint copied from the old checkpoint remained readable after the
  publisher deleted its own checkpoint.

Publish after WAL durability and before acknowledging each commit group.
One publication may cover multiple groups whose writes are already durable;
each acknowledgement waits for a checkpoint covering its group. A WAL-durable
write whose publication fails is potentially committed, never a conflict
that authorizes replaying the mutation. Retry publication or return an
unacknowledged outcome for reconciliation, as specified in RFC 0004.

Reader discovery can lag publication. A new scope with a required commit
position must acquire a checkpoint whose stored head covers that position.
Existing scopes never switch readers to satisfy a newer position. Commit-log
acknowledgements publish a checkpoint covering the won slot. A committer
holding the writer publishes after replaying its tail; another committer
opens a temporary publisher, verifies the recovered head covers its slot,
and closes without forcing an L0 flush. Publication errors report an unknown
acknowledgement outcome rather than permitting mutation replay. This can add
writer-open and fencing costs under competing committers; measure those
costs before choosing a retained publisher or another publication cadence.

Periodic publication is not the initial policy: it would add visibility delay
and could leave acknowledged WAL commits unpublished after a writer crash.
Changing that policy requires an explicit freshness contract and measurements.

A clean writer shutdown flushes memtables and republishes the resulting
manifest even if its sequence was already acknowledged. Otherwise an idle
reader can keep replaying a WAL-heavy publication after those same rows have
reached SSTs. This does not change the commit acknowledgement boundary.

Sharing a reader per checkpoint amortizes open and WAL replay costs across
transactions. Old generations still retain separate manifests and replayed
WAL memtables even when their Foyer blocks are shared. Admit those allocations
and bound live generations; do not assume that a per-memtable size setting
bounds total replay memory. Measure publication latency, reader-open latency,
manifest writes, retained SST bytes, and overlapping-generation memory before
changing publication policy. The experiment above validates visibility,
not remote-store performance, lease-expiry behavior, or GC under load.

Other patch-free approaches have different contracts:

- Validate every operation against the transaction's original `sys/head`
  stamp and abort the entire transaction if it changes. This can fail safely
  without retaining a catalog, but any visible catalog change can abort a
  long read. Per-call retries at newer stamps do not provide repeatable reads;
  emitted batches cannot be transparently retried independently.
- Reconstruct a fixed DuckLake snapshot through catalog history. Existing
  history is insufficient: options, statistics, and mappings do not all
  retain previous values, and maintenance can change state without advancing
  the DuckLake snapshot id. Extending versioning and retention is a larger
  key-layout and commit-protocol change.
- Increase the reader poll interval or add a Moraine mutex. Neither freezes
  SlateDB's internal refresh task and neither establishes a snapshot.

The source references for these APIs are SlateDB's
[checkpoint reader modes](https://github.com/slatedb/slatedb/blob/v0.18.0/slatedb/src/db_reader.rs),
[writer checkpoint API](https://github.com/slatedb/slatedb/blob/v0.18.0/slatedb/src/checkpoint.rs),
[detached checkpoint and lease APIs](https://github.com/slatedb/slatedb/blob/v0.18.0/slatedb/src/admin.rs),
and [checkpoint lifecycle documentation](https://slatedb.io/docs/design/checkpoints/).

#### Cache admission

Point lookups and table metadata scans admit fetched blocks to Foyer.
Table scans use bounded read-ahead even for millions of files. Catalog
listings, exhaustive cascades, expiry, GC, and backfills stream without
admitting scan data blocks. SST indexes and filters retain their cache
policy. Admission changes neither visibility nor result completeness.

Entity cursors emit current records followed by ended records, each in
encoded-key order. Time travel applies begin/end visibility at the selected
snapshot. Test with real SlateDB that a scoped read never decodes unrelated
records, including across commits, eviction, cancellation, and migration.
Measure memory, decode CPU, warm/cold latency, and remote reads.

### Consistency of lookup and inline operations

Equality, IN, range, and NULL index lookups, full inline scans, and requested
inline-row lookups guard the entire operation, including definition checks,
tombstones, chunk bodies, and schema reads. Manifest readers compare the full
head stamp before and after the operation. An error is returned only after
that comparison: a missing chunk during a refresh may be a torn pass rather
than stored corruption. An unchanged pass preserves its original error.

A changed pass is retried at most eight times. If every pass changes, the
operation returns `RetryBudgetExhausted`; it must not fall back to an
unguarded scan. Writer-backed isolated transactions run the operation once.
This boundary covers a single call, not several independent API calls.

### Explicit materialization

`snapshot()` and `snapshot_at()` materialize a caller-owned `CatalogSnapshot`.
Writer reads use an isolated store read point. An unscoped following reader
validates the complete head stamp before and after the materialization and
retries a changed pass. A pinned reader uses its checkpoint throughout.

The catalog handle retains neither the resulting snapshot nor the decoded
records used to construct it. Repeated calls decode again through Foyer.
Concurrent explicit materializations therefore require independent working
memory. Callers that need several reads at one revision should hold a read
scope and use point reads or cursors.

The changelog remains a compatible stored record. Ordinary demand-loaded
reads do not replay it to maintain decoded catalog maps.

### Committer read-your-writes

A staged transaction reads its fixed authored scope and overlays its pending
rows. Coalesced commits additionally fold earlier members' pending writes into
each member's operation-owned premises. No transaction retains committed
entity, snapshot, or schema-version result caches.

After acknowledgement, a new reader scope acquires a published checkpoint
covering the commit. An already-open scope continues seeing its old revision.

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

### DuckDB metadata ownership

A DuckDB transaction retains a lightweight read scope. Every metadata scan
reads that scope, so two scans of `ducklake_snapshot` agree even when another
connection commits between them. Transactions overlay their own staged rows.
Neither the attach nor the transaction retains a general metadata-row cache.

The cursor ABI currently streams 64-record pages for schemas, tables, views,
columns, files, delete files, statistics, partitions, sorts, macros, mappings,
scheduled deletions, partition columns and values, sort expressions, tags,
variant statistics, macro implementations and parameters, name mappings, snapshot headers and
changes, and column/view-column tags.
Working-memory reservations cover a decoded page
through ABI conversion; freeing the page releases its rows. Cancellation or a
failed read invalidates that cursor. An expired checkpoint lease also rejects
prefetched scan entries.

Schema-version projections stream stored and legacy rows and compute file
floors with two ordered file cursors. Options stream after a bounded prefix of
synthetic facts. Staged metadata overlays still have array-based paths. These must move to bounded cursors before claiming that every metadata
operation is independent of result size. Streaming row IDs resolve through a replay cursor; array-based overlays still
retain their operation rows for mutation resolution.

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

### Ownership layers

- Foyer shares SST metadata and blocks across handles and checkpoint generations.
- A read scope retains its store snapshot or leased checkpoint reader.
- One operation owns its decoded records and output page.
- DuckDB owns native catalog entries and query execution state.
- Bounded row lookup and Parquet metadata facilities retain their separately
  budgeted data-path results; they do not retain full catalog snapshots.

A local end-to-end metadata scan measurement is recorded in
[demand-loaded catalog memory](../demand-loaded-catalog-memory.md). It includes
default background preloading and reports its limitations separately from the
bounds required here.

Each live checkpoint generation can also replay WAL into its own memtables.
The generation cap limits overlap but is not an RSS limit.

### Shared budget, separate cache engines

The SlateDB caches and the scoped reader's parsed-Parquet cache share a
**budget and process lifetime, not an entry store**. `CACHE_MEMORY` is split
once, at the first attach: auxiliary metadata and lookup retention take their
allowances, and the remainder is the block budget shared elastically by stores.
An empty attach reserves no block memory and does not shrink another store.
Read fills, disk promotions, and admitted flush or compaction output grow
the requesting store's allowance under pressure. Growth first uses unassigned
bytes, then borrows unused allowance from other stores, retaining headroom
above their occupancy. If those bytes are insufficient, a newly busy store
can reclaim its fair share from larger borrowers; stores with no resident
entries are not counted as contenders. The allocator does not infer activity
from wall-clock time: an attached store with resident entries still contends.
An initial share comes from unused capacity without ramping through tiny
quotas; reclaiming occupied capacity instead grows with measured demand.

Capacity is transferred under one process lock, shrinking donors before
growing the recipient. The sum of all store capacities never exceeds the
block budget. A store's final detach releases its memory allowance entirely;
its disk device remains open and retains persisted blocks. Allocation checks
are amortized over bytes under pressure, and ordinary hits do not require
the process lock. Per-store metadata protection and disk isolation remain.

The allowance is a **share of the budget to a ceiling**, not a fixed figure.
It cannot share one eviction store with SlateDB's — foyer's cache is
monomorphic and both the key and the value type are SlateDB's own, neither
constructible here — so the bytes are deducted whether the parsed-metadata
cache fills them or not. That reservation is why it scales with the budget
rather than standing at a small host's number, and why it stops scaling
rather than reserving unboundedly on a large one.

The head projections take no share of the budget at all. They are per handle,
hold one head's decoded state, and are replaced when the stamp moves rather
than evicted under pressure, so they are bounded by the catalog's own size —
which is a bound, but not one an operator can set.
`ReadOnlyCatalog::projection_bytes` reports it, because a cache invisible to
sizing is what this RFC's goals rule out.

Lookup directories keep local ceilings of 64 MiB of accounted file entries and
64 MiB of inline entries per catalog, within the shared lookup allowance described
below. Charges include
retained summaries, file metadata, deferred ranges, path strings, and entry
headers. They are estimates, not allocator measurements; shared allocations
are charged conservatively. Oversized replacements remove the older cached
directory, serve the current query, and may be rebuilt on later lookups.
Eviction releases the cache's ownership, while active readers retain the value
and its budget reservation until their queries finish. It does not impose a bound on transient scan/build
memory or total process memory.

Index probes use the same revision-keyed memo as other scoped results. Hits
clone only an Arc under the mutex; copying the result rows happens after the
lock is released. Composite-key lookups borrow probe arguments without copying
them. A hash index points into an LRU slab, so lookup and recency updates do
not scan or shift all retained entries. Full revision/table/key equality checks
resolve hash collisions. Entry charges include the slab node and occupied hash
bucket; diagnostics also count spare slab capacity and hash-table allocation
slack. The local count and byte ceilings and the process lookup allowance govern
admission. They bound cache retention, not total process memory.

Their storage engines remain separate because their entries have different
identity, value, and tiering contracts:

- SlateDB owns keys scoped to an opened database and values such as SST
  filters, indexes, and decoded blocks. Its `DbCache` controls concurrent-fill
  suppression, admission, and the optional Foyer disk tier.
- The scoped reader owns immutable `Arc<ParquetMetaData>` values keyed by
  object-store identity, path, and recorded file size — one entry per file,
  whichever page-index policy first loaded it. A reader that needs the page
  index a resident footer lacks fetches only the index and upgrades the entry
  in place, so a whole-file read and a selective read of one file parse its
  footer once between them. They are useful only inside this process, stay
  memory-only, and are evicted by parsed byte weight. Concurrent misses for
  one key share one in-flight fetch and parse; the result enters the cache
  once, while a failed fill is shared only with its current waiters and
  remains retryable.

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

### Materialized compatibility views

A compatibility materialization may collect the requested record set under
working-memory admission. The set is local to that call and is never installed
on the catalog handle. Exhausting the allowance returns a typed error; it does
not authorize an unbounded allocation.

### A reader that cannot match an ended version does not read them

`history` grows with every ended version and is pruned only by snapshot
expiry, so a scan pair that always reads both halves sawtooths against the
expiry cycle rather than against the live catalog. Most of what it reads is
discarded: every DuckLake read of a versioned table carries
`{SNAPSHOT_ID} >= begin_snapshot AND ({SNAPSHOT_ID} < end_snapshot OR
end_snapshot IS NULL)`, and at the head the second half of that keeps
nothing from `history`.

The rule: **a scan may read `current` alone when its caller has been shown
that no version in `history` can satisfy it.** Two filter shapes prove
that, and the core — not the shim — checks the first:

- A record reaches `history` by being ended, stamped with the snapshot that
  ended it, which is never past the head. A record there always carries an
  `end_snapshot`; the `IS NULL` arm never keeps one.
- So a reader keeping rows only while `filter_snapshot < end_snapshot`
  matches nothing in `history` once `filter_snapshot` has reached the head.
- Time travel reads behind the head and keeps the full pair, which is what
  makes the narrowing safe by construction rather than by care.

The second shape is `end_snapshot IS NULL` standing alone, which DuckLake's
own writes carry where its reads carry the pair — the statement that ends a
rewritten file names the versions it may end and nothing else. It needs no
comparison against the head at all: a record in `history` always carries an
`end_snapshot`, so this conjunct excludes every one of them whatever the
head stands at. It travels as a snapshot id like the bounded form, valued
at every snapshot a store can reach, so one path serves both.

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

One scan is left out. Outside a staged transaction the narrowing is not
applied at all: a plain reader resolves its head per scan, so a narrowed and
an unnarrowed read of one table could stand at two heads — the tear
"one materialization per metadata table" exists to prevent. Inside a staged
transaction the read point is pinned for the transaction's life, so no such
tear exists, and that is where the flush path lives.

A scan emitting row IDs uses the same bounds and fixed scope. Streaming runs
reserve an identity range and reconstruct requested rows by replaying that
cursor, retaining at most one page during mutation resolution. Sequential
resolutions advance the cursor; an out-of-order resolution restarts it and
can therefore cost additional reads. Array-based overlay paths still resolve
against their operation-owned scan rows.

Staged reads decode the requested committed range for each operation and
apply pending mutations. Neither full nor narrowed results are cached.

### The read scope crosses the ABI

A snapshot ABI handle carries the head stamp and a read alias backed by one
fixed scope. Descriptor listing projects only the requested descriptor kind;
explicit materialization is separate. Metadata cursors retain the same scope.
Native transaction schema entries live for the transaction; a new transaction
resolves entries from its new scope.

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
where `<store>` is `v2-` followed by a hash of the explicit cache identity
and the store's path, with unambiguous component boundaries. Inside one store's
directory the keys can only be that store's, so recovery is
enabled: entries are written to disk on insertion (a shutdown is not needed
to persist them) and read back on the next open. A store re-attached in the
same order gets the same scopes and is served from disk; a different order
misses until re-read, and can never alias. The one thing the directory
cannot tell apart is a store deleted and recreated at the same location,
whose WAL ids restart: its cache directory must be removed with it. Once
SlateDB accepts a caller-supplied stable scope
([`../slatedb.md`](../slatedb.md)) the order condition goes away.

The auxiliary cache is one foyer hybrid at `CACHE_DIR/auxiliary-v2`, its keys
moraine's own, carrying the identity the caller's `DataStore` was built
with. `DataStore::new` and catalog opens without a cache identity use fresh
random namespaces, including for custom stores whose display names happen to
match. Explicit `CacheIdentity` values allow safe sharing across handles and
restarts. Equal identities assert that equal object paths name the same
immutable contents; custom wrappers must include their routing context and
prefixes. Display strings never determine storage equivalence.

DuckDB attachments supply stable identities: local stores use the resolved
filesystem root; S3 stores include the configured endpoint (including the
S3-specific override), region, bucket, addressing style, and Express setting.
These values come from the fully configured builder, including environment
configuration when no secret is supplied. Credentials are excluded so rotation
preserves reuse. In-memory attachments retain random identities.

Identity hashes and block-cache directories use a new versioned namespace;
the auxiliary disk tier uses `auxiliary-v2`. Old display-based cache entries are not recovered and
may be removed; catalog data needs no migration. Footers go to disk in the
Parquet metadata writer's form under one of two tags: with the page index
when it was loaded, else without, decoded without one since the writer keeps
the original file's index offsets, which the disk form does not carry. Footer
entries written while the key still named a page-index policy are unreachable
under the policy-free key and age out. Summaries go in their own
shape.

**Admission follows read shape.** SlateDB's defaults already split it —
point reads cache their blocks, scans do not — and moraine makes the
split deliberate: scan-option constructors, bulk (admits nothing; the
row caches absorb scan reuse) and probe (admits; the reuse is real and
block-grained), with every read path naming one. Foyer's admission picker
repeats the rule at the disk device, so scan and compaction churn cannot
wear it or evict the probe set.

A read that takes one entry — or a handful — from a wide range names a
fourth shape, seek: one block fetched at a time, admitted. The probe shape
would be wrong there, not merely wasteful: SlateDB spawns its read-ahead
fetches eagerly when the iterator opens, up to 32 of 8 MiB each per SST, and
a fetch task has no cancellation on drop, so an iterator dropped after one
`next` leaves them running to completion. The seek sites are the first entry
of a subspace or of a table's probe range (the cache warms), the highest key
of a split kind (the adaptive split's one seek from the end), and a
chunk-directory walk that a known chunk width ends within a few entries of
its last target. A directory walk with no width bound runs to the end of the
directory and stays a probe. The dead-index sweep's read of the first index
id at or past a cursor is not a seek site, though it takes one entry: a
swept index leaves its whole range as tombstones ahead of the first live key
until a store merge drops them, and SlateDB filters tombstones from the
iterator only after fetching every block they occupy, so a one-block seek
there pays a round trip per block of them on every pass. That read stays a
bulk scan, reading ahead and admitting nothing. The preload's first-entry
read of a subspace is the one seek site that consults the manifest: a
segment whose recorded size fits one read-ahead warms in probe shape, since
each SST costs one request either way and the probe's request brings the
whole segment in and admits it, where a seek would fetch one block per
subspace per SST and leave the rest cold; a larger or unrecorded segment
seeks.

Bulk and range-probe scans use fixed 8 MiB read-ahead with 32 fetches in flight,
sized for a remote object store. A third shape, streaming, serves a
sequential consumer that derives as it goes — the staged index build's
inline sources — and reads ahead 256 KiB with two fetches in flight: a
round trip's worth of blocks ahead of the cursor, admitting nothing, and
holding at most about half a mebibyte per SST the iterator is positioned in
rather than the bulk shape's window. Its previous setting of one block was
one object-store round trip per 4 KiB.
Non-unique equality probes have their own admitting shape: 1 MiB read-ahead
and two fetch tasks per SST. Prefix bounds already restrict small keys to
their blocks; the smaller window bounds buffering while retaining coalesced
reads for high-fanout keys. Range probes and preload scans keep the larger
window. A one-block equality window was rejected because a 131,072-match key
went from 3 to 420 GETs in the real-SlateDB fixture.
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
- `CACHE_MEMORY` — one memory budget across lookup retention, auxiliary metadata,
  and the block cache. Within the block share, SST metadata and data blocks
  are separated by eviction priority; an attach whose store
  outgrows the protected share says so. Unset:
  what SlateDB gave a single store, now for the whole process. Never
  inert — the memory tiers exist without a `CACHE_DIR`.
- The disk device is cut into at most 64 blocks (never smaller than
  foyer's 16 MiB), so a store holds at most 64 open files however large its
  `CACHE_SIZE`. At foyer's fixed block a 16 GiB device was 1,024 files per
  store, and a process attaching many stores ran into its descriptor limit
  before its memory.
- `CACHE_PUTS` — the flush insertion policy: SST metadata and data blocks
  enter decoded on write by default, with
  or without a disk tier. `CACHE_COMPACTION_PUTS` separately controls merge
  output admission and defaults on, so a rewritten index is resident when
  the merge lands. Either kind remains byte-bounded.
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
  read triggers it. `ReadOnlyCatalog::warm_tables` reads, in seek shape,
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
memory allowance follows demand, and the executor stays process-wide.
Disk is a ceiling per store, not a reservation — a store's device takes
`CACHE_SIZE` as its cap and fills only with what that store reads.

The SST block size is fixed at 4 KiB. Read-ahead removed the per-block network
round trip from scans, while the measured probe shape is zero bytes warm and
62 KB cold, so there is no remaining evidence for a 4/16/64 KiB sweep or an
attach option. A future profile that puts material weight on block grain must
change this binding choice and add the option and the scan/probe benchmark
together.

Two more writer settings leave SlateDB's defaults, for the same reason the
block size is fixed: they are what the remote-store read profile needs, not
per-attach policy.

- **Every SST carries a bloom filter** (`min_filter_keys` is 0, not 1000).
  SlateDB skips the filter on an SST under the threshold, and moraine's
  SSTs are routinely that small — a commit's L0 flush, a merge of a
  sparsely written subspace — so an absent-key probe, the read the filters
  exist for, was reading such an SST's data blocks to learn "not here". The
  in-process compactor writes with the writer's settings, so a merge keeps
  the filters; a standalone compaction worker, which moraine does not run,
  would have to be configured to match.
- **A writer polls the manifest every 5 s** (not every 1 s). The poll is
  one object-store GET per open writer, and it exists to notice fencing and
  compacted state, neither of which a catalog writer needs within a second.
  A reader's cadence is its own attach option and is unchanged.

Non-unique equality probes additionally use a bloom filter over the complete
encoded indexed value, excluding the row-id suffix. The extractor recognizes
the terminated storekey byte frame, so point keys and equality-prefix scans
extract identical prefixes; shorter range prefixes cannot use this filter.
Writers and readers register both the legacy whole-key policy and the named
`moraine-index-equality-v1` prefix policy. Key bytes do not change. Existing
SSTs keep their whole-key filters; new flushes and compactions add the prefix
filter, and older readers safely ignore the additional named policy.

Not changed: the WAL-flush bound on a memtable's life
(`max_wal_flushes_before_l0_flush`) stays at SlateDB's 4096, its floor —
SlateDB refuses a lower value at open — and SST compression stays off, since
turning it on is a format change with cross-version consequences that this
profile does not weigh.

Two losses, taken knowingly. Part-grain prefetch: replaced by the scan
path's own read-ahead (the measured fix for the 277 s materialization in
`BENCHMARK.md`), with admission per the shape rule. And a `CACHE_DIR`
shared between concurrent processes: a foyer device has one owner at a
time — the deployed topology never shared a directory across hosts.

### Ordinary scans use DuckDB; exact indexed scans use scoped readers

Ordinary lake scans never send data-file bytes through moraine and add no
data-page cache or read-through layer: a lakehouse's query data path belongs
to the engine reading it, and duplicating DuckDB's would be the mistake this
RFC removes from the catalog tier.

Exact paired-location indexed joins are the exception: their summary-driven
scan opens the core's projected position reader during execution. It uses the
existing bounded auxiliary byte/page-index cache shared with located reads and
index upkeep, rather than buffering the query's result during binding. DuckDB
still executes the join and predicates. Broad or unsupported query shapes keep
the ordinary scan. The auxiliary cache and DuckDB's external-file cache remain
separate budgets and can contain overlapping file bytes.

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
their position column. An object recorded at or below 1 MiB — every delete
file, and DuckLake's small per-insert data files — is instead fetched whole on
its first touch and held as one block, from which its footer, page index, and
column ranges are all cut: a delete file's positions cost one request rather
than a footer request and a dependent column request, and a small target's
selected pages are resident by the time its selection is known. A
single-touch read of a small object takes only its ranges, since it would keep
nothing. A process-wide byte-bounded cache retains the parsed
footer and page indexes for repeated touches of the same immutable file; it is
keyed by object-store identity, path, and file size, adds the page index to a
resident footer on demand rather than parsing the footer again under a second
policy, and holds only metadata. A file whose row ids are its recorded dense
range is remembered as a summary once its footer has proved that, so a repeat
location of it needs neither. Its default allowance is one eightieth of
`CACHE_MEMORY` (8 MiB at the default budget), deducted from the shared budget. Projected data-column ranges are retained under the same
allowance, keyed by the exact range a read asked for and admitted at low
priority so they leave before any footer or summary. A read declared
**single-touch** — an index build or a backfill, which touches each column
chunk once — fetches its ranges without admitting them: a build otherwise
writes roughly every byte it streams into the disk tier and churns the
footers and summaries sharing the allowance out of it. Commit-time upkeep is
not single-touch, because repeated deletes against one file re-read its
chunks. The footer and the summary are cached whatever the read declares.
This cache exists because DuckDB's metadata cache cannot be reached from the
direct Rust reader, not as a second copy of metadata DuckDB served to it.

**A scoped read's time is the store's.** On a simulated remote store at
30 ms a request, thirty-two positions spread over thirty-two row groups
spent 1.07 s fetching and 10 ms decoding; a bulk read of the same file
spent 1.11 s and 18 ms. Overlapping fetches with each other is therefore
what moves wall time — the same selection costs 597 ms across two read
units and 328 ms across four — while overlapping a fetch with a decode
cannot recover more than the decode, and is not worth a pipeline.
Decoding stays on the runtime's own workers rather than a blocking pool:
on a two-worker runtime a full-width fan-out decoding from a local store —
the adversarial case, since a remote read spends its time waiting — pushed
a one-millisecond heartbeat's worst lateness from 1.4 ms to 10 ms, a delay
rather than the stall the worker floor exists to prevent, and a blocking
task could not be cancelled when its cursor is dropped.

**Every data-file read is admitted against one process-wide bound.** The
paths that read one — scoped reads, delete files, footers, sidecars, and
the maintenance passes — each size their own fan-out, and nothing bounded
their sum: index upkeep alone asks for sixty-four files at once, a cursor
asks for up to eight read units, and a process holds a cursor per attached
catalog. Admission sits at the data store, which every one of those reads
passes through, and is sized well above the core count: these reads wait on
the store rather than on a core, so the number that matters is the requests
an object store serves at once. Its floor is the widest fan-out any single
path asks for, so admission bounds the aggregate instead of rationing a
path that already sized itself, and a cache hit takes no permit because
admission sits below the cache. The process's soft open-file limit caps it
last: reads may hold an eighth of that budget, leaving the rest for the
engine's own connections and the metadata store, and a limit low enough to
narrow admission past the floor narrows it to eight rather than closing it.
A platform that will not report the limit leaves the cores term standing.

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

Use real SlateDB on in-memory object storage. Cover consistent reads across
commits and maintenance; checkpoint publication after WAL durability; lease
ownership, renewal, expiry, and release; coalesced commit counters; staged
read-your-writes; cancellation; migration refusal; and typed errors after an
unknown commit outcome.

Corrupt unrelated records to prove scoped operations never decode them. Check
that dropping an explicit snapshot releases its decoded objects, repeated
metadata reads retain no catalog result cache, and pages remain bounded.
Measure fresh-process DuckDB peak RSS, latency, cache activity, and object-store
traffic as catalog size grows, including a single large table. Keep fixture
creation outside the measured process and report cache settings and remaining
array-based paths alongside the results.

## Alternatives considered

- **Per-get consistency:** independent gets can observe torn metadata across
  commits. Fixed read scopes preserve one revision across calls.
- **Retained decoded catalog maps:** rejected. They make memory proportional
  to catalog size and duplicate Foyer's reuse layer.
- **A permanently pinned following reader:** cannot observe new commits.
  Explicit checkpoint attaches support this intentionally; ordinary readers
  rotate between transactions.
- **Reader-created latest checkpoints:** can omit WAL-durable commits.
  Readers copy checkpoints published by the writer instead.
- **Cross-process invalidation messages:** unnecessary with published
  checkpoints and independent acquisition.
- **Using catalog history as the store snapshot:** insufficient for
  unversioned metadata and head-preserving maintenance changes.

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

## Lookup retention within the process budget

File/inline lookup directories and revision-scoped probe, positioning, location,
and deletion-ledger results share a process-wide lookup allowance. It takes one
eighth of `CACHE_MEMORY`, deducted from the SlateDB block allowance; auxiliary
metadata keeps its configured share. The first attach fixes all three shares.
Default sizing is 80 MiB for lookup retention, 8 MiB for auxiliary metadata, and
552 MiB for SlateDB blocks and metadata.

Each cache keeps its existing local byte and entry ceilings. A global reservation
also covers retained values, dynamic keys, reference-count bookkeeping, and
allocated cache containers, including unused capacity. A failed reservation
bypasses retention; it cannot fail a query. Container growth that cannot be
charged is discarded. This bounds persistent estimated cache allocation, not the
transient peak while constructing a value or growing a container.

An admitted directory or memo result carries its reservation through reader
clones. Eviction and catalog closure release only the allocations they actually
stop retaining; an active reader releases the value charge when its last clone
drops. Cached keys and containers are charged separately from values. Pressure
may evict the admitting cache's older entries; one catalog does not lock and
scan another catalog's caches to obtain a grant.

`CacheStatus` adds process-wide lookup capacity and occupancy. The SQL
`moraine_cache_status()` result appends matching columns. Existing C status
structs retain their layout; a separate status function supplies the new fields.

This is an estimated retention budget, not an RSS limit. Uncached or oversized
query results, copies returned by public Vec APIs, snapshots, staging, per-file
build buffers, DuckDB allocations, and allocator overhead remain working memory.
Inner shared buffers can outlive a directory independently of its outer reader
handle. These allocations must not be described as covered by cache eviction.

A resident full row summary replaces a separately cached membership-only entry.
Membership fills recheck after completion so a full summary built during the fill
does not leave a redundant retained representation. Active readers keep their
existing membership allocation; oversized full summaries do not displace useful
membership entries when they cannot enter memory.

### Working allocations

Cold head-view loads share one materialization per handle. Time-travel views stream
history records instead of retaining an intermediate history vector. Cache-only
query copies reserve result bytes before allocation. Live snapshots, bridge-owned
results, byte/Arrow buffers, staging, and translation use separate working-memory
admission. Legacy caller-owned vectors require caller reservations. These controls,
publication progress, and host sizing are specified in
[RFC 0026](0026-working-memory-and-resumable-publication.md).

Internal index builds and certification commits load operation-specific table
premises, then fold the batch's pending writes into that projection. Creating an
index also reads table and index names in its schema to preserve name checks.
Backfill and row-location reads omit unrelated tables and file-column statistics;
their operation-owned file inventories remain proportional to the addressed
table. The explicit public closure-commit API still materializes its premises.


### WAL replay admission

Read-only SlateDB memtables retain rows replayed from the WAL independently of
Foyer's SST block cache. The WAL-reader adapter admits each returned batch to the
process-wide working budget before copying it into compact shared bytes and
handing it to SlateDB. The reservation follows key and value slices through
memtables and returned values. Its estimate is twice the payload plus 320 bytes
per row for replay and memtable bookkeeping. Overlapping readers compete for
that same budget; an admission failure rejects the new replay without evicting
an existing reader's state.

This bounds admitted retained bytes, not process RSS or WAL decoding itself.
SlateDB produces one batch before the adapter can inspect its size. Native WAL
prefetch is limited to 512 KiB, two tasks, and 256 KiB read-ahead targets; these
are soft block-fetch limits. A large batch, custom WAL provider buffers, allocator
overhead, and DuckDB allocations remain outside this estimate. Compacting the
batch adds a replay copy but prevents surviving slices from pinning unaccounted
WAL blocks. This does not introduce a decoded catalog-object cache.

Publishing checkpoints whose data is already in SSTs avoids replay copies and
uses Foyer's normal demand-loaded path. The publication cadence and flushing
policy remain separate from admission: current publication still covers every
acknowledged durable commit and can require WAL replay.

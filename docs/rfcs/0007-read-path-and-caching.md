# RFC 0007: Read path, caching, and memory

## Summary

Every read runs at one fixed store revision and decodes only the records it
asks for. Writers read through a SlateDB snapshot. Independent readers pin
checkpoints the writer publishes at durable boundaries, and write nothing.
Reuse lives in one foyer block cache per store under one process budget, with
SST metadata protected by eviction priority. Row summaries are published as
immutable sidecars beside embedded-row-id data files. Live allocations are
admitted against a separate working-memory allowance, and refusal is an error.

## Read revisions

A transaction reads one consistent store revision across every call it makes.
The revision is fixed when the read scope opens and never changes while the
scope lives. No decoded catalog is retained between operations: each lookup
reads its requested records, decodes them, and releases them with its result.
Reuse of fetched bytes is the block cache's job, described below.

**Writer scopes.** A read-write handle reads through a SlateDB `DbSnapshot`
taken at scope open. The snapshot pins the writer's view without loading any
catalog entity. A staged transaction overlays its pending rows on that fixed
view, and coalesced commits fold earlier members' pending writes into each
member's premises. Nothing a committer reads is cached past the operation that
read it.

**Reader scopes.** An independent reader cannot take a `DbSnapshot`, so it
reads through a `DbReader` fixed at a checkpoint. The writer publishes that
checkpoint and readers pin it. `crates/moraine/src/store/checkpoint.rs`
publishes and pins. `crates/moraine/src/catalog/handle/reader_scopes.rs`
shares the fixed readers across transactions. The handle's own `DbReader`
follows the latest manifest without a checkpoint of its own, polling at
`reader_poll_interval`; it serves head reads only, and the collector's age
threshold protects what it references between polls.

Publication works as follows.

1. After a commit group's writes are WAL-durable, and before any member is
   acknowledged, the writer calls
   `Db::create_checkpoint(CheckpointScope::Durable, ...)` under the name
   `moraine-read-head-v1`.
2. The publisher refuses to publish a sequence the store has not yet made
   durable. One publication may cover several groups whose writes are already
   durable. Each acknowledgement waits for a checkpoint covering its own
   group.
3. Publications with an older manifest are retired after the new one exists.
   Retirement gives a publication an expiry of `READ_GRACE`, fifteen minutes,
   and deletes one already past its expiry. The current publication never
   carries an expiry, so readers acquire it while the writer is idle or
   offline. `READ_GRACE` must exceed the longest explicit transaction, as the
   retention window must. Files and log slots a retired publication
   references stay until it expires.
4. A WAL-durable write whose publication fails is potentially committed. The
   outcome is reported as unknown under [the commit
   protocol](0003-commit-protocol.md). It is never treated as a conflict that
   permits replaying the mutation.
5. A clean writer shutdown flushes memtables and republishes the resulting
   manifest even when its sequence was already published, so an idle reader
   does not keep replaying a WAL-heavy publication after the same rows have
   reached SSTs.

A detached checkpoint created from the latest manifest is never used for
writer publication. Such a checkpoint does not advance the manifest's WAL
boundary, so a following reader can already see durable commits it omits.

<!-- site:omit -->
Under the [commit-log seam](0011-commit-log-seam.md), a committer that holds
the writer publishes after replaying its tail. Another committer flushes,
opens a `Publisher` on the recovered store, and publishes through the durable
sequence covering its slot. The `Publisher` serializes publication within a
process and coalesces writes covered by one checkpoint.
<!-- /site:omit -->

Each component owns one layer of state and nothing above it.

| Owner | Holds |
|---|---|
| The block cache | SST metadata and blocks, shared across handles and reader generations |
| A read scope | its `DbSnapshot` or pinned checkpoint reader |
| One operation | its decoded records and output page |
| DuckDB | native catalog entries and query execution state |
| Lookup facilities | bounded row directories and probe memos under the lookup allowance |

Reader acquisition works as follows.

1. A new transaction shares the handle's latest reader without a store
   request when the publications were listed less than `reader_poll_interval`
   ago, the reader has at least half of `READ_GRACE` left, and its deadline
   has not passed. The interval is therefore how stale a reader may be; zero
   lists on every transaction.
2. Otherwise the reader lists published checkpoints and takes the one with
   the highest manifest id. If none exists, acquisition fails with
   `SnapshotExpired`, which tells the caller to reopen the writer so it
   publishes recovered progress.
3. If the latest reader is at that manifest, the listing confirms it: the
   deadline moves to the listing time plus `READ_GRACE`, or to the
   publication's own expiry when it carries one, less a one-minute allowance
   for clock skew. The transaction shares the reader.
4. Otherwise the reader opens a `DbReader` pinned to the published checkpoint
   id, with its deadline set the same way. The open writes nothing. If the
   publication expired during that step, acquisition retries discovery, up
   to `MAX_COMMIT_ATTEMPTS` times, ten, then fails with
   `RetryBudgetExhausted`.
5. Every point read and every scan yield checks the deadline through
   `CheckpointReader::reader`, which refuses a reader past it with an error
   telling the caller to restart the transaction. A transaction therefore
   cannot outlive `READ_GRACE`.

At most `MAX_READER_GENERATIONS` reader generations, eight, may be live in one
handle. Acquiring another is refused with `Constraint` until an older
transaction or cursor finishes. An unshared older generation is retired before
its replacement reserves replay memory. A new generation never changes what an
existing transaction sees. An explicit checkpoint attach, named through the
`checkpoint` attach option, uses an external reader that no deadline governs.

SlateDB checkpoints protect store files only. DuckLake's external Parquet
files are retained under [maintenance](0010-maintenance.md), which is a
separate contract.

**Scopes and cursors.** `ReadOnlyCatalog::read_scope()` opens a `ReadScope` at
the latest committed snapshot. `read_scope_at(snapshot)` opens one at a
retained historical snapshot. Both return fixed, demand-loaded views:
unversioned options and statistics are fixed by the same scope, including
during time travel. There is no eager whole-catalog entry point on the read
path. A `CatalogSnapshot` is an explicit materialization a caller asks for and
pays for under working memory.

A `CatalogCursor` streams records of one entity kind in encoded-key order,
current records followed by ended ones, applying begin and end visibility at the
selected snapshot. Across the C ABI a metadata cursor holds one page of at most
`PAGE_RECORDS` records, sixty-four, or `PAGE_BYTES`, one mebibyte, whichever
fills first. Freeing the page releases its rows and their working-memory
reservation. Cancellation or a failed read invalidates the cursor, and a reader
past its deadline rejects its prefetched entries. The DuckDB metadata tables all
stream through this cursor, so two scans of one table inside one transaction
agree even when another connection commits between them.

A cursor over the two snapshot tables opens at a floor, the first snapshot id
it reads, which the shim pushes from a `snapshot_id` comparison against a
constant, inside a staged transaction as well. DuckLake's commit-time conflict
check bounds both of its arms by the transaction's start snapshot, and the
shim answers DuckLake's head lookup from the stamp the metadata transaction
pinned, so neither reads every retained snapshot.

## Snapshot expiry and consistent operations

**Expiry.** A `ReadScope` at snapshot `S` names object-store files. Once `S`
falls below the retention horizon, maintenance may schedule those files for
deletion, so the view is safe for data access only while `S` is retained.
Snapshot ids are sequential up to the head, so a requested snapshot at or
below the head with no stored record is an expired one. Opening a scope at
such a snapshot fails with the typed `SnapshotExpired` error, and the caller
re-resolves from head. A reader never dereferences a reclaimed file silently:
either `S` is retained or the open fails.

DuckLake holds one catalog snapshot per transaction, resolved on first catalog
access and kept for the transaction's life. In autocommit mode that is one
statement. Inside `BEGIN ... COMMIT` it is the whole block, which the user
controls. The retention window must therefore exceed the longest open explicit
transaction. Sizing it to statement length is wrong.

**Head-stamp guard.** Equality, `IN`, range, and `NULL` index lookups, full
inline scans, and requested inline-row lookups each read many keys and must
see one revision. On a manifest-following handle, `store::read::consistent`
records the full head stamp before the operation and compares it after. An
error is returned only after that comparison, because a missing chunk during a
pass the head moved under is a torn read and not stored corruption. A pass
whose head changed is rerun, up to `STABLE_READ_ATTEMPTS` passes, eight. If
every pass changes, the operation fails with `RetryBudgetExhausted`. It does
not fall back to an unguarded scan. On an isolated handle, a `DbSnapshot` or a
checkpoint reader, the operation runs once. The guard covers a single call and
nothing wider.

**Store facts that survive invalidation.** Decoded head state is keyed by the
head stamp and replaced when the stamp moves. Three facts in
`crates/moraine/src/catalog/projection.rs` instead describe the store and
persist across invalidation, each resting on a monotonicity the store
enforces.

| Fact | Rule |
|---|---|
| Format floor | `sys/format` is forward-only. A version once observed is a permanent lower bound, seeded at open and raised when a landed batch carries a stamp. A commit at or below the floor reads no stamp. |
| Migration-clear head | The exact head stamp at which `sys/migration` was observed absent. Every landed commit restamps it. |
| Inline chunk-directory completeness | Per table, set once the directory has been verified against a full chunk walk. It stays set only under a format that excludes writers predating the directory. |

**Current-only narrowing.** Every DuckLake read of a versioned table carries
`begin_snapshot <= S AND (end_snapshot > S OR end_snapshot IS NULL)`. A record
in `history` always carries an `end_snapshot` at or below the head. A reader
keeping rows only while `S < end_snapshot` matches nothing in `history` once
`S` has reached the head, and a filter of `end_snapshot IS NULL` standing
alone matches nothing in `history` at any head. A scan whose caller has shown
one of those shapes reads `current` alone, `Versions::Live`. Any other scan
reads both subspaces, `Versions::LiveAndEnded`. The bound crosses the ABI as a
snapshot id, and the core compares it against the head inside the same
consistent read that serves the scan. The shim recognizes the filter shape and
consumes nothing: DuckLake keeps applying its filter, so an over-generous
recognition costs a wider read and never a wrong row. The narrowing applies
only inside a staged transaction, whose read point is pinned. A plain reader
resolves its head per scan and does not narrow.

## Block cache

**Instances and budget.** The block cache is one foyer instance per store,
shared by every `Db` and `DbReader` opened on that store in the process.
`CACHE_MEMORY` is one memory budget for the process, split once at the first
attach. The first store to open settles the budget, the disk directory, and
the shares. A later attach asking for different values is sized by what is
settled.

| Share | Size | Default at 640 MiB |
|---|---|---|
| Lookup retention | one eighth of the budget | 80 MiB |
| Auxiliary metadata | one eightieth, or `CACHE_AUXILIARY_PERCENT` up to 50 | 8 MiB |
| Blocks and SST metadata | the remainder, shared elastically by stores | 552 MiB |

The default budget without `CACHE_MEMORY` is 640 MiB. Within the block share
an empty attach reserves nothing. Read fills, disk promotions, and admitted
flush or compaction output grow a store's allowance under pressure: first from
unassigned bytes, then by borrowing unused allowance from other stores, then
by reclaiming a fair share from larger borrowers. Transfers happen under one
process lock, shrinking donors before growing the recipient, and the sum of
store capacities never exceeds the block share. A store's final detach
releases its memory allowance. Its disk device stays open and keeps its
persisted blocks.

**Metadata protection.** SST indexes, filters, and statistics live in the same
cache as data blocks. Metadata is inserted with `Hint::Normal` and data blocks
with `Hint::Low`, and foyer's LRU drains the low-priority list before touching
the high-priority one. Metadata is evicted only when no data block is left to
evict. `METADATA_PRIORITY_POOL_RATIO`, 0.9, is the most of each of the eight
shards that metadata may hold under protection. It is a ceiling on protection
and never a reservation: a data block is admitted whatever the ratio says, and
metadata beyond the ratio is demoted to compete on recency. The eviction
policy must be LRU, because the priority hint is the only thing separating the
two kinds and foyer's other policies ignore it. SlateDB does not expose an
entry's kind on insert, so the kind is recorded on the typed index, filter,
and statistics fetches, and an untyped insert is treated as a block. Every
attach compares the store's recorded filter, index, and statistics bytes
against the protected capacity in force and warns when they do not fit.

**Disk tier.** With `CACHE_DIR` set the cache is foyer's hybrid: the memory
tier spills at block grain to a device at `CACHE_DIR/<store>/blocks`, where
`<store>` is `v2-` followed by a hash of the cache identity and the store
path. Entries are written on insertion and recovered on the next open. SlateDB
scopes cache keys per opened handle from a process-local counter that follows
attach order, so a shared device could alias one store's recovered WAL block
for another after a restart in a different order. One device per store makes
that impossible. A store deleted and recreated at the same location restarts
its WAL ids, so its cache directory must be removed with it. The device is cut
into at most `DISK_CACHE_BLOCKS` files, sixty-four, each at least 16 MiB, so a
process attaching many stores does not exhaust its descriptor limit.
`CACHE_SIZE` caps the device, defaulting to 16 GiB, and the device fills only
with what the store reads. The cache builds its own runtime for foyer's
background tasks, so no attach's runtime is borrowed and later dropped under
them.

**Auxiliary cache.** The scoped Parquet reader keeps a separate foyer hybrid
at `CACHE_DIR/auxiliary-v2`, holding parsed footers, page indexes, and row
summaries keyed by cache identity, path, and recorded file size. A resident
footer gains its page index in place. Footers and summaries are admitted
whatever the read declares. Projected column ranges are admitted at low
priority, so they leave before any footer or summary. A read declared
single-touch, an index build or a backfill, fetches ranges without admitting
them. An object recorded at or below `WHOLE_OBJECT_READ_BYTES`, one mebibyte,
is fetched whole on first touch. Every data-file read passes through one
process-wide admission at the `DataStore`: eight permits per core, clamped
between `WIDEST_PATH_FAN_OUT`, sixty-four, and 256, capped at one eighth of
the soft open-file limit and never below eight. Background reads may hold half
of it.

**Cache identity.** `CacheIdentity` names the object namespace a cache entry
belongs to. Equal identities assert that equal object paths name the same
immutable contents. `CacheIdentity::new(namespace)` derives a stable identity
from a string that must include the backend, endpoint, bucket or filesystem
root, and any wrapper prefix. `CacheIdentity::local(store)` names a local
store by its resolved root. `Default` yields a random identity, which is what
in-memory attachments and opens without an identity receive. Credentials are
excluded so rotation preserves reuse. Display strings never determine
equivalence.

**Admission by read shape.** Every read path names a `ScanShape`, which fixes
its read-ahead and its admission. Foyer's admission picker repeats the rule at
the disk device.

| Shape | Read-ahead | Fetches in flight | Admits blocks | Used for |
|---|---|---|---|---|
| `Bulk` | 8 MiB | 32 | no | whole-subspace walks, cascades, expiry, backfills |
| `Probe` | 8 MiB | 32 | yes | targeted lookups and range probes |
| `Streaming` | 256 KiB | 2 | no | a sequential consumer deriving as it goes |
| `Metadata` | 256 KiB | 2 | yes | a table's metadata reads |
| `Equality` | 1 MiB | 2 | yes | one equality prefix |
| `Seek` | one block | 1 | yes | one or a few entries from a wide range |

`Seek` exists because SlateDB spawns read-ahead fetches when an iterator opens
and does not cancel them on drop, so a read-ahead shape dropped after one
entry keeps fetching. Admission changes neither visibility nor completeness.

**Adaptive split.** A SlateDB iterator is one sequential cursor however far it
prefetches. A scan over a kind whose record count scales with the data
(columns, data files, delete files, file and table column statistics) walks as
one iterator until it has streamed one read-ahead's worth of bytes, then seeks
the kind's highest key, divides the id bytes between the last key seen and
that key evenly, and reads the remainder as `SCAN_SPLIT` concurrent
sub-ranges, eight. Sub-ranges are concatenated in range order, so a split scan
yields the same records in the same order as one iterator. A catalog that fits
one read-ahead costs one iterator.

**Store settings.** The SST block size is 4 KiB. Every SST carries a bloom
filter (`MIN_FILTER_KEYS` is zero), because a commit's L0 flush is routinely
under SlateDB's default threshold and an absent-key probe is the read filters
exist for. Non-unique equality probes also use the named prefix filter policy
`moraine-index-equality-v1` over the encoded indexed value without the row-id
suffix. A writer polls the manifest every five seconds. These are
implementation constants and have no attach option.

**Warming.** `CACHE_PRELOAD` warms inside the open: `CachePreload::L0` touches
every subspace far enough to pull its SST metadata, and `CachePreload::All`
additionally walks the scan-shaped subspaces whole. Neither walks the `index`
subspace's data. `ReadOnlyCatalog::warm_tables` reads, in `Seek` shape, the
first entry of each named table's index and inline ranges, eight ranges at a
time process-wide. The extension runs it after a commit for the tables the
commit wrote, and with `CACHE_PRELOAD` set, for every table in the background
after the open.

**Lookup retention.** Row-lookup directories and revision-keyed probe memos
share the lookup allowance. Directories additionally keep local ceilings of
`DIRECTORY_CAPACITY` tables, 256, and `DIRECTORY_BYTES`, 64 MiB, per source
kind and catalog, and the index probe memo holds 8 MiB. Charges cover retained
summaries, file metadata, path strings, entry headers, keys, and container
capacity. They are estimates, and shared allocations are charged
conservatively. A failed reservation bypasses retention and cannot fail a
query. An oversized directory serves its query and is not retained. An active
reader keeps a value and its charge until its query finishes, after eviction.
`ReadOnlyCatalog::projection_bytes` reports the estimated bytes these
facilities hold.

**Attach options.** `CACHE_DIR`, `CACHE_SIZE`, `CACHE_MEMORY`,
`CACHE_AUXILIARY_PERCENT`, `CACHE_PUTS`, `CACHE_COMPACTION_PUTS`, and
`CACHE_PRELOAD` are the cache's attach options. Their tuning is documented
with the [extension surface](0006-extension-surface.md) and the README.
`moraine_cache_tally`, `moraine_cache_status`, `moraine_store_census`, and
`moraine_object_store_tally` report hits and misses per tier, capacity and
occupancy, recorded SST metadata bytes, and object-store requests.

**Lake data.** Ordinary lake scans never send data-file bytes through moraine.
Data bytes are held in DuckDB's external-file cache under its `memory_limit`.
The scoped Parquet reader exists for equality-index upkeep and located reads,
which bypass DuckDB's reader, and it never retains data pages past the
auxiliary allowance. Every registered file read carries DuckLake's recorded
file size and footer size in its `ParquetFile`, so the first request on a
large object fetches the serialized footer in one read. Data files then fetch
only indexed values and row ids, and delete files only their position column.
The two caches are separate budgets and may hold overlapping bytes. DuckDB's
own cache settings are global to the process, so the attach sets none of them.

## Published row summaries

A data file's row summary is its exact row-id membership and the position of
each id. For a file whose ids follow from `row_id_start` and its record count,
the summary is derived from catalog metadata with no read. For a file carrying
embedded row ids (flush outputs, update rewrites, compaction outputs that
preserved ids), it is derived by reading the file's row-id column. A published
summary makes that derivation happen once per deployment.

**Placement.** The sidecar is the data file's path with `.rowsum` appended,
under the same prefix and credentials. It is never named `.parquet`, because
DuckLake's orphan cleanup deletes any `.parquet` object its catalog does not
know.

**Format.** The sidecar is the auxiliary cache's disk-tier encoding, so a
fetch fills the disk tier by copying.

| Part | Contents |
|---|---|
| Header, 64 bytes | magic `MORAROWS`, version 1, `table_id`, `data_file_id`, `file_size`, cardinality, set length, order length, path length |
| Path | the data file's path as the reader resolved it |
| Set | a tag byte, then `FileRowSet::Range`, `Roaring`, or `Sorted` |
| Order | nothing for `RowOrder::Ascending`, the rank-to-position permutation for `Permuted`, and the physical row count, offsets, then positions for `Repeated` |

Positions are bit-packed at the width the physical row count needs, so an
order entry is addressable by arithmetic and a caller naming a few positions
reads a few bytes. A lookup fetches the header and set in one ranged read and
never fetches the order. The set must answer rank as well as membership, since
rank indexes the order.

**Identity.** A reader checks the header's `table_id`, `data_file_id`,
`file_size`, and path against the file it is describing. A mismatch, an
unknown version, a short read, or any decode error means absent: the reader
falls through. Data files are immutable, so a sidecar is never invalidated and
never required. `moraine_sidecar_tally` reports `SidecarTally` counts of hits,
misses, refusals, and publications.

**Producers.** A read never publishes. There are two producers, both driven by
the extension's background worker in
`crates/moraine-duckdb/src/runtime/background.rs`.

1. A backfill over the whole catalog at attach, which costs one 4 KiB header
   read per file already published.
2. A pass over the files a commit registered, queued from
   `CommitReport::registered_files` after the commit returns.

The process admits four commit passes and four backfills in separate pools.
Each pending lane holds at most `MAX_PENDING_PASSES`, 128. An attempt has a
five-minute deadline, excluding admission wait, and failures retry with
backoff from one second to one minute while attached. Detach cancels and joins
both lanes. Summary construction admits four builders process-wide,
`PUBLISH_CONCURRENCY`, of which at most two are background builds. Background
reads take at most half the data-store admission. A refused or skipped pass
logs a warning and leaves the file readable. The core exposes
`publish_row_summaries`, `publish_row_summaries_since`,
`publish_all_row_summaries`, `publish_selected_row_summaries`, and
`publish_selected_row_summaries_since` on `ReadOnlyCatalog` for explicit
publication. An encrypted lake publishes nothing, because moraine cannot read
an encrypted row-id column.

**Reclaim.** `ReadOnlyCatalog::sweep_published_summaries` lists the data path
once and deletes every `.rowsum` whose data file the same listing did not
find, reporting a `SidecarSweep` of considered, reclaimed, and failed objects.
It runs as a maintenance step after DuckLake's cleanup, which is what strands
sidecars.

**Read path.** A summary is resolved in strict fallback order.

1. The dense range from catalog metadata, with no read.
2. The cached summary: memory, then the auxiliary disk tier.
3. The sidecar: its set, plus only the order entries a caller naming positions
   asks for.
4. The data file's row-id column.

A `FileSummary` holds either membership alone or a `PositionedRowSet` with its
order, and `resolves_positions` says which. A lookup admits a membership-only
summary. A located delete or update needs positions and reads them from the
sidecar's order or the file. A full summary and a membership reader share one
immutable membership allocation. Concurrent publication of one file is safe,
because the contents are a deterministic function of the file, and in-process
publishers of one file are serialized on its key. Publishing neither reads nor
fills the summary caches, so a backfill does not evict the working set of the
process running it.

## Working memory

Cache allowances bound retention. `MORAINE_WORKING_MEMORY_BYTES`, default 1
GiB, is a separate process-wide allowance for live working allocations. Every
allowance in this section is admission accounting of estimated bytes,
calibrated to refuse a little early. None measures allocator overhead or RSS.

An owner reserves before a known allocation and holds the reservation through
use. A refused reservation is an error: refused staging or translation poisons
the transaction so an accepted prefix cannot commit, and results are never
truncated. `MemoryReservation::reserve` and `resize` take and adjust bytes.
`into_bytes` and `into_record_batch` transfer ownership to a `Bytes` value or
an Arrow batch, so clones, slices, nested buffers, and Arrow C exports hold
the allowance until the last owner drops. `working_memory_usage()` returns
reserved bytes and capacity. A caller materializing a `CatalogSnapshot` admits
its copies with `CatalogSnapshot::reserve_copy`. Legacy methods returning
plain `Vec` values leave reservation to the caller.

| Variable | Default | Scope |
|---|---|---|
| `MORAINE_WORKING_MEMORY_BYTES` | 1 GiB | live allocations, process-wide |
| `MORAINE_STAGING_MEMORY_BYTES` | 256 MiB | per staged transaction: operation vectors, cells, strings, schemas, Arrow bodies |
| `MORAINE_COMMIT_MEMORY_BYTES` | 256 MiB | per commit session: translation scratch and the write ledger, admitted before each store write |
| `MORAINE_STORE_MEMORY_BYTES` | 1 GiB | SlateDB's unflushed-bytes threshold per writer. A transaction starting at a full threshold is refused |
| `MORAINE_SUMMARY_MEMORY_BYTES` | 256 MiB, at least 64 | per summary build, admitting `bytes / 64` physical rows |
| `MORAINE_HOST_MEMORY_BYTES` | unset | enables the pre-attach host check |
| `MORAINE_HOST_HEADROOM_BYTES` | one quarter of the host value | native headroom in that check |

Staging and commit allowances are consumed alongside the shared working
allowance. Reservations follow coalesced members and commit submission through
acceptance. Submission transfers the recorded ledger without cloning it. A
refused write poisons the commit even when the caller ignores the immediate
error.

**Readers and Parquet.** Object-store range requests reserve their requested
bytes before fetching, and the returned bytes keep the reservation. A Parquet
reader reserves four times the largest selected row group's projected
uncompressed size before page decoding. Input ranges and output batches are
charged separately. That estimate can refuse a very large row group even for a
narrow selection. WAL replay for a reader generation admits each batch at
twice its payload plus 320 bytes per row before SlateDB copies it into a
memtable, and native WAL prefetch buffers at most 512 KiB.

**Exports and cursors.** Snapshot exports across the C ABI reserve the
selected descriptors, copied strings, and conversion scratch before allocating
and hold the allowance until the paired free call. Inline cursors admit their
materialized row selection before constructing it. Parquet row batches keep
their reservations through Arrow buffer ownership, including slices held after
the scan ends.

**Summary builds.** A build that exceeds its row allowance leaves the file
unpublished. Foreground reads then resolve positions by streaming row ids.
Blocking builders keep their permits and reservations until their synchronous
call returns, even after the waiter is cancelled.

**Publication progress.** `RowSummaryProgress::new(tables, since)` traverses
an attach or backfill selection with an exclusive snapshot cutoff.
`RowSummaryProgress::for_files(files)` publishes exact registered file ids
with no traversal. Each `advance` publishes at most four files and keeps its
snapshot between steps. Failed file ids stay resident up to `RESIDENT_FILES`,
1,024, and spill to an anonymous temporary file beyond that. Retry rounds
consume those ids and never rescan successful files. A cancelled step may
repeat its in-flight window. Dropping the progress value releases its snapshot
and spools.

**Release hook.** `set_memory_release(hook)` installs what the process does to
return freed pages to the kernel. The core calls it after a cold catalog view,
a durable commit, and a publication window, at most once per
`RELEASE_INTERVAL`, one second, on the thread that finished the phase. The
DuckDB extension installs glibc's `malloc_trim`. A platform with no equivalent
leaves the hook uninstalled, which skips the release.

**Host check.** When `MORAINE_HOST_MEMORY_BYTES` is set,
`validate_host_memory(duckdb_bytes, other_bytes)` runs before the store opens
and refuses the attach if DuckDB's managed limit, the working allowance, the
cache budget, one writer's store allowance, and the headroom together exceed
the host value. The embedder includes other instances and stores in
`other_bytes` and revalidates after raising DuckDB's limit. A process or
container ceiling remains the final boundary.

## Tests

- A reader scope opened after acknowledgement sees the commit, and an open
  scope keeps its revision across later commits.
- Publication fails when the covered sequence is not yet WAL-durable.
- A reader past its deadline rejects point reads and scan yields, and a
  listing that still shows its publication extends the deadline.
- A read-only attach writes nothing to the object store across open, several
  transactions, and a writer commit between them.
- A shared reader keeps its publication inside `reader_poll_interval` and
  moves to a newer one after it.
- A superseded publication carries an expiry and the newest carries none; an
  expired one is deleted at the next publication.
- A head change under a lookup rerun reaches `RetryBudgetExhausted` after
  eight passes without an unguarded fallback.
- A corrupt sidecar header, wrong identity, or unknown version falls through
  to the row-id column with the same result.
- A refused staging or commit reservation poisons the transaction and no
  prefix commits.

## Alternatives

- **Writer publication by detached checkpoint of the latest manifest.**
  Rejected: it omits WAL-durable commits a following reader already sees.
- **Reader-owned leases on the published checkpoint.** Rejected: a manifest
  write per reader generation and one per renewal, forced only by deleting
  the superseded publication at once. A grace on retirement lets readers pin
  the publication itself.
- **A fixed metadata slot.** Rejected: a fixed fifth of the budget was both
  floor and ceiling, so holding a large store's filters meant inflating the
  whole budget. Eviction priority lets idle metadata space go to blocks.
- **Retained decoded catalog maps.** Rejected: memory proportional to catalog
  size, duplicating the block cache's reuse layer.

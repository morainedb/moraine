# SlateDB upstream requests

This document collects changes moraine would like SlateDB to make. These are
not moraine implementation tasks: until a request lands in the SlateDB version
moraine pins, moraine keeps the documented limitation or fallback.

When an upstream change lands, update the owning RFC and add or adjust the
integration coverage.

## Point reads

### Batch exact-key reads through one read plan

SlateDB 0.15 exposes single-key `get` operations and range scans, but no
multi-get operation. A caller resolving hundreds of unrelated exact keys must
issue concurrent `get` calls, each of which independently enters the read path.
The caller cannot ask SlateDB to group those keys by level, SST, or block and
coalesce repeated lookup or fetch work.

Add a bounded batch point-read operation to `DbReadOps`, including support on
`DbTransaction`. One call should resolve every key against one consistent read
point, preserve each result's association with its input key, and group work by
level, SST, and block so colocated keys do not repeat the same lookup or fetch.
The transactional form must also see the transaction's own writes and retain
the isolation and conflict behavior of individual `get` calls.

Until that exists, moraine keeps a bounded window of `DbTransaction::get`
futures in flight for uniqueness enforcement. This hides remote latency but
does not reduce the underlying lookup amplification.

Owner: [RFC 0016](rfcs/0016-equality-indexes.md).

## Transaction commit memory

### Move the write batch and accept explicit conflict keys

SlateDB 0.15 clones `DbTransaction`'s complete `WriteBatch` at commit, then
materializes every batch key into a `HashSet` for conflict tracking. Large
equality-index commits therefore hold another complete batch plus one conflict
node per index key at their peak. Recent committed transactions can retain the
conflict set while an older transaction remains active.

Commit should move the batch out of the consumed transaction. The transaction
API should also accept an explicit write-conflict key set without first
materializing every batch key. Moraine serializes every catalog batch through
`sys/head`, so tracking index keys adds no conflict information.

Until upstream provides both operations, Moraine accepts SlateDB's default
all-write-key behavior and bounds commit peaks with equality-index entry and
encoded-byte limits.

Owner: [RFC 0016](rfcs/0016-equality-indexes.md).

## Cache identity

### Accept a caller-supplied stable cache scope

SlateDB wraps every shared `DbCache` with a scope id allocated from a
process-local counter. That prevents two open stores whose WAL SST ids overlap
from colliding during one process, but the counter resets on restart and its
values follow attach order. If two stores share one recovered Foyer device and
the next process attaches them in reverse order, a store can receive the other
store's recovered WAL block.

The scope should be supplied by the caller, or derived by SlateDB from a stable
store identity such as its object-store URI and database path. The identity
must be stable across processes and distinct for different stores using one
cache.

Until that exists, moraine keeps one Foyer device per store, so recovered keys
can only be that store's, and a re-attach hits only when the store takes the
same scope as before — the same attach order. A stable scope would let every
store share one device and hit regardless of order.

Owner: [RFC 0009](rfcs/0009-reader-consistency-and-caching.md).

## Cache management

### Export the SST id used by cache-manager operations

`DbCacheManagerOps::warm_sst` and `evict_cached_sst` are public, but their SST
id parameter is defined in a private module, so an external caller cannot name
an SST to call either operation.

Export the id type used by these methods. Moraine does not require it for
preload—subspace reads warm less data and match the catalog's access shape—but
the export would permit precise per-SST warming and eviction when an operator
needs them.

Owner: [RFC 0009](rfcs/0009-reader-consistency-and-caching.md).

### Name a cached entry's kind, and the level it came from

`CachedKey` and `CachedEntry` both keep their fields and accessors
`pub(crate)`, so a `DbCache` implementation cannot ask what it is holding. The
kind is knowable only from which trait method was called, and one method does
not say: `insert` carries data blocks a scan just read and, under
write-through admission, a fresh SST's index and filters.

Two consequences for a cache that holds both kinds in one store, as moraine's
does so that neither is bounded at a size an operator has to predict. Untyped
inserts must be guessed, and moraine guesses "data block" because admitting
scan traffic to the protected share would defeat the design; metadata that
arrives that way goes unprotected until its next typed fetch. And per-kind
occupancy and eviction figures — which is what says whether a store's filters
actually fit — have to be reconstructed by recording every typed admission in
a parallel index of keys.

Expose the kind. Either accessors on `CachedKey`/`CachedEntry`, or an
`insert_block` / `insert_index` / `insert_filter` / `insert_stats` family
matching the existing `fetch_*` methods, would remove both the guess and the
parallel index.

Separately, expose the SST's level. All of L0 is on the path of every point
read, because L0 is unpartitioned, while a sorted run contributes one SST per
probe — so L0 metadata is the highest-value content in the cache per byte and
is the one class worth pinning above all others. Nothing reaches moraine that
distinguishes them. (Acting on it would also need a third priority level from
foyer, whose `Hint` is `Normal` or `Low` and nothing else.)

Owner: [RFC 0009](rfcs/0009-reader-consistency-and-caching.md).

### Size the disk evictor's queue, and keep recency in process

The `FsCacheEvictor` takes its work over a `tokio::sync::mpsc::channel(100)`,
a literal with no setting behind it, and `try_send` drops the event when that
queue is full. A store serving a working set larger than its disk tier fills
it steadily: production runs report tens of thousands of dropped events per
30-second window, against a queue of 100.

Dropping a `Write` undercounts the directory, which is why
`max_cache_size_bytes` is a soft cap; a periodic scan reconciles it. Dropping
a `Read` is the one that costs reads, and no scan repairs it — the entry's
recency is simply never recorded, and the rebuild re-derives access times from
filesystem `atime`, which `relatime` mounts advance about once a day. The
evictor then picks victims from timestamps that no longer order anything, so
a hot part is as likely to be evicted as a cold one, and the next read of it
is an object-store fetch.

Make the queue's capacity a setting on `ObjectStoreCacheOptions`, and track
access recency in process — an in-memory timestamp updated on the read path,
rather than one recovered from `atime` — so a full queue costs accounting
accuracy and not eviction order.

Owner: [RFC 0009](rfcs/0009-reader-consistency-and-caching.md).

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
cache. Moraine can then enable Foyer recovery and make `CACHE_DIR` restart-warm
without risking cross-store reuse.

Until that exists, moraine opens Foyer with recovery disabled. The disk tier is
fully effective within one process, and `CACHE_PRELOAD` warms a restarted
process by re-fetching.

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

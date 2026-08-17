# RFC 0006: Extension surface

- **Date:** 2026-07-09

## Summary

Defines how the `moraine-duckdb` extension exposes the moraine core to DuckDB.
moraine is a **DuckLake metadata-catalog backend**: the extension registers a
DuckDB `StorageExtension` so DuckLake `ATTACH`es moraine as its catalog and
drives it with ordinary SQL, exactly as it drives a PostgreSQL or SQLite
catalog. moraine serves the `ducklake_*` metadata tables **row-faithfully**
from SlateDB — the tables *are* the catalog state (RFC 0002 encodes their
rows), not a re-modeled projection. Because DuckDB's stable C extension API
cannot register a catalog, the extension is a thin **C++ shim** linking
DuckDB's internals over a **C ABI** to the Rust core; all catalog logic stays
in `moraine`.

## Goals

- **DuckLake drives moraine unmodified.** moraine implements DuckLake's
  catalog contract and invents nothing (consistent with RFC 0004: moraine
  implements DuckLake's conflict model, it does not impose its own). Whatever
  SQL DuckLake issues against `ducklake_*`, moraine serves.
- **Thin extension, language-agnostic.** No DuckLake domain logic lives in
  `moraine-duckdb` — only `StorageExtension` registration, C-ABI marshalling,
  the sync↔async bridge, and (RFC 0021) sequencing calls to DuckLake's own
  maintenance SQL. Everything else is in the Rust core, testable without
  DuckDB (RFC 0001 Unit/Integration tests). The maintenance carve-out exists
  because the core cannot issue SQL at all, so no other layer can compose
  those calls; it is bounded to sequencing — the shim decides nothing from
  the results — and it is the one part of the shim that only e2e can cover.
- **Faithful catalog state.** The `ducklake_*` rows are the source of truth;
  moraine stores and returns those rows (B1). No semantic re-modeling that
  could drift from DuckLake's own reading of its tables.

### Non-goals

- **A standalone moraine attach as an end in itself.** `ATTACH '<path>' AS
  m (TYPE moraine)` ships and serves attach, listings, and every
  `ducklake_*` projection — it is the smallest real end-to-end proof of
  the shim/ABI/core stack, and every layer it exercises is on the DuckLake
  path anyway — but it is a verification surface, not a query surface: it
  refuses user-table data scans outright (below). The `moraine` core
  remains a standalone Rust library regardless.
- **Semantic projection of the catalog** (storing a re-modeled form and
  projecting it into `ducklake_*` on read). Rejected, not merely deferred;
  see Alternatives for the evidence and the one condition that would
  revive it.
- **The data-file read/write path.** DuckLake and DuckDB own object-store
  reads/writes of Parquet data files. moraine serves catalog *metadata* and
  the inlined-data tables (RFC 0005) only.

## Design

### Positioning: moraine is DuckLake's catalog

```
DuckDB engine
  └─ ducklake extension        planner, transactions, query execution
       └─ moraine catalog      DuckDB StorageExtension  (the extension surface)
            └─ moraine core     DuckLake catalog semantics on SlateDB  (Rust)
                 └─ SlateDB → object store
```

DuckLake stays the query/planner/transaction layer. moraine occupies exactly
the slot a PostgreSQL/SQLite/DuckDB catalog database occupies today: an
`ATTACH`-able catalog whose tables are the `ducklake_*` metadata tables. The
DuckLake specification requires the catalog to be a transactional SQL store
with primary-key constraints; moraine satisfies that contract over SlateDB.

### How DuckLake reaches moraine

moraine registers a `StorageExtension` under a catalog attach type so DuckLake
can point its metadata connection at it. The intended user surface:

```sql
ATTACH 'ducklake:moraine:<slatedb-uri>' AS lake (DATA_PATH '<object-store-uri>');
```

DuckLake connects to its metadata catalog by executing a literal nested
DuckDB `ATTACH` of everything after its `ducklake:` prefix
(`DuckLakeInitializer::Initialize` builds and runs the statement text;
backend dialects are a six-entry map keyed by the path's extension
prefix). So `ducklake:moraine:<slatedb-uri>` nests
`ATTACH 'moraine:<slatedb-uri>'`, resolved by DuckDB's ordinary attach
dispatch to moraine's registered storage extension — the same mechanism
`postgres:`/`sqlite:` ride, no DuckLake-side hook. moraine therefore
accepts the `moraine:` path-prefix form alongside `TYPE moraine`. Absent
from DuckLake's dialect map, moraine is spoken to in the **default
dialect**: plain DuckDB SQL, native types, no wrapper calls.

**How the prefix names and nests the attach** is settled, and settled by
observation rather than by reading DuckLake's source: DuckDB's `QueryLog`
records every statement executed on the instance, DuckLake's internal
metadata connection included, so the generated text is captured from a
running session and pinned (`tests/ducklake_load/wire_contract.rs`). For
`ATTACH 'ducklake:moraine:<uri>' AS warehouse`:

```sql
ATTACH OR REPLACE 'moraine:<uri>' AS "__ducklake_metadata_warehouse" (HIDDEN true)
SELECT NULL FROM "__ducklake_metadata_warehouse"."main".ducklake_metadata LIMIT 1
```

Three consequences. The nested catalog's **name derives from the outer
alias** (`__ducklake_metadata_` + alias), so it collides with nothing and
moves with a rename of the lake. It is **addressable**: `SELECT * FROM
__ducklake_metadata_warehouse.main.ducklake_snapshot` works, which is how
the e2e suite inspects what DuckLake wrote without going through
DuckLake's reader. And `HIDDEN true` keeps it **out of
`duckdb_databases()`**, which lists only the outer catalog — whose `path`
column is exactly the nested attach string. The pin is re-verified on
every DuckDB/DuckLake bump; nothing in moraine depends on the naming
scheme, but the e2e suite's whole verification surface does.

### Store URIs and credentials

The `<slatedb-uri>` selects moraine's object-store backend:

- **Local filesystem** — a path (`/var/lib/lake`): the catalog is a directory,
  created if absent.
- **In-memory** — `memory://`: an ephemeral store, for tests and scratch work.
- **S3 / S3-compatible** — `s3://<bucket>[/<prefix>]`: the catalog lives in
  `<bucket>` under the optional key `<prefix>` (empty places it at the bucket
  root). The bucket must already exist; moraine writes keys into it and never
  creates the bucket.

For an `s3://` store, credentials resolve through DuckDB's secret manager:
moraine looks up the `s3`-type secret whose scope matches the `s3://` path —
one created with `CREATE SECRET (TYPE s3, KEY_ID …, SECRET …, REGION …[,
ENDPOINT …, URL_STYLE 'path', USE_SSL false])`, the same secret DuckLake and
httpfs consult for `DATA_PATH`. Fields the secret leaves unset fall back to the
`AWS_*` process environment. No credentials appear in SQL text or ATTACH
options.

**The standalone attach is a metadata-only surface.** Table *data* is
served through DuckLake, which owns delete-file merging, row lineage, and
pushdown; a standalone data scan re-implementing that read path would
silently return deleted rows once merge-on-read exists. Standalone
`TYPE moraine` therefore serves listings, `DESCRIBE`, and the `ducklake_*`
projections (below), while user-table scans bind normally (so `DESCRIBE`
and `EXPLAIN` work) and raise a redirect error naming the
`ducklake:moraine:` attach at execution time. No opt-out option exists.

**Metadata projections.** Every `ducklake_*` table the keyed store models
is queryable through the attached catalog, served row-faithfully — `current`
and `history` rows both, since DuckLake filters lifecycles in SQL; unversioned
kinds serve current values. DuckDB's executor plans joins over per-table
scans. This row-faithfulness is what makes **time travel** work with no
time-travel logic in moraine: `AT (VERSION => N)` is DuckLake filtering the
served rows by begin/end snapshot — reconstructing past *schema* from the
`ducklake_column` versions as readily as past data — and it is verified live
across inline inserts, schema evolution, and flush (`ducklake_load.rs`). `ducklake_metadata` is synthesized from store facts (format
version, options) so DuckLake's exists-probe and version reads succeed on
any initialized moraine store: a moraine store is a valid DuckLake catalog
from birth, and DuckLake's bootstrap DDL batch never runs against one.

**Attach modes and the single writer.** The RFC 0004 topology is enforced at
the attach surface. An attach is either **read-write** — opening the one
SlateDB `Db` writer — or **read-only** (`READ_ONLY`, mapped to SlateDB's
`DbReader`), which never becomes a writer and never participates in
fencing. The plumbing that carries the `READ_ONLY` flag from `ATTACH`
through the shim to the store open is specified and implemented in RFC 0017. This distinction is not cosmetic: SlateDB fencing means *the
newest writer wins* — a second read-write attach from another process
fences the incumbent's committer rather than failing itself, so two
processes attaching read-write take turns breaking each other. A
deployment therefore designates exactly one read-write process; every
other DuckDB process attaches `READ_ONLY`. This is a real limitation
relative to the multi-client SQL catalogs DuckLake otherwise targets, and
it is documented at the user surface (ATTACH docs, README), not only here.

`READ_ONLY` is read-only at the *catalog* level, not at the IAM level:
SlateDB's `DbReader` in its default follow-latest mode writes a checkpoint
into the manifest on open and refreshes it for the attach's lifetime
(RFC 0004, Topology), so reader credentials need manifest write access.

**`CHECKPOINT` — the truly-zero-write attach.** Given the id of a
checkpoint minted ahead of time, the reader opens against it instead of
establishing its own: SlateDB records nothing, spawns no manifest poller,
refreshes nothing, and deletes nothing on close, so the attach issues
object-store reads and no writes at all. The core half is RFC 0004's
(`CatalogOptions::checkpoint`, `Catalog::create_checkpoint`); what this
RFC owns is carrying an id from `ATTACH` through the shim into that open,
spelled `META_CHECKPOINT` through a DuckLake attach or `CHECKPOINT`
directly on a standalone one. That the attach writes nothing is asserted
end to end by comparing the store's whole object set across it — an
absence is only worth claiming if something would break were it false.

The cost is a **fixed cut**: SlateDB stops polling the manifest entirely,
so the attach never observes a commit made after the checkpoint —
advancing means minting a new one and re-attaching. Three refusals keep
that from surprising anyone: a checkpoint id on a read-write attach (a
writer commits at head; a checkpoint is a past cut), an id the manifest no
longer carries (a reader told to serve a fixed cut must never quietly
serve a different one), and a malformed id.

The lifecycle is three table functions, and they do not all take the same
argument. `moraine_create_checkpoint('<attached catalog>')` names a
catalog because the core mints through the writer that attach already
opened — a second read-write open would fence it — and takes an optional
lifetime, since a checkpoint given none pins its objects until released by
hand. `moraine_checkpoints('<store>')` and
`moraine_delete_checkpoint('<store>', '<id>')` name a store path instead,
for the same reason `moraine_migrate` does: neither opens the writer, so
both run against a live catalog without fencing it, and the processes that
attach against the id hold no write credentials, so those calls happen
wherever the credentials are. Listing exists so a checkpoint whose id was
lost can still be found and released.

**Creating an S3 lake needs `READ_WRITE`.** DuckDB bumps any attach whose
path begins with a remote prefix (`s3://`, `gcs://`, `azure://`, `http(s)://`,
…) from `AUTOMATIC` to `READ_ONLY` before the storage extension is reached
(`DatabaseManager::AttachDatabase`, on the premise that remote DB *files* are
not writable). moraine honors that flag, so an `s3://` attach with no explicit
mode opens read-only — and a read-only open never bootstraps, so *creating* a
new S3 lake fails with the SlateDB "no manifest" error. The premise is wrong
for moraine (SlateDB is object-store-native and writes S3 happily), but the
heuristic is a blanket path-prefix rule with no per-extension opt-out. The user
contract is therefore: **creating or writing an S3-backed lake requires
`READ_WRITE` on the ATTACH** — `ATTACH 'ducklake:moraine:s3://bucket/prefix' AS
lake (DATA_PATH 's3://bucket/prefix-data/', READ_WRITE)` — the same opt-in any
writable remote DuckDB database needs. Local and `memory://` stores default to
read-write and need no flag. When a read-only attach targets an uninitialized
store the shim rewrites the terse store error to name this fix (add
`READ_WRITE`). Other DuckLake metadata backends sidestep this because their
paths are not remote-file URIs — a Postgres/MySQL connection string never
matches the prefix rule, and local DuckDB/SQLite files default read-write.

**`CACHE_DIR` — the block cache's disk tier, for S3 stores.** Every query
rebinds the catalog by reading its metadata (snapshot → tables → columns →
files → stats) from the store; on an `s3://`-backed catalog those reads are
S3 round-trips, and the in-memory tier starts empty in each new process. The
`CACHE_DIR` attach option — `ATTACH 'ducklake:moraine:s3://…' AS lake
(DATA_PATH '…', READ_WRITE, META_CACHE_DIR '/var/cache/moraine')`, or
`CACHE_DIR` directly on a standalone `moraine:` attach — gives each
store's block cache (RFC 0009) a disk device under that directory: blocks
are written there at block grain as they are read, so repeat reads skip
the GETs, and a new process recovers what the last one left, so a
re-attach in the same order starts warm without a preload. It threads
through the shim (`moraine_attach`'s `cache_dir`) into
`CatalogOptions::cache_dir` and `StoreBuilder`, applying to both the
writer and the reader; unset (the default), only the memory tier applies.
Redundant for local/`memory://` stores.

There is one budget, shared by every store the process attaches, and one
cache per store under it (RFC 0009). The first attach to open settles the
sizing and later ones are sized by it, so these options are the process's
rather than the attach's. SST indexes and filters are held at a higher
eviction priority than data blocks, so a scan cannot push them out, while
metadata a store does not need is space blocks take; each store's blocks
spill at block grain to its own device under `CACHE_DIR`, capped by
`CACHE_SIZE`, with `CACHE_MEMORY` budgeting the memory every store's cache
and the parsed-footer cache share, re-split across the stores attached.
There is no separate object-part cache beneath — a byte is cached decoded,
once.

**`CACHE_SIZE` — how much disk the cache may take.** A byte count —
`META_CACHE_SIZE 2147483648` through the DuckLake attach or `CACHE_SIZE`
directly on a standalone `moraine:` attach — capping each store's disk
device: the first attach to name a `CACHE_DIR` settles it, and every
store's device opens with that cap as a ceiling, filled only by what the
store reads. Zero on the ABI means "not given", leaving the default
cap (16 GiB); the option is inert without a `CACHE_DIR`, since there is no
disk tier to bound. It threads through the shim (`moraine_attach`'s
`cache_size_bytes`, and `moraine_migrate`'s) into
`CatalogOptions::cache_size` and `StoreBuilder`.

**`CACHE_MEMORY` — how much memory the cache may take.** The same shape for
the memory side — `META_CACHE_MEMORY` through the DuckLake attach,
`CACHE_MEMORY` standalone — and the one cache option that is never inert,
because the memory tiers exist with or without a disk device. It is one
budget across both kinds, which share one cache and are separated by
eviction priority rather than by capacity; `moraine_store_census` reports a
store's index and filter bytes, and the attach warns when the budget cannot
hold them. Unset, the budget is what SlateDB would give a *single*
store by default — now for the whole process, so a multi-store host is
strictly smaller than before, and a single-store host is unchanged. This
is the number to weigh against DuckDB's `memory_limit` when sizing a
host: DuckDB's budget covers its own buffers and the data-file cache, and
this cache is the process-wide memory consumer beside it. A handle's
decoded catalog is not in it: that is bounded by the catalog's size rather
than by an option, and is reported rather than budgeted.

**`CACHE_PUTS` — fill the cache from the write path too.** Left alone, the
cache fills only from reads: blocks the store just flushed are fetched back
from object storage and decoded the first time something asks for them,
even though the writer had them in hand. `CACHE_PUTS true` inserts the
blocks of flushed and compacted SSTs into the cache as they are written —
decoded, on the write path, at the cost of memory-tier space and no fetch.
Within the process the effect reaches every handle: a reader session served
by the shared cache reads what the writer just flushed with no round trip
at all.

It is opt-in because compaction output goes through the same insertion
policy: a merge writing a large SST can evict blocks that reads had warmed,
and a store whose merges outpace its queries is better off letting reads
decide what stays. The option threads through the shim (`moraine_attach`'s
`cache_puts`) into `CatalogOptions::cache_puts` and `StoreBuilder`.

**`CACHE_PRELOAD` — fill it before the first query, not during it.** Both of
the above leave a fresh process cold: the cache fills as queries ask for
blocks, so the first query of an attach pays every first touch itself.
`CACHE_PRELOAD` warms the store into the cache while the attach opens, by
reading it (RFC 0009) — `'l0'` (the default) touches every subspace far
enough to pull its SST metadata, `'all'` additionally walks the scan-shaped
subspaces whole, `'none'` (or `'off'`) warms nothing. Neither pulls the `index`
subspace's data bulk, so a store whose weight is a multi-GiB `index` run
preloads in metadata-sized bytes rather than store-sized ones; a table's
`index` and `inline` ranges are warmed per table instead, in the
background, the first time a query touches it and after each commit that
writes data files against it; at any level but `'none'`, the extension also
warms every table's probe ranges in the background after the open, so the
first lookup on any table finds them cached (RFC 0009). The shim's option
parsing owns the default; the level crosses the ABI as a code (`0`, `1`,
`2`) on `moraine_attach` and `moraine_migrate`, and any other code is
refused rather than treated as "none", so a misspelled level surfaces as an
error instead of an attach that merely feels slow.

The cost lands entirely on the attach. The warm runs inside the open — the
handle is returned only once it finishes — with bounded parallelism, and
the cache's caps govern the load: what exceeds them is admitted and evicted
by the cache's own policy, newest-first enumeration putting the levelled
tail last in line. Nothing in that path says it happened, which would leave
a half-warmed attach looking exactly like a warm one, so moraine compares
the bytes the warm would fetch against the governing cap as it opens and
warns with both numbers. A subspace that fails to warm is skipped rather
than fatal: a preload is an optimization, and no attach should die
because one read did not land. Subspace-awareness keeps the `'l0'`
default's cost small — on S3 it adds about 120 ms to the attach and takes
a table's first index lookup from roughly 400 ms to 3 ms — and `'all'` the
choice for an attach that can wait: it fetches metadata plus the
scan-shaped subspaces, not the store, trading a slower ATTACH for a first
query that touches object storage not at all. `'none'` suits an attach that
must return as fast as the open allows.

**`moraine_cache_tally()` — what the cache has served.** One row:
`metadata_hits`/`metadata_misses` and `block_hits`/`block_misses` with a
rate beside each, plus `errors` for lookups the cache itself failed and
read through. `preload_metadata_hits`/`misses`, `preload_block_hits`/`misses`,
and `preload_failures` attribute the attach-time warm subset. Without
arguments the numbers are the host's — a process
keeps one cache and every attached store reads through it — which is the
scope the budget is set at.

`moraine_cache_tally('lake')` narrows the same row to one attach. The
cache and its budget stay the process's; what the lake name adds is which
attach is spending them, which is the question a host with several
catalogs on one cache has and the process-wide numbers cannot answer. A
detached catalog's counts leave with it, while the process's keep them,
so the two forms do not have to agree on totals.

The two slots are reported apart because they are budgeted apart: a
healthy stack keeps metadata near fully served — that is the slot being
sized to hold the store's filters and indexes — while blocks land
wherever the working set does. A metadata rate that is not near 1 says
`CACHE_MEMORY` is too small for the store's SST metadata, which
`moraine_store_census` measures directly; a low block rate with a healthy
metadata rate says the working set exceeds the block slot, which is a
`CACHE_MEMORY` or `CACHE_DIR` decision rather than a correctness one.

The metadata case does not wait to be noticed: the attach itself compares
the store's filter and index bytes against the share of the cache metadata
holds under eviction protection, and warns when that share cannot hold
them, so a rate read later confirms a sizing problem rather than
discovering one.

A rate is NULL, not zero, before anything has been looked up: zero would
read as a cold cache to a monitoring query, and "nothing asked" is not
the same fact.

**`moraine_cache_status()` — what the process-wide caches occupy.** One row
reports capacity, current occupancy, and cumulative evictions for SlateDB
metadata, SlateDB data-block memory, and parsed Parquet metadata. It also
reports the configured data-block disk capacity, NULL without a disk tier.
Disk occupancy is omitted because Foyer does not expose a reliable live
value through the cache interface; configured capacity is not mislabeled as
usage. `moraine_store_census` adds `filter_bytes`, `index_bytes`, and
`stats_bytes` per subspace from the manifest, which is the store-side demand
to compare with metadata capacity.

**`moraine_object_store_tally('lake')` — what SlateDB sent to storage.**
One cumulative row per attach reports `main_gets`, `main_puts`, and
`main_deletes`, the same three counts for a separately configured WAL store,
and summed request time beside every count in milliseconds. Moraine currently
uses one physical store, so WAL-object traffic appears in the `main_*` fields;
the `wal_*` fields are reserved for an explicit second store. A GET includes
the read-shaped APIs such as range, head, and list, and a PUT includes multipart
operations. The instrumentation is inside SlateDB's retry loop, so attempts and
their durations, not only logical calls, are counted.

`errors` counts failed attempts, including errors SlateDB handles internally
such as an expected missing-object probe; it is diagnostic, not a statement
that the catalog operation failed. Durations are sums and can exceed wall time
when requests overlap. The core surface is
`ReadOnlyCatalog::object_store_tally`, carrying an `ObjectStoreTally`; the C ABI
copies it through `moraine_catalog_object_store_tally` without exposing the
metrics recorder. There is deliberately no process-wide SQL form: unlike the
one shared cache budget, physical requests belong to the catalog handle that
issued them.

The data path needs none of this and gets none of it. Parquet reads are
DuckDB's, not moraine's, and DuckDB caches them itself: a lake read goes
through its caching file system, so data bytes sit under `memory_limit`
rather than in a budget of moraine's (RFC 0009). What the embedding host
should set beside a moraine attach — `validate_external_file_cache =
'NO_VALIDATION'` (safe: DuckLake data files are immutable),
`parquet_metadata_cache = true`, `enable_http_metadata_cache = true` —
is embedding guidance, documented with the attach options; the shim
never sets a global for the user.

### Interception level: catalog-entry, row-faithful (B1)

moraine intercepts at DuckDB's **Catalog / table-scan / DML layer** — the
`postgres_scanner` pattern — never by parsing raw SQL. Parsing DuckLake's SQL
would mean reimplementing a query engine; instead DuckDB's own executor plans
DuckLake's statements and calls moraine per table.

moraine's catalog exposes the fixed set of `ducklake_*` tables (and the
per-schema-version inlined-data tables) as catalog entries with the DuckLake
schema, and implements:

- **Scan** — given a table and the columns DuckDB asks for, produce rows from
  SlateDB. Row filters are **not** pushed down, so a scan materializes the
  addressed kind in full and DuckDB's executor filters over the returned rows.
  Projection is pushed down, but it selects output columns from an
  already-materialized row set rather than narrowing the read. What narrowing
  exists comes from the address, not from a predicate: the RFC 0002 key layout
  puts each kind — and each table's inlined data — under its own prefix, so
  serving one table reads only that table's range.
- **Insert / Update / Delete** — apply row mutations to the store.
- **Transactions** — `begin`/`commit`/`rollback` mapped onto RFC 0004's
  **staged-row commit path**: a transaction stages row mutations; commit
  drives them through the single fenced atomic batch under head conflict
  detection. On this path moraine performs **no internal retry** — DuckLake
  authored the ids, counters, and snapshot values embedded in the staged
  rows, so any lost race (benign-shaped or not) aborts with the typed
  `CommitConflict`, surfaced to DuckLake as a transaction failure. DuckLake
  then re-drives it: its `RunCommitLoop` (source-verified) retries
  metadata-catalog commit failures with bounded jittered backoff,
  re-checking its own conflict matrix first. Two wire-contract consequences
  for the shim, both load-bearing: **(a) the error message matters** —
  DuckLake's `RetryOnError` decides retryability by substring match on the
  lowercased message (`"primary key"`, `"unique"`, `"conflict"`,
  `"concurrent"`), so the text moraine surfaces for a lost commit must
  contain `"conflict"` or DuckLake will abort instead of retrying; **(b)
  moraine must serve conflict-resolution reads mid-retry** — between
  attempts DuckLake queries `ducklake_snapshot` /
  `ducklake_snapshot_changes` for everything after its transaction
  snapshot, through the ordinary scan hook.
- **Constraints** — the primary keys DuckLake's spec relies on. Its schema
  declares exactly five, on `ducklake_snapshot`, `ducklake_snapshot_changes`,
  `ducklake_schema`, `ducklake_data_file`, and `ducklake_delete_file`, and
  moraine refuses an insert that would displace a live row under one of them
  with a typed `Constraint`. The two snapshot kinds need no lookup: a snapshot
  record exists only at or below head, so the head-advance check that already
  guards every staged commit covers them by construction — surfacing there as
  the lost race it is, which DuckLake re-drives, rather than as a constraint
  it would abandon. **No name
  uniqueness is enforced anywhere**, deliberately — DuckLake's schema declares
  no `UNIQUE` constraint on any name column and polices naming in its binder,
  so inventing one here would refuse rows its other backends accept. Both
  halves are pinned.

Because the `ducklake_*` rows are the catalog state (B1), RFC 0002 keys are an
efficient encoding of those rows and RFC 0005's inlined chunks are the storage
of specific `ducklake_*` tables — not a separate model that must be
reconciled. This keeps moraine robust to DuckLake evolving its SQL: the same scan/DML
hooks serve new access patterns over the same tables.

**The access set is known, not assumed.** DuckDB's `QueryLog` records the
statements DuckLake issues on its metadata connection, so a workload
reaching every modelled feature — DDL, bulk and inlined writes, updates,
deletes, schema evolution, views, comments, compaction, expiry, cleanup,
time travel — yields the exact set of tables read and written, pinned as
a set equality (`wire_contract.rs`). Thirty tables, of which twenty are
both read and written; the reads are the metadata-projection scans, and
the writes are the staged-row translations. Two findings fall out of it
directly. `ducklake_file_variant_stats` never appears, because DuckLake
writes it only for a column whose extra stats are VARIANT and moraine
refuses a VARIANT column at creation (its inline encoding is Arrow, which
has no VARIANT representation) — so the always-empty stand-in is exactly
right, and modelling the table is downstream of VARIANT support, not
independently owed. And nothing in the set is a pattern the row-faithful
layout serves badly, which is the evidence semantic projection (B2, below)
was waiting on.

"No semantic re-modeling" comes with **exactly one interpreted
convention**, stated so its scope is bounded. The RFC 0002 `current`/`history`
split physically encodes the begin/end-snapshot lifecycle columns, so
moraine must *recognize* the lifecycle in DuckLake's DML — an `UPDATE` that
sets a row's `end_snapshot` translates to end-version bookkeeping (delete
the `current` key, write the `history` key), not a blind value overwrite. That is
a semantic mapping, and it is where the residual drift risk concentrates:
if DuckLake ever mutates those columns in a shape moraine does not
recognize (un-ending a row, say), the translation would misfile it. The
convention is deliberately minimal — lifecycle columns only, everything
else opaque — and the e2e suite pins it against every lifecycle transition
real DuckLake SQL produces. The contract is not zero interpretation; it is
exactly one, tested.

### Composition: C++ shim over the Rust core (forced)

DuckDB's stable C extension API (`duckdb_ext_api_v1`) exposes scalar
functions, table functions, and a handful of other hooks — **not**
storage/catalog registration. A writable DuckLake catalog requires a
`StorageExtension` (DuckLake issues `CREATE`/`INSERT`/`UPDATE` against it;
read-only table functions cannot serve that), and registering one means
**linking DuckDB's C++ internals**. This is the same path the built-in
postgres/sqlite/mysql catalog attachers take. The pure-Rust extension crates
(`duckdb-rs`, `extension-template-rs`) ride the C API and therefore cannot
register a catalog.

Therefore `moraine-duckdb` is:

- a **thin C++ shim** that links DuckDB's internal C++ API and registers the
  `StorageExtension`, `Catalog`, and `TransactionManager`, plus
- the Rust **`moraine` core** compiled as a staticlib exposing a **minimal C
  ABI**: open/attach, `begin`/`commit`/`rollback`, `scan(table, projection,
  filters) -> Arrow`, and `apply(mutations)`.

The shim contains no domain logic — it translates DuckDB catalog callbacks
into C-ABI calls. This preserves RFC 0001's "thin by policy" intent, restated
**language-agnostically**: no catalog semantics in the extension layer,
regardless of the language it is written in.

- **Boundary format: typed C structs, plus the Arrow C Data Interface for
  inline chunks.** Metadata and inline *scan* results cross the ABI as
  owned arrays of `#[repr(C)]` row structs (`crates/moraine-duckdb/src/
  dumps.rs`/`inline.rs`), one `_free` per array — not Arrow arrays as
  originally intended here. Inline chunk *bodies* are the exception and do
  use the Arrow C Data Interface: the shim converts a `DataChunk` to
  `ArrowArray`/`ArrowSchema` with DuckDB's `ArrowConverter` and the Rust
  bridge (`src/arrow_ipc.rs`) serializes to Arrow IPC, with the structs
  crossing the ABI by pointer (`moraine_arrow_encode_*`/`_decode_stream`,
  consuming on encode and producing on decode; ownership rules in
  `arrow_ipc.rs`).

  **Scan results stay row structs.** The split above is the decision, not
  an interim state. Arrow's win is bulk columnar transfer, and the
  metadata path has no bulk to transfer: rows are built one at a time from
  a `BTreeMap` of protobuf-decoded records, so an Arrow builder would
  visit every field exactly as often — it moves the copy rather than
  removing it. The measured cost is elsewhere anyway (`BENCHMARK.md`:
  ~5–7 µs per live entity to materialize, and RFC 0009's open item names
  the *clone* in `dump_entities`, not the marshalling), and that fix is
  strictly cheaper and strictly prior. Against that, the row structs are
  the schema contract: each is checked field-for-field against
  `metadata_tables.cpp`'s spec table by the compiler and against
  DuckLake's own DDL by review, where an Arrow schema assembled at runtime
  would move both to a runtime failure. Inline chunk bodies are the case
  that genuinely is columnar and where DuckDB owns both export and import,
  which is exactly why they are the exception. Revisit only on a profile
  naming dump marshalling — not materialization, not cloning — as a
  material share of catalog rebind time.
- **Sync↔async bridge lives in the Rust C-ABI layer.** The core is async
  (SlateDB requires tokio, RFC 0001). The C-ABI layer owns the tokio runtime
  and `block_on`s core futures, so the C++ shim only ever calls synchronous C
  functions. This is the "FFI boundary" of RFC 0001's async rule.

### C ABI error mapping (v0)

Pinned by the extension-loads slice (`moraine-duckdb/src/error.rs`, `mod
codes`). Every fallible C-ABI entry point returns an `i32` code and, on
failure, fills a caller-provided `(code, message)` pair (`MoraineError`);
messages are UTF-8, allocated by Rust, and freed only via the exported
`moraine_error_free`. Every entry point wraps its whole body — `block_on`
included — in `catch_unwind`, so a core panic surfaces as a code, never as
an unwind into C++. The shim translates codes to DuckDB exceptions:

| Code | ABI constant | Source | Shim raises |
|---|---|---|---|
| 0 | `OK` | success | — |
| 1 | `NOT_FOUND` | `Error::NotFound` | `CatalogException` |
| 2 | `ALREADY_EXISTS` | `Error::AlreadyExists` | `CatalogException` |
| 3 | `CONSTRAINT` | `Error::Constraint` | `CatalogException` |
| 4 | `COMMIT_CONFLICT` | `Error::CommitConflict` | `TransactionException` |
| 5 | `CORRUPTION` | `Error::Corruption`, plus catalog strings that cannot cross the boundary (embedded NUL) | `IOException` |
| 6 | `STORE` | `Error::Store` (and, conservatively, any future core variant) | `IOException` |
| 7 | `INVALID_ARGUMENT` | ABI-layer validation: null pointer, invalid UTF-8, unsupported store scheme | `InvalidInputException` |
| 8 | `INTERNAL` | a panic caught at the FFI boundary | `InternalException` |
| 9 | `INTERRUPTED` | cancellation — `moraine_interrupt` or the call's interrupt probe — cancelled the read in flight (or about to start) on the handle | `InterruptException` |
| 10 | `RETRY_EXHAUSTED` | `Error::RetryBudgetExhausted` — the commit spent its whole internal retry budget without settling | `TransactionException` |
| 11 | `FENCED` | `Error::Fenced` — another process took over as the writer; the handle can no longer commit, and the message says to re-attach | `IOException` |

Wire contract: the `COMMIT_CONFLICT` message always contains the literal
substring `conflict` — DuckLake's `RetryOnError` keys its retry decision on
that substring (see the Transactions bullet above) — so the message text
is part of the ABI contract, not incidental diagnostics.

The contract runs both ways. `RETRY_EXHAUSTED` is terminal, so its message
carries **none** of the four substrings DuckLake retries on (`conflict`,
`concurrent`, `unique`, `primary key`); the same rule governs the
equality-index uniqueness rejection. DuckLake's loop is itself bounded — by
`ducklake_max_retry_count`, default 10 — so a retryable terminal error does
not spin forever, but it does multiply the work by that factor before
failing, which for a large commit is the difference between an error and an
apparent hang. Both codes raise `TransactionException`: DuckLake's retry
decision reads the text, not the exception kind.

### Diagnostics seam

The core emits `tracing` events; a loaded extension consumes none by
default, and the host cannot consume them for it — the extension is a
separate dynamically-loaded library with its own statically-linked
`tracing`, so a subscriber installed by an embedding process never sees
them. Left there, every diagnostic the core produces is dropped, which is
how a commit that spends its retry budget can present as a silent stall.

`ATTACH` installs a process-wide buffering subscriber (`logging.rs`), once,
via `try_init` — a host that already set a global subscriber keeps it. The
buffer is bounded (oldest-first eviction, with the drop count reported on
the next drain) so diagnostics never grow without limit behind a caller
that stops draining. `MORAINE_LOG` sets the captured level, default `info`.

Events fire on the handle's tokio worker threads, where no `ClientContext`
is in scope. While a catalog is attached, that does not delay them: the
catalog registers a database-scoped writer
(`Logger::Get(DatabaseInstance&)`, which is thread-safe) as its handle's
push sink via `moraine_register_log_sink(handle, sink, ctx)`, and the
handle's events write through it as they fire. A watcher querying
`duckdb_logs` from another connection therefore sees a running commit's
diagnostics while the commit runs — the stalled-commit case the logs exist
to explain — and nothing is ever evicted while a sink is registered. The
catalog's destructor unregisters via `moraine_unregister_log_sink(handle)`
before the handle or the database goes away, and unregistration returning
guarantees no push is in flight.

Delivery is routed per handle, though `tracing` itself is process-global:
each attach's runtime tags its worker threads with the handle's identity
at spawn, the FFI `block_on` wrappers tag the calling thread per call, and
an event is attributed to whichever handle tagged the thread it fires on.
One process serving many attached lakes keeps their diagnostics apart —
each lake's records surface only in its own database's `duckdb_logs`.
Records carry DuckDB's own `LogLevel` values, so the sink forwards them
unchanged, and appear under the `moraine` type after
`CALL enable_logging(level => 'info', storage => 'memory')` — the default
storage writes to stdout rather than the table. Under DuckDB's default
`LEVEL_ONLY` mode the type string is not filtered on, so no log-type
registration is required.

Events with no routable sink — fired before the handle's sink registers
(registration flushes that handle's backlog through it), after it
unregisters, or on a thread no handle tagged — buffer instead, and the
shim drains the buffer through `moraine_drain_logs(sink, ctx)` on threads
that hold what each sink needs, wherever events could otherwise strand:

- **`CommitTransaction`**, on every outcome — success, conflict, or
  exhausted budget — and **`StartTransaction`**, after the snapshot
  resolve, both writing through `Logger::Get(context)`. With push sinks
  registered these find little to drain; they exist for the gaps around
  registration and for unattributed events.
- **`Attach`**, on both exits — a failed open's events would otherwise
  wait forever, since no catalog (and so no push sink) ever exists. On
  success, registration itself flushes the handle's backlog through the
  new sink.
- **The maintenance pass**, which runs on the scheduler's own thread,
  drains through the same database-scoped writer and closes each pass with
  one shim-originated outcome line: `warn` naming every failed step,
  `info` for a clean pass.

The sink never throws: an exception escaping into the ABI would unwind
across the boundary, and a lost diagnostic must not fail the operation
that produced it.

**Event frequency is a rule, not a convention.** The buffer holds 512
records and drops oldest-first, so its capacity is a budget shared by
everything the core logs between drains. Events are emitted at
per-operation frequency — per commit, per open, per pass — never per
entry, per row, or per file. A per-entry event at `debug` would evict the
very warnings the buffer exists to preserve.

### Read cancellation seam

Pinned by `moraine-duckdb/src/{abi,runtime}.rs`. Every cancellable entry
point `block_on`s a `select!` between the core future and its
cancellation signal, `biased` toward the signal so a pending interrupt
always wins a tie and aborts before the core future does any work. A
cancelled call returns `INTERRUPTED`, which the shim raises as
`InterruptException`; the handle stays fully usable and the next call is
unaffected.

**The signal is per call, not per handle.** Cancellable entry points take
a trailing `MoraineInterruptProbe probe, void *probe_ctx` pair
(`typedef bool (*MoraineInterruptProbe)(void *)`). The probe is checked
once synchronously before the core future is first polled (a timer's
first tick is pending at the poll level even when already elapsed, so a
future completing on its first poll would otherwise beat a pending
interrupt), then the `select!` polls it on a ~100 ms interval — DuckDB's
own slice convention for interruptible waits. A null probe means
non-cancellable. The probe may run on any of the runtime's threads, so it
must be thread-safe and `probe_ctx` valid for the duration of the call —
the shim's probe is a single atomic load of `ClientContext::interrupted`,
the flag DuckDB's own executor polls. Level-triggered, so a signal cannot
leak past the call that observed it: the probe is only consulted while a
call is in flight, and DuckDB clears the flag before the next query
issues one.

Per-call is what the deployment shape requires, and it is why the earlier
per-handle `Notify` and its `moraine_interrupt(handle)` push channel are
gone rather than merely unused. DuckDB's flag is per *connection*, and
several connections share one attached catalog; a single per-handle
signal would let one connection's Ctrl-C abort another's query, or be
consumed by it. Two overlapping reads on one handle cancelling
independently is pinned by test.

This is how the shim wires Ctrl-C: DuckDB offers no interrupt callback to
a storage extension (cancellation is cooperative polling of a public
atomic flag; a thread blocked in an FFI call never reaches the executor's
poll sites), so the poll moves inside the blocked call, where the async
core makes cancellation a dropped future. No first-party remote-catalog
extension (postgres, mysql, iceberg, delta, httpfs — or DuckLake itself)
cancels a blocked external call; their shipped mitigations are timeouts.

Cancellable entry points are the ones that block on store I/O and mutate
nothing: `moraine_snapshot`, the `moraine_dump_*` reads, the
`moraine_inline_*` reads, and `moraine_tx_begin` (reads the head
snapshot; nothing is staged yet, so aborting it leaves no state). The
snapshot listing calls (`moraine_snapshot_schemas`/`tables_in`/
`columns_of`/`views_in`/`data_files_of`) walk the already-materialized
snapshot in memory and take no probe.

`moraine_attach` is cancellable too, and is the exception to
"mutates nothing": the store open is the one long blocking call an attach
makes, and against an unreachable endpoint it is the one a user is most
likely to want out of. A cancelled attach writes no handle and leaves
nothing attached, but it may already have taken the writer epoch — an
attach cannot know whether it will finish before it claims one — so a
process that was attached before it must re-attach, exactly as after any
failed attach. Its abandoned runtime is wound down with a deadline rather
than dropped: a plain drop blocks until every background task of the
half-built store finishes, which is the hang the cancellation existed to
escape.

Two paths take no probe, both deliberately. **The commit path is
shielded**: `moraine_tx_commit` lets an interrupt during `COMMIT` finish
rather than tear the commit mid-protocol, matching upstream DuckDB's own
direction of suppressing interrupts around commit irreversibility.
**`moraine_detach` is teardown**: an interrupt part-way through would
either leak the handle or leave the store half-closed, and detach's wait
is the flush that makes committed data durable — cancellation exists to
escape a wait, not that one.

### Version pinning and distribution

Linking DuckDB's C++ internals ties the extension to a specific DuckDB build;
the ABI is not stable across releases. This is the "FFI/build/version-pinning
tax" RFC 0001 explicitly chose to defer out of the core and pay only at the
extension boundary; the rest of this section is what paying it looks like.

**A build is bound to one DuckDB version string, exactly.** DuckDB refuses a
C++-ABI extension whose metadata footer names a different version —
`ParsedExtensionMetaData::GetInvalidMetadataError` compares the strings, patch
releases included — so a user on one patch release cannot load another's
build. Supporting a series therefore means building each release in it
separately, and the distribution unit is a DuckDB *release*, not a series.

**One manifest owns the pin.** `.github/duckdb-versions` lists the releases
moraine builds for, newest first, the first line carrying the commit each
submodule must sit on. `xtask` reads it, both release workflows build a
matrix from it, assets are published as
`moraine.<duckdb-version>.<platform>.duckdb_extension` so several versions
coexist in one release, and `cargo xtask check-pins` fails if any other place
naming a version disagrees. That check exists because the failure mode is
silent: a bump that misses one of the six places produces an artifact that
builds, passes every other job, and then refuses to load. `cargo xtask
bump-duckdb <version>` writes those places, so the check guards a
transcription no one has to make by hand. It covers the DuckLake commit
too, which DuckDB chooses rather than moraine — otherwise a DuckDB bump
moves it silently and only e2e says so.

**Which releases are listed** is a judgement, and a short list is the default.
Each entry multiplies the release build by five platforms, and only the
primary is proven end-to-end against a real DuckLake — the e2e suite is
moraine's whole correctness story for the shim, and running it per entry is
the real cost, not the build. So the list holds the releases users are
plausibly on; dropping one only stops future releases from carrying it, since
published assets stay where they are.

**Which DuckDB series moraine tracks:** the one DuckLake's current release
branch targets, adopted when DuckLake cuts that branch rather than when DuckDB
releases. The extension exists to be DuckLake's catalog, and a DuckDB with no
DuckLake built for it has nothing to attach. DuckLake versions by DuckDB-series
branch, so the two move together by construction.

**Signing is DuckDB's, not moraine's.** The public keys
`ExtensionHelper::GetPublicKeys` trusts are compiled into every DuckDB binary,
so no third party can produce a signature the stock CLI accepts;
`extension-upload-single.sh` zeroes the footer's 256-byte signature region
unless it holds one of those private keys. Hence: moraine's own release
workflow publishes unsigned, every e2e harness starts the CLI with
`-unsigned`, and the community-extensions pipeline — pointed at this repo by
`description.yml` — is the only route to a signed build. That is a
consequence of DuckDB's design rather than an unbuilt piece of moraine's.

**The pin, tracking latest stable:**

| What | Pinned at | Notes |
|---|---|---|
| DuckDB | **v1.5.5** | the primary entry of `.github/duckdb-versions`; both submodules sit on it, and it is the version the e2e suite proves the chain against |
| DuckLake extension | **`d8a1881e`** | what `INSTALL ducklake` resolves to against the pinned CLI: DuckDB v1.5.5 hard-codes the commit in `.github/config/extensions/ducklake.cmake`, so the pair moves only when the DuckDB pin does. Verified by running, not assumed |
| DuckLake branch | **`v1.5-variegata`** | DuckLake publishes no release tags — it versions by DuckDB-series branches (`v1.3-ossivalis`, `v1.4-andium`, `v1.5-variegata`); `main` is development |
| DuckLake catalog format | **`1.0`** (`DuckLakeVersion::V1_0`) | the highest version the stable branch writes (its migration chain ends at `'1.0'`); `V1_1_DEV_1` exists on `main` only and is not targeted |

**Patch-level ABI friction between the two does not appear.** DuckDB's own CI
builds the DuckLake extension against v1.5.3 while moraine statically links
v1.5.5, and the concern was that objects crossing the extension↔host boundary
by pointer between those two builds might disagree. They do not: both
extensions load into one process and the full chain answers correctly, pinned
by `wire_contract.rs` alongside the version strings, so a bump that introduces
friction fails there rather than in the field. The fallback to v1.5.3 an
earlier revision of this RFC held open is therefore not needed and not taken.

The source-verified behaviors this RFC suite cites (conflict matrix, commit
retry loop, `SchemaChangesMade` classification, per-table column-id
allocation, the five primary keys) were verified on `main` @ `34db89b`
(2026-07-09) and re-checked **identical on the pinned branch** — the diffs
between the two are cosmetic (accessor renames, formatting). The e2e suite
regression-pins against the table above; moving any row of it is a
deliberate, reviewed bump.

### Build and pin mechanics (as implemented)

**Built through DuckDB's extension toolchain.** moraine-duckdb builds via
`extension-ci-tools` — the same CMake+Make toolchain the community-extensions
repository uses — added as a git submodule alongside a `duckdb` submodule
pinned to the supported release. The toolchain owns the whole extension link:
it builds DuckDB from the submodule and **statically links it into the
loadable**, compiles the C++ shim as extension sources against DuckDB's full
`src/include/` tree (every internal header at its real path — the parser and
storage-extension types the shim needs, e.g. `TableFunctionRef`,
`LogicalInsert`/`LogicalUpdate`/`LogicalDelete`, which the amalgamation never
exposed), and links the moraine Rust core in as a static library via
[corrosion](https://github.com/corrosion-rs/corrosion). Static DuckDB linkage
is what makes the extension loadable into the stock CLI on every platform:
the stock Linux release CLI exports none of its C++ internals, so a `dlopen`
that deferred symbol resolution could never resolve them there — carrying its
own copy is exactly how official DuckDB C++ extensions are shaped. Objects
cross the extension↔host boundary by pointer between ABI-identical builds of
the same pinned version — the version pin is what makes this sound. None of
this linking lives in the crate: it declares `crate-type = ["staticlib",
"rlib"]` — `staticlib` is what CMake links, `rlib` is what the crate's own
integration tests link against — and its `build.rs` does one thing
unrelated to the extension link, generating `cpp/moraine_abi.h` from the
`extern "C"` surface with cbindgen so the header cannot drift from the
Rust definitions. Two build details the toolchain needs supplied per
target: a C++17 bump on the shim targets (it uses `std::optional`; DuckDB
pins the global standard to C++11), GCC 14 for Linux to match DuckLake's
extension pipeline, and, on macOS, the `IOKit`/`Security`/
`CoreFoundation`/`SystemConfiguration` frameworks the Rust dependency tree
links.

**Entry point and packaging.** DuckDB's loader `dlsym`s a fixed entry
symbol derived from the artifact filename with every `.`-suffix stripped,
so the loadable carries exactly one dot (`moraine.duckdb_extension` →
`moraine_duckdb_cpp_init`). The entry point is a C++ symbol emitted by the
toolchain's `DUCKDB_CPP_EXTENSION_ENTRY(moraine, loader)` macro
(`cpp/moraine_extension.cpp`), which forwards to the StorageExtension
registration; the toolchain's linker whitelist exports exactly that one
symbol and hides everything else (including the Rust core's C ABI, resolved
internally at static-link time). The toolchain also appends the required
512-byte metadata footer (ABI type, extension version, the exact DuckDB
version string, target platform, magic value, and a signature region left
zero for unsigned local loading, filled by DuckDB's own signing in the
community-extensions pipeline). `cargo xtask e2e` builds the loadable by
invoking the toolchain (`make release`) and points the integration tests at
`build/release/extension/moraine/moraine.duckdb_extension`.

**e2e harness.** `cargo xtask e2e` downloads and caches the pinned DuckDB
CLI under `target/` (skipping the fetch once cached), redirects
`extension_directory` under `target/duckdb-extensions/` before `INSTALL
ducklake`/`LOAD ducklake` (also cached, never the CLI's home-directory
default), then drives the CLI with `-unsigned` — required because moraine
cannot sign the artifact at all (above) — through `LOAD`, `ATTACH`, listing,
and metadata-projection queries against the standalone attach, and through
`ducklake:moraine:` for the real DuckLake read/write round trip, against
stores seeded through the `moraine` API. Full mechanics, including every
build-time discovery, are recorded in `crates/moraine-duckdb/README.md`.

### The staged-transaction ABI surface

DuckLake's own `INSERT`/`UPDATE`/`DELETE` against `ducklake_*` tables
reach moraine through four C-ABI entry points (`moraine_abi.h`):
`moraine_tx_begin(catalog) -> tx`, `moraine_tx_stage(tx, table_kind,
operation_kind, cells)` (accumulates one typed row operation — `insert`, `delete`,
or `update_set_end` — without touching the store), `moraine_tx_commit(tx)
-> snapshot_id` (translates every staged operation into one atomic SlateDB
batch and returns the new head), and `moraine_tx_rollback(tx)` (discards
the accumulated operations, no store access). The C++ shim's `PlanInsert`/
`PlanUpdate`/`PlanDelete` (`cpp/catalog.cpp`, `cpp/staged_write.cpp`)
recognize exactly one target: a `ducklake_*` metadata table entry whose
spec names a writable `table_kind`; every other target — a real user table,
or a `ducklake_*` kind this crate does not model as a store entity — still
throws `NotImplementedException`, matching the "translate staged rows,
author nothing else" scope above. Per the staged-row rules: DuckLake
authors every id/counter/`schema_version`/`begin_snapshot` value, carried
across the ABI verbatim; the one interpreted convention is an `UPDATE`
setting `end_snapshot`, translated to a `current`-key delete plus `history`-key
write; a lost race at commit returns an error whose message contains the
literal substring `conflict` (never retried internally, per the C ABI
error mapping table above).

### `data_inlining_row_limit` is deliberately not synthesized, and dynamic inline-table interception

Beyond the keys the exists-probe path reads (version, encrypted,
created_by — see the metadata-catalog section below), the synthesized
`ducklake_metadata` serves **no** row for `data_inlining_row_limit`.
Inlining is on regardless: DuckLake's `WriteNewInlinedTables`
(source-verified) skips registering a table's per-schema-version
inlined-data table only when `DataInliningRowLimit(...)` resolves to 0,
and absent any served value that resolves to a compiled default of 10.
(An earlier revision served `"0"` to keep inlining off while RFC 0005 was
unimplemented, then `"10"` to make the choice legible.)

Serving it is worse than redundant. `DataInliningRowLimit` reads the
catalog's configuration options first and only then the DuckDB setting
`ducklake_default_data_inlining_row_limit`, and an
`ATTACH ... (DATA_INLINING_ROW_LIMIT n)` lands in that same option map —
so a served row outranks both knobs and shadows them silently, leaving an
operator with no way to raise the limit. Serving nothing keeps the same
default and keeps both knobs working; a store that wants a different
limit records a real option row, which overrides the default as any
stored option does.

With inlining on, DuckLake **dynamically creates and drives per-table
physical tables** in the metadata catalog rather than writing fixed
`ducklake_*` rows (RFC 0005's "Extension surface (as implemented)" has
the full operation → keyspace mapping). This shim recognizes two dynamic
name families by pattern, not by a fixed catalog-entry list — this is
the one place moraine's catalog lookup does more than serve the fixed
`ducklake_*` set B1 describes above:

- `ducklake_inlined_data_<table_id>_<schema_version>` — recognized once
  `moraine_inline_schemas` has a matching `(table_id, schema_version)`
  record (so a `CREATE TABLE IF NOT EXISTS` existence probe correctly
  reports "does not exist" before the first `CREATE`, and the same
  connection's own `LookupInlineTableEntry` accepts the `CREATE` that
  follows).
- `ducklake_inlined_delete_<table_id>` — recognized from the table's
  first inlined file-delete until the table is dropped. DuckLake probes
  this table's existence with `SELECT NULL FROM ... LIMIT 1` and treats a
  bind error as "does not exist", so unlike the data family this one must
  report missing for a real table_id until that first file-delete, or
  DuckLake's own existence discipline breaks. It must equally keep
  existing once emptied: `ducklake_flush_inlined_data` writes the table's
  deletions out as a real delete file and clears it with an unqualified
  `DELETE`, and DuckLake caches the table's existence for the life of the
  catalog rather than re-probing — so an existence derived from whether
  any deletion is currently held vanishes under it mid-session and every
  later bind fails. Existence is recorded in the store (RFC 0005's
  `inline/file_delete_table` marker) for exactly that reason.

Both route through the same staged-row commit path (`cpp/inline_tables.
cpp`) as the fixed `ducklake_*` tables, translating into `inline/*`
records — see RFC 0005 for the exact wire shape and the encoding
deviation from that RFC's Arrow-IPC design.

`ducklake_flush_inlined_data` and DuckLake's compaction/rewrite cleanup
paths also touch fixed `ducklake_*` tables beyond the entity
projections. `ducklake_files_scheduled_for_deletion` is served for real
(the `current/gcfile` schedule, written by expiry/compaction and drained
by `ducklake_cleanup_old_files`). The tables of still-unmodeled features
(variant-column stats, name/column mapping, macros) remain always-empty
stand-ins (`metadata_tables.cpp`) purely so DuckLake's generic cleanup
`DELETE`/`INSERT` batch — issued unconditionally as part of a commit
that removes or supersedes data files, not gated on any of these
features actually being in use — binds against an existing table instead
of failing the whole commit with a "table could not be found" error.
Raw `DELETE`s against tables with no delete translation plan as
void-deletes that throw if a row ever actually matches.

Two obligations of DuckLake's maintenance functions land here rather
than in the core. **Read-your-writes on the snapshot projection:**
`ducklake_expire_snapshots` stages its snapshot deletes and then
re-reads `ducklake_snapshot` in the same transaction (its dead-row rule
is `NOT EXISTS` over the survivors), so a scan of the snapshot tables
inside a write transaction with an open staged tx serves the
transaction's own view (`moraine_tx_dump_snapshots`), not committed
state. **Head-preserving maintenance commits:** an expiry or cleanup
transaction inserts no `ducklake_snapshot` row; the staged path commits
it as reclamation-only, minting no snapshot and leaving `sys/head`
untouched (RFC 0007).

### Standalone data-scan retirement

User-table *data* is served only through DuckLake now, matching the
Non-goals decision above: the standalone attach's own scan
(`MoraineTableEntry::GetScanFunction`, `cpp/scan.cpp`) still binds
unconditionally, so `DESCRIBE`/`EXPLAIN` keep working, but its
`init_global` — called once per query execution, not once per bind —
unconditionally throws `InvalidInputException`, naming the table and the
`ducklake:moraine:<store>` attach to use instead (`DATA_PATH` stays a
placeholder; this shim has no store-level source of truth for a lake-wide
data path). The message is deliberately built to exclude DuckLake's own
retry substrings (`conflict`/`unique`/`primary key`/`concurrent`, per the C
ABI error mapping table): this is a permanent redirect, not a race to
retry. The scan machinery this replaced — a nested `read_parquet` query
over resolved file paths, path-resolution rules, streaming, and a
column-count guard — is deleted outright; see
`crates/moraine-duckdb/README.md`'s "User-table data is served only
through DuckLake" section for the full account and the exact error text.

### A known upstream race: pin `threads=1` for DuckLake re-reads after a write

DuckLake's own catalog cache has a multi-threaded race, independent of
moraine: a fresh attach's catalog listing comes back **empty** right after
a write, under DuckDB's default multi-threaded query execution. Not a
moraine defect — the identical sequence reproduces against a plain
duckdb-file-backed DuckLake attach with zero moraine code in the chain, in
**23 of 40 runs** at the tracked version. It is also not `RENAME`-specific
as an earlier revision of this RFC had it: a plain `CREATE TABLE` is
enough. Dropping the workaround fails the e2e suite in roughly four runs
out of five.

`SET threads=1;` before the attach closes it deterministically, and every
session runner in `crates/moraine-duckdb/tests/ducklake_load/helpers.rs`
sets it for exactly that reason, not as a production recommendation.
moraine's own row-faithful projections independently verify that every
write this crate translates lands correctly regardless of whether
DuckLake's cache race fires on the read side.

The workaround costs every e2e session its parallelism, so a test asserts
the race's **presence** against the reference chain
(`the_upstream_ducklake_listing_race_still_needs_threads_1`). The
inversion is deliberate: without a canary nobody learns when the
workaround stopped being needed, and its failure is the signal to delete
both it and the `SET threads=1`.

## Alternatives considered

- **A2 — a standalone moraine `ATTACH` DuckDB catalog** in addition to
  the DuckLake surface. Originally deferred; since **adopted** as the first
  shipping surface (`ATTACH ... (TYPE moraine)`, see Non-goals) — not for a
  direct-query consumer, but because it proves the shim/ABI/core stack
  end-to-end with the least machinery while the DuckLake chaining
  ergonomics are still open.
- **B2 — semantic projection.** Store a re-modeled catalog form and project it
  into the `ducklake_*` views on read, translating writes back. Rejected: it
  couples moraine to DuckLake's exact SQL shapes and re-encodes logic DuckLake
  already owns, so a DuckLake change can silently misread. B1 keeps moraine
  faithful and evolution-robust. The e2e evidence this was waiting on now
  exists and does not call for it: the pinned access set (above) is thirty
  tables of straightforward per-kind scans and staged-row writes, with no
  filter pushed down and therefore no shape a re-modeled store would serve
  better. Revisit only if a *specific* pinned access pattern turns out to be
  expensive because of the row-faithful layout itself — not because the
  catalog is large, which is 0009's lazy-materialization question, and not
  because a scan clones, which is 0009's `dump_entities` item.
- **Raw-SQL interception.** moraine parses and answers the SQL DuckLake emits.
  Rejected: reimplements a query engine for no benefit; DuckDB's executor
  already does this over moraine's table scans.
- **Pure-Rust cdylib over the stable C extension API.** Rejected on
  feasibility: the C API exposes scalar/table functions only, not catalog
  registration. A read-only table-function surface cannot be DuckLake's
  writable catalog. A `StorageExtension` requires DuckDB's C++ internals.
- **Wire-impersonating an existing backend** (present as PostgreSQL/DuckDB over
  the wire so DuckLake attaches to moraine as one of its known types).
  Rejected: reimplements a wire protocol and still must satisfy the same
  metadata semantics — cost with no offsetting benefit, and brittle against
  protocol changes.

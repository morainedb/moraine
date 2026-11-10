---
title: Configuration
description: Attach options, table functions, and environment variables.
sidebar:
  order: 3
---

## Attach options

`READ_ONLY`, `READ_WRITE`, `DATA_PATH`, and `ENCRYPTED` are DuckDB's and
DuckLake's own options. Everything else reaches Moraine with a `META_`
prefix: `META_DATA_PATH`, `META_CACHE_DIR`, `META_FLUSH_INTERVAL_MS`, and
so on. The second table omits the prefix.

| DuckDB and DuckLake option | Default | Meaning |
|---|---|---|
| `DATA_PATH` | required once | Where DuckLake writes Parquet files. |
| `READ_WRITE` | on for local paths, off for remote | Open the writer. Required to create or write a lake on S3. |
| `READ_ONLY` | off for local paths, on for remote | Open a reader that follows the writer and never fences it. |
| `ENCRYPTED` | off | DuckLake encrypts each Parquet file with a key it stores in the catalog. |

| Moraine option | Default | Meaning |
|---|---|---|
| `DATA_PATH` | required once | Same value as `DATA_PATH`. Recorded in the catalog at creation; later attaches may omit both, and a different value is refused. |
| `CHECKPOINT` | unset | Pin a read-only attach to a SlateDB checkpoint id: a fixed cut that never sees later commits. Requires `READ_ONLY`. |
| `READER_POLL_INTERVAL_MS` | 10000 | How stale a read-only attach may be before it looks for new commits. |
| `FLUSH_INTERVAL_MS` | 100 | Minimum spacing between two write-ahead-log flushes. |
| `FLUSH_ON_COMMIT` | false | Flush on every commit, one PUT each, whatever the interval. |
| `WAL_PATH` | the catalog store | A separate object store for the write-ahead log. Recorded in the lake; every attach must name the same one. |
| `CACHE_DIR` | unset | Local directory for the block cache's disk tier. |
| `CACHE_SIZE` | 16 GiB | Byte cap on the disk tier. |
| `CACHE_MEMORY` | 640 MiB | Process-wide byte cap on the in-memory cache, shared by every attached store. |
| `CACHE_AUXILIARY_PERCENT` | 1.25, capped at 50 | Share of the memory budget for parsed Parquet metadata. |
| `CACHE_PUTS` | true | Admit a store's own writes into the cache as they land. |
| `CACHE_COMPACTION_PUTS` | true | Also admit compaction output. Set false if large merges evict what queries warmed. |
| `CACHE_PRELOAD` | `l0` | Warm the cache during attach: `none`, `l0` (SST metadata), or `all`. |
| `MAINTENANCE_*` | off | Enable a scheduled maintenance pass; see [Operating manual](../operating/#scheduled-maintenance). |

The cache options are process-wide. The first attach in a process sizes
the cache and later attaches share it, so set them on the first attach.

## Commit latency

A commit is durable once one write-ahead-log PUT reaches object storage.
`FLUSH_INTERVAL_MS` is the minimum spacing between two flushes. A commit
that arrives after the spacing has elapsed flushes at once and waits only
for its own PUT. One that arrives inside the spacing joins the next flush.
A burst therefore costs at most one spacing plus one PUT, and the PUT rate
never exceeds one per spacing.

Shorten the interval for a low commit rate that wants latency. Leave it for
a high rate, where the spacing merges many commits into one request.

```sql
ATTACH 'ducklake:moraine:s3://bucket/prefix' AS lake
  (READ_WRITE, META_FLUSH_INTERVAL_MS 5);
```

### The write-ahead log on a faster bucket

`WAL_PATH` puts the log, and nothing else, on a store of its own. An S3
Express One Zone directory bucket can then serve the one latency-critical
write while standard storage holds the rest:

```sql
CREATE SECRET wal (TYPE s3, KEY_ID '…', SECRET '…', REGION 'us-west-2',
                   SCOPE 's3://lake-wal--usw2-az1--x-s3');
ATTACH 'ducklake:moraine:s3://bucket/prefix' AS lake
  (READ_WRITE, META_WAL_PATH 's3://lake-wal--usw2-az1--x-s3');
```

The log store resolves its own `s3` secret. A bucket name ending in the
suffix AWS reserves for directory buckets (`--x-s3`) is addressed as S3
Express without further configuration. Commits live only in the log until
compaction writes them into the catalog store, so the log store must be as
durable as the catalog store; One Zone keeps data in a single availability
zone.

The lake records where its log lives. Every later attach, readers
included, must name the same store or none. To move an existing lake's
log, run `moraine_move_wal` with the lake attached nowhere:

```sql
SELECT from_wal_path, to_wal_path, moved
FROM moraine_move_wal('s3://bucket/prefix',
                      wal_path => 's3://lake-wal--usw2-az1--x-s3');
```

It drains the old log before recording the new store. Pass
`wal_path => NULL` to move the log back into the catalog store.

## Read-only attaches

A read-only attach follows the writer by pinning the checkpoint the writer
publishes with each commit, and writes nothing, so read-only credentials
suffice. Each transaction keeps the publication the previous one pinned for
up to `READER_POLL_INTERVAL_MS` before looking for a newer one, so that is
how far behind the writer a reader may be. A transaction may run for up to
fifteen minutes; a longer one fails and must be restarted.

For a fixed view that never moves, create a checkpoint on the writer and pin
readers to it:

```sql
-- On the writer.
SELECT checkpoint_id FROM moraine_create_checkpoint('lake');

-- From a process holding only s3:GetObject and s3:ListBucket.
ATTACH 'ducklake:moraine:s3://bucket/prefix' AS lake
  (READ_ONLY, META_CHECKPOINT '4a1f…');
```

A pinned reader sees a fixed cut and never sees later commits. Refreshing
means creating a new checkpoint and re-attaching. `moraine_checkpoints`
lists the checkpoints a store carries, and `moraine_delete_checkpoint`
releases one.

## Caching catalog reads

Moraine keeps one block cache per process. Catalog index metadata, which
every lookup reads first, stays in memory; data blocks fill the rest of the
memory budget and spill to `CACHE_DIR` when one is set.

```sql
ATTACH 'ducklake:moraine:s3://bucket/prefix' AS lake
  (READ_WRITE, META_CACHE_DIR '/var/cache/moraine',
   META_CACHE_MEMORY 1073741824, META_CACHE_SIZE 2147483648);
```

If the memory budget cannot hold a store's index metadata, the attach logs
a warning naming the shortfall, and lookups fetch metadata from object
storage. Raise `CACHE_MEMORY` on the first attach in the process.
`moraine_store_census('lake')` reports how a store's bytes are distributed,
and `moraine_cache_tally()` reports hits and misses per cache slot for the
process, or for one attach when given a lake name.

A fresh process starts cold. `CACHE_PRELOAD 'all'` reads the catalog
subspaces whole during attach, so the first query reads no catalog data
from object storage. It costs metadata-sized reads, not store-sized ones,
because index data blocks are not preloaded. If the store exceeds
`CACHE_SIZE`, the preload stops early and logs a warning.

`CACHE_MEMORY` is the number to weigh against DuckDB's `memory_limit` when
sizing a host. DuckDB's budget covers its buffers and Parquet cache; this
one covers Moraine's caches.

## Caching data files

Parquet data files are read by DuckDB, never by Moraine. DuckDB's external
file cache holds the ranges a query fetches under `memory_limit`, so a
repeat read of the same bytes costs no storage request. The cache dies
with the process.

DuckLake data files are immutable, so three DuckDB settings that are off
by default are safe and worth enabling in the session that attaches a
lake:

```sql
SET validate_external_file_cache = 'NO_VALIDATION';
SET parquet_metadata_cache = true;
SET enable_http_metadata_cache = true;
```

These are global, so Moraine does not set them from an attach.

For short-lived processes that start cold every time, the `cache_httpfs`
community extension caches S3 reads on local disk below the Parquet
reader:

```sql
INSTALL cache_httpfs FROM community;
LOAD cache_httpfs;
SET cache_httpfs_type = 'on_disk';
SET cache_httpfs_cache_directory = '/var/cache/duckdb-httpfs';
```

Its memory is outside DuckDB's `memory_limit` and outside Moraine's
`CACHE_MEMORY`, so a host running all three has three budgets to size.

## Table functions

`'lake'` is an attached catalog's name. `'<store>'` is the catalog's path,
for functions that run with the lake attached nowhere.

| Function | Purpose |
|---|---|
| `moraine_create_checkpoint('lake')` | Create a SlateDB checkpoint for pinned readers. |
| `moraine_checkpoints('<store>')` | List a store's checkpoints. |
| `moraine_delete_checkpoint('<store>', '<id>')` | Release a checkpoint. |
| `moraine_move_wal('<store>', wal_path => …)` | Move a lake's write-ahead log to another store. |
| `moraine_maintenance('lake')` | Run one maintenance pass now. |
| `moraine_maintenance_status('lake')` | Report the retained maintenance passes. |
| `moraine_compact_store('lake')` | Run a SlateDB compaction of the catalog store. |
| `moraine_store_census('lake')` | Report bytes per subspace and cache usage. |
| `moraine_cache_tally([lake])` | Cache hits and misses, per process or per attach. |
| `moraine_cache_status()` | Current cache budgets and occupancy. |
| `moraine_memory_tally('lake')` | Working-memory reservations. |
| `moraine_object_store_tally('lake')` | Object-store requests by kind. |
| `moraine_sidecar_tally('lake')` | Hits and misses on the row summaries published beside data files. |
| `moraine_indexes('lake', 'schema', 'table')` | List a table's indexes with their state. |
| `moraine_index_verify('lake', 'schema', 'table', 'index')` | Check an index against its table. |
| `moraine_migrate('<store>')` | Run a pending on-disk format migration; see [Upgrading](../operating/#upgrading). |
| `moraine_raise_format('lake', dry_run := …)` | Raise a store's format version. |
| `moraine_raise_catalog_version('lake', dry_run => …, confirm => …)` | Raise the DuckLake catalog version from 1.0 to 1.1. |

Indexes are created with DuckDB's own `CREATE INDEX` and `DROP INDEX`; see
[Index a column](../getting-started/#index-a-column). The
[design document](/rfcs/0009-equality-indexes/) lists the lookup functions.

## Environment variables

These cap Moraine's working memory per process, separately from the cache
budget. An operation that would exceed its cap fails instead of growing.

| Variable | Default | Scope |
|---|---|---|
| `MORAINE_WORKING_MEMORY_BYTES` | 1 GiB | Total working allowance for the process. |
| `MORAINE_STAGING_MEMORY_BYTES` | 256 MiB | Rows staged by one transaction. |
| `MORAINE_COMMIT_MEMORY_BYTES` | 256 MiB | One commit's batch. |
| `MORAINE_STORE_MEMORY_BYTES` | 1 GiB | SlateDB's unflushed writes. |
| `MORAINE_SUMMARY_MEMORY_BYTES` | 256 MiB | One file's row-summary build. |
| `MORAINE_HOST_MEMORY_BYTES` | unset | The host's memory, checked against the budgets before attach. |
| `MORAINE_HOST_HEADROOM_BYTES` | a quarter of the host value | Memory to leave unclaimed on the host. |
| `MORAINE_LOG` | unset | Log filter for Moraine's own records. |

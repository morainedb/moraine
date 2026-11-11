---
title: Configuration
description: Settings, attach options, table functions, and environment variables.
sidebar:
  order: 3
---

Use `SET` for process-wide memory and cache settings before the first
attach. Use `ATTACH` options to configure individual lakes.

## Settings

Sizes accept units such as `'512MB'` and `'1GiB'`, like DuckDB's
`memory_limit`. The first attach fixes these settings for the process.
To change them, restart the process and set the new values before attaching.

```sql
SET moraine_cache_dir = '/var/cache/moraine';
SET moraine_cache_memory = '1GiB';
SET moraine_working_memory = '2GiB';
ATTACH 'ducklake:moraine:s3://bucket/prefix' AS lake (READ_WRITE);
```

| Setting | Default | Meaning |
|---|---|---|
| `moraine_cache_dir` | unset | Local directory for the block cache's disk tier. |
| `moraine_cache_size` | 16 GiB | Byte cap on the disk tier. |
| `moraine_cache_memory` | 640 MiB | Byte cap on the in-memory block cache, shared by every attached store. |
| `moraine_cache_auxiliary_percent` | 1.25, capped at 50 | Share of the memory budget for parsed Parquet metadata. |
| `moraine_working_memory` | 1 GiB | Live working allocations. An operation that would exceed it fails instead of growing. |
| `moraine_staging_memory` | 256 MiB | Rows staged by one transaction. |
| `moraine_commit_memory` | 256 MiB | One commit's batch. |
| `moraine_store_memory` | 1 GiB | SlateDB's unflushed writes per writer. |
| `moraine_summary_memory` | 256 MiB | One file's row-summary build. |
| `moraine_host_memory` | unset | The host's memory. Setting it makes every attach check that DuckDB's `memory_limit`, the budgets above, and the headroom fit. |
| `moraine_host_headroom` | a quarter of the host value | Memory to leave unclaimed on the host. |

`moraine_cache_memory` and `moraine_working_memory` are the two numbers to
weigh against DuckDB's `memory_limit` when sizing a host: DuckDB's budget
covers its buffers and Parquet cache; these cover Moraine's cache and its
working allocations.

## Attach options

`READ_ONLY`, `READ_WRITE`, `DATA_PATH`, and `ENCRYPTED` are DuckDB's and
DuckLake's own options. Everything else reaches Moraine with a `META_`
prefix.

| DuckDB and DuckLake option | Default | Meaning |
|---|---|---|
| `DATA_PATH` | required once | Where DuckLake writes Parquet files. |
| `READ_WRITE` | on for local paths, off for remote | Open the writer. Required to create or write a lake on S3. |
| `READ_ONLY` | off for local paths, on for remote | Open a reader that follows the writer and never fences it. |
| `ENCRYPTED` | off | DuckLake encrypts each Parquet file with a key it stores in the catalog. |

| Moraine option | Default | Meaning |
|---|---|---|
| `META_DATA_PATH` | the recorded root | Normally omitted: Moraine adopts the `DATA_PATH` DuckLake records on the first attach. A value differing from the recorded one is refused. |
| `META_CHECKPOINT` | unset | Pin a read-only attach to a SlateDB checkpoint id: a fixed cut that never sees later commits. Requires `READ_ONLY`. |
| `META_READER_POLL_INTERVAL_MS` | 10000 | How stale a read-only attach may be before it looks for new commits. |
| `META_FLUSH_INTERVAL_MS` | 100 | Minimum spacing between two write-ahead-log flushes. |
| `META_FLUSH_ON_COMMIT` | false | Flush on every commit, one PUT each, whatever the interval. |
| `META_WAL_PATH` | the catalog store | A separate object store for the write-ahead log. Recorded in the lake; every attach must name the same one. |
| `META_CACHE_PUTS` | true | Admit a store's own writes into the cache as they land. |
| `META_CACHE_COMPACTION_PUTS` | true | Also admit compaction output. Set false if large merges evict what queries warmed. |
| `META_CACHE_PRELOAD` | `l0` | Warm the cache during attach: `none`, `l0` (SST metadata), or `all`. |
| `META_MAINTENANCE_*` | off | Enable a scheduled maintenance pass; see [Operating manual](../operating/#scheduled-maintenance). |

Older attaches may still name `META_CACHE_DIR`, `META_CACHE_SIZE`,
`META_CACHE_MEMORY`, and `META_CACHE_AUXILIARY_PERCENT`; each stands in for
its setting on the first attach and is otherwise ignored.

## Commit latency

A commit is durable once one write-ahead-log PUT reaches object storage.
`META_FLUSH_INTERVAL_MS` is the minimum spacing between two flushes. A commit
that arrives after the spacing has elapsed flushes at once and waits only
for its own PUT. One that arrives inside the spacing joins the next flush.
A burst therefore costs at most one spacing plus one PUT, and the PUT rate
never exceeds one per spacing.

A shorter interval reduces the wait between flushes. A longer interval
groups more concurrent commits into each request.

```sql
ATTACH 'ducklake:moraine:s3://bucket/prefix' AS lake
  (READ_WRITE, META_FLUSH_INTERVAL_MS 5);
```

### The write-ahead log on a faster bucket

`META_WAL_PATH` puts the write-ahead log on a separate object store. An S3
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
up to `META_READER_POLL_INTERVAL_MS` before looking for a newer one, so that is
how far behind the writer a reader may be. A transaction may run for up to
fifteen minutes; a longer one fails and must be restarted.

To read a fixed snapshot, create a checkpoint on the writer and attach
readers to it:

```sql
-- On the writer.
SELECT checkpoint_id FROM moraine_create_checkpoint('lake');

-- From a process holding only s3:GetObject and s3:ListBucket.
ATTACH 'ducklake:moraine:s3://bucket/prefix' AS lake
  (READ_ONLY, META_CHECKPOINT '4a1f…');
```

To see later commits, create a new checkpoint and re-attach.
`moraine_checkpoints` lists the checkpoints a store carries, and
`moraine_delete_checkpoint` releases one.

## Caching catalog reads

Moraine keeps one block cache per process. Catalog index metadata, which
every lookup reads first, stays in memory; data blocks fill the rest of the
memory budget and spill to `moraine_cache_dir` when one is set.

```sql
SET moraine_cache_dir = '/var/cache/moraine';
SET moraine_cache_memory = '1GiB';
SET moraine_cache_size = '2GiB';
ATTACH 'ducklake:moraine:s3://bucket/prefix' AS lake (READ_WRITE);
```

If the memory budget cannot hold a store's index metadata, the attach logs
a warning naming the shortfall, and lookups fetch metadata from object
storage. Raise `moraine_cache_memory` before the first attach in the
process.
`moraine_store_census('lake')` reports how a store's bytes are distributed,
and `moraine_cache_tally()` reports hits and misses per cache slot for the
process, or for one attach when given a lake name.

A fresh process starts cold. `META_CACHE_PRELOAD 'all'` reads the catalog
subspaces whole during attach, so the first query reads no catalog data
from object storage. It costs metadata-sized reads, not store-sized ones,
because index data blocks are not preloaded. If the store exceeds
`moraine_cache_size`, the preload stops early and logs a warning.

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
`moraine_cache_memory`, so a host running all three has three budgets to
size.

## Table functions

`'lake'` is an attached catalog's name. `'<store>'` is the catalog's path.
Migration and WAL relocation require the lake to be detached from every
process.

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

| Variable | Default | Scope |
|---|---|---|
| `MORAINE_LOG` | unset | Log filter for Moraine's own records. |

The memory settings above can also be given as environment variables, for
a process that embeds the Rust crate rather than the extension:
`MORAINE_WORKING_MEMORY_BYTES`, `MORAINE_STAGING_MEMORY_BYTES`,
`MORAINE_COMMIT_MEMORY_BYTES`, `MORAINE_STORE_MEMORY_BYTES`,
`MORAINE_SUMMARY_MEMORY_BYTES`, `MORAINE_HOST_MEMORY_BYTES`, and
`MORAINE_HOST_HEADROOM_BYTES`, each a byte count. A setting takes
precedence over its variable.

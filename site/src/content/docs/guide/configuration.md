---
title: Configuration
description: Settings, attach options, table functions, and environment variables.
sidebar:
  order: 3
---

Moraine can be configured using `SET` commands for process-wide memory and cache
settings before the first attach. `ATTACH` options configure settings for
individual lakes.

## Settings

Sizes accept units such as `'512MB'` and `'1GiB'`, like DuckDB's `memory_limit`.
The first attach fixes these settings for the process. To change them, restart
the process and set the new values before attaching a lake.

```sql
SET moraine_cache_dir = '/var/cache/moraine';
SET moraine_cache_memory = '1GiB';
SET moraine_working_memory = '2GiB';
ATTACH 'ducklake:moraine:s3://bucket/prefix' AS lake (READ_WRITE);
```

| Setting | Default | Meaning |
|---|---|---|
| `moraine_cache_dir` | Unset | Local directory for the block cache's disk tier. |
| `moraine_cache_size` | 16 GiB | Byte cap on the disk tier. |
| `moraine_cache_memory` | 640 MiB | Byte cap on the in-memory block cache, shared by every attached store. |
| `moraine_cache_auxiliary_percent` | 1.25% | Share of the memory budget for parsed Parquet metadata (capped at 50%). |
| `moraine_working_memory` | 1 GiB | Live working allocations. An operation that would exceed it fails instead of growing. |
| `moraine_staging_memory` | 256 MiB | Rows staged by one transaction. |
| `moraine_commit_memory` | 256 MiB | One commit's batch. |
| `moraine_store_memory` | 1 GiB | SlateDB's unflushed writes per writer. |
| `moraine_summary_memory` | 256 MiB | One file's row-summary build. |
| `moraine_host_memory` | Unset | The host's memory. Setting it makes every attach check that DuckDB's `memory_limit`, the budgets above, and the headroom fit. |
| `moraine_host_headroom` | 25%  | Memory to leave unclaimed on the host. |

`moraine_cache_memory` and `moraine_working_memory` should be calibrated to
DuckDB's `memory_limit` when sizing a host. They cover Moraine's cache and its
working allocations, while DuckDB's `memory_limit` covers its buffers and
Parquet cache.

## Attach options

`READ_ONLY`, `READ_WRITE`, `DATA_PATH`, and `ENCRYPTED` are DuckDB's and
DuckLake's own options. Everything else reaches Moraine with a `META_`
prefix.

| DuckDB and DuckLake option | Default | Meaning |
|---|---|---|
| `DATA_PATH` | Required | Where DuckLake writes Parquet files. |
| `READ_WRITE` | On for local paths, off for remote | Open the writer. Required to create or write a lake on S3. |
| `READ_ONLY` | Off for local paths, on for remote | Open a reader that follows the writer and never fences it. |
| `ENCRYPTED` | Off | DuckLake encrypts each Parquet file with a key it stores in the catalog. |

| Moraine option | Default | Meaning |
|---|---|---|
| `META_CHECKPOINT` | Unset | Pin a read-only attach to a SlateDB checkpoint id: a fixed cut that never sees later commits. Requires `READ_ONLY`. |
| `META_READER_POLL_INTERVAL_MS` | 10000 | How stale a read-only attach may be before it looks for new commits. |
| `META_FLUSH_INTERVAL_MS` | 100 | Minimum spacing between two write-ahead-log flushes. |
| `META_FLUSH_ON_COMMIT` | false | Flush on every commit, one PUT each, whatever the interval. |
| `META_WAL_PATH` | The catalog store | A separate object store for the write-ahead log. Recorded in the lake; every attach must name the same one. |
| `META_CACHE_PUTS` | True | Admit a store's own writes into the cache as they land. |
| `META_CACHE_COMPACTION_PUTS` | True | Also admit compaction output. Set false if large merges evict what queries warmed. |
| `META_CACHE_PRELOAD` | `l0` | Warm the cache during attach: `none`, `l0` (SST metadata), or `all`. |
| `META_MAINTENANCE_*` | Off | Enable a scheduled maintenance pass; see [Operating manual](../operating/#scheduled-maintenance). |

## Commit latency

A commit is durable once one write-ahead-log PUT reaches object storage.
`META_FLUSH_INTERVAL_MS` is the minimum spacing between two flushes. A commit
that arrives after the spacing elapsed flushes and waits only for its own PUT.
One that arrives inside the spacing interval joins the next flush. A burst
therefore costs at most one spacing plus one PUT and the PUT rate never exceeds
one per spacing.

```sql
ATTACH 'ducklake:moraine:s3://bucket/prefix' AS lake
  (READ_WRITE, META_FLUSH_INTERVAL_MS 5);
```

### The write-ahead log on a faster and cheaper bucket

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
Express without further configuration.

A commit is acknowledged once its log write is durable. It lives only in the log
until the next flush writes it into the catalog store. Losing the log store
therefore loses the commits since the last flush. S3 Express One Zone should be
used only when that is acceptable.

To move an existing lake's log, run `moraine_move_wal` with the lake attached
nowhere:

```sql
SELECT from_wal_path, to_wal_path, moved
FROM moraine_move_wal('s3://bucket/prefix',
                      wal_path => 's3://lake-wal--usw2-az1--x-s3');
```

It drains the old log before recording the new store. Pass
`wal_path => NULL` to move the log back into the catalog store.

## Read-only attaches

A read-only attach follows the writer by pinning the checkpoint the writer
publishes with each commit. Each transaction keeps the publication the previous
one pinned for up to `META_READER_POLL_INTERVAL_MS` before looking for a newer
one, so that is how far behind the writer a reader may be. Checkpoints expire
after 15 minutes, so long-running transactions may fail.

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

Moraine keeps one block cache per process, not per attach. Catalog index
metadata stays in memory; data fills the rest of the memory budget and spills to
`moraine_cache_dir` when set.

```sql
SET moraine_cache_dir = '/var/cache/moraine';
SET moraine_cache_memory = '1GiB';
SET moraine_cache_size = '2GiB';

ATTACH 'ducklake:moraine:s3://bucket/prefix' AS lake (READ_WRITE);
```

A new process starts with an empty cache. By default the attach loads only
the objects written since the last compaction; `META_CACHE_PRELOAD 'all'`
loads the whole store, which takes longer but means the first query reads no
catalog data from object storage. A store larger than
`moraine_cache_size` is loaded up to the cap, with a warning in the log.

If the memory budget cannot hold a store's index metadata, the attach logs a
warning and lookups fetch metadata from object storage. Raise
`moraine_cache_memory` to fix. `moraine_store_census('lake')` reports how a
store's bytes are distributed, and `moraine_cache_tally()` reports hits and
misses per cache slot for the process.

## Caching data files

Parquet data files are read directly by DuckDB. DuckDB's external
file cache holds the ranges a query fetches under `memory_limit`.

DuckLake data files are immutable, so three DuckDB settings that are off
by default are safe and worth enabling alongside the cache:

```sql
SET validate_external_file_cache = 'NO_VALIDATION';
SET parquet_metadata_cache = true;
SET enable_http_metadata_cache = true;
```

To avoid cold starts, the `cache_httpfs` community extension can cache S3 reads
on local disk as well:

```sql
INSTALL cache_httpfs FROM community;
LOAD cache_httpfs;

SET cache_httpfs_type = 'on_disk';
SET cache_httpfs_cache_directory = '..';
```

## Table functions

`'<lake>'` is an attached catalog's name. `'<store>'` is the catalog's path.
Migration, WAL relocation, and the data root rewrite require the lake to be
detached from every process.

| Function | Purpose |
|---|---|
| `moraine_create_checkpoint('<lake>')` | Create a SlateDB checkpoint for pinned readers. |
| `moraine_checkpoints('<store>')` | List a store's checkpoints. |
| `moraine_delete_checkpoint('<store>', '<id>')` | Release a checkpoint. |
| `moraine_move_wal('<store>', wal_path => …)` | Move a lake's write-ahead log to another store. |
| `moraine_rewrite_data_path('<store>', '<old root>', '<new root>')` | Point a copied lake at its new data root; see [Backups and copies](../operating/#backups-and-copies). |
| `moraine_maintenance('<lake>')` | Run one maintenance pass now. |
| `moraine_maintenance_status('<lake>')` | Report the retained maintenance passes. |
| `moraine_compact_store('<lake>')` | Run a SlateDB compaction of the catalog store. |
| `moraine_store_census('<lake>')` | Report bytes per subspace and cache usage. |
| `moraine_cache_tally([<lake>])` | Cache hits and misses, per process or per attach. |
| `moraine_cache_status()` | Current cache budgets and occupancy. |
| `moraine_memory_tally('<lake>')` | Working-memory reservations. |
| `moraine_object_store_tally('<lake>')` | Object-store requests by kind. |
| `moraine_sidecar_tally('<lake>')` | Hits and misses on the row summaries published beside data files. |
| `moraine_indexes('<lake>', '<schema>', '<table>')` | List a table's indexes with their state. |
| `moraine_index_verify('<lake>', '<schema>', '<table>', '<index>')` | Check an index against its table. |
| `moraine_migrate('<store>')` | Run a pending on-disk format migration; see [Upgrading](../operating/#upgrading). |
| `moraine_raise_format('<lake>', dry_run := …)` | Raise a store's format version. |
| `moraine_raise_catalog_version('<lake>', dry_run => …, confirm => …)` | Raise the DuckLake catalog version from 1.0 to 1.1. |

Indexes are created with DuckDB's standard `CREATE INDEX` and `DROP INDEX`; see
[Index a column](../getting-started/#index-a-column).

## Environment variables

| Variable | Default | Scope |
|---|---|---|
| `MORAINE_LOG` | Unset | Log filter for Moraine's own records. |

The memory budgets can also be given as environment variables:
`MORAINE_WORKING_MEMORY_BYTES`, `MORAINE_STAGING_MEMORY_BYTES`,
`MORAINE_COMMIT_MEMORY_BYTES`, `MORAINE_STORE_MEMORY_BYTES`,
`MORAINE_SUMMARY_MEMORY_BYTES`, `MORAINE_HOST_MEMORY_BYTES`, and
`MORAINE_HOST_HEADROOM_BYTES`, each a plain byte count with no unit.

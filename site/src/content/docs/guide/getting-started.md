---
title: Getting started
description: Load the extension and attach your first moraine-backed lake.
sidebar:
  order: 2
---

## Install the extension

Moraine is not yet in the DuckDB community extension repository, so the
extension loads unsigned. Grab `moraine.<platform>.duckdb_extension` (e.g.
`moraine.osx_arm64.duckdb_extension`) from the latest `ext-v*` entry on the
[releases page](https://github.com/morainedb/moraine/releases), then start
DuckDB with `-unsigned` (the setting cannot be changed on a running
database):

```sh
duckdb -unsigned
```

```sql
LOAD 'path/to/moraine.osx_arm64.duckdb_extension';
INSTALL ducklake;
```

Once moraine is published as a community extension this becomes
`INSTALL moraine FROM community; LOAD moraine;`.

## Attach a lake

A local lake needs nothing but paths:

```sql
ATTACH 'ducklake:moraine:/tmp/demo-lake' AS lake
  (DATA_PATH '/tmp/demo-lake-data/', META_DATA_PATH '/tmp/demo-lake-data/');

CREATE TABLE lake.events (id BIGINT, payload VARCHAR);
INSERT INTO lake.events VALUES (1, 'hello, bucket');
SELECT * FROM lake.events;
```

Pass **both** path options, with the same value. `DATA_PATH` is DuckLake's:
where Parquet files go. `META_DATA_PATH` is forwarded to moraine and
**recorded in the catalog** at bootstrap — after that, re-attaches can omit
`DATA_PATH` entirely, and operations that need the data root (like indexing
a table that already holds data) work. The recorded value is fixed: an
attach supplying a conflicting `META_DATA_PATH` is refused rather than
silently diverging.

## S3 lakes need READ_WRITE

DuckDB opens any attach whose path starts with a remote prefix (`s3://`,
`gcs://`, `azure://`, …) **read-only by default**, and a read-only attach
cannot create a catalog. To create or write a lake on S3, say `READ_WRITE`:

```sql
CREATE SECRET s (TYPE s3, KEY_ID '…', SECRET '…', REGION 'us-west-2');
ATTACH 'ducklake:moraine:s3://bucket/prefix' AS lake
  (DATA_PATH 's3://bucket/prefix-data/',
   META_DATA_PATH 's3://bucket/prefix-data/', READ_WRITE);
```

Once the lake is bootstrapped, a later writer attach needs only the flag —
the data path is served back from the catalog:

```sql
ATTACH 'ducklake:moraine:s3://bucket/prefix' AS lake (READ_WRITE);
```

## One writer, many readers

A moraine lake is **single-writer, many-readers**. The selector is DuckDB's
standard attach flag — no moraine-specific grammar:

- **Read-write** (the default for local paths) opens the one SlateDB
  writer. Exactly one process should attach read-write at a time.
- **`READ_ONLY`** opens a follower that serves consistent snapshots and
  tracks the writer's commits, and never becomes a writer:

```sql
ATTACH 'ducklake:moraine:s3://bucket/prefix' AS lake (READ_ONLY);
```

Take the one-writer rule seriously: SlateDB fencing means the *newest*
writer wins, so a second read-write attach doesn't fail — it fences the
incumbent's committer. Every process past the first should attach
`READ_ONLY`.

## Faster repeat queries on S3

moraine keeps one block cache per process, shared by every store the process
attaches. It holds SST metadata — the indexes and filters every lookup walks
first — in memory, and data blocks in memory over an optional disk tier.

Give it a directory and the data blocks spill there, so a warm working set
survives more than memory alone would hold:

```sql
ATTACH 'ducklake:moraine:s3://bucket/prefix' AS lake
  (READ_WRITE, META_CACHE_DIR '/var/cache/moraine');
```

Two byte counts size it, both for the whole process rather than per store:
`META_CACHE_MEMORY` caps memory across both halves, `META_CACHE_SIZE` caps the
directory (16 GiB if unset).

```sql
ATTACH 'ducklake:moraine:s3://bucket/prefix' AS lake
  (READ_WRITE, META_CACHE_DIR '/var/cache/moraine',
   META_CACHE_MEMORY 1073741824, META_CACHE_SIZE 2147483648);
```

`META_CACHE_MEMORY` is the number to weigh against DuckDB's own `memory_limit`
when sizing a host: DuckDB's budget covers its buffers and its Parquet cache,
and this is the one memory consumer beside it.

SST filters and indexes share that budget with data blocks, and every point
read walks them before it can reach a block. They are not given a fixed
slice: metadata is evicted only once no data block is left to evict, so a
scan cannot push it out, and metadata a store does not need is space blocks
may use. What is capped is how much stays protected — most of the budget.

A store whose metadata outgrows that turns each probe into an S3 fetch,
worst for keys that are absent, which consult every filter on the path and
touch no data block at all. The attach warns when it happens, naming the
shortfall:

```text
WARN the metadata cache cannot hold this store's SST filters and indexes,
     so probes will fetch them from object storage
     metadata_bytes=625189215 metadata_capacity_bytes=125829120
```

Raise `META_CACHE_MEMORY` on the **first** attach in the process — the cache
is process-wide and the first attach builds it. `moraine_store_census`
reports the same filter and index bytes per subspace if you want to see
where the weight sits before setting a number.

By default the cache fills only as queries read, so the indexes and filters
of an SST the writer just flushed are fetched back from S3 the first time
something reads them. `META_CACHE_PUTS true` admits them as they are written
instead — metadata only, since a data block the writer produced is not one a
reader is likely to want next:

```sql
ATTACH 'ducklake:moraine:s3://bucket/prefix' AS lake
  (READ_WRITE, META_CACHE_DIR '/var/cache/moraine', META_CACHE_PUTS true);
```

It is off by default because compaction output goes through the same policy,
and a large merge can evict what queries had warmed.

A fresh process still starts cold, and the first query pays every first touch.
`META_CACHE_PRELOAD` moves that cost into the ATTACH — `'l0'` warms every
subspace's SST metadata, `'all'` additionally reads the catalog subspaces
whole, `'none'` (the default) warms nothing:

```sql
ATTACH 'ducklake:moraine:s3://bucket/prefix' AS lake
  (READ_WRITE, META_CACHE_DIR '/var/cache/moraine', META_CACHE_PRELOAD 'all');
```

Neither level pulls the index subspace's data blocks, which on an
index-carrying store is most of its bytes — so `'all'` costs metadata-sized
reads, not store-sized ones, and equality lookups stay one fetch per block
behind the filters it just warmed. `moraine_store_census` reports how the
bytes are distributed.

The first attach in a process sizes the cache; later attaches share what it
built. On a host that attaches several catalogs, set these on the first one.

`moraine_cache_tally()` reports what that cache has served — hits and misses
per slot, with a rate beside each — so the budgets can be set from measured
curves rather than the defaults. Since one cache serves every attach, those
numbers are the host's; pass a lake name to see one attach's share of them:

```sql
SELECT * FROM moraine_cache_tally();        -- the process
SELECT * FROM moraine_cache_tally('lake');  -- one attach
```

A rate is NULL rather than zero before anything has been looked up. On a host
attaching several catalogs, the per-lake form is what says which of them is
spending the budget the process-wide form reports.

## Caching the data files

The cache above is the *catalog's*. Parquet data files are read by DuckDB
itself, and moraine never touches a data byte or adds a cache of its own
for them.

DuckDB caches them for you. A lake read goes through DuckDB's built-in
external file cache, so the data ranges a query fetches are held under
`memory_limit` and a repeat read of the same bytes costs no storage
request at all. One caveat worth knowing when you measure it: data-range
caching rides on the Parquet reader's prefetch, which is used for remote
files but not for files on local disk, so a local `DATA_PATH` caches only
footers. A deployment reading `s3://` takes the remote path.

That cache is memory, and it dies with the process — see below if you need
warmth to survive a restart.

DuckLake data files are immutable once written, so a process serving repeat
queries wants three DuckDB settings that are off by default:

```sql
SET validate_external_file_cache = 'NO_VALIDATION';
SET parquet_metadata_cache = true;
SET enable_http_metadata_cache = true;
```

These are global, so moraine will not set them from an ATTACH — that would
reach into every other database in the process. Set them in the session that
attaches the lake.

### Keeping S3 data files cached across processes with cache_httpfs

The built-in cache above serves repeat reads within a live process. If
your processes are short-lived — a serverless handler, a redeploy every
few minutes — every one of them starts cold, and a cache on disk is what
survives. The `cache_httpfs` community extension replaces the `s3://`
filesystem, caching below the reader and outliving the instance:

```sql
INSTALL cache_httpfs FROM community;
LOAD cache_httpfs;

SET cache_httpfs_type = 'on_disk';
SET cache_httpfs_cache_directory = '/var/cache/duckdb-httpfs';
SET cache_httpfs_cache_block_size = 524288;             -- 512 KiB
-- Keep 8 GiB of the volume free; the cache evicts to stay under it.
SET cache_httpfs_min_disk_bytes_for_cache = 8589934592;
-- Cache reads run on DuckDB's scheduler, so `SET threads` caps them too.
SET cache_httpfs_parallel_read_mode = 'duckdb_task_scheduler';
```

**Budget its memory separately from `memory_limit`.** This cache is not
DuckDB's and does not answer to DuckDB's budget, and even in `on_disk`
mode it keeps a read-through memory cache. Two knobs bound it, and both
multiply by the block size:

```sql
-- on_disk mode: the read-through cache in front of the cache files.
--   256 blocks x 512 KiB = 128 MiB
SET cache_httpfs_disk_cache_reader_mem_cache_block_count = 256;

-- in_mem mode (cache_httpfs_type = 'in_mem'): the cache itself.
--   256 blocks x 512 KiB = 128 MiB
SET cache_httpfs_max_in_mem_cache_block_count = 256;
```

So a host running this alongside moraine has three memory consumers to
size, not one: DuckDB's `memory_limit`, moraine's `META_CACHE_MEMORY`,
and cache_httpfs's block count times its block size. Only the first two
are visible to DuckDB. Set the block counts before the first read —
they take effect once.

Two smaller notes. `cache_httpfs_max_in_mem_cache_block_count` must be
set before any filesystem access to take effect. And DuckLake never
rewrites a data file, so the default
`cache_httpfs_enable_cache_validation = false` is safe here — the file
under a cached block cannot have changed.

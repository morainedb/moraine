---
title: Getting started
description: Install the extension and attach a lake.
sidebar:
  order: 2
---

## Install the extension

Moraine is not yet in DuckDB's community extension repository, so it loads
unsigned. Download `moraine.<platform>.duckdb_extension` from the latest
`ext-v*` entry on the
[releases page](https://github.com/morainedb/moraine/releases), start
DuckDB with `-unsigned`, and load it:

```sh
duckdb -unsigned
```

```sql
LOAD 'path/to/moraine.osx_arm64.duckdb_extension';
INSTALL ducklake;
```

## Attach a local lake

```sql
ATTACH 'ducklake:moraine:/tmp/demo-lake' AS lake
  (DATA_PATH '/tmp/demo-lake-data/', META_DATA_PATH '/tmp/demo-lake-data/');

CREATE TABLE lake.events (id BIGINT, payload VARCHAR);
INSERT INTO lake.events VALUES (1, 'hello, bucket');
SELECT * FROM lake.events;
```

Pass both path options with the same value. `DATA_PATH` tells DuckLake
where Parquet files go. `META_DATA_PATH` is forwarded to moraine and
recorded in the catalog when the lake is created. After that, attaches
may omit `DATA_PATH`, and an attach that supplies a different
`META_DATA_PATH` is refused.

## Attach a lake on S3

DuckDB opens remote paths read-only by default, and a read-only attach
cannot create a catalog. Add `READ_WRITE` to create or write a lake on S3:

```sql
CREATE SECRET s (TYPE s3, KEY_ID '…', SECRET '…', REGION 'us-west-2');
ATTACH 'ducklake:moraine:s3://bucket/prefix' AS lake
  (DATA_PATH 's3://bucket/prefix-data/',
   META_DATA_PATH 's3://bucket/prefix-data/', READ_WRITE);
```

Credentials come from DuckDB secrets, never from attach options. Once the
lake exists, a writer attach needs only the flag:

```sql
ATTACH 'ducklake:moraine:s3://bucket/prefix' AS lake (READ_WRITE);
```

## One writer, many readers

One process attaches read-write. Every other process attaches
`READ_ONLY`:

```sql
ATTACH 'ducklake:moraine:s3://bucket/prefix' AS lake (READ_ONLY);
```

A second read-write attach does not fail. SlateDB gives the writer role to
the newest writer, and the previous writer's next commit fails with a
fencing error. Nothing is corrupted, but the first writer must re-attach.

A read-only attach writes a checkpoint into the store's manifest so it can
follow the writer's commits, so its credentials still need write access to
the catalog bucket. Readers with strictly read-only credentials pin a
checkpoint created ahead of time; see `CHECKPOINT` in
[Configuration](../configuration/#read-only-attaches).

## Next

- [Configuration](../configuration/) lists every attach option, with
  sections on commit latency and caching.
- [Operating a lake](../operating/) covers retention, scheduled
  maintenance, and encryption.

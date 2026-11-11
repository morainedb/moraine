---
title: Getting started
description: Install the extension and attach a lake.
sidebar:
  order: 2
---

## Install the extension

Moraine is not yet in DuckDB's community extension repository, so it loads
unsigned; `INSTALL moraine FROM community` is on the [roadmap](/roadmap/).
Each build loads only into the DuckDB version it names, so check
`SELECT version()`, then take the matching
`moraine.<duckdb-version>.<platform>.duckdb_extension` from the
[compatibility table](/releases/#compatibility) on the Releases page. Start
DuckDB with `-unsigned` and load it:

```sh
duckdb -unsigned
```

```sql
LOAD 'path/to/moraine.v2.0.0.osx_arm64.duckdb_extension';
```

DuckLake is bundled with the extension; there is nothing else to install.
No AWS account is needed to try it: the next section uses a local
directory.

## Attach a local lake

```sql
ATTACH 'ducklake:moraine:/tmp/demo-lake' AS lake (DATA_PATH '/tmp/demo-lake-data/');

CREATE TABLE lake.events (id BIGINT, payload VARCHAR);
INSERT INTO lake.events VALUES (1, 'hello, lake');
SELECT * FROM lake.events;
```

`DATA_PATH` is where DuckLake writes Parquet. The first attach records it in
the catalog, so later attaches can omit it, and Moraine reads the same root
for [indexes](#index-a-column) and row summaries.

## Attach a lake on S3

DuckDB opens remote paths read-only by default, and a read-only attach
cannot create a catalog. Add `READ_WRITE` to create or write a lake on S3:

```sql
CREATE SECRET s (TYPE s3, KEY_ID '…', SECRET '…', REGION 'us-west-2');
ATTACH 'ducklake:moraine:s3://bucket/prefix' AS lake
  (DATA_PATH 's3://bucket/prefix-data/', READ_WRITE);
```

Credentials come from DuckDB secrets, never from attach options. Once the
lake exists, a writer attach needs only the flag:

```sql
ATTACH 'ducklake:moraine:s3://bucket/prefix' AS lake (READ_WRITE);
```

Any S3-compatible store works the same way; point the secret's `ENDPOINT`
at MinIO or RustFS for a lake that never leaves your machine.

## One writer, many readers

One process attaches read-write. Every other process attaches
`READ_ONLY`:

```sql
ATTACH 'ducklake:moraine:s3://bucket/prefix' AS lake (READ_ONLY);
```

A second read-write attach does not fail. SlateDB gives the writer role to
the newest writer, and the previous writer's next commit fails with a
fencing error. Nothing is corrupted, but the first writer must re-attach.
A deployment that needs concurrent commits funnels them through one
writer process, where they share a flush.

A read-only attach follows the writer by pinning the checkpoints the writer
publishes, and writes nothing to the bucket. It looks for new commits at most
every `META_READER_POLL_INTERVAL_MS`, ten seconds by default, which is a billed
request each time. A reader that should never move pins a checkpoint created
ahead of time; see `META_CHECKPOINT` in
[Configuration](../configuration/#read-only-attaches).

## Index a column

A DuckLake table is a set of Parquet files, so a lookup by key reads every
file that statistics cannot rule out. Moraine adds ordered indexes over
table columns, created with DuckDB's own DDL and maintained in the same
commit as the rows they cover:

```sql
CREATE UNIQUE INDEX events_by_id ON lake.events (id);

SELECT * FROM lake.events WHERE id = 1;
SELECT * FROM lake.events WHERE id BETWEEN 100 AND 200;
SELECT index_name, is_unique, state
FROM moraine_indexes('lake', 'main', 'events');
```

Equality, `IN`, range, and `IS NULL` predicates probe the index instead of
scanning; nothing in the query changes. A unique index rejects a duplicate
even from concurrent transactions, and an index checked with
`moraine_index_verify` lets `DELETE … WHERE id = 1` skip the scan. Run
index DDL on the writer, outside an explicit transaction.

Options go in a `WITH` clause:

| Option | Meaning |
|---|---|
| `staged = true` | Build in bounded steps that resume if interrupted. Use for a table that already holds many files. |
| `step_entries`, `step_bytes` | Size of one staged step; both must be positive. |
| `maintenance = 'deferred'` | Add entries for new rows in a later repair pass instead of the data commit. Non-unique indexes only. |
| `directions = ['desc', 'asc']`, `nulls = ['first', 'last']` | Per-column order; DuckDB discards the standard `ASC`/`DESC` clauses. |

`moraine_index_verify('lake', 'main', 'events', 'events_by_id')` checks the
index against its table; a clean check lets a `DELETE` by key skip the scan.
Rebuilding is `DROP INDEX` then `CREATE INDEX`; in between, the index
neither accelerates queries nor enforces uniqueness. The
[design document](/rfcs/0009-equality-indexes/) has the lookup functions
and limits.

## Next

- [Configuration](../configuration/) lists every attach option, with
  sections on commit latency and caching.
- [Operating manual](../operating/) covers retention, maintenance,
  encryption, backups, and upgrades.
- [Compared with other catalogs](../comparison/) sets Moraine beside the
  DuckDB file, Postgres, and Quack catalogs.
- [Architecture](../architecture/) explains how the catalog is laid out
  and how commits and reads work.

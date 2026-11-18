---
title: Comparison
description: Moraine beside the DuckDB file, SQLite, Postgres, and Quack catalogs, and beside Iceberg and Delta Lake.
sidebar:
  order: 5
---

By design, DuckLake keeps table data as Parquet in object storage and the
catalog in a database. DuckLake's documentation on
[choosing a catalog database](https://ducklake.select/docs/stable/duckdb/usage/choosing_a_catalog_database)
describes the core SQL backends.

| Catalog | Lives | Writers | Readers | You run |
|---|---|---|---|---|
| DuckDB file | a file on one disk | one process, unless every process attaches `READ_ONLY` | processes on that disk | nothing |
| SQLite file | a file on one disk | one at a time; DuckLake retries lock conflicts | processes on that disk | nothing |
| Postgres | a database server | many, from many machines | many | the server, and its bill while idle |
| DuckDB over Quack | a DuckDB server over HTTP/2 | many, from many machines | many | the server; the protocol is in beta and DuckLake support is experimental |
| Moraine | the bucket, beside the data | one process per lake; its connections commit concurrently | any number, from any machine, read-only credentials | nothing beyond the bucket |

DuckLake also accepts MySQL, but recommends against it.

## SQLite and DuckDB

SQLite and DuckDB are the simplest catalogs: no server, one process writing a
local file. The tradeoff is that the catalog lives on one disk. A reader on
another machine needs that disk mounted and a backup requires copying the file.

Moraine keeps the catalog in the bucket beside the data files, so any machine
with bucket credentials can read it, and
[backups](../operating/#backups-and-copies) are trivial. Durability and multi-AZ
replication are built in by default. Additionally, Moraine provides checkpoints,
so a reader can see one consistent cut for as long as it likes.

## Postgres and Quack

Postgres and DuckDB + Quack support independent writers on several machines.
Both require a server to provision, patch, and back up. Quack is DuckDB's
client-server protocol; DuckDB considers the protocol and its DuckLake
catalog support experimental.

Moraine admits one writer process per lake. Multiple writers should funnel their
commits through that process. If writers on several machines must commit
independently, use Postgres or Quack. Moraine requires no catalog server;
object-storage costs come from storage and requests. Readers need only
`s3:GetObject` and `s3:ListBucket`. Moraine also maintains
[indexes](../getting-started/#index-a-column), so a lookup by key reads the
files that hold it instead of every file statistics cannot rule out.

## Iceberg-style catalogs

Apache Iceberg keeps a table's metadata as immutable files in the bucket, a
metadata file pointing at manifest lists and manifests, and writes a new
metadata file for every commit. A catalog service (Polaris, Lakekeeper, Nessie,
Unity, AWS Glue) points clients to the current metadata file for
each table.

DuckLake replaces the metadata files with database tables, so a query plans from
a few indexed reads instead of a manifest walk. Moraine stores that database in
object storage and provides indexes for efficient query planning.

## Delta Lake

Delta Lake has no catalog: each table is a directory with its own log and a
commit writes the next numbered log entry with a put-if-absent request. Any
number of writers can commit to a table that way, and the log is the whole
truth about that table. Nothing in the format spans two tables, so renames,
namespaces, and multi-table transactions come from a separate service such as
Unity Catalog or a Hive metastore.

Moraine's commit is also one conditional object write, but the object is a
batch over the whole catalog. A transaction can write to several tables,
rename one, and create a schema atomically, and renames never move data
files. The price is that one process commits to a lake at a time.

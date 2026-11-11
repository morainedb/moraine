---
title: Compared with other catalogs
description: Moraine beside the DuckDB file, SQLite, Postgres, and Quack catalogs, and beside Iceberg-style catalog services.
sidebar:
  order: 5
---

DuckLake keeps table data as Parquet in object storage and the catalog in a
database. The database decides who can write, what you run, and what you
pay while idle. DuckLake's documentation on
[choosing a catalog database](https://ducklake.select/docs/stable/duckdb/usage/choosing_a_catalog_database)
covers the first four rows; Moraine is the fifth.

| Catalog | Lives | Writers | Readers | You run |
|---|---|---|---|---|
| DuckDB file | a file on one disk | one process, unless every process attaches `READ_ONLY` | processes on that disk | nothing |
| SQLite file | a file on one disk | one at a time; DuckLake retries lock conflicts | processes on that disk | nothing |
| Postgres | a database server | many, from many machines | many | the server, and its bill while idle |
| DuckDB over Quack | a DuckDB server over HTTP/2 | many, from many machines | many | the server; the protocol is in beta and DuckLake support is experimental |
| Moraine | the bucket, beside the data | one process per lake; its connections commit concurrently | any number, from any machine, read-only credentials | nothing beyond the bucket |

DuckLake also accepts MySQL and recommends against it.

## The DuckDB file

Most lakes start here: nothing to run, one `ATTACH`. The limit is the disk.
One process writes the file, a second machine cannot read it, and moving or
backing up the lake means copying the file by hand.

Moraine keeps the zero-operations property and removes the disk. The
catalog is objects in the bucket next to the Parquet, so any machine with
read access can attach a reader, the lake outlives the machine that wrote
it, and a copy of the bucket is a backup. The price is commit latency: a
DuckDB file commits a single-row insert in about ten milliseconds, nearly
all of it DuckLake's own metadata SQL, and Moraine adds one object-store
PUT on top, 50 to 100 ms on S3 Standard or 5 to 10 ms with the log on
[S3 Express One Zone](../configuration/#the-write-ahead-log-on-a-faster-bucket).
Concurrent transactions in the writer share that PUT.

## Postgres and Quack

Both admit independent writers from many machines, and both are a server:
provisioned, patched, backed up, and billed around the clock. Quack is
DuckDB's own client-server protocol, so it is the same shape with DuckDB as
the server; DuckDB marks the protocol beta and its DuckLake catalog support
experimental.

Moraine admits one writer process per lake. A fleet that writes funnels its
commits through that process, and a second read-write attach takes over
rather than joining. If writers on several machines must commit
independently, use Postgres or Quack. In return Moraine has no server, bills
as storage and requests, lets readers attach with `s3:GetObject` and
`s3:ListBucket` alone, and keeps [indexes](../getting-started/#index-a-column)
over table columns in the same commit as the rows.

## Iceberg-style catalogs

Apache Iceberg keeps table metadata as files in the bucket, manifests and
manifest lists, and uses a catalog service (Polaris, Lakekeeper, Nessie,
Unity, AWS Glue) for the pointer to the current one. DuckLake replaces the
metadata files with database tables, so a query plans from a few indexed
reads instead of a manifest walk. Moraine puts that database in the bucket:
the deployment shape of Iceberg, without the file walk.

Iceberg is the choice when Spark, Trino, Flink, or a warehouse must read the
tables. A Moraine lake is read through the DuckDB extension or the Rust
crate.

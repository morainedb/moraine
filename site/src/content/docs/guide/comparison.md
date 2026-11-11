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
describes the supported SQL backends.

| Catalog | Lives | Writers | Readers | You run |
|---|---|---|---|---|
| DuckDB file | a file on one disk | one process, unless every process attaches `READ_ONLY` | processes on that disk | nothing |
| SQLite file | a file on one disk | one at a time; DuckLake retries lock conflicts | processes on that disk | nothing |
| Postgres | a database server | many, from many machines | many | the server, and its bill while idle |
| DuckDB over Quack | a DuckDB server over HTTP/2 | many, from many machines | many | the server; the protocol is in beta and DuckLake support is experimental |
| Moraine | the bucket, beside the data | one process per lake; its connections commit concurrently | any number, from any machine, read-only credentials | nothing beyond the bucket |

DuckLake also accepts MySQL and recommends against it.

## The DuckDB file

A DuckDB-file catalog needs no server. One process writes the file, and
other machines need access to that file to read the catalog.

Moraine stores the catalog in the bucket beside the Parquet files, so readers on
other machines need only bucket credentials. Back up both the catalog and data
files using the procedure in
[Backups and copies](../operating/#backups-and-copies). The price is commit
latency: a DuckDB file commits a single-row insert in about ten milliseconds,
nearly all of it DuckLake's own metadata SQL, and Moraine adds one object-store
PUT on top, 50 to 100 ms on S3 Standard or 5 to 10 ms with the log on
[S3 Express One Zone](../configuration/#the-write-ahead-log-on-a-faster-bucket).
Concurrent transactions in the writer share that PUT.

## Postgres and Quack

Postgres and DuckDB over Quack support independent writers on several machines.
Both require a server to provision, patch, and back up. Quack is DuckDB's
client-server protocol; DuckDB marks the protocol beta and its DuckLake catalog
support experimental.

Moraine admits one writer process per lake. A fleet that writes funnels its
commits through that process, and a second read-write attach takes over rather
than joining. If writers on several machines must commit independently, use
Postgres or Quack. Moraine requires no catalog server; object-storage costs come
from storage and requests. Readers need only `s3:GetObject` and `s3:ListBucket`.
Moraine also maintains [indexes](../getting-started/#index-a-column) in the same
commit as the rows.

## Iceberg-style catalogs

Apache Iceberg keeps table metadata as files in the bucket, manifests and
manifest lists, and uses a catalog service (Polaris, Lakekeeper, Nessie, Unity,
AWS Glue) for the pointer to the current one. DuckLake replaces the metadata
files with database tables, so a query plans from a few indexed reads instead of
a manifest walk. Moraine stores that database in object storage.

Iceberg is the choice when Spark, Trino, Flink, or a warehouse must read the
tables. A Moraine lake is read through the DuckDB extension or the Rust
crate.

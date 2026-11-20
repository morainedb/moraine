---
title: "Introducing Moraine: DuckLake entirely in object storage, no catalog service required"
date: 2026-11-18
authors: alex
excerpt:
  Moraine moves the DuckLake catalog into object storage, removing the need for
  a separate catalog service.
---

We're excited to introduce you to
[Moraine](https://github.com/morainedb/moraine), which moves your DuckLake
catalog to object storage right alongside your data. That means you don't need
to host a separate Postgres or DuckDB/Quack server for each data lake.

We built it when faced with hosting tenant-scoped data warehouses for our
analytics clients at [Contour](https://contour.app). We first moved to
[DuckLake](https://ducklake.select) to avoid commercial data warehouse costs
(and because it's a great spec), but the Postgres hosting for each lake also got
expensive, even though it sat idle most of the time. Moraine fixes exactly that.
An idle lake costs just object storage. And, spinning up a new, separate lake is
as simple as picking a new S3 bucket or prefix.

In Moraine, the catalog lives in [SlateDB](https://slatedb.io), an LSM-based
key-value store that stores the catalog in object storage alongside your data.
That means a commit is a single object write and there's no separate service to
run. SlateDB is a database, so we can maintain DuckLake's transactional
properties and inline small data changes. With proper tuning, write and read
performance is [competitive](/benchmarks/) with DuckLake on Postgres.

There are a couple of drawbacks to our approach: only one process can attach
read-write per lake at a time, with others limited to read only. And, attach
times are slower than the other catalogs. If you have one ingest service and
many readers (our use case) this works well. If you need lots of independent
writers, it may not be a good fit.

We've also used SlateDB's key-value storage to add native indexes to DuckLake.
Just like in stock DuckDB, run `CREATE INDEX` on a column and the index is kept
in step with every commit, so lookups by that column read only the files that
hold it.

Moraine is still alpha-quality software, though we've started putting it through
its paces with production clients. For more on how to get started, see
[Getting started](/guide/getting-started/). Dual-licensed under MIT or
Apache-2.0. A big thank you to the developers of DuckDB and SlateDB for the
building blocks.

Feedback welcome, especially on workloads and use cases where Moraine breaks
down. Open an [issue](https://github.com/morainedb/moraine/issues) or start a
thread in [Discussions](https://github.com/morainedb/moraine/discussions). We
can't wait to see what you build with it!

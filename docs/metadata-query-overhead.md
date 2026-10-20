# Metadata-query overhead in a single-row commit

The roadmap carries an unimplemented performance item: reduce the
metadata-query overhead DuckDB and DuckLake add to a commit. It rested on
profiling old enough to predate commit-served projections, so this is a
fresh measurement of where a one-row `UPDATE` actually spends its time, and
which statements make up the metadata share.

The short answer: DuckLake's own metadata SQL is **14.5 ms of a 23–25 ms
commit**, and two statements are 69% of it — the file-list query DuckLake
plans every scan from, and the snapshot insert.

## Method

A local RustFS endpoint stands in for S3, so the catalog's requests are
real HTTP without a wide-area round trip; Parquet stays local, so every
measured object-store request belongs to metadata.

```bash
target/rustfs/bin/rustfs.1.0.0 server --address 127.0.0.1:9124 \
  --access-key moraineadmin --secret-key moraineadmin target/rustfs/data

MORAINE_S3_BUCKET=moraine MORAINE_S3_ENDPOINT=http://127.0.0.1:9124 \
  AWS_REGION=us-east-1 AWS_ACCESS_KEY_ID=moraineadmin \
  AWS_SECRET_ACCESS_KEY=moraineadmin \
  cargo xtask commit-bench --files 16,128 --commits 7 --flush-ms 25
```

The per-statement split comes from the same session shape — attach, one
table, 1,600 rows over 16 files, then 100 one-row `UPDATE`s — grouping
`duckdb_logs_parsed('DuckLakeMetadata')` by query. One hundred commits
rather than seven because DuckLake's logger records whole milliseconds, and
at seven commits the smaller statements sit on that floor.

## Where a commit goes

From `commit-bench`, per commit, medians over seven commits:

| Files | Commit | DuckLake metadata | Moraine core | Durable wait | Requests |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 16 | 23.0 ms | 13.4 ms | 3.3 ms | 3.0 ms | 1 PUT, 2.7 ms |
| 128 | 25.0 ms | 14.1 ms | 3.4 ms | 3.0 ms | 1 PUT, 2.9 ms |

Metadata is 58–63% of the commit. Moraine's own commit work is about 14%
and the durable write wait about 12%. Eight metadata statements run per
commit at both file counts, and the metadata total barely moves between 16
and 128 files: this is a fixed per-commit cost, not one that scales with
the table's file count.

## Which statements

Per commit, over 100 commits, three repetitions:

| Statement | Calls | ms/commit | Share of metadata |
| --- | ---: | ---: | ---: |
| File list with per-column stats | 2 | 6.14 | 42% |
| `INSERT INTO ducklake_snapshot` | 1 | 3.80 | 26% |
| Inlined-row read (`SELECT CAST(id …)`) | 1 | ~1.5 | 10% |
| Table and schema version lookup | 1 | ~1.0 | 7% |
| Table and column statistics | 1 | ~1.0 | 7% |
| Newest snapshot lookup | 1 | ~1.0 | 7% |
| Inlined-delete and row-id probes | ~1 | ~0 | ~0 |
| **Total** | **8.02** | **14.48** | |

The three repetitions give 14.39, 14.48 and 15.2 ms for the total, 6.03,
6.14 and 6.56 for the file list, and 3.73, 3.80 and 3.83 for the snapshot
insert, so the ordering is not noise.

The dominant statement is DuckLake's scan-planning query: a per-column
`col_N_stats` CTE over `ducklake_file_column_stats`, joined against the
data-file list for paths, sizes, footers and record counts. It runs once
per column of the target table — two calls here for a two-column table,
which is why its call count is 2 and not 1, and which means its cost grows
with column count rather than with file count.

## Why those statements are slow

None of it is data access. The same workload against a local-filesystem
store costs 14.31 ms per commit against the S3-backed 14.48, and
`commit-bench` records zero GETs per commit: catalog reads are served from
moraine's in-memory projections, so there is nothing to wait for.

Nor is it data volume. `EXPLAIN ANALYZE` on the file-list query reports
every operator at 0µs over 1 to 3 rows, while the statement as a whole
takes 3.1 ms.

The time is DuckDB's SQL front-end. Profiling the same query in detail:

| Phase | Fresh statement | Prepared `EXECUTE` |
| --- | ---: | ---: |
| Parse | 0.78 ms | 0.02 ms |
| Plan (bind) | 0.98 ms | 0.99 ms |
| Optimize | 0.66 ms | 0.55 ms |
| Physical plan | 0.05 ms | 0.03 ms |
| **Execute** | **0.21 ms** | **0.14 ms** |

About 92% of the statement is turning roughly a kilobyte of generated SQL
into a plan, to then run a query that returns one row.

Preparing the statement does not fix it. Over 500 iterations, re-planning
each time costs 2.69 ms per query and a prepared `EXECUTE` 2.12 ms — 21%
off, which is the parse and nothing else. DuckDB re-binds and re-optimizes
on every execution, and binding is the single largest phase.

The front-end cost tracks how many metadata tables a statement names,
which is why the file-list query is the most expensive one:

| Moraine tables in the query | Front-end | Execute |
| ---: | ---: | ---: |
| 1 | 0.65 ms | 0.01 ms |
| 2 | 0.84 ms | 0.06 ms |
| 3 | 1.43 ms | 0.08 ms |

Eight such statements per commit is the 14.5 ms.

## How much of it is moraine's

Almost none, which took a wrong turn to establish.

Timing 500 iterations of the simplest possible query gives about 0.41 ms
against a native in-memory DuckDB table and about 0.73 ms against a
moraine metadata table. Under `perf` that 0.33 ms per statement is real:
233 ms of CPU for the native run against 400 ms for the moraine one, with
`MoraineSchemaEntry::EnsureTablesLoaded` at 13% of the moraine run and
`PopulateMetadataTables` at 11%. Those build 29 synthetic `ducklake_*`
table entries carrying 196 column definitions, each column routed through
`MapColumnType`'s string match, and they run on the first table lookup of
a *transaction*.

That last word is what makes the microbenchmark misleading. Each of its
500 statements was its own autocommit transaction, so it paid the
population 500 times. DuckLake's metadata connection holds one
transaction across the statements of a commit, so the real workload pays
it once per commit and `PopulateMetadataTables` never reaches 2% of it.

Profiling the commit workload itself — 100 one-row `UPDATE`s, 2,064 ms of
CPU, so about 20.6 ms per commit — puts moraine almost nowhere:

| Category | Share | Per commit |
| --- | ---: | ---: |
| DuckDB core | 56.5% | 11.7 ms |
| Allocator and `memcpy` | 20.8% | 4.3 ms |
| libc, kernel, refcounting | 17.7% | 3.6 ms |
| SlateDB reads and the async bridge | 2.4% | 0.49 ms |
| Moraine's own code | 1.5% | 0.30 ms |

Moraine's code and its reads together are under 4% of a commit. There is
no material win on this path inside this repository, and the per-statement
gap above does not recur in practice.

## Where execution overtakes planning

Growing the lake and re-profiling the same file-list query, medians of
five runs each:

| Files | Front-end | Execute | Execute share |
| ---: | ---: | ---: | ---: |
| 16 | 2.18 ms | 0.16 ms | 6.8% |
| 128 | 2.00 ms | 0.21 ms | 9.3% |
| 512 | 1.99 ms | 0.33 ms | 14.3% |
| 2,048 | 2.07 ms | 0.82 ms | 28.3% |

The front-end is flat — it depends on the SQL text, which does not change
— while execution grows with the file count, by roughly 2.5× for each 4×
of files in the upper range. Extrapolating that slope, the two meet at
somewhere around five thousand files for this two-column table. Below
that, planning dominates; above it, the balance tips and a storage-side
improvement starts to matter.

## Whether the re-planning can be avoided

Not by preparing the statement, which was the obvious first idea.
`perf` on 300 SQL-level `EXECUTE`s shows `Binder::BindNode` and
`Planner::Optimize` in the profile over a native DuckDB table as readily
as over moraine's, so going through `PREPARE`/`EXECUTE` in SQL re-binds
and re-optimizes either way.

DuckDB's rule for reusing a prepared plan is in
`PreparedStatementData::RequireRebind`, which asks `CheckCatalogIdentity`
for every database the statement reads. That function opens with:

```cpp
// some catalogs don't support catalog version, we can't check identity in that case
if (!catalog_identity.catalog_version.IsValid()) {
    return false;
}
```

A `false` return means "identity does not match", so a catalog that
reports no version forces a rebind on every execution. Moraine's
`MoraineCatalog::GetCatalogVersion` delegates to `IndexCatalogVersion`,
which returns an invalid `optional_idx` unless an index-dependency state
happens to be registered for the catalog, its association is
unambiguous, the transaction is clean, and the snapshot revision reads —
so in the ordinary case moraine reports no version at all.

That is a real difference against a native catalog, and it is on this
side of the boundary. It does not help the commit path: DuckLake issues
fresh SQL text rather than reusing prepared statements, and a commit
changes the catalog anyway, so a plan could not survive to be reused. It
would matter to a client holding prepared statements against a lake that
is not changing — a reader fleet rather than a writer. Confirming it
needs a client-API prepared statement, which the CLI cannot express, and
that has not been done.

## What this says about the roadmap item

The premise holds: metadata queries really are the largest single
component of a small commit, and larger than everything moraine does for
itself. Two statements carry 69% of it, and the biggest is a *read*
DuckLake issues to plan the `UPDATE`'s scan — not part of writing the
commit at all.

But the item's proposed remedy does not follow from the measurement.
Native metadata tables are a storage change, and storage is not what the
commit waits on: execution over the rows is 0.14–0.21 ms of a 2.7 ms
statement. Serving the same SQL surface from a different store would move
that fifth of a millisecond and leave the other 2.5 ms — parse, bind,
optimize — exactly where it is. The leverage is in not re-planning the
same generated SQL on every commit, which is DuckLake's and DuckDB's side
of the boundary rather than moraine's: a bind-stable cached plan, or a
catalog call that never becomes SQL text at all.

Three caveats bound these numbers. A local endpoint understates the
durable and request components, so against real S3 the metadata share
falls even though its absolute ~14.5 ms would not. The file-list query's
cost tracks column count, so a wide table pays more than this two-column
one. And this lake is small — 16 files, 1,600 rows — so execution is near
its floor; a lake large enough for real scan work shifts the balance back
toward execution, which is visible already in the 13.4 → 14.1 ms step
from 16 to 128 files.

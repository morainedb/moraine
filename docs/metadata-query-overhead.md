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

## What this says about the roadmap item

The premise holds: metadata queries really are the largest single
component of a small commit, and larger than everything moraine does for
itself. But the cost is concentrated rather than diffuse, which changes
what is worth building. Two statements carry 69% of it, and the biggest is
a *read* DuckLake issues to plan the `UPDATE`'s scan — not part of writing
the commit at all.

Two caveats bound these numbers. A local endpoint understates the durable
and request components, so against real S3 the metadata share falls even
though its absolute ~14.5 ms would not. And the file-list query's cost
tracks column count, so a wide table pays more than this two-column one.

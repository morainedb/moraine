# The Appender on the inlined-write path, re-measured

`MoraineMetadataManager::WriteNewInlinedData` overrides DuckLake's inlined
write to append a commit's rows through the Appender API, and
`patches/ducklake/0003` exists only to let it reuse the inlined-table name
cache DuckLake invalidates. Both were worth re-testing at DuckDB 2.0:
DuckLake's own path no longer formats `INSERT ... VALUES` text per row. It
builds a `ColumnDataCollection` and one parsed statement, which is the cost
the override was written to avoid.

It still pays, so both stay.

## Method

200 commits, each inserting 100 rows into an inlined table
(`DATA_INLINING_ROW_LIMIT 1000000`, `threads=1`), timed end to end through
the CLI. The two builds differ only in the override and the patch: one
extension artifact each, loaded by the same CLI, **alternated round by
round** so a drifting machine moves both equally. Eight rounds each; the
first attempt at this measured the two builds twenty minutes apart and is
not reported, because a build ran between them.

```sql
ATTACH 'ducklake:moraine:<store>' AS lake (DATA_PATH '<data>', DATA_INLINING_ROW_LIMIT 1000000);
CREATE TABLE lake.main.t(a BIGINT, b VARCHAR, c DOUBLE);
-- x200:
INSERT INTO lake.main.t SELECT i, 'row-' || i, i * 1.5 FROM range(100) t(i);
```

## Result

| | runs (ms, sorted) | median | min |
| --- | --- | ---: | ---: |
| Appender override | 2998 3015 3100 3224 3419 3583 3876 4149 | **3224** | 2998 |
| DuckLake's own path | 3289 3323 3630 3633 3654 3673 3682 3784 | **3633** | 3289 |

**+409 ms over 200 commits, or ~2.0 ms per commit** — ~13%. By minima,
the quieter statistic here, +291 ms or ~1.5 ms per commit. Either way it is
large against the ~3.3 ms a one-row commit spends in moraine's own work
([metadata-query-overhead.md](metadata-query-overhead.md)), and it is the
bind of one more statement per table per commit: DuckLake's path hands the
collection to an `INSERT` that still binds and plans, where the Appender
writes to the table directly.

The spread is wide enough that the medians overlap at the tails, so this
says the override is worth keeping — not what it is worth to a digit.

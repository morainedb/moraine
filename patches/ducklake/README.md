# Patched DuckLake inlined writes and commit cleanup

This directory carries the downstream DuckLake patch series moraine bundles,
for the DuckDB `.github/duckdb-versions` names as primary. DuckDB patches
DuckLake itself before building it, and those patches are what make the
pinned commit compile against that DuckDB at all — so every build applies
DuckDB's own
(`duckdb/.github/patches/extensions/ducklake/`, three-way, as DuckDB
applies them) and then the series below, in file-name order:

1. `0003-feat-share-the-DuckLake-inlined-table-cache.patch` moves the
   `insert_inlined_table_name_cache` member to `protected`, so a registered
   metadata manager overriding the inlined-data write reuses the cache
   DuckLake's own `ClearInlinedTableCaches` invalidates. moraine's
   `MoraineMetadataManager` overrides that write to append a commit's
   inlined rows through the Appender API instead of formatting them into an
   `INSERT ... VALUES` list, whose cost is per row and dominated by binding
   it; a subclass cache would never be cleared, since the invalidation hook
   is public but not virtual. Re-measured at DuckDB 2.0, where DuckLake's
   own path stopped formatting per-row SQL, over 200 commits of 100 inlined
   rows with the two builds alternated round by round: the Appender is
   still ~2 ms a commit faster, the bind of one more statement per table,
   so both it and this patch stay.
2. `0004-fix-retain-files-after-unknown-commit-outcomes.patch` recognizes the
   metadata backend's structured `commit_outcome=unknown` error field, with
   Moraine's fixed message as a fallback when DuckDB's COMMIT wrapper drops
   extra fields. It stops retries and releases the transaction's file-cleanup
   ownership before rollback, so an unacknowledged commit cannot lose files it
   registered. Unregistered files remain eligible for orphan cleanup. Ordinary
   SQL deletion and unknown commit outcomes are tested by `cargo xtask e2e`.
The positional DELETE the optimizer selects when a certified index answers the
entire equality predicate is `moraine_delete_positions`, registered by the
extension over DuckLake's public transaction API rather than patched in.
Ordinary `DELETE ... WHERE` selects this path without a caller naming row
positions. UPDATE keeps DuckLake's operator, fed by moraine's located scan,
which serves each row's file and position as DuckLake's own scan would.

`0011-perf-stage-snapshots-and-defer-inline-appends.patch` adds optional
metadata-manager hooks for staging a finalized snapshot without SQL and
appending inline chunks after the commit batch creates their tables. The
snapshot hook runs after changes finalize the next-id counters and schema
version. Both paths remain inside the metadata transaction; rollback clears
queued appends before retry. Other backends retain their SQL path. Query-log
regressions and inline roundtrips in `cargo xtask e2e` cover the Moraine path.

A later patch edits files an earlier one creates, so the series is applied in
one `git apply` invocation rather than one per file.

The series is pinned to the DuckLake revision selected by the DuckDB release
moraine supports, as patched by that DuckDB. The patched DuckLake is built
alongside moraine and linked into its loadable, so `LOAD moraine` registers both
and no separate DuckLake extension is installed or loaded.

Every hunk carries context, so `git apply` locates it by content in each
pinned source rather than by line number, and a hunk whose region moved fails
the apply instead of landing somewhere the result still compiles. Blank
context lines are written without their trailing space, which `git apply`
reads as the blank line it is.

The source mapping lives in `source-pins`. Each entry binds one DuckDB release
to the upstream DuckLake commit that release selects, and a release's build
cannot fetch its DuckLake without one. `cargo xtask bump-duckdb` writes the
new release's entry and `check-pins` requires one per supported release.
Neither reads that source: `cargo xtask check-patch-pins` applies the series
to every one of them and names the release whose source rejects it, which
`cargo xtask e2e` also runs before building. Only the primary pin is built
locally, so without it a hunk the other sources have moved out from under
would first surface in that release's CI build.

## Build

`ducklake.cmake` is included by the repository's `extension_config.cmake`, so
every moraine build carries the series. Without `DUCKLAKE_PATCH_SOURCE` it
fetches the DuckLake commit `source-pins` names for the DuckDB being built
and applies the patches, which is how the release and community pipelines
build. `cargo xtask e2e` instead prepares a checkout under
`target/patched-ducklake/` first: it fetches the pinned DuckLake and vcpkg
revisions, applies the series and verifies the checkout's complete diff
byte-for-byte, and passes the checkout in. Either way DuckLake's `roaring`
dependency resolves through vcpkg (`vcpkg.json` at the repository root
declares it).

The build workflows run a row-ID statistics and positional-delete smoke
against what they build, in the container that built it
(`cargo xtask validate-release-artifact`).

## Load in DuckDB

A locally built artifact is unsigned, so start DuckDB with `-unsigned`. One
load registers DuckLake and moraine:

```sh
build/release/duckdb -unsigned
```

```sql
LOAD 'build/release/extension/moraine/moraine.duckdb_extension';

ATTACH 'ducklake:moraine:s3://bucket/catalog' AS lake (
    DATA_PATH 's3://bucket/data/',
    READ_ONLY
);
```

A `LOAD ducklake` afterward is a no-op: the bundled DuckLake is recorded as
the loaded `ducklake` extension, reporting its source revision through
`duckdb_extensions()`. Loading stock DuckLake *before* moraine is refused,
since it lacks the series and would collide with the bundle. The CLI and the
artifact must match on DuckDB version.

## Index-assisted read

Every lookup function returns the `row_id` of each row holding the key,
once, wherever its live copy sits. Join the lookup directly to the DuckLake
table on the row id:

```sql
SELECT data.*
FROM lake.main.items AS data
JOIN moraine_index_in(
    'lake', 'main', 'items', 'by_external_key',
    ['key-a', 'key-b']
) AS hits
  ON data.rowid = hits.row_id;
```

The same shape works with `moraine_index_lookup`, `moraine_index_range`, and
`moraine_index_nulls`. The Moraine extension replaces the table's scan
beneath the join with its own located scan, which reads the named rows from
the files and inlined tables holding them and evaluates the scan's filters
itself, so the read touches only the files holding the rows. An ordinary
predicate a certified index answers plans the same way, with no join in the
query. Inside a transaction that has inserted rows, the located scan serves
the committed rows and the transaction's inlined ones, under an UPDATE or
DELETE as well; once the transaction has written a file for the table, a read
joins DuckLake's scan over those files and a mutation keeps DuckLake's scan.
Where the located scan cannot
serve a query — a projection of part of a struct, a column of a type it
cannot decode — the plan runs as written.

The same condition locates rows for DML — `DELETE … USING` and
`UPDATE … FROM` — and inside an `EXISTS` probe:

```sql
DELETE FROM lake.main.items
USING moraine_index_lookup(
    'lake', 'main', 'items', 'by_external_key', 'key-a'
) AS hits
WHERE items.rowid = hits.row_id;
```

## Release

There is no separate DuckLake release. The moraine release workflow builds
the bundle for the version in `.github/duckdb-versions` on the four native
platforms, each build running the row-ID statistics and positional-delete
smoke (`release-smoke.sql`) against itself, through
`cargo xtask validate-release-artifact`.

`0012-perf-scope-and-stage-append-statistics.patch` lets a metadata manager
declare that a lookup of one table's statistics loads that table alone, which
DuckLake then asks for through its table-scoped statistics read. Optional write
hooks stage append statistics and snapshot changes through the backend's
existing row contract. Direct statistics writes apply to appends without schema,
deletion, or maintenance changes; other commits retain SQL ordering. Pending
statistics flush after successful SQL execution and clear on rollback. Empty SQL
batches still run the pending writes and commit the metadata transaction.
Default hooks retain DuckLake's SQL behavior for other metadata backends.

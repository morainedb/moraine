# Patched DuckLake row-ID statistics, pruning, inlined writes, commit cleanup, and positional deletes

This directory carries the downstream DuckLake patch series moraine bundles,
for the DuckDB `.github/duckdb-versions` names as primary. DuckDB patches
DuckLake itself before building it, and those patches are what make the
pinned commit compile against that DuckDB at all — so every build applies
DuckDB's own
(`duckdb/.github/patches/extensions/ducklake/`, three-way, as DuckDB
applies them) and then the series below, in file-name order:

1. `0001-perf-prune-DuckLake-files-by-row-id.patch` stores file-level row-ID
   min/max statistics in DuckLake's existing `ducklake_file_column_stats`
   table and pushes `rowid` filters into its file list before any Parquet
   reader is created. Moraine can therefore return stable row ids without
   taking ownership of DuckLake scans or physical placement.
2. `0002-feat-expose-DuckLake-data-file-ids-to-scans.patch` exposes
   `data_file_id` as an internal virtual `UBIGINT` column. A physical file
   emits its persistent catalog id; inlined and transaction-local sources
   emit NULL. Filters on it are pushed into the metadata file-list query as
   predicates on `ducklake_data_file.data_file_id`, using no column
   statistics, so a located index result restricts the file list as well as
   the rows read within it. A filter shape the translation does not cover
   adds no predicate, which keeps every file rather than guessing.
3. `0003-feat-share-the-DuckLake-inlined-table-cache.patch` moves the
   `insert_inlined_table_name_cache` member to `protected`, so a registered
   metadata manager overriding the inlined-data write reuses the cache
   DuckLake's own `ClearInlinedTableCaches` invalidates. moraine's
   `MoraineMetadataManager` overrides that write to append a commit's
   inlined rows through the Appender API instead of formatting them into an
   `INSERT ... VALUES` list, whose cost is per row and dominated by binding
   it; a subclass cache would never be cleared, since the invalidation hook
   is public but not virtual.

4. `0004-fix-retain-files-after-unknown-commit-outcomes.patch` recognizes the
   metadata backend's structured `commit_outcome=unknown` error field, with
   Moraine's fixed message as a fallback when DuckDB's COMMIT wrapper drops
   extra fields. It
   stops retries and releases the transaction's file-cleanup ownership before
   rollback, so an unacknowledged commit cannot lose files it registered.
   Unregistered files remain eligible for orphan cleanup. Ordinary SQL deletion and
   unknown commit outcomes are tested by `cargo xtask e2e`.

5. `0005-feat-change-DuckLake-rows-by-position.patch` adds
   `ducklake_delete_positions(catalog, schema, table, files, snapshot := NULL)`,
   which stages deletes of already-located rows in the current DuckLake
   transaction without scanning the table. `files` is a list of
   `STRUCT(data_file_id UBIGINT, positions UBIGINT[], existing_positions BLOB)`
   naming positions within committed data files at the transaction's
   snapshot. `existing_positions` carries the deletions the caller already
   decoded from that file's current delete file, as whole uint64 positions;
   NULL means the caller does not know them and DuckLake reads the file
   itself. `snapshot`, when given, is
   the snapshot the caller resolved them against, refused with a
   transaction error if it is older than the one the transaction reads. The
   function validates every file id and position against that snapshot,
   subtracts deletions the file
   already carries, and then stages the rest exactly as `DELETE` would:
   inlined file deletions below the inlining threshold, otherwise a new
   delete file that replaces the file's current one. `COMMIT` publishes
   these together with the transaction's other changes, and `ROLLBACK`
   discards them and removes any file written. The function returns
   `file_rows_deleted` and `delete_files_written`. It has no Moraine
   dependency. Positional UPDATE, inline-row arguments, and deferred
   position tokens are removed; the optimizer supplies committed file
   positions directly. Rollback, repeated deletes, prepared statements,
   and autocommit are covered by `cargo xtask e2e`.

`0007-fix-keep-the-row-ids-a-merged-file-carries.patch` stops
`ducklake_merge_adjacent_files` registering the merged file at a dense
`row_id_start`. DuckLake writes the row-id column into every merged file,
then judges two files adjacent from `row_id_start + record_count` and
registers the merge at the first file's start, which discards those ids. A
file's start says nothing about the ids its rows carry: a flushed file
whose batch straddled two partitions holds ids with gaps under a start that
is only the lowest of them, and an update's output holds its rows' original
ids under a start at the next free id. Either chains onto a neighbour, and
every row after a gap is renumbered onto ids other rows hold. Covered by
`cargo xtask e2e`, which merges both shapes and reads the ids back.

`0005` supplies the positional DELETE used by the optimizer when a certified
index answers the entire equality predicate. The public located-change SQL
verbs remain withdrawn; ordinary `DELETE ... WHERE` selects this path without
a caller naming row positions. UPDATE retains DuckLake's operator with
index-derived scan filters.

A later patch edits files an earlier one creates, so the series is applied in
one `git apply` invocation rather than one per file.

The series is pinned to the DuckLake revision selected by the DuckDB
release moraine supports, as patched by that DuckDB. The patched DuckLake is built alongside
moraine and linked into its loadable, so `LOAD moraine` registers both and
no separate DuckLake extension is installed or loaded.

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

`cargo xtask e2e` then runs the series' row-ID write, pruning and
flushed-file-merge sqllogictests against the built moraine artifact, and
the build workflows run the same row-ID statistics and pruning smoke
against what they build, in the container that built it
(`cargo xtask validate-release-artifact`).

`cargo xtask ducklake-patch` builds the series as a standalone loadable under
`target/patched-ducklake/build-extension-static/`, against moraine's DuckDB
submodule and prebuilt static library. Only `cargo xtask session-bench`
needs it: its pinned revisions predate the bundle and load DuckLake beside
their own moraine. Use `--root DIRECTORY` to move the gitignored cache, or
`--duckdb-static FILE` to select another static archive built from moraine's
exact DuckDB pin.

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

Every lookup function returns a `row_id` column and a nullable
`data_file_id` — NULL for a row living in inlined data rather than a
Parquet file. Join the lookup directly to the DuckLake table in one
relational query: ordinary equality on the row id, null-safe equality on
the file id.

```sql
SELECT data.*
FROM lake.main.items AS data
JOIN moraine_index_in(
    'lake', 'main', 'items', 'by_external_key',
    ['key-a', 'key-b']
) AS hits
  ON data.rowid = hits.row_id
 AND data.data_file_id IS NOT DISTINCT FROM hits.data_file_id;
```

The same shape works with `moraine_index_lookup`, `moraine_index_range`, and
`moraine_index_nulls`; a query that selects only `row_id` can join on it
alone. The Moraine extension restates the resolved rows as static row-id and
file-id filters on the scan, and DuckDB adds a dynamic row-ID filter from the
join key; the patched DuckLake applies both while constructing the
physical-file list, so the read touches only the files holding the rows.

The same conditions locate rows for DML — `DELETE … USING` and
`UPDATE … FROM` — and inside an `EXISTS` probe:

```sql
DELETE FROM lake.main.items
USING moraine_index_lookup(
    'lake', 'main', 'items', 'by_external_key', 'key-a'
) AS hits
WHERE items.rowid = hits.row_id
  AND items.data_file_id IS NOT DISTINCT FROM hits.data_file_id;
```

**Do not** compare the two columns as a tuple —
`(rowid, data_file_id) IN (SELECT row_id, data_file_id …)`. Tuple `IN`
compares with plain equality, under which a NULL file id matches nothing, so
every inlined row silently drops out of the result; as a DELETE or UPDATE
predicate that strands the row with no error. The file-id condition is an
optimization, not a correctness requirement — when in doubt, join on
`row_id` alone and let DuckLake pick the visible copy.

## Release

There is no separate DuckLake release. The moraine release workflow builds
the bundle for the version in `.github/duckdb-versions` on the four native
platforms, each build running the row-ID statistics, one-file pruning, and
positional-delete smoke (`release-smoke.sql`) against itself, through
`cargo xtask validate-release-artifact`.

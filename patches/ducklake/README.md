# Patched DuckLake row-ID statistics, pruning, inlined writes, commit cleanup, and positional deletes

This directory carries the downstream DuckLake patch series moraine bundles,
for DuckDB v1.5.5, applied in file-name order:

1. `0001-perf-prune-DuckLake-files-by-row-id.patch` stores file-level row-ID
   min/max statistics in DuckLake's existing `ducklake_file_column_stats`
   table and pushes `rowid` filters into its file list before any Parquet
   reader is created. Moraine can therefore return stable row ids without
   taking ownership of DuckLake scans or physical placement.
2. `0002-feat-backfill-DuckLake-row-id-file-statistics.patch` adds the
   metadata-only `ducklake_backfill_row_id_stats` function, which repairs
   files written before the statistics patch was installed.
3. `0003-feat-expose-DuckLake-data-file-ids-to-scans.patch` exposes
   `data_file_id` as an internal virtual `UBIGINT` column. A physical file
   emits its persistent catalog id; inlined and transaction-local sources
   emit NULL. Filters on it are pushed into the metadata file-list query as
   predicates on `ducklake_data_file.data_file_id`, using no column
   statistics, so a located index result restricts the file list as well as
   the rows read within it. A filter shape the translation does not cover
   adds no predicate, which keeps every file rather than guessing.
4. `0004-perf-append-DuckLake-inlined-data-rows.patch` writes a commit's
   inlined rows through the DuckDB Appender API instead of formatting them
   into an `INSERT ... VALUES` list, whose cost is per row and dominated by
   binding it. Backends without an appender keep the SQL branch, as does the
   server-side commit path. Appends run while the commit's SQL batch is still
   being assembled, so rows bound for a table that batch has yet to `CREATE`
   keep the SQL branch too; DuckLake registers a table's inlined table when
   the table itself is created or altered, so this covers same-transaction
   `CREATE`-then-`INSERT` and tables that predate inlining being enabled.

5. `0005-fix-retain-files-after-unknown-commit-outcomes.patch` recognizes the
   metadata backend's structured `commit_outcome=unknown` error field, with
   Moraine's fixed message as a fallback when DuckDB's COMMIT wrapper drops
   extra fields. It
   stops retries and releases the transaction's file-cleanup ownership before
   rollback, so an unacknowledged commit cannot lose files it registered.
   Unregistered files remain eligible for orphan cleanup. Ordinary SQL deletion
   and standalone located deletion cancellation are tested by `cargo xtask e2e`.

6. `0006-feat-change-DuckLake-rows-by-position.patch` adds
   `ducklake_delete_positions(catalog, schema, table, files, inlined_rows := [], snapshot := NULL)`,
   which stages deletes of already-located rows in the current DuckLake
   transaction without scanning the table. `files` is a list of
   `STRUCT(data_file_id UBIGINT, positions UBIGINT[])` naming positions
   within committed data files at the transaction's snapshot; `inlined_rows`
   lists row ids of committed inlined rows, and `snapshot`, when given, is
   the snapshot the caller resolved them against, refused with a
   transaction error if it is older than the one the transaction reads. The
   function validates every file id and position against that snapshot,
   subtracts deletions the file
   already carries, and then stages the rest exactly as `DELETE` would:
   inlined file deletions below the inlining threshold, otherwise a new
   delete file that replaces the file's current one. `COMMIT` publishes
   these together with the transaction's other changes, and `ROLLBACK`
   discards them and removes any file written. Moraine's
   `moraine_delete_located` resolves row ids to positions through its
   file-row summaries and rewrites itself into this call. The same patch
   adds `ducklake_update_positions(catalog, schema, table, files,
   replacement, inlined_rows := [], snapshot := NULL)`: `replacement` is a
   `SELECT` producing the table's columns followed by each row's id, bound
   through DuckDB's binder and planned through the operators `UPDATE` uses
   in their row-id-writing mode, so the rows keep their ids, with the
   positional deletes staged once the rows are written, all in the current
   transaction; `moraine_update` rewrites into it. Neither function has a Moraine
   dependency; explicit rollback, failed replacement inserts, repeated
   calls, and standalone autocommit are covered by `cargo xtask e2e`.

Later patches address the lines earlier ones produce, so the series is applied
in one `git apply` invocation rather than one per file.

The series is pinned separately to the DuckLake revisions selected by every
DuckDB release moraine supports. The patched DuckLake is built alongside
moraine and linked into its loadable, so `LOAD moraine` registers both and
no separate DuckLake extension is installed or loaded.
Most hunks use zero context to satisfy moraine's whitespace gate across both
source pins. The control-flow-sensitive row-ID statistics hunk replaces and
re-emits its function's return so it cannot land after that return.

The source mapping lives in `source-pins`. Each entry binds one DuckDB release
to the upstream DuckLake commit that release selects. A DuckDB bump must add a
validated mapping before that release's build can fetch its DuckLake.

## Build

`ducklake.cmake` is included by the repository's `extension_config.cmake`, so
every moraine build carries the series. Without `DUCKLAKE_PATCH_SOURCE` it
fetches the DuckLake commit `source-pins` names for the DuckDB being built
and applies the patches, which is how the release and community pipelines
build. `cargo xtask e2e` instead prepares a checkout under
`target/patched-ducklake/` first: it fetches the pinned DuckLake and vcpkg
revisions, applies the series and verifies the checkout's complete diff
byte-for-byte, and passes the checkout in. Either way the CMake configure
refuses a tree where the row-ID statistics hunk landed after its function's
return, and DuckLake's `roaring` dependency resolves through vcpkg
(`vcpkg.json` at the repository root declares it).

`cargo xtask e2e` then runs the series' row-ID write, backfill, pruning, and
inlined-append sqllogictests against the built moraine artifact, and the
release workflow runs the same backfill-and-prune smoke against every
published build (`cargo xtask validate-release-artifact`).

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
target/duckdb-cli/v1.5.5/cli/duckdb -unsigned
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

## Backfill existing files

New files receive row-ID statistics when DuckLake registers them. Files that
were already active when the patched extension was installed remain safe but
unpruned: the absence of a statistics row means "unknown," so DuckLake keeps
the file in every row-ID-filtered scan. Repair them with:

```sql
SELECT * FROM ducklake_backfill_row_id_stats('lake');
```

The result has one row per selected table:

```text
schema_name  table_name  files_backfilled  files_remaining
```

Scope a run by schema or table and bound the total files processed by one
statement:

```sql
SELECT *
FROM ducklake_backfill_row_id_stats(
    'lake',
    schema := 'main',
    table_name := 'items',
    max_files := 100
);
```

Repeat bounded calls until every row reports `files_remaining = 0`.
`max_files` is shared across the selected tables and must be greater than
zero. Omitting it processes every missing non-empty active file.

The operation changes metadata only: it neither rewrites Parquet nor mints a
DuckLake snapshot. For an ordinary dense file it verifies that the reserved
row-ID column is absent, then derives the range from `row_id_start` and
`record_count`. For a rewrite or flushed file it reads the embedded row-ID
column's Parquet min/max; if those footer statistics are absent, it scans only
that physical column. Existing valid rows are left untouched, so the function
is idempotent. A concurrent catalog commit can make a call fail; rerun it.

With a Moraine metadata catalog, the Moraine extension must include support
for head-preserving reserved row-ID-stat inserts. Older Moraine binaries reject
the backfill commit even when the patched DuckLake binary exposes the
function.

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
the bundle for every version in `.github/duckdb-versions` on the four native
platforms, and before publishing runs the row-ID backfill and one-file
pruning smoke (`release-smoke.sql`) against the Linux amd64 and macOS arm64
builds of each version. A failed run leaves no partial public release.

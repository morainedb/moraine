# Patched DuckLake row-ID statistics, pruning, inlined writes, commit cleanup, and positional deletes

This directory carries a downstream DuckLake patch series for DuckDB v1.5.5,
applied in file-name order:

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
   `SELECT` producing the table's columns, bound as an ordinary `INSERT`
   through DuckDB's binder and planned with the positional deletes staged
   once the insert completes, all in the current transaction, which
   `moraine_update` rewrites into. Neither function has a Moraine
   dependency; explicit rollback, failed replacement inserts, repeated
   calls, and standalone autocommit are covered by `cargo xtask e2e`.

Later patches address the lines earlier ones produce, so the series is applied
in one `git apply` invocation rather than one per file.

The series is pinned separately to the DuckLake revisions selected by every
DuckDB release moraine supports. It is released as an unsigned companion
extension, not bundled into the moraine extension.
Most hunks use zero context to satisfy moraine's whitespace gate across both
source pins. The control-flow-sensitive row-ID statistics hunk replaces and
re-emits its function's return so it cannot land after that return.

The source mapping lives in `source-pins`. Each entry binds one DuckDB release
to the upstream DuckLake commit that release selects. A DuckDB bump must add a
validated mapping before the companion release can build.

## Build

Initialize the DuckDB submodule and build moraine once:

```sh
git submodule update --init duckdb
CC=gcc-14 CXX=g++-14 make release GEN=ninja OVERRIDE_GIT_DESCRIBE=v1.5.5
```

The compiler names above are for Debian and Ubuntu. Amazon Linux packages
the same pair as `gcc14-gcc` and `gcc14-g++`. macOS uses Apple Clang. Remove
`build/release` first if that tree was configured with a different compiler.

The first build supplies `build/release/src/libduckdb_static.a`. DuckDB's CLI
does not export the C++ symbols required by a thin extension, so the patched
extension must link that archive. The archive is reused; the following
command compiles DuckLake, not DuckDB core:

```sh
cargo xtask ducklake-patch
```

On Linux, the command selects GCC 14 to match DuckLake's extension pipeline.
This keeps the downstream source patch limited to the behaviour above; it
does not carry compiler-compatibility edits.

The command:

1. fetches the pinned DuckLake and vcpkg revisions under
   `target/patched-ducklake/`;
2. rejects a dirty DuckDB submodule, then applies the tracked patch series
   and verifies the cached checkout's complete diff byte-for-byte;
3. builds only `ducklake_loadable_extension` against moraine's exact DuckDB
   submodule and prebuilt static library; and
4. downloads the pinned DuckDB CLI if needed and verifies that the artifact
   loads; then runs the series' row-ID write, backfill, pruning, and
   inlined-append sqllogictests against that artifact.

The resulting extension is:

```text
target/patched-ducklake/build-extension-static/extension/ducklake/ducklake.duckdb_extension
```

Use `--root DIRECTORY` to move the gitignored build cache, or
`--duckdb-static FILE` to select another static archive built from moraine's
exact DuckDB pin.

## Load in DuckDB

Both local artifacts are unsigned, so start DuckDB with `-unsigned`. Load
moraine first, then the patched DuckLake extension by path:

```sh
target/duckdb-cli/v1.5.5/cli/duckdb -unsigned
```

```sql
LOAD 'build/release/extension/moraine/moraine.duckdb_extension';
LOAD 'target/patched-ducklake/build-extension-static/extension/ducklake/ducklake.duckdb_extension';

ATTACH 'ducklake:moraine:s3://bucket/catalog' AS lake (
    DATA_PATH 's3://bucket/data/',
    READ_ONLY
);
```

Do not `INSTALL` or `LOAD ducklake` afterward in the same process: that would
select the stock extension instead of this artifact. The CLI, moraine, the
DuckDB static archive, and DuckLake patches must all match DuckDB v1.5.5.

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

Dispatch the `Patched DuckLake extension` workflow with a tag such as
`ducklake-rowid-v0.1.0`. It calls DuckDB's extension distribution workflow for
every version in `.github/duckdb-versions` and publishes the same four native
platforms as moraine:

```text
ducklake.v1.5.5.linux_amd64.duckdb_extension
ducklake.v1.5.5.linux_arm64.duckdb_extension
ducklake.v1.5.5.osx_amd64.duckdb_extension
ducklake.v1.5.5.osx_arm64.duckdb_extension
```

The corresponding four v1.5.4 assets are included in the same release. The
publisher stays in draft mode until all eight assets exist and the Linux amd64
and macOS arm64 artifacts for both DuckDB versions pass the row-ID backfill
and one-file-pruning smoke test. A failed or cancelled run leaves no partial
public release.

Each artifact only loads into the exact DuckDB version and platform in its
name. Start DuckDB with unsigned extensions enabled, load moraine first, then
load the matching DuckLake artifact by path. Do not install or load stock
DuckLake afterward in the same process.

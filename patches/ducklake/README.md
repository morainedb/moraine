# Patched DuckLake row-ID statistics and pruning

This directory carries a downstream DuckLake patch for DuckDB v1.5.5. It
stores file-level row-ID min/max statistics in DuckLake's existing
`ducklake_file_column_stats` table and pushes `rowid` filters into its file
list before any Parquet reader is created. Moraine can therefore return stable
row ids without taking ownership of DuckLake scans or physical placement. A
metadata-only backfill repairs files written before the statistics patch was
installed.

The patch is pinned separately to the DuckLake revisions selected by every
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
This keeps the downstream source patch limited to row-ID statistics and
pruning; it does not carry compiler-compatibility edits.

The command:

1. fetches the pinned DuckLake and vcpkg revisions under
   `target/patched-ducklake/`;
2. rejects a dirty DuckDB submodule, then applies the tracked patch and
   verifies the cached checkout's complete diff byte-for-byte;
3. builds only `ducklake_loadable_extension` against moraine's exact DuckDB
   submodule and prebuilt static library; and
4. downloads the pinned DuckDB CLI if needed and verifies that the artifact
   loads; then runs the patch's row-ID write, backfill, and pruning
   sqllogictest against that artifact.

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
DuckDB static archive, and DuckLake patch must all match DuckDB v1.5.5.

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

Every lookup function returns a single `row_id` column. Join the lookup
directly to the DuckLake table in one relational query:

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
`moraine_index_nulls`. DuckDB turns the join key into a dynamic row-ID filter.
The patched DuckLake maps it to the reserved internal row-ID field and applies
its existing zone-map pruning while constructing the physical-file list.

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

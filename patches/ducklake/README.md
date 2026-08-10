# Patched DuckLake row-ID pruning

This directory carries a downstream DuckLake patch for DuckDB v1.5.5. It
stores file-level row-ID min/max statistics in DuckLake's existing
`ducklake_file_column_stats` table and pushes `rowid` filters into its file
list before any Parquet reader is created. Moraine can therefore return stable
row ids without taking ownership of DuckLake scans or physical placement.

The patch is pinned to the DuckLake revision selected by DuckDB v1.5.5. It is
an evaluation build, not part of the released moraine extension.
Its zero-context diff is intentional: the exact source pin makes context
unnecessary and keeps the patch file compatible with moraine's whitespace
gate.

## Build

Initialize the DuckDB submodule and build moraine once:

```sh
git submodule update --init duckdb
make release GEN=ninja OVERRIDE_GIT_DESCRIBE=v1.5.5
```

The first build supplies `build/release/src/libduckdb_static.a`. DuckDB's CLI
does not export the C++ symbols required by a thin extension, so the patched
extension must link that archive. The archive is reused; the following
command compiles DuckLake, not DuckDB core:

```sh
cargo xtask ducklake-patch
```

The patch also contains three explicit `std::move` returns needed only by
this standalone source build. DuckLake's local pointers have a derived type
while the functions return a base type; the v1.5.5 standalone CMake path uses
C++11, where DuckDB's custom `unique_ptr` does not apply the C++20 implicit
move rule. These lines transfer ownership without changing projection or
query-result behavior.

The command:

1. fetches the pinned DuckLake and vcpkg revisions under
   `target/patched-ducklake/`;
2. applies and verifies the tracked patch;
3. builds only `ducklake_loadable_extension` against moraine's exact DuckDB
   submodule and prebuilt static library; and
4. downloads the pinned DuckDB CLI if needed and verifies that the artifact
   loads.

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

Dense files receive a synthesized interval of
`[row_id_start, row_id_start + record_count - 1]`. Rewrite, UPDATE, and inline
flush files retain the actual min/max produced for their embedded
`_ducklake_internal_row_id` Parquet column. A legacy file with no row-ID stats
is always included. Sparse files can therefore cause false-positive reads
when their interval is broad, but cannot cause false negatives.

An absent lookup has exact cardinality zero at bind time. Moraine's optimizer
replaces it with `EMPTY_RESULT`, so DuckLake does not start a scan. For a hit,
DuckLake's table-global row id identifies the exact row while its ordinary
scan preserves delete, inline, and snapshot semantics.

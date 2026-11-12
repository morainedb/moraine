# moraine-duckdb

The DuckDB extension surface for [Moraine](../moraine). Two layers:
`cpp/` (a C++ shim linking DuckDB's internal C++ API) and `src/` (the Rust
C-ABI core, sync↔async bridge). The shim carries no DuckLake domain logic;
see the crate root docs and the
[extension surface design document](../../docs/rfcs/0006-extension-surface.md).

The extension registers a `duckdb::StorageExtension` under attach type
`moraine`, reachable two ways:

- **Primary path — `ATTACH 'ducklake:moraine:<store>' AS lake (DATA_PATH
  '<data-path>')`.** DuckLake nests `ATTACH 'moraine:<store>'` as its
  metadata connection (see "The `moraine:` prefix" below); every
  `ducklake_*` table the store models is synthesized in the catalog's
  `main` schema, and the writable ones accept DuckLake's own
  `INSERT`/`UPDATE`/`DELETE` as staged row mutations committed atomically.
  User-table data is read through **DuckLake's own reader**, not this
  crate's scan.
- **Secondary path — `ATTACH '<path>' AS m (TYPE moraine)`, or the bare
  `moraine:<path>` prefix.** Schema/table/view listing and `DESCRIBE` work
  through the C ABI; every `ducklake_*` projection is queryable directly,
  for independent verification of what DuckLake wrote. A `SELECT` against a
  real user table binds normally but raises `InvalidInputException` at
  execution time, redirecting to the `ducklake:moraine:` attach — table
  data is served only through DuckLake, never through the standalone
  attach.

DDL issued directly against a user schema/table (outside DuckLake's own
`ducklake_*` writes), plus querying a view's definition, raises
`NotImplementedException`.

## Using the extension

Attach options, table functions, caching, the write-ahead log, read-only
attaches, indexes, maintenance, encryption, backups, and upgrades are
documented on the project site:

- [Getting started](https://morainedb.com/guide/getting-started/)
- [Configuration](https://morainedb.com/guide/configuration/)
- [Operating manual](https://morainedb.com/guide/operating/)

The site is the reference for user-facing behaviour; this file covers how
the crate is built and how it serves DuckLake.

## How it is built

moraine-duckdb builds through DuckDB's own extension toolchain
(`extension-ci-tools`) — the same one the community-extensions repository
uses. The Rust crate is compiled as a **static library** (`crate-type =
["staticlib"]`) exporting the C ABI declared in `cpp/moraine_abi.h`; CMake
compiles the C++ shim, links that static library into it, and statically
links DuckDB — producing the loadable `moraine.duckdb_extension`. The
toolchain also writes the extension's metadata footer and, in the community
pipeline, signs the result. None of the extension↔DuckDB linking lives in
this crate; there is no `build.rs`.

Two git submodules pin the build:

- `duckdb/` — DuckDB source at the commit the manifest pins: the shim
  compiles against its full `src/include/` tree and links its static
  library. The pin is a commit rather than a tag because DuckDB 2.0 has
  none yet.
- `extension-ci-tools/` — the toolchain (Make + CMake helpers) at the
  commit the same manifest line names.

The moraine Rust static library is bridged into CMake with
[corrosion](https://github.com/corrosion-rs/corrosion) (see the repo-root
`CMakeLists.txt` and `extension_config.cmake`). Build locally with:

```sh
CC=gcc-14 CXX=g++-14 make release GEN=ninja   # Linux; also needs Rust + ninja
```

Linux builds use GCC 14, matching DuckLake's extension pipeline. macOS uses
the platform Apple Clang toolchain. `cargo xtask e2e` selects the Linux
compiler pair automatically. Remove `build/release` before the direct `make`
command if that tree was configured with a different compiler.

The loadable lands at `build/release/extension/moraine/moraine.duckdb_extension`
(gitignored). It bundles the patched DuckLake from `patches/ducklake/`, built
in the same tree and linked in, so one `LOAD` serves both. `cargo xtask e2e`
builds it that way and drives it through a real DuckDB CLI.

### The pin, and what a bump touches

`.github/duckdb-versions` is the single source of truth: one DuckDB release
per line, newest first, the first line carrying the commit each submodule
must sit on. `xtask` reads it (`include_str!`) and `cargo xtask check-pins`
fails if any other place naming a version disagrees — the two submodules,
all four workflow files, the table below, the DuckLake source pin every
supported release is built from, and the patch task's own target. It also
fails when a workflow builds the extension without waiting on the job that
loads what it built.

The single entry is DuckDB's 2.0 LTS release. The CLI the suites run is the
one the build produces beside the extension, stamped with the pin and so
certain to report it. A published build is proven in a second CLI: the one
that release itself carries, downloaded per platform, which is the engine
its users run.

| What | Pinned at |
|---|---|
| DuckDB | **v2.0.0** (git hash `d54cbbe3df`, codename Cyanoptera) |
| Toolchain | `duckdb/extension-ci-tools` at `78bd1e4abb` |
| C++ standard | C++17 |
| Linux C++ compiler | GCC 14 |
| DuckDB CLI (for `LOAD` testing) | `build/release/duckdb`, built beside the extension and verified to report the pin; a release is validated against the published CLI for the pin |
| DuckLake (bundled) | the commit `INSTALL ducklake` resolves to against the pinned CLI, plus `patches/ducklake/` — see "The bundled DuckLake" below |

**Bumping** is `cargo xtask bump-duckdb v2.0.0`: it moves both submodules
to that release, rewrites the manifest around it, and carries every derived
reference along — the workflow refs, the table above, the new release's
DuckLake source pin, and the DuckLake commit the new DuckDB declares,
wherever each is named. It stops short of the three things that are
judgement rather than transcription: the codename above, whether an older
release should now leave the manifest, and where the patch series lands in
the DuckLake the new release declares.

Then `cargo xtask check-pins` names whatever is still stale, and `cargo
xtask e2e` re-proves the whole chain against the new pair, including the
regression pins in `tests/ducklake_load/wire_contract.rs` — the nested
attach text, DuckLake's catalog access set, and the DuckLake commit
`INSTALL ducklake` resolves to. Expect that last one to move: DuckDB
hard-codes which DuckLake it installs, so a DuckDB bump changes it without
asking, and the bump prints the upstream compare URL for exactly that
reason.

**Which releases get builds.** Every one still listed in the manifest.
This is not a preference: DuckDB refuses a C++-ABI extension whose footer
names a different version *string*, patch releases included
(`ParsedExtensionMetaData::GetInvalidMetadataError`), so a user on one patch
release cannot load another's build. The list is short by design — each entry
multiplies the release build by five platforms, and only the primary is
proven end-to-end against a real DuckLake — so it holds the releases users
are plausibly on, and older ones keep whatever assets they were already
published with. While the only entry is a development pin, the builds run
and prove themselves but nothing is published.

**Which DuckDB series moraine tracks.** The one DuckLake's current release
branch targets, adopted when DuckLake cuts that branch rather than when
DuckDB releases: the extension exists to be DuckLake's catalog, and a
DuckDB with no DuckLake built for it has nothing to attach. DuckLake
versions by DuckDB-series branch (`v1.3-ossivalis`, `v1.4-andium`,
`v1.5-variegata`), so the two move together.

## Installing

The signed path is the extension repository the site serves, documented in
[Getting started](https://morainedb.com/guide/getting-started/#install-the-extension).
Its `.gz` assets on each release are the builds below with the signature
footer filled in; the publish action signs them with the
`DUCKDB_EXTENSION_SIGNING_PK` secret through `cargo xtask sign-release-assets`.

Release assets named `moraine.<duckdb-version>.<platform>.duckdb_extension`
are unsigned. Pick the one matching your DuckDB version exactly; a mismatch
is refused even when unsigned, because a C++-ABI extension is bound to the
version string in its footer. DuckDB decides at startup whether it loads
unsigned extensions, and the setting cannot be changed on a running
database: start the CLI with `-unsigned`, then load the file:

```sh
duckdb -unsigned -c "LOAD './moraine.v2.0.0.osx_arm64.duckdb_extension';"
```

DuckDB derives the entry symbol from the filename before its first `.`, so
the `moraine` prefix must stay.

Signing is DuckDB's: the public keys `ExtensionHelper::GetPublicKeys` trusts
are compiled into every DuckDB binary, so only the community-extensions
pipeline produces a signed build. `description.yml` points that pipeline at
this repository. Once published there, `INSTALL moraine FROM community;
LOAD moraine;` needs no flag.

## Obtaining a CLI for testing

`make release` builds `build/release/duckdb` from the `duckdb` submodule
alongside the extension, stamped with the pinned version string. That is
the CLI the suites load the artifact into: a loadable is refused by any
DuckDB reporting another version, and the build is what makes the two
agree. `cargo xtask e2e` passes it to every suite as
`MORAINE_DUCKDB_CLI` and checks that it reports the pin before doing so.

## The bundled DuckLake

Moraine does not require a separate DuckLake extension: the loadable links the
patched DuckLake from [`patches/ducklake/`](../../patches/ducklake/README.md)
and registers it from its own entry point, so `LOAD moraine` is the whole setup.
The bundle is recorded as the loaded `ducklake` extension, so a later
`LOAD ducklake` is a no-op and `duckdb_extensions()` reports its source
revision:

```text
$ duckdb -unsigned \
    -c "LOAD './build/release/extension/moraine/moraine.duckdb_extension';" \
    -c "SELECT extension_name, loaded, extension_version \
        FROM duckdb_extensions() WHERE extension_name IN ('ducklake', 'moraine');"
┌────────────────┬─────────┬───────────────────┐
│ extension_name │ loaded  │ extension_version │
├────────────────┼─────────┼───────────────────┤
│ ducklake       │ true    │ 417a911           │
│ moraine        │ true    │ …                 │
└────────────────┴─────────┴───────────────────┘
```

Loading stock DuckLake *before* moraine is refused: it lacks the patches
moraine's located functions rely on, and its log type would collide with
the bundle's.

The bundled source is the DuckLake commit the pinned DuckDB's own build-time pin
selects (`.github/config/extensions/ducklake.cmake` in the `duckdb/duckdb`
source tree names `GIT_URL https://github.com/duckdb/ducklake` at
`GIT_TAG 417a9117f590e8141742ac0579d02f6a90162e37`), so the bundle tracks
exactly what `INSTALL ducklake` would install against this DuckDB: that commit,
then DuckDB's own DuckLake patches, then the series on top.
`patches/ducklake/source-pins` records that commit per supported DuckDB release;
`cargo xtask check-pins` keeps it in step.

The patch series records file-level row-id min/max statistics, pushes
static and dynamic `rowid` filters into DuckLake's file-list query, exposes
data-file ids to scans, appends inlined rows, keeps files after unknown
commit outcomes, and adds positional deletes and updates. Index-assisted
reads remain a direct join on the stable row id; DuckLake owns file
selection, inlined data, snapshots, and deletes.

## Serving as DuckLake's metadata catalog

DuckLake drives moraine as its own metadata catalog by nesting an
`ATTACH 'moraine:<path>' ...` inside `ATTACH 'ducklake:moraine:<path>' AS
lake (DATA_PATH ...)`. The facts that attach chain depends on are pinned
against the DuckLake source at commit
`ac7595b0a1305bea3d4cfaca763b0ce964c763a2`.

### The `moraine:` prefix

No shim code parses it. DuckDB's own core does, unconditionally, for any
top-level `ATTACH '<prefix>:<path>' AS <name>` where no explicit `TYPE` is
given (`src/execution/operator/schema/physical_attach.cpp`):

```cpp
if (options.db_type.empty()) {
    DBPathAndType::ExtractExtensionPrefix(path, options.db_type);
}
```

`ExtractExtensionPrefix` (`src/main/database_path_and_type.cpp`) takes
everything before the first `:` (rejecting `<2`-character prefixes, so
Windows drive letters like `C:` are never misread, and rejecting a `://`
suffix, so URLs are never misread), lowercases it, and hands the
*stripped* remainder on as `info.path`. `AttachDatabase` then looks up a
`StorageExtension` registered under that exact name — which is `"moraine"`,
the name `RegisterMoraineStorageExtension` (`cpp/storage_extension.cpp`)
registers for the `TYPE moraine` form. So `moraine:<path>` and `<path>` +
`TYPE moraine` converge on the identical `MoraineCatalog::Attach` call with
an identical, already-stripped `info.path` — no code change needed.

DuckLake's own `DuckLakeAttach` (`src/storage/ducklake_storage.cpp`)
constructs the nested path: `options.metadata_path = info.path` (the literal
string after `ducklake:` is stripped by the *same* mechanism one level up),
and `options.metadata_database = "__ducklake_metadata_" + name`.
`DuckLakeInitializer::Initialize` then issues that as a top-level statement,
so the prefix dispatch fires again, unmodified. The generated text is
captured from a running session rather than read off the source — DuckDB's
`QueryLog` records DuckLake's own metadata connection — and pinned by
`tests/ducklake_load/wire_contract.rs`:

```sql
ATTACH OR REPLACE 'moraine:<path>' AS "__ducklake_metadata_warehouse" (HIDDEN true)
SELECT NULL FROM "__ducklake_metadata_warehouse"."main".ducklake_metadata LIMIT 1
```

Two consequences of that shape. The catalog name is derived from the outer
alias, so `AS warehouse` nests `__ducklake_metadata_warehouse` — the
metadata catalog is addressable by name (`SELECT * FROM
__ducklake_metadata_warehouse.main.ducklake_snapshot` works, and is how
these tests inspect what DuckLake wrote). And `HIDDEN true` keeps it out of
`duckdb_databases()`, which lists only the outer `warehouse` — whose `path`
column is exactly the nested attach string, `moraine:<path>`.

The schema DuckLake queries is `main` — `duckdb::Catalog`'s base-class
default, which `MoraineCatalog` never overrides, and the schema bootstrap
mints from snapshot 0. So every moraine store is DuckLake-attachable from
birth, with synthesized `ducklake_*` tables and any user tables sharing the
same catalog and `main` schema namespace.

### Pinned `ducklake_*` table shapes

Every column list is transcribed verbatim from
`DuckLakeMetadataManager::InitializeDuckLake`'s bootstrap SQL text in the
pinned DuckLake source (`src/storage/ducklake_metadata_manager.cpp`).
`not null` marks only columns DuckLake itself declares `NOT NULL`/`PRIMARY
KEY`. `moraine-duckdb`'s C++ source is the single source of truth
(`cpp/metadata_table_specs.cpp`'s `MetadataTableSpecsImpl`); this table is a
human-readable mirror of it.

| Table | Fed from | Notes |
|---|---|---|
| `ducklake_snapshot` | `moraine_dump_snapshots` | shares one dump call with `ducklake_snapshot_changes` (the store models them as one merged record) |
| `ducklake_snapshot_changes` | `moraine_dump_snapshots` | see above |
| `ducklake_schema` | `moraine_dump_schemas` | current + history rows |
| `ducklake_table` | `moraine_dump_tables` | current + history rows; `table_id` has no `PRIMARY KEY` in DuckLake's own schema, left nullable to match |
| `ducklake_view` | `moraine_dump_views` | current + history rows |
| `ducklake_column` | `moraine_dump_columns` | `column_type` is translated, not passed through verbatim — see below |
| `ducklake_data_file` | `moraine_dump_data_files` | widest row (16 real columns) |
| `ducklake_delete_file` | `moraine_dump_delete_files` | |
| `ducklake_table_stats` | `moraine_dump_table_stats` | unversioned |
| `ducklake_table_column_stats` | `moraine_dump_table_column_stats` | unversioned |
| `ducklake_file_column_stats` | `moraine_dump_file_column_stats` | unversioned |
| `ducklake_metadata` | store facts plus `moraine_dump_options` | staged reads overlay option changes |
| `ducklake_schema_versions` | `moraine_dump_schema_versions` | one row per `(table_id, schema_version)` transition |
| `ducklake_inlined_data_tables` | `moraine_inline_registered_tables` (`ProvideInlinedDataTables`) | every `(table_id, schema_version)` with a recorded `inline/schema`; see "Data inlining" below — writable only as a no-op (`kVoidInsertable`), since `CreateInlineDataTable` already stages the registration this table's own `INSERT` would double-register |
| `ducklake_partition_info` | `moraine_dump_partition_info` | current + history rows; partition columns embed in the spec's record |
| `ducklake_partition_column` | `moraine_dump_partition_columns` | flattened from the spec records' embedded columns |
| `ducklake_file_partition_value` | `moraine_dump_file_partition_values` | flattened from the data-file records' embedded values |
| `ducklake_sort_info` | `moraine_dump_sort_info` | current + history rows; expressions embed in the spec's record |
| `ducklake_sort_expression` | `moraine_dump_sort_expressions` | flattened from the spec records' embedded expressions |
| `ducklake_tag` | `moraine_dump_tags` | one row per embedded entry of the object's container record, ended entries included (each carries its own begin/end) |
| `ducklake_column_tag` | `moraine_dump_column_tags` | flattened from each column's latest record — a column version transition carries entries forward, so only that record's set is emitted |
| `ducklake_files_scheduled_for_deletion` | `moraine_dump_scheduled_deletions` | the physical-deletion schedule (`current/gcfile`, keyed by the scheduled file's id); written by expiry/compaction, drained by `ducklake_cleanup_old_files` |
| `ducklake_macro`, `ducklake_macro_impl`, `ducklake_macro_parameters`, `ducklake_file_variant_stats`, `ducklake_column_mapping`, `ducklake_name_mapping` | corresponding `moraine_dump_*` providers | empty when the store has no records of that kind |

Every table DuckLake queries is registered, even when its provider returns no
rows. DuckLake's catalog queries join these tables unconditionally; a missing
table would fail binding before its emptiness could matter.

The exists-probe `SELECT NULL FROM ducklake_metadata LIMIT 1` references
zero real columns; DuckDB's optimizer only takes that "virtual column" scan
shape for a table function that advertises `projection_pushdown = true`
(otherwise `Not implemented Error: Virtual columns require projection
pushdown` fires before the scan callback runs). `MetadataScanTableFunction`
(`cpp/metadata_tables.cpp`) sets the flag and carries
`TableFunctionInitInput`'s `column_ids` through to the `.function` callback,
which projects exactly those columns out of the already-materialized row
set.

### `ducklake_column.column_type`: two type vocabularies, one stored string

Moraine's catalog stores column types as DuckDB SQL syntax (`"BIGINT"`,
`"DOUBLE"`, ...) — what `ColumnDef::column_type` carries, and what
`MapColumnType` (used for the standalone attach's `DESCRIBE`) parses.
DuckLake's `ducklake_column.column_type`, read back through its own
`DuckLakeTypes::FromString` (`src/common/ducklake_types.cpp`), accepts a
*different*, lowercase vocabulary instead (`"int64"`, `"float64"`,
`"timestamptz"`, ...); serving the stored string verbatim throws `Invalid
Input Error: Failed to parse DuckLake type - unsupported type 'BIGINT'`. One
translation point (`DuckLakeColumnType` in `cpp/metadata_table_specs.cpp`)
reparses the stored SQL string through `MapColumnType`, then names the
resulting `LogicalTypeId` DuckLake's way — never two independently
maintained type tables. Every type `MapColumnType` supports maps exactly,
except `DECIMAL`'s width/scale suffix and `JSON`. `JSON` is a `VARCHAR`
carrying a `"JSON"` alias, so it is matched on the alias in both directions
(`MapColumnType` maps `"json"` to `LogicalType::JSON()`; `DuckLakeColumnType`
names an aliased-JSON type `"json"` before its `LogicalTypeId` would collapse
to `"varchar"`) — mirroring DuckLake's own `DuckLakeTypes` handling.

The supported scalars track DuckLake's full vocabulary: every signed/unsigned
integer width including `int128`/`uint128`, `float32`/`float64`, `decimal`,
`varchar`/`blob`/`boolean`/`uuid`, the temporal types
(`date`, `time`, `time_ns`, `timetz`, `timestamp`, `timestamp_s`/`_ms`/`_ns`,
`timestamptz`, `interval`), `json`, and `geometry` (a distinct `LogicalTypeId`
needing the `spatial` extension at runtime for values, not for the type or its
Arrow encoding). `variant` is **not** supported: moraine serializes inline data
through Arrow, and DuckDB's Arrow format has no VARIANT representation, so a
`VARIANT` column is rejected at creation with an actionable error (unlike
GEOMETRY, whose Arrow support `spatial` registers).

### `ducklake_metadata` synthesis

Pinned from `DuckLakeInitializer::LoadExistingDuckLake`
(`src/storage/ducklake_initializer.cpp`) — the keys it reads after the
exists-probe (`SELECT NULL FROM ducklake_metadata LIMIT 1`) succeeds:

| Key | Served value | Why |
|---|---|---|
| `version` | the recorded catalog version, `"1.0"` or `"1.1"` | chooses the `ducklake_*` shape served; raised with `moraine_raise_catalog_version` |
| `encrypted` | the value recorded at creation | set from `ENCRYPTED`; DuckLake generates per-file keys and Moraine stores them |
| `created_by` | `"moraine"` | never read back by DuckLake's own init path; served anyway since DuckLake itself writes it at bootstrap and it costs nothing |
| `data_path` | the recorded root, or **not served** | Recorded once, by DuckLake's bootstrap insert or from `META_DATA_PATH` on a lake that predates it, and served back normalized with a trailing separator, so `LoadExistingDuckLake` reads it and a re-attach need not repeat `DATA_PATH`; a lake with none recorded omits the row, leaving the ATTACH `DATA_PATH` as the authority. The recorded value is fixed — a conflicting `META_DATA_PATH` is refused |

All rows are global (`scope`/`scope_id` `NULL`) — no schema/table-scoped
DuckLake settings exist to serve.

### Data inlining

Inlining is on catalog-wide at DuckLake's own compiled default of ten
rows, which `ducklake_metadata` serves no row for on purpose: an option
row outranks both `ATTACH ... (DATA_INLINING_ROW_LIMIT n)` and
`SET ducklake_default_data_inlining_row_limit`, so serving one would
shadow every way an operator has of raising the limit. With inlining
on, DuckLake dynamically creates and drives per-table physical tables in
the metadata catalog instead of writing fixed `ducklake_*` rows for small
inserts; `cpp/inline_tables.cpp` recognizes two dynamic name families —
`ducklake_inlined_data_<table_id>_<schema_version>` (columns `row_id`,
`begin_snapshot`, `end_snapshot`, the table's user columns) and
`ducklake_inlined_delete_<table_id>` (columns `file_id`, `row_id`,
`begin_snapshot`) — and routes `CREATE`/`INSERT`/`UPDATE`/`DELETE`/
`SELECT` against them into the `inline/*` keyspace over the same
staged-row commit path the fixed tables ride, instead of materializing
real tables.

A `SELECT` against the data family is served in windows rather than
materialized. Binding reads nothing; the scan opens a core cursor when the
plan initializes it and pulls 8 × `STANDARD_VECTOR_SIZE` rows at a time,
decoding only those rows' chunk bodies and releasing them before the next
window, so a flush of a large inlined table never holds more than a window
of it. A scan that projects no user column — the flush's own `DELETE`, a
`COUNT(*)` — reads no body at all. `UPDATE` and `DELETE` resolve their
physical rowids against a per-statement ledger of what that plan's scan
emitted, which carries row ids and begin snapshots and no payload.

One property of the delete family is worth stating because it is easy to get
wrong: it exists from the table's first inlined deletion until the table is
dropped, and **emptying it does not remove it**. `ducklake_flush_inlined_data`
writes those deletions out as a real delete file and then clears the table with
an unqualified `DELETE`; DuckLake caches the table's existence for the life of
the catalog and never re-probes, so anything it runs afterwards in the same
session still binds against it. Existence is therefore recorded in the store
(`inline/file_delete_table`) rather than derived from whether any deletion is
currently held. What removes it is the `DROP TABLE` the expiry cleanup issues
once the table has left live state and history, which takes marker and deletions
together. See `docs/rfcs/0004-data-inlining.md`'s "Extension surface (as
implemented)" for the exact operation → keyspace mapping.

Chunk bodies (`inline/schema`, `inline/insert`) are Arrow IPC. DuckDB's C++
has no IPC serializer, so the work splits along the Arrow C Data
Interface: `inline_tables.cpp` converts a `DataChunk`'s user columns to
`ArrowArray`/`ArrowSchema` with DuckDB's `ArrowConverter` and hands them
to the Rust bridge (`src/arrow_ipc.rs`), which serializes them to IPC with
`arrow-rs`. Decode reverses it — Rust rebuilds the C Data Interface
structs from the IPC bytes and the shim feeds them to DuckDB's own
record-batch importer (`ArrowTableFunction::ArrowToDuckDB`). Because DuckDB
owns both export and import, the encoding is exactly as type-faithful as
DuckDB's Arrow support, nulls and nested types included.

Two DuckDB-internal contracts the import path depends on, both silent on
violation: `ArrowToDuckDB` reads `output.size()` as the row count to
convert, so the output `DataChunk`'s cardinality must be set *before* the
call; and the per-column `ColumnArrowToDuckDB` does not apply a column's
validity itself — its caller must run `SetValidityMask` first, or every
null silently reads back as a default value. `inline/insert` carries the
record-batch body only (no schema message), decoded against the version's
`inline/schema` schema-only stream so the schema is not re-serialized per
chunk; `inline/schema` also reconstructs a looked-up table's columns.

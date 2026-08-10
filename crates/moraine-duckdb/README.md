# moraine-duckdb

The DuckDB extension surface for [moraine](../moraine). Two layers:
`cpp/` (a C++ shim linking DuckDB's internal C++ API) and `src/` (the Rust
C-ABI core, sync↔async bridge). The shim carries no DuckLake domain logic —
see the crate root docs and RFC 0006.

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

## Attach modes: one writer, many readers

**Exactly one process may attach a given store read-write.** Every other
process must pass `READ_ONLY`. SlateDB fences by *newest writer wins*, so a
second read-write attach does not fail — it takes the writer role from the
incumbent, whose next commit then raises the `FENCED` error telling it to
re-attach. Two processes attaching read-write take turns breaking each
other, and neither is told at attach time.

```sql
-- The one writer.
ATTACH 'ducklake:moraine:s3://bucket/prefix' AS lake
  (DATA_PATH 's3://bucket/prefix-data/', READ_WRITE);

-- Every other process.
ATTACH 'ducklake:moraine:s3://bucket/prefix' AS lake
  (DATA_PATH 's3://bucket/prefix-data/', READ_ONLY);
```

A read-only attach opens SlateDB's `DbReader`, never the writer `Db`: it
never fences anyone, never participates in fencing, and any number may run
alongside the writer. DuckDB resolves `READ_ONLY` into the access mode this
shim reads, and DuckLake forwards it into the nested metadata attach.

`READ_ONLY` is read-only at the **catalog** level, not the IAM level. A
follow-latest reader writes a checkpoint into the manifest on open and
refreshes it for the attach's lifetime, so reader credentials still need
manifest write access. For strictly read-only credentials, see
`CHECKPOINT` below.

**`CHECKPOINT` — a truly-zero-write read-only attach.** Given the id of a
SlateDB checkpoint created ahead of time, moraine opens the reader against
that checkpoint instead of establishing its own: no manifest CAS, no
refresh, no delete on close — the attach issues object-store reads and
nothing else.

```sql
-- On the writer, once: mint a checkpoint and note its id.
SELECT checkpoint_id FROM moraine_create_checkpoint('lake');

-- Thereafter, from a process holding only s3:GetObject/ListBucket:
ATTACH 'ducklake:moraine:s3://bucket/prefix' AS lake
  (DATA_PATH 's3://bucket/prefix-data/', READ_ONLY,
   META_CHECKPOINT '4a1f…');
```

The cost is that the attach reads a **fixed cut**: it never sees commits
made after the checkpoint, because seeing them is exactly what the manifest
poll it is forgoing would do. Refreshing means minting a new checkpoint and
re-attaching. `CHECKPOINT` requires `READ_ONLY` — a read-write attach
naming one is refused — and a checkpoint the manifest no longer carries
fails the attach rather than silently falling back to latest.

`moraine_create_checkpoint` names an **attached catalog** rather than a
store path, because the core mints through the writer that attach already
opened and a second read-write open would fence it. The other two name a
store path, and neither opens the writer, so both run against a live
catalog: `moraine_checkpoints('<store>')` lists what the manifest carries
— which is how a checkpoint whose id was lost is found, since one given no
lifetime pins its objects until deleted — and
`moraine_delete_checkpoint('<store>', '<id>')` releases one.

**Creating or writing an S3 lake requires `READ_WRITE`.** DuckDB opens any
attach whose path starts with a remote prefix (`s3://`, `gcs://`, `azure://`,
…) read-only by default, and a read-only attach cannot create a catalog. To
create or write a lake whose moraine catalog lives on S3, add `READ_WRITE`:

```sql
CREATE SECRET s (TYPE s3, KEY_ID '…', SECRET '…', REGION 'us-west-2');
ATTACH 'ducklake:moraine:s3://bucket/prefix' AS lake
  (DATA_PATH 's3://bucket/prefix-data/', READ_WRITE);
```

Local and `memory://` stores default to read-write and need no flag. A
read-only attach of an uninitialized store fails with an error that names this
fix.

**`CACHE_DIR` — on-disk object cache for S3 catalogs.** Each query reads the
catalog metadata from the store; on S3 that is network latency every time, and
the in-memory caches start empty in each new process. Point SlateDB's disk
cache of fetched object parts at a local directory so warm reads survive
restarts and repeat queries skip the GETs — `META_CACHE_DIR` through the
DuckLake attach (or `CACHE_DIR` directly on a standalone `moraine:` attach):

```sql
ATTACH 'ducklake:moraine:s3://bucket/prefix' AS lake
  (DATA_PATH 's3://bucket/prefix-data/', READ_WRITE, META_CACHE_DIR '/var/cache/moraine');
```

Unset, only the in-memory caches apply; redundant for local/`memory://` stores.
Those are a separate tier — a block cache and a metadata cache, in memory, per
attached store — and moraine leaves both at SlateDB's sizes.

**`CACHE_SIZE` — how much disk that object cache may take.** Set the cap
explicitly — a byte count — and size the volume for the number of stores
attached, since the cap is per store rather than per directory:

```sql
ATTACH 'ducklake:moraine:s3://bucket/prefix' AS lake
  (DATA_PATH 's3://bucket/prefix-data/', READ_WRITE,
   META_CACHE_DIR '/var/cache/moraine', META_CACHE_SIZE 2147483648);
```

Unset, SlateDB's 16 GiB cap stands; without a `META_CACHE_DIR` there is no
object cache to bound.

**`CACHE_PUTS` — fill the cache as you write, not just as you read.** By
default the cache is filled by reads alone, so an object the store just wrote
is fetched back from S3 the first time it is read. `META_CACHE_PUTS true`
caches it at write time instead — one local write, no fetch — and because store
objects are immutable and land atomically, read-only sessions sharing the same
`META_CACHE_DIR` on that host read what the writer cached:

```sql
ATTACH 'ducklake:moraine:s3://bucket/prefix' AS lake
  (DATA_PATH 's3://bucket/prefix-data/', READ_WRITE,
   META_CACHE_DIR '/var/cache/moraine', META_CACHE_PUTS true);
```

Opt-in, because compaction output is cached the same way and a large merge can
evict what reads had warmed.

**`CACHE_PRELOAD` — warm the cache during ATTACH, not during the first query.**
Both of the above still leave a fresh process cold. `META_CACHE_PRELOAD 'all'`
loads every object the manifest references before the attach returns, `'l0'`
only the objects no merge has folded down yet, `'none'` (the default) nothing:

```sql
ATTACH 'ducklake:moraine:s3://bucket/prefix' AS lake
  (DATA_PATH 's3://bucket/prefix-data/', READ_WRITE,
   META_CACHE_DIR '/var/cache/moraine', META_CACHE_PRELOAD 'all');
```

The wait is the attach's: the load runs inside the open and skips anything it
cannot fetch. It is bounded by `META_CACHE_SIZE`, and it **stops** at the first
object that would exceed the cap rather than skipping that one and continuing —
so a cap smaller than the store leaves the tail of it unloaded. moraine logs a
warning at attach when that is the case. `'all'` pays a slower ATTACH for a
first query that touches S3 not at all, and is worth it when the whole store
fits the cache — check with `moraine_store_census`.

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

- `duckdb/` — DuckDB source at tag **v1.5.5**: the shim compiles against its
  full `src/include/` tree and links its static library.
- `extension-ci-tools/` — the toolchain (Make + CMake helpers) at the
  matching **v1.5.5**.

The moraine Rust static library is bridged into CMake with
[corrosion](https://github.com/corrosion-rs/corrosion) (see the repo-root
`CMakeLists.txt` and `extension_config.cmake`). Build locally with:

```sh
make release GEN=ninja   # needs ninja + a Rust toolchain
```

The loadable lands at `build/release/extension/moraine/moraine.duckdb_extension`
(gitignored). `cargo xtask e2e` builds it that way and drives it through a
real DuckDB CLI plus a real `INSTALL ducklake`.

### The pin, and what a bump touches

`.github/duckdb-versions` is the single source of truth: one DuckDB release
per line, newest first, the first line carrying the commit each submodule
must sit on. `xtask` reads it (`include_str!`), the release workflows build
a matrix from it (`cargo xtask version-matrix`), and `cargo xtask
check-pins` fails if any other place naming a version disagrees — the two
submodules, both workflow files, and the table below.

| What | Pinned at |
|---|---|
| DuckDB | **v1.5.5** (git hash `d8cdaa33fd`, codename Variegata) |
| Toolchain | `duckdb/extension-ci-tools` branch **v1.5.5** |
| C++ standard | C++17 |
| DuckDB CLI (for `LOAD` testing) | downloaded from the GitHub release, cached under `target/duckdb-cli/<version>/` (never committed) |
| DuckLake extension | `INSTALL ducklake` against the pinned CLI — see "Obtaining the DuckLake extension" below |

**Bumping** is `cargo xtask bump-duckdb v1.5.6`: it moves both submodules
to that release, rewrites the manifest around it, and carries every derived
reference along — the workflow refs, the table above, and the DuckLake
commit the new DuckDB declares. It stops short of the two things that are
judgement rather than transcription: the codename above, and whether an
older release should now leave the manifest.

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
published with.

**Which DuckDB series moraine tracks.** The one DuckLake's current release
branch targets, adopted when DuckLake cuts that branch rather than when
DuckDB releases: the extension exists to be DuckLake's catalog, and a
DuckDB with no DuckLake built for it has nothing to attach. DuckLake
versions by DuckDB-series branch (`v1.3-ossivalis`, `v1.4-andium`,
`v1.5-variegata`), so the two move together.

## Installing

```sql
INSTALL moraine FROM community;
LOAD moraine;
```

That path verifies a signature and needs no flags. **Signing is DuckDB's,
not moraine's**: the public keys `ExtensionHelper::GetPublicKeys` trusts are
compiled into every DuckDB binary, so no third party can produce a
signature the stock CLI accepts. `extension-upload-single.sh` fills the
footer's 256-byte signature region with zeros unless it holds one of those
private keys, which is why every artifact moraine's own release workflow
publishes is unsigned, and why the e2e harness starts the CLI with
`-unsigned`. The community-extensions pipeline is the only route to a
signed build, and `description.yml` is what points it at this repo.

To load a release asset or a locally built artifact directly, start the CLI
with `-unsigned` — the setting cannot be changed on a running database:

```sh
duckdb -unsigned -c "LOAD './build/release/extension/moraine/moraine.duckdb_extension';"
```

Release assets are named `moraine.<duckdb-version>.<platform>.duckdb_extension`
(`moraine.v1.5.5.linux_amd64.duckdb_extension`). Pick the one matching your
DuckDB *exactly*: a mismatch is rejected even when unsigned, because a
C++-ABI extension is bound to the version string in its footer. The
loadable's base filename (`moraine`) is load-bearing too — DuckDB derives
the entry symbol (`moraine_duckdb_cpp_init`, defined in
`cpp/moraine_extension.cpp`) from the filename before the first `.` — so
rename an asset to `moraine.duckdb_extension` before loading it.

## Obtaining a DuckDB v1.5.5 CLI for testing

Downloaded directly from the GitHub release, no build required:

```
https://github.com/duckdb/duckdb/releases/download/v1.5.5/duckdb_cli-osx-arm64.zip   # this machine
https://github.com/duckdb/duckdb/releases/download/v1.5.5/duckdb_cli-osx-universal.zip
https://github.com/duckdb/duckdb/releases/download/v1.5.5/duckdb_cli-linux-amd64.zip
https://github.com/duckdb/duckdb/releases/download/v1.5.5/duckdb_cli-linux-arm64.zip
# (+ windows-amd64/arm64, and -musl variants for linux)
```

Cached under `target/duckdb-cli/<version>/` (gitignored, never committed) —
keyed by version, so a pin bump downloads afresh rather than handing back
a CLI the new build cannot load into. The CLI is
downloaded from a release asset; the DuckDB *source* the extension builds
against comes from the `duckdb` submodule, not this download.

## Obtaining the DuckLake extension

`INSTALL ducklake` against the pinned `v1.5.5` CLI deterministically
resolves and installs DuckLake — no version pin of our own is needed beyond
the DuckDB version:

```
$ target/duckdb-cli/v1.5.5/cli/duckdb \
    -c "INSTALL ducklake;" -c "LOAD ducklake;" \
    -c "SELECT extension_name, extension_version, install_mode, installed_from \
        FROM duckdb_extensions() WHERE extension_name='ducklake';"
┌────────────────┬────────────────────┬──────────────┬────────────────┐
│ extension_name │ extension_version  │ install_mode │ installed_from │
├────────────────┼────────────────────┼──────────────┼────────────────┤
│ ducklake       │ d8a1881e           │ REPOSITORY   │ core           │
└────────────────┴────────────────────┴──────────────┴────────────────┘
```

`extension_version` is DuckLake's own short git commit hash, resolved from
DuckDB v1.5.5's own build-time pin
(`.github/config/extensions/ducklake.cmake` in the `duckdb/duckdb` source
tree names `GIT_URL https://github.com/duckdb/ducklake` at
`GIT_TAG d8a1881e22516ea3d186d73e83c65fe5bd1a1dc4`) — `INSTALL ducklake`
against this exact CLI build always resolves to this exact commit,
deterministically, from DuckDB's `core` extension repository
(`installed_from: core`, not the community repository).

**Caching under `target/`, not the CLI's default `~/.duckdb/extensions/`.**
`INSTALL`'s default cache is the user's home directory, outside this
repo's `target/` convention. Redirect it with a `SET` run before
`INSTALL`/`LOAD`:

```
$ duckdb -c "SET extension_directory='target/duckdb-extensions';" \
         -c "INSTALL ducklake;" -c "LOAD ducklake;" \
         -c "SELECT install_path FROM duckdb_extensions() WHERE extension_name='ducklake';"
┌──────────────────────────────────────────────────────────────────┐
│                            install_path                          │
├──────────────────────────────────────────────────────────────────┤
│ target/duckdb-extensions/v1.5.5/osx_arm64/ducklake.duckdb_extension │
└──────────────────────────────────────────────────────────────────┘
```

`xtask e2e` runs `SET extension_directory=...` + `INSTALL ducklake` +
`LOAD ducklake` for real on every invocation, against
`crates/moraine-duckdb/tests/ducklake_load.rs`.

For evaluating pre-reader pruning from moraine index row ids, the repository
also carries a pinned downstream DuckLake patch and an extension-only build
command. It records file-level row-id min/max statistics and pushes static and
dynamic `rowid` filters into DuckLake's file-list query. Index-assisted reads
remain a direct join on the stable row id; DuckLake owns file selection,
inlined data, snapshots, and deletes. See
[`patches/ducklake/`](../../patches/ducklake/README.md) for the build, load,
and query shape.

## Serving as DuckLake's metadata catalog

DuckLake drives moraine as its own metadata catalog by nesting an
`ATTACH 'moraine:<path>' ...` inside `ATTACH 'ducklake:moraine:<path>' AS
lake (DATA_PATH ...)`. The facts that attach chain depends on are pinned
against the DuckLake source at commit
`d8a1881e22516ea3d186d73e83c65fe5bd1a1dc4`.

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
(`cpp/metadata_tables.cpp`'s `MetadataTableSpecsImpl`); this table is a
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
| `ducklake_metadata` | synthesized in C++, no ABI call | see below |
| `ducklake_schema_versions` | `moraine_dump_schema_versions` (`ProvideSchemaVersions`) | one row per `(table_id, schema_version)` transition |
| `ducklake_inlined_data_tables` | `moraine_inline_registered_tables` (`ProvideInlinedDataTables`) | every `(table_id, schema_version)` with a recorded `inline/schema`; see "Data inlining" below — writable only as a no-op (`kVoidInsertable`), since `CreateInlineDataTable` already stages the registration this table's own `INSERT` would double-register |
| `ducklake_partition_info` | `moraine_dump_partition_info` | current + history rows; partition columns embed in the spec's record |
| `ducklake_partition_column` | `moraine_dump_partition_columns` | flattened from the spec records' embedded columns |
| `ducklake_file_partition_value` | `moraine_dump_file_partition_values` | flattened from the data-file records' embedded values |
| `ducklake_sort_info` | `moraine_dump_sort_info` | current + history rows; expressions embed in the spec's record |
| `ducklake_sort_expression` | `moraine_dump_sort_expressions` | flattened from the spec records' embedded expressions |
| `ducklake_tag` | `moraine_dump_tags` | one row per embedded entry of the object's container record, ended entries included (each carries its own begin/end) |
| `ducklake_column_tag` | `moraine_dump_column_tags` | flattened from each column's latest record — a column version transition carries entries forward, so only that record's set is emitted |
| `ducklake_files_scheduled_for_deletion` | `moraine_dump_scheduled_deletions` | the physical-deletion schedule (`current/gcfile`, keyed by the scheduled file's id); written by expiry/compaction, drained by `ducklake_cleanup_old_files` |
| `ducklake_macro`, `ducklake_macro_impl`, `ducklake_macro_parameters`, `ducklake_file_variant_stats`, `ducklake_column_mapping`, `ducklake_name_mapping` | always empty | store models none of these kinds — a missing table is a bind-time error even for a query that would return zero rows (see below) |

Every table DuckLake's own schema defines is served, either with real data
or as an always-empty stand-in; none are left unbound. This is required:
`DuckLakeMetadataManager::BuildCatalogForSnapshot`, the query DuckLake's own
attach/snapshot-load always runs, joins and correlated-subqueries
`ducklake_tag`, `ducklake_column_tag`, `ducklake_inlined_data_tables`,
`ducklake_macro(_impl/_parameters)`, and `ducklake_partition_info`/`_column`
unconditionally while resolving basic table/view/schema info — a *missing*
table is a bind-time `Catalog Error` even though the query would otherwise
return zero rows for it. So absent store-modeled data means an empty table
(`ProvideEmpty` in `cpp/metadata_tables.cpp`), never an absent SQL table.

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
translation point (`DuckLakeColumnType` in `cpp/metadata_tables.cpp`)
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
| `version` | `"1.0"` | compared against `"1.0"` exactly; anything else triggers migration logic (`MigrateV01`/`V02`/...) never wired up — the schema served is already 1.0-shaped |
| `encrypted` | `"false"` | read unconditionally, sets `DuckLakeEncryption`; moraine has no encryption support |
| `created_by` | `"moraine"` | never read back by DuckLake's own init path; served anyway since DuckLake itself writes it at bootstrap and it costs nothing |
| `data_path` | the recorded root, or **not served** | Recorded once from `META_DATA_PATH` (at bootstrap, or on the first attach of a lake that predates it) and served back normalized with a trailing separator, so `LoadExistingDuckLake` reads it and a re-attach need not repeat `DATA_PATH`; a lake with none recorded omits the row, leaving the ATTACH `DATA_PATH` as the authority. The recorded value is fixed — a conflicting `META_DATA_PATH` is refused |

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

One property of the delete family is worth stating because it is easy to
get wrong: it exists from the table's first inlined deletion until the
table is dropped, and **emptying it does not remove it**.
`ducklake_flush_inlined_data` writes those deletions out as a real delete
file and then clears the table with an unqualified `DELETE`; DuckLake
caches the table's existence for the life of the catalog and never
re-probes, so anything it runs afterwards in the same session still binds
against it. Existence is therefore recorded in the store
(`inline/file_delete_table`) rather than derived from whether any
deletion is currently held. See `docs/rfcs/0005-data-inlining.md`'s "Extension surface
(as implemented)" for the exact operation → keyspace mapping.

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

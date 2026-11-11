# RFC 0002: Public API

## Summary

The `moraine` crate exposes four handle types, `Catalog`, `ReadOnlyCatalog`,
`ReadScope`, and `Transaction`, over hand-written domain structs and one
`Error` enum with a variant per failure domain. A host opens a catalog on an
`object_store`, reads it through pinned scopes, and mutates it through a
closure the catalog may run more than once. No SlateDB or protobuf type
appears in a public signature. `crates/moraine/src/lib.rs` is the complete
list of exported items.

## Handle types

- **`ReadOnlyCatalog`** is the read surface. It carries every read and no
  mutator.
- **`Catalog`** is the read-write handle. It implements `Deref<Target =
  ReadOnlyCatalog>` and adds the mutators. Clones share one store handle, so
  `close` through any clone closes all. One process holds a `Catalog` per
  store, and a second open fences the first.
- **`ReadScope`** pins one store revision and one snapshot id, current or
  historical. Each lookup reads only the records it needs, and the scope
  retains no decoded catalog. `CatalogCursor<T>` is the streaming form and
  decodes one record per `next()`.
- **`Transaction`** is the mutation handle a commit closure receives. Its
  metadata accessors match `ReadScope` and include its own staged changes.

Read-only mode is in the type. A `ReadOnlyCatalog` has no `commit`, so
committing through one fails to compile, and a `compile_fail` doctest pins
that. The runtime refusal exists only in the DuckDB shim, where a C ABI has
one handle type.

`CatalogSnapshot` is also exported: an immutable decoded view at one snapshot,
used by `ffi_support`. `CatalogSnapshot::reserve_copy` admits a caller-owned
copy against the process working-memory allowance.

## Opening

```rust
let catalog = Catalog::open(object_store, CatalogOptions::default()).await?;
let reader = Catalog::open_read_only(object_store, options).await?;
```

`Catalog::open(Arc<dyn ObjectStore>, CatalogOptions)` takes the writer and
bootstraps an empty store. `Catalog::open_read_only` takes the same arguments,
returns a `ReadOnlyCatalog`, never fences a writer, and refuses a store no
writer has initialized.

`CatalogOptions` is moraine's own type. `path` places the catalog under a prefix
within the bucket. `wal_store: Option<WalStore>` puts the write-ahead log on a
second object store, paired with the name the catalog records, and every later
open must name the same store. `checkpoint: Option<String>` pins a read-only
open to a checkpoint: that reader never sees a later commit. `open` refuses it.
A read-only open without it pins the writer's publications and writes nothing
either; `reader_poll_interval` bounds how stale it may be. Other fields tune
flushing, caches<!-- site:omit -->, and
[the commit-log seam](0011-commit-log-seam.md)<!-- /site:omit -->.

Checkpoint ids are `String`. A writer mints one with
`create_checkpoint(lifetime)`. `delete_checkpoint` and `checkpoints` are
associated functions that touch only the manifest and never fence a live
catalog.

## Reads

```rust
let current = catalog.read_scope().await?;
let past = catalog.read_scope_at(snapshot_id).await?;
let mut tables = current.scan_tables_in(schema_id).await?;
while let Some(table) = tables.next().await? {}
```

Both constructors pin a store revision and validate the snapshot against
retention. A scope that cannot pin fails and does not fall back to a following
reader. `current_snapshot()` is synchronous because scope acquisition read the
header. Every other accessor is async and fallible. `scan_*` cursors hold one
store page and one decoded record.

Scope level means `ReadScope` and `Transaction` unless marked. Catalog level
means `ReadOnlyCatalog`.

| Accessor | Returns | Level |
|---|---|---|
| `current_snapshot()` | `SnapshotInfo` of the pinned view | scope |
| `schemas`, `tables_in`, `views_in`, `macros_in`, and the `*_by_name` and `*_by_id` lookups for each | schemas, tables, views, macros | scope |
| `indexes_of`, `index_by_name`, `index_by_schema_name`, `index_on_columns`, `index_by_id` | index definitions | scope |
| `columns_of`, `partitioning_of`, `sorting_of`, `mappings_of`, `tags_of`, `option`, `data_path`, `table_stats`, `column_stats`, `data_files_of`, `delete_files_of` | per-table definitions, options, statistics, and live files with inlined chunks | scope |
| `scheduled_deletions()` | `Vec<ScheduledDeletion>` | `ReadScope` only |
| `scan_schemas`, `scan_tables_in`, `scan_views_in`, `scan_macros_in`, `scan_columns_of`, `scan_data_files_of`, `scan_delete_files_of`, `scan_indexes_of`, `scan_mappings_of` | `CatalogCursor<T>` | `ReadScope` only |
| `recent_rows`, `recent_rows_at`, `recent_row` | inlined rows as `RecentRow` | catalog |
| `index_lookup`, `index_lookup_many`, `index_range`, `index_nulls` | candidate row ids at head | catalog |
| `locate_row_ids`, `scan_rows_at`, `locate_row_positions_at`, `index_predicate_rows` | row positions and scans through a `DataStore` | catalog |
| `warm_tables`, `verify_index`, `store_census`, `maintenance_status` | cache warm-up, index audit, store weight, past passes | catalog |

[Data inlining](0004-data-inlining.md) defines the inlined rows, which a scope
does not serve. [Equality and range indexes](0009-equality-indexes.md) defines
the head-only index lookups.

## Commit model

```rust
let snapshot: SnapshotId = catalog.commit(async |tx| {
    let schema = tx.create_schema("sales").await?;
    tx.create_table(schema, "orders", &columns).await?;
    Ok(())
}).await?;
```

`commit` runs the closure against the head, loads each operation's
dependencies on demand, and publishes the accumulated delta as one atomic
store write. A later read inside the closure sees earlier staged mutations.
After a benign head race the catalog runs the closure again against the new
head, so the closure must be pure and idempotent: it must not consume captured
state or perform external side effects. Write Parquet before calling `commit`.
A closure error aborts the commit.

`commit_group(&[CommitMember])` commits several closures as one atomic group.
Members run in order, each sees the staged writes of the members before it,
and each receives its own `SnapshotId`. A race runs the whole group again. Any
member's error aborts the group.

Retry bounds, conflict rules, and group commit are defined by [the commit
protocol](0003-commit-protocol.md). The DuckDB extension commits
DuckLake-authored rows through that protocol's staged path and does not call
these closures.

## Errors

`Error` is `#[non_exhaustive]`. Only `CommitConflict` renders a message
containing `conflict`, `concurrent`, `unique`, or `primary key`, the
substrings DuckLake's commit loop retries on.

| Variant | Meaning |
|---|---|
| `CommitConflict` | Another writer committed a conflicting change. Retryable. |
| `RetryBudgetExhausted` | The internal retry budget was spent on benign races. Re-drive as smaller commits. |
| `Corruption` | Stored bytes failed to decode, or carry a newer encoding. |
| `NotFound` | The named entity does not exist or is not live. |
| `AlreadyExists` | The operation would violate name uniqueness. |
| `Constraint` | The operation would violate a structural constraint. |
| `Unsupported` | A DuckLake feature moraine does not implement. Terminal. |
| `SnapshotExpired` | A snapshot fell below the retention horizon. Re-resolve from head. |
| `CommitOutcomeUnknown` | A submitted commit was not acknowledged and may have landed. Reconcile before resubmitting. |
| `Interrupted` | A host interrupt cancelled the operation. |
| `Migration` | The store requires, is undergoing, or carries an unsupported structural format. Terminal. |
| `IndexBuilding` | The index's staged backfill has not completed. |
| `Configuration` | An environment or option value is unparsable or out of range. |
| `Fenced` | Another process opened the store read-write. This writer can no longer commit. |
| `OpenRaced` | Another process created the store during this open. Nothing was written. Reopen to adopt it. |
| `Store` | The underlying store failed. Carries the source error. |
| `ConcurrentModification` | Concurrent modification during index maintenance. |
| `RowPosition { row_id, data_file_id, reason }` | A row could not be positioned against the named file. Positioning never guesses. |

## Transaction operations

Every mutator is an async method on `Transaction` and reads as a DuckLake
catalog verb.

| Group | Operations |
|---|---|
| Schemas | `create_schema`, `drop_schema` |
| Tables | `create_table`, `rename_table`, `set_table_schema`, `drop_table` |
| Columns | `add_column`, `rename_column`, `alter_column`, `drop_column` |
| Views | `create_view`, `alter_view`, `drop_view` |
| Macros | `create_macro`, `drop_macro` |
| Partitioning | `set_partitioning`, `clear_partitioning` |
| Sorting | `set_sorting`, `clear_sorting`. A sort change does not advance the schema version. |
| Indexes | `create_index`, `create_index_ordered`, `create_index_ordered_with_maintenance`, `create_index_staged`, `create_index_staged_ordered`, `create_index_staged_ordered_with_maintenance`, `build_index_step`, `record_index_verification`, `drop_index` |
| Data files | `register_data_file`, carrying column statistics, partition values, and index entries. `expire_data_file` |
| Delete files | `register_delete_file`, carrying index removals. `expire_delete_file` |
| Statistics | `update_table_stats`, `update_column_stats` |
| Tags | `set_tag`, `remove_tag` on a schema, table, or view. Column tags travel on the column record. |
| Options | `set_option`, `unset_option`. An options-only commit mints no snapshot. |
| Inlined data | `inline_insert`, `inline_delete`, `flush_inlined_data` |

Column and name mappings, snapshot expiry, and file cleanup have no verb.
`register_data_file` leaves `mapping_id` unset, and the `expire_*` verbs write
no scheduled deletion.

## Catalog-level operations

These operations mint no snapshot through the closure path, or need the writer
itself, so they are `Catalog` methods or associated functions.

| Operation | Role |
|---|---|
| `commit`, `commit_group` | The closure commit path. |
| `set_option`, `adopt_data_path` | One-option commits that load no unrelated entities. |
| `create_index`, `drop_index`, `create_index_staged`, `create_index_staged_with_maintenance`, `repair_deferred_indexes`, `certify_index` | Index builds reading data files through a `DataStore`, over one or many commits. |
| `maintain(MaintenanceRequest)`, `record_maintenance_pass`, `reclaim_index_entries`, `reregister_stranded_inline_schemas` | A bounded maintenance pass that mints no snapshot, and its pieces for a host that sequences them itself. |
| `compact_store(CompactStoreRequest)` | Merges a subspace's sorted runs into one. Needs the writer. |
| `create_checkpoint(lifetime)`, and the associated `delete_checkpoint` and `checkpoints` | Mint, release, or list checkpoints. None fences a writer. |
| `raise_format(dry_run)`, `raise_catalog_version(dry_run)` | Stamp the store forward to the ceiling this build serves. |
| `close()` | Flushes background work and shuts the shared store handle. |
| `store_census_of` (associated) | The manifest leg of a census without opening the catalog. |
| `migrate(object_store, options, MigrationRequest)` (associated) | Rewrites the store to the newest structural format. Takes the writer epoch and fences a running catalog. |
| `move_wal_store(object_store, options, to)` (associated) | Moves the write-ahead log to another store. Takes the writer. |
| <!-- site:omit -->`demote(object_store, options)` (associated) | Takes a lake off its commit log.  <!-- /site:omit -->|

[Maintenance](0010-maintenance.md) defines `maintain`, `store_census`, and
`compact_store`. [Storage format](0001-storage-format.md) defines `migrate`.

## Domain types

Public values are hand-written structs, decoupled from the protobuf messages
`store` persists, so a field change on disk is not a public break. Ids are
`Copy` newtypes over the catalog's `u64` id space with `new` and `get`:
`SnapshotId`, `SchemaId`, `TableId`, `ColumnId`, `DataFileId`, `DeleteFileId`,
`ViewId`, `MacroId`, `MappingId`, `IndexId`, `PartitionId`, `SortId`. Instants
cross the API as `Timestamp`, a signed microsecond count from the Unix epoch.
`DataStore` wraps the `DATA_PATH` object store with the `CacheIdentity` the
footer and row-summary caches key on. `WalStore` pairs a log store with its
recorded name. `MemoryReservation` holds a working-memory admission, and
`configure_memory(MemoryBudgets)` sets the process's budgets once, ahead of
their environment variables; `store_memory_budget` reads the store's.

## Versioning

Pre-1.0 semver: a breaking change bumps the minor version. Cargo features are
additive-only. The `fault-injection` and `fuzzing` exports and `ffi_support`
are `#[doc(hidden)]` and outside the semver contract.

## Alternatives

- **A store abstraction in the signature.** `Catalog::open` taking a SlateDB
  handle or a KV trait object would expose the substrate and tie moraine's
  semver to SlateDB's. Rejected: `object_store` is the deployment primitive.

# RFC 0005: DuckLake feature mapping

## Summary

DuckLake supplies catalog rows for schema changes, partitioning, sorting,
column mappings, macros, and encryption. Moraine validates their structure,
stores them, and serves them back to DuckLake. DuckLake evaluates expressions
and reconstructs the change data feed from those records.

## Shared rules

1. A child row with no lifecycle of its own is embedded in its parent's value.
   Partition columns, sort expressions, per-file partition values, macro
   implementations and parameters, and name-mapping rows are all embedded. A
   child row staged without its parent in the same commit is refused.
2. Rows are stored and served verbatim. Types, transforms, sort expressions,
   macro bodies, encryption keys, and mapping types are strings moraine copies
   and returns. The write path checks structure, never meaning.
3. moraine evaluates no predicate, transform, expression, or feed. Pruning,
   sorting, macro expansion, and change-feed reconstruction run in DuckLake
   over the rows moraine serves.
4. Every mutation of a versioned entity is a version transition: the live
   record ends into `history` with an `end_snapshot`, and the new record lands
   at the same `current` key. No versioned record is edited in place. Time
   travel is the ordinary `current` plus `history` filter described in [the
   storage format](0001-storage-format.md).

Which commits advance `schema_version` is decided by the typed operation log,
as described in [the commit protocol](0003-commit-protocol.md). The staged
path stores the `schema_version` DuckLake authored and performs no
classification of its own.

## Schema evolution

| Aspect | Design |
|---|---|
| Kind and key | `column`, keyed `(table_id, column_id)` |
| Value | `ColumnValue`: `column_order`, `column_name`, `column_type`, `nulls_allowed`, defaults, `parent_column`, tags |
| Versioned | Yes |
| Bumps `schema_version` | Every column operation |
| Verbs | `create_table`, `add_column`, `rename_column`, `alter_column`, `drop_column` |

`column_id` is DuckLake's per-table field id, allocated from 1 and never
reused. A dropped id never returns. Data files reference columns by field id,
so a drop, rename, or type change touches no data file. `column_order` is a
position numbered from 1 that is never renumbered, so a drop leaves a gap. No
verb reorders columns.

Each operation is one version transition per column touched:

| Operation | Key effect | Value effect |
|---|---|---|
| `add_column` | New `current` key at a fresh `column_id` | Full record |
| `drop_column` | Live version ends into `history` | Unchanged |
| `rename_column` | Same key, new version | New `column_name` |
| `alter_column` | Same key, new version | New `column_type`, `nulls_allowed`, or `default_value` per `ColumnAlteration` |

Untouched siblings produce no write. Type-change legality belongs to DuckLake.
moraine stores the new type string and does not check that it widens the old
one.

### Field id allocation

The table record carries `next_column_id`, a moraine field with no DuckLake
column. `create_table` seeds it to the number of column nodes plus one.
`add_column` allocates from `max(next_column_id, max live column_id + 1)` and
writes a new table version carrying the advanced counter, so the add is also a
table version transition. The counter is persisted so allocation never scans
`history`, and so snapshot expiry cannot regress the high-water mark. A table
version whose counter is absent or stale allocates past the highest live id.
On the staged path a new table version inherits the counter from the prior
version and advances it past any column id the commit assigned.

### Nested fields

Every struct member, list element, and map key or value is its own `column`
row with `parent_column` set. Ids and positions are assigned in pre-order from
the same two counters a top-level column draws from. The verb path takes the
nested type as a tree: `ColumnDef` carries DuckLake's marker in `column_type`
and the fields in `children`. Three rules follow:

- A field's name is unique among its siblings. Two structs may each hold an
  `x`. `add_column` and `rename_column` check this through the parent's scope.
- Dropping a column drops every field beneath it in the same commit.
- The refusal to drop a table's last column counts top-level columns only.
  Dropping the last field of a struct leaves the struct.

### Column validation

`validate_column_defs` refuses an empty name and a duplicate name among
siblings. `drop_column` refuses the last top-level column and any column in
the subtree that an index covers. `alter_column` refuses a type change on an
indexed column, because the index encoding is type-bound. Those are
`Error::Constraint`. A name collision is `Error::AlreadyExists`.

### Schema versions

`ducklake_schema_versions` has its own kind, `schema_version`, keyed
`(table_id, begin_snapshot)` and holding the `schema_version` the snapshot
minted. The commit writes one record per table or view in the snapshot's
`schema_changed_table_ids`, which `CreateTable`, `AlterTable`, `CreateView`,
and `AlterView` populate. The records are retained across snapshot expiry and
removed only with the table.

Two schema changes to the same table conflict at table grain. Changes to
different tables are disjoint.

## Catalog version

The DuckLake catalog version names the shape of the `ducklake_*` tables
themselves and is independent of both `schema_version` and the store format.
It is the global `version` option. A store with no row reads as `1.0`, the
version every new store records. This build serves `1.0`, `1.1`, and the
superseded development label `1.1-dev1`, which is served as the `1.1` shape.
`serves_catalog_version` names that set.

`version` and `encrypted` are reserved from `set_option` and `unset_option`
with `Error::Constraint`. Only a raise moves `version`.

`Catalog::raise_catalog_version(dry_run)` moves the recorded version to `1.1`
and returns `CatalogVersionRaise { from, to }`. A dry run, or a store already
at `1.1`, reports the move and records nothing. A recorded version this build
cannot serve was written by a newer build. The raise returns
`Error::Migration` and never lowers it. The SQL surface is
`moraine_raise_catalog_version(lake, dry_run => BOOLEAN, confirm => BOOLEAN)`.
It takes an attached catalog name, reads the move with a dry run first, and
refuses a raise that would record unless `confirm => true` is passed. The
raise is one way: a DuckLake that requires `1.0` refuses a raised store at its
own version check.

The shape is chosen once per attach, when the shim registers its catalog
entries, by reading the recorded version. A raise is visible to sessions that
attach after it. The `1.1` shape adds `ducklake_view_column_tag`,
`row_group_count` on `ducklake_data_file` and `ducklake_delete_file`,
`min_is_exact` and `max_is_exact` on the column-stats tables, and
`parent_schema_id` on `ducklake_schema`, which is always NULL.

## Partitioning and sorting

| Aspect | Partitioning | Sorting |
|---|---|---|
| Kind and key | `partition`, `(table_id, partition_id)` | `sort`, `(table_id, sort_id)` |
| Value | `PartitionValue` with embedded `PartitionColumn { partition_key_index, column_id, transform }` | `SortValue` with embedded `SortExpression { sort_key_index, expression, dialect, sort_direction, null_order }` |
| Versioned | Yes | Yes |
| Bumps `schema_version` | Yes (`AlterTable`) | No (`AlterTableSorting`) |
| Verbs | `set_partitioning`, `clear_partitioning` | `set_sorting`, `clear_sorting` |
| Per-file record | `FilePartitionValue { partition_key_index, partition_value }` embedded in `DataFileValue.partition_values`, with `partition_id` naming the spec | None |

Spec ids come from the global catalog id counter. A table has at most one live
spec of each kind. Setting a spec ends the live one and inserts the new one in
the same batch. A data file records the `partition_id` it was written under,
so files written under different specs coexist and each file's values are read
in the same per-table file scan.

A partition column references its source by `column_id`, so a rename leaves
the spec intact. A bare partition column carries the transform `identity`, and
the transform string is never empty. A sort key names its column inside a
verbatim SQL string. DuckLake handles a rename of a sorted column by
committing a new sort version with the new text, and the old text stays in
`history`. Dropping a partitioned or sorted column is refused by DuckLake
before it reaches the catalog. moraine does not check whether a dropped column
is referenced by a live spec.

`RESET PARTITIONED BY` arrives on the staged path as an end of the old spec
plus an insert of a spec with zero columns, and moraine stores that shape.
`RESET SORTED BY` ends the old spec and inserts nothing. The verb-path
`clear_partitioning` and `clear_sorting` both end the live spec and insert
nothing.

### Spec validation

`set_partitioning` refuses an empty column list, a column that is not live,
and a column named twice. `set_sorting` refuses an empty key list.
`clear_partitioning` and `clear_sorting` return `Error::NotFound` when no live
spec exists. The staged fold refuses a `partition_column` or `sort_expression`
row whose parent insert is absent from the commit.

moraine never prunes files by partition value and never sorts rows. Correct
pruning would need DuckLake's transform semantics and typed comparison, and a
lexicographic compare over stored strings would drop rows.

## Column and name mapping

| Aspect | Design |
|---|---|
| Kind and key | `mapping`, keyed `(table_id, mapping_id)` |
| Value | `MappingValue { mapping_id, table_id, map_type }` with embedded `NameMapping { column_id, source_name, target_field_id, parent_column, is_partition }` |
| Versioned | No. Create-only, never mirrored to `history` |
| Bumps `schema_version` | No. A mapping rides the data-file commit that carries it |
| Verbs | None |

`mapping_id` is allocated by DuckLake from the file-id counter, the same
counter as `data_file_id`. The only writer is `ducklake_add_data_files`, so
mappings reach the store through the staged path alone. `register_data_file`
on the verb path writes `mapping_id = None`. Mappings are read through
`mappings_of`, which returns `MappingInfo` records.

Because the kind is unversioned, every snapshot materialization includes all
mappings regardless of the time-travel target. A historical file read finds
its mapping in `current`.

### Mapping validation

The staged fold groups `ducklake_name_mapping` inserts by `mapping_id` and
each `ducklake_column_mapping` insert consumes its group. Five rules apply:

1. A `column_mapping` insert with no `name_mapping` rows is refused.
2. `name_mapping` rows left unconsumed after the commit are refused.
3. `column_id` must be unique within a mapping.
4. `parent_column` must reference a smaller `column_id`, which is what makes
   DuckLake's one-pass tree rebuild sound.
5. A `(table_id, mapping_id)` already present in the base snapshot is refused.

`column_id` contiguity and the `map_type` literal are not checked. The table
specs carry no end-snapshot column, so an UPDATE against either table has no
target and is refused. A `ducklake_column_mapping` DELETE is a direct removal
of the `current` key, the one unversioned kind on the hard-delete path. A
`ducklake_name_mapping` DELETE is accepted and does nothing, because the rows
left with their parent.

## Macros

| Aspect | Design |
|---|---|
| Kind and key | `macro`, keyed `macro_id` |
| Value | `MacroValue { macro_id, begin_snapshot, end_snapshot, schema_id, macro_name }` with embedded `MacroImplementation { impl_id, dialect, sql, macro_type }`, each with embedded `MacroParameter { column_id, parameter_name, parameter_type, default_value, default_value_type }` |
| Versioned | Yes, like `view` |
| Bumps `schema_version` | Yes (`CreateMacro`, `DropMacro`) |
| Verbs | `create_macro`, `drop_macro` |

`macro_id` comes from the global catalog id counter. `impl_id` is the 0-based
position of the overload and `column_id` the 0-based position of the
parameter. Implementations are stored in `impl_id` order and parameters in
`column_id` order, and the projections serve them in that order. DuckLake's
reader consumes both positionally with no `ORDER BY`. `macro_type` is stored
per implementation because that is where the table carries it.

Ending a macro mirrors the whole record, children included, into `history`, so
the implementation and parameter projections keep serving rows for every
snapshot that can see the macro. `DROP MACRO` is the generic end-snapshot
update. `CREATE OR REPLACE` is a drop plus a create under a new `macro_id`.
There is no alter.

`create_macro(schema, name, implementations)` returns a `MacroId`. The name
must be free among the schema's live macros of the same `macro_type`. Macros
do not share the table and view namespace. `drop_macro(macro_id)` ends the
live version. Neither contributes a `ducklake_schema_versions` row.

`ChangeSet` reads and writes the four `changes_made` tokens:
`created_scalar_macro`, `created_table_macro`, `dropped_scalar_macro`, and
`dropped_table_macro`. Both operations classify as schema-changing.

### Macro validation

The verb refuses zero implementations and a mixed `macro_type` with
`Error::Constraint`, and a taken name with `Error::AlreadyExists`. The staged
fold refuses a macro insert with zero `macro_impl` rows, implementation or
parameter rows left unconsumed, `impl_id` or `column_id` values that are not
contiguous from zero, and a mixed `macro_type` across one macro's
implementations.

## Encryption

The global `encrypted` option is recorded at bootstrap from the `ENCRYPTED`
attach option, which reaches a nested moraine attach through DuckLake's
`META_ENCRYPTED true` passthrough. Later attaches ignore the option, and
`set_option` refuses the key. Moraine serves the flag back to DuckLake, which
encrypts the files. The data-file keys DuckLake writes to `encryption_key` on
`ducklake_data_file` and `ducklake_delete_file` are stored and served verbatim.
moraine decrypts nothing and reads no Parquet byte of an encrypted file.

The catalog has no client-side encryption. Confidentiality of the SlateDB
objects relies on the bucket's server-side encryption, so an untrusted catalog
bucket is unsupported. Inlined rows live inside catalog values and are covered
only by that catalog-at-rest protection. See the
[operating manual](https://morainedb.com/guide/operating/#encryption) for
key policy and bucket configuration.

## Change data feed

`ducklake_table_changes`, `ducklake_table_insertions`, and
`ducklake_table_deletions` need no moraine support. DuckLake resolves the
bounds against `ducklake_snapshot` and reconstructs each snapshot's inserted
and deleted rows from `begin_snapshot`, `end_snapshot`, and `partial_max` on
the file tables, from per-row begin and end snapshots on inlined data, and
from `row_id_start` and the inlined `row_id` column. moraine serves all of
those verbatim and allocates none of them. `changes_made` is not consulted by
the feed. The e2e suite pins the feed over inlined data, across a flush, and
against a stock DuckLake catalog in
`crates/moraine-duckdb/tests/ducklake_load/change_feed.rs`.

## Tests

- Reconstructing the column set, names, order, and types at any past snapshot
  matches DuckLake for an arbitrary verb sequence.
- No `column_id` is allocated twice across any sequence of drops and adds, and
  nested ids follow pre-order.
- A catalog-version dry run records nothing, a raise is idempotent, and a
  version this build cannot serve is refused.
- Partition and sort specs, per-file partition values, mappings, and macros
  round-trip through their codecs byte for byte.
- Each staged fold rule for mappings and macros refuses the malformed batch it
  names.
- Setting, changing, and clearing partitioning and sorting time-travels, and
  every file reports the spec it was written under.

## Alternatives

- **In-place value edits for rename and type change.** Rejected. An overwrite
  destroys the record live at earlier snapshots, so time travel to a
  pre-rename snapshot would return the new name and the `ducklake_column` scan
  would return rows DuckLake's catalog never held.
- **Separate kinds for child rows.** Rejected for partition columns, sort
  expressions, file partition values, macro children, and name mappings. The
  rows have no lifecycle apart from their parent, and a separate kind splits
  one spec load into a join the reader immediately re-flattens.

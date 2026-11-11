# RFC 0001: Storage format

## Summary

Moraine stores DuckLake catalog state in SlateDB. Keys start with a
subspace byte and use `storekey` encoding. Values are protobuf messages
with a five-byte framing header. Each catalog commit is one `WriteBatch`.

`sys/format` records the store format. Each binary supports a range of
formats; older stores require an explicit, resumable migration.

## Subspaces

The first byte of every key names its subspace. Every store is created with a
fixed-length one-byte segment extractor, `TagSegmentExtractor` in
`crates/moraine/src/store/segment.rs`, so each subspace is a SlateDB segment
with its own LSM state. SlateDB persists the extractor identity in the
manifest and refuses an open with a different one. A batch spanning several
subspaces stays atomic. Below the leading byte the layout is moraine
convention and opaque to SlateDB.

| Subspace | Contents | Mutability |
|---|---|---|
| `system` | Format stamp, head pointer, migration marker, maintenance status | Overwritten in place |
| `snapshot` | One record per snapshot | Append-only |
| `current` | Live catalog entities | Insert and delete |
| `history` | Ended entity versions | Append-only |
| `inline` | Inlined row data, see [data inlining](0004-data-inlining.md) | Per that document |
| `index` | Equality-index entries, see [equality indexes](0009-equality-indexes.md) | Live-only, insert and delete |
| `schema_version` | `ducklake_schema_versions` rows | Insert and delete, survives snapshot expiry |
| `changelog` | The `current` keys each recent commit wrote | Sliding window of 64 commits |
| `name` | Exact names of current schemas and tables | Atomic with their entities |
| `delete_target` | Current delete files by target data file | Atomic with their delete records |

The `Key` enum in `crates/moraine/src/store/key.rs` declares the subspaces in
this order, and `SubspaceName` reports them in the census and compaction
APIs.

### Current and history

DuckLake versions catalog rows with `begin_snapshot` and `end_snapshot`.
Moraine splits them physically. A live version lives in `current` under its
entity key. Ending a version deletes the `current` key and puts a `history`
key, which appends `end_snapshot` as the final component, in the same batch.
Loading the live catalog scans `current` alone. Reading the catalog at
snapshot `S` scans both. From `current` it keeps `begin_snapshot <= S`. From
`history` it keeps `begin_snapshot <= S < end_snapshot`. Snapshot expiry in
[maintenance](0010-maintenance.md) removes `history` records.

## Key encoding

A key is a typed tree of nested Rust enums: subspace, kind, components. The
on-disk bytes are the derived order-preserving encoding from the `storekey`
crate: one discriminant byte per enum level, assigned by declaration order,
then the components in field order. Integer components are fixed-width
big-endian `u64`.

- Variant order is permanent. A new variant appends. Golden-vector tests pin
  the encoded bytes of every kind, so a reordered variant, a changed derive,
  or a `storekey` upgrade that alters the encoding fails CI.
- Scan bounds are derived by truncation. The code encodes a sample key of
  the target shape and cuts it after the components the scan fixes. A
  shorter path through the tree is a byte prefix of every key beneath it.
  Table-scoped bounds go through a typed constructor that accepts only kinds
  whose first component is a table id.
- Catalog entities are keyed by DuckLake-allocated ids. Schemas, tables,
  views, macros, and index definitions use the global catalog-id counter.
  Files and column mappings use the file-id counter. Columns use a per-table
  field id. Entity names live in values.
- Two subspaces carry non-integer components. `name` keys hold the entity's
  exact UTF-8 name as a `storekey` string with its terminator. `index` keys
  hold the canonical encoding of the indexed value as a byte string. Every
  other key is discriminants and `u64` components only.
- SlateDB iterates forward only. `sys/head` is an explicit pointer. No read
  finds the head by scanning for the maximum key, and every range read
  ascends. A new kind must keep this.

## Keyspace map

### Kinds within `current`

Temporally versioned kinds mirror into `history` with `end_snapshot`
appended. Unversioned kinds are overwritten in place and never enter
`history`. `EntityKey::is_versioned` encodes the distinction.

| Kind | Key components | DuckLake table | Versioned | Embedded children |
|---|---|---|---|---|
| `Schema` | `schema_id` | `ducklake_schema` | yes | |
| `Table` | `table_id` | `ducklake_table` | yes | |
| `View` | `view_id` | `ducklake_view` | yes | |
| `Macro` | `macro_id` | `ducklake_macro` | yes | `ducklake_macro_impl`, `ducklake_macro_parameters` |
| `Column` | `table_id, column_id` | `ducklake_column`, one record per row including nested fields | yes | `ducklake_column_tag` |
| `Partition` | `table_id, partition_id` | `ducklake_partition_info` | yes | `ducklake_partition_column` |
| `Sort` | `table_id, sort_id` | `ducklake_sort_info` | yes | `ducklake_sort_expression` |
| `File` | `table_id, data_file_id` | `ducklake_data_file` | yes | |
| `DeleteFile` | `table_id, delete_file_id` | `ducklake_delete_file` | yes | |
| `Index` | `table_id, index_id` | none, a moraine index definition | yes | |
| `Mapping` | `table_id, mapping_id` | `ducklake_column_mapping` | no, written once and never overwritten | `ducklake_name_mapping` |
| `FileColumnStats` | `table_id, data_file_id, column_id` | `ducklake_file_column_stats` | no | variant stats |
| `TableStats` | `table_id` | `ducklake_table_stats` | no | |
| `TableColumnStats` | `table_id, column_id` | `ducklake_table_column_stats` | no | |
| `Tag` | `object_id` | `ducklake_tag`, one container per tagged object | no, each entry carries its own begin and end | all of the object's tag rows |
| `Option` | `scope_kind, scope_id` | `ducklake_metadata` and `set_option` scopes | no | the scope's options as a map |

`scope_kind` is 0 for global, 1 for schema, and 2 for table, and the global
scope uses `scope_id` 0. Object ids are unique across entity types, so `Tag`
needs no type discriminator. Time travel serves the current statistics and
filters `Tag` entries by their begin and end. A `FileColumnStats` record
outlives its file's live version and is removed with the file's history.

A 1:1 side table merges into its
parent record. A child table with no independent lifecycle embeds in the
parent's value. A table gets its own kind when its rows have their own
begin and end. Per-table kinds are keyed `table_id` first, so everything
about one table is one contiguous range per subspace. `FileColumnStats` is
file-major so one file's columns sit together.

`current` also holds `GcFile { data_file_id }`, the
`ducklake_files_scheduled_for_deletion` row keyed by the scheduled file's id.
It has no lifecycle and no `history` mirror.

### Other subspaces

| Key | Components | Contents |
|---|---|---|
| `sys/format` | none | `format_version`, the writer's moraine version<!-- site:omit -->, and the commit-log batch origin when the store is a commit-log catalog<!-- /site:omit --> |
| `sys/head` | none | Latest committed `snapshot_id` and `batch_seq`, the count of batches landed. Every batch rewrites it, so it is the write-write conflict anchor and the stamp a reader validates a cut against |
| `sys/migration` | none | `{from_format, to_format, cursor}`, present only mid-migration |
| `sys/maintenance_status` | none | The last 16 completed maintenance passes |
| `snapshot` | `snapshot_id` | `ducklake_snapshot` and `ducklake_snapshot_changes` merged, plus the conflict facts of [the commit protocol](0003-commit-protocol.md) |
| `changelog` | `snapshot_id` | The encoded `current` keys one commit wrote. Each commit deletes the record 64 snapshots back |
| `schema_version` | `table_id, begin_snapshot` | The catalog schema version that snapshot minted. Outlives the snapshot record and is removed with the table |
| `inline/*` | `table_id, schema_version, ...` | Owned by [data inlining](0004-data-inlining.md) |
| `index` | `Unique { index_id, key }` or `Multi { index_id, key, row_id }` | Equality-index entries. A unique entry's value is the holding row id. A multi entry's value is empty |
| `name` | `kind, parent, name, entity_id` | Empty value. `kind` 0 is a schema with `parent` 0. `kind` 1 is a table with `parent` its schema id |
| `delete_target` | `table_id, data_file_id, delete_file_id` | Empty value. A `(table_id, data_file_id)` prefix lists the current delete files targeting one data file |

The `name` and `delete_target` subspaces are directories over `current`. The
commit that changes a current schema, table, or delete file deletes the old
directory entry and inserts the new one in the same batch. An ended entity
has no entry. A lookup resolves the entry, then point-reads and validates the
current record. An exact-name lookup chooses the greatest matching
`entity_id`.

## Value codec

Values are protobuf messages, one message type per kind, compiled at build
time with `protox` and `prost-build`. Each value begins with a five-byte
framing header: the magic `MRNE`, then one encoding-version byte, which this
binary writes as 0.

`unframe` in `crates/moraine/src/store/frame.rs` rejects a truncated header,
a wrong magic, or an encoding version above the one it writes, each as
`Error::Corruption`. A payload that fails protobuf decoding is also
`Error::Corruption`. No partial decode is returned.

Protobuf field evolution is forward and backward compatible: a reader skips
unknown fields and defaults missing ones. Entity values carry
`begin_snapshot`, and `history` values also carry `end_snapshot`, so a value
is self-contained. Timestamps are microseconds since the Unix epoch, UTC.

Statistics are stored verbatim. DuckLake encodes min and max as strings
regardless of column type, and moraine round-trips those strings without
interpretation. Re-serializing through a typed value would corrupt pruning.

## Atomicity

One DuckLake catalog commit is one SlateDB `WriteBatch`: the snapshot record,
the `sys/head` update, every entity insert and end, every directory and
index entry, and the changelog record. No commit spans batches or depends on
a read-modify-write across them. Every mutation a commit needs is a put or a
delete at a statically computable key. [The commit
protocol](0003-commit-protocol.md) builds on this.

## Format versions and migration

### Two kinds of change

A value change adds or deprecates protobuf fields, or raises the framing
encoding byte. Readers absorb it on decode. It needs no migration and does
not touch `sys/format`.

A structural change alters the subspace set or the key structure. It bumps
`sys/format`. Structural changes come in two shapes:

- **Additive.** A new subspace or kind, moving no existing key. A binary that
  knows the tag reads older stores unchanged. A binary that does not refuses
  the newer stamp.
- **Rewriting or backfilling.** Existing keys move, or a directory is built
  over existing records. These run through the migration protocol below. A
  rewrite raises the floor of the formats a binary reads, because the old
  keys are gone. A backfill keeps the floor when a scan fallback remains for
  older formats.

A binary reads a range of formats, `MIN_FORMAT_VERSION..=MAX_FORMAT_VERSION`
in `crates/moraine/src/transaction/commit.rs`. A fresh store is stamped at
`MAX_FORMAT_VERSION`.

### Format versions

| Format | Added |
|---|---|
| 1 | The original layout |
| 2 | The `index` subspace and the `Index` entity kind |
| 3 | Staged index definitions, built before serving |
| 4 | Deferred index additions |
| 5 | `sys/maintenance_status` |
| 6 | The inline chunk row-range directory |
| 7 | Inline schema references between schema versions |
| 8 | Inline chunk identity locators |
| 9 | Inline tombstones keyed per deletion snapshot, a rewrite. The floor |
| <!-- site:omit -->10 | Commit-log catalogs, whose commits are slot objects in a bucket-resident log  <!-- /site:omit -->|
| 11 | The `name` directory |
| 12 | The `delete_target` directory. The newest |

Formats 1 through 8 are below the floor.<!-- site:omit --> Format 10 marks a
commit-log catalog. From 11, a nonempty `commit_log_batch_origin` in
`sys/format` marks one, and migration, adoption, and demotion preserve that
origin. Opening a commit-log store requires the commit-log library linked and
registered, see
[the commit-log seam](0011-commit-log-seam.md).<!-- /site:omit -->

### On-open check

Every open reads `sys/migration` and `sys/format` before anything else.

| Store | Action |
|---|---|
| Marker present | Refuse with `Error::Migration`. The message names `Catalog::migrate` |
| Format above `MAX_FORMAT_VERSION` | Refuse with `Error::Migration`. Nothing is written |
| <!-- site:omit -->Commit-log format without the library | Refuse with `Error::Migration`  <!-- /site:omit -->|
| Format below `MIN_FORMAT_VERSION` | Refuse with `Error::Migration`. The message names `Catalog::migrate` |
| Format in range | Open |
| Absent | Uninitialized. A read-write open bootstraps the store. A read-only open refuses with `Error::Corruption` |

Within the range, a store below 11 or 12 reads names or delete targets by
scanning `current` and writes no directory entries until migrated, because a
partially built directory cannot answer absence.

The DuckDB shim maps the refusal to one naming `moraine_migrate('<path>')`.
The same binary that refuses the open performs the migration.

### Migration marker

`sys/migration` is reserved from format 1. It exists only between a
migration's start and finish batches. Every read session checks it under its
pinned read snapshot and refuses with `Error::Migration` while it is present.
Readers refuse a mid-migration store because the step loop deletes old keys
as it goes, and a reader on the old layout would see a shrinking catalog.

The start batch is the only creator of the marker, and it writes the marker
together with a `sys/head` re-put whose `batch_seq` advances. A handle caches
the marker's absence keyed on the head it observed, so a marker written
without moving the head would be invisible to it.

### Start, step, finish

A migration runs under the single fenced writer. `Catalog::migrate` opens the
writer itself, taking the same epoch as `Catalog::open`, so it fences a running
catalog and is fenced by one.<!-- site:omit --> A commit-log catalog submits
every batch through a commit session to an authoritative log
slot.<!-- /site:omit -->

1. **Start**, one batch. Put `sys/migration` with
   `{from_format, to_format, cursor: empty}` and the advanced `sys/head`. A
   start that loses a conflict against a commit retries under the commit
   protocol's bounded backoff.
2. **Step**, many batches. Each step reads a bounded slice of work in key
   order, writes the new keys, deletes the old keys they supersede, and
   advances the cursor, all in one batch. The cursor never claims more
   progress than is durable. Re-running a step whose new keys exist and old
   keys are gone is a no-op.
3. **Finish**, one batch. Put `sys/format` with `to_format` and delete
   `sys/migration`.

After a crash, reopen reads both records and lands in one of three states:

| Reopen sees | Action |
|---|---|
| No marker, old format | Nothing started. The store is coherently old |
| Marker, old format | Resume from the cursor |
| No marker, new format | Finished. The store is coherently new |

New format with a marker present cannot occur, because the flip and the
clear share a batch. A marker whose `from_format` disagrees with
`sys/format` is refused as `Error::Corruption`. A marker naming a unit this
binary does not ship is refused as `Error::Migration`.

After a native migration completes, the writer flushes and publishes a
checkpoint before closing, so fixed read-only scopes acquire the migrated
version without a writer reopen.

### Units and composition

A migration unit is a named `v_n -> v_{n+1}` rewrite with its own step
function and tests. `MIGRATIONS` in
`crates/moraine/src/transaction/migration.rs` lists them in ascending order:

| Unit | From | To | Work per step |
|---|---|---|---|
| `version-inline-tombstones` | 8 | 9 | 256 tombstones, each re-keyed as `(table_id, row_id, end_snapshot)` with its framed value preserved. Cursor: the last source key |
| `index-current-names` | 9 or 10 | 11 | 256 schema or table records, or about 1 MiB of name keys. Cursor: the last source key |
| `index-delete-file-targets` | 11 | 12 | 256 delete-file records, or about 1 MiB of staged keys. Cursor: the last delete-file key |

A multi-version jump is the chain of units starting at the store's format,
each with its own start, step, and finish. A store at format 1 through 7
without a marker is first stamped to 8, the first unit's source, since no key
moves between those formats. An interrupted unit resumes at its recorded
format and cursor, and the chain continues from its target.
`MigrationReport` records `from_format`, `to_format`, the `units_run`, and
whether the call `resumed`. Against a store at the newest format it reports
the same format twice and no units.

Migrations are irreversible. A step deletes old keys only in the batch that
writes their replacements, so an interrupted migration retains the data needed
to resume. `MigrationRequest { checkpoint: true }` takes a whole-store
checkpoint before the first rewrite and releases it after the last finish batch
is durable. A failed migration keeps the checkpoint.

### Triggering

Nothing migrates on open. The operator runs
`Catalog::migrate(object_store, options, request)`, which takes the object
store because the store it acts on is one an ordinary open refuses. The
DuckDB surface is the unattached table function `moraine_migrate('<path>')`,
which refuses to run inside an explicit transaction.

`Catalog::raise_format` and the attached table function
`moraine_raise_format('<lake>', dry_run := ...)` stamp an attached store up
to its additive ceiling: the newest format reachable without crossing a
unit's source format. Ordinary commits never raise the stamp. With the floor
at 9 and units sourced at 9, 10, and 11, every store that opens is already
at its ceiling, and the call reports without writing.

Structural migration is not online across binary versions. Once the store
flips, an older binary meets a newer `sys/format` and refuses. A rolling
deployment drains readers across the flip or accepts the unavailability.

## Tests

- Every key kind roundtrips through encode and decode, and golden vectors pin
  the bytes per kind.
- Encoded byte order equals component order for arbitrary key pairs.
- Every value message type roundtrips through framing and protobuf.
- A truncated header, wrong magic, newer encoding byte, or garbage payload fails
  as `Error::Corruption`.
- Decoding arbitrary bytes as a key or a value never panics.
- A migration interrupted after start, after any step, and before and after
  finish resumes to a coherent store, and a reader with the marker present gets
  `Error::Migration`.

## Alternatives

- **One subspace with begin and end in values.** Every live read filters the
  full history, and the live load degrades without bound between expiries.
  The `current` and `history` split costs one extra delete and put per ended
  version.
- **Version in the key, `(id, begin_snapshot)`, with no split.** Live reads
  become reverse scans, which SlateDB does not offer, and the live load
  still filters ended versions.

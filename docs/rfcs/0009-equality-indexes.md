# RFC 0009: Equality and range indexes

## Summary

Moraine adds a native, ordered index over DuckLake table columns. The definition is a catalog entity. Entries live in the `index` subspace, point at stable row ids, and land in the same commit as the rows they cover. Unique entries are keyed on the value itself, so racing inserts of one value collide in the store. Builds too large for one commit run staged across bounded steps. The planner narrows DuckLake scans through lookups pinned to one read revision.

## The index definition

An index is an `index` kind in the `current` and `history` keyspaces, keyed `(table_id, index_id)`. The `index_id` comes from the global catalog-id counter. The value carries the name, the indexed columns referenced by field id in declared order, the unique flag, per-column direction and NULL placement, the maintenance mode, the build state and cursors, the poison flag, and the certificate snapshot. Columns are referenced by field id, so `rename_column` leaves an index untouched. The definition is temporally versioned like every entity. The entries are live-only.

`ReadScope` and `CatalogSnapshot` expose `indexes_of(table)`, `index_by_name(table, name)`, `index_by_schema_name(schema, name)`, `index_on_columns(table, columns)`, and `index_by_id(table, id)`. The domain types are `IndexId`, `IndexDef`, `IndexInfo`, `IndexState`, `IndexMaintenance`, `ColumnOrder`, `IndexEntry`, `FileIndexRemoval`, and `BuildStep`. A new index name must be unique within its schema, compared case-insensitively, and the check runs inside the creating commit.

The `Transaction` verbs:

| Verb | Effect |
|---|---|
| `create_index(table, def, backfill)` | Inserts a ready definition and stages the writer-supplied backfill entries in the same commit. Ascending, NULLS LAST. |
| `create_index_ordered(table, def, orders, backfill)` | As `create_index`, with one `ColumnOrder` per column. |
| `create_index_ordered_with_maintenance(table, def, orders, maintenance, backfill)` | As `create_index_ordered`, selecting synchronous or deferred upkeep. Deferred is refused for a unique index. |
| `create_index_staged(table, def)` | Inserts the definition in `building` state with no entries. |
| `create_index_staged_ordered(table, def, orders)` | The staged form with per-column orders. |
| `create_index_staged_ordered_with_maintenance(table, def, orders, maintenance)` | The staged form with orders and upkeep mode. |
| `build_index_step(index, batch, is_final)` | Stages one batch of writer-supplied entries, advances the cursor, and flips the index ready when `is_final`. |
| `record_index_verification(index, observed, at)` | Records or withdraws the certificate. Refused if the index is not ready or its definition moved since `observed`. |
| `drop_index(index)` | Ends the definition into `history`. Entries are orphaned for later reclamation. |

Index DDL is recorded in `changes_made` as `altered_table:<table_id>` and sets `schema_changed`. An alter conflicts with concurrent inserts, deletes, and other alters, which is what index DDL requires: a `create_index` racing a `register_data_file` aborts with `CommitConflict` and the caller re-drives its backfill against the new file set. The [commit protocol](0003-commit-protocol.md) defines the conflict matrix.

`drop_column` and `alter_column` with a type change on a column a live index references fail with `Constraint`. The canonical encoding is type-bound, so the index must be dropped first. `drop_table` ends the table's indexes with it.

`create_index` refuses columns whose Parquet form and inline Arrow form cannot derive the same bytes. `HUGEINT`, `UHUGEINT`, `INT128`, and `UINT128` are refused because DuckDB writes them to Parquet as a lossy `double`. `VARIANT` is refused because it has no single scalar to encode. The check is `ensure_indexable` in `crates/moraine/src/catalog/index_policy.rs`.

## Entry keys

Entries occupy their own subspace, one leading discriminant byte in the `Key` enum, so entry churn compacts apart from the metadata subspaces. The layout is pinned by golden vectors like every other key kind.

| Kind | Key components | Value |
|---|---|---|
| `index/unique` | `index_id`, `canon(values)` | `row_id` |
| `index/multi` | `index_id`, `canon(values)`, `row_id` | empty |

A unique entry's key is the value alone. Two commits inserting the same value write the same key and collide in SlateDB's write-write conflict detection. The loser retries, re-runs its closure, sees the winner's entry, and returns `Constraint`. This is the only race primitive the store offers, and it is sufficient.

A non-unique entry appends the row id, so rows sharing a value occupy distinct keys and concurrent appends of different rows stay benign. A lookup for `v` is the ascending prefix scan over `index/multi/{index_id}/{canon(v)}/`.

`index_id` leads both kinds, so each index is one contiguous range that lookups scan and reclamation deletes.

## Canonical encoding

Entry keys embed column values. The encoding is canonical and order-preserving per DuckLake column type: `encode(x) < encode(y)` as unsigned bytes if and only if `x < y` in the type's SQL order. Golden vectors and proptest roundtrips pin it.

| Type class | Encoding |
|---|---|
| Integers | Fixed-width big-endian with the sign bit flipped, width normalized per column type. |
| Strings and blobs | The UTF-8 or raw bytes, `storekey`-escaped so component boundaries stay unambiguous. Order is DuckDB's default binary order. |
| Floats | `-0.0` normalized to `+0.0`, every NaN collapsed to one pattern, then the total-order transform. NaN sorts greatest. |
| Temporal types | Their underlying integer count. |
| `UUID` | A 16-byte blob on both write paths. |
| Composite keys | Component encodings concatenated in declared column order under `storekey` tuple framing, so `("ab","c")` and `("a","bc")` stay distinct and a shorter value sorts before its extension. |

Direction is per column. A `Descending` column stores the bitwise complement of its fully framed component, terminator included, so variable-length values reverse correctly. The leading NULL flag is not complemented. An ascending store scan yields the declared composite order and a descending scan yields its opposite.

NULL placement is per column, `NullOrder::First` or `NullOrder::Last`. Each component carries a leading flag byte separating NULL from non-null, ordered per the placement. A row with a NULL in any indexed column is stored, so `IS NULL` can find it. Such a row is always multi-shaped and exempt from the value collision, so a unique index admits any number of NULL rows. An equality point lookup on NULL has no answer.

Orders are recorded on the definition only where they diverge from ascending, NULLS LAST, so an all-default index is byte-identical to one created without orders.

A composite key whose component encodings exceed `MAX_INDEX_KEY_BYTES`, 1024 bytes, is refused with `Constraint` at insert or registration.

## Coverage

An index covers every live row of its table, or the operation that would breach that is refused. Who derives an entry depends on who has the rows.

| Moment | Embedding API | Extension path |
|---|---|---|
| `inline_insert` | Moraine derives entries from the rows and stages them in the same batch. | Same, from the Arrow body DuckLake stages. |
| Inline flush | Nothing. Entries name row ids, which flush preserves. | Same. |
| `register_data_file` | Writer-supplied `(ordinal, values)` pairs, mapped to `row_id_start + ordinal` at commit. | Moraine reads the registered file's indexed columns and derives entries. |
| `inline_delete` of a store-resident row | Moraine recomputes the values from the chunk and stages the removal. | Same. |
| `register_delete_file`, or `inline_delete` against flushed rows | Writer-supplied `FileIndexRemoval` values `(row_id, values)` per killed row. | Moraine reads the killed positions from the target file. |

On the embedding API, `register_data_file`, `register_delete_file`, and `inline_delete` on an indexed table fail with `Constraint` when they omit the required entries. A delete removal must name the row being removed, and a dense target's row id must lie inside the target's range.

On the extension path a DuckLake registration carries no entries, so every entry is moraine's own work. A commit registering a data or delete file on a table with a maintained index fails with `Constraint` when the attach supplied no `DATA_PATH` store. The error names the file that could not be read. The data path is supplied once at creation through `META_DATA_PATH`, recorded in the `data_path` metadata option, and served back as DuckLake's `ducklake_metadata` row. A re-attach that supplies a conflicting `META_DATA_PATH` is refused. A table whose rows stay inlined needs no data store.

Small SQL inserts DuckLake stages inline, and their values cross the ABI as an Arrow body, so moraine derives entries from them as it does for `inline_insert`. Bulk SQL inserts are written to Parquet by DuckLake and registered, so the committer reads the registered file, projecting the indexed columns, the row positions, and the row-id column when the file carries one. A staged `register_delete_file` names positions, and moraine reads those positions' values and row ids from the target file. The inline body is encoded losslessly, matching how DuckDB writes Parquet, so a value inserted inline and the same value written to a file derive the same canonical bytes and collide as they must.

A unique index's scoped read is synchronous at commit, and a failed read aborts the commit. The check is never skipped.

### Backfill at creation

Rows moraine holds inlined are backfilled by scanning the chunks. Rows in external files need writer-supplied entries passed to `create_index`. `CREATE INDEX` has no writer and backfills by scoped-reading every live data file of the table, so its cost is the table's indexed columns. The build is one commit and one atomic batch, and uniqueness is validated over the assembled set before staging. A backfill above the commit bound fails typed before staging anything, and the caller re-drives it as a staged build. A table that already holds files cannot be indexed without a data store to backfill from.

### Re-derivation and compaction

A registered file is not always new rows. Multi entries re-derive as idempotent puts, and a unique entry already naming the same row is a no-op, so UPDATE rewrites and inline flush do not abort against their own entries. No register-side path stages an entry delete.

A commit whose change set names only compaction derives nothing. `ChangeSet::compaction_only_tables` returns the tables named by `merge_adjacent`, `rewrite_delete`, or the legacy `compacted_table` kind when nothing else appears in the set. An inline flush classifies the same way when its only deletes are the tombstone moves of the flushed tables and it deletes no data file. Every entry such a commit's files would derive is already stored, so the files are read only for the row-id columns that hold them to their sources. A change set naming compaction and anything else derives as before. This rule is what keeps a large indexed table compactable, because a merge decides its own batch size and cannot split.

## Uniqueness enforcement

At commit, each new unique entry is point-read through the transaction. Present with a different row id is `Constraint`. Present with the same row id is a no-op. Absent stages the put. Duplicates within one commit collide in memory. The window between the read and a concurrent identical put is closed by the key collision.

Entries are live-only, so "present" means a live row holds the value. Deletes remove entries in the batch that kills the row, so delete-then-reinsert behaves as SQL expects within or across commits.

A writer-supplied deletion of a unique entry is guarded: the committer reads the entry and drops the deletion unless the stored row id is the row being removed. A deletion the committer derived itself against a ready index is staged without the read. Against a building or poisoned index the guard stays.

The rejection names the index, the claiming row, the holding row, and whether the collision was within the commit or against a stored entry. It carries no key data. Its text avoids the substrings DuckLake's commit loop retries on, so a rejected bulk `INSERT` surfaces at once. The [extension surface](0006-extension-surface.md) states that wire contract.

## Commit bounds

A commit's memory footprint follows how many entries it stages. `stage_index_entries` refuses a commit above `MAX_INDEX_ENTRIES_PER_COMMIT`, one million entries, before doing any work, naming the count, the limit, and the remedy of splitting the load. `MAX_INDEX_STAGED_BYTES_PER_COMMIT`, 64 MiB over encoded key and value bytes, is checked before each write enters the batch. Both are fixed invariants in `crates/moraine/src/transaction/index_maintenance.rs`. Neither is a `CatalogOptions` field.

A staged build step carries two further bounds in `BuildStep`. `entries` defaults to one million and bounds one step's buffer. `bytes` defaults to 8 MiB and bounds the single object-store request a batch becomes, because a write batch lands as one WAL object under a single conditional PUT, and a batch too large for the request timeout retries forever. A step ends at whichever bound it reaches first and always carries at least one entry. The byte figure is measured on the physical keys the commit stages. Both are settable per call.

## Row-id payload and lookups

The entry payload is a row id. Flush, compaction, and UPDATE rewrites re-home rows and preserve their ids, so no maintenance operation rewrites an entry. Nothing removes a live index's entries when a data file leaves the catalog: a file's rows never leave without a row-grain deletion having taken their entries, or a replacement carrying them under preserved ids.

Lookups are accessors on `ReadOnlyCatalog` in `crates/moraine/src/catalog/handle/index_lookup.rs`:

| Accessor | Result |
|---|---|
| `index_lookup(table, index, values)` | Row ids for one complete equality key. A point get on a unique index, a prefix scan on a non-unique one. |
| `index_lookup_many(table, index, keys)` | The union of row ids for a set of complete keys, deduplicated and resolved under one head view. An empty set returns no rows after validating the index. |
| `index_range(table, index, lower, upper, reverse)` | Row ids for a value window. Each bound is `Included`, `Excluded`, or `Unbounded`. Results come back in stored order, or its opposite when `reverse` is set. |
| `index_nulls(table, index, prefix, reverse)` | Row ids for an `IS NULL` query. `prefix` is a leading run of `Some(value)` and `None`, at least one `None` required. |
| `index_predicate_rows(scope, table, predicates)` | Selects a ready index for conjunctive predicates. Described under planning. |

A range query names an equality prefix over the leading columns plus one range on the next column. A bare range on a non-leading column is refused. Comparison predicates never match NULL, so `index_range` clamps open sides to the non-null region, and `index_nulls` serves the NULL subrange separately. A multi-column range bound is a tuple, and a tuple window requires every named column to sort the same way.

Lookups are head-only. Entries are live-only, so a lookup through `read_scope_at(snapshot)` fails typed and time travel remains a scan. A lookup against a `Building` or `Maintaining` index fails with `Error::IndexBuilding`. A warm read-write handle serves lookups from the view it already holds with plain reads against the writer's `Db`. A read-only handle probes under a read session. The [read path](0007-read-path-and-caching.md) covers pinned handles.

`locate_row_ids(store, prefix, table, row_ids)` resolves row ids to `FileRowCandidate { row_id, data_file_id }` at head. `data_file_id` is `None` for a live inlined row and for a row located nowhere. Location is derived cache state and adds no `index` keys.

## Staged builds

A backfill larger than one commit runs as a `building` to `ready` lifecycle built from writer-maintained coverage, the value-keyed collision, and bounded steps.

1. The definition commits in `building` state as `altered_table` with `schema_changed`. From this commit forward every writer maintains the index: inserts stage entries, deletes stage removals, entry-less registrations are refused, and DuckLake writes are covered by the scoped read. Unique entries are value-keyed from the first commit.
2. A derivation pass reads the definition, inline rows, file registrations, schemas, and delete bookkeeping through one snapshot-isolated read session. It walks live inline chunks, then external files in durable source order, applying delete files and inline deletes as each source is read, and streams entries into the next step.
3. Each step commits as `inserted_into_table:<table_id>` and advances a source cursor on the definition: an inline cursor of `(schema_version, begin_snapshot, chunk_seq)` and row offset, and an external cursor of `(data_file_id, position)`. The step checks the derivation snapshot inside its commit closure. A concurrent delete writes `deleted_from_table`, which conflicts with the step, so the driver re-derives at a fresh snapshot from the durable cursor. An intermediate step mints a snapshot but writes no `ducklake_schema_versions` row.
4. When the cursor reaches the end of the live set and the definition is not poisoned, one final commit flips `building` to `ready` as `altered_table` with `schema_changed`. In-flight writers conflict, re-run, and see `ready`, so their re-runs apply full enforcement. From the flip forward the index is indistinguishable from a single-commit build: the entry range is byte-identical to what a from-scratch build over the same live rows produces.

The builder covers the rows live at its derivation snapshot. Everything committed after the definition is covered by the writer that committed it, so the two sets meet with no gap. A row live at one pass can die before its step lands. Every case resolves into derivation from a fresh snapshot: a delete committed before the pass is excluded by the delete bookkeeping the scoped read applies, a delete committed between derivation and the step is caught by the in-closure snapshot check, and a delete racing an open step surfaces as `CommitConflict` through the classification conflict. The driver re-derives after any snapshot advance, including an unrelated append.

A duplicate found during `building` poisons the build. The entry set is incomplete, so an absent probe proves nothing, but a collision a put does discover is real. Whether a backfill step or a writer finds it, the commit stages a terminal `poisoned` flag on the definition, drops the colliding claim, and lands normally. The driver stops at its next step, ends the definition into `history`, and surfaces `Constraint` to the creating caller. Poisoning writes the definition key, so it conflicts with cursor advances and every interleaving converges.

Before its first pass a build fills a bloom filter with every unique key the index already holds. A unique entry whose key the filter has never seen is staged as known absent without a read. A positive probes as usual.

The `Catalog` drivers are `create_index_staged(table, def, orders, data_store, data_prefix, step)` and `create_index_staged_with_maintenance`. They return once the index is ready or the build has failed, and a failed build drops its definition. An interrupted call leaves the definition `building`, and calling again with the same `def` resumes from the cursor. `drop_index` abandons a build. The driver requires a data store whenever the table has live registered files, checked before creating or adopting a definition, inside the definition's commit closure, and at the start of every pass. A missing store leaves a resumable build intact.

`moraine_indexes` exposes `is_building` and `state`. A poisoned definition reports `is_building` true, so a caller gating writes on readiness reads `state`.

### Deferred upkeep

An index created with `IndexMaintenance::Deferred` moves SQL additions out of the data commit. The mode is refused for a unique index. When a SQL commit adds rows covered by a deferred index, the same snapshot flips the index from `ready` to `maintaining`, persists the repair cursor, and stages no additions for it. Deletions remain synchronous in every state, so an UPDATE removes the old value in its data commit and leaves the new value for the repair.

`maintaining` serves no lookups. After the data snapshot is durable, `Catalog::repair_deferred_indexes(data_store, data_prefix, step)` drives the bounded backfill over the missing rows and flips the index back to `ready`. Intermediate steps classify `inserted_into_table`. The final flip is `FinishIndexMaintenance`, an `altered_table` operation that does not advance schema history. A failed repair leaves the durable `maintaining` marker, and the maintenance scheduler repairs maintaining indexes before sweeping dropped ones.

## Row-id resolution for data files

UPDATE, `rewrite_data_files`, and non-adjacent `merge_adjacent_files` write a trailing `_ducklake_internal_row_id` column tagged with DuckDB's reserved field id `ROW_ID_FIELD_ID`, and record `row_id_start` as NULL or as the file's minimum embedded id. A flushed inline file carries both the column and a non-NULL `row_id_start`, and its ids may hold gaps.

The scoped read resolves a row's id with `RowIdSource::Resolve`: the embedded column when the file carries one, else `row_id_start + position`, else `Corruption`. Column presence wins over the dense range even when both are recorded. A NULL or negative embedded id is `Corruption`. `RowIdSource::Ordinal` serves the extension's registration helper, whose positions are re-mapped onto a freshly allocated dense range. In that mode a file carrying the embedded column is refused, because registering it under new ids would fork its identity.

The delete side filters by position. Killed positions ride to the target's scoped read, and an entry is removed when its ordinal is named by a delete file or by an inline file-delete. Indexed columns are located in the file by field id through the [column mapping rules](0005-ducklake-feature-mapping.md). Missing fields use `initial_default`, and old physical values are converted to the indexed column's current type with checked casts.

Every `ducklake_file_column_stats` insert is versioned state and requires a `ducklake_snapshot` insert in the same commit. The reserved row-id column's statistics are no exception. A head-preserving maintenance batch may carry only raw deletes, deletion-schedule inserts, `ducklake_metadata` writes, and inline operations. `translate_maintenance` in `crates/moraine/src/transaction/staged.rs` refuses anything else with `Constraint`.

## Extension surface

DuckLake rejects index DDL, so the shim registers an `OperatorExtension` that binds `CREATE INDEX`, `CREATE UNIQUE INDEX`, and `DROP INDEX` for moraine-backed lakes, and a `PlannerExtension` that handles `DROP INDEX IF EXISTS`. Both execute through the core build and drop engine.

```sql
CREATE UNIQUE INDEX by_id ON lake.main.users(id);
CREATE INDEX IF NOT EXISTS by_region ON lake.main.users(region)
    WITH (staged = true, step_entries = 100000);
DROP INDEX IF EXISTS lake.main.by_region;
```

Native DDL requires autocommit and a writable lake, commits autonomously, and returns no rows. Options are `staged`, `step_entries`, `step_bytes`, `maintenance`, `directions`, and `nulls`. `step_entries` and `step_bytes` must be positive. `maintenance = 'deferred'` is limited to non-unique indexes. DuckDB's parser discards per-column `ASC`, `DESC`, and NULL-placement clauses, so ordering is given as `WITH (directions = ['desc', 'asc'], nulls = ['first', 'last'])`, one element per column. Expression indexes, partial indexes, `PRIMARY KEY`, and table-level `UNIQUE` are unsupported. `IF NOT EXISTS` tests the schema-level name, and repeating an interrupted staged create without it resumes the build.

The table functions take `catalog, schema, table, index` as leading strings, written `…` below. `moraine_indexes` stops at the table.

| Function | Effect |
|---|---|
| `moraine_index_lookup(…, v…)` | `(row_id, data_file_id)` for one equality key, one variadic value per indexed column. |
| `moraine_index_in(…, keys)` | Rows for the union of complete keys. A single-column index takes a list of scalars, a composite index a list of `row(...)` values. A key containing NULL matches no row. |
| `moraine_index_range(…, lower, upper, lower_inclusive, upper_inclusive, reverse := b)` | Rows for a value window. A NULL bound is an open side. |
| `moraine_index_nulls(…, prefix…, reverse := b)` | Rows for an `IS NULL` query. A `NULL` argument means `IS NULL`, any other means equality. |
| `moraine_index_verify(…)` | Recomputes the entry set and reports `stored`, `expected`, `missing`, `dead`, `mismatched`, and `certified_at`. |
| `moraine_indexes(catalog, schema, table)` | `index_id`, `index_name`, `is_unique`, `is_building`, `state`, and `certified_at`. `state` is `ready`, `building`, `maintaining`, or `poisoned`. |

Each function resolves the store handle from the attached catalog and the table id from `ducklake_table`, then drives the same core accessors as the embedding API. The read functions bind their arguments as constants and resolve against the revision the current DuckDB transaction pinned. A `Constraint` raised on the SQL write path carries none of the four substrings DuckLake's commit loop retries on: `conflict`, `concurrent`, `unique`, and `primary key`. A genuine `CommitConflict` does carry `conflict`, so DuckLake retries, the re-read finds the winner's entry, and the permanent `Constraint` surfaces then. `data_file_id` is NULL for an inlined or unlocated row, so a statement naming exact rows joins with `rowid = row_id AND data_file_id IS NOT DISTINCT FROM hits.data_file_id`. Tuple `IN` against these columns drops every NULL file id and is unsafe as a DELETE or UPDATE predicate.

## Verification and the certificate

`ReadOnlyCatalog::verify_index(table, index, data_store, data_prefix)` recomputes the entry set at the current snapshot through the same scoped reads the backfill uses and diffs it against the index. `IndexVerification` reports:

| Count | Meaning |
|---|---|
| `stored` | Entries the index holds. |
| `expected` | Entries the live rows require. |
| `missing` | A required entry the index does not hold. |
| `dead` | A held entry no live row requires. |
| `mismatched` | A unique entry under the right value naming another row. |

Verification costs what a build costs and holds the stored entry set in memory. A `building` index is refused.

`certify_index` runs the pass through a read-write attach and records the outcome on the definition with `record_index_verification`. A clean pass writes the snapshot it agreed at as `verified_at_snapshot`, and a pass that found any discrepancy clears an earlier agreement. The recording names the definition version the pass observed and is refused if the definition moved, so a rebuild landing between a pass and its commit does not inherit the agreement. A staged build step, a rebuild, and a deferred repair withdraw the certificate. Ordinary maintained writes do not. The certificate is a separate field, and the only state coupling is that it requires `Ready`. `moraine_indexes` reports it as `certified_at`. A read-only attach reports without writing.

## Index-accelerated planning

DuckLake owns the planner. The extension's optimizer rule translates pushed constant predicates into conjunctive column comparisons and offers them to the core. `ReadOnlyCatalog::index_predicate_rows(scope, table, predicates)` accepts `IndexPredicate { column, comparison }`, where `IndexComparison` is an equality list, `IsNull`, or an inclusive or exclusive lower or upper bound. It selects a ready index with the longest usable prefix, probes it, and returns `IndexPredicateRows { rows, certified_exact }`, or `None` when no ready index covers a leading predicate. The original SQL predicate remains evaluated over candidate rows. A certified full-key probe with no residual predicate is the one case where a `DELETE` is rewritten to positional deletion without a scan.

The mechanisms:

- **`IndexReadScope`** pins catalog, index, file, and inline reads to one committed store revision, and its `IndexReadIdentity` lets a plan validate reuse without retaining a transaction. DuckLake's file-list and metadata reads for the statement observe the same scope.
- **`locate_row_positions_at(scope, data_store, data_prefix, table, pairs)`** resolves `(row_id, data_file_id)` pairs to `LocatedDeletion` positions within committed files at the scope's revision.
- **`LocatedRowScan`**, returned by `scan_rows_at`, reads only the requested columns at the resolved positions, one projected Arrow batch at a time. The SQL form is `moraine_summary_scan`.
- **File directories and summaries** are per-table in-memory structures mapping row ids to the current files and inline chunks that hold them, built from each file's row-id source and refreshed in place as the file map changes. The [read path](0007-read-path-and-caching.md) describes published row summaries.

Candidate row ids and file ids feed DuckLake's scan as filters on `rowid` and the virtual `data_file_id` column, so the file list and the rows read within surviving files both narrow. Results are memoized per pinned revision and invalidated when the revision moves. A DuckDB transaction with staged writes keeps probing at the revision it started on, because index entries carry no transaction-local overlay.

The rule declines and leaves the ordinary plan alone for a scan at a snapshot other than the transaction's, for a transaction with local inserts, where no ready index covers a leading predicate, for a value of a type the index does not cover, and for a probe that failed, which is logged. The positional DELETE rewrite additionally requires a certified index, every pushed predicate answered in full, every resolved row in a committed file, no deletion vectors on the table, no `RETURNING`, and nothing but projections between the delete and its scan. UPDATE keeps the ordinary operator and narrows its reads through the pushed filters.

## Format gate and reclamation

The minimum readable format already includes the index subspace, staged build state, and deferred upkeep, so index operations stamp no format on their own and an older writer that would maintain no entries is excluded at open. The [storage format](0001-storage-format.md) owns the version ladder. Read-only attaches over an indexed store are unaffected.

`drop_index`, and `drop_table` ending a table's indexes, orphan the entry range. The dropping commit stays bounded and deletes no entries. `Catalog::maintain` with `sweep_orphaned_index_entries` discovers dead index ids by seeking through the `index` subspace and reclaims each range in head-preserving batches. Ids are never reused, so an id absent from the live catalog is dead forever. [Maintenance](0010-maintenance.md) specifies the sweep.

## Tests

- Canonical encoding roundtrips and preserves SQL order for every type, direction, and NULL placement.
- Racing inserts of one unique value resolve to one `Constraint` and one committed row.
- A staged build's entry range is byte-identical to a single-commit build over the same live rows.
- A concurrent delete during a staged build leaves no entry for the dead row.
- Entries survive flush, UPDATE rewrite, merge, expiry, and cleanup, held to the table's own answer.
- A maintenance batch inserting file statistics without a snapshot is refused, the reserved row-id column included.

## Alternatives

- **Entries stored in Parquet beside the data files.** Uniqueness would need a read-write check with no store collision to close the race, and every lookup would open files. Rejected.
- **Row id in the key for unique entries, converted to value-only at the ready flip.** Represents duplicates without poisoning, but the conversion is an entry-proportional rewrite that itself needs staging, and writers would maintain two key forms during the window. Rejected.
- **One index per data file.** Compaction and rewrites would rebuild indexes proportional to moved rows, and uniqueness would span every file's index. Rejected.

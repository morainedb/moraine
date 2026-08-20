# RFC 0012: Schema evolution and versioning

- **Date:** 2026-07-09

## Summary

Schema evolution in moraine needs **no new store machinery**. Adding,
dropping, renaming, reordering, or type-promoting a column is expressed as
`current`/`history` version transitions, landed in one `WriteBatch` like
any other commit ([RFC 0002](0002-slatedb-key-encoding.md),
[RFC 0004](0004-commit-protocol.md)). This RFC is mostly a proof of that
claim — and the claim is verified live: every column op DuckLake's `ALTER
TABLE` can express — `ADD`/`RENAME`/`DROP COLUMN` and `ALTER COLUMN … TYPE`
(type promotion, including over rows inlined under the old type, which read
back coerced across the version boundary) — lands as `ducklake_column`
version transitions over the generic staged-write path, with no
schema-mutation path in the shim (e2e
`ducklake_column_schema_evolution_through_staged_writes` and
`ducklake_column_type_promotion_over_inlined_data`). Column **reorder** is
not reachable through DuckLake SQL — there is no reorder `ALTER` — so it
stays a latent capability of the same machinery (a position change is a
value edit like any other), never issued in the DuckLake-driven surface.
The load-bearing decisions it pins are two: **column identity vs.
position** — a column is keyed by its DuckLake-allocated field id, never by
its ordinal, field ids are never reused, and everything mutable about a
column (name, position, type) lives in the value — and **every mutation of
a column record is a temporal version transition**, never an in-place value
edit, so the record live at any past snapshot is preserved verbatim in
`history`. Schema time travel then falls out of RFC 0002's temporal
reconstruction for free — names and order included — and data files survive
evolution because they reference columns by field id.

## Goals

- Every column-level schema operation (`add`/`drop`/`rename`/reorder/type
  promotion) is a `current`/`history` version transition inside a single
  commit batch — no dedicated schema-mutation path.
- Column identity is stable and eternal: reconstructing the schema at any
  past snapshot yields the exact column set, **names**, order, and types at
  that snapshot — matching the row set DuckLake's own temporally-versioned
  catalog would hold ([RFC 0006](0006-extension-surface.md)
  row-faithfulness) — and historical Parquet remains readable by field id.
- Rename and reorder touch **only the columns whose name or position
  changed** — untouched siblings see no transition and no key churn.
- The column-level operations that must set RFC 0004's `schema_changed`
  flag are enumerated, so `schema_version` advances exactly when DuckLake's
  does.

Non-goals:

- The temporal reconstruction algorithm itself — [RFC 0002](0002-slatedb-key-encoding.md).
- Conflict detection and `schema_version` counter mechanics —
  [RFC 0004](0004-commit-protocol.md). This RFC only names which column
  operations trip the flag.
- Type-promotion *legality*. DuckLake owns which promotions are legal;
  moraine stores the type verbatim and does not validate (row-faithful,
  [RFC 0006](0006-extension-surface.md)).
- Physical rewrite of data files on drop — that is compaction,
  [RFC 0008](0008-compaction.md).
- Partition- and sort-spec evolution —
  [RFC 0013](0013-partitioning-sorting-and-pruning.md).

## Background

DuckLake assigns every column a stable **field id** (`column_id`) that is
**per-table**, not drawn from `next_catalog_id` (source-verified: the
global counter allocates schema/table/view/macro/partition/sort ids only —
[RFC 0004](0004-commit-protocol.md)). At `CREATE TABLE`, ids are assigned
from 1 in pre-order over the column list (nested fields included — see
Nested types); at `ADD COLUMN`, the next id is `MAX(column_id) + 1` over
the table's **entire column history, ended versions included**
(`DuckLakeMetadataManager::GetNextColumnId`, whose adjacent comment states
the id must be unique across dropped columns too). The field id is the
column's permanent identity: it is what data files record, and what a
reader uses to project a physical Parquet column onto a logical catalog
column. Field ids are **never reused** — a dropped column's id can never
collide with a later `add_column`. That is precisely what lets old
Parquet, written against the old schema, be read back: the file references
field ids, and a retired id in a file simply is not projected by the
current schema.

DuckLake *derives* the next field id by `MAX(column_id) + 1` over the
table's entire column history because a SQL catalog can run that query
cheaply. moraine does not copy the derivation — it **persists the counter
instead**: the table record carries `next_column_id`, seeded past the
highest id at `CREATE TABLE` and advanced by each `add_column` (the add is
therefore also a new version of the table record). Deriving from history
in moraine would force every head materialization to scan `history` —
violating RFC 0002's "loading the current catalog is a scan of `current`
only" — and, fatally, the derived high-water mark would *regress* once
RFC 0007 expires the very `history` records it was derived from, silently
re-issuing retired field ids. A persisted counter survives history expiry
by construction. Defensively, allocation never goes below the live
columns: the next id is `max(next_column_id, max live column_id + 1)`, so
a table version whose counter field is absent (e.g. authored by the
RFC 0006 staged-row path, where DuckLake computes ids itself) degrades to
live-max allocation rather than colliding with a live id.

RFC 0002 keys the `column` kind by `(table_id, column_id)` and keeps names
out of keys entirely. RFC 0004 tracks a `schema_changed` flag and advances
the snapshot record's `schema_version` counter — DuckDB's schema-cache key
([RFC 0009](0009-reader-consistency-and-caching.md)) — only on
schema-changing commits. This RFC sits on top of both and adds no new rule
to either.

## Design

### Identity in the key, everything mutable in the value

The `column` key is `(table_id, column_id)` (RFC 0002). The column's
**ordinal** — DuckLake's `column_order`, a position numbered from 1 that is
never renumbered, so a drop leaves a gap the survivors keep — is a **value
field**, not a key component. This split is half the
design. The other half: **every change to a column record is a version
transition** — end the current version into `history` (preserving its value
verbatim), write the new version at the *same* `current` key. Each column
operation maps mechanically:

| Operation | Key effect | Value effect |
|---|---|---|
| `add_column` | new `current` key at a freshly allocated `column_id` | full column record |
| `drop_column` | end the `current` version → `history`; id retired forever | (record moves to `history` unchanged) |
| `rename_column` | end old version → `history`, new version at same `column_id` | new record with new `name` |
| reorder | end old version → `history`, new version at same `column_id`, per **moved** column | new record with new `position` |
| type promotion | end old version → `history`, new version at same `column_id` | new record with new `type` |

The version transition is not optional bookkeeping — it is what makes the
old name/position/type recoverable at all. An in-place value edit would
*overwrite* the record that was live at earlier snapshots: time travel to a
pre-rename snapshot would silently return the new name, and a scan of
`ducklake_column` at that snapshot would return rows DuckLake's own
temporally-versioned catalog never held (DuckLake models these changes as
new row versions with begin/end snapshots). The temptation to special-case
the "harmless" mutations is real and is rejected in Alternatives.

The `current` **key** never changes across any of these — identity is the field
id, and the field id is eternal. What churns is bounded and proportional to
the change: one `history` record per column actually touched. A rename touches
one column. A reorder touches the columns whose position changed — with
positional ordinals that can be most of the table, but it is
one small `history` record per moved column in one batch, O(columns), never
O(data). Untouched siblings produce nothing. `add` allocates the table's
next per-table `column_id` from the table record's persisted
`next_column_id` counter (Background). `drop` ends the column's `current` version
exactly as any entity end does under RFC 0002 — delete the `current` key, write
the `history` key with `end_snapshot` appended, both in the same batch — and
the id is never handed out again.

Identity lives in the key; name, position, and type live in the value. A
schema change is therefore never a "schema operation" to the store — it is
the same puts and deletes at statically computable keys that every commit
already uses.

### Type promotion

DuckLake permits only **widening** promotions (e.g. `INT32 → INT64`) and
owns the legality rule entirely. moraine stores the column's type verbatim
and **does not validate the promotion** — row-faithfulness
([RFC 0006](0006-extension-surface.md)): if DuckLake staged it, moraine
lands it.

A type change is modeled as an entity version boundary: end the old column
version to `history` (carrying the pre-promotion type), write a new `current`
record at the **same `column_id`** with the new type. The field id is
preserved — data files written under either type still project correctly —
and time travel to a snapshot before the promotion reconstructs the old
type, because the pre-promotion record is the live one in the `current`+`history`
filter at that snapshot.

Rename, reorder, and type promotion are deliberately **uniform**: all three
open a new version. There is no cheaper in-place form for the "harmless"
ones, because time travel does not distinguish harmless — a reader at the
old snapshot needs the old type to interpret old files, and equally needs
the old name and order to reproduce what the catalog *said* at that
snapshot (and what DuckLake's own versioned rows would say, per RFC 0006
row-faithfulness).

### Schema time travel is free

Reconstructing the catalog at snapshot `S` already yields, from RFC 0002's
`current`+`history` filter over the `(table_id, *)` column range, the exact set of
columns live at `S`, each carrying its `name`, `position`, and `type` **as
of `S`**. There is nothing schema-specific to do: the same filter that
reconstructs tables and files reconstructs the schema. A dropped column
reappears (it is in `history` with `begin <= S < end`); a renamed column shows
its `S`-era name and a reordered column its `S`-era position (the
pre-change version is a distinct `history` record, preserved verbatim by the
version transition); a promoted column shows its `S`-era type. This only
holds *because* every mutation is a version transition — it is the payoff
of the uniformity rule above, and it needs nothing extra: the temporal
machinery is already built.

### Data files survive schema evolution

Data files reference columns **by field id**, not by name or position.
Consequences:

- **Drop does not rewrite files.** `drop_column` ends the catalog column
  version; the physical column stays in existing Parquet, simply no longer
  projected (its field id is retired). Physical removal is compaction's job
  ([RFC 0008](0008-compaction.md)), decoupled from the schema commit.
- **Reorder does not touch files.** Position is a catalog value; files are
  unaffected.
- **Promotion does not rewrite files.** Old files keep the old physical
  type; DuckLake reads them through the widening cast. moraine stores both
  catalog type versions and never reinterprets file contents.

This decoupling is what keeps a schema change O(columns-touched) instead of
O(data), and it is only sound because ids are never reused.

### Nested types

DuckLake assigns field ids to **nested** fields too (struct members, list
elements, map key/value), and — source-verified — represents them as
**rows in `ducklake_column`**: every nested field is its own row with its
own per-table `column_id`, linked to its parent by the `parent_column`
column (top-level roots have `parent_column IS NULL`). Ids are assigned
**pre-order** from the same per-table counter — a struct column takes the
next id, then its members recursively
(`DuckLakeFieldId::FieldIdFromType`). Row-faithfulness (RFC 0006)
therefore fixes moraine's representation: **one `column` record per
`ducklake_column` row, nested fields included**, each keyed
`(table_id, column_id)` under RFC 0002's existing `column` kind, with
`parent_column` in the value. There is no embedded type tree to invent —
a scan of `ducklake_column` must return exactly DuckLake's rows, and
storing the tree inside the root column's value would require exploding
it back into rows on every scan (re-modeling, the thing B1 forbids).
Nested evolution — adding a struct field, widening a list element — is
then just column operations on the affected field rows: an added struct
member is a new `current` record at a freshly allocated per-table id with
`parent_column` set; a widened element is a version transition on that
field's row. The uniformity rule above applies to nested field rows
unchanged.

moraine allocates nested ids the same way DuckLake does — pre-order from
the table's own counter, never against a global counter — so the verb path
and the staged-row path (RFC 0006) assign identical ids to the same
`CREATE TABLE` or `ADD COLUMN`.

The verb path takes the nested type as a **tree**, not a type string to
parse: `ColumnDef` carries DuckLake's marker (`STRUCT`, `LIST`, `MAP`) and
its fields as children, and `create_table`/`add_column` walk it depth
first, taking the next field id and the next position from the same two
counters a top-level column draws from. Parsing a SQL type string in the
core was rejected: DuckLake supplies the decomposition over the staged path
already, and a type grammar in the catalog layer is a surface that would
have to track DuckDB's, forever, to stay correct.

Three rules follow from the same row-faithfulness and are pinned against a
real DuckLake catalog:

- **A field's name is scoped to its parent.** Two structs may each hold an
  `x`; only siblings collide.
- **Dropping a nested column drops its whole subtree**, to any depth — a
  field whose parent is gone is not a column anyone can name, and DuckLake
  ends parent and descendants in one snapshot.
- **A table's "last column" refusal counts top-level columns only.**
  Dropping the last field of a struct leaves the struct, which is still
  selectable.

### Column and name mapping for external Parquet

Externally-written Parquet may not carry DuckLake field ids, so DuckLake
maps physical columns to logical ones via `ducklake_column_mapping` and
`ducklake_name_mapping`. These are entity kinds under RFC 0002's
key structure — `(tag, kind, u64 components)` tuples, keyed
`table_id`-first so a table's mapping is one contiguous
range — and are stored row-faithfully. RFC 0002's keyspace map pins the
shape: one `mapping` kind keyed `table_id, mapping_id`, with the
`ducklake_name_mapping` rows embedded in its value rather than given keys
of their own, since they are pure child rows with no independent
lifecycle. A
mapping registration is **data-only** — it rides the data-file
registration that carries it and does not bump `schema_version`
(source-verified: mapping writes sit outside DuckLake's
`SchemaChangesMade`; RFC 0004).

### Which operations bump `schema_version`

RFC 0004 advances `schema_version` iff the committer's `schema_changed`
flag is set, and DuckDB caches metadata on it
([RFC 0009](0009-reader-consistency-and-caching.md)). This RFC pins the
column-level half of that classification:

| Operation | `schema_changed` |
|---|---|
| `add_column` | set |
| `drop_column` | set |
| `rename_column` | set |
| reorder columns | set |
| type promotion | set |
| nested-type change | set |
| column/name-mapping registration | **not set** — data-only (rides `register_data_file`) |

Every *column* operation in this RFC is schema-changing; mapping
registration is the one exception, data-only per DuckLake's own
classification (`SchemaChangesMade` tests catalog-entry changes, and
mapping writes ride data-file registration outside it — RFC 0004,
source-verified). This matches RFC 0004's structural-mutator list; this
RFC is the detailed enumeration behind its "add/rename/alter/drop column"
entry. The flag is an explicit property of
the staged mutation set (RFC 0004), never inferred from the batch key set —
a batch's keys do not distinguish a column version transition from, say, a
stats-driven touch of table state, and any after-the-fact inference would
eventually misclassify in one direction or the other. (On the staged-row
path the flag does not apply at all — DuckLake authors `schema_version`
itself; RFC 0004.)

The `schema_version` → snapshot reverse index is not derived and not
invented: DuckLake persists it as a catalog table.
`ducklake_schema_versions(begin_snapshot, schema_version)` names the
snapshot at which each schema version took effect — backfilled from
`ducklake_snapshot` on metadata upgrade, and carrying a `table_id` column
for per-table schema versions from metadata version 1.1 onward. moraine
stores those rows row-faithfully like every other `ducklake_*` table, in
the `schema_version` subspace (RFC 0002) — their own keys rather than a
field of the snapshot record, so snapshot expiry (RFC 0007) cannot take
the reverse index a surviving data file still needs.

### Conflicts

Two concurrent schema changes to the same table overlap on `table_id` and
are therefore a **true conflict** by RFC 0004's table-level rule — no new
conflict rule is needed. A rename of column 3 and an add of column 7 on the
same table both mutate that `table_id`; they conflict, and one aborts with
`CommitConflict`. Schema changes to *different* tables are disjoint and both
succeed via benign-race retry. `CREATE`/`DROP SCHEMA` remains at
schema-list grain per RFC 0004.

### Property-test and e2e obligations

Per [RFC 0001](0001-repository-structure-and-conventions.md), against real
SlateDB on in-memory `object_store` and against real DuckLake SQL in e2e:

- **Reconstruct-schema-at-snapshot matches DuckLake.** For an arbitrary
  sequence of column operations and an arbitrary snapshot `S`, the column
  set / order / types moraine reconstructs at `S` equals what DuckLake
  reports for the catalog at `S`.

  This is two obligations wearing one sentence, and they are met by two
  tests. The *rules* half is a core property test: an arbitrary sequence
  over the verb path, replayed by an independent model that restates the id
  and position rules, compared at every snapshot the sequence produced —
  fast, and covering far more sequences than a fixed script. The
  *agreement-with-DuckLake* half is differential and belongs at the e2e
  tier: a fixed but longer DDL script fed to both moraine and a stock
  DuckLake catalog, with the live column set compared at **every** historical
  version rather than at head, since a divergence that cancels out by head is
  exactly what a head-only probe cannot see. It stays a fixed script: a
  proptest that spawns a CLI per case would cost minutes per run.
- **Field ids are never reused.** Across any sequence including drops and
  re-adds, no `column_id` is ever allocated twice; a dropped id never
  reappears in a later `add`. Drop-then-add yields a strictly larger id.
- **Allocation order matches DuckLake.** Verb-path `add_column` assigns the
  ids DuckLake would — per-table `MAX(column_id) + 1` over the column
  history, nested fields in pre-order — so the verb and staged-row paths
  never diverge.
- **Rename/reorder are version transitions and time-travel-correct.** After
  a rename (or reorder), reconstruction at a pre-change snapshot yields the
  old name (or order) exactly; at a post-change snapshot, the new. The
  batch writes exactly one `history` record per column actually touched.
- **Untouched columns churn nothing.** A rename of one column, or a reorder
  moving `k` columns, writes no `history` record and no `current` change for any
  other column of the table.
- **Promotion is time-travel-correct.** After a widening promotion,
  reconstruction at a pre-promotion snapshot yields the old type; at a
  post-promotion snapshot, the new type.
- **`schema_version` transitions match DuckLake** for every operation in
  the table above (the RFC 0004 schema-version matrix, extended to
  column-level ops).

## Alternatives considered

- **In-place value edits for rename and reorder (no version transition).**
  The seductive "zero key churn" optimization: since the field id does not
  change, just overwrite the `current` value. Rejected as a correctness bug,
  not an optimization: an in-place edit destroys the value that was live at
  earlier snapshots, so time travel to a pre-rename snapshot returns the
  *new* name (and pre-reorder, the new order) — contradicting both this
  RFC's reconstruction goal and RFC 0006 row-faithfulness, since DuckLake's
  own catalog holds these changes as distinct row versions with begin/end
  snapshots. The churn saved is one small `history` record per touched column;
  the price is silent wrong answers under time travel. Every column
  mutation versions, uniformly.
- **Column position in the key (`table_id, position`).** Rejected: a
  reorder becomes key churn across the *whole table* — every column past the
  insertion point ends its `current` version and re-lands, and worse, position
  is not a stable identity, so data files could not reference columns
  through it. The field id must be the key.
- **Reusing dropped column ids.** Rejected: it breaks field-id-based reads
  of historical Parquet. A file written against a dropped column's id would
  be silently reinterpreted as the new column that reused the id —
  data corruption, not an error. Ids retire forever.
- **A separate "schema" snapshot structure distinct from `current`/`history`.**
  Rejected: it duplicates the exact temporal machinery RFC 0002 already
  provides. Schema is just another set of temporally-versioned entities;
  giving it a parallel versioning system would mean two reconstruction
  paths to keep consistent, two time-travel filters, and two things to get
  wrong. Columns live in `current`/`history` like everything else.

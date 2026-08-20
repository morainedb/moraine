# RFC 0013: Partitioning, sorting, and pruning

- **Date:** 2026-07-09 (sort orders added 2026-07-13)

## Summary

Places DuckLake's partitioning and sorting tables in the moraine keyspace and
fixes moraine's stance on partition pruning. Five DuckLake tables are in
scope. Partitioning: `ducklake_partition_info` (a table's partition **spec**),
`ducklake_partition_column` (the **columns and transforms** of a spec), and
`ducklake_file_partition_value` (a data file's partition **values**). The spec
already has a home — the `partition` kind of [RFC 0002](0002-slatedb-key-encoding.md);
this RFC confirms its lifecycle, embeds partition columns per RFC 0002's
convention, and embeds per-file partition values into the `file` record so a
table's files and their partition values are one contiguous scan. Sorting:
`ducklake_sort_info` (a table's sort **spec**) and `ducklake_sort_expression`
(the **ordered expressions** of a spec) mirror that placement exactly — a
`sort` kind for the spec, expressions embedded. On pruning it mirrors RFC 0002
exactly: moraine stores specs, transforms, expressions, and values
**verbatim** and serves them efficiently; DuckLake's planner does the pruning
and DuckLake's writer does the sorting. Server-side partition pruning is
not a moraine responsibility under this architecture, matching RFC 0002's
answer for stats-pruning pushdown.

## Goals

- Every partitioning and sorting table has a defined home, validated against
  real DuckLake SQL in the e2e suite.
- A planner's core question — "scan the files of table T with their partition
  values" — is one contiguous `table_id`-first `current` range, no join, no second
  subspace.
- Partition and sort specs evolve over a table's life (set, change, clear) and
  time travel reconstructs the spec-in-force at any snapshot for free.
- A data file keeps the partition spec it was written under, so files under
  different specs coexist correctly.

Non-goals:

- Server-side partition pruning. DuckLake owns typed predicate evaluation;
  moraine serves the complete row-faithful metadata view.
- Evaluating transforms or sort expressions. moraine stores transform
  definitions and sort expressions verbatim and never applies them (RFC 0006
  row-faithfulness).
- Sorting data. DuckLake's writer sorts rows on `INSERT` against the table's
  sort spec; moraine only serves the spec.
- Compaction planning across partitions — [RFC 0008](0008-compaction.md) owns
  the rewrite; this RFC only states what partition state a rewrite must carry.
- Hidden/implicit partitioning schemes beyond DuckLake's spec.

## Background

DuckLake partitions a table by a **partition spec**: an ordered list of
partition columns, each pairing a source column with a **transform** (identity,
`year`/`month`/`day`/`hour`, `bucket(N)`, `truncate`). A written data file
records the partition **values** it falls under — one value per partition
column of the spec it was written against. A table can be repartitioned: the
spec is set, later changed, or cleared, and files written under older specs
remain valid. DuckLake models this with `ducklake_partition_info` (the spec,
temporally versioned like every catalog entity), `ducklake_partition_column`
(spec columns), and `ducklake_file_partition_value` (per-file values).

DuckLake sorts a table by a **sort spec**: an ordered list of sort keys, each
pairing a SQL **expression** (a verbatim string, tagged with the dialect it is
written in) with a sort direction and a null order. The spec is set with
`ALTER TABLE … SET SORTED BY (…)` and cleared with `RESET SORTED BY`; when a
spec is live, DuckLake's own writer sorts incoming rows on `INSERT` (its
`sort_on_insert` option). DuckLake models this with `ducklake_sort_info` (the
spec, temporally versioned like every catalog entity) and
`ducklake_sort_expression` (the spec's ordered keys). Unlike partitioning,
no per-file record exists: a data file does not name the sort spec it was
written under — the spec is a write-time instruction, not file provenance.

RFC 0002 already reserves the `partition` kind for the spec and states the
embedding convention this RFC applies. RFC 0002 also fixes the pruning stance
this RFC extends from stats to partitions: min/max are stored verbatim, the
comparison belongs to DuckLake, and any future server-side pruning must be
type-aware rather than a naive lexicographic compare.

## Design

### Placement

| DuckLake table | Home | Rationale |
|---|---|---|
| `ducklake_partition_info` | `partition` kind (RFC 0002), `(table_id, partition_id)`, in `current`/`history` | The spec has an independent lifecycle — a table repartitions over time — so it earns its own kind and is temporally versioned. |
| `ducklake_partition_column` | **Embedded** in the `partition` value | Pure child of the spec, no independent lifecycle: columns + transforms live and die with their spec (RFC 0002 convention). |
| `ducklake_file_partition_value` | **Embedded** in the `file` value | Per-data-file, 1:N (one value per partition column), no independent lifecycle: values live and die with their data file (RFC 0002 convention). |
| `ducklake_sort_info` | `sort` kind, `(table_id, sort_id)`, in `current`/`history` | The sort spec has an independent lifecycle — a table re-sorts over time — so it earns its own kind and is temporally versioned, exactly like `partition`. |
| `ducklake_sort_expression` | **Embedded** in the `sort` value | Pure child of the spec, no independent lifecycle: expressions live and die with their `sort_id` (RFC 0002 convention). |

The load-bearing choice is embedding `file_partition_value` in the `file`
record rather than giving it its own kind or subspace. A file's partition
values travel with its `current`/`history` record. Because per-table collections are
keyed `table_id`-first (RFC 0002), "scan the files of table T with their
partition values" is exactly one contiguous `current` range — no join against a
second subspace, no scatter. That is the shape a planner needs, and it costs no
new keyspace.

Embedding also costs nothing on writes — partition values are written once
when the file is registered and never updated, so carrying them in the `file`
value provokes no rewrite. Nothing reads them selectively, and both placements
would be read by full scan in any case, so the choice is not contingent on
observed access patterns; it reopens only with server-side pruning, on the
terms RFC 0009 sets.

### The partition spec record

The `partition` value carries the spec's `begin_snapshot`, its ordered
partition columns, and for each column the source column reference and the
transform. Partition columns reference their source column **by field id**
(`column_id`), never by name — see schema evolution below. Transforms are
stored **verbatim** as DuckLake defines them (the transform name and its
parameter, e.g. `bucket(16)` or `truncate(10)`); moraine never parses or
applies them.

The spec is wholly explicit — there is no implicit or derived partitioning
alongside it. A bare partition column is written out with the transform
`identity` rather than left blank, so every partition key is one stored row
with a named transform and nothing has to be inferred.

### The sort spec record

The `sort` value carries the spec's `begin_snapshot` and its ordered sort
keys; each key carries the `expression`, its `dialect`, the `sort_direction`,
and the `null_order`, all stored **verbatim** as DuckLake wrote them, plus the
explicit `sort_key_index` — the order is implicit in the embedding, but the
index is a `ducklake_sort_expression` column and row-faithfulness keeps it.
moraine never parses or evaluates an expression. Note the contrast with
partition columns: a sort key names its column inside a free-form SQL string,
not by field id — see schema evolution below.

### Spec evolution

Partitioning and sorting are set, changed, or cleared over a table's life
(sorting via `SET SORTED BY` / `RESET SORTED BY`). Both conflict at table
level as alters, but they differ on `schema_version`
([RFC 0004](0004-commit-protocol.md)): a partition change is a
**schema-changing commit** that bumps it, while a sort change does not —
DuckLake marks the table altered without a bump (source-verified; a sort
spec never invalidates cross-file compaction). On the staged path moraine
stores DuckLake's authored `schema_version` verbatim either way; the verb
path enforces the split itself, `set_partitioning`/`clear_partitioning`
classifying as schema-changing alters and `set_sorting`/`clear_sorting` as
alters that are not. Both
specs are versioned temporally like any entity, and the transitions are
identical:

- **Set / change** — end the current spec's `current` version into `history` with
  `end_snapshot` (if one existed) and insert the new spec into `current`, in the
  same `WriteBatch` (RFC 0002 atomicity invariant).
- **Clear** — end the current spec's `current` version into `history`. What
  DuckLake actually issues differs by feature (source-verified):
  `RESET SORTED BY` is a genuine clear (end the old spec, insert nothing;
  no live spec afterward), while `RESET PARTITIONED BY` is a
  **set-to-empty** — the old spec ends and a new spec with zero columns
  lands live. moraine stores either shape row-faithfully; the bare
  end-without-insert also arrives when `DROP TABLE` ends a table's specs.

A data file records, in its `file` value, the `partition_id` of the spec it was
**written under**. Files written under different specs therefore coexist
unambiguously: each names its own spec. Reconstructing the spec-in-force at
snapshot `S` is the ordinary `current`+`history` temporal filter (RFC 0002) — no
special path. Time travel gets the historical spec, and each file's values, for
free.

Sort specs have no per-file counterpart: DuckLake records no link from a data
file to the sort spec in force when it was written, so there is nothing for
moraine to embed. Time travel reconstructs the sort spec-in-force by the same
temporal filter, and that is the whole story.

### Pruning — moraine does not prune, DuckLake does

This is the spine, and it is RFC 0002's stats stance applied to partitions.

moraine **serves** partition specs, transforms, and per-file partition values —
faithfully and efficiently, available inside the single per-table file scan —
and **does not evaluate** them. DuckLake/DuckDB's planner reads the spec, reads
each candidate file's partition values (in the same scan), applies the
transforms it already understands, and prunes. moraine returns rows; the
planner decides which files to read.

A moraine-side **server-side partition-pruning pushdown** is rejected under
this architecture, with the same answer as RFC 0002's stats-pruning pushdown,
RFC 0006's pushdown surface, and RFC 0009's partial materialization. No row
filter is pushed into moraine, the whole live catalog is already resident, and
DuckLake owns the typed planner semantics. Any replacement would have to be
both **transform-aware** and **type-aware**:
correctly pruning a `bucket(16)` or `year(...)` partition means reproducing
DuckLake's transform semantics, and comparing a value means DuckLake's typed
comparison, never a lexicographic compare over stored strings. Doing it wrong
silently drops correct rows — the exact failure RFC 0002 warns against.
moraine therefore stays row-faithful; evidence for a different architecture
would require a new design rather than an attach option or a dormant code path.

### Sorting — moraine does not sort, DuckLake does

The same spine, applied to write-time ordering. moraine **serves** the sort
spec — DuckLake loads it as one `sort_info ⋈ sort_expression` join, which
moraine answers from the single contiguous `sort` kind scan — and **never
sorts data**. DuckLake's writer reads the spec, evaluates the expressions,
and orders rows on `INSERT`; a sort spec never changes what moraine stores
about a data file, only what DuckLake writes into it. There is no server-side
work to defer here: sorting has no pruning analog, because a spec constrains
writes, not reads.

### Interaction with schema evolution (RFC 0012)

Partition columns reference source columns by **field id** (`column_id`), and
[RFC 0012](0012-schema-evolution-and-versioning.md)'s field-id stability is
exactly what keeps a spec valid across column renames and reorders: the spec
names field 7, and field 7 remains field 7 whatever it is called or wherever it
sits. This is why keying by name would be wrong (see Alternatives).

Sort specs sit on the other side of that contrast: a sort key names its
column **inside a verbatim SQL expression string**, and no field-id
indirection can protect a string from a rename. DuckLake resolves that by
rewriting the expression: renaming a sorted column commits a new sort-spec
version carrying the new name, leaving the old text in `history` where it
belongs to the snapshots that were written under it. A stale live expression
therefore never arises.

Dropping a partitioned or sorted column is refused outright, by DuckLake's
own binder, before anything reaches the catalog — *"Cannot drop column … the
table is partitioned by this column"*, and the matching message for sorted.
Rename and type change are allowed. So moraine enforces none of this: it
stores the expression untouched, records the new sort-spec version a rename
produces, and never sees the drops that are refused upstream.

Whether a column that a live spec partitions or sorts on **may be dropped or
altered** is a DuckLake rule, not moraine's. moraine follows DuckLake
row-faithfully: it does not invent its own validation that could drift from
DuckLake's. The validation boundary sits in DuckLake's planner/executor, which
issues (or refuses to issue) the commit; moraine records whatever committed
state results. Where DuckLake relies on catalog constraints to enforce this,
those are the constraints RFC 0006 discusses enforcing at the catalog layer.

### Interaction with compaction (RFC 0008)

Compaction rewrites data files and must preserve or recompute the correct
partition values for the rewritten files; RFC 0008 owns that. moraine stores
whatever partition values the rewrite produces, embedded in the new `file`
records, and ends the compacted-away files' `current` versions into `history` as
usual. If moraine ever drives compaction planning, partition boundaries
constrain which files may merge — a merge must not cross partitions whose values
differ under the governing spec. That constraint is noted here and specified in
RFC 0008; this RFC only fixes that the partition state a rewrite needs is
already present in the per-table file scan. Sort specs impose no such
constraint: whether a rewrite re-sorts merged data against the live spec is
DuckLake's (or RFC 0008's) choice, and nothing in the catalog records the
outcome either way.

### Property-test and e2e obligations

Per RFC 0001, `store`-layer encoding ships with proptest coverage, and the
partitioning and sorting tables extend it:

- **Spec + values round-trip.** A partition spec (columns + transforms), a
  sort spec (expressions + dialects + directions + null orders), and a file's
  partition values encode and decode verbatim, byte-for-byte against what
  DuckLake wrote — transforms and expressions included, unparsed.
- **Evolution time-travels.** Set → change → clear a table's partitioning and
  sorting across snapshots; time travel to each snapshot reconstructs the
  spec-in-force, and every file still reports the partition spec it was
  written under and the values under it.
- **Access pattern captured in e2e.** DuckLake's own partition-pruning queries
  and `SET SORTED BY` / sorted-`INSERT` / `RESET SORTED BY` round trips are
  captured in the e2e suite, both to validate the mapping against real
  DuckLake SQL and to settle the file-partition-value placement (below) against
  observed reads.

## Alternatives considered

- **A separate `pval` subspace/kind for `file_partition_value`.** Rejected: the
  values have no independent lifecycle, and a separate kind would split "the
  files of table T and their partition values" into a join across two ranges.
  Embedding keeps them in one contiguous `table_id`-first scan (RFC 0002
  convention) — the shape the planner reads.
- **moraine evaluating transforms and pruning server-side now.** Rejected for
  the same reason RFC 0002 rejects server-side stats pruning: row-faithfulness,
  and correctness would demand full transform-awareness plus type-aware
  comparison. A wrong compare silently drops rows. Deferred, gated on e2e
  evidence.
- **Keying partition columns by name instead of field id.** Rejected: it breaks
  under column rename and reorder. Field-id references (RFC 0012) keep a spec
  valid across schema evolution.
- **A single global partition spec per table with no history.** Rejected: it
  defeats repartitioning and time travel. Files written under an earlier spec
  would have nowhere to point, and reconstructing a past catalog would report
  the wrong partitioning. The temporal `current`/`history` spec is what makes both
  correct.
- **Embedding the sort spec in the `table` value.** Rejected: `sort_info`
  carries its own `begin_snapshot`/`end_snapshot`, independent of the table
  row's — a `SET SORTED BY` must not force a new version of the table record,
  and time-traveling a sort spec must not entangle table history. An
  independent lifecycle earns its own kind (RFC 0002's rule).
- **A separate kind for `ducklake_sort_expression`.** Rejected for the same
  reason as a separate `pval` subspace: expressions have no independent
  lifecycle (no begin/end of their own — they live and die with their
  `sort_id`), and a second kind would split DuckLake's one spec-load join
  across two ranges. Embedding serves it from one contiguous scan.

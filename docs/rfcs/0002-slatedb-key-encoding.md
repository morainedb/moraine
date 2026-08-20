# RFC 0002: SlateDB key encoding for DuckLake catalog state

- **Date:** 2026-07-08

## Summary

Defines how DuckLake catalog state (spec v1.0) is laid out in SlateDB:
keyspace structure, key layout, and value codec. The layout serves the two
read patterns that matter: loading the **current** catalog (the hot path,
every attach) and reconstructing the catalog **at a past snapshot** (time
travel, rare). Inlined data (RFC 0005) gets a reserved subspace but is
otherwise out of scope.

## Goals

- Loading the current catalog costs proportional to the live catalog, never
  to its history.
- Time travel may scan an entity range's history, never unrelated tables'
  state.
- Every DuckLake v1.0 catalog table has a defined home in the keyspace.
- Values can evolve (add/deprecate fields) without rewriting the store.

Non-goals: the commit protocol (RFC 0004); inlined record formats
(RFC 0005); multi-writer coordination.

## Background

DuckLake versions catalog entities temporally: rows carry `begin_snapshot` /
`end_snapshot`, and the catalog at snapshot `S` is the rows with
`begin_snapshot <= S < end_snapshot` (live rows have no end). A relational
catalog answers "current" and "as of S" with the same filtered scan; a KV
layout must choose a physical split.

SlateDB (pinned 0.14.x) provides ordered keys, prefix/range scans, point
gets, atomic `WriteBatch` writes, pinned read-snapshots (`Db::snapshot()`),
and transactions with write-write conflict detection (`Db::begin`), under a
single fenced writer; readers attach via `DbReader`. RFC 0004 builds the
commit protocol on the transactions; RFC 0009 builds reader consistency on
the read-snapshots.

## Design

### Subspaces

The keyspace is partitioned by its leading byte — the subspace
discriminant — and that byte is registered with SlateDB: every store is
created with a **fixed-length one-byte segment extractor** (SlateDB
RFC-0024), making each subspace a SlateDB **segment** with its own LSM
state. `inline` churn — bulky row
data, the launch workload of RFC 0005 — therefore compacts independently
of the small metadata subspaces. Multi-subspace commit batches remain
atomic (the segment check precedes the single WAL append), and SlateDB
persists the extractor identity, refusing a mismatched open. Everything
below the leading byte is moraine convention, opaque to SlateDB.

| Subspace | Contents | Mutability |
|---|---|---|
| `system` | Format version, head pointer, catalog-level options | Overwritten in place |
| `snapshot` | One record per snapshot | Append-only, immutable |
| `current` | Live catalog entities (no `end_snapshot`) | Insert + delete |
| `history` | Ended entity versions | Append-only |
| `inline` | Inlined data — reserved for RFC 0005 | Per RFC 0005 |
| `index` | Equality-index entries (RFC 0016) | Live-only; insert + delete |
| `schema_version` | `ducklake_schema_versions` rows | Insert + delete, and never expired with the snapshot that wrote one |

(Subspace declaration order — and therefore each subspace's discriminant
byte — is fixed by the `Key` type and pinned by golden vectors; see Keys.)

The `current`/`history` split is the load-bearing decision. Loading the current
catalog is a scan of `current` only. Ending an entity version (drop, alter,
file compacted away) atomically deletes its `current` key and writes a `history`
key in the same commit batch. History accumulates in `history`, where the hot
path never looks; snapshot expiry (RFC 0007) garbage-collects it.
Reconstructing the catalog at snapshot `S` scans both and filters: from
`current`, keep `begin_snapshot <= S`; from `history`, keep
`begin_snapshot <= S < end_snapshot`.

### Keys

A key is a **typed tree** — subspace, kind, components — defined as
nested Rust enums, and its on-disk bytes are the derived order-preserving
encoding from the **`storekey`** crate (pinned 0.11.x; SlateDB's own
data-modeling guidance recommends it): one discriminant byte per enum
level, assigned by declaration order, then fixed-width big-endian `u64`
components in field order. The structure is the format — variant order is
permanent once written — and golden-vector tests pin the exact bytes per
kind, so any drift (a reordered variant, a changed derive, a `storekey`
bump that alters encoding) fails CI before it can reach a store.

- The **first byte is the subspace discriminant** — the invariant the
  segment extractor keys on.
- Scan bounds are derived, never hand-assembled: encode a sampled key of
  the target shape and truncate (a shorter path through the tree is a
  byte prefix of every key beneath it). Table-scoped bounds go through a
  typed constructor that only accepts the kinds whose first component is
  a table id.
- **No strings in keys.** Entities are keyed by their DuckLake-allocated
  ids — schemas/tables/views/macros from the global `next_catalog_id`,
  files and column mappings from `next_file_id` (RFC 0018), columns from a
  per-table counter (RFC 0012).
  Names live in values; name→id resolution runs against the in-memory
  snapshot built by scanning `current` at attach (a persistent name index is
  complexity without payoff at catalog scale).
- `history` keys append the version's `end_snapshot` as the final component,
  making ended versions of one entity distinct and time-ordered.
- SlateDB iterates **forward only**; the layout depends on no descending
  scan — `sys/head` is an explicit pointer (never a find-max-key scan),
  and every range read (`current` load, time travel, `history` dead-prefix
  expiry, `snapshot` refresh) is ascending by construction. New kinds must
  preserve this.

### Keyspace map

Kinds within `current`. Temporally versioned kinds are mirrored in `history`
with `end_snapshot` appended; the **statistics kinds (`fstat`, `tstat`,
`tcstat`) are unversioned** — DuckLake's stats tables carry no begin/end
columns, so these records are overwritten in place, never transition to
`history`, and a time-travel view serves the current stats (stats are
advisory pruning data, not catalog history). An `fstat` record outlives
its file's live version — historical snapshots still prune by it — and
is removed only when RFC 0007 GC prunes the file's history. The `mapping`
kind is a third shape, unversioned and **immutable create-only**: written
once (by `ducklake_add_data_files`), never overwritten, never mirrored,
removed only by RFC 0007 GC (RFC 0018). The `tag` kind is a fourth shape:
an unversioned container record whose embedded entries are individually
begin/end-versioned (see its row below):

| Kind | Key components | DuckLake table(s) |
|---|---|---|
| `schema` | `schema_id` | `ducklake_schema` |
| `table` | `table_id` | `ducklake_table` |
| `view` | `view_id` | `ducklake_view` |
| `macro` | `macro_id` | `ducklake_macro` (+ `ducklake_macro_impl`, `ducklake_macro_parameters` embedded — immutable child rows with no independent lifecycle, served back in `impl_id`/`column_id` order because DuckLake's `LIST()` reconstruction has no `ORDER BY`; RFC 0019) |
| `column` | `table_id, column_id` | `ducklake_column` (+ `ducklake_column_tag` embedded). One record per row, **nested fields included** — struct members / list elements / map key-value are their own rows with per-table field ids; `parent_column` lives in the value (RFC 0012). |
| `partition` | `table_id, partition_id` | `ducklake_partition_info` (+ `ducklake_partition_column` embedded) |
| `sort` | `table_id, sort_id` | `ducklake_sort_info` (+ `ducklake_sort_expression` embedded) |
| `file` | `table_id, data_file_id` | `ducklake_data_file` |
| `delfile` | `table_id, delete_file_id` | `ducklake_delete_file` |
| `mapping` | `table_id, mapping_id` | `ducklake_column_mapping` (+ `ducklake_name_mapping` embedded). Immutable create-only (see above); `mapping_id` is DuckLake-allocated from `next_file_id`, **not** `next_catalog_id` — the lazy mapping reader cursors on that id space (RFC 0018). |
| `fstat` | `table_id, data_file_id, column_id` | `ducklake_file_column_stats` (+ variant stats) |
| `tstat` | `table_id` | `ducklake_table_stats` |
| `tcstat` | `table_id, column_id` | `ducklake_table_column_stats` |
| `tag` | `object_id` | `ducklake_tag`, **one container record per tagged object** holding all of the object's tag rows as embedded entries (object ids are unique across entity types via the shared counter — no type discriminator needed). An object can carry many tags, and tag keys are strings that stay out of the keyspace, so the rows cannot each own a store key; they embed instead, exactly as `ducklake_column_tag` embeds in the column value. The container is **overwritten in place** (no `history` mirror), while each embedded entry carries its own `begin_snapshot`/`end_snapshot` verbatim from its `ducklake_tag` row — a tag change rewrites the container, ending or appending entries; time travel filters entries by begin/end at read; ended entries are pruned from the container when RFC 0007 GC passes the retention horizon. Row-faithfulness holds because entries are `ducklake_tag` rows, not a re-modeling. |
| `option` | `scope_kind, scope_id` | `ducklake_metadata` / `set_option` scopes. `scope_kind` ∈ {global = 0, schema = 1, table = 2}; global uses `scope_id` 0. One record per scope holding its options as a map (option *names* are strings and stay out of keys); set/unset rewrites the record. Options are **unversioned** — DuckLake's `set_option` writes outside the snapshot protocol, last-write-wins (RFC 0004) — so they never transition to `history`, and an options-only mutation doesn't advance head. |

Other subspaces:

| Subspace/kind | Key components | Contents |
|---|---|---|
| `sys/format` | — | Layout format version (this RFC = 1), moraine version that wrote it |
| `sys/head` | — | Latest committed `snapshot_id`, plus the count of batches that have landed. Every batch writes this record and increments the count — a maintenance batch that reuses the snapshot id included (RFC 0004) — so the pair moves whenever any committed state does. That is what makes it both the single write-write conflict anchor and the stamp a read-only reader validates a consistent cut against (RFC 0009) |
| `sys/migration` | — | Structural-migration marker (RFC 0015): `{from_format, to_format, cursor}`, present only mid-migration. **Reserved from format v1**: every materialization checks it and refuses a mid-migration store (RFC 0009) — the check must predate the first migration ever run. |
| `sys/maintenance_status` | — | The last 16 completed maintenance passes, oldest-to-newest, with each pass's start time, trigger, and step outcomes (RFC 0021). Unversioned and overwritten atomically outside the DuckLake snapshot protocol; its first write lazily stamps additive format 5. |
| `snapshot` | `snapshot_id` | `ducklake_snapshot` + `ducklake_snapshot_changes` merged into one record (1:1, always written together), plus one moraine-internal field the DuckLake grammar has no room for: the data files this commit's delete files target, so a later commit classifies a delete against it at file grain. Absent on a DuckLake-authored row, which reads back as "deletes from the whole table" |
| `changelog` | `snapshot_id` | The encoded `current` keys one commit's batch wrote — the changelog a reader replays to advance a held view across that commit (RFC 0009). Its own subspace rather than a field of the snapshot record because snapshot records are scanned in full on a read path DuckLake takes every transaction, and the changelog measures several times their size (`BENCHMARK.md`); only a refresh reads these, one point read per snapshot in its gap. Bounded by a sliding window: each commit deletes the record N snapshots back, so nothing else — not expiry, not a maintenance sweep — has to reclaim them |
| `current/gcfile` | `data_file_id` | `ducklake_files_scheduled_for_deletion` — keyed by the scheduled file's id, the row's identity in DuckLake's own schema (inserts carry it, cleanup deletes by it); unique because a file's catalog rows are removed in the same transaction that schedules it, so no moraine-allocated id or counter exists (RFC 0007) |
| `inline/*` | `table_id, schema_version, …` | Inlined data — RFC 0005 owns seven kinds (`schema`, `insert`, `inline_delete`, `file_delete`, the `file_delete_table` existence marker, the per-chunk `chunk_range` directory, and the `schema_dropped` deregistration marker). |
| `schema_version` | `table_id, begin_snapshot` | The catalog `schema_version` that snapshot minted — the third column of a `ducklake_schema_versions` row. Its own subspace rather than a field of the snapshot record, because expiry (RFC 0007) deletes snapshot records and these rows must outlive them: a data file resolves the schema version it was written under by joining its `begin_snapshot` against them, long after that snapshot is gone. Removed only by the dead-table cleanup, exactly as DuckLake's own catalogs remove them |

Two mapping conventions apply throughout: **1:1 side tables merge** into
their parent record, and **pure child tables with no independent lifecycle
embed** in the parent's value (partition columns, sort expressions, column
tags). A DuckLake
table earns its own kind only when its rows have an independent begin/end
lifecycle. With `macro` (RFC 0019) and `mapping` (RFC 0018) mapped, every
table of the v1.0 spec's ~28 has a home above — as a kind, an embedded
child, or a merged 1:1 side table; any future spec table follows the same
conventions — this RFC is updated, not diverged from.

Per-table collections (`column`, `file`, `fstat`, …) are keyed
`table_id`-first so "everything about table T" — the unit DuckLake reads
and invalidates — is one contiguous range per subspace.

Within `fstat` the remaining components are file-major: `data_file_id`
before `column_id`, so a file's columns sit together. Column-major would win
only for a read that wants one column across many files, and no such read
exists — DuckDB pushes no row filter into the served projections (RFC 0009)
and nothing inside moraine reads statistics selectively, so the only
operation on `fstat` is a full scan, costing the same either way. The
ordering reopens only with server-side pruning, on the terms RFC 0009 sets;
reversing it once the collection is large needs a migration, so that is the
moment to weigh it.

### Value codec

Values are protobuf messages (via `prost`; schemas compiled at build time
with `protox` feeding `prost-build`, so there is no system `protoc`
dependency), one message type per kind, behind a fixed 5-byte framing
header.

- **Framing header:** a 4-byte magic (`b"MRNE"`) and a 1-byte encoding
  version precede the payload. Corrupt, truncated, or wrong-kind values
  fail loudly as `Corruption` (RFC 0003) instead of decoding
  plausibly-wrong, and a reader that meets a newer encoding version errors
  rather than misreads.
- Explicit field tags give forward/backward compatibility (old readers
  skip unknown fields, new readers default missing ones), and the format
  stays language-neutral for external tooling.
- `sys/format` gates **structural** changes (subspace or key-structure
  changes bump it and require migration, RFC 0015); protobuf field
  evolution and the per-value encoding version do not.

Entity values carry `begin_snapshot` (in `history`, `end_snapshot` appears in
both key and value — values are self-contained). Timestamps are
microseconds since epoch, UTC.

**Statistics are stored verbatim, never interpreted.** DuckLake encodes
min/max stats as strings regardless of column type; moraine round-trips
them exactly (RFC 0006 row-faithfulness) — re-serializing through a typed
value is lossy and would corrupt pruning. DuckLake owns the comparison. If
moraine ever pruned server-side (no such verb today, RFC 0003), the
comparison would have to be **type-aware**, never lexicographic — for a
numeric column `'9'` is not `> '10'`, and a naive compare silently drops
correct rows.

### Atomicity invariant

One DuckLake catalog commit — snapshot record, head-pointer update, every
entity insert/end it implies — is **exactly one SlateDB `WriteBatch`**. No
commit spans batches or depends on read-modify-write across them. Batches
are atomic, so a crash leaves the whole commit or none; one commit ≈ one
durable WAL flush. RFC 0004 builds on this invariant; the layout
guarantees it is *possible* — every mutation a commit needs is puts and
deletes at statically computable keys.

### Property-test obligations

Per RFC 0001:

- `decode(encode(k)) == k` for every key kind, with golden vectors pinning
  the exact on-disk bytes per kind (so an encoding change in a `storekey`
  bump — or a reordered variant — fails CI instead of silently forking
  the format). Order preservation itself is `storekey`'s documented
  contract and is not re-proven here; the goldens pin our use of it.
- Value roundtrip (framing included) for every message type; framing
  rejection — corrupt magic, truncated header, unknown encoding version —
  fails as `Corruption`, never a partial decode.

## Alternatives considered

- **Single subspace, begin/end in values:** every current read filters
  full history — the hot path pays for time travel whether used or not,
  degrading without bound between expiries. Rejected.
- **Version-in-key (`id, begin_snapshot`), no `current`/`history` split:**
  append-only and elegant, but live-version reads become reverse scans and
  the live-catalog load still filters ended versions. Rejected; the split
  buys O(live) reads for one extra delete+put per ended version.
- **Names in keys:** renames become key churn across every child range.
  Rejected; attach-time maps suffice.
- **Postcard/bincode values:** positional — adding a field is a format
  break. Rejected for durable state.
- **Arrow IPC for entity values:** a columnar-batch model for
  record-at-a-time metadata; wrong shape.
- **SQL-engine pages over KV (SQLite/DuckDB file in SlateDB):** abandons
  control of the layout, the split, and single-batch atomicity. Worst of
  both worlds.
- **Hand-rolled key codec** (or hand-written `storekey` trait impls with
  explicit tag/kind byte constants): viable, and byte values would be
  explicit rather than declaration-derived — but it maintains a parallel
  encode/decode surface (~300 lines) whose only job is restating the type
  structure, and the golden vectors pin the bytes either way. Rejected in
  favor of deriving `Encode`/`Decode` on the key tree: the structure is
  the format, and the tests are the byte contract.
- **Unsegmented stores (SlateDB's default):** the longer-exercised
  crash/recovery path, but the extractor is fixed per store at creation,
  making format-v1 genesis the only free adoption moment — deferring would
  price the `inline`-isolation payoff at an RFC 0015 rebuild per store.
  The risk is tested instead: every store opens with the extractor
  configured, writer and reader alike, so RFC 0011's crash cases exercise
  the segmented path by construction — and the decision can still flip for
  free before first release.
  Prefix bloom filters (the other half of SlateDB's prefix machinery) stay
  unused — a catalog this small doesn't earn them; they can be enabled per
  open at any time.

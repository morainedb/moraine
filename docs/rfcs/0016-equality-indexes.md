# RFC 0016: Equality and range indexes

- **Date:** 2026-07-10 (reverse iteration settled 2026-08-08)

## Summary

Adds a moraine-native **equality index**: a catalog object (`create_index` /
`drop_index` on the [RFC 0003](0003-public-api-shape.md) verb surface) whose
entries live in a new `index` subspace and serve two reads — **row identity**
(key values → stable row ids) and **uniqueness
enforcement** at commit. DuckLake v1.0 models no indexes, so this is a native
feature: real inside moraine, invisible to every DuckLake catalog scan.
Entries are **live-only** (no temporal versioning) and point at **row ids**,
which DuckLake preserves across flush, update-rewrites, and compaction — data
movement never touches the index. Uniqueness rides SlateDB's write-write
conflict detection: a unique entry's store key *is* the key value, so racing
commits inserting the same value collide mechanically and the loser resolves
to a typed `Constraint` under [RFC 0004](0004-commit-protocol.md)'s ordinary
retry. Rows moraine holds are indexed automatically; externally written
Parquet is covered by **writer-supplied entries** at registration (embedding
API) or by moraine's **scoped read** of the registered file (extension path).
Builds too large for one commit run **staged**: the definition lands in a
`building` state, backfill streams in bounded batches while writers maintain
entries from day one, and a final commit flips the index `ready` (Staged
builds).

The index is **ordered**. The canonical encoding is order-preserving for
every type, so the same `index` storage answers comparison queries
(`<`, `<=`, `>`, `>=`, `BETWEEN`, half-open) as a bounded sub-scan —
equality is the degenerate closed `[v, v]` case. Each column carries a
declared **direction** (`ASC`/`DESC`, realized by complementing its framed
bytes, so an ascending store scan yields the declared order) and a **NULL
placement** (`NULLS FIRST`/`LAST`). A descending store scan serves the exact
opposite order from the same index. None of this adds a maintenance path:
coverage, staged builds, uniqueness, and the scoped read are unchanged — they
just call the ordered encoder (Range and comparison queries).

The store is one index; the ways in are two. The **embedding (verb) API**
creates and maintains indexes directly — the bulk of this RFC. The
**extension path** reaches the same `index` storage from DuckDB SQL through
registered moraine functions, and covers DuckLake-written Parquet by the
scoped read, which also enforces uniqueness over SQL writes.
DuckLake-native `CREATE INDEX` belongs to DuckLake, which owns the user-table
binder and refuses index DDL before moraine is consulted (Extension path).

## Goals

- An index covers **all live rows** of its table — inlined, flushed, and
  externally registered — or refuses the operation that would breach that;
  uniqueness that silently under-covers is worse than none.
- Index maintenance rides the commit it belongs to: entries land in the same
  single `WriteBatch` (RFC 0002 atomicity invariant), and a unique violation
  aborts the commit that caused it, never a later one.
- Flush, compaction, and update rewrites — every operation that moves a row
  without changing its identity — cost the index **zero writes**.
- Concurrent appends to one indexed table keep RFC 0004's append-append
  benignity unless they actually collide on a unique key value.
- Reads are consistent: a lookup and the snapshot it serves reflect one cut
  of the store (RFC 0009 pinned handles).
- Stores that never create an index are byte-identical to stores built
  before this RFC, and remain readable by older binaries.

Non-goals:

- **Serving DuckLake's planner transparently.** DuckLake owns the user-table
  binder and optimizer. The extension path integrates through an explicit
  surface — registered functions and the scoped read — never downstream
  `ducklake_*` changes or a planner fork. Requests for a supported upstream
  integration live in [`../ducklake.md`](../ducklake.md).
- **Locale/collation-aware order.** String order is DuckDB's default binary
  (bytewise) order; collation-sensitive ordering is out of scope.
- **Approximate structures** (bloom filters, zone maps) — they cannot carry
  uniqueness; RFC 0013's pruning stance already covers the skipping use case.

## Background

DuckLake v1.0 has no index tables, so an index cannot be smuggled in as
another row-faithful `ducklake_*` mapping — it is moraine inventing a
capability, and the honest shape for that is a native feature stored where no
catalog scan (current or time-traveling) can see it.

Three established facts carry most of this design:

- **Row ids are stable identity.** DuckLake allocates row ids per table
  (`next_row_id` in `tstat`, RFC 0004) and preserves them across inline
  flush, UPDATE's delete-and-rewrite, and compaction — row lineage. Ordinary
  files derive dense ids from `row_id_start`; rewrite and flush files may
  instead carry arbitrary preserved ids in `_ducklake_internal_row_id`.
  The row id is therefore the durable join key, while physical placement and
  delete application remain DuckLake's responsibility.
- **moraine does not read Parquet on the scan path** (RFC 0006 non-goal) —
  merge-on-read, lineage, and pushdown are DuckLake's. On the embedding API
  it sees row contents at exactly two moments: `inline_insert` and flush
  (moraine writes that Parquet itself); entries for external rows are
  writer-supplied, like file column stats. The extension path adds one
  bounded exception: projecting the indexed columns of a freshly registered
  file (Extension path) — a raw-value projection with no merge, not the scan
  path the non-goal guards.
- **Write-write conflict detection is the store's only race primitive**
  (RFC 0004): no read-write detection, no key CAS. A design that needs
  "detect that someone else inserted this value" must arrange for the race
  to be a write-write collision on one key.

## Design

### The index definition — a catalog entity

A new `index` kind in `current`/`history`, keyed `(table_id, index_id)` with
`index_id` allocated from the global `next_catalog_id`. The value carries the
index name, the **ordered list of indexed columns referenced by field id**
(never by name — the RFC 0012/0013 rule, so renames are free), the unique
flag, and `begin_snapshot`. Staged builds add a state field and a build
cursor to the same value (Staged builds); a definition without a state field
is `ready`, so single-commit creates are unchanged on disk. The *definition*
is temporally versioned like any entity — time travel reconstructs which
indexes existed at snapshot `S` — even though entries (below) are not.

Verbs on `Transaction`:

| Verb | Effect |
|---|---|
| `create_index(table, name, columns, unique)` | Insert the definition; build entries for every live row (see Coverage) in the same commit. |
| `drop_index(index)` | End the definition into `history`. Entries are orphaned and reclaimed lazily (see Reclamation). |

`CatalogSnapshot` gains `indexes_of(table)`; domain types gain `IndexId` and
`IndexInfo`.

**Commit classification.** Index DDL is recorded in `changes_made` as
`altered_table:<table_id>` — DuckLake's parser throws on unknown kinds
(RFC 0004), so the entry must use vocabulary it parses. `altered_table` is
also *correct*, not just parseable: an alter truly conflicts with concurrent
inserts, deletes, and other alters, in both directions — exactly what index
DDL needs. A `create_index` racing a `register_data_file` must not win
mechanically (its backfill was staged against the old file set), so the race
aborts as `CommitConflict` and the caller re-drives with fresh backfill. For
coherence with DuckLake's own classification (every alter bumps), index DDL
sets `schema_changed`; the cost is one spurious schema-cache refresh per
index DDL.

**Column DDL on indexed columns.** `drop_column` and `alter_column` (type
change) on a column a live index references fail with `Constraint` — the
canonical encoding is type-bound, and a silent cascade would discard an
object the host created deliberately. Drop the index first. `rename_column`
is unaffected (field-id references).

### The `index` subspace and entry keys

A new subspace, one leading discriminant byte appended to the `Key` enum —
a SlateDB segment of its own (RFC 0002), so entry churn from a hot indexed
table compacts independently of the metadata subspaces, the same isolation
`inline` gets. Golden vectors pin the discriminant like every other kind.

| Kind | Key components | Value |
|---|---|---|
| `index/unique` | `index_id, canon(key values)` | `row_id` |
| `index/multi` | `index_id, canon(key values), row_id` | (empty) |

Two shapes, one deliberate asymmetry:

- **Unique entries key on the value alone.** The store key *is* the claim of
  uniqueness. Two commits inserting the same value — the race no read-side
  check can close, since SlateDB detects write-write conflicts only — write
  the *same key* and collide in the store's own conflict detection. The loser
  retries per RFC 0004, re-runs its closure, sees the winner's entry, and
  returns `Constraint`. Uniqueness under concurrency is correct by
  construction, with zero new race machinery.
- **Non-unique entries append the row id**, so rows sharing a value occupy
  distinct keys: concurrent appends of different rows write disjoint entry
  keys and stay **benign** under the append-append refinement — indexing a
  table does not serialize its writers. A lookup for `v` is the ascending
  prefix scan `index/multi/{index_id}/{canon(v)}/` (RFC 0002 forward-only).

`index_id`-first keying makes each index one contiguous range — the unit
lookups scan and drop orphans (Reclamation).

### Canonical value encoding

Entry keys embed column values — a deliberate, bounded exception to
RFC 0002's "no strings in keys" rule, which exists so *entity* keys stay
rename-stable; an index key's whole job is to be the value. The contract is
**canonical *and* order-preserving bytes**, per DuckLake column type
(`encode(x) < encode(y)` as unsigned bytes iff `x < y` in the type's SQL
order), pinned by golden vectors and proptest roundtrips like every other
store codec:

- Integers: fixed-width big-endian, sign bit flipped; widths normalized per
  the column's type. Order-preserving.
- Strings/blobs: the UTF-8/raw bytes, `storekey`-escaped so component
  boundaries stay unambiguous in composite keys; bytewise order is DuckDB's
  default binary order.
- Floats: `-0.0` normalized to `+0.0` and every NaN collapsed to one pattern,
  then the standard total-order transform (flip the sign bit of a
  non-negative value, all bits of a negative one) so the big-endian bytes
  sort numerically, with NaN greatest.
- Temporal types: their underlying integer representation.
- Composite indexes concatenate component encodings in declared column
  order; `storekey` tuple framing keeps `("ab","c")` distinct from
  `("a","bc")` **and** preserves order (a shorter value sorts before its
  extension).

**Direction** is per column. A `DESC` column stores the bitwise complement
of its fully framed component (terminator included, so variable-length
values reverse correctly — `"ab" < "a"`); the leading NULL flag is not
complemented, so NULL placement is independent of direction. An ascending
scan yields the declared composite order and a descending scan yields its
exact opposite, both directly from SlateDB's iterator.

**NULL placement** is per column (`NULLS FIRST`/`LAST`). Each column's
component carries a leading flag byte separating NULL from non-null, ordered
per the placement. A row with a NULL in an indexed column **is stored** — so
`IS NULL` can find it (Range and comparison queries) — but always
**multi-shaped** (the row id is in the key) and **exempt from the value
collision**: SQL treats NULLs as distinct, so a unique index still admits any
number of NULL rows. An equality *point* lookup on NULL still has no answer
(`= NULL` is unknown); NULL rows are reached only through `IS NULL`.

The explicit read surface deliberately keeps comparison and NULL scans
separate. `index_range` clamps open sides to the non-null region, while
`index_nulls` emits only the matching NULL subrange in either scan direction.
It does not combine the two into an `ORDER BY … NULLS FIRST/LAST` result;
that behavior belongs to any future DuckLake planner integration, not this
API's contract.

**Oversized values are refused.** Indexed values beyond a fixed cap fail
with `Constraint` at insert/registration — huge keys degrade the whole
segment, and equality over megabyte values is not this feature's job.
Hash-overflow is rejected for v1.

### Coverage — who writes entries, and when

Coverage is total over live rows, maintained at the three moments rows
enter, and the two moments they die:

| Moment | Who has the rows | Entry source |
|---|---|---|
| `inline_insert` | moraine (rows arrive through the API) | computed by moraine, staged in the same batch |
| flush (RFC 0005) | moraine (it writes the Parquet) | **nothing to do** — entries point at row ids, which flush preserves |
| `register_data_file` | the writer | **writer-supplied**: `(row ordinal, key values)` pairs alongside the file, mapped to row ids at commit (`row_id_start + ordinal`) — or `(row_id, key values)` for rewrite files that preserve row ids — like the file column stats the writer already computes |
| `inline_delete` of a store-resident row | moraine (the chunk is in the store) | moraine recomputes the key values from the chunk and stages the entry delete |
| `register_delete_file`, or `inline_delete` against flushed rows | the writer (it read the rows to produce positions) | **writer-supplied**: `(row_id, key values)` per deleted row; moraine deletes the named entries |

The embedding-path rule generalizing the last two: any operation that kills
rows moraine cannot read from the store must name their indexed key values —
entries are keyed by value, and moraine cannot derive a value from a row id
it cannot dereference. (The extension path lifts that premise by reading the
file.)

**Registration without entries is refused.** A `register_data_file` or
`register_delete_file` on an indexed table that omits the required entries
fails with `Constraint` — a silently under-covered unique index is a lie
(mark-stale is recorded in Alternatives).

**DuckLake registrations read instead of refusing.** The refusal above is the
embedding-API contract: the writer computes entries as it computes stats. A
DuckLake staged registration carries no entries by construction, so on any
indexed table moraine derives them by the scoped read (Extension path) —
however the index was created. Reject-vs-read splits by who writes, not by
which API made the index.

An UPDATE on the verb path (expire + register preserving row ids) composes:
the delete side removes old entries, the insert side adds new — same commit,
same batch, uniqueness checked against the post-image.

**Backfill at `create_index`.** Rows moraine holds (inlined, any schema
version) are backfilled by scanning the chunks. Rows in external files need
writer-supplied backfill entries passed to `create_index` — except through
`moraine_create_index`, which has no writer and backfills by scoped-reading
**every live data file** of the table, so its build cost is the table's
indexed columns, and the one-commit bound bites hardest exactly there. The
reader overlaps a fixed number of immutable files and sends bounded Arrow
batches through a bounded channel, so it retains no complete per-file result
sets. The transaction still owns the table-wide encoded entry set: the whole
build is one commit and one atomic batch, uniqueness is validated over that
assembled set before staging, and a duplicate aborts the create. A
backfill that exceeds the store's batch bound fails typed before staging
anything, and the caller re-drives as a staged build (Staged builds).

### Uniqueness enforcement

At commit, for each new unique entry the committer point-gets the entry key
through the transaction handle (RFC 0004 step 1's read discipline): present
with a **different** row id → `Constraint`; present with the **same** row id
→ no-op (a re-derived entry for a rewrite file, Extension path); absent →
stage the put. Duplicates *within* the commit are checked in memory. The race window between the get and a concurrent commit's
identical put is closed by the key collision described above — that is the
load-bearing property of keying unique entries on the value alone.

Because entries are live-only, "present" means "a live row holds this
value": deletes remove entries in the same batch that kills the row, so
delete-then-reinsert behaves as SQL expects, within one commit or across
commits.

The `Constraint` here is a verb-path error (embedding API) — no DuckLake
wire contract applies to its text. It is nonetheless worded free of the four
substrings DuckLake's commit loop retries on, so a rejected bulk INSERT
surfaces at once rather than being re-run (RFC 0006's wire contract).

**How the probes run.** A bulk load stages one entry per indexed row, so
this step decides whether a large commit is minutes or hours, and whether it
fits in memory at all. Three properties are load-bearing:

- **One probe per distinct key, not per entry.** Repeats within a batch
  collapse in memory; two entries claiming one value for different rows
  collide there, before any read.
- **Bounded concurrency, in bounded groups, at every batch size.** Probes
  are independent point reads, so serializing them makes a batch cost one
  store round-trip of latency *per entry*. They run with a bounded fan-out,
  resolved a group at a time — peak memory is one group of keys, not the
  batch's, whatever the batch size. Results are applied in batch order, so
  which entry a rejection names does not depend on which probe finished
  first. A bulk load probes nearly every key it stages, but each probe is a
  bloom-filtered point read whose cost does not grow with the index, and the
  bounded fan-out overlaps the object store's per-read latency — so the batch
  stays linear in rows without a second resolution mode. A sorted range scan
  would be worse for the common case: a bulk load's indexed values are not
  store-ordered, so one scan sweeps the whole index serially where the
  concurrent point reads touch only the blocks they need, in parallel.
- **Entries stage onto the transaction directly.** They never enter the
  write list the committer retains for the maintained projections. No
  projection reflects an index entry, so retaining them would hold a second
  copy of the batch's largest part — plus the clone staging makes of it —
  in memory for nothing.

### Commit size is bounded, and why it must be

Heap profiling of a staging commit attributes roughly **a kilobyte of peak
memory to every staged entry**, and almost none of it to moraine: the write
batch, the WAL buffer's copy of it, the memtable's skiplist node, and the
transaction's write-key set each hold their own copy or node. Removing
moraine's own retained copy (above) accounted for about 8% of the total;
after that there is nothing material left to win on this side. The cost is
inherent to putting a key into a batch.

A commit's footprint is therefore set by how many entries it stages, and a
bulk load stages one per indexed row per index — memory proportional to the
whole table. Tens of millions of rows ask for tens of gigabytes, and the
failure mode is not an error but a thrash: swapping, no progress, nothing
logged. So `stage_index_entries` refuses a commit above
`MAX_INDEX_ENTRIES_PER_COMMIT` before doing any work, with a message naming
the count, the limit, the memory it would have needed, and the remedy —
split the load. The limit admits commits needing about 8 GiB.

The refusal's text avoids DuckLake's four retry substrings, as the
uniqueness rejection does: it is terminal, and re-running it reaches the
same answer more slowly.

The limit is a fixed safety invariant, not a `CatalogOptions` field. A caller
cannot raise it and turn a bounded commit back into memory thrash; workloads
above it must split the load or use a staged build. Changing the limit or
making it configurable requires new measurements and a new design decision.

The remedy the refusal names — split the load — is only available to a
writer that chooses its own batch boundaries. A DuckLake maintenance call
does not: `ducklake_merge_adjacent_files` decides for itself how many files
one snapshot merges. A commit that derived an entry per merged row would
therefore hit an unsplittable refusal, and an indexed table past a few
million rows could never be compacted again. It derives none (Compaction
derives nothing), so the limit binds only writers who can obey it.

### Two bounds on a step

Entry count is not the only thing that can make a commit impossible, and
the second bound is not a smaller version of the first — they fail
differently and are set by different facts about the machine.

The memory bound above is about the process: a kilobyte of write-path
memory per staged entry, so a big enough batch thrashes. The **transfer**
bound is about the link. A durable commit becomes exactly one object-store
request: the store guarantees a write batch lands in a single WAL object,
and WAL objects are written with a conditional single PUT — the fencing
they rely on cannot be expressed through the multipart API — so the whole
batch goes in one request body. `object_store` applies its request timeout
(30 seconds by default) per attempt to that entire request, upload
included. A batch too large to push inside the timeout therefore does not
fail: it times out, is retried from the first byte, and is retried beneath
moraine indefinitely. It never lands and never errors.

So a staged build step carries two bounds, and ends at whichever it reaches
first:

- `entries` — the memory bound, defaulting to a million.
- `bytes` — the transfer bound, defaulting to 8 MiB. That clears the
  30-second default at a little over 270 KiB/s, which is slower than any
  link a build has a right to expect.

A step always carries at least one entry: an entry wider than the byte
bound still has to be committed, and a step that admitted nothing would
never advance.

At the defaults the byte bound always binds first, and the entry bound
never does: an entry costs at least 19 bytes (the entry prefix, a one-byte
NULL flag, and the row id), so 8 MiB admits at most about 440,000 of them.
The entry bound is therefore not a second safety net at rest — it is what
holds when `bytes` is *raised*. An operator on a fast in-region link who
lifts the byte bound to cut commit count is exactly who needs a step still
capped at a million entries, because at 256 MiB the byte bound alone would
admit fourteen million and the memory limit above is real.

The byte figure is **nominal** — summed before the keys are encoded, since
the step boundary has to be chosen first. Framing escapes `0x00` and
`0x01`, which at worst doubles a value, so the committed batch can be up to
twice the bound. The margin against the timeout absorbs that; a property
test pins the bracket.

Both are settable per call, because neither default can know the link. The
transfer bound is the one an operator on a slow or distant link reaches
for, and lowering it is strictly a latency trade: more commits, each
cheaper and independently resumable from the cursor.

The commit stall warning reports the batch's staged bytes for the same
reason. A stalled durable write carries no error — the failure is retried
below moraine, so there is nothing to report but the wait — and the batch's
size is the one fact that separates a batch that cannot be transferred from
credentials that will not authorize it.

### Deferred upkeep for non-unique indexes

Synchronous upkeep remains the default: the data snapshot and every index
entry land atomically. An index created with `maintenance := 'deferred'`
instead moves **additions** out of DuckLake's data commit. This mode is
limited to non-unique indexes: unique enforcement cannot acknowledge a row
before its value has won the index collision.

When a SQL commit adds rows covered by a deferred index, the same snapshot
that registers the rows flips that index from `ready` to `maintaining` and
persists the repair cursor. It stages no additions for that index. Deletions
remain synchronous in every state, so no stale entry survives while a repair
is pending. An UPDATE therefore removes the old value in its data commit and
leaves the new value for the repair.

`maintaining` is unavailable, never partially readable: every lookup fails
with the same typed building error used by an initial staged backfill. After
the data snapshot is durable, the extension drives the ordinary bounded
backfill machinery over the missing live rows and flips the index back to
`ready`. Repair failure cannot roll back a data snapshot that already landed;
it is logged and leaves the durable `maintaining` marker for the next SQL
commit or moraine maintenance pass to resume. Process loss has the same
outcome. The maintenance scheduler repairs maintaining indexes before
sweeping dropped ones.

Further writers arriving while a repair runs continue to defer additions and
maintain deletions. Intermediate repair steps classify
`inserted_into_table`, so they can advance beside concurrent appends while
retaining conflicts with deletes, schema alters, and drops. The final `ready`
flip uses a non-schema-changing `altered_table` operation, so a racing writer
makes the repair re-derive from its durable source cursor rather than publish
incomplete coverage. Both operations mint snapshots for durability but add no
`ducklake_schema_versions` rows. Only an index's initial publication and first
build-to-ready flip are schema changes; routine upkeep cannot churn table
schema history.

### What data movement costs the index: nothing

The entry payload is a row id, not a location. Flush re-homes rows from
chunks to Parquet; compaction and UPDATE rewrites re-home them across files;
row ids survive all three. Moraine returns that identity directly. DuckLake
uses its own file statistics and delete metadata to locate and adjudicate the
row under the scan's snapshot. No maintenance operation rewrites an entry.
This is why live-only entries plus row-id payloads is the whole design: every
alternative payload (file id, chunk key) turns flush and compaction into index
rewrites proportional to moved rows.

Nothing removes a live index's entries when a data file's *row* leaves the
catalog, either — the maintenance sweep reclaims only the entries of
indexes that are themselves dropped. What keeps that sound is an invariant
worth stating, because it is the thing a future data-file lifecycle could
break: **a file's rows never leave without either a row-grain deletion
having already taken their entries, or a replacement carrying them under
their preserved row ids.** A rewrite materializes deletes that each removed
their entries when their delete file was registered; its survivors keep the
entries they already had, untouched, because an entry names a row and not
the file holding it (Compaction derives nothing). A merge subsumes its
sources. Expiry and cleanup prune rows a replacement already covers. The
whole sequence is held to the table's own answer by the e2e test
`moraine_index_entries_survive_the_data_file_lifecycle`.

A leaked entry would not be silent: the lookup table function exposes the
stale row id directly and, under a unique index, the entry causes a bogus
duplicate rejection when its value is claimed again. The ordinary DuckLake
join still filters that identity when no live row carries it.

### Compaction derives nothing

That an entry *survives* compaction untouched is the payload argument
above. The commit path must also decline to re-derive it. A registration
on an indexed table is otherwise read and derived unconditionally — the
property that keeps coverage total — but under compaction that read costs
one scoped read per merged file and one staged entry per row, to write
back the index that is already there. At scale it is not merely wasted
work: it is more entries than a commit may stage at all (Commit size is
bounded), which would leave a large indexed table permanently
uncompactable.

**The signal is the commit's own change set.** DuckLake refuses to mix
compaction with any other change in one transaction — a transaction either
makes changes or compacts, enforced upstream where `changes_made` is built
— so a snapshot naming `merge_adjacent` or `rewrite_delete` and nothing
else re-homes rows and does nothing else to them: no ids allocated (RFC
0008), no values changed, no rows killed. Every entry its files would
derive is already stored under the same key, so those files are not read
at all.

The rule is read off the staged `ducklake_snapshot_changes` row, so what
it trusts is the commit's own account of what it did — the same account
the conflict matrix already decides races on (RFC 0004). The fallback is
deliberately one-sided: a change set naming compaction *and* anything else
— an append, a delete, a kind this binary does not model — names no
compaction-only table, and every registration in it derives as before.
Only an account claiming compaction and nothing whatever else skips; every
less certain reading pays for the read.

**What still derives.** UPDATE also writes row-id-preserving files, but its
rows' values change; its commits carry `inserted_into_table` /
`deleted_from_table`, never a compaction kind, so they derive. Inline flush
likewise — `inline_flush` is a kind moraine does not model, so it can never
read as compaction-only — and its output re-derives entries idempotently,
bounded by how much data may sit inlined.

### Lookups

One accessor family on `CatalogSnapshot`, served under the snapshot's pinned
read handle so the lookup and the catalog it points into are one consistent
cut:

- `index_lookup(table, index, key_values) -> Vec<u64>` — point-get
  (unique) or prefix scan (non-unique) in `index`. Consumers join the returned
  row ids to DuckLake and let it select files and apply deletes. The extension
  path surfaces this accessor as `moraine_index_lookup`.
- `index_lookup_many(table, index, keys) -> Vec<u64>` — the `IN`
  accessor: deduplicate complete equality keys, resolve every distinct key
  under the same pinned read as one logical lookup, and return the union of
  their row ids. An empty key set returns no rows after validating the
  index. The probes run through a continuously refilled bounded window of
  512 futures, matching uniqueness enforcement: completion frees one slot
  immediately rather than waiting for a fixed chunk's slowest read. The
  extension path surfaces this accessor as `moraine_index_in`.
- `index_range(table, index, lower, upper) -> Vec<u64>` — the
  comparison accessor (Range and comparison queries). Each bound is
  `Included`/`Excluded`/`Unbounded`; results come back in the index's stored
  order. The extension path surfaces it as `moraine_index_range`.
- `index_nulls(table, index, prefix) -> Vec<u64>` — the `IS NULL`
  accessor. `prefix` is a leading run of `Some(value)` (equality) and `None`
  (`IS NULL`) predicates — `[None]` is `a IS NULL`, `[Some(5), None]` is
  `a = 5 AND b IS NULL`. At least one `None` is required; a gap (an
  unconstrained leading column, so a bare non-leading `IS NULL`) is not
  expressible and is left to a scan filter. Surfaced as `moraine_index_nulls`.

Every equality lookup emits one `index lookup resolved` diagnostic event.
Durations are integer milliseconds: total lookup, head view, probe wall
window, and summed probe service. Counts include requested and deduplicated
keys, hits, misses, peak in flight, metadata/block cache deltas, cache errors,
and object-store GET count, duration, and errors. The event measures the
whole pinned lookup without changing the SQL result surface.

Lookups, ranges, and null queries are **head-only**: entries are live-only, so
`snapshot_at(S)` fails with a typed error and time travel falls back to what
it always was — a scan problem. The hot path (current head) gets the index;
the rare path pays nothing to keep it honest.

### File-located lookups

A lookup resolves an indexed value to stable row ids as above. It may then
locate those rows in the current physical data files, so a DuckLake scan
prunes by file id as well as row id.

Physical location is **derived cache state**. It is not stored in the
equality entry, takes no part in the commit protocol, and adds no `index`
subspace keys — compaction and file rewrites leave every entry untouched.
That is the same property as [What data movement costs the index:
nothing](#what-data-movement-costs-the-index-nothing), extended to
location: the index knows row ids, and where a row id currently lives is
recomputable from the head view at any time.

The core returns a located row:

```text
FileRowCandidate {
    row_id: u64,
    data_file_id: Option<DataFileId>,
}
```

`ReadOnlyCatalog::locate_row_ids` takes the `DATA_PATH` `DataStore`, its path
prefix, a table id, and stable row ids, resolving every row against the same
head view that enumerates current data files. More than one file may be a
candidate for one row id: an update can leave an expired physical copy in an
otherwise-current source file while the visible copy is written elsewhere.

`None` names a live inlined row. A row id in neither a current data file nor
current inline data is also returned with `None` — a conservative fallback,
so incomplete cache construction or catalog state cannot hide a row.

The extension surfaces two columns, `row_id BIGINT` and
`data_file_id UBIGINT`, the latter nullable for inlined or unlocated rows.
A consumer joins with null-safe equality on file id and ordinary equality on
row id; a caller selecting only `row_id` is unaffected.

A tuple comparison — `(rowid, data_file_id) IN (SELECT row_id, data_file_id
…)` — is **unsafe** against these columns: tuple `IN` compares with plain
equality, under which a NULL file id matches nothing, so every inlined or
unlocated row silently drops out of the result. As a DELETE or UPDATE
predicate that strands the row with no error. The join with null-safe
equality on file id is the supported shape.

### File-row sets

One immutable summary describes the physical row ids of one immutable data
file, in whichever representation is smallest for that file:

- a half-open dense range, for a file carrying `row_id_start` and no
  reserved row-id column;
- a run-optimized 64-bit Roaring set, when it beats the raw ids;
- a sorted `u64` vector, when fragmentation makes Roaring larger.

A recorded `row_id_start` does not by itself imply the range: a flushed file
carries both a dense start and the reserved column, and that column's ids may
hold gaps a range would invent rows across and, worse, end before — excluding
a row the file holds. Which representation applies is therefore a question
about the file's schema, settled by its footer, and only then does a dense
file skip reading any column. A miss on any other file reads only the
reserved internal-row-id column, and reads of separate immutable files
overlap under the bounded concurrency index backfill already uses.

Summaries are built lazily, by the lookup that first needs one. Nothing
constructs them at write time, and nothing could cheaply: for a file the
catalog numbers itself the summary *is* the catalog record, and for one
carrying embedded ids — a compaction output or a rewrite — moraine holds only
the file's metadata, never its rows, so building at registration would mean
reading back the Parquet it was just told about, inside the commit path. A
summary is also process-local, and the writer is not the process that reads.

`ReadOnlyCatalog::warm_row_summaries` therefore exists to move that cost off
the first lookup rather than into the commit: a caller spawns it after a
commit that lands compaction outputs, and it builds exactly the summaries a
cold lookup would. It is best-effort and idempotent — a resident or dense
file costs nothing, an unreadable one is counted rather than raised — and
warms one process, so a separate reader still builds its own. A pass that
never runs changes latency and nothing else.

The summary cache is keyed by store identity, table id, data file id, path,
and recorded file size. The store is held weakly, which both keeps the
process-wide cache from pinning a catalog alive and separates two catalogs
whose table and file ids coincide. Catalog ids are never reused; path and
size make stale reuse fail
closed even against an imported catalog that violates that. Decoded sparse
summaries are byte-budgeted (RFC 0009); range summaries are cheap enough to
rederive and take none of the budget.

A batch lookup sorts and deduplicates its row ids once, then intersects each
file summary against that set — file-grouped candidates without a per-row
location map.

### Locating cannot lose a row

Equality entries remain the sole source of indexed values and uniqueness, so
the location cache can neither create nor remove a result. Every degraded
path *broadens* the candidate file set: a missing, evicted, unsupported, or
failed summary leaves every requested row id a candidate for that file. The
optimization may read files it did not have to; it cannot exclude a matching
row. Choosing the visible physical copy of a stable row id remains DuckLake's
delete and snapshot processing.

Locating is head-only, exactly as equality lookup is: a time-travel scan
consumes neither live-only entries nor this cache.

### Locating across the DuckLake file list

The companion DuckLake patch exposes `data_file_id` as an internal virtual
`UBIGINT` column. A physical file emits its persistent catalog id as a
constant; inlined and transaction-local sources emit NULL.

Static and dynamic filters on that column are pushed into DuckLake's
metadata file-list query as predicates on `ducklake_data_file.data_file_id`,
using no column statistics; row-id filters continue through the existing
reserved-field statistics path. A predicate naming both columns therefore
restricts the file list and the rows read within the files that survive.

A join condition does not reach that far by itself. A hash join's runtime
filters are generated after DuckLake has built its file list, and DuckDB
generates none at all for the null-safe equality the file-id column requires.
Left alone, a located join reads every file whose row-id statistics admit the
key — which, because each update writes a file spanning the ids it preserved,
is every update the table has taken.

An extension optimizer rule closes that gap. An index read resolves its rows
while binding, so a join against one is a join against a list of constants
already visible to the planner; the rule restates that list as an `IN` filter
on the other side of an inner or semi join. The filter only repeats what the
join enforces, so it is added beside the join rather than replacing it:
whatever the query projects from either side is unaffected, and an outer
join — which keeps rows meeting no condition — is left alone. A file id
resolved as NULL contributes an `IS NULL` disjunct under null-safe equality
and nothing under plain equality, matching what each comparison would have
accepted.

Each derived list is bounded, and the bounds differ by what evaluating the
list costs. The row-id list is checked against every row, so past a few
hundred distinct ids the lookup is a scan in disguise and the rule declines —
the dynamic row-id filter still covers that side. The file-id list dedups to
the files holding the rows and is checked against each file's constant id,
not against every row, so it stays worthwhile far wider — its much larger
bound only keeps a pathological plan bounded. A wide probe over a churned
table is exactly the case that needs this: near one row per rewrite file,
the distinct file ids grow with the probe, and the file-id list is the only
filter that names files exactly rather than testing row-id spans.

The rule runs after DuckDB's own optimizers, because that is when every
qualifying join exists: a `SELECT` writes its join directly, but `DELETE …
USING` and `UPDATE … FROM` bind theirs through the WHERE clause as a filter
over a cross product, and an `EXISTS` probe as a dependent join — both formed
into comparison joins only during optimization, the latter flattened to a
semi join over a projection of the lookup that the rule looks through. The
built-in filter pushdown has already run by then, so the rule pushes the
derived filter down its own subtree, landing it in the scan as table filters.
A semi join is also the shape that most needs it: DuckDB generates no runtime
join filters for one, so the derived filter is the only pruning an `EXISTS`
probe gets, and only within the bounded list length.

The current DuckLake catalog view stays authoritative for file lifetime.
Ended compaction inputs are simply absent, new outputs are cache misses, and
nothing pairs sources with outputs or hooks catalog changes.

### Range and comparison queries

Because the canonical encoding is order-preserving (Canonical value
encoding), byte order *is* value order, so both entry kinds — `index/unique`
(one value per key) and `index/multi` (value then row id) — are already single
contiguous value-ordered ranges. A comparison query is therefore a bounded
`ByteRangeBounds` sub-scan of the same range an equality lookup keys into.
There is **no new index kind, no new discriminant, and no new maintenance
surface**; the whole feature is the ordered encoding plus the accessor.

- **Equality stays a point-get.** `=` is a degenerate closed `[v, v]` range,
  but a unique lookup remains one `get` and a non-unique one a value-prefix
  scan; the model unifies the surface without making the point case pay a
  range's cost.
- **The definition carries per-column direction and NULL placement.** The
  definition value grows a direction (`ASC`/`DESC`, default `ASC`) and a NULL
  placement (`FIRST`/`LAST`, default `LAST`) per column, recorded only when
  they diverge from the default so an ascending index is byte-unchanged on
  disk. `create_index` gains an ordered variant; the rest of the verb surface
  is untouched.
- **Direction orients the scan.** A `DESC` column's byte order reverses value
  order, so `index_range` maps a value-lower bound to the byte-upper bound
  (and vice versa) for the range column — a single-column `DESC` index scans
  high value first, and half-open bounds map correctly.
- **The B-tree access rule.** A range query names an equality prefix over the
  leading columns plus one range on the next column (`a = 5 AND b > 10`); a
  bare range on a non-leading column cannot use the index and is refused,
  never scanned wrong. `BETWEEN` is a closed lower and upper; the strict
  comparisons are half-open; `Unbounded` is an open side.
- **`IS NULL` is a prefix scan of the stored NULL rows.** Comparison
  predicates never match a NULL (SQL: `NULL > 5` is unknown), so range results
  exclude NULLs by construction. `IS NULL` is served separately: `= v` and
  `IS NULL` are both *exact* prefixes (a fixed value blob, or a fixed null
  flag), so a leading run of them is one bounded scan. Because a NULL-bearing
  row is stored multi-shaped (Canonical value encoding), `index_nulls` scans
  the `multi` subrange on the encoded prefix, with the value framing's
  terminator dropped so it matches every key that extends the run. A gap
  (unconstrained leading column) is not a prefix and is not expressible.

Everything else — coverage, uniqueness enforcement, the value-keyed
collision, staged builds, the scoped read, rewrite-file resolution — is
unchanged, calling the ordered encoder. Storing NULL rows is the one coverage
change: a NULL-bearing row is now maintained as a collision-exempt multi
entry, so `IS NULL` is total over live rows. Movement invariance holds
identically: order lives in the key, which flush and compaction do not touch.

### Staged builds — multi-commit backfill

A backfill too large for one `WriteBatch` cannot be one commit, and the
bounded-commit rule (Reclamation's posture) is not negotiable. The staged
protocol splits the build into a `building`→`ready` lifecycle built from the
same three primitives as everything else here: writer-maintained coverage,
the value-keyed collision, and bounded batches. No new race machinery.

**The lifecycle.** `create_index(…, staged)` commits the definition in
`building` state — `altered_table`, `schema_changed`, exactly like the
single-commit create. From that commit forward the **full Coverage contract
applies to every writer**: inserts stage entries, deletes stage entry
removals, entry-less registrations are refused, DuckLake writes are covered
by the scoped read — a building index is maintained as if it were real,
because by the time it flips `ready` it must already have been. Unique
entries are value-keyed from the first commit; there is no second key form
to convert later. What `building` withholds is the *outward* surface:
`index_lookup` fails typed (`IndexBuilding`) — the coverage Goal is kept by
refusal, the same way it is kept everywhere else — and a unique collision
does not fail the writer (below).

**Backfill batches.** The builder covers the rows live at its current
snapshot; everything committed after the definition is writer-covered by
the paragraph above, so the two sets meet with no gap. A derivation pass
walks live inline chunks and then external files in durable source order.
Parquet is decoded as bounded Arrow batches and entries flow directly into
the next `BuildStep`; the driver never assembles or sorts the table-wide
entry set. Delete files and inline deletes are applied as each source is
read. Each step ends at whichever `BuildStep` bound it reaches first (Two
bounds on a step) and always carries at least one entry.

SQL-write upkeep compiles one projection plan per live index, deduplicates
shared Arrow columns, and decodes each Parquet or inline batch once. A scalar
stays borrowed from its Arrow array while the store-layer canonical builder
writes its NULL flag, canonical bytes, escaping, terminator, and direction
transform directly into the final key. No row-wide value vector or per-index
value copy crosses the batch boundary. NULL presence is accumulated by the
same builder and selects the multi-shaped unique-NULL entry. The builder then
expands those canonical bytes in place into the final physical `index` key;
the staged entry retains no intermediate canonical value or second payload
allocation. For inline input, each `(table, schema version)` IPC schema is
decoded once per commit and shared by all of its chunks. A chunk's owning
`Bytes` is sliced directly into Arrow's immutable data buffer, avoiding a
second copy of the body after it moves to a blocking decode worker.

Delete sources are grouped by physical target from their staged metadata
before any object is opened. Each target resolves its own delete files, so
independent additions and inline removals start beside delete discovery; only
the target whose positions are still being discovered waits. Data-file
additions and removals retain their bounded producer windows; nested
delete-file reads share one commit-wide allowance rather than multiplying
that bound per target.

Each step atomically advances a **source cursor** persisted in the definition
value: the completed inline row watermark, then the data-file id and physical
position most recently covered. Files are immutable and ids are monotonic, so
the cursor survives a crash without depending on embedded row-id order. A
replacement file receives a later id and is safe to re-derive; entries name
stable row ids, so the put is idempotent whether the source row was already
covered. A crash within one file resumes after its last durable physical
position. Deferred repair seeds the file cursor at the preceding snapshot's
tail, so it consumes only later files. It re-derives the policy-bounded inline
live set because UPDATE may preserve a row id while changing its value. The
driver retains at most one step of derived entries plus one Arrow batch, so
its entry memory is bounded by `BuildStep` rather than table size.
Two builders racing the same build both write the definition key and collide
write-write. Re-running either batch is idempotent, and the persisted source
cursor advances monotonically, so a stale retry cannot move it backward.
Intermediate steps classify `inserted_into_table:<table_id>`: a concurrent
append is benign and re-runs whichever commit loses the head race, while the
ordinary conflict matrix still rejects a concurrent delete, schema alter, or
drop. An intermediate cursor advance does **not** change the table schema: it
mints its ordinary snapshot while retaining the current global schema version
and writes no `ducklake_schema_versions` row. Only the initial definition
publication and final `ready` flip advance schema history.

**The delete race.** A row live at one derivation pass can die before its
step lands, and a stale entry for a dead row is corruption — for a unique
index it manufactures false `Constraint`s. Both tenses resolve into one
mechanism: derivation from a fresh snapshot.

- *Past deletes* (committed before the pass's snapshot): excluded by
  construction — the scoped read applies the table's delete bookkeeping,
  so a dead row produces no entry.
- *Concurrent deletes* (racing a step): the killing commit writes
  `deleted_from_table`, which conflicts with the step's
  `inserted_into_table` classification, so the
  loser surfaces `CommitConflict` — never an internal closure re-run that
  would re-stage a stale batch. The driver answers a surfaced conflict by
  re-deriving at a fresh snapshot (which excludes the newly dead rows) and
  re-committing from the cursor.

**Duplicates poison the build, not the writer.** During `building` the
entry set is incomplete, so an absent probe proves nothing and enforcement
cannot be offered — but a violation a put *does* discover is real: the
colliding entry belongs to a live row, because unique keys are value-keyed
from day one. Every such collision — a backfill step finding a pre-existing
duplicate, or a writer inserting a value another live row holds — stages a
terminal `poisoned` flag on the definition and drops the colliding claim,
so the live holder's entry survives; the commit that found it lands
normally, rows and all. One rule covers both discoverers, and it is the
partial coverage that demands it: enforcement during a build depends on how
far the backfill has run, so failing the finder would decide which party a
duplicate falls on by timing. A poisoned build stops at its next step: the
driver ends the definition into `history` (ordinary drop, Reclamation
sweeps the range) and surfaces `Constraint` to the `create_index` caller —
the create fails, exactly as its single-commit form would have.

Poisoning writes the definition key, so it conflicts with cursor advances —
both re-run, and the flag is terminal, so every interleaving converges.
Because the flag rides the commit's ordinary entity diff, the current
record and its history mirror are written together and the maintained
projections fold it like any other definition change.

**The ready flip.** When the cursor reaches the end of the `S₀` set and the
definition is not poisoned, one final commit flips `building`→`ready` —
`altered_table`, `schema_changed`, like the create: the flip changes what
readers may do, so it must conflict with in-flight writes, whose re-runs
then see `ready` and apply full enforcement (`Constraint` instead of
poison). From the flip forward the index is indistinguishable from a
single-commit build: same keys, same coverage, same guarantees. The
before/after entry ranges are byte-identical to what a from-scratch build
over the same live rows would produce — that equivalence is a test
obligation.

**Driving the build.** The embedding API returns from `create_index(staged)`
once the definition commits; the host advances the build with a
`build_index_step(index)` verb — one bounded batch per call, returning the
cursor position or terminal state — and loops at its own pace. The
extension path's `moraine_index_create(…, staged := true)` drives the loop
internally in the same autonomous-commit style as the other DDL functions,
returning once the index is `ready` or the build has failed. A cancelled
or crashed call leaves the definition `building`: re-issuing the same call
resumes from the cursor, and `moraine_index_drop` abandons the build.
`moraine_indexes` exposes the state (`is_building`) for progress.
`drop_index` on a building index is an ordinary drop: the builder's next
step re-runs against the ended definition and stops.

The driver emits progress at `info`. A derivation-started event makes the
otherwise quiet Parquet-read phase explicit; the matching derived event names
the live entry total and the derivation and sort times. Every successfully
durable step then reports the entries in that step, cumulative entries for the
current invocation, the live-table total estimate, percentage, row and source
cursors, final-step flag, and commit time. A resumed invocation starts its
counter at zero but names its durable starting cursor. A conflicting step
emits a warning with the last durable count before the driver re-derives.
Attempted but non-durable work is never reported as completed.

**Format.** Staged builds stamp **format 3** at the first staged
`create_index` — lazily, like format 2. A format-2 binary would ignore the
unknown state field, see a `building` definition as a ready index, and
serve lookups and enforcement from an under-covered entry set — precisely
the silent corruption the gate exists to refuse. Single-commit-only stores
stay format 2; completing or dropping the build does not downgrade the
stamp, per the existing posture.

### Extension path: SQL surface and the scoped read

moraine is DuckLake's metadata catalog, not the catalog that owns user tables
(RFC 0006). `CREATE INDEX` and `PRIMARY KEY`/`UNIQUE` on a DuckLake table die
in DuckLake's own binder (`NotImplementedException`) before moraine is
consulted, so DuckLake-native syntax is not moraine's to offer. The extension
path instead ships its own surface over the same native index and covers
DuckLake's writes itself.

**The SQL surface.** DuckDB's stable C API registers table functions (only
catalog registration forces the C++ shim, RFC 0006), so the extension exposes
the native index without touching DuckLake's binder:

Every function names its target by four leading positional strings —
`catalog, schema, table, index` (`moraine_indexes` stops at the table) —
written `…` below for brevity.

| Function | Effect |
|---|---|
| `moraine_index_create(…, columns, unique, directions := ['asc'\|'desc'], nulls := ['first'\|'last'], staged := b, maintenance := 'synchronous'\|'deferred', step_entries := n, step_bytes := n)` | insert the definition, backfill live rows (Coverage). `directions`/`nulls` are optional, parallel to `columns`, defaulting ascending / NULLS LAST. `staged := true` runs the multi-commit build (Staged builds) — required past the single-commit bound, resumable by re-issuing the call. `maintenance := 'deferred'` opts a non-unique index into bounded post-commit SQL-write upkeep; unique indexes refuse it. `step_entries`/`step_bytes` bound one staged build step (Two bounds on a step); each must be positive |
| `moraine_index_drop(…)` | end the definition (Reclamation) |
| `moraine_index_lookup(…, v…)` | table function: row ids for the equality key `v…` — one variadic value per indexed column, in the index's column order (a single value for a single-column index); the count must equal the index width |
| `moraine_index_in(…, keys)` | table function: row ids for the union of complete equality keys in `keys`. A single-column index takes a list of scalar values; a composite index takes a list of `row(...)` values in index-column order. Duplicate keys are one predicate, a key containing NULL matches no row, and an empty list returns no rows |
| `moraine_index_range(…, lower, upper, lower_inclusive, upper_inclusive, reverse := b)` | table function: row ids for a value window. Each bound is a scalar (single-column index) or a `row(...)` tuple over the leading columns; a NULL bound is an open side (half-open). `reverse` serves the opposite of the index's order |
| `moraine_index_nulls(…, prefix…, reverse := b)` | table function: row ids for an `IS NULL` query; the variadic prefix is the leading columns, a `NULL` arg meaning `IS NULL` and any other `= value` |
| `moraine_indexes(catalog, schema, table)` | table function: index introspection |

`moraine_index_in` binds `keys` as one constant list value (including a
prepared-statement parameter), like the arguments of the other explicit
lookup functions; it does not consume keys streamed from another relation.
A DuckLake read joins on `row_id` alone, so UPDATE, compaction, inline flush,
and delete semantics remain under DuckLake's snapshot-aware scan.

A multi-column range bound is a tuple. Tuple windows require every named
column to sort the same way; one byte range then spans them. With mixed
directions no such range exists, so both bounds must name the same leading
values and differ only in the last. Other shapes are refused.

Each resolves the store handle from the attached catalog and the `table_id`
from `ducklake_table`, then drives the same core verbs as the embedding API.
The DDL functions commit autonomously — their own moraine commit, outside any
enclosing DuckDB transaction — and race concurrent DuckLake commits through
the ordinary `altered_table` conflict. Non-native syntax, explicit reads
(below), but real without a DuckLake change.

**Coverage: intercept inline, read bulk.** Small inserts DuckLake stages
inline; their values cross the ABI as an Arrow body (RFC 0005), and moraine
derives entries from them as embedding `inline_insert` does. Bulk data
DuckLake writes to Parquet directly — those values never cross moraine's
boundary, so there is no "before" to intercept — and the committer instead
**reads the just-registered file**, projecting only the indexed columns, the
row positions, and the row-id column when the file carries one. Entry row ids
follow DuckLake's own resolution rule (source-verified in its reader): the
file's embedded row-id column if present — rewrite files from UPDATE and
compaction preserve old ids there — else `row_id_start + ordinal`. Deletes
are symmetric: a staged `register_delete_file` names positions; moraine reads
those positions' indexed values and row ids from the target file to name the
entries to remove. Indexed columns are located in the file by field id
through the column-mapping rules (RFC 0018). The prohibition on reading
Parquet guards the *scan* path — merge-on-read, lineage, pushdown — not this
raw-value projection.

A value is indexable only if its Parquet form and its inline Arrow form
derive the *same* canonical bytes, so the two write paths collide as they
must. That holds for the scalar types, strings, blobs, `UUID` (a 16-byte
blob on both paths, once the inline body is Arrow-encoded losslessly), and
the temporal types by their underlying integer count. It fails for a 128-bit
integer: DuckDB writes `HUGEINT` to Parquet as a *lossy* `double`, so its
data-file and inline forms disagree and distinct values could collide —
`CREATE INDEX` on one is refused rather than built silently wrong.

Two placement consequences. The Parquet projection and the inline Arrow-body
decode are **core** capabilities (`arrow` and `parquet` enter `moraine`) —
entry derivation is catalog-domain logic, not shim marshalling (RFC 0001).
And the core needs an object-store handle for **data** files: they live under
`DATA_PATH`, not the catalog store. That path is fixed when the lake is
created and does not reach the metadata attach on its own (DuckLake keeps its
`DATA_PATH` and forwards only `META_`-prefixed options), so it is supplied
once at creation via `META_DATA_PATH` and **recorded** in the global
`data_path` metadata option at bootstrap — beside `encrypted`. From then on
the recorded value is authoritative: the core serves it back as DuckLake's
`ducklake_metadata` `data_path` row (so a re-attach need not repeat it, and
DuckLake refuses a `DATA_PATH` that disagrees), and the shim resolves the
maintenance store from it, reusing the catalog store's secret. A re-attach
that supplies a conflicting `META_DATA_PATH` is refused rather than silently
honoured. Data-file paths resolve against that store.

**Re-derivation is idempotent.** A registered file is not always new rows:
rewrite files carry rows that already have entries. Multi entries re-derive
as idempotent puts (the key includes the row id), and the unique check's
same-row-id no-op arm (Uniqueness enforcement) covers the rest — so DuckLake
UPDATE and inline flush do not abort against their own existing entries.
Compaction, whose files are *all* such rows, is skipped before the read
rather than re-derived (Compaction derives nothing); idempotence is what
makes the remaining overlap harmless, not what makes it affordable.

**Uniqueness on the SQL write path.** Enforcement hinges on one thing: the
value-keyed put lands in the same atomic commit that adds the row (Uniqueness
enforcement). Both paths do that — inline from the Arrow body, bulk from the
scoped read — so the value's provenance is irrelevant, *provided both derive
the same canonical bytes for a given value*. That holds because the shim
encodes the inline Arrow body losslessly, matching how DuckDB writes Parquet
— a `UUID`, for one, is a 16-byte blob on both paths, not a blob in a data
file and a string inline — so a value inserted inline and the same value
written to a file collide as they must. Two further guarantees are
load-bearing:

- **A unique index's scoped read is synchronous at commit.** Deferring it
  would let a duplicate commit unchecked.
- **A failed read aborts the commit.** If the registered file cannot be read,
  the commit fails with a store error; the check is never skipped.

The `Constraint` message must avoid DuckLake's four retry substrings
(`"conflict"`, `"concurrent"`, `"unique"`, `"primary key"` — RFC 0006): a
unique violation is permanent, and retryable-looking text would spin
DuckLake's `RunCommitLoop` before surfacing. A concurrent collision is the
opposite case — a genuine `CommitConflict` containing `"conflict"`; DuckLake
retries (repeating the scoped read), the re-read finds the winner's entry,
and the permanent `Constraint` surfaces then. On a bulk violation the Parquet
DuckLake already wrote is left orphaned for ordinary cleanup — space, not
correctness.

**Reads are explicit.** DuckLake owns the planner, so nothing routes a
predicate to an index. The extension path reads through
`moraine_index_lookup`; the caller joins back to the table, whose scan
DuckLake adjudicates against delete files. The optimizer rule above does not
change that: it restates a join the caller already wrote, and never chooses
an index. v1 scope: creation, uniqueness enforcement, and explicit equality,
range, and NULL reads.

**The upstream boundary is explicit.** Moraine does not carry a downstream
DuckLake binder or optimizer patch. If DuckLake defines native index metadata,
the protobuf definition value's reserved `ducklake_index_id` can map its
stable identifier onto the existing `index` range; the reservation preserves
that option but commits to no implementation. Native DDL, writer-supplied
maintenance, and equality, comparison, or `ORDER BY` pushdown are upstream
requests tracked in [`../ducklake.md`](../ducklake.md), not open work in this
RFC.

Read-only attaches over an indexed store are unaffected.

### Rewrite files: row-id resolution

The scoped read's "embedded row-id column if present" rule (Coverage) is
made concrete here. Source-verified in DuckLake: UPDATE (always),
`rewrite_data_files` (always), and `merge_adjacent_files` (only when the
merged files' row-id ranges are not adjacent) append a trailing
`_ducklake_internal_row_id` BIGINT column to the files they write, tagged
with DuckDB's reserved Parquet **field id 2147483540**
(`MultiFileReader::ROW_ID_FIELD_ID`); its catalog row carries
`row_id_start` NULL. `merge_adjacent_files` and inline-data flush also
append `_ducklake_internal_snapshot_id` (field id 2147483539), which
index maintenance ignores. DuckLake's reader resolves a row's id as: the
field-id column when the file carries one, else `row_id_start + position`,
else a read error. Moraine's scoped read applies the identical rule.

**Column presence wins over `row_id_start` — the precedence is
load-bearing, not a tiebreak.** A flushed inline-data file carries embedded
row ids *and* a non-NULL `row_id_start` (DuckLake records the file's
minimum embedded id there), and its ids may hold gaps: inline rows deleted
before the flush are materialized out, but the survivors keep their
original ids. Dense derivation against such a file writes entries under
row ids the rows do not have — silent index corruption, no error raised.
Any resolution that consults `row_id_start` first is therefore wrong by
construction; the fallback order is fixed as column, then range, then
refusal.

**File-level row-ID statistics are derived, repairable metadata.** The
patched DuckLake writer records min/max under the same reserved field id for
every new non-empty file and uses those values to prune the physical-file list
for static and dynamic `rowid` filters. A missing row is "unknown" and keeps
the file, so installing the patch over an existing lake is safe but does not
accelerate legacy files until they are repaired.

`ducklake_backfill_row_id_stats(catalog [, schema := name] [, table_name :=
name] [, max_files := n])` is the migration surface. It returns one row per
selected table — schema, table, files repaired, and files still missing — and
is idempotent. `max_files` is a positive global bound across the selected
tables; omitting it repairs all missing non-empty active files. Dense files
first prove the embedded field is absent and derive `[row_id_start,
row_id_start + record_count - 1]`. Sparse rewrite and flush files use the
embedded BIGINT field's Parquet min/max, falling back to reading only that
column when its footer lacks statistics. The operation never rewrites a data
file and never mints a DuckLake snapshot. Through Moraine, its reserved-stat
inserts land as head-preserving maintenance batches; other file-stat inserts
remain illegal without a snapshot. A bounded partial run remains correct
because every unrepaired file is still included conservatively.

**Resolution lives inside the scoped read.** The reader already fetches the
file's footer; discovering the field-id column there costs nothing and
keeps the precedence rule in one place instead of at every call site.
Callers state intent, two modes:

- **DuckLake-parity** — registration, delete-side maintenance, and index
  backfill. The caller passes the catalog row's `row_id_start` verbatim
  (`Option`); the reader prefers the field-id column, falls back to the
  dense range, and refuses (`Corruption` — the catalog row and the file
  disagree) when neither exists. A NULL or negative embedded id is
  likewise `Corruption`.
- **Ordinal** — the extension-path registration helper, whose contract is
  positions for `register_data_file` to re-map onto a freshly allocated
  dense range. In this mode a file carrying the field-id column is
  **refused**: registering a rewrite file under new dense ids would fork
  its identity — readers honour the embedded column, the catalog would
  claim the dense range, and the index would follow the wrong one.

**The delete side filters by position, not by pre-computed row id.** A
delete file names positions within its target; converting them to row ids
before reading the target bakes in the dense assumption. Instead the
killed positions ride verbatim to the target's scoped read, which returns
entries in file order — an entry is removed when its ordinal is named by
a delete file or by an inline file-delete. DuckLake writes a physical
`file_row_number` into `ducklake_inlined_delete_<t>.row_id` despite the
column's name, so both kinds name a position and one rule serves dense and
per-row-id targets alike.

**Maintenance stays derivation, never removal.** Registering a rewrite
file only re-derives entries that exist (the idempotent puts and the
unique same-row-id no-op arm, Uniqueness enforcement) or adds entries for
changed values (UPDATE: the paired delete file removes the old-value
entries in the same commit). No register-side path stages an entry
delete, so a rewrite cannot drop a surviving row's entry — the property
that makes derivation safe to run without knowing what a file holds. It
is skipped in exactly one case, decided on the commit's change set rather
than the file's shape: a commit that only compacts (Compaction derives
nothing).

Indexed columns keep their positional location (Coverage): rewrite and
flush outputs are written under the table's current schema with the
internal columns trailing, so table-column positions are undisturbed.

**Embedding-API boundary.** The verb surface has no way to register a
per-row-id file — `register_data_file` allocates dense ranges, which is
why its entries name rows by ordinal: the ids do not exist until the
commit allocates them. Deletion never has that problem — the target is
registered and its ids are known facts — so `register_delete_file`
removals name rows **by row id against every target**, exactly the
Coverage table's `(row_id, key values)` contract: `row_id_start +
ordinal` for a dense target, where moraine checks the id lies inside the
target's range (the same strength as an ordinal bounds check), and the
embedded id for a per-row-id target, taken verbatim and trusted exactly
as the entry's values are — the writer read the target to learn its
positions and values, and the row-id column sits in the same file. One
shape, one staging path, no per-target rules. What stays out is a
rewrite-*registration* verb: the embedding API cannot express a file
that preserves row ids, indexed or not — that is absent compaction
surface (RFC 0008 keeps compaction DuckLake-driven), not index
maintenance, and would be its own RFC.

### Format gate

An older moraine writer committing to an indexed table would maintain no
entries — silent index corruption. The gate is `sys/format`, bumped
**lazily**: the first `create_index` writes format 2 in the same commit, and
older binaries refuse to open a format-2 store (RFC 0002 bootstrap
validation). Index-free stores stay format 1, byte-identical to today,
compatible in both directions. Format 2 is format 1 plus the `index` subspace
and `index` kind — no migration, no rewrite; dropping the last index does not
downgrade the stamp. Staged builds bump once more, to format 3 (Staged
builds), under the same lazy posture. Deferred upkeep bumps to format 4: a
format-3 writer does not know the deferred marker and could otherwise rewrite
a partially covered definition as ready. The `index` discriminant leaves the
segment extractor ("first byte") untouched, so existing segments and the RFC
0011 crash cases are unaffected.

### Reclamation

`drop_index` (and `drop_table`, which ends the table's indexes with it)
orphans the entry range. Entries are invisible the moment the definition
ends, so reclamation is pure space hygiene: a bounded sweep deletes
`index/{index_id}` in batches — never inside the dropping commit, whose
batch must stay bounded.

RFC 0021 specifies and implements that sweep: `Catalog::maintain` discovers
dead index ids by seeking
through the `index` subspace (ids are monotonic and never reused, so an id
absent from the live catalog is dead forever) and reclaims each range in
head-preserving batches. It runs as one step of the maintenance pass. Batched
scan-and-delete is the binding design. A SlateDB range delete could optimize
the mechanism without changing its semantics; that upstream request is
tracked in [`../slatedb.md`](../slatedb.md).

### Test obligations

Per RFC 0001 — store codecs get proptests, protocol claims get integration
tests against real SlateDB on in-memory `object_store`:

- **Encoding roundtrips + goldens.** `decode(encode(k)) == k` for both entry
  kinds; golden vectors pin the `index` discriminant and the canonical
  encoding of every indexable type, including the float normalizations, NULL
  skip, and composite framing (`("ab","c") ≠ ("a","bc")`). The borrowed
  builder is differential-tested byte-for-byte against the former owned
  encoder across all scalar categories, directions, NULL orders, composite
  keys, NaNs, signed zero, and framing escape bytes. In-place physical
  finalization is compared with the owned `Key::Index` encoder under arbitrary
  values, index ids, unique shapes, NULLs, and row ids. Arrow extraction has
  the same differential coverage across every supported Arrow representation
  and overlapping projections; a multi-chunk commit proves every body is
  decoded once while its table-version schema is decoded once for the commit.
- **Order preservation.** For every indexable type, `encode(x) < encode(y)`
  iff `x < y` in the type's SQL order (proptest); `DESC` reverses it,
  variable-length strings included (`"ab" < "a"`); a mixed `(a ASC, b DESC)`
  composite yields the declared order in one forward pass; `NULLS
  FIRST`/`LAST` place the null flag correctly under both directions.
- **Comparison accessors.** `<`, `<=`, `>`, `>=`, `BETWEEN`, and half-open
  bounds return exactly the matching live rows; a `DESC` index returns them
  high-value first; a leading-equality prefix plus a trailing range uses the
  index; a bare non-leading range is refused; equality stays a point-get on a
  unique index. `moraine_index_range` and a `directions`/`nulls`-carrying
  `moraine_create_index` drive these end to end.
- **Unique race.** Two concurrent commits inserting the same unique value:
  exactly one lands; the other returns `Constraint` after its closure re-run
  — never two entries, never a lost insert.
- **Benign distinct-value appends.** Two concurrent appends of different
  values to one indexed table both land via the append-append path.
- **Movement invariance.** Insert inlined rows → flush → compact: lookups
  return the same rows throughout; the `index` range is byte-identical before
  and after flush and compaction.
- **Delete coverage.** Store-resident delete (self-sufficient) and
  writer-supplied delete both remove entries; delete-then-reinsert of a
  unique value succeeds; a `register_delete_file` omitting entries on an
  indexed table is refused.
- **Registration contract.** Entry-less `register_data_file` on an indexed
  table → `Constraint`; with entries → covered lookups; ordinal→row-id
  mapping lands entries above the winner's `next_row_id` under concurrent
  append retry.
- **DDL interactions.** `create_index` racing `register_data_file` on one
  table → `CommitConflict` for one side; `drop_column`/type-change on an
  indexed column refused; rename unaffected; backfill validates uniqueness
  (duplicate in existing rows aborts the create).
- **Format gate.** First `create_index` stamps format 2 atomically with the
  definition; a format-1-only binary (simulated via the version check)
  refuses the store; index-free stores remain format 1. First *staged*
  create stamps format 3; a format-2-only binary refuses it;
  single-commit-only stores remain format 2.
- **Staged lifecycle.** A staged build over a table too large for one batch,
  under concurrent inserts, deletes, and updates: lookups fail typed while
  `building`; after the flip, the `index` range is byte-identical to a
  from-scratch single-commit build over the same live rows. Intermediate
  steps serialize as `inserted_into_table`; publication commits remain
  `altered_table`.
- **Staged delete races.** A row deleted between `S₀` and its batch is
  excluded via the delete bookkeeping; a row deleted *concurrently* with its
  batch collides on the entry key, the batch re-runs, and the entry is
  absent after both commits — for multi and unique kinds.
- **Staged duplicate poisoning.** A pre-existing duplicate discovered
  cross-batch poisons the build: the create surfaces `Constraint`, the
  definition ends, entries are reclaimed. A writer inserting a duplicate
  during `building` lands its rows — file and all — and poisons the index
  instead of failing; the same collision against a **ready** index still
  fails the writer, so the flip is what turns partial coverage into
  enforcement. A duplicate within one backfill step poisons it too, rather
  than failing the step.
- **Staged resume and racing builders.** A builder killed mid-build resumes
  from the persisted cursor with idempotent re-puts; two builders advancing
  one build serialize on the definition key, and a stale retry cannot regress
  the source cursor.
- **Ready-flip visibility.** A write in flight across the flip re-runs
  (altered-table conflict) and commits under full enforcement — a duplicate
  in that write gets `Constraint`, not poison.
- **Head-only lookups.** `index_lookup` on `snapshot_at(S)` fails typed;
  on a pinned current snapshot it reflects that snapshot's cut even as
  newer commits land.
- **Scoped data-file read.** A staged registration on an indexed table
  derives entries for exactly the indexed columns; a subsequent lookup
  returns the file's rows; the read touches no non-indexed column. Golden
  file fixtures pin the reader against each indexable type. A staged
  `register_delete_file` removes exactly the named positions' entries.
- **Compaction skips derivation.** A commit whose change set names only
  `merge_adjacent` or `rewrite_delete` leaves the `index` range untouched
  without reading the file it registers — pinned by registering a file
  that is not on the store at all, which a deriving commit could not
  commit. A change set mixing a compaction kind with any other still
  reads and derives.
- **Rewrite idempotence.** A rewrite file carrying a row-id column derives
  entries under the preserved ids; DuckLake compaction over an indexed
  table (unique included) leaves the `index` range byte-identical and never
  aborts on the rows' own existing entries. An UPDATE-shaped commit
  (delete file plus per-row-id file with changed values) removes the
  old-value entries and lands the new-value entries in one commit; an
  unchanged indexed value survives its same-commit delete-then-re-add.
- **Row-id precedence.** A file carrying both the field-id column and a
  non-NULL `row_id_start`, with gaps in its embedded ids (the flushed
  shape), derives entries under the embedded ids — a golden fixture pins
  the precedence. A per-row-id catalog row over a file lacking the column
  fails `Corruption`; ordinal mode refuses a file that carries it.
- **Rewrite delete side.** A delete file targeting a per-row-id file
  removes exactly the named positions' entries; an inline file-delete
  against one removes exactly the named row's. Backfill over a table
  holding per-row-id files derives their entries under embedded ids.
  Embedding: `register_delete_file` removals name rows by id against any
  target — verbatim against a per-row-id file, range-checked against a
  dense one; an id outside a dense target's range refuses with
  `Constraint`.
- **SQL-path uniqueness.** A staged registration whose file duplicates an
  existing unique value aborts with `Constraint`, message free of the four
  retry substrings — no retry storm; a non-duplicate registration lands and
  the next colliding one aborts.
- **Function surface.** The four registered functions resolve the store
  handle and `table_id` from an attached catalog and drive the same core
  verbs as the embedding API; `moraine_create_index` backfills existing
  external files via the scoped read; a lookup joins back to the table under
  merge-on-read.
- **E2E.** DuckLake flows over non-indexed tables are unaffected end to end;
  a `CALL moraine_create_index` then a bulk `INSERT` that duplicates a
  unique value fails the `INSERT` without a retry storm (message-text
  contract), and one that does not duplicate lands and is found by
  `moraine_index_lookup`. Over an indexed table: `UPDATE`, then
  `rewrite_data_files`, then `merge_adjacent_files` (one adjacent merge,
  one not — dense and per-row-id outputs) all commit; lookups stay
  correct after each; a `DELETE` against the rewritten file removes its
  entries; `moraine_create_index` on a table already holding rewrite
  files backfills them.
- **File-located lookups.** Each summary representation is chosen and
  intersected correctly — dense range, Roaring, and sorted vector — and the
  intersections are exact for both dense and sparse files. A duplicate
  physical row id across two current files returns both as candidates; an
  inlined row returns a NULL file id. An unreadable sparse file falls back
  to every requested row id remaining a candidate for it, proving the
  degradation is conservative rather than lossy. Over DuckLake:
  `data_file_id` projects, prunes statically and dynamically, and an
  indexed sparse-file lookup opens only the candidate Parquet files.
  Benchmarks report cache bytes, sparse-summary build time, located-lookup
  time, and `Total Files Read` warm and cold.

## Alternatives considered

- **Temporally versioned entries (`begin`/`end`, current/history-style).**
  Buys index-accelerated time travel at the cost of read-modify-move on every
  row death, entry history joining RFC 0007's GC surface, and a uniqueness
  check that filters dead versions instead of point-getting. A permanent
  hot-path tax to accelerate the path the keyspace treats as rare. Rejected.
- **Derive-at-attach hybrid** (persist external-file entries only, rebuild
  inline entries at attach). Attach cost grows with data, every lookup spans
  two structures, and the uniqueness check stops being a point-get. Rejected.
- **A reverse map (`index_id, row_id → key values`)** to make row-id-only
  deletes self-sufficient. Doubles entry storage and write amplification to
  spare deleting writers values they already read. Rejected; the
  writer-supplied contract keeps delete parity with registration.
- **Location payloads (file id / chunk key) instead of row ids.** One step
  shorter lookups; flush and compaction become index rewrites proportional
  to moved rows. Row lineage is the stabler identity. Rejected.
- **Hashed fixed-width entry keys.** Uniform key size, no cap — but
  collisions need verify-against-payload machinery and the order-compatible
  upgrade path dies. Rejected for v1; recorded as the oversized-value escape.
- **Mark-stale on the extension write path** (a DuckLake write flips the
  index to a `stale` flag). A stale unique index enforces nothing and says so
  only to callers who ask — silent degradation of the exact guarantee the
  feature exists to give. Rejected for the scoped read, which keeps DuckLake
  flows working *and* the index honest; stale-mode survives only as an
  unsettled option for non-unique indexes, where correctness is not at stake.
- **Refusing the extension write path outright** (this RFC's first stance).
  Reading a registered file's indexed columns is a bounded, merge-free
  projection — not the scan path the non-goal guards — so blanket refusal
  traded a real capability for a boundary that did not need defending.
  Superseded by the scoped read (Extension path).
- **Modeling the index as a pseudo-DuckLake table** (an invented
  `ducklake_index` served row-faithfully). Invents non-spec surface real
  DuckLake would never read and future versions could collide with.
  Native-and-invisible is the right shape. Rejected.
- **Uniqueness by scan at commit** (no persistent index; check by scanning
  the table's rows). O(data) per commit against the index's one point-get.
  The scoped read stays bounded to the one registered file precisely
  because the index covers the rest; dropping the index turns that into a
  whole-table scan. Rejected.
- **Staged builds in multi form, converted at ready** (build unique entries
  as `(value, row_id)` keys — faithful under duplicates — then validate and
  rewrite to value-only keys at the flip). Represents duplicates without
  poisoning, but the conversion is an O(entries) rewrite that itself needs
  staging, and the window where writers must maintain both key forms is a
  second protocol. Value-keyed-from-day-one plus the poison flag gets the
  same outcome with the machinery already on the page. Rejected.
- **Blocking table writes for the build's duration.** Turns the build into
  one logical commit and deletes every race — by serializing a table for
  hours precisely when it is largest. The append-append Goal exists to
  forbid this shape. Rejected.
- **Stale entries adjudicated at read** (skip the delete-race machinery;
  let lookups filter dead rows like a scan does). Works for row location —
  candidates are already adjudicated by the consumer — but a unique index
  cannot carry stale entries: a dead row's entry manufactures false
  `Constraint`s at commit, where there is no adjudication step. Rejected.

## Prior art: HelixDB on SlateDB

HelixDB (an OLTP graph-vector database) runs on the same substrate — stock
SlateDB over object storage — making its index surface the closest available
comparison. The engine is closed; what follows is read from its public Rust
SDK (`helix-db` 2.0.6: `IndexSpec`, `SourcePredicate`) and its SlateDB fork,
so the *contract* is observed and the on-disk key layout is not.

**Near-stock SlateDB was enough.** `HelixDB/slatedb` carries a handful of
commits over upstream (reader snapshots, a multi-get batching point-gets) —
no index primitives: no range-delete, no merge operator. The whole index
family, uniqueness included, rides the same `get` / `WriteBatch` /
prefix-scan surface this RFC assumes. A production index engine living
without range-delete settles the Reclamation question: the batched sweep
is a legitimate permanent design, not a workaround awaiting a SlateDB
feature.

**A wider taxonomy, the same equality core.** `IndexSpec` spans equality,
range (with a physical `Asc`/`Desc` direction), vector, and text. Only
equality carries a `unique` flag — matching the decision here to key
uniqueness on the value alone — and its "uniqueness for supported non-null
values" doc confirms the NULL-exempt semantics, arrived at independently.

**Range is equality plus committed order.** `RangeIndexDirection` bakes the
sort direction into the stored key, not applied at read time — exactly the
shape this RFC adopts (Range and comparison queries): order-preserving bytes
plus a committed order contract plus a per-column direction, realized by
complementing the framed component. Independent convergence on the same
design.

**The pushdown boundary is drawn in the same place.** HelixDB splits a
restricted `SourcePredicate` (`Eq`/`Neq`/`Gt`/`Between`/`StartsWith`/`And`/
`Or`, used at source selection) from a general `Predicate` run as a scan-time
filter: the index serves source selection, everything else filters during the
scan. This RFC draws the same line one notch tighter — no range, so no
`Between`/`StartsWith` on the fast path.

**Where it diverges — and why.** HelixDB manages indexes as runtime
control-plane steps (`create_index_if_not_exists`) against a server that
holds and authored every row; entries are the engine's private business.
moraine does not own the write path — a separate writer produces the Parquet
— and that one fact drives both coverage models: the embedding writer
supplies entries alongside the file it wrote (Coverage); DuckLake supplies
nothing, so moraine derives them by the scoped read. HelixDB never
meets the register-with-entries problem. The divergence is a property of the
embedding, not a different answer to the same question.

**Why equality-only, when HelixDB shows the substrate carries vector and
text too.** HelixDB models both over the same ordered KV surface: its HNSW
persists vectors under `(id, level)` keys and the proximity graph as
adjacency keyed `(source, level, sink)`; its BM25 is an inverted index plus
length/frequency tables. (Layouts read from its earlier LMDB engine; the
modeling ports, since both are ordered KV stores with prefix scans.) So
SlateDB could store these indexes; this RFC excludes them because of what
they cost to *read*. An equality lookup is one point-get or one bounded
prefix scan — fast even on a cold cache. An HNSW search is dozens of
*dependent* point-gets down a graph, and on an LSM over object storage every
hop that misses the block cache is an object-storage round trip: fast only
while the whole graph stays cache-resident, seconds when it does not.
Equality (and the range upgrade) fits RFC 0009's read model; vector search
does not, and that is the entire reason it is out. The boundary is read
cost, not storability.

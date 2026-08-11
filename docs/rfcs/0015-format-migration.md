# RFC 0015: On-disk format migration

- **Date:** 2026-07-09

## Summary

[RFC 0002](0002-slatedb-key-encoding.md) defines two independent axes of
format evolution, and it is essential to keep them apart because they are
handled by opposite mechanisms. **Axis 1** is value evolution: protobuf field
add/deprecate plus the per-value 1-byte encoding version in the framing
header. It is forward/backward compatible and handled **lazily by readers** on
the fly — a reader translates an old value as it decodes it, and there is
**no migration**. **Axis 2** is structural: a change to the subspace tag set
or the key structure — the physical shape of the keyspace itself. A structural
change cannot be absorbed by a reader on the fly, because it changes *where*
data lives and *how keys are ordered*; it requires rewriting keys. This RFC
specifies the mechanism for **axis 2 only**: how a store written at an older
structural `sys/format` version is rewritten in place to a newer one, safely,
under the single fenced writer, and crash-resumably. Axis 1 is out of scope
here and stays out — a reader meeting an old *value* encoding never invokes
any of this machinery.

## Goals

- One in-place rewrite mechanism for structural (`sys/format`) upgrades that
  preserves every invariant already established: single fenced writer
  ([RFC 0004](0004-commit-protocol.md)), one-batch atomicity per step
  ([RFC 0002](0002-slatedb-key-encoding.md)), data-before-metadata ordering
  ([RFC 0007](0007-snapshot-expiry-and-gc.md)), and crash-resumability
  ([RFC 0011](0011-crash-recovery.md)).
- Refuse, loudly and typed, to open a store whose `sys/format` is *newer* than
  the running binary understands (RFC 0002's meet-a-newer-format rule) — never
  silently downgrade or misread.
- Migrations are **named, composable, individually tested** `v_n → v_{n+1}`
  units; a multi-version jump is their composition.
- A migration is crash-resumable at every seam: a crash mid-rewrite reopens
  and resumes from a durable cursor, never losing catalog state and never
  exposing a half-migrated store to a reader.
- A structural rewrite is **operator-triggered**, not a silent side effect of
  opening a store in production.

Non-goals:

- **Value (axis 1) evolution.** Protobuf field changes and the framing
  header's encoding-version byte are RFC 0002's domain and need no migration;
  this RFC does not touch the value codec.
- **Online migration across mixed binary versions.** A structural rewrite
  changes key structure, so an old binary cannot read the new layout. Rolling a
  fleet across a structural bump is unresolved, not solved here.
- **Automatic rollback / downgrade.** Migrations are one-way; recovery is
  manual and rests on old objects surviving until new ones are durable
  (below). Rollback strategy is unresolved.
- **Inventing new atomicity or fencing primitives.** This RFC composes the
  existing ones exactly as [RFC 0011](0011-crash-recovery.md)
  composes them for genesis.

## Background

`sys/format` lives in the `system` subspace (RFC 0002) and records two things:
the **structural format version** (RFC 0002's layout = 1) and the moraine
version that wrote the store. The structural version is bumped only by a
change to the subspace tag set or the key structure — precisely the changes a
reader cannot absorb lazily. RFC 0002 states the governing rule: *a
reader/writer that meets a newer format than it understands errors rather
than misreading.* This RFC is the other half of that rule — what happens when
the store is **older** than the binary, and the operator chooses to upgrade it
in place. Not every older store needs upgrading, though — see "Additive
versus rewriting, within axis 2".

The genesis protocol ([RFC 0011](0011-crash-recovery.md), the genesis cases)
already establishes the template this RFC follows: a multi-step state change
that cannot always be one `WriteBatch`, made safe by a durable marker, a
resume-from-cursor loop, and a final atomic flip. Genesis births a store;
migration rewrites one. The shapes are deliberately the same.

## Design

### The two axes, drawn sharply

This is the distinction most likely to be conflated, so it is stated as a
table before anything else.

| | Axis 1: value evolution | Axis 2: structural migration |
|---|---|---|
| What changes | Protobuf fields; per-value encoding version | Subspace tags; key structure |
| Signal | Framing-header encoding byte (RFC 0002) | `sys/format` structural version |
| Compatibility | Forward/backward (skip unknown, default missing) | Breaking for a binary that cannot name the new tags |
| Handled by | **Readers, lazily, on decode** | **A lazy stamp, or a one-time rewrite of the store** |
| Migration? | **None** | **This RFC — for the rewriting kind only (next section)** |

If a proposed change can be expressed as a new protobuf field a reader
defaults or ignores, it is axis 1 and does **not** bump `sys/format`. Only a
change to *where a key lives* or *how it sorts* is axis 2. The bar for axis 2
is deliberately high, because axis 2 is the expensive one.

### Additive versus rewriting, within axis 2

Axis 2 splits again, and only one half needs this RFC's machinery.

An **additive** structural change introduces a new subspace or a new kind and
**moves no existing key**. The `index` subspace ([RFC 0016](0016-equality-indexes.md))
and the per-chunk inline row-range directory
([RFC 0005](0005-data-inlining.md)) are worked examples: a store that grows
either is stamped a higher
`sys/format` so binaries that cannot name the new tag refuse it, but nothing
already written is rewritten, and a binary that *does* know the tag reads the
older store unchanged. The stamp is therefore **lazy** — written the first
time the feature is used, not on open — and there is **no migration**.

A **rewriting** structural change alters where existing keys live or how they
sort. Old keys must be read, re-encoded, and deleted. This is the only kind
that needs the start/step/finish protocol below, and the only kind that makes
an older store unreadable rather than merely older.

The practical consequence is that a binary reads a **range** of formats, not
one: every format from the oldest it can still make sense of (its floor) up
to the newest it can name. Additive bumps widen that range; a rewriting bump
raises the floor, because below it the keys are somewhere else.

### On-open version check

Every attach reads `sys/format` before doing anything else and compares its
structural version to the binary's:

| Store vs. binary | Action |
|---|---|
| **Within the binary's range** (floor ≤ store ≤ newest it names) | Proceed normally. A store *below* the binary's newest is not migration-eligible: the intervening bumps were additive, so its keys are exactly where this binary looks for them. It is stamped forward lazily if and when it uses the newer feature, never rewritten on open. |
| **Below the floor** | **Refuse to open.** Return the typed `Migration` error (RFC 0003) naming the store's version, the floor, and the verb that repairs it. A rewriting migration put this store's keys somewhere this binary does not look; it must be migrated up first. |
| **Newer than the binary names** | **Refuse to open.** Return the typed `Migration` error (RFC 0003, per RFC 0002's meet-a-newer-format rule). Never guess, never downgrade, never write. |
| **Absent** | The store is uninitialized. A read-write attach bootstraps it; a read-only attach refuses, having nothing committed to read. |

The newer-than-binary refusal is not optional or best-effort: a binary that
cannot name every subspace tag it might encounter cannot safely read, so it
stops. This is the same discipline the framing header applies to an unknown
value encoding, lifted to the structural level.

Until the first rewriting migration exists, the floor sits at the base format
and the below-the-floor arm is **dormant** — no store in the world is below
it. It is specified and implemented now anyway, for the same reason the
reader gate is: the arm has to be in the fielded binaries *before* the format
that makes it fire.

### Single-writer migration

A structural migration is a sequence of writes, so it runs under the **one
fenced writer** (RFC 0004). This is not new machinery: SlateDB's
`writer_epoch` CAS-on-open means a second process attempting to migrate the
same store is fenced — it loses the epoch and writes nothing (RFC 0011, the
takeover cases).
So **exactly one migrator runs**, by the same guarantee that makes an
accidental second catalog writer safe. Readers, meanwhile, must never observe
a half-migrated store; the resume protocol below is structured so the only
externally visible flip of `sys/format` is atomic and last.

### Crash-safe resumable migration

A large migration cannot be one `WriteBatch` — the rewritten keyspace is
unbounded in size, and one giant batch has no resume point (see Alternatives).
So migration gets its own resume protocol, modeled directly on genesis
(RFC 0011). It has three phases.

**1. Start (one batch).** The migrator writes a `sys/migration` marker
recording `{ from_format, to_format, cursor }`, where `cursor` is a
resumption position in the migration's key-ordered work (e.g. the last
source key processed). This first batch is atomic: either the marker exists
or it does not.

**2. Step loop (many batches, each idempotent).** The migration walks its
work in key order. Each step batch does two things, in this order:

- **Writes new-format keys _before_ deleting the old-format keys they
  supersede** — the structural analog of data-before-metadata (RFC 0007). A
  crash between the write and the delete leaves a *resumable* overlap
  (new keys present, old keys not yet gone), never a *lost* record. Catalog
  state is never absent at any seam.
- **Advances the `cursor`** in the *same* batch that performs the rewrite, so
  the durable cursor never claims more progress than is durably rewritten.

Every step is **idempotent**: re-applying a step whose new keys already exist
and whose old keys are already gone is a no-op. On reopen, the migrator reads
the cursor and resumes from exactly there; steps at or before the cursor that
partially re-execute converge without duplication.

**3. Finish (one batch).** The final batch **atomically flips `sys/format` to
`to_format` and clears the `sys/migration` marker** — together, in one
`WriteBatch`. Until this batch lands, `sys/format` still reads the old version
and the marker is present. After it lands, the store *is* the new version and
there is no marker. There is no observable in-between: a reader sees either
"old format, migration possibly in progress" or "new format, done."

**Crash and resume.** A crash at any point reopens into one of three states,
distinguished by reading `sys/format` and `sys/migration`:

| Reopen sees | Meaning | Action |
|---|---|---|
| No marker, old format | Migration never started (or start batch lost) | Nothing to resume; store is coherently old. |
| Marker present, old format | Migration in progress | Resume from `cursor`; re-run steps idempotently. |
| No marker, new format | Migration completed (finish batch landed) | Nothing to do; store is coherently new. |

The one combination that must be impossible — **new format with the marker
still present** — is made impossible by putting the flip and the clear in the
same batch. This is exactly `GenesisInterrupted`'s "one `WriteBatch` for the
terminal transition" applied to the migration's end instead of genesis's.

### Reader coherence during migration

Because axis 2 changes key structure, **a reader running the old binary cannot
read the new layout**, and a reader on the new binary cannot make sense of a
store still on the old format. This is an honest, structural consequence, not
a limitation to be engineered around cheaply.

Note first what the `sys/format` check alone does **not** cover. It would
be tempting to reason that a pre-migration reader "keeps resolving the
pre-migration state for as long as the old keys it needs remain" — but the
step loop is *deleting* old keys as it progresses. Mid-migration,
`sys/format` still reads the old version, so an old-binary reader would
open without complaint, scan the old-layout ranges, and see a catalog with
a **growing hole in it** — records vanishing from under it as they move to
the new layout. That is not "briefly unavailable"; it is **silently
wrong**, the exact failure class this document set exists to make
impossible.

Closing it is why the **`sys/migration` marker is a reader-side gate**, not
merely migrator bookkeeping:

- Every materialization and refresh reads `sys/migration` under its pinned
  read-snapshot (RFC 0009) and, if the marker is present, fails loudly with
  the typed `Migration` error (RFC 0003) instead of returning a view. A
  reader mid-migration is therefore *unavailable*, never partial.
- Consequently the marker key and this check must exist **from format
  version 1, before any migration is ever written** — RFC 0002 reserves the
  key and RFC 0009 specifies the check. A reader binary that predates the
  check cannot be protected retroactively; shipping the gate with the first
  release is what makes the first structural migration, years later, safe
  against every reader in the field.
- Once the store flips, an old-binary reader meets a newer `sys/format` and
  refuses to open (previous section). Both failure modes are typed and
  loud; neither returns wrong data.
- The new-binary migrator is the single writer; it does not serve reads of a
  half-migrated intermediate to anyone.

Consequently **structural migration is _not_ online across mixed binary
versions**, and that is the permanent answer, not an open gap. A rolling
deployment that spans a structural bump either drains readers across the flip
or tolerates a brief unavailability window. Serving one structural bump across
two binary versions at once is not offered: the key layout differs, so an old
binary cannot read the new store, and no scheme reconciles that without keeping
a second layout live alongside the first.

### One-way, composable migrations

Migrations are directed `v_n → v_{n+1}`, each a **named unit** with its own
step logic and its own tests. A jump from v1 to v3 is the **composition**
v1→v2 then v2→v3, run in sequence, each with its own start/step/finish and its
own cursor. There is no bespoke v1→v3 path to write or test; correctness of
the composition follows from correctness of each link.

The unit registry is intentionally empty while every shipped format change is
additive. It is complete for the formats that exist, not a missing
implementation. The change that first moves an existing key must add its
`v_n → v_{n+1}` unit, raise `MIN_FORMAT_VERSION`, pin the registry chain, and
drive that released unit through the SQL surface in the same change.

There is **no automatic rollback**, by decision. Recovery from a failed
migration is manual, resting on two mitigations already load-bearing elsewhere
in the design:

- Old objects are not deleted until the new ones that supersede them are
  durable (the step-loop ordering), so a failed migration can be reasoned
  about against surviving old state.
- An **optional pre-migration checkpoint** gives a manual recovery point,
  alongside SlateDB's object history. It is a whole-store
  `Db::create_checkpoint()` behind an operator flag on the `migrate` verb, off
  by default; when taken, it is released after the finish batch is durable.

Paired inverse migrations are deliberately not written or tested: the
sanctioned recovery path is the pre-migration checkpoint, not a reverse
rewrite.

### Trigger policy

A structural rewrite is heavyweight — it rewrites the keyspace and takes the
single writer for its duration. It is therefore gated behind **explicit
operator opt-in**: a dedicated verb/flag (e.g. a `migrate` operation, distinct
from ordinary attach), **not** a silent auto-run on open. The reasoning:

- An unbounded rewrite triggered implicitly by "someone opened the store with
  a newer binary" can surprise a rolling fleet — the first upgraded node to
  attach would begin rewriting under every still-running old-binary reader,
  which is exactly the mixed-version hazard the previous section names.
- Migration cost (time, object-store traffic, writer occupancy) is an
  operational decision with a maintenance-window shape; the operator owns it.

The verb takes the object store rather than an open catalog, because the
store it must act on is precisely the one an ordinary attach refuses: below
the floor, or carrying a marker. It opens the writer itself — taking the
same epoch `open` takes, so it fences a running catalog and is fenced by
one — runs the plan the durable state implies, and closes.

That is also what makes the refusal actionable, and the refusal says so: the
verb never runs the format check, so the binary that refuses to *open* a
below-the-floor store is the same binary that migrates it. An operator
reading "refused" as "wrong binary" would go looking for one that does not
exist. The refusal names the verb — the core names `Catalog::migrate`, and
the DuckDB shim, which is the layer that holds the store path and the only
one that may name a SQL function, turns the same error into one naming
`moraine_migrate('<path>')`.

Every migration is triggered by the explicit verb; nothing auto-runs on open.
This includes bounded metadata-only rewrites: one trigger rule is easier to
operate and audit than classifying which mutations are harmless enough to run
as a side effect of attach.

### Test obligations

These extend [RFC 0011](0011-crash-recovery.md)'s cases; they
run against real SlateDB on in-memory `object_store`, no store mocks
(RFC 0001), and are naturally expressed as new `CrashCase`-style cases.

They need a unit to run, and no shipped format has one — every format to
date is additive. A fault-injection build therefore installs a synthetic
rewriting unit into the driver's own registry, so the obligations below are
driven through the public verb against the planner that ships. The unit
lands the store on the newest format this binary reads, which is what lets
the post-migration assertions go through an ordinary attach.

- **Crash at every migration seam.** Inject a crash after the start batch,
  after each step batch, before the finish flip, and after it. Each reopen
  resumes to a **coherent store** — new format if the finish landed, else
  resumed from the cursor — and never exposes new-format-with-marker.

  Those four are the *whole* set, because a seam is only meaningful where a
  durable state exists to crash into. A step's new-key write, its old-key
  delete, and its cursor advance all land in **one batch**, so there is no
  durable intermediate between them to inject at: the interleavings are
  unrepresentable rather than merely untested. The same holds for the finish
  flip and the marker clear. Injecting "inside" a batch would test SlateDB's
  atomicity, not this protocol's.
- **Refuse-to-open on a future format.** A store whose `sys/format` exceeds
  the binary errors typed, writes nothing.
- **Reader gate mid-migration.** With the `sys/migration` marker present, a
  materialization or refresh — on either binary version — returns the typed
  `Migration` error and never a partial view (the RFC 0009 check; this is
  the row that forbids the silently-shrinking-catalog failure). Including a
  read-only handle that was already open and healthy when the marker was
  planted: it polls object storage on its own cadence, so the marker reaches
  it a poll late, and the read it is mid-way through when that happens must
  refuse rather than serve the view it had.
- **Idempotent re-run of a completed migration.** Running the migration verb
  against an already-migrated store is a no-op (no marker, format already at
  target), not a re-rewrite.
- **Migrate-then-time-travel.** After migration, resolving the catalog at a
  pre-migration snapshot (RFC 0002 `history`/`snapshot`) still returns the correct
  historical state — the rewrite preserves temporal semantics, it does not
  flatten history.

## Alternatives considered

- **Silent auto-migrate on open.** Rejected for migrations of every size. A
  heavyweight, unbounded rewrite should be the operator's explicit choice:
  triggered implicitly, the first
  upgraded node to attach begins rewriting the keyspace under every
  still-running old-binary reader, surprising a rolling fleet and coupling an
  operational decision to an incidental attach. Explicit opt-in makes the
  cost and timing owned, while one rule keeps bounded metadata rewrites
  observable too.
- **Lazy / online per-key migration on read.** Translate old keys to the new
  layout on the fly, forever, the way axis 1 translates old *values*.
  Rejected for key structure specifically: it leaves the store **permanently
  bimodal** (both layouts live indefinitely), every read pays translation
  forever, and range scans must union two key layouts. The crucial contrast:
  axis-1 value field-evolution *is* handled lazily precisely because a value
  is opaque bytes a reader decodes in place — but a **key's structure governs
  ordering and placement**, so "translate on read" cannot fix where a key
  physically sorts in a range scan. What works for values cannot work for
  keys.
- **Never migrate — a new major version is a new store plus an external
  copy.** Rejected. It abandons in-place durability for the single
  most-expensive-to-reverse state moraine owns (RFC 0002 calls the on-disk
  format "the single most expensive-to-reverse decision"), forcing a
  full external dump/reload for any structural change and giving up the
  atomicity and history guarantees the store provides.
- **One giant `WriteBatch` for the whole migration.** Rejected. Batch size is
  unbounded in the size of the keyspace, and a single batch has no resume
  point — a crash restarts the entire rewrite from zero, and a large enough
  store may never fit a batch at all. The start/step/finish protocol with a
  durable cursor is the bounded, resumable form, matching genesis (RFC 0011).

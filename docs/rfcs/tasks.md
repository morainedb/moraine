# Open work

The consolidated open questions and deferred work for every RFC in this
directory. RFCs state the binding design; this file states what about that
design is undecided or unbuilt. An RFC that has no entry here is fully
settled and fully implemented.

Requests owned by upstream projects are tracked separately in
[`../ducklake.md`](../ducklake.md) and [`../slatedb.md`](../slatedb.md). They
do not remain open work here unless moraine has an independent implementation
or decision to make.

Each item is tagged:

- **DECISION** — a design question with no answer yet.
- **DEFERRED** — agreed work, postponed on purpose.
- **IMPL** — specified in an RFC, not built.
- **VALIDATE** — a test the design depends on, closed by writing it.
- **MEASURE** — a number the design wants from a benchmark or profile. No
  assertion closes one of these; running something and recording the result
  does.
- **DOC** — an operator- or user-facing gap.

A VALIDATE whose subject does not exist yet is blocked on the IMPL item above
it, not independently actionable: writing that test *is* building the feature.

Resolving an item means updating the owning RFC and deleting the entry.

RFC 0022 (the commit log and the leader role) is wholly unimplemented and is
deliberately not itemized here.

## 0005 — Data inlining on SlateDB

- **DEFERRED** — Auto-flush policy: when to trigger an inline flush. This RFC
  specifies only the mechanism; the policy is an operational concern.

## 0007 — Snapshot expiry and garbage collection

- **DEFERRED** — Expose live reader snapshots at the extension layer so
  operators can size retention windows from observed reader durations.
  Policy-only for now.

## 0008 — Compaction and delete-file consolidation

- **DEFERRED** — Finer file-set-grain conflict detection, so two compactions of
  disjoint file sets in one table can run concurrently. Table grain today.

## 0013 — Partitioning, sorting, and pruning

- **DEFERRED** — Server-side partition-pruning pushdown. One deferral with
  0002's stats pushdown, 0006's pushdown surface, and 0009's partial
  materialization — not four. Nothing pushes a predicate into moraine today, and
  0009 records why pushdown cannot pay off while the whole catalog is resident,
  so this revives only alongside that decision. If built it must be
  transform-aware and type-aware, never a naive compare.

## 0014 — Catalog and data encryption

- **DECISION** — Whether to support untrusted-bucket deployments, via SlateDB's
  `BlockTransformer` or a native scheme, if demand appears. Nothing is
  designed.
- **DEFERRED** — If store objects ever need encryption independent of the
  bucket, implement it at SlateDB's `BlockTransformer` seam. Manifests and SST
  footers stay plaintext today.

## 0015 — On-disk format migration

- **IMPL** — The first real `v_n → v_{n+1}` unit. The registry ships empty:
  the driver, the unit shape, and the composition are built and tested
  against synthetic units, but every format to date is additive, so no
  rewrite exists to register. The first format that moves an existing key
  adds the first entry — and must raise `MIN_FORMAT_VERSION` with it, since
  its `to_format` is where the keys then live. A test pins the two together
  so that cannot be forgotten, along with the chain shape `chain_from`
  depends on.
- **VALIDATE** — Drive a real rewrite end to end through SQL. Blocked on a
  shipped unit, and on nothing else that is buildable: the core tier drives
  the whole protocol against a synthetic unit the `fault-injection` feature
  installs, but the e2e tier loads a *released* extension binary, and
  building that one with fault injection would ship test scaffolding to
  every user. So this closes when the first real key-moving format lands,
  not before. No candidate is in sight: the mapping kinds that were the
  near-term one are now pinned in 0002's map as built, which moved no key,
  and every format to date remains additive.
- **DEFERRED** — Allowing a trivial, bounded `system`-only migration to
  auto-run on open. The shipping behavior is explicit-verb-only for every
  migration regardless of size; auto-run is a later refinement, not the first
  cut.
- **DEFERRED** — Rolling a fleet across a structural bump with mixed binary
  versions online.

## 0021 — Maintenance orchestration

- **DEFERRED** — Multi-tier scheduling: per-step cadences and step sets, so a
  cheap sweep interval can differ from an expensive
  `delete_orphaned_files` interval. One interval drives the whole fixed
  sequence today.
- **DEFERRED** — Wire checkpoint lifecycle in as a consumer of the maintenance
  pass surface, if and when it lands.
- **MEASURE** — The absolute number against a real endpoint. Both halves of
  the model are measured and recorded (`BENCHMARK.md`): attach cost tracks
  the physical bytes a read touches, and under injected per-GET latency it
  tracks the GET count, which a merge cuts. Both are linear, so the
  production regime extrapolates — what is missing is only the endpoint's
  own latency term, which `object_storage.rs` needs a real bucket to see,
  exactly as the 0004 commit-latency row does.
- **DECISION** — Whether the read-ahead and fetch-concurrency figures
  (4 MiB, 8) deserve to be attach options. They are moraine's choice today,
  picked to make a scan latency-insensitive rather than measured against a
  ladder, and the right values plainly differ between a local store and S3.

# RFCs

Design records for moraine. An RFC is **required** for decisions that are
expensive to reverse — KV key layout, commit/transaction protocol, public API
shape — and optional for everything else. RFCs double as an ADR log: they
record *why* the project is the way it is.

## Process

1. Copy `0000-template.md` to `NNNN-kebab-title.md` (next free number).
2. RFCs carry no status field. Every RFC in this directory is the current
   design and is binding: if implementation reveals a better design, update
   the RFC (or replace it with a successor that points back) — don't
   silently diverge.
3. An RFC states the design, not what is left to do about it. Undecided
   questions and deferred work are tracked outside the RFC.

Design documents produced in brainstorming/design sessions are written
directly here as RFCs; there is no separate specs directory.

## Index

| RFC | Topic |
|---|---|
| [0001](0001-repository-structure-and-conventions.md) | Repository structure and conventions |
| [0002](0002-slatedb-key-encoding.md) | SlateDB key encoding for catalog state |
| [0003](0003-public-api-shape.md) | Public API shape of the core |
| [0004](0004-commit-protocol.md) | Commit and transaction protocol |
| [0005](0005-data-inlining.md) | Data inlining on SlateDB |
| [0006](0006-extension-surface.md) | Extension surface (DuckDB) |
| [0007](0007-snapshot-expiry-and-gc.md) | Snapshot expiry and garbage collection |
| [0008](0008-compaction.md) | Compaction and delete-file consolidation |
| [0009](0009-reader-consistency-and-caching.md) | Reader consistency and snapshot caching |
| [0010](0010-async-sync-bridge.md) | Async↔sync bridge |
| [0011](0011-crash-recovery.md) | Crash recovery |
| [0012](0012-schema-evolution-and-versioning.md) | Schema evolution and versioning |
| [0013](0013-partitioning-sorting-and-pruning.md) | Partitioning, sorting, and pruning |
| [0014](0014-encryption.md) | Catalog and data encryption |
| [0015](0015-format-migration.md) | On-disk format migration |
| [0016](0016-equality-indexes.md) | Equality indexes |
| [0017](0017-read-write-and-read-only-attach.md) | Read-write and read-only attach paths |
| [0018](0018-column-and-name-mapping.md) | Column and name mapping for external Parquet |
| [0019](0019-macros.md) | Scalar and table macros |
| [0020](0020-change-data-feed.md) | Change data feed |
| [0021](0021-maintenance-model.md) | Maintenance orchestration |
| [0022](0022-commit-log-and-leader-role.md) | The commit log and the leader role |
| [0023](0023-published-row-summaries.md) | Published file row summaries |

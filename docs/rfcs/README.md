# RFCs

Design documents for Moraine. Each one specifies one part of the design as
it stands and is kept current with the code; where the two disagree, that
is a bug.

## Process

An RFC is required for decisions that are expensive to reverse: the key
layout, the commit protocol, the public API.

1. Copy `0000-template.md` to `NNNN-kebab-title.md` using the next free
   number.
2. There is no status field. Every RFC here is the current design and is
   binding. When implementation finds a better design, update the RFC.
3. An RFC states the design, not what is left to do. Open questions and
   deferred work live outside it.
4. Code never cites an RFC. RFCs name the code they describe.

Design sessions write their output directly here as RFCs.

## Index

Start with the storage format, the commit protocol, the read path, and the
extension surface; the rest builds on those. The public API matters to
Rust embedders.

### Core

- [Storage format](0001-storage-format.md) — key layout, value framing,
  format versions, migration
- [Public API](0002-public-api.md) — the core crate's types, operations,
  and errors
- [Commit protocol](0003-commit-protocol.md) — atomic commits, conflicts,
  retry, group commit, the write-ahead log

### Catalog features

- [DuckLake feature mapping](0005-ducklake-feature-mapping.md) — schema
  evolution, partitioning, name mapping, macros, encryption, change feed
- [Data inlining](0004-data-inlining.md) — small writes stored in the
  catalog and flushed to Parquet
- [Equality and range indexes](0009-equality-indexes.md) — index entries,
  canonical encoding, builds, lookups

### Reads and durability

- [Read path, caching, and memory](0007-read-path-and-caching.md) — read
  revisions, the block cache, row summaries, working memory
- [Crash recovery](0008-crash-recovery.md) — what must hold after a
  process dies mid-operation
- [Testing](0012-testing.md) — the guarantees under test, how crashes and
  fences are staged, fuzzing, the suite against real DuckDB

### Integration and operation

- [Extension surface](0006-extension-surface.md) — the DuckDB extension:
  attach, options, functions, the C ABI, runtime
- [Maintenance](0010-maintenance.md) — expiry and compaction translation,
  the scheduled pass, sweeps
- [Data root relocation](0013-data-root-relocation.md) — relocating a copied
  catalog to a new data root

### Internal

- [Commit-log seam](0011-commit-log-seam.md) — the boundary an external
  commit log plugs into; not published on the site

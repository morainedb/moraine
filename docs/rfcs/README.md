# RFCs

Design records for moraine. An RFC is required for decisions that are
expensive to reverse: the key layout, the commit protocol, the public API.
Each one states the current design and the constraints a reader must
respect. It is not a history, a plan, or a measurement log.

## Process

1. Copy `0000-template.md` to `NNNN-kebab-title.md` using the next free
   number.
2. There is no status field. Every RFC here is the current design and is
   binding. When implementation finds a better design, update the RFC.
3. An RFC states the design, not what is left to do. Open questions and
   deferred work live outside it.
4. Code never cites an RFC. RFCs name the code they describe.

Design sessions write their output directly here as RFCs.

## Index

| RFC | Topic | Covers |
|---|---|---|
| [0001](0001-storage-format.md) | Storage format | Key layout, value framing, format versions, migration |
| [0002](0002-public-api.md) | Public API | The core crate's types, operations, and errors |
| [0003](0003-commit-protocol.md) | Commit protocol | Atomic commits, conflicts, retry, group commit, the write-ahead log |
| [0004](0004-data-inlining.md) | Data inlining | Small writes stored in the catalog and flushed to Parquet |
| [0005](0005-ducklake-feature-mapping.md) | DuckLake feature mapping | Schema evolution, partitioning, name mapping, macros, encryption, change feed |
| [0006](0006-extension-surface.md) | Extension surface | The DuckDB extension: attach, options, functions, the C ABI, runtime |
| [0007](0007-read-path-and-caching.md) | Read path, caching, and memory | Read revisions, the block cache, row summaries, working memory |
| [0008](0008-crash-recovery.md) | Crash recovery | What must hold after a process dies mid-operation |
| [0009](0009-equality-indexes.md) | Equality and range indexes | Index entries, canonical encoding, builds, lookups |
| [0010](0010-maintenance.md) | Maintenance | Expiry and compaction translation, the scheduled pass, sweeps |
| [0011](0011-commit-log-seam.md) | Commit-log seam | The boundary an external commit log plugs into |

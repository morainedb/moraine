# DuckLake upstream requests

This document collects changes moraine would like DuckLake to make. These are
not moraine implementation tasks: until a request lands in the DuckLake
version moraine pins, moraine keeps the documented workaround or omits the
dependent feature.

When an upstream change lands, update the owning RFC, add or adjust the
end-to-end coverage, remove the corresponding workaround, and delete the
request from this document.

## Catalog correctness and concurrency

### Make catalog-cache refresh thread-safe

After a write, a fresh attach can return an empty catalog listing when DuckDB
uses more than one thread. The failure reproduces with a plain
DuckDB-file-backed DuckLake catalog and does not involve moraine. At the
currently pinned DuckLake revision, 23 of 40 runs reproduced it after a plain
`CREATE TABLE`.

The upstream request is that a fresh attach observe committed catalog state
reliably with `threads > 1`. Once that holds, moraine can remove its
`SET threads=1` end-to-end workaround and invert the presence test that
currently guards the known failure.

Owner: [RFC 0006](rfcs/0006-extension-surface.md), with the cache-consistency
boundary also recorded in [RFC 0009](rfcs/0009-reader-consistency-and-caching.md).

### Admit concurrent metadata committers

DuckLake's serialized metadata connection currently leaves no independent
committer for moraine's staged-row path to batch with. The upstream request is
a supported concurrency model in which independent metadata transactions can
commit concurrently and resolve conflicts through DuckLake's commit protocol.

Once available, moraine can revisit allowing a staged-row transaction to lead
a batch containing compatible commits. This is an optimization; correctness
does not depend on it.

Owner: [RFC 0004](rfcs/0004-commit-protocol.md).

## Scan pushdown

### Push filename filters into the file list

DuckLake currently discards filters on `rowid`, `filename`, and `file_index`
before they reach the file list. Consequently an external index lookup opens
every live file footer before its row-id filter rejects unrelated rows.

A row id is not a physical locator. Rewrites preserve logical row ids while
moving rows, and non-adjacent rewrites may store sparse absolute row ids in the
replacement file. `file_index` is unsuitable too: its value changes when
pruning changes the file-list order. `filename` is the existing stable
per-snapshot locator and is already what DuckLake uses for late
materialization.

The upstream request is to apply static and dynamic equality/`IN` filters on
the `filename` virtual column while constructing the DuckLake file list, before
opening Parquet readers. An external index can then resolve its current
`data_file_id` to a filename and join on `(filename, rowid)`, while the ordinary
DuckLake scan continues to own snapshots, deletes, schema mapping, encryption,
and inlined data. Dense positional files may additionally use safe row-id
range pruning, but correctness must not infer a dense interval for a file with
embedded row ids.

This directly determines the read-side value of a moraine equality index:
moraine resolves an indexed value to the row's current file and logical row
id, but only DuckLake can avoid opening unrelated data files afterward.

Owner: [RFC 0013](rfcs/0013-partitioning-sorting-and-pruning.md).

### Push filters into every scan mode

Filter pushdown is currently gated to `SCAN_TABLE`. `SCAN_INSERTIONS`,
`SCAN_DELETIONS`, and the scan used by flush therefore receive no filters,
including filters on ordinary stored columns.

The upstream request is to pass applicable filters through all scan modes and
let their file lists prune with the same semantics as an equivalent table
scan. This would let change-feed reads and flushes avoid unrelated files.

Owner: [RFC 0013](rfcs/0013-partitioning-sorting-and-pruning.md).

## Index integration

### Provide supported native-index extension points

DuckLake currently rejects `CREATE INDEX`, `PRIMARY KEY`, and `UNIQUE` in its
binder and has no native index metadata. Moraine therefore exposes explicit
`moraine_index_*` functions and performs index maintenance itself. Carrying a
downstream binder and optimizer fork is not the intended integration path.

The upstream request is a supported index contract covering:

- catalog metadata and binding for indexes and enforced uniqueness;
- a stable identifier that moraine can map to its existing `index` key range;
- writer-supplied additions and removals so the storage extension can maintain
  index entries in the same commit as data-file changes; and
- planner hooks for equality, comparison, and `ORDER BY` routing, including
  scan direction and declared NULL ordering.

Once DuckLake defines that contract, moraine can map the upstream identifier
through its reserved `ducklake_index_id` field and serve the same physical
index through native syntax and optimizer pushdown. Until then, explicit SQL
functions remain the supported surface.

DuckLake currently documents indexes and enforced key constraints as
unsupported features that are unlikely to be added. This request should
therefore be revisited only if DuckLake changes that position or offers a
general external access-method API.

Owner: [RFC 0016](rfcs/0016-equality-indexes.md). See also DuckLake's
[unsupported-features documentation](https://ducklake.select/docs/stable/duckdb/unsupported_features).

## Requests owned by other projects

SlateDB requests are intentionally tracked in [`slatedb.md`](slatedb.md).
Moraine-local choices such as index-entry caps and explicit index scan
behavior remain with their RFCs.

# RFC 0023: Published file row summaries

- **Date:** 2026-09-29 (binary encoding and its measured cost settled 2026-09-30)

## Summary

A data file's **row summary** — exact row-id membership and the positions those
ids sit at — is derived by reading the file's row-id column and cached per
process, so every process pays the derivation again. This publishes it as one
immutable **sidecar** per data file, in the encoding the auxiliary cache's disk
tier already uses, derived once for the deployment.

The sidecar's two halves are fetched separately. A lookup needs membership
only — **1.25 MB and 1 ms against the 5.21 MB and 60 ms it replaces** — and
pays for positions only on the paths that resolve them.

It applies **only to files carrying embedded row ids** — flush outputs, update
rewrites, and compaction outputs that preserved ids. Ids that follow from
`row_id_start` and a record count are answered from catalog metadata for
nothing. A sidecar is never invalidated, the files being immutable; never
required, a missing one falling back to today's read; and never named
`*.parquet`, which DuckLake deletes.

## Goals

- A cold process answers a lookup over an embedded-row-id file without reading
  that file's row-id column, once per deployment rather than once per process.
- A lookup does not pay for positions it will not use.
- No invalidation protocol: a sidecar matches the file it describes, or is
  absent — and absent, truncated or corrupt, the answer is today's.
- No change to how a lookup is planned or served. The sidecar is a cheaper
  source for the summary the directory already builds.
- A sidecar never outlives its file by more than a sweep.

Non-goals: speeding up a cache **hit**, this being about the cost of a miss;
delete-file positions and inlined data, which have their own shape and lifetime;
a user-facing surface; and files moraine cannot read, or whose ids are derivable.

## Design

### What a summary is

`PositionedRowSet` pairs a set — `Range`, `Roaring(RoaringTreemap)` or
`Sorted(Vec<u64>)` — with an order: `Ascending` (position is rank),
`Permuted(positions)`, or `Repeated { offsets, positions }` — an id at several
positions, which a flushed inline file produces because its chunks retain
historical versions of a row ([RFC 0016](0016-equality-indexes.md)). It is
derived by a scoped read of the row-id column, cached under
`{store identity, table_id, data_file_id, path, file_size}`.

The two halves have different consumers. `FileDirectory::place` — every
lookup — calls only `contains`, the set. Positions are resolved separately, by
`visit_positions`, and only for a located delete or update. `FileSummary`
therefore carries its order **optionally**, so a summary can be resident and
useful with membership alone.

### The format

The sidecar is the auxiliary cache's disk-tier encoding, published: a fixed
header, then the set, then the order. The disk tier adopts the packed order
below under new tags, so the two forms are byte-identical and a sidecar fetch
fills the disk tier by copying rather than re-encoding. The tags they supersede
stay readable, so adopting them does not cold-start the tier.

| | contents |
| --- | --- |
| header | version, `table_id`, `data_file_id`, the file's path as the reader resolved it, `file_size`, `row_count`, and the byte length of each half |
| set | a tag byte, then `RoaringBitmap::serialize_into` or a sorted `u64` run |
| order | for `Permuted`, the rank-to-position permutation; for `Repeated`, the physical row count, then offsets, then positions |

Positions are **bit-packed at `ceil(log2(record_count))` bits**, replacing the
`u32` the disk tier writes today, because a permutation of `0..n` never needs
more — 20 bits at a million rows, which is 2.5 MB rather than 4 MB. `Ascending`
writes no order at all, position being rank. A writer refuses a position its
width cannot hold rather than truncating it, so a corrupt order cannot be
written, only read from an older one.

Fixed width is not only a size choice: it makes the order **randomly
addressable**. `position = order[rank]`, so entry `rank` is at
`order_start + rank·w/8` by arithmetic, with no index to consult. A caller
resolving a handful of positions reads a few bytes each, never the 2.5 MB.
`Repeated` costs one extra hop — `offsets` is fixed-width and addressable, the
`positions` it delimits are not.

A reader takes the header and the set in one ranged GET, speculatively sized and
extended if the header says the set runs longer.

The set must answer **`rank`**, not only `contains`: rank is what indexes the
order. Roaring answers it from per-container cardinalities and a sorted run by
binary search, so both encodings qualify and the tag says which one a writer
used — a reader that would have chosen differently re-encodes after decoding.
An encoding that answers membership but not rank does not qualify.

### Where it sits

A sidecar is the data file's path with `.rowsum` appended, reached with the
credentials that already reached the data file and found without a catalog
lookup. The suffix matters, because DuckLake's orphan cleanup is, verbatim:

```sql
SELECT filename FROM read_blob({DATA_PATH} || '**') files
WHERE suffix(filename, '.parquet')
  AND NOT EXISTS (SELECT 1 FROM known_files WHERE known_files.full_path = files.filename)
```

Any `.parquet` object under the data path that DuckLake does not know is deleted
by an ordinary `ducklake_delete_orphaned_files`, and a sidecar will never be in
that list. Two consequences accepted rather than solved: the scan `read_blob`s
everything before filtering, and a shared data path is subject to whatever else
sweeps there.

### Identity, and why nothing is invalidated

A data file is immutable — deletions become delete files, rewrites produce new
files with new ids — so a summary of one is correct forever. A reader checks the
header's `data_file_id`, `file_path` and `file_size` against the file described.
A mismatch, an unknown version, a short read or any decode error means
**absent**: log once, fall through.

### Who writes and deletes one

Two producers:

1. **The reader that derived it**, publishing after answering its caller on a
   task the caller does not wait for; a failed publish is logged and otherwise
   ignored.
2. **Warming**, which walks a table's files deriving summaries and so publishes
   whatever the readers have not — the producer for files that predate this or
   whose publish failed.

**Not the commit**, though its scoped read passes the ids by. That read is
filtered by the file's delete positions, and a summary describes a file
physically, deleted rows included, so what the commit sees is the wrong set. A
commit-time producer would need a second, unfiltered read, which is not the free
by-product it looks like.

There is no write-time producer either, because moraine writes no data file:
`Transaction::flush_inlined_data` registers files the caller already wrote.
Publishing needs a resolvable data path, the condition that already governs
delete files; an absent one publishes nothing and is not an error, and
concurrent publishes are safe, the contents being a deterministic function of
the file. An **encrypted lake publishes nothing and loses nothing by it**: the
`parquet` dependency does not enable that crate's `encryption` feature and
nothing in `data_file` decrypts, so moraine cannot read an encrypted file's
row-id column in the first place ([RFC 0014](0014-encryption.md)).

A sidecar dies with its data file. moraine never deletes a data file — DuckLake
schedules and removes them — so the reclaim is a sweep: one listing of the data
path, and every `.rowsum` whose file that listing did not also find is deleted.
Deciding from the listing alone, rather than from the catalog, keeps a summary
whose file an older snapshot still reads. A leftover is garbage, never
corruption: its header names a `data_file_id` nothing matches.

### Read path

1. **Dense files** — `row_id_start` and a record count, from catalog metadata.
   No read.
2. **The cached summary** — the auxiliary cache: memory, then the local-disk
   tier.
3. **The sidecar** — its set, and only the order entries a caller naming
   positions asks for.
4. **The data file's row-id column**, as today — then publish.

Each step is a strict fallback, and step 3 yields what step 4 would have built —
the same set, and the same order when the caller asked for one. Nothing
downstream changes: `file_summary` gains a source, and `place` stays synchronous
over resident summaries.

A caller says which it needs. A lookup wants membership, and its summary is
cached apart from a whole one so neither evicts the other; a located delete or
update wants positions, and a membership summary never answers it. Metrics
count sidecar hit, miss, rejected-on-identity and publish outcome, so a
deployment can tell "none published yet" from "published and refused".

### Memory

A resident summary never consults a sidecar, so this adds nothing to a hit. It
changes the cost of a **miss**, and of an eviction, from 5.21 MB to 1.25 MB for
a lookup. The summary share of the cache budget stays charged per file, as
today; what shrinks is the penalty for getting the size wrong.

Resident positions stay `Vec<u32>`: `position_of` is a per-row call on the
located-delete path, and holding the packed buffer instead would trade
nanoseconds there for a share of the budget nobody has measured — most files are
small or ascending, which stores no order at all. Worth revisiting once resident
order is measured.

A lookup that needs no positions admits a membership-only summary, which is also
smaller resident. A later located delete against the same file reads only the
order entries it names — bytes, not megabytes — and admits the whole order only
if something wants it resident.

### Cost

One object per embedded-row-id file — the population whose summaries are
expensive today, and a minority in an append-mostly lake. About 3.75 MB for a
million rows, **19%** of the data file it describes, against the 41% a Parquet
form would cost, and two thirds of it is order that a lookup never transfers.
The producer that already holds the ids pays nothing for the write.

## Measurements

One million sparse row ids over a ten-million range, written as a data file (six
user columns, ids in **file order** — an update rewrite's shape). Asked for one
id, **all nine** of the data file's row-group statistics admit it: file order is
not id order, so the whole row-id column must be read. That read is 5,209,534
bytes of column plus a 156,578-byte footer.

What a whole-summary load costs from each source:

| source | bytes | CPU |
| --- | --- | --- |
| data file's row-id column (today) | 5,209,534 | **58–63 ms** — Parquet decode, sort file order into id order, build the set |
| sidecar, set only | **1,254,136** | **0.8–1.0 ms** |
| sidecar, set + bit-packed order | ~3,754,200 | **2.7–3.4 ms** |
| sidecar, set + `u32` order (disk tier as written) | 5,254,145 | 2.7–3.4 ms |
| Parquet sidecar, both columns | 8,158,483 | Parquet decode ×2, build the set |

Bytes are measured except the bit-packed order, which is arithmetic at 20 bits
per position. The Roaring figure is built as moraine builds it: per-high-word
`RoaringBitmap`, each `optimize()`d, 153 containers. Times are from a program
replicating that work rather than the crate's own path, three rounds, release;
today's figure excludes the Parquet decode it sits on top of, so it is a floor.
Publishing costs 3.8–5.0 ms.

**A lookup is 4.2× cheaper on bytes and 60× cheaper on CPU**, and never fetches
the order. A located delete is 1.4× and 20× if it loads the whole order, which
is its ceiling: naming a few rows, it reads a few bytes each.

The CPU is the larger finding, and nothing before this accounted for it. A
locate fanning out over the 353 files one production statement touched spends
**about 21 core-seconds** deriving summaries — a cost that no cache size
removes, that bandwidth does not shrink, and that survives on a deployment whose
data is local. Decoding the same summaries costs 0.35 core-seconds.

The sidecar is also a cheaper thing to *evict*: 1.25 MB to refetch rather than
5.21 MB and 60 ms to re-derive.

### The order is at its floor; the set is not

Bit-packing the order at `ceil(log2(record_count))` bits is 2,500,000 bytes
against a `log2(n!)` bound of 2,311,108 — within 8%, so no encoding wins more.
The set has room: Roaring's 1,254,136 sits against an entropy bound of 586,250,
and Elias-Fano reaches about 687,500 (see Alternatives).

The order's floor is a floor for a **uniformly random** permutation, which is
the worst case and probably not the real one: an update rewrite copies surviving
rows in file order, so its permutation should be a merge of a few monotone runs,
and with `k` runs the order tends toward `n·log2(k)` — about 415 KB at `k = 10`
rather than 2.5 MB. One real production file settles it; more arithmetic does
not.

### What Parquet would buy, and what it needs first

A Parquet sidecar at 65,536-row row groups and 1,024-row pages answers one id
against one file in 15,636 bytes — footer 3,558, one row group's page index
3,732, one page pair 8,346 — whether the answer is yes or no, and **without any
of the file's set resident**.

That last clause is the whole of what it buys. Resolving a *position* needs no
Parquet: given a resident set, rank is O(1) in memory and the position is three
bytes at a computed offset. What Parquet does that the blob cannot is answer
**membership without residency**, which matters exactly where the blob is
weakest — a locate fanning across hundreds of files, most of which do not hold
the id, each costing 1.25 MB to find that out. At the 353 files one production
statement touched that is 443 MB against 5.5 MB.

It is unreachable now. `file_summary` takes no row ids; it returns a whole
summary or builds one. `FileDirectory::place` answers arbitrary ids
synchronously from `spanned`, which holds fully resident summaries. Nothing in
the lookup path can ask storage about one id, so a Parquet sidecar would only
ever be read wholesale — 8.16 MB against 5.21 MB, worse than doing nothing.

Reaching it means an id-directed lookup: the directory holding spans rather than
summaries, `place` becoming asynchronous, and `file_summary` gaining a variant
that takes the ids asked about. That is an architecture change, and the
successor to this RFC rather than part of it. What narrows its value first is
file-level pruning: `narrow_to_wanted` already drops files whose recorded row-id
bounds exclude the id, so the population Parquet would save is the files those
bounds admit and the set refutes. How large that is has not been measured.

### Ordering the row-id column instead

The data file itself `ORDER BY row_id`, deriving the summary from its own
statistics rather than storing it:

| | file size | `row_id` column | worst row group | locate |
| --- | --- | --- | --- | --- |
| data file, file order | 19,750,846 | 5,209,534 | 640,261 | **5,366,102** |
| data file, `ORDER BY row_id` | 23,944,918 | 4,041,843 | 496,696 | **653,266** (8.2×) |

It costs +21% and no new object, and deletes positions outright because position
becomes rank. The +21% is the punishing end, the synthetic columns being
monotonic in position; the cost that matters is unmeasured — ordering widens
every user column's statistics, against the pruning ordinary queries depend on.

### A bloom filter on the row-id column

Parquet's own membership structure, and it does not pay. DuckDB builds filters
in the dictionary path, so a unique column gets none (`PLAIN`, 1,045,852 bytes);
forcing one with `DICTIONARY_SIZE_LIMIT` costs +78% for 262,161 bytes per
122,880-row group. A filter is sized per element — 2.13 bytes per value at 1%
false positives — and there is one per column chunk, so probing one id reads
every group's filter: 2.13 MB against 5.21 MB, and it answers neither position
nor exact membership.

## Alternatives considered

- **A Parquet sidecar.** Measured above: membership without a resident set, at
  15,636 bytes a file — but unreachable until lookups are id-directed, and
  8.16 MB, worse than doing nothing, until then. The successor to this RFC, not
  a competitor to it.
- **Order the data file's row-id column, and publish nothing.** 8.2× for +21%
  and no new object. It must hold **inductively**, one writer dropping the order
  returning that file to the expensive branch, so it needs a commit-time hold
  like the one `row_id_invariants.rs` applies to the ids, and it competes with
  DuckLake's sort spec
  ([RFC 0013](0013-partitioning-sorting-and-pruning.md)). It is nearly free for
  exactly one population: inlined rows draw ids from the counter in order, so a
  flush preserving that order writes an ascending file needing no sidecar.
- **One object per half.** Membership and order as separate objects, so a
  lookup's fetch needs no speculation. Rejected for doubling the object count
  and the sweep, to save one ranged GET on the rarer path.
- **Publish the `u32` permutation unchanged**, as the disk tier writes it. Less
  work, and a wash on bytes against reading the data file — the saving would be
  CPU alone.
- **Elias-Fano for the set.** 625,000 bytes near the entropy bound, against
  Roaring's 1,254,136 — but this design needs `rank`, and Elias-Fano's native
  operation is its inverse, `select`. Rank is a predecessor query needing rank
  support on the high-bits vector, about 62,500 bytes more, so the comparison is
  1.8× rather than 2×. Roaring's `rank` is native, and it is already the
  in-memory type and a dependency: no new code, decoding in under a millisecond.
  Revisit if the set half comes to dominate. Delta-plus-varint sits between them
  at 1,000,000 bytes but answers rank only by decoding.
- **Publish the set alone**, leaving located writes to read the data file. Saves
  13% of the data file in storage and costs the positions path its entire
  improvement — 60 ms of derivation per file instead of 3 ms of decode — while
  saving a lookup nothing, since a lookup never fetches the order anyway.
- **Embed the summary in the data file.** Bytes in a data file's body are a
  patch to **DuckDB's Parquet writer** — `DuckLakeInsert` plans a
  `PhysicalCopyToFile` over DuckDB's own copy function, as do the flush, update
  and compaction paths — a repository moraine patches zero times, against ten in
  `patches/ducklake`. It could never cover a file already written, and DuckDB's
  `kv_metadata` option is no way round it: bound before the write, and
  footer-resident, where the whole summary would be parsed with every footer.
- **Keep summaries in the SlateDB keyspace.** Write amplification in the commit
  path and growth in a keyspace whose scan cost is already watched, to hold
  per-file immutable blobs every compaction would rewrite forever.
- **Name sidecars `*.parquet`.** Deleted by `ducklake_delete_orphaned_files`,
  silently, on any lake where someone runs cleanup.
- **Store sidecars under the store's own prefix.** Safe without a suffix gate,
  but it splits a file's bytes across two credentials and two lifetimes; revisit
  if a deployment cannot tolerate extra objects under its data path.

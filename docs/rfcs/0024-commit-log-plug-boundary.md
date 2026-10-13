# RFC 0024: Commit-log plug boundary

- **Date:** 2026-10-13

## Summary

A bucket-resident commit log — slot objects written with create-if-absent
conditional puts, with the SlateDB store as their fold — is designed and
implemented in a separate library under its own license. This RFC fixes
what moraine owns so that library can be linked in without a fork: a
provider trait that hands the catalog SlateDB write-ahead-log plugs, one
process-wide registry plus a per-open override, a format stamp that marks a
store as log-resident, and the refusal an open gives such a store when no
provider is present. Moraine never depends on the library; the crates.io
build stays complete on the single-writer path of RFC 0004.

## Goals

- **Link, do not fork.** An enterprise build is the open extension plus the
  library plus a few lines of glue. No source in this repository changes
  per deployment.
- **The open build is complete alone.** Every test, the DuckDB extension,
  and every single-writer deployment work with no provider. The boundary
  is covered by a real in-tree implementation under test, not by the
  library.
- **Nothing silent.** A store stamped for the log is refused by a binary
  without a provider, naming what it needs. A provider on a path this
  version cannot drive is refused, not ignored.
- **Public API.** The trait, the registration function, and the option are
  part of the crate's semver contract.

Non-goals:

- The protocol. Envelopes, the slot race, hole detection, truncation, and
  the fold loops are the library's; this RFC never names a slot.
- The committer. Writing slots instead of the store's own log changes the
  commit path, the in-process coalescer, and id allocation. That lands as
  its own change under this boundary; until it does, a read-write open
  with a provider is refused.
- Extension signing and distribution. An enterprise extension loads under
  DuckDB's unsigned-extension setting from a private repository; that is
  the enterprise repository's concern.

## Design

### The provider

```rust
pub trait CommitLogProvider: fmt::Debug + Send + Sync + 'static {
    fn writer_init(&self, log_store: Arc<dyn ObjectStore>, path: &str) -> Box<dyn WriterInit>;
    fn reader(&self, log_store: Arc<dyn ObjectStore>, path: &str) -> Arc<dyn WalReader>;
}
```

`WriterInit` and `WalReader` are SlateDB's pluggable write-ahead-log
traits, which shipped in SlateDB 0.16 and 0.17. Moraine re-exposes nothing
of its own for them: the plug *is* SlateDB's interface, so the library
targets one contract rather than two, and moraine's store layer stays
unaware of what a slot is.

Both methods receive the object store the catalog records as its **log
store** — the catalog store itself unless a separate one was configured
through `CatalogOptions::wal_store` — wrapped in the catalog's retrying
transport, and the catalog's path within it. The log therefore lives where
RFC 0004 already lets the log live, including on S3 Express One Zone, and
`move_wal_store` keeps its meaning.

`writer_init` returns a fresh box per call because SlateDB consumes it at
build; `reader` returns a shared handle because a reader is cloned into
every read session.

### Supplying a provider

Two routes, one precedence rule:

1. **Per open.** `CatalogOptions::commit_log: Option<Arc<dyn
   CommitLogProvider>>`. An embedding host that constructs the library's
   provider passes it here.
2. **Per process.** `moraine::register_commit_log(provider)` stores it in a
   `OnceLock`. A second registration is `Error::Configuration`: one
   process, one library. This is how the DuckDB extension is reached — it
   builds `CatalogOptions` from attach options and cannot construct a
   private crate's provider, so the enterprise glue crate registers one at
   load, and every attach picks it up.

An open uses the option when set, else the registration, else nothing.
There is no way to say "ignore the registration" for one open, because no
deployment mixes log-resident and single-writer lakes in one process on
purpose; a stamped store refuses the wrong path anyway.

### What the plug reaches

- **Read-only open.** The `DbReader` is built with
  `with_wal_reader(provider.reader(log_store, path))`. The reader follows
  the folded store through its manifest and tails the log through the
  plug. Nothing else in the read path changes.
- **Read-write open.** Refused with `Error::Configuration` while the
  committer is absent. The alternative — opening the writer over the plug
  and letting commits go through SlateDB's own write path — would land
  every commit in the memtable only and never in a slot, which every other
  process reads. The refusal happens before any store opens, so a refused
  open fences nothing.
- **Maintenance, migration, and the log-store move** open stores without
  consulting the provider. They reach a log-resident store only through
  the format check below, which refuses them.

### The format stamp

`FORMAT_WITH_COMMIT_LOG = 10` joins the structural format ladder of RFC
0002. A store at that version keeps its commits in the log; the stamp is
written by the library's migration when a store adopts the log, and never
by this repository. `validate_format` refuses it before the newer-than-max
check with a message naming the commit-log library, on every open path, so
an older or provider-less binary never commits past the slots. The maximum
format this binary understands stays at 9: this version does not *drive*
format 10, it only recognizes it.

### Errors

- `Error::Configuration` — a second registration; a read-write open with
  a provider in this version.
- `Error::Migration` — a format-10 store opened without the library, in
  keeping with every other format refusal.

### Test obligations

- The boundary is tested in-tree with a provider over an empty log: a
  read-only open asks the provider for its reader by the catalog's path
  and resolves the log's end through it; a read-write open with a
  provider is refused before the provider is consulted; a store stamped
  10 is refused by both open paths with the library named.
- The registry is global, so its test is its own binary: one registration
  succeeds, a second is refused, a read-write open after it is refused, an
  open with no provider uses the registered one, and an open carrying its
  own never consults it.
- No test in the unit-test binary registers a provider, since every other
  test's reader would inherit it.

### What the committer adds

The next change under this boundary makes a read-write open with a
provider work. It is specified here so the boundary is not re-cut when it
lands; it is **unimplemented**.

- **Two kinds of read-write process.** SlateDB fences its `Db`, so only
  one process — the folder — holds one. Every other committer runs over a
  `DbReader` plus an in-memory overlay of the slots past the fold, reads
  through that overlay, and commits directly to the log. The catalog's
  transaction machinery therefore has to prepare and validate a change
  set against a reader view, not only against a `DbTransaction`. That is
  the bulk of the work, and the reference implementation is the
  `slot_commit`, `folder`, and `coalesce` modules of pull request 81,
  minus the leader.
- **The committer plug.** `CommitLogProvider` gains a committer whose
  contract stays protocol-free: it takes a transaction id, the validated
  head, the `changes_made` string, and the staged writes as opaque key
  and value bytes; it answers *won at sequence* or *lost, with the
  winning slots' writes to fold before a retry*; and it reports the last
  sequence it has folded. Envelopes and the race stay the library's.
- **Where it plugs in.** `submit_batch` offers the batch to the committer
  instead of committing the SlateDB transaction. A win applies the batch
  locally and reports the sequence folded, so the next memtable freeze
  records it as the manifest's replay point — SlateDB takes that point
  from the plugged writer's flushed id. A loss folds the winners' writes
  into the local store and returns the existing lost-race outcome, so the
  library path re-prepares with fresh ids and the DuckDB path surfaces
  the conflict DuckLake retries on, unchanged.
- **Durability and visibility.** A commit is durable at its slot, so a
  submission waits on no flush. The store keeps journaling through the
  plug, which acknowledges each append at once and writes nothing — the
  slot is the durable record — so the paced flush that keeps readers
  current is an L0 write rather than a WAL write. Rows the folder writes
  for itself outside a slot are durable only at that flush and must be
  re-derivable after a crash.
- **Adoption.** The first read-write open with a provider on a format-9
  store commits the format-10 stamp as the first slot. A store created
  with a provider bootstraps on the single-writer path, flushes, then
  adopts the same way.
- **Readers.** `reader` gains a flag for tail replay; the default stays
  folded-only.

## Alternatives considered

- **A cargo feature with an optional dependency on the library.** The
  library is private; a feature that names it makes the open crate
  unbuildable from crates.io for anyone without access. Rejected.
- **A runtime-loaded plugin over the C ABI.** Would let the signed
  community extension load the log at attach time. The plugs are async
  Rust traits whose futures and row types cross no C boundary cleanly;
  the wrapper would be larger than the library. Rejected in favor of a
  statically linked enterprise extension.
- **A link-time registry (`linkme`, `inventory`).** Saves the glue crate
  one function call at load, at the cost of a dependency and link-section
  behavior that differs across the static-library toolchains the
  extension builds under. A `OnceLock` and a public function suffice.
- **Options only, no registry.** Clean for embedding hosts; unreachable
  from the extension, which cannot construct the provider. Both routes,
  with the explicit one winning.
- **Letting a read-write open with a provider proceed now.** Commits would
  bypass the log silently. Refusing until the committer lands is the only
  honest state.

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
- **Read-write open.** The `Db` is built with
  `with_wal_writer(provider.writer_init(log_store, path))`; see The
  committer for what follows.
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

- `Error::Configuration` — a second registration.
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
- The folder path is tested over the in-tree log: adoption lands the
  stamp as the first slot, a commit rides the next slot and a tailing
  reader serves it before any flush, a lost race folds the peer's slot
  before the retry lands after it, and an adopted store needs a provider
  to open.
- Two processes over one in-tree log, each with a cursor of its own: a
  committer over a reader lands its batch as a slot and sees it once
  served, the folder's next commit loses to it, folds it, and lands after
  it, and the committer is served the folder's slot in turn.
- A committer arriving at an unadopted log adopts it and holds the store;
  one arriving at a long tail takes the store at attach and has replayed
  it; a holder fenced by another process gives the store up and its next
  commit lands through the log.
- A committer's session records every write it is handed and commits none
  of it into the store it reads.
- No test in the unit-test binary registers a provider, since every other
  test's reader would inherit it.

### The committer

The provider also hands the catalog a **committer**, whose contract stays
protocol-free: it takes a transaction id, the validated head, the
`changes_made` string, and the staged writes as opaque key and value
bytes; it answers *won at sequence* or *lost, with every slot landed from
the raced sequence on, to fold before a retry*; it reports whether the log
is adopted, races a stamp at the first slot to adopt it, takes the
sequence the store has applied through, and lets a committer over a
tailing reader wait for that reader to be served a sequence. Envelopes
and the race stay the library's.

**The folder path is implemented.** A read-write open with a provider:

1. Asks the committer whether the log is adopted. If not, it opens the
   store on its own log first and flushes it into the tree, because the
   plug replays the log *in place of* the store's own and would otherwise
   skip whatever a crashed writer left there.
2. Opens the store over the plug. The store keeps journaling through the
   plug, which acknowledges each append at once and writes nothing — the
   slot is the durable record — so the paced flush that keeps readers
   current is an L0 write rather than a WAL write. Rows the folder writes
   for itself outside a slot are durable only at that flush and must be
   re-derivable after a crash.
3. On a format below 10, adopts: flushes, then races the format-10 stamp
   as the first slot and folds whichever stamp holds it. Slots take WAL
   ids counted from a **base** the plug fixes at adoption — just past the
   store's own WAL — because SlateDB's next WAL id may never move
   backwards; the library records the base beside the slots.
4. From then on `submit_batch` offers the staged writes to the committer
   before committing the SlateDB transaction. A win commits locally and
   reports the sequence folded, so the next memtable freeze records it as
   the replay point. A loss folds the winners into the store and returns
   the existing lost-race outcome, so the library path re-prepares with
   fresh ids and the DuckDB path surfaces the conflict DuckLake retries
   on, unchanged. A local failure after a won slot is reported as an
   unknown outcome: the slot is committed, and a reopen folds it.

**Committers over a reader.** SlateDB fences its `Db`, so only the
folder holds one. Every other read-write process opens with
`CatalogOptions::commit_log_role = Committer`: a tailing `DbReader`
polled at `commit_log_poll_interval`, with a committer from the provider,
and the log must already be adopted. Every batch, on either role, stages
against a **commit session** — the folder's snapshot transaction, or the
committer's reader — and every write it stages passes through that
session, index entries included, so the slot the batch becomes carries all
of it. The session is also where the two roles differ at commit: the
folder's commits into the store and marks the sequence folded; the
committer's commits nothing locally and waits, bounded, for its reader to
be served the sequence, so its next read sees its own commit. A loss
folds the winners into the folder's store, or waits for the committer's
reader to be served them, before the existing lost-race outcome returns.
Folder-only operations — maintenance, migration, the log-store move —
refuse a committer.

**The extension.** `COMMIT_LOG_ROLE` names the attach's part when a
provider is registered: `'committer'`, the default, or `'folder'`.
`COMMIT_LOG_TAIL` makes a read-only attach tail the log, and
`COMMIT_LOG_POLL_INTERVAL_MS` sets the committer's poll. They reach the
core through `moraine_attach_with_commit_log`, whose `commit_log`
argument the older attach entry points pass as null. Without a registered
provider all three are ignored. The folder is a designated process, never
an attach by default: a fleet of sessions that each took the writer would
fence one another.

### The sequence a committer races

A committer's plug reports a slot served when it hands the rows to
SlateDB, a moment before SlateDB applies them to the reader's memory, so
"the sequence after the last slot served" can name a free slot while the
head the commit was prepared against predates a taken one. The sequence
to race is therefore derived from the state the commit was prepared
against, never from the plug: every slot past the first carries exactly
one `sys/head` stamp, the head's batch counter advances by one per slot,
adoption records the counter the first slot stands on in the format
stamp, and a batch races the slot its own stamp's counter implies. A
stale head names a taken slot and loses, which re-prepares correctly.
The served signal remains a hint for when to read again. A folded slot
carrying any other number of head stamps is refused as corruption.
Direct writes the folder makes for itself — the format raise,
maintenance status, the sweep's tombstones — stamp no head, and
migration refuses a log-resident store, so nothing moves the counter
outside a slot.

### Folding, flushing, truncation, and the sprint

- **Flush by slot count.** Under the plug the folder's paced flush writes
  nothing, so the folder writes its memtable out after
  `commit_log_flush_slots` folded slots and then runs SlateDB's collector
  through the provider's plug, truncating the log below the replay point
  (the collector keeps the slot at the replay point itself) once slots
  are older than `commit_log_retention`. A cold open replays at most
  that many slots. The bound is a count because a cold open costs one
  GET per slot, whatever the bytes.
- **The idle poll.** Every `commit_log_fold_poll` the folder lists the
  tail and folds what peers landed, under a guard that also spans the
  folder's own offer and local commit, so the poll never folds a slot the
  folder is in the middle of committing itself.
- **Taking and holding the store.** Every attach is a committer. A
  committer that finds the unfolded tail — counted past the manifest's
  replay point, which moraine reads itself, since the reader plug sees
  only its own in-memory watermark — past `commit_log_sprint_slots` takes
  the store: it opens it as the folder, which replays the tail, and keeps
  it, folding its own wins and peers' slots into it while still reading
  through its tailing reader. It checks at attach and after every loss,
  where the tail's length is already known. A holder that another process
  fences gives the store up and carries on committing; nothing is lost,
  since every slot is durable, and the fence shows at the holder's next
  flush. The first committer to find an unadopted log takes the store and
  adopts it. The threshold sits well above the flush count, so a holder
  doing its job is never taken from. `CatalogOptions::commit_log_role =
  Folder` remains as a hint for a process that should take the store at
  attach regardless of the tail, which a scheduled job wants; nothing
  else needs it. `Catalog::fold_sprint` takes the store for one fold and
  closes, for a job with no session to keep, and a committer that never
  took the store sprints at close if the tail is long.

### The folder's own rows

A replayed slot's rows take the sequence numbers `base + k × rows_per_slot`
for slot `k`, and a replay skips rows below what the tree already holds.
So once an open has replayed a slot, the rows the folder writes for
itself outside any slot — the sweeps — must stay below the next slot's
number, or the next replay after a crash would skip that slot. The plug
reports the headroom as of the writer's open, every fold since buys
another slot's worth, and the sweeps reserve their batch before writing
it: when the headroom runs short the folder commits an empty
head-preserving slot first. Before any replay the headroom is the whole
distance to the first slot's number, which no sweep reaches.

### What remains

**End to end.** Two DuckDB processes on one bucket, one as the folder and
one as a committer, and the attach option's C++ side, which has compiled
only by inspection. The flush, poll, retention, and sprint thresholds
reach the library API only; the extension's attach exposes the role,
tailing, and poll.

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

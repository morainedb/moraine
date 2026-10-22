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

**Committers over a reader.** SlateDB fences its `Db`, so only one
process holds one. Every read-write open on a commit log is a committer: a
tailing `DbReader`
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
Migration and the log-store move refuse a committer; maintenance runs on
one, since its sweeps and status record are head-preserving slots and its
merge is a submission.

**The extension.** A read-write attach with a registered provider is a
committer; a read-only attach is a reader. `COMMIT_LOG_TAIL` makes a
read-only attach tail the log, `COMMIT_LOG_POLL_INTERVAL_MS` sets the
committer's poll. The fold cadence reaches the library API only: every
attach folds, because a lake that nothing folds grows a log without
bound. They reach the core through
`moraine_attach_with_commit_log`, whose `commit_log` argument the older
attach entry points pass as null. Without a registered provider all three
are ignored. `READER_POLL_INTERVAL_MS`, beside them, is the freshness
bound both roles read within, and applies with no provider at all.

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
The one direct write a holder still makes for itself, the format raise,
stamps no head, and migration refuses a log-resident store, so nothing
moves the counter outside a slot.

### Folding, flushing, truncation, and the sprint

- **Flush by slot count.** Under the plug the folder's paced flush writes
  nothing, so the folder writes its memtable out after
  `commit_log_flush_slots` folded slots and then runs SlateDB's collector
  through the provider's plug, truncating the log below the replay point
  (the collector keeps the slot at the replay point itself) once slots
  are older than `commit_log_retention`. A cold open replays at most
  that many slots. The bound is a count because a cold open costs one
  GET per slot, whatever the bytes. A holder that also reads through its
  own tailing reader truncates one flush late: the reader's checkpoint
  pins the range it opened on until its next poll moves it past the
  flush.
- **The idle poll.** Every `commit_log_fold_poll` the holder lists the
  tail past its fold cursor and folds what peers landed, under a guard
  that also spans its own offer and local commit, so the poll never folds
  a slot it is in the middle of committing itself.
- **A committer refreshes, a reader asks.** A committer's read
  refreshes the reader before it is served, so the log is listed when a
  read asks and the read is served what that listing found; a commit
  awaiting its own slot refreshes until the plug reports it served, so
  its wait is round trips rather than ticks. Nothing between reads lists
  anything, and an idle committer makes no request of the log at all.

  A read-only tailing reader wants the same saving and not the same
  freshness: its poll interval is the staleness it promised, so a read
  sets a flag and the next tick lists, which costs a tick it was already
  going to wait. One nothing reads lists nothing. Refreshing before
  every read instead was tried and is wrong here — it doubles the
  requests of a reader that is polling anyway, and the saving only
  arrives if the clock stops, which also stops an in-progress pass
  seeing a commit land.

- **The fold cursor, not the served point.** A holder that reads through
  its own tailing reader is served a slot before the store it holds has
  folded it, so everything it folds starts at the fold cursor, never at
  what its reader was served. Its own win is folded the same way: read
  back from the log together with any gap before it, so the cursor never
  passes a slot the store lacks.
- **Folding is a cadence, not a role.** Every
  `commit_log_fold_slots` slots, the committer that lands one folds: it
  opens the store, applies every slot the store lacks, writes it out,
  truncates the log below the replay point, and closes. The sequence it
  just won is the whole trigger, so nothing is measured and nothing is
  listed to decide, and the work falls where the commits are. A fold that
  fails is logged and let go — the commit has landed and its slot is
  durable, the next multiple folds what this one did not, and a process
  without permission to write the store simply never succeeds at one.
  Nothing is elected, nothing is resident, and a lake needs no process
  beyond the ones already committing to it. Delta Lake's checkpoints are
  the same shape: the writer that lands the nth commit writes the
  checkpoint, and a checkpoint that fails does not fail the commit.

  Acquiring the store and keeping it was tried first and is worse in
  every deployment that has more than one writer: it elects an arbitrary
  pod to be stateful, it has no liveness story, two of them fence each
  other, and the acquisition lands inline on whichever commit happened to
  lose a race. A process can still take the store outright with
  `Catalog::take_store`, which holds it until close, for a job that
  writes into the store directly rather than through slots;
  `Catalog::fold_sprint` is the fold alone, for a scheduled drain; and
  `Catalog::unfolded_slots` reports how far behind the store is, in one
  listing and folding nothing. None of the three is on the commit path.
  Adoption is a fold too: the first committer to find an unadopted log
  stamps it and gives the store straight back. A read-only open is a
  reader and never commits.

### The folder's own rows

A replayed slot's rows take the sequence numbers `base + k × rows_per_slot`
for slot `k`, and a replay skips rows below what the tree already holds.
So once an open has replayed a slot, any row the folder wrote for itself
outside a slot would have to stay below the next slot's number, or the
next replay after a crash would skip that slot. The folder writes none:
the sweeps and the status record are head-preserving slots, and the
format raise, the one direct write left, is a single row from an
operator's verb, well inside the headroom any fold leaves.

### What remains

**End to end.** Two DuckDB processes on one local store, one holding and
one folding, each seeing the other's slots and a pass from the second
fencing neither, run in the library's repository against the extension it
builds. The same against a bucket, and the attach option's C++ side, which
has compiled only by inspection, remain. The flush, retention, and sprint
thresholds reach the library API only, the fold cadence with them; the
extension's attach exposes tailing and both polls.

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

## Per-open progress and held writers

`CommitLogProvider::session` creates a provider scoped to one open. Stateful
providers return fresh state; stateless implementations may return `None` to
retain the supplied provider. A catalog obtains its reader and committer from
the same session. Each writer open creates another session, and a promoted holder
retains that writer's committer alongside its `Db`. Its tail reads and applied
cursor updates use the held committer, while reader catch-up uses the catalog's
reader session. A fenced writer must never advance its replacement's replay point.

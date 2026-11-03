# RFC 0027: Store-shaped commit-log boundary

- **Date:** 2026-11-03

## Summary

RFC 0024 fixed the boundary between moraine and the commit-log library at
the slot: the library hands the catalog SlateDB's write-ahead-log plugs and
a committer that races one sequence, and the catalog drives the loop above
them — when to offer, what to fold after a loss, when to flush and
truncate, which process takes the store and when it gives it back. That
loop has since been written a second time, in the library, as a write any
SlateDB embedder shares, and the library's copy is the one simulated under
faults. Two loops is one too many. This RFC moves the boundary up: the
library hands the catalog a **commit store**, the shared write itself, and
the catalog keeps only what is the catalog's — the sequence a batch
implies, the stamp, the conflict it surfaces, and how it opens its own
store. Nothing on disk changes, and the open build stays complete alone.

## Goals

- **One loop.** Offers, folds after a loss, the fold cadence, flushing and
  truncation, the idle poll, taking the store and giving it back, and
  retries live in the library and nowhere else. moraine's commit path
  calls them; it does not reimplement them.
- **Parity.** Every behaviour RFC 0024 specifies for the folder, the
  committer, the sprint, and the fence holds unchanged for a catalog on
  the new boundary, and the test obligations of RFC 0024 pass unchanged
  in meaning.
- **The catalog still opens its own stores.** Settings, caches, the
  segment extractor, and the retrying transport are the catalog's; the
  library never opens a SlateDB store. It is handed the one the catalog
  opened, with the session it was opened over.
- **Link, do not fork; complete alone.** As RFC 0024: no dependency on the
  library, every test and deployment works with no provider, the
  boundary is covered by an in-tree implementation.

Non-goals:

- The protocol and the store's internals. What a slot is, how a loss is
  absorbed, how retries are paced: the library's, as before.
- Changing what a slot carries. The two classification fields and the id
  ride the batch exactly as they ride the envelope today.

## Design

### The commit store

```rust
#[async_trait]
pub trait CommitStore: fmt::Debug + Send + Sync + 'static {
    fn holds_store(&self) -> bool;
    fn db(&self) -> Option<slatedb::Db>;
    fn reader(&self) -> Option<Arc<slatedb::DbReader>>;
    fn known_sequence(&self) -> u64;

    async fn adopted(&self) -> Result<bool>;
    async fn adopt(&self, stamp: SlotCommit) -> Result<SlotAdoption>;
    async fn write_at(&self, sequence: u64, commit: SlotCommit) -> Result<SlotOffer>;

    async fn fold_tail(&self) -> Result<u64>;
    async fn fold_now(&self) -> Result<u64>;
    async fn unfolded(&self, replay_point: u64) -> Result<u64>;
    async fn take_store(&self) -> Result<bool>;
    async fn release_store(&self) -> Result<()>;
    async fn flush(&self) -> Result<()>;
    async fn close(&self) -> Result<()>;
}
```

A commit store is on one of three sides, and may change side while it
lives:

- **Holding.** Opened over the store's writer; every slot folds into it,
  the idle poll runs, and a fence is final.
- **Following.** Opened over a tailing reader; its writes land as slots
  and it waits, bounded, to be served them.
- **Promoted.** A follower holding the store it took: folds go to the
  store, reads still go through the reader, and a fence hands the store
  back rather than ending the session.

`write_at` is the whole commit path. It offers the commit as the slot at
`sequence`, conditional on the log holding nothing there, and returns:

```rust
pub enum SlotOffer {
    Won { sequence: u64 },
    Lost { sequence: u64, winner: SlotEnvelope, through: u64 },
}
```

On a win the store has applied the slot, or has been served it; on a loss
it has folded, or been served, everything that beat the offer, and
`through` is how far it reaches. The catalog never sees the slots it lost
to: it re-prepares against the store, which is what it does today. The
sequence may be at most one past what the store knows, which is what the
head stamp's counter implies for a batch prepared against a current head;
a stale head names a taken slot and loses.

`SlotCommit` keeps its shape — the transaction id, the validated head,
`changes_made`, the writes — and the library carries all four in the slot
as it does now. The library mints no id for a commit that brings one.

### The provider

```rust
pub trait CommitLogProvider: fmt::Debug + Send + Sync + 'static {
    fn writer_session(&self, log_store: Arc<dyn ObjectStore>, path: &str) -> Arc<dyn WriterSession>;
    fn reader_session(&self, log_store: Arc<dyn ObjectStore>, path: &str, tail: bool) -> Arc<dyn ReaderSession>;
    fn holding(&self, db: slatedb::Db, session: Arc<dyn WriterSession>, options: CommitStoreOptions) -> Arc<dyn CommitStore>;
    fn following(&self, reader: Arc<slatedb::DbReader>, session: Arc<dyn ReaderSession>, take: Arc<dyn TakeStore>, options: CommitStoreOptions) -> Arc<dyn CommitStore>;
    fn garbage_collector(&self, log_store: Arc<dyn ObjectStore>, path: &str) -> Arc<dyn WalGc>;
    fn external_compaction(&self) -> bool;
}

pub trait WriterSession: Any + Send + Sync { fn writer_init(&self) -> Box<dyn WriterInit>; }
pub trait ReaderSession: Any + Send + Sync { fn reader(&self) -> Arc<dyn WalReader>; }
```

A **session** is the cursor one open shares with its plug: the provider
mints it, the catalog builds the SlateDB store or reader over the plug it
yields, and hands both back to the provider, which recognizes the session
as its own and builds the commit store over it. This replaces
`CommitLogProvider::session` and the per-path plug methods of RFC 0024:
the cursor a plug and its store share was always one object; now it is
named, and the catalog cannot pair a store with the wrong one.

`garbage_collector` stays, for maintenance's collector, and so does
`external_compaction`. Registration and the per-open option are as in RFC
0024.

### What the catalog supplies

```rust
#[async_trait]
pub trait TakeStore: Send + Sync + 'static {
    async fn take(&self) -> Result<Option<(slatedb::Db, Arc<dyn WriterSession>)>>;
}

pub struct CommitStoreOptions {
    pub fold_every: u64,            // commit_log_fold_slots
    pub flush_every: u64,           // commit_log_flush_slots
    pub retention: Duration,        // commit_log_retention
    pub poll: Option<Duration>,     // commit_log_fold_poll
    pub catch_up: Duration,
    pub refresh_backoff: Duration,  // commit_log_poll_interval
    pub collect: Option<(String, Arc<dyn ObjectStore>)>,
    pub inspect: Option<Arc<dyn Fn(u64, &SlotEnvelope) -> Result<()> + Send + Sync>>,
}
```

- **`TakeStore`** is how a follower opens the store as its writer, for the
  cadence fold, the sprint, and `Catalog::take_store`. The catalog
  implements it with `open_writer_store`: a fresh writer session from the
  provider, the store built over its plug, and the open's replay as the
  fold. It declines — returns `None` — when the store is at its limit of
  unmerged files and nothing is compacting, which is the check
  `fold_sprint` makes today; the library folds nothing and the log grows
  until a compactor serves the lake, as RFC 0024 specifies.
- **`inspect`** is the catalog's one-head-stamp-per-slot check from RFC
  0024's "The sequence a committer races", run by the library on every
  slot before it folds it; a slot it refuses is not folded and the fold
  stops with the refusal, as corruption, as today.
- The rest are the options of RFC 0024 under the library's names.

### What moves, what stays

**Moves into the library**, and is deleted from `commit_log.rs`: the
holder and its idle poll; the fold after a loss, for either side; the
flush-by-count and the collector run; adoption's folding of whichever
stamp won; promotion, demotion, and the fence that demotes; the catch-up a
committer over a reader waits on; the fold cadence and the one-shot fold.

**Stays in this repository**: the traits above and their in-tree
implementation; the sequence a batch races, derived from its head stamp's
counter and the adoption's recorded origin; the format stamp and
migration; `submit_batch`'s staging against a commit session and its
lost-race outcome; the conflict the DuckDB path surfaces; the L0-ceiling
decline in `TakeStore`; `Catalog::take_store`, `fold_sprint`, and
`unfolded_slots`, which become one call each on the commit store.

**Errors.** The library reports a won slot its store could not take — a
holder fenced or refusing, a follower whose reader has closed — as a
distinct outcome carrying the sequence. The in-tree mapping surfaces it as
`Error::CommitOutcomeUnknown` naming the sequence, which is what the
catalog reports for that case today: the slot is committed, and a reopen
folds it. A fence on a held store is `Error::Fenced`; on a taken store it
is absorbed, the store is given back, and the write that met it returns
as landed.

### Compatibility

The provider trait is public, so replacing its methods is a breaking
change to the crate's API, released as a minor bump as the crate's 0.x
versioning allows and named in the changelog. The only implementation
known is the enterprise glue crate, which moves with this RFC; the
in-tree provider is a test fixture. The old trait is not kept beside the
new one: an adapter from the old committer to the new store would need
the loop this RFC deletes, and a deprecated trait nobody implements is
weight without a reader.

### Migration

The on-disk format, the slot contents, the WAL-id base, and the format
stamp are unchanged. A binary on either boundary opens a store the other
wrote. The change is in-process: the enterprise glue crate implements the
new provider over the library's store, and the open build's in-tree
provider implements it over the in-tree log, which gains the same
store-shaped loop in miniature for the tests.

### Test obligations

Every obligation of RFC 0024 stands, re-pointed at the commit store. In
addition:

- An in-tree commit store on each side: a holder applies its win and
  folds a loss; a follower is served its win and waits for a loss; a
  promoted follower folds into the store and reads through its reader.
- The cadence: with no holder alive, the follower that lands slot
  `n × fold_every` takes the store, folds, writes out, and lets go, and
  the manifest's replay point moves to that slot.
- The fence that demotes: a promoted follower another process fences
  drops back to following and its next commit lands through the log.
- A commit the store could not take surfaces as an unknown outcome
  naming its sequence, and a reopen folds it.
- The two-process harness of RFC 0024 runs unchanged over the new
  boundary.

## Alternatives considered

- **Keep both loops and converge them by review.** Two implementations of
  one protocol drift, and only one of them is simulated under faults.
  Rejected: this RFC exists because that happened.
- **Move moraine's loop into the library instead.** The library's loop is
  the one written for any SlateDB embedder, hardened against a faulty
  store, and covered by the admin and clone paths; moraine's is the one
  with catalog knowledge woven in. The generic one is the keeper.
- **A trait shaped like the library's `SlotStore` exactly, including its
  batch type.** The catalog already has `SlotCommit`, and the library's
  batch carries the same four things. One shape at the boundary, the
  catalog's, keeps the open crate's public API stable.
- **Have the library open SlateDB stores.** It would need the catalog's
  settings, caches, segment extractor, and transport, or a builder hook
  that amounts to the same thing. The catalog opens; the library is
  handed the result with its session.

# RFC 0027: The commit-log seam

- **Date:** 2026-11-03

## Summary

RFC 0024 fixed the boundary with the commit-log library at the slot: the
library hands the catalog SlateDB's write-ahead-log plugs and a committer
that races one sequence, and the catalog drives the loop above them —
when to offer, what to fold after a loss, when to flush and truncate,
which process takes the store and when it gives it back. That loop has
since been written again in the library, as a write any SlateDB embedder
shares, and the library's copy is the one simulated under faults. Two
loops is one too many, and multi-writer is not this crate's concern:
moraine is single-writer, and the library is what a deployment adds to
let a second process commit. This RFC moves the whole loop out of moraine
and leaves a seam: the trait the library implements, the hooks the
catalog supplies, and the two places the catalog calls through. Nothing
on disk changes, and the open build stays complete alone.

## Goals

- **The loop is the library's, entirely.** Offers, folds after a loss, the
  fold cadence, flushing and truncation, the idle poll, taking the store
  and giving it back, retries, and the maintenance verbs over them. Moraine
  calls the seam; it decides nothing about slots.
- **The seam is as small as the catalog's own needs.** Committing a batch,
  adopting at open, closing, and opening the store for the library when it
  asks. Nothing else crosses.
- **Parity.** Every behaviour RFC 0024 specifies for the folder, the
  committer, the sprint, and the fence holds unchanged for a catalog on
  the seam, and the test obligations of RFC 0024 pass unchanged in meaning.
- **The catalog still opens its own stores.** Settings, caches, the
  segment extractor, and the retrying transport are the catalog's; the
  library never opens a SlateDB store. It is handed the one the catalog
  opened, with the session it was opened over.
- **Link, do not fork; complete alone.** As RFC 0024.

Non-goals:

- The protocol and the store's internals, as before.
- Changing what a slot carries. The two classification fields and the id
  ride the batch exactly as they ride the envelope today.

## Design

### Why a seam at all

The open catalog is on the multi-writer path whenever a provider is
registered: its `submit_batch` is what stages a batch and asks whether it
committed, and its open paths are what build the SlateDB store and reader
with the catalog's settings. A build that links the library needs those
two places to call through to it, and a fork is ruled out. The seam is
the smallest declaration of those two places.

### The commit store

```rust
#[async_trait]
pub trait CommitStore: Any + fmt::Debug + Send + Sync + 'static {
    async fn adopted(&self) -> Result<bool>;
    async fn adopt(&self, stamp: SlotCommit) -> Result<SlotAdoption>;
    async fn write_at(&self, sequence: u64, commit: SlotCommit) -> Result<SlotOffer>;
    async fn fold_tail(&self) -> Result<u64>;
    async fn close(&self) -> Result<()>;
    fn as_any(&self) -> &dyn Any;
}

pub enum SlotOffer {
    Won { sequence: u64 },
    Lost { sequence: u64, winner: SlotEnvelope, through: u64 },
}
```

`write_at` is the whole commit path. It offers the commit as the slot at
`sequence`, conditional on the log holding nothing there. On a win the
library's store has applied the slot, or has been served it, and a published
durable checkpoint covers it; on a loss it
has folded, or been served, everything that beat the offer, and `through`
is how far it reaches. The catalog never commits a logged batch into
SlateDB itself and never sees the slots it lost to: it re-prepares against
the store, which is what it does today. The sequence may be at most one
past what the library's store knows, which is what the head stamp's
counter implies for a batch prepared against a current head; a stale head
names a taken slot and loses.

`adopted` and `adopt` serve the open path alone: a read-write open on a
format below the log's flushes, then adopts with the format stamp, and
folds whichever stamp won — the library's, now. `fold_tail` folds what has
landed past the store's cursor into a held store, or refreshes a followed
reader, and returns how many slots that was; demoting uses it to fold the
tail and then to prove no committer is still writing. `close` writes out
and lets go.

`SlotCommit` keeps its shape — the transaction id, the validated head,
`changes_made`, the writes — and the library carries all four in the slot
as it does now. The trait is `Any` so the crate that implements it can
reach its own store behind a catalog, for everything the seam does not
carry.

### The provider

```rust
pub trait CommitLogProvider: fmt::Debug + Send + Sync + 'static {
    fn writer_session(&self, log_store: Arc<dyn ObjectStore>, path: &str) -> Arc<dyn WriterSession>;
    fn reader_session(&self, log_store: Arc<dyn ObjectStore>, path: &str, tail: bool) -> Arc<dyn ReaderSession>;
    fn holding(&self, db: slatedb::Db, session: Arc<dyn WriterSession>, hooks: CatalogHooks) -> Arc<dyn CommitStore>;
    fn following(&self, reader: Arc<slatedb::DbReader>, session: Arc<dyn ReaderSession>, hooks: CatalogHooks) -> Arc<dyn CommitStore>;
    fn garbage_collector(&self, log_store: Arc<dyn ObjectStore>, path: &str) -> Arc<dyn WalGc>;
    fn external_compaction(&self) -> bool;
}

#[async_trait]
pub trait WriterSession: Any + Send + Sync + 'static {
    fn writer_init(&self) -> Box<dyn WriterInit>;
    async fn adopted(&self) -> Result<bool>;
    fn as_any(&self) -> &dyn Any;
}

pub trait ReaderSession: Any + Send + Sync + 'static {
    fn reader(&self) -> Arc<dyn WalReader>;
    fn as_any(&self) -> &dyn Any;
}
```

A **session** is the cursor one open shares with its plug: the provider
mints it, the catalog builds the SlateDB store or reader over the plug it
yields, and hands both back to the provider, which recognizes the session
as its own and builds the commit store over it. This replaces
`CommitLogProvider::session` and the per-path plug methods of RFC 0024.
A writer session answers `adopted` before the store is built: the plug
replays the log in place of the store's own, so a store on a log nobody
has adopted drains its own log into the tree first, as RFC 0024's open
does.

`garbage_collector` stays, for maintenance's collector, and so does
`external_compaction`. Registration and the per-open option are as in RFC
0024.

### What the catalog supplies

```rust
pub struct CatalogHooks {
    pub take: Arc<dyn TakeStore>,
    pub inspect: Arc<dyn Fn(u64, &SlotEnvelope) -> Result<()> + Send + Sync>,
    pub path: String,
    pub object_store: Arc<dyn ObjectStore>,
    pub settings: CommitLogSettings,
}

#[async_trait]
pub trait TakeStore: Send + Sync + 'static {
    async fn take(&self) -> Result<Option<(slatedb::Db, Arc<dyn WriterSession>)>>;
    async fn take_for_publication(&self) -> Result<(slatedb::Db, Arc<dyn WriterSession>)>;
}
```

- **`TakeStore`** is how the library opens the store as its writer, for
  the cadence fold and for holding it. The catalog implements it with its
  writer open: a fresh writer session from the provider, the store built
  over its plug, and the open's replay as the fold. It declines — returns
  `None` — when the store is at its limit of unmerged files and nothing is
  compacting, as `fold_sprint` does today; the library then folds nothing
  and the log grows until a compactor serves the lake, as RFC 0024
  specifies.
- **`inspect`** is the one-head-stamp-per-slot check of RFC 0024's "The
  sequence a committer races", run by the library on every slot before it
  folds it; a refused slot is not folded and the fold stops with the
  refusal, as corruption, as today.
- **`path`** and **`object_store`** are where the store lives, as it was
  opened: what the library's collector run needs to read the manifests and
  the checkpoints that pin the log.
- **`CommitLogSettings`** carries the existing `commit_log_*` options —
  the fold cadence, the flush count, the retention, the fold poll, and the
  committer's poll — as values the library reads. The catalog interprets
  none of them.

`CatalogHooks::publish(&db)` flushes WAL durability and publishes the current
read checkpoint using the catalog's path and object store. The library calls it
before reporting a won slot; a publication failure after the slot lands is an
unknown commit outcome, never permission to offer that mutation again. A holder
publishes its applied state; a follower takes a temporary writer through
`TakeStore::take_for_publication`, verifies replay covers its won slot, publishes,
and closes without flushing an L0 file. The default publication take delegates
to `take` and reports a refusal; the catalog's implementation bypasses the L0
ceiling because publication does not write an L0 file. A subsequent flush must
republish the flushed state before releasing the writer so new fixed readers do
not replay an obsolete WAL. The library continues to own all offer, fold, and
publication sequencing.

The catalog also exposes two things for the enterprise maintenance crate,
which today reaches the loop through catalog verbs:

- `Catalog::commit_log_hooks(object_store, options) -> CatalogHooks`: the
  hooks as a live catalog would hand them, so a maintenance process can
  fold a lake once, measure its unfolded tail, or hold its store with the
  library and no live catalog; the inspection is the catalog's to supply,
  since it reads the catalog's own keys.
  `Catalog::store_taker(object_store, options) -> Arc<dyn TakeStore>` is
  its `take` alone.
- `Catalog::commit_store(&self) -> Option<Arc<dyn CommitStore>>`: the
  store behind a live catalog, for the same crate to reach through `Any`.

### What moves, what goes

**Moves into the library**, and is deleted from `commit_log.rs`: the
holder and its idle poll; the fold after a loss, for either side; the
flush-by-count and the collector run; adoption's folding of whichever
stamp won; promotion, demotion, and the fence that demotes; the catch-up a
committer over a reader waits on; the fold cadence and the one-shot fold.

**Leaves the catalog's API**: `Catalog::take_store`, `Catalog::fold_sprint`,
`Catalog::sprint_if_stalled`, `Catalog::unfolded_slots`, and
`Catalog::unfolded_slots_of`. Their only callers are in the enterprise
maintenance crate, which performs them with the library over the taker
and the store above. The read-only tailing attach and the committer's poll
interval stay as options; they configure the seam, not the loop.

**Stays in this repository**: the traits above and their in-tree test
fixture; the sequence a batch races, derived from its head stamp's
counter and the adoption's recorded origin; the format stamp and
migration; `submit_batch`'s staging against a commit session and its
lost-race outcome; the conflict the DuckDB path surfaces; the L0-ceiling
decline in `TakeStore`.

**Errors.** The library reports a won slot its store could not take — a
holder fenced or refusing, a follower whose reader has closed — as a
distinct outcome carrying the sequence. The in-tree mapping surfaces it as
`Error::CommitOutcomeUnknown` naming the sequence, which is what the
catalog reports for that case today: the slot is committed, and a reopen
folds it. A fence on a held store is `Error::Fenced`; on a taken store it
is absorbed by the library, the store is given back, and the write that
met it returns as landed.

### Compatibility

The provider trait and the five catalog verbs are public, so this is a
breaking change to the crate's API, released as a minor bump as the
crate's 0.x versioning allows and named in the changelog. The only
implementation of the provider known is the enterprise glue crate, and
the only callers of the verbs are the enterprise maintenance crate; both
move with this RFC. The in-tree provider is a test fixture. The old trait
is not kept beside the new one: an adapter from the old committer to the
new store would need the loop this RFC deletes, and a deprecated trait
nobody implements is weight without a reader.

### Migration

The on-disk format, the slot contents, the WAL-id base, and the format
stamp are unchanged. A binary on either boundary opens a store the other
wrote. The change is in-process: the enterprise glue crate implements the
new provider over the library's store, the enterprise maintenance crate
performs its verbs through the taker and the store, and the open build's
in-tree provider implements the seam over the in-tree log, which gains
the same loop in miniature for the tests.

### Test obligations

Every obligation of RFC 0024 stands, re-pointed at the seam. In addition:

- An in-tree commit store on each side: a holder applies its win and
  folds a loss; a follower is served its win and waits for a loss. The
  in-tree store holds only for adoption, demotion, and the cadence's take;
  promotion, the fence that demotes it, and a won slot the store could not
  take are the library's, tested where the library is.
- The cadence: with no holder alive, the follower that lands slot
  `n × fold_every` takes the store through the catalog's taker, folds,
  writes out, and lets go, and the manifest's replay point moves to it.
- The fence that demotes, in the library: a promoted follower another
  process fences drops back to following and its next commit lands through
  the log.
- A commit the store could not take surfaces as an unknown outcome
  naming its sequence, and a reopen folds it; the library's error, mapped
  by the provider that carries it.
- The sequence invariant: a batch prepared against a stale head names a
  taken slot and loses, never being refused as past the store; a batch
  prepared against the current head of a committer whose reader was served
  further than its view wins at the sequence its head implies. A demoted
  lake is refused re-adoption, as RFC 0001 of the library specifies.
- The two-process harness of RFC 0024 runs unchanged over the seam.

## Alternatives considered

- **Keep both loops and converge them by review.** Two implementations of
  one protocol drift, and only one of them is simulated under faults.
  Rejected: this RFC exists because that happened.
- **Move moraine's loop into the library instead.** The library's loop is
  the one written for any SlateDB embedder, hardened against a faulty
  store, and covered by the admin and clone paths; moraine's is the one
  with catalog knowledge woven in. The generic one is the keeper.
- **A store-shaped trait carrying the maintenance verbs too.** An earlier
  draft of this RFC. It kept `fold_now`, `take_store`, `release_store`,
  `flush` and the handle accessors on the open trait so the catalog's
  verbs could pass through. Their callers are all enterprise code, which
  can reach the library's store directly; a seam that carries only what
  the catalog itself needs is smaller and says plainly that multi-writer
  is not the open crate's concern.
- **No seam: an enterprise build that patches the two call sites.** A
  fork by another name, and RFC 0024's first goal rules it out.
- **Have the library open SlateDB stores.** It would need the catalog's
  settings, caches, segment extractor, and transport, or a builder hook
  that amounts to the same thing. The catalog opens; the library is
  handed the result with its session.

# RFC 0011: Commit-log seam

## Summary

Moraine is single-writer, and its single-writer path is complete on its own. A
bucket-resident commit log, which lets several processes commit to one lake,
lives in a separate library and plugs into one boundary in this crate: a
provider trait the library implements, a commit store the catalog commits
through, and the hooks the catalog hands back. The offer, fold, flush, truncate,
promote, and demote loop lives in the library. Nothing on disk changes.

## Seam boundary

The seam is the module `crates/moraine/src/commit_log.rs` and the two places the
catalog calls through it: committing a batch, and opening a store or reader with
a provider present. The catalog decides nothing about slots. It derives the
sequence a batch names, stamps the format, supplies the hooks, and calls
`write_at`.

Every byte that crosses the seam is opaque to the library. A `SlotWrite` is a
key and an optional value. A `SlotCommit` is a batch of writes under a
committer-minted `transaction_id`, with its `validated_head` and the DuckLake
`changes_made` of the snapshot it mints. A `SlotEnvelope` is the commits that
won one slot, in order.

The library never opens a SlateDB store. The catalog opens the `Db` or
`DbReader` with its own settings over the plug a session yields, and hands the
handle back to the provider.

## Commit store

`CommitStore` is the write every process shares, over the store or reader the
catalog opened.

| Method | Role |
|---|---|
| `adopted` | Whether a store's stamp holds the log's first slot. |
| `adopt(stamp)` | Races `stamp` for the first slot. Returns `SlotAdoption::Adopted` or `SlotAdoption::AdoptedByPeer(envelope)`. Only a holder may call it. A follower gets `Error::Constraint`. |
| `write_at(sequence, commit)` | Offers `commit` as the slot at `sequence`. Returns `SlotOffer`. |
| `fold_tail` | Folds what landed past this process's cursor into a held store, or refreshes a followed reader. Returns the slot count. |
| `close` | Writes a held store out, lets it go, and closes the store or reader beneath. |
| `as_any` | The store as the library knows it. |

`write_at` is the commit path through the log. The caller derives `sequence`
from the state the commit was prepared against. The sequence may be at most one
past what this process's store knows. A stale head names a taken slot and loses.

`SlotOffer::Won { sequence }` means the commit holds the slot, this process's
store has applied it or been served it, and a durable read checkpoint covers it.
`SlotOffer::Lost { sequence, winner, through }` means another committer holds
the raced sequence. Everything that beat the offer is in this process's store
before the result returns, and `through` is the last sequence the store covers.
The catalog re-prepares against the store. It never commits a logged batch into
SlateDB itself.

`adopted` and `adopt` serve the open path. `fold_tail` serves demotion.

## Provider and sessions

`CommitLogProvider` is what the library hands the catalog.

| Method | Returns |
|---|---|
| `writer_session(log_store, path)` | A fresh `WriterSession` for one read-write open. |
| `reader_session(log_store, path, tail)` | A fresh `ReaderSession` for one read-only open. A tailing reader is served the unfolded tail. One that does not tail follows the folded store. |
| `holding(db, session, hooks)` | A `CommitStore` over a writer the catalog opened with the session's plug: a holder, into which every slot folds. |
| `following(reader, session, hooks)` | A `CommitStore` over a reader the catalog opened with the session's plug: a follower, whose writes land as slots. |
| `garbage_collector(log_store, path)` | The `WalGc` plug SlateDB's collector truncates the log through. |
| `external_compaction` | Whether another process compacts the stores opened over this provider. Defaults to `false`. |

A session is the cursor one open shares with its plug. `WriterSession` yields
`writer_init`, the SlateDB `WriterInit` the `Db` is built with, and answers
`adopted` before the store is built. The plug replays the log in place of the
store's own log, so a store on a log nobody has adopted drains its own log into
the tree first. `ReaderSession` yields `reader`, the SlateDB `WalReader` the
`DbReader` is built with. Both carry `as_any`.

A read-write open with a provider commits as a follower over a tailing reader.
When the log is not adopted, the open first takes the store as a holder, adopts,
and closes it. The catalog builds a holder only for adoption and demotion. Every
other hold is the library's, through the hooks.

When `external_compaction` returns `true`, a writer starts neither SlateDB's
compactor nor its collector, the same as `CatalogOptions::external_compaction`.

## Catalog hooks

`CatalogHooks` is what the catalog hands the library with a store or reader.

| Field | Content |
|---|---|
| `take: Arc<dyn TakeStore>` | How the library opens the store as its writer. |
| `inspect: Inspect` | A check run on every slot before it is folded. A refused slot is not folded and the fold stops with the refusal. |
| `path` | The store's path, as it was opened. |
| `object_store` | The store's object store, as it was opened. |
| `settings: CommitLogSettings` | The cadence and the waits. |

`TakeStore::take` opens the store as its writer through the catalog's own writer
open, over a fresh writer session from the provider, and the open's replay is
the fold. It returns `None` when the store is one file short of SlateDB's
`l0_max_ssts` ceiling. The library then folds nothing and the log grows until a
compactor serves the lake. `TakeStore::take_for_publication` opens a temporary
writer for checkpoint publication, which writes no L0 file, so the catalog's
implementation skips the ceiling check. The default implementation delegates to
`take` and reports a decline as `Error::Constraint`.

The catalog's `inspect` is the head-stamp check: every slot past the first
carries one `sys/head` write. Any other count is `Error::Corruption`.

`CommitLogSettings` carries the catalog's commit-log options as values the
library reads. The catalog interprets none of them.

| Field | Source option | Default |
|---|---|---|
| `fold_every` | `commit_log_fold_slots` | 1024 |
| `flush_every` | `commit_log_flush_slots`, at least 1 | 256 |
| `retention` | `commit_log_retention` | 5 minutes |
| `poll` | `commit_log_fold_poll` | 2 seconds |
| `catch_up` | `READER_CATCHUP_TIMEOUT` | 5 seconds |
| `refresh_backoff` | `commit_log_poll_interval` | 50 ms |

`CatalogHooks::publish(&db)` flushes WAL durability and publishes the current
read checkpoint under the hooks' path and object store. The library calls it
before reporting a won slot, and again after flushing one to SSTs. A publication
failure after the slot lands is an unknown commit outcome. Offering the same
mutation again does not happen.

## Catalog entry points

Three public items on `Catalog` expose the seam to a maintenance process with no
live catalog.

- `Catalog::commit_log_hooks(&object_store, &options) -> CatalogHooks` returns
  the hooks as a live catalog would hand them.
- `Catalog::store_taker(object_store, &options) -> Arc<dyn TakeStore>` returns
  the `take` of those hooks alone.
- `Catalog::commit_store(&self) -> Option<Arc<dyn CommitStore>>` returns the
  store behind a live catalog, for the library to reach through `as_any`. It is
  `None` on a catalog that is not on a log.

`Catalog::demote(object_store, options)` takes a lake back off its log: it takes
the store, folds every slot, stamps the format below the log's, writes that out,
and lets go. It returns `Error::Configuration` without the provider the lake was
adopted with, and `Error::Constraint` when the store is at its L0 ceiling or a
committer landed a slot after the fold.

## Provider registration

A host supplies a provider one of two ways.

1. Per open, through `CatalogOptions::commit_log`.
2. Per process, through `moraine::register_commit_log(provider)`, which stores
   it in a `OnceLock`. A second registration is `Error::Configuration`.

An open uses the option when set, else the registration, else nothing. There is
no way to ignore the registration for one open. The registration route is how
the DuckDB extension is reached: it cannot construct a private crate's provider,
so a library linked into the extension registers one at load.

## Format stamp

Format 10 marks a commit-log catalog. Formats 11 and 12 mark one through the
`commit_log_batch_origin` field of the format stamp. `uses_commit_log` is true
when the version is 10 or the origin is set. Adoption stamps the format at
`max(current, 10)` and records the head's batch counter as
`commit_log_batch_origin`. Demotion clears the origin and preserves the
directories formats 11 and 12 add.

`validate_format` refuses a format newer than the binary first, then refuses a
commit-log format when no provider is present, as `Error::Migration` naming the
library. Maintenance, migration, and the log-store move open stores without
consulting the provider, and reach a log-resident store only through this
refusal.

## Log store and slot sequence

The log lives in the object store the catalog records as its log store: the
catalog store itself unless `CatalogOptions::wal_store` names a separate one.
Every provider method receives that store wrapped in the catalog's retrying
transport, with the catalog's path within it. `Catalog::move_wal_store` keeps
its meaning.

A committer's plug reports a slot served a moment before SlateDB applies its
rows, so the sequence a batch races is derived from the state the commit was
prepared against. Every slot past the first carries one head stamp, and the
head's batch counter advances by one per slot. Adoption records the counter the
first slot stands on in the format stamp. Slot `k` carries the head stamp at
`batch_origin + k - 1`, and a batch stamping the head at `batch_seq` races slot
`batch_seq - batch_origin + 1`. A batch with no head stamp is
`Error::Corruption`.

## Attach options

| Option | Effect |
|---|---|
| `COMMIT_LOG_TAIL` | A read-only attach tails the log, seeing a commit once its slot lands. Maps to `CatalogOptions::commit_log_tail`. |
| `COMMIT_LOG_POLL_INTERVAL_MS` | How long a committer waits between refreshes while its slot is not yet served. Maps to `CatalogOptions::commit_log_poll_interval`. |
| `READER_POLL_INTERVAL_MS` | How often a read-only attach looks for new commits. On a committer it keeps the reader's checkpoint alive. |

The first two reach the core through `moraine_attach_with_commit_log`, whose
`commit_log` argument the older attach entry points pass as null. Without a
registered provider they are ignored. `READER_POLL_INTERVAL_MS` applies with no
provider at all.

## Parity behaviors

The library preserves these behaviors for a catalog on the seam.

- Committers fence nothing. Every read-write open with a provider is a follower
  over a tailing reader.
- A lost race is folded before the retry lands. `Lost` returns only after every
  slot that beat the offer is in this process's store.
- Folding runs on a cadence from any committer. The committer that lands slot
  `n * fold_every` takes the store through the hooks, folds, writes it out,
  truncates the log below the replay point, and lets go. A fold that fails does
  not fail the commit.
- A commit over a follower waits, bounded by `catch_up`, for its reader to be
  served the slot. The slot is durable either way.
- Demote takes a lake back off its log. A demoted lake is refused re-adoption.

## Errors

| Case | Error |
|---|---|
| A second `register_commit_log` | `Error::Configuration` |
| A commit-log format opened without a provider | `Error::Migration` |
| A won slot this process's store could not take | `Error::CommitOutcomeUnknown` naming the sequence. A reopen folds it. |
| Another process took a held store | `Error::Fenced` |
| An invalid envelope, or a wrong head-stamp count | `Error::Corruption` |
| `adopt` on a follower, or on a log adopted already | `Error::Constraint` |

## Compatibility

The on-disk format, the slot contents, the WAL-id base, and the format stamp are
unchanged by the seam. The in-tree provider is a test fixture.

## Tests

- A store stamped for the log is refused without a provider, naming the library.
- One registration succeeds, a second is refused, and an open carrying its own
  provider never consults the registered one.
- A committer adopts an empty log and commits through it, and a tailing
  read-only open is served the slots.
- A lost race is served the winner before the retry lands.
- The committer that lands the nth slot takes the store, folds, and truncates,
  and the manifest's replay point moves.
- A demoted lake opens without its provider, and demoting needs the provider.

## Alternatives

- **Keep the offer-and-fold loop in this crate beside the library's.** Two
  implementations of one protocol drift, and only the library's runs under fault
  simulation. Rejected.

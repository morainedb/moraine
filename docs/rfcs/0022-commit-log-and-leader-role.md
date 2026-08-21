# RFC 0022: The commit log and the leader role

- **Date:** 2026-07-29

## Summary

Replaces RFC 0004's single-writer topology. That design let exactly one
process hold the read-write `Db`, so a fleet of independent DuckDB clients
could not commit concurrently — the common lakehouse shape.

The commit point moves out of SlateDB and into the bucket. A **commit
log** — immutable objects written with create-if-absent conditional puts —
is the source of truth: exactly one committer can win each slot, so the
object store serializes writers the way SlateDB's head-CAS did. SlateDB
becomes a **derived index** of the log, maintained by a single fenced
**folder** using the existing single-writer machinery unchanged. A commit
is durable the moment its slot PUT is acknowledged.

**This is the only commit topology and the only mode.** There is no mode
flag. Old-format stores migrate on their first read-write attach, in one
atomic metadata batch.

What varies is not a mode but a **role**. A long-lived process may hold
the **leader** role: it opens a listener, announces itself through the
log, and accepts forwarded catalog *transactions* from contending peers,
which it serializes and coalesces into shared slots. Clients forward only
after losing slot races, and fall back to direct commits the moment a
leader is unreachable. A solo writer never connects and pays nothing; a
fleet with no long-lived process never has a leader and runs purely
direct. The only configuration anywhere is the leader host's addresses —
clients configure nothing.

Safety never depends on clocks, leases, leadership, or any node's disk.

**Implemented.** The log, the folder, and truncation are live: every
commit races a conditional put against `commits/<seq>`; the log is the
store's own write-ahead log, so a fenced folder folds it by opening the
store — the replay past its cursor *is* the fold — and flushing; slots
truncate oldest-first, bounded by the durable flush horizon and by
live-reader checkpoints past a retention margin. Old-format stores
migrate on their first read-write attach (`sys/format = 4`), fencing any
incumbent old-binary writer; a too-new format refuses. Group commit runs through in-process
coalescing (`CatalogOptions::commit_batch_window`); a committer that
crashes with an ambiguous PUT resolves its outcome by scanning for its
transaction id (`Catalog::transaction_outcome`). The leader role is live
behind the `leader` feature and the extension's `MAINTENANCE_LEADER`
attach option: the designated folder binds a listener, mints or reads the
store-held forwarding token, and announces through the log; a client that
loses a race forwards its transaction, and the funnel coalesces forwarded
commits into shared slots and races them hot (near-zero backoff base).
Verified live over real SlateDB on in-memory `object_store`:
prefix-consistent truncation past the retention margin
(`truncation_holds_the_live_reader_bound_past_the_margin`), a stale
reader surviving a peer's fold and truncation
(`a_stale_reader_survives_a_peer_fold_and_truncation`), migration
(`a_legacy_store_migrates_on_first_write_attach_and_serves_its_data`),
and a contended fleet converging onto the leader
(`two_contending_handles_converge_onto_the_leader`).

## Goals

- **Fleet multi-writer.** N independent DuckDB processes commit
  concurrently with nothing beyond the bucket.
- **Liveness = one process + the bucket.** No quorum, reachable leader, or
  lease is ever a precondition for progress. The leader is assistance; the
  direct path is permanently load-bearing.
- **Safety from conditional puts alone.** Nothing above the conditional
  put — coalescing, leadership, announcements — can affect correctness.
  Wrong, stale, or duplicated leadership degrades throughput only.
- **Durability at the slot.** The commit record and the durable artifact
  are the same object; no acknowledged commit is ever the sole property of
  any node's disk.
- **Exactly-once client outcome.** A committer that crashes mid-commit can
  always determine whether its commit landed; a retry never double-applies.
- **Existing semantics preserved.** RFC 0004's conflict model and typed
  errors are unchanged; only where the race is arbitrated moves. RFC
  0002's atomicity holds in the index: one slot folds as one `WriteBatch`,
  fold cursor included.
- **Group commit where it is achievable.** Commits share a slot exactly
  when their author can chain them (below). That constraint is why the
  leader receives transactions, never finished commits.

Non-goals:

- **Sub-object-store commit latency.** The floor is one conditional PUT;
  going lower needs a consensus-replicated log (see Alternatives).
- **Changing SlateDB.** Runs on stock SlateDB; cooperative multi-writer
  inside SlateDB is the successor design (see Alternatives).
- **A leader as a security boundary.** Every client holds bucket
  credentials; a leader mediates contention, not access. (A
  credential-free thin client is possible later on the same wire format.)
- **Read-path changes.** RFC 0009 governs reader consistency; this RFC
  only adds tail replay past the last fold.
- **Cross-slot transactions.** One commit is one atomic unit.

## Design

### Layering: the log is truth, the store is an index

1. **The commit log** — objects `commits/<seq>` (fixed-width names, so
   lexicographic order is numeric), each written exactly once with
   `PutMode::Create`. Any process may attempt slot N+1; the object store
   guarantees one winner.
2. **The SlateDB store** — unchanged on-disk format, maintained by the one
   fenced **folder** (RFC 0004's writer). The store takes the log as its
   own write-ahead log, so the folder folds by opening it: SlateDB replays
   every slot past the store's replay point into the memtable, and the
   flush that follows lands them in an L0 SST and moves that point. The
   cursor is the store's own manifest, so a successor resumes exactly and
   the fold advances state and cursor as one act.

Every other process is a `DbReader` (RFC 0017) reading the same log the
same way: it replays the slots past the store's cursor for itself, so the
head is store state plus the unfolded tail without either side folding by
hand. This is not *like* SlateDB's WAL-to-L0 relationship — it is that
relationship, with the commit log in the WAL's place.

The layering exists to change the liveness model. SlateDB ties write
liveness to its one writer — right for an embedded store owned by one
application, wrong for a fleet of ephemeral sessions with no owner, where
fencing's newest-writer-wins turns contenders into mutual assassins (RFC
0004's ping-pong). The conditional put is the only arbitration primitive
independent processes share, so the commit point moves onto it. SlateDB's
single-writer model is not discarded but demoted to the roles where a dead
process is benign: the folder (cost: fold lag) and the leader (cost: a
timeout).

The reader side deviates in the opposite direction. In SlateDB, readers
see only what the writer has flushed — a dead writer freezes reads. Here
no process sits in the freshness path: the tail is read fresh from the
bucket, so a commit is visible to every reader the moment its PUT lands,
and manifest staleness only decides how many slots a reader replays. Lag
is a cost, not a visibility property — provided truncation respects
readers (below).

At fleet scale, note RFC 0004's `DbReader` caveat: latest-mode readers
each write a manifest checkpoint and pin SSTs. Large fleets should open
against existing checkpoint ids — traffic hygiene, not correctness.

### The slot

An immutable object holding one envelope:

- **commits** — one or more change sets, each carrying a
  committer-generated transaction id (UUID), the change set (the same
  content RFC 0004 stages into a commit batch), the head it validated
  against, and its DuckLake `changes_made` string for classification.
- **leader advert** (optional) — the leader's announcement; see The
  leader role.

Snapshot ids and all other allocated ids derive deterministically from
slot order; no id is minted outside the log.

The envelope codecs live in **`moraine-wal`**, a crate below `moraine`
that knows the log protocol and nothing else: sequence naming, envelope
and payload schema, the conditional-put race, tail enumeration,
truncation, and the generic commit and fold loops. Payload keys, values,
and the classification string are opaque bytes to it. The boundary is
shape versus meaning, and it is what lets the protocol be
simulation-tested without SlateDB.

### Commit protocol

1. **Materialize the head**: `DbReader` state plus tail replay past
   the store's replay point.
2. **Validate**: RFC 0004's conflict detection against that head; a
   genuine conflict is the same typed error as before.
3. **Payload first**: objects the change set references are written to
   collision-free keys before the slot; a slot never references bytes not
   already durable. The DuckLake write path satisfies this for free (data
   files land during the statement, before `COMMIT`); inlined writes (RFC
   0005) ride the envelope itself.
4. **Race the slot**: conditional-put the envelope at the next sequence.
   A win is committed and durable at the ack. On `AlreadyExists`: read the
   winning slot (needed anyway to advance the local head), re-validate,
   retry at the next sequence with jittered backoff under a bounded
   attempt count. Re-validation failure surfaces as a typed conflict.

Crash analysis: before the PUT, nothing committed (orphaned payloads are
swept below). After the PUT, the transaction id resolves the outcome: the
recovering committer scans the tail and folded state for its id, then
reports success or retries safely. A PUT whose outcome is unknown — the
request failed after the object landed — resolves the same way: re-read
the slot and look for your own id. This is the only ambiguity the
protocol admits, and it is always answerable.

The slot PUT is the commit point and the durability point at once:
standard object storage acknowledges only after redundant multi-zone
storage. Single-zone classes (e.g. S3 Express One Zone) are not supported
as the log's home.

### Integrity: what replay refuses

The log is authoritative, so replay must tell damage from an ending. Two
checks, both free:

- **Holes.** Tail enumeration is one LIST, so a sequence absent below a
  present one is visible. That is a destroyed slot, never an end of log:
  serving the prefix would hide committed state and let the next committer
  re-win the sequence, forking history. Replay refuses. The one benign
  cause is a reader behind a truncation, distinguished by re-reading the
  fold cursor: if it has advanced past the hole, the slots were
  legitimately retired.
- **Continuity.** Each commit records the head it validated against.
  Before applying, replay compares it with the view's snapshot id:
  **greater** → slots are missing, refuse; **equal** → apply; **less** →
  already folded, skip. The **greater** case catches a substituted or
  reordered slot. The **less** case is sound only ahead of the first apply:
  folding advances in log order under a prefix cursor, so a legitimately
  already-folded commit is a leading prefix of the tail. A latch enforces
  that — once any commit in the replay has applied, a following **less** is
  a broken chain and refuses, not a lagging cursor. This is what makes a
  multi-commit envelope safe: its commits must chain (each staged against
  the previous one's result), and one that does not fails replay rather
  than applying a prefix and dropping the rest.

Both are detection. Prevention stays with bucket guard rails (versioning
on `commits/`, lifecycle exclusions).

### The folder

RFC 0004's single writer with a new job: tail the log, fold each slot as
one atomic act, advance the store's replay point. Folding is deterministic and
idempotent; a successor resumes from the cursor.

The folder is **availability-optional**. Down, commits continue; the only
symptom is a growing tail that lengthens materialization. A dead folder
cannot lose a commit, because folding happens strictly after durability.

Folding never enters the commit path in either direction. Committers do
not fold (that would fence-ping-pong the writer role per commit), and
commits do not wait for folds (a fold adds no durability and no
visibility — only shorter future replays). Folding is derived-state
maintenance, like compaction.

**Appointment is the act of opening.** `Db::open` read-write fences the
incumbent; the only question is when to open. The failure detector is the
tail itself — a growing unfolded tail is the clock-free signal that no
folder is live. The rule: observe tail beyond a threshold; after a
jittered delay, re-check fold progress; if the cursor advanced, stand
down; otherwise open and fold, staying (long-lived host) or folding to
drained and closing (a **fold sprint**). If fenced, stand down. The
progress check turns a stampede into one opener; fencing plus idempotent
folds means duelling folders waste, never corrupt.

Postures, in order of preference: a **designated long-lived process** (an
RFC 0021 maintenance sidecar, any service) holds the role continuously —
and that posture, not the role, is what qualifies a process to also take
the leader role; otherwise **opportunistic** fold sprints, where "whoever
last bothered" is a legitimate steady state. With no long-lived posture,
no leader ever exists.

**The store journals nothing of its own.** The commit log is its
write-ahead log, so journaling a fold again would double every logged
byte for nothing — commits are durable at the slot PUT and fold state is
re-derivable. Folds go to the memtable, durability arrives at the L0
flush, which the sprint forces explicitly (the default L0 size threshold
is far above a catalog fold). Consequences, neither safety-relevant:
truncation lags to the flush horizon, and a fenced zombie notices its
demotion at its next flush rather than its next WAL write.

**Sequence numbers are shared, so slots take every millionth one.** A
store numbers its rows from one space, and the rows a replay produces are
numbered from it too: a slot's rows all carry the number its ordinal
fixes. Two rules keep the two kinds of writer out of each other's way.
The log's first slot starts at 2^32, above anything a store wrote before
it adopted the log, so a migrated store's history stays ordered before
every slot rather than shadowing it — a store already past that is
refused rather than folded underneath. And each slot reserves 2^20
numbers, so the writes a store still makes for itself — the derived state
its maintenance keeps — take the numbers between the slot last folded and
the next one: they order after everything folded and before everything
still to fold, which is where they belong. A store write that reaches the
next slot's own number is refused, because the fold would then skip that
slot as already covered; the interval is a million writes wide, so
reaching it means a session wrote without ever folding.

**The chain is checked in the log, not against the view.** A reader
replays the tail for itself, so its view already reflects the slots a
replay would apply and cannot reveal a substituted one. Each commit
instead declares the head it was validated against, and the head it
leaves is its own `sys/head` write (or the one it found, for a commit
that mints no snapshot); consecutive slots must agree. A slot whose
commit was validated against anything else was substituted or reordered,
and replay refuses.

### Truncation and GC

Slot deletion is the one operation that can destroy the authoritative
copy of a commit, so it has **two** bounds:

> A slot may be deleted only when its sequence is ≤ the fold cursor **as
> durably flushed** — the flush horizon, not the memtable — **and** no
> live reader still needs tail replay across it.

The first bound alone already makes deletion safe: a slot folds into the
store, durably, strictly before it becomes eligible for deletion, so the
store holds every truncated slot's content at the flush horizon. A reader
whose tail replay lags behind a truncation and lands on a
truncated-but-folded hole recovers by the mechanism the Integrity section
above specifies for a benign hole: re-reading the fold cursor through a
fresh reader finds it has advanced past the hole, and the reader
re-materializes from the fresher folded state.

The second bound is a cost and consistency guarantee layered on top, not
a correctness requirement. Without it, every reader whose replay crosses
a truncated range pays that re-materialization on its own, independently,
each time it happens — safe, but a repeated, per-reader cost that
compounds across a fleet. Respecting live readers instead keeps tail
replay prefix-consistent (stale-but-consistent) for every reader at once,
turning the re-materialization from a recurring cost into one that need
never happen.

Live readers are discoverable because SlateDB already keeps the registry:
a latest-mode `DbReader` writes a checkpoint into the manifest, which is
how SlateDB's own GC stays safe. Never truncate past the fold cursor as
of the oldest live checkpoint. Checkpoints expire, so a crashed reader's
pin lapses. Add a retention margin on top (mirroring the minimum age
SlateDB's WAL GC applies), so the common case never depends on timing.
Envelopes are small: over-retention costs storage, under-retention costs
consistency.

Slot GC and orphaned-payload GC ride RFC 0007's expiry machinery; an
orphaned payload — a data file whose commit never won a slot — is the
multi-writer face of DuckLake's aborted-transaction cleanup.

Maintenance truncates by the two bounds above, which are ordinal and so
need no clock. The store's own collector can reclaim slots instead, since
the log is its write-ahead log and the ranges its live manifests
reference are the same two bounds computed upstream; that path is
temporal (a minimum age) rather than ordinal, and a host that runs it
gets the collector from the log rather than reimplementing the policy.

### Group commit and chaining

An envelope may hold several commits; folding many into one slot is the
only way to amortize the per-commit PUT. It is possible exactly when the
commits **chain**: each authored against its predecessor's result, so
commit n+1's validated head is commit n's minted snapshot.

Chaining is unavoidable, not stylistic. A commit's writes carry absolute
values from the head it read — the snapshot id, `begin_snapshot` on every
row, ids allocated from `next_catalog_id`/`next_file_id`. Two commits
authored against the same head allocate colliding ids; folding them would
silently lose one. Replay's continuity check makes that failure loud, but
the constraint stands: only an author who knows the predecessor's result
can produce a foldable batch.

Group commit is therefore achievable in exactly two places:

- **In-process coalescing.** A process with several pending commits holds
  their closures, so it can run each against the accumulating head and
  emit a chain. SlateDB's WAL buffer did the same for the old topology —
  also in-process — so coalescing is parity, not a new capability.
  Without it, N concurrent in-process commits race each other and cost
  ~N²/2 PUTs.
- **The leader role.** A leader owns the forwarded transaction, so its
  clients read counters through it and are serialized by it; their
  commits chain by construction.

Nothing else works. A forwarded *finished* change set cannot be folded
(ids baked; the receiver holds no closure; RFC 0004 forbids re-stamping
without re-validation; RFC 0006 forbids re-authoring DuckLake rows). Nor
can a receiver assign positions in advance: that head-of-line blocks, and
DuckLake pins its head at transaction begin, so a position would be held
for a whole transaction.

### The leader role

Leadership is a role a process holds, not a deployment a user chooses —
the advisory counterpart of the folder. A process holding the fenced
writer in a **long-lived posture** also opens a listener and announces;
an ephemeral fold sprint does not. Folder fenced and safety-relevant,
leader advisory and safety-irrelevant.

**Announcement rides the log.** Address and instance id travel in the
envelope's advert field — on a real commit when one is queued, else an
empty commit. Announce on taking the role, withdraw on leaving it (an
advert with no endpoint — distinct from a slot carrying no advert, which
says nothing about leadership), nothing in between: no heartbeats. The folder folds the freshest advert
into a `sys` key so it survives truncation. Clients are bucket-attached,
so discovery needs no RPC: they see the advert during tail replay they
already perform.

**A stale advert costs one bounded probe, and nothing decays it
globally.** Direct commits above an advert prove nothing — uncontended
clients commit directly past a healthy leader by design — so slot traffic
is *not* a liveness signal, and none is needed. A client that cannot
connect ages the hint locally (commit directly, stop trying this
session). A dead leader's advert is *superseded*, not decayed: its
absence eventually grows the unfolded tail, the tail signal appoints a
successor folder, a long-lived successor announces, and freshest advert
wins. Fencing already forces a leader that loses the folder role to
withdraw, so an advert cannot outlive its holder's posture longer than
the gap until someone needs the role again.

**Clients forward only under contention, and the trigger is one lost
race.** A lost race is proof of contention now; no threshold or window is
needed, and an uncontended client never connects. Forwarding starts at a
transaction boundary, because chaining requires it: the transaction's
catalog reads must run through the leader from its start. The lost race
itself provides that boundary — on the staged path it surfaces to
DuckLake, whose retry re-drives as a fresh transaction, and the re-drive
forwards. Even a session that lives for one commit gets relief on its
second attempt: direct first try, forwarded retry. A client then stays
forwarded while the leader is reachable and its advert fresh, and drops
back to direct the moment either fails. Always connecting when an advert
exists is rejected: it would make every session of every client pay a
timeout against a crashed leader's advert (which nothing decays on a
quiet store), and it would route the whole fleet through one process —
and expose it to that process's death — without contention to justify
either.

- **Sessions are connection-scoped.** A forwarded `BEGIN…COMMIT` pins to
  its leader. A dropped connection rolls the session back; DuckLake's
  retry re-drives, landing directly or on a successor.
- **No acknowledgement precedes a durable slot PUT.** A leader that dies
  with accepted-but-unwritten work leaves clients an ambiguous outcome,
  resolved by the transaction-id scan like any other.
- **Error kinds are a wire contract.** DuckLake's retry loop dispatches
  on error text, so a conflict must reach the client carrying the
  substring that loop scans for, and an exhausted budget must not. The
  protocol carries typed kinds; text is rendered client-side.
- **Authentication comes from the bucket.** A store-held secret — minted
  at creation, or by an ordinary commit on first need for older stores —
  is readable through bucket access, which is already the trust boundary:
  anyone holding it can write the log directly. Constant-time compare;
  TLS for cross-host transport is deployment documentation.
- **Duels are transient waste.** Two announcers resolve by freshest
  advert; the superseded leader drains, withdraws, stands down.

**Reachability is best-effort.** Processes that share only a bucket may
share no network path; a client that cannot reach the advertised address
times out once and commits directly. The direct path is therefore not a
fallback that can rot: it is the substrate, exercised by every
uncontended client, every unreachable pairing, and the leader's own
commits.

### Contention behavior

Slot races are lock-free with a guaranteed winner per round; the system
cannot livelock. Under sustained contention the failure shape is
quadratic request amplification (~N²/2 PUTs to drain N commits) — damped
by coalescing, capped by backoff, and relieved by the leader role, which
admits contenders serially so losers spend nothing at the object store.
The relief recruits itself: losing races is the trigger to forward.

The race can also be tipped. Winning a slot is a timing outcome, never a
right, so backoff asymmetry is a legitimate lever: the leader retries hot
(near-zero base), direct clients retry with standard jitter. A sustained
process then wins most contested rounds — on top of its natural edge, a
warm head that skips materialization before the PUT — and the bias
compounds with the trigger: the ephemeral loser's retry forwards, landing
in the winner's next coalesced envelope. Two bounds keep it fair: the
leader attempts once per coalesced batch, not continuously, and the
standard backoff cap stays modest so a client that cannot reach the
leader is delayed, never starved.

Distinguish this **mechanical contention** from **semantic contention**
(genuinely overlapping change sets), which conflicts under any scheme and
surfaces through RFC 0004's model unchanged.

The cost profile inverts the old topology's. The WAL flush timer capped
write PUTs regardless of commit rate; slots are linear in it. And
materialization performs a LIST — billed in the PUT tier — where a
reader's cached view previously cost nothing. Both are bounded by a
willingness-to-wait knob, the same bargain SlateDB's flush and poll
intervals make: a coalescing window on the write side, a refresh interval
on the read side.

Measured over the coalescing bench (50 single-statement commits, real
SlateDB on in-memory `object_store`): the slot path costs 1.00 PUT/commit
serial (LIST 1.02/commit, GET 25.60/commit before the head-materialization
cache) and 0.02 PUT/commit concurrent, identical at a zero and a 5ms
coalescing window. The head cache holds serial GET at 0.12/commit instead
of 25.60/commit. Folding amortizes further: many slots land in one L0
SST. These figures sit inside the accepted band.

The leader's relief is measured over the adaptive bench (`adaptive_leader_bench`:
a fleet of contending committers on one bucket, real SlateDB on in-memory
`object_store`, PUTs counted per store). Direct racing amplifies with the
fleet: PUT/commit rises from 1.9 at two committers to 4.2 at sixteen, the
write amplification the leader exists to relieve. With a leader present the
fleet forwards and coalesces: at eight committers PUT/commit falls to 0.26
at a zero coalescing window and 0.13 at a 2 ms window, with a 100% win
share — every commit lands through the leader, so no committer races a slot
of its own. Killed mid-run, the survivors degrade to direct racing: a
bounded spike back toward the direct figure, never a stall, every commit
still landing. The no-leader figure is the pre-leader path unchanged — with
no advert on the log a client never forwards — so the role costs nothing
when absent. The bias strength follows from these numbers: a near-zero
backoff base converges the whole fleet at eight contenders, so the tip needs
no strengthening; and widening the coalescing window (zero to 2 ms) halves
the leader's own slot PUTs, which is the convergence lever the design reaches
for before any harder backoff asymmetry — it absorbs more per win without
changing what a client that cannot reach the leader experiences.

### Format and compatibility

The log stamps a new structural format version (RFC 0002's mechanism):
the store gains a `commits/` prefix, a fold cursor in its own manifest, and log-derived
id allocation, and an older binary — which would commit through the RFC
0004 path and bypass the log — must refuse it.

Existing stores convert by RFC 0015's *trivial metadata migration*: the
store already is the folded state and the log starts empty, so the whole
migration is one atomic batch writing the format stamp and a zero fold
cursor. No marker, no cursor loop, no keyspace walk; idempotent by the
format check. It **auto-runs on read-write attach**, settling RFC 0015's
open trigger-boundary question for exactly this bounded class. Migrating
fences any incumbent old-binary writer — safe by RFC 0004 fencing, and a
release-note item. Read-only attaches never migrate and need not: an
absent fold cursor reads as zero with an empty tail.

RFC 0011's crash cases grow: crash between payload and slot, crash
after slot before ack, folder crash mid-batch, fold-then-truncate races,
folder-takeover fencing races, concurrent migrations, a destroyed or
substituted slot, a stale reader meeting a peer's truncation, leader loss
mid-session. Each must resolve to "durable and discoverable" or "never
happened" — nothing between. The injection mechanism grows too: the
interesting crash boundaries are now PUT boundaries, including the
ambiguous PUT, so the matrix gains a fault-injecting object-store
decorator alongside drop-and-reopen.

## Alternatives considered

- **Keeping single-writer as a supported mode.** Doubles the commit path
  forever, to preserve a topology whose one advantage — amortized WAL
  batching — coalescing recovers. One path, one set of invariants.
- **The batcher as originally specified** ("accepts forwarded change
  sets, folds many into one slot"). Cannot be built: forwarded change
  sets carry baked ids, so concurrent clients' commits are an unfoldable
  fan, and no receiver may re-author them. The leader role keeps its
  adaptive structure — advisory, self-appointed, log-discovered,
  contention-triggered — and fixes what is forwarded: the transaction.
- **An explicit served deployment mode** (clients configured with leader
  addresses; optionally thin credential-free clients). Rejected as a
  user-facing concept: it forks deployment into modes and moves discovery
  into configuration, when every client already holds bucket access (for
  discovery and auth) and a direct path (for fallback). The wire format
  is the same, so an always-forward thin client remains reachable later.
- **A declarative operation wire format** (forward intent — names, not
  ids — so a receiver authors against its own head). Works, and would
  give a direct-committing fleet cross-process grouping. Deferred: it
  covers only the statically expressible subset of the verb API, and the
  leader delivers grouping for the fleet shape that motivates it.
- **Replicated WAL via consensus (openraft).** Millisecond durability,
  but a stateful service and recent commits' sole copy on node disks. It
  remains the only path below object-store latency.
- **Consensus for leader election.** Pays quorum liveness for an
  exclusion guarantee the design never uses — serialization already comes
  from the conditional put, and appointment is advisory.
- **Mandatory leader (no leaderless base).** Hangs commit liveness on one
  process, and a partitioned client could only seize the writer role,
  inviting ping-pong. The contention-triggered role dissolves both.
- **Cooperative multi-writer inside SlateDB.** Its WAL files are already
  `PutMode::Create`; treating a lost race as contention rather than
  fencing would let many writers share one WAL, and the fenced folder
  would go too. The log already *is* the store's write-ahead log, so what
  remains is upstream: contention where fencing is today. That change
  lands as a deletion here — the folder role retires and coalescing and
  the leader role survive, since neither depends on how commits are
  serialized.
- **External object-storage queue (e.g. OpenData Buffer).** Its consumer
  contracts (deterministic commit identity, ack after durable sink
  commit) validate the folder design and are borrowed. The queue itself
  is blind-append — no conditional commit, which is the whole mechanism.
- **Sequence-then-validate (Calvin-style).** Blind-append intents,
  validate at apply order. Coherent, but the durability ack no longer
  means the transaction succeeded, and admission still serializes on a
  conditional put somewhere.
- **A content-digest chain over slots.** Predecessor digests verified on
  replay. Not built: both damage classes are already covered (hole
  detection; continuity), and against tampering a chain adds nothing when
  every candidate anchor lives in the same bucket under the same
  credential. Adopt only alongside an external anchor — or if speculative
  pipelining of slot PUTs is ever wanted, which the numeric continuity
  check cannot make safe alone.
- **Lease objects or election services for leader discovery.** Advisory
  leadership needs no consensus-grade guarantee. In-log announcement
  rides reads every participant already performs, and its failure signal
  cannot be stale relative to the commits themselves.

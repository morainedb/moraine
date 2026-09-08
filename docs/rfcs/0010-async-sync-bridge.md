# RFC 0010: Async↔sync bridge: mechanism, runtime, threading, and cancellation

- **Date:** 2026-07-09

## Summary

RFC 0003 makes the moraine core `async` and states it "spawns no runtime and
no threads of its own — the caller drives it," scoping the DuckDB extension
entry points and the sync↔async bridge out to RFC 0006 and this RFC. RFC 0006
locates the
bridge — "the C-ABI layer owns the tokio runtime and `block_on`s core futures"
— but does not specify the runtime's shape, how it stays live for SlateDB's
background work, how a DuckDB query interrupt cancels an in-flight catalog
operation, or how a panic is kept from crossing the C ABI. This RFC settles
those. It first **chooses the bridge mechanism** — `block_on` on a
multi-threaded runtime, over a completion-callback or a
channel-to-a-background-runtime, because object-store latency dominates any
bridge overhead by three to four orders of magnitude — then designs the
runtime that choice needs: **one long-lived multi-threaded tokio runtime per
moraine catalog instance**, owned by the C-ABI layer; FFI entry points
`block_on` core futures from DuckDB's (external, non-runtime) threads; DuckDB
interrupts map to **future cancellation** with a defined cancel-safety point at
the commit write; and every FFI boundary **`catch_unwind`s** so a panic becomes
a typed error, never undefined behavior.

## Goals

- **Pick the bridge mechanism deliberately.** Choose how a synchronous FFI call
  drives an async core — `block_on`, a completion-callback, or a
  channel-to-a-dispatcher — with the tradeoffs recorded, so it is a decision and
  not an accident, and no more complex than the workload justifies.
- **The core stays runtime-free.** No `tokio::runtime` or thread spawning in
  `moraine` (RFC 0003). The runtime lives entirely in the `moraine-duckdb`
  C-ABI layer; a pure-Rust host brings its own and awaits directly. The core
  does spawn *tasks* onto whichever runtime is driving it — that is how the
  commit write is shielded, below — but it never creates one.
- **Background work makes progress.** SlateDB's background tasks (WAL flush,
  compaction) and concurrent DuckDB scan threads all progress while one or more
  DuckDB threads are blocked on a catalog call.
- **`block_on` is used only where it is safe.** FFI entry points are invoked on
  DuckDB's own OS threads, which are *outside* the runtime, so `block_on` there
  cannot deadlock a worker. Core code never `block_on`s.
- **Cancellation is honored and safe.** A DuckDB query interrupt cancels an
  in-flight catalog operation at an `await` point and returns a typed
  `Interrupted`; cancellation before the RFC 0004 commit write has no effect
  on catalog state, and a committed commit is never "half-cancelled" — the
  write itself is shielded from cancellation, at the documented cost that an
  interrupt racing the write yields an ambiguous (`Interrupted`-but-landed)
  outcome equivalent to RFC 0011's `CommitDurableNotAcknowledged`.
- **Panics never cross the C ABI.** Every FFI boundary catches unwinding and
  converts it to a typed error mapped to a DuckDB error code (RFC 0006), never
  UB. Consistent with the CLAUDE.md no-panic-in-library policy and the
  `// SAFETY:`-commented `unsafe` allowed only in `moraine-duckdb`.

Non-goals:

- **The core's async API surface** — RFC 0003. This RFC wraps it; it does not
  change it.
- **`StorageExtension` registration, C-ABI marshalling, error-code mapping** —
  RFC 0006. This RFC covers the runtime, threading, and cancellation that ride
  *inside* that C-ABI layer.
- **The runtime of a pure-Rust embedding host.** RFC 0003 already says the host
  provides it; this RFC covers only the DuckDB extension's runtime.
- **The commit protocol** — RFC 0004. This RFC defines where cancellation is
  safe *relative to* that protocol, not the protocol itself.

## Background

RFC 0001 forbids the core from owning threads and confines `unsafe` to
`moraine-duckdb` with a `// SAFETY:` comment; RFC 0003 makes the core surface
`async` because "SlateDB is async (tokio)" and insists the core "spawns no
runtime and no threads of its own — the caller drives it, and the DuckDB
bridge owns the sync↔async translation," leaving the bridge to this RFC.
RFC 0006 makes `moraine-duckdb` a thin C++ shim over a C ABI to the Rust core,
with the sync↔async bridge in the Rust C-ABI layer that "owns the tokio
runtime and `block_on`s core futures, so the C++ shim only ever calls
synchronous C functions," and scopes the extension so **moraine does not own
the data-file read/write path** — DuckDB and DuckLake do.

The problem this leaves: DuckDB drives an extension synchronously, from its own
execution threads, and expects those calls to return values and to be
interruptible; the moraine core is a set of `async fn`s over SlateDB. Bridging
them needs a runtime, a `block_on` discipline that cannot deadlock, a
cancellation path, and panic containment.

## Design

### The bridge mechanism: `block_on` vs. the alternatives

Bridging a synchronous DuckDB call to the async core has three real mechanisms.
This RFC chooses the first; the rest of the Design section implements it.

**A. `block_on` on a multi-threaded runtime (chosen).** The FFI entry point, on
DuckDB's own (external, non-runtime) thread, `block_on`s the core future;
worker threads drive the IO and SlateDB's background tasks.

- *For:* the simplest possible bridge; matches DuckDB's synchronous operator
  model exactly (call, wait, return); the calling thread is external to the
  runtime, so `block_on` cannot deadlock a worker (see next subsection); the C
  ABI stays synchronous (RFC 0006); cancellation is a `select!` of the future
  against an interrupt probe (see cancellation, below).
- *Against:* one OS thread is parked per concurrent in-flight call — parked
  *threads*, not cores, bounded by DuckDB's thread count. This is why the
  runtime must be multi-threaded: background IO must progress while those
  threads park.

**B. Completion-callback (async C ABI).** The FFI call submits the operation and
returns immediately; the core future runs on the runtime and invokes a C
callback on completion that resumes DuckDB.

- *For:* no thread parked while waiting; in principle more concurrency from
  fewer threads.
- *Against:* DuckDB's storage/catalog extension points are **synchronous** (RFC
  0006 registers a synchronous `StorageExtension`); there is no
  async-operator / suspend-resume contract for a catalog callback to resume
  into, so it would park the DuckDB thread anyway (defeating its only advantage)
  or demand deep DuckDB changes moraine does not control. It also adds a
  callback protocol crossing the C ABI — more surface, more unwind-across-FFI
  risk — against RFC 0006's "thin, synchronous C functions." Should DuckDB
  ever grow an async catalog or operator contract, this is the option to
  re-open — but nothing here is built in anticipation of one.

**C. Channel to a background runtime (dispatcher/actor).** The runtime runs on
dedicated background thread(s); the FFI thread serializes each request onto a
channel to a dispatcher and blocks on a reply channel.

- *For:* decouples the runtime from calling threads; a single owned dispatcher
  could serialize commits, echoing RFC 0004's single-committer funnel.
- *Against:* it is `block_on` with extra hops — the calling thread still blocks
  on the reply, saving nothing on latency — plus channel queueing and a
  dispatcher to manage. tokio's multi-threaded runtime already *is* a
  work-stealing dispatcher, so A gets the same behavior with less machinery. Its
  one genuine use, funneling commits through one committer, is an RFC 0004
  concern realizable *above* the bridge.

**Decision: A.** B and C exist to avoid parking a thread; A parks a thread but
is the simplest correct mechanism and the only one that matches DuckDB's
synchronous execution model without faking async (B collapses back to blocking)
or re-implementing the runtime's own dispatcher (C).

**Why bridge overhead is the wrong thing to optimize.** This is the crux and the
reason the elaborate options lose. Per-call bridge overhead — parking a thread,
a `select!`, a channel hop — is on the order of **microseconds**. The operation
it wraps is one or more **object-store round trips**: a commit is PUT-bound
(~5–10 ms S3 Express, ~50–100 ms S3 Standard, per the README), a cold read is a
GET. Bridge overhead sits **three to four orders of magnitude below** the
operation, so a more sophisticated bridge optimizes noise. This inverts the
usual "async avoids blocking threads, so it is faster" intuition: async's
thread-efficiency wins when many tasks each wait on cheap IO and threads are the
scarce resource; here the waited-on thing is a tens-of-milliseconds PUT and the
parked resource is a *thread* (bounded by DuckDB's thread count), not a core.
Parking a thread to wait on a 50 ms PUT costs nothing worth reclaiming. The
rule: pick the simplest correct mechanism and spend complexity where the
milliseconds are — WAL group commit (RFC 0004), read consistency and caching
(RFC 0009) — not on the bridge.

### One runtime, multi-threaded, per catalog instance

The C-ABI layer creates **one `tokio` multi-threaded runtime per attached
moraine catalog** (per `slatedb::Db` / `Catalog` instance, RFC 0003), at
`ATTACH` time, and holds it for the instance's lifetime. It is:

- **Multi-threaded**, because multiple DuckDB threads (parallel scans) may each
  `block_on` a catalog call at once, and SlateDB's background flush/compaction
  tasks need worker threads to run *while* those threads are blocked. A
  `current_thread` runtime would stall background IO whenever a DuckDB thread
  blocked, deadlocking parallel readers.
- **Long-lived**, because SlateDB's durability and compaction run on its
  background tasks; the runtime must outlive any single call and live as long
  as the catalog is attached. It is dropped (gracefully, flushing SlateDB) at
  `DETACH`.
- **Per instance, not global**, because each attached moraine catalog owns its
  own `slatedb::Db` and its lifecycle is tied to that attach. Multiple attached
  moraine catalogs get independent runtimes; this keeps `DETACH` teardown local
  and avoids a process-global whose worker count must serve an unknown number
  of catalogs. (Revisit if many-catalog processes make a shared runtime worth
  the coupling.)

**Worker count: DuckDB's own thread setting, clamped to `2..=8`.** The attach
entry point takes the host's execution-thread count and sizes the pool from
it. Three constraints fix the rule:

- **Track the host.** DuckDB's `threads` setting is the only number in the
  process that states how much parallelism the operator asked for, and it
  bounds how many DuckDB threads can be inside a catalog call at once. A
  session pinned with `SET threads=1` must not get a pool sized to the
  machine, on top of DuckDB's own.
- **Floor of two.** A CPU-bound poll holds its worker to completion, and
  SlateDB's flush and compaction have to progress while it does — the same
  reason the runtime is multi-threaded at all, so it is a floor and not a
  tuning knob.
- **Ceiling of eight.** The pool waits on object storage, which yields its
  worker at every await: four workers absorb thirty-two concurrent
  materializations at a flat batch time, and eight concurrent decodes leave a
  short call at ~0.4 ms (`BENCHMARK.md` → Core measurements, both halves).
  Past a handful, further workers only park — and they park in addition to
  DuckDB's own threads, on cores DuckDB already sized itself to.

The size is fixed at attach. A session that changes `threads` afterwards keeps
the pool it attached with; rebuilding a runtime that owns live background
tasks costs more than the mismatch does.

### `block_on` discipline

DuckDB calls each C-ABI entry point on one of DuckDB's own OS threads — a
thread that is **not** a runtime worker. The entry point acquires the
instance's runtime `Handle` and `block_on`s the core future:

- **Legal here.** `block_on` on a non-runtime thread parks that thread while
  worker threads drive the future and SlateDB's background tasks. Because
  DuckDB's calling threads are external, no worker thread is ever blocked by a
  `block_on`, so the classic "`block_on` inside the runtime deadlocks"
  hazard cannot arise.
- **The rule.** FFI entry points (external DuckDB threads) `block_on`; **core
  code never `block_on`s** — it only `.await`s, so an embedding host (RFC 0003)
  can drive it on its own runtime unchanged. This split is the one invariant
  that keeps the async core portable and the bridge deadlock-free.

### No reentrancy across the FFI in the catalog path

RFC 0006 scopes moraine to catalog *metadata*: DuckDB/DuckLake own data-file
reads and writes. Therefore a catalog operation **never calls back into DuckDB**
mid-flight — it reads and writes SlateDB and returns. There is no re-entrant
"core → DuckDB → core" call chain in the catalog path, so no nested `block_on`
and no cross-FFI reentrancy hazard. This simplifying invariant is a *gift* of
RFC 0006's scoping and is stated here so it is not accidentally broken by
routing a data-path callback through the catalog FFI.

### Cancellation maps DuckDB interrupt → future drop

DuckDB supports query interruption; a long catalog operation (a large time-
travel materialization, a commit stalled on a slow object-store PUT) must
respond. The bridge implements cancellation as:

1. The FFI entry point `block_on`s a `select!` of the operation against a
   **cancellation probe** supplied by the caller.
2. On interrupt, the cancellable portion of the operation is **dropped**
   (async cancellation *is* drop), unwinding its `await` stack and releasing
   resources, and the entry point returns a typed **`Interrupted`** (RFC
   0003 taxonomy → DuckDB error code, RFC 0006).

**How the interrupt arrives: a pollable flag, not a callback or a handle.**
DuckDB offers no way to push a cancellation into an extension — there is no
callback to register and no cancellation handle to hold. What it does offer
is `ClientContext::IsInterrupted()`, one atomic load of the same flag its own
executor polls, safe to call from any thread. So the bridge pulls: every
cancellable entry point takes a probe function pointer and an opaque context,
the shim passes its `moraine_shim_is_interrupted` and the `ClientContext`, and
the bridge polls it — once before the future is first polled, then every
~100 ms while it is pending. The pre-poll check is load-bearing: a timer's
first tick is pending even when already elapsed, so a future that completes on
its first poll would otherwise beat an interrupt that was already standing.

Two consequences follow from the probe being per **call** rather than per
handle. Cancellation is scoped to the query that asked for it — several
connections share one attached catalog, and one connection's Ctrl-C must not
abort or be consumed by another's query. And the signal is level-triggered:
nothing consumes it, so a probe that reports an interrupt cancels every call
that observes it, and a later call with a quiet probe succeeds normally.

`DETACH` takes no probe and never will: it is teardown, and its wait is the
flush that makes committed data durable.

**The commit write is shielded — `select!` alone is not the design.** A bare
`select!` over the whole commit future would drop it at *whatever* `await`
it is parked on when the interrupt fires — including inside the durable
batch write itself, where the write has already been issued and dropping
the future cannot retract it. No design can both race a probe against the
whole future *and* promise that past the point of no return the operation
runs to completion; those are contradictory mechanisms. The bridge is
therefore split at the point of no return:

- **Phases before the batch write** (load-head, id allocation, batch
  assembly, conflict retry) run inside the `select!` and are freely
  cancellable: dropping them discards only staged in-memory state.
- **The batch write itself is spawned onto the runtime** rather than
  awaited inline in the cancellable future, so no interrupt — and no drop
  of the FFI-side future — can abort a write mid-flight. The caller then
  waits for the spawned task, still racing the probe.

Both commit paths take that split. The verb path's group commit (RFC 0004)
already had it structurally: a batch is spawned the moment it seals, and its
members wait on a channel rather than on the write, because a member that
sealed the batch must not be able to take every other member's commit down
with it when its own caller is interrupted. The staged-row path has no batch
to share, so it spawns its own write and waits on the join handle.

A spawned write that never reports back — its runtime shut down, or the task
lost to a panic — leaves the outcome unknown, which is the one answer a
caller must not read as "nothing landed". The core raises `CommitOutcomeUnknown` for
it. The caller retains external files and reconciles the operation before
resubmitting; a newer head alone does not resolve its fate.

The bridge observes whether it has polled the commit future, not the exact
submission instant inside the core. It reports `INTERRUPTED` only before that
first poll; later cancellation conservatively reports `COMMIT_OUTCOME_UNKNOWN`.
The physical cases below explain why preparation may safely stop while a
submitted write must survive.

**Cancel-safety, relative to RFC 0004**, then has three cases instead of a
clean two:

- **Cancelled before the write is spawned**: dropping preparation discards
  its staged state. The ABI reports `INTERRUPTED` only if the commit future
  was never polled; once polling starts it conservatively reports
  `COMMIT_OUTCOME_UNKNOWN` because it does not observe the core's exact
  submission boundary.
- **Cancelled while the spawned write is in flight**: the bridge returns
  `COMMIT_OUTCOME_UNKNOWN` promptly and the spawned write continues. It may
  land durably or fail, but is never torn. The caller retains external files
  and reconciles the operation before resubmitting. A newer head by itself
  does not establish whether this operation landed.
- **The core future reports its result**: the bridge returns the committed
  snapshot normally. If cancellation stops the wait before that result is
  received, the outcome remains unknown even if the write is already durable.

Read operations (materialization, RFC 0009) are trivially cancel-safe —
they are pure `select!`, dropping them frees the read-snapshot and touches
no durable state.

### Panic containment at the boundary

Unwinding across the C ABI is undefined behavior. Every FFI entry point wraps
its `block_on` in `catch_unwind`; a caught panic becomes a typed internal error
mapped to a DuckDB error code (RFC 0006), and the process survives. This is the
enforcement point for CLAUDE.md's no-`panic`/`unwrap`/`expect` policy at the
one place the policy could otherwise cause UB rather than a clean error, and it
is the kind of boundary `unsafe` that `moraine-duckdb` carries a `// SAFETY:`
comment for.

### Two provisioning stories, one core

The runtime is provisioned in exactly one of two ways, never both:

| Host | Runtime | How the core is driven |
|---|---|---|
| Pure-Rust embedding (RFC 0003) | the host's own tokio runtime | `.await` directly |
| DuckDB extension (this RFC) | one per-instance runtime in the C-ABI layer | FFI `block_on` |

The core is identical in both; only who owns the runtime differs. This is the
concrete meaning of RFC 0003's "the caller drives it."

### Test obligations

Per RFC 0001, tested against real SlateDB on in-memory `object_store`; the FFI
paths are exercised by the `cargo xtask e2e` DuckDB harness (RFC 0006):

- **Concurrency progresses.** Multiple threads each `block_on` a catalog call
  concurrently and all complete; SlateDB background flush advances while a
  thread is blocked (no `current_thread` stall).
- **Cancel before polling.** An operation interrupted before its commit future
  is polled returns `INTERRUPTED` and leaves catalog state exactly as before.
  Cancellation after polling begins reports `COMMIT_OUTCOME_UNKNOWN` even
  when the future was still preparing.
- **Cancel during the shielded write.** An interrupt delivered while the
  spawned batch write is in flight returns `COMMIT_OUTCOME_UNKNOWN` promptly; the
  write still completes (or fails) in the background, never torn — a
  subsequent read observes head at exactly `N` or exactly `N+1`, and a
  re-drive behaves per RFC 0011's `CommitDurableNotAcknowledged`.
- **Commit past the point of no return.** An interrupt delivered after the
  durable write completed still reports the committed snapshot; state is
  consistent.
- **Read cancellation.** An interrupted materialization returns `Interrupted`,
  releases its read-snapshot, and leaves no durable effect.
- **Panic containment.** A forced panic in a core call is caught at the FFI
  boundary and surfaced as a DuckDB error; the process does not abort and the
  runtime remains usable for the next call.
- **Teardown.** `DETACH` drops the runtime and flushes SlateDB with no
  outstanding-task leak.

## Alternatives considered

- **A runtime per DuckDB connection.** Rejected: connection-scoped runtimes
  churn thread pools and, worse, SlateDB's background flush/compaction want a
  stable runtime tied to the `Db`'s lifetime, not to whichever connection
  happens to be open. Per-catalog-instance matches the resource that actually
  owns background work.
- **A `current_thread` runtime driven by the calling DuckDB thread.** Rejected:
  a single-threaded runtime cannot advance SlateDB background IO while a DuckDB
  thread is blocked in `block_on`, so parallel scans deadlock and durability
  stalls. Multi-threaded is required, not merely preferred.
- **The completion-callback (B) and channel-to-dispatcher (C) mechanisms.**
  Analyzed and rejected in "The bridge mechanism" above: B needs an async DuckDB
  execution contract that does not exist and collapses back to blocking without
  it; C is `block_on` with extra hops and no latency benefit.
- **A hybrid that ships A but adds C's dispatcher speculatively.** Rejected: it
  pays C's complexity now for a commit-funnel need RFC 0004 can add later if a
  many-committer process appears. Ship A; add a dispatcher as an RFC 0004-layer
  change only when a workload demands it.
- **Making the core synchronous (drop async, call SlateDB blocking).** Rejected:
  SlateDB is async (RFC 0001), and a sync core would have to `block_on`
  *internally*, welding the core to a runtime and breaking RFC 0003's
  runtime-free, host-drivable surface. The async core is what lets a pure-Rust
  host embed moraine with its own runtime.
- **Ignoring cancellation (let long operations run to completion).** Rejected:
  DuckDB users expect query interruption to work; a commit stalled on a slow
  object-store PUT would hang a session uninterruptibly. Cancel-safety is cheap
  because RFC 0004's atomic-batch boundary already defines the only point of no
  return.
- **Letting panics unwind across the C ABI (or aborting the process on panic).**
  Rejected: unwinding across the C ABI is UB, and aborting turns a recoverable
  catalog error into a process crash. `catch_unwind` at every boundary,
  converting to a typed error, is mandatory and is why `moraine-duckdb` is the
  crate allowed boundary `unsafe`.

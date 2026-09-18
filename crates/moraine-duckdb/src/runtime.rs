//! Opaque handles owned across the FFI boundary, and the sync↔async
//! bridge: one tokio multi-threaded runtime per attached catalog.

use std::{
    ffi::c_void,
    future::Future,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use futures::future::join_all;
use moraine::{Catalog, CatalogSnapshot, ReadOnlyCatalog, TableId};
use tokio::{
    runtime::{Builder, Handle, Runtime},
    task::JoinHandle,
};
use tracing::{info, warn};

use crate::{
    error::AbiError,
    logging::{HandleId, enter_handle, tag_thread_for_handle},
};

/// A C-side cancellation probe polled while a cancellable call's core
/// future is pending; returning `true` cancels the call. `None` disables
/// the pull channel for that call. Mirrors `MoraineInterruptProbe` in
/// `cpp/moraine_abi.h`.
pub type MoraineInterruptProbe = Option<unsafe extern "C" fn(probe_ctx: *mut c_void) -> bool>;

/// How often a cancellable call polls its interrupt probe while the core
/// future is pending. The first poll fires immediately, so a pending
/// interrupt cancels before the future does any work.
const INTERRUPT_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// An attached catalog: owns the tokio runtime created at `ATTACH` and
/// the [`Catalog`] handle opened on it. Every FFI entry point `block_on`s
/// through `runtime`.
///
/// Opaque to C — only ever seen as a `MoraineCatalogHandle*` obtained
/// from [`moraine_attach`](crate::abi::moraine_attach) and released via
/// [`moraine_detach`](crate::abi::moraine_detach).
pub struct MoraineCatalogHandle {
    pub(crate) runtime: Arc<Runtime>,
    pub(crate) catalog: AttachedCatalog,
    /// Routes this handle's `tracing` events to its registered log sink.
    pub(crate) log_id: HandleId,
    /// The `DATA_PATH` object store, present only when `META_DATA_PATH`
    /// was given; index maintenance and scoped reads are skipped without it.
    pub(crate) data_store: Option<moraine::DataStore>,
    /// The bucket-relative key prefix of `DATA_PATH` (empty for a local or
    /// bare-bucket store), prepended to a data file's stored path.
    pub(crate) data_prefix: String,
    /// In-flight row-summary warming passes; ended by
    /// [`finish_warming`](MoraineCatalogHandle::finish_warming) before the
    /// catalog closes.
    warming: Mutex<Vec<JoinHandle<()>>>,
}

/// Which mode the attach opened in. A write on a read-only attach is
/// refused at runtime by [`AttachedCatalog::writer`].
pub(crate) enum AttachedCatalog {
    Writer(Catalog),
    Reader(ReadOnlyCatalog),
}

impl AttachedCatalog {
    /// The read surface, which both modes serve.
    pub(crate) fn reads(&self) -> &ReadOnlyCatalog {
        match self {
            Self::Writer(catalog) => catalog,
            Self::Reader(catalog) => catalog,
        }
    }

    /// The mutator surface, or the refusal a read-only attach gets.
    pub(crate) fn writer(&self) -> Result<&Catalog, moraine::Error> {
        match self {
            Self::Writer(catalog) => Ok(catalog),
            Self::Reader(_) => Err(moraine::Error::Constraint(
                "catalog opened read-only; writes are unavailable".to_string(),
            )),
        }
    }
}

impl MoraineCatalogHandle {
    pub(crate) fn new(runtime: Runtime, catalog: AttachedCatalog, log_id: HandleId) -> Self {
        let runtime = Arc::new(runtime);
        watch_runtime(&runtime, log_id);
        Self {
            runtime,
            catalog,
            log_id,
            data_store: None,
            data_prefix: String::new(),
            warming: Mutex::new(Vec::new()),
        }
    }

    /// A borrowed read-only surface; its owner drops it without closing the
    /// shared store.
    pub(crate) fn read_alias(&self, reads: ReadOnlyCatalog) -> Self {
        Self {
            runtime: Arc::clone(&self.runtime),
            catalog: AttachedCatalog::Reader(reads),
            log_id: self.log_id,
            data_store: self.data_store.clone(),
            data_prefix: self.data_prefix.clone(),
            warming: Mutex::new(Vec::new()),
        }
    }

    /// Spawns the attach's best-effort warming pass: with `preload`, every
    /// table's index and inline probe ranges into the block cache; with a
    /// `DATA_PATH` store, the row summaries a later located lookup would
    /// otherwise build cold. Spawns nothing when neither applies.
    pub(crate) fn spawn_warm_at_attach(&self, preload: bool) {
        let data_store = self.data_store.clone();
        if !preload && data_store.is_none() {
            return;
        }
        let catalog = self.catalog.reads().clone();
        let data_prefix = self.data_prefix.clone();

        self.track(self.runtime.spawn(async move {
            if preload {
                warm_all_tables(&catalog).await;
            }
            let Some(data_store) = data_store else {
                return;
            };
            if let Err(error) = catalog
                .warm_all_row_summaries(data_store, &data_prefix)
                .await
            {
                warn!(%error, "row summary warming skipped this attach");
            }
        }));
    }

    /// Spawns a best-effort pass warming the index and inline ranges of the
    /// tables a commit just registered data files against, then their row
    /// summaries. An empty `tables` spawns nothing; without a `DATA_PATH`
    /// store only the block cache is warmed.
    pub(crate) fn spawn_warm_tables(&self, tables: Vec<TableId>) {
        if tables.is_empty() {
            return;
        }
        let catalog = self.catalog.reads().clone();
        let data_store = self.data_store.clone();
        let data_prefix = self.data_prefix.clone();

        self.track(self.runtime.spawn(async move {
            if let Err(error) = catalog.warm_tables(&tables).await {
                warn!(%error, "table warming skipped this commit");
            }
            let Some(data_store) = data_store else {
                return;
            };
            if let Err(error) = catalog
                .warm_selected_row_summaries(data_store, &data_prefix, tables)
                .await
            {
                warn!(%error, "row summary warming skipped this commit");
            }
        }));
    }

    /// Retains `task` so a detach can end it, dropping already-finished
    /// passes.
    fn track(&self, task: JoinHandle<()>) {
        let Ok(mut warming) = self.warming.lock() else {
            return;
        };
        warming.retain(|task| !task.is_finished());
        warming.push(task);
    }

    /// Cancels the warming passes still in flight and waits for them to
    /// end, reporting how many were outstanding. Must precede the close:
    /// warming holds a catalog handle of its own.
    pub(crate) fn finish_warming(&self) -> usize {
        let tasks = match self.warming.lock() {
            Ok(mut warming) => std::mem::take(&mut *warming),
            Err(_) => return 0,
        };

        for task in &tasks {
            task.abort();
        }
        let outstanding = tasks.len();
        // A cancelled pass reports `JoinError`, which is the ask here.
        let _ = self.runtime.block_on(join_all(tasks));

        outstanding
    }

    /// Runs `future` on the handle's runtime, attributing events the
    /// calling thread emits to this handle.
    pub(crate) fn block_on<F: Future>(&self, future: F) -> F::Output {
        let _guard = enter_handle(self.log_id);
        self.runtime.block_on(future)
    }

    /// Runs `future` on the handle's runtime unless cancelled first by
    /// `probe` returning `true` (polled immediately, then every
    /// [`INTERRUPT_POLL_INTERVAL`]). Cancellation drops the future and
    /// returns the interrupted error.
    ///
    /// # Safety
    ///
    /// `probe`, if `Some`, must be safe to call with `probe_ctx` from any
    /// thread for the duration of this call.
    pub(crate) unsafe fn block_on_cancellable<T, E>(
        &self,
        probe: MoraineInterruptProbe,
        probe_ctx: *mut c_void,
        future: impl Future<Output = Result<T, E>>,
    ) -> Result<T, AbiError>
    where
        AbiError: From<E>,
    {
        let _guard = enter_handle(self.log_id);
        // SAFETY: forwarded caller contract.
        unsafe { block_on_cancellable_in(&self.runtime, probe, probe_ctx, future) }
    }

    /// Cancels before polling with certainty; after polling, the commit may
    /// have landed.
    ///
    /// # Safety
    ///
    /// `probe` must be safe to call with `probe_ctx` for this call's duration.
    pub(crate) unsafe fn block_on_commit<T, E>(
        &self,
        probe: MoraineInterruptProbe,
        probe_ctx: *mut c_void,
        future: impl Future<Output = Result<T, E>>,
    ) -> Result<T, AbiError>
    where
        AbiError: From<E>,
    {
        let started = std::cell::Cell::new(false);
        // SAFETY: forwarded caller contract.
        let result = unsafe {
            self.block_on_cancellable(probe, probe_ctx, async {
                started.set(true);
                future.await
            })
        };
        result.map_err(|error| {
            if started.get() && error.code == crate::error::codes::INTERRUPTED {
                <AbiError as From<moraine::Error>>::from(moraine::Error::CommitOutcomeUnknown(
                    "the caller stopped waiting".into(),
                ))
            } else {
                error
            }
        })
    }
}

/// Warms the probe ranges of every table in the head view; failures are
/// logged, never returned.
async fn warm_all_tables(catalog: &ReadOnlyCatalog) {
    let snapshot = match catalog.snapshot().await {
        Ok(snapshot) => snapshot,
        Err(error) => {
            warn!(%error, "table warming skipped this attach");
            return;
        }
    };
    let tables = snapshot
        .schemas()
        .into_iter()
        .flat_map(|schema| snapshot.tables_in(schema.id))
        .map(|table| table.id)
        .collect::<Vec<_>>();

    if let Err(error) = catalog.warm_tables(&tables).await {
        warn!(%error, "table warming skipped this attach");
    }
}

/// Runs `future` on `runtime` unless `probe` cancels it first. Cancellation
/// is per call, not per handle: concurrent calls on one handle cancel
/// independently.
///
/// # Safety
///
/// `probe`, if `Some`, must be safe to call with `probe_ctx` from any
/// thread for the duration of this call.
pub(crate) unsafe fn block_on_cancellable_in<T, E>(
    runtime: &Runtime,
    probe: MoraineInterruptProbe,
    probe_ctx: *mut c_void,
    future: impl Future<Output = Result<T, E>>,
) -> Result<T, AbiError>
where
    AbiError: From<E>,
{
    // Checked before the first poll: a future that completes immediately
    // must not win over an already-pending interrupt.
    if let Some(probe) = probe {
        // SAFETY: caller contract — `probe` is callable with `probe_ctx`
        // for the duration of this call.
        if unsafe { probe(probe_ctx) } {
            return Err(AbiError::interrupted());
        }
    }

    runtime.block_on(async {
        let probe_fired = async {
            let Some(probe) = probe else {
                return std::future::pending::<()>().await;
            };
            let mut ticks = tokio::time::interval(INTERRUPT_POLL_INTERVAL);
            loop {
                ticks.tick().await;
                // SAFETY: caller contract — `probe` is callable with
                // `probe_ctx` for the duration of this call.
                if unsafe { probe(probe_ctx) } {
                    return;
                }
            }
        };

        // `biased`: a cancellation signal wins whenever ready, even if
        // the core future is also immediately ready.
        tokio::select! {
            biased;
            () = probe_fired => Err(AbiError::interrupted()),
            result = future => result.map_err(AbiError::from),
        }
    })
}

/// How long a cancelled attach waits for its abandoned runtime to wind
/// down before abandoning it in turn; a plain runtime drop would block on
/// the half-built store's background tasks.
pub(crate) const CANCELLED_ATTACH_SHUTDOWN: Duration = Duration::from_secs(5);

/// A materialized snapshot view, held across the FFI boundary so
/// listing calls need no further store I/O.
///
/// Opaque to C — only ever seen as a `MoraineSnapshotHandle*` obtained
/// from [`moraine_snapshot`](crate::abi::moraine_snapshot) and released
/// via [`moraine_snapshot_free`](crate::abi::moraine_snapshot_free).
pub struct MoraineSnapshotHandle {
    pub(crate) snapshot: Arc<CatalogSnapshot>,
    pub(crate) read_alias: Option<Box<MoraineCatalogHandle>>,
    pub(crate) read_identity: Option<moraine::IndexReadIdentity>,
}

impl MoraineSnapshotHandle {
    pub(crate) fn new(snapshot: Arc<CatalogSnapshot>) -> Self {
        Self {
            snapshot,
            read_alias: None,
            read_identity: None,
        }
    }
}

/// The fewest workers an attached catalog's runtime may have. Must stay
/// at least two: a CPU-bound poll must not stall SlateDB's flush and
/// compaction.
const MIN_WORKER_THREADS: usize = 2;

/// The most workers an attached catalog's runtime may have, however many
/// threads the host asks for.
const MAX_WORKER_THREADS: usize = 8;

/// The worker count for a host that asks for `requested` threads of its
/// own, or the [floor](MIN_WORKER_THREADS) if it asks for nothing.
pub(crate) fn worker_threads(requested: usize) -> usize {
    requested.clamp(MIN_WORKER_THREADS, MAX_WORKER_THREADS)
}

/// Builds the multi-threaded tokio runtime an attached catalog owns for
/// the lifetime of its handle, sized for a host running `requested`
/// threads of its own (`0` when the host does not say). Each worker is
/// tagged with `log_id` at spawn and named for it, so a thread dump can
/// tell which catalog a worker or driver belongs to. The size is fixed
/// at attach.
pub(crate) fn new_runtime(log_id: HandleId, requested: usize) -> std::io::Result<Runtime> {
    let runtime = Builder::new_multi_thread()
        .worker_threads(worker_threads(requested))
        .enable_all()
        // At most 15 bytes: the kernel truncates longer names in `/proc`.
        .thread_name(format!("moraine-{log_id}"))
        .on_thread_start(move || tag_thread_for_handle(log_id))
        .build()?;
    runtime.spawn(heartbeat());
    Ok(runtime)
}

/// How often the out-of-band watch samples its runtime.
const WATCH_INTERVAL: Duration = Duration::from_secs(30);

/// How long a probe sleeps before recording that a timer fired. Short
/// enough that a healthy runtime always finishes one within a sample.
const PROBE_SLEEP: Duration = Duration::from_secs(1);

/// What one probe task reached before the next sample read it.
#[derive(Default)]
struct RuntimeProbe {
    /// A worker began the task: the scheduler is still polling.
    polled: AtomicBool,
    /// The task's sleep returned: the time driver is still firing.
    timer_fired: AtomicBool,
}

/// Watches `runtime` from a thread that is not on it, until the handle
/// owning the runtime drops.
///
/// Every other instrument here runs as a task, so it reports only while
/// the runtime is healthy -- a stalled time driver, a scheduler that never
/// polls, and a dropped runtime are all silence from the inside. This
/// samples from a foreign thread and sleeps off the time driver, so its
/// records still arrive when nothing on the runtime can run.
fn watch_runtime(runtime: &Arc<Runtime>, log_id: HandleId) {
    watch_runtime_every(runtime, log_id, WATCH_INTERVAL);
}

/// [`watch_runtime`], sampling every `interval`. Returns the watching
/// thread so a test can see it end.
fn watch_runtime_every(
    runtime: &Arc<Runtime>,
    log_id: HandleId,
    interval: Duration,
) -> Option<std::thread::JoinHandle<()>> {
    let watched: Weak<Runtime> = Arc::downgrade(runtime);
    let handle = runtime.handle().clone();

    // A detached thread: it observes the runtime rather than belonging to
    // it, and ends on its own when the last handle drops.
    let spawned = std::thread::Builder::new()
        .name("moraine-watch".to_owned())
        .spawn(move || {
            tag_thread_for_handle(log_id);
            let started = std::time::Instant::now();
            let mut probe = arm_probe(&handle);

            loop {
                std::thread::sleep(interval);

                // Checked without upgrading: holding the last reference
                // here would run the runtime's shutdown on this thread.
                if watched.strong_count() == 0 {
                    return;
                }

                let metrics = handle.metrics();
                info!(
                    probe_polled = probe.polled.load(Ordering::Relaxed),
                    probe_timer_fired = probe.timer_fired.load(Ordering::Relaxed),
                    alive_tasks = metrics.num_alive_tasks(),
                    global_queue_depth = metrics.global_queue_depth(),
                    workers = metrics.num_workers(),
                    watched_seconds = started.elapsed().as_secs(),
                    "watching an attached runtime from off it"
                );

                probe = arm_probe(&handle);
            }
        });

    match spawned {
        Ok(thread) => Some(thread),
        Err(error) => {
            warn!(%error, "could not watch this runtime from off it");
            None
        }
    }
}

/// Spawns a probe on `handle` and returns what it will record.
fn arm_probe(handle: &Handle) -> Arc<RuntimeProbe> {
    let probe = Arc::new(RuntimeProbe::default());
    let armed = Arc::clone(&probe);
    handle.spawn(async move {
        armed.polled.store(true, Ordering::Relaxed);
        tokio::time::sleep(PROBE_SLEEP).await;
        armed.timer_fired.store(true, Ordering::Relaxed);
    });
    probe
}

/// How often an attached runtime says it is still advancing.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);

/// Reports that this runtime's timers still fire, for as long as it lives.
///
/// A commit parked on an await cannot be told apart from a runtime that
/// stopped advancing: both leave every thread asleep with nothing running,
/// and a thread dump cannot say which runtime a driver belongs to. This can,
/// because it runs on the runtime in question and is tagged with its handle
/// -- so a wedge where this keeps ticking is a lost wakeup, and one where it
/// stops is a runtime that died under the commit.
async fn heartbeat() {
    let started = std::time::Instant::now();
    let mut ticks: u64 = 0;
    loop {
        tokio::time::sleep(HEARTBEAT_INTERVAL).await;
        ticks = ticks.saturating_add(1);
        // Counted before it is reported, so a tick survives a record that
        // does not.
        moraine::note_runtime_tick();
        // Elapsed as well as the count: a runtime advancing late is a
        // different fault from one not advancing at all.
        info!(
            ticks,
            alive_seconds = started.elapsed().as_secs(),
            "attached runtime is advancing"
        );
    }
}

#[cfg(test)]
mod tests {
    use std::{ffi::CString, ptr};

    use super::{MAX_WORKER_THREADS, MIN_WORKER_THREADS, MoraineCatalogHandle, worker_threads};
    use crate::{
        abi::{moraine_attach, moraine_detach},
        error::{MoraineError, codes},
        test_support::{TempDir, attach_ok, attach_with_data_path},
    };

    /// An attach without a `DATA_PATH` store spawns no warming.
    #[test]
    fn an_attach_without_a_data_path_store_spawns_no_warming() {
        let lake = TempDir::new("warm-no-store");
        let handle = attach_ok(lake.path());
        // SAFETY: freshly attached above and not yet detached.
        let warming = unsafe { &*handle }.finish_warming();

        assert_eq!(warming, 0);
        // SAFETY: attached above, detached exactly once.
        unsafe { moraine_detach(handle) };
    }

    #[test]
    fn an_attach_with_a_data_path_store_spawns_one_warming_pass() {
        let lake = TempDir::new("warm-lake");
        let data = TempDir::new("warm-data");
        let handle = attach_with_data_path(lake.path(), data.path());
        // SAFETY: freshly attached above and not yet detached.
        let warming = unsafe { &*handle }.finish_warming();

        assert_eq!(warming, 1, "the attach spawned no warming pass");
        // SAFETY: attached above, detached exactly once.
        unsafe { moraine_detach(handle) };
    }

    /// An attach that preloads the cache warms every table's probe ranges
    /// even without a `DATA_PATH` store.
    #[test]
    fn an_attach_with_cache_preload_spawns_one_warming_pass() {
        let lake = TempDir::new("warm-preload");
        let c_path = CString::new(lake.path().to_str().expect("test path is UTF-8"))
            .expect("no NUL in path");
        let mut handle: *mut MoraineCatalogHandle = ptr::null_mut();
        let mut err = MoraineError::default();
        // SAFETY: `c_path` is a valid C string; outputs are valid local slots.
        let code = unsafe {
            moraine_attach(
                c_path.as_ptr(),
                ptr::null(),
                false,
                false,
                0,
                false,
                ptr::null(),
                0,
                0,
                1,
                false,
                ptr::null(),
                ptr::null(),
                0,
                None,
                ptr::null_mut(),
                &raw mut handle,
                &raw mut err,
            )
        };
        assert_eq!(code, codes::OK);
        // SAFETY: freshly attached above and not yet detached.
        let warming = unsafe { &*handle }.finish_warming();

        assert_eq!(warming, 1, "the preloading attach spawned no warming pass");
        // SAFETY: attached above, detached exactly once.
        unsafe { moraine_detach(handle) };
    }

    /// Detach ends the warming it spawned before closing the store.
    #[test]
    fn detaching_ends_the_warming_the_attach_spawned() {
        let lake = TempDir::new("warm-detach");
        let data = TempDir::new("warm-detach-data");
        let handle = attach_with_data_path(lake.path(), data.path());

        // SAFETY: attached above, detached exactly once.
        unsafe { moraine_detach(handle) };
    }

    /// The pool tracks the host between the floor and the ceiling, and is
    /// never single-threaded.
    #[test]
    fn the_worker_pool_tracks_the_host_between_its_floor_and_ceiling() {
        assert_eq!(
            worker_threads(0),
            MIN_WORKER_THREADS,
            "a host that says nothing takes the floor"
        );
        assert_eq!(
            worker_threads(1),
            MIN_WORKER_THREADS,
            "`SET threads=1` still gets a background worker"
        );
        assert_eq!(worker_threads(4), 4, "in range, the host's setting stands");
        assert_eq!(
            worker_threads(128),
            MAX_WORKER_THREADS,
            "a huge host thread count is capped"
        );
        const { assert!(MIN_WORKER_THREADS >= 2) };
    }
}

#[cfg(test)]
mod heartbeat_tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    };

    /// The production shape exactly: a 30s heartbeat on an attach runtime,
    /// observed past its third due tick.
    #[test]
    #[ignore = "runs for 95 seconds"]
    fn a_thirty_second_heartbeat_keeps_ticking() {
        let runtime = super::new_runtime(crate::logging::allocate_handle_id(), 2).unwrap();
        let ticks = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let counter = std::sync::Arc::clone(&ticks);
        runtime.spawn(async move {
            let started = std::time::Instant::now();
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::info!(
                    ticks = counter.load(std::sync::atomic::Ordering::Relaxed),
                    alive_seconds = started.elapsed().as_secs(),
                    "attached runtime is advancing"
                );
            }
        });
        std::thread::sleep(std::time::Duration::from_secs(95));
        let fired = ticks.load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            fired >= 3,
            "only {fired} ticks in 95s; production sees exactly 1"
        );
    }

    /// A runtime keeps firing timers while foreign threads drive work on it
    /// through `block_on`, which is the only thing an attach runtime does
    /// that the cache runtime never does.
    ///
    /// Production saw every attach runtime report once and then fall silent,
    /// so the question is whether that traffic can leave a runtime unable to
    /// advance its own timers.
    #[test]
    fn foreign_block_on_traffic_does_not_stop_the_timer() {
        let runtime = super::new_runtime(crate::logging::allocate_handle_id(), 2).unwrap();
        let ticks = Arc::new(AtomicU64::new(0));
        let counter = Arc::clone(&ticks);
        runtime.spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                counter.fetch_add(1, Ordering::Relaxed);
            }
        });

        let stop = Arc::new(AtomicBool::new(false));
        let handle = runtime.handle().clone();
        let callers: Vec<_> = (0..4)
            .map(|_| {
                let handle = handle.clone();
                let stop = Arc::clone(&stop);
                std::thread::spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        handle.block_on(async {
                            tokio::task::yield_now().await;
                        });
                    }
                })
            })
            .collect();

        std::thread::sleep(std::time::Duration::from_millis(600));
        stop.store(true, Ordering::Relaxed);
        for caller in callers {
            caller.join().unwrap();
        }

        let fired = ticks.load(Ordering::Relaxed);
        assert!(
            fired >= 4,
            "timer stopped advancing under foreign block_on traffic: {fired} ticks"
        );
    }
}

#[cfg(test)]
mod watch_tests {
    use std::{
        sync::{Arc, atomic::Ordering},
        time::{Duration, Instant},
    };

    use super::{PROBE_SLEEP, arm_probe, new_runtime, watch_runtime_every};
    use crate::logging::allocate_handle_id;

    /// A probe on a healthy runtime records both that a worker polled it
    /// and that its timer fired -- the two the watch tells apart.
    #[test]
    fn a_probe_records_polling_and_its_timer_firing() {
        let runtime = new_runtime(allocate_handle_id(), 2).unwrap();
        let probe = arm_probe(runtime.handle());

        let deadline = Instant::now() + PROBE_SLEEP + Duration::from_secs(5);
        while !probe.timer_fired.load(Ordering::Relaxed) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }

        assert!(
            probe.polled.load(Ordering::Relaxed),
            "a healthy runtime polls a spawned probe"
        );
        assert!(
            probe.timer_fired.load(Ordering::Relaxed),
            "a healthy runtime fires a probe's timer"
        );
    }

    /// The watch ends when the last handle to its runtime drops, so an
    /// attach does not leak a thread per detach.
    #[test]
    fn the_watch_ends_when_its_runtime_drops() {
        let log_id = allocate_handle_id();
        let runtime = Arc::new(new_runtime(log_id, 2).unwrap());
        let watching = watch_runtime_every(&runtime, log_id, Duration::from_millis(20))
            .expect("thread spawns");

        drop(runtime);

        let deadline = Instant::now() + Duration::from_secs(10);
        while !watching.is_finished() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            watching.is_finished(),
            "the watch must end with the runtime it watches"
        );
    }

    /// The watch's record reaches its handle's sink, prefixed with the
    /// event target: it emits from a detached thread that tags itself
    /// rather than from a runtime worker, and readers match on the
    /// delivered form, not the message literal.
    #[test]
    fn the_watchs_record_reaches_its_handles_sink() {
        use std::{
            ffi::{CStr, c_char, c_void},
            sync::Mutex,
        };

        static SEEN: Mutex<Vec<String>> = Mutex::new(Vec::new());

        unsafe extern "C" fn collect(_: *mut c_void, _: i32, message: *const c_char) {
            // SAFETY: the ABI documents `message` as NUL-terminated and
            // valid for the duration of this call.
            let text = unsafe { CStr::from_ptr(message) }
                .to_string_lossy()
                .into_owned();
            SEEN.lock().unwrap().push(text);
        }

        crate::logging::install();
        let log_id = allocate_handle_id();
        // SAFETY: `collect` never unwinds, emits no events, and re-enters
        // no entry point.
        unsafe { crate::logging::register_sink(log_id, collect, std::ptr::null_mut()) };

        let runtime = Arc::new(new_runtime(log_id, 2).unwrap());
        let watching = watch_runtime_every(&runtime, log_id, Duration::from_millis(50))
            .expect("thread spawns");

        let deadline = Instant::now() + Duration::from_secs(10);
        let mut delivered = None;
        while delivered.is_none() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(50));
            delivered = SEEN
                .lock()
                .unwrap()
                .iter()
                .find(|record| record.contains("watching an attached runtime from off it"))
                .cloned();
        }

        crate::logging::unregister_sink(log_id);
        drop(runtime);
        let _ = watching.join();

        let delivered = delivered.expect("the watch's record must reach its handle's sink");
        assert!(
            delivered.starts_with("moraine_duckdb::runtime: "),
            "a delivered record leads with its target, not its message: {delivered}"
        );
    }
}

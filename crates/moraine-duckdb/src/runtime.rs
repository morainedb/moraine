//! Opaque handles owned across the FFI boundary, and the sync↔async
//! bridge: one tokio multi-threaded runtime per attached catalog.

use std::{
    ffi::c_void,
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};

use moraine::{Catalog, CatalogSnapshot, ReadOnlyCatalog, TableId};
use object_store::ObjectStore;
use tokio::{
    runtime::{Builder, Runtime},
    task::JoinHandle,
};
use tracing::warn;

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
/// the [`Catalog`] handle opened on it.
///
/// Opaque to C — only ever seen as a `MoraineCatalogHandle*` obtained
/// from [`moraine_attach`](crate::abi::moraine_attach) and released via
/// [`moraine_detach`](crate::abi::moraine_detach).
///
/// Every FFI entry point `block_on`s through `runtime`; nothing in
/// `moraine` core ever blocks on itself.
pub struct MoraineCatalogHandle {
    pub(crate) runtime: Runtime,
    pub(crate) catalog: AttachedCatalog,
    /// Routes this handle's `tracing` events to its registered log sink:
    /// the runtime's worker threads carry it for life, and the `block_on`
    /// wrappers below tag the calling thread with it per call.
    pub(crate) log_id: HandleId,
    /// The `DATA_PATH` object store, resolved at attach from `META_DATA_PATH`.
    /// Present only when that option was given; index maintenance and
    /// scoped-read backfill need it, and are skipped when it is absent.
    pub(crate) data_store: Option<Arc<dyn ObjectStore>>,
    /// The bucket-relative key prefix of `DATA_PATH` (empty for a local or
    /// bare-bucket store), prepended to a data file's stored path.
    pub(crate) data_prefix: String,
    /// Row-summary warming spawned by the attach and by each commit that
    /// registers data files. Ended by [`finish_warming`](
    /// MoraineCatalogHandle::finish_warming) before the catalog closes, so
    /// a close never races an in-flight scoped read.
    warming: Mutex<Vec<JoinHandle<()>>>,
}

/// Which mode the attach opened in. The core types the two apart, so a
/// mutator is unavailable on the read-only one at compile time; this is
/// where that meets a C ABI with one handle type and no types of its own,
/// and a write on a read-only attach becomes a runtime refusal again —
/// [`AttachedCatalog::writer`] is the single place that happens.
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
        Self {
            runtime,
            catalog,
            log_id,
            data_store: None,
            data_prefix: String::new(),
            warming: Mutex::new(Vec::new()),
        }
    }

    /// Spawns a best-effort pass building the row summaries a later located
    /// lookup would otherwise build cold, over every table.
    ///
    /// Scoped reads need the `DATA_PATH` store, so an attach without one
    /// spawns nothing.
    pub(crate) fn spawn_warm_all(&self) {
        let Some(data_store) = self.data_store.clone() else {
            return;
        };
        let catalog = self.catalog.reads().clone();
        let data_prefix = self.data_prefix.clone();

        self.track(self.runtime.spawn(async move {
            if let Err(error) = catalog
                .warm_all_row_summaries(data_store, &data_prefix)
                .await
            {
                warn!(%error, "row summary warming skipped this attach");
            }
        }));
    }

    /// As [`spawn_warm_all`](Self::spawn_warm_all), for the tables a commit
    /// just registered data files against. An empty `tables` spawns nothing.
    pub(crate) fn spawn_warm_tables(&self, tables: Vec<TableId>) {
        if tables.is_empty() {
            return;
        }
        let Some(data_store) = self.data_store.clone() else {
            return;
        };
        let catalog = self.catalog.reads().clone();
        let data_prefix = self.data_prefix.clone();

        self.track(self.runtime.spawn(async move {
            if let Err(error) = catalog
                .warm_selected_row_summaries(data_store, &data_prefix, tables)
                .await
            {
                warn!(%error, "row summary warming skipped this attach");
            }
        }));
    }

    /// Retains `task` so a detach can end it, dropping the passes that have
    /// already finished — a long insert session spawns one per commit.
    fn track(&self, task: JoinHandle<()>) {
        let Ok(mut warming) = self.warming.lock() else {
            return;
        };
        warming.retain(|task| !task.is_finished());
        warming.push(task);
    }

    /// Cancels the warming passes still in flight and waits for them to
    /// end, reporting how many were outstanding.
    ///
    /// Warming holds a catalog handle of its own, so this must precede the
    /// close: a cancelled pass loses only cache, where one still reading
    /// when the store shuts under it would not.
    pub(crate) fn finish_warming(&self) -> usize {
        let tasks = match self.warming.lock() {
            Ok(mut warming) => std::mem::take(&mut *warming),
            Err(_) => return 0,
        };

        for task in &tasks {
            task.abort();
        }
        let outstanding = tasks.len();
        for task in tasks {
            // A cancelled pass reports `JoinError`, which is the ask here.
            let _ = self.runtime.block_on(task);
        }

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
}

/// Runs `future` on `runtime` unless `probe` cancels it first — the whole
/// of the cancellation seam, shared by every cancellable entry point.
///
/// Cancellation is per **call**, not per handle: the probe and its context
/// come from the caller, and each in-flight call selects over its own. Two
/// concurrent reads on one handle therefore cancel independently, and
/// neither can consume the other's signal.
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
    // Checked before the future is first polled, not left to the interval
    // below: a timer's first tick is pending at the poll level even when
    // already elapsed, and a future that completes on its first poll would
    // otherwise win over a pending interrupt.
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
/// down before abandoning it in turn. An interrupted open drops a
/// half-built store whose background tasks may still be mid-request, and
/// a plain runtime drop blocks until every one of them finishes — turning
/// a cancellation into exactly the hang it was meant to escape.
pub(crate) const CANCELLED_ATTACH_SHUTDOWN: Duration = Duration::from_secs(5);

/// A materialized snapshot view, held across the FFI boundary so
/// listing calls need no further store I/O.
///
/// Opaque to C — only ever seen as a `MoraineSnapshotHandle*` obtained
/// from [`moraine_snapshot`](crate::abi::moraine_snapshot) and released
/// via [`moraine_snapshot_free`](crate::abi::moraine_snapshot_free).
pub struct MoraineSnapshotHandle {
    pub(crate) snapshot: Arc<CatalogSnapshot>,
}

impl MoraineSnapshotHandle {
    pub(crate) fn new(snapshot: Arc<CatalogSnapshot>) -> Self {
        Self { snapshot }
    }
}

/// The fewest workers an attached catalog's runtime may have.
///
/// Two, not one: a CPU-bound poll (an SST decode on a large catalog) holds
/// its worker to completion, and SlateDB's flush and compaction must
/// progress while it does or durability stalls behind a scan. That is the
/// same reason the runtime is multi-threaded at all, so it is the floor
/// rather than a tuning knob.
const MIN_WORKER_THREADS: usize = 2;

/// The most workers an attached catalog's runtime may have, however many
/// threads the host asks for.
///
/// The pool's work is object-store round trips, which yield their worker
/// at every await: a four-worker runtime absorbs thirty-two concurrent
/// materializations with a flat batch time (`BENCHMARK.md` → Core
/// measurements). Past a handful, further workers only park — and they
/// park in addition to the host's own threads, on cores the host already
/// sized itself to.
const MAX_WORKER_THREADS: usize = 8;

/// The worker count for a host that asks for `requested` threads of its
/// own, or the [floor](MIN_WORKER_THREADS) if it asks for nothing.
///
/// The host's thread setting is the only number in the process that says
/// how much parallelism the operator wanted, so a session pinned to one
/// thread does not get a catalog pool sized to the machine.
pub(crate) fn worker_threads(requested: usize) -> usize {
    requested.clamp(MIN_WORKER_THREADS, MAX_WORKER_THREADS)
}

/// Builds the one multi-threaded tokio runtime an attached catalog owns
/// for the lifetime of its handle, sized for a host running `requested`
/// threads of its own (`0` when the host does not say). Worker threads
/// exist only to run that handle's work, so each is tagged with `log_id`
/// at spawn — every event they emit routes to the handle's log sink.
///
/// The size is fixed at attach. A host that changes its own thread count
/// later keeps the pool it attached with, which is the trade for never
/// rebuilding a runtime that owns live background tasks.
pub(crate) fn new_runtime(log_id: HandleId, requested: usize) -> std::io::Result<Runtime> {
    Builder::new_multi_thread()
        .worker_threads(worker_threads(requested))
        .enable_all()
        .on_thread_start(move || tag_thread_for_handle(log_id))
        .build()
}

#[cfg(test)]
mod tests {
    use super::{MAX_WORKER_THREADS, MIN_WORKER_THREADS, worker_threads};
    use crate::{
        abi::moraine_detach,
        test_support::{TempDir, attach_ok, attach_with_data_path},
    };

    /// A scoped read needs the `DATA_PATH` store, so an attach without one
    /// spawns no warming rather than a pass that could read nothing.
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

    /// Detach ends the warming it spawned before closing the store, so a
    /// close never races an in-flight scoped read — and never hangs on one.
    #[test]
    fn detaching_ends_the_warming_the_attach_spawned() {
        let lake = TempDir::new("warm-detach");
        let data = TempDir::new("warm-detach-data");
        let handle = attach_with_data_path(lake.path(), data.path());

        // SAFETY: attached above, detached exactly once.
        unsafe { moraine_detach(handle) };
    }

    /// The pool tracks the host between the floor and the ceiling, and is
    /// never single-threaded — a one-worker runtime would let a CPU-bound
    /// poll stall SlateDB's flush, which is the whole reason the runtime
    /// is multi-threaded.
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

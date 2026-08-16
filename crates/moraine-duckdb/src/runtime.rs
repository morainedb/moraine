//! Opaque handles owned across the FFI boundary, and the sync↔async
//! bridge: one tokio multi-threaded runtime per attached catalog.

use std::{
    ffi::c_void,
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};

use futures::future::join_all;
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
/// the [`Catalog`] handle opened on it. Every FFI entry point `block_on`s
/// through `runtime`.
///
/// Opaque to C — only ever seen as a `MoraineCatalogHandle*` obtained
/// from [`moraine_attach`](crate::abi::moraine_attach) and released via
/// [`moraine_detach`](crate::abi::moraine_detach).
pub struct MoraineCatalogHandle {
    pub(crate) runtime: Runtime,
    pub(crate) catalog: AttachedCatalog,
    /// Routes this handle's `tracing` events to its registered log sink.
    pub(crate) log_id: HandleId,
    /// The `DATA_PATH` object store, present only when `META_DATA_PATH`
    /// was given; index maintenance and scoped reads are skipped without it.
    pub(crate) data_store: Option<Arc<dyn ObjectStore>>,
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
}

impl MoraineSnapshotHandle {
    pub(crate) fn new(snapshot: Arc<CatalogSnapshot>) -> Self {
        Self { snapshot }
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
/// tagged with `log_id` at spawn. The size is fixed at attach.
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

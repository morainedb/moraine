//! Runtime ownership for core background maintenance.

use std::sync::Arc;

pub(super) use moraine::BackgroundRequest as Pass;
use moraine::{BackgroundWork, DataStore, ReadOnlyCatalog};
use tokio::{runtime::Runtime, task::JoinHandle};

pub(super) struct Worker {
    pending: Arc<BackgroundWork>,
    task: JoinHandle<()>,
}

impl Worker {
    pub(super) fn start(
        runtime: &Runtime,
        catalog: ReadOnlyCatalog,
        store: Option<DataStore>,
        prefix: String,
    ) -> Self {
        let pending = Arc::new(BackgroundWork::default());
        let queue = Arc::clone(&pending);
        let task = runtime.spawn(async move {
            Box::pin(queue.run(&catalog, store.as_ref(), &prefix)).await;
        });
        Self { pending, task }
    }

    pub(super) fn push(&self, pass: Pass) {
        self.pending.push(pass);
    }

    pub(super) fn stop(mut self, runtime: &Runtime) {
        self.task.abort();
        let _ = runtime.block_on(&mut self.task);
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stop_joins_the_worker_and_releases_pending_work() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let catalog = runtime
            .block_on(moraine::Catalog::open(
                Arc::new(object_store::memory::InMemory::new()),
                moraine::CatalogOptions::default(),
            ))
            .unwrap();
        let worker = Worker::start(&runtime, (*catalog).clone(), None, String::new());
        let pending = Arc::downgrade(&worker.pending);
        worker.push(Pass::attach(true));
        worker.stop(&runtime);
        assert!(pending.upgrade().is_none());
        runtime.block_on(catalog.close()).unwrap();
    }
}

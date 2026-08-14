//! A byte-budgeted cache of decoded file row-id summaries.

use std::{
    collections::VecDeque,
    sync::{Arc, LazyLock, Mutex, Weak},
};

use object_store::{ObjectStore, path::Path};

use crate::{data_file::row_set::FileRowSet, store::cache};

/// What makes one summary reusable. Catalog file ids are never reused, so
/// the path and recorded size only guard an imported catalog that violates
/// that: a mismatch misses rather than serving another file's rows. A weak
/// store reference both keeps this process-wide cache from holding catalogs
/// alive and separates two catalogs whose ids and paths coincide.
struct SummaryEntry {
    store: Weak<dyn ObjectStore>,
    table_id: u64,
    data_file_id: u64,
    path: Path,
    file_size: u64,
    rows: Arc<FileRowSet>,
    bytes: usize,
}

impl SummaryEntry {
    fn matches(&self, store: &Weak<dyn ObjectStore>, key: &SummaryKey<'_>) -> bool {
        Weak::ptr_eq(&self.store, store)
            && self.table_id == key.table_id
            && self.data_file_id == key.data_file_id
            && self.path == *key.path
            && self.file_size == key.file_size
    }
}

/// One file's identity within its store.
pub(super) struct SummaryKey<'a> {
    pub(super) table_id: u64,
    pub(super) data_file_id: u64,
    pub(super) path: &'a Path,
    pub(super) file_size: u64,
}

/// Decoded summaries in insertion order, evicted oldest-first against the
/// allowance. Eviction only costs a later rebuild.
#[derive(Default)]
struct RowSetCache {
    entries: VecDeque<SummaryEntry>,
    bytes: usize,
}

impl RowSetCache {
    /// Drops entries whose store is gone and re-totals what remains.
    fn compact(&mut self) {
        self.entries.retain(|entry| entry.store.strong_count() > 0);
        self.bytes = self.entries.iter().map(|entry| entry.bytes).sum();
    }

    fn get(
        &mut self,
        store: &Arc<dyn ObjectStore>,
        key: &SummaryKey<'_>,
    ) -> Option<Arc<FileRowSet>> {
        self.compact();
        cache::set_row_summary_usage(self.bytes);

        let weak = Arc::downgrade(store);
        let position = self
            .entries
            .iter()
            .position(|entry| entry.matches(&weak, key))?;
        let entry = self.entries.remove(position)?;
        let rows = Arc::clone(&entry.rows);
        self.entries.push_back(entry);
        Some(rows)
    }

    fn insert(
        &mut self,
        store: &Arc<dyn ObjectStore>,
        key: &SummaryKey<'_>,
        rows: &Arc<FileRowSet>,
    ) {
        let capacity = cache::row_summary_memory();
        let bytes = usize::try_from(rows.estimated_bytes()).unwrap_or(usize::MAX);
        if capacity == 0 || bytes > capacity {
            return;
        }

        self.compact();
        let weak = Arc::downgrade(store);
        self.entries.retain(|entry| !entry.matches(&weak, key));
        self.entries.push_back(SummaryEntry {
            store: weak,
            table_id: key.table_id,
            data_file_id: key.data_file_id,
            path: key.path.clone(),
            file_size: key.file_size,
            rows: Arc::clone(rows),
            bytes,
        });
        self.bytes = self.entries.iter().map(|entry| entry.bytes).sum();

        while self.bytes > capacity {
            let Some(evicted) = self.entries.pop_front() else {
                self.bytes = 0;
                break;
            };
            self.bytes = self.bytes.saturating_sub(evicted.bytes);
            cache::row_summary_evicted();
        }
        cache::set_row_summary_usage(self.bytes);
    }
}

static ROW_SET_CACHE: LazyLock<Mutex<RowSetCache>> =
    LazyLock::new(|| Mutex::new(RowSetCache::default()));

/// The cached summary for this file, if one is resident.
pub(super) fn cached(
    store: &Arc<dyn ObjectStore>,
    key: &SummaryKey<'_>,
) -> Option<Arc<FileRowSet>> {
    ROW_SET_CACHE
        .lock()
        .ok()
        .and_then(|mut cache| cache.get(store, key))
}

/// Offers a freshly built summary to the cache. A poisoned lock leaves the
/// process uncached rather than failing a lookup.
pub(super) fn store(store: &Arc<dyn ObjectStore>, key: &SummaryKey<'_>, rows: &Arc<FileRowSet>) {
    if let Ok(mut cache) = ROW_SET_CACHE.lock() {
        cache.insert(store, key, rows);
    }
}

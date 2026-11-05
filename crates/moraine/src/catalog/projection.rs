//! Small format and inline-directory state. Catalog records are
//! operation-owned.

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    catalog::CatalogSnapshot,
    error::Error,
    store::{
        batch::{BatchWrite, DecodedValue, decode_batch},
        inline::InlineChunkLocator,
        key::{InlineKey, InlineOperation, Key, SysKey},
        proto::HeadValue,
    },
    transaction::commit::StagedWrite,
};

/// One batch's writes to the inline chunk directory, gathered as the batch
/// is walked and applied once it is known to have landed.
///
/// The locator subspace is the directory, so a write there is the whole of
/// what moves it. A chunk written without its locator is a batch this
/// cannot follow: the table's set is dropped rather than left a range
/// short, because a short set resolves a row to the wrong chunk.
#[derive(Default)]
struct InlineFold {
    writes: Vec<(u64, InlineOperation, Option<InlineChunkLocator>)>,
    chunks: BTreeSet<(u64, InlineOperation)>,
    unfollowable: BTreeSet<u64>,
}

impl InlineFold {
    fn observe(&mut self, write: &BatchWrite<'_>) {
        match write.key() {
            Key::Inline(InlineKey::ChunkLocator {
                table_id,
                row_id_end,
                schema_version,
                begin_snapshot,
                chunk_seq,
            }) => {
                let operation = InlineOperation::Insert {
                    table_id: *table_id,
                    schema_version: *schema_version,
                    begin_snapshot: *begin_snapshot,
                    chunk_seq: *chunk_seq,
                };
                let located = write.value().and_then(|value| match value {
                    None => Ok(None),
                    Some(DecodedValue::InlineChunkRange(range)) => {
                        InlineChunkLocator::from_directory(
                            operation,
                            range.row_id_start,
                            *row_id_end,
                        )
                        .map(Some)
                    }
                    Some(_) => Err(unexpected_value(write.key())),
                });
                match located {
                    Ok(locator) => self.writes.push((*table_id, operation, locator)),
                    Err(_) => {
                        self.unfollowable.insert(*table_id);
                    }
                }
            }
            Key::Inline(InlineKey::Live(operation) | InlineKey::Arch(operation)) => {
                self.chunks.insert((operation.table_id(), *operation));
            }
            _ => {}
        }
    }

    fn apply(
        mut self,
        sets: &mut BTreeMap<u64, imbl::OrdMap<InlineOperation, InlineChunkLocator>>,
    ) {
        let located: BTreeSet<(u64, InlineOperation)> = self
            .writes
            .iter()
            .map(|(table_id, operation, _)| (*table_id, *operation))
            .collect();
        for (table_id, _) in self.chunks.difference(&located) {
            self.unfollowable.insert(*table_id);
        }
        for table_id in &self.unfollowable {
            sets.remove(table_id);
        }
        for (table_id, operation, locator) in self.writes {
            if self.unfollowable.contains(&table_id) {
                continue;
            }
            let Some(set) = sets.get_mut(&table_id) else {
                continue;
            };
            match locator {
                Some(locator) => set.insert(operation, locator),
                None => set.remove(&operation),
            };
        }
    }
}

/// The head record a batch leaves behind: the last head write in the
/// batch, since a group commit stages one per member and the store keeps
/// the final write of a key.
fn head_stamp(writes: &[BatchWrite<'_>]) -> Option<HeadValue> {
    writes
        .iter()
        .rev()
        .filter(|write| matches!(write.key(), Key::Sys(SysKey::Head)))
        .find_map(|write| match write.value() {
            Ok(Some(DecodedValue::Head(head))) => Some(head),
            _ => None,
        })
}

/// A value decoded as a kind its key does not hold; unreachable through
/// the store's own decoder, so reported as corruption.
fn unexpected_value(key: &Key) -> Error {
    Error::Corruption(format!("value at {key:?} decoded as another kind"))
}

/// Whether two head records name the same store state.
fn same_head(a: &HeadValue, b: &HeadValue) -> bool {
    a.snapshot_id == b.snapshot_id && a.batch_seq == b.batch_seq
}

/// The head record a materialized view stands at. On a read-write handle
/// this is the head itself, so a cache lookup keys on it without a read.
pub(crate) fn view_head(view: &CatalogSnapshot) -> HeadValue {
    HeadValue {
        snapshot_id: view.snapshot.snapshot_id,
        batch_seq: view.batch_seq,
    }
}

/// Folds a just-committed batch into the shared cache. A poisoned lock is
/// recovered: the fold cannot panic, so its state is never half-applied.
pub(crate) fn fold_committed_batch(
    cache: &std::sync::RwLock<ProjectionCache>,
    writes: &[StagedWrite],
    new_head: u64,
) {
    cache
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .apply_batch(writes, new_head);
}

/// The install epoch to capture before reading the store.
#[cfg(test)]
pub(crate) fn cache_epoch(cache: &std::sync::RwLock<ProjectionCache>) -> u64 {
    cache
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .epoch()
}

/// The highest store format version this handle has observed.
pub(crate) fn format_floor(cache: &std::sync::RwLock<ProjectionCache>) -> u64 {
    cache
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .format_floor()
}

/// Records a format version read from the store.
pub(crate) fn raise_format_floor(cache: &std::sync::RwLock<ProjectionCache>, observed: u64) {
    cache
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .raise_format_floor(observed);
}

/// Whether the migration marker was already found absent at `head`.
pub(crate) fn migration_clear_at(
    cache: &std::sync::RwLock<ProjectionCache>,
    head: &HeadValue,
) -> bool {
    cache
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .migration_clear_at(head)
}

/// Records the migration marker as absent at `head`.
pub(crate) fn note_migration_clear(cache: &std::sync::RwLock<ProjectionCache>, head: HeadValue) {
    cache
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .note_migration_clear(head);
}

/// Whether `table_id`'s inline chunk-range directory is known complete.
/// Whether an inline directory for `table_id` built at `built_at` still
/// describes the table's inline state at `head`: every batch since
/// `built_at` has folded here and none wrote the table's inline keys.
/// `table_id`'s live chunk locators, folded forward to `head`, or `None`
/// when no set stands there and the store has to be scanned.
pub(crate) fn folded_inline_locators(
    cache: &std::sync::RwLock<ProjectionCache>,
    table_id: u64,
    head: &HeadValue,
) -> Option<imbl::OrdMap<InlineOperation, InlineChunkLocator>> {
    cache
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .folded_inline_locators(table_id, head)
}

/// Holds a scanned locator set so later batches fold it forward.
pub(crate) fn seed_inline_locators(
    cache: &std::sync::RwLock<ProjectionCache>,
    table_id: u64,
    head: &HeadValue,
    locators: imbl::OrdMap<InlineOperation, InlineChunkLocator>,
) {
    cache
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .seed_inline_locators(table_id, head, locators);
}

pub(crate) fn inline_directory_complete(
    cache: &std::sync::RwLock<ProjectionCache>,
    table_id: u64,
) -> bool {
    cache
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .inline_directory_complete(table_id)
}

/// The widest inline chunk `table_id` is known to hold, as an upper bound
/// on `row_id_end - row_id_start`. `None` means unknown, which bounds
/// nothing.
pub(crate) fn inline_chunk_width(
    cache: &std::sync::RwLock<ProjectionCache>,
    table_id: u64,
) -> Option<u64> {
    cache
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .inline_chunk_width(table_id)
}

/// Raises `table_id`'s known widest chunk to at least `width`.
pub(crate) fn note_inline_chunk_width(
    cache: &std::sync::RwLock<ProjectionCache>,
    table_id: u64,
    width: u64,
) {
    cache
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .note_inline_chunk_width(table_id, width);
}

/// Records `table_id`'s inline chunk-range directory as verified complete.
pub(crate) fn note_inline_directory_complete(
    cache: &std::sync::RwLock<ProjectionCache>,
    table_id: u64,
) {
    cache
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .note_inline_directory_complete(table_id);
}

pub(crate) fn invalidate_head_view(cache: &std::sync::RwLock<ProjectionCache>) {
    cache
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clear_head_view();
}

/// Drops every projection over the `current` half, for a write that removed
/// records without going through the commit protocol.
pub(crate) fn invalidate_current_state(cache: &std::sync::RwLock<ProjectionCache>) {
    cache
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clear_current_state();
}

/// Format observations and inline lookup bounds shared by a handle.
#[derive(Default)]
pub(crate) struct ProjectionCache {
    folded_head: Option<HeadValue>,
    format_floor: u64,
    migration_clear: Option<HeadValue>,
    inline_directory_complete: BTreeSet<u64>,
    inline_locators: BTreeMap<u64, imbl::OrdMap<InlineOperation, InlineChunkLocator>>,
    inline_chunk_width: BTreeMap<u64, u64>,
    epoch: u64,
}
impl ProjectionCache {
    pub(crate) fn empty() -> Self {
        Self::default()
    }
    #[cfg(test)]
    pub(crate) fn epoch(&self) -> u64 {
        self.epoch
    }
    pub(crate) fn clear_head_view(&mut self) {
        self.epoch = self.epoch.wrapping_add(1);
    }
    pub(crate) fn clear_current_state(&mut self) {
        self.clear_head_view();
        self.inline_locators.clear();
    }
    /// `table_id`'s live chunk locators, if a seeded set stands at `head`.
    fn folded_inline_locators(
        &self,
        table_id: u64,
        head: &HeadValue,
    ) -> Option<imbl::OrdMap<InlineOperation, InlineChunkLocator>> {
        self.folded_head
            .as_ref()
            .is_some_and(|folded| same_head(folded, head))
            .then(|| self.inline_locators.get(&table_id).cloned())
            .flatten()
    }

    /// Holds `locators` as `table_id`'s live chunk set, which every later
    /// batch folds forward. Seeding a set the fold cannot follow would
    /// serve wrong ranges, so a set only stands while it stands at
    /// `folded_head`.
    pub(crate) fn seed_inline_locators(
        &mut self,
        table_id: u64,
        head: &HeadValue,
        locators: imbl::OrdMap<InlineOperation, InlineChunkLocator>,
    ) {
        if self
            .folded_head
            .as_ref()
            .is_some_and(|folded| same_head(folded, head))
        {
            self.inline_locators.insert(table_id, locators);
        }
    }

    pub(crate) fn inline_directory_complete(&self, table_id: u64) -> bool {
        self.inline_directory_complete.contains(&table_id)
    }

    /// Records `table_id`'s directory as verified complete.
    pub(crate) fn note_inline_directory_complete(&mut self, table_id: u64) {
        self.inline_directory_complete.insert(table_id);
    }

    /// An upper bound on `table_id`'s widest live inline chunk, or `None`
    /// when nothing has been observed.
    pub(crate) fn inline_chunk_width(&self, table_id: u64) -> Option<u64> {
        self.inline_chunk_width.get(&table_id).copied()
    }

    /// Raises the bound for `table_id`; never lowers it.
    pub(crate) fn note_inline_chunk_width(&mut self, table_id: u64, width: u64) {
        let slot = self.inline_chunk_width.entry(table_id).or_default();
        *slot = (*slot).max(width);
    }

    /// The highest format version observed, 0 before any has been.
    pub(crate) fn format_floor(&self) -> u64 {
        self.format_floor
    }

    /// Records an observed format version, keeping the highest.
    pub(crate) fn raise_format_floor(&mut self, observed: u64) {
        self.format_floor = self.format_floor.max(observed);
    }

    /// Whether `sys/migration` was observed absent at exactly `head`.
    pub(crate) fn migration_clear_at(&self, head: &HeadValue) -> bool {
        self.migration_clear.as_ref() == Some(head)
    }

    /// Records `sys/migration` as absent at `head`.
    pub(crate) fn note_migration_clear(&mut self, head: HeadValue) {
        self.migration_clear = Some(head);
    }

    pub(crate) fn apply_batch(&mut self, writes: &[StagedWrite], new_head: u64) {
        let Ok(writes) = decode_batch(writes) else {
            self.inline_locators.clear();
            return;
        };
        let Some(stamp) = head_stamp(&writes).filter(|stamp| stamp.snapshot_id == new_head) else {
            self.inline_locators.clear();
            return;
        };
        let mut inline = InlineFold::default();
        for write in &writes {
            inline.observe(write);
        }
        inline.apply(&mut self.inline_locators);
        self.folded_head = Some(stamp);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;

    use crate::{
        Catalog, CatalogOptions, ColumnDef,
        ffi_support::{dump_snapshots, dump_table_column_stats, dump_table_stats},
        store::proto::SnapshotValue,
    };
    async fn scanned_snapshots(catalog: &Catalog) -> Vec<SnapshotValue> {
        let session = catalog.begin_read().await.unwrap();
        let rows = crate::store::read::scan_snapshots(session.handle())
            .await
            .unwrap();
        session.finish();
        rows
    }

    /// After every commit, the served projections equal fresh scans at the
    /// same head — served through the cache (installed by the first dump,
    /// folded forward by each commit), proven equal to the store's truth.
    #[tokio::test]
    async fn dumps_after_commits_match_fresh_scans() {
        let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
            .await
            .unwrap();

        for round in 0..8u64 {
            // Prime/serve before the commit so the fold path (not the
            // install path) is what keeps the cache current.
            let _ = dump_snapshots(&catalog).await.unwrap();

            catalog
                .commit(|tx| {
                    let main = tx.schemas()[0].id;
                    let table = tx.create_table(
                        main,
                        &format!("t{round}"),
                        &[ColumnDef {
                            name: "id".into(),
                            column_type: "BIGINT".into(),
                            nulls_allowed: false,
                            default_value: None,
                            children: Vec::new(),
                        }],
                    )?;
                    if round % 2 == 0 {
                        tx.rename_table(table, &format!("t{round}_renamed"))?;
                    }
                    Ok(())
                })
                .await
                .unwrap();

            let served = dump_snapshots(&catalog).await.unwrap();
            assert_eq!(served, scanned_snapshots(&catalog).await, "round {round}");

            assert_eq!(catalog.memory_tally().projection_bytes, 0);

            let _ = dump_table_stats(&catalog).await.unwrap();
            let _ = dump_table_column_stats(&catalog).await.unwrap();
        }
    }
}

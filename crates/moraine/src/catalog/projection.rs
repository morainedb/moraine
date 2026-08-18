//! Maintained projections: decoded snapshot and statistics rows a
//! read-write catalog serves without rescanning, folded forward from each
//! committed batch. Every serve is guarded by the head snapshot id the
//! caller observed; a mismatch (or an undecodable fold) degrades to a
//! fresh scan, never to wrong rows.

use std::{collections::BTreeMap, sync::Arc};

use crate::{
    catalog::CatalogSnapshot,
    store::{
        key::{CurrentKey, EntityKey, Key, SysKey},
        proto::{HeadValue, SnapshotValue, TableColumnStatsValue, TableStatsValue},
        read::EntityRecord,
        value,
    },
    transaction::commit::StagedWrite,
};

/// One maintained projection: decoded rows stamped with the head snapshot
/// id they are valid at. `head: None` means not installed — serves refuse
/// and folds skip until a fresh scan installs it.
struct Maintained<K: Ord, V> {
    // The whole head stamp: a maintenance batch reuses the snapshot id.
    head: Option<HeadValue>,
    // Shared so a serve is a refcount bump: materializing rows is the
    // caller's work to do after it has dropped the cache lock.
    rows: Arc<BTreeMap<K, V>>,
}

/// The head record a batch leaves behind: the last head write in the
/// batch, since a group commit stages one per member and the store keeps
/// the final write of a key.
fn head_stamp(writes: &[StagedWrite]) -> Option<HeadValue> {
    let head_key = Key::Sys(SysKey::Head).encode();
    writes.iter().rev().find_map(|(key, bytes)| {
        (key == &head_key)
            .then_some(bytes.as_ref())
            .flatten()
            .and_then(|bytes| value::decode_value::<HeadValue>(bytes).ok())
    })
}

/// Copies a served projection's rows out. Called after the cache lock is
/// dropped: the copying is what a serve deliberately leaves undone.
pub(crate) fn materialize<K: Ord, V: Clone>(rows: &BTreeMap<K, V>) -> Vec<V> {
    rows.values().cloned().collect()
}

/// Restamps a retained record half at the state the batch left behind.
/// The rows are unchanged, so only the stamp they answer to moves.
fn advance_half(half: &mut Option<(HeadValue, Arc<Vec<EntityRecord>>)>, new_head: HeadValue) {
    if let Some((head, _)) = half {
        *head = new_head;
    }
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

impl<K: Ord, V> Maintained<K, V> {
    fn empty() -> Self {
        Self {
            head: None,
            rows: Arc::new(BTreeMap::new()),
        }
    }

    fn install(&mut self, head: HeadValue, rows: BTreeMap<K, V>) {
        self.head = Some(head);
        self.rows = Arc::new(rows);
    }

    fn clear(&mut self) {
        self.head = None;
        self.rows = Arc::new(BTreeMap::new());
    }

    /// Roughly what the maintained rows hold, keys included.
    fn estimated_bytes(&self) -> u64
    where
        V: prost::Message,
    {
        self.rows
            .values()
            .map(|value| {
                u64::try_from(value.encoded_len()).unwrap_or(u64::MAX)
                    + u64::try_from(std::mem::size_of::<K>()).unwrap_or(0)
            })
            .sum()
    }

    fn advance(&mut self, new_head: HeadValue) {
        if self.head.is_some() {
            self.head = Some(new_head);
        }
    }

    /// Clears an installed projection standing anywhere but `expected`.
    /// Folding a batch onto rows that already missed one would leave rows
    /// no stamp describes.
    fn clear_unless_at(&mut self, expected: HeadValue) {
        if self
            .head
            .as_ref()
            .is_some_and(|head| !same_head(head, &expected))
        {
            self.clear();
        }
    }

    /// The rows if they stand at exactly `expected`, shared rather than
    /// copied so the caller can materialize them off the cache lock.
    fn serve(&self, expected: &HeadValue) -> Option<Arc<BTreeMap<K, V>>> {
        self.head
            .as_ref()
            .is_some_and(|head| same_head(head, expected))
            .then(|| Arc::clone(&self.rows))
    }

    /// Applies one folded write; on an undecodable put, clears — the
    /// projection degrades to a rescan rather than serving wrong rows.
    ///
    /// Copies the rows away from any serve still holding them, so a reader
    /// materializing off the lock never sees a batch half-applied.
    fn fold(&mut self, key: K, bytes: Option<&[u8]>)
    where
        K: Clone,
        V: prost::Message + Default + Clone,
    {
        if self.head.is_none() {
            return;
        }
        // Decoded before the rows are copied away from any outstanding
        // serve, so a corrupt value clears without paying for the copy.
        let decoded = match bytes {
            None => None,
            Some(bytes) => {
                let Ok(decoded) = value::decode_value(bytes) else {
                    self.clear();
                    return;
                };
                Some(decoded)
            }
        };

        let rows = Arc::make_mut(&mut self.rows);
        match decoded {
            None => {
                rows.remove(&key);
            }
            Some(decoded) => {
                rows.insert(key, decoded);
            }
        }
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

/// The cached head view iff it stands at exactly the state `expected`
/// names.
pub(crate) fn cached_head_view(
    cache: &std::sync::RwLock<ProjectionCache>,
    expected: &HeadValue,
) -> Option<Arc<CatalogSnapshot>> {
    cache
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .head_view(expected)
}

/// The cached head view whatever state it stands at — the base an
/// incremental refresh advances when it has fallen behind.
pub(crate) fn held_head_view(
    cache: &std::sync::RwLock<ProjectionCache>,
) -> Option<Arc<CatalogSnapshot>> {
    cache
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .head_view
        .clone()
}

pub(crate) fn install_head_view(
    cache: &std::sync::RwLock<ProjectionCache>,
    view: Arc<CatalogSnapshot>,
) {
    cache
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .set_head_view(view);
}

/// The install epoch to capture before reading the store.
pub(crate) fn cache_epoch(cache: &std::sync::RwLock<ProjectionCache>) -> u64 {
    cache
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .epoch()
}

/// Installs `view` only if no invalidation has intervened since `epoch` was
/// captured.
pub(crate) fn install_head_view_at(
    cache: &std::sync::RwLock<ProjectionCache>,
    epoch: u64,
    view: Arc<CatalogSnapshot>,
) {
    cache
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .set_head_view_at(epoch, view);
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

/// The shared `current` half at exactly `head`, if installed.
pub(crate) fn shared_current_entities(
    cache: &std::sync::RwLock<ProjectionCache>,
    head: &HeadValue,
) -> Option<Arc<Vec<EntityRecord>>> {
    cache
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .current_entities_at(head)
}

/// Installs the shared `current` half. Unconditional: the stamp keying
/// makes a stale install self-invalidating.
pub(crate) fn install_shared_current_entities(
    cache: &std::sync::RwLock<ProjectionCache>,
    head: HeadValue,
    records: Arc<Vec<EntityRecord>>,
) {
    cache
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .install_current_entities(head, records);
}

/// The projections DuckLake re-reads per transaction, maintained on a
/// read-write catalog so serving them does not rescan the store.
pub(crate) struct ProjectionCache {
    snapshots: Maintained<u64, SnapshotValue>,
    table_stats: Maintained<u64, TableStatsValue>,
    table_column_stats: Maintained<(u64, u64), TableColumnStatsValue>,
    /// The `current` half of the shared decoded record set, stamped with
    /// the head it was scanned at. Dropped by a batch that writes into
    /// that subspace; otherwise restamped, since its rows still stand.
    current_entities: Option<(HeadValue, Arc<Vec<EntityRecord>>)>,
    /// The `history` half of the shared record set, stamped the same way.
    history_entities: Option<(HeadValue, Arc<Vec<EntityRecord>>)>,
    /// The materialized head view, folded forward on every commit; a fold
    /// that cannot be applied faithfully clears it.
    head_view: Option<Arc<CatalogSnapshot>>,
    /// The state the last folded batch left, and so the state anything
    /// carried across a batch must already have been at.
    ///
    /// A reader installs what it scanned stamped with the head it read
    /// before scanning, which a commit landing mid-scan leaves behind the
    /// cache. Serving on an exact stamp match makes that harmless on its
    /// own — those rows are right at that stamp — but carrying such rows
    /// across a batch would restamp them as a state they were never
    /// scanned at. Anything not standing here when a batch arrives is
    /// dropped rather than carried.
    folded_head: Option<HeadValue>,
    /// Bumped by every invalidation.
    epoch: u64,
}

impl ProjectionCache {
    /// Roughly what this handle's decoded catalog holds, in bytes: the
    /// shared record sets, the maintained projections, and the folded head
    /// view. Encoded lengths throughout, so it understates the decoded form.
    pub(crate) fn estimated_bytes(&self) -> u64 {
        let records = |half: &Option<(HeadValue, Arc<Vec<EntityRecord>>)>| {
            half.as_ref().map_or(0, |(_, records)| {
                records.iter().map(EntityRecord::estimated_bytes).sum()
            })
        };

        records(&self.current_entities)
            .saturating_add(records(&self.history_entities))
            .saturating_add(self.snapshots.estimated_bytes())
            .saturating_add(self.table_stats.estimated_bytes())
            .saturating_add(self.table_column_stats.estimated_bytes())
            .saturating_add(
                self.head_view
                    .as_ref()
                    .map_or(0, |view| view.estimated_bytes()),
            )
    }

    pub(crate) fn empty() -> Self {
        Self {
            snapshots: Maintained::empty(),
            table_stats: Maintained::empty(),
            table_column_stats: Maintained::empty(),
            current_entities: None,
            history_entities: None,
            head_view: None,
            folded_head: None,
            epoch: 0,
        }
    }

    /// The head view iff it stands at exactly the state `expected` names,
    /// both halves of the stamp checked.
    pub(crate) fn head_view(&self, expected: &HeadValue) -> Option<Arc<CatalogSnapshot>> {
        self.head_view
            .as_ref()
            .filter(|view| {
                view.snapshot.snapshot_id == expected.snapshot_id
                    && view.batch_seq == expected.batch_seq
            })
            .cloned()
    }

    pub(crate) fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Installs unconditionally, and moves the epoch so a pending
    /// [`set_head_view_at`](Self::set_head_view_at) is refused.
    pub(crate) fn set_head_view(&mut self, view: Arc<CatalogSnapshot>) {
        self.head_view = Some(view);
        self.epoch = self.epoch.wrapping_add(1);
    }

    /// Installs only if no invalidation has intervened since `epoch`.
    pub(crate) fn set_head_view_at(&mut self, epoch: u64, view: Arc<CatalogSnapshot>) {
        if self.epoch == epoch {
            self.head_view = Some(view);
        }
    }

    pub(crate) fn clear_head_view(&mut self) {
        self.head_view = None;
        self.epoch = self.epoch.wrapping_add(1);
    }

    /// Drops the head view and the shared `current` half together. A write
    /// that reclaims records without moving the head leaves both standing at
    /// a stamp that no longer names what the store holds, so neither may be
    /// served and neither can be folded forward.
    pub(crate) fn clear_current_state(&mut self) {
        self.head_view = None;
        self.current_entities = None;
        self.epoch = self.epoch.wrapping_add(1);
    }

    pub(crate) fn install_current_entities(
        &mut self,
        head: HeadValue,
        records: Arc<Vec<EntityRecord>>,
    ) {
        self.current_entities = Some((head, records));
    }

    pub(crate) fn install_history_entities(
        &mut self,
        head: HeadValue,
        records: Arc<Vec<EntityRecord>>,
    ) {
        self.history_entities = Some((head, records));
    }

    /// Serves the `current` half of the shared record set if it stands at
    /// exactly `expected`, both halves of the stamp checked.
    pub(crate) fn current_entities_at(
        &self,
        expected: &HeadValue,
    ) -> Option<Arc<Vec<EntityRecord>>> {
        self.current_entities
            .as_ref()
            .and_then(|(head, records)| same_head(head, expected).then(|| Arc::clone(records)))
    }

    /// As [`current_entities_at`](Self::current_entities_at), for the
    /// `history` half.
    pub(crate) fn history_entities_at(
        &self,
        expected: &HeadValue,
    ) -> Option<Arc<Vec<EntityRecord>>> {
        self.history_entities
            .as_ref()
            .and_then(|(head, records)| same_head(head, expected).then(|| Arc::clone(records)))
    }

    pub(crate) fn install_snapshots(&mut self, head: HeadValue, rows: Vec<SnapshotValue>) {
        self.snapshots
            .install(head, rows.into_iter().map(|r| (r.snapshot_id, r)).collect());
    }

    pub(crate) fn install_table_stats(&mut self, head: HeadValue, rows: Vec<TableStatsValue>) {
        self.table_stats
            .install(head, rows.into_iter().map(|r| (r.table_id, r)).collect());
    }

    pub(crate) fn install_table_column_stats(
        &mut self,
        head: HeadValue,
        rows: Vec<TableColumnStatsValue>,
    ) {
        self.table_column_stats.install(
            head,
            rows.into_iter()
                .map(|r| ((r.table_id, r.column_id), r))
                .collect(),
        );
    }

    /// Serves the snapshot projection if it is exactly at `expected_head`.
    /// Shared, not copied: [`materialize`] turns it into rows off the lock.
    pub(crate) fn snapshots_at(
        &self,
        expected: &HeadValue,
    ) -> Option<Arc<BTreeMap<u64, SnapshotValue>>> {
        self.snapshots.serve(expected)
    }

    pub(crate) fn table_stats_at(
        &self,
        expected: &HeadValue,
    ) -> Option<Arc<BTreeMap<u64, TableStatsValue>>> {
        self.table_stats.serve(expected)
    }

    pub(crate) fn table_column_stats_at(
        &self,
        expected: &HeadValue,
    ) -> Option<Arc<BTreeMap<(u64, u64), TableColumnStatsValue>>> {
        self.table_column_stats.serve(expected)
    }

    /// Folds one committed batch, stamping every installed projection with
    /// the head record the batch itself wrote. An undecodable key, or a
    /// batch with no head write matching `new_head`, clears everything.
    pub(crate) fn apply_batch(&mut self, writes: &[StagedWrite], new_head: u64) {
        // A half the batch did not write is still exactly right at the new
        // head, so it rides the stamp forward instead of being rescanned.
        // Inline, index-entry, and deletion-schedule batches write neither.
        let mut wrote_current = false;
        let mut wrote_history = false;
        self.drop_what_lags_behind();
        for (encoded_key, write) in writes {
            let bytes = write.as_deref();
            let key = Key::decode(encoded_key);
            match key {
                Ok(Key::Current(_)) => wrote_current = true,
                Ok(Key::History(_)) => wrote_history = true,
                _ => {}
            }
            match key {
                Ok(Key::Snapshot { snapshot_id }) => self.snapshots.fold(snapshot_id, bytes),
                Ok(Key::Current(CurrentKey::Entity(EntityKey::TableStats { table_id }))) => {
                    self.table_stats.fold(table_id, bytes);
                }
                Ok(Key::Current(CurrentKey::Entity(EntityKey::TableColumnStats {
                    table_id,
                    column_id,
                }))) => self.table_column_stats.fold((table_id, column_id), bytes),
                Ok(_) => {}
                Err(_) => {
                    self.current_entities = None;
                    self.history_entities = None;
                    self.snapshots.clear();
                    self.table_stats.clear();
                    self.table_column_stats.clear();
                    return;
                }
            }
        }
        if wrote_current {
            self.current_entities = None;
        }
        if wrote_history {
            self.history_entities = None;
        }
        // Cleared rather than asserted: this runs under the projection write
        // lock inside a spawned commit, where a panic strands the joiner.
        let Some(stamp) = head_stamp(writes).filter(|stamp| stamp.snapshot_id == new_head) else {
            self.current_entities = None;
            self.history_entities = None;
            self.snapshots.clear();
            self.table_stats.clear();
            self.table_column_stats.clear();
            return;
        };
        self.snapshots.advance(stamp);
        self.table_stats.advance(stamp);
        self.table_column_stats.advance(stamp);
        advance_half(&mut self.current_entities, stamp);
        advance_half(&mut self.history_entities, stamp);
        self.folded_head = Some(stamp);
    }

    /// Drops everything not standing at the state the last batch left, so
    /// a fold or a restamp only ever moves rows that are one batch behind.
    /// Everything is kept the first time, when there is nothing to be
    /// behind.
    fn drop_what_lags_behind(&mut self) {
        let Some(folded_head) = self.folded_head else {
            return;
        };
        let lags = |half: &Option<(HeadValue, Arc<Vec<EntityRecord>>)>| {
            half.as_ref()
                .is_some_and(|(head, _)| !same_head(head, &folded_head))
        };
        if lags(&self.current_entities) {
            self.current_entities = None;
        }
        if lags(&self.history_entities) {
            self.history_entities = None;
        }
        self.snapshots.clear_unless_at(folded_head);
        self.table_stats.clear_unless_at(folded_head);
        self.table_column_stats.clear_unless_at(folded_head);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;

    use super::*;
    use crate::{
        Catalog, CatalogOptions, ColumnDef,
        ffi_support::{dump_snapshots, dump_table_column_stats, dump_table_stats},
        store::{
            key::{EntityKey, Key},
            proto::{SnapshotValue, TableColumnStatsValue, TableStatsValue},
            value::encode_value,
        },
    };

    /// A view holding nothing but its head stamp.
    fn view_at(snapshot_id: u64) -> Arc<CatalogSnapshot> {
        Arc::new(CatalogSnapshot {
            snapshot: SnapshotValue {
                snapshot_id,
                ..SnapshotValue::default()
            },
            ..CatalogSnapshot::default()
        })
    }

    /// The head record naming the state `view_at` builds a view of.
    fn head_at(snapshot_id: u64) -> HeadValue {
        HeadValue {
            snapshot_id,
            batch_seq: 0,
        }
    }

    /// A reader pins its handle and then reads, so an invalidation can land
    /// mid-read. Installing afterwards must not resurrect the view that
    /// invalidation existed to discard.
    #[test]
    fn an_interleaved_invalidation_voids_an_install() {
        let cache = std::sync::RwLock::new(ProjectionCache::empty());

        let epoch = cache_epoch(&cache);
        invalidate_head_view(&cache);
        install_head_view_at(&cache, epoch, view_at(7));

        assert!(cached_head_view(&cache, &head_at(7)).is_none());
    }

    /// Without an intervening invalidation the same install must land, or
    /// no read path could ever warm the cache.
    #[test]
    fn an_uncontended_install_lands() {
        let cache = std::sync::RwLock::new(ProjectionCache::empty());

        let epoch = cache_epoch(&cache);
        install_head_view_at(&cache, epoch, view_at(7));

        assert!(cached_head_view(&cache, &head_at(7)).is_some());
    }

    /// The store's head record, which is what the projections key on.
    async fn current_head(catalog: &Catalog) -> HeadValue {
        let session = catalog.begin_read().await.unwrap();
        let head = crate::store::read::read_head(session.handle())
            .await
            .unwrap()
            .expect("an initialized store has a head");
        session.finish();
        head
    }

    /// Snapshot rows read directly from the store, bypassing the cache.
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

            let head = current_head(&catalog).await;
            let cache_current = {
                let guard = catalog
                    .projections()
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                guard.snapshots_at(&head).is_some()
            };
            assert!(
                cache_current,
                "cache must be current at head {head:?} after serving"
            );

            let _ = dump_table_stats(&catalog).await.unwrap();
            let _ = dump_table_column_stats(&catalog).await.unwrap();
        }
    }

    /// Commits one table, so the handle has a head view to hold.
    async fn seed_one_table(catalog: &Catalog, name: &str) {
        catalog
            .commit(|tx| {
                let main = tx.schemas()[0].id;
                tx.create_table(
                    main,
                    name,
                    &[ColumnDef {
                        name: "id".into(),
                        column_type: "BIGINT".into(),
                        nulls_allowed: false,
                        default_value: None,
                        children: Vec::new(),
                    }],
                )?;
                Ok(())
            })
            .await
            .unwrap();
    }

    /// Every read seam a warm handle serves, once.
    async fn read_every_seam(catalog: &Catalog) {
        let _ = catalog.snapshot().await.unwrap();
        let _ = dump_snapshots(catalog).await.unwrap();
        let _ = dump_table_stats(catalog).await.unwrap();
        let _ = dump_table_column_stats(catalog).await.unwrap();
    }

    /// A read-write handle is the store's only writer, so the view it holds
    /// is at head by construction. Once warm it must resolve the head from
    /// that view and never from the store — the point read per call is the
    /// floor under every probe and dump on the write path.
    #[tokio::test]
    async fn a_warm_writer_resolves_head_without_reading_the_store() {
        let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
            .await
            .unwrap();
        seed_one_table(&catalog, "t").await;

        // The first pass installs each projection; from here it is warm.
        read_every_seam(&catalog).await;
        let warmed = catalog.head_reads();

        for _ in 0..8 {
            read_every_seam(&catalog).await;
        }

        assert_eq!(
            catalog.head_reads(),
            warmed,
            "a warm read-write handle read `sys/head`"
        );
    }

    /// The warm path must not go stale: a commit folds the held view
    /// forward, so the read that follows serves the new state without ever
    /// having read the head.
    #[tokio::test]
    async fn a_warm_writer_still_sees_its_own_later_commits() {
        let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
            .await
            .unwrap();
        seed_one_table(&catalog, "first").await;
        read_every_seam(&catalog).await;
        let warmed = catalog.head_reads();

        seed_one_table(&catalog, "second").await;

        let view = catalog.snapshot().await.unwrap();
        let main = view.schemas()[0].id;
        assert!(
            view.table_by_name(main, "second").is_some(),
            "a warm read missed the commit that preceded it"
        );
        assert_eq!(
            dump_snapshots(&catalog).await.unwrap(),
            scanned_snapshots(&catalog).await,
            "the warm dump disagreed with the store"
        );
        assert_eq!(
            catalog.head_reads(),
            warmed,
            "seeing the commit cost a head read"
        );
    }

    /// A read-only handle follows another process's commits, so it has no
    /// such premise and must keep resolving the head from the store.
    #[tokio::test]
    async fn a_read_only_handle_resolves_head_from_the_store_every_read() {
        let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let writer = Catalog::open(Arc::clone(&object_store), CatalogOptions::default())
            .await
            .unwrap();
        seed_one_table(&writer, "t").await;
        writer.close().await.unwrap();

        let reader = Catalog::open_read_only(object_store, CatalogOptions::default())
            .await
            .unwrap();
        let _ = reader.snapshot().await.unwrap();
        let after_first = reader.head_reads();

        for _ in 0..4 {
            let _ = reader.snapshot().await.unwrap();
        }

        assert_eq!(
            reader.head_reads(),
            after_first + 4,
            "a read-only handle served a read without resolving the head"
        );
    }

    /// Dropping the view is what stops a writer serving state the store has
    /// left — a head-preserving batch reuses the snapshot id, so the stamp
    /// alone would not give it away.
    #[tokio::test]
    async fn an_invalidated_view_sends_the_writer_back_to_the_store() {
        let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
            .await
            .unwrap();
        seed_one_table(&catalog, "t").await;
        let _ = catalog.snapshot().await.unwrap();
        assert!(catalog.writer_head_view().is_some());

        invalidate_head_view(catalog.projections());

        assert!(
            catalog.writer_head_view().is_none(),
            "an invalidated cache still served a warm read"
        );
    }

    /// A commit's folded view outranks anything a read materialized before
    /// it: the writer serves the held view as head, so an install landing
    /// on top of one would answer from a state the store has left.
    #[test]
    fn a_commit_install_refuses_an_older_readers_install() {
        let cache = std::sync::RwLock::new(ProjectionCache::empty());

        // A read captures the epoch, then a commit installs its fold.
        let epoch = cache_epoch(&cache);
        install_head_view(&cache, view_at(9));

        // The read's own install, now stale, must not land.
        install_head_view_at(&cache, epoch, view_at(7));

        assert!(cached_head_view(&cache, &head_at(9)).is_some());
        assert!(cached_head_view(&cache, &head_at(7)).is_none());
    }

    fn snapshot_value(id: u64) -> SnapshotValue {
        SnapshotValue {
            snapshot_id: id,
            snapshot_time_micros: 1,
            schema_version: 0,
            next_catalog_id: 1,
            next_file_id: 0,
            changes_made: String::new(),
            author: None,
            commit_message: None,
            commit_extra_info: None,
            schema_changed_table_ids: Vec::new(),
            deleted_data_file_ids: Vec::new(),
        }
    }

    fn stats_value(table_id: u64, record_count: u64) -> TableStatsValue {
        TableStatsValue {
            table_id,
            record_count,
            next_row_id: record_count,
            file_size_bytes: 100,
        }
    }

    fn column_stats_value(table_id: u64, column_id: u64) -> TableColumnStatsValue {
        TableColumnStatsValue {
            table_id,
            column_id,
            contains_null: Some(false),
            contains_nan: None,
            min_value: Some("1".into()),
            max_value: Some("9".into()),
            extra_stats: None,
        }
    }

    /// A head stamp, as the projections key on.
    fn stamp(snapshot_id: u64, batch_seq: u64) -> HeadValue {
        HeadValue {
            snapshot_id,
            batch_seq,
        }
    }

    /// The `sys/head` write every batch carries. `apply_batch` reads the
    /// stamp out of it rather than being told separately.
    fn head_write(snapshot_id: u64, batch_seq: u64) -> StagedWrite {
        (
            Key::Sys(SysKey::Head).encode(),
            Some(encode_value(&stamp(snapshot_id, batch_seq))),
        )
    }

    fn installed_at_three() -> ProjectionCache {
        let mut cache = ProjectionCache::empty();
        cache.install_snapshots(stamp(3, 3), (0..=3).map(snapshot_value).collect());
        cache.install_table_stats(stamp(3, 3), vec![stats_value(7, 10)]);
        cache.install_table_column_stats(stamp(3, 3), vec![column_stats_value(7, 1)]);
        cache
    }

    #[test]
    fn fold_inserts_snapshot_and_updates_stats() {
        let mut cache = installed_at_three();

        let writes = vec![
            (
                Key::Snapshot { snapshot_id: 4 }.encode(),
                Some(encode_value(&snapshot_value(4))),
            ),
            (
                Key::current(EntityKey::TableStats { table_id: 7 }).encode(),
                Some(encode_value(&stats_value(7, 11))),
            ),
            head_write(4, 4),
        ];
        cache.apply_batch(&writes, 4);

        let at = stamp(4, 4);
        let snapshots = materialize(&cache.snapshots_at(&at).unwrap());
        assert_eq!(snapshots.len(), 5);
        assert_eq!(snapshots.last().unwrap().snapshot_id, 4);

        let stats = materialize(&cache.table_stats_at(&at).unwrap());
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].record_count, 11);

        assert_eq!(cache.table_column_stats_at(&at).unwrap().len(), 1);
    }

    #[test]
    fn fold_deletes_remove_rows() {
        let mut cache = installed_at_three();

        let writes = vec![
            (Key::Snapshot { snapshot_id: 2 }.encode(), None),
            (
                Key::current(EntityKey::TableColumnStats {
                    table_id: 7,
                    column_id: 1,
                })
                .encode(),
                None,
            ),
            // Head-preserving, as a maintenance batch is: the snapshot id
            // stands and the batch count moves.
            head_write(3, 4),
        ];
        cache.apply_batch(&writes, 3);

        let at = stamp(3, 4);
        let snapshots = materialize(&cache.snapshots_at(&at).unwrap());
        assert_eq!(snapshots.len(), 3);
        assert!(snapshots.iter().all(|s| s.snapshot_id != 2));
        assert!(cache.table_column_stats_at(&at).unwrap().is_empty());
        // And the state it replaced is no longer served, though its
        // snapshot id is unchanged — the reason the key is the whole stamp.
        assert!(cache.snapshots_at(&stamp(3, 3)).is_none());
    }

    #[test]
    fn serve_refuses_a_mismatched_head() {
        let cache = installed_at_three();
        assert!(cache.snapshots_at(&stamp(4, 4)).is_none());
        assert!(cache.table_stats_at(&stamp(2, 2)).is_none());
        // Same snapshot id, different batch: a maintenance commit's shape.
        assert!(cache.snapshots_at(&stamp(3, 4)).is_none());
    }

    #[test]
    fn fold_on_an_empty_cache_is_a_noop() {
        let mut cache = ProjectionCache::empty();
        cache.apply_batch(
            &[
                (
                    Key::Snapshot { snapshot_id: 1 }.encode(),
                    Some(encode_value(&snapshot_value(1))),
                ),
                head_write(1, 1),
            ],
            1,
        );
        assert!(cache.snapshots_at(&stamp(1, 1)).is_none());
    }

    #[test]
    fn an_undecodable_value_clears_only_the_touched_projection() {
        let mut cache = installed_at_three();
        cache.apply_batch(
            &[
                (
                    Key::Snapshot { snapshot_id: 4 }.encode(),
                    Some(vec![0xff, 0xff, 0xff, 0xff]),
                ),
                head_write(4, 4),
            ],
            4,
        );
        // Snapshots degrade to a rescan; the untouched stats fold forward.
        assert!(cache.snapshots_at(&stamp(4, 4)).is_none());
        assert!(cache.table_stats_at(&stamp(4, 4)).is_some());
    }

    #[test]
    fn an_undecodable_key_clears_everything() {
        let mut cache = installed_at_three();
        cache.apply_batch(&[(vec![0xff, 0xee], Some(vec![])), head_write(4, 4)], 4);
        assert!(cache.snapshots_at(&stamp(4, 4)).is_none());
        assert!(cache.table_stats_at(&stamp(4, 4)).is_none());
        assert!(cache.table_column_stats_at(&stamp(4, 4)).is_none());
    }

    /// A reader whose scan straddled a commit installs rows stamped
    /// behind the cache. Those rows are right at that stamp and may be
    /// served there, but the next batch must not fold onto them or carry
    /// them forward — either would leave rows no stamp describes.
    #[test]
    fn rows_installed_behind_the_cache_are_dropped_rather_than_carried() {
        let mut cache = ProjectionCache::empty();

        // The cache folds a batch, so it knows what state it stands at.
        cache.install_snapshots(stamp(1, 1), vec![snapshot_value(0), snapshot_value(1)]);
        cache.install_current_entities(
            stamp(1, 1),
            Arc::new(vec![EntityRecord::TableStats(stats_value(7, 10))]),
        );
        cache.apply_batch(
            &[
                (
                    Key::Snapshot { snapshot_id: 2 }.encode(),
                    Some(encode_value(&snapshot_value(2))),
                ),
                head_write(2, 2),
            ],
            2,
        );
        assert_eq!(
            materialize(&cache.snapshots_at(&stamp(2, 2)).unwrap()).len(),
            3
        );

        // A slow reader now installs what it scanned before that batch.
        cache.install_snapshots(stamp(1, 1), vec![snapshot_value(0), snapshot_value(1)]);
        cache.install_current_entities(
            stamp(1, 1),
            Arc::new(vec![EntityRecord::TableStats(stats_value(7, 10))]),
        );

        // The next batch writes no entity key, so the half would ride
        // forward and the snapshot fold would land on rows missing
        // snapshot 2. Both are dropped instead.
        cache.apply_batch(
            &[
                (
                    Key::Snapshot { snapshot_id: 3 }.encode(),
                    Some(encode_value(&snapshot_value(3))),
                ),
                head_write(3, 3),
            ],
            3,
        );
        assert!(cache.snapshots_at(&stamp(3, 3)).is_none());
        assert!(cache.current_entities_at(&stamp(3, 3)).is_none());
    }

    /// A batch writing neither record subspace leaves both halves in
    /// place, restamped — an inline or index-entry commit costs the next
    /// reader no rescan. A batch that does write one drops that one.
    #[test]
    fn untouched_record_halves_ride_the_stamp_forward() {
        let half = || Arc::new(vec![EntityRecord::TableStats(stats_value(7, 10))]);

        let mut cache = installed_at_three();
        cache.install_current_entities(stamp(3, 3), half());
        cache.install_history_entities(stamp(3, 3), half());

        // An inline chunk names no entity key at all.
        cache.apply_batch(
            &[
                (
                    Key::Inline(crate::store::key::InlineKey::Live(
                        crate::store::key::InlineOperation::Insert {
                            table_id: 7,
                            schema_version: 1,
                            begin_snapshot: 4,
                            chunk_seq: 0,
                        },
                    ))
                    .encode(),
                    Some(vec![1, 2, 3]),
                ),
                head_write(4, 4),
            ],
            4,
        );
        assert!(cache.current_entities_at(&stamp(4, 4)).is_some());
        assert!(cache.history_entities_at(&stamp(4, 4)).is_some());
        assert!(cache.current_entities_at(&stamp(3, 3)).is_none());

        // A current write drops the current half and spares history.
        cache.apply_batch(
            &[
                (
                    Key::current(EntityKey::Schema { schema_id: 9 }).encode(),
                    Some(vec![1, 2, 3]),
                ),
                head_write(5, 5),
            ],
            5,
        );
        assert!(cache.current_entities_at(&stamp(5, 5)).is_none());
        assert!(cache.history_entities_at(&stamp(5, 5)).is_some());
    }

    #[test]
    fn irrelevant_keys_still_advance_the_head() {
        let mut cache = installed_at_three();
        cache.apply_batch(
            &[
                (
                    Key::current(EntityKey::Schema { schema_id: 9 }).encode(),
                    Some(vec![1, 2, 3]),
                ),
                head_write(4, 4),
            ],
            4,
        );
        assert_eq!(cache.snapshots_at(&stamp(4, 4)).unwrap().len(), 4);
        assert!(cache.snapshots_at(&stamp(3, 3)).is_none());
    }

    /// A batch with no head write cannot be attributed to a state, so
    /// nothing may keep claiming one. Every catalog batch writes the head,
    /// so this is a corruption guard rather than a live path.
    #[test]
    fn a_batch_without_a_head_write_clears_everything() {
        let mut cache = installed_at_three();
        cache.apply_batch(
            &[(
                Key::Snapshot { snapshot_id: 4 }.encode(),
                Some(encode_value(&snapshot_value(4))),
            )],
            4,
        );
        assert!(cache.snapshots_at(&stamp(4, 4)).is_none());
        assert!(cache.snapshots_at(&stamp(3, 3)).is_none());
        assert!(cache.table_stats_at(&stamp(3, 3)).is_none());
    }
}

//! Maintained projections: decoded snapshot and statistics rows a catalog
//! serves without rescanning. Every serve is guarded by the head snapshot
//! id the caller observed; a mismatch degrades to a fresh scan, never to
//! wrong rows.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use crate::store::{
    proto::{HeadValue, SnapshotValue, TableColumnStatsValue, TableStatsValue},
    read::EntityRecord,
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

/// Copies a served projection's rows out. Called after the cache lock is
/// dropped: the copying is what a serve deliberately leaves undone.
pub(crate) fn materialize<K: Ord, V: Clone>(rows: &BTreeMap<K, V>) -> Vec<V> {
    rows.values().cloned().collect()
}

/// Whether two head records name the same store state.
fn same_head(a: &HeadValue, b: &HeadValue) -> bool {
    a.snapshot_id == b.snapshot_id && a.batch_seq == b.batch_seq
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

    /// The rows if they stand at exactly `expected`, shared rather than
    /// copied so the caller can materialize them off the cache lock.
    fn serve(&self, expected: &HeadValue) -> Option<Arc<BTreeMap<K, V>>> {
        self.head
            .as_ref()
            .is_some_and(|head| same_head(head, expected))
            .then(|| Arc::clone(&self.rows))
    }
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

/// Whether `table_id`'s inline chunk-range directory is known complete.
pub(crate) fn inline_directory_complete(
    cache: &std::sync::RwLock<ProjectionCache>,
    table_id: u64,
) -> bool {
    cache
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .inline_directory_complete(table_id)
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

/// Drops every projection over the `current` half, for a write that removed
/// records without going through the commit protocol.
pub(crate) fn invalidate_current_state(cache: &std::sync::RwLock<ProjectionCache>) {
    cache
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clear_current_state();
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
    /// The highest format version this handle has seen the store stamped
    /// at. The stamp is forward-only and no writer lowers it, so a floor
    /// observed once stays true; a commit whose target sits at or below it
    /// owes no stamp and need not read one. Invalidation leaves it
    /// standing: it describes the store, not a view of it.
    format_floor: u64,
    /// Tables whose inline chunk-range directory names every chunk,
    /// verified once against the chunk scan. Monotone from there: with the
    /// store stamped at or past the directory format, every binary that
    /// can write a chunk writes its locator in the same batch.
    /// Invalidation leaves it standing: it describes the store, not a view
    /// of it.
    inline_directory_complete: BTreeSet<u64>,
}

impl ProjectionCache {
    /// Roughly what this handle's decoded catalog holds, in bytes: the
    /// shared record sets and the maintained projections. Encoded lengths
    /// throughout, so it understates the decoded form.
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
    }

    pub(crate) fn empty() -> Self {
        Self {
            snapshots: Maintained::empty(),
            table_stats: Maintained::empty(),
            table_column_stats: Maintained::empty(),
            current_entities: None,
            history_entities: None,
            format_floor: 0,
            inline_directory_complete: BTreeSet::new(),
        }
    }

    /// Whether `table_id`'s chunk-range directory is known to name every
    /// chunk.
    pub(crate) fn inline_directory_complete(&self, table_id: u64) -> bool {
        self.inline_directory_complete.contains(&table_id)
    }

    /// Records `table_id`'s directory as verified complete.
    pub(crate) fn note_inline_directory_complete(&mut self, table_id: u64) {
        self.inline_directory_complete.insert(table_id);
    }

    /// The highest format version observed, 0 before any has been.
    pub(crate) fn format_floor(&self) -> u64 {
        self.format_floor
    }

    /// Records an observed format version, keeping the highest.
    pub(crate) fn raise_format_floor(&mut self, observed: u64) {
        self.format_floor = self.format_floor.max(observed);
    }

    /// Drops the shared `current` half. A write that reclaims records
    /// without moving the head leaves it standing at a stamp that no longer
    /// names what the store holds, so it may not be served.
    pub(crate) fn clear_current_state(&mut self) {
        self.current_entities = None;
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
            .and_then(|(head, records)| (head == expected).then(|| Arc::clone(records)))
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
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;

    use super::*;
    use crate::{
        Catalog, CatalogOptions, ColumnDef,
        ffi_support::{dump_snapshots, dump_table_column_stats, dump_table_stats},
        store::proto::{SnapshotValue, TableColumnStatsValue, TableStatsValue},
    };

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

    /// A read that follows a commit on the same handle serves the new
    /// state, and repeated dumps agree with it.
    #[tokio::test]
    async fn a_warm_writer_still_sees_its_own_later_commits() {
        let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default())
            .await
            .unwrap();
        seed_one_table(&catalog, "first").await;
        read_every_seam(&catalog).await;

        seed_one_table(&catalog, "second").await;

        let view = catalog.snapshot().await.unwrap();
        let main = view.schemas()[0].id;
        assert!(
            view.table_by_name(main, "second").is_some(),
            "a warm read missed the commit that preceded it"
        );
        assert_eq!(
            dump_snapshots(&catalog).await.unwrap(),
            dump_snapshots(&catalog).await.unwrap(),
            "the warm dump disagreed with the store"
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

    /// A head stamp, as the projections key on.
    fn stamp(snapshot_id: u64, batch_seq: u64) -> HeadValue {
        HeadValue {
            snapshot_id,
            batch_seq,
        }
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
            transaction_id: None,
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

    fn installed_at_three() -> ProjectionCache {
        let mut cache = ProjectionCache::empty();
        cache.install_snapshots(stamp(3, 3), (0..=3).map(snapshot_value).collect());
        cache.install_table_stats(stamp(3, 3), vec![stats_value(7, 10)]);
        cache.install_table_column_stats(stamp(3, 3), vec![column_stats_value(7, 1)]);
        cache
    }

    #[test]
    fn serve_refuses_a_mismatched_head() {
        let cache = installed_at_three();
        assert!(cache.snapshots_at(&stamp(4, 4)).is_none());
        assert!(cache.table_stats_at(&stamp(2, 2)).is_none());
        // Same snapshot id, different batch: a maintenance commit's shape.
        assert!(cache.snapshots_at(&stamp(3, 4)).is_none());
    }
}

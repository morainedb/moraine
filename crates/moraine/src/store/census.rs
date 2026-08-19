//! Measuring a store: physical bytes per segment from the manifest, and
//! live keys per subspace from a scan.
//!
//! The manifest half is size-independent — two object reads — and needs no
//! open handle, so it serves a store nobody has attached. The scan half
//! reads every key in the subspace and decodes none of the values.

use std::sync::Arc;

use futures::StreamExt;
use object_store::{ObjectStore, path::Path};
use slatedb::{
    admin::AdminBuilder,
    manifest::{Segment, SortedRun, SsTableView, VersionedManifest},
};
use tracing::warn;

use crate::{
    error::{Error, Result},
    store::{
        handle::ReadHandle,
        key::{CurrentKey, Key, Subspace, subspace_prefix},
    },
};

/// Physical size of one segment, as the manifest records it. The prefix is
/// the segment's own; empty means the unsegmented root tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SegmentSize {
    pub(crate) prefix: Vec<u8>,
    pub(crate) bytes: u64,
    pub(crate) l0_ssts: u32,
    pub(crate) sorted_runs: u32,
    pub(crate) sorted_run_ssts: u32,
}

impl SegmentSize {
    /// Whether the segment holds anything at all.
    fn is_empty(&self) -> bool {
        self.bytes == 0 && self.l0_ssts == 0 && self.sorted_runs == 0
    }
}

/// What the object store holds, by the kind of object holding it.
///
/// The manifest accounts for SST bytes only, so a store whose weight is in
/// its write-ahead log reads as nearly empty by segment while costing every
/// reader dearly — a reader replays the log at open. Counting the objects
/// is the only way to tell those apart.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ObjectTotals {
    pub(crate) total_objects: u64,
    pub(crate) total_bytes: u64,
    pub(crate) wal_objects: u64,
    pub(crate) wal_bytes: u64,
    pub(crate) manifest_objects: u64,
    pub(crate) manifest_bytes: u64,
    pub(crate) sst_objects: u64,
    pub(crate) sst_bytes: u64,
    /// Everything else the store's layout carries — the compactions file
    /// and any object a newer layout adds. Counted rather than dropped, so
    /// the parts always sum to the total.
    pub(crate) other_objects: u64,
    pub(crate) other_bytes: u64,
}

/// Every segment's physical size, at one manifest version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ManifestCensus {
    pub(crate) manifest_id: u64,
    pub(crate) segments: Vec<SegmentSize>,
    /// `None` when the store could not be listed — read-only credentials
    /// often grant `GetObject` without `ListBucket`, and a census that
    /// failed wholesale for that would be useless to the operator most
    /// likely to be holding them.
    pub(crate) objects: Option<ObjectTotals>,
}

impl ManifestCensus {
    /// The recorded size of the segment named by `prefix`, if the manifest
    /// carries one.
    pub(crate) fn segment(&self, prefix: &[u8]) -> Option<&SegmentSize> {
        self.segments
            .iter()
            .find(|segment| segment.prefix == prefix)
    }
}

/// Live keys and bytes of one subspace, as a scan finds them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct LiveTally {
    pub(crate) keys: u64,
    pub(crate) key_bytes: u64,
    pub(crate) value_bytes: u64,
    /// Deletion-schedule entries among `keys`; only `current` has any.
    pub(crate) scheduled_files: u64,
}

/// Reads the latest manifest and reports each segment's physical size.
///
/// Opens no `Db`, so it fences no writer and costs the same on a 3 GB store
/// as on an empty one.
pub(crate) async fn read_manifest_census(
    path: &str,
    object_store: Arc<dyn ObjectStore>,
) -> Result<ManifestCensus> {
    let view = AdminBuilder::new(path, Arc::clone(&object_store))
        .build()
        .read_compactor_state_view()
        .await
        .map_err(Error::from)?;

    let mut census = census_of_manifest(view.manifest());
    census.objects = match count_objects(path, object_store).await {
        Ok(totals) => Some(totals),
        Err(err) => {
            warn!(path, error = %err, "store listing failed; census reports segments only");
            None
        }
    };

    Ok(census)
}

/// Bytes the manifest accounts for, without the store listing a full
/// census pays for. One manifest read, so it costs the same on a large
/// store as on a small one.
pub(crate) async fn manifest_bytes(path: &str, object_store: Arc<dyn ObjectStore>) -> Result<u64> {
    let view = AdminBuilder::new(path, object_store)
        .build()
        .read_compactor_state_view()
        .await
        .map_err(Error::from)?;

    Ok(census_of_manifest(view.manifest())
        .segments
        .iter()
        .fold(0, |total, segment| total.saturating_add(segment.bytes)))
}

/// Totals every object under the store's prefix, by kind.
///
/// One listing, paginated by the object store — the only part of a census
/// whose cost grows with the store, and the only part that sees bytes the
/// manifest does not account for.
async fn count_objects(
    path: &str,
    object_store: Arc<dyn ObjectStore>,
) -> std::result::Result<ObjectTotals, object_store::Error> {
    let prefix = Path::from(path);
    let mut listing = object_store.list(Some(&prefix));
    let mut totals = ObjectTotals::default();

    while let Some(object) = listing.next().await {
        let object = object?;
        let size = object.size;
        totals.total_objects += 1;
        totals.total_bytes = totals.total_bytes.saturating_add(size);

        // SlateDB lays a store out as `<path>/{wal,compacted,manifest}/…`,
        // plus the compactions file beside them.
        match kind_of(&object.location, &prefix).as_deref() {
            Some("wal") => {
                totals.wal_objects += 1;
                totals.wal_bytes = totals.wal_bytes.saturating_add(size);
            }
            Some("manifest") => {
                totals.manifest_objects += 1;
                totals.manifest_bytes = totals.manifest_bytes.saturating_add(size);
            }
            Some("compacted") => {
                totals.sst_objects += 1;
                totals.sst_bytes = totals.sst_bytes.saturating_add(size);
            }
            // The compactions file, and whatever a layout this build
            // predates adds beside it.
            _ => {
                totals.other_objects += 1;
                totals.other_bytes = totals.other_bytes.saturating_add(size);
            }
        }
    }

    Ok(totals)
}

/// The directory directly under the store prefix that `location` sits in.
fn kind_of(location: &Path, prefix: &Path) -> Option<String> {
    let mut remainder = location.prefix_match(prefix)?;
    remainder.next().map(|part| part.as_ref().to_string())
}

/// The per-segment sizes a manifest records, root tree included.
fn census_of_manifest(manifest: &VersionedManifest) -> ManifestCensus {
    let mut segments: Vec<SegmentSize> = manifest.segments().iter().map(size_of_segment).collect();

    // A store created with the segment extractor keeps the root tree empty
    // by construction. Reporting it when it is not lets a census describe a
    // store written without one rather than hiding its bytes.
    let root = SegmentSize {
        prefix: Vec::new(),
        bytes: total_bytes(manifest.l0().iter().map(SsTableView::estimate_size)),
        l0_ssts: count(manifest.l0().len()),
        sorted_runs: count(manifest.compacted().len()),
        sorted_run_ssts: count(
            manifest
                .compacted()
                .iter()
                .map(|sr| sr.sst_views().len())
                .sum(),
        ),
    };
    if !root.is_empty() {
        segments.push(root);
    }

    ManifestCensus {
        manifest_id: manifest.id(),
        segments,
        objects: None,
    }
}

fn size_of_segment(segment: &Segment) -> SegmentSize {
    let l0 = segment.l0().iter().map(SsTableView::estimate_size);
    let runs = segment.compacted().iter().map(SortedRun::estimate_size);

    SegmentSize {
        prefix: segment.prefix().to_vec(),
        bytes: total_bytes(l0.chain(runs)),
        l0_ssts: count(segment.l0().len()),
        sorted_runs: count(segment.compacted().len()),
        sorted_run_ssts: count(
            segment
                .compacted()
                .iter()
                .map(|run| run.sst_views().len())
                .sum(),
        ),
    }
}

fn total_bytes(sizes: impl Iterator<Item = u64>) -> u64 {
    sizes.fold(0, u64::saturating_add)
}

/// Counts are per-tree SST and run tallies, which no store approaches
/// `u32::MAX` of; saturating keeps the census describing a store that did.
fn count(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

/// Counts the live keys of `subspace` and the bytes they and their values
/// occupy, decoding keys only where the tally distinguishes kinds.
///
/// Costs a full read of the subspace.
pub(crate) async fn scan_live(handle: ReadHandle<'_>, subspace: Subspace) -> Result<LiveTally> {
    let mut iterator = handle.scan_prefix(subspace_prefix(subspace), ..).await?;
    let mut tally = LiveTally::default();

    while let Some(entry) = iterator.next().await? {
        tally.keys += 1;
        tally.key_bytes += entry.key.len() as u64;
        tally.value_bytes += entry.value.len() as u64;

        // The deletion schedule shares `current` with the entity records,
        // and only their split says whether a bloated `current` is dead
        // weight a merge reclaims or a schedule cleanup drains.
        if subspace == Subspace::Current
            && matches!(
                Key::decode(&entry.key)?,
                Key::Current(CurrentKey::GcFile { .. })
            )
        {
            tally.scheduled_files += 1;
        }
    }

    Ok(tally)
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;
    use slatedb::{
        Db, IsolationLevel, WriteBatch,
        config::{FlushOptions, FlushType},
    };

    use super::*;
    use crate::store::{key::EntityKey, open::StoreBuilder};

    fn memory_store() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    /// Freezes the memtable and writes it out, so the manifest carries what
    /// was written. The census reads the manifest, so unflushed writes are
    /// not in it — a property, not an omission.
    async fn flush_to_l0(db: &Db) {
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .unwrap();
    }

    /// The census names the subspaces the store wrote, and only those.
    #[tokio::test]
    async fn manifest_census_reports_written_segments() {
        let store = memory_store();
        let db = StoreBuilder::new("census/store", Arc::clone(&store))
            .open_writer()
            .await
            .unwrap();

        let mut batch = WriteBatch::new();
        batch.put(
            Key::current(EntityKey::Schema { schema_id: 1 }).encode(),
            b"schema".as_slice(),
        );
        batch.put(
            Key::Snapshot { snapshot_id: 1 }.encode(),
            b"snap".as_slice(),
        );
        db.write(batch).await.unwrap();
        flush_to_l0(&db).await;

        let census = read_manifest_census("census/store", store).await.unwrap();

        let current = census
            .segment(&subspace_prefix(Subspace::Current))
            .expect("current segment");
        assert!(current.bytes > 0, "current: {current:?}");
        assert_eq!(current.l0_ssts, 1, "current: {current:?}");
        assert!(
            census
                .segment(&subspace_prefix(Subspace::Snapshot))
                .is_some()
        );
        assert!(census.segment(&subspace_prefix(Subspace::Index)).is_none());

        // The root tree stays empty under an extractor-configured store, so
        // it is reported not at all rather than as a zero row.
        assert!(census.segment(&[]).is_none(), "{census:?}");
    }

    /// The census reads the manifest, so a write still in the write-ahead
    /// log is not in it. Anyone reading a census against a busy writer is
    /// reading what has been written out, not what has been accepted.
    #[tokio::test]
    async fn manifest_census_omits_unflushed_writes() {
        let store = memory_store();
        let db = StoreBuilder::new("census/unflushed", Arc::clone(&store))
            .open_writer()
            .await
            .unwrap();

        let mut batch = WriteBatch::new();
        batch.put(
            Key::current(EntityKey::Schema { schema_id: 1 }).encode(),
            b"schema".as_slice(),
        );
        db.write(batch).await.unwrap();
        db.flush().await.unwrap();

        let census = read_manifest_census("census/unflushed", store)
            .await
            .unwrap();
        assert!(census.segments.is_empty(), "{census:?}");
    }

    /// The listing leg counts every object under the prefix, including the
    /// write-ahead log and the manifest — bytes the per-segment figures do
    /// not account for and no merge reclaims.
    #[tokio::test]
    async fn the_listing_counts_objects_the_manifest_does_not() {
        let store = memory_store();
        let db = StoreBuilder::new("census/objects", Arc::clone(&store))
            .open_writer()
            .await
            .unwrap();

        let mut batch = WriteBatch::new();
        batch.put(
            Key::current(EntityKey::Schema { schema_id: 1 }).encode(),
            b"schema".as_slice(),
        );
        db.write(batch).await.unwrap();
        db.flush().await.unwrap();

        let census = read_manifest_census("census/objects", store).await.unwrap();
        let objects = census.objects.expect("an in-memory store lists");

        // Every store carries a manifest, and this one's write is still in
        // the log — so the manifest accounts for no SST bytes at all while
        // the object store plainly holds some.
        assert!(objects.manifest_objects > 0, "{objects:?}");
        assert!(objects.total_bytes > 0, "{objects:?}");
        assert_eq!(
            objects.total_objects,
            objects.wal_objects
                + objects.manifest_objects
                + objects.sst_objects
                + objects.other_objects,
            "an object was counted in the total but attributed nowhere: {objects:?}"
        );
        assert!(
            objects.total_bytes > objects.sst_bytes,
            "the log and manifest weigh nothing: {objects:?}"
        );
    }

    /// The live leg counts what a scan finds, and splits `current` into
    /// entity records and deletion-schedule entries.
    #[tokio::test]
    async fn scan_live_counts_and_splits_current() {
        let store = memory_store();
        let db = StoreBuilder::new("census/live", Arc::clone(&store))
            .open_writer()
            .await
            .unwrap();

        let mut batch = WriteBatch::new();
        for schema_id in 0..3 {
            batch.put(
                Key::current(EntityKey::Schema { schema_id }).encode(),
                b"schema".as_slice(),
            );
        }
        for data_file_id in 0..2 {
            batch.put(
                Key::Current(CurrentKey::GcFile { data_file_id }).encode(),
                b"path".as_slice(),
            );
        }
        db.write(batch).await.unwrap();

        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let tally = scan_live(ReadHandle::Tx(&tx), Subspace::Current)
            .await
            .unwrap();
        tx.rollback();

        assert_eq!(tally.keys, 5);
        assert_eq!(tally.scheduled_files, 2);
        assert_eq!(tally.value_bytes, 3 * 6 + 2 * 4);
        assert!(tally.key_bytes > 0);
    }

    /// A deleted key is not live: the tally counts what a reader would see,
    /// not what the substrate still holds.
    #[tokio::test]
    async fn scan_live_ignores_deleted_keys() {
        let store = memory_store();
        let db = StoreBuilder::new("census/deleted", Arc::clone(&store))
            .open_writer()
            .await
            .unwrap();

        let mut batch = WriteBatch::new();
        for schema_id in 0..4 {
            batch.put(
                Key::current(EntityKey::Schema { schema_id }).encode(),
                b"schema".as_slice(),
            );
        }
        db.write(batch).await.unwrap();

        let mut batch = WriteBatch::new();
        for schema_id in 0..3 {
            batch.delete(Key::current(EntityKey::Schema { schema_id }).encode());
        }
        db.write(batch).await.unwrap();

        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let tally = scan_live(ReadHandle::Tx(&tx), Subspace::Current)
            .await
            .unwrap();
        tx.rollback();

        assert_eq!(tally.keys, 1);
    }

    /// A subspace nothing wrote tallies zero rather than failing.
    #[tokio::test]
    async fn scan_live_of_an_untouched_subspace_is_zero() {
        let db = StoreBuilder::new("census/empty", memory_store())
            .open_writer()
            .await
            .unwrap();

        let tx = db.begin(IsolationLevel::Snapshot).await.unwrap();
        let tally = scan_live(ReadHandle::Tx(&tx), Subspace::Index)
            .await
            .unwrap();
        tx.rollback();

        assert_eq!(tally, LiveTally::default());
    }
}

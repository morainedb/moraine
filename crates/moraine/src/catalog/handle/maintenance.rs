//! Store census, maintenance status, reclamation, and compaction.

use std::{collections::HashSet, sync::Arc};

use futures::{StreamExt, TryStreamExt, stream};

use super::{Catalog, ReadOnlyCatalog};
use crate::{
    catalog::{
        IndexId, Timestamp,
        census::{
            CensusRequest, CompactStoreReport, CompactStoreRequest, CompactionTarget, LiveCount,
            MergeOutcome, StoreCensus, StoreObjects, SubspaceCensus, SubspaceMerge, SubspaceName,
        },
    },
    error::{Error, Result},
    store::{
        StagedBytes,
        census::{self as store_census, SegmentSize},
        compaction::{self as store_compaction, MergeEnd},
        handle::{ReadHandle, ScanShape},
        key::{
            IndexKey, IndexKind, Key, Subspace, index_index_prefix, index_kind_prefix,
            subspace_prefix,
        },
    },
    transaction::{commit, index_maintenance, maintenance_status},
};

/// One subspace's row, zeroed when the manifest carries no segment for it.
fn measure(subspace: SubspaceName, segment: Option<&SegmentSize>) -> SubspaceCensus {
    SubspaceCensus {
        subspace,
        bytes: segment.map_or(0, |segment| segment.bytes),
        l0_ssts: segment.map_or(0, |segment| segment.l0_ssts),
        sorted_runs: segment.map_or(0, |segment| segment.sorted_runs),
        sorted_run_ssts: segment.map_or(0, |segment| segment.sorted_run_ssts),
        live: None,
    }
}

/// Counts the live entries of each measured subspace under one read
/// session.
async fn count_live_entries(
    handle: ReadHandle<'_>,
    subspaces: &mut [SubspaceCensus],
) -> Result<()> {
    if subspaces.is_empty() {
        return Ok(());
    }

    let concurrency = subspaces.len();
    stream::iter(subspaces.iter_mut().filter_map(|measured| {
        // An unknown segment addresses no keys this build can decode, so
        // there is nothing to scan and no count to report.
        let subspace = measured.subspace.subspace()?;
        Some(async move {
            let tally = store_census::scan_live(handle, subspace).await?;
            measured.live = Some(LiveCount {
                keys: tally.keys,
                key_bytes: tally.key_bytes,
                value_bytes: tally.value_bytes,
                scheduled_files: tally.scheduled_files,
            });

            Ok(())
        })
    }))
    .buffer_unordered(concurrency)
    .try_collect()
    .await
}

/// The physical bytes `census` recorded for `subspace`, or zero if it
/// carries no such subspace.
fn bytes_of(census: &StoreCensus, subspace: &SubspaceName) -> u64 {
    census
        .subspaces
        .iter()
        .find(|measured| &measured.subspace == subspace)
        .map_or(0, |measured| measured.bytes)
}

/// What a maintenance pass should reclaim.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct MaintenanceRequest {
    /// Reclaim the entry ranges of indexes that are no longer live —
    /// orphaned by `drop_index`, or by a `drop_table` that ended the
    /// table's indexes with it.
    pub sweep_orphaned_index_entries: bool,
    /// Maximum entries deleted per commit. Each batch is one atomic
    /// write; the pass yields between them so a large reclamation never
    /// holds the writer. Must be nonzero.
    pub batch_size: usize,
}

impl Default for MaintenanceRequest {
    fn default() -> Self {
        Self {
            sweep_orphaned_index_entries: true,
            batch_size: 1024,
        }
    }
}

/// What a maintenance pass reclaimed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct MaintenanceReport {
    /// Dead indexes whose entry ranges were reclaimed.
    pub indexes_swept: u64,
    /// Entry keys deleted across those ranges.
    pub index_entries_reclaimed: u64,
}

/// One step reported by a completed maintenance pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceStatusStep {
    /// Maintenance operation name.
    pub step: String,
    /// Outcome name, such as `ran`, `skipped`, or `failed`.
    pub status: String,
    /// Human-readable outcome detail.
    pub detail: String,
}

impl MaintenanceStatusStep {
    /// Builds one reported maintenance step.
    #[must_use]
    pub fn new(
        step: impl Into<String>,
        status: impl Into<String>,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            step: step.into(),
            status: status.into(),
            detail: detail.into(),
        }
    }
}

/// One completed maintenance pass in the catalog's durable status history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceStatusPass {
    /// When the pass began.
    pub started_at: Timestamp,
    /// What triggered the pass, such as `scheduled` or `manual`.
    pub trigger: String,
    /// Steps in execution order.
    pub steps: Vec<MaintenanceStatusStep>,
}

impl MaintenanceStatusPass {
    /// Builds one completed maintenance pass.
    #[must_use]
    pub fn new(
        started_at: Timestamp,
        trigger: impl Into<String>,
        steps: Vec<MaintenanceStatusStep>,
    ) -> Self {
        Self {
            started_at,
            trigger: trigger.into(),
            steps,
        }
    }
}

impl ReadOnlyCatalog {
    /// Returns the last 16 completed maintenance passes, newest first.
    ///
    /// Status is stored in the catalog, so a read-only attach and a process
    /// that reopens the store see passes completed by the previous writer.
    /// Recording status does not create a DuckLake snapshot.
    ///
    /// # Errors
    ///
    /// Returns a store or decoding error if the durable status cannot be
    /// read.
    pub async fn maintenance_status(&self) -> Result<Vec<MaintenanceStatusPass>> {
        maintenance_status::read(self).await
    }

    /// What the store weighs, subspace by subspace.
    ///
    /// The default request reads the store's manifest and nothing else —
    /// two object reads, a cost independent of how large the store is — and
    /// reports physical bytes, SST counts, and sorted-run counts per
    /// subspace. Those figures include superseded versions and tombstones,
    /// which is the point: the gap between them and the live count is what
    /// [`compact_store`](Catalog::compact_store) reclaims.
    ///
    /// Setting [`CensusRequest::count_live_entries`] adds a scan of every
    /// subspace, which costs a full read of the store.
    ///
    /// Available on a read-only catalog: both legs read, neither writes.
    ///
    /// # Errors
    ///
    /// Returns an error if the manifest or, for the scanning leg, the store
    /// cannot be read.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use moraine::{Catalog, CatalogOptions, CensusRequest, SubspaceName};
    /// # use object_store::memory::InMemory;
    /// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
    /// let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default()).await?;
    /// catalog.commit(|tx| tx.create_schema("sales").map(|_| ())).await?;
    ///
    /// let census = catalog.store_census(CensusRequest::default()).await?;
    /// // Physical figures count what has been written out, so a store whose
    /// // commits are still in the write-ahead log reports nothing yet.
    /// assert!(census.total_bytes() >= 0);
    /// # Ok::<(), moraine::Error>(()) }).unwrap();
    /// ```
    pub async fn store_census(&self, request: CensusRequest) -> Result<StoreCensus> {
        let physical = store_census::read_manifest_census(
            &self.location.path,
            Arc::clone(&self.location.object_store),
        )
        .await?;

        // Every subspace is reported, whether or not the manifest carries a
        // segment for it: a subspace absent from the manifest is one whose
        // writes have not been written out, which is a measurement rather
        // than a reason to omit the row. Two censuses of one store are then
        // always comparable row by row.
        let mut subspaces: Vec<SubspaceCensus> = Subspace::ALL
            .into_iter()
            .map(|subspace| {
                let prefix = subspace_prefix(subspace);
                measure(SubspaceName::from(subspace), physical.segment(&prefix))
            })
            .collect();
        subspaces.extend(
            physical
                .segments
                .iter()
                .filter(|segment| {
                    matches!(
                        SubspaceName::of_prefix(&segment.prefix),
                        SubspaceName::Unknown(_)
                    )
                })
                .map(|segment| measure(SubspaceName::of_prefix(&segment.prefix), Some(segment))),
        );

        if request.count_live_entries {
            // One session for every subspace, so the counts are one
            // consistent cut rather than a sequence of unrelated ones.
            let session = self.begin_read().await?;
            let counted = count_live_entries(session.handle(), &mut subspaces).await;
            session.finish();
            counted?;
        }

        Ok(StoreCensus {
            manifest_id: physical.manifest_id,
            subspaces,
            objects: physical.objects.map(|totals| StoreObjects {
                total_objects: totals.total_objects,
                total_bytes: totals.total_bytes,
                wal_objects: totals.wal_objects,
                wal_bytes: totals.wal_bytes,
                manifest_objects: totals.manifest_objects,
                manifest_bytes: totals.manifest_bytes,
                sst_objects: totals.sst_objects,
                sst_bytes: totals.sst_bytes,
                other_objects: totals.other_objects,
                other_bytes: totals.other_bytes,
            }),
        })
    }

    /// The lowest index id at or after `from` holding an entry of `kind`,
    /// or `None` past the last one. One seek per distinct index present —
    /// the scan stops at the first key rather than walking the range.
    pub(crate) async fn first_index_id_from(
        &self,
        kind: IndexKind,
        from: u64,
    ) -> Result<Option<u64>> {
        let kind_prefix = index_kind_prefix(kind);
        let start = index_index_prefix(kind, from);
        // `scan_prefix` takes its bounds as a suffix of the prefix.
        let suffix = start[kind_prefix.len()..].to_vec();

        let session = self.begin_read().await?;
        let first = session
            .handle()
            .scan_prefix(kind_prefix, suffix.., ScanShape::Probe)
            .await
            .map_err(Error::from)?
            .next()
            .await
            .map_err(Error::from)?;
        session.finish();

        let Some(entry) = first else {
            return Ok(None);
        };
        match Key::decode(&entry.key)? {
            Key::Index(IndexKey::Unique { index_id, .. } | IndexKey::Multi { index_id, .. }) => {
                Ok(Some(index_id))
            }
            other => Err(Error::Corruption(format!(
                "key in the index subspace decoded as {other:?}"
            ))),
        }
    }
}

impl Catalog {
    /// Durably records a completed maintenance pass.
    ///
    /// The history retains the newest 16 passes. This is an unversioned
    /// catalog update: it does not advance the DuckLake snapshot head.
    ///
    /// # Errors
    ///
    /// Returns an error if the status cannot be encoded or durably written.
    pub async fn record_maintenance_pass(&self, pass: MaintenanceStatusPass) -> Result<()> {
        maintenance_status::record(self, pass).await
    }

    /// Deletes up to `limit` orphaned entries of a dropped index, in one
    /// bounded batch outside the commit protocol (entries are not catalog
    /// entities, and the dropping commit's batch must stay bounded). Returns
    /// the number deleted; a host loops until it returns 0. Index ids are
    /// never reused, so a concurrent create cannot collide with a sweep.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Constraint`] if the index is still live (reclaiming
    /// a live index's entries would corrupt it), or a store error.
    pub async fn reclaim_index_entries(&self, index: IndexId, limit: usize) -> Result<usize> {
        let head = self.snapshot().await?;
        if head
            .indexes
            .values()
            .any(|per_table| per_table.contains_key(&index.get()))
        {
            return Err(Error::Constraint(format!(
                "index {index} is still live; drop it before reclaiming its entries"
            )));
        }

        let tx = self.begin_write_tx().await?;
        let mut staged = StagedBytes::default();
        let deleted =
            index_maintenance::reclaim_entries(&tx, index.get(), limit, &mut staged).await?;
        commit::commit_durable(tx, "entry reclamation", staged)
            .await
            .map_err(Error::from)?;

        Ok(deleted)
    }

    /// Runs one maintenance pass, reclaiming what only moraine knows is
    /// dead, and reports what it did.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Constraint`] on a read-only catalog,
    /// [`Error::Configuration`] for a zero `batch_size`, or a store error.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use moraine::{Catalog, CatalogOptions, MaintenanceRequest};
    /// # use object_store::memory::InMemory;
    /// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
    /// let catalog = Catalog::open(Arc::new(InMemory::new()), CatalogOptions::default()).await?;
    /// // A fresh catalog has nothing to reclaim.
    /// let report = catalog.maintain(MaintenanceRequest::default()).await?;
    /// assert_eq!(report.index_entries_reclaimed, 0);
    /// # Ok::<(), moraine::Error>(()) }).unwrap();
    /// ```
    pub async fn maintain(&self, request: MaintenanceRequest) -> Result<MaintenanceReport> {
        // Refuse before doing anything, including before the
        // nothing-to-do shortcut: a pass that reclaims nothing is still a
        // pass, and answering it differently on a read-only catalog would
        // make the outcome depend on the request rather than the handle.
        self.writer()?;
        if request.batch_size == 0 {
            return Err(Error::Configuration(
                "batch_size must be nonzero; zero would reclaim nothing and never terminate"
                    .to_string(),
            ));
        }

        let mut report = MaintenanceReport::default();
        if !request.sweep_orphaned_index_entries {
            return Ok(report);
        }

        // Index ids come from the monotonic catalog-id counter and are
        // never reused, so an id absent from this view can never become
        // live again: deciding liveness once, here, is sound for the
        // whole pass however long it runs.
        let live: HashSet<u64> = self
            .snapshot()
            .await?
            .indexes
            .values()
            .flat_map(|per_table| per_table.keys().copied())
            .collect();

        for kind in [IndexKind::Unique, IndexKind::Multi] {
            let mut from = 0u64;
            while let Some(index_id) = self.first_index_id_from(kind, from).await? {
                if !live.contains(&index_id) {
                    let reclaimed = self
                        .reclaim_dead_range(kind, index_id, request.batch_size)
                        .await?;
                    if reclaimed > 0 {
                        report.indexes_swept += 1;
                        report.index_entries_reclaimed += reclaimed;
                    }
                }
                // Seek past this index rather than walking its entries.
                match index_id.checked_add(1) {
                    Some(next) => from = next,
                    None => break,
                }
            }
        }

        Ok(report)
    }

    /// Merges each targeted subspace's sorted runs into one, reclaiming the
    /// superseded versions and tombstones they hold.
    ///
    /// moraine plans nothing: SlateDB decides which runs merge into which,
    /// and the plan it makes for a whole tree destines that tree's bottom
    /// run — which is what permits dropping a tombstone rather than carrying
    /// it forward. A subspace with no sorted runs is skipped, as is one
    /// already being merged.
    ///
    /// With [`CompactStoreRequest::wait`] set, the call returns once every
    /// submitted merge has committed or failed. A merge that outlives the
    /// wait is **not** cancelled: it keeps running, is reported
    /// [`MergeOutcome::Pending`], and a later census shows the result.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Constraint`] if the catalog was opened read-only —
    /// the compactor that executes a submitted merge runs inside the writer,
    /// so a reader would queue work nothing would run — or a store error if
    /// the merge cannot be submitted.
    pub async fn compact_store(&self, request: CompactStoreRequest) -> Result<CompactStoreReport> {
        self.writer()?;

        let target = match &request.target {
            CompactionTarget::WholeStore => None,
            CompactionTarget::Subspace(name) => match name.subspace() {
                Some(subspace) => Some(subspace_prefix(subspace)),
                // An unknown subspace addresses no keys, so there is no
                // tree to name in a request.
                None => {
                    return Err(Error::Configuration(format!(
                        "{name} is not a subspace this build can merge"
                    )));
                }
            },
        };

        let before = self.store_census(CensusRequest::default()).await?;
        let submitted = store_compaction::submit_full_merge(
            &self.location.path,
            Arc::clone(&self.location.object_store),
            target.as_deref(),
        )
        .await?;

        let outcomes = if submitted.is_empty() {
            Vec::new()
        } else if let Some(budget) = request.wait {
            let mut outcomes: Vec<_> =
                stream::iter(submitted.iter().enumerate().map(|(position, merge)| {
                    let object_store = Arc::clone(&self.location.object_store);
                    async move {
                        store_compaction::await_merge(
                            &self.location.path,
                            object_store,
                            &merge.compaction,
                            budget,
                        )
                        .await
                        .map(|outcome| (position, outcome))
                    }
                }))
                .buffer_unordered(submitted.len())
                .try_collect()
                .await?;
            outcomes.sort_unstable_by_key(|(position, _)| *position);
            outcomes
                .into_iter()
                .map(|(_, outcome)| Some(outcome))
                .collect()
        } else {
            vec![None; submitted.len()]
        };

        let mut merges = Vec::new();
        for (merge, outcome) in submitted.iter().zip(outcomes) {
            let subspace = SubspaceName::of_prefix(&merge.segment);
            let outcome = match outcome {
                None => MergeOutcome::Pending,
                Some(outcome) => match outcome {
                    MergeEnd::Completed => MergeOutcome::Completed,
                    MergeEnd::Failed => {
                        MergeOutcome::Failed("the merge ended without committing".to_string())
                    }
                    MergeEnd::Pending => MergeOutcome::Pending,
                },
            };

            merges.push(SubspaceMerge {
                subspace: subspace.clone(),
                outcome,
                bytes_before: bytes_of(&before, &subspace),
                bytes_after: None,
            });
        }

        // Every subspace the request covered but nothing was submitted for
        // is reported rather than dropped, so two calls stay comparable.
        for measured in &before.subspaces {
            let covered = target
                .as_ref()
                .is_none_or(|prefix| SubspaceName::of_prefix(prefix) == measured.subspace);
            if !covered || merges.iter().any(|m| m.subspace == measured.subspace) {
                continue;
            }
            // The only reason a plan omits a tree: L0 SSTs are not
            // eligible sources, so a tree without sorted runs has nothing
            // to merge. A tree already being merged is adopted rather than
            // omitted, so it never reaches here.
            merges.push(SubspaceMerge {
                subspace: measured.subspace.clone(),
                outcome: MergeOutcome::Skipped("no sorted runs to merge"),
                bytes_before: measured.bytes,
                bytes_after: None,
            });
        }

        if merges
            .iter()
            .any(|merge| merge.outcome == MergeOutcome::Completed)
        {
            let after = self.store_census(CensusRequest::default()).await?;
            for merge in &mut merges {
                if merge.outcome == MergeOutcome::Completed {
                    merge.bytes_after = Some(bytes_of(&after, &merge.subspace));
                }
            }
        }

        Ok(CompactStoreReport { merges })
    }

    /// Deletes every entry of one dead index, `batch_size` per commit,
    /// returning the total. The caller has already established that the
    /// index is not live.
    async fn reclaim_dead_range(
        &self,
        kind: IndexKind,
        index_id: u64,
        batch_size: usize,
    ) -> Result<u64> {
        let mut total = 0u64;
        // Each batch resumes where the last one stopped. Restarting at
        // the range's beginning would make every batch step over the
        // tombstones its predecessors left, which is quadratic in the
        // size of the range.
        let mut cursor: Option<Vec<u8>> = None;
        loop {
            let tx = self.begin_write_tx().await?;
            // Non-durable below, so nothing reads the staged size.
            let mut staged = StagedBytes::default();
            let (deleted, last) = index_maintenance::reclaim_entries_from(
                &tx,
                kind,
                index_id,
                batch_size,
                cursor.as_deref(),
                &mut staged,
            )
            .await?;
            if deleted == 0 {
                tx.rollback();
                return Ok(total);
            }
            // Batches commit non-durably: awaiting a flush tick per batch
            // makes the whole sweep flush-bound, and durability buys nothing
            // here. A dead index id is never reused and the deletes are
            // idempotent, so a batch lost to a crash simply leaves entries a
            // later pass rediscovers.
            tx.commit_with_options(&commit::non_durable())
                .await
                .map_err(Error::from)?;
            total += deleted as u64;
            cursor = last;
        }
    }
}

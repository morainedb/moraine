//! Store census, maintenance status, reclamation, and compaction.

use std::{collections::HashSet, sync::Arc};

use futures::{StreamExt, TryStreamExt, stream};
use slatedb::{Db, IsolationLevel};

use super::{Catalog, ReadOnlyCatalog};
use crate::{
    catalog::{
        IndexId, Timestamp,
        census::{
            CensusRequest, CompactStoreReport, CompactStoreRequest, CompactionTarget, LiveCount,
            MergeOutcome, StoreCensus, StoreObjects, SubspaceCensus, SubspaceMerge, SubspaceName,
        },
        projection::invalidate_current_state,
    },
    error::{Error, Result},
    store::{
        StagedBytes,
        census::{self as store_census, SegmentSize},
        compaction::{self as store_compaction, MergeEnd},
        handle::{ReadHandle, ScanShape},
        key::{
            EntityKey, EntityKind, IndexKey, IndexKind, Key, Subspace, history_entity_kind_prefix,
            index_index_prefix, index_kind_prefix, subspace_prefix,
        },
    },
    transaction::{commit, folder, index_maintenance, maintenance_status},
};

/// One subspace's row, zeroed when the manifest carries no segment for it.
fn measure(subspace: SubspaceName, segment: Option<&SegmentSize>) -> SubspaceCensus {
    SubspaceCensus {
        subspace,
        bytes: segment.map_or(0, |segment| segment.bytes),
        filter_bytes: segment.map_or(0, |segment| segment.filter_bytes),
        index_bytes: segment.map_or(0, |segment| segment.index_bytes),
        stats_bytes: segment.map_or(0, |segment| segment.stats_bytes),
        l0_ssts: segment.map_or(0, |segment| segment.l0_ssts),
        sorted_runs: segment.map_or(0, |segment| segment.sorted_runs),
        sorted_run_ssts: segment.map_or(0, |segment| segment.sorted_run_ssts),
        live: None,
    }
}

/// Counts the live entries of every known subspace under one read session.
/// An unknown segment addresses no keys this build can decode, so it has
/// no count.
async fn count_live_entries(handle: ReadHandle<'_>) -> Result<Vec<(Subspace, LiveCount)>> {
    stream::iter(Subspace::ALL.into_iter().map(|subspace| async move {
        let tally = store_census::scan_live(handle, subspace).await?;
        let live = LiveCount {
            keys: tally.keys,
            key_bytes: tally.key_bytes,
            value_bytes: tally.value_bytes,
            scheduled_files: tally.scheduled_files,
        };

        Ok((subspace, live))
    }))
    .buffer_unordered(Subspace::ALL.len())
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

fn verify_merges(merges: &[SubspaceMerge]) -> Result<()> {
    let Some(merge) = merges.iter().find(|merge| {
        matches!(
            merge.outcome,
            MergeOutcome::Pending | MergeOutcome::Failed(_)
        )
    }) else {
        return Ok(());
    };
    let detail = match &merge.outcome {
        MergeOutcome::Pending => "still pending".to_owned(),
        MergeOutcome::Failed(detail) => detail.clone(),
        MergeOutcome::Completed | MergeOutcome::Skipped(_) => String::new(),
    };
    Err(Error::Constraint(format!(
        "store merge for {} did not complete: {detail}",
        merge.subspace
    )))
}

/// What a maintenance pass should reclaim.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct MaintenanceRequest {
    /// Reclaim the entry ranges of indexes that are no longer live —
    /// orphaned by `drop_index`, or by a `drop_table` that ended the
    /// table's indexes with it.
    pub sweep_orphaned_index_entries: bool,
    /// Reclaim file column statistics describing a data file no snapshot
    /// can still resolve.
    ///
    /// Statistics carry no snapshot of their own, so the one record is
    /// what every read of that file resolves through — including a
    /// time-travelling one, whose file record comes from history. They
    /// are therefore reclaimable only once the file is absent from both
    /// live state and history, which is where expiry leaves it.
    pub sweep_orphaned_file_column_stats: bool,
    /// Maximum entries deleted per commit. Must be nonzero.
    pub batch_size: usize,
}

impl Default for MaintenanceRequest {
    fn default() -> Self {
        Self {
            sweep_orphaned_index_entries: true,
            sweep_orphaned_file_column_stats: true,
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
    /// File column statistics deleted for unresolvable data files.
    pub file_column_stats_reclaimed: u64,
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
    /// The default request reads the store's manifest and nothing else, and
    /// reports physical bytes, SST counts, and sorted-run counts per
    /// subspace. Those figures include superseded versions and tombstones;
    /// the gap between them and the live count is what
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
        );
        let live = async {
            if !request.count_live_entries {
                return Ok(Vec::new());
            }
            // One session for every subspace: one consistent cut.
            let session = self.begin_read().await?;
            let counted = count_live_entries(session.handle()).await;
            session.finish();
            counted
        };
        let (physical, live) = futures::try_join!(physical, live)?;

        // Every subspace is reported, whether or not the manifest carries a
        // segment for it, so two censuses stay comparable row by row.
        let mut subspaces: Vec<SubspaceCensus> = Subspace::ALL
            .into_iter()
            .map(|subspace| {
                let prefix = subspace_prefix(subspace);
                let mut measured = measure(SubspaceName::from(subspace), physical.segment(&prefix));
                measured.live = live
                    .iter()
                    .find(|(counted, _)| *counted == subspace)
                    .map(|(_, live)| *live);
                measured
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

        // Through the dump read for its fresh reader: the sweep folds the tail
        // first and then reads the store, and the shared reader only refreshes
        // on its poll interval, so it can still be behind that fold. The
        // overlay is deliberately unused — what the fold left is the subject.
        let read = self.begin_dump().await?;
        let first = read
            .handle()
            .scan_prefix(kind_prefix, suffix.., ScanShape::Probe)
            .await
            .map_err(Error::from)?
            .next()
            .await
            .map_err(Error::from)?;
        read.finish().await;

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
    /// bounded batch outside the commit protocol. Returns the number
    /// deleted; a host loops until it returns 0.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Constraint`] if the index is still live, or a store
    /// error.
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

        // Dead entries live in the unfolded tail until a fold applies them,
        // and the sweep reads and deletes them through the folded store, so
        // the tail folds in first.
        let store = self.folder_store()?;
        folder::fold_sprint(store, u64::MAX).await?;

        self.with_writer(async |db| {
            let tx = db
                .begin(IsolationLevel::Snapshot)
                .await
                .map_err(Error::from)?;
            let mut staged = StagedBytes::default();
            let deleted =
                index_maintenance::reclaim_entries(&tx, index.get(), limit, &mut staged).await?;
            commit::commit_durable(db, tx, "entry reclamation", staged)
                .await
                .map_err(Error::from)?;

            Ok(deleted)
        })
        .await
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
    /// assert_eq!(report.file_column_stats_reclaimed, 0);
    /// # Ok::<(), moraine::Error>(()) }).unwrap();
    /// ```
    pub async fn maintain(&self, request: MaintenanceRequest) -> Result<MaintenanceReport> {
        // Refused before the nothing-to-do shortcut too.
        self.ensure_writable()?;
        if request.batch_size == 0 {
            return Err(Error::Configuration(
                "batch_size must be nonzero; zero would reclaim nothing and never terminate"
                    .to_string(),
            ));
        }

        let mut report = MaintenanceReport::default();
        if request.sweep_orphaned_file_column_stats {
            report.file_column_stats_reclaimed = self
                .reclaim_orphaned_file_column_stats(request.batch_size)
                .await?;
        }
        if !request.sweep_orphaned_index_entries {
            return Ok(report);
        }

        // Index ids are never reused, so liveness decided once holds for
        // the whole pass.
        let live: HashSet<u64> = self
            .snapshot()
            .await?
            .indexes
            .values()
            .flat_map(|per_table| per_table.keys().copied())
            .collect();

        // Dead entries live in the unfolded tail until a fold applies them,
        // and the sweep reads and deletes them through the folded store, so
        // the tail folds in first. Every read below is then store-only.
        let store = self.folder_store()?;
        folder::fold_sprint(store, u64::MAX).await?;

        // The entry deletions are derived-state upkeep in the index subspace,
        // never replayed into a view, so they run under the folder role: the
        // single direct writer of a slot-backed store.
        let swept = self
            .with_writer(async |db| {
                let mut swept = MaintenanceReport::default();
                for kind in [IndexKind::Unique, IndexKind::Multi] {
                    let mut from = 0u64;
                    while let Some(index_id) = self.first_index_id_from(kind, from).await? {
                        if !live.contains(&index_id) {
                            let reclaimed = self
                                .reclaim_dead_range(db, kind, index_id, request.batch_size)
                                .await?;
                            if reclaimed > 0 {
                                swept.indexes_swept += 1;
                                swept.index_entries_reclaimed += reclaimed;
                            }
                        }
                        // Seek past this index rather than walking its entries.
                        match index_id.checked_add(1) {
                            Some(next) => from = next,
                            None => break,
                        }
                    }
                }
                Ok(swept)
            })
            .await?;
        report.indexes_swept += swept.indexes_swept;
        report.index_entries_reclaimed += swept.index_entries_reclaimed;

        Ok(report)
    }

    /// Merges each targeted subspace's sorted runs into one, reclaiming the
    /// superseded versions and tombstones they hold.
    ///
    /// A subspace with no sorted runs is skipped, as is one already being
    /// merged.
    ///
    /// With [`CompactStoreRequest::wait`] set, the call returns once every
    /// submitted merge has committed or failed. A merge that outlives the
    /// wait is **not** cancelled: it keeps running, is reported
    /// [`MergeOutcome::Pending`], and a later census shows the result.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Constraint`] if the catalog was opened read-only, or
    /// a store error if the merge cannot be submitted.
    #[allow(
        clippy::too_many_lines,
        reason = "the method keeps one ordered submit, await, classify, and measure protocol"
    )]
    pub async fn compact_store(&self, request: CompactStoreRequest) -> Result<CompactStoreReport> {
        self.ensure_writable()?;
        if request.require_completed && request.wait.is_none() {
            return Err(Error::Configuration(
                "require_completed needs a nonzero wait budget".to_owned(),
            ));
        }

        let target = match &request.target {
            CompactionTarget::WholeStore => None,
            CompactionTarget::Subspace(name) => match name.subspace() {
                Some(subspace) => Some(subspace_prefix(subspace)),
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

        let mut merges: Vec<SubspaceMerge> = submitted
            .iter()
            .zip(outcomes)
            .map(|(merge, outcome)| {
                let subspace = SubspaceName::of_prefix(&merge.segment);
                let outcome = match outcome {
                    None | Some(MergeEnd::Pending) => MergeOutcome::Pending,
                    Some(MergeEnd::Completed) => MergeOutcome::Completed,
                    Some(MergeEnd::Failed) => {
                        MergeOutcome::Failed("the merge ended without committing".to_string())
                    }
                };
                SubspaceMerge {
                    bytes_before: bytes_of(&before, &subspace),
                    subspace,
                    outcome,
                    bytes_after: None,
                }
            })
            .collect();

        // Every subspace the request covered but nothing was submitted for
        // is reported rather than dropped, so two calls stay comparable.
        for measured in &before.subspaces {
            let covered = target
                .as_ref()
                .is_none_or(|prefix| SubspaceName::of_prefix(prefix) == measured.subspace);
            if !covered || merges.iter().any(|m| m.subspace == measured.subspace) {
                continue;
            }
            // A plan omits a tree only when it has no sorted runs; one
            // already being merged is adopted, not omitted.
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

        if request.require_completed {
            verify_merges(&merges)?;
        }

        Ok(CompactStoreReport { merges })
    }

    /// Deletes every entry of one dead index, `batch_size` per commit,
    /// returning the total. The caller has already established that the
    /// index is not live.
    /// Every `(table_id, data_file_id)` an ended data-file version names,
    /// which is what a time-travelling read resolves a dropped or replaced
    /// file through.
    async fn historical_data_files(&self) -> Result<HashSet<(u64, u64)>> {
        // Through the dump read, not a plain session: the shared reader only
        // refreshes on its poll interval, and a file ended by a commit this
        // handle just made lives in the unfolded tail until then. Reading the
        // store alone would call it unresolvable and reclaim its statistics.
        let dump = self.begin_dump().await?;
        let prefix = history_entity_kind_prefix(EntityKind::File);
        // One kind's prefix, not the whole subspace: every other kind's
        // history would be read and decoded only to be discarded below.
        let mut iter = dump
            .handle()
            .scan_prefix(prefix.clone(), .., ScanShape::Bulk)
            .await?;

        let mut seen = HashSet::new();
        let note = |key: &[u8], seen: &mut HashSet<(u64, u64)>| {
            if let Ok(Key::History(history)) = Key::decode(key)
                && let EntityKey::File {
                    table_id,
                    data_file_id,
                } = history.entity
            {
                seen.insert((table_id, data_file_id));
            }
        };
        while let Some(entry) = iter.next().await? {
            note(&entry.key, &mut seen);
        }
        for (key, value) in dump
            .overlay()
            .into_iter()
            .flat_map(|tail| tail.prefixed(&prefix))
        {
            if value.is_some() {
                note(key, &mut seen);
            }
        }
        dump.finish().await;

        Ok(seen)
    }

    /// Deletes the statistics of every data file absent from both live
    /// state and history, `batch_size` per commit, and returns how many
    /// records went.
    async fn reclaim_orphaned_file_column_stats(&self, batch_size: usize) -> Result<u64> {
        let snapshot = self.snapshot().await?;
        let historical = self.historical_data_files().await?;

        let mut victims = Vec::new();
        for (&table_id, per_file) in &snapshot.file_column_stats {
            let live = snapshot.data_files.get(&table_id);
            for &(data_file_id, column_id) in per_file.keys() {
                let resolvable = live.is_some_and(|files| files.contains_key(&data_file_id))
                    || historical.contains(&(table_id, data_file_id));
                if !resolvable {
                    victims.push(EntityKey::FileColumnStats {
                        table_id,
                        data_file_id,
                        column_id,
                    });
                }
            }
        }

        self.with_writer(async |db| {
            let mut total = 0u64;
            for batch in victims.chunks(batch_size) {
                let tx = db
                    .begin(IsolationLevel::Snapshot)
                    .await
                    .map_err(Error::from)?;
                for entity in batch {
                    tx.delete(Key::current(*entity).encode())
                        .map_err(Error::from)?;
                }
                // Non-durable: the deletes are idempotent, so a batch lost to
                // a crash leaves records a later pass rediscovers.
                tx.commit_with_options(&commit::non_durable())
                    .await
                    .map_err(Error::from)?;
                // This batch bypassed the commit protocol, so nothing folded
                // it into the maintained projections and the head stamp they
                // key on did not move. Unless they are dropped here, the
                // handle keeps serving the records the batch removed — and
                // every later pass rediscovers the same victims.
                invalidate_current_state(&self.projections);
                total += batch.len() as u64;
            }
            Ok(total)
        })
        .await
    }

    async fn reclaim_dead_range(
        &self,
        db: &Db,
        kind: IndexKind,
        index_id: u64,
        batch_size: usize,
    ) -> Result<u64> {
        let mut total = 0u64;
        // Each batch resumes past the tombstones the last one left.
        let mut cursor: Option<Vec<u8>> = None;
        loop {
            let tx = db
                .begin(IsolationLevel::Snapshot)
                .await
                .map_err(Error::from)?;
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
            // Non-durable: the deletes are idempotent, so a batch lost to a
            // crash leaves entries a later pass rediscovers.
            tx.commit_with_options(&commit::non_durable())
                .await
                .map_err(Error::from)?;
            total += deleted as u64;
            cursor = last;
        }
    }
}
